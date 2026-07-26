"""斗鱼直播监控器模块"""

import asyncio
import contextlib
import time
from collections.abc import Callable
from typing import Any

from aiodouyu import (
    EVENT_CONNECTED,
    EVENT_DISCONNECTED,
    DanmakuClient,
    RoomNotFound,
    fetch_room,
)
from astrbot.api import logger

# 状态对账固定用 betard 源:open 源无法识别视频轮播,会把轮播误判为开播
RESYNC_SOURCE = "betard"
RESYNC_TIMEOUT = 10.0
# 对账失败的重试退避:5/10/20/40s,封顶 60s,直到成功为止。
# 断连窗口内的转换只能靠对账补齐,单次失败即放弃会让"断连窗口丢通知"
# 的修复在 betard 偶发故障时留下缺口
RESYNC_RETRY_BASE = 5.0
RESYNC_RETRY_MAX = 60.0
# 对账返回"房间不存在"(封禁/删除)是确定性结论而非瞬时故障:
# 按长间隔复查(封禁常为临时),不按秒级退避打接口刷日志
RESYNC_ROOM_GONE_INTERVAL = 1800.0
# 冷却期待定状态的校准周期(秒)
RECONCILE_INTERVAL = 1.0
# stop() 等待消费协程退出的上限(秒)
STOP_TIMEOUT = 5.0


class DouyuMonitor:
    """斗鱼直播监控器(单协程 asyncio 模型)

    用 aiodouyu.DanmakuClient 订阅直播间 rss 状态消息,检测开播/下播
    并通过回调通知上层。

    状态对账:斗鱼只在状态**变化**时推送 rss,断连窗口内发生的转换不会
    在重连后重放——因此每次连接建立(EVENT_CONNECTED,含自动重连)后都
    用 HTTP 接口拉取当前状态喂入状态机,补上窗口内丢失的转换。

    并发模型:
        状态机只在事件循环上被触碰(消费协程与校准协程),所有状态更新
        都在无 await 点的同步代码块内完成,天然串行,无需加锁。
        回调在事件循环上同步调用。
    """

    def __init__(
        self,
        room_id: int,
        live_callback: Callable[[int, dict], None] | None = None,
        offline_callback: Callable[[int, float], None] | None = None,
        inherit_state: dict[str, Any] | None = None,
        client_factory: Callable[[], DanmakuClient] | None = None,
    ):
        """初始化监控器

        Args:
            room_id: 斗鱼直播间房间号
            live_callback: 开播回调函数,参数为 (room_id, msg)
            offline_callback: 下播回调函数,参数为 (room_id, duration_seconds)
            inherit_state: 重启时继承旧监控器的状态(export_state() 的返回值),
                避免对已播报过的直播间重复发送开播通知
            client_factory: 弹幕客户端工厂(测试注入用);默认创建
                只订阅 rss、产出连接事件的 DanmakuClient
        """
        self.room_id = room_id
        self.live_callback = live_callback
        self.offline_callback = offline_callback
        self.created_at = time.time()
        self._client_factory = client_factory or (
            lambda: DanmakuClient(
                room_id, types={"rss"}, emit_connection_events=True
            )
        )

        # ---- 直播状态机(仅事件循环线程访问,同步块内更新,无需锁)----
        # 使用 None 表示未知状态,避免首次消息误判
        self.last_live_status: bool | None = None
        self.live_start_time: float | None = None  # 开播时间戳
        self._has_announced_live = False  # 是否已发布开播通知
        # 上次通知时间,防止短时间内重复通知
        self._last_notify_time: float = 0.0
        self._notify_cooldown = 30.0  # 通知冷却时间(秒)
        # 冷却期内观测到的待定状态,冷却结束后由 _reconcile_pending 补处理
        self._pending_status: bool | None = None
        self._pending_msg: dict | None = None
        # 待定转换若来自对账观测,保留其开播时间戳供补发时修正时长基准
        self._pending_started_at: float | None = None

        if inherit_state:
            self.last_live_status = inherit_state.get("last_live_status")
            self.live_start_time = inherit_state.get("live_start_time")
            self._has_announced_live = bool(
                inherit_state.get("has_announced_live", False)
            )
            self._last_notify_time = float(inherit_state.get("last_notify_time", 0.0))
            # 待定转换必须一并继承:斗鱼只在状态变化时推送 rss,
            # 丢弃待定转换会让状态机与真实状态失步(连带吞掉下一条通知);
            # 失步兜底靠重连对账,但没必要靠兜底
            self._pending_status = inherit_state.get("pending_status")
            self._pending_msg = inherit_state.get("pending_msg")
            self._pending_started_at = inherit_state.get("pending_started_at")

        # ---- 生命周期 ----
        self._stop_flag = False
        self._client: DanmakuClient | None = None
        self._task: asyncio.Task | None = None
        # 弹幕连接是否就绪(EVENT_CONNECTED/EVENT_DISCONNECTED 驱动);
        # 供 /douyu ls 区分"运行中"与"重连中"——客户端无限退避重连时
        # 消费协程始终存活,仅靠 is_healthy 看不出连接已长期不通
        self.connected = False

        # ---- 对账调度(仅事件循环访问)----
        # 对账统一由校准协程串行执行(消费循环只登记请求):
        # 消费不被 HTTP 阻塞,也不存在两处并发调用 _resync 的问题
        self._resync_pending = False  # 有一次对账待执行
        self._resync_at = 0.0  # monotonic,早于此不执行
        self._resync_failures = 0
        self._resync_room_gone = False  # 上次对账返回房间不存在
        # 新鲜度基线:fetch 在途期间状态机前进(_obs_seq)或连接更替
        # (_conn_gen)时,带回的快照可能过期,须丢弃重拉
        self._obs_seq = 0
        self._conn_gen = 0

    # ==================== 状态查询 ====================

    @property
    def is_healthy(self) -> bool:
        """监控是否存活(消费协程在运行且未被停止)"""
        return (
            not self._stop_flag
            and self._task is not None
            and not self._task.done()
        )

    def export_state(self) -> dict[str, Any]:
        """导出直播状态,供重启时新监控器继承"""
        return {
            "last_live_status": self.last_live_status,
            "live_start_time": self.live_start_time,
            "has_announced_live": self._has_announced_live,
            "last_notify_time": self._last_notify_time,
            "pending_status": self._pending_status,
            "pending_msg": self._pending_msg,
            "pending_started_at": self._pending_started_at,
        }

    # ==================== 状态机 ====================

    def _apply_transition(
        self, is_live: bool, msg: dict, now: float
    ) -> tuple[Callable | None, tuple]:
        """执行一次状态转换

        Returns:
            (callback, args): 需要调用的回调及参数,无则 (None, ())
        """
        self._obs_seq += 1
        self._pending_status = None
        self._pending_msg = None
        self._pending_started_at = None
        self.last_live_status = is_live

        if is_live:
            logger.info(f"斗鱼直播间 {self.room_id} 开播了!")
            if self.live_start_time is None:
                self.live_start_time = now
            self._last_notify_time = now
            self._has_announced_live = True
            if self.live_callback:
                return self.live_callback, (self.room_id, msg)
            return None, ()

        logger.info(f"斗鱼直播间 {self.room_id} 下播了!")
        duration = 0.0
        if self.live_start_time:
            duration = now - self.live_start_time
            self.live_start_time = None
        announced = self._has_announced_live
        self._has_announced_live = False
        if announced:
            self._last_notify_time = now
            if self.offline_callback:
                return self.offline_callback, (self.room_id, duration)
        else:
            logger.debug(
                f"斗鱼直播间 {self.room_id} 检测到下播,但尚未发布开播通知,忽略"
            )
        return None, ()

    def _apply_observation(
        self, is_live: bool, msg: dict, started_at: float | None = None
    ) -> None:
        """把一次状态观测(rss 消息或 HTTP 对账结果)喂入状态机

        Args:
            is_live: 观测到的直播状态
            msg: 事件消息(对账结果用伪消息)
            started_at: 观测源提供的本场开播时间戳(仅对账结果有),
                用于修正跨断连窗口的直播时长
        """
        if self._stop_flag:
            return
        try:
            # 每次观测都推进序号:在途对账据此判定快照是否已过期
            self._obs_seq += 1
            now = time.time()
            callback: Callable | None = None
            args: tuple = ()

            if self.last_live_status is None:
                # 首次观测(未继承状态),若已开播则立即通知
                logger.info(
                    f"斗鱼直播间 {self.room_id} 当前状态: "
                    f"{'直播中' if is_live else '未开播'}"
                )
                self.last_live_status = is_live
                if is_live:
                    self.live_start_time = started_at or now
                    self._has_announced_live = True
                    self._last_notify_time = now
                    logger.info(f"斗鱼直播间 {self.room_id} 开播了! (初始状态)")
                    if self.live_callback:
                        callback = self.live_callback
                        args = (self.room_id, msg)
            elif is_live == self.last_live_status:
                # 状态回稳(等于当前状态),清除待定转换
                self._pending_status = None
                self._pending_msg = None
                self._pending_started_at = None
                # 对账源可修正开播时间(如断连窗口内先下播又开播的场景
                # 无法逐段还原,至少让时长基于最新一场)
                if is_live and started_at is not None:
                    self.live_start_time = started_at
            elif now - self._last_notify_time < self._notify_cooldown:
                # 冷却期内记为待定,冷却结束后由 _reconcile_pending 补处理。
                # 不能直接丢弃:直接丢弃会导致状态机与真实状态失步,
                # 连带丢失后续通知
                self._pending_status = is_live
                self._pending_msg = msg
                self._pending_started_at = started_at
                logger.debug(
                    f"斗鱼直播间 {self.room_id} 状态变化处于冷却期内,"
                    f"已记为待定状态,冷却结束后校准"
                )
            else:
                if is_live and started_at is not None:
                    self.live_start_time = started_at
                callback, args = self._apply_transition(is_live, msg, now)

            if callback:
                callback(*args)
        except Exception as e:
            logger.error(f"处理直播状态时出错: {e}", exc_info=True)

    def _rss_handler(self, msg: dict) -> None:
        """处理 rss 直播状态变化消息"""
        if self._stop_flag:
            return
        ss = msg.get("ss", "0")
        ivl = msg.get("ivl", "1")
        # ss='1' 表示正在直播, ivl='0' 表示不是视频轮播
        self._apply_observation(ss == "1" and ivl == "0", msg)

    def _reconcile_pending(self) -> None:
        """冷却结束后校准待定状态(由校准协程周期调用)"""
        if self._stop_flag:
            # 与其他状态机入口保持一致:stop 后不再产生任何回调
            return
        try:
            if self._pending_status is None:
                return
            now = time.time()
            if now - self._last_notify_time < self._notify_cooldown:
                return
            if self._pending_status == self.last_live_status:
                self._obs_seq += 1
                self._pending_status = None
                self._pending_msg = None
                self._pending_started_at = None
                return
            logger.info(f"斗鱼直播间 {self.room_id} 冷却结束,补发待定状态转换")
            if self._pending_status and self._pending_started_at is not None:
                # 补发开播时用观测源的开播时间,时长不含冷却延迟
                self.live_start_time = self._pending_started_at
            callback, args = self._apply_transition(
                self._pending_status, self._pending_msg or {}, now
            )
            if callback:
                callback(*args)
        except Exception as e:
            logger.error(f"校准待定状态时出错: {e}", exc_info=True)

    # ==================== 对账 ====================

    def _schedule_resync(self, delay: float = 0.0) -> None:
        """登记一次对账请求,由校准协程在到期后执行"""
        self._resync_pending = True
        self._resync_at = time.monotonic() + delay

    async def _resync(self) -> None:
        """与 HTTP 接口对账当前直播状态(仅由校准协程串行调用)

        断连窗口内的状态转换不会在重连后重放,这里主动拉取真实状态
        喂入状态机补齐。对账失败会按退避重试直到成功——单次失败即
        放弃会让断连窗口内的转换整场丢失。

        新鲜度校验:fetch 在途期间若状态机已前进(收到 rss)或连接已
        更替,带回的快照可能反映过期状态,直接应用会覆盖较新状态、
        甚至记出虚假的反向 pending;此时丢弃本次结果并立即重拉。
        """
        seq_before = self._obs_seq
        gen_before = self._conn_gen
        try:
            info = await fetch_room(
                self.room_id, source=RESYNC_SOURCE, timeout=RESYNC_TIMEOUT
            )
        except RoomNotFound as e:
            # 确定性结论:房间被封禁/删除。长间隔复查,状态变化时只告警一次
            self._schedule_resync(RESYNC_ROOM_GONE_INTERVAL)
            if not self._resync_room_gone:
                self._resync_room_gone = True
                logger.warning(
                    f"斗鱼直播间 {self.room_id} 对账返回房间不存在"
                    f"(可能被封禁或删除),改为每"
                    f" {RESYNC_ROOM_GONE_INTERVAL / 60:.0f} 分钟复查: {e}"
                )
            return
        except Exception as e:
            self._resync_failures += 1
            # 指数须钳制:计数无界增长时 2**n 的 int->float 转换会在
            # 约 1025 次连续失败后抛 OverflowError(2**6*5=320 已超过
            # RESYNC_RETRY_MAX,钳到 6 不改变退避语义)
            delay = min(
                RESYNC_RETRY_MAX,
                RESYNC_RETRY_BASE * (2 ** min(self._resync_failures - 1, 6)),
            )
            self._schedule_resync(delay)
            logger.warning(
                f"斗鱼直播间 {self.room_id} 开播状态对账失败"
                f"(第 {self._resync_failures} 次),{delay:.0f}s 后重试: {e}"
            )
            return
        self._resync_pending = False
        self._resync_failures = 0
        self._resync_room_gone = False
        if self._stop_flag:
            return
        if seq_before != self._obs_seq or gen_before != self._conn_gen:
            logger.debug(
                f"斗鱼直播间 {self.room_id} 对账快照已过期"
                f"(fetch 在途期间状态机前进或连接更替),丢弃并重拉"
            )
            self._schedule_resync()
            return
        self._apply_observation(
            info.is_live,
            {"type": "aiodouyu.resync", "roomid": str(self.room_id)},
            started_at=float(info.started_at) if info.started_at else None,
        )

    # ==================== 生命周期 ====================

    async def _reconcile_loop(self) -> None:
        """周期校准:冷却期待定状态补发 + 对账请求的串行执行

        循环体必须兜底异常:此协程静默死亡的话,pending 补发与对账
        全部失效而 is_healthy 仍为 True,watchdog 无从感知。
        """
        while True:
            await asyncio.sleep(RECONCILE_INTERVAL)
            try:
                self._reconcile_pending()
                if (
                    self._resync_pending
                    and not self._stop_flag
                    and time.monotonic() >= self._resync_at
                ):
                    await self._resync()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(
                    f"斗鱼直播间 {self.room_id} 校准协程出错: {e}", exc_info=True
                )

    async def _run(self) -> None:
        """消费协程:驱动弹幕客户端,分发事件到状态机"""
        client = self._client
        if client is None:  # start() 保证已创建;防御性检查
            return
        reconcile_task = asyncio.create_task(self._reconcile_loop())
        try:
            async for msg in client:
                if self._stop_flag:
                    break
                msg_type = msg.get("type")
                if msg_type == EVENT_CONNECTED:
                    self.connected = True
                    self._conn_gen += 1
                    # 对账登记给校准协程执行,不在消费循环内联 await:
                    # 消费不被 HTTP 阻塞(rss 照常处理,过期快照由
                    # 新鲜度校验丢弃),对账请求也不会因重试在途而丢失
                    self._schedule_resync()
                    logger.info(
                        f"斗鱼监控器 {self.room_id} 弹幕连接就绪,已登记状态对账"
                    )
                elif msg_type == EVENT_DISCONNECTED:
                    self.connected = False
                elif msg_type == "rss":
                    self._rss_handler(msg)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"斗鱼监控器 {self.room_id} 运行出错: {e}", exc_info=True)
        finally:
            self.connected = False
            reconcile_task.cancel()
            await asyncio.gather(reconcile_task, return_exceptions=True)
            with contextlib.suppress(Exception):
                await client.close()
            if not self._stop_flag:
                # 客户端自带重连,消费循环退出说明发生了意外异常,
                # 交给 watchdog 重建监控器
                logger.warning(
                    f"斗鱼监控器 {self.room_id} 消费循环退出,等待 watchdog 重启"
                )

    def start(self) -> bool:
        """启动监控(非阻塞,须在事件循环上调用)

        连接建立与重连由客户端在后台自理,启动即视为成功;
        运行期故障通过 is_healthy 暴露给 watchdog。
        """
        if self.is_healthy:
            return True
        if self._stop_flag:
            return False
        try:
            self._client = self._client_factory()
            self._task = asyncio.create_task(self._run())
        except Exception as e:
            logger.error(f"斗鱼监控器 {self.room_id} 启动失败: {e}", exc_info=True)
            return False
        logger.info(f"斗鱼监控器 {self.room_id} 已启动")
        return True

    async def stop(self) -> None:
        """停止监控:关闭客户端连接,等待消费协程退出"""
        self._stop_flag = True
        try:
            client = self._client
            if client is not None:
                with contextlib.suppress(Exception):
                    await client.close()
            task = self._task
            if task is not None and not task.done():
                # 用 asyncio.wait 而非 wait_for+shield:wait 不取消被等
                # 任务、超时不抛异常,也没有 shield 的歧义——若任务被
                # 第三方取消,shield 的 await 会抛 CancelledError,被外层
                # 误判为 stop() 自身被取消而向调用方虚假传播
                done, pending = await asyncio.wait({task}, timeout=STOP_TIMEOUT)
                if pending:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        except asyncio.CancelledError:
            # 调用方自身被取消(可能落在 close 或 wait 的任意挂起点):
            # 先把消费协程也取消掉再传播,避免任务与连接脱管
            if self._task is not None and not self._task.done():
                self._task.cancel()
            raise
        logger.info(f"斗鱼直播间 {self.room_id} 监控已停止")

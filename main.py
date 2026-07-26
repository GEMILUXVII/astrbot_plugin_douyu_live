"""斗鱼直播通知插件

支持多房间监控、订阅推送、@全体成员等功能。
"""

import asyncio
import time
from dataclasses import dataclass, field

from aiodouyu import ApiError, RoomNotFound, fetch_room
from astrbot.api import logger, star
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.core.star.filter.command import GreedyStr

from .core import DouyuMonitor, Notifier
from .models import RoomInfo
from .storage import DataManager
from .storage.session_log import MonitorStateStore, SessionLog
from .utils.ratelimit import RoomInfoCache
from .utils.text import sanitize_display_text

# 通知最大发送尝试次数（含首次发送，即最多重试 NOTIFY_MAX_RETRIES-1 轮）
NOTIFY_MAX_RETRIES = 5
# 通知队列容量上限（防止无界增长）
NOTIFY_QUEUE_MAX = 1000
# 队列处理轮询间隔（秒）
NOTIFY_POLL_INTERVAL = 0.5
# 重试退避：第 n 次重试前等待 NOTIFY_RETRY_BACKOFF_BASE * 3^(n-1) 秒，
# 上限 NOTIFY_RETRY_BACKOFF_MAX——5/15/45/120s，覆盖分钟级的平台抖动
# （此前重试间隔只有轮询周期 0.5s，5 次重试约 2.5s 内耗尽，形同虚设）
NOTIFY_RETRY_BACKOFF_BASE = 5.0
NOTIFY_RETRY_BACKOFF_MAX = 120.0
# 同一房间同类通知的去重窗口（秒）：重启交接等场景下吸收重复的开播/下播事件
NOTIFY_DEDUP_TTL = 10.0
# watchdog 检查间隔与新监控启动宽限期（秒）
WATCHDOG_INTERVAL = 60.0
WATCHDOG_STARTUP_GRACE = 30.0

# 配置默认值:_conf_schema.json 是 WebUI 载体,这里是运行时兜底
# (宿主未传 config、或旧宿主不支持 schema 时按默认运行)
DEFAULT_CONFIG = {
    "notify_enrich": True,
    "notify_cover": True,
    "catchup_announce": True,
    "notify_cooldown": 30,
    "subscribe_permission": "everyone",
    "session_log_retention_days": 90,
}


@dataclass
class PendingNotification:
    """待发送的通知

    2.2.0 起携带结构化事件而非成品消息:消息在队列处理器侧(事件循环、
    可 await)首次投递前构建,富化外呼不占用监控回调路径。
    message 字段保留兼容:非空时跳过构建直接发送。
    """
    subscriber_settings: dict[str, bool] = field(default_factory=dict)  # {umo -> at_all}
    message: str = ""
    retry_count: int = 0
    next_attempt_at: float = 0.0  # monotonic 时间，早于此不投递
    # ---- 结构化事件(kind 非空时生效)----
    kind: str = ""  # "live" | "offline"
    room_id: int = 0
    room_name: str = ""
    duration: float = 0.0  # offline: 直播时长(秒)
    event_ts: float = 0.0  # 事件发生时刻(epoch)
    cover_url: str | None = None  # 构建时填充


def _retry_backoff(retry_count: int) -> float:
    """第 retry_count 次重试的退避秒数"""
    return min(
        NOTIFY_RETRY_BACKOFF_MAX,
        NOTIFY_RETRY_BACKOFF_BASE * (3 ** (retry_count - 1)),
    )


class Main(star.Star):
    """斗鱼直播开播通知插件

    命令列表:
    - /douyu help - 命令帮助
    - /douyu add <房间号> [名称] - 添加监控直播间（管理员）
    - /douyu del <房间号> - 删除监控直播间（管理员）
    - /douyu ls - 查看监控列表
    - /douyu live - 查看当前在播房间
    - /douyu sub <房间号> - 订阅直播间开播通知
    - /douyu unsub <房间号> - 取消订阅
    - /douyu offline <房间号> [on/off] - 本群下播通知开关
    - /douyu mysub - 查看我的订阅
    - /douyu status - 查看监控状态
    - /douyu restart [房间号] - 重启监控（管理员）
    - /douyu atall <房间号> [on/off] - 设置@全体（管理员）
    """

    def __init__(self, context: star.Context, config=None) -> None:
        super().__init__(context)
        self.context = context
        # AstrBotConfig(dict 语义);宿主无 schema 支持时为 None
        self.conf = config if config is not None else {}

        # 初始化模块
        self.data = DataManager()
        self.notifier = Notifier(context)
        self.monitors: dict[int, DouyuMonitor] = {}
        # 外呼缓存:/douyu live 与通知富化共用,TTL 60s + 并发 5
        self._room_cache = RoomInfoCache()
        # 场次历史与监控状态快照
        self.sessions = SessionLog(
            self.data.data_dir,
            retention_days=int(self._cfg("session_log_retention_days")),
        )
        self._state_store = MonitorStateStore(self.data.data_dir)
        # 启动回灌的监控状态(load 后按房间一次性消费)
        self._boot_states: dict[int, dict] = {}

        # 通知队列：监控协程投递，队列处理任务串行发送。
        # 统一走队列保证发送顺序、限制并发，并承担带退避的重试。
        # 迁移到 aiodouyu 后全部组件都在事件循环上，无跨线程访问。
        self._notification_queue: asyncio.Queue[PendingNotification] = asyncio.Queue(
            maxsize=NOTIFY_QUEUE_MAX
        )
        self._queue_processor_task: asyncio.Task | None = None
        self._watchdog_task: asyncio.Task | None = None

        # 按房间串行化监控生命周期操作（start/stop/restart），
        # 防止 watchdog 与管理员命令并发操作同一房间导致监控器泄漏
        self._room_locks: dict[int, asyncio.Lock] = {}
        # 插件关停标志：置位后拒绝创建新监控
        self._closing = False

        # 通知去重表：{(kind, room_id) -> monotonic 时间}
        self._notify_dedup: dict[tuple[str, int], float] = {}

    async def initialize(self) -> None:
        """插件激活时启动所有监控"""
        self._closing = False
        # 回灌上次干净关停的监控状态(一次性),清理超期场次
        self._boot_states = await asyncio.to_thread(
            self._state_store.load_and_clear
        )
        await asyncio.to_thread(self.sessions.prune)
        # 启动通知队列处理与监控看门狗任务
        self._queue_processor_task = asyncio.create_task(
            self._process_notification_queue()
        )
        self._watchdog_task = asyncio.create_task(self._watchdog())

        # 启动所有已保存房间的监控（start 仅创建协程任务，不阻塞）
        room_ids = self.data.get_all_room_ids()
        results = await asyncio.gather(
            *(self._start_monitor(rid) for rid in room_ids)
        )
        started = sum(1 for ok in results if ok)
        logger.info(
            f"斗鱼直播通知插件已启动，成功启动 {started}/{len(room_ids)} 个直播间监控"
        )

    async def terminate(self) -> None:
        """插件禁用时停止所有监控"""
        # 先拒绝新监控的创建（正在 await 中的 restart/start 会在复查时放弃）
        self._closing = True
        # 停止后台任务
        for task in (self._queue_processor_task, self._watchdog_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._queue_processor_task = None
        self._watchdog_task = None

        # 并发停止所有监控;停止后导出状态快照供下次启动回灌
        monitor_map = dict(self.monitors)
        self.monitors.clear()
        if monitor_map:
            await asyncio.gather(
                *(m.stop() for m in monitor_map.values()), return_exceptions=True
            )
            await asyncio.to_thread(
                self._state_store.save,
                {rid: m.export_state() for rid, m in monitor_map.items()},
            )
        # 注：数据在每次变更时已即时保存，此处无需（也不应）再次保存
        logger.info("斗鱼直播通知插件已停止")

    # ==================== 监控管理 ====================

    def _cfg(self, key: str):
        """读配置项,缺失时回退默认值"""
        try:
            value = self.conf.get(key)
        except Exception:
            value = None
        return DEFAULT_CONFIG[key] if value is None else value

    def _new_monitor(
        self, room_id: int, inherit_state: dict | None = None
    ) -> DouyuMonitor:
        """创建监控器（统一回调装配）"""
        return DouyuMonitor(
            room_id,
            live_callback=self._on_live_start,
            offline_callback=self._on_live_end,
            inherit_state=inherit_state,
            notify_cooldown=float(self._cfg("notify_cooldown")),
            announce_initial_live=bool(self._cfg("catchup_announce")),
        )

    def _room_lock(self, room_id: int) -> asyncio.Lock:
        """获取房间级生命周期锁（惰性创建，仅事件循环线程访问）"""
        return self._room_locks.setdefault(room_id, asyncio.Lock())

    async def _start_monitor(self, room_id: int) -> bool:
        """启动单个房间的监控"""
        async with self._room_lock(room_id):
            if self._closing or not self.data.has_room(room_id):
                return False
            existing = self.monitors.pop(room_id, None)
            if existing and existing.is_healthy:
                self.monitors[room_id] = existing
                return True

            state = (
                existing.export_state()
                if existing
                else self._boot_states.pop(room_id, None)  # 启动回灌,一次性
            )
            if existing:
                # 覆盖前显式清理旧监控，即使它已不健康（幂等安全）
                await existing.stop()
                # await 期间世界可能已变化
                if self._closing or not self.data.has_room(room_id):
                    return False

            # start() 非阻塞：创建任务与装入字典之间无 await 点，
            # 不存在旧实现中协程被取消导致已启动监控脱管的窗口
            monitor = self._new_monitor(room_id, inherit_state=state)
            if not monitor.start():
                await monitor.stop()
                return False
            self.monitors[room_id] = monitor
            return True

    async def _stop_monitor(self, room_id: int) -> None:
        """停止单个房间的监控"""
        async with self._room_lock(room_id):
            monitor = self.monitors.pop(room_id, None)
            if monitor:
                await monitor.stop()

    async def _restart_monitor(self, room_id: int) -> bool:
        """重启单个房间的监控

        先停旧再启新：新监控在连接建立后会用 HTTP 接口对账开播状态，
        停机窗口内的状态转换由对账补齐，无需新旧重叠来兜底。
        状态在旧监控完全停止后导出，包含停止前最后时刻的转换
        （旧实现在交接窗口前导出快照，窗口内的转换会随旧监控消亡）。
        房间锁保证同一房间的生命周期操作串行（watchdog 与命令不互踩）。
        """
        async with self._room_lock(room_id):
            if self._closing or not self.data.has_room(room_id):
                return False
            old = self.monitors.pop(room_id, None)
            state = None
            if old:
                await old.stop()
                state = old.export_state()
                if self._closing or not self.data.has_room(room_id):
                    return False

            new = self._new_monitor(room_id, inherit_state=state)
            if not new.start():
                await new.stop()
                return False
            self.monitors[room_id] = new
            return True

    async def _watchdog(self) -> None:
        """监控看门狗：定期检测失效的监控并自动重启"""
        while True:
            try:
                await asyncio.sleep(WATCHDOG_INTERVAL)
                for room_id in self.data.get_all_room_ids():
                    monitor = self.monitors.get(room_id)
                    if monitor and monitor.is_healthy:
                        continue
                    # 新启动的监控给予宽限期，避免误判
                    if (
                        monitor
                        and time.time() - monitor.created_at
                        < WATCHDOG_STARTUP_GRACE
                    ):
                        continue
                    logger.warning(
                        f"[watchdog] 直播间 {room_id} 监控已失效，尝试自动重启"
                    )
                    if not await self._restart_monitor(room_id):
                        logger.error(
                            f"[watchdog] 直播间 {room_id} 自动重启失败，将在下轮重试"
                        )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"[watchdog] 出错: {e}", exc_info=True)

    # ==================== 通知管线 ====================

    async def _process_notification_queue(self) -> None:
        """处理通知队列的后台任务

        所有通知（监控协程投递）都经由此任务串行发送：
        保证发送顺序、限制并发，并对失败目标做指数退避的有限重试。
        """
        while True:
            try:
                await asyncio.sleep(NOTIFY_POLL_INTERVAL)

                # 先取空队列再处理；未到重试时间的项原样放回，下轮再看
                pending_items: list[PendingNotification] = []
                while True:
                    try:
                        pending_items.append(self._notification_queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break

                # 未到重试时间的项先同步放回（取空与放回之间无 await，
                # 不会与新投递竞争容量），到期项再逐个发送
                now = time.monotonic()
                due_items: list[PendingNotification] = []
                for item in pending_items:
                    if item.next_attempt_at > now:
                        self._notification_queue.put_nowait(item)
                    else:
                        due_items.append(item)

                for item in due_items:
                    if not item.message:
                        await self._build_notification_message(item)
                    # 多轮重试后降级:先弃封面图(可能被平台拒收),再弃 @全体
                    use_at_all = item.retry_count < 2
                    cover = item.cover_url if item.retry_count < 1 else None
                    failed = await self.notifier.send_to_subscribers(
                        item.subscriber_settings,
                        item.message,
                        use_at_all=use_at_all,
                        cover_url=cover,
                    )
                    if not failed:
                        continue
                    item.retry_count += 1
                    if item.retry_count >= NOTIFY_MAX_RETRIES:
                        logger.error(
                            f"通知发送失败，已达最大重试次数，"
                            f"放弃 {len(failed)} 个目标"
                        )
                        continue
                    # 只重试失败的目标，避免对已成功目标重复发送；
                    # 按指数退避安排下次尝试，扛住分钟级平台抖动
                    item.subscriber_settings = {
                        umo: item.subscriber_settings[umo] for umo in failed
                    }
                    backoff = _retry_backoff(item.retry_count)
                    item.next_attempt_at = time.monotonic() + backoff
                    try:
                        self._notification_queue.put_nowait(item)
                        logger.warning(
                            f"{len(failed)} 个目标发送失败，{backoff:.0f}s 后重试 "
                            f"({item.retry_count}/{NOTIFY_MAX_RETRIES})"
                        )
                    except asyncio.QueueFull:
                        logger.error("通知队列已满，放弃重试")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"通知队列处理器出错: {e}", exc_info=True)

    def _log_session_event(
        self,
        kind: str,
        room_id: int,
        event_ts: float,
        title: str = "",
        category: str = "",
        duration: float = 0.0,
    ) -> None:
        """场次事件落盘(fire-and-forget,失败只记日志)"""
        if not self.sessions.enabled:
            return
        if kind == "start":
            event = {"e": "start", "ts": event_ts, "title": title, "cat": category}
        else:
            event = {"e": "end", "ts": event_ts, "dur": round(duration, 1)}
        task = asyncio.create_task(
            asyncio.to_thread(self.sessions.append, room_id, event)
        )
        task.add_done_callback(lambda t: t.exception())  # 取回异常防未观察警告

    async def _build_notification_message(self, item: PendingNotification) -> None:
        """在队列侧构建通知文本(首次投递前调用一次)

        开播通知按配置富化(标题/分类/封面):外呼走 TTL 缓存 + 并发
        限制,失败降级为基础文本,不阻塞、不失败。
        """
        if item.kind == "live":
            title = category = None
            if self._cfg("notify_enrich"):
                try:
                    info = await self._room_cache.get(item.room_id, timeout=5.0)
                    title, category = info.title, info.category
                    if self._cfg("notify_cover"):
                        item.cover_url = info.cover_url
                except Exception as e:
                    logger.warning(
                        f"直播间 {item.room_id} 通知富化失败,降级为基础文本: {e}"
                    )
            item.message = self.notifier.build_notification(
                item.room_id,
                item.room_name,
                timestamp=item.event_ts or None,
                title=title,
                category=category,
            )
            # 场次落盘:开播事件(带富化到的标题/分类)
            self._log_session_event(
                "start", item.room_id, item.event_ts,
                title=title or "", category=category or "",
            )
        elif item.kind == "offline":
            item.message = self.notifier.build_offline_notification(
                item.room_id,
                item.room_name,
                item.duration,
                timestamp=item.event_ts or None,
            )
            self._log_session_event(
                "end", item.room_id, item.event_ts, duration=item.duration
            )

    def _schedule_notification(
        self,
        subscriber_settings: dict[str, bool],
        message: str = "",
        dedup_key: tuple[str, int] | None = None,
        **fields,
    ) -> None:
        """调度通知发送（仅在事件循环上调用）

        Args:
            subscriber_settings: {umo -> at_all} 每个订阅者的 @全体设置
            message: 预构建消息(留空则按 fields 的结构化事件在队列侧构建)
            dedup_key: (类型, 房间号)。同键通知在 NOTIFY_DEDUP_TTL 秒内
                只投递一次——吸收重启交接等场景产生的重复事件
            **fields: PendingNotification 的结构化事件字段
                (kind/room_id/room_name/duration/event_ts)
        """
        if not subscriber_settings:
            return
        now = time.monotonic()
        if dedup_key is not None:
            last = self._notify_dedup.get(dedup_key, 0.0)
            if now - last < NOTIFY_DEDUP_TTL:
                logger.debug(
                    f"通知去重: {dedup_key} 在 {NOTIFY_DEDUP_TTL}s 窗口内重复，忽略"
                )
                return
        try:
            self._notification_queue.put_nowait(
                PendingNotification(
                    subscriber_settings=subscriber_settings,
                    message=message,
                    **fields,
                )
            )
        except asyncio.QueueFull:
            logger.error("通知队列已满，丢弃一条通知")
            return
        # 入队成功才记录去重时间戳：若先记后投、投递因队满失败，
        # 同事件在窗口内的补发会被误吸收
        if dedup_key is not None:
            self._notify_dedup[dedup_key] = now
            # 顺手清理过期条目，字典规模上界为 2×房间数
            for key in [
                k for k, t in self._notify_dedup.items()
                if now - t >= NOTIFY_DEDUP_TTL
            ]:
                del self._notify_dedup[key]

    # ==================== 监控回调（运行于事件循环）====================

    def _on_live_start(self, room_id: int, msg: dict) -> None:
        """开播回调 - 登记结构化事件,消息在队列侧构建(含富化)"""
        sub_configs = self.data.get_all_subscription_configs(room_id)
        if not sub_configs:
            return

        room_info = self.data.get_room(room_id)
        room_name = room_info.name if room_info else f"房间{room_id}"

        subscriber_settings = {
            umo: config.at_all for umo, config in sub_configs.items()
        }
        self._schedule_notification(
            subscriber_settings,
            dedup_key=("live", room_id),
            kind="live",
            room_id=room_id,
            room_name=room_name,
            event_ts=time.time(),
        )

    def _on_live_end(self, room_id: int, duration_seconds: float) -> None:
        """下播回调 - 发送下播通知给所有订阅者

        Args:
            room_id: 房间号
            duration_seconds: 直播时长（秒）
        """
        sub_configs = self.data.get_all_subscription_configs(room_id)
        if not sub_configs:
            return

        room_info = self.data.get_room(room_id)
        room_name = room_info.name if room_info else f"房间{room_id}"

        # 下播通知不 @全体;且只发给未关闭下播通知的群
        subscriber_settings = {
            umo: False
            for umo, config in sub_configs.items()
            if getattr(config, "offline_notify", True)
        }

        self._schedule_notification(
            subscriber_settings,
            dedup_key=("offline", room_id),
            kind="offline",
            room_id=room_id,
            room_name=room_name,
            duration=duration_seconds,
            event_ts=time.time(),
        )

    # ==================== 命令辅助 ====================

    def _save_warning(self) -> str:
        """数据保存失败时附加到回复中的警告"""
        if self.data.last_save_ok:
            return ""
        return "\n注意: 数据保存失败，重启后此更改可能丢失，请检查磁盘空间与文件权限"

    async def _resolve_toggle(
        self, room_id: int, umo: str, attr: str, enable: str
    ) -> tuple[RoomInfo | None, bool, str | None]:
        """解析并应用订阅级开关命令的公共逻辑

        Args:
            room_id: 房间号
            umo: unified_msg_origin
            attr: SubscriptionConfig 上的字段名
            enable: "on"/"off"/空（空表示切换；其他值报错而非静默切换）

        Returns:
            (房间信息, 新状态, 错误消息)；错误消息非 None 时前两项无效
        """
        room_info = self.data.get_room(room_id)
        if not room_info:
            return None, False, f"注意: 直播间 {room_id} 不在监控列表中"

        sub_config = self.data.get_subscription_config(room_id, umo)
        if not sub_config:
            return None, False, (
                f"注意: 当前群还没有订阅直播间 {room_id}\n"
                f"请先使用 /douyu sub {room_id} 订阅"
            )

        enable = enable.strip().lower()
        if enable == "on":
            new_status = True
        elif enable == "off":
            new_status = False
        elif enable == "":
            new_status = not getattr(sub_config, attr)
        else:
            # 不认识的参数不能当"切换"处理——用户想强制开启时误触发
            # 取反会得到与预期相反的结果
            return None, False, f"注意: 无法识别的参数「{enable}」，请使用 on/off 或留空切换"

        # 保存含磁盘 I/O，移出事件循环执行
        await asyncio.to_thread(
            self.data.update_subscription_config, room_id, umo, **{attr: new_status}
        )
        return room_info, new_status, None

    def _subscribe_gate(self, event: AstrMessageEvent) -> str | None:
        """订阅类命令的权限档位检查;返回错误文案或 None(放行)

        运行时检查而非 @filter.permission_type:装饰器是静态的,
        无法跟随配置切换。
        """
        if self._cfg("subscribe_permission") != "admin":
            return None
        try:
            if event.is_admin():
                return None
        except Exception:
            return None  # 宿主无此 API 时不拦截,宁可放行
        return "注意: 当前实例已将订阅操作限制为管理员,请联系管理员代为操作"

    # ==================== 命令组 ====================

    @filter.command_group("douyu")
    def douyu(self):
        """斗鱼直播通知命令组"""
        # 裸 /douyu 由宿主框架直接回复自动命令树(组过滤器在唤醒阶段
        # 拦截,本函数体不会被执行);详细说明走 /douyu help
        pass

    @douyu.command("help")
    async def douyu_help(self, event: AstrMessageEvent):
        """查看命令帮助"""
        lines = [
            "【斗鱼开播通知 - 命令帮助】",
                        "订阅(所有人):",
            "  /douyu ls - 查看监控列表",
            "  /douyu live - 查看当前在播的房间",
            "  /douyu sub <房间号> - 订阅开播通知",
            "  /douyu unsub <房间号> - 取消订阅",
            "  /douyu offline <房间号> [on/off] - 本群下播通知开关",
            "  /douyu mysub - 查看本群订阅",
            "  /douyu status - 监控总览",
        ]
        is_admin = False
        try:
            is_admin = bool(event.is_admin())
        except Exception:
            pass
        if is_admin:
            lines += [
                "管理(管理员):",
                "  /douyu add <房间号> [名称] - 添加监控",
                "  /douyu del <房间号> - 删除监控",
                "  /douyu restart [房间号] - 重启监控",
                "  /douyu atall <房间号> [on/off] - 本群 @全体开关",
            ]
        lines.append("状态图例: 🟢 运行中 / 🟡 重连中 / 🔴 已停止")
        yield event.plain_result("\n".join(lines))

    @douyu.command("offline")
    async def douyu_offline(
        self, event: AstrMessageEvent, room_id: int, enable: str = ""
    ):
        """开启/关闭当前群的下播通知

        此设置只对当前群生效。下播通知默认开启。

        Args:
            room_id: 斗鱼直播间房间号
            enable: on/off 或留空切换状态
        """
        gate = self._subscribe_gate(event)
        if gate:
            yield event.plain_result(gate)
            return
        room_info, new_status, error = await self._resolve_toggle(
            room_id, event.unified_msg_origin, "offline_notify", enable
        )
        if error:
            yield event.plain_result(error)
            return

        status_text = "开启" if new_status else "关闭"
        yield event.plain_result(
            f"直播间 {room_info.name}({room_id})\n"
            f"当前群的下播通知已{status_text}{self._save_warning()}"
        )

    @filter.permission_type(filter.PermissionType.ADMIN)
    @douyu.command("add")
    async def douyu_add(self, event: AstrMessageEvent, room_id: int, name: GreedyStr):
        """添加监控直播间（管理员）

        Args:
            room_id: 斗鱼直播间房间号
            name: 直播间名称（可选，可含空格，不填则自动获取）
        """
        if room_id <= 0:
            yield event.plain_result("注意: 房间号必须为正整数")
            return

        if self.data.has_room(room_id):
            yield event.plain_result(f"注意: 直播间 {room_id} 已在监控列表中")
            return

        # 验证房间是否存在，同时获取主播名称。
        # auto 源：betard 优先，接口异常时回退公开 API——添加房间只需要
        # 存在性与名称，不涉及轮播判定，回退是安全的
        room_name = name.strip()
        try:
            api_info = await fetch_room(room_id, source="auto")
        except RoomNotFound:
            yield event.plain_result(
                f"注意: 直播间 {room_id} 不存在\n请检查房间号是否正确"
            )
            return
        except ApiError as e:
            logger.warning(f"获取斗鱼直播间 {room_id} 信息失败: {e}")
            yield event.plain_result(
                f"注意: 斗鱼接口暂时不可用，无法验证直播间 {room_id}\n请稍后重试"
            )
            return
        except Exception as e:
            # 兜底:罕见的未映射网络异常不应让命令处理器抛裸异常
            logger.warning(f"获取斗鱼直播间 {room_id} 信息时发生未预期错误: {e}")
            yield event.plain_result(
                f"注意: 斗鱼接口暂时不可用，无法验证直播间 {room_id}\n请稍后重试"
            )
            return

        # 如果没有提供名称，使用 API 获取的名称
        if not room_name:
            room_name = api_info.owner or f"房间{room_id}"
        # 入口清洗：API 来源的主播名是外部可控文本，存储前先清洗，
        # 防止其经由各命令回复注入伪造内容
        room_name = sanitize_display_text(room_name)

        # 保存房间信息（保存含磁盘 I/O，移出事件循环）
        info = RoomInfo(
            name=room_name,
            added_by=event.get_sender_id(),
            added_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        )
        await asyncio.to_thread(self.data.add_room, room_id, info)

        # 启动监控
        if await self._start_monitor(room_id):
            yield event.plain_result(
                f"已添加直播间监控\n"
                f"房间号: {room_id}\n"
                f"名称: {room_name}\n"
                f"使用 /douyu sub {room_id} 订阅开播通知"
                f"{self._save_warning()}"
            )
        else:
            await asyncio.to_thread(self.data.remove_room, room_id)
            yield event.plain_result("失败: 启动监控失败，请查看日志获取详细错误")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @douyu.command("del")
    async def douyu_del(self, event: AstrMessageEvent, room_id: int):
        """删除监控直播间（管理员）"""
        room_info = self.data.get_room(room_id)

        # 先删数据再停监控：并发的 watchdog 重启在房间锁内复查
        # has_room 时会看到房间已删除而放弃，避免装回僵尸监控
        removed = await asyncio.to_thread(self.data.remove_room, room_id)
        await self._stop_monitor(room_id)
        if not removed:
            yield event.plain_result(f"注意: 直播间 {room_id} 不在监控列表中")
            return

        room_name = room_info.name if room_info else str(room_id)
        yield event.plain_result(
            f"已删除直播间 {room_name}({room_id}) 的监控{self._save_warning()}"
        )

    @douyu.command("ls")
    async def douyu_ls(self, event: AstrMessageEvent):
        """查看监控列表"""
        rooms = self.data.get_all_rooms()
        if not rooms:
            yield event.plain_result("当前没有监控的直播间\n使用 /douyu add <房间号> 添加")
            return

        lines = ["【斗鱼监控列表】"]
        for idx, (room_id, info) in enumerate(rooms.items(), 1):
            sub_count = len(self.data.get_subscribers(room_id))
            monitor = self.monitors.get(room_id)
            if monitor and monitor.is_healthy:
                # 消费协程存活但弹幕连接未就绪(库在退避重连)时如实展示,
                # 避免长期断连被"运行中"掩盖
                status = "🟢 运行中" if monitor.connected else "🟡 重连中"
            else:
                status = "🔴 已停止"
            lines.append(
                f"{idx}. {info.name}\n"
                f"   房间号: {room_id}\n"
                f"   订阅数: {sub_count}\n"
                f"   状态: {status}"
            )

        yield event.plain_result("\n".join(lines))

    @douyu.command("live")
    async def douyu_live(self, event: AstrMessageEvent):
        """查看当前在播的房间"""
        # 在播判定用内存状态机(rss+对账维护,权威),HTTP 只做富化
        live_rooms = [
            (rid, m) for rid, m in self.monitors.items() if m.last_live_status
        ]
        if not live_rooms:
            yield event.plain_result(
                "当前没有监控中的房间在播\n使用 /douyu ls 查看监控列表"
            )
            return

        infos = await asyncio.gather(
            *(self._room_cache.get(rid) for rid, _ in live_rooms),
            return_exceptions=True,
        )
        now = time.time()
        lines = ["【当前在播】"]
        for (rid, monitor), info in zip(live_rooms, infos):
            room = self.data.get_room(rid)
            name = room.name if room else str(rid)
            duration = ""
            if monitor.live_start_time:
                mins = int(max(0.0, now - monitor.live_start_time) // 60)
                duration = (
                    f"已播 {mins // 60}小时{mins % 60}分钟"
                    if mins >= 60
                    else f"已播 {mins}分钟"
                )
            url = f"https://www.douyu.com/{rid}"
            if isinstance(info, BaseException):
                # 富化失败降级:只显示名称与时长,不阻塞命令
                lines.append(f"• {name}\n  {duration} · {url}")
            else:
                title = sanitize_display_text(info.title)
                category = (
                    f" [{sanitize_display_text(info.category, max_len=16)}]"
                    if info.category
                    else ""
                )
                lines.append(f"• {name}{category}\n  {title}\n  {duration} · {url}")
        yield event.plain_result("\n".join(lines))

    @douyu.command("sub")
    async def douyu_sub(self, event: AstrMessageEvent, room_id: int):
        """订阅直播间开播通知"""
        gate = self._subscribe_gate(event)
        if gate:
            yield event.plain_result(gate)
            return
        room_info = self.data.get_room(room_id)
        if not room_info:
            yield event.plain_result(
                f"注意: 直播间 {room_id} 不在监控列表中\n"
                f"请联系管理员添加，或使用 /douyu ls 查看可订阅的直播间"
            )
            return

        umo = event.unified_msg_origin
        operator = event.get_sender_id()
        success, restored = await asyncio.to_thread(
            self.data.subscribe, room_id, umo, operator
        )
        if not success:
            # False 有两义:已订阅,或房间恰在命令处理期间被管理员删除
            # (数据层在锁内判存,不会再产生孤立订阅)
            if not self.data.has_room(room_id):
                yield event.plain_result(
                    f"注意: 直播间 {room_id} 刚被移出监控列表,订阅未生效"
                )
            else:
                yield event.plain_result(f"注意: 你已经订阅了直播间 {room_id}")
            return

        # 审计日志：记录订阅操作者
        logger.info(f"用户 {operator} 订阅了直播间 {room_id} ({umo})")

        # 检查监控状态并提示
        monitor = self.monitors.get(room_id)
        status_tip = ""
        if not (monitor and monitor.is_healthy):
            status_tip = "\n注意: 该直播间监控未运行，请联系管理员检查"
        restored_tip = "\n已自动恢复此前的订阅配置" if restored else ""

        yield event.plain_result(
            f"订阅成功！\n直播间: {room_info.name}({room_id})\n"
            f"开播时将在此处收到通知{restored_tip}{status_tip}{self._save_warning()}"
        )

    @douyu.command("unsub")
    async def douyu_unsub(self, event: AstrMessageEvent, room_id: int):
        """取消订阅直播间"""
        gate = self._subscribe_gate(event)
        if gate:
            yield event.plain_result(gate)
            return
        umo = event.unified_msg_origin
        room_info = self.data.get_room(room_id)
        room_name = room_info.name if room_info else str(room_id)

        if not await asyncio.to_thread(self.data.unsubscribe, room_id, umo):
            yield event.plain_result(f"注意: 你没有订阅直播间 {room_id}")
            return

        # 审计日志：记录退订操作者（配置保留，重新订阅时恢复）
        logger.info(
            f"用户 {event.get_sender_id()} 取消了直播间 {room_id} 的订阅 ({umo})"
        )

        yield event.plain_result(
            f"已取消订阅直播间 {room_name}({room_id})\n"
            f"订阅配置已保留，重新订阅时将自动恢复{self._save_warning()}"
        )

    @douyu.command("mysub")
    async def douyu_mysub(self, event: AstrMessageEvent):
        """查看当前群的订阅"""
        umo = event.unified_msg_origin
        room_ids = self.data.get_user_subscriptions(umo)

        if not room_ids:
            yield event.plain_result(
                "当前群还没有订阅任何直播间\n"
                "使用 /douyu ls 查看可订阅的直播间\n"
                "使用 /douyu sub <房间号> 订阅"
            )
            return

        my_subs = []
        for room_id in room_ids:
            room_info = self.data.get_room(room_id)
            room_name = room_info.name if room_info else str(room_id)
            # 获取当前群的订阅配置
            sub_config = self.data.get_subscription_config(room_id, umo)
            if sub_config:
                at_all_icon = "开" if sub_config.at_all else "关"
                my_subs.append(
                    f"• {room_name} ({room_id})\n  @全体:{at_all_icon}"
                )
            else:
                my_subs.append(f"• {room_name} ({room_id})")

        yield event.plain_result(
            "【本群订阅列表】\n" + "\n".join(my_subs)
        )

    @douyu.command("status")
    async def douyu_status(self, event: AstrMessageEvent):
        """查看监控状态"""
        total_rooms = len(self.data.get_all_room_ids())
        running = sum(1 for m in self.monitors.values() if m.is_healthy)
        total_subs = self.data.get_total_subscriptions()

        yield event.plain_result(
            f"【斗鱼监控状态】\n"
            f"监控直播间: {total_rooms}\n"
            f"运行中: {running}\n"
            f"总订阅数: {total_subs}"
        )

    @filter.permission_type(filter.PermissionType.ADMIN)
    @douyu.command("restart")
    async def douyu_restart(
        self, event: AstrMessageEvent, room_id: int | None = None
    ):
        """重启监控（管理员）

        Args:
            room_id: 指定房间号，不填则重启所有
        """
        if room_id is not None:
            if not self.data.has_room(room_id):
                yield event.plain_result(f"注意: 直播间 {room_id} 不在监控列表中")
                return

            if await self._restart_monitor(room_id):
                yield event.plain_result(f"直播间 {room_id} 监控已重启")
            else:
                yield event.plain_result(f"失败: 直播间 {room_id} 监控重启失败")
        else:
            # 重启所有
            room_ids = self.data.get_all_room_ids()
            success = 0
            for rid in room_ids:
                if await self._restart_monitor(rid):
                    success += 1
                else:
                    logger.warning(f"重启直播间 {rid} 监控失败")

            yield event.plain_result(
                f"已重启 {success}/{len(room_ids)} 个直播间监控"
            )

    @filter.permission_type(filter.PermissionType.ADMIN)
    @douyu.command("atall")
    async def douyu_atall(
        self, event: AstrMessageEvent, room_id: int, enable: str = ""
    ):
        """开启/关闭当前群的 @全体成员（管理员）

        此设置只对当前群生效，不影响其他订阅了同一直播间的群。

        Args:
            room_id: 斗鱼直播间房间号
            enable: on/off 或留空切换状态
        """
        room_info, new_status, error = await self._resolve_toggle(
            room_id, event.unified_msg_origin, "at_all", enable
        )
        if error:
            yield event.plain_result(error)
            return

        status_text = "开启" if new_status else "关闭"
        yield event.plain_result(
            f"直播间 {room_info.name}({room_id})\n"
            f"当前群的 @全体成员 已{status_text}{self._save_warning()}"
        )

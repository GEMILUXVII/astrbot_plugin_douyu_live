"""斗鱼直播监控器模块"""

import time
from collections.abc import Callable
from threading import Event, Lock, Thread
from typing import Any

from pydouyu.client import Client

from astrbot.api import logger


class DouyuMonitor:
    """斗鱼直播监控器

    使用 pydouyu 库监控指定直播间的开播状态，
    当检测到开播/下播时通过回调函数通知上层。

    线程模型:
        - start() 创建后台线程运行 _run_client
        - pydouyu 内部线程调用 _rss_handler
        - _run_client 的等待循环周期性调用 _reconcile_pending，
          补发被冷却期吞掉的状态转换
    """

    def __init__(
        self,
        room_id: int,
        live_callback: Callable[[int, dict], None] | None = None,
        offline_callback: Callable[[int, float], None] | None = None,
        inherit_state: dict[str, Any] | None = None,
    ):
        """初始化监控器

        Args:
            room_id: 斗鱼直播间房间号
            live_callback: 开播回调函数，参数为 (room_id, msg)
            offline_callback: 下播回调函数，参数为 (room_id, duration_seconds)
            inherit_state: 重启时继承旧监控器的状态（export_state() 的返回值），
                避免对已播报过的直播间重复发送开播通知
        """
        self.room_id = room_id
        self.live_callback = live_callback
        self.offline_callback = offline_callback
        self.client: Client | None = None
        self.running = False
        self.thread: Thread | None = None
        self.created_at = time.time()

        # ---- 直播状态机（由 _state_lock 保护）----
        # 使用 None 表示未知状态，避免首次消息误判
        self.last_live_status: bool | None = None
        self.live_start_time: float | None = None  # 开播时间戳
        self._has_announced_live = False  # 是否已发布开播通知
        # 上次通知时间，防止短时间内重复通知
        self._last_notify_time: float = 0.0
        self._notify_cooldown = 30.0  # 通知冷却时间（秒）
        # 冷却期内观测到的待定状态，冷却结束后由 _reconcile_pending 补处理
        self._pending_status: bool | None = None
        self._pending_msg: dict | None = None
        self._state_lock = Lock()

        if inherit_state:
            self.last_live_status = inherit_state.get("last_live_status")
            self.live_start_time = inherit_state.get("live_start_time")
            self._has_announced_live = bool(
                inherit_state.get("has_announced_live", False)
            )
            self._last_notify_time = float(inherit_state.get("last_notify_time", 0.0))
            # 待定转换必须一并继承：斗鱼只在状态变化时推送 rss，
            # 丢弃待定转换会让状态机与真实状态永久失步（连带吞掉下一条通知）
            self._pending_status = inherit_state.get("pending_status")
            self._pending_msg = inherit_state.get("pending_msg")

        # ---- 生命周期（由 _lock 保护）----
        self._stop_flag = False  # 停止标志
        self._lock = Lock()  # 保护 client 生命周期
        # 启动结果：_startup_done 置位后，_startup_error 表示是否启动失败
        self._startup_done = Event()
        self._startup_error: Exception | None = None

    # ==================== 状态查询 ====================

    @property
    def is_healthy(self) -> bool:
        """监控是否存活（线程在运行且未被停止）"""
        return (
            self.running
            and not self._stop_flag
            and self.thread is not None
            and self.thread.is_alive()
        )

    def export_state(self) -> dict[str, Any]:
        """导出直播状态，供重启时新监控器继承"""
        with self._state_lock:
            return {
                "last_live_status": self.last_live_status,
                "live_start_time": self.live_start_time,
                "has_announced_live": self._has_announced_live,
                "last_notify_time": self._last_notify_time,
                "pending_status": self._pending_status,
                "pending_msg": self._pending_msg,
            }

    # ==================== 状态机 ====================

    def _apply_transition_locked(
        self, is_live: bool, msg: dict, now: float
    ) -> tuple[Callable | None, tuple]:
        """执行一次状态转换（调用者需持有 _state_lock）

        Returns:
            (callback, args): 需要在锁外调用的回调及参数，无则 (None, ())
        """
        self._pending_status = None
        self._pending_msg = None
        self.last_live_status = is_live

        if is_live:
            logger.info(f"斗鱼直播间 {self.room_id} 开播了!")
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
                f"斗鱼直播间 {self.room_id} 检测到下播，但尚未发布开播通知，忽略"
            )
        return None, ()

    def _rss_handler(self, msg: dict) -> None:
        """处理直播状态变化

        Args:
            msg: pydouyu 的 rss 事件消息
        """
        if self._stop_flag:
            return
        try:
            ss = msg.get("ss", "0")
            ivl = msg.get("ivl", "1")
            # ss='1' 表示正在直播, ivl='0' 表示不是回放
            is_live = ss == "1" and ivl == "0"
            now = time.time()
            callback: Callable | None = None
            args: tuple = ()

            with self._state_lock:
                if self.last_live_status is None:
                    # 首次收到状态消息（未继承状态），若已开播则立即通知
                    logger.info(
                        f"斗鱼直播间 {self.room_id} 当前状态: "
                        f"{'直播中' if is_live else '未开播'}"
                    )
                    self.last_live_status = is_live
                    if is_live:
                        self.live_start_time = now
                        self._has_announced_live = True
                        self._last_notify_time = now
                        logger.info(
                            f"斗鱼直播间 {self.room_id} 开播了! (初始状态)"
                        )
                        if self.live_callback:
                            callback = self.live_callback
                            args = (self.room_id, msg)
                elif is_live == self.last_live_status:
                    # 状态回稳（等于当前状态），清除待定转换
                    self._pending_status = None
                    self._pending_msg = None
                elif now - self._last_notify_time < self._notify_cooldown:
                    # 冷却期内记为待定，冷却结束后由 _reconcile_pending 补处理。
                    # 不能直接丢弃：斗鱼只在状态变化时推送 rss，直接丢弃会
                    # 导致状态机与真实状态永久不一致，连带丢失后续通知。
                    self._pending_status = is_live
                    self._pending_msg = msg
                    logger.debug(
                        f"斗鱼直播间 {self.room_id} 状态变化处于冷却期内，"
                        f"已记为待定状态，冷却结束后校准"
                    )
                else:
                    callback, args = self._apply_transition_locked(is_live, msg, now)

            if callback:
                callback(*args)
        except Exception as e:
            logger.error(f"处理直播状态时出错: {e}", exc_info=True)

    def _reconcile_pending(self) -> None:
        """冷却结束后校准待定状态（由监控线程周期调用）"""
        try:
            callback: Callable | None = None
            args: tuple = ()
            with self._state_lock:
                if self._pending_status is None:
                    return
                now = time.time()
                if now - self._last_notify_time < self._notify_cooldown:
                    return
                if self._pending_status == self.last_live_status:
                    self._pending_status = None
                    self._pending_msg = None
                    return
                logger.info(
                    f"斗鱼直播间 {self.room_id} 冷却结束，补发待定状态转换"
                )
                callback, args = self._apply_transition_locked(
                    self._pending_status, self._pending_msg or {}, now
                )
            if callback:
                callback(*args)
        except Exception as e:
            logger.error(f"校准待定状态时出错: {e}", exc_info=True)

    # ==================== 生命周期 ====================

    def _run_client(self) -> None:
        """在线程中运行客户端"""
        client: Client | None = None
        try:
            with self._lock:
                if self._stop_flag:
                    return
                # 创建 Client 实例
                client = Client(room_id=self.room_id)
                self.client = client
                # 注册直播状态处理器
                client.add_handler("rss", self._rss_handler)
                self.running = True

            # 之后只使用局部变量 client，避免与 stop() 置空 self.client 竞态
            client.start()
            # stop() 可能恰在锁释放后、client.start() 前插入：它对未启动的
            # client 调用 stop()（pydouyu 的 stop 会把内部 socket/worker
            # 替换为全新实例）并置 self.client=None，随后本线程启动的是
            # 全新连接且 finally 的守卫不再匹配——必须在此复查并亲手停掉，
            # 否则连接与线程泄漏到进程结束
            with self._lock:
                if self._stop_flag:
                    try:
                        client.stop()
                    except Exception:
                        pass
                    return
            self._startup_done.set()
            logger.info(f"斗鱼监控器 {self.room_id} 已启动")

            # 等待内部线程结束或收到停止信号，
            # 并周期性校准冷却期内的待定状态
            worker = getattr(client, "message_worker", None)
            while not self._stop_flag and worker is not None and worker.is_alive():
                time.sleep(1)
                self._reconcile_pending()

            if not self._stop_flag:
                logger.warning(
                    f"斗鱼监控器 {self.room_id} 消息线程已退出"
                    f"（连接可能已断开），等待 watchdog 重启"
                )
        except Exception as e:
            self._startup_error = e
            self._startup_done.set()
            logger.error(f"斗鱼监控器 {self.room_id} 运行出错: {e}", exc_info=True)
        finally:
            with self._lock:
                self.running = False
                # 只有当 client 没有被 stop() 清理时才在这里清理
                if self.client is client and client is not None:
                    self._cleanup_client_internal()

    def _cleanup_client_internal(self) -> None:
        """内部清理客户端资源（调用者需持有 _lock）"""
        if self.client:
            try:
                self.client.stop()
            except Exception:
                pass
            self.client = None

    def start(self, wait: float = 3.0) -> bool:
        """启动监控

        注意：本方法会阻塞最长 wait 秒等待启动结果，
        从事件循环调用时应使用 asyncio.to_thread。

        Args:
            wait: 等待启动结果的最长时间（秒）

        Returns:
            False 表示启动过程中出错；True 表示已启动或仍在连接中
            （pydouyu 内部会持续重连，超时未确认不视为失败）
        """
        if self.is_healthy:
            return True

        self._stop_flag = False
        self._startup_done.clear()
        self._startup_error = None
        self.thread = Thread(
            target=self._run_client,
            daemon=True,
            name=f"douyu-monitor-{self.room_id}",
        )
        self.thread.start()

        # 等待启动结果；超时视为"仍在连接中"而非失败
        self._startup_done.wait(wait)
        return self._startup_error is None

    def stop(self) -> None:
        """停止监控

        注意：本方法可能阻塞最长 5 秒等待线程退出，
        从事件循环调用时应使用 asyncio.to_thread。
        """
        self._stop_flag = True
        with self._lock:
            self.running = False
            self._cleanup_client_internal()
        # 等待线程结束
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5.0)
            if self.thread.is_alive():
                logger.warning(
                    f"斗鱼直播间 {self.room_id} 监控线程未在 5 秒内退出，"
                    f"已标记停止（守护线程将随进程回收，回调已被屏蔽）"
                )
                return
        logger.info(f"斗鱼直播间 {self.room_id} 监控已停止")

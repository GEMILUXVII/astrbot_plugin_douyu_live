"""斗鱼直播监控器模块"""

import time
from collections.abc import Callable
from threading import Lock, Thread

from astrbot.api import logger

try:
    from pydouyu.client import Client

    PYDOUYU_AVAILABLE = True
except ImportError:
    PYDOUYU_AVAILABLE = False
    Client = None
    logger.warning("pydouyu 库未安装，请运行: pip install pydouyu")


class DouyuMonitor:
    """斗鱼直播监控器

    使用 pydouyu 库监控指定直播间的开播状态和礼物消息，
    当检测到开播或收到礼物时通过回调函数通知上层。
    """

    def __init__(
        self,
        room_id: int,
        live_callback: Callable[[int, dict], None] | None = None,
        gift_callback: Callable[[int, dict], None] | None = None,
        offline_callback: Callable[[int, float], None] | None = None,
    ):
        """初始化监控器

        Args:
            room_id: 斗鱼直播间房间号
            live_callback: 开播回调函数，参数为 (room_id, msg)
            gift_callback: 礼物回调函数，参数为 (room_id, msg)
            offline_callback: 下播回调函数，参数为 (room_id, duration_seconds)
        """
        self.room_id = room_id
        self.live_callback = live_callback
        self.gift_callback = gift_callback
        self.offline_callback = offline_callback
        self.client: Client | None = None
        self.running = False
        self.thread: Thread | None = None
        # 使用 None 表示未知状态，避免首次消息误判
        self.last_live_status: bool | None = None
        self._stop_flag = False  # 停止标志
        self.live_start_time: float | None = None  # 开播时间戳
        self._has_announced_live = False  # 是否已发布开播通知
        # 上次通知时间，防止短时间内重复通知
        self._last_notify_time: float = 0.0
        self._notify_cooldown = 30.0  # 通知冷却时间（秒）
        # 线程锁，保护 client 和状态变量
        self._lock = Lock()

    def _rss_handler(self, msg: dict) -> None:
        """处理直播状态变化

        Args:
            msg: pydouyu 的 rss 事件消息
        """
        try:
            ss = msg.get("ss", "0")
            ivl = msg.get("ivl", "1")
            # ss='1' 表示正在直播, ivl='0' 表示不是回放
            is_live = ss == "1" and ivl == "0"
            now = time.time()

            # 首次收到状态消息，若已开播则立即通知
            if self.last_live_status is None:
                logger.info(
                    f"斗鱼直播间 {self.room_id} 当前状态: "
                    f"{'直播中' if is_live else '未开播'}"
                )
                self.last_live_status = is_live
                if is_live:
                    self.live_start_time = now
                    self._has_announced_live = True
                    self._last_notify_time = now
                    logger.info(f"斗鱼直播间 {self.room_id} 开播了! (初始状态)")
                    if self.live_callback:
                        self.live_callback(self.room_id, msg)
                return

            # 状态没有变化，忽略
            if is_live == self.last_live_status:
                return

            # 检查通知冷却
            time_since_notify = now - self._last_notify_time
            if time_since_notify < self._notify_cooldown:
                logger.debug(
                    f"斗鱼直播间 {self.room_id} 状态变化但在冷却期内 "
                    f"({time_since_notify:.1f}s < {self._notify_cooldown}s)，忽略"
                )
                # 冷却期内不更新状态，保持原状态不变
                return

            if is_live and not self.last_live_status:
                # 从未开播变为开播，触发通知
                logger.info(f"斗鱼直播间 {self.room_id} 开播了!")
                self.live_start_time = now
                self._last_notify_time = now
                self._has_announced_live = True
                if self.live_callback:
                    self.live_callback(self.room_id, msg)

            elif not is_live and self.last_live_status:
                # 从开播变为下播，触发下播通知
                logger.info(f"斗鱼直播间 {self.room_id} 下播了!")
                duration = 0.0
                if self.live_start_time:
                    duration = now - self.live_start_time
                    self.live_start_time = None
                if self._has_announced_live:
                    self._last_notify_time = now
                    if self.offline_callback:
                        self.offline_callback(self.room_id, duration)
                else:
                    logger.debug(
                        f"斗鱼直播间 {self.room_id} 检测到下播，但尚未发布开播通知，忽略"
                    )
                self._has_announced_live = False

            self.last_live_status = is_live
        except Exception as e:
            logger.error(f"处理直播状态时出错: {e}")

    def _dgb_handler(self, msg: dict) -> None:
        """处理礼物消息

        Args:
            msg: pydouyu 的 dgb 礼物消息

        消息字段说明：
            - nn: 用户昵称
            - uid: 用户 ID
            - gfid: 礼物 ID
            - gfcnt / hits: 礼物数量
            - level: 用户等级
        """
        try:
            if self.gift_callback:
                self.gift_callback(self.room_id, msg)
        except Exception as e:
            logger.error(f"处理礼物消息时出错: {e}")

    def _run_client(self) -> None:
        """在线程中运行客户端"""
        client_to_cleanup = None
        try:
            with self._lock:
                if self._stop_flag:
                    return
                # 创建 Client 实例
                self.client = Client(room_id=self.room_id)
                client_to_cleanup = self.client
                # 注册直播状态处理器
                self.client.add_handler("rss", self._rss_handler)
                # 注册礼物消息处理器
                self.client.add_handler("dgb", self._dgb_handler)
                self.running = True

            self.client.start()
            logger.info(f"斗鱼监控器 {self.room_id} 已启动")

            # 等待内部线程结束或收到停止信号
            # message_worker 是一个 Thread，我们等待它
            if hasattr(self.client, "message_worker") and self.client.message_worker:
                while not self._stop_flag and self.client.message_worker.is_alive():
                    time.sleep(1)

        except Exception as e:
            logger.error(f"斗鱼监控器 {self.room_id} 运行出错: {e}")
        finally:
            with self._lock:
                self.running = False
                # 只有当 client 没有被 stop() 清理时才在这里清理
                if self.client is client_to_cleanup and self.client is not None:
                    self._cleanup_client_internal()

    def _cleanup_client_internal(self) -> None:
        """内部清理客户端资源（调用者需持有锁）"""
        if self.client:
            try:
                self.client.stop()
            except Exception:
                pass
            self.client = None

    def _cleanup_client(self) -> None:
        """清理客户端资源（线程安全）"""
        with self._lock:
            self._cleanup_client_internal()

    def start(self) -> bool:
        """启动监控

        Returns:
            是否成功启动
        """
        if not PYDOUYU_AVAILABLE:
            logger.error("pydouyu 库未安装，无法启动监控")
            return False

        if self.running:
            return True

        self._stop_flag = False
        self.thread = Thread(target=self._run_client, daemon=True)
        self.thread.start()
        return True

    def stop(self) -> None:
        """停止监控"""
        self._stop_flag = True
        with self._lock:
            self.running = False
            self._cleanup_client_internal()
        # 等待线程结束
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5.0)
        logger.info(f"斗鱼直播间 {self.room_id} 监控已停止")


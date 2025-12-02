"""斗鱼直播监控器模块"""

from threading import Thread
from typing import Callable

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
    ):
        """初始化监控器
        
        Args:
            room_id: 斗鱼直播间房间号
            live_callback: 开播回调函数，参数为 (room_id, msg)
            gift_callback: 礼物回调函数，参数为 (room_id, msg)
        """
        self.room_id = room_id
        self.live_callback = live_callback
        self.gift_callback = gift_callback
        self.client = None
        self.running = False
        self.thread: Thread | None = None
        self.last_live_status = False  # 上次的直播状态，防止重复通知

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

            if is_live and not self.last_live_status:
                # 从未开播变为开播，触发通知
                logger.info(f"斗鱼直播间 {self.room_id} 开播了!")
                if self.live_callback:
                    self.live_callback(self.room_id, msg)

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
        try:
            self.client = Client(room_id=self.room_id)
            # 注册直播状态处理器
            self.client.add_handler("rss", self._rss_handler)
            # 注册礼物消息处理器
            self.client.add_handler("dgb", self._dgb_handler)
            self.running = True
            self.client.start()
        except Exception as e:
            logger.error(f"斗鱼监控器 {self.room_id} 运行出错: {e}")
            self.running = False

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

        self.thread = Thread(target=self._run_client, daemon=True)
        self.thread.start()
        logger.info(f"斗鱼直播间 {self.room_id} 监控已启动")
        return True

    def stop(self) -> None:
        """停止监控"""
        self.running = False
        if self.client:
            try:
                self.client.stop()
            except Exception:
                pass
        logger.info(f"斗鱼直播间 {self.room_id} 监控已停止")

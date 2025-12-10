"""通知发送模块"""

import time
from typing import TYPE_CHECKING

from astrbot.api import logger
from astrbot.api.event import MessageEventResult
from astrbot.api.message_components import AtAll, Plain

from ..utils.constants import get_gift_name

if TYPE_CHECKING:
    from astrbot.api import star


class Notifier:
    """通知发送器

    负责构建和发送开播通知、礼物通知消息。
    """

    def __init__(self, context: "star.Context"):
        """初始化通知器

        Args:
            context: AstrBot 上下文
        """
        self.context = context

    def build_notification(
        self,
        room_id: int,
        room_name: str,
        timestamp: float | None = None,
    ) -> str:
        """构建开播通知消息文本

        Args:
            room_id: 房间号
            room_name: 房间/主播名称
            timestamp: 时间戳，默认当前时间

        Returns:
            格式化的通知消息
        """
        if timestamp is None:
            timestamp = time.time()

        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
        live_url = f"https://www.douyu.com/{room_id}"

        return (
            f"🎉 斗鱼直播开播通知\n"
            f"━━━━━━━━━━━━━━\n"
            f"👤 主播: {room_name}\n"
            f"🔢 房间号: {room_id}\n"
            f"⏰ 时间: {time_str}\n"
            f"🔗 链接: {live_url}\n"
            f"━━━━━━━━━━━━━━\n"
            f"快去观看吧！"
        )

    def build_gift_notification(
        self,
        room_id: int,
        room_name: str,
        user_name: str,
        gift_id: str | int,
        gift_count: int,
        timestamp: float | None = None,
    ) -> str:
        """构建礼物通知消息文本

        Args:
            room_id: 房间号
            room_name: 房间/主播名称
            user_name: 送礼用户昵称
            gift_id: 礼物 ID
            gift_count: 礼物数量
            timestamp: 时间戳，默认当前时间

        Returns:
            格式化的礼物通知消息
        """
        if timestamp is None:
            timestamp = time.time()

        time_str = time.strftime("%H:%M:%S", time.localtime(timestamp))
        gift_name = get_gift_name(gift_id)

        return (
            f"🎁 斗鱼直播礼物播报\n"
            f"━━━━━━━━━━━━━━\n"
            f"📺 直播间: {room_name}\n"
            f"👤 用户: {user_name}\n"
            f"🎁 礼物: {gift_name} x{gift_count}\n"
            f"⏰ 时间: {time_str}"
        )

    def build_offline_notification(
        self,
        room_id: int,
        room_name: str,
        duration_seconds: float,
        timestamp: float | None = None,
    ) -> str:
        """构建下播通知消息文本

        Args:
            room_id: 房间号
            room_name: 房间/主播名称
            duration_seconds: 直播时长（秒）
            timestamp: 时间戳，默认当前时间

        Returns:
            格式化的下播通知消息
        """
        if timestamp is None:
            timestamp = time.time()

        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))

        # 计算时长
        if duration_seconds > 0:
            hours = int(duration_seconds // 3600)
            minutes = int((duration_seconds % 3600) // 60)
            if hours > 0:
                duration_str = f"{hours}小时{minutes}分钟"
            else:
                duration_str = f"{minutes}分钟"
        else:
            duration_str = "未知"

        return (
            f"📴 斗鱼直播下播通知\n"
            f"━━━━━━━━━━━━━━\n"
            f"👤 主播: {room_name}\n"
            f"🔢 房间号: {room_id}\n"
            f"⏱️ 本次直播时长: {duration_str}\n"
            f"⏰ 下播时间: {time_str}\n"
            f"━━━━━━━━━━━━━━\n"
            f"感谢观看，下次再见！"
        )

    async def send_to_subscribers(
        self,
        subscriber_settings: dict[str, bool],
        message: str,
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ) -> None:
        """发送通知给所有订阅者

        Args:
            subscriber_settings: {umo -> at_all} 每个订阅者的 @全体设置
            message: 通知消息内容
            max_retries: 最大重试次数
            retry_delay: 重试间隔（秒）
        """
        import asyncio

        for umo, at_all in subscriber_settings.items():
            for attempt in range(max_retries):
                try:
                    result = MessageEventResult()
                    # 第一次尝试时使用 @全体，重试时不用（避免权限问题）
                    if at_all and attempt == 0:
                        result.chain.append(AtAll())
                        result.chain.append(Plain("\n"))
                    result.chain.append(Plain(message))
                    await self.context.send_message(umo, result)
                    logger.info(f"已发送通知到: {umo} (at_all={at_all})")
                    break  # 发送成功，跳出重试循环
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"发送通知失败 ({umo})，{retry_delay}秒后重试 "
                            f"({attempt + 1}/{max_retries}): {e}"
                        )
                        await asyncio.sleep(retry_delay)
                    else:
                        logger.error(
                            f"发送通知失败 ({umo})，已达最大重试次数: {e}"
                        )


"""通知发送模块"""

import asyncio
import time
from typing import TYPE_CHECKING

from astrbot.api import logger
from astrbot.api.event import MessageEventResult
from astrbot.api.message_components import AtAll, Plain

from ..utils.text import sanitize_display_text

if TYPE_CHECKING:
    from astrbot.api import star

# 单条通知扇出时的最大并发发送数
SEND_CONCURRENCY = 5


class Notifier:
    """通知发送器

    负责构建和发送开播/下播通知消息。
    重试策略由上层的通知队列负责（见 main.Main._process_notification_queue），
    本类只做单轮发送并报告失败目标。
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
        room_name = sanitize_display_text(room_name)

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
        room_name = sanitize_display_text(room_name)

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
        use_at_all: bool = True,
    ) -> set[str]:
        """发送通知给所有订阅者（单轮，不含重试）

        Args:
            subscriber_settings: {umo -> at_all} 每个订阅者的 @全体设置
            message: 通知消息内容
            use_at_all: 是否允许携带 @全体（重试降级时由上层置 False）

        Returns:
            发送失败且值得重试的 umo 集合。
            send_message 返回 False（无匹配平台）的目标不计入——
            平台不存在时重试不会成功。
        """
        failed: set[str] = set()
        semaphore = asyncio.Semaphore(SEND_CONCURRENCY)

        async def _send(umo: str, at_all: bool) -> None:
            async with semaphore:
                try:
                    result = MessageEventResult()
                    if at_all and use_at_all:
                        result.chain.append(AtAll())
                        result.chain.append(Plain("\n"))
                    result.chain.append(Plain(message))
                    ok = await self.context.send_message(umo, result)
                    if ok:
                        logger.debug(f"已发送通知到: {umo} (at_all={at_all})")
                    else:
                        # 未找到匹配的平台适配器，重试无意义
                        logger.warning(
                            f"发送通知失败: 未找到匹配的平台适配器 ({umo})，不重试"
                        )
                except Exception as e:
                    logger.warning(f"发送通知失败 ({umo}): {e}")
                    failed.add(umo)

        await asyncio.gather(
            *(_send(umo, at_all) for umo, at_all in subscriber_settings.items())
        )
        return failed

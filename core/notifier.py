"""通知发送模块"""

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from astrbot.api import logger
from astrbot.api.event import MessageEventResult
from astrbot.api.message_components import AtAll, Image, Plain

from ..utils.text import sanitize_display_text

# 斗鱼是中国平台,通知里的时间一律按北京时间展示——Docker 默认 UTC
# 部署下 time.localtime() 会偏 8 小时,业务时间与部署环境解耦
BEIJING_TZ = timezone(timedelta(hours=8))


def _fmt_beijing(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")


if TYPE_CHECKING:
    from astrbot.api import star

# 单条通知扇出时的最大并发发送数
SEND_CONCURRENCY = 5
# 单个目标的发送超时(秒):in-tree 适配器自带 15-300s 超时,此为纵深
# 防御,把单个慢/死目标对串行通知队列的队头阻塞压到有界(第三方或
# 未来适配器的超时行为未知)
SEND_TIMEOUT = 30.0


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
        title: str | None = None,
        category: str | None = None,
    ) -> str:
        """构建开播通知消息文本

        Args:
            room_id: 房间号
            room_name: 房间/主播名称
            timestamp: 时间戳，默认当前时间
            title: 直播间标题(富化,可选)
            category: 分类名(富化,可选)

        Returns:
            格式化的通知消息
        """
        if timestamp is None:
            timestamp = time.time()

        time_str = _fmt_beijing(timestamp)
        live_url = f"https://www.douyu.com/{room_id}"
        room_name = sanitize_display_text(room_name)

        lines = [
            "【斗鱼开播提醒】",
            f"主播: {room_name}",
        ]
        if title:
            lines.append(f"标题: {sanitize_display_text(title, max_len=48)}")
        if category:
            lines.append(f"分类: {sanitize_display_text(category, max_len=16)}")
        lines += [
            f"房间号: {room_id}",
            f"开播时间: {time_str}",
            f"直播间: {live_url}",
        ]
        return "\n".join(lines)

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

        time_str = _fmt_beijing(timestamp)
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
            f"【斗鱼下播提醒】\n"
            f"主播: {room_name}\n"
            f"房间号: {room_id}\n"
            f"本次直播时长: {duration_str}\n"
            f"下播时间: {time_str}"
        )

    async def send_to_subscribers(
        self,
        subscriber_settings: dict[str, bool],
        message: str,
        use_at_all: bool = True,
        cover_url: str | None = None,
    ) -> set[str]:
        """发送通知给所有订阅者（单轮，不含重试）

        Args:
            subscriber_settings: {umo -> at_all} 每个订阅者的 @全体设置
            message: 通知消息内容
            use_at_all: 是否允许携带 @全体（重试降级时由上层置 False）
            cover_url: 附带的封面图 URL（重试降级时由上层置 None,
                规避"因图片被平台拒绝而反复重试"）

        Returns:
            发送失败且值得重试的 umo 集合。
            send_message 返回 False 通常表示平台正在重载或尚未就绪，
            同样交给上层持续重试。
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
                    if cover_url:
                        result.chain.append(Image.fromURL(cover_url))
                    ok = await asyncio.wait_for(
                        self.context.send_message(umo, result),
                        timeout=SEND_TIMEOUT,
                    )
                    if ok:
                        logger.info(f"斗鱼通知发送成功: {umo} (at_all={at_all})")
                    else:
                        logger.warning(
                            f"斗鱼通知暂未发送: 未找到匹配的平台适配器 ({umo})"
                        )
                        failed.add(umo)
                except Exception as e:
                    logger.warning(f"斗鱼通知发送失败 ({umo}): {e}")
                    failed.add(umo)

        await asyncio.gather(
            *(_send(umo, at_all) for umo, at_all in subscriber_settings.items())
        )
        return failed

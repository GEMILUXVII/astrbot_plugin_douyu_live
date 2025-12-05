"""斗鱼直播通知插件

支持多房间监控、订阅推送、@全体成员、礼物播报等功能。
"""

import asyncio
import time
from dataclasses import dataclass
from queue import Empty, Queue

from astrbot.api import logger, star
from astrbot.api.event import AstrMessageEvent, filter

from .core import PYDOUYU_AVAILABLE, DouyuAPI, DouyuMonitor, Notifier
from .models import RoomInfo
from .storage import DataManager
from .utils.constants import is_high_value_gift


@dataclass
class PendingNotification:
    """待发送的通知"""
    subscribers: set[str]
    message: str
    at_all: bool = False
    retry_count: int = 0


class Main(star.Star):
    """斗鱼直播开播通知插件

    命令列表:
    - /douyu add <房间号> [名称] - 添加监控直播间（管理员）
    - /douyu del <房间号> - 删除监控直播间（管理员）
    - /douyu ls - 查看监控列表
    - /douyu sub <房间号> - 订阅直播间开播通知
    - /douyu unsub <房间号> - 取消订阅
    - /douyu mysub - 查看我的订阅
    - /douyu status - 查看监控状态
    - /douyu restart [房间号] - 重启监控（管理员）
    - /douyu atall <房间号> [on/off] - 设置@全体（管理员）
    - /douyu gift <房间号> [on/off] - 开启/关闭礼物播报（管理员）
    - /douyu giftfilter <房间号> [on/off] - 开启/关闭高价值礼物过滤（管理员）
    """

    def __init__(self, context: star.Context) -> None:
        super().__init__(context)
        self.context = context

        # 主事件循环引用（用于子线程回调）
        self.loop: asyncio.AbstractEventLoop | None = None

        # 初始化模块
        self.data = DataManager()
        self.notifier = Notifier(context)
        self.monitors: dict[int, DouyuMonitor] = {}

        # 通知队列，用于事件循环不可用时缓存通知
        self._notification_queue: Queue[PendingNotification] = Queue()
        self._queue_processor_task: asyncio.Task | None = None

    async def initialize(self) -> None:
        """插件激活时启动所有监控"""
        # 保存主事件循环引用，用于子线程中的异步调用
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.get_event_loop()

        if not PYDOUYU_AVAILABLE:
            logger.error("pydouyu 库未安装，斗鱼直播通知插件无法正常工作")
            return

        # 启动通知队列处理任务
        self._queue_processor_task = asyncio.create_task(self._process_notification_queue())

        # 启动所有已保存房间的监控
        for room_id in self.data.room_info.keys():
            self._start_monitor(room_id)

        logger.info(f"斗鱼直播通知插件已启动，监控 {len(self.monitors)} 个直播间")

    async def terminate(self) -> None:
        """插件禁用时停止所有监控"""
        # 停止队列处理任务
        if self._queue_processor_task:
            self._queue_processor_task.cancel()
            try:
                await self._queue_processor_task
            except asyncio.CancelledError:
                pass

        for monitor in self.monitors.values():
            monitor.stop()
        self.monitors.clear()
        self.data.save()
        logger.info("斗鱼直播通知插件已停止")

    # ==================== 监控管理 ====================

    def _start_monitor(self, room_id: int) -> bool:
        """启动单个房间的监控"""
        if room_id in self.monitors:
            return True

        monitor = DouyuMonitor(
            room_id,
            live_callback=self._on_live_start,
            gift_callback=self._on_gift,
            offline_callback=self._on_live_end,
        )
        if monitor.start():
            self.monitors[room_id] = monitor
            return True
        return False

    def _stop_monitor(self, room_id: int) -> None:
        """停止单个房间的监控"""
        if room_id in self.monitors:
            self.monitors[room_id].stop()
            del self.monitors[room_id]

    async def _process_notification_queue(self) -> None:
        """处理通知队列的后台任务"""
        MAX_RETRIES = 5
        while True:
            try:
                # 每秒检查一次队列
                await asyncio.sleep(1)

                # 处理队列中的所有通知
                pending_items: list[PendingNotification] = []
                while True:
                    try:
                        item = self._notification_queue.get_nowait()
                        pending_items.append(item)
                    except Empty:
                        break

                for item in pending_items:
                    try:
                        await self.notifier.send_to_subscribers(
                            item.subscribers, item.message, item.at_all
                        )
                    except Exception as e:
                        item.retry_count += 1
                        if item.retry_count < MAX_RETRIES:
                            # 放回队列稍后重试
                            self._notification_queue.put(item)
                            logger.warning(
                                f"发送通知失败，将重试 ({item.retry_count}/{MAX_RETRIES}): {e}"
                            )
                        else:
                            logger.error(f"发送通知失败，已达最大重试次数: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"通知队列处理器出错: {e}")

    def _schedule_notification(
        self, subscribers: set[str], message: str, at_all: bool = False
    ) -> None:
        """安全地调度通知发送

        如果事件循环可用，直接调度；否则放入队列稍后处理。
        """
        if self.loop and self.loop.is_running():
            asyncio.run_coroutine_threadsafe(
                self.notifier.send_to_subscribers(subscribers, message, at_all),
                self.loop,
            )
        else:
            # 事件循环不可用，放入队列稍后处理
            logger.warning("事件循环暂时不可用，通知已加入队列")
            self._notification_queue.put(
                PendingNotification(subscribers=subscribers, message=message, at_all=at_all)
            )

    def _on_live_start(self, room_id: int, msg: dict) -> None:
        """开播回调 - 发送通知给所有订阅者"""
        subscribers = self.data.get_subscribers(room_id)
        if not subscribers:
            return

        room_info = self.data.get_room(room_id)
        room_name = room_info.name if room_info else f"房间{room_id}"
        at_all_enabled = room_info.at_all if room_info else False

        notification = self.notifier.build_notification(room_id, room_name)

        # 安全地调度通知发送
        self._schedule_notification(subscribers, notification, at_all_enabled)

    def _on_gift(self, room_id: int, msg: dict) -> None:
        """礼物回调 - 发送礼物播报给所有订阅者

        Args:
            room_id: 房间号
            msg: 礼物消息，包含:
                - nn: 用户昵称
                - uid: 用户 ID
                - gfid: 礼物 ID
                - gfcnt / hits: 礼物数量
        """
        room_info = self.data.get_room(room_id)

        # 检查是否开启了礼物播报
        if not room_info or not room_info.gift_notify:
            return

        # 解析礼物 ID
        gift_id = msg.get("gfid", "0")

        # 如果开启了高价值过滤，只播报飞机及以上的礼物
        if room_info.high_value_only and not is_high_value_gift(gift_id):
            return

        subscribers = self.data.get_subscribers(room_id)
        if not subscribers:
            return

        # 解析礼物信息
        user_name = msg.get("nn", "未知用户")
        # 礼物数量可能在 gfcnt 或 hits 字段，添加异常处理
        try:
            gift_count_raw = msg.get("gfcnt", msg.get("hits", "1"))
            gift_count = int(gift_count_raw) if gift_count_raw else 1
        except (ValueError, TypeError):
            logger.warning(f"礼物数量解析失败: {msg.get('gfcnt')}/{msg.get('hits')}，默认为 1")
            gift_count = 1

        room_name = room_info.name

        # 构建礼物通知
        notification = self.notifier.build_gift_notification(
            room_id=room_id,
            room_name=room_name,
            user_name=user_name,
            gift_id=gift_id,
            gift_count=gift_count,
        )

        # 安全地调度通知发送
        self._schedule_notification(subscribers, notification, at_all=False)

    def _on_live_end(self, room_id: int, duration_seconds: float) -> None:
        """下播回调 - 发送下播通知给所有订阅者

        Args:
            room_id: 房间号
            duration_seconds: 直播时长（秒）
        """
        subscribers = self.data.get_subscribers(room_id)
        if not subscribers:
            return

        room_info = self.data.get_room(room_id)
        room_name = room_info.name if room_info else f"房间{room_id}"

        notification = self.notifier.build_offline_notification(
            room_id, room_name, duration_seconds
        )

        # 安全地调度通知发送
        self._schedule_notification(subscribers, notification, at_all=False)

    # ==================== 命令组 ====================

    @filter.command_group("douyu")
    def douyu(self):
        """斗鱼直播通知命令组"""
        pass

    @douyu.command("add")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def douyu_add(self, event: AstrMessageEvent, room_id: int, name: str = ""):
        """添加监控直播间（管理员）

        Args:
            room_id: 斗鱼直播间房间号
            name: 直播间名称（可选，不填则自动获取）
        """
        if not PYDOUYU_AVAILABLE:
            yield event.plain_result("❌ pydouyu 库未安装，请先安装: pip install pydouyu")
            return

        if self.data.has_room(room_id):
            yield event.plain_result(f"⚠️ 直播间 {room_id} 已在监控列表中")
            return

        # 验证房间是否存在，同时获取主播名称
        room_name = name
        api_info = await DouyuAPI.fetch_room_info(room_id)
        if not api_info:
            yield event.plain_result(
                f"⚠️ 无法获取直播间 {room_id} 的信息\n"
                f"请检查房间号是否正确，或稍后重试"
            )
            return

        # 如果没有提供名称，使用 API 获取的名称
        if not room_name:
            room_name = api_info.get("owner_name") or api_info.get("nickname") or f"房间{room_id}"

        # 保存房间信息
        info = RoomInfo(
            name=room_name,
            added_by=event.get_sender_id(),
            added_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            at_all=False,
        )
        self.data.add_room(room_id, info)

        # 启动监控
        if self._start_monitor(room_id):
            yield event.plain_result(
                f"✅ 已添加直播间监控\n"
                f"房间号: {room_id}\n"
                f"名称: {room_name}\n"
                f"使用 /douyu sub {room_id} 订阅开播通知"
            )
        else:
            self.data.remove_room(room_id)
            yield event.plain_result("❌ 启动监控失败，请检查房间号是否正确")

    @douyu.command("del")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def douyu_del(self, event: AstrMessageEvent, room_id: int):
        """删除监控直播间（管理员）"""
        room_info = self.data.get_room(room_id)
        if not room_info:
            yield event.plain_result(f"⚠️ 直播间 {room_id} 不在监控列表中")
            return

        room_name = room_info.name

        # 停止监控并删除数据
        self._stop_monitor(room_id)
        self.data.remove_room(room_id)

        yield event.plain_result(f"✅ 已删除直播间 {room_name}({room_id}) 的监控")

    @douyu.command("ls")
    async def douyu_ls(self, event: AstrMessageEvent):
        """查看监控列表"""
        rooms = self.data.get_all_rooms()
        if not rooms:
            yield event.plain_result("📋 当前没有监控的直播间\n使用 /douyu add <房间号> 添加")
            return

        lines = ["📋 斗鱼直播监控列表", "━━━━━━━━━━━━━━"]
        for idx, (room_id, info) in enumerate(rooms.items(), 1):
            sub_count = len(self.data.get_subscribers(room_id))
            status = "🟢 运行中" if room_id in self.monitors else "🔴 已停止"
            at_all_status = "✅" if info.at_all else "❌"
            gift_status = "✅" if info.gift_notify else "❌"
            gift_filter = "仅高价值" if info.high_value_only else "全部"
            lines.append(
                f"{idx}. {info.name}\n"
                f"   房间号: {room_id}\n"
                f"   订阅数: {sub_count}\n"
                f"   @全体: {at_all_status} | 礼物: {gift_status}({gift_filter})\n"
                f"   状态: {status}"
            )

        yield event.plain_result("\n".join(lines))

    @douyu.command("sub")
    async def douyu_sub(self, event: AstrMessageEvent, room_id: int):
        """订阅直播间开播通知"""
        room_info = self.data.get_room(room_id)
        if not room_info:
            yield event.plain_result(
                f"⚠️ 直播间 {room_id} 不在监控列表中\n"
                f"请联系管理员添加，或使用 /douyu ls 查看可订阅的直播间"
            )
            return

        umo = event.unified_msg_origin
        if not self.data.subscribe(room_id, umo):
            yield event.plain_result(f"⚠️ 你已经订阅了直播间 {room_id}")
            return

        # 检查监控状态并提示
        is_running = room_id in self.monitors and self.monitors[room_id].running
        status_tip = ""
        if not is_running:
            status_tip = "\n⚠️ 注意: 该直播间监控未运行，请联系管理员检查"

        yield event.plain_result(
            f"✅ 订阅成功！\n直播间: {room_info.name}({room_id})\n开播时将在此处收到通知{status_tip}"
        )

    @douyu.command("unsub")
    async def douyu_unsub(self, event: AstrMessageEvent, room_id: int):
        """取消订阅直播间"""
        umo = event.unified_msg_origin
        room_info = self.data.get_room(room_id)
        room_name = room_info.name if room_info else str(room_id)

        if not self.data.unsubscribe(room_id, umo):
            yield event.plain_result(f"⚠️ 你没有订阅直播间 {room_id}")
            return

        yield event.plain_result(f"✅ 已取消订阅直播间 {room_name}({room_id})")

    @douyu.command("mysub")
    async def douyu_mysub(self, event: AstrMessageEvent):
        """查看我的订阅"""
        umo = event.unified_msg_origin
        room_ids = self.data.get_user_subscriptions(umo)

        if not room_ids:
            yield event.plain_result(
                "📋 你还没有订阅任何直播间\n"
                "使用 /douyu ls 查看可订阅的直播间\n"
                "使用 /douyu sub <房间号> 订阅"
            )
            return

        my_subs = []
        for room_id in room_ids:
            room_info = self.data.get_room(room_id)
            room_name = room_info.name if room_info else str(room_id)
            my_subs.append(f"• {room_name} ({room_id})")

        yield event.plain_result("📋 你的订阅列表\n━━━━━━━━━━━━━━\n" + "\n".join(my_subs))

    @douyu.command("status")
    async def douyu_status(self, event: AstrMessageEvent):
        """查看监控状态"""
        if not PYDOUYU_AVAILABLE:
            yield event.plain_result("⚠️ pydouyu 库未安装\n请运行: pip install pydouyu")
            return

        total_rooms = len(self.data.room_info)
        running = sum(1 for m in self.monitors.values() if m.running)
        total_subs = self.data.get_total_subscriptions()

        yield event.plain_result(
            f"📊 斗鱼直播监控状态\n"
            f"━━━━━━━━━━━━━━\n"
            f"📺 监控直播间: {total_rooms}\n"
            f"🟢 运行中: {running}\n"
            f"👥 总订阅数: {total_subs}\n"
            f"━━━━━━━━━━━━━━\n"
            f"pydouyu: {'✅ 已安装' if PYDOUYU_AVAILABLE else '❌ 未安装'}"
        )

    @douyu.command("restart")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def douyu_restart(self, event: AstrMessageEvent, room_id: int | None = None):
        """重启监控（管理员）

        Args:
            room_id: 指定房间号，不填则重启所有
        """
        if room_id is not None:
            if not self.data.has_room(room_id):
                yield event.plain_result(f"⚠️ 直播间 {room_id} 不在监控列表中")
                return

            # 先创建新监控器，成功后再停止旧的，减少通知丢失窗口
            old_monitor = self.monitors.get(room_id)
            new_monitor = DouyuMonitor(
                room_id,
                live_callback=self._on_live_start,
                gift_callback=self._on_gift,
                offline_callback=self._on_live_end,
            )
            if new_monitor.start():
                # 新监控启动成功，停止旧监控
                if old_monitor:
                    old_monitor.stop()
                self.monitors[room_id] = new_monitor
                yield event.plain_result(f"✅ 直播间 {room_id} 监控已重启")
            else:
                yield event.plain_result(f"❌ 直播间 {room_id} 监控重启失败")
        else:
            # 重启所有
            success = 0
            for rid in list(self.data.room_info.keys()):
                old_monitor = self.monitors.get(rid)
                new_monitor = DouyuMonitor(
                    rid,
                    live_callback=self._on_live_start,
                    gift_callback=self._on_gift,
                    offline_callback=self._on_live_end,
                )
                if new_monitor.start():
                    if old_monitor:
                        old_monitor.stop()
                    self.monitors[rid] = new_monitor
                    success += 1
                else:
                    logger.warning(f"重启直播间 {rid} 监控失败")

            yield event.plain_result(
                f"✅ 已重启 {success}/{len(self.data.room_info)} 个直播间监控"
            )

    @douyu.command("atall")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def douyu_atall(self, event: AstrMessageEvent, room_id: int, enable: str = ""):
        """开启/关闭 @全体成员（管理员）

        Args:
            room_id: 斗鱼直播间房间号
            enable: on/off 或留空切换状态
        """
        room_info = self.data.get_room(room_id)
        if not room_info:
            yield event.plain_result(f"⚠️ 直播间 {room_id} 不在监控列表中")
            return

        current = room_info.at_all

        if enable.lower() == "on":
            new_status = True
        elif enable.lower() == "off":
            new_status = False
        else:
            new_status = not current

        self.data.update_room(room_id, at_all=new_status)

        status_text = "开启" if new_status else "关闭"
        yield event.plain_result(f"✅ 直播间 {room_info.name}({room_id})\n@全体成员 已{status_text}")

    @douyu.command("gift")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def douyu_gift(self, event: AstrMessageEvent, room_id: int, enable: str = ""):
        """开启/关闭礼物播报（管理员）

        Args:
            room_id: 斗鱼直播间房间号
            enable: on/off 或留空切换状态
        """
        room_info = self.data.get_room(room_id)
        if not room_info:
            yield event.plain_result(f"⚠️ 直播间 {room_id} 不在监控列表中")
            return

        current = room_info.gift_notify

        if enable.lower() == "on":
            new_status = True
        elif enable.lower() == "off":
            new_status = False
        else:
            new_status = not current

        self.data.update_room(room_id, gift_notify=new_status)

        status_text = "开启" if new_status else "关闭"
        filter_status = "仅高价值" if room_info.high_value_only else "全部"
        yield event.plain_result(
            f"✅ 直播间 {room_info.name}({room_id})\n"
            f"🎁 礼物播报 已{status_text}\n"
            f"📊 过滤模式: {filter_status}"
        )

    @douyu.command("giftfilter")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def douyu_giftfilter(self, event: AstrMessageEvent, room_id: int, enable: str = ""):
        """开启/关闭高价值礼物过滤（管理员）

        开启后只播报飞机及以上的礼物，关闭后播报所有礼物。

        Args:
            room_id: 斗鱼直播间房间号
            enable: on/off 或留空切换状态
        """
        room_info = self.data.get_room(room_id)
        if not room_info:
            yield event.plain_result(f"⚠️ 直播间 {room_id} 不在监控列表中")
            return

        current = room_info.high_value_only

        if enable.lower() == "on":
            new_status = True
        elif enable.lower() == "off":
            new_status = False
        else:
            new_status = not current

        self.data.update_room(room_id, high_value_only=new_status)

        if new_status:
            yield event.plain_result(
                f"✅ 直播间 {room_info.name}({room_id})\n"
                f"🎁 礼物过滤: 仅播报高价值礼物（飞机及以上）"
            )
        else:
            yield event.plain_result(
                f"✅ 直播间 {room_info.name}({room_id})\n"
                f"🎁 礼物过滤: 播报所有礼物"
            )


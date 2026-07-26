"""斗鱼直播通知插件

支持多房间监控、订阅推送、@全体成员等功能。
"""

import asyncio
import threading
import time
from dataclasses import dataclass, field
from queue import Empty, Full, Queue

from astrbot.api import logger, star
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.core.star.filter.command import GreedyStr

from .core import DouyuAPI, DouyuMonitor, Notifier
from .models import RoomInfo
from .storage import DataManager
from .utils.text import sanitize_display_text

# 通知最大发送尝试次数（含首次发送，即最多重试 NOTIFY_MAX_RETRIES-1 轮）
NOTIFY_MAX_RETRIES = 5
# 通知队列容量上限（防止无界增长）
NOTIFY_QUEUE_MAX = 1000
# 队列处理轮询间隔（秒）
NOTIFY_POLL_INTERVAL = 0.5
# 同一房间同类通知的去重窗口（秒）：重启的新旧监控重叠期等场景下
# 吸收重复的开播/下播事件
NOTIFY_DEDUP_TTL = 10.0
# watchdog 检查间隔与新监控启动宽限期（秒）
WATCHDOG_INTERVAL = 60.0
WATCHDOG_STARTUP_GRACE = 30.0


@dataclass
class PendingNotification:
    """待发送的通知"""
    subscriber_settings: dict[str, bool] = field(default_factory=dict)  # {umo -> at_all}
    message: str = ""
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
    """

    def __init__(self, context: star.Context) -> None:
        super().__init__(context)
        self.context = context

        # 初始化模块
        self.data = DataManager()
        self.notifier = Notifier(context)
        self.monitors: dict[int, DouyuMonitor] = {}

        # 通知队列：监控线程投递，队列处理任务在事件循环上串行发送。
        # 统一走队列保证发送顺序、限制并发，并承担重试。
        self._notification_queue: Queue[PendingNotification] = Queue(
            maxsize=NOTIFY_QUEUE_MAX
        )
        self._queue_processor_task: asyncio.Task | None = None
        self._watchdog_task: asyncio.Task | None = None

        # 按房间串行化监控生命周期操作（start/stop/restart），
        # 防止 watchdog 与管理员命令并发操作同一房间导致监控器泄漏
        self._room_locks: dict[int, asyncio.Lock] = {}
        # 插件关停标志：置位后拒绝创建新监控
        self._closing = False

        # 通知去重表：{(kind, room_id) -> monotonic 时间}，pydouyu 线程访问
        self._notify_dedup: dict[tuple[str, int], float] = {}
        self._dedup_lock = threading.Lock()

    async def initialize(self) -> None:
        """插件激活时启动所有监控"""
        self._closing = False
        # 启动通知队列处理与监控看门狗任务
        self._queue_processor_task = asyncio.create_task(
            self._process_notification_queue()
        )
        self._watchdog_task = asyncio.create_task(self._watchdog())

        # 并发启动所有已保存房间的监控（监控启动在线程中，不阻塞事件循环）
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

        # 并发停止所有监控；stop() 会阻塞等待线程退出，必须移出事件循环
        monitors = list(self.monitors.values())
        self.monitors.clear()
        if monitors:
            await asyncio.gather(
                *(asyncio.to_thread(m.stop) for m in monitors)
            )
        # 注：数据在每次变更时已即时保存，此处无需（也不应）再次保存
        logger.info("斗鱼直播通知插件已停止")

    # ==================== 监控管理 ====================

    def _new_monitor(
        self, room_id: int, inherit_state: dict | None = None
    ) -> DouyuMonitor:
        """创建监控器（统一回调装配）"""
        return DouyuMonitor(
            room_id,
            live_callback=self._on_live_start,
            offline_callback=self._on_live_end,
            inherit_state=inherit_state,
        )

    def _room_lock(self, room_id: int) -> asyncio.Lock:
        """获取房间级生命周期锁（惰性创建，仅事件循环线程访问）"""
        return self._room_locks.setdefault(room_id, asyncio.Lock())

    async def _start_monitor(self, room_id: int) -> bool:
        """启动单个房间的监控"""
        async with self._room_lock(room_id):
            existing = self.monitors.get(room_id)
            if existing and existing.is_healthy:
                return True

            monitor = self._new_monitor(
                room_id,
                inherit_state=existing.export_state() if existing else None,
            )
            if not await asyncio.to_thread(monitor.start):
                await asyncio.to_thread(monitor.stop)
                return False
            # await 期间世界可能已变化：房间被删除或插件正在关停时放弃
            if self._closing or not self.data.has_room(room_id):
                await asyncio.to_thread(monitor.stop)
                return False
            self.monitors[room_id] = monitor
            return True

    async def _stop_monitor(self, room_id: int) -> None:
        """停止单个房间的监控"""
        async with self._room_lock(room_id):
            monitor = self.monitors.pop(room_id, None)
            if monitor:
                await asyncio.to_thread(monitor.stop)

    async def _restart_monitor(self, room_id: int) -> bool:
        """重启单个房间的监控

        先启动新监控、成功后再停止旧监控，减少通知丢失窗口；
        新监控继承旧状态（含冷却期待定转换），重叠窗口的重复播报
        由 _schedule_notification 的去重窗口吸收。
        房间锁保证同一房间的生命周期操作串行（watchdog 与命令不互踩）。
        """
        async with self._room_lock(room_id):
            old = self.monitors.get(room_id)
            new = self._new_monitor(
                room_id,
                inherit_state=old.export_state() if old else None,
            )
            if not await asyncio.to_thread(new.start):
                await asyncio.to_thread(new.stop)
                return False
            # await 期间房间可能已被删除、插件可能已进入关停
            if self._closing or not self.data.has_room(room_id):
                await asyncio.to_thread(new.stop)
                return False
            if old:
                await asyncio.to_thread(old.stop)
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

        所有通知（监控线程投递）都经由此任务在事件循环上发送：
        保证发送顺序、限制并发，并对失败目标做有限重试。
        """
        while True:
            try:
                await asyncio.sleep(NOTIFY_POLL_INTERVAL)

                # 先取空队列再处理，重试项将在下一轮投递（自带重试间隔）
                pending_items: list[PendingNotification] = []
                while True:
                    try:
                        pending_items.append(self._notification_queue.get_nowait())
                    except Empty:
                        break

                for item in pending_items:
                    # 多轮重试后降级 @全体，降低风控概率
                    use_at_all = item.retry_count < 2
                    failed = await self.notifier.send_to_subscribers(
                        item.subscriber_settings, item.message, use_at_all=use_at_all
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
                    # 只重试失败的目标，避免对已成功目标重复发送
                    item.subscriber_settings = {
                        umo: item.subscriber_settings[umo] for umo in failed
                    }
                    try:
                        self._notification_queue.put_nowait(item)
                        logger.warning(
                            f"{len(failed)} 个目标发送失败，将重试 "
                            f"({item.retry_count}/{NOTIFY_MAX_RETRIES})"
                        )
                    except Full:
                        logger.error("通知队列已满，放弃重试")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"通知队列处理器出错: {e}", exc_info=True)

    def _schedule_notification(
        self,
        subscriber_settings: dict[str, bool],
        message: str,
        dedup_key: tuple[str, int] | None = None,
    ) -> None:
        """调度通知发送（线程安全，可从监控线程调用）

        Args:
            subscriber_settings: {umo -> at_all} 每个订阅者的 @全体设置
            message: 通知消息内容
            dedup_key: (类型, 房间号)。同键通知在 NOTIFY_DEDUP_TTL 秒内
                只投递一次——吸收重启时新旧监控重叠窗口产生的重复事件
        """
        if not subscriber_settings:
            return
        if dedup_key is not None:
            now = time.monotonic()
            with self._dedup_lock:
                last = self._notify_dedup.get(dedup_key, 0.0)
                if now - last < NOTIFY_DEDUP_TTL:
                    logger.debug(f"通知去重: {dedup_key} 在 {NOTIFY_DEDUP_TTL}s 窗口内重复，忽略")
                    return
                self._notify_dedup[dedup_key] = now
                # 顺手清理过期条目，字典规模上界为 2×房间数
                for key in [
                    k for k, t in self._notify_dedup.items()
                    if now - t >= NOTIFY_DEDUP_TTL
                ]:
                    del self._notify_dedup[key]
        try:
            self._notification_queue.put_nowait(
                PendingNotification(
                    subscriber_settings=subscriber_settings, message=message
                )
            )
        except Full:
            logger.error("通知队列已满，丢弃一条通知")

    # ==================== 监控回调（运行于 pydouyu 线程）====================

    def _on_live_start(self, room_id: int, msg: dict) -> None:
        """开播回调 - 发送通知给所有订阅者"""
        # 获取所有订阅者的配置
        sub_configs = self.data.get_all_subscription_configs(room_id)
        if not sub_configs:
            return

        room_info = self.data.get_room(room_id)
        room_name = room_info.name if room_info else f"房间{room_id}"

        notification = self.notifier.build_notification(room_id, room_name)

        # 构建每个订阅者的 at_all 设置
        subscriber_settings = {
            umo: config.at_all for umo, config in sub_configs.items()
        }

        self._schedule_notification(
            subscriber_settings, notification, dedup_key=("live", room_id)
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

        notification = self.notifier.build_offline_notification(
            room_id, room_name, duration_seconds
        )

        # 下播通知不 @全体
        subscriber_settings = dict.fromkeys(sub_configs.keys(), False)

        self._schedule_notification(
            subscriber_settings, notification, dedup_key=("offline", room_id)
        )

    # ==================== 命令辅助 ====================

    def _save_warning(self) -> str:
        """数据保存失败时附加到回复中的警告"""
        if self.data.last_save_ok:
            return ""
        return "\n⚠️ 数据保存失败，重启后此更改可能丢失，请检查磁盘空间与文件权限"

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
            return None, False, f"⚠️ 直播间 {room_id} 不在监控列表中"

        sub_config = self.data.get_subscription_config(room_id, umo)
        if not sub_config:
            return None, False, (
                f"⚠️ 当前群还没有订阅直播间 {room_id}\n"
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
            return None, False, f"⚠️ 无法识别的参数「{enable}」，请使用 on/off 或留空切换"

        # 保存含磁盘 I/O，移出事件循环执行
        await asyncio.to_thread(
            self.data.update_subscription_config, room_id, umo, **{attr: new_status}
        )
        return room_info, new_status, None

    # ==================== 命令组 ====================

    @filter.command_group("douyu")
    def douyu(self):
        """斗鱼直播通知命令组"""
        pass

    @filter.permission_type(filter.PermissionType.ADMIN)
    @douyu.command("add")
    async def douyu_add(self, event: AstrMessageEvent, room_id: int, name: GreedyStr):
        """添加监控直播间（管理员）

        Args:
            room_id: 斗鱼直播间房间号
            name: 直播间名称（可选，可含空格，不填则自动获取）
        """
        if room_id <= 0:
            yield event.plain_result("⚠️ 房间号必须为正整数")
            return

        if self.data.has_room(room_id):
            yield event.plain_result(f"⚠️ 直播间 {room_id} 已在监控列表中")
            return

        # 验证房间是否存在，同时获取主播名称
        room_name = name.strip()
        api_info = await DouyuAPI.fetch_room_info(room_id)
        if not api_info:
            yield event.plain_result(
                f"⚠️ 无法获取直播间 {room_id} 的信息\n"
                f"请检查房间号是否正确，或稍后重试"
            )
            return

        # 如果没有提供名称，使用 API 获取的名称
        if not room_name:
            room_name = (
                api_info.get("owner_name")
                or api_info.get("nickname")
                or f"房间{room_id}"
            )
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
                f"✅ 已添加直播间监控\n"
                f"房间号: {room_id}\n"
                f"名称: {room_name}\n"
                f"使用 /douyu sub {room_id} 订阅开播通知"
                f"{self._save_warning()}"
            )
        else:
            await asyncio.to_thread(self.data.remove_room, room_id)
            yield event.plain_result("❌ 启动监控失败，请查看日志获取详细错误")

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
            yield event.plain_result(f"⚠️ 直播间 {room_id} 不在监控列表中")
            return

        room_name = room_info.name if room_info else str(room_id)
        yield event.plain_result(
            f"✅ 已删除直播间 {room_name}({room_id}) 的监控{self._save_warning()}"
        )

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
            monitor = self.monitors.get(room_id)
            status = "🟢 运行中" if monitor and monitor.is_healthy else "🔴 已停止"
            lines.append(
                f"{idx}. {info.name}\n"
                f"   房间号: {room_id}\n"
                f"   订阅数: {sub_count}\n"
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
        operator = event.get_sender_id()
        success, restored = await asyncio.to_thread(
            self.data.subscribe, room_id, umo, operator
        )
        if not success:
            yield event.plain_result(f"⚠️ 你已经订阅了直播间 {room_id}")
            return

        # 审计日志：记录订阅操作者
        logger.info(f"用户 {operator} 订阅了直播间 {room_id} ({umo})")

        # 检查监控状态并提示
        monitor = self.monitors.get(room_id)
        status_tip = ""
        if not (monitor and monitor.is_healthy):
            status_tip = "\n⚠️ 注意: 该直播间监控未运行，请联系管理员检查"
        restored_tip = "\n已自动恢复此前的订阅配置" if restored else ""

        yield event.plain_result(
            f"✅ 订阅成功！\n直播间: {room_info.name}({room_id})\n"
            f"开播时将在此处收到通知{restored_tip}{status_tip}{self._save_warning()}"
        )

    @douyu.command("unsub")
    async def douyu_unsub(self, event: AstrMessageEvent, room_id: int):
        """取消订阅直播间"""
        umo = event.unified_msg_origin
        room_info = self.data.get_room(room_id)
        room_name = room_info.name if room_info else str(room_id)

        if not await asyncio.to_thread(self.data.unsubscribe, room_id, umo):
            yield event.plain_result(f"⚠️ 你没有订阅直播间 {room_id}")
            return

        # 审计日志：记录退订操作者（配置保留，重新订阅时恢复）
        logger.info(
            f"用户 {event.get_sender_id()} 取消了直播间 {room_id} 的订阅 ({umo})"
        )

        yield event.plain_result(
            f"✅ 已取消订阅直播间 {room_name}({room_id})\n"
            f"订阅配置已保留，重新订阅时将自动恢复{self._save_warning()}"
        )

    @douyu.command("mysub")
    async def douyu_mysub(self, event: AstrMessageEvent):
        """查看当前群的订阅"""
        umo = event.unified_msg_origin
        room_ids = self.data.get_user_subscriptions(umo)

        if not room_ids:
            yield event.plain_result(
                "📋 当前群还没有订阅任何直播间\n"
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
                at_all_icon = "✅" if sub_config.at_all else "❌"
                my_subs.append(
                    f"• {room_name} ({room_id})\n  @全体:{at_all_icon}"
                )
            else:
                my_subs.append(f"• {room_name} ({room_id})")

        yield event.plain_result(
            "📋 当前群的订阅列表\n━━━━━━━━━━━━━━\n" + "\n".join(my_subs)
        )

    @douyu.command("status")
    async def douyu_status(self, event: AstrMessageEvent):
        """查看监控状态"""
        total_rooms = len(self.data.get_all_room_ids())
        running = sum(1 for m in self.monitors.values() if m.is_healthy)
        total_subs = self.data.get_total_subscriptions()

        yield event.plain_result(
            f"📊 斗鱼直播监控状态\n"
            f"━━━━━━━━━━━━━━\n"
            f"📺 监控直播间: {total_rooms}\n"
            f"🟢 运行中: {running}\n"
            f"👥 总订阅数: {total_subs}"
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
                yield event.plain_result(f"⚠️ 直播间 {room_id} 不在监控列表中")
                return

            if await self._restart_monitor(room_id):
                yield event.plain_result(f"✅ 直播间 {room_id} 监控已重启")
            else:
                yield event.plain_result(f"❌ 直播间 {room_id} 监控重启失败")
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
                f"✅ 已重启 {success}/{len(room_ids)} 个直播间监控"
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
            f"✅ 直播间 {room_info.name}({room_id})\n"
            f"当前群的 @全体成员 已{status_text}{self._save_warning()}"
        )

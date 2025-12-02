import asyncio
import json
import os
import time
from threading import Thread

from astrbot.api import logger, star
from astrbot.api.event import AstrMessageEvent, MessageEventResult, filter
from astrbot.api.message_components import AtAll, Plain
from astrbot.api.star import StarTools

try:
    from pydouyu.client import Client

    PYDOUYU_AVAILABLE = True
except ImportError:
    PYDOUYU_AVAILABLE = False
    logger.warning("pydouyu 库未安装，请运行: pip install pydouyu")


class DouyuMonitor:
    """斗鱼直播监控器"""

    def __init__(self, room_id: int, callback):
        self.room_id = room_id
        self.callback = callback
        self.client = None
        self.running = False
        self.thread = None
        self.last_live_status = False  # 上次的直播状态，防止重复通知

    def _rss_handler(self, msg):
        """处理直播状态变化"""
        try:
            ss = msg.get("ss", "0")
            ivl = msg.get("ivl", "1")
            # ss='1' 表示正在直播, ivl='0' 表示不是回放
            is_live = ss == "1" and ivl == "0"

            if is_live and not self.last_live_status:
                # 从未开播变为开播，触发通知
                logger.info(f"斗鱼直播间 {self.room_id} 开播了!")
                self.callback(self.room_id, msg)

            self.last_live_status = is_live
        except Exception as e:
            logger.error(f"处理直播状态时出错: {e}")

    def _run_client(self):
        """在线程中运行客户端"""
        try:
            self.client = Client(room_id=self.room_id)
            self.client.add_handler("rss", self._rss_handler)
            self.running = True
            self.client.start()
        except Exception as e:
            logger.error(f"斗鱼监控器 {self.room_id} 运行出错: {e}")
            self.running = False

    def start(self):
        """启动监控"""
        if not PYDOUYU_AVAILABLE:
            logger.error("pydouyu 库未安装，无法启动监控")
            return False

        if self.running:
            return True

        self.thread = Thread(target=self._run_client, daemon=True)
        self.thread.start()
        logger.info(f"斗鱼直播间 {self.room_id} 监控已启动")
        return True

    def stop(self):
        """停止监控"""
        self.running = False
        if self.client:
            try:
                self.client.stop()
            except Exception:
                pass
        logger.info(f"斗鱼直播间 {self.room_id} 监控已停止")


class Main(star.Star):
    """斗鱼直播开播通知插件

    使用方法:
    - /douyu add <房间号> - 添加监控直播间
    - /douyu del <房间号> - 删除监控直播间
    - /douyu ls - 查看监控列表
    - /douyu sub <房间号> - 订阅直播间开播通知
    - /douyu unsub <房间号> - 取消订阅
    - /douyu mysub - 查看我的订阅
    - /douyu status - 查看监控状态
    """

    def __init__(self, context: star.Context) -> None:
        super().__init__(context)
        self.context = context
        self.data_dir = StarTools.get_data_dir("astrbot_plugin_douyu_live")
        self.data_file = self.data_dir / "douyu_live_data.json"

        # 主事件循环引用（用于子线程回调）
        self.loop: asyncio.AbstractEventLoop = None

        # 数据结构
        self.monitors: dict[int, DouyuMonitor] = {}  # room_id -> DouyuMonitor
        self.subscriptions: dict[
            int, set[str]
        ] = {}  # room_id -> set of unified_msg_origin
        self.room_info: dict[int, dict] = {}  # room_id -> {name, added_by, added_time}

        # 加载配置
        self._load_data()

    async def initialize(self):
        """插件激活时启动所有监控"""
        # 保存主事件循环引用，用于子线程中的异步调用
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.get_event_loop()

        if not PYDOUYU_AVAILABLE:
            logger.error("pydouyu 库未安装，斗鱼直播通知插件无法正常工作")
            return

        for room_id in list(self.subscriptions.keys()):
            self._start_monitor(room_id)

        logger.info(f"斗鱼直播通知插件已启动，监控 {len(self.monitors)} 个直播间")

    async def terminate(self):
        """插件禁用时停止所有监控"""
        for monitor in self.monitors.values():
            monitor.stop()
        self.monitors.clear()
        self._save_data()
        logger.info("斗鱼直播通知插件已停止")

    def _load_data(self):
        """加载持久化数据"""
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, encoding="utf-8") as f:
                    data = json.load(f)
                    # 将字符串键转为整数
                    self.subscriptions = {
                        int(k): set(v) for k, v in data.get("subscriptions", {}).items()
                    }
                    self.room_info = {
                        int(k): v for k, v in data.get("room_info", {}).items()
                    }
            except Exception as e:
                logger.error(f"加载斗鱼直播数据失败: {e}")
                self.subscriptions = {}
                self.room_info = {}
        else:
            self.subscriptions = {}
            self.room_info = {}

    def _save_data(self):
        """保存数据到文件"""
        try:
            data = {
                "subscriptions": {
                    str(k): list(v) for k, v in self.subscriptions.items()
                },
                "room_info": {str(k): v for k, v in self.room_info.items()},
            }
            with open(self.data_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存斗鱼直播数据失败: {e}")

    def _start_monitor(self, room_id: int) -> bool:
        """启动单个房间的监控"""
        if room_id in self.monitors:
            return True

        monitor = DouyuMonitor(room_id, self._on_live_start)
        if monitor.start():
            self.monitors[room_id] = monitor
            return True
        return False

    def _stop_monitor(self, room_id: int):
        """停止单个房间的监控"""
        if room_id in self.monitors:
            self.monitors[room_id].stop()
            del self.monitors[room_id]

    def _on_live_start(self, room_id: int, msg: dict):
        """开播回调 - 发送通知给所有订阅者"""
        subscribers = self.subscriptions.get(room_id, set())
        if not subscribers:
            return

        room_info = self.room_info.get(room_id, {})
        room_name = room_info.get("name", f"房间{room_id}")
        at_all_enabled = room_info.get("at_all", False)
        now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        live_url = f"https://www.douyu.com/{room_id}"

        notification = (
            f"🎉 斗鱼直播开播通知\n"
            f"━━━━━━━━━━━━━━\n"
            f"📺 直播间: {room_name}\n"
            f"🔢 房间号: {room_id}\n"
            f"⏰ 时间: {now}\n"
            f"🔗 链接: {live_url}\n"
            f"━━━━━━━━━━━━━━\n"
            f"快去观看吧！"
        )

        # 异步发送通知（从子线程调度到主事件循环）
        if self.loop and self.loop.is_running():
            asyncio.run_coroutine_threadsafe(
                self._send_notifications(subscribers, notification, at_all_enabled),
                self.loop,
            )
        else:
            logger.error("事件循环不可用，无法发送开播通知")

    async def _send_notifications(
        self, subscribers: set[str], message: str, at_all: bool = False
    ):
        """发送通知给所有订阅者"""
        for umo in subscribers:
            try:
                result = MessageEventResult()
                if at_all:
                    result.chain.append(AtAll())
                    result.chain.append(Plain("\n"))
                result.chain.append(Plain(message))
                await self.context.send_message(umo, result)
                logger.info(f"已发送开播通知到: {umo}")
            except Exception as e:
                logger.error(f"发送通知失败 ({umo}): {e}")

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
            name: 直播间名称（可选）
        """
        if not PYDOUYU_AVAILABLE:
            yield event.plain_result(
                "❌ pydouyu 库未安装，请先安装: pip install pydouyu"
            )
            return

        if room_id in self.room_info:
            yield event.plain_result(f"⚠️ 直播间 {room_id} 已在监控列表中")
            return

        # 保存房间信息
        self.room_info[room_id] = {
            "name": name or f"房间{room_id}",
            "added_by": event.get_sender_id(),
            "added_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "at_all": False,  # 默认不开启 @全体成员
        }

        # 初始化订阅集合
        if room_id not in self.subscriptions:
            self.subscriptions[room_id] = set()

        # 启动监控
        if self._start_monitor(room_id):
            self._save_data()
            yield event.plain_result(
                f"✅ 已添加直播间监控\n"
                f"房间号: {room_id}\n"
                f"名称: {self.room_info[room_id]['name']}\n"
                f"使用 /douyu sub {room_id} 订阅开播通知"
            )
        else:
            del self.room_info[room_id]
            yield event.plain_result("❌ 启动监控失败，请检查房间号是否正确")

    @douyu.command("del")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def douyu_del(self, event: AstrMessageEvent, room_id: int):
        """删除监控直播间（管理员）"""
        if room_id not in self.room_info:
            yield event.plain_result(f"⚠️ 直播间 {room_id} 不在监控列表中")
            return

        room_name = self.room_info[room_id].get("name", str(room_id))

        # 停止监控
        self._stop_monitor(room_id)

        # 删除数据
        del self.room_info[room_id]
        if room_id in self.subscriptions:
            del self.subscriptions[room_id]

        self._save_data()
        yield event.plain_result(f"✅ 已删除直播间 {room_name}({room_id}) 的监控")

    @douyu.command("ls")
    async def douyu_ls(self, event: AstrMessageEvent):
        """查看监控列表"""
        if not self.room_info:
            yield event.plain_result(
                "📋 当前没有监控的直播间\n使用 /douyu add <房间号> 添加"
            )
            return

        lines = ["📋 斗鱼直播监控列表", "━━━━━━━━━━━━━━"]
        for idx, (room_id, info) in enumerate(self.room_info.items(), 1):
            sub_count = len(self.subscriptions.get(room_id, set()))
            status = "🟢 运行中" if room_id in self.monitors else "🔴 已停止"
            at_all_status = "✅" if info.get("at_all", False) else "❌"
            lines.append(
                f"{idx}. {info['name']}\n"
                f"   房间号: {room_id}\n"
                f"   订阅数: {sub_count}\n"
                f"   @全体: {at_all_status}\n"
                f"   状态: {status}"
            )

        yield event.plain_result("\n".join(lines))

    @douyu.command("sub")
    async def douyu_sub(self, event: AstrMessageEvent, room_id: int):
        """订阅直播间开播通知"""
        if room_id not in self.room_info:
            yield event.plain_result(
                f"⚠️ 直播间 {room_id} 不在监控列表中\n"
                f"请联系管理员添加，或使用 /douyu ls 查看可订阅的直播间"
            )
            return

        umo = event.unified_msg_origin

        if room_id not in self.subscriptions:
            self.subscriptions[room_id] = set()

        if umo in self.subscriptions[room_id]:
            yield event.plain_result(f"⚠️ 你已经订阅了直播间 {room_id}")
            return

        self.subscriptions[room_id].add(umo)
        self._save_data()

        room_name = self.room_info[room_id].get("name", str(room_id))
        yield event.plain_result(
            f"✅ 订阅成功！\n直播间: {room_name}({room_id})\n开播时将在此处收到通知"
        )

    @douyu.command("unsub")
    async def douyu_unsub(self, event: AstrMessageEvent, room_id: int):
        """取消订阅直播间"""
        umo = event.unified_msg_origin

        if room_id not in self.subscriptions or umo not in self.subscriptions[room_id]:
            yield event.plain_result(f"⚠️ 你没有订阅直播间 {room_id}")
            return

        self.subscriptions[room_id].discard(umo)
        self._save_data()

        room_name = self.room_info.get(room_id, {}).get("name", str(room_id))
        yield event.plain_result(f"✅ 已取消订阅直播间 {room_name}({room_id})")

    @douyu.command("mysub")
    async def douyu_mysub(self, event: AstrMessageEvent):
        """查看我的订阅"""
        umo = event.unified_msg_origin
        my_subs = []

        for room_id, subscribers in self.subscriptions.items():
            if umo in subscribers:
                room_name = self.room_info.get(room_id, {}).get("name", str(room_id))
                my_subs.append(f"• {room_name} ({room_id})")

        if not my_subs:
            yield event.plain_result(
                "📋 你还没有订阅任何直播间\n"
                "使用 /douyu ls 查看可订阅的直播间\n"
                "使用 /douyu sub <房间号> 订阅"
            )
            return

        yield event.plain_result(
            "📋 你的订阅列表\n━━━━━━━━━━━━━━\n" + "\n".join(my_subs)
        )

    @douyu.command("status")
    async def douyu_status(self, event: AstrMessageEvent):
        """查看监控状态"""
        if not PYDOUYU_AVAILABLE:
            yield event.plain_result("⚠️ pydouyu 库未安装\n请运行: pip install pydouyu")
            return

        total_rooms = len(self.room_info)
        running = sum(1 for m in self.monitors.values() if m.running)
        total_subs = sum(len(s) for s in self.subscriptions.values())

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
    async def douyu_restart(self, event: AstrMessageEvent, room_id: int = None):
        """重启监控（管理员）

        Args:
            room_id: 指定房间号，不填则重启所有
        """
        if room_id:
            if room_id not in self.room_info:
                yield event.plain_result(f"⚠️ 直播间 {room_id} 不在监控列表中")
                return

            self._stop_monitor(room_id)
            if self._start_monitor(room_id):
                yield event.plain_result(f"✅ 直播间 {room_id} 监控已重启")
            else:
                yield event.plain_result(f"❌ 直播间 {room_id} 监控重启失败")
        else:
            # 重启所有
            for rid in list(self.monitors.keys()):
                self._stop_monitor(rid)

            success = 0
            for rid in self.room_info.keys():
                if self._start_monitor(rid):
                    success += 1

            yield event.plain_result(
                f"✅ 已重启 {success}/{len(self.room_info)} 个直播间监控"
            )

    @douyu.command("atall")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def douyu_atall(
        self, event: AstrMessageEvent, room_id: int, enable: str = ""
    ):
        """开启/关闭 @全体成员（管理员）

        Args:
            room_id: 斗鱼直播间房间号
            enable: on/off 或留空切换状态
        """
        if room_id not in self.room_info:
            yield event.plain_result(f"⚠️ 直播间 {room_id} 不在监控列表中")
            return

        current = self.room_info[room_id].get("at_all", False)

        if enable.lower() == "on":
            new_status = True
        elif enable.lower() == "off":
            new_status = False
        else:
            # 切换状态
            new_status = not current

        self.room_info[room_id]["at_all"] = new_status
        self._save_data()

        room_name = self.room_info[room_id].get("name", str(room_id))
        status_text = "开启" if new_status else "关闭"
        yield event.plain_result(
            f"✅ 直播间 {room_name}({room_id})\n@全体成员 已{status_text}"
        )

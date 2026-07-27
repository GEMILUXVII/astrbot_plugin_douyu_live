"""2.2.0 六件套功能测试"""

import asyncio
import json
import time
from pathlib import Path
from types import SimpleNamespace

from astrbot_plugin_douyu_live.models.room import RoomInfo
from astrbot_plugin_douyu_live.models.subscription import SubscriptionConfig


class FakeEvent:
    """最小命令事件桩"""

    def __init__(self, umo="umoT", sender="u1", admin=False):
        self.unified_msg_origin = umo
        self._sender = sender
        self._admin = admin
        self.replies = []

    def get_sender_id(self):
        return self._sender

    def is_admin(self):
        return self._admin

    def plain_result(self, text):
        self.replies.append(text)
        return text


async def drain_command(gen):
    return [x async for x in gen]


# ==================== _conf_schema ====================


def test_conf_schema_valid_and_matches_defaults():
    """schema 类型在宿主白名单内,default 与运行时 DEFAULT_CONFIG 一致"""
    from astrbot_plugin_douyu_live.main import DEFAULT_CONFIG

    root = Path(__file__).resolve().parent.parent
    schema = json.loads((root / "_conf_schema.json").read_text(encoding="utf-8"))
    # 宿主 DEFAULT_VALUE_MAP 白名单(core/config/default.py)
    allowed = {
        "int",
        "float",
        "bool",
        "string",
        "text",
        "list",
        "file",
        "object",
        "template_list",
    }
    for key, spec in schema.items():
        assert spec["type"] in allowed, f"{key} 类型 {spec['type']} 不在宿主白名单"
        assert "description" in spec and "default" in spec
        assert key in DEFAULT_CONFIG, f"{key} 缺运行时默认值"
        assert spec["default"] == DEFAULT_CONFIG[key], f"{key} 默认值不一致"
        if "options" in spec:
            # options 是 string 类型的属性,不是独立类型(宿主实现如此)
            assert spec["type"] == "string"
            assert spec["default"] in spec["options"]
    assert set(schema) == set(DEFAULT_CONFIG)


def test_cfg_fallback_and_override(make_main):
    from astrbot_plugin_douyu_live.main import Main

    m = make_main()
    assert m._cfg("notify_enrich") is True  # 无 config -> 默认

    class Ctx:
        async def send_message(self, umo, result):
            return True

    m2 = Main(
        Ctx(),
        config={
            "notify_enrich": False,
            "notify_cooldown": 5,
            "offline_confirmation": 7,
            "status_reconcile_interval": 0,
        },
    )
    assert m2._cfg("notify_enrich") is False
    assert m2._cfg("notify_cooldown") == 5
    assert m2._cfg("subscribe_permission") == "everyone"  # 未覆盖走默认
    assert m2._new_monitor(1)._periodic_resync_interval == 0
    assert m2._new_monitor(1)._offline_confirmation == 7

    m3 = Main(Ctx())
    assert m3._new_monitor(1)._periodic_resync_interval == 300
    assert m3._new_monitor(1)._offline_confirmation == 10


# ==================== offline 开关 ====================


def test_subscription_offline_notify_roundtrip():
    cfg = SubscriptionConfig(at_all=True, offline_notify=False)
    restored = SubscriptionConfig.from_dict(cfg.to_dict())
    assert restored.offline_notify is False
    # 旧数据无字段 -> 默认开
    legacy = SubscriptionConfig.from_dict({"at_all": True})
    assert legacy.offline_notify is True


def test_offline_filtering(make_main):
    """关闭下播通知的群不进下播扇出"""
    m = make_main()
    m.data.add_room(910, RoomInfo(name="N"))
    m.data.subscribe(910, "umoOn")
    m.data.subscribe(910, "umoOff")
    m.data.update_subscription_config(910, "umoOff", offline_notify=False)

    m._on_live_end(910, 120.0)
    assert m._notification_queue.qsize() == 1
    item = m._notification_queue.get_nowait()
    assert set(item.subscriber_settings) == {"umoOn"}
    assert item.kind == "offline" and item.duration == 120.0


def test_offline_command_toggle(make_main):
    m = make_main()
    m.data.add_room(911, RoomInfo(name="N"))
    m.data.subscribe(911, "umoT")
    event = FakeEvent()

    async def run():
        await drain_command(m.douyu_offline(event, 911, "off"))
        cfg = m.data.get_subscription_config(911, "umoT")
        assert cfg.offline_notify is False
        await drain_command(m.douyu_offline(event, 911, ""))  # 切换回开
        assert m.data.get_subscription_config(911, "umoT").offline_notify is True

    asyncio.run(run())
    assert "已关闭" in event.replies[0]


def test_subscribe_permission_gate(make_main):
    from astrbot_plugin_douyu_live.main import Main

    class Ctx:
        async def send_message(self, umo, result):
            return True

    m = Main(Ctx(), config={"subscribe_permission": "admin"})
    m.data.add_room(912, RoomInfo(name="N"))
    member = FakeEvent(admin=False)
    admin = FakeEvent(admin=True)

    async def run():
        await drain_command(m.douyu_sub(member, 912))
        assert "限制为管理员" in member.replies[-1]
        await drain_command(m.douyu_sub(admin, 912))
        assert "订阅成功" in admin.replies[-1]

    asyncio.run(run())


# ==================== 富化 + 队列侧构建 ====================


def test_live_notification_enriched_in_queue(make_main, monkeypatch):
    m = make_main()
    m.data.add_room(920, RoomInfo(name="主播A"))
    m.data.subscribe(920, "umoE")

    async def fake_get(room_id, **kwargs):
        return SimpleNamespace(
            title="今晚上分",
            category="DOTA2",
            cover_url="https://x/cover.jpg",
        )

    monkeypatch.setattr(m._room_cache, "get", fake_get)
    sent = []

    async def fake_send(settings, message, use_at_all=True, cover_url=None):
        sent.append((message, cover_url))
        return set()

    m.notifier.send_to_subscribers = fake_send

    async def run():
        m._on_live_start(920, {})
        task = asyncio.create_task(m._process_notification_queue())
        await asyncio.sleep(1.2)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(run())
    assert sent, "通知未发送"
    message, cover = sent[0]
    assert "今晚上分" in message and "DOTA2" in message
    assert cover == "https://x/cover.jpg"
    assert "主播A" in message


def test_enrich_failure_degrades(make_main, monkeypatch):
    m = make_main()
    m.data.add_room(921, RoomInfo(name="主播B"))
    m.data.subscribe(921, "umoE")

    async def failing_get(room_id, **kwargs):
        raise RuntimeError("接口不可用")

    monkeypatch.setattr(m._room_cache, "get", failing_get)
    sent = []

    async def fake_send(settings, message, use_at_all=True, cover_url=None):
        sent.append((message, cover_url))
        return set()

    m.notifier.send_to_subscribers = fake_send

    async def run():
        m._on_live_start(921, {})
        task = asyncio.create_task(m._process_notification_queue())
        await asyncio.sleep(1.2)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(run())
    assert sent
    message, cover = sent[0]
    assert "主播B" in message and cover is None  # 降级为基础文本


def test_live_notification_reuses_monitor_snapshot(make_main, monkeypatch):
    m = make_main()
    m.data.add_room(922, RoomInfo(name="主播C"))
    m.data.subscribe(922, "umoE")

    async def unexpected_get(room_id, **kwargs):
        raise AssertionError("monitor snapshot should avoid a second HTTP request")

    monkeypatch.setattr(m._room_cache, "get", unexpected_get)
    m._on_live_start(
        922,
        {
            "type": "aiodouyu.resync",
            "room_info": {
                "title": "快照标题",
                "category": "快照分类",
                "cover_url": "https://x/snapshot.jpg",
            },
        },
    )
    item = m._notification_queue.get_nowait()

    asyncio.run(m._build_notification_message(item))

    assert item.snapshot_available is True
    assert "快照标题" in item.message
    assert "快照分类" in item.message
    assert item.cover_url == "https://x/snapshot.jpg"


# ==================== /douyu live ====================


def test_live_command(make_main, monkeypatch):
    m = make_main()
    m.data.add_room(930, RoomInfo(name="在播主播"))
    m.data.add_room(931, RoomInfo(name="下播主播"))
    m.monitors[930] = SimpleNamespace(
        last_live_status=True, live_start_time=time.time() - 3900
    )
    m.monitors[931] = SimpleNamespace(last_live_status=False, live_start_time=None)

    async def fake_get(room_id, **kwargs):
        return SimpleNamespace(title="标题X", category="英雄联盟")

    monkeypatch.setattr(m._room_cache, "get", fake_get)
    event = FakeEvent()

    async def run():
        await drain_command(m.douyu_live(event))

    asyncio.run(run())
    reply = event.replies[0]
    assert "在播主播" in reply and "下播主播" not in reply
    assert "标题X" in reply and "英雄联盟" in reply
    assert "已播 1小时5分钟" in reply


def test_live_command_empty(make_main):
    m = make_main()
    event = FakeEvent()

    async def run():
        await drain_command(m.douyu_live(event))

    asyncio.run(run())
    assert "没有监控中的房间在播" in event.replies[0]


# ==================== help ====================


def test_help_command_admin_sections(make_main):
    m = make_main()
    member, admin = FakeEvent(admin=False), FakeEvent(admin=True)

    async def run():
        await drain_command(m.douyu_help(member))
        await drain_command(m.douyu_help(admin))

    asyncio.run(run())
    assert "/douyu sub" in member.replies[0]
    assert "/douyu add" not in member.replies[0]  # 普通用户不见管理段
    assert "/douyu add" in admin.replies[0]


# ==================== 场次落盘 + 快照回灌 ====================


def test_session_log_append_and_prune(data_dir):
    from astrbot_plugin_douyu_live.storage.session_log import SessionLog

    log = SessionLog(Path(data_dir), retention_days=90)
    old_ts = time.time() - 100 * 86400
    log.append(940, {"e": "start", "ts": old_ts, "title": "旧", "cat": ""})
    log.append(940, {"e": "start", "ts": time.time(), "title": "新", "cat": "x"})
    log.prune()
    lines = (
        (Path(data_dir) / "sessions" / "940.jsonl")
        .read_text(encoding="utf-8")
        .strip()
        .splitlines()
    )
    assert len(lines) == 1 and "新" in lines[0]

    disabled = SessionLog(Path(data_dir), retention_days=0)
    assert not disabled.enabled
    disabled.append(941, {"e": "start", "ts": time.time()})
    assert not (Path(data_dir) / "sessions" / "941.jsonl").exists()


def test_monitor_state_store_roundtrip(data_dir):
    from astrbot_plugin_douyu_live.storage.session_log import MonitorStateStore

    store = MonitorStateStore(Path(data_dir))
    store.save({930: {"last_live_status": True, "has_announced_live": True}})
    loaded = store.load_and_clear()
    assert loaded[930]["last_live_status"] is True
    assert store.load_and_clear() == {}  # 一次性消费


def test_monitor_state_store_stale_ignored(data_dir):
    from astrbot_plugin_douyu_live.storage import session_log as sl

    store = sl.MonitorStateStore(Path(data_dir))
    store.save({1: {"last_live_status": True}})
    # 篡改 saved_at 使其超过 24h
    payload = json.loads(store.path.read_text(encoding="utf-8"))
    payload["saved_at"] = time.time() - sl.STATE_MAX_AGE - 10
    store.path.write_text(json.dumps(payload), encoding="utf-8")
    assert store.load_and_clear() == {}


def test_pending_notification_store_is_one_shot(data_dir):
    from astrbot_plugin_douyu_live.storage.session_log import (
        PendingNotificationStore,
    )

    store = PendingNotificationStore(Path(data_dir))
    records = [{"message": "hello", "subscriber_settings": {"umo": False}}]
    store.save(records)
    assert store.load_and_clear() == records
    assert store.load_and_clear() == []


# ==================== catchup_announce ====================


def test_monitor_announce_initial_live_off(fake_time):
    from astrbot_plugin_douyu_live.core.monitor import DouyuMonitor

    events = []
    m = DouyuMonitor(
        950,
        live_callback=lambda r, msg: events.append("live"),
        offline_callback=lambda r, d: events.append("off"),
        announce_initial_live=False,
    )
    m._rss_handler({"ss": "1", "ivl": "0"})  # 首次观测已在播
    assert events == []  # 不补发
    assert m.last_live_status is True  # 但状态被接管
    fake_time.now = 1100.0
    m._rss_handler({"ss": "0", "ivl": "0"})  # 初始开播静默,后续下播仍需通知
    assert events == ["off"]
    fake_time.now = 1200.0
    m._rss_handler({"ss": "1", "ivl": "0"})  # 下一场正常播报
    assert events == ["off", "live"]

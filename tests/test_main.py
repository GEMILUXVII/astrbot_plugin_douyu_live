"""Main 插件层：通知队列、去重、开关命令测试"""

import asyncio
import time

from astrbot_plugin_douyu_live.models.room import RoomInfo


def test_no_gift_residue(make_main):
    m = make_main()
    assert not hasattr(m, "_on_gift")


def test_queue_retries_only_failed_targets(make_main, monkeypatch):
    """失败目标按退避重试，且只重试失败的目标"""
    import astrbot_plugin_douyu_live.main as main_mod

    # 缩小退避让测试窗口内能观察到重试；真实值为 5/15/45/120s
    monkeypatch.setattr(main_mod, "NOTIFY_RETRY_BACKOFF_BASE", 0.2)
    m = make_main()
    calls = []

    class FakeNotifier:
        async def send_to_subscribers(
            self, settings, message, use_at_all=True, cover_url=None
        ):
            calls.append(dict(settings))
            return {u for u in settings if u == "bad"} if len(calls) < 3 else set()

    m.notifier = FakeNotifier()
    m._schedule_notification({"good": True, "bad": False}, "msg")

    async def run():
        task = asyncio.create_task(m._process_notification_queue())
        await asyncio.sleep(2.2)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(run())
    assert calls and set(calls[0]) == {"good", "bad"}
    assert len(calls) >= 2 and set(calls[1]) == {"bad"}


def test_retry_backoff_schedule():
    """退避序列 5/15/45/120 且封顶"""
    from astrbot_plugin_douyu_live.main import _retry_backoff

    assert [_retry_backoff(n) for n in (1, 2, 3, 4, 5)] == [
        5.0,
        15.0,
        45.0,
        120.0,
        120.0,
    ]


def test_dedup_recorded_only_after_enqueue(make_main):
    """队满丢弃时不得记录去重时间戳（否则窗口内补发被误吸收）"""
    m = make_main()
    m.data.add_room(900, RoomInfo(name="F"))
    m.data.subscribe(900, "umoF")

    # 塞满队列
    import astrbot_plugin_douyu_live.main as main_mod

    while m._notification_queue.qsize() < main_mod.NOTIFY_QUEUE_MAX:
        m._notification_queue.put_nowait(main_mod.PendingNotification())

    m._on_live_start(900, {})  # 队满 -> 丢弃，且不应记 dedup
    assert ("live", 900) not in m._notify_dedup

    # 腾出空间后窗口内补发必须放行
    m._notification_queue.get_nowait()
    m._on_live_start(900, {})
    assert ("live", 900) in m._notify_dedup


def test_notify_dedup_window(make_main):
    """同房间同类通知在去重窗口内只投递一次（吸收重启重叠期重复事件）"""
    m = make_main()
    m.data.add_room(800, RoomInfo(name="D"))
    m.data.subscribe(800, "umoD")

    m._on_live_start(800, {})
    m._on_live_start(800, {})  # 窗口内重复 -> 抑制
    assert m._notification_queue.qsize() == 1

    m._on_live_end(800, 60.0)  # 不同类型 -> 放行
    assert m._notification_queue.qsize() == 2


def test_schedule_empty_settings_noop(make_main):
    m = make_main()
    m._schedule_notification({}, "msg")
    assert m._notification_queue.qsize() == 0


def test_notification_queue_wakes_without_poll_delay(make_main):
    """A newly queued notification should wake the worker immediately."""
    m = make_main()
    sending = asyncio.Event()

    class FakeNotifier:
        async def send_to_subscribers(
            self, settings, message, use_at_all=True, cover_url=None
        ):
            sending.set()
            return set()

    m.notifier = FakeNotifier()
    m._schedule_notification({"umo": False}, "msg")

    async def run():
        task = asyncio.create_task(m._process_notification_queue())
        await asyncio.wait_for(sending.wait(), timeout=0.3)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(run())


def test_cancelled_send_is_returned_to_queue(make_main, monkeypatch):
    import astrbot_plugin_douyu_live.main as main_mod

    monkeypatch.setattr(main_mod, "NOTIFY_POLL_INTERVAL", 0.01)
    m = make_main()
    sending = asyncio.Event()

    class BlockingNotifier:
        async def send_to_subscribers(
            self, settings, message, use_at_all=True, cover_url=None
        ):
            sending.set()
            await asyncio.Event().wait()

    m.notifier = BlockingNotifier()
    m._schedule_notification({"umo": False}, "msg")

    async def run():
        task = asyncio.create_task(m._process_notification_queue())
        await asyncio.wait_for(sending.wait(), timeout=1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(run())
    assert m._notification_queue.qsize() == 1


def test_pending_notification_round_trip():
    from astrbot_plugin_douyu_live.main import PendingNotification

    item = PendingNotification(
        subscriber_settings={"default:GroupMessage:1": False},
        message="hello",
        retry_count=2,
        next_attempt_at=time.monotonic() + 10,
        kind="live",
        room_id=1,
        room_name="room",
        event_ts=time.time(),
        title="title",
        category="category",
        snapshot_available=True,
        realtime=True,
        cover_url="https://example.com/cover.jpg",
    )
    restored = PendingNotification.from_record(item.to_record())
    assert restored is not None
    assert restored.subscriber_settings == item.subscriber_settings
    assert restored.message == item.message
    assert restored.retry_count == 2
    assert restored.kind == "live"
    assert restored.room_id == 1
    assert restored.title == "title"
    assert restored.category == "category"
    assert restored.snapshot_available is True
    assert restored.realtime is True
    assert restored.next_attempt_at > time.monotonic()


def test_reload_restores_unsent_notification(make_main):
    first = make_main()
    first._schedule_notification(
        {"default:GroupMessage:1": False},
        "persist me",
        kind="live",
        room_id=1,
        event_ts=time.time(),
    )
    asyncio.run(first.terminate())

    second = make_main()

    async def run():
        await second.initialize()
        assert second._notification_queue.qsize() == 1
        restored = second._notification_queue.get_nowait()
        assert restored.message == "persist me"
        second._notification_queue.put_nowait(restored)
        await second.terminate()

    asyncio.run(run())


def test_resolve_toggle(make_main):
    m = make_main()
    m.data.add_room(700, RoomInfo(name="T"))

    async def run():
        _, _, err = await m._resolve_toggle(700, "u1", "at_all", "")
        assert err is not None and "还没有订阅" in err

        m.data.subscribe(700, "u1")
        _, status, err = await m._resolve_toggle(700, "u1", "at_all", "")
        assert err is None and status is True  # 空参数切换

        _, status, err = await m._resolve_toggle(700, "u1", "at_all", "off")
        assert err is None and status is False

        _, status, err = await m._resolve_toggle(700, "u1", "at_all", "on")
        assert err is None and status is True

        # 不认识的参数必须报错而非静默切换
        _, _, err = await m._resolve_toggle(700, "u1", "at_all", "开启")
        assert err is not None and "无法识别" in err

        _, _, err = await m._resolve_toggle(999, "u1", "at_all", "")
        assert err is not None and "不在监控列表" in err

    asyncio.run(run())

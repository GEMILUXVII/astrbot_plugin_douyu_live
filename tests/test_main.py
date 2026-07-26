"""Main 插件层：通知队列、去重、开关命令测试"""

import asyncio

from astrbot_plugin_douyu_live.models.room import RoomInfo


def test_no_gift_residue(make_main):
    m = make_main()
    assert not hasattr(m, "_on_gift")


def test_queue_retries_only_failed_targets(make_main):
    m = make_main()
    calls = []

    class FakeNotifier:
        async def send_to_subscribers(self, settings, message, use_at_all=True):
            calls.append(dict(settings))
            return {u for u in settings if u == "bad"} if len(calls) < 3 else set()

    m.notifier = FakeNotifier()
    m._schedule_notification({"good": True, "bad": False}, "msg")

    async def run():
        task = asyncio.get_event_loop().create_task(m._process_notification_queue())
        await asyncio.sleep(2.2)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(run())
    assert calls and set(calls[0]) == {"good", "bad"}
    assert len(calls) >= 2 and set(calls[1]) == {"bad"}


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

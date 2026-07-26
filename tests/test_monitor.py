"""DouyuMonitor 状态机与生命周期测试"""

import threading

import astrbot_plugin_douyu_live.core.monitor as monitor_mod
from astrbot_plugin_douyu_live.core.monitor import DouyuMonitor


def test_initial_live_announced(fake_time):
    events = []
    m = DouyuMonitor(1, live_callback=lambda r, msg: events.append(("live", r)))
    m._rss_handler({"ss": "1", "ivl": "0"})
    assert events == [("live", 1)]


def test_initial_offline_silent(fake_time):
    events = []
    m = DouyuMonitor(1, live_callback=lambda r, msg: events.append("live"))
    m._rss_handler({"ss": "0", "ivl": "0"})
    assert events == []
    assert m.last_live_status is False


def test_cooldown_records_pending_and_reconciles(fake_time):
    """冷却期内的真实转换必须补发，且不能破坏状态机（回归：漏发下播+漏发下一场开播）"""
    events = []
    m = DouyuMonitor(
        1,
        live_callback=lambda r, msg: events.append(("live", r)),
        offline_callback=lambda r, d: events.append(("off", r, round(d))),
    )
    m._rss_handler({"ss": "1", "ivl": "0"})  # 开播播报
    fake_time.now = 1018.0
    m._rss_handler({"ss": "0", "ivl": "0"})  # 冷却期内下播 -> 待定
    assert events == [("live", 1)]
    assert m.last_live_status is True  # 冷却期内状态不被破坏

    fake_time.now = 1025.0
    m._reconcile_pending()  # 冷却未结束
    assert events == [("live", 1)]

    fake_time.now = 1031.0
    m._reconcile_pending()  # 冷却结束，补发下播
    assert events == [("live", 1), ("off", 1, 31)]
    assert m.last_live_status is False

    fake_time.now = 2000.0
    m._rss_handler({"ss": "1", "ivl": "0"})  # 下一场开播不能被吞
    assert events[-1] == ("live", 1)


def test_jitter_suppressed(fake_time):
    """冷却期内闪断又恢复：不发下播、不重复开播"""
    events = []
    m = DouyuMonitor(
        2,
        live_callback=lambda r, msg: events.append("live"),
        offline_callback=lambda r, d: events.append("off"),
    )
    m._rss_handler({"ss": "0", "ivl": "0"})
    fake_time.now = 1050.0
    m._rss_handler({"ss": "1", "ivl": "0"})
    fake_time.now = 1055.0
    m._rss_handler({"ss": "0", "ivl": "0"})  # 闪断 -> 待定
    fake_time.now = 1058.0
    m._rss_handler({"ss": "1", "ivl": "0"})  # 恢复 -> 待定清除
    fake_time.now = 1100.0
    m._reconcile_pending()
    assert events == ["live"]


def test_inherit_state_no_reannounce(fake_time):
    """重启继承状态：不重复播报，下播时长跨重启正确"""
    m1 = DouyuMonitor(3, live_callback=lambda r, msg: None)
    m1._rss_handler({"ss": "1", "ivl": "0"})

    events = []
    m2 = DouyuMonitor(
        3,
        live_callback=lambda r, msg: events.append("live"),
        offline_callback=lambda r, d: events.append(("off", round(d))),
        inherit_state=m1.export_state(),
    )
    fake_time.now = 1005.0
    m2._rss_handler({"ss": "1", "ivl": "0"})
    assert events == []
    fake_time.now = 1100.0
    m2._rss_handler({"ss": "0", "ivl": "0"})
    assert events == [("off", 100)]


def test_pending_state_survives_restart(fake_time):
    """冷却期待定转换必须随 export_state 继承并在新监控器补发"""
    m1 = DouyuMonitor(9, live_callback=lambda r, msg: None)
    m1._rss_handler({"ss": "1", "ivl": "0"})
    fake_time.now = 1010.0
    m1._rss_handler({"ss": "0", "ivl": "0"})  # 冷却期内 -> 待定
    state = m1.export_state()
    assert state["pending_status"] is False

    events = []
    m2 = DouyuMonitor(
        9,
        offline_callback=lambda r, d: events.append(("off", round(d))),
        inherit_state=state,
    )
    fake_time.now = 1031.0
    m2._reconcile_pending()
    assert events == [("off", 31)]


def test_stop_flag_blocks_handlers():
    events = []
    m = DouyuMonitor(4, live_callback=lambda r, msg: events.append("live"))
    m._stop_flag = True
    m._rss_handler({"ss": "1", "ivl": "0"})
    assert events == []
    assert m.last_live_status is None


def test_stop_window_does_not_leak_client():
    """stop() 与线程启动窗口竞态时，被创建的 client 必须被停掉"""
    created = []
    real_client = monitor_mod.Client

    class TrackingClient(real_client):
        def __init__(self, room_id):
            super().__init__(room_id)
            created.append(self)
            self.stopped = False

        def stop(self):
            self.stopped = True
            super().stop()

    monitor_mod.Client = TrackingClient
    try:
        m = DouyuMonitor(11)
        m._stop_flag = True  # 线程启动前就已请求停止
        t = threading.Thread(target=m._run_client)
        t.start()
        t.join(timeout=5)
        assert not t.is_alive()
        assert all(c.stopped for c in created) or not created
    finally:
        monitor_mod.Client = real_client


def test_no_gift_residue():
    m = DouyuMonitor(1)
    assert not hasattr(m, "gift_callback")
    assert not hasattr(m, "_dgb_handler")

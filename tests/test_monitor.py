"""DouyuMonitor 状态机与生命周期测试"""

import asyncio
from types import SimpleNamespace

import astrbot_plugin_douyu_live.core.monitor as monitor_mod
from astrbot_plugin_douyu_live.core.monitor import DouyuMonitor
from conftest import FakeDanmakuClient


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


def test_no_gift_residue():
    m = DouyuMonitor(1)
    assert not hasattr(m, "gift_callback")
    assert not hasattr(m, "_dgb_handler")


# ==================== 协程生命周期（注入假客户端，不联网）====================


async def _drain():
    """让事件循环处理完已就绪的回调"""
    for _ in range(10):
        await asyncio.sleep(0)


def test_run_dispatches_rss_and_stop_closes_client():
    """消费协程分发 rss 到状态机；stop() 关闭客户端并结束任务"""

    async def run():
        events = []
        client = FakeDanmakuClient()
        m = DouyuMonitor(
            5,
            live_callback=lambda r, msg: events.append(("live", r)),
            client_factory=lambda: client,
        )
        assert m.start() is True
        assert m.is_healthy

        client.push({"type": "rss", "ss": "1", "ivl": "0"})
        await _drain()
        assert events == [("live", 5)]

        await m.stop()
        assert client.closed
        assert not m.is_healthy
        assert m._task.done()

    asyncio.run(run())


def test_connected_event_triggers_resync(monkeypatch):
    """EVENT_CONNECTED 必须触发 HTTP 对账并喂入状态机（断连窗口补偿）

    对账由校准协程延迟执行（不在消费循环内联），故需推进真实时间。
    """

    async def run():
        monkeypatch.setattr(monitor_mod, "RECONCILE_INTERVAL", 0.01)
        events = []
        calls = []

        async def fake_fetch_room(room_id, *, source, timeout):
            calls.append((room_id, source))
            return SimpleNamespace(is_live=True, started_at=999.0)

        monkeypatch.setattr(monitor_mod, "fetch_room", fake_fetch_room)

        client = FakeDanmakuClient()
        m = DouyuMonitor(
            6,
            live_callback=lambda r, msg: events.append(("live", r, msg["type"])),
            client_factory=lambda: client,
        )
        m.start()
        from aiodouyu import EVENT_CONNECTED

        client.push({"type": EVENT_CONNECTED, "roomid": "6"})
        await _drain()
        assert m.connected is True  # 事件同步生效,不等对账
        assert m._resync_pending  # 对账已登记
        await asyncio.sleep(0.05)  # 校准协程执行对账

        assert calls == [(6, "betard")]  # 对账固定走 betard（open 不识别轮播）
        assert events == [("live", 6, "aiodouyu.resync")]
        assert m.live_start_time == 999.0  # 用对账源的开播时间修正时长基准
        assert not m._resync_pending

        await m.stop()

    asyncio.run(run())


def test_resync_failure_is_skipped(monkeypatch):
    """对账失败不得破坏状态机或杀死协程,且已登记退避重试"""

    async def run():
        monkeypatch.setattr(monitor_mod, "RECONCILE_INTERVAL", 0.01)
        events = []

        async def failing_fetch_room(room_id, *, source, timeout):
            raise RuntimeError("接口不可用")

        monkeypatch.setattr(monitor_mod, "fetch_room", failing_fetch_room)

        client = FakeDanmakuClient()
        m = DouyuMonitor(
            7,
            live_callback=lambda r, msg: events.append("live"),
            client_factory=lambda: client,
        )
        m.start()
        from aiodouyu import EVENT_CONNECTED

        client.push({"type": EVENT_CONNECTED, "roomid": "7"})
        await asyncio.sleep(0.05)  # 校准协程执行了一次失败的对账
        assert events == []
        assert m.last_live_status is None  # 状态未被污染
        assert m.is_healthy  # 协程仍在运行
        assert m._resync_pending and m._resync_failures == 1  # 已登记重试

        # 后续 rss 仍正常驱动
        client.push({"type": "rss", "ss": "1", "ivl": "0"})
        await _drain()
        assert events == ["live"]

        await m.stop()

    asyncio.run(run())


def test_default_client_factory_config():
    """默认工厂是生产环境唯一的客户端构造路径,配置错误 = 对账整体失效"""
    from aiodouyu import DanmakuClient

    m = DouyuMonitor(42)
    client = m._client_factory()
    assert isinstance(client, DanmakuClient)
    assert client.room_id == 42
    assert client.types == {"rss"}
    # 关掉连接事件 = 永远收不到 EVENT_CONNECTED = 断连窗口对账静默失效
    assert client.emit_connection_events is True


def test_reconcile_loop_flushes_pending(monkeypatch):
    """周期校准链路(create_task 接线)必须真实补发冷却期待定转换"""

    async def run():
        monkeypatch.setattr(monitor_mod, "RECONCILE_INTERVAL", 0.01)
        events = []
        client = FakeDanmakuClient()
        m = DouyuMonitor(
            10,
            live_callback=lambda r, msg: events.append("live"),
            offline_callback=lambda r, d: events.append("off"),
            client_factory=lambda: client,
        )
        m.start()
        client.push({"type": "rss", "ss": "1", "ivl": "0"})  # 开播播报
        await _drain()
        client.push({"type": "rss", "ss": "0", "ivl": "0"})  # 冷却期内下播
        await _drain()
        assert events == ["live"]
        assert m._pending_status is False

        m._last_notify_time = 0.0  # 令冷却立即结束
        await asyncio.sleep(0.05)  # 由 _reconcile_loop 补发,而非手工直调
        assert events == ["live", "off"]

        await m.stop()
        # 无残留任务(reconcile/消费协程都已回收)
        leftovers = [
            t for t in asyncio.all_tasks() if t is not asyncio.current_task()
        ]
        assert leftovers == []

    asyncio.run(run())


def test_resync_failure_retried_until_success(monkeypatch):
    """对账失败必须按退避重试直到成功(断连窗口补偿不能因单次失败失效)"""

    async def run():
        monkeypatch.setattr(monitor_mod, "RECONCILE_INTERVAL", 0.01)
        calls = []

        async def flaky_fetch_room(room_id, *, source, timeout):
            calls.append(source)
            if len(calls) < 2:
                raise RuntimeError("betard 5xx")
            return SimpleNamespace(is_live=True, started_at=None)

        monkeypatch.setattr(monitor_mod, "fetch_room", flaky_fetch_room)

        events = []
        client = FakeDanmakuClient()
        m = DouyuMonitor(
            13,
            live_callback=lambda r, msg: events.append("live"),
            client_factory=lambda: client,
        )
        m.start()
        from aiodouyu import EVENT_CONNECTED

        client.push({"type": EVENT_CONNECTED, "roomid": "13"})
        await asyncio.sleep(0.05)  # 校准协程执行首次对账(失败)
        assert calls == ["betard"] and events == []
        assert m._resync_pending  # 已登记待重试

        m._resync_at = 0.0  # 令退避立即到期
        await asyncio.sleep(0.05)
        assert calls == ["betard", "betard"]
        assert events == ["live"]
        assert not m._resync_pending

        await m.stop()

    asyncio.run(run())


def test_resync_stale_snapshot_discarded(monkeypatch):
    """fetch 在途期间状态机前进:过期快照必须丢弃重拉,不得记出虚假 pending"""

    async def run():
        monkeypatch.setattr(monitor_mod, "RECONCILE_INTERVAL", 0.01)
        events = []
        gate = asyncio.Event()
        calls = []

        async def gated_fetch_room(room_id, *, source, timeout):
            calls.append(1)
            if len(calls) == 1:
                await gate.wait()  # 首次对账挂起,模拟慢速 betard
                # 带回的是"未开播"的过期快照
                return SimpleNamespace(is_live=False, started_at=None)
            return SimpleNamespace(is_live=True, started_at=None)

        monkeypatch.setattr(monitor_mod, "fetch_room", gated_fetch_room)

        client = FakeDanmakuClient()
        m = DouyuMonitor(
            16,
            live_callback=lambda r, msg: events.append("live"),
            offline_callback=lambda r, d: events.append("off"),
            client_factory=lambda: client,
        )
        m.start()
        from aiodouyu import EVENT_CONNECTED

        client.push({"type": EVENT_CONNECTED, "roomid": "16"})
        await asyncio.sleep(0.05)  # 首次对账进入 fetch 并挂起
        assert calls == [1]

        # fetch 在途期间收到 rss:真实开播,状态机前进并播报
        client.push({"type": "rss", "ss": "1", "ivl": "0"})
        await _drain()
        assert events == ["live"]

        gate.set()  # 过期快照(未开播)此刻返回
        await asyncio.sleep(0.05)  # 校准协程完成首次对账并重拉

        # 过期快照必须被丢弃:不产生反向 pending,也不吞后续通知
        assert m._pending_status is None
        assert m.last_live_status is True
        assert len(calls) == 2  # 已用新鲜数据重拉
        assert events == ["live"]  # 无虚假通知

        await m.stop()

    asyncio.run(run())


def test_resync_overflow_clamped(monkeypatch):
    """失败计数极大时退避计算不得溢出,校准协程不得死亡"""

    async def run():
        monkeypatch.setattr(monitor_mod, "RECONCILE_INTERVAL", 0.01)

        async def failing_fetch_room(room_id, *, source, timeout):
            raise RuntimeError("持续失败")

        monkeypatch.setattr(monitor_mod, "fetch_room", failing_fetch_room)

        client = FakeDanmakuClient()
        m = DouyuMonitor(17, client_factory=lambda: client)
        m.start()
        # 直接注入巨大的失败计数(等价于连续失败约 17 小时后的状态)
        m._resync_failures = 5000
        m._schedule_resync()
        await asyncio.sleep(0.05)  # 旧实现在此抛 OverflowError 杀死协程

        assert m._resync_failures == 5001  # 又失败了一次,但没有崩
        assert m._resync_pending  # 重试仍在调度
        assert m.is_healthy

        # 校准协程仍活着:pending 补发路径仍工作
        client.push({"type": "rss", "ss": "1", "ivl": "0"})
        await _drain()
        assert m.last_live_status is True

        await m.stop()

    asyncio.run(run())


def test_room_not_found_uses_long_recheck_interval(monkeypatch):
    """封禁/删除房间的对账结论按长间隔复查,不做秒级退避刷接口"""

    async def run():
        monkeypatch.setattr(monitor_mod, "RECONCILE_INTERVAL", 0.01)
        from aiodouyu import RoomNotFound

        async def gone_fetch_room(room_id, *, source, timeout):
            raise RoomNotFound("房间不存在")

        monkeypatch.setattr(monitor_mod, "fetch_room", gone_fetch_room)

        client = FakeDanmakuClient()
        m = DouyuMonitor(18, client_factory=lambda: client)
        m.start()
        from aiodouyu import EVENT_CONNECTED

        client.push({"type": EVENT_CONNECTED, "roomid": "18"})
        await asyncio.sleep(0.05)

        assert m._resync_room_gone is True
        assert m._resync_pending  # 仍会复查(封禁常为临时)
        # 复查间隔是长间隔(30 分钟),不是秒级退避
        import time as _time

        assert m._resync_at - _time.monotonic() > 1000
        assert m._resync_failures == 0  # 不计入瞬时故障退避

        await m.stop()

    asyncio.run(run())


def test_consumer_exception_closes_client_and_reports_unhealthy():
    """消费循环意外异常:客户端必须被关闭,is_healthy 转 False 交给 watchdog"""

    async def run():
        client = FakeDanmakuClient()
        m = DouyuMonitor(12, client_factory=lambda: client)
        m.start()
        client.push_error(RuntimeError("库内部异常"))
        await _drain()
        assert m._task.done()
        assert client.closed  # finally 兜底关闭,连接不泄漏
        assert not m.is_healthy

    asyncio.run(run())


def test_stop_timeout_cancels_stuck_task(monkeypatch):
    """close 后迭代不退出的极端情况:stop() 超时后必须取消消费协程"""

    class StubbornClient(FakeDanmakuClient):
        async def close(self):
            self.closed = True  # 不投终止哨兵,迭代永不退出

    async def run():
        monkeypatch.setattr(monitor_mod, "STOP_TIMEOUT", 0.05)
        client = StubbornClient()
        m = DouyuMonitor(14, client_factory=lambda: client)
        m.start()
        await _drain()
        await m.stop()
        assert m._task.done()
        assert not m.is_healthy

    asyncio.run(run())


def test_connected_visibility():
    """EVENT_CONNECTED/DISCONNECTED 驱动 connected 标志(供 ls 展示重连中)"""

    async def run():
        client = FakeDanmakuClient()
        m = DouyuMonitor(15, client_factory=lambda: client)
        m.start()
        assert m.connected is False

        from aiodouyu import EVENT_CONNECTED, EVENT_DISCONNECTED

        async def noop_fetch(room_id, *, source, timeout):
            return SimpleNamespace(is_live=False, started_at=None)

        monitor_mod_fetch = monitor_mod.fetch_room
        monitor_mod.fetch_room = noop_fetch
        try:
            client.push({"type": EVENT_CONNECTED, "roomid": "15"})
            await _drain()
            assert m.connected is True
            client.push({"type": EVENT_DISCONNECTED, "roomid": "15"})
            await _drain()
            assert m.connected is False
        finally:
            monitor_mod.fetch_room = monitor_mod_fetch

        await m.stop()

    asyncio.run(run())


def test_resync_compensates_missed_transition(monkeypatch):
    """断连窗口内开播：重连对账必须补发开播通知（高危回归）"""

    async def run():
        monkeypatch.setattr(monitor_mod, "RECONCILE_INTERVAL", 0.01)
        events = []

        async def fake_fetch_room(room_id, *, source, timeout):
            return SimpleNamespace(is_live=True, started_at=None)

        monkeypatch.setattr(monitor_mod, "fetch_room", fake_fetch_room)

        client = FakeDanmakuClient()
        m = DouyuMonitor(
            8,
            live_callback=lambda r, msg: events.append("live"),
            offline_callback=lambda r, d: events.append("off"),
            # 模拟重启继承:未播状态,且远离冷却期
            inherit_state={
                "last_live_status": False,
                "live_start_time": None,
                "has_announced_live": False,
                "last_notify_time": 0.0,
                "pending_status": None,
                "pending_msg": None,
            },
            client_factory=lambda: client,
        )
        m.start()
        from aiodouyu import EVENT_CONNECTED

        # 断连窗口内主播开播了:重连后没有 rss 重放,只有对账能发现
        client.push({"type": EVENT_CONNECTED, "roomid": "8"})
        await asyncio.sleep(0.05)  # 校准协程执行对账
        assert events == ["live"]
        assert m.last_live_status is True

        # 之后正常收到下播 rss,不能被"回稳"分支吞掉
        m._last_notify_time = 0.0  # 跳过冷却期,聚焦转换正确性
        client.push({"type": "rss", "ss": "0", "ivl": "0"})
        await _drain()
        assert events == ["live", "off"]

        await m.stop()

    asyncio.run(run())

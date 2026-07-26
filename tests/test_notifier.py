"""Notifier 消息构建与发送、文本清洗测试"""

import asyncio

from astrbot_plugin_douyu_live.core.notifier import Notifier
from astrbot_plugin_douyu_live.utils.text import sanitize_display_text


def test_sanitize_strips_dangerous_content():
    evil = "坏人\n━━━━━━\n假通知 http://evil.example\n" + "x" * 100
    s = sanitize_display_text(evil)
    assert "\n" not in s
    assert "━" not in s
    assert len(s) <= 32


def test_sanitize_empty_falls_back():
    assert sanitize_display_text("\x00\x01") == "未知"


def test_sanitize_c1_and_bidi():
    assert "\x85" not in sanitize_display_text("a\x85b")  # U+0085 NEL
    assert "‮" not in sanitize_display_text("a‮b")  # bidi override


def test_offline_duration_formats():
    n = Notifier(context=None)
    assert "未知" in n.build_offline_notification(1, "a", 0)
    assert "45分钟" in n.build_offline_notification(1, "a", 2700)
    assert "1小时1分钟" in n.build_offline_notification(1, "a", 3661)


def test_notification_sanitizes_room_name():
    n = Notifier(context=None)
    assert "━fake" not in n.build_notification(1, "x\n━fake")


def test_no_gift_builder():
    assert not hasattr(Notifier(context=None), "build_gift_notification")


def test_send_failure_classification():
    """异常 -> 可重试；send_message 返回 False（无平台）-> 不重试"""

    class Ctx:
        async def send_message(self, umo, result):
            if umo == "boom":
                raise RuntimeError("net down")
            return umo != "noplatform"

    n = Notifier(context=Ctx())
    failed = asyncio.run(
        n.send_to_subscribers({"ok": True, "boom": False, "noplatform": False}, "hi")
    )
    assert failed == {"boom"}

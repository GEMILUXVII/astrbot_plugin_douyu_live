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
    # bidi isolate（Trojan-Source 载体）、方向标记与零宽字符必须一并移除
    for ch in [
        "⁦",  # LRI
        "⁧",  # RLI
        "⁨",  # FSI
        "⁩",  # PDI
        "‎",  # LRM
        "‏",  # RLM
        "؜",  # ALM
        "​",  # ZWSP
        "‍",  # ZWJ
        "⁠",  # WJ
        "﻿",  # BOM/ZWNBSP
    ]:
        assert ch not in sanitize_display_text(f"a{ch}b"), f"U+{ord(ch):04X} 泄漏"


def test_notifications_are_emoji_free():
    """通知面保持纯文本:emoji 在正式推送里不专业(回归锁)"""
    import unicodedata

    n = Notifier(context=None)
    messages = [
        n.build_notification(9999, "主播A", title="标题", category="DOTA2"),
        n.build_notification(9999, "主播A"),
        n.build_offline_notification(9999, "主播A", 7325),
    ]
    for msg in messages:
        for ch in msg:
            # Symbol-other 覆盖绝大多数 emoji/图形符号;中文与常规标点不在其中
            assert unicodedata.category(ch) != "So", (
                f"通知含图形符号 {ch!r} (U+{ord(ch):04X}): {msg!r}"
            )
        assert "━" not in msg  # 装饰性分隔线也一并去掉


def test_offline_duration_formats():
    n = Notifier(context=None)
    assert "未知" in n.build_offline_notification(1, "a", 0)
    assert "1秒" in n.build_offline_notification(1, "a", 0.1)
    assert "42秒" in n.build_offline_notification(1, "a", 41.6)
    assert "60秒" in n.build_offline_notification(1, "a", 59.6)
    assert "45分钟" in n.build_offline_notification(1, "a", 2700)
    assert "1小时1分钟" in n.build_offline_notification(1, "a", 3661)


def test_notification_sanitizes_room_name():
    n = Notifier(context=None)
    assert "━fake" not in n.build_notification(1, "x\n━fake")


def test_no_gift_builder():
    assert not hasattr(Notifier(context=None), "build_gift_notification")


def test_send_failure_classification():
    """异常与 send_message=False 都必须交给上层重试。"""

    class Ctx:
        async def send_message(self, umo, result):
            if umo == "boom":
                raise RuntimeError("net down")
            return umo != "noplatform"

    n = Notifier(context=Ctx())
    failed = asyncio.run(
        n.send_to_subscribers({"ok": True, "boom": False, "noplatform": False}, "hi")
    )
    assert failed == {"boom", "noplatform"}


def test_send_appends_cover_image():
    captured = []

    class Ctx:
        async def send_message(self, umo, result):
            captured.append((umo, result))
            return True

    n = Notifier(context=Ctx())
    failed = asyncio.run(
        n.send_to_subscribers(
            {"umo": False},
            "hi",
            cover_url="https://example.com/cover.jpg",
        )
    )

    assert failed == set()
    assert captured[0][0] == "umo"
    assert captured[0][1].chain[-1].file == "https://example.com/cover.jpg"


def test_send_timeout_counts_as_failed(monkeypatch):
    """挂起的发送按超时计入 failed 走上层退避重试(纵深防御)"""
    import astrbot_plugin_douyu_live.core.notifier as notifier_mod

    monkeypatch.setattr(notifier_mod, "SEND_TIMEOUT", 0.05)

    class Ctx:
        async def send_message(self, umo, result):
            if umo == "stuck":
                await asyncio.sleep(30)  # 无超时的第三方适配器挂死
            return True

    n = Notifier(context=Ctx())
    failed = asyncio.run(n.send_to_subscribers({"ok": True, "stuck": False}, "hi"))
    assert failed == {"stuck"}

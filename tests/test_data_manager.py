"""DataManager 持久化、迁移与容错测试"""

import json

from astrbot_plugin_douyu_live.models.room import RoomInfo
from astrbot_plugin_douyu_live.storage.data_manager import DataManager


def test_atomic_save_creates_file_and_backup(data_dir):
    dm = DataManager()
    dm.add_room(100, RoomInfo(name="A"))
    assert dm.data_file.exists()
    dm.add_room(101, RoomInfo(name="B"))
    assert dm.backup_file.exists()
    assert not (data_dir / (dm.data_file.name + ".tmp")).exists()
    on_disk = json.loads(dm.data_file.read_text(encoding="utf-8"))
    # 房间级通知设置字段已随 2.0.0 移除，不应再持久化
    assert "at_all" not in on_disk["room_info"]["100"]


def test_corrupt_primary_recovers_from_backup(data_dir):
    dm = DataManager()
    dm.add_room(100, RoomInfo(name="A"))
    dm.subscribe(100, "umoA", "op")
    dm.add_room(101, RoomInfo(name="B"))  # 轮换出一份完好备份
    dm.data_file.write_text('{"broken', encoding="utf-8")

    dm2 = DataManager()
    assert 100 in dm2.room_info
    assert "umoA" in dm2.subscriptions[100]
    assert len(list(data_dir.glob("*.corrupt.*"))) == 1


def test_corrupt_without_backup_quarantines_not_wipes(data_dir):
    (data_dir / "douyu_live_data.json").write_text("NOT JSON", encoding="utf-8")
    dm = DataManager()
    assert dm.room_info == {} and dm.subscriptions == {}
    corrupt = list(data_dir.glob("*.corrupt.*"))
    assert len(corrupt) == 1
    assert corrupt[0].read_text(encoding="utf-8") == "NOT JSON"
    assert dm.save() is True  # 隔离成功后允许继续写入


def test_valid_json_wrong_shape_treated_as_corrupt(data_dir):
    """合法 JSON 但顶层非 dict（null/[]）必须走隔离+备份回退（回归：覆盖完好 .bak）"""
    dm = DataManager()
    dm.add_room(100, RoomInfo(name="A"))
    dm.subscribe(100, "umoA")
    dm.add_room(101, RoomInfo(name="B"))
    dm.data_file.write_text("null", encoding="utf-8")

    dm2 = DataManager()
    assert 100 in dm2.room_info
    assert "umoA" in dm2.subscriptions[100]
    assert len(list(data_dir.glob("*.corrupt.*"))) == 1


def test_legacy_list_migration_carries_at_all_only(data_dir):
    legacy = {
        "subscriptions": {"123": ["umoA", "umoB"]},
        "room_info": {
            "123": {
                "name": "老主播",
                "at_all": True,
                "gift_notify": True,
                "high_value_only": False,
            }
        },
    }
    (data_dir / "douyu_live_data.json").write_text(
        json.dumps(legacy), encoding="utf-8"
    )
    dm = DataManager()
    cfg = dm.get_subscription_config(123, "umoA")
    assert cfg is not None and cfg.at_all is True
    on_disk = json.loads(dm.data_file.read_text(encoding="utf-8"))
    sub = on_disk["subscriptions"]["123"]["umoA"]
    assert "gift_notify" not in sub and "high_value_only" not in sub
    assert "at_all" not in on_disk["room_info"]["123"]


def test_v15_dict_format_gift_keys_dropped(data_dir):
    v15 = {
        "subscriptions": {
            "123": {
                "umoA": {
                    "at_all": True,
                    "gift_notify": True,
                    "high_value_only": False,
                    "subscribed_by": "x",
                }
            }
        },
        "room_info": {"123": {"name": "n"}},
    }
    (data_dir / "douyu_live_data.json").write_text(json.dumps(v15), encoding="utf-8")
    dm = DataManager()
    cfg = dm.get_subscription_config(123, "umoA")
    assert cfg is not None and cfg.at_all is True and cfg.subscribed_by == "x"


def test_bad_entries_skipped_not_fatal(data_dir):
    payload = {
        "subscriptions": {"123": {"umoA": {}}, "bad": {"x": {}}, "999": None},
        "room_info": {"123": {"name": "ok"}, "_comment": {"name": "junk"}, "456": "s"},
    }
    (data_dir / "douyu_live_data.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    dm = DataManager()
    assert 123 in dm.room_info and dm.room_info[123].name == "ok"
    assert 456 not in dm.room_info
    assert "umoA" in dm.subscriptions[123]


def test_unsub_restores_config_on_resubscribe(data_dir):
    dm = DataManager()
    dm.add_room(200, RoomInfo(name="C"))
    ok, restored = dm.subscribe(200, "umoX", "alice")
    assert ok and not restored
    dm.update_subscription_config(200, "umoX", at_all=True)
    assert dm.unsubscribe(200, "umoX") is True
    assert dm.get_subscription_config(200, "umoX") is None

    ok2, restored2 = dm.subscribe(200, "umoX", "bob")
    cfg = dm.get_subscription_config(200, "umoX")
    assert ok2 and restored2
    assert cfg.at_all is True and cfg.subscribed_by == "bob"


def test_unsub_history_skips_default_and_caps(data_dir):
    dm = DataManager()
    dm.add_room(300, RoomInfo(name="H"))

    # 默认配置无恢复价值，不归档
    dm.subscribe(300, "umoDefault")
    dm.unsubscribe(300, "umoDefault")
    assert "umoDefault" not in dm.unsub_history.get(300, {})

    # 非默认配置归档
    dm.subscribe(300, "umoCustom")
    dm.update_subscription_config(300, "umoCustom", at_all=True)
    dm.unsubscribe(300, "umoCustom")
    assert "umoCustom" in dm.unsub_history[300]

    # 超上限逐出最旧
    for i in range(dm.UNSUB_HISTORY_MAX_PER_ROOM + 10):
        umo = f"umo{i}"
        dm.subscribe(300, umo)
        dm.update_subscription_config(300, umo, at_all=True)
        dm.unsubscribe(300, umo)
    assert len(dm.unsub_history[300]) == dm.UNSUB_HISTORY_MAX_PER_ROOM
    assert "umoCustom" not in dm.unsub_history[300]


def test_remove_room_cleans_orphans_and_history(data_dir):
    dm = DataManager()
    dm.subscribe(300, "umoOrphan")  # 无房间记录的孤立订阅
    assert dm.remove_room(300) is True
    assert 300 not in dm.subscriptions
    assert dm.remove_room(999) is False

    # 仅存在于退订历史的房间也应可删除且落盘（回归：内存改了不保存）
    dm.add_room(400, RoomInfo(name="X"))
    dm.subscribe(400, "umoY")
    dm.update_subscription_config(400, "umoY", at_all=True)
    dm.unsubscribe(400, "umoY")
    dm.subscriptions.pop(400, None)
    dm.room_info.pop(400, None)
    assert dm.remove_room(400) is True
    assert 400 not in dm.unsub_history


def test_room_name_sanitized_on_load(data_dir):
    dm = DataManager()
    dm.add_room(500, RoomInfo(name="bad\n━━ fake"))
    dm2 = DataManager()
    assert "\n" not in dm2.room_info[500].name
    assert "━" not in dm2.room_info[500].name

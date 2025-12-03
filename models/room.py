"""房间信息数据模型"""

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class RoomInfo:
    """直播间信息

    Attributes:
        name: 主播/房间名称
        added_by: 添加者 ID
        added_time: 添加时间
        at_all: 是否开启 @全体成员（开播通知）
        gift_notify: 是否开启礼物播报
        high_value_only: 是否只播报高价值礼物（飞机及以上）
        summary_notify: 是否开启直播总结（下播时推送本场统计）
    """
    name: str
    added_by: str = ""
    added_time: str = ""
    at_all: bool = False
    gift_notify: bool = False
    high_value_only: bool = True  # 默认只播报高价值礼物
    summary_notify: bool = True  # 默认开启直播总结

    def to_dict(self) -> dict[str, Any]:
        """转换为字典"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RoomInfo":
        """从字典创建实例"""
        return cls(
            name=data.get("name", ""),
            added_by=data.get("added_by", ""),
            added_time=data.get("added_time", ""),
            at_all=data.get("at_all", False),
            gift_notify=data.get("gift_notify", False),
            high_value_only=data.get("high_value_only", True),
            summary_notify=data.get("summary_notify", True),
        )


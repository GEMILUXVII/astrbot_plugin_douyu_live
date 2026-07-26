"""房间信息数据模型"""

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class RoomInfo:
    """直播间信息

    通知相关设置（如 @全体成员）为订阅级别配置，
    存放于 SubscriptionConfig，房间级别不再保存这些字段。

    Attributes:
        name: 主播/房间名称
        added_by: 添加者 ID
        added_time: 添加时间
    """
    name: str
    added_by: str = ""
    added_time: str = ""

    def to_dict(self) -> dict[str, Any]:
        """转换为字典"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RoomInfo":
        """从字典创建实例

        旧版本数据中的房间级通知设置字段（at_all 等）
        仅在订阅数据迁移时读取（见 DataManager.load），此处忽略。
        名称做入口清洗，覆盖清洗机制引入前持久化的存量数据。
        """
        from ..utils.text import sanitize_display_text

        name = data.get("name", "")
        return cls(
            name=sanitize_display_text(name) if name else "",
            added_by=data.get("added_by", ""),
            added_time=data.get("added_time", ""),
        )

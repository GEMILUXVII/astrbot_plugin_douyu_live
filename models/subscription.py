"""订阅配置数据模型"""

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class SubscriptionConfig:
    """订阅配置

    每个群对每个房间的独立配置。

    Attributes:
        at_all: 是否开启 @全体成员（开播通知）
        subscribed_by: 订阅操作者 ID（审计用）
    """

    at_all: bool = False
    subscribed_by: str = ""

    def to_dict(self) -> dict[str, Any]:
        """转换为字典"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SubscriptionConfig":
        """从字典创建实例

        旧版本数据中的礼物播报字段（gift_notify / high_value_only）
        已随礼物播报功能移除，此处直接忽略。
        """
        return cls(
            at_all=bool(data.get("at_all", False)),
            subscribed_by=str(data.get("subscribed_by", "")),
        )

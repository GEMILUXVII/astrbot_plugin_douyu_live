"""数据持久化管理模块"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from astrbot.api import logger
from astrbot.api.star import StarTools

if TYPE_CHECKING:
    from ..models.room import RoomInfo
    from ..models.subscription import SubscriptionConfig


class DataManager:
    """数据管理器

    负责插件数据的加载、保存和管理。
    数据存储在 JSON 文件中。
    """

    def __init__(self, plugin_name: str = "astrbot_plugin_douyu_live"):
        """初始化数据管理器

        Args:
            plugin_name: 插件名称，用于确定数据目录
        """
        self.data_dir: Path = StarTools.get_data_dir(plugin_name)
        self.data_file: Path = self.data_dir / "douyu_live_data.json"

        # 数据结构
        # room_id -> {umo -> SubscriptionConfig}
        self.subscriptions: dict[int, dict[str, SubscriptionConfig]] = {}
        self.room_info: dict[int, RoomInfo] = {}  # room_id -> RoomInfo

        # 加载数据
        self.load()

    def load(self) -> None:
        """从文件加载数据，兼容旧格式"""
        from ..models.room import RoomInfo as RoomInfoClass
        from ..models.subscription import SubscriptionConfig as SubConfigClass

        if not os.path.exists(self.data_file):
            self.subscriptions = {}
            self.room_info = {}
            return

        try:
            with open(self.data_file, encoding="utf-8") as f:
                data = json.load(f)

                # 使用 from_dict 以兼容旧版本数据
                self.room_info = {
                    int(k): RoomInfoClass.from_dict(v)
                    for k, v in data.get("room_info", {}).items()
                }

                # 加载订阅数据，兼容旧格式
                raw_subs = data.get("subscriptions", {})
                self.subscriptions = {}

                for room_id_str, sub_data in raw_subs.items():
                    room_id = int(room_id_str)
                    self.subscriptions[room_id] = {}

                    if isinstance(sub_data, list):
                        # 旧格式: list of umo strings，需要迁移
                        # 从房间信息中获取默认配置
                        room_info = self.room_info.get(room_id)
                        for umo in sub_data:
                            # 迁移时继承房间级别的设置
                            if room_info:
                                self.subscriptions[room_id][umo] = SubConfigClass(
                                    at_all=room_info.at_all,
                                    gift_notify=room_info.gift_notify,
                                    high_value_only=room_info.high_value_only,
                                )
                            else:
                                self.subscriptions[room_id][umo] = SubConfigClass()
                        logger.info(f"已迁移房间 {room_id} 的 {len(sub_data)} 个订阅到新格式")
                    elif isinstance(sub_data, dict):
                        # 新格式: {umo -> config dict}
                        for umo, config in sub_data.items():
                            if isinstance(config, dict):
                                self.subscriptions[room_id][umo] = SubConfigClass.from_dict(config)
                            else:
                                # 兼容意外情况
                                self.subscriptions[room_id][umo] = SubConfigClass()

                # 如果有旧格式数据迁移，立即保存
                if any(isinstance(v, list) for v in raw_subs.values()):
                    self.save()
                    logger.info("订阅数据格式已迁移并保存")

        except Exception as e:
            logger.error(f"加载斗鱼直播数据失败: {e}")
            self.subscriptions = {}
            self.room_info = {}

    def save(self) -> None:
        """保存数据到文件"""
        try:
            data = {
                "subscriptions": {
                    str(room_id): {
                        umo: config.to_dict()
                        for umo, config in sub_dict.items()
                    }
                    for room_id, sub_dict in self.subscriptions.items()
                },
                "room_info": {
                    str(k): v.to_dict() for k, v in self.room_info.items()
                },
            }
            with open(self.data_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存斗鱼直播数据失败: {e}")

    # ==================== 房间管理 ====================

    def add_room(self, room_id: int, info: RoomInfo) -> None:
        """添加房间

        Args:
            room_id: 房间号
            info: 房间信息
        """
        self.room_info[room_id] = info
        if room_id not in self.subscriptions:
            self.subscriptions[room_id] = {}
        self.save()

    def remove_room(self, room_id: int) -> bool:
        """删除房间

        Args:
            room_id: 房间号

        Returns:
            是否成功删除
        """
        if room_id not in self.room_info:
            return False
        del self.room_info[room_id]
        if room_id in self.subscriptions:
            del self.subscriptions[room_id]
        self.save()
        return True

    def get_room(self, room_id: int) -> RoomInfo | None:
        """获取房间信息

        Args:
            room_id: 房间号

        Returns:
            房间信息，不存在返回 None
        """
        return self.room_info.get(room_id)

    def has_room(self, room_id: int) -> bool:
        """检查房间是否存在"""
        return room_id in self.room_info

    def get_all_rooms(self) -> dict[int, RoomInfo]:
        """获取所有房间"""
        return self.room_info.copy()

    def update_room(self, room_id: int, **kwargs: Any) -> bool:
        """更新房间信息

        Args:
            room_id: 房间号
            **kwargs: 要更新的字段

        Returns:
            是否成功更新
        """
        if room_id not in self.room_info:
            return False
        for key, value in kwargs.items():
            if hasattr(self.room_info[room_id], key):
                setattr(self.room_info[room_id], key, value)
        self.save()
        return True

    # ==================== 订阅管理 ====================

    def subscribe(self, room_id: int, umo: str) -> bool:
        """添加订阅

        Args:
            room_id: 房间号
            umo: unified_msg_origin

        Returns:
            是否成功（False 表示已订阅）
        """
        from ..models.subscription import SubscriptionConfig as SubConfigClass

        if room_id not in self.subscriptions:
            self.subscriptions[room_id] = {}
        if umo in self.subscriptions[room_id]:
            return False

        # 新订阅使用默认配置
        self.subscriptions[room_id][umo] = SubConfigClass()
        self.save()
        return True

    def unsubscribe(self, room_id: int, umo: str) -> bool:
        """取消订阅

        Args:
            room_id: 房间号
            umo: unified_msg_origin

        Returns:
            是否成功（False 表示未订阅）
        """
        if room_id not in self.subscriptions:
            return False
        if umo not in self.subscriptions[room_id]:
            return False
        del self.subscriptions[room_id][umo]
        self.save()
        return True

    def get_subscribers(self, room_id: int) -> set[str]:
        """获取房间的订阅者列表"""
        if room_id not in self.subscriptions:
            return set()
        return set(self.subscriptions[room_id].keys())

    def get_subscription_config(self, room_id: int, umo: str) -> SubscriptionConfig | None:
        """获取指定订阅的配置

        Args:
            room_id: 房间号
            umo: unified_msg_origin

        Returns:
            订阅配置，不存在返回 None
        """
        if room_id not in self.subscriptions:
            return None
        return self.subscriptions[room_id].get(umo)

    def get_all_subscription_configs(self, room_id: int) -> dict[str, SubscriptionConfig]:
        """获取房间所有订阅的配置

        Args:
            room_id: 房间号

        Returns:
            {umo -> SubscriptionConfig} 字典
        """
        return self.subscriptions.get(room_id, {}).copy()

    def update_subscription_config(self, room_id: int, umo: str, **kwargs: Any) -> bool:
        """更新指定订阅的配置

        Args:
            room_id: 房间号
            umo: unified_msg_origin
            **kwargs: 要更新的字段 (at_all, gift_notify, high_value_only)

        Returns:
            是否成功更新
        """
        if room_id not in self.subscriptions:
            return False
        if umo not in self.subscriptions[room_id]:
            return False

        config = self.subscriptions[room_id][umo]
        for key, value in kwargs.items():
            if hasattr(config, key):
                setattr(config, key, value)
        self.save()
        return True

    def get_user_subscriptions(self, umo: str) -> list[int]:
        """获取用户订阅的房间列表"""
        return [
            room_id
            for room_id, sub_dict in self.subscriptions.items()
            if umo in sub_dict
        ]

    def get_total_subscriptions(self) -> int:
        """获取总订阅数"""
        return sum(len(s) for s in self.subscriptions.values())

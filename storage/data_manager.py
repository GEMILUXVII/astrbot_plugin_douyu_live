"""数据持久化管理模块"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from astrbot.api import logger
from astrbot.api.star import StarTools

from ..utils.constants import PLUGIN_NAME

if TYPE_CHECKING:
    from ..models.room import RoomInfo
    from ..models.subscription import SubscriptionConfig


class DataManager:
    """数据管理器

    负责插件数据的加载、保存和管理。
    数据存储在 JSON 文件中，写入为原子操作并保留上一代备份。

    线程安全：
        数据变更经 asyncio.to_thread 移出事件循环执行（磁盘 I/O），
        工作线程与事件循环线程会并发读写本类，
        所有对内部字典的访问都由 self._lock 保护；磁盘 I/O 在独立的
        _io_lock 下进行，不占用 _lock。
    """

    # 退订历史每房间最多保留的条数（防无界增长）
    UNSUB_HISTORY_MAX_PER_ROOM = 100

    def __init__(self, plugin_name: str = PLUGIN_NAME):
        """初始化数据管理器

        Args:
            plugin_name: 插件名称，用于确定数据目录
        """
        self.data_dir: Path = StarTools.get_data_dir(plugin_name)
        self.data_file: Path = self.data_dir / "douyu_live_data.json"
        self.backup_file: Path = self.data_dir / "douyu_live_data.json.bak"

        # 数据结构
        # room_id -> {umo -> SubscriptionConfig}
        self.subscriptions: dict[int, dict[str, SubscriptionConfig]] = {}
        self.room_info: dict[int, RoomInfo] = {}  # room_id -> RoomInfo
        # 退订历史：退订时保留配置，重新订阅时恢复（防误删/恶意退订丢配置）
        self.unsub_history: dict[int, dict[str, SubscriptionConfig]] = {}

        # 可重入锁：保护内存数据结构（持锁时间应保持在微秒级）
        self._lock = threading.RLock()
        # IO 锁：串行化磁盘写入（fsync 等慢操作在此锁下、_lock 之外进行）
        self._io_lock = threading.Lock()
        # 快照序列号：并发写盘时用于丢弃过期快照
        self._save_seq = 0
        self._written_seq = 0
        # 写失败的最高序列号：防止随后落盘的更旧快照把 last_save_ok
        # 洗白——旧快照写成功不代表失败快照里的变更已在磁盘上
        self._failed_seq = 0
        # 上次保存是否成功（命令层据此提示用户）
        self.last_save_ok = True
        # 加载失败且无法隔离损坏文件时置位，拒绝一切写入以保护原数据
        self._write_blocked = False

        # 加载数据
        self.load()

    # ==================== 加载 ====================

    @staticmethod
    def _parse_room_id(key: Any) -> int | None:
        """解析 JSON 键为房间号；非法键返回 None（不抛异常）

        必须限定 ASCII：str.isdigit() 对 "²"/"①" 等 Unicode 数字字符
        返回 True 但 int() 会抛 ValueError，手工编辑过的数据文件含此类
        键时会让整个 load() 崩溃、插件无法启动。
        """
        if isinstance(key, str) and key.isascii() and key.isdigit():
            return int(key)
        logger.warning(f"跳过非法房间号键: {key!r}")
        return None

    def _quarantine(self, path: Path, block_on_failure: bool = True) -> None:
        """隔离疑似损坏的数据文件，避免后续保存覆盖它

        Args:
            path: 待隔离的文件
            block_on_failure: 隔离失败时是否禁写。主文件隔离失败必须
                禁写保护原文件；备份文件隔离失败只记日志（禁写会把
                健康的主文件也一并冻结）
        """
        try:
            target = path.with_name(f"{path.name}.corrupt.{int(time.time())}")
            os.replace(path, target)
            logger.error(f"已将疑似损坏的数据文件隔离至 {target.name}，可手动恢复")
        except OSError as e:
            if block_on_failure:
                # 连隔离都失败（如文件被占用）：禁止写入，保护原文件
                self._write_blocked = True
                logger.error(
                    f"无法隔离损坏的数据文件，已禁用自动保存以保护原文件: {e}"
                )
            else:
                logger.error(f"无法隔离损坏的备份文件 {path.name}: {e}")

    def load(self) -> None:
        """从文件加载数据，兼容旧格式

        容错策略：
        - 主文件损坏时尝试从备份加载，并隔离损坏的主文件
        - 单个非法条目只跳过自身，不影响其余数据
        - 任何失败都不会触发对现有文件的覆盖写入
        """
        from ..models.room import RoomInfo as RoomInfoClass
        from ..models.subscription import SubscriptionConfig as SubConfigClass

        with self._lock:
            self.subscriptions = {}
            self.room_info = {}
            self.unsub_history = {}

            raw: Any = None
            for path in (self.data_file, self.backup_file):
                if not os.path.exists(path):
                    continue
                try:
                    with open(path, encoding="utf-8") as f:
                        raw = json.load(f)
                    # 形状校验必须在此处：合法 JSON 但顶层非 dict（[]/null/字符串）
                    # 同样视为损坏，走隔离 + 备份回退，否则下次保存会把
                    # 畸形文件轮转覆盖到完好的 .bak 上
                    if not isinstance(raw, dict):
                        raise ValueError("顶层结构不是 JSON 对象")
                    if path == self.backup_file:
                        logger.warning("主数据文件不可用，已从备份文件恢复数据")
                    break
                except Exception as e:
                    raw = None
                    logger.error(f"读取数据文件 {path.name} 失败: {e}")
                    if path == self.data_file:
                        self._quarantine(path)
                    else:
                        # 备份损坏同样隔离:不隔离的话第二次保存的轮转会
                        # 静默覆盖它,而它可能是唯一还能手工抢救数据的副本
                        self._quarantine(path, block_on_failure=False)

            if raw is None:
                return

            # ---- 房间信息 ----
            raw_rooms = raw.get("room_info") or {}
            if not isinstance(raw_rooms, dict):
                logger.error("room_info 结构非法，已忽略")
                raw_rooms = {}
            for key, value in raw_rooms.items():
                room_id = self._parse_room_id(key)
                if room_id is None:
                    continue
                if room_id in self.room_info:
                    logger.warning(f"房间号键 {key!r} 与已加载的 {room_id} 冲突，跳过")
                    continue
                if not isinstance(value, dict):
                    logger.warning(f"房间 {room_id} 的信息结构非法，跳过")
                    continue
                try:
                    self.room_info[room_id] = RoomInfoClass.from_dict(value)
                except Exception as e:
                    logger.warning(f"房间 {room_id} 的信息解析失败，跳过: {e}")

            # ---- 订阅数据（兼容旧 list 格式）----
            migrated = False
            raw_subs = raw.get("subscriptions") or {}
            if not isinstance(raw_subs, dict):
                logger.error("subscriptions 结构非法，已忽略")
                raw_subs = {}
            for key, sub_data in raw_subs.items():
                room_id = self._parse_room_id(key)
                if room_id is None:
                    continue
                bucket = self.subscriptions.setdefault(room_id, {})

                if isinstance(sub_data, list):
                    # 旧格式: list of umo strings，需要迁移。
                    # 迁移时继承旧文件中房间级别的通知设置
                    # （这些字段已从 RoomInfo 模型移除，直接读原始 dict；
                    # 礼物播报相关字段已随功能移除，不再迁移）
                    raw_room = raw_rooms.get(key)
                    flags: dict[str, bool] = {}
                    if isinstance(raw_room, dict):
                        flags = {
                            "at_all": bool(raw_room.get("at_all", False)),
                        }
                    for umo in sub_data:
                        if isinstance(umo, str):
                            bucket[umo] = SubConfigClass(**flags)
                    migrated = True
                    logger.info(
                        f"已迁移房间 {room_id} 的 {len(bucket)} 个订阅到新格式"
                    )
                elif isinstance(sub_data, dict):
                    # 新格式: {umo -> config dict}
                    for umo, config in sub_data.items():
                        if not isinstance(umo, str):
                            continue
                        if isinstance(config, dict):
                            try:
                                bucket[umo] = SubConfigClass.from_dict(config)
                            except Exception as e:
                                logger.warning(
                                    f"订阅配置解析失败 ({room_id}/{umo})，"
                                    f"使用默认配置: {e}"
                                )
                                bucket[umo] = SubConfigClass()
                        else:
                            bucket[umo] = SubConfigClass()
                else:
                    logger.error(
                        f"房间 {room_id} 的订阅数据格式无法识别 "
                        f"({type(sub_data).__name__})，已跳过"
                    )

            # ---- 退订历史 ----
            raw_history = raw.get("unsub_history") or {}
            if isinstance(raw_history, dict):
                for key, sub_data in raw_history.items():
                    room_id = self._parse_room_id(key)
                    if room_id is None or not isinstance(sub_data, dict):
                        continue
                    bucket = self.unsub_history.setdefault(room_id, {})
                    for umo, config in sub_data.items():
                        if isinstance(umo, str) and isinstance(config, dict):
                            try:
                                bucket[umo] = SubConfigClass.from_dict(config)
                            except Exception:
                                pass

            # ---- 孤立订阅检查（有订阅但无房间记录）----
            orphans = set(self.subscriptions) - set(self.room_info)
            for room_id in orphans:
                logger.warning(
                    f"房间 {room_id} 存在 {len(self.subscriptions[room_id])} 条订阅"
                    f"但无房间信息，可使用 /douyu del {room_id} 清理"
                )

            # 如果有旧格式数据迁移，立即保存（RLock 可重入，且此时处于
            # 启动阶段，同步写盘不影响事件循环）
            if migrated:
                if self.save():
                    logger.info("订阅数据格式已迁移并保存")

    # ==================== 保存 ====================

    def save(self) -> bool:
        """保存数据到文件

        序列化在 RLock 内完成（微秒级），磁盘 I/O（含 fsync）在独立的
        IO 锁下进行，避免持有 RLock 做 fsync 时阻塞其他线程的读取。
        本方法包含 fsync，从事件循环调用时应使用 asyncio.to_thread
        （mutator 方法已包含保存，整体经 to_thread 调用即可）。

        Returns:
            是否保存成功
        """
        with self._lock:
            seq, payload = self._serialize_locked()
        return self._write_payload(seq, payload)

    def _serialize_locked(self) -> tuple[int, str]:
        """序列化当前状态（调用者需持有 _lock）

        返回 (序列号, JSON 文本)。序列号用于并发写盘时丢弃过期快照。
        """
        self._save_seq += 1
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
            "unsub_history": {
                str(room_id): {
                    umo: config.to_dict()
                    for umo, config in sub_dict.items()
                }
                for room_id, sub_dict in self.unsub_history.items()
            },
        }
        # 完整序列化，序列化错误不会碰到磁盘文件
        return self._save_seq, json.dumps(data, ensure_ascii=False, indent=2)

    def _write_payload(self, seq: int, payload: str) -> bool:
        """把序列化快照原子写入磁盘（IO 锁内，可能阻塞数百毫秒）

        写入流程：写临时文件并 fsync -> 轮换备份 -> 原子替换。
        任何一步失败都不会破坏当前的主数据文件。
        """
        with self._io_lock:
            if seq <= self._written_seq:
                # 已有更新的快照落盘，本快照过期，跳过（数据不丢失）
                return True
            if self._write_blocked:
                logger.error(
                    "数据文件处于保护状态（加载失败且无法隔离原文件），已拒绝保存"
                )
                self.last_save_ok = False
                return False
            try:
                tmp = self.data_file.with_name(self.data_file.name + ".tmp")
                with open(tmp, "w", encoding="utf-8") as f:
                    f.write(payload)
                    f.flush()
                    os.fsync(f.fileno())
                # 保留上一代数据作为备份，再原子替换主文件
                if os.path.exists(self.data_file):
                    os.replace(self.data_file, self.backup_file)
                os.replace(tmp, self.data_file)
                self._written_seq = seq
                # 只有当本快照不早于最近一次失败的快照时才能洗白
                # last_save_ok:更旧快照的成功并不包含失败快照的变更,
                # 洗白会让用户失去"保存失败"的警告(静默丢数据)。
                # 下一次任意变更的全量保存会自然治愈磁盘状态
                if seq >= self._failed_seq:
                    self.last_save_ok = True
                else:
                    logger.warning(
                        "较旧快照落盘成功,但更新的快照曾写入失败,"
                        "保持保存失败状态直至下次成功保存"
                    )
                return True
            except Exception as e:
                logger.error(f"保存斗鱼直播数据失败: {e}", exc_info=True)
                self._failed_seq = max(self._failed_seq, seq)
                self.last_save_ok = False
                return False

    # ==================== 房间管理 ====================

    def add_room(self, room_id: int, info: RoomInfo) -> None:
        """添加房间

        Args:
            room_id: 房间号
            info: 房间信息
        """
        with self._lock:
            self.room_info[room_id] = info
            self.subscriptions.setdefault(room_id, {})
        self.save()

    def remove_room(self, room_id: int) -> bool:
        """删除房间（同时清理订阅数据，包括孤立订阅与退订历史）

        Args:
            room_id: 房间号

        Returns:
            是否有数据被删除
        """
        with self._lock:
            existed = self.room_info.pop(room_id, None) is not None
            orphaned = self.subscriptions.pop(room_id, None)
            history = self.unsub_history.pop(room_id, None)
            if not existed and orphaned is None and history is None:
                return False
            if not existed and orphaned:
                logger.warning(
                    f"已清理直播间 {room_id} 的 {len(orphaned)} 条孤立订阅"
                )
        self.save()
        return True

    def get_room(self, room_id: int) -> RoomInfo | None:
        """获取房间信息

        Args:
            room_id: 房间号

        Returns:
            房间信息，不存在返回 None
        """
        with self._lock:
            return self.room_info.get(room_id)

    def has_room(self, room_id: int) -> bool:
        """检查房间是否存在"""
        with self._lock:
            return room_id in self.room_info

    def get_all_rooms(self) -> dict[int, RoomInfo]:
        """获取所有房间"""
        with self._lock:
            return self.room_info.copy()

    def get_all_room_ids(self) -> list[int]:
        """获取所有房间号"""
        with self._lock:
            return list(self.room_info.keys())

    # ==================== 订阅管理 ====================

    def subscribe(
        self, room_id: int, umo: str, operator: str = ""
    ) -> tuple[bool, bool]:
        """添加订阅

        Args:
            room_id: 房间号
            umo: unified_msg_origin
            operator: 操作者 ID（审计用）

        Returns:
            (是否成功, 是否恢复了历史配置)；成功为 False 表示已订阅
        """
        from ..models.subscription import SubscriptionConfig as SubConfigClass

        with self._lock:
            # 存在性判定必须在锁内:命令层的"先 get_room 判存再订阅"与
            # remove_room 并发时,窗口内的订阅会重建已删房间的孤立订阅
            # 并落盘(用户收到"订阅成功"但永远收不到通知)
            if room_id not in self.room_info:
                return False, False
            bucket = self.subscriptions.setdefault(room_id, {})
            if umo in bucket:
                return False, False

            # 若有退订历史则恢复原配置
            restored = False
            history = self.unsub_history.get(room_id)
            config = history.pop(umo, None) if history else None
            if config is not None:
                restored = True
                if not history:
                    self.unsub_history.pop(room_id, None)
            else:
                config = SubConfigClass()
            if operator:
                config.subscribed_by = operator

            bucket[umo] = config
        self.save()
        return True, restored

    def unsubscribe(self, room_id: int, umo: str) -> bool:
        """取消订阅（非默认配置保留在退订历史中，重新订阅时恢复）

        Args:
            room_id: 房间号
            umo: unified_msg_origin

        Returns:
            是否成功（False 表示未订阅）
        """
        from ..models.subscription import SubscriptionConfig as SubConfigClass

        with self._lock:
            bucket = self.subscriptions.get(room_id)
            if not bucket or umo not in bucket:
                return False
            config = bucket.pop(umo)

            # 仅归档有恢复价值的配置（与默认值不同，忽略审计字段），
            # 避免退订历史随房间×群数无界增长
            current = config.to_dict()
            default = SubConfigClass().to_dict()
            current.pop("subscribed_by", None)
            default.pop("subscribed_by", None)
            if current != default:
                history = self.unsub_history.setdefault(room_id, {})
                # 重新插入使其位于末尾（dict 保序 ≈ 归档时间序）
                history.pop(umo, None)
                history[umo] = config
                # 每房间历史条数上限，超出时逐出最旧条目
                while len(history) > self.UNSUB_HISTORY_MAX_PER_ROOM:
                    history.pop(next(iter(history)))
        self.save()
        return True

    def get_subscribers(self, room_id: int) -> set[str]:
        """获取房间的订阅者列表"""
        with self._lock:
            bucket = self.subscriptions.get(room_id)
            return set(bucket.keys()) if bucket else set()

    def get_subscription_config(
        self, room_id: int, umo: str
    ) -> SubscriptionConfig | None:
        """获取指定订阅的配置

        Args:
            room_id: 房间号
            umo: unified_msg_origin

        Returns:
            订阅配置，不存在返回 None
        """
        with self._lock:
            bucket = self.subscriptions.get(room_id)
            return bucket.get(umo) if bucket else None

    def get_all_subscription_configs(
        self, room_id: int
    ) -> dict[str, SubscriptionConfig]:
        """获取房间所有订阅的配置

        Args:
            room_id: 房间号

        Returns:
            {umo -> SubscriptionConfig} 字典（浅拷贝）
        """
        with self._lock:
            return self.subscriptions.get(room_id, {}).copy()

    def update_subscription_config(
        self, room_id: int, umo: str, **kwargs: Any
    ) -> bool:
        """更新指定订阅的配置

        Args:
            room_id: 房间号
            umo: unified_msg_origin
            **kwargs: 要更新的字段 (如 at_all)

        Returns:
            是否成功更新
        """
        with self._lock:
            bucket = self.subscriptions.get(room_id)
            if not bucket or umo not in bucket:
                return False
            config = bucket[umo]
            for key, value in kwargs.items():
                if hasattr(config, key):
                    setattr(config, key, value)
        self.save()
        return True

    def get_user_subscriptions(self, umo: str) -> list[int]:
        """获取用户订阅的房间列表"""
        with self._lock:
            return [
                room_id
                for room_id, sub_dict in self.subscriptions.items()
                if umo in sub_dict
            ]

    def get_total_subscriptions(self) -> int:
        """获取总订阅数"""
        with self._lock:
            return sum(len(s) for s in self.subscriptions.values())

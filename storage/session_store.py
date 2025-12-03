"""直播会话数据存储模块

使用 SQLite 持久化存储直播会话和礼物数据。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.star import StarTools


@dataclass
class GiftEvent:
    """单次礼物事件"""

    timestamp: float
    user_name: str
    user_id: str
    gift_id: str
    gift_name: str
    gift_count: int
    gift_value: float = 0.0  # 礼物价值（元），可选

    def to_dict(self) -> dict[str, Any]:
        """转换为字典"""
        return {
            "timestamp": self.timestamp,
            "user_name": self.user_name,
            "user_id": self.user_id,
            "gift_id": self.gift_id,
            "gift_name": self.gift_name,
            "gift_count": self.gift_count,
            "gift_value": self.gift_value,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> GiftEvent:
        """从字典创建"""
        return cls(
            timestamp=data.get("timestamp", 0.0),
            user_name=data.get("user_name", ""),
            user_id=data.get("user_id", ""),
            gift_id=data.get("gift_id", ""),
            gift_name=data.get("gift_name", ""),
            gift_count=data.get("gift_count", 0),
            gift_value=data.get("gift_value", 0.0),
        )


@dataclass
class SessionStats:
    """直播会话统计（内存中）"""

    session_id: int | None = None  # 数据库中的 ID
    room_id: int = 0
    start_time: float = 0.0
    gifts: list[GiftEvent] = field(default_factory=list)
    total_gift_count: int = 0
    total_gift_value: float = 0.0
    gift_user_count: int = 0  # 送礼用户数

    def add_gift(self, event: GiftEvent) -> None:
        """添加礼物事件"""
        self.gifts.append(event)
        self.total_gift_count += event.gift_count
        self.total_gift_value += event.gift_value * event.gift_count

    def get_top_gifts(self, limit: int = 5) -> list[tuple[str, str, int]]:
        """获取贡献最多的礼物（按数量）

        Returns:
            列表，每项为 (user_name, gift_name, total_count)
        """
        # 按用户+礼物聚合
        aggregated: dict[tuple[str, str], int] = {}
        for g in self.gifts:
            key = (g.user_name, g.gift_name)
            aggregated[key] = aggregated.get(key, 0) + g.gift_count

        # 按数量降序排列
        sorted_items = sorted(aggregated.items(), key=lambda x: x[1], reverse=True)
        return [(k[0], k[1], v) for k, v in sorted_items[:limit]]

    def get_top_users(self, limit: int = 5) -> list[tuple[str, float]]:
        """获取贡献最多的用户（按价值）

        Returns:
            列表，每项为 (user_name, total_value)
        """
        user_values: dict[str, float] = {}
        for g in self.gifts:
            user_values[g.user_name] = (
                user_values.get(g.user_name, 0) + g.gift_value * g.gift_count
            )

        sorted_users = sorted(user_values.items(), key=lambda x: x[1], reverse=True)
        return sorted_users[:limit]

    def get_unique_users(self) -> set[str]:
        """获取所有送礼用户"""
        return {g.user_name for g in self.gifts}


class SessionStore:
    """直播会话存储

    使用 SQLite 数据库持久化存储会话和礼物数据。
    """

    def __init__(self, plugin_name: str = "astrbot_plugin_douyu_live"):
        """初始化存储

        Args:
            plugin_name: 插件名称，用于确定数据目录
        """
        self.data_dir: Path = StarTools.get_data_dir(plugin_name)
        self.db_path: Path = self.data_dir / "sessions.db"
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None

        # 当前活跃的会话（内存缓存）
        self._active_sessions: dict[int, SessionStats] = {}  # room_id -> SessionStats

        # 初始化数据库
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """获取数据库连接（线程安全）"""
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def _init_db(self) -> None:
        """初始化数据库表"""
        with self._lock:
            conn = self._get_conn()
            cursor = conn.cursor()

            # 会话表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    room_id INTEGER NOT NULL,
                    start_time REAL NOT NULL,
                    end_time REAL,
                    duration REAL DEFAULT 0,
                    total_gift_count INTEGER DEFAULT 0,
                    total_gift_value REAL DEFAULT 0,
                    gift_user_count INTEGER DEFAULT 0,
                    summary_json TEXT,
                    created_at REAL DEFAULT (strftime('%s', 'now'))
                )
            """)

            # 礼物表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gifts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL,
                    timestamp REAL NOT NULL,
                    user_name TEXT NOT NULL,
                    user_id TEXT,
                    gift_id TEXT NOT NULL,
                    gift_name TEXT NOT NULL,
                    gift_count INTEGER DEFAULT 1,
                    gift_value REAL DEFAULT 0,
                    FOREIGN KEY (session_id) REFERENCES sessions(id)
                )
            """)

            # 索引
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_sessions_room ON sessions(room_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_gifts_session ON gifts(session_id)"
            )

            conn.commit()
            logger.debug("直播会话数据库已初始化")

    def start_session(self, room_id: int) -> SessionStats:
        """开始新的直播会话

        Args:
            room_id: 房间号

        Returns:
            新创建的会话统计对象
        """
        now = time.time()

        with self._lock:
            conn = self._get_conn()
            cursor = conn.cursor()

            # 先检查是否有未结束的会话，如果有则结束它
            cursor.execute(
                "SELECT id FROM sessions WHERE room_id = ? AND end_time IS NULL",
                (room_id,),
            )
            row = cursor.fetchone()
            if row:
                # 结束旧会话
                cursor.execute(
                    """
                    UPDATE sessions SET end_time = ?, duration = ? - start_time
                    WHERE id = ?
                    """,
                    (now, now, row["id"]),
                )
                logger.warning(f"房间 {room_id} 存在未结束的会话，已自动结束")

            # 创建新会话
            cursor.execute(
                "INSERT INTO sessions (room_id, start_time) VALUES (?, ?)",
                (room_id, now),
            )
            session_id = cursor.lastrowid
            conn.commit()

        # 创建内存统计对象
        stats = SessionStats(
            session_id=session_id,
            room_id=room_id,
            start_time=now,
        )
        self._active_sessions[room_id] = stats

        logger.debug(f"房间 {room_id} 开始新会话 (ID: {session_id})")
        return stats

    def add_gift(
        self,
        room_id: int,
        user_name: str,
        user_id: str,
        gift_id: str,
        gift_name: str,
        gift_count: int,
        gift_value: float = 0.0,
    ) -> bool:
        """添加礼物事件

        Args:
            room_id: 房间号
            user_name: 用户昵称
            user_id: 用户 ID
            gift_id: 礼物 ID
            gift_name: 礼物名称
            gift_count: 礼物数量
            gift_value: 单个礼物价值（元）

        Returns:
            是否成功（会话存在时为 True）
        """
        stats = self._active_sessions.get(room_id)
        if not stats or stats.session_id is None:
            logger.warning(f"房间 {room_id} 没有活跃会话，礼物未记录")
            return False

        now = time.time()
        event = GiftEvent(
            timestamp=now,
            user_name=user_name,
            user_id=user_id,
            gift_id=gift_id,
            gift_name=gift_name,
            gift_count=gift_count,
            gift_value=gift_value,
        )

        # 更新内存统计
        stats.add_gift(event)

        # 写入数据库
        with self._lock:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO gifts
                (session_id, timestamp, user_name, user_id, gift_id, gift_name, gift_count, gift_value)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stats.session_id,
                    now,
                    user_name,
                    user_id,
                    gift_id,
                    gift_name,
                    gift_count,
                    gift_value,
                ),
            )
            conn.commit()

        return True

    def end_session(self, room_id: int) -> SessionStats | None:
        """结束直播会话

        Args:
            room_id: 房间号

        Returns:
            会话统计对象，如果没有活跃会话则返回 None
        """
        stats = self._active_sessions.pop(room_id, None)
        if not stats or stats.session_id is None:
            logger.warning(f"房间 {room_id} 没有活跃会话")
            return None

        now = time.time()
        duration = now - stats.start_time
        unique_users = stats.get_unique_users()
        stats.gift_user_count = len(unique_users)

        # 构建摘要 JSON
        summary = {
            "top_gifts": stats.get_top_gifts(5),
            "top_users": stats.get_top_users(5),
            "unique_users": list(unique_users),
        }

        with self._lock:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE sessions SET
                    end_time = ?,
                    duration = ?,
                    total_gift_count = ?,
                    total_gift_value = ?,
                    gift_user_count = ?,
                    summary_json = ?
                WHERE id = ?
                """,
                (
                    now,
                    duration,
                    stats.total_gift_count,
                    stats.total_gift_value,
                    stats.gift_user_count,
                    json.dumps(summary, ensure_ascii=False),
                    stats.session_id,
                ),
            )
            conn.commit()

        logger.debug(
            f"房间 {room_id} 会话结束 (ID: {stats.session_id}, "
            f"时长: {duration:.0f}s, 礼物: {stats.total_gift_count})"
        )
        return stats

    def get_active_session(self, room_id: int) -> SessionStats | None:
        """获取当前活跃会话

        Args:
            room_id: 房间号

        Returns:
            会话统计对象，如果没有则返回 None
        """
        return self._active_sessions.get(room_id)

    def get_session_history(
        self, room_id: int, limit: int = 10
    ) -> list[dict[str, Any]]:
        """获取历史会话列表

        Args:
            room_id: 房间号
            limit: 返回条数

        Returns:
            会话列表
        """
        with self._lock:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, room_id, start_time, end_time, duration,
                       total_gift_count, total_gift_value, gift_user_count, summary_json
                FROM sessions
                WHERE room_id = ? AND end_time IS NOT NULL
                ORDER BY start_time DESC
                LIMIT ?
                """,
                (room_id, limit),
            )
            rows = cursor.fetchall()

        return [
            {
                "id": row["id"],
                "room_id": row["room_id"],
                "start_time": row["start_time"],
                "end_time": row["end_time"],
                "duration": row["duration"],
                "total_gift_count": row["total_gift_count"],
                "total_gift_value": row["total_gift_value"],
                "gift_user_count": row["gift_user_count"],
                "summary": json.loads(row["summary_json"])
                if row["summary_json"]
                else None,
            }
            for row in rows
        ]

    def close(self) -> None:
        """关闭数据库连接"""
        with self._lock:
            if self._conn:
                self._conn.close()
                self._conn = None

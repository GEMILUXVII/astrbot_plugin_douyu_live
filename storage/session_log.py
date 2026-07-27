"""场次历史落盘与监控状态快照

- SessionLog: 每房间一个 append-only JSONL(data_dir/sessions/{rid}.jsonl),
  记录开播/下播事件,供 /douyu stats、群周报等数据功能消费。
  行格式: {"e":"start","ts":epoch,"title":...,"cat":...}
          {"e":"end","ts":epoch,"dur":秒}
- MonitorStateStore: 干净关停时保存各监控器的 export_state(),下次启动
  回灌为 inherit_state——干净重启不再对在播房间重复播报(崩溃重启无
  快照,按首次观测语义处理,配置 catchup_announce 决定是否补发)。

线程模型: 写入方法含磁盘 I/O,应经 asyncio.to_thread 调用;
内部 threading.Lock 串行化文件访问。
"""

import json
import threading
import time
from pathlib import Path
from typing import Any

from astrbot.api import logger

# 快照最大可信年龄:超过后按无快照处理(过期的"在播"状态没有意义,
# 且会让状态机以陈旧基线起步)
STATE_MAX_AGE = 24 * 3600.0
PENDING_NOTIFICATION_MAX_AGE = 6 * 3600.0


class SessionLog:
    """场次历史(JSONL,append-only)"""

    def __init__(self, data_dir: Path, retention_days: int = 90):
        self.dir = data_dir / "sessions"
        self.retention_days = retention_days
        self._lock = threading.Lock()
        if self.enabled:
            self.dir.mkdir(parents=True, exist_ok=True)

    @property
    def enabled(self) -> bool:
        return self.retention_days > 0

    def _path(self, room_id: int) -> Path:
        return self.dir / f"{int(room_id)}.jsonl"

    def append(self, room_id: int, event: dict[str, Any]) -> None:
        """追加一条场次事件(含磁盘 I/O,经 to_thread 调用)"""
        if not self.enabled:
            return
        try:
            with self._lock:
                with open(self._path(room_id), "a", encoding="utf-8") as f:
                    f.write(json.dumps(event, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.warning(f"场次落盘失败(房间 {room_id}): {e}")

    def prune(self) -> None:
        """启动时清理超期场次行(重写文件;损坏行一并丢弃)"""
        if not self.enabled or not self.dir.exists():
            return
        cutoff = time.time() - self.retention_days * 86400
        for path in self.dir.glob("*.jsonl"):
            try:
                with self._lock:
                    kept = []
                    with open(path, encoding="utf-8") as f:
                        for line in f:
                            try:
                                event = json.loads(line)
                                if float(event.get("ts", 0)) >= cutoff:
                                    kept.append(line.rstrip("\n"))
                            except (ValueError, TypeError):
                                continue  # 损坏行丢弃
                    tmp = path.with_suffix(".jsonl.tmp")
                    with open(tmp, "w", encoding="utf-8") as f:
                        f.write("\n".join(kept) + ("\n" if kept else ""))
                    tmp.replace(path)
            except Exception as e:
                logger.warning(f"场次清理失败({path.name}): {e}")


class MonitorStateStore:
    """监控状态快照(干净关停 -> 启动回灌,一次性消费)"""

    def __init__(self, data_dir: Path):
        self.path = data_dir / "monitor_state.json"

    def save(self, states: dict[int, dict[str, Any]]) -> None:
        """保存快照(terminate 时经 to_thread 调用)"""
        try:
            payload = {
                "saved_at": time.time(),
                "states": {str(rid): s for rid, s in states.items()},
            }
            tmp = self.path.with_suffix(".json.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            tmp.replace(self.path)
        except Exception as e:
            logger.warning(f"监控状态快照保存失败: {e}")

    def load_and_clear(self) -> dict[int, dict[str, Any]]:
        """读取并删除快照(一次性:陈旧快照重复回灌有害无益)

        超过 STATE_MAX_AGE 的快照按不存在处理。
        """
        try:
            if not self.path.exists():
                return {}
            with open(self.path, encoding="utf-8") as f:
                payload = json.load(f)
            self.path.unlink(missing_ok=True)
            if not isinstance(payload, dict):
                return {}
            if time.time() - float(payload.get("saved_at", 0)) > STATE_MAX_AGE:
                logger.info("监控状态快照已过期,忽略")
                return {}
            states = payload.get("states")
            if not isinstance(states, dict):
                return {}
            out: dict[int, dict[str, Any]] = {}
            for key, value in states.items():
                if (
                    isinstance(key, str)
                    and key.isascii()
                    and key.isdigit()
                    and isinstance(value, dict)
                ):
                    out[int(key)] = value
            return out
        except Exception as e:
            logger.warning(f"监控状态快照读取失败,按无快照处理: {e}")
            try:
                self.path.unlink(missing_ok=True)
            except OSError:
                pass
            return {}


class PendingNotificationStore:
    """干净重载期间保存尚未确认发送成功的通知。"""

    def __init__(self, data_dir: Path):
        self.path = data_dir / "pending_notifications.json"

    def save(self, records: list[dict[str, Any]]) -> None:
        try:
            if not records:
                self.path.unlink(missing_ok=True)
                return
            payload = {
                "saved_at": time.time(),
                "notifications": records,
            }
            tmp = self.path.with_suffix(".json.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            tmp.replace(self.path)
        except Exception as e:
            logger.warning(f"待发送通知快照保存失败: {e}")

    def load_and_clear(self) -> list[dict[str, Any]]:
        try:
            if not self.path.exists():
                return []
            with open(self.path, encoding="utf-8") as f:
                payload = json.load(f)
            self.path.unlink(missing_ok=True)
            if not isinstance(payload, dict):
                return []
            saved_at = float(payload.get("saved_at", 0))
            if time.time() - saved_at > PENDING_NOTIFICATION_MAX_AGE:
                logger.warning("待发送通知快照已过期，已忽略")
                return []
            records = payload.get("notifications")
            if not isinstance(records, list):
                return []
            return [record for record in records if isinstance(record, dict)]
        except Exception as e:
            logger.warning(f"待发送通知快照读取失败，已忽略: {e}")
            try:
                self.path.unlink(missing_ok=True)
            except OSError:
                pass
            return []

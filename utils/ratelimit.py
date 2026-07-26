"""斗鱼外呼节流:RoomInfo 短 TTL 缓存 + 并发上限

/douyu live 与通知富化都会触发斗鱼 HTTP 外呼;不加约束的话,用户刷
命令会把请求全额打到斗鱼接口,风险是本机 IP 被风控(殃及所有房间的
监控与富化)。宿主的会话级限流管不到"是否触发外呼"这一层。
"""

import asyncio
import time

from aiodouyu import RoomInfo, fetch_room


class RoomInfoCache:
    """房间信息缓存(仅事件循环访问)

    - TTL 内直接命中,不外呼
    - 未命中时经信号量限并发拉取,双检避免同房间重复外呼
    - 失败不缓存(下次调用重试),异常原样抛给调用方决定降级方式
    """

    def __init__(self, ttl: float = 60.0, concurrency: int = 5):
        self._ttl = ttl
        self._cache: dict[int, tuple[float, RoomInfo]] = {}
        self._sem = asyncio.Semaphore(concurrency)

    def _hit(self, room_id: int) -> RoomInfo | None:
        entry = self._cache.get(room_id)
        if entry and time.monotonic() - entry[0] < self._ttl:
            return entry[1]
        return None

    async def get(
        self, room_id: int, *, source: str = "auto", timeout: float = 5.0
    ) -> RoomInfo:
        info = self._hit(room_id)
        if info is not None:
            return info
        async with self._sem:
            info = self._hit(room_id)  # 双检:排队期间可能已有人拉回
            if info is not None:
                return info
            info = await fetch_room(room_id, source=source, timeout=timeout)
            self._cache[room_id] = (time.monotonic(), info)
            return info

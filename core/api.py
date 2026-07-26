"""斗鱼 API 调用模块"""

from typing import TypedDict

import httpx

from astrbot.api import logger


class DouyuRoomData(TypedDict, total=False):
    """斗鱼房间接口返回的房间信息

    注意与 models.room.RoomInfo（插件持久化模型）区分。
    """
    owner_name: str
    nickname: str
    room_name: str


class DouyuAPI:
    """斗鱼 API 封装类

    提供获取直播间信息等功能。
    使用公开的 betard 接口，无需鉴权。
    """

    BASE_URL = "https://www.douyu.com/betard"
    TIMEOUT = 10.0
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.douyu.com/",
    }

    @classmethod
    async def fetch_room_info(cls, room_id: int) -> DouyuRoomData | None:
        """从斗鱼获取直播间信息

        Args:
            room_id: 斗鱼直播间房间号

        Returns:
            包含 owner_name, nickname, room_name 的字典；
            房间不存在或获取失败返回 None
        """
        if room_id <= 0:
            return None

        url = f"{cls.BASE_URL}/{room_id}"
        try:
            async with httpx.AsyncClient(
                timeout=cls.TIMEOUT, headers=cls.HEADERS, follow_redirects=True
            ) as client:
                response = await client.get(url)
                if response.status_code != 200:
                    logger.warning(
                        f"获取斗鱼直播间 {room_id} 信息失败: HTTP {response.status_code}"
                    )
                    return None

                data = response.json()
                room = data.get("room") if isinstance(data, dict) else None
                if not isinstance(room, dict):
                    # 不存在的房间号斗鱼也可能返回 200，但 body 中没有 room 字段
                    logger.warning(
                        f"斗鱼直播间 {room_id} 返回数据中无房间信息（房间可能不存在）"
                    )
                    return None

                info = DouyuRoomData(
                    owner_name=str(room.get("owner_name") or ""),
                    nickname=str(room.get("nickname") or ""),
                    room_name=str(room.get("room_name") or ""),
                )
                if not (info["owner_name"] or info["nickname"] or info["room_name"]):
                    logger.warning(f"斗鱼直播间 {room_id} 返回的房间信息为空")
                    return None
                return info
        except Exception as e:
            logger.warning(f"获取斗鱼直播间 {room_id} 信息失败: {e}")
        return None

"""文本清洗工具

来自斗鱼 HTTP 接口的主播/房间名称（以及管理员输入的名称）是外部可控
文本，直接插入通知与命令回复可能被用来伪造消息框架或注入多行钓鱼内容。
所有进入展示文本的外部字段都应先经过 sanitize_display_text 清洗。
"""

import re

# 需要移除的不可见/控制字符（用显式转义书写，避免源码中出现不可见字符）：
# - C0 控制字符与 DEL、C1 控制字符（\x00-\x1f\x7f-\x9f，含换行、制表符）
# - Unicode 行/段分隔符 U+2028/U+2029
# - bidi 控制符：旧式嵌入/覆盖符 U+202A-202E，isolate 系 U+2066-2069
#   （Trojan-Source 攻击的标准载体），方向标记 U+200E/U+200F 与 U+061C
#   ——外部可控昵称含这些字符时可视觉重排通知行内容
# - 零宽字符 U+200B-U+200D、U+2060、U+FEFF（防填充伪装）
_CONTROL_CHARS = re.compile(
    "["
    "\x00-\x1f\x7f-\x9f"  # C0 控制字符、DEL、C1 控制字符
    "\u2028\u2029"  # 行/段分隔符
    "\u202a-\u202e"  # bidi 嵌入/覆盖符
    "\u2066-\u2069"  # bidi isolate (LRI/RLI/FSI/PDI)
    "\u200e\u200f\u061c"  # bidi 方向标记 (LRM/RLM/ALM)
    "\u200b-\u200d\u2060\ufeff"  # 零宽字符
    "]"
)


def sanitize_display_text(
    text: object, max_len: int = 32, default: str = "未知"
) -> str:
    """清洗用于展示的外部文本

    - 移除换行/控制字符（含 C1、bidi 控制符与零宽字符），防止伪造消息框架
    - 归一化框线字符，防止仿冒通知分隔线
    - 截断超长内容

    Args:
        text: 原始文本（任意类型，会先转为 str）
        max_len: 最大保留长度
        default: 清洗后为空时的回退值

    Returns:
        清洗后的文本
    """
    s = _CONTROL_CHARS.sub(" ", str(text))
    s = s.replace("━", "-").strip()
    if len(s) > max_len:
        s = s[: max_len - 1] + "…"
    return s or default

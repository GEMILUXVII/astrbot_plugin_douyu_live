"""文本清洗工具

来自斗鱼 HTTP 接口的主播/房间名称（以及管理员输入的名称）是外部可控
文本，直接插入通知与命令回复可能被用来伪造消息框架或注入多行钓鱼内容。
所有进入展示文本的外部字段都应先经过 sanitize_display_text 清洗。
"""

import re

# C0/C1 控制字符（含换行、制表符、U+0085 NEL）、DEL、
# Unicode 行/段分隔符、双向文本覆盖控制符
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f-\x9f  ‪-‮]")


def sanitize_display_text(text: object, max_len: int = 32, default: str = "未知") -> str:
    """清洗用于展示的外部文本

    - 移除换行/控制字符（含 C1 与 bidi 控制符），防止伪造消息框架
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

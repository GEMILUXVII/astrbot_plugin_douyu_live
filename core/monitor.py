"""aiodouyu 高层直播状态监控器的插件兼容导出。"""

import logging

from aiodouyu import LiveStatusMonitor
from astrbot.api import logger


class _AstrBotLogHandler(logging.Handler):
    """把依赖库日志接入 AstrBot 的统一日志输出。"""

    _astrbot_douyu_bridge = True

    def emit(self, record: logging.LogRecord) -> None:
        message = self.format(record)
        if record.levelno >= logging.ERROR:
            logger.error(message)
        elif record.levelno >= logging.WARNING:
            logger.warning(message)
        elif record.levelno >= logging.INFO:
            logger.info(message)
        else:
            logger.debug(message)


def _install_log_bridge() -> None:
    dependency_logger = logging.getLogger("aiodouyu.monitor")
    if any(
        getattr(handler, "_astrbot_douyu_bridge", False)
        for handler in dependency_logger.handlers
    ):
        return
    handler = _AstrBotLogHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    dependency_logger.addHandler(handler)
    dependency_logger.setLevel(logging.INFO)
    dependency_logger.propagate = False


_install_log_bridge()

# 保留既有插件内部名称，避免主插件与状态快照迁移产生无意义改动。
DouyuMonitor = LiveStatusMonitor

__all__ = ["DouyuMonitor"]

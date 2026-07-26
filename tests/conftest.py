"""pytest 共享夹具：注入 astrbot 桩模块

插件运行在 AstrBot 宿主内；单元测试不依赖宿主，在导入任何插件模块之前
把最小桩模块塞进 sys.modules（因此桩注入必须在 conftest 顶层执行）。
aiodouyu 是真实依赖（pip install aiodouyu），不打桩；监控器测试通过
client_factory 注入假弹幕客户端。

注意：仓库检出目录名必须与包名一致（astrbot_plugin_douyu_live），
测试通过包导入方式加载插件模块。
"""

import sys
import types
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT.parent))

# DataManager 通过 StarTools.get_data_dir 取数据目录；
# 测试用 data_dir 夹具把它指向每个测试独立的临时目录
_DATA_DIR: dict = {"dir": None}


class _Logger:
    def _log(self, *args, **kwargs):
        pass

    debug = info = warning = error = _log


class _Star:
    def __init__(self, context):
        self.context = context


class _StarTools:
    @staticmethod
    def get_data_dir(name):
        assert _DATA_DIR["dir"] is not None, "请使用 data_dir 夹具设置数据目录"
        return Path(_DATA_DIR["dir"])


class _CmdGroup:
    def command(self, *args, **kwargs):
        def deco(fn):
            return fn

        return deco


def _command_group(name):
    def deco(fn):
        return _CmdGroup()

    return deco


def _passthrough_deco(*args, **kwargs):
    def deco(fn):
        return fn

    return deco


class _PermissionType:
    ADMIN = 1
    MEMBER = 2


class MessageEventResult:
    def __init__(self):
        self.chain = []


class AtAll:
    pass


class Plain:
    def __init__(self, text=""):
        self.text = text


class GreedyStr(str):
    pass


def _install_stubs() -> None:
    api_mod = types.ModuleType("astrbot.api")
    api_mod.logger = _Logger()

    star_mod = types.ModuleType("astrbot.api.star")
    star_mod.Star = _Star
    star_mod.Context = object
    star_mod.StarTools = _StarTools
    api_mod.star = star_mod

    event_mod = types.ModuleType("astrbot.api.event")
    event_mod.AstrMessageEvent = object
    event_mod.MessageEventResult = MessageEventResult

    filter_mod = types.ModuleType("astrbot.api.event.filter")
    filter_mod.command_group = _command_group
    filter_mod.command = _passthrough_deco
    filter_mod.permission_type = _passthrough_deco
    filter_mod.PermissionType = _PermissionType
    event_mod.filter = filter_mod

    comp_mod = types.ModuleType("astrbot.api.message_components")
    comp_mod.AtAll = AtAll
    comp_mod.Plain = Plain

    astrbot_mod = types.ModuleType("astrbot")
    astrbot_mod.api = api_mod

    core_mod = types.ModuleType("astrbot.core")
    core_star_mod = types.ModuleType("astrbot.core.star")
    core_star_filter_mod = types.ModuleType("astrbot.core.star.filter")
    core_star_filter_command_mod = types.ModuleType("astrbot.core.star.filter.command")
    core_star_filter_command_mod.GreedyStr = GreedyStr

    for name, mod in {
        "astrbot": astrbot_mod,
        "astrbot.core": core_mod,
        "astrbot.core.star": core_star_mod,
        "astrbot.core.star.filter": core_star_filter_mod,
        "astrbot.core.star.filter.command": core_star_filter_command_mod,
        "astrbot.api": api_mod,
        "astrbot.api.star": star_mod,
        "astrbot.api.event": event_mod,
        "astrbot.api.event.filter": filter_mod,
        "astrbot.api.message_components": comp_mod,
    }.items():
        sys.modules[name] = mod


_install_stubs()


class FakeTime:
    """可注入 monitor 模块的 time 替身"""

    def __init__(self, start=1000.0):
        self.now = start

    def time(self):
        return self.now

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class FakeDanmakuClient:
    """aiodouyu.DanmakuClient 替身：消息由测试脚本注入，不联网

    复刻真实客户端的关键语义：close() 立即唤醒阻塞中的消费者并终止迭代、
    close 幂等、同一实例只允许一个消费迭代器、close 后再迭代抛
    ConnectionClosed——防止假客户端掩盖对真实库的误用。
    """

    def __init__(self):
        import asyncio

        self._queue: asyncio.Queue = asyncio.Queue()
        self.closed = False
        self._iterating = False

    def push(self, msg: dict) -> None:
        """注入一条消息给消费方"""
        self._queue.put_nowait(msg)

    def push_error(self, exc: Exception) -> None:
        """注入一个异常，消费方在迭代中收到时原样抛出"""
        self._queue.put_nowait(exc)

    async def close(self) -> None:
        self.closed = True
        self._queue.put_nowait(None)  # 终止哨兵

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        from aiodouyu import ConnectionClosed

        if self._iterating:
            raise RuntimeError("同一客户端只允许一个消费迭代器")
        if self.closed:
            raise ConnectionClosed("客户端已关闭")
        self._iterating = True
        try:
            while True:
                msg = await self._queue.get()
                if msg is None or self.closed:
                    return
                if isinstance(msg, Exception):
                    raise msg
                yield msg
        finally:
            self._iterating = False


@pytest.fixture
def data_dir(tmp_path):
    """为每个测试提供独立的数据目录"""
    _DATA_DIR["dir"] = tmp_path
    yield tmp_path


@pytest.fixture
def fake_time():
    """把 monitor 模块的 time 换成可控替身"""
    import astrbot_plugin_douyu_live.core.monitor as monitor_mod

    ft = FakeTime()
    real = monitor_mod.time
    monitor_mod.time = ft
    yield ft
    monitor_mod.time = real


@pytest.fixture
def make_main(data_dir):
    """构造带假上下文的 Main 实例"""

    def _make():
        from astrbot_plugin_douyu_live.main import Main

        class Ctx:
            async def send_message(self, umo, result):
                return True

        return Main(Ctx())

    return _make

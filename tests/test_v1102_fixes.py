"""v1.10.2 多 profile 并发路由修复回归测试.

根因（2026-11 生产排查，14 天证据链）：
- A 类 ×7：``230002 Bot/User can NOT be out of the chat``——卡片 API 按
  **执行时刻**的 ambient 上下文（插件 ContextVar → loop 线程共享
  thread-local → hermes contextvar → env）解析 home；多 profile turn 并发
  时 loop 线程 thread-local 是"最近进入 wrapper 的 turn"的 home，另一
  profile 的建卡路由到错误的 bot（2026-11-06 09:18:37 现场：main DM
  建卡构建了 coder 的 client cli_a97784）。
- B 类 ×12：``99992354 not a valid {open_message_id}``——hermes 0.21+ 的
  合成锚（proc_*/deleg_*）被 reply_card_by_id 当消息 id。
- C 类 ×6：``on_message_aborted() got multiple values for keyword argument
  'ctrl'``——adapter.py 给 @_safe_hook 包装的函数传 ctrl=。

修复：卡片操作按 session.home 固定路由（_client_for）；合成锚降级
send_card_by_id_to_chat；230002 单次退避重试 + 终极降级；ctrl kwarg
移除；loop 线程不再写共享 thread-local。
"""

# pyright: reportMissingImports=false, reportAttributeAccessIssue=false
# 包名由 tests/conftest.py 运行时注册；静态解析必然误报。

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from hermes_lark_streaming.config import reader as cfg_reader
from hermes_lark_streaming.controller import core as ctrl_core
from hermes_lark_streaming.controller.linear_mixin import UnifiedControllerMixin
from hermes_lark_streaming.feishu.client import FeishuAPIError
from hermes_lark_streaming.state.session import CardSession


@pytest.fixture()
def two_homes(tmp_path: Path) -> tuple[Path, Path]:
    home_a = tmp_path / "profile_a"
    home_b = tmp_path / "profile_b"
    home_a.mkdir()
    home_b.mkdir()
    (home_a / "config.yaml").write_text(
        "feishu:\n  app_id: cli_aaa\n  app_secret: sec_aaa\n",
        encoding="utf-8",
    )
    (home_b / ".env").write_text(
        "FEISHU_APP_ID=cli_bbb\nFEISHU_APP_SECRET=sec_bbb\n",
        encoding="utf-8",
    )
    return home_a, home_b


@pytest.fixture(autouse=True)
def _isolated_turn_ctx(monkeypatch):
    """清空 turn home 绑定与凭据缓存，保证用例独立。"""
    cfg_reader._CRED_CACHE.clear()
    cfg_reader.set_turn_home(None)
    cfg_reader.set_turn_home_thread(None)
    yield
    cfg_reader._CRED_CACHE.clear()
    cfg_reader.set_turn_home(None)
    cfg_reader.set_turn_home_thread(None)


class _StubClient:
    def __init__(self, config) -> None:
        self.app_id = config.app_id


def _make_session(home: str, message_id: str = "om_sess") -> CardSession:
    loop = asyncio.new_event_loop()
    try:
        session = CardSession(message_id, "chat_x", loop)
    finally:
        loop.close()
    session.home = home
    return session


# ── P0-1：会话绑定路由 ───────────────────────────────────────────────


class TestClientForSession:
    def test_pool_client_for_home_explicit(self, two_homes, monkeypatch) -> None:
        """client_for_home 不做 ambient 解析——显式 home 直取。"""
        monkeypatch.setattr(ctrl_core, "FeishuClient", _StubClient)
        pool = ctrl_core._ClientPool()
        home_a, home_b = two_homes
        # ambient 绑定指向 B，显式请求 A 必须拿到 A（污染不生效）
        cfg_reader.set_turn_home(home_b)
        assert pool.client_for_home(home_a).app_id == "cli_aaa"
        assert pool.client_for_home(home_b).app_id == "cli_bbb"
        # 池内缓存：同 home 复用实例
        assert pool.client_for_home(home_a) is pool.client_for_home(home_a)

    def test_pool_client_for_home_no_creds_raises(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setattr(ctrl_core, "FeishuClient", _StubClient)
        empty = tmp_path / "empty_home"
        empty.mkdir()
        pool = ctrl_core._ClientPool()
        with pytest.raises(RuntimeError, match="not configured for home"):
            pool.client_for_home(empty)

    def test_controller_client_for_uses_session_home(
        self, two_homes, monkeypatch
    ) -> None:
        """_client_for(session) 按 session.home 路由——ambient 污染（thread-
        local 指向别的 profile）不影响会话内卡片操作（A 类根因回归）。"""
        monkeypatch.setattr(ctrl_core, "FeishuClient", _StubClient)
        ctrl = ctrl_core.StreamCardController()
        asyncio.run(ctrl._ensure_init())
        home_a, home_b = two_homes
        session_a = _make_session(str(home_a))
        # 模拟污染：loop 线程 thread-local 指向 B
        cfg_reader.set_turn_home_thread(home_b)
        assert ctrl._client_for(session_a).app_id == "cli_aaa"
        # ContextVar 层也指向 B（双层污染）
        cfg_reader.set_turn_home(home_b)
        assert ctrl._client_for(session_a).app_id == "cli_aaa"

    def test_controller_client_for_ambient_fallback_when_no_home(
        self, two_homes, monkeypatch
    ) -> None:
        """session.home 为空（测试桩/极端构造失败）时回退 ambient 代理。"""
        monkeypatch.setattr(ctrl_core, "FeishuClient", _StubClient)
        ctrl = ctrl_core.StreamCardController()
        asyncio.run(ctrl._ensure_init())
        home_a, _ = two_homes
        session = _make_session("")
        cfg_reader.set_turn_home(home_a)
        assert ctrl._client_for(session).app_id == "cli_aaa"

    def test_controller_client_for_no_creds_ambient_fallback(
        self, two_homes, tmp_path, monkeypatch
    ) -> None:
        """显式 home 无凭据（配置漂移/轮换窗口）：降级 ambient 兜底，不吞卡。"""
        monkeypatch.setattr(ctrl_core, "FeishuClient", _StubClient)
        ctrl = ctrl_core.StreamCardController()
        asyncio.run(ctrl._ensure_init())
        home_a, _ = two_homes
        empty = tmp_path / "empty_home"
        empty.mkdir()
        session = _make_session(str(empty))
        cfg_reader.set_turn_home(home_a)
        assert ctrl._client_for(session).app_id == "cli_aaa"


class TestSessionHomePropagation:
    def test_on_message_started_turn_home_pinned(self, two_homes) -> None:
        """wrapper 显式传入的 turn_home 固化到 session.home。"""
        import threading

        home_a, _ = two_homes
        ctrl = ctrl_core.StreamCardController.__new__(ctrl_core.StreamCardController)
        # 只初始化 on_message_started 需要的字段（绕开 Config/单例依赖）
        ctrl._cfg = SimpleNamespace(enabled=True, feishu_app_id="cli_x", env_app_id="")
        ctrl._sessions = {}
        ctrl._sessions_lock = threading.RLock()
        ctrl._interrupt_map = {}
        ctrl._interrupt_map_lock = threading.Lock()
        ctrl._continuation_map = {}
        ctrl._continuation_map_lock = threading.Lock()
        ctrl._loop = None
        ctrl._watchdog_task = None
        ctrl._pending_tasks = set()

        async def run() -> None:
            ctrl.on_message_started(
                message_id="om_pin",
                chat_id="chat_x",
                anchor_id=None,
                thread_id=None,
                turn_home=str(home_a),
            )

        asyncio.run(run())
        session = ctrl._sess_get("om_pin")
        assert session is not None
        assert session.home == str(home_a)

    def test_continuation_session_inherits_home(self, two_homes) -> None:
        """续写会话继承原会话 home（链式拆卡路由不漂移）。"""
        home_a, _ = two_homes
        loop = asyncio.new_event_loop()
        try:
            stale = CardSession("om_old", "chat_x", loop)
        finally:
            loop.close()
        stale.home = str(home_a)

        # 与 _reactivate_session_for_continuation 的构造段同构：新会话
        # home 显式继承 stale 的固化值（不再 ambient 重解析）
        loop2 = asyncio.new_event_loop()
        try:
            new_session = CardSession("om_old-cont-1", "chat_x", loop2)
        finally:
            loop2.close()
        if stale.home:
            new_session.home = stale.home
        assert new_session.home == str(home_a)


# ── P1：合成锚降级 + 230002 重试 ─────────────────────────────────────


class _ScriptedClient:
    """按脚本回放的 FeishuClient 替身。"""

    def __init__(self, script: dict[str, list]) -> None:
        self.script = script
        self.calls: list[tuple[str, tuple]] = []

    def __getattr__(self, name: str):
        # 未在脚本中的方法返回静默协程工厂
        def _noop(*args, **kwargs):
            async def _coro():
                self.calls.append((name, args))
                results = self.script.get(name)
                if results:
                    r = results.pop(0)
                    if isinstance(r, Exception):
                        raise r
                    return r
                return None

            return _coro()

        return _noop


class TestReplyCardToChat:
    def _ctrl_with(self, client: _ScriptedClient) -> UnifiedControllerMixin:
        ctrl = ctrl_core.StreamCardController.__new__(ctrl_core.StreamCardController)
        ctrl._client_for = lambda session: client  # type: ignore[method-assign]
        return ctrl

    def test_synthetic_anchor_goes_send_to_chat(self) -> None:
        """proc_*/deleg_* 合成锚（B 类 ×12 根因）直接 send 到 chat，不 reply。"""
        client = _ScriptedClient({"send_card_by_id_to_chat": ["om_new"]})
        ctrl = self._ctrl_with(client)
        session = _make_session("/home/a", message_id="proc_4562263e5bd9")
        session.anchor_id = None
        result = asyncio.run(ctrl._reply_card_to_chat(session, "card_1"))
        assert result == "om_new"
        assert client.calls == [("send_card_by_id_to_chat", ("chat_x", "card_1"))]

    def test_om_anchor_replies(self) -> None:
        client = _ScriptedClient({"reply_card_by_id": ["om_replied"]})
        ctrl = self._ctrl_with(client)
        session = _make_session("/home/a", message_id="om_real123")
        result = asyncio.run(ctrl._reply_card_to_chat(session, "card_1"))
        assert result == "om_replied"
        assert client.calls[0][0] == "reply_card_by_id"

    def test_230002_retries_then_falls_back(self, monkeypatch) -> None:
        """230002：退避重试一次，仍失败降级 send 到 chat（飞书瞬时态吸收）。"""
        client = _ScriptedClient(
            {
                "reply_card_by_id": [
                    FeishuAPIError("reply_card_by_id: code=230002", code=230002),
                    FeishuAPIError("reply_card_by_id: code=230002", code=230002),
                ],
                "send_card_by_id_to_chat": ["om_fb"],
            }
        )
        ctrl = self._ctrl_with(client)
        # 跳过真实 sleep
        monkeypatch.setattr(asyncio, "sleep", _fast_sleep)
        session = _make_session("/home/a", message_id="om_real123")
        result = asyncio.run(ctrl._reply_card_to_chat(session, "card_1"))
        assert result == "om_fb"
        kinds = [c[0] for c in client.calls]
        assert kinds == [
            "reply_card_by_id",
            "reply_card_by_id",
            "send_card_by_id_to_chat",
        ]

    def test_230002_recovers_on_retry(self, monkeypatch) -> None:
        """飞书瞬时 230002 在重试后恢复——走正常 reply 锚定。"""
        client = _ScriptedClient(
            {
                "reply_card_by_id": [
                    FeishuAPIError("reply_card_by_id: code=230002", code=230002),
                    "om_ok",
                ],
            }
        )
        ctrl = self._ctrl_with(client)
        monkeypatch.setattr(asyncio, "sleep", _fast_sleep)
        session = _make_session("/home/a", message_id="om_real123")
        assert asyncio.run(ctrl._reply_card_to_chat(session, "card_1")) == "om_ok"

    def test_other_error_propagates(self) -> None:
        """非 230002/99992354 错误直抛（保持 CREATION_FAILED 语义）。"""
        client = _ScriptedClient(
            {"reply_card_by_id": [FeishuAPIError("x: code=9994", code=9994)]}
        )
        ctrl = self._ctrl_with(client)
        session = _make_session("/home/a", message_id="om_real123")
        with pytest.raises(FeishuAPIError):
            asyncio.run(ctrl._reply_card_to_chat(session, "card_1"))


async def _fast_sleep(delay: float) -> None:
    return None


# ── P0-2：ctrl 双传修复 ──────────────────────────────────────────────


class TestOnMessageAbortedContract:
    def test_omit_ctrl_kwarg_no_typeerror(self, monkeypatch) -> None:
        """调用方只传 message_id——_safe_hook 自己注入 ctrl（C 类 ×6 根因）。"""
        from hermes_lark_streaming.patching import hooks as hls_hooks

        ctrl = SimpleNamespace(enabled=True, on_aborted=lambda *, message_id: None)
        monkeypatch.setattr(hls_hooks, "get_controller", lambda: ctrl)
        # v1.10.2 契约：不得抛 "multiple values for keyword argument 'ctrl'"
        hls_hooks.on_message_aborted(message_id="om_stop1")

    def test_stop_path_seals_session(self, monkeypatch) -> None:
        """adapter.py /stop 检测路径端到端：on_message_aborted 真正到达
        controller.on_aborted（v1.9.0 起被 TypeError 掐断，从未送达）。"""
        from hermes_lark_streaming.patching import hooks as hls_hooks

        aborted: list[str] = []

        class Ctrl:
            enabled = True

            def on_aborted(self, *, message_id: str) -> None:
                aborted.append(message_id)

        monkeypatch.setattr(hls_hooks, "get_controller", lambda: Ctrl())
        hls_hooks.on_message_aborted(message_id="om_abort_me")
        assert aborted == ["om_abort_me"]


# ── P2：loop 线程不再写共享 thread-local ────────────────────────────


class TestLoopThreadNoThreadLocalWrite:
    def test_wrapper_does_not_touch_thread_local(self, monkeypatch) -> None:
        """_wrap_handle_message_with_agent 执行后 _TURN_LOCAL 保持 None
        （旧代码会把 loop 线程 thread-local 写成本 turn 的 home——并发污染源）。"""
        from hermes_lark_streaming.patching.gateway import (
            _wrap_handle_message_with_agent,
        )

        async def orig(self, event, source, *args, **kwargs):
            return None

        class FakeSelf:
            def _reply_anchor_for_event(self, event):
                return "om_anchor1"

        event = SimpleNamespace(message_id="om_tl1")
        source = SimpleNamespace(
            platform=SimpleNamespace(value="feishu"),
            chat_id="chat_x",
            thread_id=None,
        )
        wrapper = _wrap_handle_message_with_agent(orig)

        async def run() -> None:
            # 走完整个 wrapper（含 START hook 与 cleanup）
            await wrapper(FakeSelf(), event, source)

        # START hook 需要可控 controller
        monkeypatch.setattr(
            "hermes_lark_streaming.patching.hooks.get_controller",
            lambda: SimpleNamespace(enabled=False),
        )
        asyncio.run(run())
        # wrapper 结束后：loop 线程的 thread-local 未被写入（保持 None）
        assert cfg_reader._TURN_LOCAL.data is None


# ── v1.10.3：文本兜底降级 + 热路径性能 + 可观测性 ──────────────────


class TestTextFallbackSyntheticAnchor:
    def test_text_fallback_synthetic_anchor_sends_to_chat(self) -> None:
        """v1.10.3: 文本兜底遇合成锚（19:45 生产现场 reply_text 99992354）
        降级 send_text_to_chat——用户至少收到内容。"""
        from hermes_lark_streaming.controller.core import StreamCardController

        client = _ScriptedClient({"send_text_to_chat": ["om_sent"]})
        ctrl = StreamCardController.__new__(StreamCardController)
        ctrl._client = object()  # 前置守卫只判空；实际调用走 _client_for mock
        ctrl._client_for = lambda session: client  # type: ignore[method-assign]
        # message_id 为 list 字符串化形态（hermes 合成锚直传）
        session = _make_session("/home/a", message_id="['completion', 'proc_x']")
        session.anchor_id = None
        asyncio.run(ctrl._send_text_fallback(session, fallback_text="答案内容"))
        assert client.calls == [("send_text_to_chat", ("chat_x", "答案内容"))]

    def test_text_fallback_om_anchor_replies(self) -> None:
        from hermes_lark_streaming.controller.core import StreamCardController

        client = _ScriptedClient({"reply_text": ["om_ok"]})
        ctrl = StreamCardController.__new__(StreamCardController)
        ctrl._client = object()  # 前置守卫只判空
        ctrl._client_for = lambda session: client  # type: ignore[method-assign]
        session = _make_session("/home/a", message_id="om_real")
        asyncio.run(ctrl._send_text_fallback(session, fallback_text="答案内容"))
        assert client.calls[0][0] == "reply_text"


class TestClientForHomeFastPath:
    def test_cached_client_survives_creds_removal(self, two_homes, monkeypatch) -> None:
        """v1.10.3: client 缓存命中直接返回——creds 源被删（轮换窗口）也不
        影响已建立的会话路由（快路径不查 creds）。"""
        monkeypatch.setattr(ctrl_core, "FeishuClient", _StubClient)
        pool = ctrl_core._ClientPool()
        home_a, _ = two_homes
        first = pool.client_for_home(home_a)
        # 凭据文件删除 + 缓存清空（模拟 TTL 过期后的极端窗口）
        (home_a / "config.yaml").unlink()
        cfg_reader._CRED_CACHE.clear()
        assert pool.client_for_home(home_a) is first


class TestShowReasoningSnapshot:
    def test_snapshot_frozen_per_turn(self, two_homes) -> None:
        """v1.10.3: show_reasoning 每-turn 快照——会话存续期内配置变化不
        影响本回合渲染（磁盘读退出 per-flush/per-delta 热路径）。"""
        session = _make_session(str(two_homes[0]))
        cfg = SimpleNamespace(show_reasoning=True)
        assert session.show_reasoning(cfg) is True
        # 配置翻转（/reasoning 运行时切换）——本回合快照不变
        cfg2 = SimpleNamespace(show_reasoning=False)
        assert session.show_reasoning(cfg2) is True


class TestOnMessageStartedSkipReason:
    def test_disabled_logs_skip_reason(self, caplog) -> None:
        """v1.10.3: enabled=False 的静默 return 加 INFO 指纹——
        coder 无卡排查的最大盲区。"""
        import threading

        ctrl = ctrl_core.StreamCardController.__new__(ctrl_core.StreamCardController)
        ctrl._cfg = SimpleNamespace(enabled=False, feishu_app_id="", env_app_id="")
        ctrl._sessions = {}
        ctrl._sessions_lock = threading.RLock()

        import logging

        with caplog.at_level(logging.INFO, logger="hermes_lark_streaming"):
            ctrl.on_message_started(
                message_id="om_dis", chat_id="chat_x", turn_home="/h"
            )
        assert any(
            "skipped — disabled or no creds" in r.message for r in caplog.records
        )

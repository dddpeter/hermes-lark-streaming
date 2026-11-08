"""v1.10.1 回归测试 — 压缩场景封卡修复（用户报告：会话需要压缩时卡片不封口，
继续聊天显示「上下文压缩已推迟 — 摘要仍在生成中。本回合将不压缩继续」）。

hermes 侧考证（hermes-agent 镜像 gateway/run_turn_hygiene.py + run_turn_persistence.py）：

* turnhold_deferred 通知（locales/zh.yaml gateway.compress.turnhold_deferred）在
  会话卫生压缩超出 turn-hold 预算时发出，之后回合**继续不压缩运行**——通知本身
  不终止回合；
* 会话超大时回合以 provider 400/500 异常结束 → ``_handle_message_with_agent``
  的 ``except Exception`` 把异常转成**错误回复字符串**（``_hmwa_agent_error_reply``；
  上下文超限走 ``_context_overflow_reply()`` 确定性拒绝）——COMPLETE hook 从未
  触发；
* 插件外层包装器（_wrap_handle_message_with_agent）对该字符串只抑制文本不封卡
  → 流式卡永转圈、错误回复被吞、非终态会话泄漏 —— 本版主修复；
* 压缩结局 dict（compression_exhausted / compression_deferred，均 partial=True +
  error=<英文 guidance>）此前按 partial→中断封成「已停止」，且 guidance 当答案
  合并会走 MISMATCH 分支整段替换已流式真实答案 —— 改为错误态封卡 + 答案置空。
"""

# pyright: reportMissingImports=false, reportAttributeAccessIssue=false
# hermes_lark_streaming.* 由 tests/conftest.py 运行时注册包名（目录名带连字
# 符 ≠ 包名）；gateway.* 等宿主模块仅存在于 hermes 运行环境。

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from hermes_lark_streaming.controller import CardSession
from hermes_lark_streaming.controller.mixin import ABORTED, COMPLETED
from hermes_lark_streaming.patching import (
    _msg_ctx,
    _thread_local_ctx,
    _wrap_handle_message_with_agent,
    _wrap_run_agent,
)
from hermes_lark_streaming.patching.gateway import (
    _compression_error_prefix,
    _compression_turn_outcome,
)
from tests.test_controller import _setup_ctrl, _make_session


# ── helpers ──────────────────────────────────────────────────────────

_OVERFLOW_REPLY = "⚠ 上下文已超限。请使用 /compress 压缩或 /new 开启新会话。"


class _HookSpyCtrl:
    """get_controller 测试替身——记录 hooks 分发，不执行真实封卡链。"""

    enabled = True

    def __init__(self, session: CardSession | None = None):
        self.session = session
        self.started: list[dict] = []
        self.completed: list[dict] = []
        self.aborted: list[dict] = []

    def _sess_get(self, mid):
        if self.session and mid in {self.session.message_id, self.session.anchor_id}:
            return self.session
        return None

    def on_message_started(self, **k):
        self.started.append(k)

    def on_completed(self, **k):
        self.completed.append(k)
        return True

    def on_aborted(self, **k):
        self.aborted.append(k)


class _Ev:
    def __init__(self, message_id: str = "om_wrap_1"):
        self.message_id = message_id


class _Plat:
    value = "feishu"


class _Src:
    platform = _Plat()
    chat_id = "chat_wrap"
    thread_id = None


def _patch_ctrl(monkeypatch: pytest.MonkeyPatch, spy: _HookSpyCtrl) -> None:
    import hermes_lark_streaming.controller as ctrl_mod
    import hermes_lark_streaming.patching.hooks as hooks_mod

    monkeypatch.setattr(ctrl_mod, "get_controller", lambda: spy)
    monkeypatch.setattr(hooks_mod, "get_controller", lambda: spy)


# ── 压缩结局判定 / 双语前缀 ──────────────────────────────────────────


class TestCompressionOutcomeHelpers:
    def test_exhausted_and_deferred_detected(self) -> None:
        assert _compression_turn_outcome(
            {"compression_exhausted": True, "partial": True, "error": "x"}
        )
        assert _compression_turn_outcome(
            {"compression_deferred": True, "failed": False, "error": "y"}
        )

    def test_normal_and_non_dict_not_detected(self) -> None:
        assert not _compression_turn_outcome({"final_response": "hi"})
        assert not _compression_turn_outcome({"interrupted": True, "partial": True})
        assert not _compression_turn_outcome("error string")
        assert not _compression_turn_outcome(None)

    def test_error_prefix_bilingual(self) -> None:
        prefixed = _compression_error_prefix("Context compression timed out.")
        assert prefixed.startswith("⚠ 上下文已满")
        assert "Context Full" in prefixed
        assert "Context compression timed out." in prefixed

    def test_error_prefix_empty_passthrough(self) -> None:
        assert _compression_error_prefix("") == ""


# ── 内层 COMPLETE hook：压缩结局 → 错误态封卡 ────────────────────────


class TestInnerCompleteCompression:
    @pytest.mark.asyncio
    async def test_exhausted_result_seals_error_not_aborted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        spy = _HookSpyCtrl()
        _patch_ctrl(monkeypatch, spy)
        inner = _wrap_run_agent(AsyncMock(return_value=None))
        orig = AsyncMock(
            return_value={
                "final_response": "Context compression timed out without reducing.",
                "error": "Context compression timed out without reducing.",
                "partial": True,
                "failed": True,
                "completed": False,
                "compression_exhausted": True,
                "api_calls": 3,
            }
        )
        inner = _wrap_run_agent(orig)
        _msg_ctx.set(
            {
                "message_id": "om_in_1",
                "chat_id": "chat_wrap",
                "anchor_id": None,
                "event_message_id": "",
                "card_sent": False,
                "_msg_start_time": time.monotonic(),
            }
        )
        try:
            out = await inner(
                SimpleNamespace(),
                "msg",
                "",
                [],
                _Src(),
                "sess",
                session_key="k",
                run_generation=None,
                _interrupt_depth=0,
                event_message_id="om_in_1",
                channel_prompt=None,
            )
        finally:
            _msg_ctx.set(None)
            _thread_local_ctx.data = None

        # 回合结果标记 already_sent，网关侧不再发纯文本
        assert out.get("already_sent") is True
        assert len(spy.completed) == 1
        kw = spy.completed[0]
        assert kw["message_id"] == "om_in_1"
        # 压缩结局：不是用户中断（旧逻辑 partial→aborted 会封成「已停止」）
        assert kw["aborted"] is False
        # guidance 不能当答案合并（MISMATCH 替换已流式真实答案）
        assert kw["answer"] == ""
        # 错误面板带双语前缀
        assert kw["error_message"].startswith("⚠ 上下文已满")
        assert "Context compression timed out" in kw["error_message"]
        assert kw["compression_exhausted"] is True

    @pytest.mark.asyncio
    async def test_interrupted_result_still_aborted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """回归守护：非压缩结局的 partial/interrupted 仍按中断封卡。"""
        spy = _HookSpyCtrl()
        _patch_ctrl(monkeypatch, spy)
        orig = AsyncMock(
            return_value={
                "final_response": "part answer",
                "interrupted": True,
                "partial": True,
            }
        )
        inner = _wrap_run_agent(orig)
        _msg_ctx.set(
            {
                "message_id": "om_in_2",
                "chat_id": "chat_wrap",
                "anchor_id": None,
                "event_message_id": "",
                "card_sent": False,
                "_msg_start_time": time.monotonic(),
            }
        )
        try:
            await inner(
                SimpleNamespace(),
                "msg",
                "",
                [],
                _Src(),
                "sess",
                session_key="k",
                run_generation=None,
                _interrupt_depth=0,
                event_message_id="om_in_2",
                channel_prompt=None,
            )
        finally:
            _msg_ctx.set(None)
            _thread_local_ctx.data = None

        assert len(spy.completed) == 1
        kw = spy.completed[0]
        assert kw["aborted"] is True
        assert kw["answer"] == "part answer"


# ── 外层包装器：hermes 错误回复字符串 → 补封卡（主修复）─────────────


class TestOuterErrorReplySeal:
    def _make_streaming_session(self, msg_id: str) -> CardSession:
        session = _make_session(msg_id, linear=True)
        session.state = "streaming"
        session.card_msg_id = "card_msg_1"
        session.card_id = "card_1"
        return session

    @pytest.mark.asyncio
    async def test_error_reply_string_seals_unsealed_card(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """_run_agent 异常 → hermes 转错误回复字符串返回；流式卡此前只被
        抑制不封卡（永转圈）。现在必须以错误字符串封卡。"""
        session = self._make_streaming_session("om_out_1")
        ev = _Ev("om_out_1")
        spy = _HookSpyCtrl(session=session)
        _patch_ctrl(monkeypatch, spy)
        orig = AsyncMock(return_value=_OVERFLOW_REPLY)
        outer = _wrap_handle_message_with_agent(orig)

        ret = await outer(
            SimpleNamespace(_reply_anchor_for_event=lambda e: None),
            ev,
            _Src(),
        )

        assert ret is None  # 回复仍被抑制（错误已在卡上）
        assert len(spy.completed) == 1
        kw = spy.completed[0]
        assert kw["message_id"] == "om_out_1"
        assert kw["answer"] == ""
        assert kw["aborted"] is False
        assert _OVERFLOW_REPLY in kw["error_message"]
        assert not spy.aborted

    @pytest.mark.asyncio
    async def test_error_reply_with_terminal_card_suppressed_only(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """会话已终态（COMPLETE hook 已封过）→ 只抑制，不重复封卡。"""
        session = self._make_streaming_session("om_out_2")
        ev = _Ev("om_out_2")
        session.state = COMPLETED
        spy = _HookSpyCtrl(session=session)
        _patch_ctrl(monkeypatch, spy)
        orig = AsyncMock(return_value=_OVERFLOW_REPLY)
        outer = _wrap_handle_message_with_agent(orig)

        ret = await outer(
            SimpleNamespace(_reply_anchor_for_event=lambda e: None),
            ev,
            _Src(),
        )

        assert ret is None
        assert not spy.completed
        assert not spy.aborted

    @pytest.mark.asyncio
    async def test_error_reply_event_message_id_empty_falls_back_to_mid(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """回合在 _run_agent 之前失败（COMPLETE hook 没跑）→
        event_message_id 为空，回退 message_id 查会话。"""
        session = self._make_streaming_session("om_out_3")
        ev = _Ev("om_out_3")
        spy = _HookSpyCtrl(session=session)
        _patch_ctrl(monkeypatch, spy)
        orig = AsyncMock(return_value="❌ generic failure reply")
        outer = _wrap_handle_message_with_agent(orig)

        # event.message_id == "om_out_3"；外层 wrapper 的 msg_context 在
        # START 时建立，event_message_id 留空（_run_agent 未到）
        ret = await outer(
            SimpleNamespace(_reply_anchor_for_event=lambda e: None),
            ev,
            _Src(),
        )

        assert ret is None
        assert len(spy.completed) == 1
        assert spy.completed[0]["message_id"] == "om_out_3"

    @pytest.mark.asyncio
    async def test_error_reply_without_card_passthrough(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """无卡会话（卡片创建失败等）→ 错误回复原样放行（网关正常投递）。"""
        spy = _HookSpyCtrl(session=None)
        _patch_ctrl(monkeypatch, spy)
        orig = AsyncMock(return_value=_OVERFLOW_REPLY)
        outer = _wrap_handle_message_with_agent(orig)
        ev = _Ev("om_out_4")

        ret = await outer(
            SimpleNamespace(_reply_anchor_for_event=lambda e: None),
            ev,
            _Src(),
        )

        assert ret == _OVERFLOW_REPLY
        assert not spy.completed


# ── 控制器封卡语义：错误态 + 保留已流式答案 ──────────────────────────


class TestCompressionSealSemantics:
    @pytest.mark.asyncio
    async def test_completion_error_keeps_streamed_answer(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """压缩结局封卡：❌ 错误态（非「已停止」）、页脚「上下文已满」、
        已流式真实答案不被 guidance 替换。"""
        from hermes_lark_streaming.cardkit import (
            ANSWER_ELEMENT_ID,
            UNIFIED_PANEL_ELEMENT_ID,
        )

        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_cp1", linear=True)
        session.state = "streaming"
        session.card_id = "card_cp1"
        session._creation_stages.update({"answer", "panel", "hint_removed"})
        session.existing_elements.update({ANSWER_ELEMENT_ID, UNIFIED_PANEL_ELEMENT_ID})
        session.unified_state.on_answer_delta("真实答案正文")
        ctrl._sessions["msg_cp1"] = session

        ctrl.on_completed(
            message_id="msg_cp1",
            answer="",
            error_message=(
                "⚠ 上下文已满 / ⚠ Context Full\nContext compression timed out."
            ),
            compression_exhausted=True,
            aborted=False,
        )
        # 封卡协程被派发到 session 的 loop（测试内未运行）——直接 await 封卡
        # 路径（与 test_v1100_fixes 的直驱模式一致）
        await ctrl._do_linear_complete(session)

        # 搜集全部 batch_update actions
        actions: list[dict] = []
        for call in ctrl._client.cardkit_batch_update.call_args_list:
            actions.extend(call.args[1])

        # 已流式答案保留（不被 guidance 的 MISMATCH 替换、也不被清空）
        answer_contents = [
            a["params"]["partial_element"]["content"]
            for a in actions
            if a.get("action") == "partial_update_element"
            and a["params"].get("element_id") == ANSWER_ELEMENT_ID
        ]
        assert answer_contents, "answer element never updated"
        assert "真实答案正文" in answer_contents[0]
        assert "Context compression timed out" not in answer_contents[0]

        # 错误面板上卡（is_error=True）
        all_text = str(actions)
        assert "Context compression timed out" in all_text

        # 页脚「上下文已满」标记（compression_exhausted 字段）
        assert "上下文已满" in all_text

        # 终态：COMPLETED（非 ABORTED——不是用户停止）
        assert session.state == COMPLETED
        assert session.state != ABORTED

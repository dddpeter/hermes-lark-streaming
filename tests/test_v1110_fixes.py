"""v1.11.0 修复测试 — 200850 流式会话超时自愈（docs/UPSTREAM-ADOPTION.md P0）.

上游 v2.0.36 移植（v2.0.32 落地）：官方 CardKit 流式会话寿命 ~600s，到期后
stream_element 必返 200850。三层防线：
  第一层 _renew_stream_if_due — 540s 主动续期（settings streaming_mode=true）
  第二层 call_with_seq_recovery — 200850 重开+重发 / 300317 seq+1 重发
  （第三层降级收口为本地 v1.10.x 封卡兜底链既有能力，不在本文件范围）

复用 tests/test_controller 的 _setup_ctrl/_make_session harness。
注：本机 macOS Python 的 time.monotonic() 进程起点为 0（now-age 会得负值，
撞 ``stream_session_at <= 0`` 的未打点哨兵），故全部用例 monkeypatch
monotonic 到固定大值，测试不依赖单调钟原点。
"""

# pyright: reportMissingImports=false, reportAttributeAccessIssue=false
# hermes_lark_streaming.* 由 tests/conftest.py 运行时注册包名（目录名带连字
# 符 ≠ 包名），gateway.* 等宿主模块仅存在于 hermes 运行环境——静态解析必然
# 误报，文件级豁免（运行时零影响）。

from __future__ import annotations

import time
from unittest.mock import AsyncMock

import pytest
from hermes_lark_streaming.controller.linear_mixin import _STREAM_RENEW_AGE_SEC
from hermes_lark_streaming.controller.mixin import STREAMING
from hermes_lark_streaming.feishu.client import FeishuAPIError
from tests.test_controller import _make_session, _setup_ctrl

_NOW = 100000.0  # monkeypatch 后的固定 monotonic 值（远大于 600s 会话寿命）


def _fix_clock(monkeypatch) -> None:
    monkeypatch.setattr(time, "monotonic", lambda: _NOW)


def _stream_session(session, *, age_sec: float = 0.0) -> None:
    """把 session 置于 Phase 3 流式路径（answer 元素已建、待写文本）.

    age_sec：流式会话已历时（写入 stream_session_at 基线）。
    """
    session.state = STREAMING
    session.card_id = "card_v1110"
    session._creation_stages.add("answer")
    session.unified_state.on_answer_delta("HEAD")
    session.stream_session_at = _NOW - age_sec


class TestStreamTimeoutSelfHeal:
    """200850 → seq+1 重开流式 → seq+1 重发同内容（官方恢复动作）."""

    @pytest.mark.asyncio
    async def test_200850_reopens_streaming_and_resends(self, monkeypatch) -> None:
        _fix_clock(monkeypatch)
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_t1", linear=True)
        _stream_session(session)  # 会话新鲜 → 第一层续期不触发

        err = FeishuAPIError("card streaming timeout", code=200850)
        ctrl._client.cardkit_stream_element = AsyncMock(side_effect=[err, None])
        ctrl._client.cardkit_reopen_streaming = AsyncMock()

        await ctrl._do_unified_flush(session)

        # 200850 → seq+1 重开（settings streaming_mode=true）→ seq+1 重发
        assert ctrl._client.cardkit_reopen_streaming.await_count == 1
        assert ctrl._client.cardkit_stream_element.await_count == 2
        # session.sequence 起始 1：flush +1=2 首发；自愈 +1=3 重开；+1=4 重发
        assert ctrl._client.cardkit_stream_element.call_args_list[0].kwargs[
            "sequence"
        ] == 2
        assert ctrl._client.cardkit_reopen_streaming.call_args.kwargs[
            "sequence"
        ] == 3
        assert ctrl._client.cardkit_stream_element.call_args_list[1].kwargs[
            "sequence"
        ] == 4
        # 重发成功 → 水位守卫清 dirty（无新 delta）；重开刷新续期基线
        assert not session.unified_state.answer_dirty
        assert session.stream_session_at == _NOW

    @pytest.mark.asyncio
    async def test_300317_conflict_self_heals_by_seq_bump(self, monkeypatch) -> None:
        _fix_clock(monkeypatch)
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_t2", linear=True)
        _stream_session(session)

        err = FeishuAPIError("sequence compare failed", code=300317)
        ctrl._client.cardkit_stream_element = AsyncMock(side_effect=[err, None])
        ctrl._client.cardkit_reopen_streaming = AsyncMock()

        await ctrl._do_unified_flush(session)

        # 300317 → 仅 seq+1 重发，不重开流式
        assert ctrl._client.cardkit_reopen_streaming.await_count == 0
        assert ctrl._client.cardkit_stream_element.await_count == 2
        assert ctrl._client.cardkit_stream_element.call_args_list[0].kwargs[
            "sequence"
        ] == 2
        assert ctrl._client.cardkit_stream_element.call_args_list[1].kwargs[
            "sequence"
        ] == 3
        assert not session.unified_state.answer_dirty

    @pytest.mark.asyncio
    async def test_three_consecutive_conflicts_give_up(self, monkeypatch) -> None:
        """连续 3 次冲突 → 上抛（上限外交降级红线兜底，消息不丢）."""
        _fix_clock(monkeypatch)
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_t3", linear=True)
        _stream_session(session)

        err = FeishuAPIError("sequence compare failed", code=300317)
        ctrl._client.cardkit_stream_element = AsyncMock(side_effect=[err, err, err])
        ctrl._client.cardkit_reopen_streaming = AsyncMock()

        # flush 的 except FeishuAPIError 分支吞掉非 300309 码（现状语义），
        # 但重试必须已耗尽：3 次发送、dirty 不清（下轮 flush 重试）。
        await ctrl._do_unified_flush(session)

        assert ctrl._client.cardkit_stream_element.await_count == 3
        assert ctrl._client.cardkit_reopen_streaming.await_count == 0
        assert session.unified_state.answer_dirty, (
            "上限外上抛后 dirty 被误清 — 增量将困住到封卡"
        )

    @pytest.mark.asyncio
    async def test_non_recoverable_error_not_retried(self, monkeypatch) -> None:
        """非自愈范围错误（如 300315 schema）原样上抛，不消耗 seq 自愈."""
        _fix_clock(monkeypatch)
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_t4", linear=True)
        _stream_session(session)

        err = FeishuAPIError("unknown property", code=300315)
        ctrl._client.cardkit_stream_element = AsyncMock(side_effect=err)
        ctrl._client.cardkit_reopen_streaming = AsyncMock()

        await ctrl._do_unified_flush(session)

        assert ctrl._client.cardkit_stream_element.await_count == 1
        assert ctrl._client.cardkit_reopen_streaming.await_count == 0


class TestStreamRenewProactive:
    """第一层防线 — 600s 墙前 60s（540s）主动续期."""

    @pytest.mark.asyncio
    async def test_renew_at_541s(self, monkeypatch) -> None:
        _fix_clock(monkeypatch)
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_r1", linear=True)
        session.card_id = "card_r1"
        session.stream_session_at = _NOW - (_STREAM_RENEW_AGE_SEC + 1.0)
        ctrl._client.cardkit_reopen_streaming = AsyncMock()

        await ctrl._renew_stream_if_due(session)

        assert ctrl._client.cardkit_reopen_streaming.await_count == 1
        # session.sequence 1 → +1 = 2 续期
        assert ctrl._client.cardkit_reopen_streaming.call_args.kwargs[
            "sequence"
        ] == 2
        # 续期成功刷新计时基线
        assert session.stream_session_at == _NOW

    @pytest.mark.asyncio
    async def test_no_renew_at_539s(self, monkeypatch) -> None:
        _fix_clock(monkeypatch)
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_r2", linear=True)
        session.card_id = "card_r2"
        session.stream_session_at = _NOW - (_STREAM_RENEW_AGE_SEC - 1.0)
        ctrl._client.cardkit_reopen_streaming = AsyncMock()

        await ctrl._renew_stream_if_due(session)

        assert ctrl._client.cardkit_reopen_streaming.await_count == 0
        assert session.sequence == 1

    @pytest.mark.asyncio
    async def test_no_renew_when_streaming_closed(self, monkeypatch) -> None:
        """TTL/竞态已关流式的卡不复活（300309 语义不受续期干扰）."""
        _fix_clock(monkeypatch)
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_r3", linear=True)
        session.card_id = "card_r3"
        session._streaming_closed = True
        session.stream_session_at = _NOW - (_STREAM_RENEW_AGE_SEC + 60.0)
        ctrl._client.cardkit_reopen_streaming = AsyncMock()

        await ctrl._renew_stream_if_due(session)

        assert ctrl._client.cardkit_reopen_streaming.await_count == 0

    @pytest.mark.asyncio
    async def test_renew_failure_refreshes_baseline_no_raise(self, monkeypatch) -> None:
        """预防性续期失败不阻断本帧：基线仍刷新（退避语义），不抛."""
        _fix_clock(monkeypatch)
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_r4", linear=True)
        session.card_id = "card_r4"
        session.stream_session_at = _NOW - (_STREAM_RENEW_AGE_SEC + 1.0)
        stale = session.stream_session_at
        ctrl._client.cardkit_reopen_streaming = AsyncMock(
            side_effect=FeishuAPIError("boom", code=99991400)
        )

        await ctrl._renew_stream_if_due(session)  # 不应抛出

        assert ctrl._client.cardkit_reopen_streaming.await_count == 1
        assert session.stream_session_at == _NOW > stale, (
            "续期失败未刷新基线 — 将每帧重复尝试撞限频"
        )

    @pytest.mark.asyncio
    async def test_no_renew_when_unstamped(self, monkeypatch) -> None:
        """stream_session_at <= 0（未建卡/未打点）不续期 — 哨兵语义."""
        _fix_clock(monkeypatch)
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_r5", linear=True)
        session.card_id = "card_r5"
        session.stream_session_at = 0.0
        ctrl._client.cardkit_reopen_streaming = AsyncMock()

        await ctrl._renew_stream_if_due(session)

        assert ctrl._client.cardkit_reopen_streaming.await_count == 0

"""v1.10.0 回归测试 — 封卡自愈（seal watchdog + 链式拆卡）.

背景：「任务已完成但流式卡永不封卡」。封卡链路（on_completed → COMPLETING
→ fire-and-forget _do_linear_complete_with_fallback → _preservative_seal →
close_streaming）存在四个互为放大的缺口：

1. fire-and-forget 的 create_task 路径异常零日志（done 回调只 discard），
   封卡协程死亡 = 会话永卡 COMPLETING；
2. 所有兜底（on_completed 幂等短路 / gateway stuck 检查 / _prune_stale_
   sessions）都显式跳过 COMPLETING，且无周期 watchdog——丢失的封卡无人补位；
3. _preservative_seal 失败即终态化，服务端 streaming 从未 close——卡片永
   转圈（文本兜底还会重复发答案）；
4. COMPLETE hook 不触发的形态（result 非 dict/str 等）→ 会话停在 STREAMING。

修复：控制器层周期 watchdog 三规则（COMPLETING 重派 / 超长静默拆卡 / 拆卡
终态收尾）+ 封卡串行锁与 _seal_done 防重入 + seal 失败最小化兜底关卡 +
fire-and-forget 异常日志。拆卡语义：部分封卡（内容保留）后保持非终态武装
续写，后续输出经既有 continuation 机制开新卡（链式，上限 20 深度）。
"""

# pyright: reportMissingImports=false, reportAttributeAccessIssue=false
# hermes_lark_streaming.* 由 tests/conftest.py 运行时注册包名（目录名带连字
# 符 ≠ 包名），gateway.* 等宿主模块仅存在于 hermes 运行环境——静态解析必然
# 误报，文件级豁免（运行时零影响）。

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from hermes_lark_streaming.controller import CardSession, StreamCardController
from hermes_lark_streaming.controller.mixin import (
    ABORTED,
    COMPLETED,
    COMPLETING,
    CREATION_FAILED,
    STREAMING,
)
from hermes_lark_streaming.feishu import FeishuAPIError
from tests.test_controller import (
    _enable,
    _loops_to_cleanup,
    _make_session,
    _setup_ctrl,
)


# ── helpers ──────────────────────────────────────────────────────────


def _make_loop() -> asyncio.AbstractEventLoop:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _loops_to_cleanup.append(loop)
    return loop


def _enable_with_thresholds(
    ctrl: StreamCardController, *, seal: int = 120, stuck: int = 1800
) -> None:
    _enable(ctrl, linear=True)
    ctrl._cfg._raw["hermes_lark_streaming"]["seal_watchdog_sec"] = seal
    ctrl._cfg._raw["hermes_lark_streaming"]["stuck_card_timeout_sec"] = stuck


# ── 规则 1：COMPLETING 重派（封卡派发丢失自愈）──────────────────────


class TestWatchdogCompletingReseal:
    def test_stale_dispatch_triggers_reseal(self) -> None:
        ctrl = _setup_ctrl(linear=True)
        _enable_with_thresholds(ctrl, seal=60)
        session = _make_session("msg_wd1", linear=True)
        session.state = COMPLETING
        session._seal_dispatched_at = time.monotonic() - 999
        ctrl._sessions["msg_wd1"] = session
        dispatched: list[CardSession] = []
        ctrl._dispatch_watchdog_seal = lambda s: dispatched.append(s)  # type: ignore[method-assign]

        ctrl._seal_watchdog_pass()

        assert dispatched == [session]

    def test_fresh_dispatch_not_resealed(self) -> None:
        ctrl = _setup_ctrl(linear=True)
        _enable_with_thresholds(ctrl, seal=3600)
        session = _make_session("msg_wd2", linear=True)
        session.state = COMPLETING
        session._seal_dispatched_at = time.monotonic() - 10
        ctrl._sessions["msg_wd2"] = session
        dispatched: list[CardSession] = []
        ctrl._dispatch_watchdog_seal = lambda s: dispatched.append(s)  # type: ignore[method-assign]

        ctrl._seal_watchdog_pass()

        assert dispatched == []

    def test_zero_dispatch_stamp_not_resealed(self) -> None:
        """从未派发过封卡（_seal_dispatched_at=0）不触发规则 1。"""
        ctrl = _setup_ctrl(linear=True)
        _enable_with_thresholds(ctrl, seal=30)
        session = _make_session("msg_wd3", linear=True)
        session.state = COMPLETING
        session._seal_dispatched_at = 0.0
        ctrl._sessions["msg_wd3"] = session
        dispatched: list[CardSession] = []
        ctrl._dispatch_watchdog_seal = lambda s: dispatched.append(s)  # type: ignore[method-assign]

        ctrl._seal_watchdog_pass()

        assert dispatched == []

    def test_seal_lock_held_not_resealed(self) -> None:
        """首封在飞（持锁）→ 不重派（等锁串行化，防双 footer）。"""
        ctrl = _setup_ctrl(linear=True)
        _enable_with_thresholds(ctrl, seal=30)
        session = _make_session("msg_wd4", linear=True)
        session.state = COMPLETING
        session._seal_dispatched_at = time.monotonic() - 999
        ctrl._sessions["msg_wd4"] = session
        dispatched: list[CardSession] = []
        ctrl._dispatch_watchdog_seal = lambda s: dispatched.append(s)  # type: ignore[method-assign]

        async def _hold_and_pass() -> None:
            async with session._seal_lock:
                ctrl._seal_watchdog_pass()

        _make_loop().run_until_complete(_hold_and_pass())

        assert dispatched == []

    def test_disabled_seal_watchdog(self) -> None:
        ctrl = _setup_ctrl(linear=True)
        _enable_with_thresholds(ctrl, seal=0)
        session = _make_session("msg_wd5", linear=True)
        session.state = COMPLETING
        session._seal_dispatched_at = time.monotonic() - 99999
        ctrl._sessions["msg_wd5"] = session
        dispatched: list[CardSession] = []
        ctrl._dispatch_watchdog_seal = lambda s: dispatched.append(s)  # type: ignore[method-assign]

        ctrl._seal_watchdog_pass()

        assert dispatched == []


# ── 规则 2：超长静默拆卡 ─────────────────────────────────────────────


class TestWatchdogSplitStuck:
    def test_stale_streaming_triggers_split(self) -> None:
        ctrl = _setup_ctrl(linear=True)
        _enable_with_thresholds(ctrl, stuck=600)
        session = _make_session("msg_sp1", linear=True)
        session.state = STREAMING
        session.card_id = "card_sp1"
        session._last_event_at = time.monotonic() - 9999
        ctrl._sessions["msg_sp1"] = session
        dispatched: list[CardSession] = []
        ctrl._dispatch_watchdog_split = lambda s: dispatched.append(s)  # type: ignore[method-assign]

        ctrl._seal_watchdog_pass()

        assert dispatched == [session]

    def test_fresh_streaming_not_split(self) -> None:
        ctrl = _setup_ctrl(linear=True)
        _enable_with_thresholds(ctrl, stuck=600)
        session = _make_session("msg_sp2", linear=True)
        session.state = STREAMING
        session.card_id = "card_sp2"
        ctrl._sessions["msg_sp2"] = session
        dispatched: list[CardSession] = []
        ctrl._dispatch_watchdog_split = lambda s: dispatched.append(s)  # type: ignore[method-assign]

        ctrl._seal_watchdog_pass()

        assert dispatched == []

    def test_disabled_stuck_watchdog(self) -> None:
        ctrl = _setup_ctrl(linear=True)
        _enable_with_thresholds(ctrl, stuck=0)
        session = _make_session("msg_sp3", linear=True)
        session.state = STREAMING
        session.card_id = "card_sp3"
        session._last_event_at = time.monotonic() - 999999
        ctrl._sessions["msg_sp3"] = session
        dispatched: list[CardSession] = []
        ctrl._dispatch_watchdog_split = lambda s: dispatched.append(s)  # type: ignore[method-assign]

        ctrl._seal_watchdog_pass()

        assert dispatched == []

    def test_cardless_stuck_terminalized(self) -> None:
        """卡从未建成（CREATING 卡死）——无从拆卡，直接终态回收。"""
        ctrl = _setup_ctrl(linear=True)
        _enable_with_thresholds(ctrl, stuck=600)
        session = _make_session("msg_sp4", linear=True)
        session.state = STREAMING
        session.card_id = None
        session._last_event_at = time.monotonic() - 9999
        ctrl._sessions["msg_sp4"] = session

        ctrl._seal_watchdog_pass()

        assert session.state == CREATION_FAILED
        assert session.is_terminal_phase
        assert session.terminal_source == "watchdog_creating_stuck"


# ── 规则 3：拆卡终态收尾 ─────────────────────────────────────────────


class TestWatchdogFinalizeSplit:
    def test_split_session_finalized_after_double_window(self) -> None:
        ctrl = _setup_ctrl(linear=True)
        _enable_with_thresholds(ctrl, stuck=600)
        session = _make_session("msg_fz1", linear=True)
        session.state = STREAMING
        session.card_id = "card_fz1"
        session._watchdog_split = True
        session._seal_done = True
        session._chain_depth = 2
        session._last_event_at = time.monotonic() - 99999  # > 2 × 600
        ctrl._sessions["msg_fz1"] = session

        ctrl._seal_watchdog_pass()

        assert session.state == COMPLETED
        assert session.is_terminal_phase
        assert session.terminal_source == "watchdog_finalize_split"
        assert session.unified_state is None  # 重数据已释放

    def test_split_session_within_window_not_finalized(self) -> None:
        """一个窗口内仍有活动可能——续写链还开着，不终态。"""
        ctrl = _setup_ctrl(linear=True)
        _enable_with_thresholds(ctrl, stuck=600)
        session = _make_session("msg_fz2", linear=True)
        session.state = STREAMING
        session.card_id = "card_fz2"
        session._watchdog_split = True
        session._seal_done = True
        session._last_event_at = time.monotonic() - 700  # < 2 × 600
        ctrl._sessions["msg_fz2"] = session

        ctrl._seal_watchdog_pass()

        assert not session.is_terminal_phase


# ── 拆卡执行体：_watchdog_split_seal ─────────────────────────────────


class TestWatchdogSplitSeal:
    @pytest.mark.asyncio
    async def test_success_arms_continuation(self) -> None:
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_ss1", linear=True)
        session.state = STREAMING
        session.card_id = "card_ss1"
        ctrl._sessions["msg_ss1"] = session
        ctrl._preservative_seal = AsyncMock(return_value=True)  # type: ignore[method-assign]

        await ctrl._watchdog_split_seal(session)

        ctrl._preservative_seal.assert_awaited_once()
        kwargs = ctrl._preservative_seal.await_args.kwargs
        assert kwargs["partial"] is True
        assert kwargs["partial_note"] is not None  # 拆卡指示文案
        assert session._watchdog_split is True
        assert session._seal_done is True
        # 关键：拆卡后保持非终态（续写链的前提）
        assert not session.is_terminal_phase

    @pytest.mark.asyncio
    async def test_failure_not_armed(self) -> None:
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_ss2", linear=True)
        session.state = STREAMING
        session.card_id = "card_ss2"
        ctrl._sessions["msg_ss2"] = session
        ctrl._preservative_seal = AsyncMock(return_value=False)  # type: ignore[method-assign]

        await ctrl._watchdog_split_seal(session)

        # 失败且流式未关 → 不武装，下一轮 watchdog 重试
        assert session._watchdog_split is False
        assert session._seal_done is False

    @pytest.mark.asyncio
    async def test_streaming_closed_counts_as_sealed(self) -> None:
        """seal 返回 False 但服务端流式已关（300309 race）——卡已脱转圈态，
        视为拆卡成功武装续写。"""
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_ss3", linear=True)
        session.state = STREAMING
        session.card_id = "card_ss3"
        session._streaming_closed = True
        ctrl._sessions["msg_ss3"] = session
        ctrl._preservative_seal = AsyncMock(return_value=False)  # type: ignore[method-assign]

        await ctrl._watchdog_split_seal(session)

        assert session._watchdog_split is True
        assert session._seal_done is True

    @pytest.mark.asyncio
    async def test_already_sealed_noop(self) -> None:
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_ss4", linear=True)
        session.state = STREAMING
        session.card_id = "card_ss4"
        session._seal_done = True
        ctrl._sessions["msg_ss4"] = session
        ctrl._preservative_seal = AsyncMock()  # type: ignore[method-assign]

        await ctrl._watchdog_split_seal(session)

        ctrl._preservative_seal.assert_not_awaited()


# ── 链式拆卡路由：_maybe_reactivate_for_continuation ─────────────────


class TestChainedContinuation:
    def _split_session(self, msg_id: str) -> CardSession:
        session = _make_session(msg_id, linear=True)
        session.state = STREAMING
        session._watchdog_split = True
        session._seal_done = True
        session._streaming_closed = True
        return session

    def test_split_continuation_session_can_chain(self) -> None:
        """cont-1 被再次拆卡后仍可续链（旧逻辑 _is_continuation 直接拒绝）。"""
        ctrl = _setup_ctrl(linear=True)
        cont1 = self._split_session("om_x-cont-1")
        cont1.anchor_id = "om_x"
        cont1._chain_depth = 1
        ctrl._sessions["om_x-cont-1"] = cont1
        cont2 = _make_session("om_x-cont-2", linear=True)
        ctrl._reactivate_session_for_continuation = (  # type: ignore[method-assign]
            lambda stale: cont2
        )

        result = ctrl._maybe_reactivate_for_continuation("om_x-cont-1")

        assert result == "om_x-cont-2"

    def test_chain_depth_cap(self) -> None:
        ctrl = _setup_ctrl(linear=True)
        cont_max = self._split_session("om_x-cont-20")
        cont_max.anchor_id = "om_x"
        cont_max._chain_depth = 20
        ctrl._sessions["om_x-cont-20"] = cont_max

        def _fail(stale: CardSession) -> CardSession:
            pytest.fail("should not reactivate past cap")

        ctrl._reactivate_session_for_continuation = _fail  # type: ignore[method-assign]

        assert ctrl._maybe_reactivate_for_continuation("om_x-cont-20") is None

    def test_existing_map_target_healthy_passthrough(self) -> None:
        """既有映射且目标健康 → 直接返回（delegate_task 原行为不变）。"""
        ctrl = _setup_ctrl(linear=True)
        cont1 = _make_session("om_x-cont-1", linear=True)
        cont1.state = STREAMING
        ctrl._sessions["om_x-cont-1"] = cont1
        with ctrl._continuation_map_lock:
            ctrl._continuation_map["om_x"] = "om_x-cont-1"

        def _fail(stale: CardSession) -> CardSession:
            pytest.fail("healthy target must not re-chain")

        ctrl._reactivate_session_for_continuation = _fail  # type: ignore[method-assign]

        assert ctrl._maybe_reactivate_for_continuation("om_x") == "om_x-cont-1"

    def test_existing_map_split_target_rechains_and_repoints(self) -> None:
        """映射目标被再次拆卡 → 从目标续链，且所有指向目标的映射键改指新卡
        （on_completed 的 _pop_continuation_id 必须拿到链尾）。"""
        ctrl = _setup_ctrl(linear=True)
        cont1 = self._split_session("om_x-cont-1")
        cont1.anchor_id = "om_x"
        ctrl._sessions["om_x-cont-1"] = cont1
        with ctrl._continuation_map_lock:
            ctrl._continuation_map["om_x"] = "om_x-cont-1"
        cont2 = _make_session("om_x-cont-2", linear=True)
        ctrl._reactivate_session_for_continuation = (  # type: ignore[method-assign]
            lambda stale: cont2
        )

        result = ctrl._maybe_reactivate_for_continuation("om_x")

        assert result == "om_x-cont-2"
        with ctrl._continuation_map_lock:
            # 原始键与链中键都必须指向链尾
            assert ctrl._continuation_map["om_x"] == "om_x-cont-2"
            assert ctrl._continuation_map["om_x-cont-1"] == "om_x-cont-2"

    def test_non_split_guards_unchanged(self) -> None:
        """未拆卡的原会话：_streaming_closed / count 守卫行为不变。"""
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("om_y", linear=True)
        session.state = STREAMING
        # 流式健康 → 不重激活
        assert ctrl._maybe_reactivate_for_continuation("om_y") is None

    def test_non_split_count_guard_holds(self) -> None:
        """count>=1 且非拆卡 → 不再重激活（旧守卫保留，防 300309 循环）。"""
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("om_z", linear=True)
        session.state = STREAMING
        session._streaming_closed = True
        session._continuation_reactivation_count = 1
        ctrl._sessions["om_z"] = session

        def _fail(stale: CardSession) -> CardSession:
            pytest.fail("count guard must hold for non-split")

        ctrl._reactivate_session_for_continuation = _fail  # type: ignore[method-assign]

        assert ctrl._maybe_reactivate_for_continuation("om_z") is None

    def test_reactivation_seq_follows_chain_depth(self) -> None:
        """v1.10.0 关键修复：续写卡再续写的 seq 按链深计算——cont-1 续写得
        cont-2（旧 count+1 会生成与自身相同的 cont-1，撞冲突守卫链必断）。"""
        ctrl = _setup_ctrl(linear=True)
        cont1 = self._split_session("om_x-cont-1")
        cont1.anchor_id = "om_x"
        cont1.chat_id = "chat_456"
        cont1._chain_depth = 1
        ctrl._sessions["om_x-cont-1"] = cont1
        created: list[str] = []

        real_reactivate = StreamCardController._reactivate_session_for_continuation

        def _spy(self: StreamCardController, stale: CardSession) -> CardSession | None:
            # 只验证 seq 推导，不真建卡：拷贝核心 id 生成逻辑
            seq = stale._chain_depth + 1
            created.append(f"{stale.anchor_id}-cont-{seq}")
            return None  # 中止后续派发

        StreamCardController._reactivate_session_for_continuation = (  # type: ignore[method-assign]
            _spy
        )
        try:
            ctrl._maybe_reactivate_for_continuation("om_x-cont-1")
        finally:
            StreamCardController._reactivate_session_for_continuation = (  # type: ignore[method-assign]
                real_reactivate
            )

        assert created == ["om_x-cont-2"]

    @pytest.mark.asyncio
    async def test_real_reactivation_chain_id(self) -> None:
        """评审 Important#3：驱动**真实** _reactivate_session_for_continuation
        守护 id 生成——上面 spy 测试复制了实现逻辑，回归到 count+1 仍会假绿。"""
        ctrl = _setup_ctrl(linear=True)
        cont1 = self._split_session("om_x-cont-1")
        cont1.anchor_id = "om_x"
        cont1.chat_id = "chat_456"
        cont1._chain_depth = 1
        ctrl._sessions["om_x-cont-1"] = cont1

        new_session = ctrl._reactivate_session_for_continuation(cont1)

        assert new_session is not None
        assert new_session.message_id == "om_x-cont-2"
        assert new_session._chain_depth == 2
        # 清理 fire-and-forget 任务（建卡 + 拆卡会话收尾），避免 loop 关闭告警
        pending = [t for t in ctrl._pending_tasks if not t.done()]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    def test_chain_depth_cap_on_mapped_path(self) -> None:
        """评审 Important#2：映射主路径同样受链深上限约束——映射目标是
        主路径（事件以原始 message_id 到达），上限只查直达分支形同虚设。"""
        ctrl = _setup_ctrl(linear=True)
        cont_max = self._split_session("om_x-cont-20")
        cont_max.anchor_id = "om_x"
        cont_max._chain_depth = 20
        ctrl._sessions["om_x-cont-20"] = cont_max
        with ctrl._continuation_map_lock:
            ctrl._continuation_map["om_x"] = "om_x-cont-20"

        def _fail(stale: CardSession) -> CardSession:
            pytest.fail("mapped path must respect chain cap")

        ctrl._reactivate_session_for_continuation = _fail  # type: ignore[method-assign]

        assert ctrl._maybe_reactivate_for_continuation("om_x") is None


# ── _do_linear_complete 防重入 + 最小化兜底关卡 ──────────────────────


class TestSealReentryAndMinimalClose:
    @pytest.mark.asyncio
    async def test_seal_done_reentry_no_api(self) -> None:
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_re1", linear=True)
        session.state = STREAMING
        session.card_id = "card_re1"
        session._seal_done = True
        ctrl._sessions["msg_re1"] = session

        result = await ctrl._do_linear_complete(session)

        assert result is True
        # 重入不做任何卡操作
        ctrl._client.cardkit_close_streaming.assert_not_called()
        ctrl._client.cardkit_batch_update.assert_not_called()
        # 终态化（防 watchdog 反复重派）+ 重数据释放
        assert session.is_terminal_phase
        assert session.unified_state is None

    @pytest.mark.asyncio
    async def test_reentry_split_state_alive_sends_tail(self) -> None:
        """拆卡会话（unified_state 保留）拆卡后答案又增长了 → 重入只补发
        超出拆卡时已上卡长度的尾段。"""
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_re2", linear=True)
        session.state = COMPLETING
        session.card_id = "card_re2"
        session._seal_done = True
        session._watchdog_split = True
        session.unified_state.on_answer_delta("拆卡前已上卡的内容")
        session._split_sealed_answer_len = len("拆卡前已上卡的内容")
        session.unified_state.on_answer_delta("，拆卡后新到的尾段")
        ctrl._sessions["msg_re2"] = session
        ctrl._send_text_fallback = AsyncMock()  # type: ignore[method-assign]

        result = await ctrl._do_linear_complete(session)

        assert result is True
        ctrl._send_text_fallback.assert_awaited_once()
        assert ctrl._send_text_fallback.await_args.kwargs["fallback_text"] == (
            "，拆卡后新到的尾段"
        )

    @pytest.mark.asyncio
    async def test_reentry_split_state_released_sends_full(self) -> None:
        """拆卡会话状态已释放（异常路径）→ on_completed 投递在 text 里的
        完整答案整段文本兜底。"""
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_re3", linear=True)
        session.state = COMPLETING
        session.card_id = "card_re3"
        session._seal_done = True
        session._watchdog_split = True
        session.unified_state = None
        session.text.on_deliver("完整答案正文")
        ctrl._sessions["msg_re3"] = session
        ctrl._send_text_fallback = AsyncMock()  # type: ignore[method-assign]

        result = await ctrl._do_linear_complete(session)

        assert result is True
        ctrl._send_text_fallback.assert_awaited_once()
        assert ctrl._send_text_fallback.await_args.kwargs["fallback_text"] == (
            "完整答案正文"
        )

    @pytest.mark.asyncio
    async def test_reentry_split_no_new_tail_no_fallback(self) -> None:
        """拆卡后答案没有增长（正常链式续写路径的重入）→ 不发兜底。"""
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_re4", linear=True)
        session.state = COMPLETING
        session.card_id = "card_re4"
        session._seal_done = True
        session._watchdog_split = True
        session.unified_state.on_answer_delta("已上卡内容")
        session._split_sealed_answer_len = len("已上卡内容")
        ctrl._sessions["msg_re4"] = session
        ctrl._send_text_fallback = AsyncMock()  # type: ignore[method-assign]

        result = await ctrl._do_linear_complete(session)

        assert result is True
        ctrl._send_text_fallback.assert_not_awaited()
        assert session.is_terminal_phase
        assert session.unified_state is None

    @pytest.mark.asyncio
    async def test_reentry_honors_abort(self) -> None:
        """评审 Minor#8：拆卡后用户 /stop 的重入收尾终态为 ABORTED
        （页脚"已停止"），而非 NORMAL/COMPLETED。"""
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_re5", linear=True)
        session.state = COMPLETING
        session.card_id = "card_re5"
        session._seal_done = True
        session._watchdog_split = True
        session._was_aborted = True
        ctrl._sessions["msg_re5"] = session
        ctrl._send_text_fallback = AsyncMock()  # type: ignore[method-assign]

        result = await ctrl._do_linear_complete(session)

        assert result is True
        assert session.state == ABORTED
        assert session.terminal_reason == "abort"
        ctrl._send_text_fallback.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_aborted_terminal_session_still_seals(self) -> None:
        """守护：_finalize_abort 先置 ABORTED 终态再派发封卡——防重入判据
        必须用 _seal_done 而非 is_terminal_phase，否则 abort 卡永不能封。"""
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_ab1", linear=True)
        session.state = STREAMING
        session.card_id = "card_ab1"
        ctrl._sessions["msg_ab1"] = session
        session.unified_state.on_answer_delta("部分答案")

        # 走真实 abort 语义：置 ABORTED 终态后直接派发封卡
        session._was_aborted = True
        session.state = ABORTED
        session.flush.mark_completed()
        assert await ctrl._do_linear_complete(session) is True
        ctrl._client.cardkit_close_streaming.assert_called()
        assert session.state == ABORTED  # abort 语义保留
        assert session._seal_done is True

    @pytest.mark.asyncio
    async def test_minimal_close_on_seal_failure(self) -> None:
        """seal 的 close_streaming 永久失败 → 单独补一次裸 close_streaming，
        卡脱离转圈态（缺口 3 自愈）。"""
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_mc1", linear=True)
        session.state = STREAMING
        session.card_id = "card_mc1"
        ctrl._sessions["msg_mc1"] = session
        # 第 1 次（seal 主路径内）永久失败；第 2 次（最小化兜底）成功
        ctrl._client.cardkit_close_streaming = AsyncMock(
            side_effect=[FeishuAPIError("close race", code=99999), None]
        )

        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await ctrl._do_linear_complete(session)

        assert result is False  # seal 整体仍算失败
        assert session.state == CREATION_FAILED
        # 但最小化 close 已单独执行 → 服务端流式已关，卡不再转圈
        assert ctrl._client.cardkit_close_streaming.await_count == 2
        assert session._streaming_closed is True


# ── fire-and-forget 异常日志（缺口 1）───────────────────────────────


class TestFireAndForgetLogging:
    @pytest.mark.asyncio
    async def test_exception_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        ctrl = _setup_ctrl(linear=True)

        async def _boom() -> None:
            raise RuntimeError("boom-seal-coroutine")

        ctrl._fire_and_forget(_boom(), asyncio.get_running_loop())
        tasks = list(ctrl._pending_tasks)
        assert tasks
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(0.01)  # 让 done callback 跑完
        # 异常详情在 exc_info 的 traceback 里 → 用 caplog.text 全文断言
        assert "background task failed" in caplog.text
        assert "boom-seal-coroutine" in caplog.text

    @pytest.mark.asyncio
    async def test_cancellation_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        ctrl = _setup_ctrl(linear=True)

        async def _hang() -> None:
            await asyncio.sleep(999)

        ctrl._fire_and_forget(_hang(), asyncio.get_running_loop())
        (task,) = list(ctrl._pending_tasks)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0.01)
        assert any("CANCELLED" in rec.getMessage() for rec in caplog.records)


# ── 评审 Critical#1：partial_note 直测（唯一能抓到解包 bug 的测试层）──


class TestPartialNoteBuilder:
    def test_partial_note_overrides_bilingually(self) -> None:
        from hermes_lark_streaming.cardkit import build_preservative_seal_actions

        actions = build_preservative_seal_actions(
            partial=True,
            footer_data={},
            existing_elements=set(),
            partial_note=("EN note text", "中文提示文本"),
        )
        adds = [a for a in actions if a.get("action") == "add_elements"]
        assert adds, f"partial indicator action missing: {actions}"
        elems = adds[0]["params"]["elements"]
        md = [e for e in elems if e.get("tag") == "markdown"]
        assert md, f"partial markdown element missing: {elems}"
        content = md[0]["content"]
        # 回归守护：解包错误会把 tuple repr 渲染进卡片
        assert "EN note text" in content
        assert "中文提示文本" in md[0]["i18n_content"]["zh_cn"]
        assert "tuple" not in content
        assert "'" not in content

    def test_default_partial_note_unchanged(self) -> None:
        from hermes_lark_streaming.cardkit import build_preservative_seal_actions
        from hermes_lark_streaming.cardkit.i18n import _T

        actions = build_preservative_seal_actions(
            partial=True, footer_data={}, existing_elements=set()
        )
        adds = [a for a in actions if a.get("action") == "add_elements"]
        assert adds
        elems = adds[0]["params"]["elements"]
        md = [e for e in elems if e.get("tag") == "markdown"][0]
        en_default, zh_default = _T["partial_continues"]
        assert en_default in md["content"]
        assert zh_default in md["i18n_content"]["zh_cn"]


# ── watchdog 判活时间戳 + 基础字段 ───────────────────────────────────


class TestEventTimestampsAndSessionFields:
    def test_on_answer_bumps_last_event_at(self) -> None:
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_ts1", linear=True)
        session._last_event_at = time.monotonic() - 100
        ctrl._sessions["msg_ts1"] = session

        ctrl.on_answer(message_id="msg_ts1", text="hello")

        assert session._last_event_at > time.monotonic() - 100

    def test_on_tool_update_bumps_last_event_at(self) -> None:
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_ts2", linear=True)
        session._last_event_at = time.monotonic() - 100
        ctrl._sessions["msg_ts2"] = session

        ctrl.on_tool_update(message_id="msg_ts2", tool_name="bash", status="started")

        assert session._last_event_at > time.monotonic() - 100

    def test_session_new_fields_initialized(self) -> None:
        loop = _make_loop()
        session = CardSession("msg_fields", "chat_456", loop)
        assert isinstance(session.home, str)
        assert session._seal_dispatched_at == 0.0
        assert session._seal_done is False
        assert session._watchdog_split is False
        assert session._chain_depth == 0
        assert session._last_event_at > 0
        assert not session._seal_lock.locked()

    def test_config_defaults_and_clamps(self) -> None:
        from hermes_lark_streaming.config.reader import Config

        cfg = Config()
        cfg._raw = {"hermes_lark_streaming": {}}
        assert cfg.seal_watchdog_sec == 120
        assert cfg.stuck_card_timeout_sec == 1800
        cfg._raw = {"hermes_lark_streaming": {"seal_watchdog_sec": 5}}
        assert cfg.seal_watchdog_sec == 30  # 下限 clamp
        cfg._raw = {"hermes_lark_streaming": {"stuck_card_timeout_sec": 10}}
        assert cfg.stuck_card_timeout_sec == 600  # 下限 clamp
        cfg._raw = {"hermes_lark_streaming": {"stuck_card_timeout_sec": 0}}
        assert cfg.stuck_card_timeout_sec == 0  # 显式禁用


# ── 派发器：loop 已关闭的安全降级 + home 绑定 ────────────────────────


class TestDispatchers:
    def test_dispatch_watchdog_seal_closed_loop_noop(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_dl1", linear=True)
        loop = _make_loop()
        session._loop = loop
        loop.close()

        ctrl._dispatch_watchdog_seal(session)  # 不应抛异常

        assert any("loop closed" in rec.getMessage() for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_watchdog_seal_with_home_binds_and_resets(self) -> None:
        """watchdog 重派协程在自身 context 绑定会话 home 并在退出时复位。"""
        from hermes_lark_streaming.config.reader import (
            current_turn_home,
            reset_turn_home,
            set_turn_home,
        )

        ctrl = _setup_ctrl(linear=True)
        session = _make_session("msg_hm1", linear=True)
        session.home = "/tmp/hermes-profile-a"
        captured: list[str | None] = []
        seal_calls: list[CardSession] = []

        async def _fake_seal(sess: CardSession) -> None:
            seal_calls.append(sess)
            captured.append(current_turn_home())

        ctrl._do_linear_complete_with_fallback = _fake_seal  # type: ignore[method-assign]

        saved = current_turn_home()
        token = set_turn_home(None)  # 模拟 watchdog 无 turn 绑定
        try:
            await ctrl._watchdog_seal_with_home(session)
            captured.append(current_turn_home())  # 协程退出后应已复位
        finally:
            reset_turn_home(token)
            _ = saved

        assert seal_calls == [session]
        assert captured[0] == "/tmp/hermes-profile-a"  # 协程内绑定生效
        assert captured[1] is None  # 退出后复位

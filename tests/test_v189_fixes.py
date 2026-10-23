"""v1.8.9 修复回归测试。

覆盖 docs/review-2026-10-23-deep-review.md 核实后的六项修复：
- P0-2（定级下调 P2）: on_aborted 与在飞 flush 的时序 + answer_dirty 水位守卫
- P2-4: 两份 collapse hint 实现合一（cards.py ↔ elements.py ↔ linear_mixin）
- §5.2-A/C: 回调 / _run_agent_inner 签名漂移探测
- P2-1: _build_panel_payload 收敛 7 处 build_unified_panel 参数块
- P2-8: 答案两级上限（流式视图 50K / 存储 150K）+ 封卡按 ## 标题分块
"""

from __future__ import annotations

import asyncio
import re
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hermes_lark_streaming.cardkit.cards import _enforce_card_element_limit
from hermes_lark_streaming.cardkit.elements import (
    _LOADING_ELEMENT_ID,
    ANSWER_ELEMENT_ID,
    UNIFIED_PANEL_ELEMENT_ID,
    _collapse_hint_digit_sum,
    _is_collapse_hint_child,
)
from hermes_lark_streaming.controller.linear_mixin import (
    _answer_seal_chunk_chars,
    _split_answer_chunks,
)
from hermes_lark_streaming.controller.mixin import STREAMING

# _do_unified_flush 水位守卫测试复用 test_controller 的现成 harness
# （conftest 的 loop 清理同样从 tests.test_controller 收集）。
from tests.test_controller import _make_session, _setup_ctrl

# ─────────────────────────────────────────────────────────────────────────────
# P2-4: collapse hint 合一
# ─────────────────────────────────────────────────────────────────────────────


class TestCollapseHintUnification:
    def test_digit_sum_single_group(self) -> None:
        assert _collapse_hint_digit_sum("⚡ 还有 5 项已折叠") == 5

    def test_digit_sum_dual_group_panel_builder_format(self) -> None:
        # 面板构造器格式：两组数字之和才是已折叠总数。旧 find("项") 回扫
        # 在此格式下找不到锚点、计数归零。
        assert _collapse_hint_digit_sum("⚡ 还有 3 轮早期推理、4 步早期操作已折叠") == 7

    def test_digit_sum_english_hint(self) -> None:
        assert _collapse_hint_digit_sum("⚡ 9 items collapsed") == 9

    def test_hint_child_recognizes_english(self) -> None:
        assert _is_collapse_hint_child({"content": "⚡ 9 items collapsed"})
        assert _is_collapse_hint_child({"content": "⚡ 还有 9 项已折叠"})
        # 长正文提到 collapse 词不算 hint（长度守卫）
        assert not _is_collapse_hint_child({"content": "collapse " * 40})

    @staticmethod
    def _panel_child(i: int) -> dict:
        # 每个子元素 1 个 tag 对象；content 不以 ⚡ 开头 → 不会被误判为 hint
        return {"tag": "markdown", "content": f"第 {i} 轮推理内容 {'x' * 20}"}

    @staticmethod
    def _big_card(children: list[dict]) -> dict:
        return {
            "body": {
                "elements": [
                    {
                        "tag": "collapsible_panel",
                        "element_id": UNIFIED_PANEL_ELEMENT_ID,
                        "elements": children,
                    }
                ]
            }
        }

    @staticmethod
    def _hints_of(card: dict) -> list[dict]:
        return [
            c
            for c in card["body"]["elements"][0]["elements"]
            if _is_collapse_hint_child(c)
        ]

    def test_enforce_english_hint_counted_not_reset(self) -> None:
        # 卡片已有英文 hint（历史构造器产物）+ 超限触发 trim → 计数必须在
        # 原有基础上累加，不能归零重计、不能重复插入第二条 hint。
        # 阈值 = 200 - 5 = 195；hint(1) + 200 children = 202 个 tag 对象。
        hint = {"tag": "markdown", "content": "⚡ 4 items collapsed"}
        children = [hint] + [self._panel_child(i) for i in range(200)]
        card = self._big_card(children)

        out = _enforce_card_element_limit(card)

        hints = self._hints_of(out)
        assert len(hints) == 1, "英文 hint 未被识别（重复插入或漏检）"
        new_count = sum(int(n) for n in re.findall(r"\d+", hints[0]["content"]))
        assert new_count > 4, "折叠计数归零重计（未累加原有 4）"

    def test_enforce_new_hint_is_bilingual(self) -> None:
        children = [self._panel_child(i) for i in range(200)]
        card = self._big_card(children)

        out = _enforce_card_element_limit(card)

        hints = self._hints_of(out)
        assert len(hints) == 1
        assert "i18n_content" in hints[0], "新 hint 缺 i18n_content（旧实现纯中文）"
        assert "collapsed" in str(hints[0]["i18n_content"]).lower()


# ─────────────────────────────────────────────────────────────────────────────
# P2-8: 答案两级上限 + 封卡分块
# ─────────────────────────────────────────────────────────────────────────────


class TestAnswerTwoTierCaps:
    def test_stream_view_capped_but_text_keeps_growing(self, monkeypatch) -> None:
        from hermes_lark_streaming.state.linear import UnifiedLinearState

        monkeypatch.setattr(UnifiedLinearState, "_MAX_ANSWER_CHARS", 1000)
        monkeypatch.setattr(UnifiedLinearState, "_MAX_ANSWER_STORE_CHARS", 3000)
        state = UnifiedLinearState()
        state.on_answer_delta("a" * 2500)

        assert not state.answer_truncated, "未触存储上限不应置截断标志"
        assert len(state.answer_text) == 2500, "视图上限不应截断存储"
        assert len(state.escaped_answer_view()) <= 1000, "流式视图未按展示上限截取"

    def test_store_cap_still_truncates_with_notice(self, monkeypatch) -> None:
        from hermes_lark_streaming.state.linear import UnifiedLinearState

        monkeypatch.setattr(UnifiedLinearState, "_MAX_ANSWER_CHARS", 1000)
        monkeypatch.setattr(UnifiedLinearState, "_MAX_ANSWER_STORE_CHARS", 2000)
        state = UnifiedLinearState()
        state.on_answer_delta("b" * 5000)

        assert state.answer_truncated
        assert "截断" in state.answer_text
        before = len(state.answer_text)
        state.on_answer_delta("c" * 100)
        assert len(state.answer_text) == before


class TestSplitAnswerChunks:
    def test_short_text_single_chunk(self) -> None:
        assert _split_answer_chunks("hello world", 100) == ["hello world"]

    def test_split_on_headings(self) -> None:
        text = "引导段落\n\n" + "".join(
            f"## 标题{i}\n{'内容' * 30}\n\n" for i in range(6)
        )
        chunks = _split_answer_chunks(text, 200)
        assert len(chunks) > 1
        # 首块之后的每块都落在标题边界，且全部不超限、无损拼接
        for c in chunks[1:]:
            assert c.startswith("##"), "切块未落在标题边界"
        assert all(len(c) <= 200 for c in chunks)
        assert "".join(chunks) == text, "分块必须无损拼接"

    def test_no_headings_hard_split(self) -> None:
        text = "平文本" * 500  # 无任何标题
        chunks = _split_answer_chunks(text, 300)
        assert all(len(c) <= 300 for c in chunks)
        assert "".join(chunks) == text

    def test_hash_in_code_fence_not_a_boundary(self) -> None:
        # 标题 + 正文 + 围栏（内含 # 注释行）交替；围栏内的 # 不是标题。
        parts = [
            f"## 标题{i}\n正文{str(i) * 40}\n\n```bash\n# 注释 {i}\n# 再来 {i}\n```\n\n"
            for i in range(8)
        ]
        text = "".join(parts)

        chunks = _split_answer_chunks(text, 300)

        assert "".join(chunks) == text
        assert all(len(c) <= 300 for c in chunks)
        assert len(chunks) > 1, "文本足够长应产生多块"

        # 围栏内 # 注释行的起始位置
        comment_starts: set[int] = set()
        offset = 0
        in_fence = False
        for line in text.splitlines(keepends=True):
            s = line.lstrip()
            if s.startswith("```"):
                in_fence = not in_fence
            elif in_fence and s.startswith("#"):
                comment_starts.add(offset)
            offset += len(line)

        # 块边界（累计长度）不得恰好落在任何围栏注释行开头
        boundary = 0
        for c in chunks[:-1]:
            boundary += len(c)
            assert boundary not in comment_starts, (
                "代码围栏内的 # 行被当成了标题切点"
            )


class _ChunkCfg:
    # clamp 下限 4000——再小的配置值会被抬到 4000
    answer_chunk_chars = 4000


class TestAnswerSealActions:
    """_answer_seal_actions 是 mixin 方法，用 __new__ 绕 init 的最小替身。"""

    @staticmethod
    def _mixin(cfg) -> object:
        from hermes_lark_streaming.controller.linear_mixin import (
            UnifiedControllerMixin,
        )

        mixin = UnifiedControllerMixin.__new__(UnifiedControllerMixin)
        mixin._cfg = cfg
        return mixin

    @staticmethod
    def _session(existing_loading: bool) -> SimpleNamespace:
        return SimpleNamespace(
            existing_elements={"loading_icon"} if existing_loading else set()
        )

    def test_short_answer_single_partial_update(self) -> None:
        mixin = self._mixin(_ChunkCfg())
        state = SimpleNamespace(answer_text="短答案")
        actions = mixin._answer_seal_actions(self._session(True), state)

        assert len(actions) == 1
        assert actions[0]["action"] == "partial_update_element"
        assert actions[0]["params"]["element_id"] == ANSWER_ELEMENT_ID

    def test_long_answer_split_with_anchor(self) -> None:
        mixin = self._mixin(_ChunkCfg())
        state = SimpleNamespace(
            # 8 节 × ~1400 字符 ≈ 11K > 4000 → 必产生多块
            answer_text="".join(f"## S{i}\n{'内容' * 700}\n\n" for i in range(8))
        )
        actions = mixin._answer_seal_actions(self._session(True), state)

        assert len(actions) == 2
        assert actions[0]["action"] == "partial_update_element"
        assert actions[1]["action"] == "add_elements"
        assert actions[1]["params"]["type"] == "insert_before"
        assert actions[1]["params"]["target_element_id"] == _LOADING_ELEMENT_ID
        assert len(actions[1]["params"]["elements"]) >= 1

    def test_long_answer_without_anchor_falls_back_to_single(self) -> None:
        mixin = self._mixin(_ChunkCfg())
        state = SimpleNamespace(answer_text="长" * 5000)
        actions = mixin._answer_seal_actions(self._session(False), state)

        assert len(actions) == 1
        assert actions[0]["action"] == "partial_update_element"

    def test_chunk_chars_clamp(self) -> None:
        class BigCfg:
            answer_chunk_chars = 999_999

        assert _answer_seal_chunk_chars(BigCfg()) == 50_000
        assert _answer_seal_chunk_chars(_ChunkCfg()) == 4_000  # 低于下限抬到 4000
        assert _answer_seal_chunk_chars(object()) == 20_000


# ─────────────────────────────────────────────────────────────────────────────
# §5.2-C: 签名漂移探测
# ─────────────────────────────────────────────────────────────────────────────


class TestSignatureDrift:
    def _drift(self, monkeypatch):
        import hermes_lark_streaming.patching as patching_mod

        records: list[str] = []

        class _Log:
            def warning(self, msg, *args) -> None:
                records.append(msg % args if args else msg)

        monkeypatch.setattr(patching_mod, "_logger", _Log(), raising=True)
        # 去重集合是模块级的——全量跑时更早的 patch/callback 测试可能已把
        # 同名 label 记入（Mock 回调签名必然漂移），必须一并隔离。
        monkeypatch.setattr(patching_mod, "_SIG_DRIFT_WARNED", set(), raising=True)
        # helper 内部引用的是定义时模块全局名，monkeypatch 换的是模块属性，
        # 两者同一对象命名空间，直接取换过 _logger 后的函数即可。
        return patching_mod.warn_signature_drift, records

    def test_matching_signature_silent(self, monkeypatch) -> None:
        drift, records = self._drift(monkeypatch)

        def progress(event_type, tool_name=None, preview=None, args=None, **kw):
            return None

        assert (
            drift(
                progress,
                ("event_type", "tool_name", "preview"),
                label="tool_progress_callback",
            )
            is False
        )
        assert records == []

    def test_reordered_signature_warns_once(self, monkeypatch) -> None:
        drift, records = self._drift(monkeypatch)

        def progress(event_type, preview=None, tool_name=None, **kw):  # hermes 重排
            return None

        assert (
            drift(
                progress,
                ("event_type", "tool_name", "preview"),
                label="tool_progress_callback",
            )
            is True
        )
        assert len(records) == 1
        assert "signature drift" in records[0]
        # 同一 label 第二次不再刷屏
        assert (
            drift(
                progress,
                ("event_type", "tool_name", "preview"),
                label="tool_progress_callback",
            )
            is True
        )
        assert len(records) == 1

    def test_dropped_param_warns(self, monkeypatch) -> None:
        drift, records = self._drift(monkeypatch)

        def start(call_id, tool_name):  # hermes 删掉了第 3 个参数
            return None

        assert (
            drift(start, ("call_id", "tool_name", "args"), label="tool_start_callback")
            is True
        )
        assert len(records) == 1

    def test_unintrospectable_silent(self, monkeypatch) -> None:
        drift, records = self._drift(monkeypatch)

        class _NotCallable:
            # inspect.signature 对不可调用对象抛 TypeError —— 即 helper 的
            # "无法内省"分支（真实场景：C 扩展/内置；新版 Python 连 len 都
            # 可内省了，只能这样构造确定性反例）
            pass

        assert drift(_NotCallable(), ("x",), label="opaque") is False
        assert records == []

    def test_variadic_args_signature_silent(self, monkeypatch) -> None:
        drift, records = self._drift(monkeypatch)

        def cb(*args, **kwargs):  # *args 形回调按名校验无意义
            return None

        assert (
            drift(cb, ("event_type", "tool_name", "preview"), label="variadic")
            is False
        )
        assert records == []

    @staticmethod
    def _gateway_logger(monkeypatch):
        import hermes_lark_streaming.patching.gateway as gw

        records: list[str] = []

        class _Log:
            def warning(self, msg, *args) -> None:
                records.append(msg % args if args else msg)

        monkeypatch.setattr(gw, "_logger", _Log(), raising=True)
        return gw, records

    def test_run_agent_kwargs_passthrough_no_warn(self, monkeypatch) -> None:
        """生产 0.21.4 的 GatewayRunner._run_agent 是 (…, session_id,
        **turn_kwargs) 透传签名——11 个参数名折叠进 kwargs，不得误报漂移
        （首次安装即在生产日志误报过一次）。"""
        gw, records = self._gateway_logger(monkeypatch)

        async def orig(
            self, message, context_prompt, history, source, session_id, **turn_kwargs
        ):
            return {}

        assert callable(gw._wrap_run_agent(orig))
        assert records == [], "**kwargs 透传签名被误报为漂移"

    def test_run_agent_explicit_missing_warns(self, monkeypatch) -> None:
        gw, records = self._gateway_logger(monkeypatch)

        async def orig(self, message, context_prompt, history, source, session_id):
            # 全显式签名且缺 channel_prompt 等 → 转发必 TypeError，应告警
            return {}

        assert callable(gw._wrap_run_agent(orig))
        assert len(records) == 1
        assert "channel_prompt" in records[0]


# ─────────────────────────────────────────────────────────────────────────────
# P0-2: on_aborted 时序 + answer_dirty 水位守卫
# ─────────────────────────────────────────────────────────────────────────────


class TestOnAbortedWaitsForInFlightFlush:
    @staticmethod
    def _bare_ctrl(monkeypatch):
        """__new__ 绕 init 的裸 controller；enabled 是只读 property，
        在类上换成恒 True。"""
        from hermes_lark_streaming.controller.core import StreamCardController

        monkeypatch.setattr(
            StreamCardController, "enabled", property(lambda self: True)
        )
        return StreamCardController.__new__(StreamCardController)

    def test_on_aborted_defers_mark_completed_until_flush_lands(
        self, monkeypatch
    ) -> None:
        """在飞 flush 未落地时 on_aborted 不得 mark_completed（旧实现立刻封死
        flush/controller 的补刷门，drain 只能空转），须调度 _wait_and_abort
        等落地后再 finalize。"""
        from hermes_lark_streaming.flush.controller import FlushController

        async def scenario() -> None:
            ctrl = self._bare_ctrl(monkeypatch)

            fc = FlushController()
            in_flight = asyncio.Event()

            async def do_flush() -> None:
                in_flight.set()
                await asyncio.sleep(0.05)

            flush_task = asyncio.get_running_loop().create_task(
                FlushController._do_flush(fc, do_flush)
            )
            await in_flight.wait()
            assert fc._flush_in_progress

            session = SimpleNamespace(
                state=STREAMING,
                message_id="om_test",
                _was_aborted=False,
                is_terminal_phase=False,
                flush=fc,
            )
            session.enter_terminal = lambda **kw: None
            ctrl._get_active_session = lambda mid: session
            ctrl._get_loop = lambda: asyncio.get_running_loop()

            scheduled: list = []
            ctrl._fire_and_forget = lambda coro, loop: scheduled.append(coro)
            completed: list = []
            ctrl._complete_session = lambda s: completed.append(s)

            ctrl.on_aborted(message_id="om_test")

            assert session._was_aborted, "应立即标记 _was_aborted（seal 要显示 stopped）"
            assert not fc._completed, (
                "on_aborted 在在飞 flush 落地前就 mark_completed（旧 P0-2 时序）"
            )
            assert len(scheduled) == 1, "未调度 _wait_and_abort"

            await flush_task  # 在飞 flush 落地
            await scheduled[0]  # _wait_and_abort 收尾

            assert fc._completed, "flush 落地后未执行 mark_completed"
            assert completed, "_complete_session 未派发"

        asyncio.run(scenario())

    def test_on_aborted_immediate_when_no_flush_in_flight(self, monkeypatch) -> None:
        from hermes_lark_streaming.flush.controller import FlushController

        async def scenario() -> None:
            ctrl = self._bare_ctrl(monkeypatch)

            fc = FlushController()
            session = SimpleNamespace(
                state=STREAMING,
                message_id="om_now",
                _was_aborted=False,
                flush=fc,
            )
            session.enter_terminal = lambda **kw: None
            ctrl._get_active_session = lambda mid: session
            ctrl._get_loop = lambda: asyncio.get_running_loop()
            completed: list = []
            ctrl._complete_session = lambda s: completed.append(s)

            ctrl.on_aborted(message_id="om_now")

            assert fc._completed, "无在飞 flush 时应立即 finalize"
            assert completed

        asyncio.run(scenario())

    @pytest.mark.asyncio
    async def test_flush_keeps_dirty_when_delta_lands_during_await(self) -> None:
        """水位守卫：await cardkit_stream_element 期间新到的 delta 会重置
        answer_dirty；成功后仅在文本未再增长时才清 dirty。旧实现无条件
        `answer_dirty = False` → lost-update，最后一段增量被困到封卡。"""
        ctrl = _setup_ctrl()
        session = _make_session("msg_wm", linear=True)
        session.state = STREAMING
        session.card_id = "card_wm"
        session._creation_stages.add("answer")  # 直接走 Phase 3 流式路径
        state = session.unified_state
        state.on_answer_delta("HEAD")
        ctrl._sessions["msg_wm"] = session

        async def _delta_during_flight(*args, **kwargs):
            # 网络往返窗口内新 delta 到达（worker 线程回调时序）
            state.on_answer_delta("TAIL")

        ctrl._client.cardkit_stream_element = AsyncMock(
            side_effect=_delta_during_flight
        )

        await ctrl._do_unified_flush(session)

        assert state.answer_dirty, (
            "在飞期间到达的增量被 lost-update 清掉（水位守卫失效）"
        )
        assert "TAIL" in state.answer_text

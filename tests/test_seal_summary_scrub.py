"""Regression tests for seal-summary think-tag scrubbing.

These tests guard against the production incident where unbalanced
``</mm:think>`` tails leaked into the CardKit seal summary and showed up in
Feishu cards (P0 UX bug, 2026-09-07).

The scrub runs in :func:`_scrub_think_tags` and is invoked from
:func:`_build_seal_summary` so any user-visible summary is guaranteed
free of internal-reasoning tags, regardless of which buffer the
content came from (answer_text or reasoning fallback).
"""

from __future__ import annotations

from hermes_lark_streaming.controller.linear_mixin import (
    _build_seal_summary,
    _scrub_think_tags,
)


class TestScrubThinkTags:
    def test_passthrough_when_no_tags(self) -> None:
        assert _scrub_think_tags("正常的回答内容，没有标签") == "正常的回答内容，没有标签"

    def test_strip_balanced_block(self) -> None:
        text = "<mm:think>思考过程，不应外露</mm:think>真实答案"
        assert _scrub_think_tags(text) == "真实答案"

    def test_strip_multiple_balanced_blocks(self) -> None:
        # The tags themselves occupy zero visible characters, so a balanced
        # block sandwiched between two Chinese tokens collapses them with
        # no separator — this is the same behaviour ``str.replace`` would
        # produce on visible text.  We only collapse whitespace when the
        # deletion leaves a *run* of spaces (see ``test_collapse_*``).
        text = "<mm:think>a</mm:think>中间<mm:think>b</mm:think>结束"
        assert _scrub_think_tags(text) == "中间结束"

    def test_strip_orphan_closing_tag(self) -> None:
        """The actual production leak: only ``</mm:think>`` shows up."""
        text = "</mm:think></mm:think></mm:think>德哥发了个 C"
        assert _scrub_think_tags(text) == "德哥发了个 C"

    def test_strip_orphan_opening_tag(self) -> None:
        """LLM truncated mid-think before emitting the closing tag.

        The leading ``<mm:think>`` is dropped, then the interior double
        space that the deletion exposes gets collapsed to a single space
        by the whitespace-folding step.
        """
        text = "answer first <mm:think> 然后没了"
        assert _scrub_think_tags(text) == "answer first 然后没了"

    def test_strip_mixed_balanced_and_orphan(self) -> None:
        text = "<mm:think>deep</mm:think>Real</mm:think>tail"
        assert _scrub_think_tags(text) == "Realtail"

    def test_strip_production_leak_with_long_prefix(self) -> None:
        text = "开干。先拿凭据 + 启动。      </mm:think>xiaoaima 凭"
        assert _scrub_think_tags(text) == "开干。先拿凭据 + 启动。 xiaoaima 凭"

    def test_collapse_whitespace_after_strip(self) -> None:
        """Block deletion can leave runs of whitespace; collapse to single space."""
        text = "before   <mm:think>hidden</mm:think>   after"
        assert _scrub_think_tags(text) == "before after"

    def test_only_thinking_block_returns_empty(self) -> None:
        text = "<mm:think>nothing else</mm:think>"
        assert _scrub_think_tags(text) == ""

    def test_empty_string(self) -> None:
        assert _scrub_think_tags("") == ""


class _FakeRound:
    """Minimal stand-in for state.linear.ReasoningRound for these tests."""

    def __init__(self, text: str) -> None:
        self.text = text


class _FakeState:
    """Minimal stand-in for UnifiedLinearState — only the fields we read."""

    def __init__(self, answer_text: str = "", reasoning_texts: list[str] | None = None) -> None:
        self.answer_text = answer_text
        # state.answer_text is checked first; only fall back to reasoning when empty.
        self.reasoning_rounds = [_FakeRound(t) for t in (reasoning_texts or [])]


class TestBuildSealSummary:
    def test_clean_answer_unchanged(self) -> None:
        state = _FakeState(answer_text="干净回答，前 120 字符")
        assert _build_seal_summary(state) == "干净回答，前 120 字符"

    def test_answer_with_think_block_is_scrubbed(self) -> None:
        state = _FakeState(answer_text="<mm:think>不该显示</mm:think>最终答案")
        assert _build_seal_summary(state) == "最终答案"

    def test_falls_back_to_reasoning_when_answer_empty(self) -> None:
        state = _FakeState(answer_text="", reasoning_texts=["从 reasoning 来的内容"])
        assert _build_seal_summary(state) == "从 reasoning 来的内容"

    def test_reasoning_fallback_is_scrubbed(self) -> None:
        """Production leak path: answer empty, reasoning has stray tags."""
        state = _FakeState(
            answer_text="",
            reasoning_texts=["</mm:think></mm:think></mm:think>德哥发了个 ClawdChat 的"],
        )
        assert _build_seal_summary(state) == "德哥发了个 ClawdChat 的"

    def test_none_state_returns_empty(self) -> None:
        assert _build_seal_summary(None) == ""

    def test_empty_state_returns_empty(self) -> None:
        assert _build_seal_summary(_FakeState()) == ""

    def test_truncates_after_scrubbing(self) -> None:
        """Long answer should still be clipped to 120 chars, but scrub first."""
        long = "a" * 200
        state = _FakeState(answer_text=f"<mm:think>thinking</mm:think>{long}")
        result = _build_seal_summary(state)
        assert len(result) == 120
        assert "thinking" not in result
        assert result == "a" * 120

    def test_newlines_collapsed(self) -> None:
        state = _FakeState(answer_text="第一行\n第二行\n第三行")
        result = _build_seal_summary(state)
        assert "\n" not in result
        assert "第一行 第二行 第三行" == result

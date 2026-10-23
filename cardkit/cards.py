"""CardKit v2.0 — Card assemblers: streaming, complete, and linear cards."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .elements import (
    ANSWER_ELEMENT_ID,
    CARD_WIDTH_MODE,
    UNIFIED_PANEL_ELEMENT_ID,
    _build_unified_panel_placeholder,
    _collapse_hint_digit_sum,
    _count_tag_objects,
    _is_collapse_hint_child,
    _loading_element,
    _loading_hint_element,
    _streaming_element,
)
from .i18n import _LOCALES, _T, _i18n, _t

if TYPE_CHECKING:
    pass

__all__ = [
    "build_streaming_card_v2",
    "_enforce_card_element_limit",
]

# Feishu Card 2.0 element limit — every JSON object with a ``tag`` key
# counts toward this limit at all nesting levels.
_FEISHU_ELEMENT_LIMIT = 200

_ELEMENT_LIMIT_MARGIN = 5


def _enforce_card_element_limit(
    card: dict[str, Any],
    *,
    panel_element_id: str = UNIFIED_PANEL_ELEMENT_ID,
) -> dict[str, Any]:
    """Counts **all** tag objects in the card (including nested ones)."""
    threshold = _FEISHU_ELEMENT_LIMIT - _ELEMENT_LIMIT_MARGIN
    total = _count_tag_objects(card)
    if total <= threshold:
        return card

    # ── Find the unified panel element in card body ──
    body = card.get("body", {})
    elements = body.get("elements", [])
    panel = None
    for elem in elements:
        if (
            elem.get("element_id") == panel_element_id
            and elem.get("tag") == "collapsible_panel"
        ):
            panel = elem
            break

    if panel is None:
        # No panel found — nothing to trim (answer/footer must not be trimmed)
        return card

    children: list[dict] = panel.get("elements", [])

    # ── Check if a collapse hint already exists ──
    # v1.8.9 (P2-4): 与 elements.py/linear_mixin 收敛为同一实现——旧的
    # "已折叠"子串判定不认识英文 hint，find("项") 回扫也不累加面板构造器
    # 双数字格式（"X 轮早期推理、Y 步早期操作"）的计数，一旦命中即归零
    # 重计。判定/跳过/计数/写入全部走共享助手，新 hint 补 i18n_content。
    hint_idx = None
    for i, child in enumerate(children):
        if _is_collapse_hint_child(child):
            hint_idx = i
            break
    _HINT_TAG_COUNT = 1  # markdown hint element itself
    if hint_idx is None:
        total += _HINT_TAG_COUNT  # Reserve exact space for the new collapse hint

    # ── Trim oldest items from panel children until under threshold ──
    trimmed_count = 0
    while total > threshold and len(children) > 1:
        # Skip the collapse hint (first child when it is one)
        remove_idx = 1 if _is_collapse_hint_child(children[0]) else 0
        removed = children.pop(remove_idx)
        total -= _count_tag_objects([removed])
        trimmed_count += 1

    if trimmed_count > 0:
        # Update or add collapse hint
        # Re-find hint_idx (may have shifted due to removals)
        hint_idx = None
        for i, child in enumerate(children):
            if _is_collapse_hint_child(child):
                hint_idx = i
                break
        _en_hint, _zh_hint = _T["collapse_hint_count"]
        if hint_idx is not None:
            old_hint = str(children[hint_idx].get("content", ""))
            # Sums digit groups of BOTH hint spellings (see
            # _collapse_hint_digit_sum) instead of the old zh-only
            # "项"-anchored backward scan.
            total_trimmed = _collapse_hint_digit_sum(old_hint) + trimmed_count
            children[hint_idx]["content"] = _zh_hint.format(total_trimmed)
            children[hint_idx]["i18n_content"] = _i18n(
                _en_hint.format(total_trimmed),
                _zh_hint.format(total_trimmed),
            )
        else:
            children.insert(
                0,
                {
                    "tag": "markdown",
                    "content": _zh_hint.format(trimmed_count),
                    "i18n_content": _i18n(
                        _en_hint.format(trimmed_count),
                        _zh_hint.format(trimmed_count),
                    ),
                    "text_size": "notation",
                },
            )

    # Update panel children in the card
    panel["elements"] = children
    return card


def _build_summary(text: str) -> dict[str, Any]:
    """Feishu CardKit 2.0 displays ``i18n_content.<locale>`` for users"""
    truncated = text[:120].replace("\n", " ").replace("```", "").strip()
    return {
        "content": truncated,
        "i18n_content": _i18n(truncated, truncated),
    }


def build_streaming_card_v2(
    *,
    tool_steps: list[dict] | None = None,
    elapsed_ms: float = 0,
    show_tool_use: bool = True,
    show_reasoning: bool = False,
    show_streaming_element: bool = True,
    streaming_panel_expanded: bool = True,
    print_strategy: str = "delay",
    print_step: int = 4,
    include_unified_panel: bool = True,
    include_loading_hint: bool = True,
    include_answer_element: bool = True,
) -> dict[str, Any]:
    """Card lifecycle (v1.0.2+):"""
    elements: list[dict] = []

    # ── Unified panel placeholder (linear mode — single panel for reasoning+tools) ──
    if include_unified_panel:
        elements.append(
            _build_unified_panel_placeholder(expanded=streaming_panel_expanded)
        )

    # ── Streaming answer element ──
    if show_streaming_element and include_answer_element:
        elements.append(_streaming_element(element_id=ANSWER_ELEMENT_ID))

    # ── Loading hint (context loading placeholder, removed on first LLM token) ──
    if include_loading_hint:
        elements.append(_loading_hint_element())

    # ── Loading spinner ──
    elements.append(_loading_element())

    card: dict[str, Any] = {
        "schema": "2.0",
        "config": {
            "streaming_mode": True,
            "streaming_config": {
                "print_frequency_ms": {"default": 70},
                "print_step": {"default": print_step},
                "print_strategy": print_strategy,
            },
            "width_mode": CARD_WIDTH_MODE,
            "locales": _LOCALES,
            "summary": {
                "content": _T["processing"][0],
                "i18n_content": _t("processing"),
            },
        },
        "body": {"elements": elements},
    }
    return card

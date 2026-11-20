"""CardKit 2.0 表单卡 — clarify 多选 / 开放题（v1.12.0）.

上游 ``plugin/clarify_form.py``（v2.0.36 终态）移植，本地化改造：
- i18n 走 ``_T``/``_i18n``（上游硬编码中文）
- 卡片 config 对齐本地三态卡（streaming_mode/width_mode/locales，无 header）
- 问题行样式对齐 build_clarify_card（info_outlined 图标 + 加粗）

官方组件语义（上游真飞书验证）：
- ``multi_select_static``：仅 form 容器内合法；提交选项 value 数组
- ``input``（multiline_text + auto_resize 3→6）：仅 form 容器内合法；提交字符串
- 提交按钮 ``form_action_type="submit"``：callback 携带 ``action.form_value``
- 取消按钮 ``form_action_type="reset"``：纯客户端重置，无 callback

两个硬知识（上游踩坑存档）：
1. **200830-safe**：飞书拒绝用 JSON 1.0 卡片更新 JSON 2.0 卡片——落定卡必须
   保持 schema 2.0 结构（``build_settled_form_card``）。
2. **多选答案回填格式**：``extract_form_answer`` 把选项下标还原为 labels 后
   ``json.dumps(labels)``——这是 Hermes multi-select 的线上格式（JSON 数组
   字符串），不是逗号拼接。
"""

from __future__ import annotations

import json
from typing import Any

from .elements import CARD_WIDTH_MODE, _escape_md
from .i18n import _LOCALES, _T, _i18n

__all__ = [
    "build_clarify_form_card",
    "build_settled_form_card",
    "extract_form_answer",
    "extract_form_value",
]


def build_clarify_form_card(
    question: str,
    choices: list[str] | None,
    clarify_id: str,
    *,
    multi_select: bool,
) -> dict[str, Any]:
    """构建 clarify 表单卡（多选 multi_select_static / 开放题 multiline input）."""
    if multi_select and choices:
        en_ph, zh_ph = _T["clarify_multi_placeholder"]
        component = {
            "tag": "multi_select_static",
            "element_id": "clarify_multi",
            "name": "clarify_multi",
            "placeholder": {
                "tag": "plain_text",
                "content": en_ph,
                "i18n_content": _i18n(en_ph, zh_ph),
            },
            "width": "fill",
            "required": True,
            "behaviors": [
                {
                    "type": "callback",
                    "value": {
                        "hermes_clarify_action": "form_component",
                        "clarify_id": clarify_id,
                    },
                }
            ],
            "options": [
                {
                    "text": {
                        "tag": "plain_text",
                        "content": str(choice),
                    },
                    # value = 选项下标（extract_form_answer 据此还原 labels）
                    "value": str(index),
                }
                for index, choice in enumerate(choices)
            ],
        }
        form_kind = "multi"
    else:
        en_ph, zh_ph = _T["clarify_input_placeholder"]
        component = {
            "tag": "input",
            "element_id": "clarify_text",
            "name": "clarify_text",
            "placeholder": {
                "tag": "plain_text",
                "content": en_ph,
                "i18n_content": _i18n(en_ph, zh_ph),
            },
            "width": "fill",
            "required": True,
            "input_type": "multiline_text",
            "rows": 3,
            "auto_resize": True,
            "max_rows": 6,
            "label": {"tag": "plain_text", "content": str(question)},
        }
        form_kind = "text"

    en_submit, zh_submit = _T["clarify_form_submit"]
    en_reset, zh_reset = _T["clarify_form_reset"]
    submit = {
        "tag": "button",
        "text": {
            "tag": "plain_text",
            "content": en_submit,
            "i18n_content": _i18n(en_submit, zh_submit),
        },
        "type": "primary",
        "form_action_type": "submit",
        "name": "clarify_submit",
        "behaviors": [
            {
                "type": "callback",
                "value": {
                    "hermes_clarify_action": "form_submit",
                    "clarify_id": clarify_id,
                    "form_kind": form_kind,
                },
            }
        ],
    }
    reset = {
        "tag": "button",
        "text": {
            "tag": "plain_text",
            "content": en_reset,
            "i18n_content": _i18n(en_reset, zh_reset),
        },
        "type": "default",
        # reset 无 callback——纯客户端表单重置
        "form_action_type": "reset",
        "name": "clarify_reset",
    }

    elements: list[dict[str, Any]] = [
        {
            "tag": "div",
            "icon": {
                "tag": "standard_icon",
                "token": "info_outlined",
                "size": "20px 20px",
                "color": "blue",
            },
            "text": {
                "tag": "lark_md",
                "content": f"**{_escape_md(str(question))}**",
            },
        },
        {
            "tag": "form",
            "name": "clarify_form",
            "elements": [
                component,
                {
                    "tag": "column_set",
                    "columns": [
                        {
                            "tag": "column",
                            "width": "auto",
                            "elements": [submit],
                        },
                        {
                            "tag": "column",
                            "width": "auto",
                            "elements": [reset],
                        },
                    ],
                },
            ],
        },
    ]

    return {
        "schema": "2.0",
        "config": {
            "streaming_mode": False,
            "width_mode": CARD_WIDTH_MODE,
            "locales": _LOCALES,
        },
        "body": {"elements": elements},
    }


def build_settled_form_card(
    question: str,
    line: str,
    template: str = "green",
    *,
    line_i18n: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """构建表单卡落定态（200830-safe：保持 schema 2.0 结构）.

    ``line`` 为已 escape 的展示行（如 ``**用户:** 答案``）；``line_i18n``
    可选传入 ``_i18n(en_line, zh_line)`` 结果做双语。

    ⚠ line_i18n 陷阱：``i18n_content`` 会**整体覆盖**该元素的合并文案
    （question + line），不是只替换 line——传入的 i18n 文案必须自带完整
    内容。当前唯一调用方是过期路径（question 已原子清空），故传 line-only
    安全；未来若 question 非空，需把 i18n 文案拼上问题行或拆成两个元素。
    """
    text: dict[str, Any] = {
        "tag": "lark_md",
        "content": f"**{_escape_md(str(question))}**\n\n{line}",
    }
    if line_i18n is not None:
        text["i18n_content"] = line_i18n
    return {
        "schema": "2.0",
        "config": {
            "streaming_mode": False,
            "width_mode": CARD_WIDTH_MODE,
            "locales": _LOCALES,
        },
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "icon": {
                        "tag": "standard_icon",
                        "token": "info_outlined",
                        "size": "20px 20px",
                        "color": template,
                    },
                    "text": text,
                }
            ]
        },
    }


def extract_form_value(event: Any) -> dict[str, Any]:
    """取 ``event.action.form_value`` — SDK 对象与裸 dict 双路兼容.

    跨 SDK 版本安全（getattr + isinstance），拿不到返回空 dict。
    """
    action = getattr(event, "action", None)
    if action is None and isinstance(event, dict):
        action = event.get("action")
    value = getattr(action, "form_value", None)
    if value is None and isinstance(action, dict):
        value = action.get("form_value")
    return value if isinstance(value, dict) else {}


def extract_form_answer(
    form_kind: str,
    form_value: dict[str, Any],
    choices: list[str] | None,
) -> str | None:
    """表单提交 → clarify_gateway 期望的答案字符串.

    multi：选项下标还原为 labels 后 ``json.dumps(labels)``（Hermes
    multi-select 线上格式，JSON 数组字符串，非逗号拼接）；text：trim 后
    原文。无效提交（空/越界）返回 None（调用方提示"完成必填项"）。
    """
    if form_kind == "multi":
        raw = form_value.get("clarify_multi")
        if isinstance(raw, str):
            raw = [raw]
        if not isinstance(raw, list):
            return None
        labels: list[str] = []
        source = list(choices or [])
        for token in raw:
            try:
                index = int(str(token))
            except (TypeError, ValueError):
                continue
            if 0 <= index < len(source):
                labels.append(source[index])
        return json.dumps(labels, ensure_ascii=False) if labels else None
    if form_kind == "text":
        text = str(form_value.get("clarify_text") or "").strip()
        return text or None
    return None

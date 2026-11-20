"""v1.12.0 测试 — clarify 多选/开放题表单卡（docs/UPSTREAM-ADOPTION.md P5）.

上游 v2.0.36 plugin/clarify_form.py 移植：
- 构建器：cardkit/clarify_form.py（multi_select_static / multiline input / form 按钮）
- 接线：patching/adapter.py（send 判定 / form_submit 回调 / retire 落定）
- 硬知识：200830-safe（落定保持 schema 2.0）；多选答案 = json.dumps(labels)
"""

# pyright: reportMissingImports=false, reportAttributeAccessIssue=false
# hermes_lark_streaming.* 由 tests/conftest.py 运行时注册包名（目录名带连字
# 符 ≠ 包名），gateway.* 等宿主模块仅存在于 hermes 运行环境——静态解析必然
# 误报，文件级豁免（运行时零影响）。

from __future__ import annotations

import asyncio
import json
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from hermes_lark_streaming.cardkit.clarify_form import (
    build_clarify_form_card,
    build_settled_form_card,
    extract_form_answer,
    extract_form_value,
)

# 本地 venv 的 lark-oapi 版本可能无 CallBackCard（回调卡响应构建降级为空响应，
# import-tolerant 设计）——落定卡断言仅在 SDK 支持时执行。
try:
    from lark_oapi.api.cardkit.v1 import CallBackCard  # noqa: F401

    _HAS_CALLBACK_CARD = True
except ImportError:
    _HAS_CALLBACK_CARD = False


def _find_tag(elements: list, tag: str) -> dict | None:
    """递归找第一个 tag 匹配的元素（form/column_set 内层）。"""
    for el in elements:
        if not isinstance(el, dict):
            continue
        if el.get("tag") == tag:
            return el
        for key in ("elements", "columns"):
            inner = el.get(key)
            if isinstance(inner, list):
                found = _find_tag(inner, tag)
                if found is not None:
                    return found
            for col in el.get("columns", []) or []:
                found = _find_tag(col.get("elements", []), tag)
                if found is not None:
                    return found
    return None


class TestBuildClarifyFormCard:
    def test_multi_card_structure(self) -> None:
        card = build_clarify_form_card(
            "Pick tools", ["read", "grep", "bash"], "cid_m", multi_select=True
        )
        assert card["schema"] == "2.0"
        assert card["config"]["streaming_mode"] is False
        body = card["body"]["elements"]
        form = _find_tag(body, "form")
        assert form is not None, "form 容器缺失（multi_select_static 仅在 form 内合法）"
        multi = _find_tag([form], "multi_select_static")
        assert multi is not None
        assert [o["value"] for o in multi["options"]] == ["0", "1", "2"]
        assert multi["required"] is True
        # 组件 callback（勾选变化）
        assert multi["behaviors"][0]["value"]["hermes_clarify_action"] == "form_component"

    def test_submit_button_carries_form_kind_multi(self) -> None:
        card = build_clarify_form_card(
            "Pick", ["a", "b"], "cid_m", multi_select=True
        )
        body = card["body"]["elements"]
        buttons = [
            el
            for el in _walk_tags(body)
            if el.get("tag") == "button"
        ]
        submit = next(b for b in buttons if b.get("form_action_type") == "submit")
        value = submit["behaviors"][0]["value"]
        assert value == {
            "hermes_clarify_action": "form_submit",
            "clarify_id": "cid_m",
            "form_kind": "multi",
        }
        reset = next(b for b in buttons if b.get("form_action_type") == "reset")
        assert "behaviors" not in reset or not reset.get("behaviors"), (
            "reset 按钮不应带 callback（纯客户端重置）"
        )

    def test_text_card_structure(self) -> None:
        card = build_clarify_form_card("Describe", None, "cid_t", multi_select=False)
        body = card["body"]["elements"]
        inp = _find_tag(body, "input")
        assert inp is not None, "开放题应有 multiline input 组件"
        assert inp["input_type"] == "multiline_text"
        assert inp["auto_resize"] is True
        assert inp["max_rows"] == 6
        submit = next(
            b for b in _walk_tags(body) if b.get("tag") == "button"
            and b.get("form_action_type") == "submit"
        )
        assert submit["behaviors"][0]["value"]["form_kind"] == "text"


def _walk_tags(elements: list):
    for el in elements:
        if not isinstance(el, dict):
            continue
        yield el
        for col in el.get("columns", []) or []:
            yield from _walk_tags(col.get("elements", []))
        yield from _walk_tags(el.get("elements", []) or [])


class TestSettledFormCard:
    def test_settled_keeps_schema_2_0(self) -> None:
        """200830-safe：落定卡必须保持 schema 2.0（1.0 更新 2.0 会被拒）。"""
        card = build_settled_form_card("Q?", "**答**案", "green")
        assert card["schema"] == "2.0"
        assert card["config"]["streaming_mode"] is False


class TestExtractFormValue:
    def test_sdk_object_path(self) -> None:
        event = SimpleNamespace(
            action=SimpleNamespace(form_value={"clarify_text": "hi"})
        )
        assert extract_form_value(event) == {"clarify_text": "hi"}

    def test_plain_dict_path(self) -> None:
        event = {"action": {"form_value": {"clarify_multi": ["0"]}}}
        assert extract_form_value(event) == {"clarify_multi": ["0"]}

    def test_missing_returns_empty(self) -> None:
        assert extract_form_value(SimpleNamespace()) == {}
        assert extract_form_value(None) == {}
        assert extract_form_value({"action": {"form_value": "junk"}}) == {}


class TestExtractFormAnswer:
    def test_multi_indices_to_json_labels(self) -> None:
        out = extract_form_answer(
            "multi", {"clarify_multi": ["0", "2"]}, ["read", "grep", "bash"]
        )
        assert out == json.dumps(["read", "bash"], ensure_ascii=False)

    def test_multi_single_string_token(self) -> None:
        out = extract_form_answer("multi", {"clarify_multi": "1"}, ["a", "b"])
        assert json.loads(out) == ["b"]

    def test_multi_out_of_range_ignored(self) -> None:
        out = extract_form_answer("multi", {"clarify_multi": ["0", "9"]}, ["a"])
        assert json.loads(out) == ["a"]

    def test_multi_all_invalid_returns_none(self) -> None:
        assert extract_form_answer("multi", {"clarify_multi": []}, ["a"]) is None
        assert extract_form_answer("multi", {"clarify_multi": "x"}, ["a"]) is None

    def test_text_trimmed(self) -> None:
        assert extract_form_answer("text", {"clarify_text": "  hi \n"}, None) == "hi"

    def test_text_empty_returns_none(self) -> None:
        assert extract_form_answer("text", {"clarify_text": "   "}, None) is None

    def test_unknown_kind_returns_none(self) -> None:
        assert extract_form_answer("other", {"x": 1}, None) is None


def _fake_clarify_gateway(monkeypatch, *, multi: bool | None, resolved: list):
    mod = ModuleType("tools.clarify_gateway")

    def get_pending(clarify_id):
        if multi is None:
            raise RuntimeError("no pending")
        return SimpleNamespace(multi_select=multi)

    def resolve_gateway_clarify(clarify_id, answer):
        resolved.append((clarify_id, answer))

    def mark_awaiting_text(clarify_id):
        pass

    mod.get_pending = get_pending
    mod.resolve_gateway_clarify = resolve_gateway_clarify
    mod.mark_awaiting_text = mark_awaiting_text

    tools = sys.modules.get("tools")
    tools_pkg = tools if tools is not None else ModuleType("tools")
    monkeypatch.setitem(sys.modules, "tools", tools_pkg)
    monkeypatch.setitem(sys.modules, "tools.clarify_gateway", mod)
    return mod


class TestSendPathFormRouting:
    def _run_send(self, wrapped, choices):
        async def call():
            return await wrapped(
                MagicMock(), "chat_1", "Q?", choices, "cid_form", "sess_1"
            )

        return asyncio.run(call())

    def _make_ctrl(self):
        mock_client = AsyncMock()
        mock_client.send_card_to_chat = AsyncMock(return_value="msg_form_1")
        ctrl = MagicMock()
        ctrl.enabled = True
        ctrl._client_ok.return_value = True
        ctrl._client = mock_client
        ctrl._sess_items_snapshot.return_value = []
        return ctrl, mock_client

    def test_multi_select_sends_form_card(self, monkeypatch) -> None:
        from hermes_lark_streaming.patching import (
            _clarify_form_kinds,
            _wrap_feishu_adapter_send_clarify,
        )

        _fake_clarify_gateway(monkeypatch, multi=True, resolved=[])
        wrapped = _wrap_feishu_adapter_send_clarify(AsyncMock())
        ctrl, client = self._make_ctrl()
        with (
            patch(
                "hermes_lark_streaming.controller.get_controller", return_value=ctrl
            ),
            patch("hermes_lark_streaming.patching._register_gateway_card"),
        ):
            self._run_send(wrapped, ["A", "B"])
        card = client.send_card_to_chat.call_args.args[1]
        assert _find_tag(card["body"]["elements"], "multi_select_static") is not None
        assert _clarify_form_kinds.get("cid_form") == "multi"
        _clarify_form_kinds.pop("cid_form", None)

    def test_no_choices_sends_text_form_card(self, monkeypatch) -> None:
        from hermes_lark_streaming.patching import (
            _clarify_form_kinds,
            _wrap_feishu_adapter_send_clarify,
        )

        # get_pending 不可用（老 hermes）也应发开放题表单（无选项不依赖 entry）
        _fake_clarify_gateway(monkeypatch, multi=None, resolved=[])
        wrapped = _wrap_feishu_adapter_send_clarify(AsyncMock())
        ctrl, client = self._make_ctrl()
        with (
            patch(
                "hermes_lark_streaming.controller.get_controller", return_value=ctrl
            ),
            patch("hermes_lark_streaming.patching._register_gateway_card"),
        ):
            self._run_send(wrapped, None)
        card = client.send_card_to_chat.call_args.args[1]
        assert _find_tag(card["body"]["elements"], "input") is not None
        assert _clarify_form_kinds.get("cid_form") == "text"
        _clarify_form_kinds.pop("cid_form", None)

    def test_single_select_keeps_legacy_card(self, monkeypatch) -> None:
        from hermes_lark_streaming.patching import (
            _clarify_form_kinds,
            _wrap_feishu_adapter_send_clarify,
        )

        _fake_clarify_gateway(monkeypatch, multi=False, resolved=[])
        wrapped = _wrap_feishu_adapter_send_clarify(AsyncMock())
        ctrl, client = self._make_ctrl()
        with (
            patch(
                "hermes_lark_streaming.controller.get_controller", return_value=ctrl
            ),
            patch("hermes_lark_streaming.patching._register_gateway_card"),
        ):
            self._run_send(wrapped, ["A", "B"])
        card = client.send_card_to_chat.call_args.args[1]
        assert _find_tag(card["body"]["elements"], "form") is None, (
            "单选不应升级表单卡（行为不变）"
        )
        assert _clarify_form_kinds.get("cid_form", "") == ""
        _clarify_form_kinds.pop("cid_form", None)


class TestFormSubmitCallback:
    def _handle(self, monkeypatch, *, form_kind: str, form_value: dict, resolved: list):
        from hermes_lark_streaming.patching import adapter as adapter_mod

        _fake_clarify_gateway(monkeypatch, multi=False, resolved=resolved)
        adapter_mod._clarify_questions["cid_form"] = "Q?"
        adapter_mod._clarify_choices["cid_form"] = ["read", "grep", "bash"]
        adapter_mod._clarify_form_kinds["cid_form"] = form_kind
        adapter_mod._clarify_card_msg_ids.pop("cid_form", None)  # confirm 短路

        adapter_instance = MagicMock()
        adapter_instance._is_interactive_operator_authorized = lambda ou: True
        adapter_instance._loop = None

        data = SimpleNamespace(
            event=SimpleNamespace(
                action=SimpleNamespace(form_value=form_value),
                operator=SimpleNamespace(open_id="ou_1"),
            )
        )
        action_value = {
            "hermes_clarify_action": "form_submit",
            "clarify_id": "cid_form",
            "form_kind": form_kind,
        }
        try:
            return adapter_mod._handle_clarify_card_action(
                adapter_instance, data, "form_submit", action_value
            )
        finally:
            adapter_mod._clarify_questions.pop("cid_form", None)
            adapter_mod._clarify_choices.pop("cid_form", None)
            adapter_mod._clarify_form_kinds.pop("cid_form", None)
            adapter_mod._clarify_selections.pop("cid_form", None)

    def test_multi_submit_resolves_with_json_labels(self, monkeypatch) -> None:
        resolved: list = []
        resp = self._handle(
            monkeypatch,
            form_kind="multi",
            form_value={"clarify_multi": ["0", "2"]},
            resolved=resolved,
        )
        assert resolved == [("cid_form", json.dumps(["read", "bash"], ensure_ascii=False))]
        if not _HAS_CALLBACK_CARD:
            return  # 旧 SDK：落定卡响应降级为空（import-tolerant）
        card_data = resp.card.data
        assert card_data["schema"] == "2.0"
        # 展示行：、拼接 labels
        content = card_data["body"]["elements"][0]["text"]["content"]
        assert "read、bash" in content

    def test_text_submit_resolves_with_trimmed_text(self, monkeypatch) -> None:
        resolved: list = []
        resp = self._handle(
            monkeypatch,
            form_kind="text",
            form_value={"clarify_text": "  free answer "},
            resolved=resolved,
        )
        assert resolved == [("cid_form", "free answer")]
        if _HAS_CALLBACK_CARD:
            assert resp.card.data["schema"] == "2.0"

    def test_empty_submission_keeps_form_alive(self, monkeypatch) -> None:
        """空提交不 resolve、不落定——用户可补填后重提。"""
        resolved: list = []
        resp = self._handle(
            monkeypatch,
            form_kind="text",
            form_value={"clarify_text": "   "},
            resolved=resolved,
        )
        assert resolved == []
        assert resp is None or getattr(resp, "card", None) is None

    def test_expired_state_settles_grey(self, monkeypatch) -> None:
        """无存储状态（TTL 已清）→ 灰色过期落定卡。"""
        from hermes_lark_streaming.patching import adapter as adapter_mod

        resolved: list = []
        _fake_clarify_gateway(monkeypatch, multi=False, resolved=resolved)
        adapter_instance = MagicMock()
        adapter_instance._is_interactive_operator_authorized = lambda ou: True
        adapter_instance._loop = None
        data = SimpleNamespace(
            event=SimpleNamespace(
                action=SimpleNamespace(form_value={"clarify_text": "x"}),
                operator=SimpleNamespace(open_id="ou_1"),
            )
        )
        resp = adapter_mod._handle_clarify_card_action(
            adapter_instance,
            data,
            "form_submit",
            {"hermes_clarify_action": "form_submit", "clarify_id": "cid_gone"},
        )
        assert resolved == []
        if _HAS_CALLBACK_CARD:
            assert resp.card.data["schema"] == "2.0"
            assert resp.card.data["body"]["elements"][0]["icon"]["color"] == "grey"

"""v1.9.0 回归测试 — hermes v0.22.7 适配（reaction wrapper 返回值契约）。

背景（上游考证 2026-11-05，/works/workspace/ai/hermes-agent @ b200f5bc60）：

* hermes v0.22+ 已删除公开 ``add_reaction``/``delete_reaction``，仅剩私有
  ``_add_reaction(message_id, emoji_type) -> Optional[str]``（reaction_id）与
  ``_remove_reaction(message_id, reaction_id) -> bool``（
  ``on_processing_start``/``on_processing_complete`` 的 ack 源）。
* 插件安装逻辑对此的 fallback 是直接 wrap 私有名——但私有方法的返回值被
  上游当作**状态句柄**消费：``on_processing_start`` 把 ``_add_reaction`` 的
  返回值存进 ``_pending_processing_reactions``，``on_processing_complete``
  先按它删除、再依 ``_remove_reaction`` 的 bool 决定是否 pop 缓存并补
  FAILURE 徽标。抑制分支必须按同一契约返回（私有 add 语义返 None =
  "没发成"；私有 remove 语义返 True = "已处理"），伪造 SendResult 会让
  句柄缓存被污染、FAILURE 徽标丢失。
* 已知限制沿用 v1.7.0：上游 reaction 打在用户消息上而非网关卡上，卡片
  状态指示仍处 dormant——本组测试守护的是"契约正确"，未来上游对卡片
  消息发 reaction 时自然激活而不炸缓存。
"""

# pyright: reportMissingImports=false
# 本文件的 hermes_lark_streaming.* 导入由 tests/conftest.py 在运行时注册包名
# （目录名带连字符 ≠ 包名，静态分析器不执行 conftest 故无法解析）——与存量
# 测试（test_v188_fixes.py 等）同一形态，非缺依赖。

from __future__ import annotations

import asyncio
import sys
import types
from typing import Any
from unittest.mock import patch

import pytest
from hermes_lark_streaming.patching import _apply_feishu_adapter_patches
from hermes_lark_streaming.patching import adapter as adapter_mod
from hermes_lark_streaming.patching.adapter import (
    _wrap_feishu_adapter_add_reaction,
    _wrap_feishu_adapter_delete_reaction,
)

_CARD_MSG_ID = "CARDMSG0001"


class _StubSendResult:
    """gateway.platforms.base.SendResult 的测试替身（最小形状）。"""

    def __init__(self, success: bool = True, message_id: Any = None, error: Any = None):
        self.success = success
        self.message_id = message_id
        self.error = error


def _stub_gateway_base(monkeypatch: pytest.MonkeyPatch) -> None:
    module = types.ModuleType("gateway.platforms.base")
    module.SendResult = _StubSendResult  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "gateway.platforms.base", module)


def _card_env(monkeypatch: pytest.MonkeyPatch, card_calls: list[dict]):
    monkeypatch.setattr(
        adapter_mod,
        "_gateway_cards",
        {_CARD_MSG_ID: {"card_id": "cid-1", "category": "system"}},
    )
    monkeypatch.setattr(
        adapter_mod, "_get_config", lambda: types.SimpleNamespace(gateway_cards=True)
    )

    class _Ctrl:
        enabled = True

        async def _do_gateway_card_status(self, **kwargs):
            card_calls.append(kwargs)
            return True

    return patch(
        "hermes_lark_streaming.controller.get_controller", return_value=_Ctrl()
    )


class TestReactionSuppressionContract:
    """抑制分支的返回值必须与被 wrap 目标的返回值契约一致。"""

    def test_private_add_suppression_returns_none(self, monkeypatch) -> None:
        """私有 _add_reaction 语义（Optional[str]）：抑制返 None，不得伪造 SendResult。"""
        card_calls: list[dict] = []
        calls: list[tuple] = []

        async def orig_add(self_feishu, message_id, emoji, **kw):
            calls.append((message_id, emoji))
            return "should-not-be-cached"

        wrapper = _wrap_feishu_adapter_add_reaction(
            orig_add, send_result_semantics=False
        )
        env = _card_env(monkeypatch, card_calls)
        with env:
            result = asyncio.run(
                wrapper(object(), _CARD_MSG_ID, "Typing")
            )
        assert result is None  # on_processing_start 存句柄前先见 None → 不缓存
        assert calls == []  # 实际 reaction 被抑制
        assert card_calls and card_calls[0]["status_label"] == "Processing"

    def test_public_add_suppression_returns_send_result(self, monkeypatch) -> None:
        """公开 add_reaction 语义：抑制返 SendResult(success=True)（旧版契约）。"""
        _stub_gateway_base(monkeypatch)
        card_calls: list[dict] = []

        async def orig_add(self_feishu, message_id, emoji, **kw):
            raise AssertionError("公开语义下抑制时不得触达 orig")

        wrapper = _wrap_feishu_adapter_add_reaction(
            orig_add, send_result_semantics=True
        )
        env = _card_env(monkeypatch, card_calls)
        with env:
            result = asyncio.run(wrapper(object(), _CARD_MSG_ID, "✅"))
        assert isinstance(result, _StubSendResult)
        assert result.success is True

    def test_public_add_suppression_without_gateway_module_returns_none(
        self, monkeypatch
    ) -> None:
        """无 gateway 包环境（测试机常态）：公开语义抑制兜底返 None 不抛。"""
        monkeypatch.delitem(sys.modules, "gateway.platforms.base", raising=False)
        card_calls: list[dict] = []

        async def orig_add(self_feishu, message_id, emoji, **kw):
            return None

        wrapper = _wrap_feishu_adapter_add_reaction(
            orig_add, send_result_semantics=True
        )
        env = _card_env(monkeypatch, card_calls)
        with env:
            result = asyncio.run(wrapper(object(), _CARD_MSG_ID, "Typing"))
        assert result is None

    def test_private_remove_with_reaction_id_passes_through(self, monkeypatch) -> None:
        """私有 _remove_reaction 的第二参数是 reaction_id，永远不命中 emoji 映射——
        实际永远透传（上游 ack 原生行为），orig 的 bool 原样返回，不画卡。
        抑制返 True 分支仅在公开 emoji 形态可达（防御性保留）。"""
        card_calls: list[dict] = []
        calls: list[tuple] = []

        async def orig_remove(self_feishu, message_id, reaction_id, **kw):
            calls.append((message_id, reaction_id))
            return False  # 上游删失败时返 False，wrapper 不得篡改

        wrapper = _wrap_feishu_adapter_delete_reaction(
            orig_remove, send_result_semantics=False
        )
        env = _card_env(monkeypatch, card_calls)
        with env:
            result = asyncio.run(
                wrapper(object(), _CARD_MSG_ID, "rid-not-in-map")
            )
        assert result is False  # 透传 orig 真实返回值
        assert calls == [(_CARD_MSG_ID, "rid-not-in-map")]
        assert card_calls == []

    def test_public_delete_suppression_returns_send_result(self, monkeypatch) -> None:
        """公开 delete_reaction 语义：抑制返 SendResult。"""
        _stub_gateway_base(monkeypatch)
        card_calls: list[dict] = []

        async def orig_delete(self_feishu, message_id, emoji, **kw):
            raise AssertionError("公开语义下抑制时不得触达 orig")

        wrapper = _wrap_feishu_adapter_delete_reaction(
            orig_delete, send_result_semantics=True
        )
        env = _card_env(monkeypatch, card_calls)
        with env:
            result = asyncio.run(wrapper(object(), _CARD_MSG_ID, "Typing"))
        assert isinstance(result, _StubSendResult)

    def test_private_add_passthrough_returns_reaction_id(
        self, monkeypatch
    ) -> None:
        """非网关卡消息：私有语义透传 orig 返回值（reaction_id 原样）。"""
        card_calls: list[dict] = []

        async def orig_add(self_feishu, message_id, emoji, **kw):
            return "rid-123"

        wrapper = _wrap_feishu_adapter_add_reaction(
            orig_add, send_result_semantics=False
        )
        env = _card_env(monkeypatch, card_calls)
        with env:
            result = asyncio.run(wrapper(object(), "USERMSG", "Typing"))
        assert result == "rid-123"
        assert card_calls == []  # 用户消息不画卡

    def test_private_remove_passthrough_ignores_reaction_id_key(
        self, monkeypatch
    ) -> None:
        """非网关卡消息：reaction_id 不在 emoji 映射内，照常透传删除。"""
        card_calls: list[dict] = []
        calls: list[tuple] = []

        async def orig_remove(self_feishu, message_id, reaction_id, **kw):
            calls.append((message_id, reaction_id))
            return True

        wrapper = _wrap_feishu_adapter_delete_reaction(
            orig_remove, send_result_semantics=False
        )
        env = _card_env(monkeypatch, card_calls)
        with env:
            result = asyncio.run(wrapper(object(), "USERMSG", "rid-9"))
        assert result is True
        assert calls == [("USERMSG", "rid-9")]
        assert card_calls == []


class TestInstallOnV022Shape:
    """v0.22.7 形态（仅私有 reaction 名）的安装点契约。"""

    @staticmethod
    def _fake_adapter_cls():
        class _FakeFeishuAdapter:
            async def send(self, *a, **k): ...

            async def edit_message(self, *a, **k): ...

            async def _add_reaction(self, *a, **k):
                return "rid"

            async def _remove_reaction(self, *a, **k):
                return True

            async def send_clarify(self, *a, **k): ...

            async def _handle_card_action_event(self, *a, **k): ...

        return type("PatchedV022Adapter", (_FakeFeishuAdapter,), {})

    def test_private_reaction_names_get_wrapped_with_private_semantics(
        self, monkeypatch
    ) -> None:
        """安装后私有名被替换，且抑制分支按私有契约返回（None / True）。"""
        cls = self._fake_adapter_cls()
        assert _apply_feishu_adapter_patches(cls)
        assert cls.__dict__.get("_add_reaction") is not None
        assert cls.__dict__.get("_remove_reaction") is not None

        card_calls: list[dict] = []
        env = _card_env(monkeypatch, card_calls)
        instance = cls()
        with env:
            add_result = asyncio.run(instance._add_reaction(_CARD_MSG_ID, "Typing"))
            remove_result = asyncio.run(
                instance._remove_reaction(_CARD_MSG_ID, "rid-x")
            )
        # 抑制：句柄缓存不被污染（None），上游缓存照常 pop（True）
        assert add_result is None
        assert remove_result is True
        assert card_calls  # 卡片状态更新确有发生

    def test_v022_passthrough_after_install(self, monkeypatch) -> None:
        """安装后非卡消息透传：用户消息的 ack reaction 行为不变。"""
        cls = self._fake_adapter_cls()
        assert _apply_feishu_adapter_patches(cls)

        card_calls: list[dict] = []
        env = _card_env(monkeypatch, card_calls)
        instance = cls()
        with env:
            add_result = asyncio.run(instance._add_reaction("USERMSG", "Typing"))
        assert add_result == "rid"  # orig 原样返回
        assert card_calls == []

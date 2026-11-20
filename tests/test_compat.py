"""compat/hermes_api.py 测试 — hermes 私有 import 收敛层（v1.11.1）.

三层验证：
1. 无 hermes 环境 import 本模块零副作用（不影响启动路径）
2. 无 hermes 时访问器在**调用点**抛 ImportError（延迟 import 语义）
3. 注入伪 hermes 模块后直通转发正确（类访问器/直通包装）

hermes 运行环境（tests/integration/）下的真表面核对由 S4' 探针负责
（scripts/compat_probe.py，UPSTREAM-ADOPTION P4）。
"""

# pyright: reportMissingImports=false, reportAttributeAccessIssue=false
# hermes_lark_streaming.* 由 tests/conftest.py 运行时注册包名（目录名带连字
# 符 ≠ 包名），gateway.* 等宿主模块仅存在于 hermes 运行环境——静态解析必然
# 误报，文件级豁免（运行时零影响）。

from __future__ import annotations

import sys
from types import ModuleType

import pytest


@pytest.fixture
def no_hermes(monkeypatch):
    """移除全部伪 hermes 模块（隔离用例间的注入）。"""
    for name in list(sys.modules):
        if name.startswith(("gateway.", "agent.", "gateway", "agent")):
            monkeypatch.delitem(sys.modules, name, raising=False)


@pytest.fixture
def fake_hermes(monkeypatch):
    """注入最小伪 hermes 模块树（gateway.* / agent.*）."""
    fake_classes = {}

    def _add(fullname: str, **attrs) -> ModuleType:
        mod = ModuleType(fullname)
        for k, v in attrs.items():
            setattr(mod, k, v)
        monkeypatch.setitem(sys.modules, fullname, mod)
        # 父包也须在 sys.modules（from X.Y.Z import S 遍历 X / X.Y）
        parts = fullname.split(".")
        for i in range(1, len(parts)):
            parent = ".".join(parts[:i])
            if parent not in sys.modules:
                monkeypatch.setitem(sys.modules, parent, ModuleType(parent))
        return mod

    class _FakeSendResult:
        def __init__(self, success: bool = False, message_id: str | None = None):
            self.success = success
            self.message_id = message_id

    class _FakeEphemeral:
        pass

    class _FakePlatform:
        RELAY = "relay"

    class _FakeRunner:
        pass

    class _FakeRelayAdapter:
        pass

    _add("gateway.platforms.base", SendResult=_FakeSendResult, EphemeralReply=_FakeEphemeral)
    _add("gateway.config", Platform=_FakePlatform)
    _add("gateway.run", GatewayRunner=_FakeRunner)
    _add("gateway.relay.adapter", RelayAdapter=_FakeRelayAdapter)
    _add("gateway.platform_registry", platform_registry=object())
    _add("agent.async_utils", safe_schedule_threadsafe=lambda *a, **k: ("scheduled", a, k))
    _add("agent.conversation_loop", run_conversation=lambda *a, **k: "ran")
    _add(
        "agent.display",
        build_tool_preview=lambda name, args: f"preview:{name}",
        redact_tool_args_for_display=lambda name, args: {"redacted": True},
        _detect_tool_failure=lambda name, result: (True, "err"),
    )

    fake_classes.update(
        SendResult=_FakeSendResult,
        EphemeralReply=_FakeEphemeral,
        Platform=_FakePlatform,
        GatewayRunner=_FakeRunner,
        RelayAdapter=_FakeRelayAdapter,
    )
    return fake_classes


class TestZeroSideEffects:
    def test_import_without_hermes_is_silent(self, no_hermes) -> None:
        """无 hermes 环境 import 收敛层零副作用（不抛、不注册任何东西）."""
        import importlib

        import hermes_lark_streaming.compat.hermes_api as m

        importlib.reload(m)  # 重新执行模块体 — 必须不触发任何 gateway/agent import


class TestLazyFailure:
    @pytest.mark.parametrize(
        "accessor",
        [
            "send_result_cls",
            "ephemeral_reply_cls",
            "platform_cls",
            "gateway_runner_cls",
            "platform_registry",
            "relay_adapter_cls",
            "run_conversation_func",
            "conversation_loop_module",
        ],
    )
    def test_class_accessors_raise_at_call_site(self, no_hermes, accessor: str) -> None:
        from hermes_lark_streaming.compat import hermes_api

        with pytest.raises(ImportError):
            getattr(hermes_api, accessor)()

    def test_pass_through_wrappers_raise_at_call_site(self, no_hermes) -> None:
        from hermes_lark_streaming.compat import hermes_api

        with pytest.raises(ImportError):
            hermes_api.safe_schedule_threadsafe(None, None)
        with pytest.raises(ImportError):
            hermes_api.build_tool_preview("read", {})
        with pytest.raises(ImportError):
            hermes_api.redact_tool_args_for_display("read", {})
        with pytest.raises(ImportError):
            hermes_api.detect_tool_failure("read", None)


class TestForwarding:
    def test_class_accessors_return_hermes_symbols(self, fake_hermes) -> None:
        from hermes_lark_streaming.compat import hermes_api

        assert hermes_api.send_result_cls() is fake_hermes["SendResult"]
        assert hermes_api.ephemeral_reply_cls() is fake_hermes["EphemeralReply"]
        assert hermes_api.platform_cls() is fake_hermes["Platform"]
        assert hermes_api.platform_cls().RELAY == "relay"
        assert hermes_api.gateway_runner_cls() is fake_hermes["GatewayRunner"]
        assert hermes_api.relay_adapter_cls() is fake_hermes["RelayAdapter"]

    def test_send_result_construction(self, fake_hermes) -> None:
        from hermes_lark_streaming.compat import hermes_api

        result = hermes_api.send_result_cls()(success=True, message_id="om_x")
        assert result.success is True
        assert result.message_id == "om_x"

    def test_safe_schedule_forwards_args(self, fake_hermes) -> None:
        from hermes_lark_streaming.compat import hermes_api

        out = hermes_api.safe_schedule_threadsafe(
            "coro", "loop", logger="lg", log_message="msg"
        )
        assert out[0] == "scheduled"
        assert out[1] == ("coro", "loop")
        assert out[2] == {"logger": "lg", "log_message": "msg"}

    def test_display_helpers_forward(self, fake_hermes) -> None:
        from hermes_lark_streaming.compat import hermes_api

        assert hermes_api.build_tool_preview("read", {"path": "a"}) == "preview:read"
        assert hermes_api.redact_tool_args_for_display("read", {"path": "a"}) == {
            "redacted": True
        }
        assert hermes_api.detect_tool_failure("read", None) == (True, "err")

    def test_conversation_loop_accessors(self, fake_hermes) -> None:
        from hermes_lark_streaming.compat import hermes_api

        assert hermes_api.run_conversation_func()() == "ran"
        mod = hermes_api.conversation_loop_module()
        assert mod.__name__ == "agent.conversation_loop"

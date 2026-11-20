"""Hermes 私有 API 收敛层 — 全部 hermes 内部 import 的唯一入口.

v1.11.1（docs/UPSTREAM-ADOPTION.md P3，上游 ``compat/hermes_api.py`` 模式）：
hermes 无公开插件 seam，插件直接 import 其私有模块。此前散落全仓
（patching/gateway.py、patching/adapter.py、patching/__init__.py、
__main__.py、patching/callbacks.py），hermes 升级改锚点要全仓找
（v1.9.0 reaction 契约修正即人工适配一轮）。收敛到本模块后下次只改这里。

设计（上游同款）：
- **函数内延迟 import**：保持"找不到时在调用点抛"的原语义——找不到
  hermes 的场景（单测、独立部署）import 本模块零副作用，不影响启动路径。
- **类访问器**（``xxx_cls()`` / ``xxx()``）返回类/模块级单例；**直通包装**
  （``safe_schedule_threadsafe`` 等）转发调用。调用点各自的
  ``try/except (ImportError, ...)`` 降级行为原样保留——本层只收敛
  **模块路径**，不改错误处理策略。
- 范围：``gateway.*`` / ``agent.*`` 模块路径锚点。``tools.clarify_gateway``
  （clarify 子系统，13 处）不在本层——clarify 回调链在表单卡升级
  （UPSTREAM-ADOPTION P5）中统一处理。

新 hermes 版本适配步骤：只改本文件内对应函数的 import 路径，全仓调用点
签名不变。
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "build_tool_preview",
    "conversation_loop_module",
    "detect_tool_failure",
    "ephemeral_reply_cls",
    "gateway_runner_cls",
    "platform_cls",
    "platform_registry",
    "redact_tool_args_for_display",
    "relay_adapter_cls",
    "run_conversation_func",
    "safe_schedule_threadsafe",
    "send_result_cls",
]


# ── gateway.platforms.base — 回复结果类型 ──────────────────────────────


def send_result_cls() -> type:
    """``gateway.platforms.base.SendResult``（成功回复构造/isinstance 用）."""
    from gateway.platforms.base import SendResult

    return SendResult


def ephemeral_reply_cls() -> type:
    """``gateway.platforms.base.EphemeralReply``（透传判定用）."""
    from gateway.platforms.base import EphemeralReply

    return EphemeralReply


# ── gateway.config — 平台枚举 ──────────────────────────────────────────


def platform_cls() -> type:
    """``gateway.config.Platform``（平台枚举 / RELAY 查找用）."""
    from gateway.config import Platform

    return Platform


# ── gateway.run / registry / relay — 网关装配 ──────────────────────────


def gateway_runner_cls() -> type:
    """``gateway.run.GatewayRunner``（import-probe / 版本探测用）."""
    from gateway.run import GatewayRunner

    return GatewayRunner


def platform_registry() -> Any:
    """``gateway.platform_registry.platform_registry``（注册表单例）."""
    from gateway.platform_registry import platform_registry as _pr

    return _pr


def relay_adapter_cls() -> type:
    """``gateway.relay.adapter.RelayAdapter``（relay 补丁目标类）."""
    from gateway.relay.adapter import RelayAdapter

    return RelayAdapter


# ── agent.async_utils — 跨线程调度 ────────────────────────────────────


def safe_schedule_threadsafe(*args: Any, **kwargs: Any) -> Any:
    """``agent.async_utils.safe_schedule_threadsafe`` 直通.

    实参形态随 hermes 版本演进（coro, loop, logger=, log_message=,
    log_level=…），故用 *args/**kwargs 直通，不做签名钉死。
    """
    from agent.async_utils import safe_schedule_threadsafe as _fn

    return _fn(*args, **kwargs)


# ── agent.conversation_loop — 会话主循环（hermes_adapter 多策略解析的
#    Strategy 3 标准路径；锚点解析逻辑本体仍在 patching/hermes_adapter.py）──


def run_conversation_func() -> Any:
    """``agent.conversation_loop.run_conversation`` 函数对象."""
    from agent.conversation_loop import run_conversation

    return run_conversation


def conversation_loop_module() -> Any:
    """``agent.conversation_loop`` 模块对象（monkey-patch 锚点探测用）."""
    import importlib

    return importlib.import_module("agent.conversation_loop")


# ── agent.display — 工具预览/脱敏/失败判定 ────────────────────────────


def build_tool_preview(tool_name: str, display_args: Any) -> Any:
    """``agent.display.build_tool_preview`` 直通（返回 None 表示无预览）."""
    from agent.display import build_tool_preview

    return build_tool_preview(tool_name, display_args)


def redact_tool_args_for_display(tool_name: str, display_args: Any) -> Any:
    """``agent.display.redact_tool_args_for_display`` 直通（脱敏器）."""
    from agent.display import redact_tool_args_for_display

    return redact_tool_args_for_display(tool_name, display_args)


def detect_tool_failure(tool_name: str, result: Any) -> tuple[bool, str]:
    """``agent.display._detect_tool_failure`` 直通（canonical 失败判定）."""
    from agent.display import _detect_tool_failure

    return _detect_tool_failure(tool_name, result)

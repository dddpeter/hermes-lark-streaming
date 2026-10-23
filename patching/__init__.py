"""Runtime monkey patching — replaces AST source injection at import time."""

from __future__ import annotations

import contextvars
import logging
import sys
import threading
import time
from typing import Any, Callable

from .. import __version__

try:
    from .hermes_adapter import HermesCompat
except ImportError:  # pragma: no cover — fallback for pytest-only path
    from hermes_lark_streaming.patching.hermes_adapter import (
        HermesCompat,  # type: ignore[no-redef]
    )

__all__ = [
    # Shared state
    "_thread_local_ctx",
    "_logger",
    "_msg_ctx",
    "_started_msg_ids",
    "_started_msg_ids_lock",
    "_gateway_cards",
    "_gateway_cards_lock",
    "_gw_runner_patched",
    "_patch_status",
    # v1.4.0: FeishuAdapter patched-class registry (deferred loading fix)
    "_patched_feishu_classes",
    # v1.7.0: RelayAdapter patched-class registry (RELAY-fronted deployments)
    "_apply_relay_adapter_patches",
    "_wrap_relay_adapter_send",
    "_wrap_relay_adapter_send_for_platform",
    "_send_result_ok",
    # Functions
    "_get_config",
    "_get_event_message_id",
    "_get_thread_local_ctx",
    "_hls_is_wrapped",
    "_hls_mark_wrapped",
    "_apply_gateway_runner_patches",
    "apply_patches",
    "_apply_direct_agent_patch",
    # FeishuAdapter patch helpers
    "_apply_feishu_adapter_patches",
    "_verify_feishu_patch_identity",
    # v1.8.8: official retire_clarify_card hook (clarify ends without a click)
    "_make_feishu_adapter_retire_clarify",
    # v1.6.0: hook platform_registry.create_adapter — main-chain fix for deferred loading
    "_wrap_platform_registry_create_adapter",
    "_apply_create_adapter_hook",
    # From gateway
    "_wrap_handle_message",
    "_wrap_handle_message_with_agent",
    "_wrap_run_agent",
    "_wrap_run_background_task",
    "_wrap_cron_deliver",
    "_wrap_run_conversation",
    # From callbacks
    "_maybe_wrap_callbacks",
    # From adapter
    "_classify_gateway_message",
    "_wrap_feishu_adapter_send",
    "_register_gateway_card",
    "_unregister_gateway_card",
    "_wrap_feishu_adapter_edit",
    "_wrap_feishu_adapter_add_reaction",
    "_wrap_feishu_adapter_delete_reaction",
    "_wrap_feishu_adapter_send_clarify",
    "_wrap_handle_card_action_event",
    "_handle_clarify_card_action",
    "_REACTION_STATUS_MAP",
    "_clarify_choices",
    "_clarify_questions",
    "_clarify_card_msg_ids",
    "_clarify_selections",
    "_clarify_answers",
    "_clarify_card_info",
    # From hooks
    "on_feishu_normalize",
    "on_message_started",
    "on_message_completed",
    "on_tool_updated",
    "on_answer_delta",
    "on_thinking_delta",
    "on_reasoning_delta",
    "on_background_review_message",
    "on_message_aborted",
    "on_message_interrupted",
    "_safe_hook",
]

# Thread-local storage for context propagation into worker threads
_thread_local_ctx = threading.local()
_thread_local_ctx.data = None

_logger = logging.getLogger("hermes_lark_streaming")


def _get_config():
    from ..config import Config

    return Config()


_msg_ctx: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "hermes_lark_streaming_msg_ctx", default=None
)

_started_msg_ids: set[str] = set()
_started_msg_ids_lock = threading.Lock()

_gateway_cards: dict[str, dict[str, Any]] = {}
_gateway_cards_lock = threading.Lock()

_gw_runner_patched: bool = False

_patch_status: dict[str, Any] = {}

_patched_feishu_classes: set[int] = set()

# When both the module-level patch and the direct AIAgent patch are active,
# The guard prevents the second call from injecting the prefix again.


def _get_event_message_id() -> str | None:
    ctx = _msg_ctx.get()
    if ctx is None:
        ctx = _get_thread_local_ctx()
    if ctx is None:
        return None
    return ctx.get("event_message_id")


def _get_thread_local_ctx() -> dict | None:
    return getattr(_thread_local_ctx, "data", None)


def _send_result_ok(message_id: str | None = None):
    """v1.7.0: Build a success SendResult, import-tolerant for old hermes.

    Lives here (not in patching/adapter.py) so both gateway.py and adapter.py
    can import it from the package without circular imports."""
    try:
        from gateway.platforms.base import SendResult

        if message_id:
            return SendResult(success=True, message_id=message_id)
        return SendResult(success=True)
    except (ImportError, AttributeError):
        return None


# v1.8.4: multiplex duplicate-copy dedupe.
#
# hermes multiplex mode (gateway_multiplex_mode.py: "one gateway per host
# serves every profile") instantiates one PluginManager per profile home
# (hermes_cli/plugins.py get_plugin_manager caches per home) and loads this
# plugin once per profile that has it installed — each copy under a distinct
# module name (plugins_loader._directory_module_name appends __home_<digest>
# for scopes after the first).  Every copy then runs apply_patches() with its
# OWN module-level guards (_applied / _gw_runner_patched), which cannot see
# sibling copies.  Shared targets (GatewayRunner methods, module functions
# like run_conversation / _deliver_result) got wrapped once per copy; each
# wrapper layer fires its own copy's hooks against its own controller
# (independent _sessions store) → one streaming card PER plugin copy per
# message (observed: 3 reply cards on default profile with 3 profile
# installs).
#
# Fix: mark the wrapped callable itself (a shared object — same class /
# module across copies) with a sentinel and refuse to re-wrap.  This mirrors
# the existing class-level sentinels (_hls_patched on FeishuAdapter,
# _hls_direct_patched on AIAgent.run_conversation), which already survive
# across copies for the same reason.


def _hls_is_wrapped(func: Any) -> bool:
    """True when *func* was already wrapped by this or a sibling plugin copy."""
    return bool(getattr(func, "_hls_wrapped", False))


def _hls_mark_wrapped(func: Callable) -> Callable:
    """Mark *func* as HLS-wrapped.  Set AFTER functools.wraps so the sentinel
    is not clobbered by wraps' __dict__ update."""
    try:
        setattr(func, "_hls_wrapped", True)
    except (AttributeError, TypeError):
        pass
    return func


# These imports must come AFTER shared state is defined to avoid circular

from .adapter import (  # noqa: E402
    _REACTION_STATUS_MAP,
    _clarify_answers,
    _clarify_card_info,
    _clarify_card_msg_ids,
    _clarify_choices,
    _clarify_questions,
    _clarify_selections,
    _classify_gateway_message,
    _handle_clarify_card_action,
    _make_feishu_adapter_retire_clarify,
    _register_gateway_card,
    _send_result_ok,
    _unregister_gateway_card,
    _wrap_feishu_adapter_add_reaction,
    _wrap_feishu_adapter_delete_reaction,
    _wrap_feishu_adapter_edit,
    _wrap_feishu_adapter_send,
    _wrap_feishu_adapter_send_clarify,
    _wrap_handle_card_action_event,
    _wrap_relay_adapter_send,
    _wrap_relay_adapter_send_for_platform,
)
from .callbacks import (  # noqa: E402
    _maybe_wrap_callbacks,
)
from .gateway import (  # noqa: E402
    _wrap_cron_deliver,
    _wrap_handle_message,
    _wrap_handle_message_with_agent,
    _wrap_run_agent,
    _wrap_run_background_task,
    _wrap_run_conversation,
)
from .hooks import (  # noqa: E402
    _safe_hook,
    on_answer_delta,
    on_background_review_message,
    on_feishu_normalize,
    on_message_aborted,
    on_message_completed,
    on_message_interrupted,
    on_message_started,
    on_reasoning_delta,
    on_thinking_delta,
    on_tool_updated,
)

# ── Public entry point ─────────────────────────────────────────────


def _apply_gateway_runner_patches(compat: Any | None = None) -> bool:
    """Apply the three critical GatewayRunner method patches.

    v1.8.8: *compat* 可选——调用方（_apply_resolved_targets）传入已解析的
    compat；缺省 fresh 解析（延迟补丁线程/旧调用方兼容）。"""
    global _gw_runner_patched

    if _gw_runner_patched:
        return True  # Already patched (e.g. immediate path succeeded)

    GatewayRunner = (compat if compat is not None else HermesCompat()).gateway_runner_class
    if GatewayRunner is None:
        return False  # Not available yet

    try:
        # Patch each method individually so one missing method
        # doesn't prevent the others from being patched.
        # v1.8.4: each wrap is guarded by a sentinel ON the patched method
        # (shared class object) so sibling plugin copies under multiplex
        # (one module instance per profile home) cannot double-wrap —
        # double-wrap = hooks fire N times = one streaming card per copy.
        # v1.8.7: sentinel 检查+wrap 必须在**类级锁**内进行——两个副本的延迟
        # 补丁线程几乎同时醒来时（gateway.run 加载后 TOCTOU 窗口），裸 sentinel
        # 检查会双双通过并嵌套双 wrap（生产日志实证：default/coder 两副本
        # controller 同时活跃）。锁挂共享类上，跨副本互斥。
        # v1.8.7: 类级锁（挂在共享类上，跨副本互斥）内完成 sentinel 检查+wrap，
        # 消除两个副本延迟补丁线程并发醒来的 TOCTOU 双 wrap 竞态。
        _gw_lock = getattr(GatewayRunner, "_hls_patch_lock", None)
        if _gw_lock is None:
            _gw_lock = threading.Lock()
            setattr(GatewayRunner, "_hls_patch_lock", _gw_lock)

        with _gw_lock:
            _patched_methods = []
            if hasattr(GatewayRunner, "_handle_message"):
                _m = GatewayRunner._handle_message
                if not _hls_is_wrapped(_m):
                    GatewayRunner._handle_message = _hls_mark_wrapped(
                        _wrap_handle_message(_m)
                    )
                _patched_methods.append("_handle_message")
            else:
                _logger.warning(
                    "hermes-lark-streaming: GatewayRunner._handle_message not found, skipping patch"
                )

            if hasattr(GatewayRunner, "_handle_message_with_agent"):
                _m = GatewayRunner._handle_message_with_agent
                if not _hls_is_wrapped(_m):
                    GatewayRunner._handle_message_with_agent = _hls_mark_wrapped(
                        _wrap_handle_message_with_agent(_m)
                    )
                _patched_methods.append("_handle_message_with_agent")
            else:
                _logger.warning(
                    "hermes-lark-streaming: GatewayRunner._handle_message_with_agent not found, skipping patch"
                )

            if hasattr(GatewayRunner, "_run_agent"):
                _m = GatewayRunner._run_agent
                if not _hls_is_wrapped(_m):
                    GatewayRunner._run_agent = _hls_mark_wrapped(_wrap_run_agent(_m))
                _patched_methods.append("_run_agent")
            else:
                _logger.warning(
                    "hermes-lark-streaming: GatewayRunner._run_agent not found, skipping patch"
                )

            try:
                _m = GatewayRunner._run_background_task
                if not _hls_is_wrapped(_m):
                    GatewayRunner._run_background_task = _hls_mark_wrapped(
                        _wrap_run_background_task(_m)
                    )
                _patched_methods.append("_run_background_task")
            except AttributeError:
                _logger.debug(
                    "hermes-lark-streaming: _run_background_task not found, background cards disabled"
                )

            if not _patched_methods:
                _logger.error(
                    "hermes-lark-streaming: GatewayRunner patch FAILED — "
                    "no methods found. Streaming cards will NOT work."
                )
                return False

            _gw_runner_patched = True
            _logger.info(
                "hermes-lark-streaming: GatewayRunner patched methods: %s",
                ", ".join(_patched_methods),
            )
            return True
    except (ImportError, AttributeError) as e:
        _logger.error(
            "hermes-lark-streaming: GatewayRunner patch FAILED — "
            "gateway.run found but incompatible. "
            "Streaming cards will NOT work. Error: %s",
            e,
        )
        return False


def _likely_gateway_process() -> bool:
    """v1.8.8: 判定本进程是否会运行网关（缺失目标是否值得开延迟补丁线程）。

    headless CLI（hermes chat / plugins / cron 子进程）永不重试——fresh
    import 重型模块（run_agent 冷导入实测 ~15s）只会白费并触发插件加载器
    10s 超时；网关进程 argv 带 gateway/serve，或 gateway.run 已（部分）加载。"""
    if sys.modules.get("gateway.run") is not None:
        return True
    argv = {str(a).lower() for a in sys.argv[1:]}
    return bool(argv & {"gateway", "serve"})


def _apply_resolved_targets(compat: Any, *, sys_modules_only: bool) -> dict[str, Any]:
    """把 *compat* 的解析结果落到补丁上（eager 与延迟补丁线程共用）。

    v1.8.8 抽出：apply_patches（register 阶段，sys_modules_only 解析）与
    延迟补丁线程（fresh 解析）走同一落补丁路径。所有 wrap 均有跨副本
    哨兵（v1.8.4），重复调用幂等。
    """
    global _patch_status

    # ── Patch GatewayRunner ──
    # This is the core patch — without it, streaming cards cannot work.
    gw_patched = _apply_gateway_runner_patches(compat)
    if gw_patched:
        _logger.info("hermes-lark-streaming: GatewayRunner patched ✓")

    _module_patch_applied = False
    if compat.has_conversation_loop:
        _cl_mod = compat.conversation_loop_module
        _cl_run_conversation = compat.conversation_loop_func
        try:
            # v1.8.4: sentinel on the shared module function — sibling plugin
            # copies under multiplex must not re-wrap.
            if _hls_is_wrapped(_cl_run_conversation):
                _module_patch_applied = True
                _logger.info(
                    "hermes-lark-streaming: agent.conversation_loop already "
                    "wrapped (multiplex duplicate copy detected), skipping"
                )
            else:
                _cl_mod.run_conversation = _hls_mark_wrapped(
                    _wrap_run_conversation(_cl_run_conversation)
                )
                _module_patch_applied = True
                _logger.info(
                    "hermes-lark-streaming: agent.conversation_loop module patched ✓"
                )
        except (AttributeError, TypeError) as e:
            _logger.warning(
                "hermes-lark-streaming: agent.conversation_loop found but "
                "patch failed (%s). Falling back to direct AIAgent patch.",
                e,
            )

    if not _module_patch_applied:
        # Hermes <v0.10 OR module not loaded yet: use direct AIAgent patch
        _logger.info(
            "hermes-lark-streaming: using direct AIAgent patch "
            "(conversation_loop %s)",
            "not loaded yet" if sys_modules_only else "incompatible",
        )

    _apply_direct_agent_patch(compat)

    cron_patched = False
    if compat.has_cron_scheduler:
        try:
            _cron_mod = compat.cron_scheduler_module
            # v1.8.4: sentinel on the shared module function — sibling plugin
            # copies under multiplex must not re-wrap.
            _cron_orig = _cron_mod._deliver_result
            if _hls_is_wrapped(_cron_orig):
                cron_patched = True
                _logger.info(
                    "hermes-lark-streaming: cron scheduler already wrapped "
                    "(multiplex duplicate copy detected), skipping"
                )
            else:
                _cron_mod._deliver_result = _hls_mark_wrapped(
                    _wrap_cron_deliver(_cron_orig)
                )
                cron_patched = True
                _logger.info(
                    "hermes-lark-streaming: cron scheduler patched ✓ (module=%s)",
                    getattr(_cron_mod, "__name__", "?"),
                )
        except (AttributeError, TypeError) as e:
            _logger.debug("hermes-lark-streaming: cron.scheduler patch failed (%s)", e)

    feishu_patched = False
    FeishuAdapter = compat.feishu_adapter_class
    if FeishuAdapter is not None:
        feishu_patched = _apply_feishu_adapter_patches(FeishuAdapter, is_repatch=False)
    else:
        _logger.info(
            "hermes-lark-streaming: FeishuAdapter not available via HermesCompat, patch skipped"
        )

    # v1.6.0: hook platform_registry.create_adapter — main-chain fix for
    # hermes v0.17.0+ bundled platform deferred loading.  The FeishuAdapter
    # class resolved above may be a "替身" (source-path class A) because the
    # gateway's "真身" (hermes_plugins.feishu_platform.adapter class B) is not
    # loaded yet at apply_patches() time.  v1.4.0 used 2s+10s timer repatch
    # (赌时窗，治标); v1.5.0 replaced it with on-demand repatch inside
    # _wrap_feishu_adapter_send (chicken-and-egg 死结: wrapper only installed
    # on already-patched class, so unpatched 真身 never triggers it; and
    # clarify goes through send_clarify not send, so even a working on-demand
    # check on send cannot save clarify).  v1.6.0 hooks the single public
    # adapter-creation entry — platform_registry.create_adapter — so every
    # FeishuAdapter instance hermes ever creates (initial / reconnect / multiplex)
    # has its class patched BEFORE it is handed to callers.  No timer, no
    # chicken-and-egg, covers clarify (send_clarify) and all other methods.
    create_adapter_hooked = _apply_create_adapter_hook(
        sys_modules_only=sys_modules_only
    )

    # v1.7.0 (P1 RELAY fix): best-effort direct patch of the RelayAdapter
    # class. The create_adapter hook above is the main chain (relay adapters
    # are built via platform_registry.create_adapter("relay", config)); this
    # direct import covers a gateway that already imported gateway.relay
    # before plugin registration. Missing module → not a relay deployment.
    relay_patched = False
    if sys_modules_only:
        _relay_mod = sys.modules.get("gateway.relay.adapter")
        if _relay_mod is not None:
            _RelayAdapter = getattr(_relay_mod, "RelayAdapter", None)
            if _RelayAdapter is not None:
                relay_patched = _apply_relay_adapter_patches(_RelayAdapter)
    else:
        try:
            from gateway.relay.adapter import RelayAdapter as _RelayAdapter

            relay_patched = _apply_relay_adapter_patches(_RelayAdapter)
        except ImportError:
            relay_patched = False
        except Exception:
            _logger.debug(
                "hermes-lark-streaming: direct RelayAdapter patch failed", exc_info=True
            )

    return {
        "gw_patched": gw_patched,
        "conversation_loop": _module_patch_applied,
        "cron_patched": cron_patched,
        "feishu_patched": feishu_patched,
        "create_adapter_hooked": create_adapter_hooked,
        "relay_patched": relay_patched,
    }


def _apply_deferred_once() -> bool:
    """延迟补丁线程单轮：fresh 解析 + 落补丁（幂等）。True = GatewayRunner 已就绪。"""
    compat = HermesCompat()
    _apply_resolved_targets(compat, sys_modules_only=False)
    return _gw_runner_patched


def apply_patches() -> None:
    """Apply all runtime monkey patches to ``GatewayRunner`` and ``AIAgent``.

    v1.8.8: register 阶段一律 sys_modules_only 解析（零 fresh import）——
    headless CLI 进程 fresh import run_agent/gateway.run 会拖上整棵
    agent/gateway 模块树（run_agent 冷导入实测 ~15s），超过 hermes 插件
    加载器 10s 窗口 → "load timed out after 10s"。补丁就绪的网关进程里
    这些模块已在 sys.modules（eager 路径不变）；冷启动的网关进程由下方
    延迟补丁线程在窗口外用 fresh 模式补齐全部目标（不再只补
    GatewayRunner）；headless 进程不开线程、静默跳过。
    """
    if getattr(apply_patches, "_applied", False):
        return
    apply_patches._applied = True  # type: ignore[attr-defined]

    _logger.info("hermes-lark-streaming v%s: apply_patches() starting", __version__)

    compat = HermesCompat(sys_modules_only=True)
    # ``layout`` is kept for the doctor CLI's ``hermes_layout`` print and
    # for parity with the legacy ``_detect_hermes_layout()`` contract.
    layout = compat.get_layout_report()

    status = _apply_resolved_targets(compat, sys_modules_only=True)
    gw_patched = status["gw_patched"]
    gw_delayed = False

    if not gw_patched and _likely_gateway_process():
        # gateway.run not yet loaded — start delayed-patch poll thread
        # (fresh imports live OUTSIDE the plugin-loader 10s window)
        _logger.info(
            "hermes-lark-streaming: gateway.run not loaded yet — "
            "starting delayed patch poll (2s interval, 60s timeout)",
        )
        gw_delayed = True

        def _delayed_gw_patch():
            """Poll for gateway.run and apply ALL deferred patches once available."""
            deadline = time.monotonic() + 60.0  # 60-second timeout
            while time.monotonic() < deadline:
                time.sleep(2.0)  # Poll every 2 seconds
                if _apply_deferred_once():
                    _logger.info(
                        "hermes-lark-streaming: GatewayRunner patched (delayed) ✓"
                    )
                    return
            # Timeout — gateway.run never became available
            _logger.error(
                "hermes-lark-streaming: gateway.run NOT FOUND after 60s — "
                "this Hermes version may be too old or installed incorrectly. "
                "Streaming cards will NOT work. "
                "Please check: 1) Hermes is running via gateway mode, "
                "2) Hermes version >= v0.5.0, "
                "3) Re-run: hermes setup && hermes gateway start",
            )

        _delayed_thread = threading.Thread(target=_delayed_gw_patch, daemon=True)
        _delayed_thread.start()
    elif not gw_patched:
        _logger.info(
            "hermes-lark-streaming: headless context (no gateway modules "
            "loaded, argv=%s) — deferred patch poll skipped, streaming "
            "cards stay gateway-only",
            sys.argv[1:3],
        )

    # ── Summary ──
    # v1.1.0: Record patch status in a structured dict for doctor command
    global _patch_status
    _patch_status = {
        "version": __version__,
        "gateway_runner": "✓" if gw_patched else ("pending" if gw_delayed else "✗"),
        "conversation_loop": "✓" if status["conversation_loop"] else "n/a (direct AIAgent)",
        "aiagent_direct": "applied",
        "cron_scheduler": "✓" if status["cron_patched"] else "n/a",
        "background_task": "✓" if gw_patched else ("pending" if gw_delayed else "n/a"),
        "feishu_adapter": "✓" if status["feishu_patched"] else "✗",
        "create_adapter_hook": "✓" if status["create_adapter_hooked"] else "✗",
        "relay_adapter": "✓" if status["relay_patched"] else "n/a (hook-covered)",
        "hermes_layout": layout,
    }
    _logger.info(
        "HLS: patch summary v%s — GatewayRunner=%s conversation_loop=%s "
        "AIAgent=applied cron=%s background=%s FeishuAdapter=%s create_adapter_hook=%s relay=%s layout=%s",
        __version__,
        _patch_status["gateway_runner"],
        _patch_status["conversation_loop"],
        _patch_status["cron_scheduler"],
        _patch_status["background_task"],
        _patch_status["feishu_adapter"],
        _patch_status["create_adapter_hook"],
        _patch_status["relay_adapter"],
        layout,
    )


def _apply_feishu_adapter_patches(FeishuAdapter, *, is_repatch: bool = False) -> bool:
    """Apply all FeishuAdapter method patches to the given class."""
    if FeishuAdapter is None:
        return False

    # v1.7.0 (R1-06): the sentinel attr is the PRIMARY dedupe — id() values
    # can be reused after GC (hermes hot module reload), which previously
    # made this id-only set skip a freshly reloaded class. The id set stays
    # as a mirror for compatibility with existing tests / doctor output.
    if (
        getattr(FeishuAdapter, "_hls_patched", False)
        or id(FeishuAdapter) in _patched_feishu_classes
    ):
        if is_repatch:
            pass
        return True

    try:
        FeishuAdapter.send = _wrap_feishu_adapter_send(FeishuAdapter.send)
        try:
            FeishuAdapter.edit_message = _wrap_feishu_adapter_edit(
                FeishuAdapter.edit_message
            )
        except AttributeError:
            _logger.debug(
                "hermes-lark-streaming: FeishuAdapter.edit_message not found, edit interception skipped"
            )
        try:
            FeishuAdapter.add_reaction = _wrap_feishu_adapter_add_reaction(
                FeishuAdapter.add_reaction
            )
        except AttributeError:
            try:
                FeishuAdapter._add_reaction = _wrap_feishu_adapter_add_reaction(
                    FeishuAdapter._add_reaction
                )
            except AttributeError:
                _logger.debug(
                    "hermes-lark-streaming: FeishuAdapter.add_reaction/_add_reaction not found, reaction interception skipped"
                )
        try:
            FeishuAdapter.delete_reaction = _wrap_feishu_adapter_delete_reaction(
                FeishuAdapter.delete_reaction
            )
        except AttributeError:
            try:
                FeishuAdapter._remove_reaction = _wrap_feishu_adapter_delete_reaction(
                    FeishuAdapter._remove_reaction
                )
            except AttributeError:
                _logger.debug(
                    "hermes-lark-streaming: FeishuAdapter.delete_reaction/_remove_reaction not found, reaction interception skipped"
                )
        # NOTE(v0.15.4): send_image_file / send_image interceptors DELETED (2026-06-09).

        try:
            FeishuAdapter.send_clarify = _wrap_feishu_adapter_send_clarify(
                FeishuAdapter.send_clarify
            )
            _logger.info(
                "hermes-lark-streaming: FeishuAdapter.send_clarify patched ✓ (clarify interactive card)"
            )
        except AttributeError:
            _logger.debug(
                "hermes-lark-streaming: FeishuAdapter.send_clarify not found, clarify card skipped"
            )
        try:
            FeishuAdapter._handle_card_action_event = _wrap_handle_card_action_event(
                FeishuAdapter._handle_card_action_event
            )
            _logger.info(
                "hermes-lark-streaming: FeishuAdapter._handle_card_action_event patched ✓ (card action /card suppression)"
            )
        except AttributeError:
            _logger.debug(
                "hermes-lark-streaming: FeishuAdapter._handle_card_action_event not found, /card suppression skipped"
            )

        # v1.8.8: retire_clarify_card — 官方 clarify 收尾钩子（clarify 未点击而
        # 结束时 gateway 调用，要求持久卡停用按钮）。stock FeishuAdapter 未实现
        # （base 声明 MAY-style）；若类自身已定义（未来 hermes 原生实现）， ours
        # 包装原方法（先调 orig，插件卡收尾其后）。__dict__ 只看本类自有方法，
        # 不误收基类/其它 adapter（如 slack）的实现。
        try:
            _orig_retire = FeishuAdapter.__dict__.get("retire_clarify_card")
            FeishuAdapter.retire_clarify_card = _make_feishu_adapter_retire_clarify(
                _orig_retire
            )
            _logger.info(
                "hermes-lark-streaming: FeishuAdapter.retire_clarify_card installed ✓ "
                "(clarify cards retire on typed answer / timeout / cancellation)"
            )
        except (AttributeError, TypeError) as e:
            _logger.debug(
                "hermes-lark-streaming: retire_clarify_card install failed (%s)", e
            )

        # Record this class as patched AFTER successful patch (only on success,
        # so a failed attempt can be retried later in the deferred stage).
        # v1.7.0 (R1-06): sentinel attr first (GC-safe), id set kept as mirror.
        try:
            FeishuAdapter._hls_patched = True  # type: ignore[attr-defined]
        except (AttributeError, TypeError):
            pass  # e.g. built-in/extension classes — id mirror still set below
        _patched_feishu_classes.add(id(FeishuAdapter))
        _logger.info(
            "hermes-lark-streaming: FeishuAdapter.send/edit/reaction/image/clarify patched ✓ "
            "(gateway message cards enabled, class_id=%s)",
            id(FeishuAdapter),
        )
        return True
    except AttributeError as e:
        _logger.info("hermes-lark-streaming: FeishuAdapter patch skipped (%s)", e)
        return False


def _apply_relay_adapter_patches(RelayAdapter, *, is_repatch: bool = False) -> bool:
    """v1.7.0: Apply RELAY-mode patches to the RelayAdapter class (P1 fix).

    In relay-fronted deployments hermes routes ALL Feishu-bound sends through
    the single RelayAdapter (DeliveryTransport.send → send_for_platform for
    the cron/scheduled lane; RelayAdapter.send for interactive traffic). The
    plugin's FeishuAdapter patches never fire there, so cron pushes degraded
    to plain text. Wrapping RelayAdapter.send / send_for_platform routes
    Feishu-bound relay sends through the SAME _dispatch_feishu_outbound logic
    (turn-final suppression, /stop detection, cron cards, gateway cards).

    Dedupe uses a GC-safe sentinel attr (same rationale as R1-06); the id set
    is only a mirror. Non-Feishu platforms pass through untouched — the
    per-send platform check lives in the wrappers.
    """
    if RelayAdapter is None:
        return False

    if getattr(RelayAdapter, "_hls_relay_patched", False):
        return True

    try:
        RelayAdapter.send = _wrap_relay_adapter_send(RelayAdapter.send)
        try:
            RelayAdapter.send_for_platform = _wrap_relay_adapter_send_for_platform(
                RelayAdapter.send_for_platform
            )
        except AttributeError:
            _logger.debug(
                "hermes-lark-streaming: RelayAdapter.send_for_platform not found "
                "(older hermes relay) — relay cron lane skipped, send lane still patched"
            )
        try:
            RelayAdapter._hls_relay_patched = True  # type: ignore[attr-defined]
        except (AttributeError, TypeError):
            pass
        _logger.info(
            "hermes-lark-streaming: RelayAdapter.send/send_for_platform patched ✓ "
            "(RELAY-fronted Feishu deployments — cron/gateway cards via relay lane, "
            "class_id=%s)",
            id(RelayAdapter),
        )
        return True
    except AttributeError as e:
        _logger.info("hermes-lark-streaming: RelayAdapter patch skipped (%s)", e)
        return False


def _verify_feishu_patch_identity(adapter_instance: Any) -> bool:
    """Verify that an adapter instance's class has been patched by HLS."""
    if adapter_instance is None:
        return False
    cls = type(adapter_instance)
    # v1.7.0 (R1-06): sentinel attr first (GC-safe), id set as mirror.
    if getattr(cls, "_hls_patched", False) or id(cls) in _patched_feishu_classes:
        return True
    _logger.error(
        "HLS: FeishuAdapter identity mismatch! adapter instance class id=%s "
        "not in patched classes %s. Clarify/delegate cards will fall back to "
        "text. Run /aowen doctor.",
        id(cls),
        sorted(_patched_feishu_classes),
    )
    return False


def _wrap_platform_registry_create_adapter(orig_create_adapter: Callable) -> Callable:
    """v1.6.0: wrap platform_registry.create_adapter so every adapter instance
    hermes creates has its class patched BEFORE it reaches callers.

    This is the main-chain fix for hermes v0.17.0+ bundled platform deferred
    loading.  See _apply_create_adapter_hook docstring for the full rationale.
    """

    def _wrapped(name, config):
        adapter = orig_create_adapter(name, config)
        if adapter is None:
            return adapter
        # Only patch feishu adapters — other platforms (irc/telegram/...) are
        # none of our business.  ``name`` is platform.value ("feishu"/"lark");
        # also sniff the class module as a belt-and-suspenders match.
        _is_feishu = False
        if isinstance(name, str) and name.lower() in ("feishu", "lark"):
            _is_feishu = True
        else:
            _cls_mod = getattr(type(adapter), "__module__", "") or ""
            if "feishu" in _cls_mod.lower():
                _is_feishu = True
        if _is_feishu:
            cls = type(adapter)
            # v1.7.0 (R1-06): sentinel attr is primary; id set is a mirror.
            if (
                getattr(cls, "_hls_patched", False)
                or id(cls) in _patched_feishu_classes
            ):
                return adapter
            try:
                _apply_feishu_adapter_patches(cls, is_repatch=True)
                _logger.info(
                    "HLS: FeishuAdapter class patched at create_adapter hook "
                    "(class_id=%s, deferred loading intercepted, name=%s)",
                    id(cls),
                    name,
                )
            except Exception as e:
                _logger.warning(
                    "HLS: create_adapter hook patch failed (class_id=%s name=%s): %s",
                    id(cls),
                    name,
                    e,
                )
            return adapter

        # v1.7.0 (P1 RELAY fix): relay adapters front N logical platforms and
        # are created through the SAME platform_registry.create_adapter entry
        # (gateway/relay/__init__.py register_relay_adapter registers a
        # factory). Patching the class here means relay-fronted Feishu
        # deployments get cron/gateway cards through the relay lane.
        _is_relay = False
        if isinstance(name, str) and name.lower() == "relay":
            _is_relay = True
        else:
            _cls_mod = getattr(type(adapter), "__module__", "") or ""
            if _cls_mod.startswith("gateway.relay"):
                _is_relay = True
        if _is_relay:
            try:
                _apply_relay_adapter_patches(type(adapter), is_repatch=True)
            except Exception as e:
                _logger.warning(
                    "HLS: create_adapter hook relay patch failed (name=%s): %s",
                    name,
                    e,
                )
        return adapter

    _wrapped._hls_create_adapter_wrapped = True  # type: ignore[attr-defined]
    return _wrapped


def _apply_create_adapter_hook(*, sys_modules_only: bool = False) -> bool:
    """v1.6.0: install the platform_registry.create_adapter hook.

    Why this is the main-chain fix (not a fallback/compat shim):

    - hermes v0.17.0+ loads bundled platforms (feishu/telegram/...) via a
      *deferred loader*: the real FeishuAdapter class object is only created
      when the gateway first asks for it, which happens AFTER the plugin's
      apply_patches() runs at startup.  So apply_patches() sees only the
      source-path "替身" class A, patches it, but the gateway later builds a
      different "真身" class B object (same source, different module object →
      different class object) and uses class B instances.  Class B is never
      patched → clarify/delegate/cards fall back to hermes' plain-text
      BasePlatformAdapter defaults.

    - v1.4.0 fixed this with a 2s+10s timer that re-resolved & re-patched
      class B.  That works but bets on a time window (fragile if hermes ever
      loads slower) and is a "兜底" pattern, not a main-chain fix.

    - v1.5.0 replaced the timer with an on-demand repatch inside
      _wrap_feishu_adapter_send.  That has a chicken-and-egg deadlock: the
      on-demand check is itself a wrapper installed only on already-patched
      classes, so the unpatched 真身 never runs it; and clarify dispatches
      through send_clarify (not send), so even a working on-demand check on
      send cannot save clarify.

    - v1.6.0 hooks the single PUBLIC adapter-creation entry,
      platform_registry.create_adapter (gateway/platform_registry.py:278),
      which ALL four adapter-creation paths (initial / reconnect / multiplex
      start / multiplex reconnect) funnel through.  Every FeishuAdapter
      instance hermes ever builds has its class patched before it is returned
      to callers — no timer, no chicken-and-egg, covers send_clarify and
      every other method.  platform_registry.py had ZERO commits between
      hermes v0.17.0 and v0.19.0 (22 days, 2976 commits), so this hook point
      is extremely stable.

    Returns True if the hook was installed (or was already installed).

    v1.8.8: *sys_modules_only* —— register 阶段只认已加载的
    gateway.platform_registry（fresh import 属重型路径，留给延迟补丁线程）。
    """
    if sys_modules_only:
        _pr = sys.modules.get("gateway.platform_registry")
        if _pr is None:
            _logger.debug(
                "hermes-lark-streaming: platform_registry not loaded yet "
                "(register phase), create_adapter hook deferred"
            )
            return False
    else:
        try:
            from gateway.platform_registry import platform_registry as _pr
        except (ImportError, AttributeError):
            _logger.info(
                "hermes-lark-streaming: platform_registry not available yet, "
                "create_adapter hook deferred (deferred patch thread will retry)"
            )
            return False

    _current = getattr(_pr, "create_adapter", None)
    if _current is None:
        _logger.info(
            "hermes-lark-streaming: platform_registry.create_adapter missing, "
            "create_adapter hook skipped"
        )
        return False

    if getattr(_current, "_hls_create_adapter_wrapped", False):
        return True  # already wrapped

    _pr.create_adapter = _wrap_platform_registry_create_adapter(_current)
    _logger.info(
        "hermes-lark-streaming: platform_registry.create_adapter hooked ✓ "
        "(main-chain deferred-loading fix — every FeishuAdapter instance gets "
        "its class patched at creation)"
    )
    return True


def _apply_direct_agent_patch(compat: Any | None = None) -> None:
    """Directly patch AIAgent.run_conversation as belt-and-suspenders.

    v1.8.8: *compat* 可选——register 阶段传入 sys_modules_only 解析结果
    （headless 进程 AIAgent 未加载时此处不做 fresh import）。"""
    AIAgent = (compat if compat is not None else HermesCompat()).aiagent_class
    if AIAgent is None:
        _logger.info(
            "hermes-lark-streaming: AIAgent.run_conversation direct patch deferred (run_agent not yet loaded)"
        )
        return

    try:
        _orig_method = AIAgent.run_conversation

        # Guard: skip if already patched
        if getattr(_orig_method, "_hls_direct_patched", False):
            _logger.info(
                "hermes-lark-streaming: AIAgent.run_conversation already directly patched, skip"
            )
            return

        # v1.3.4 fix (P1): inspect.signature 可能对 C 扩展/wrapped callable 抛异常
        import inspect

        try:
            _has_persist_ts = (
                "persist_user_timestamp" in inspect.signature(_orig_method).parameters
            )
        except (ValueError, TypeError):
            _has_persist_ts = False

        def _patched_run_conversation(
            self,
            user_message,
            system_message=None,
            conversation_history=None,
            task_id=None,
            stream_callback=None,
            persist_user_message=None,
            persist_user_timestamp=None,
            **kwargs,
        ):
            # v1.3.0: inject_time removed — Hermes v0.17.0+ has built-in
            # gateway.message_timestamps.enabled for this purpose.

            _maybe_wrap_callbacks(self)
            try:
                # 用关键字参数传递，兼容有/无 persist_user_timestamp 的 Hermes 版本
                # 如果原方法不支持 persist_user_timestamp，它会被 **kwargs 吞掉
                call_kwargs = {
                    "system_message": system_message,
                    "conversation_history": conversation_history,
                    "task_id": task_id,
                    "stream_callback": stream_callback,
                    "persist_user_message": persist_user_message,
                }
                # v1.3.0 perf: cache inspect.signature result at wrap time
                # (the signature never changes at runtime — was ~10-50μs/message wasted)
                if _has_persist_ts:
                    call_kwargs["persist_user_timestamp"] = persist_user_timestamp
                call_kwargs.update(kwargs)
                return _orig_method(self, user_message, **call_kwargs)
            finally:
                pass  # v1.3.0: inject_time guard removed

        _patched_run_conversation._hls_direct_patched = True
        AIAgent.run_conversation = _patched_run_conversation
        _logger.info("hermes-lark-streaming: AIAgent.run_conversation patched directly")
    except AttributeError as e:
        _logger.info(
            "hermes-lark-streaming: AIAgent.run_conversation direct patch deferred (run_agent not yet loaded: %s)",
            e,
        )

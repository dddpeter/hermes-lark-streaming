"""Hermes compatibility adapter — isolates all Hermes internal interface access."""

from __future__ import annotations

import importlib
import importlib.util
import logging
import sys
from pathlib import Path
from typing import Any

_logger = logging.getLogger("hermes_lark_streaming")


class HermesCompat:
    """Encapsulates all Hermes internal module access."""

    # 类级默认：测试用 __new__ 绕过 __init__ 时 _import 仍可判定模式。
    _sys_modules_only: bool = False

    def __init__(self, *, sys_modules_only: bool = False):
        """v1.8.8: *sys_modules_only* — 只从 sys.modules 解析，不做任何
        fresh import（register 阶段专用）。

        headless CLI 进程（hermes chat / plugins / cron 子进程）里 fresh
        import 这些模块会拖上整棵 agent/gateway 模块树（实测 run_agent
        冷导入 ~15s，远超 hermes 插件加载器 10s 窗口 → "load timed out
        after 10s"）；更早的 A2A 场景直接死锁插件发现（见 plugin/
        __init__.py 的 HERMES_A2A_PEER 守卫）。补丁就绪的进程里这些模块
        必然已在 sys.modules；冷启动的网关进程由 patching 的延迟补丁
        线程在加载器窗口之外用 fresh 模式补齐。
        """
        self._sys_modules_only = sys_modules_only
        self._detect_version()
        self._resolve_modules()

    def _import(self, mod_path: str) -> Any | None:
        """按模式导入：sys_modules_only 只查缓存（缺失返回 None 不抛）；
        fresh 模式 importlib.import_module（ImportError 吞掉返回 None）。"""
        if self._sys_modules_only:
            return sys.modules.get(mod_path)
        try:
            return importlib.import_module(mod_path)
        except ImportError:
            return None

    def _detect_version(self) -> None:
        """Detect Hermes version from various sources."""
        self.hermes_version: str = "unknown"

        # Try importlib.metadata
        try:
            from importlib.metadata import version

            self.hermes_version = version("hermes-agent")
        except Exception:
            pass

        # Try __version__ attribute
        if self.hermes_version == "unknown":
            try:
                import hermes_cli

                self.hermes_version = getattr(hermes_cli, "__version__", "unknown")
            except Exception:
                pass

        _logger.info("HLS: Hermes version detected: %s", self.hermes_version)

    def _resolve_modules(self) -> None:
        """Resolve all Hermes internal modules, recording what's available.

        v1.8.8: 全部经 self._import（sys_modules_only 模式下零 fresh
        import）。部分初始化（正在导入中）的模块取属性失败按缺失处理，
        交给延迟补丁线程兜底。
        """
        self.gateway_runner_class: Any | None = None
        self.aiagent_class: Any | None = None
        self.feishu_adapter_class: Any | None = None
        self.cron_scheduler_module: Any | None = None
        self.conversation_loop_module: Any | None = None
        self.conversation_loop_func: Any | None = None
        self.run_agent_module: Any | None = None

        # GatewayRunner
        gw_mod = self._import("gateway.run")
        if gw_mod is not None:
            cls = getattr(gw_mod, "GatewayRunner", None)
            if cls is not None:
                self.gateway_runner_class = cls
            else:
                _logger.debug("HLS: gateway.run loaded but GatewayRunner not defined yet")

        # AIAgent
        ra_mod = self._import("run_agent")
        if ra_mod is not None:
            cls = getattr(ra_mod, "AIAgent", None)
            if cls is not None:
                self.aiagent_class = cls
                self.run_agent_module = ra_mod

        # FeishuAdapter — 抽取到 _resolve_feishu_adapter()，
        # 便于 resolve_feishu_adapter_class_fresh() 复用（v1.4.0: fix deferred loading patch miss）
        self.feishu_adapter_class = self._resolve_feishu_adapter()

        # Cron scheduler
        for mod_name in ("cron.scheduler", "gateway.cron.scheduler"):
            mod = self._import(mod_name)
            if mod is not None and hasattr(mod, "_deliver_result"):
                self.cron_scheduler_module = mod
                break

        # Conversation loop (with namespace collision workaround)
        self._resolve_conversation_loop()

    def _resolve_feishu_adapter(self) -> Any | None:
        """Resolve FeishuAdapter class through the gateway's namespace."""
        # v1.8.8: 顺序修正。现役 hermes（v0.21+，GatewayRunner mixin 化重构
        # 之后）的 platform_registry 走 deferred loader，最终 import 的就是
        # 源码路径 plugins.platforms.feishu.adapter——运行时真身与本路径解析
        # 出的是同一个模块对象，类身份一致。旧首选 hermes_plugins.feishu_
        # platform.adapter 属 v0.17–v0.21 时代的"真身/替身"双类结构，该
        # 路径已消亡（现役两版本均无 hermes_plugins 包），降级为末位兜底
        # 以兼容仍跑老版本的部署，避免每次 apply_patches 白吃一次
        # ImportError。
        _feishu_import_paths = [
            "plugins.platforms.feishu.adapter",  # 现役真身（source path，deferred loader 的最终目标）
            "gateway.platforms.feishu",  # Legacy path (Hermes < v0.17)
            "hermes_plugins.feishu_platform.adapter",  # 已消亡 (v0.17–v0.21 双类结构)，末位兜底
        ]
        for _mod_path in _feishu_import_paths:
            mod = self._import(_mod_path)
            if mod is not None:
                cls = getattr(mod, "FeishuAdapter", None)
                if cls is not None:
                    _logger.debug("HLS: FeishuAdapter resolved via %s", _mod_path)
                    return cls
        _logger.debug("HLS: FeishuAdapter not available via any import path")
        return None

    def resolve_feishu_adapter_class_fresh(self) -> Any | None:
        """Re-resolve FeishuAdapter class without reusing cached state."""
        return self._resolve_feishu_adapter()

    def _resolve_conversation_loop(self) -> None:
        """Resolve agent.conversation_loop, handling Apple Silicon namespace collision."""
        # Strategy 1: sys.modules cache
        cl_mod = sys.modules.get("agent.conversation_loop")
        if cl_mod is not None:
            func = getattr(cl_mod, "run_conversation", None)
            if func is not None:
                self.conversation_loop_module = cl_mod
                self.conversation_loop_func = func
                _logger.debug("HLS: conversation_loop resolved via sys.modules")
                return

        # Strategy 2/3（anchor 文件加载 / 标准导入）是 fresh import——
        # sys_modules_only 模式（register 阶段）跳过，交给延迟补丁线程。
        if self._sys_modules_only:
            return

        # Strategy 2: Anchor-based discovery
        for anchor_name in ("gateway.run", "run_agent"):
            anchor = sys.modules.get(anchor_name)
            if anchor is None:
                try:
                    anchor = importlib.import_module(anchor_name)
                except ImportError:
                    continue
            anchor_file = getattr(anchor, "__file__", None)
            if not anchor_file:
                continue
            repo_root = Path(anchor_file).resolve().parent
            if anchor_name == "gateway.run":
                repo_root = repo_root.parent
            cl_file = repo_root / "agent" / "conversation_loop.py"
            if not cl_file.is_file():
                continue
            spec = importlib.util.spec_from_file_location(
                "agent.conversation_loop", str(cl_file)
            )
            if spec is None or spec.loader is None:
                continue
            try:
                mod = importlib.util.module_from_spec(spec)
                sys.modules["agent.conversation_loop"] = mod
                spec.loader.exec_module(mod)
                func = getattr(mod, "run_conversation", None)
                if func is not None:
                    self.conversation_loop_module = mod
                    self.conversation_loop_func = func
                    _logger.debug(
                        "HLS: conversation_loop resolved via anchor %s", anchor_name
                    )
                    return
            except Exception as e:
                _logger.debug("HLS: anchor-based load failed: %s", e)

        # Strategy 3: Standard import
        try:
            import agent.conversation_loop as _mod
            from agent.conversation_loop import run_conversation as _func

            self.conversation_loop_module = _mod
            self.conversation_loop_func = _func
        except (ImportError, AttributeError):
            pass

    @property
    def has_gateway_runner(self) -> bool:
        return self.gateway_runner_class is not None

    @property
    def has_aiagent(self) -> bool:
        return self.aiagent_class is not None

    @property
    def has_feishu_adapter(self) -> bool:
        return self.feishu_adapter_class is not None

    @property
    def has_cron_scheduler(self) -> bool:
        return self.cron_scheduler_module is not None

    @property
    def has_conversation_loop(self) -> bool:
        return self.conversation_loop_func is not None

    def get_layout_report(self) -> dict[str, bool]:
        """Return a dict of what's available — for doctor command and logging."""
        return {
            "has_gateway_runner": self.has_gateway_runner,
            "has_aiagent": self.has_aiagent,
            "has_feishu_adapter": self.has_feishu_adapter,
            "has_cron_scheduler": self.has_cron_scheduler,
            "has_conversation_loop": self.has_conversation_loop,
            "hermes_version": self.hermes_version,
        }

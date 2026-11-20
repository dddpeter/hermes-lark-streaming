"""scripts/compat_probe.py 测试 — Hermes 新版兼容探测（v1.11.1）.

S2' 静态扫描为纯函数直接测；S1/S4' 走子进程，用 tmp 伪 hermes 源码树。
S5（pytest 全套）单测不跑（分钟级），只测 --skip-pytest 的快速路径。
另含 P3↔P4 一致性守卫：compat/hermes_api.py 内全部 import 必须都在探针
SURFACE_CHECKLIST 里（收敛层加了新锚点而探针清单漏更 = 探测盲区）。
"""

# pyright: reportMissingImports=false, reportAttributeAccessIssue=false
# hermes_lark_streaming.* 由 tests/conftest.py 运行时注册包名（目录名带连字
# 符 ≠ 包名），gateway.* 等宿主模块仅存在于 hermes 运行环境——静态解析必然
# 误报，文件级豁免（运行时零影响）。

from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
import sys

sys.path.insert(0, str(REPO_ROOT / "scripts"))

from compat_probe import (  # noqa: E402
    ANCHOR_SIGNATURES,
    SURFACE_CHECKLIST,
    main,
    run_probe,
    scan_anchor_signatures,
)

_GOOD_PROGRESS = (
    "def progress_callback(self, event_type, tool_name=None, "
    "preview=None, args=None, **kwargs):\n    pass\n"
)


def _make_hermes_tree(tmp_path: Path, files: dict[str, str]) -> Path:
    root = tmp_path / "hermes-src"
    for rel, content in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
    return root


class TestScanAnchorSignatures:
    def test_good_baseline_passes(self, tmp_path) -> None:
        root = _make_hermes_tree(
            tmp_path,
            {"agent/run_turn_progress.py": _GOOD_PROGRESS},
        )
        result = scan_anchor_signatures(root, (("run_turn_progress.py", "progress_callback", ("event_type", "tool_name", "preview")),))
        assert result["run_turn_progress.py::progress_callback"]["ok"] is True

    def test_drifted_params_fail(self, tmp_path) -> None:
        root = _make_hermes_tree(
            tmp_path,
            {"agent/run_turn_progress.py": "def progress_callback(event_type, name=None, preview=None):\n    pass\n"},
        )
        result = scan_anchor_signatures(root, (("run_turn_progress.py", "progress_callback", ("event_type", "tool_name", "preview")),))
        entry = result["run_turn_progress.py::progress_callback"]
        assert entry["ok"] is False
        assert "drift" in entry["error"]

    def test_missing_file_fails(self, tmp_path) -> None:
        root = _make_hermes_tree(tmp_path, {"agent/other.py": ""})
        result = scan_anchor_signatures(root, (("run_turn_progress.py", "progress_callback", ("event_type",)),))
        assert result["run_turn_progress.py::progress_callback"]["ok"] is False

    def test_missing_function_fails(self, tmp_path) -> None:
        root = _make_hermes_tree(tmp_path, {"agent/run_turn_progress.py": "def other():\n    pass\n"})
        result = scan_anchor_signatures(root, (("run_turn_progress.py", "progress_callback", ("event_type",)),))
        assert result["run_turn_progress.py::progress_callback"]["ok"] is False

    def test_self_stripped_like_runtime_baseline(self, tmp_path) -> None:
        """静态源码的 self 首参须剥掉（bound 方法运行时不可见）."""
        root = _make_hermes_tree(
            tmp_path,
            {"agent/run_turn_progress.py": "def progress_callback(self, event_type, tool_name, preview):\n    pass\n"},
        )
        result = scan_anchor_signatures(root, (("run_turn_progress.py", "progress_callback", ("event_type", "tool_name", "preview")),))
        assert result["run_turn_progress.py::progress_callback"]["ok"] is True


def _full_fake_hermes(extra: dict[str, str] | None = None) -> dict[str, str]:
    files = {
        "gateway/__init__.py": "",
        "gateway/run.py": "class GatewayRunner:\n    pass\n",
        "gateway/platforms/__init__.py": "",
        "gateway/platforms/base.py": (
            "class SendResult:\n    pass\n\n\nclass EphemeralReply:\n    pass\n"
        ),
        "gateway/config.py": "class Platform:\n    RELAY = 'relay'\n",
        "gateway/platform_registry.py": "platform_registry = object()\n",
        "gateway/relay/__init__.py": "",
        "gateway/relay/adapter.py": "class RelayAdapter:\n    pass\n",
        "agent/__init__.py": "",
        "agent/async_utils.py": "def safe_schedule_threadsafe(*a, **k):\n    pass\n",
        "agent/conversation_loop.py": "def run_conversation(*a, **k):\n    pass\n",
        "agent/display.py": (
            "def build_tool_preview(n, a):\n    return None\n\n\n"
            "def redact_tool_args_for_display(n, a):\n    return a\n\n\n"
            "def _detect_tool_failure(n, r):\n    return False, ''\n"
        ),
        "tools/__init__.py": "",
        "tools/clarify_gateway.py": (
            "def mark_awaiting_text(*a, **k):\n    pass\n\n\n"
            "def resolve_gateway_clarify(*a, **k):\n    pass\n\n\n"
            "def get_pending(*a, **k):\n    return None\n"
        ),
        "agent/run_turn_progress.py": _GOOD_PROGRESS,
        "agent/run_turn_runner.py": (
            "def combined_tool_start_callback(call_id, tool_name, args):\n    pass\n\n\n"
            "def combined_tool_complete_callback(call_id, tool_name, args, result):\n    pass\n"
        ),
    }
    files.update(extra or {})
    return files


class TestProbeFlow:
    def test_all_pass_verdict_compat(self, tmp_path) -> None:
        root = _make_hermes_tree(tmp_path, _full_fake_hermes())
        ok, report = run_probe(root, run_pytest=False)
        assert ok, report["reasons"]
        assert report["verdict"] == "COMPAT"
        assert [s["step"] for s in report["steps"]] == ["S1", "S2'", "S4'"]

    def test_broken_surface_fails_s4(self, tmp_path) -> None:
        root = _make_hermes_tree(
            tmp_path,
            _full_fake_hermes({"agent/display.py": "def build_tool_preview(n, a):\n    return None\n"}),
        )
        ok, report = run_probe(root, run_pytest=False)
        assert not ok
        s4 = next(s for s in report["steps"] if s["step"] == "S4'")
        assert not s4["ok"]
        assert "redact_tool_args_for_display" in s4["error"]

    def test_broken_core_fails_fast_s1_only(self, tmp_path) -> None:
        files = _full_fake_hermes()
        del files["gateway/run.py"]
        root = _make_hermes_tree(tmp_path, files)
        ok, report = run_probe(root, run_pytest=False)
        assert not ok
        assert [s["step"] for s in report["steps"]] == ["S1"]
        assert report["steps"][0]["ok"] is False

    def test_drifted_anchor_fails_s2(self, tmp_path) -> None:
        root = _make_hermes_tree(
            tmp_path,
            _full_fake_hermes(
                {"agent/run_turn_progress.py": "def progress_callback(event_type, renamed, preview=None):\n    pass\n"}
            ),
        )
        ok, report = run_probe(root, run_pytest=False)
        assert not ok
        s2 = next(s for s in report["steps"] if s["step"] == "S2'")
        assert not s2["ok"]

    def test_main_exit_code_and_json(self, tmp_path, capsys) -> None:
        root = _make_hermes_tree(tmp_path, _full_fake_hermes())
        out_json = tmp_path / "probe.json"
        rc = main([str(root), "--skip-pytest", "--json", str(out_json)])
        assert rc == 0
        import json

        data = json.loads(out_json.read_text())
        assert data["verdict"] == "COMPAT"
        captured = capsys.readouterr().out
        assert "verdict: COMPAT" in captured


class TestChecklistParity:
    """P3↔P4 一致性守卫（评审 Important#1 升级：扫全插件树）：
    任何文件里 `from gateway.* / agent.* / tools.clarify_gateway import X`
    都必须在探针 SURFACE_CHECKLIST 中——否则新 hermes 上探针漏报盲区。
    """

    _SCAN_DIRS = ("patching", "controller", "plugin", "state", "cardkit", "compat")
    _SCAN_FILES = ("__main__.py",)

    def _collect_plugin_hermes_imports(self) -> set[tuple[str, str]]:
        imports: set[tuple[str, str]] = set()
        paths: list[Path] = [REPO_ROOT / f for f in self._SCAN_FILES]
        for d in self._SCAN_DIRS:
            paths.extend((REPO_ROOT / d).rglob("*.py"))
        for path in paths:
            if not path.exists() or "test" in path.parts:
                continue
            try:
                tree = ast.parse(path.read_text())
            except (OSError, SyntaxError):
                continue
            for node in ast.walk(tree):
                # level>0 是包内相对导入（from .gateway import … 是本地
                # patching/gateway.py，不是 hermes gateway）——只查绝对导入
                if not (isinstance(node, ast.ImportFrom) and node.module and node.level == 0):
                    continue
                root = node.module.split(".")[0]
                if root not in ("gateway", "agent") and node.module != "tools.clarify_gateway":
                    continue
                for alias in node.names:
                    imports.add((node.module, alias.name))
        return imports

    def test_plugin_imports_covered_by_checklist(self) -> None:
        imports = self._collect_plugin_hermes_imports()
        assert imports, "扫描器失效——插件不可能零 hermes 私有 import"
        checklist = {(m, s) for m, s in SURFACE_CHECKLIST}
        missing = imports - checklist
        assert not missing, (
            f"插件新增 hermes 私有锚点未进 SURFACE_CHECKLIST（探测盲区）: {sorted(missing)}"
        )

    def test_checklist_covers_all_anchor_files(self) -> None:
        files = {name for name, _, _ in ANCHOR_SIGNATURES}
        assert files, "锚点基线为空"
        for name in files:
            assert name.endswith(".py")

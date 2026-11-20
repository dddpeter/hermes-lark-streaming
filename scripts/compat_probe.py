#!/usr/bin/env python3
"""Hermes 新版兼容自动探测（v1.11.1，docs/UPSTREAM-ADOPTION.md P4）.

上游 hermes-release-watch 工作流的五步探测改造为 V1 AST-patching 架构：

| 步 | 内容 | 失败含义 |
|---|---|---|
| S1 | PYTHONPATH 前置新版 hermes 源码，import gateway.run | 依赖/结构级失配（明细含 ImportError 链） |
| S2' | 静态 AST 扫描回调锚点签名（v1.8.9 warn_signature_drift 的离线版） | 插件 wrapper 静默错绑（如 preview 当 tool_name 渲染） |
| S4' | compat/hermes_api.py 全部私有表面 + clarify_gateway 逐项 import 核对 | 该 hermes 版本下部分能力失灵（S3 类继承面 V1 不适用，略） |
| S5 | PYTHONPATH 前置新版源码跑插件 pytest 全套（可 --skip-pytest） | 新版场景回归 |

任何一步失败立即定性并跳过后续步骤（S2' 除外——全部锚点逐项出明细）。
退出码 0/1；``--json`` 落盘结构化结论。

上游踩坑教训（照抄）：S4 清单只放 **hermes 侧表面**——上游曾误把插件自身
FeishuClient 的方法列为"基类必须有"，导致 0.23.2 起每次探针误报。

用法：
    python3 scripts/compat_probe.py /path/to/hermes-src [--json out.json]
                                    [--skip-pytest] [-- pytest -k foo ...]
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import subprocess
import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path

# ── 探测目标 ───────────────────────────────────────────────────────────

#: S1 hermes 核心模块（结构级失配探测）
S1_CORE_MODULES = ("gateway.run",)

#: S2' 回调锚点签名基线（与 patching/callbacks.py 的 warn_signature_drift
#: 同源；静态 AST 扫描，跳过 self/cls 首参）。文件名按 hermes 仓库内
#: 递归匹配。基线漂移 = wrapper 不报错只错绑，必须离线可探测。
ANCHOR_SIGNATURES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    # hermes run_turn_progress.py: progress_callback(self, event_type,
    # tool_name=None, preview=None, args=None, **kwargs)
    ("run_turn_progress.py", "progress_callback",
     ("event_type", "tool_name", "preview")),
    # combined_tool_start_callback(call_id, tool_name, args)
    ("run_turn_runner.py", "combined_tool_start_callback",
     ("call_id", "tool_name", "args")),
    # combined_tool_complete_callback(call_id, tool_name, args, result)
    ("run_turn_runner.py", "combined_tool_complete_callback",
     ("call_id", "tool_name", "args", "result")),
)

#: S4' hermes 私有表面清单（compat/hermes_api.py 收敛层的家底 + clarify
#: 子系统两个符号）。上游教训：只放 hermes 侧，不放插件自身表面。
SURFACE_CHECKLIST: tuple[tuple[str, str], ...] = (
    ("gateway.platforms.base", "SendResult"),
    ("gateway.platforms.base", "EphemeralReply"),
    ("gateway.config", "Platform"),
    ("gateway.run", "GatewayRunner"),
    ("gateway.platform_registry", "platform_registry"),
    ("gateway.relay.adapter", "RelayAdapter"),
    ("agent.async_utils", "safe_schedule_threadsafe"),
    ("agent.conversation_loop", "run_conversation"),
    ("agent.display", "build_tool_preview"),
    ("agent.display", "redact_tool_args_for_display"),
    ("agent.display", "_detect_tool_failure"),
    ("tools.clarify_gateway", "mark_awaiting_text"),
    ("tools.clarify_gateway", "resolve_gateway_clarify"),
)

_PLUGIN_ROOT = Path(__file__).resolve().parent.parent

# S4' 驱动脚本：在目标 hermes 环境下逐项 import 核对（hermes 侧表面）。
# 注意：不能用 str.format 组装（脚本体内有大量字面花括号）。
def _make_surface_driver() -> str:
    surface = [(m, s) for m, s in SURFACE_CHECKLIST]
    return (
        "import importlib, json\n"
        f"SURFACE = {surface!r}\n"
        "results = {}\n"
        "for mod_path, symbol in SURFACE:\n"
        "    key = mod_path + '.' + symbol\n"
        "    try:\n"
        "        mod = importlib.import_module(mod_path)\n"
        "        getattr(mod, symbol)\n"
        "        results[key] = {'ok': True}\n"
        "    except Exception as exc:\n"
        "        results[key] = {'ok': False, 'error': type(exc).__name__ + ': ' + str(exc)}\n"
        "print(json.dumps(results))\n"
    )


# ── 结果结构 ───────────────────────────────────────────────────────────


@dataclass
class StepResult:
    step: str
    ok: bool
    detail: dict = field(default_factory=dict)
    error: str = ""

    def as_dict(self) -> dict:
        return {"step": self.step, "ok": self.ok, "detail": self.detail, "error": self.error}


# ── S2'：静态锚点签名扫描（纯函数，可单测） ────────────────────────────


def _positional_params(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    names: list[str] = []
    for p in fn.args.args:
        names.append(p.arg)
    # 静态源码含 self/cls 首参（bound 后不可见）——与运行时基线对齐剥掉
    if names and names[0] in ("self", "cls"):
        names = names[1:]
    return names


def scan_anchor_signatures(
    hermes_root: Path,
    anchors: tuple[tuple[str, str, tuple[str, ...]], ...] = ANCHOR_SIGNATURES,
) -> dict[str, dict]:
    """逐锚点扫描：文件可定位、函数存在、位置参数前缀与基线一致."""
    results: dict[str, dict] = {}
    for filename, func_name, expected in anchors:
        label = f"{filename}::{func_name}"
        matches = sorted(hermes_root.rglob(filename))
        if not matches:
            results[label] = {
                "ok": False,
                "error": f"source file not found anywhere under {hermes_root}",
            }
            continue
        found: dict | None = None
        for path in matches:
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except (OSError, SyntaxError) as exc:
                results[label] = {"ok": False, "error": f"parse {path}: {exc}"}
                continue
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if node.name == func_name:
                        found = {"path": str(path), "params": _positional_params(node)}
                        break
            if found:
                break
        if not found:
            results[label] = {"ok": False, "error": f"{func_name} not defined in {filename}"}
            continue
        actual = tuple(found["params"][: len(expected)])
        ok = actual == expected
        results[label] = {
            "ok": ok,
            "path": found["path"],
            "expected": list(expected),
            "actual": list(actual),
            **({} if ok else {"error": "signature drift — wrapper will mis-bind silently"}),
        }
    return results


# ── S1 / S4' / S5：子进程探测 ─────────────────────────────────────────


def _run_py(code: str, hermes_root: Path, *, extra_path: Path | None = None,
            args: list[str] | None = None) -> tuple[int, str]:
    env = dict(os.environ)
    paths = [str(hermes_root)]
    if extra_path is not None:
        paths.append(str(extra_path))
    env["PYTHONPATH"] = os.pathsep.join(paths + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else []))
    proc = subprocess.run(
        [sys.executable, "-c", code, *(args or [])],
        capture_output=True, text=True, env=env, timeout=600,
    )
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


def probe_s1(hermes_root: Path) -> StepResult:
    code = "import " + ", ".join(S1_CORE_MODULES)
    rc, out = _run_py(code, hermes_root)
    return StepResult(
        "S1", rc == 0,
        {"code": code},
        "" if rc == 0 else out.strip()[-2000:],
    )


def probe_s4(hermes_root: Path) -> StepResult:
    code = _make_surface_driver()
    rc, out = _run_py(code, hermes_root, extra_path=_PLUGIN_ROOT.parent)
    if rc != 0:
        return StepResult("S4'", False, {"symbols": len(SURFACE_CHECKLIST)}, out.strip()[-2000:])
    # 驱动脚本只打一行 JSON；容错取最后一行
    try:
        results = json.loads(out.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return StepResult("S4'", False, {}, f"unparsable driver output: {out[:500]}")
    missing = [k for k, v in results.items() if not v.get("ok")]
    return StepResult(
        "S4'", not missing,
        {"symbols": len(SURFACE_CHECKLIST), "results": results},
        "" if not missing else "; ".join(f"{k}: {results[k]['error']}" for k in missing),
    )


def probe_s5(hermes_root: Path, pytest_args: list[str] | None) -> StepResult:
    code = (
        "import sys, pytest;"
        f"sys.path.insert(0, {str(_PLUGIN_ROOT.parent)!r});"
        "raise SystemExit(pytest.main(sys.argv[1:]))"
    )
    args = list(pytest_args or [])
    if not pytest_args:
        args = [str(_PLUGIN_ROOT / "tests"), "-q",
                "--ignore", str(_PLUGIN_ROOT / "tests" / "e2e"),
                "--ignore", str(_PLUGIN_ROOT / "tests" / "integration")]
    rc, out = _run_py(code, hermes_root, args=args)
    tail = out.strip().splitlines()[-3:]
    return StepResult("S5", rc == 0, {"args": args}, "" if rc == 0 else "\n".join(tail))


# ── 主流程 ─────────────────────────────────────────────────────────────


def run_probe(hermes_root: Path, *, run_pytest: bool = True,
              pytest_args: list[str] | None = None) -> tuple[bool, dict]:
    steps: list[StepResult] = []
    hermes_root = hermes_root.resolve()

    s1 = probe_s1(hermes_root)
    steps.append(s1)
    if not s1.ok:  # 结构级失配 — 后续步骤无意义
        return _finalize(steps, hermes_root)

    # S2' 全锚点逐项出明细（不因单锚点失败中断同类扫描）
    anchors = scan_anchor_signatures(hermes_root)
    steps.append(StepResult(
        "S2'", all(v.get("ok") for v in anchors.values()),
        {"anchors": anchors},
        "" if all(v.get("ok") for v in anchors.values())
        else "; ".join(f"{k}: {v.get('error')}" for k, v in anchors.items() if not v.get("ok")),
    ))
    if not steps[-1].ok:
        return _finalize(steps, hermes_root)

    s4 = probe_s4(hermes_root)
    steps.append(s4)
    if not s4.ok:
        return _finalize(steps, hermes_root)

    if run_pytest:
        steps.append(probe_s5(hermes_root, pytest_args))
    return _finalize(steps, hermes_root)


def _finalize(steps: list[StepResult], hermes_root: Path) -> tuple[bool, dict]:
    ok = all(s.ok for s in steps)
    verdict = "COMPAT" if ok else "INCOMPAT"
    reasons = [f"{s.step}: {s.error}" for s in steps if not s.ok]
    report = {
        "hermes_root": str(hermes_root),
        "verdict": verdict,
        "ok": ok,
        "steps": [s.as_dict() for s in steps],
        "reasons": reasons,
    }
    return ok, report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("hermes_root", type=Path, help="新版 hermes 源码根目录")
    parser.add_argument("--json", type=Path, default=None, help="结构化结论落盘路径")
    parser.add_argument("--skip-pytest", action="store_true", help="跳过 S5（快速探测）")
    parser.add_argument("pytest_args", nargs="*", help="透传给 S5 的 pytest 参数（-- 之后）")
    args = parser.parse_args(argv)

    if not args.hermes_root.is_dir():
        print(f"ERROR: {args.hermes_root} is not a directory", file=sys.stderr)
        return 1

    ok, report = run_probe(
        args.hermes_root,
        run_pytest=not args.skip_pytest,
        pytest_args=args.pytest_args or None,
    )
    for s in report["steps"]:
        mark = "PASS" if s["ok"] else "FAIL"
        print(f"[{mark}] {s['step']}" + (f" — {s['error']}" if s["error"] else ""))
    print(f"\nverdict: {report['verdict']}")
    if args.json:
        args.json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"report: {args.json}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

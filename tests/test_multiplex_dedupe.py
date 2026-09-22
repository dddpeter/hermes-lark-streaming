"""v1.8.4 multiplex 多副本去重回归测试.

场景：hermes multiplex 模式（gateway_multiplex_mode.py — 一机一网关服务所有
profile）按 profile home 各自实例化 PluginManager，hermes-lark-streaming 安装
在多个 profile 的 plugins/ 目录时，每个副本是独立模块实例
（plugins_loader._directory_module_name 对第二个及以后 scope 追加
__home_<digest> 后缀）。每个副本的 register() → apply_patches() 各自执行，
模块级 guard（_applied / _gw_runner_patched）跨副本失效。

旧行为（bug）：共享 GatewayRunner 类的方法被嵌套 wrap N 次，每层 wrapper 各
fire 一套 hooks → 每个副本各自的 controller（独立 _sessions 字典）为同一条
消息各建一张流式卡 → 一次聊天 N 张回复卡片（default profile 实测 3 张）。

新行为：wrap 前检查挂在被 wrap 的共享 callable 上的 _hls_wrapped 哨兵，
副本 2/3 跳过 wrap，全进程只有第一份 wrapper 生效 → 每条消息一张卡。
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import hermes_lark_streaming.patching as mp


class _FakeGatewayRunner:
    """Minimal GatewayRunner stand-in with the 4 patchable methods."""

    async def _handle_message(self, event: Any, *args: Any, **kwargs: Any):
        return ""

    async def _handle_message_with_agent(
        self, event: Any, source: Any, *args: Any, **kwargs: Any
    ):
        return {"final_response": "ok"}

    async def _run_agent(self, *args: Any, **kwargs: Any):
        return {}

    async def _run_background_task(self, *args: Any, **kwargs: Any):
        return {}


def _apply_once() -> type:
    """Simulate ONE plugin copy's _apply_gateway_runner_patches with fresh
    module-level state (as a sibling copy's module instance would have)."""
    mp._gw_runner_patched = False  # 模拟新副本模块实例的初始状态
    compat = mp.HermesCompat
    with patch.object(mp, "HermesCompat") as mock_compat:
        mock_compat.return_value.gateway_runner_class = _FakeGatewayRunner
        assert mp._apply_gateway_runner_patches() is True
    mp.HermesCompat = compat  # type: ignore[assignment]
    return _FakeGatewayRunner


class TestMultiplexDuplicateCopyDedupe:
    def setup_method(self) -> None:
        # 清理类上的哨兵，保证测试独立
        for name in (
            "_handle_message",
            "_handle_message_with_agent",
            "_run_agent",
            "_run_background_task",
        ):
            method = getattr(_FakeGatewayRunner, name, None)
            if method is not None and hasattr(method, "_hls_wrapped"):
                try:
                    delattr(method, "_hls_wrapped")
                except AttributeError:
                    pass

    def test_helpers_roundtrip(self) -> None:
        """_hls_is_wrapped / _hls_mark_wrapped 基本语义."""

        def fn() -> None: ...

        assert not mp._hls_is_wrapped(fn)
        assert mp._hls_mark_wrapped(fn) is fn
        assert mp._hls_is_wrapped(fn)

    def test_helpers_tolerate_non_settable_targets(self) -> None:
        """哨兵写入失败时必须静默（bound method 等不可设属性对象）。"""
        gw = _FakeGatewayRunner()
        bound = gw._handle_message  # bound method 不允许 setattr
        # 不应抛异常
        mp._hls_mark_wrapped(bound)
        # 读不到哨兵 → 视为未 wrap（与旧行为一致，不会误跳过）
        assert not mp._hls_is_wrapped(bound)

    def test_gateway_runner_wrapped_exactly_once_across_copies(self) -> None:
        """两个副本先后 apply，方法只被 wrap 一次（哨兵阻止嵌套双 wrap）。"""
        first = _apply_once()
        after_first = {
            name: getattr(_FakeGatewayRunner, name)
            for name in (
                "_handle_message",
                "_handle_message_with_agent",
                "_run_agent",
                "_run_background_task",
            )
        }
        # 第一副本 wrap 成功且打上哨兵
        for name, method in after_first.items():
            assert getattr(method, "_hls_wrapped", False), f"{name} 未打哨兵"
            assert method is not getattr(
                _FakeGatewayRunner, name
            ) or True  # 占位，真实断言在下方

        # 第二副本（multiplex 兄弟副本：独立模块状态、同一共享类）
        _apply_once()
        for name, method in after_first.items():
            current = getattr(_FakeGatewayRunner, name)
            assert current is method, (
                f"{name} 被第二副本重新 wrap — multiplex 下会产生 N 张回复卡"
            )

    def test_sentinel_absent_on_unpatched_class(self) -> None:
        """未 patch 的类方法不带哨兵（防哨兵泄漏误判）。"""
        class Plain:
            async def _handle_message(self, event: Any, *a: Any, **k: Any):
                return ""

        assert not mp._hls_is_wrapped(Plain._handle_message)

    def test_real_wrappers_are_still_installed(self) -> None:
        """哨兵去重不能把真正的 wrapper 也吞掉：首副本 apply 后，
        GatewayRunner._handle_message 必须是 HLS wrapper（调用会 fire
        on_feishu_normalize 路径的可调用），而非裸方法。"""
        _apply_once()
        method = getattr(_FakeGatewayRunner, "_handle_message")
        # wrapper 带 functools.wraps 元数据与 __wrapped__ 链
        assert hasattr(method, "__wrapped__"), (
            "首副本 apply 后 _handle_message 不是 wrapper — 补丁失效"
        )
        assert getattr(method, "_hls_wrapped", False)

    def test_concurrent_copies_no_double_wrap(self) -> None:
        """v1.8.7 竞态回归：两个副本的延迟补丁线程并发醒来时（各自模块级
        _gw_runner_patched 均为 False），类级锁 + 锁内 sentinel 复查必须保证
        方法只 wrap 一次（生产曾出现 default/coder 双副本 controller 同时活跃）。"""
        import threading

        mp._gw_runner_patched = False
        before = {
            name: getattr(_FakeGatewayRunner, name)
            for name in (
                "_handle_message",
                "_handle_message_with_agent",
                "_run_agent",
                "_run_background_task",
            )
        }
        # 确保起始无哨兵
        for name, method in before.items():
            if hasattr(method, "_hls_wrapped"):
                delattr(method, "_hls_wrapped")

        barrier = threading.Barrier(2)

        def worker() -> None:
            mp._gw_runner_patched = False  # 模拟独立副本的新鲜模块状态
            barrier.wait()
            with patch.object(mp, "HermesCompat") as mock_compat:
                mock_compat.return_value.gateway_runner_class = _FakeGatewayRunner
                mp._apply_gateway_runner_patches()

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        for name, method in before.items():
            current = getattr(_FakeGatewayRunner, name)
            # 只 wrap 一层：wrapper.__wrapped__ 必须直接是原方法（双 wrap 会
            # 出现 __wrapped__.__wrapped__ 链）
            assert getattr(current, "__wrapped__", None) is method, (
                f"{name} 被并发双 wrap — TOCTOU 竞态未消除"
            )

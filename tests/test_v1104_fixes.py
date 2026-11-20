"""v1.10.4 修复测试 — 99991400 限频分道退避（上游 v2.0.32 审计移植）.

图标 token 修复的测试在 tests/test_tooluse.py（TestToolUseTracker 族）。

背景（docs/UPSTREAM-ADOPTION.md P2）：99991400（per-API 限频，HTTP 400）
此前与普通瞬态码共用 (0.1, 0.3, 0.6) 快退避——0.1s 后重试限频等于继续
撞墙，三连耗尽后整回合降级。分道为秒级 (1.0, 3.0, 6.0)。
"""

# pyright: reportMissingImports=false, reportAttributeAccessIssue=false
# hermes_lark_streaming.* 由 tests/conftest.py 运行时注册包名（目录名带连字
# 符 ≠ 包名），lark_oapi 仅存在于安装了 SDK 的运行环境——静态解析必然误报，
# 文件级豁免（运行时零影响）。

from __future__ import annotations

from unittest.mock import patch

import pytest
from hermes_lark_streaming.feishu.client import (
    _TRANSIENT_RETRY_DELAYS,
    FeishuAPIError,
    FeishuClient,
    FeishuClientConfig,
)


def _make_client() -> FeishuClient:
    return FeishuClient(
        FeishuClientConfig(app_id="cli_test_app", app_secret="test_secret_value")
    )


def _flaky_factory(code: int, failures: int):
    """coro_factory：前 failures 次抛 FeishuAPIError(code)，之后返回成功."""
    state = {"n": 0}

    async def factory():
        state["n"] += 1
        if state["n"] <= failures:
            raise FeishuAPIError(f"simulated code={code}", code=code)
        return "ok"

    return factory, state


async def _collect_delays(code: int, failures: int) -> list[float]:
    client = _make_client()
    factory, state = _flaky_factory(code, failures)
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    with patch(
        "hermes_lark_streaming.feishu.client.asyncio.sleep", fake_sleep
    ):
        result = await client._retry_transient("test_op", factory)
    assert result == "ok"
    assert state["n"] == failures + 1, "transient error was not retried"
    return sleeps


class TestRateLimitBackoffTrack:
    """99991400 走秒级退避；其他瞬态码保持快退避."""

    @pytest.mark.asyncio
    async def test_rate_limit_uses_second_level_delays(self) -> None:
        sleeps = await _collect_delays(99991400, failures=2)
        assert sleeps == [1.0, 3.0]

    @pytest.mark.asyncio
    async def test_rate_limit_full_track_is_1_3_6(self) -> None:
        # 连续 3 次限频 → 第 3 次重试 delay=6.0 后成功（max_retries=3）
        sleeps = await _collect_delays(99991400, failures=3)
        assert sleeps == [1.0, 3.0, 6.0]

    @pytest.mark.asyncio
    async def test_other_transient_keeps_fast_delays(self) -> None:
        sleeps = await _collect_delays(2200, failures=2)
        assert sleeps == list(_TRANSIENT_RETRY_DELAYS[:2])

    @pytest.mark.asyncio
    async def test_rate_limit_exhausted_raises(self) -> None:
        client = _make_client()

        async def always_limited():
            raise FeishuAPIError("rate limited", code=99991400)

        sleeps: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleeps.append(delay)

        with patch(
            "hermes_lark_streaming.feishu.client.asyncio.sleep", fake_sleep
        ):
            with pytest.raises(FeishuAPIError) as exc_info:
                await client._retry_transient("test_op", always_limited)
        assert exc_info.value.code == 99991400
        assert sleeps == [1.0, 3.0, 6.0]

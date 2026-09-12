"""v1.8.3 regression tests — hardening fixes from the v1.8.2 code review.

  * Singleton lock: ``get_controller()`` is created under a lock — two
    threads racing on first call previously produced two controllers
    (two FeishuClients + split-brain session stores).
  * _fire_and_forget thread safety: scheduling from a non-loop thread now
    uses ``run_coroutine_threadsafe``. ``loop.create_task`` internally uses
    non-threadsafe ``call_soon`` — the callback is appended without writing
    the self-pipe, so a loop blocked in select() never wakes up and the
    coroutine silently stalls (FlushController.schedule_update already did
    this correctly; this aligns the controller with it).
  * reply_* transient retry: ``reply_card`` / ``reply_card_by_id`` /
    ``reply_text`` now go through ``_retry_transient`` like the cardkit_*
    methods. The text fallback path (last-resort delivery when a card seal
    fails) previously had NO retry — one network blip and the user received
    nothing. Also: ``_send_text_fallback`` no longer swallows failures
    silently (`except Exception: pass` → warning log).
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

import hermes_lark_streaming.controller.core as controller_core
from hermes_lark_streaming.controller import StreamCardController
from hermes_lark_streaming.feishu import (
    FeishuAPIError,
    FeishuClient,
    FeishuClientConfig,
)
from hermes_lark_streaming.feishu.client import MSG_NOT_FOUND

from tests.test_controller import _make_session, _setup_ctrl


# ══════════════════════════════════════════════════════════════════════
# Fix 1 — get_controller() singleton lock
# ══════════════════════════════════════════════════════════════════════

class TestSingletonLock:
    """Two threads racing on the first get_controller() call must yield
    exactly one StreamCardController (double-checked locking)."""

    def test_concurrent_first_call_creates_single_instance(self, monkeypatch) -> None:
        monkeypatch.setattr(controller_core, "_controller", None)

        instances: list[object] = []
        a_in_init = threading.Event()
        b_checked = threading.Event()

        def slow_init(self) -> None:
            instances.append(self)
            if len(instances) == 1:
                # First caller parks inside init until the second caller
                # has passed the outer None-check (still seeing None).
                a_in_init.set()
                b_checked.wait(timeout=5)
            # Deterministic interleave achieved; nothing else to do.

        monkeypatch.setattr(controller_core.StreamCardController, "__init__", slow_init)

        results: list[object] = []

        def worker_a() -> None:
            results.append(controller_core.get_controller())

        # Worker A: enters init and parks on b_checked.
        t1 = threading.Thread(target=worker_a)
        t1.start()
        assert a_in_init.wait(timeout=5), "first worker never entered __init__"

        # Worker B: will pass the outer None-check (still None) and then
        # either (unlocked) create a second instance or (locked) block on
        # the lock until A finishes.
        class _WorkerB(threading.Thread):
            def run(self) -> None:  # noqa: D102 — test coordination only
                b_checked.set()
                results.append(controller_core.get_controller())

        t2 = _WorkerB()
        t2.start()

        t1.join(timeout=10)
        t2.join(timeout=10)

        assert not t1.is_alive() and not t2.is_alive(), "deadlock in get_controller()"
        assert len(instances) == 1, (
            f"expected exactly 1 StreamCardController instance, "
            f"got {len(instances)} — singleton is racy"
        )
        assert results[0] is results[1] is controller_core._controller

    def test_subsequent_calls_reuse_instance(self, monkeypatch) -> None:
        sentinel = object()
        monkeypatch.setattr(controller_core, "_controller", sentinel)
        assert controller_core.get_controller() is sentinel


# ══════════════════════════════════════════════════════════════════════
# Fix 2 — _fire_and_forget thread safety
# ══════════════════════════════════════════════════════════════════════

class TestFireAndForgetThreadSafety:
    def test_off_loop_thread_wakes_blocked_loop(self) -> None:
        """Scheduling from a non-loop thread must wake a select()-blocked loop.

        Regression: loop.create_task() from the wrong thread appends the
        task via non-threadsafe call_soon — no self-pipe write — so a loop
        parked in select() never noticed and the coroutine stalled forever
        (silently dropped answer-chunk dispatch / reactivation dispatch).
        """
        ctrl = StreamCardController()
        loop = asyncio.new_event_loop()
        started = threading.Event()
        ran = threading.Event()

        def run_loop() -> None:
            asyncio.set_event_loop(loop)
            loop.call_soon_threadsafe(started.set)
            loop.run_forever()

        worker = threading.Thread(target=run_loop, daemon=True)
        worker.start()
        try:
            assert started.wait(timeout=5), "background loop never started"
            # Let the loop drain its ready queue and block in select().
            time.sleep(0.05)

            async def work() -> None:
                ran.set()

            ctrl._fire_and_forget(work(), loop)

            assert ran.wait(timeout=2.0), (
                "coroutine scheduled from a non-loop thread never ran — "
                "loop was not woken (create_task from wrong thread)"
            )
        finally:
            loop.call_soon_threadsafe(loop.stop)
            worker.join(timeout=5)
            loop.close()

    async def test_on_loop_thread_still_tracks_and_runs_task(self) -> None:
        """On the loop thread the create_task path is unchanged (strong-ref
        tracking via _pending_tasks, discard on completion)."""
        ctrl = StreamCardController()
        loop = asyncio.get_running_loop()
        done: list[int] = []

        async def work() -> None:
            done.append(1)

        ctrl._fire_and_forget(work(), loop)
        assert len(ctrl._pending_tasks) == 1, "task not strong-ref tracked"

        for _ in range(5):
            await asyncio.sleep(0)
        assert done == [1]
        assert len(ctrl._pending_tasks) == 0, "completed task not discarded"


# ══════════════════════════════════════════════════════════════════════
# Fix 3 — reply_* transient retry + text-fallback failure logging
# ══════════════════════════════════════════════════════════════════════

class _FakeOkResp:
    """Minimal stand-in for an lark SDK response with success()."""

    def __init__(self, message_id: str = "om_ok") -> None:
        self._message_id = message_id

    def success(self) -> bool:
        return True

    @property
    def data(self):
        return SimpleNamespace(message_id=self._message_id)


def _make_client() -> FeishuClient:
    return FeishuClient(FeishuClientConfig(app_id="cli_test_app", app_secret="test_secret_value"))


class TestReplyTransientRetry:
    """reply_card / reply_card_by_id / reply_text must retry transient
    errors (network blips, token refresh hiccups) like cardkit_* does."""

    @pytest.mark.parametrize(
        ("method", "kwargs"),
        [
            ("reply_text", {"text": "hello"}),
            ("reply_card", {"card": {"tag": "card"}}),
            ("reply_card_by_id", {"card_id": "card_123"}),
        ],
        ids=["reply_text", "reply_card", "reply_card_by_id"],
    )
    async def test_retries_on_network_error_then_succeeds(
        self, monkeypatch, method: str, kwargs: dict
    ) -> None:
        import hermes_lark_streaming.feishu.client as client_mod

        # Simulate a transient network error without depending on httpx.
        monkeypatch.setattr(client_mod, "_NETWORK_ERROR_BASES", (ValueError,))

        client = _make_client()
        calls = {"n": 0}

        async def flaky_areply(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise ValueError("simulated network error")
            return _FakeOkResp()

        monkeypatch.setattr(client._client.im.v1.message, "areply", flaky_areply)

        result = await getattr(client, method)("om_target", **kwargs)

        assert calls["n"] == 2, "transient network error was not retried"
        assert result == "om_ok"

    @pytest.mark.parametrize(
        ("method", "kwargs"),
        [
            ("reply_text", {"text": "hello"}),
            ("reply_card", {"card": {"tag": "card"}}),
            ("reply_card_by_id", {"card_id": "card_123"}),
        ],
        ids=["reply_text", "reply_card", "reply_card_by_id"],
    )
    async def test_does_not_retry_permanent_error(
        self, monkeypatch, method: str, kwargs: dict
    ) -> None:
        client = _make_client()
        calls = {"n": 0}

        async def permanent_fail(*args, **kwargs):
            calls["n"] += 1
            raise FeishuAPIError(
                f"reply: code={MSG_NOT_FOUND}, msg=message not found",
                MSG_NOT_FOUND,
            )

        monkeypatch.setattr(client._client.im.v1.message, "areply", permanent_fail)

        with pytest.raises(FeishuAPIError):
            await getattr(client, method)("om_target", **kwargs)

        assert calls["n"] == 1, "permanent error must not be retried"


class TestTextFallbackLogging:
    async def test_fallback_failure_is_logged_not_swallowed(self, caplog) -> None:
        """A failed text fallback previously vanished silently
        (`except Exception: pass`) — the user received nothing and the
        logs had no trace. It must at least log a warning."""
        ctrl = _setup_ctrl()
        session = _make_session("msg_fb", linear=True)
        ctrl._sessions["msg_fb"] = session
        ctrl._client.reply_text = AsyncMock(side_effect=RuntimeError("boom"))

        with caplog.at_level(logging.DEBUG, logger="hermes_lark_streaming"):
            # Must not raise.
            await ctrl._send_text_fallback(session, fallback_text="final answer")

        ctrl._client.reply_text.assert_awaited_once()
        failure_logs = [
            r for r in caplog.records
            if r.levelno >= logging.WARNING and "fallback" in r.getMessage().lower()
        ]
        assert failure_logs, (
            "text fallback failure produced no WARNING log — "
            "silent failure remains"
        )

    async def test_fallback_success_still_quiet(self, caplog) -> None:
        ctrl = _setup_ctrl()
        session = _make_session("msg_fb_ok", linear=True)
        ctrl._sessions["msg_fb_ok"] = session

        with caplog.at_level(logging.WARNING, logger="hermes_lark_streaming"):
            await ctrl._send_text_fallback(session, fallback_text="final answer")

        warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert not warnings, "successful fallback must not log warnings"

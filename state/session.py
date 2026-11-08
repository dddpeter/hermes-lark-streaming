"""CardSession — 单条消息的卡片会话状态."""

from __future__ import annotations

import asyncio
import logging
import time
from threading import Lock
from typing import TYPE_CHECKING, Any

# Phase constants — single source of truth in state.phase
from .phase import (
    TERMINAL_PHASES,
    CardPhase,
    TerminalReason,
    is_legal_transition,
)

# Backward-compatible alias — old code imports IDLE from this module
IDLE = CardPhase.IDLE

from ..feishu import UnavailableGuard
from ..flush import FlushController
from .linear import UnifiedLinearState
from .text import TextState
from .tooluse import ToolUseTracker

if TYPE_CHECKING:
    pass

_logger = logging.getLogger("hermes_lark_streaming")


class CardSession:
    """单条消息的卡片会话状态."""

    __slots__ = (
        "_card_ready",
        "_chain_depth",
        "_continuation_reactivation_count",
        "_create_epoch_snap",
        "_creation_stages",
        "_first_answer_time",
        "_first_flush_done",
        "_is_continuation",
        "_last_event_at",
        "_loop",
        "_pending_flush",
        "_seal_dispatched_at",
        "_seal_done",
        "_seal_lock",
        "_split_sealed_answer_len",
        "_streaming_closed",
        "_streaming_closed_logged",
        "_was_aborted",
        "_watchdog_split",
        "anchor_id",
        "card_created_at",
        "card_id",
        "card_msg_id",
        "card_trace_id",
        "chat_id",
        "create_epoch",
        "created_at",
        "deferred_background_review_closed",
        "deferred_background_review_lock",
        "deferred_background_reviews",
        "error_message",
        "existing_elements",
        "flush",
        "footer",
        "guard",
        "home",
        "linear",
        "message_id",
        "sequence",
        "state",
        "terminal_reason",
        "terminal_source",
        "text",
        "thread_id",
        "tool_use",
        "unified_state",
    )

    def __init__(
        self,
        message_id: str,
        chat_id: str,
        loop: asyncio.AbstractEventLoop,
        thread_id: str | None = None,
    ) -> None:
        self.message_id = message_id
        self.anchor_id: str | None = None
        self.chat_id = chat_id
        # v1.8.1 (P1): 飞书话题隔离键。话题群/群内话题中同一 chat 的不同
        # topic 共享 chat_id 但各自是独立的 hermes session（session key 含
        # thread_id，gateway/session.py:1198）；并发 seal 检查据此跳过
        # 其它话题的活跃卡。普通群聊/私聊为 None → 退回 chat_id 维度。
        self.thread_id: str | None = thread_id
        self.state: str = IDLE
        self.card_msg_id: str | None = None
        self.card_id: str | None = None
        # v1.1.0: card_trace_id — short unique ID for correlating all logs
        # belonging to one card's lifecycle. Format: last 6 chars of msg_id.
        self.card_trace_id: str = (message_id or "??????")[-6:]
        self.text = TextState()
        self.tool_use = ToolUseTracker()
        self.flush = FlushController()
        self.footer: dict[str, Any] = {}
        self.sequence = 1
        self._loop = loop
        self.created_at = time.time()
        self.deferred_background_review_closed = False
        self.deferred_background_reviews: list[tuple[str, Any]] = []
        self.deferred_background_review_lock = Lock()

        # ── State machine enhancements ──
        self.create_epoch: int = 0  # Incremented on terminal phase entry
        self._create_epoch_snap: int = 0  # Snapshotted when creation starts
        self.terminal_reason: str = ""  # Why the session ended
        self.terminal_source: str = ""  # Which code path triggered terminal

        self.guard = UnavailableGuard(
            reply_to_message_id=message_id,
            get_card_message_id=lambda: self.card_msg_id,
            on_terminate=self._on_guard_terminate,
        )

        self.linear = False
        self.unified_state: UnifiedLinearState | None = None
        self.existing_elements: set[str] = set()
        self._creation_stages: set[str] = set()
        self.card_created_at: float = 0.0
        self._was_aborted: bool = False
        self.error_message: str = ""
        self._first_flush_done: bool = False
        self._first_answer_time: float = 0.0
        self._pending_flush: bool = False
        self._streaming_closed: bool = False
        # v1.2.0 L1: "streaming closed" 日志去重——同一张卡第一次打 INFO，之后降 DEBUG
        self._streaming_closed_logged: bool = False
        self._card_ready: asyncio.Event = asyncio.Event()
        self._is_continuation: bool = False
        self._continuation_reactivation_count: int = 0

        # ── v1.10.0 seal watchdog / 拆卡自愈 ──────────────────────────
        # 会话所属 profile home（创建时解析），watchdog 重派封卡时据此绑定
        # multiplex 路由（协程自身 task context 内 set，不污染主 loop）。
        try:
            from ..config.reader import current_hermes_home

            self.home: str = str(current_hermes_home())
        except Exception:
            self.home = ""
        # 最后一次流式事件（answer/thinking/reasoning/tool）的 monotonic
        # 时间戳；初始化为创建时刻，防止新卡瞬间触发 watchdog 判死。
        self._last_event_at: float = time.monotonic()
        # 封卡协程最近一次派发时间（_do_linear_complete_with_fallback 入口
        # 统一打点）——watchdog COMPLETING 重派规则的判据。
        self._seal_dispatched_at: float = 0.0
        # 封卡已执行过（含 watchdog 拆卡的部分封卡与完整封卡，成功或失败
        # 均算）——_do_linear_complete 防重入判据；重入时补发遗留答案后释放。
        self._seal_done: bool = False
        # 串行化封卡执行，防双重派发导致 footer/error 面板重复 add。
        self._seal_lock = asyncio.Lock()
        # watchdog 拆卡标记：卡片已被 watchdog 部分封卡（close_streaming），
        # 会话保持非终态等待后续输出触发链式续写（_watchdog_split）。
        self._watchdog_split: bool = False
        # 拆卡封卡时已上卡的答案长度——重入兜底据此只补发拆卡之后新到的
        # 尾段（unified_state 在拆卡时保留，on_completed 的合并会延长它）。
        self._split_sealed_answer_len: int = 0
        # watchdog 链式拆卡深度（0 = 非 watchdog 链），上限防失控循环。
        self._chain_depth: int = 0

    def transition(self, to: str, source: str = "", reason: str = "") -> bool:
        """rejected.  Illegal transitions are logged but do not raise."""
        from_phase = self.state
        if from_phase == to:
            return True  # idempotent

        if not is_legal_transition(from_phase, to):
            _logger.warning(
                "phase transition rejected: %s → %s (source=%s msg=%s)",
                from_phase,
                to,
                source,
                (self.message_id or "?")[:12],
            )
            return False

        self.state = to
        _logger.info(
            "phase transition: %s → %s (source=%s msg=%s)",
            from_phase,
            to,
            source,
            (self.message_id or "?")[:12],
        )

        # Track terminal metadata
        if to in TERMINAL_PHASES:
            self.enter_terminal(
                reason=reason or TerminalReason.ERROR,
                source=source,
            )

        # Snapshot epoch when entering CREATING (for stale-create detection)
        if to == CardPhase.CREATING:
            self._create_epoch_snap = self.create_epoch

        return True

    def should_proceed(self, source: str = "") -> bool:
        """Combines:"""
        if self.state in TERMINAL_PHASES:
            return False
        if self.guard.should_skip(source):
            return False
        return True

    @property
    def is_terminal_phase(self) -> bool:
        """Whether the session is in a terminal (absorbing) phase."""
        return self.state in TERMINAL_PHASES

    def is_stale_create(self, epoch: int) -> bool:
        """Usage::"""
        return epoch != self.create_epoch

    def enter_terminal(self, reason: str, source: str = "") -> None:
        """This is called automatically by ``transition()`` when the target"""
        if self.terminal_reason:
            return  # Already recorded — keep the first reason
        self.terminal_reason = reason
        self.terminal_source = source
        self.create_epoch += 1

    def _on_guard_terminate(self) -> None:
        """Callback from UnavailableGuard — message was deleted/recalled."""
        if self.state in TERMINAL_PHASES:
            return
        self.state = CardPhase.TERMINATED
        self.enter_terminal(
            reason=TerminalReason.UNAVAILABLE,
            source="unavailable_guard",
        )
        # Signal readiness so awaiters don't deadlock
        self._card_ready.set()

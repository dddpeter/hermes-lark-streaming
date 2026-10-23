"""Callback wrapping for AIAgent streaming callbacks."""

from __future__ import annotations

from . import (
    _get_event_message_id,
    _logger,
    _msg_ctx,
    _thread_local_ctx,
)


def _resolve_eid(fallback_eid: str | None) -> str | None:
    """Re-resolve the current event_message_id from _msg_ctx at call time."""
    _eid = _get_event_message_id()
    return _eid if _eid else fallback_eid


def _maybe_wrap_callbacks(agent) -> None:
    """Replace streaming callbacks on *agent* with wrappers that also fire
    Feishu CardKit updates.  Skips silently when outside a Feishu message
    context (i.e. no event_message_id in context)."""
    _logger.debug(
        "HLS: _maybe_wrap_callbacks invoked, has_stream=%s, eid_lookup=%s",
        bool(getattr(agent, "stream_delta_callback", None)),
        bool(_get_event_message_id()),
    )

    eid = _get_event_message_id()
    if not eid:
        _logger.debug("HLS: skip — no event_message_id in ctx")
        return  # Not in a hermes-lark-streaming context — skip

    _current_stream = getattr(agent, "stream_delta_callback", None)
    _current_interim = getattr(agent, "interim_assistant_callback", None)
    _any_wrapped = (
        _current_stream and getattr(_current_stream, "_hls_wrapper", False)
    ) or (_current_interim and getattr(_current_interim, "_hls_wrapper", False))
    if _any_wrapped:
        # ── Late-arriving reasoning_callback fix ──
        _late_reasoning = getattr(agent, "reasoning_callback", None)
        if _late_reasoning and not getattr(_late_reasoning, "_hls_wrapper", False):
            _orig_late = _late_reasoning

            def _late_reasoning_wrapper(text, *args, **kwargs):
                _eid = _resolve_eid(eid)
                try:
                    from .hooks import on_reasoning_delta

                    if text and _eid:
                        on_reasoning_delta(message_id=_eid, text=text)
                except Exception:
                    _logger.debug("HLS: suppressed exception", exc_info=True)
                # again with a stale eid, duplicating reasoning text.
                if not getattr(_orig_late, "_hls_wrapper", False):
                    return _orig_late(text, *args, **kwargs)

            agent.reasoning_callback = _late_reasoning_wrapper
            setattr(agent.reasoning_callback, "_hls_wrapper", True)
        return

    # v1.3.2 fix (P3-02): _stream_consumed_len is cleaned up when the thinking
    _stream_consumed_len: dict[str, int] = {}

    def _cleanup_consumed_len(_eid: str) -> None:
        """Remove consumed-length tracking for a completed message."""
        _stream_consumed_len.pop(_eid, None)

    if getattr(agent, "stream_delta_callback", None):
        _orig_stream = agent.stream_delta_callback

        def _answer_wrapper(text, *args, **kwargs):
            _eid = _resolve_eid(eid)
            if not _eid:
                return _orig_stream(text, *args, **kwargs)
            try:
                from .hooks import on_answer_delta

                if text and on_answer_delta(message_id=_eid, text=text):
                    # Record total consumed length for dedup with interim_assistant_callback
                    _stream_consumed_len[_eid] = _stream_consumed_len.get(
                        _eid, 0
                    ) + len(text)
                    return
            except Exception:
                _logger.debug("HLS: answer_wrapper exception", exc_info=True)
            return _orig_stream(text, *args, **kwargs)

        agent.stream_delta_callback = _answer_wrapper
        _logger.debug("HLS: _maybe_wrap_callbacks stream_delta_callback wrapped")
    else:
        # Fix: Create our own stream_delta_callback that routes answer tokens to
        def _answer_wrapper_synthetic(text, *args, **kwargs):
            # Handle None — stream boundary signal from conversation_loop
            # (tool boundary flush / end-of-stream). Just ignore it.
            if text is None:
                return
            _eid = _resolve_eid(eid)
            if not _eid:
                return
            try:
                from .hooks import on_answer_delta

                if text and on_answer_delta(message_id=_eid, text=text):
                    _stream_consumed_len[_eid] = _stream_consumed_len.get(
                        _eid, 0
                    ) + len(text)
                    return
            except Exception:
                _logger.debug("HLS: answer_wrapper_synthetic exception", exc_info=True)
            # No original callback to call — Hermes didn't provide one

        agent.stream_delta_callback = _answer_wrapper_synthetic
        setattr(agent.stream_delta_callback, "_hls_wrapper", True)

    if getattr(agent, "interim_assistant_callback", None):
        _orig_interim = agent.interim_assistant_callback

        def _thinking_wrapper(text, *args, **kwargs):
            _eid = _resolve_eid(eid)
            if not _eid:
                return _orig_interim(text, *args, **kwargs)
            try:
                # ── already_streamed passthrough (Hermes hint) ──
                already_streamed = kwargs.get("already_streamed", False)
                if already_streamed:
                    # v1.3.2 fix (P3-02): clean up consumed-length tracking for
                    _cleanup_consumed_len(_eid)
                    return _orig_interim(text, *args, **kwargs)

                # ── Length-based dedup ──
                consumed_len = _stream_consumed_len.get(_eid, 0)
                if text and consumed_len > 0 and len(text) <= consumed_len:
                    # v1.3.2 fix (P3-02): same cleanup — text fully consumed,
                    # message streaming is done.
                    _cleanup_consumed_len(_eid)
                    return _orig_interim(text, *args, **kwargs)

                if text:
                    from .hooks import on_thinking_delta

                    consumed = on_thinking_delta(message_id=_eid, text=text)
                    if consumed:
                        return
            except Exception:
                _logger.debug("HLS: thinking_wrapper exception", exc_info=True)
            return _orig_interim(text, *args, **kwargs)

        agent.interim_assistant_callback = _thinking_wrapper
        setattr(agent.interim_assistant_callback, "_hls_wrapper", True)
        _logger.debug("HLS: _maybe_wrap_callbacks interim_assistant_callback wrapped")
    else:
        _logger.debug(
            "HLS: _maybe_wrap_callbacks NO interim_assistant_callback on agent"
        )

    # ── TOOL: wrap tool_progress_callback ──
    if getattr(agent, "tool_progress_callback", None):
        _orig_tool = agent.tool_progress_callback

        def _tool_wrapper(event_type, tool_name=None, preview=None, *args, **kwargs):
            _eid = _resolve_eid(eid)
            if not _eid:
                return _orig_tool(event_type, tool_name, preview, *args, **kwargs)
            # v1.8.8: 稳定 ID 回调（tool_start/complete_callback）接管
            # started/completed 后将其**吞掉**而非透传——feishu 注册
            # display_tier=medium，默认 tool_progress="new"（开启），网关
            # progress_callback 收到 tool.started 会真发气泡（run_turn_
            # progress.py _progress_emit）；透传 = 气泡 + 卡片面板双展示。
            # 吞掉与下方消费路径同构：卡片面板是唯一工具展示面。
            if event_type in ("tool.started", "tool.completed") and getattr(
                agent, "_hls_stable_tool_ids", False
            ):
                return
            try:
                from .hooks import on_tool_updated

                if event_type in ("tool.started", "tool.completed"):
                    if on_tool_updated(
                        message_id=_eid,
                        tool_name=tool_name or "",
                        status="started"
                        if event_type == "tool.started"
                        else "completed",
                        detail=preview or "",
                    ):
                        return
            except Exception:
                _logger.debug("HLS: tool_wrapper exception", exc_info=True)
            return _orig_tool(event_type, tool_name, preview, *args, **kwargs)

        agent.tool_progress_callback = _tool_wrapper

    # ── TOOL (v1.8.8): 稳定 call_id 回调 — tool_start_callback / tool_complete_callback ──
    # 新版 hermes（生产 v0.21+ 已具备）除 tool_progress_callback 外还触发携带
    # 稳定调用 ID 的成对回调：tool_start_callback(call_id, name, display_args) /
    # tool_complete_callback(call_id, name, display_args, result)。并行同名工具
    # 调用（max_parallel_turns）下按名字配对会错卡，这里按 call_id 精确配对；
    # 旧版 hermes 无这两个属性（hasattr 判定，不能用真值——默认 None）时整段
    # 跳过，走上方 progress 按名字配对的老路径。
    if hasattr(agent, "tool_start_callback") and hasattr(
        agent, "tool_complete_callback"
    ):
        _orig_tool_start = getattr(agent, "tool_start_callback", None)
        _orig_tool_complete = getattr(agent, "tool_complete_callback", None)

        def _stable_args_preview(tool_name: str, display_args) -> str:
            """稳定路径 started 预览：优先复用 hermes 自己的预览构造器
            （agent.display.build_tool_preview，与 progress 路径卡片展示同源），
            不可导入时退回紧凑 JSON——退回前先过 hermes 的参数脱敏器：上游
            ``_redact_tool_args_for_display(...) or ref.args`` 在脱敏器返回空
            时会把**原始 args** 传下来，整包 dump 的暴露面大于"主参数预览"。"""
            try:
                from agent.display import build_tool_preview

                return build_tool_preview(tool_name, display_args) or ""
            except Exception:
                pass
            try:
                from agent.display import redact_tool_args_for_display

                display_args = (
                    redact_tool_args_for_display(tool_name, display_args)
                    or display_args
                )
            except Exception:
                pass
            try:
                import json

                s = json.dumps(display_args, ensure_ascii=False, default=str)
                return s if len(s) <= 120 else s[:119] + "…"
            except Exception:
                return str(display_args)[:120]

        def _stable_detect_failure(tool_name: str, result) -> tuple[bool, str]:
            """(is_error, 可读后缀)。优先复用 hermes 的 canonical 判定
            agent.display._detect_tool_failure（progress 路径 is_error 的同源
            实现），不可导入时用保守启发式。"""
            try:
                from agent.display import _detect_tool_failure

                return _detect_tool_failure(tool_name, result)
            except Exception:
                pass
            data = result if isinstance(result, dict) else None
            if data is not None:
                err = data.get("error") or (
                    data.get("message") if data.get("success") is False else None
                )
                if err:
                    return True, f" [{str(err)[:80]}]"
            elif isinstance(result, str) and result.lstrip()[:5].lower().startswith(
                ("error", "trace")
            ):
                return True, ""
            return False, ""

        def _tool_start_wrapper(call_id, tool_name=None, *args, **kwargs):
            # 扇出语义：先喂卡片，再**始终**透传 hermes 原回调——网关在
            # approval 会话把 tool_start 挂成 combined_tool_start_callback
            # （session API SSE 推送 / Discord 语音回执 / Slack 任务卡，见
            # run_turn_runner._wire_turn_agent_callbacks），是功能通道而非
            # 展示冗余，"卡片消费即吞"会掐断它们。与 reasoning_callback
            # 包装器同构（feed + 透传）。
            _eid = _resolve_eid(eid)
            if _eid:
                try:
                    from .hooks import on_tool_updated

                    on_tool_updated(
                        message_id=_eid,
                        tool_name=tool_name or "",
                        status="started",
                        detail=_stable_args_preview(
                            tool_name or "",
                            args[0] if args else kwargs.get("display_args"),
                        ),
                        tool_call_id=str(call_id or ""),
                    )
                except Exception:
                    _logger.debug("HLS: tool_start_wrapper exception", exc_info=True)
            if _orig_tool_start is not None:
                return _orig_tool_start(call_id, tool_name, *args, **kwargs)

        def _tool_complete_wrapper(call_id, tool_name=None, *args, **kwargs):
            # 扇出语义同上：喂卡 + 透传 orig（approval 会话的
            # combined_tool_complete_callback 承担 SSE/任务卡收尾）。
            _eid = _resolve_eid(eid)
            # hermes 调用形状: (call_id, name, display_args, result)
            _result = (
                kwargs["result"]
                if "result" in kwargs
                else (args[1] if len(args) > 1 else None)
            )
            if _eid:
                try:
                    from .hooks import on_tool_updated

                    is_error, suffix = _stable_detect_failure(
                        tool_name or "", _result
                    )
                    on_tool_updated(
                        message_id=_eid,
                        tool_name=tool_name or "",
                        status="error" if is_error else "completed",
                        detail=(suffix or "").strip() if is_error else "",
                        tool_call_id=str(call_id or ""),
                    )
                except Exception:
                    _logger.debug(
                        "HLS: tool_complete_wrapper exception", exc_info=True
                    )
            if _orig_tool_complete is not None:
                return _orig_tool_complete(call_id, tool_name, *args, **kwargs)

        agent.tool_start_callback = _tool_start_wrapper
        agent.tool_complete_callback = _tool_complete_wrapper
        setattr(agent.tool_start_callback, "_hls_wrapper", True)
        setattr(agent.tool_complete_callback, "_hls_wrapper", True)
        # started/completed 由稳定 ID 路径接管，progress 包装器据此防双记。
        agent._hls_stable_tool_ids = True
        _logger.debug(
            "HLS: _maybe_wrap_callbacks tool_start/complete_callback wrapped (stable call_id)"
        )

    # Mark wrapper functions so guard can detect them next time
    if getattr(agent, "stream_delta_callback", None):
        setattr(agent.stream_delta_callback, "_hls_wrapper", True)
    # interim_assistant_callback is already marked above (in its wrapper block)
    if getattr(agent, "tool_progress_callback", None):
        setattr(agent.tool_progress_callback, "_hls_wrapper", True)

    # ── REASONING: wrap reasoning_callback ──
    _orig_reasoning = getattr(agent, "reasoning_callback", None)

    def _reasoning_wrapper(text, *args, **kwargs):
        _eid = _resolve_eid(eid)
        if not _eid:
            # No ctx — call original if present
            if _orig_reasoning and not getattr(_orig_reasoning, "_hls_wrapper", False):
                return _orig_reasoning(text, *args, **kwargs)
            return
        try:
            from .hooks import on_reasoning_delta

            if text:
                on_reasoning_delta(message_id=_eid, text=text)
        except Exception:
            _logger.debug("HLS: reasoning_wrapper exception", exc_info=True)
        if _orig_reasoning and not getattr(_orig_reasoning, "_hls_wrapper", False):
            return _orig_reasoning(text, *args, **kwargs)

    agent.reasoning_callback = _reasoning_wrapper
    setattr(agent.reasoning_callback, "_hls_wrapper", True)

    # ── BACKGROUND_REVIEW: wrap background_review_callback ──
    if getattr(agent, "background_review_callback", None):
        _orig_bg = agent.background_review_callback

        def _bg_wrapper(message, *args, **kwargs):
            _eid = _resolve_eid(eid)
            if not _eid:
                return _orig_bg(message, *args, **kwargs)
            try:
                from .hooks import on_background_review_message

                deferred = on_background_review_message(
                    message_id=_eid,
                    text=message,
                    sender=_orig_bg,
                )
                if deferred:
                    return
            except Exception:
                _logger.debug("HLS: bg_wrapper exception", exc_info=True)
            return _orig_bg(message, *args, **kwargs)

        agent.background_review_callback = _bg_wrapper

    # Mark background_review_callback wrapper (already marked above for others)
    if getattr(agent, "background_review_callback", None):
        setattr(agent.background_review_callback, "_hls_wrapper", True)

    # ── Store agent reference for cache token extraction ──
    ctx = _msg_ctx.get()
    if ctx is not None:
        ctx["_agent_ref"] = agent
        _thread_local_ctx.data = dict(ctx)

"""v1.8.8 回归测试 — 适配 hermes-agent 现役版本（GatewayRunner mixin 化重构后）.

三个主题（对照新版 hermes-agent 接口核对结论，见 CHANGELOG v1.8.8）：

A. FeishuAdapter 解析路径顺序 — 现役真身是源码路径
   ``plugins.platforms.feishu.adapter``（platform_registry deferred loader 的
   最终 import 目标）；v0.17–v0.21 时代的 ``hermes_plugins.feishu_platform``
   双类结构已消亡，降级末位兜底。

B. ``retire_clarify_card`` 官方钩子 — clarify 在未点击结束时（打字作答 /
   超时 / 取消）gateway 会调 ``adapter.retire_clarify_card(clarify_id,
   notice)``，要求持久卡停用按钮。此前插件不实现，卡片按钮永久假活。

C. 稳定 call_id 工具配对 — 新版 hermes 触发携带稳定调用 ID 的成对回调
   ``tool_start_callback(call_id, name, display_args)`` /
   ``tool_complete_callback(call_id, name, display_args, result)``；
   并行同名工具按名字配对会错卡，现按 call_id 精确配对，progress 路径
   防双记，旧版 hermes（无这两个属性）自动回退老路径。
"""

from __future__ import annotations

import asyncio
import sys
import threading
import types
from unittest.mock import MagicMock, patch

import pytest
from hermes_lark_streaming.cardkit import (
    build_clarify_confirmed_card,
    build_clarify_retired_card,
)
from hermes_lark_streaming.patching import (
    _apply_feishu_adapter_patches,
    _make_feishu_adapter_retire_clarify,
    _maybe_wrap_callbacks,
    _msg_ctx,
    _thread_local_ctx,
)
from hermes_lark_streaming.patching import adapter as hls_adapter
from hermes_lark_streaming.patching import hooks as hls_hooks
from hermes_lark_streaming.state.tooluse import ToolUseTracker

# ══════════════════════════════════════════════════════════════════════
# A. FeishuAdapter 解析路径顺序
# ══════════════════════════════════════════════════════════════════════


def _fake_module(name: str, cls_name: str) -> types.ModuleType:
    mod = types.ModuleType(name)
    setattr(mod, cls_name, type(cls_name, (), {"_origin": name}))
    return mod


class TestFeishuAdapterResolutionOrder:
    """v1.8.8: 源码路径第一、消亡的 hermes_plugins 路径末位。"""

    def test_source_path_wins_over_dead_and_legacy(self):
        """三个候选同时存在时取 plugins.platforms.feishu.adapter。"""
        mods = {
            "plugins.platforms.feishu.adapter": _fake_module(
                "plugins.platforms.feishu.adapter", "FeishuAdapter"
            ),
            "plugins.platforms.feishu": types.ModuleType("plugins.platforms.feishu"),
            "plugins.platforms": types.ModuleType("plugins.platforms"),
            "plugins": types.ModuleType("plugins"),
            "gateway.platforms.feishu": _fake_module(
                "gateway.platforms.feishu", "FeishuAdapter"
            ),
            "gateway.platforms": types.ModuleType("gateway.platforms"),
            "gateway": types.ModuleType("gateway"),
            "hermes_plugins.feishu_platform.adapter": _fake_module(
                "hermes_plugins.feishu_platform.adapter", "FeishuAdapter"
            ),
            "hermes_plugins.feishu_platform": types.ModuleType(
                "hermes_plugins.feishu_platform"
            ),
            "hermes_plugins": types.ModuleType("hermes_plugins"),
        }
        saved = {k: sys.modules.get(k) for k in mods}
        try:
            sys.modules.update(mods)
            from hermes_lark_streaming.patching.hermes_adapter import HermesCompat

            # __new__ 绕过 __init__：后者会连带解析 GatewayRunner/cron 等重型
            # 目标（在本机 hermes venv 下会 import 真实 gateway.run）。
            hc = HermesCompat.__new__(HermesCompat)
            cls = hc._resolve_feishu_adapter()
            assert cls is mods["plugins.platforms.feishu.adapter"].FeishuAdapter
        finally:
            for k, v in saved.items():
                if v is None:
                    sys.modules.pop(k, None)
                else:
                    sys.modules[k] = v

    def test_dead_path_still_resolvable_as_legacy_fallback(self, monkeypatch):
        """现役路径全缺失时，老部署的 hermes_plugins 路径仍兜底可解析。

        用 stub 掉的 import_module 隔离：本机 hermes venv 的 sys.path 上就有
        真实 hermes 源码（plugins.platforms.feishu.adapter 可真导入），仅摘
        sys.modules 不足以制造"缺席"。"""
        legacy_mod = _fake_module(
            "hermes_plugins.feishu_platform.adapter", "FeishuAdapter"
        )
        served = {"hermes_plugins.feishu_platform.adapter": legacy_mod}

        class _StubImportlib:
            @staticmethod
            def import_module(name):
                if name in served:
                    return served[name]
                raise ImportError(name)

        from hermes_lark_streaming.patching import hermes_adapter

        monkeypatch.setattr(hermes_adapter, "importlib", _StubImportlib)
        hc = hermes_adapter.HermesCompat.__new__(hermes_adapter.HermesCompat)
        cls = hc._resolve_feishu_adapter()
        assert cls is legacy_mod.FeishuAdapter


# ══════════════════════════════════════════════════════════════════════
# B. retire_clarify_card 官方钩子
# ══════════════════════════════════════════════════════════════════════


@pytest.fixture()
def clarify_state():
    """注入一条 clarify 状态，返回其 id。"""
    cid = "clr_v188"
    with hls_adapter._clarify_lock:
        hls_adapter._clarify_card_msg_ids[cid] = "om_card_v188"
        hls_adapter._clarify_questions[cid] = "继续吗？"
        hls_adapter._clarify_choices[cid] = ["是", "否"]
        hls_adapter._clarify_selections[cid] = "是"
        hls_adapter._clarify_timestamps[cid] = 1_000_000.0
    yield cid
    with hls_adapter._clarify_lock:
        for store in (
            hls_adapter._clarify_card_msg_ids,
            hls_adapter._clarify_questions,
            hls_adapter._clarify_choices,
            hls_adapter._clarify_selections,
            hls_adapter._clarify_timestamps,
        ):
            store.pop(cid, None)


def _ctrl_with_client(client: MagicMock) -> MagicMock:
    ctrl = MagicMock()
    ctrl.enabled = True
    ctrl._client_ok.return_value = True
    ctrl._client = client
    return ctrl


class _RetireClient:
    """update_card 时断言插件侧状态已被同步 pop（先于第一个 await 的契约）。"""

    def __init__(self):
        self.updates = []

    async def update_card(self, card_msg_id, card_data):
        with hls_adapter._clarify_lock:
            # 契约：pop 必须发生在第一个 await（即本次 update_card）之前。
            assert not any(
                cid == "clr_v188"
                for cid in hls_adapter._clarify_card_msg_ids
            ), "retire must pop plugin state before its first await"
        self.updates.append((card_msg_id, card_data))


class TestRetireClarifyCard:
    def test_pops_state_before_first_await_and_updates_card(self, clarify_state):
        client = _RetireClient()
        method = _make_feishu_adapter_retire_clarify(None)
        with patch(
            "hermes_lark_streaming.controller.get_controller",
            return_value=_ctrl_with_client(client),
        ):
            asyncio.run(method(object(), clarify_state, "已作答"))
        assert len(client.updates) == 1
        card_msg_id, card_data = client.updates[0]
        assert card_msg_id == "om_card_v188"
        # retired 卡：无按钮（不再"看起来可回答"），notice 入正文
        body = card_data["body"]["elements"]
        assert all(el.get("tag") != "button" for el in body)
        assert any("已作答" in str(el) for el in body)

    def test_no_card_is_noop_and_never_raises(self):
        method = _make_feishu_adapter_retire_clarify(None)
        # 无状态、无 controller —— 静默通过。
        asyncio.run(method(object(), "clr_missing", "timeout"))

    def test_orig_called_first_when_class_defines_one(self, clarify_state):
        order = []

        async def orig_retire(self_feishu, cid, notice):
            order.append("orig")

        class _Client:
            async def update_card(self, *a, **k):
                order.append("update")

        method = _make_feishu_adapter_retire_clarify(orig_retire)
        with patch(
            "hermes_lark_streaming.controller.get_controller",
            return_value=_ctrl_with_client(_Client()),
        ):
            asyncio.run(method(object(), clarify_state, "cancelled"))
        assert order == ["orig", "update"]

    def test_update_failure_never_propagates(self, clarify_state):
        class _BoomClient:
            async def update_card(self, *a, **k):
                raise RuntimeError("network down")

        method = _make_feishu_adapter_retire_clarify(None)
        with patch(
            "hermes_lark_streaming.controller.get_controller",
            return_value=_ctrl_with_client(_BoomClient()),
        ):
            asyncio.run(method(object(), clarify_state, "answered"))  # 不抛即通过

    def test_installed_by_apply_feishu_adapter_patches(self):
        class _FakeFeishuAdapter:
            async def send(self, *a, **k): ...

            async def edit_message(self, *a, **k): ...

            async def _add_reaction(self, *a, **k): ...

            async def _remove_reaction(self, *a, **k): ...

            async def send_clarify(self, *a, **k): ...

            async def _handle_card_action_event(self, *a, **k): ...

        cls = type("PatchedFeishuAdapter", (_FakeFeishuAdapter,), {})
        assert _apply_feishu_adapter_patches(cls)
        assert callable(cls.__dict__.get("retire_clarify_card"))
        # 幂等：二次 apply 不重装（sentinel 生效）。
        first = cls.__dict__["retire_clarify_card"]
        assert _apply_feishu_adapter_patches(cls)
        assert cls.__dict__["retire_clarify_card"] is first


class TestRetiredCardBuilder:
    def test_renders_bilingual_notice_without_buttons(self):
        card = build_clarify_retired_card(question="选哪个？", notice="已作答")
        assert card["schema"] == "2.0"
        body = card["body"]["elements"]
        assert all(el.get("tag") != "button" for el in body)
        texts = [str(el.get("text", {})) for el in body]
        assert any("选哪个？" in t for t in texts)
        assert any("已作答" in t for t in texts)

    def test_empty_notice_falls_back(self):
        card = build_clarify_retired_card(question="q", notice="")
        assert card["body"]["elements"]  # 不因空 notice 崩溃

    def test_confirmed_card_still_builds(self):
        # 回归守护：v1.8.8 编辑 special.py 时曾误删 confirmed 构造器 def 行。
        card = build_clarify_confirmed_card(question="q", selected="s")
        assert card["schema"] == "2.0"


# ══════════════════════════════════════════════════════════════════════
# C. 稳定 call_id 工具配对
# ══════════════════════════════════════════════════════════════════════


class TestStableCallIdPairing:
    def test_parallel_same_name_tools_pair_by_call_id(self):
        t = ToolUseTracker()
        t.record_start("read_file", "a.py", call_id="call_A")
        t.record_start("read_file", "b.py", call_id="call_B")
        # 乱序完成：先 A 后 B，B 带错误。
        t.record_end("read_file", output="ra", call_id="call_A")
        t.record_end("read_file", error="boom", call_id="call_B")
        steps = t._session.steps
        assert [s.status for s in steps] == ["success", "error"]
        assert steps[0].detail == "a.py" and steps[0].output == "ra"
        assert steps[1].detail == "b.py" and steps[1].error == "boom"

    def test_duplicate_start_same_call_id_is_idempotent(self):
        t = ToolUseTracker()
        t.record_start("web_search", "q1", call_id="call_X")
        t.record_start("web_search", "q1", call_id="call_X")
        assert len(t._session.steps) == 1

    def test_call_id_miss_falls_back_to_name_match(self):
        t = ToolUseTracker()
        t.record_start("terminal", "ls")  # 无 call_id（旧版路径）
        t.record_end("terminal", output="ok", call_id="never_seen")
        assert t._session.steps[0].status == "success"
        assert t._session.steps[0].output == "ok"

    def test_name_fallback_unchanged_without_call_id(self):
        t = ToolUseTracker()
        t.record_start("terminal", "ls")
        t.record_start("terminal", "pwd")
        t.record_end("terminal", output="ok")
        steps = t._session.steps
        # 最近一个 running（pwd）被结束 —— 既有行为不变。
        assert steps[1].status == "success" and steps[0].status == "running"


class _StableAgent:
    """带新版 hermes 稳定回调属性（默认 None）的 agent 替身。"""

    def __init__(self):
        self.stream_delta_callback = lambda *a, **k: None
        self.interim_assistant_callback = lambda *a, **k: None
        self.tool_progress_calls = []
        self.tool_progress_callback = lambda *a, **k: self.tool_progress_calls.append((a, k))
        self.tool_start_callback = None
        self.tool_complete_callback = None
        self.reasoning_callback = None
        self.background_review_callback = None


class _LegacyAgent(_StableAgent):
    """旧版 hermes：无 tool_start/complete_callback 属性。"""

    def __init__(self):
        super().__init__()
        del self.tool_start_callback
        del self.tool_complete_callback


def _set_msg_ctx(eid: str = "eid_v188_stable") -> None:
    _msg_ctx.set(
        {
            "message_id": "msg_v188",
            "chat_id": "chat_v188",
            "anchor_id": None,
            "event_message_id": eid,
            "card_sent": False,
        }
    )
    _thread_local_ctx.data = dict(_msg_ctx.get())


def _clear_msg_ctx() -> None:
    _msg_ctx.set(None)
    _thread_local_ctx.data = None


class TestStableToolCallbacks:
    @pytest.fixture(autouse=True)
    def _ctx(self, monkeypatch):
        _set_msg_ctx()
        # controller mock 贯穿整个用例（包装时 + 回调触发时都要在位——
        # with patch 在 _wrap 返回时退出，会漏掉后续回调触发）。
        ctrl = MagicMock()
        ctrl.enabled = True
        calls: list[dict] = []
        ctrl.on_tool_update = MagicMock(
            side_effect=lambda **kw: (calls.append(kw), True)[1]
        )
        monkeypatch.setattr(hls_hooks, "get_controller", lambda: ctrl)
        self._calls = calls
        yield
        _clear_msg_ctx()

    def _wrap(self, agent) -> list[dict]:
        _maybe_wrap_callbacks(agent)
        return self._calls

    def test_wrappers_installed_and_flag_set(self):
        agent = _StableAgent()
        self._wrap(agent)
        assert getattr(agent.tool_start_callback, "_hls_wrapper", False)
        assert getattr(agent.tool_complete_callback, "_hls_wrapper", False)
        assert agent._hls_stable_tool_ids is True

    def test_start_complete_route_with_call_id(self):
        agent = _StableAgent()
        calls = self._wrap(agent)
        agent.tool_start_callback("call_A", "read_file", {"path": "a.py"})
        agent.tool_start_callback("call_B", "read_file", {"path": "b.py"})
        agent.tool_complete_callback(
            "call_A", "read_file", {"path": "a.py"}, {"content": "ra"}
        )
        agent.tool_complete_callback(
            "call_B",
            "read_file",
            {"path": "b.py"},
            {"error": "not found"},  # 失败结果 → error 态
        )
        assert [c["tool_call_id"] for c in calls] == [
            "call_A",
            "call_B",
            "call_A",
            "call_B",
        ]
        assert [c["status"] for c in calls] == [
            "started",
            "started",
            "completed",
            "error",
        ]
        # started 事件带 args 预览（hermes 预览器或 JSON 兜底）。
        assert calls[0]["detail"]

    def test_progress_started_completed_suppressed_when_stable_active(self):
        agent = _StableAgent()
        calls = self._wrap(agent)
        # 稳定路径接管后，progress 的 started/completed 被吞——既不进卡片
        # （防双记），也不透传 orig：feishu 档位 display_tier=medium 默认
        # tool_progress="new"，网关 progress_callback 收到 started 会真发
        # 气泡，透传 = 气泡 + 卡片面板双展示（评审 P1 修复的回归守护）。
        agent.tool_progress_callback("tool.started", "read_file", "pv")
        agent.tool_progress_callback("tool.completed", "read_file", None)
        assert calls == []
        assert agent.tool_progress_calls == []

    def test_progress_other_events_untouched(self):
        agent = _StableAgent()
        self._wrap(agent)
        agent.tool_progress_callback(
            "tool.output_risk", "terminal", None, tool_call_id="c1", risk_metadata={}
        )
        assert len(agent.tool_progress_calls) == 1

    def test_legacy_agent_keeps_progress_path(self):
        agent = _LegacyAgent()
        calls = self._wrap(agent)
        assert not hasattr(agent, "_hls_stable_tool_ids")
        agent.tool_progress_callback("tool.started", "read_file", "pv")
        assert len(calls) == 1
        assert calls[0]["status"] == "started"
        assert calls[0]["tool_call_id"] == ""

    def test_late_callback_routes_via_wrap_time_eid(self):
        # ctx 清除后的迟到回调（如线程池 worker）经闭包 fallback eid 仍正确
        # 路由到卡片会话 —— 与 stream/thinking 包装器同一设计意图。
        agent = _StableAgent()
        orig_calls = []
        agent.tool_start_callback = lambda *a, **k: orig_calls.append(a)
        agent.tool_complete_callback = lambda *a, **k: orig_calls.append(a)
        calls = self._wrap(agent)
        _clear_msg_ctx()
        agent.tool_start_callback("call_A", "read_file", {})
        agent.tool_complete_callback("call_A", "read_file", {}, {"ok": 1})
        assert len(calls) == 2
        assert [c["tool_call_id"] for c in calls] == ["call_A", "call_A"]
        assert [c["status"] for c in calls] == ["started", "completed"]
        # 扇出语义：orig 与卡片同收（见下个用例）。
        assert len(orig_calls) == 2

    def test_orig_fanout_alongside_card(self):
        # 评审 P2 修复的回归守护：approval 会话把 tool_start/complete 挂成
        # combined_*（session API SSE / 语音回执 / Slack 任务卡），是功能
        # 通道——卡片消费后 orig 仍必须被调用（扇出，非二选一）。
        agent = _StableAgent()
        orig_calls = []
        agent.tool_start_callback = lambda *a, **k: orig_calls.append(a)
        agent.tool_complete_callback = lambda *a, **k: orig_calls.append(a)
        calls = self._wrap(agent)
        agent.tool_start_callback("call_A", "read_file", {"path": "a.py"})
        agent.tool_complete_callback(
            "call_A", "read_file", {"path": "a.py"}, {"content": "ok"}
        )
        assert len(calls) == 2  # 卡片收到
        assert len(orig_calls) == 2  # orig 也收到


class TestControllerCallIdIntegration:
    """controller.on_tool_update → ToolUseTracker 的 call_id 端到端透传。

    评审补缺：hooks 层（mock ctrl）与 tracker 层（直调 record_*）各自的
    用例不覆盖这段胶水——签名演进时若 controller 忘传 call_id，只有这里
    能拦住（配对静默退化为按名字）。
    """

    def test_call_id_flows_to_tracker_pairing(self):
        from hermes_lark_streaming.controller.core import StreamCardController

        from tests.test_controller import _enable, _make_session, _mock_client

        ctrl = StreamCardController()
        _enable(ctrl, linear=True)
        ctrl._initialized = True
        ctrl._client = _mock_client()
        session = _make_session("msg_v188_ctrl", linear=True)
        ctrl._sessions["msg_v188_ctrl"] = session

        ctrl.on_tool_update(
            message_id="msg_v188_ctrl",
            tool_name="read_file",
            status="started",
            detail="a.py",
            tool_call_id="cA",
        )
        ctrl.on_tool_update(
            message_id="msg_v188_ctrl",
            tool_name="read_file",
            status="started",
            detail="b.py",
            tool_call_id="cB",
        )
        ctrl.on_tool_update(
            message_id="msg_v188_ctrl",
            tool_name="read_file",
            status="completed",
            tool_call_id="cA",
        )

        steps = session.tool_use._session.steps
        assert [s.call_id for s in steps] == ["cA", "cB"]
        # cA 按 call_id 精确完成；cB 仍 running（旧按名字配对会错关 cB）。
        assert steps[0].status == "success"
        assert steps[1].status == "running"
        assert steps[0].detail == "a.py"


class TestCardWidthMode:
    """v1.8.8: 全部建卡点统一 config.width_mode=fill（撑满聊天窗口）。

    官方三档枚举 default(600px)/compact(400px)/fill；此前不填=default。
    feishu/client.py 的 cardkit settings 更新（streaming_mode/summary）是
    存量卡配置，width_mode 属建卡期属性，不在其列——本用例守护"所有
    建卡 builder 都带 fill"以防新增卡漏配退回 600px。"""

    def test_all_card_builders_carry_fill(self):
        from hermes_lark_streaming.aowen import build_interrupt_hint_card
        from hermes_lark_streaming.cardkit import (
            build_clarify_card,
            build_clarify_confirmed_card,
            build_clarify_retired_card,
            build_clarify_submitted_card,
            build_cron_card,
            build_gateway_card,
        )
        from hermes_lark_streaming.cardkit.cards import build_streaming_card_v2

        cards = {
            "streaming": build_streaming_card_v2(print_step=4, print_strategy="delay"),
            "cron": build_cron_card("content"),
            "gateway": build_gateway_card("hi"),
            "clarify_pending": build_clarify_card(
                question="q", choices=["a", "b"], clarify_id="c1"
            ),
            "clarify_submitted": build_clarify_submitted_card(
                question="q", selected="a", clarify_id="c1"
            ),
            "clarify_confirmed": build_clarify_confirmed_card(
                question="q", selected="a"
            ),
            "clarify_retired": build_clarify_retired_card(question="q", notice="n"),
            "aowen_hint": build_interrupt_hint_card(),
        }
        for name, card in cards.items():
            assert card["config"].get("width_mode") == "fill", name


class TestHeadlessLoadGuard:
    """v1.8.8: headless CLI 的 10s 加载超时修复。

    根因：register 阶段 fresh import run_agent/gateway.run 拖上整棵
    agent/gateway 模块树（run_agent 冷导入实测 ~15s）> hermes 插件加载器
    10s 窗口。修复：register 一律 sys_modules_only 解析（零 fresh
    import），仅网关进程（argv / gateway.run 已加载）开延迟补丁线程。"""

    @staticmethod
    def _fake_hermes_modules():
        mods = {}
        gw = types.ModuleType("gateway.run")

        class _FakeGW2:
            async def _handle_message(self, event): ...
            async def _handle_message_with_agent(self, event, source, *a, **k): ...
            async def _run_agent(self, *a, **k): ...
            async def _run_background_task(self, *a, **k): ...

        gw.GatewayRunner = _FakeGW2
        mods["gateway.run"] = gw
        mods["gateway"] = types.ModuleType("gateway")
        ra = types.ModuleType("run_agent")
        ra.AIAgent = type("_AIAgent", (), {})
        mods["run_agent"] = ra
        cs = types.ModuleType("cron.scheduler")
        cs._deliver_result = lambda *a, **k: None
        mods["cron.scheduler"] = cs
        mods["cron"] = types.ModuleType("cron")
        fa = types.ModuleType("plugins.platforms.feishu.adapter")
        fa.FeishuAdapter = type("_FA", (), {})
        mods["plugins.platforms.feishu.adapter"] = fa
        cl = types.ModuleType("agent.conversation_loop")
        cl.run_conversation = lambda *a, **k: None
        mods["agent.conversation_loop"] = cl
        mods["agent"] = types.ModuleType("agent")
        pr = types.ModuleType("gateway.platform_registry")
        pr.platform_registry = types.SimpleNamespace(
            create_adapter=lambda name, config: None
        )
        mods["gateway.platform_registry"] = pr
        return mods

    def test_sys_modules_only_resolves_without_fresh_imports(self, monkeypatch):
        from hermes_lark_streaming.patching import hermes_adapter

        mods = self._fake_hermes_modules()
        for k in list(mods):
            monkeypatch.setitem(sys.modules, k, mods[k])

        class _NoFreshImportlib:
            @staticmethod
            def import_module(name):
                raise AssertionError(f"fresh import attempted in sys_modules_only mode: {name}")

        monkeypatch.setattr(hermes_adapter, "importlib", _NoFreshImportlib)
        hc = hermes_adapter.HermesCompat(sys_modules_only=True)
        assert hc.gateway_runner_class is mods["gateway.run"].GatewayRunner
        assert hc.aiagent_class is mods["run_agent"].AIAgent
        assert hc.feishu_adapter_class is mods["plugins.platforms.feishu.adapter"].FeishuAdapter
        assert hc.cron_scheduler_module is mods["cron.scheduler"]
        assert hc.conversation_loop_func is mods["agent.conversation_loop"].run_conversation

    def test_headless_apply_starts_no_thread_and_no_imports(self, monkeypatch):
        import hermes_lark_streaming.patching as mp
        from hermes_lark_streaming.patching import hermes_adapter

        # headless：无 gateway 模块、argv 是 chat
        for k in ("gateway.run", "gateway", "gateway.platform_registry",
                  "run_agent", "cron.scheduler", "cron",
                  "plugins.platforms.feishu.adapter", "agent.conversation_loop"):
            monkeypatch.delitem(sys.modules, k, raising=False)
        monkeypatch.setattr(sys, "argv", ["hermes", "chat"])

        class _NoFreshImportlib:
            @staticmethod
            def import_module(name):
                raise AssertionError(f"fresh import attempted at register: {name}")

        monkeypatch.setattr(hermes_adapter, "importlib", _NoFreshImportlib)

        threads = []
        _real_thread = threading.Thread

        class _RecordingThread(_real_thread):
            def __init__(self, *a, **k):
                threads.append(self)
                super().__init__(*a, **k)

        monkeypatch.setattr(mp.threading, "Thread", _RecordingThread)
        monkeypatch.setattr(mp, "_gw_runner_patched", False)
        monkeypatch.setattr(mp.apply_patches, "_applied", False, raising=False)

        mp.apply_patches()
        assert threads == []  # headless：不开延迟补丁线程
        monkeypatch.setattr(mp.apply_patches, "_applied", True, raising=False)

    def test_gateway_argv_defers_and_thread_patches(self, monkeypatch):
        import hermes_lark_streaming.patching as mp

        for k in ("gateway.run", "gateway", "gateway.platform_registry",
                  "run_agent", "cron.scheduler", "cron",
                  "plugins.platforms.feishu.adapter", "agent.conversation_loop"):
            monkeypatch.delitem(sys.modules, k, raising=False)
        monkeypatch.setattr(sys, "argv", ["hermes", "gateway", "run"])
        monkeypatch.setattr(mp, "_gw_runner_patched", False)
        monkeypatch.setattr(mp.apply_patches, "_applied", False, raising=False)

        threads = []
        _real_thread = threading.Thread

        class _RecordingThread(_real_thread):
            def __init__(self, *a, **k):
                threads.append(self)
                super().__init__(*a, **k)

        monkeypatch.setattr(mp.threading, "Thread", _RecordingThread)
        try:
            mp.apply_patches()
            assert len(threads) == 1  # 网关进程：延迟补丁线程已开
            # register 返回后（线程首个 2s poll 前）注入 gateway.run ——
            # fresh 解析走 importlib.import_module，sys.modules 命中不发重型导入
            mods = self._fake_hermes_modules()
            for k, v in mods.items():
                sys.modules.setdefault(k, v)
            threads[0].join(timeout=8)
            assert getattr(mods["gateway.run"].GatewayRunner._handle_message, "_hls_wrapped", False)
        finally:
            monkeypatch.setattr(mp.apply_patches, "_applied", True, raising=False)

    def test_a2a_guard_skips_register(self, monkeypatch):
        import hermes_lark_streaming.plugin as plugin_init

        monkeypatch.setenv("HERMES_A2A_PEER", "1")
        called = []
        monkeypatch.setattr(
            plugin_init, "_check_lark_oapi_floor",
            lambda: called.append(1) or (_ for _ in ()).throw(AssertionError("floor check must not run under A2A guard")),
        )
        plugin_init.register(None)  # 守卫先于一切——直接返回
        assert called == []

    def test_likely_gateway_process_matrix(self, monkeypatch):
        from hermes_lark_streaming.patching import _likely_gateway_process

        monkeypatch.delitem(sys.modules, "gateway.run", raising=False)
        monkeypatch.setattr(sys, "argv", ["hermes", "gateway", "run"])
        assert _likely_gateway_process() is True
        monkeypatch.setattr(sys, "argv", ["hermes", "serve"])
        assert _likely_gateway_process() is True
        monkeypatch.setattr(sys, "argv", ["hermes", "chat"])
        assert _likely_gateway_process() is False
        fake = types.ModuleType("gateway.run")
        monkeypatch.setitem(sys.modules, "gateway.run", fake)
        assert _likely_gateway_process() is True

    def test_client_module_has_no_top_level_lark_import(self):
        """feishu/client.py 模块级不得出现 lark_oapi 导入（AST 级守护）——
        它会拉起整个 lark_oapi（冷 pyc ~10s），回到插件加载器超时窗口内。"""
        import ast
        from pathlib import Path

        src = Path("feishu/client.py")
        if not src.exists():  # 以包定位兜底
            from hermes_lark_streaming import feishu as _f

            src = Path(_f.__file__).parent / "client.py"
        tree = ast.parse(src.read_text(encoding="utf-8"))
        for node in tree.body:  # 只查模块级
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                names = (
                    [a.name for a in node.names]
                    if isinstance(node, ast.Import)
                    else [node.module or ""]
                )
                for n in names:
                    assert not n.startswith("lark_oapi"), (
                        f"feishu/client.py 顶层导入 {n} 会在加载器窗口内拉起 "
                        "lark_oapi（冷 pyc ~10s）——请走 _ensure_lark_names()"
                    )

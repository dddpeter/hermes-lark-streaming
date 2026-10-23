"""v1.8.5 multiplex 多 bot 回归测试.

场景：hermes multiplex 网关下三个 profile 绑三个飞书 bot。v1.8.4 的
"首副本胜出"去重后，胜出副本的 controller/FeishuClient 凭据在 init 时定死，
导致非启动 profile 的卡片跨 bot reply 失败（无卡）。

v1.8.5：_client 改为路由代理，卡片 API 调用时按当前 turn 所属 profile home
（patching wrapper 绑定的 _TURN_HOME / thread-local → hermes 实时解析 → env）
从 client 池取对应 FeishuClient；凭据来自各 home 的 config.yaml feishu 节 /
.env / env 兜底（config.reader.feishu_creds_for_home，60s TTL）。
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest
from hermes_lark_streaming.config import reader as cfg_reader
from hermes_lark_streaming.controller import core as ctrl_core


@pytest.fixture()
def two_homes(tmp_path: Path) -> tuple[Path, Path]:
    home_a = tmp_path / "profile_a"
    home_b = tmp_path / "profile_b"
    home_a.mkdir()
    home_b.mkdir()
    (home_a / "config.yaml").write_text(
        "feishu:\n  app_id: cli_aaa\n  app_secret: sec_aaa\n",
        encoding="utf-8",
    )
    (home_b / ".env").write_text(
        "FEISHU_APP_ID=cli_bbb\nFEISHU_APP_SECRET=sec_bbb\n",
        encoding="utf-8",
    )
    return home_a, home_b


@pytest.fixture(autouse=True)
def _isolated_turn_ctx(monkeypatch):
    """清空 turn home 绑定与凭据缓存，保证用例独立。"""
    cfg_reader._CRED_CACHE.clear()
    cfg_reader.set_turn_home(None)
    cfg_reader.set_turn_home_thread(None)
    yield
    cfg_reader._CRED_CACHE.clear()
    cfg_reader.set_turn_home(None)
    cfg_reader.set_turn_home_thread(None)


class TestTurnHomeTokenRoundtrip:
    """v1.8.5 hotfix 回归：set_turn_home 必须返回 ContextVar token，
    reset_turn_home 按 token 恢复——此前 gateway.py 把 `.reset` 调在普通
    函数上（'function' object has no attribute 'reset'，生产 AttributeError）。"""

    def test_token_roundtrip(self) -> None:
        cfg_reader.set_turn_home("/tmp/home_outer")
        assert cfg_reader._TURN_HOME.get() == "/tmp/home_outer"

        token = cfg_reader.set_turn_home("/tmp/home_inner")
        assert token is not None
        assert cfg_reader._TURN_HOME.get() == "/tmp/home_inner"

        cfg_reader.reset_turn_home(token)
        assert cfg_reader._TURN_HOME.get() == "/tmp/home_outer"

        # None token 安全降级为解绑
        cfg_reader.reset_turn_home(None)
        assert cfg_reader._TURN_HOME.get() is None

    def test_wrap_handle_message_no_attrerror(self) -> None:
        """_wrap_handle_message 的 token 式 set/reset 不得抛 AttributeError
        （生产报错 'function' object has no attribute 'reset' 的回归测试）。"""
        import asyncio
        from types import SimpleNamespace

        from hermes_lark_streaming.patching.gateway import _wrap_handle_message

        async def orig(self, event, *args, **kwargs):
            return "ok"

        class FakeSelf:
            def _reply_anchor_for_event(self, event):
                return None

        event = SimpleNamespace(
            message_id="m_tok",
            text="hello",
            raw_message=None,
            source=SimpleNamespace(
                platform=SimpleNamespace(value="feishu"), chat_id="c1"
            ),
        )
        wrapper = _wrap_handle_message(orig)
        # 不应抛 AttributeError；wrapper 内部设置的 home 随 task context 结束
        assert asyncio.run(wrapper(FakeSelf(), event)) == "ok"
        assert cfg_reader._TURN_HOME.get() is None


class TestFeishuCredsForHome:
    def test_creds_from_home_config_yaml(self, two_homes) -> None:
        home_a, _ = two_homes
        creds = cfg_reader.feishu_creds_for_home(home_a)
        assert creds["app_id"] == "cli_aaa"
        assert creds["app_secret"] == "sec_aaa"

    def test_creds_from_home_dotenv(self, two_homes) -> None:
        _, home_b = two_homes
        creds = cfg_reader.feishu_creds_for_home(home_b)
        assert creds["app_id"] == "cli_bbb"
        assert creds["app_secret"] == "sec_bbb"

    def test_home_config_yaml_beats_dotenv(self, tmp_path: Path) -> None:
        home = tmp_path / "profile_c"
        home.mkdir()
        (home / "config.yaml").write_text(
            "feishu:\n  app_id: cli_cfg\n  app_secret: sec_cfg\n", encoding="utf-8"
        )
        (home / ".env").write_text("FEISHU_APP_ID=cli_env\n", encoding="utf-8")
        assert cfg_reader.feishu_creds_for_home(home)["app_id"] == "cli_cfg"

    def test_env_fallback_when_no_home(self, monkeypatch) -> None:
        monkeypatch.setenv("FEISHU_APP_ID", "cli_env")
        monkeypatch.setenv("FEISHU_APP_SECRET", "sec_env")
        creds = cfg_reader.feishu_creds_for_home(None)
        assert creds["app_id"] == "cli_env"

    def test_no_creds_returns_empty(self, tmp_path: Path, monkeypatch) -> None:
        home = tmp_path / "empty"
        home.mkdir()
        monkeypatch.delenv("FEISHU_APP_ID", raising=False)
        monkeypatch.delenv("FEISHU_APP_SECRET", raising=False)
        monkeypatch.delenv("LARK_APP_ID", raising=False)
        monkeypatch.delenv("LARK_APP_SECRET", raising=False)
        assert cfg_reader.feishu_creds_for_home(home) == {}

    def test_ttl_cache_reuses_result(self, two_homes, monkeypatch) -> None:
        home_a, _ = two_homes
        first = cfg_reader.feishu_creds_for_home(home_a)
        # 改磁盘：TTL 内不得重读
        (home_a / "config.yaml").write_text(
            "feishu:\n  app_id: cli_changed\n", encoding="utf-8"
        )
        second = cfg_reader.feishu_creds_for_home(home_a)
        assert second["app_id"] == first["app_id"] == "cli_aaa"
        # TTL 过期后读到新值
        monkeypatch.setattr(cfg_reader.time, "monotonic", lambda: time.time() + 999)
        assert cfg_reader.feishu_creds_for_home(home_a)["app_id"] == "cli_changed"

    def test_thread_safe_cache(self, two_homes) -> None:
        home_a, home_b = two_homes
        errors: list[Exception] = []

        def worker(home: Path) -> None:
            try:
                for _ in range(50):
                    creds = cfg_reader.feishu_creds_for_home(home)
                    assert creds["app_id"]
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [
            threading.Thread(target=worker, args=(h,))
            for h in (home_a, home_b, home_a, home_b)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors


class _StubClient:
    """FeishuClient 替身——只暴露 app_id 供路由断言。"""

    def __init__(self, config) -> None:
        self.app_id = config.app_id
        self.base_url = config.base_url


class TestClientPoolRouting:
    @pytest.fixture()
    def pool(self, monkeypatch) -> ctrl_core._ClientPool:
        monkeypatch.setattr(ctrl_core, "FeishuClient", _StubClient)
        return ctrl_core._ClientPool()

    def test_routes_per_turn_home(self, pool, two_homes) -> None:
        home_a, home_b = two_homes
        cfg_reader.set_turn_home(home_a)
        client_a = pool.client_for_current_home()
        assert client_a.app_id == "cli_aaa"

        cfg_reader.set_turn_home(home_b)
        client_b = pool.client_for_current_home()
        assert client_b.app_id == "cli_bbb"

        # 池内缓存：同 home 再次解析拿到同一实例
        assert pool.client_for_current_home() is client_b
        cfg_reader.set_turn_home(home_a)
        assert pool.client_for_current_home() is client_a

    def test_thread_local_binding(self, pool, two_homes) -> None:
        home_a, _ = two_homes
        cfg_reader.set_turn_home_thread(home_a)
        assert pool.client_for_current_home().app_id == "cli_aaa"

    def test_no_creds_falls_back_to_env(self, pool, tmp_path, monkeypatch) -> None:
        home_empty = tmp_path / "no_creds"
        home_empty.mkdir()
        monkeypatch.setenv("FEISHU_APP_ID", "cli_envfb")
        monkeypatch.setenv("FEISHU_APP_SECRET", "sec_envfb")
        cfg_reader.set_turn_home(home_empty)
        assert pool.client_for_current_home().app_id == "cli_envfb"

    def test_no_creds_anywhere_raises(self, pool, tmp_path, monkeypatch) -> None:
        home_empty = tmp_path / "no_creds2"
        home_empty.mkdir()
        monkeypatch.delenv("FEISHU_APP_ID", raising=False)
        monkeypatch.delenv("FEISHU_APP_SECRET", raising=False)
        monkeypatch.delenv("LARK_APP_ID", raising=False)
        monkeypatch.delenv("LARK_APP_SECRET", raising=False)
        monkeypatch.setenv("HERMES_HOME", str(home_empty))
        cfg_reader.set_turn_home(None)
        with pytest.raises(RuntimeError, match="credentials not configured"):
            pool.client_for_current_home()


class TestRoutingProxy:
    def test_proxy_delegates_to_resolved_client(self, monkeypatch, two_homes) -> None:
        monkeypatch.setattr(ctrl_core, "FeishuClient", _StubClient)
        pool = ctrl_core._ClientPool()
        proxy = ctrl_core._RoutingFeishuClient(pool)
        home_a, home_b = two_homes

        cfg_reader.set_turn_home(home_a)
        assert proxy.app_id == "cli_aaa"
        cfg_reader.set_turn_home(home_b)
        assert proxy.app_id == "cli_bbb"

    def test_proxy_resolves_lazily_per_access(self, monkeypatch, two_homes) -> None:
        """代理不在构建时定死 client——每次访问按当前绑定重解析。"""
        monkeypatch.setattr(ctrl_core, "FeishuClient", _StubClient)
        pool = ctrl_core._ClientPool()
        proxy = ctrl_core._RoutingFeishuClient(pool)
        home_a, home_b = two_homes

        cfg_reader.set_turn_home(home_a)
        first = proxy.app_id
        cfg_reader.set_turn_home(home_b)
        second = proxy.app_id
        assert (first, second) == ("cli_aaa", "cli_bbb")


class TestEnsureInitPrewarm:
    """v1.8.7 hotfix 回归：_ensure_init 不得 await 同步的
    client_for_current_home()（生产 TypeError: object FeishuClient can't be
    used in 'await' expression，全部卡片创建失败）。"""

    def test_ensure_init_completes_and_builds_routing_client(
        self, monkeypatch, two_homes
    ) -> None:
        import asyncio

        monkeypatch.setattr(ctrl_core, "FeishuClient", _StubClient)
        ctrl = ctrl_core.StreamCardController()
        home_a, _ = two_homes
        cfg_reader.set_turn_home(home_a)

        asyncio.run(ctrl._ensure_init())

        assert ctrl._initialized is True
        assert isinstance(ctrl._client, ctrl_core._RoutingFeishuClient)
        # 预热真实构建了 client（池中有一个实例），且可直接同步解析
        assert ctrl._pool.client_for_current_home().app_id == "cli_aaa"

    def test_ensure_init_is_idempotent(self, monkeypatch, two_homes) -> None:
        import asyncio

        monkeypatch.setattr(ctrl_core, "FeishuClient", _StubClient)
        ctrl = ctrl_core.StreamCardController()
        cfg_reader.set_turn_home(two_homes[0])
        asyncio.run(ctrl._ensure_init())
        pool_first = ctrl._pool
        asyncio.run(ctrl._ensure_init())
        assert ctrl._pool is pool_first  # 二次调用不得重建池（防状态分裂）

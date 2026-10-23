"""读取 Hermes 配置. 刷新: /aowen config reload 或重启网关. 不做 mtime 检测."""

from __future__ import annotations

import contextvars
import logging
import math
import os
import threading
import time
from pathlib import Path
from typing import Any

import yaml

_logger = logging.getLogger("hermes_lark_streaming")

def _get_hermes_config_path() -> Path:
    """动态获取 Hermes 配置文件路径."""
    return Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))) / "config.yaml"


# ── v1.8.5: multiplex 多 bot 的 per-turn home 路由 ────────────────────
#
# hermes multiplex（一机一网关服务所有 profile）用 ContextVar 做 per-turn
# 的 HERMES_HOME override（hermes_constants.set_hermes_home_override，刻意
# 不动 os.environ——进程环境变量全线程共享）。因此插件无法用 os.environ
# 感知"这条消息属于哪个 profile"，必须走 hermes_constants.get_hermes_home()
# （contextvar → env → 默认）。
#
# 卡片操作分散在 event-loop 任务、flush 定时器（asyncio Handle 在调度时
# 捕获 context，继承调度点上下文）与 worker 线程（contextvar 不传播）三
# 种执行位。这里提供两层绑定：
#   - _TURN_HOME ContextVar：wrapper 在 turn 任务上下文里 set，flush 定时
#     器经调度点 context 自然继承；
#   - _TURN_LOCAL thread-local：worker 线程（hermes 线程池回调）由
#     patching/gateway.py 与 _thread_local_ctx 同步维护。
# 解析顺序：_TURN_HOME → _TURN_LOCAL → hermes 实时解析 → env。

_TURN_HOME: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "hermes_lark_streaming_turn_home", default=None
)
_TURN_LOCAL = threading.local()


def set_turn_home(home: str | Path | None) -> "contextvars.Token[str | None] | None":
    """绑定当前任务上下文的 turn home（None = 解绑，回退实时解析）。

    返回 ContextVar token，供 token 式恢复（`reset_turn_home(token)`）——
    同步包装器（如 cron _deliver_result）不能裸 set 不 reset，否则 home
    会泄漏进当前任务的后续代码。
    """
    return _TURN_HOME.set(str(home) if home else None)


def reset_turn_home(token: "contextvars.Token[str | None] | None") -> None:
    """按 token 恢复 turn home（token 为 None 时安全降级为解绑）。"""
    if token is None:
        _TURN_HOME.set(None)
        return
    _TURN_HOME.reset(token)


def set_turn_home_thread(home: str | Path | None) -> None:
    """绑定当前线程的 turn home（worker 线程专用，contextvar 不可达）。"""
    _TURN_LOCAL.data = str(home) if home else None


def current_turn_home() -> str | None:
    """当前绑定的 turn home（任务 → 线程），未绑定为 None。"""
    v = _TURN_HOME.get()
    if v:
        return v
    return getattr(_TURN_LOCAL, "data", None)


def current_hermes_home() -> Path:
    """当前生效的 Hermes home：turn 绑定 → hermes 实时解析（contextvar →
    env → 默认；旧版 hermes 无该模块时退回 env）。"""
    v = current_turn_home()
    if v:
        return Path(v)
    try:
        from hermes_constants import get_hermes_home  # type: ignore[import-not-found]

        return get_hermes_home()  # type: ignore[no-any-return]
    except ImportError:
        return Path(
            os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))
        )


def _read_env_file(path: Path) -> dict[str, str]:
    """极简 .env 解析：KEY=VALUE / # 注释 / 引号剥除。失败返回空表。"""
    out: dict[str, str] = {}
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key:
                out[key] = val
    except (OSError, UnicodeDecodeError):
        pass
    return out


_CRED_CACHE: dict[str, tuple[dict[str, str], float]] = {}
_CRED_CACHE_TTL = 60.0  # 运行时可变凭据缓存 TTL（与 _RELOAD_CACHE_TTL 对齐）
_CRED_LOCK = threading.Lock()


def feishu_creds_for_home(home: Path | None) -> dict[str, str]:
    """解析指定 profile home 的飞书凭据（v1.8.5 multiplex 多 bot）。

    解析顺序（三段）：
      1. ``<home>/config.yaml`` 的 feishu/lark 节（节内 app_id 优先）；
      2. ``<home>/.env`` 的 FEISHU_APP_ID/LARK_APP_ID 等；
      3. home 为 None（env 兜底路径）时走进程 env / 进程 home 的 config.yaml。

    返回 {"app_id", "app_secret", "base_url"?, "domain"?}；解析不到凭据
    返回 {}。带 60s TTL 缓存（keyed by home 路径串），凭据轮换后
    ``/aowen config reload`` 或 TTL 过期生效。

    安全性：secret 只在内存/缓存流转，日志侧由 feishu/client.py 的
    app_secret 掩码规则兜底，本函数绝不打日志。
    """
    cache_key = str(home) if home is not None else "__env__"
    now = time.monotonic()
    with _CRED_LOCK:
        hit = _CRED_CACHE.get(cache_key)
        if hit is not None and now - hit[1] < _CRED_CACHE_TTL:
            return dict(hit[0])

    creds: dict[str, str] = {}

    if home is not None:
        # 1) <home>/config.yaml 的 feishu/lark 节
        try:
            raw = yaml.safe_load((home / "config.yaml").read_text(encoding="utf-8")) or {}
        except (OSError, UnicodeDecodeError, yaml.YAMLError):
            raw = {}
        for key in ("feishu", "lark"):
            sec = raw.get(key)
            if isinstance(sec, dict) and sec.get("app_id"):
                creds["app_id"] = str(sec["app_id"])
                creds["app_secret"] = str(sec.get("app_secret", ""))
                if sec.get("base_url"):
                    creds["base_url"] = str(sec["base_url"])
                break
        # 2) <home>/.env
        if not creds.get("app_id"):
            envf = _read_env_file(home / ".env")
            app_id = envf.get("FEISHU_APP_ID") or envf.get("LARK_APP_ID") or ""
            if app_id:
                creds["app_id"] = app_id
                creds["app_secret"] = (
                    envf.get("FEISHU_APP_SECRET") or envf.get("LARK_APP_SECRET") or ""
                )
                base = envf.get("FEISHU_BASE_URL") or envf.get("LARK_BASE_URL") or ""
                if base:
                    creds["base_url"] = base
                domain = (envf.get("FEISHU_DOMAIN") or "").strip().lower()
                if domain:
                    creds["domain"] = domain
    else:
        # env 兜底：进程 env（原 Config 行为）→ 进程 home 的 config.yaml
        app_id = os.environ.get("FEISHU_APP_ID") or os.environ.get("LARK_APP_ID") or ""
        if app_id:
            creds["app_id"] = app_id
            creds["app_secret"] = (
                os.environ.get("FEISHU_APP_SECRET")
                or os.environ.get("LARK_APP_SECRET")
                or ""
            )
            base = os.environ.get("FEISHU_BASE_URL") or os.environ.get("LARK_BASE_URL")
            if base:
                creds["base_url"] = base
            domain = (os.environ.get("FEISHU_DOMAIN") or "").strip().lower()
            if domain:
                creds["domain"] = domain
        else:
            env_home = Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes")))
            creds = feishu_creds_for_home(env_home)

    with _CRED_LOCK:
        _CRED_CACHE[cache_key] = (dict(creds), now)
    return dict(creds)

_RELOAD_CACHE_TTL = 60.0  # 运行时可变配置缓存 TTL.

def _to_bool(val: Any, default: bool = False) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ("true", "1", "yes", "on")
    if isinstance(val, (int, float)):
        return val != 0
    return default

def _to_int(val: Any, default: int) -> int:
    if isinstance(val, bool):
        return int(val)
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        # int(float('inf')/'nan') raises OverflowError/ValueError.
        try:
            return int(val)
        except (OverflowError, ValueError):
            _logger.warning("HLS: config float value %r cannot convert to int, using default %d", val, default)
            return default
    if isinstance(val, str):
        try:
            return int(val)
        except ValueError:
            _logger.warning("HLS: config value %r is not a valid int, using default %d", val, default)
            return default
    return default

def _to_float(val: Any, default: float) -> float:
    """安全 float 转换, 拒绝 nan/inf (NaN 破坏 max/min 比较节流逻辑)."""
    if isinstance(val, bool):
        return float(val)
    if isinstance(val, (int, float)):
        result = float(val)
        if math.isnan(result) or math.isinf(result):
            _logger.warning("HLS: config float value %r is nan/inf, using default %f", val, default)
            return default
        return result
    if isinstance(val, str):
        try:
            result = float(val)
        except ValueError:
            _logger.warning("HLS: config value %r is not a valid float, using default %f", val, default)
            return default
        if math.isnan(result) or math.isinf(result):
            _logger.warning("HLS: config float value %r is nan/inf, using default %f", val, default)
            return default
        return result
    return default

class Config:
    """插件配置, 惰性读取. 单例模式: reload() 才能清掉 controller 持有实例缓存."""

    _instance: "Config | None" = None
    # v1.8.9 (P2-10): 双检锁。此前 __new__ 里 "判空 → 创建 → 设 _initialized"
    # 完全无锁：两个线程同时首次取 Config 会各造一个实例，后写者覆盖
    # _instance，先写者的 _raw/_lock 引用于是成了孤儿（表现为"配置偶发读不到、
    # /aowen config reload 不生效"）——event-loop 线程 + worker 线程都会取。
    _instance_lock = threading.Lock()

    def __new__(cls) -> "Config":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    inst = super().__new__(cls)
                    inst._initialized = False
                    cls._instance = inst
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return
        self._raw: dict[str, Any] | None = None
        self._reload_cache: dict[str, Any] | None = None
        self._reload_cache_at: float = 0.0
        # _lock: Config singleton shared across event-loop + worker threads.
        self._lock = threading.Lock()
        self._initialized = True

    def reload(self) -> None:
        """Force reload from disk. Called by /aowen config reload."""
        with self._lock:
            self._raw = None
            self._reload_cache = None
            self._reload_cache_at = 0.0
        _logger.info("HLS: config reload triggered — caches cleared")

    @property
    def enabled(self) -> bool:
        """默认 True."""
        sec = self._plugin_sec()
        return _to_bool(sec.get("enabled", True), default=True)

    @property
    def linear(self) -> bool:
        """默认 True."""
        sec = self._plugin_sec()
        return _to_bool(sec.get("linear", True), default=True)

    @property
    def panel_expanded(self) -> bool:
        sec = self._plugin_sec()
        return _to_bool(sec.get("panel_expanded", False))

    @property
    def streaming_panel_expanded(self) -> bool:
        """与 panel_expanded 独立."""
        sec = self._plugin_sec()
        return _to_bool(sec.get("streaming_panel_expanded", False))

    @property
    def max_tool_steps(self) -> int:
        sec = self._plugin_sec()
        val = _to_int(sec.get("max_tool_steps", 20), default=20)
        return max(1, min(100, val))

    @property
    def max_reasoning_rounds(self) -> int:
        sec = self._plugin_sec()
        val = _to_int(sec.get("max_reasoning_rounds", 20), default=20)
        return max(1, min(100, val))

    @property
    def print_strategy(self) -> str:
        """"fast" 或 "delay". 默认 delay."""
        sec = self._plugin_sec()
        strategy = sec.get("print_strategy", "delay")
        return strategy if strategy in ("fast", "delay") else "delay"

    @property
    def print_step(self) -> int:
        """飞书打字机每次渲染字符数. 默认 4, 范围 1~10."""
        sec = self._plugin_sec()
        val = _to_int(sec.get("print_step", 4), default=4)
        return max(1, min(10, val))

    @property
    def flush_interval_ms(self) -> float:
        """stream_element API 节流间隔 (ms). 默认 200."""
        sec = self._plugin_sec()
        ms = _to_float(sec.get("flush_interval_ms", 200), default=200.0)
        return max(70.0, min(2000.0, ms))

    @property
    def flush_interval_sec(self) -> float:
        return self.flush_interval_ms / 1000.0

    @property
    def answer_flush_interval_ms(self) -> float:
        """answer-only 流式刷新间隔 (ms). 默认 150.

        v1.7.0 (R2-09): previously hardcoded as _ANSWER_FAST_STREAM_MS=150 in
        controller/linear_mixin.py with no way to tune it apart from the
        general flush_interval_ms (which also gates panel updates)."""
        sec = self._plugin_sec()
        ms = _to_float(sec.get("answer_flush_interval_ms", 150), default=150.0)
        return max(70.0, min(2000.0, ms))

    @property
    def answer_flush_interval_sec(self) -> float:
        return self.answer_flush_interval_ms / 1000.0

    @property
    def answer_chunk_chars(self) -> int:
        """v1.8.9 (P2-8): 封卡时超长答案按 ## 标题切分的单元素目标字符数.

        默认 20000（移动端超长单元素白屏的经验阈值），范围 4000~50000。
        只影响封卡切分（controller/linear_mixin._answer_seal_actions）；
        流式期单元素展示上限是 state 侧 _MAX_ANSWER_CHARS（50000）。"""
        sec = self._plugin_sec()
        val = _to_int(sec.get("answer_chunk_chars", 20000), default=20000)
        return max(4000, min(50000, val))

    @property
    def show_reasoning(self) -> bool:
        """TTL 缓存读取 (/reasoning 命令运行时修改配置)."""
        display = self._reload_cached().get("display")
        if not isinstance(display, dict):
            return False
        platforms = display.get("platforms")
        if isinstance(platforms, dict):
            feishu = platforms.get("feishu")
            if isinstance(feishu, dict) and "show_reasoning" in feishu:
                return _to_bool(feishu["show_reasoning"])
        return _to_bool(display.get("show_reasoning", False))

    @property
    def feishu_app_id(self) -> str:
        return str(self._platform_cfg().get("app_id", ""))

    @property
    def feishu_app_secret(self) -> str:
        return str(self._platform_cfg().get("app_secret", ""))

    @property
    def feishu_base_url(self) -> str:
        return str(self._platform_cfg().get("base_url", "https://open.feishu.cn/open-apis"))

    @property
    def card_duration_sec(self) -> int:
        return _to_int(self._plugin_sec().get("card_ttl_sec", 600), default=600)

    @property
    def footer_fields(self) -> list[list[str]]:
        sec = self._plugin_sec()
        footer = sec.get("footer", {})
        if not isinstance(footer, dict):
            return self._default_footer_fields()
        fields = footer.get("fields")
        if not fields:
            return self._default_footer_fields()
        if not isinstance(fields, list):
            return self._default_footer_fields()
        if fields and isinstance(fields[0], str):
            return [fields]
        return fields

    @property
    def footer_show_label(self) -> bool:
        sec = self._plugin_sec()
        footer = sec.get("footer", {})
        return _to_bool(footer.get("show_label", False))

    @property
    def gateway_cards(self) -> bool:
        """默认 True. TTL 缓存读取."""
        sec = self._reload_cached().get("hermes_lark_streaming")
        if not isinstance(sec, dict):
            return True
        return _to_bool(sec.get("gateway_cards", True), default=True)

    @staticmethod
    def _default_footer_fields() -> list[list[str]]:
        return [["status", "elapsed", "model", "cost", "compression_exhausted"]]

    @property
    def env_app_id(self) -> str:
        return os.environ.get("FEISHU_APP_ID") or os.environ.get("LARK_APP_ID") or ""

    @property
    def env_app_secret(self) -> str:
        return os.environ.get("FEISHU_APP_SECRET") or os.environ.get("LARK_APP_SECRET") or ""

    def _plugin_sec(self) -> dict[str, Any]:
        raw = self._load()
        sec = raw.get("hermes_lark_streaming")
        if isinstance(sec, dict):
            return sec
        return {}

    def _platform_cfg(self) -> dict[str, Any]:
        """从环境变量或平台配置找飞书凭据."""
        if self.env_app_id and self.env_app_secret:
            base_url = (
                os.environ.get("FEISHU_BASE_URL")
                or os.environ.get("LARK_BASE_URL")
                or None
            )
            if not base_url:
                domain = os.environ.get("FEISHU_DOMAIN", "").lower()
                if domain == "lark":
                    base_url = "https://open.larksuite.com/open-apis"
                else:
                    base_url = "https://open.feishu.cn/open-apis"
            return {
                "app_id": self.env_app_id,
                "app_secret": self.env_app_secret,
                "base_url": base_url,
            }
        raw = self._load()
        for key in ("feishu", "lark"):
            pf = raw.get(key)
            if isinstance(pf, dict) and pf.get("app_id"):
                return pf
        return {}

    def _load(self) -> dict[str, Any]:
        with self._lock:
            if self._raw is not None:
                return self._raw
            config_path = _get_hermes_config_path()
            if config_path.exists():
                try:
                    text = config_path.read_text(encoding="utf-8")
                    self._raw = yaml.safe_load(text) or {}
                except yaml.YAMLError:
                    _logger.warning("HLS: config YAML syntax error in %s, using empty config", config_path)
                    self._raw = {}
                except (OSError, UnicodeDecodeError):
                    _logger.warning("HLS: config file read error in %s, using empty config", config_path)
                    self._raw = {}
            else:
                self._raw = {}
            raw = self._raw
            assert raw is not None  # 上面所有分支均已赋值
            return raw

    def _reload_cached(self) -> dict[str, Any]:
        """带 TTL 缓存的磁盘重读 (运行时可变配置项). 避免高频属性访问读磁盘."""
        now = time.monotonic()
        with self._lock:
            if self._reload_cache is not None and (now - self._reload_cache_at) < _RELOAD_CACHE_TTL:
                return self._reload_cache
            config_path = _get_hermes_config_path()
            if config_path.exists():
                try:
                    text = config_path.read_text(encoding="utf-8")
                    self._reload_cache = yaml.safe_load(text) or {}
                except yaml.YAMLError:
                    _logger.warning("HLS: config YAML syntax error in %s (reload), using empty config", config_path)
                    self._reload_cache = {}
                except (OSError, UnicodeDecodeError):
                    _logger.warning("HLS: config file read error in %s (reload), using empty config", config_path)
                    self._reload_cache = {}
            else:
                self._reload_cache = {}
            self._reload_cache_at = now
            cached = self._reload_cache
            assert cached is not None  # 上面所有分支均已赋值
            return cached

# hermes-lark-streaming v1.8.8 深度代码审查报告

审查对象：`/docs/myworks/hermes-lark-streaming` @ `5834095`（v1.8.8）
审查日期：2026-10-23
审查范围：`controller/{linear_mixin,core,mixin}.py`、`patching/{adapter,gateway,__init__,callbacks,hooks}.py`、`cardkit/{elements,cards,special}.py`、`feishu/{client,guard}.py`、`state/*`、`flush/controller.py`、`aowen/__init__.py`、`plugin/__init__.py`、`config/reader.py`
外部交叉验证：`/works/workspace/ai/hermes-agent`（`gateway/run_turn_runner.py`、`gateway/run_inbound.py`、`gateway/run_turn.py`、`gateway/authz_mixin.py`、`hermes_constants.py`、`locales/{zh,en}.yaml`）

> **先说结论**：这是一份工程质量很高的代码——1.8.x 系列把 v1.7.0 那批 R1/R2/R3 编号的历史 bug 逐条修掉了，注释里能看出每条修复背后的生产事故。**本轮没有找到"必炸"级别的崩溃 bug**，但有 2 处会导致"用户永久收不到回复"的静默失败路径（P0），以及一批在 multiplex/并发/长会话下会被放大的竞态（P1）。
>
> 另外两处**事实订正**（请先看，它们推翻了流传中的两个口径）：
> - 仓库里**没有 410 个 `except Exception`**，也没有 **194 个裸 `except:`**。实测：`src` 子集（controller/patching/state/feishu/cardkit/flush/config/aowen/plugin）共 **108 处 `except Exception` 类捕获，裸 `except:` = 0**；全仓（含 tests）121 处。真正危险的是其中 **12 处"只 `pass` 不记日志"** 的，已逐条点名（见 §3）。
> - 既有文档/记忆里说的 `asyncio.get_event_loop()` 在 **Python 3.14 会 RuntimeError** 不准确：3.14 的变更只在"没有运行中 loop"时停止自动创建，有运行 loop 时仍返回它。代码在 `flush/controller.py:31` 的 try/except 兜底是对的，不需要按 3.14 风险重构。

---

## P0 — 会导致用户永久拿不到回复 / 卡死

### P0-1 `feishu/client.py:430-439` `update_card()` 是唯一没接 `_retry_transient` 的写路径

```python
async def update_card(self, message_id: str, card: dict[str, Any]) -> None:
    request = (PatchMessageRequest.builder()...)
    resp = await self._client.im.v1.message.apatch(request)   # ← 裸调用
    self._check(resp, "update_card")
```

`reply_card` / `reply_text` / `reply_card_by_id` / `cardkit_create` / `cardkit_batch_update` / `cardkit_close_streaming` **全部**走了 `_retry_transient`（v1.8.3 专门补的），唯独 `update_card` 漏了。

**为什么危险**：三个关键用户路径全靠它，且都**没有二级兜底**：

| 调用点 | 失败后果 |
|---|---|
| `patching/adapter.py:985` `_make_feishu_adapter_retire_clarify` | 用户已打字回答 → 卡片按钮永远保持可点 → 用户以为没生效 → **重复提交同一个 clarify**（`_schedule_confirm_card` 此时已 pop 状态，第二次提交进 `_clarify_selections` 缺失分支，静默 `_empty_response()`） |
| `patching/adapter.py:1109` `_schedule_confirm_card` | 同上，软锁卡片不升级为确认态，重试按钮变死链 |
| `controller/mixin.py:110` `_do_gateway_card_update` | `edit_message` 拦截后卡片内容卡在旧版本，用户看到过期的网关消息 |

一次 `httpx.ReadTimeout` 就足以命中。

**怎么改**：
```python
async def update_card(self, message_id: str, card: dict[str, Any]) -> None:
    async def _do():
        request = (PatchMessageRequest.builder()
                   .message_id(message_id)
                   .request_body(PatchMessageRequestBody.builder()
                                 .content(self._dumps(card)).build())
                   .build())
        resp = await self._client.im.v1.message.apatch(request)
        self._check(resp, "update_card")
    await self._retry_transient("update_card", _do)
```
并补一条 `tests/test_client_retry.py` 用例覆盖 IM PATCH 通道的重试次数。

---

### P0-2 `flush/controller.py:99-101` + `controller/core.py:717` `mark_completed()` 与在飞 flush 的竞态，可让 `_do_linear_complete` 的 drain 完全空转

```python
# flush/controller.py:99
def mark_completed(self) -> None:
    self._completed = True          # ← 立即置位
    self._cancel_timer()
    for r in self._flush_resolvers: # ← 只唤醒已登记的 waiter
        if not r.done():
            r.set_result(None)
    self._flush_resolvers.clear()
```
```python
# controller/core.py:789 (on_aborted) / 844 (on_interrupted)
session.flush.mark_completed()
self._complete_session(session)   # → _do_linear_complete → await wait_for_flush()
```

`mark_completed()` 对**尚未开始**的 flush 立即返回，对**正在飞**的 flush 只等 `_flush_resolvers`——而 waiter 只在 `wait_for_flush()` 被调用时登记。`on_aborted` 是"先 `mark_completed`，再 `_complete_session`（异步）"，中间存在一个窗口：

1. flush task 已经 `_flush_in_progress = True`、正在 `await cardkit_batch_update`（网络往返 100~300ms）
2. `on_aborted` 执行 `mark_completed()`
3. `_do_linear_complete` → `await session.flush.wait_for_flush()`：`_flush_in_progress` 为 True → 创建 future 并 `await`
4. 在飞 flush 的 `finally` 里把 `answer_dirty` / `panel_dirty` 清掉 → 唤醒 waiter
5. drain 循环 `range(8)` 看到的 dirty 全为 False → **直接跳出**，**且末轮没有 `await asyncio.sleep`**
6. `preservative_seal` 里 `dirty data detected at seal time` 的兜底也进不去 → 卡片以"答案缺失"的状态封卡

**为什么危险**：用户点 `/stop` 的**同一瞬间**，如果某个 flush 正在网络往返，最后 100~300ms 的文字会丢。v1.7.0 R2-07 那次"省掉不必要 sleep"的优化恰好把这个窗口的兜底（无条件的 `await asyncio.sleep(0)` yield）也省掉了。

**怎么改**（二选一，推荐 A）：
- **A**：`mark_completed()` 在 `_flush_in_progress` 为 True 时不要立即唤醒——改为登记一个"pending completion future"，由 `_do_flush` 的 `finally` 统一唤醒：
  ```python
  def mark_completed(self) -> None:
      self._completed = True
      self._cancel_timer()
      if self._flush_in_progress:
          self._completion_pending = True   # finally 里统一 resolve
          return
      self._resolve_waiters()
  ```
- **B**：`_do_linear_complete` 在 drain 循环**入口**无条件 `await asyncio.sleep(0)` 一次，让在飞 flush 的 `finally` 先落地。

另需补测试：`tests/test_flush.py` 目前只覆盖"waiter 已登记"的场景（`test_waits_for_flush_to_complete`），缺"mark_completed 先于 wait_for_flush"的时序用例。

---

## P1 — 竞态 / 状态机 / 静默失败（生产可观测）

### P1-1 `controller/core.py:551-558` anchor_id 别名键造成 session **强引用自环**

```python
self._sess_put(message_id, session)
if anchor_id and anchor_id != message_id:
    session.anchor_id = anchor_id
    self._sess_put(anchor_id, session)     # ← 同一个对象两个 key
```

`CardSession` 里有 `anchor_id`，dict 里有两个 key 指向同一对象。虽然 `_cleanup` 会双向删除，但：

- `_sess_clear()`（`unregister` 路径）只清 dict，**不调 `_cleanup`** → `flush` 的 `_pending_flush_tasks`、`_flush_resolvers` 里的 future 无人 resolve
- `record_card_created()` 在 `on_message_started` 的 reuse 分支被重复计数（同一条消息 2 次 `cards_created`），影响 `/aowen monitor` 的卡片存活率读数
- `_prune_stale_sessions()` 遍历 snapshot 时同一个 session 出现两次，`seen_sessions` 只用在了 concurrency seal 循环里，prune 循环**没有去重**

**怎么改**：别名映射与主存分离——`self._alias: dict[str, str]`（alias_id → message_id），`_sess_get` 先查 alias。这样只有一个强引用，`_sess_clear` 一行就干净。

---

### P1-2 `state/linear.py:175` `has_dirty` 不含 `tool_steps_dirty`，导致"只有工具变、答案没变"的 flush 被静默丢弃

```python
@property
def has_dirty(self) -> bool:
    return (self.panel_dirty or self.answer_dirty or bool(self.bg_review_messages))
```

而 `_schedule_linear_flush`（`linear_mixin.py:243`）第一道门就是 `if state is None or not state.has_dirty: return`。

`on_tool_event()` 会同时置 `tool_steps_dirty=True` 和 `panel_dirty=True`，所以正常路径不触发；但 **Phase 3 的 300315 自愈分支**（`linear_mixin.py:585-600`）会 `session._creation_stages.discard("panel")` **只保留 dirty 标志**——若此时 `panel_dirty` 已被前一次成功批次清掉（`linear_mixin.py:565`），只剩 `tool_steps_dirty=True`，下一次 `_schedule_linear_flush` 直接 return，工具面板**永久停在旧状态直到封卡**。

**怎么改**：`has_dirty` 加上 `or self.tool_steps_dirty`；顺手把这三个 dirty 标志的"清/读"收敛成 `mark_clean()` / `has_dirty` 一对方法，避免散落在 5 处手写赋值（`linear_mixin.py:377/430/510/607/698` 附近）。

---

### P1-3 `patching/callbacks.py:76-110` `_stream_consumed_len` 是无界模块级闭包 dict

```python
_stream_consumed_len: dict[str, int] = {}
def _cleanup_consumed_len(_eid: str) -> None:
    _stream_consumed_len.pop(_eid, None)
```

清理只在 `interim_assistant_callback` 的 `already_streamed=True` 分支或 `len(text) <= consumed_len` 分支发生。**纯答案流（无 reasoning / 无 interim）的消息永远不会清**——`_answer_wrapper` 每次都 `+= len(text)`。

一个 24h 跑 500 条消息的网关，就是 500 个 `eid → int` 永久驻留，且 key 是飞书 `om_` 消息 id（~40 字节），属于慢速泄漏。更糟的是 `eid` 是闭包级变量：**每个 agent turn 都会 `_maybe_wrap_callbacks` 生成一套新闭包**，旧的 `_stream_consumed_len` 随闭包对象一起存活到 agent 被回收。

**怎么改**：在 `on_message_completed` 的 controller 侧加一个 `self._consumed_len_registry.clear(message_id)` 的收口；或把 `_stream_consumed_len` 挂到 `_msg_ctx`（随 turn 上下文一起销毁），不要放闭包里。

---

### P1-4 `patching/gateway.py:700-760` `/background` 与 `_wrap_cron_deliver` 的 `adapter.send` 猴补丁**无锁**

```python
adapter.send = _intercepting_send                       # bg 路径
adapter._hls_bg_sending = getattr(adapter, "_hls_bg_sending", 0) + 1
...
finally:
    if getattr(adapter, "send", None) is _intercepting_send:
        adapter.send = original_send                    # ← 条件还原
    adapter._hls_bg_sending = getattr(adapter, "_hls_bg_sending", 0) - 1
```

v1.7.0 R1-08 的条件还原**修了一半**：能避免 A 覆盖 B 的 wrapper，但解决不了下面两种真实交错：

1. **A 在 B 之后才开始、先结束**：A 保存 `original_send = 真orig`，B 保存 `original_send = A的wrapper`（因为 B 进入时 `adapter.send` 已是 A 的）。B 先结束时 `adapter.send is B的wrapper` → 还原成 A 的 wrapper；A 再结束时 `adapter.send is A的wrapper` → 还原成真 orig。**看起来对，但 A 结束前 B 的计数已经归零**，任何新消息走 A 的 wrapper，`ctx.get("card_sent")` 是 A 的上下文——如果 A 已 `_msg_ctx.set(None)`，新消息的卡片抑制失效。
2. `_hls_bg_sending` 的 `+1/-1` 没有锁，两线程并发 `getattr(...) + 1` 会丢计数，导致 cron 路径的 `if getattr(adapter, "_hls_cron_sending", 0)` 判断失准 → **cron 卡被当成普通 agent 回复而丢弃**（`adapter.py:133` 的 guard）。

**怎么改**：把这两个计数器换成一把模块级 `threading.Lock` 下的 `dict[int, int]`（按 `id(adapter)`），包装器改成**引用计数 + 栈式注册**：第一个进入者装 wrapper，最后一个退出者卸 wrapper，中间层一律透传到栈底。20 行以内能写完，比现在三处手写条件还原可靠得多。

---

### P1-5 `patching/adapter.py:766-912` `send_clarify` 的 `mark_awaiting_text` 用 `except (ImportError, Exception)` —— 等价于裸 except

```python
try:
    from tools.clarify_gateway import mark_awaiting_text
    mark_awaiting_text(clarify_id)
except (ImportError, Exception) as e:
    _logger.debug("clarify card: mark_awaiting_text failed (%s), card callback will handle resolution", e)
```

`except (ImportError, Exception)` 是**冗余写法**（ImportError ⊂ Exception），审查时容易误读成"只吞 ImportError"。更关键：`mark_awaiting_text` 失败意味着**用户后续打字不会被识别为该 clarify 的答案**，会走成普通新消息 → 原 clarify 一直挂到超时 30 分钟。这属于"用户以为答了、系统没收到"，但日志只有 `debug`。

**怎么改**：拆成两个 except，`ImportError` 用 debug（老版本兼容），其余用 `warning` 并带上 `clarify_id`。

---

### P1-6 `patching/adapter.py:745-763` `_prune_expired_clarify()` **只在 `send_clarify` 里被调用**

```python
def _prune_expired_clarify() -> None: ...
# 唯一调用点
async def _intercepted_send_clarify(...):
    _prune_expired_clarify()
```

如果 agent 不再发起 clarify（非常常见的静默期），已过期的 5 个 dict（`_clarify_choices/_questions/_card_msg_ids/_selections/_timestamps`）**永不回收**。虽然单条很小，但同一进程长时间不下线时是单调增长。

**怎么改**：跟 P1-3 一起收口——在 controller 的 `_prune_stale_sessions()`（本身每次 `on_message_started` 都会跑）里捎带调一次 `_prune_expired_clarify()`，零额外调度成本。

---

### P1-7 `patching/adapter.py:113-127` `EphemeralReply` 每次 send 都做一次 import

```python
try:
    from gateway.platforms.base import EphemeralReply
    if isinstance(content, EphemeralReply):
        return await passthrough()
except (ImportError, AttributeError):
    pass
```

`_dispatch_feishu_outbound` 是**每条出站消息**的必经路径。`from ... import` 命中 `sys.modules` 时成本不高（~1μs），但这是热路径上的可消除开销；更重要的是 `except (ImportError, AttributeError)` 会把 `EphemeralReply` 类本身的构造错误一并吞掉。

**怎么改**：模块加载时 `_EPHEMERAL_REPLY = None`，首次调用时解析并缓存（`nonlocal`/模块级），后续直接 `isinstance(content, _EPHEMERAL_REPLY)`。同文件 `_schedule_confirm_card` 里已删过类似的冗余局部 import（v1.3.2），保持风格一致。

---

### P1-8 `controller/core.py:498-522` 并发 seal 抛异常后**照常创建新卡**，旧卡留在非终态

```python
try:
    self.on_interrupted(old_message_id=..., new_message_id=message_id, ...)
except Exception:
    _logger.warning("HLS: concurrency seal failed", exc_info=True)
# ← 不检查 seal 是否成功，继续往下建新 session
```

`on_interrupted` 内部有 3 条 `_complete_session` 异步路径，任一条抛异常（`AssertionError` from `assert self._client`、`RuntimeError` from closed loop）都会让 old_session 停在 `STREAMING`。之后：

- `_prune_stale_sessions()` 只清**终态**session → old_session 永远滞留
- 下一次同 scope 的 `on_message_started` 会**再次** seal 它（`is_terminal_phase` 为 False），日志刷屏
- 用户的旧卡永远转圈

**怎么改**：catch 之后显式兜底
```python
except Exception:
    _logger.warning(...)
    if not existing_session.is_terminal_phase:
        existing_session.enter_terminal(TerminalReason.ERROR, "concurrency_seal_failed")
        existing_session.flush.mark_completed()
```
`_reactivate_session_for_continuation` 的 R1-04 修复（`core.py:377-390`）已经是这个模式，照抄即可。

---

### P1-9 `controller/core.py:1229-1250` `_send_text_fallback` 硬截 4000 字符，且**没有分段**

```python
if len(text) > 4000:
    text = text[:4000] + "..."
```

飞书单条文本消息上限是 ~150KB（远超 4000），4000 是保守值；但**截断而不分段**意味着：卡片创建 §P0 类失败时，用户的最终保底回复直接被砍掉 90%。`_MAX_ANSWER_CHARS = 50_000`（`state/linear.py`）说明作者预期答案能到 50K。

**怎么改**：按 `\n` 优先切分，每段 3000 字，循环 `reply_text`；失败中途停止并记 warning。同时把 `4000` 提成 `_FALLBACK_CHUNK_CHARS` 常量（顺便消掉魔数）。

---

### P1-10 `feishu/client.py:630-660` `upload_image` 无大小上限、无超时传递、**全仓零调用**

```python
data = await loop.run_in_executor(None, self._download_image, image_url)
...
file = io.BytesIO(data)
```

- `_download_image` 会 `bytes(resp.read())` **全量读入内存**，`timeout=15` 只管连接，不管总字节数
- `run_in_executor(None, ...)` 用默认线程池（`min(32, cpu+4)`），慢图片会**挤占**其他 `to_thread` 调用
- `grep` 全仓（排除 tests）：`upload_image` / `upload_local_image` **没有任何调用方**，是死代码，但会把 `lark_oapi` 的 `im.v1.image` 面一直挂在 `_ensure_lark_names` 里

**怎么改**：要么删掉（连同 `_download_image`、`io`/`urllib` import），要么补 `max_bytes=10MB` 的 `resp.read(max_bytes+1)` 检查 + 改用独立 `ThreadPoolExecutor(max_workers=2)`。

---

### P1-11 `plugin/__init__.py:139-186` `_ensure_streaming_config()` **非原子重写用户的 config.yaml**

```python
raw = yaml.safe_load(text) or {}
...
if changed:
    prepped = _prepare_config(raw)
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(prepped, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
```

三个问题：
1. **非原子**：`open(w)` 先截断再写。如果此时 hermes 正在读（`/aowen config reload`、`hermes` CLI 并发），读到空文件 → 用户配置全丢
2. **注释全丢**：`yaml.safe_load` + `yaml.dump` 会抹掉用户 config 里所有注释和顺序之外的格式（`sort_keys=False` 保序但保不了注释）
3. **无并发保护**：`_backup_config()` 的 glob 检查与写入之间无锁

**怎么改**：`tempfile.NamedTemporaryFile(dir=config_path.parent)` 写完 `os.replace()`；注释丢失无解，但至少可以先做"是否已存在 `hermes_lark_streaming` 节"的**只读检查**，不需要改动时**绝不落盘**（现在 `changed=False` 时确实不写，这点是对的，保持）。

---

### P1-12 `feishu/client.py:27-86` `_ensure_lark_names()` 的**幂等标志在失败后仍可能半初始化**

```python
def _ensure_lark_names() -> None:
    global lark, BatchUpdateCardRequest, ...
    if _lark_names_loaded:
        return
    import lark_oapi as lark
    from lark_oapi.api.cardkit.v1 import (...)
    from lark_oapi.api.im.v1 import (...)
```

第 2、3 行的 `import` 一旦抛 `ImportError`（lark-oapi 版本太老，缺 `api.cardkit`——1.4.0 就没有 async 方法，注释里也提到了），**前面的 `global lark` 已经被赋值**，但 `_lark_names_loaded` 仍是 False。下一次调用会走完整流程再抛一次，行为还算对；真正的风险是**多线程同时首次构造 FeishuClient**（multiplex 下 3 个 profile 并发首条消息）——两个线程都通过 `if _lark_names_loaded` 检查，一个在赋值中途、另一个开始读全局名 → `NameError`。

**怎么改**：加模块级 `threading.Lock`，`double-checked locking`：
```python
_LARK_IMPORT_LOCK = threading.Lock()
def _ensure_lark_names():
    if _lark_names_loaded: return
    with _LARK_IMPORT_LOCK:
        if _lark_names_loaded: return
        ...  # 全部 import 成功后再置 _lark_names_loaded = True
```

---

## P2 — 性能 / 可维护性 / 架构风险

### P2-1 `controller/linear_mixin.py:302 _do_unified_flush` = **427 行单函数**，`_preservative_seal` = **510 行**，`_do_linear_complete` = **310 行**

实测（AST）：`_preservative_seal` 510 行、`_do_unified_flush` 427 行、`_do_linear_complete` 310 行。同文件 `_do_unified_flush` 里 Phase 2/Phase 3 的 `build_unified_panel(...)` 参数块**逐字重复了 4 次**（行 318、456、487、812），`except FeishuAPIError` 的 300309/300315/300313 三分支逻辑重复 5 次。

**建议拆分**（按现有注释里的 Phase 自然边界）：
```
_do_unified_flush
├── _flush_phase2_create()      # add_elements + delete hint（~120 行）
├── _flush_phase3_update()      # panel partial_update（~150 行）
├── _flush_stream_answer()      # cardkit_stream_element + 错误分类（~60 行）
└── _handle_flush_api_error()   # 300309/300315/300313 单点分派（~80 行）
```
`_build_panel_payload(session, state, expanded, fragment_cache)` 一个 helper 就能吃掉那 4 份重复参数块。

### P2-2 `patching/adapter.py:1144 _handle_clarify_card_action` = **367 行**，四段几乎相同的"resolve + schedule confirm"

`select` / `input_submit` / `button_submit` / `retry_submit` 四个分支的结构**完全同构**：取文本 → 存 `_clarify_selections` → `safe_schedule_threadsafe(_do_resolve → _schedule_confirm_card)` → 同步兜底 → `return _submitted_card_response(...)`。大约 240 行是复制粘贴。

**建议**：抽成
```python
def _resolve_and_confirm(self, adapter, clarify_id, text, choices, question):
    ...
```
四个分支各剩 ~10 行取参与校验。

### P2-3 `patching/gateway.py:298 _wrap_run_agent` 内嵌 `wrapper` = **369 行**，且 child/parent 两条 completion 路径 ~80 行重复

`ctx["_agent_ref"]` 的 6 个 `getattr` 取值（cache_read/write/reasoning_tokens/estimated_cost_usd/cost_status）在 `_saved_parent_ctx` 分支、`elif ctx` 分支、`_wrap_run_background_task` 分支**各写了一遍**。
抽 `_agent_metrics(ctx) -> dict` 一处，可省 ~60 行。

### P2-4 `cardkit/cards.py:34-135` 与 `cardkit/elements.py:962-1010` **两份 collapse hint 实现不一致**

| | `cards.py::_enforce_card_element_limit` | `elements.py`(seal 侧) |
|---|---|---|
| hint 判定 | `"已折叠" in content` | `_is_collapse_hint_child()`：`startswith("⚡") and len<80 and (已折叠 or collapse)` |
| 数字解析 | `old_hint.find("项")` 向前扫数字 | `sum(int(n) for n in re.findall(r"\d+", old_hint))` |
| i18n | 无 | 有 `i18n_content` |

`cards.py` 那份**不认识英文 hint**，也**不会累加 "X 轮推理、Y 步操作" 的双数字**——一旦 `_enforce_card_element_limit` 在中文环境之外被触发，折叠计数会归零重新计。`elements.py` 的 v1.7.0 R4 注释里明说这个坑修过了，但 `cards.py` 没跟上。

**怎么改**：`cards.py` 直接 `from .elements import _is_collapse_hint_child`，数字解析也共用。两份实现合成一份是本次审查里**性价比最高**的一处收编。

### P2-5 `patching/gateway.py:227` / `:258` / `:404` 三处 `_ctrl._sess_get()` **直捅私方法**

`get_controller()` 返回的 `StreamCardController` 有 `_sess_get` / `_sess_values_snapshot` / `_client_ok` 等 `_` 前缀方法被 patching 层跨模块直接调用（共 14 处）。

这是 **hook 契约的隐式部分**：只要 controller 重构（比如 P1-1 建议的 alias 分离），patching 层就跟着碎。建议在 `StreamCardController` 上开一组显式只读 API：
```python
def lookup(self, message_id) -> SessionView | None      # 不可变视图
def active_sessions(self) -> tuple[SessionView, ...]
def is_ready(self) -> bool
```
patching 层只依赖这 3 个方法，controller 内部随便改。

### P2-6 `controller/core.py:1130-1160` `_cleanup()` 调 `flush.mark_completed()`，但 `_sess_clear()` **不调**

`unregister()`（`plugin/__init__.py:355`）走 `ctrl._sess_clear()`，直接 `self._sessions.clear()`。此时所有 session 的 `flush._pending_timer` 还在事件循环里排队，`flush.flush_now` 的协程还持有 `session` 引用 → **unregister 之后定时器仍会触发对已注销 controller 的 API 调用**（`_client` 还在，因为 `_ClientPool` 没清）。热卸载/热重载插件时会看到"幽灵卡片更新"。

**怎么改**：`_sess_clear()` 遍历时对每个 session 调 `flush.mark_completed()`，并置 `self._initialized = False`、`self._pool._clients.clear()`。

### P2-7 `feishu/client.py:118-125` 错误码常量混用了三种语义

```python
CARDKIT_ELEMENT_LIMIT = 11310           # 子错误码
CARDKIT_ELEMENT_LIMIT_DIRECT = 300305   # 顶层码
CARDKIT_SCHEMA_ERROR = 300315           # 顶层码 AND "element not found"
CARDKIT_ELEMENT_NOT_FOUND = 300313      # 顶层码 AND ...
CARDKIT_ELEMENT_NOT_FOUND_ALT = 300314
```

300315 同时是"Schema 非法属性"和"insert_before 找不到元素"，靠 `_RE_ELEMENT_NOT_FOUND` 正则区分（`is_schema_error` / `is_element_not_found_error`）。这本身是合理的规避，但 `_check()` 到各调用点之间**至少有 6 处**重复写了 `if e.code == CARDKIT_STREAMING_CLOSED: ... elif is_schema_error(e): ... elif is_element_not_found_error(e): ...`（`linear_mixin.py` 5 处 + `cards.py` 0 处）。

**怎么改**：定义 `class CardAPIErrorKind(Enum)`，把 `_classify(e) -> Kind` 一处实现，各调用点 `match _classify(e):`。

### P2-8 `state/linear.py:66` `_MAX_ANSWER_CHARS = 50_000` 会与飞书卡片 200 元素上限**互相踩**

`_count_tag_objects` 上限 200（`cards.py:35`），answer 是单个 markdown 元素，所以长度不受元素数限制——但 50K 字的中文 markdown 在飞书客户端渲染会**反序列化超时**（业界实测中文 >20K 字符的卡片在移动端会白屏）。

50K 是"服务端能存"的值，不等于"客户端能画"。建议降到 20K，或者把答案按 `##` 标题切成多个 markdown 元素（顺带能命中元素上限的 trim 逻辑）。

### P2-9 `cardkit/elements.py:169 _FRAGMENT_CACHE_MAX = 512` FIFO 淘汰，但 **key 永不复用**

`("r", uid)` / `("s", uid)` 的 uid 来自进程级 `itertools.count(1)`，永不重复。FIFO 淘汰（`cache.pop(next(iter(cache)))`）在 dict 上是 O(1)，但 **512 个条目对一条长会话不够**：`_MAX_REASONING_ROUNDS_STORED = 50` + `max_tool_steps = 128` ≈ 178 个活跃条目，加上被 trim 的历史条目，长会话会**反复命中/淘汰**，cache 退化为纯开销（每次 `render()` 后都写、下一轮可能已被淘汰）。

**怎么改**：改为 LRU（`collections.OrderedDict` + `move_to_end`），或把 `_FRAGMENT_CACHE_MAX` 提到 `max_tool_steps + max_reasoning_rounds + 64` 并随 session 生命周期销毁（现在它挂在 `UnifiedLinearState` 上，随 `_release_session_data` 一起清，这点是对的）。

### P2-10 `config/reader.py:257-265` `Config()` 是单例，但 **`__init__` 里的 `_initialized` 检查非线程安全**

```python
def __new__(cls):
    if cls._instance is None:
        cls._instance = super().__new__(cls)
        cls._instance._initialized = False
    return cls._instance

def __init__(self):
    if getattr(self, "_initialized", False):
        return
```

与 `get_controller()`（`core.py:1360` 用双检锁）不同，`Config` 的 `__new__` 无锁。两个线程同时首次 `Config()` → 两个对象，`_instance` 后写覆盖前写，**已捕获旧实例的 controller 与新建的 Config 各自持有不同 `_raw` 缓存**。虽然后果不严重（都读同一文件），但 `/aowen config reload` 只清一个实例的缓存，另一个继续用旧配置。

**怎么改**：照抄 `get_controller()` 的双检锁模式，或直接在 `__new__` 上加模块级 `_CONFIG_LOCK`。

### P2-11 `aowen/__init__.py:933-971` `_send_card_async` 依赖 `asyncio.get_running_loop()`，在 hook 上下文里**无兜底**

`pre_gateway_dispatch` hook 由 hermes 在事件循环里调（已验证 `run_inbound.py:1242` 是 `async def`），所以现在能拿到 loop。但这与 `patching/gateway.py` 里到处 `try: loop = self._get_loop()` 的谨慎风格不一致——一旦 hermes 改成在 executor 里调 hook，`/aowen help` 会静默只打一条 warning。

**建议**：注入方式跟 controller 保持一致，用 `self._get_loop()` 的等价实现（`get_running_loop` → 缓存 loop → `run_coroutine_threadsafe`）。

### P2-12 `patching/hooks.py:18 _safe_hook` 的 `getattr(_logger, log_level)` **字符串拼 logger 方法**

```python
def _safe_hook(default_return=None, log_level: str = "warning"):
    ...
    getattr(_logger, log_level)("%s error: %s", func.__name__, exc, exc_info=True)
```

`log_level` 是自由字符串，写错（比如 `"warn"`——`logging.Logger` 没有 `warn` 之外的别名，`"WARN"` 也没有）→ `AttributeError` **在 except 块里再抛**，变成异常掩盖异常，hook 直接炸穿到 hermes。

**怎么改**：`log_level: int = logging.WARNING`，调用点改常量。

### P2-13 `patching/__init__.py:400-470` 延迟补丁线程 **60s 轮询 2s**，且失败后**不重试不告警上传**

```python
deadline = time.monotonic() + 60.0
while time.monotonic() < deadline:
    time.sleep(2.0)
    if _apply_deferred_once():
        return
_logger.error("gateway.run NOT FOUND after 60s — ...")
```

- `time.sleep(2.0)` 在 daemon 线程里是**真正的忙等**（30 次唤醒），对 CPU 无影响但对日志节奏有影响
- 60s 超时后**永久放弃**。hermes 冷启动在低配机器（用户实测 J4125）可能 >60s → 插件静默降级为"纯文本"，用户只看到一条 error 日志
- 修复建议：改成 `threading.Event().wait(2.0)` + 超时 180s，并暴露 `/aowen status` 里的 `patch_status

### P2-14 `controller/linear_mixin.py:243` 每次 flush 都调 `session.flush.set_throttle(...)`，**两个常量写死**

```python
_answer_only = (state.answer_dirty and not state.panel_dirty and not state.tool_steps_dirty)
if _answer_only:
    session.flush.set_throttle(_answer_fast_stream_sec(self._cfg))   # 150ms
else:
    session.flush.set_throttle(self._cfg.flush_interval_sec)         # 200ms
```

`_answer_fast_stream_sec` 里 `try: return cfg.answer_flush_interval_sec except: return 0.150`——这个 `except Exception` 会把 **`Config` 内部任何异常**（YAML 解析失败、属性拼写错误）静默降级为 150ms，用户改配置不生效时完全无感。

**怎么改**：`Config` 侧确保 `answer_flush_interval_sec` 属性永远存在（有默认值），这里去掉 try/except。

### P2-15 `state/tooluse.py:301-345` `record_end` 的 fallback 匹配用 `reversed(steps)` **线性扫**

```python
if call_id:
    for step in reversed(self._session.steps):
        if step.call_id == call_id and step.status == "running":
for step in reversed(self._session.steps):
    if step.name == name and step.status == "running":
```

`_max_steps = 128`，每次 `record_end` 最多扫 256 次比较。在 128 步的长会话里，所有 `record_end` 都是 O(n)，整体 O(n²)。实际影响 ~微秒级，可接受；但如果未来把 `max_steps` 提到 1000（比如给长任务），需要加 `_running_by_call_id: dict[str, ToolStep]`。

**这是 P3 级**，列出来是因为它和 P2-9 的缓存同属"随会话长度退化"的族。

### P2-16 `patching/adapter.py:38-100` `_classify_gateway_message` 用**关键字子串**分类，误分类面大

```python
if any(kw in content for kw in ("❌", "⚠️", "error", "failed", "Error", "Failed")): return "error"
if any(kw in content for kw in ("Session", "session", "🔄", "♻", "compress", "compres")): return "session"
```

任何包含 "error" 的正常消息（比如 agent 回答里说 "this error happens when..."）都会被渲染成红框错误卡。v1.8.0 P3-4 修过 lifecycle 误报（13/13 误分类），但**"正文含 error 字样"**这条路没堵。

**怎么改**：把 error 判定收紧为 `content.startswith(("❌", "⚠️"))` 或前缀白名单（hermes 的 gateway-internal 消息都有固定开头），不要做全文子串匹配。

### P2-17 `patching/gateway.py:620-640` `_wrap_run_conversation` **是同步函数包装异步调用**

```python
def wrapper(self, user_message, ...):     # ← 注意：不是 async def
    _maybe_wrap_callbacks(self)
    ...
    return orig(self, user_message, **call_kwargs)
```

`orig` = `agent.conversation_loop.run_conversation`，签名是同步的（返回 dict），所以这里是**对的**。但这与 `_apply_direct_agent_patch` 里包装 `AIAgent.run_conversation`（同样同步）以及 `_wrap_run_agent`（async）三处形成"同名字段的三种异步性"。任何 hermes 侧把 `run_conversation` 改成 `async def` 的改动，会**静默**让返回值从 dict 变成 coroutine，`result.get("final_response")` 全部 KeyError。

**建议**：在 wrapper 里加防御：
```python
result = orig(self, user_message, **call_kwargs)
if inspect.isawaitable(result):
    _logger.error("HLS: run_conversation returned awaitable — hermes API changed, plugin disengaged")
```
（这条也适用于 P2-1 的架构建议——见 §5）

---

## §3 「静默失败」精确定位（12 处 `pass`-only，逐条定性）

实测 `src` 子集 108 处 `except Exception` 类捕获，**裸 `except:` 0 处**；其中紧接 `pass` 的 12 处，逐条定性：

| # | 位置 | 后果 | 定级 |
|---|---|---|---|
| 1 | `patching/adapter.py:593` `_intercepted_edit` 的 `except Exception: pass` | **卡片更新失败 → 静默 fallback 到 `orig_edit`**，用户看到纯文本替代卡片，无日志 | **P1**（应 `_logger.warning`，因为这是"本该是卡片但退化成文本"的可观测事件） |
| 2 | `patching/adapter.py:671` `_intercepted_add_reaction` | reaction 不上卡 → 用户按了 emoji 无反应，无日志 | P2 |
| 3 | `patching/adapter.py:718` `_intercepted_delete_reaction` | 同上 | P2 |
| 4 | `patching/callbacks.py:218` `_stable_args_preview` 的 `build_tool_preview` 失败 | 退回 JSON dump（有兜底），可接受 | OK |
| 5 | `patching/callbacks.py:227` `redact_tool_args_for_display` 失败 | **退回原始 args 整包 dump**——脱敏器不可用时把未脱敏参数塞进卡片！注释也承认这点 | **P1**（应退回 `str(type)` 或空串，不能退回原始值） |
| 6 | `patching/callbacks.py:245` `_stable_detect_failure` 失败 | 退回启发式，可接受 | OK |
| 7 | `patching/gateway.py:940` `finally` 里的 `adapter._hls_bg_sending - 1`（无 pass，但异常会被 `finally` 掩盖） | 计数器漂移，见 P1-4 | P1 |
| 8 | `patching/__init__.py:192` `_hls_mark_wrapped` 的 `setattr` 失败 | 内置类无法打标记 → 每次 repatch 都重包 | P2 |
| 9 | `patching/__init__.py:708` `FeishuAdapter._hls_patched = True` 失败 | 只靠 id 镜像，GC 后可重复 patch | P2 |
| 10 | `patching/__init__.py:756` 同上（RelayAdapter） | 同上 | P2 |
| 11 | `feishu/client.py:302` `_check` 里 `response.get_log_id()` 失败 | log_id 缺失，仅影响排查 | OK |
| 12 | `controller/core.py:1237` `_send_text_fallback`（v1.8.3 已改 warning） | 已修，保留 | OK |

**结论**：真正需要立刻改的是 **#1、#5**。其余是"已知可接受"或"已修"。

---

## §5 插件架构：monkey-patch 稳定性风险（按"hermes 升级易碎度"排序）

### 5.1 已正确做的部分（先说好的）

- **跨副本哨兵**（`_hls_wrapped` / `_hls_patched` / `_hls_direct_patched`）+ **类级 patch 锁**（`GatewayRunner._hls_patch_lock`）——这是 multiplex 场景下**唯一正确**的解法，v1.8.4~v1.8.7 两轮迭代踩出来了
- **`platform_registry.create_adapter` 单点 hook**（`patching/__init__.py:800`）——注释里"v0.17.0→v0.19.0 之间该文件零 commit"的论证是对的，这是全仓**最稳的 hook 点**
- **`sys_modules_only` 分阶段解析**（v1.8.8）——解决了 headless CLI 的 10s 加载超时，思路正确
- **lark_oapi 惰性导入**（v1.8.8）——`_ensure_lark_names` 的"注册链零 lark 依赖"是对的

### 5.2 高风险脆弱点

| # | 脆点 | 为什么易碎 | 建议 |
|---|---|---|---|
| A | `patching/gateway.py:298 _wrap_run_agent` 硬编码 **11 个位置参数名** + `**kwargs` | hermes `_run_agent_inner` 当前有 `persist_user_message` 等 14 个 kwarg，插件靠 `**kwargs` 透传。**一旦新增必填关键字参数**，`orig(...)` 会 `TypeError` | 改成 `async def wrapper(self, *args, **kwargs)` + `inspect.signature(orig).bind_partial()` 校验；或至少对 `TypeError` 做一次"去掉自己注入的 kwarg 重试"的降级 |
| B | `patching/gateway.py:93 _wrap_handle_message_with_agent(self, event, source, *args)` | 只锁了前 2 个位置参数，第 3 个 `_quick_key`、第 4 个 `run_generation` 走 `*args`——`_hls_cleanup_ctx` 里用 `mid` 作 key，若 future hermes 让 `event.message_id` 为 None 则 `_started_msg_ids.add(None)` 污染全局集合 | 显式声明 `_quick_key=None, run_generation=None` 并断言非 None |
| C | `patching/callbacks.py:202-300` 五种 callback 的**签名硬编码** | `tool_progress_callback(event_type, tool_name, preview, *args)`、`tool_start_callback(call_id, name, *args)`、`interim_assistant_callback(text, *args, **kwargs)`——hermes 任一处改参数顺序，插件**不会报错**，只会把参数错配（比如把 `preview` 当 `tool_name`），表现为卡片里工具名变成一段命令 | 每个 wrapper 加 `inspect.signature` 一次校验，启动时打一条 `HLS: callback signature drift` |
| D | `patching/adapter.py:100` `_classify_gateway_message` 依赖 hermes 的 locale 文案 | 已踩过一次（v1.8.0 P3-4）。`locales/zh.yaml:846-847` 的 `notice_restart/notice_shutdown` 一旦改字，lifecycle 又落回 error 桶 | 改成按 **`metadata` 字段**判定（hermes 的 gateway-internal 消息带 `category`，`_is_cron_delivery_metadata` 已是这个思路），彻底摆脱文案依赖 |
| E | `patching/adapter.py:556` `_wrap_feishu_adapter_edit(self, chat_id, message_id, content, metadata=None, **kwargs)` | 直接吞掉 `metadata` 不传给 `orig_edit`（`_fallback_kwargs` 里 pop 掉）——hermes 若用 metadata 做去重/追踪，会丢 | 只在 `TypeError` 时 pop 重试，正常路径传 `metadata=metadata` |
| F | `patching/adapter.py:1250` 四处 `from agent.async_utils import safe_schedule_threadsafe` | hermes 内部模块路径，非公开 API | 加**模块级**探测（启动时一次），失败则降级到 `run_coroutine_threadsafe` |

### 5.3 hook 契约的隐性依赖（建议写进 README）

插件实际依赖了 **7 个未在 `plugin.yaml` 的 `provides_hooks` 里声明的 hermes 内部契约**：

1. `GatewayRunner._reply_anchor_for_event(event)` 存在且返回 `om_` id
2. `source.thread_id` 是话题隔离键（`patching/gateway.py:104`）
3. `AIAgent.run_conversation` 是**同步**函数且返回 dict
4. `agent.session_cache_read_tokens` / `session_reasoning_tokens` / `session_estimated_cost_usd` 等 5 个属性
5. `gateway.platforms.base.EphemeralReply` 类
6. `tools.clarify_gateway.{resolve_gateway_clarify, mark_awaiting_text}`（内部函数，非 API）
7. `adapter._is_interactive_operator_authorized(open_id)`（**已 fail-closed 处理，这点是对的**）

建议在 `hermes_adapter.py` 里建一张"契约探测表"，启动时逐条 `getattr` 并写进 `/aowen doctor` 输出——**升级前先跑 doctor 就知道会不会碎**。

---

## 附：优先级修复顺序建议

```
第 1 批（本周，2 处，改动 < 30 行）
  P0-1 update_card 接 _retry_transient
  P0-2 mark_completed 与在飞 flush 的时序

第 2 批（P1 里的"静默失败"类，4 处，改动 < 100 行）
  P1-5 mark_awaiting_text 日志级别
  §3-#1 edit 静默 fallback
  §3-#5 未脱敏参数回落
  P1-2 has_dirty 补 tool_steps_dirty

第 3 批（状态/资源，5 处，改动 ~200 行）
  P1-1 anchor alias 分离
  P1-3 + P1-6 泄漏收口（一起做）
  P1-4 adapter.send 引用计数
  P1-8 seal 失败兜底终态
  P2-6 unregister 清 flush

第 4 批（架构，1~2 周）
  P2-4 两份 collapse hint 合并（最高性价比）
  P2-5 controller 开只读 API
  5.2-A/C hook 签名漂移探测
  5.3 契约探测表进 doctor

可延后
  文件拆分（P2-1/2/3）、P2-13 补丁线程、P2-16 分类收紧
```

---

*本报告未修改仓库任何文件。所有行号基于 `5834095`。*

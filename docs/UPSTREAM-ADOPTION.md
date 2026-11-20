# hermes-lark-streaming 上游借鉴功能清单

> 基准：本地 `github_sync` @ `1dfbdbfa`（v1.10.3）vs 上游 `Aowen-Nowor/hermes-lark-streaming` @ `c826ee85`（v2.0.36，2026-11-20 终态）。
> 分叉点：`7c174a59`（2026-09-08，v1.8.2）。两侧架构已分道（V1 AST patching vs V2 native streaming），
> **不 merge、不 cherry-pick**，本文档列出可"抄代码/抄思路"移植的功能及完整细节。
> 上游快照：`/Users/dddpeter/myworks/hermes-lark-streaming-upstream-v2.0.36.bundle`（2027-01-01 上游转私有前已全量备份，86 refs 含全部 85 tag）。
>
> 上游源码查看命令（无需 checkout）：
> ```bash
> cd /Users/dddpeter/myworks/hermes-lark-streaming
> git show upstream/github_sync:<文件路径>
> ```

---

## P0 — 200850 流式会话超时自愈（最高优先级，本地完全缺失）

### 背景（上游生产实证）

- 飞书 CardKit 官方**流式会话寿命约 10 分钟（600s 整必现）**：
  - 实证一：11-07 12:01 建卡 → 12:11:08 撞 `200850 card streaming timeout`
  - 实证二：11-07 15:08 建卡 → 15:18:35 同族错误
- 到期后 `stream_element` 必返回 `200850`。
- **本地现状**：全仓 grep `200850` 零命中。深度思考 agent 回合超 10 分钟时整回合裸奔，无任何恢复。
- 本地 v1.10.0 watchdog（300s 静默拆卡）只覆盖"静默"场景；**持续输出的长回合（一直在流式写答案）不会触发拆卡，照样撞 600s 墙**。

### 官方语义（上游已验证，直接采信）

- 200850 官方恢复动作：同一卡片实体（14 天有效期内）通过 `settings` 再次置 `streaming_mode=true` 重开流式。
- sequence 规则（真飞书实验 s98 实证，卡片 `om_x100b63f0d183a880b1f89b86413d0d1`）：
  - 重放/回退 seq → 必现 `300317 sequence compare failed`
  - **seq+1 → 立即自愈**
  - **跳号（seq 大幅领先服务端）→ 允许**
- 300317 同样适用 seq+1 对齐自愈（上游生产事故 gateway19.log 23:09:50：网络重试用同一 sequence 重发，首次请求实际已达服务端，重发必撞 300317 整回合降级）。

### 上游三层防线

**第一层：主动续期（预防）** — `composer/engine.py`

```python
_STREAM_RENEW_AGE_SEC = 540.0   # 卡在 600s 墙前 60s 续期

# CardTurnState 增加字段
stream_session_at: float = 0.0  # 建卡时置 time.monotonic()

async def _renew_stream_if_due(self, st: CardTurnState) -> None:
    if not st.card_id or st.stream_session_at <= 0:
        return
    if time.monotonic() - st.stream_session_at < _STREAM_RENEW_AGE_SEC:
        return
    reopen = self._reopen_closure(st)
    if reopen is None:
        return
    try:
        st.seq += 1
        await reopen(st.seq)                    # settings streaming_mode=true
        st.stream_session_at = time.monotonic()
        _logger.info("composer: streaming session renewed card=%s age>=%.0fs",
                     str(st.card_id)[:12], _STREAM_RENEW_AGE_SEC)
    except Exception:  # 预防性续期失败不阻断本帧；事后自愈仍闭环
        st.stream_session_at = time.monotonic()  # 防每帧重复尝试（退避语义）
        _logger.info("composer: streaming renew skipped card=%s",
                     str(st.card_id)[:12], exc_info=True)
```

**第二层：事后自愈（`cardkit/seq.py`，约 110 行，零外部依赖可直接移植）**

```python
SEQ_CONFLICT = 300317
STREAM_TIMEOUT = 200850
_MAX_SEQ_RECOVERIES = 3   # 网络重试最多让服务端领先 1 次；3 是防御余量
                          # （200850 自愈一次消耗 2 个 seq，同在上限内闭环）

def is_seq_conflict(exc) -> bool:
    return getattr(exc, "code", None) == SEQ_CONFLICT   # duck-type，兼容任何客户端异常

def is_stream_timeout(exc) -> bool:
    return getattr(exc, "code", None) == STREAM_TIMEOUT

async def call_with_seq_recovery(st, send, *, what, reopen=None):
    """执行 send(st.seq)；300317 → seq+1 重发；200850 且有 reopen →
    seq+1 重开 → seq+1 重发。非自愈范围异常/超上限原样上抛。"""
    for attempt in range(_MAX_SEQ_RECOVERIES):
        try:
            await send(st.seq)
            return
        except Exception as exc:
            if attempt == _MAX_SEQ_RECOVERIES - 1:
                raise
            if reopen is not None and is_stream_timeout(exc):
                st.seq += 1
                await reopen(st.seq)
                st.stream_session_at = time.monotonic()   # 重开成功刷新续期基线
                st.seq += 1
                continue
            if not is_seq_conflict(exc):
                raise
            st.seq += 1
```

关键设计：所有走该 helper 的调用内容均为**全量语义**（answer 缓冲/状态快照/页脚插入），重发幂等，seq+1 重发不会产生重复内容。

**第三层：降级收口**（可选借鉴）— 降级（`_degrade`）后立即复用 finalize 优雅封卡（answer 终值 + 页脚 + 关流式），跑马灯永挂根治。本地 v1.10.x 的封卡兜底链已部分覆盖，优先级低。

### 本地移植方案（V1 架构接线点已确认）

1. **新文件 `cardkit/seq.py`**：上游原样复制（`git show upstream/github_sync:cardkit/seq.py > cardkit/seq.py`），仅改 logger 名。
2. **`feishu/client.py` 增方法 `cardkit_reopen_streaming`**（SDK 类已 import 好，骨架现成——本地 client.py 第 31/49 行已有 `SettingsCardRequest/SettingsCardRequestBody`）：

```python
async def cardkit_reopen_streaming(self, card_id: str, *, sequence: int = 0) -> None:
    """重开流式会话（200850 官方恢复动作：settings streaming_mode=true）."""
    settings = {"config": {"streaming_mode": True}}

    async def _do():
        body_builder = SettingsCardRequestBody.builder().settings(self._dumps(settings))
        body_builder = body_builder.sequence(sequence)
        request = (SettingsCardRequest.builder()
                   .card_id(card_id).request_body(body_builder.build()).build())
        resp = await self._client.cardkit.v1.card.asettings(request)
        self._check(resp, "cardkit_reopen_streaming")

    await self._retry_transient("cardkit_reopen_streaming", _do)
```

3. **`controller/linear_mixin.py` 接线**（`_do_unified_flush` 内，两处 `cardkit_stream_element` 调用：第 ~665 行和 ~840 行）：
   - `CardSession` 增 `stream_session_at` 字段，建卡时置 `time.monotonic()`
   - flush 入口调用 `_renew_stream_if_due` 同款逻辑（第一层）
   - `stream_element` 调用包进 `call_with_seq_recovery`，`reopen` 闭包注入 `cardkit_reopen_streaming`（第二层）
   - 现有 `CARDKIT_STREAMING_CLOSED` 早退逻辑保留在自愈循环外层（300309 与 200850 是不同错误码，互不干扰）
4. **常量**：`feishu/client.py` 增 `CARDKIT_STREAMING_TIMEOUT = 200850`（与既有 `CARDKIT_SEQUENCE_CONFLICT = 300317` 并列）。

### 测试要点

- mock client：首次 `stream_element` 抛 `.code=200850` → 断言调了 `settings(streaming_mode=True)` 且 seq 两次 +1 后重发成功
- mock `.code=300317` → 断言 seq+1 重发成功
- 连续 3 次冲突 → 断言上抛（降级红线兜底，消息不丢）
- 540s 边界：`stream_session_at` 距今 539s 不续期 / 541s 续期

---

## P1 — 官方图标映射表 `cardkit/tool_icons.py`（直接复制，零风险）

### 背景

- **本地 bug**：`cardkit/elements.py:629` 默认工具图标 `tool_02` **不在飞书官方 enumerations-for-icons 清单内**（上游 v2.0.10 用真飞书验证发现）。正确默认 token：`code_outlined`。
- 上游 `tool_icons.py`（180 行）是纯映射表 + 永不抛异常的解析函数，**零外部依赖**（仅一个内部相对 import `split_notice_lead` 依赖 `progline.py` 的两个正则，可一并摘或内联）。

### 内容明细

**通报行 emoji → 官方图标**（2026-10 官方清单实测存档，全部 `_outlined` 线性风格）：

```python
DEFAULT_TOOL_ICON = "code_outlined"    # tool_02 非官方枚举，弃用
DEFAULT_NOTICE_ICON = "info_outlined"

_NOTICE_ICONS = {
    "⏱": "time_outlined",       # 限流/耗时（含 U+FE0F 变体）
    "⏲": "time_outlined", "⏰": "time_outlined", "⏳": "time_outlined",
    "⚠": "warning_outlined",    # provider 错误/警告
    "🚨": "warning_outlined",
    "ℹ": "info_outlined", "💡": "info_outlined",
    "💾": "storage_outlined",    # Self-improvement review 等后台通知
    "🔄": "refresh_outlined", "♻": "refresh_outlined",
}
# 实现细节：查表前剥 U+FE0F 变体选择符
```

**工具名 → 官方图标**（V1 state/tooluse.py 移植 + hermes 生产名补充）：

```python
_TOOL_ICONS = (
    (("skill",), "app-default_outlined"),
    (("read", "open"), "file-link-text_outlined"),
    (("write", "edit"), "edit_outlined"),
    (("web_search", "web-search", "search"), "search_outlined"),
    (("tool_search",), "search_outlined"),   # hermes 工具发现桥工具
    (("web_fetch", "web-fetch", "fetch"), "language_outlined"),
    (("grep",), "doc-search_outlined"),
    (("glob",), "folder_outlined"),
    (("exec", "bash", "command", "run", "terminal"), "code_outlined"),
    (("browser", "playwright", "navigate"), "browser-mac_outlined"),
    (("agent", "task", "spawn"), "robot_outlined"),
    (("check", "determine", "verify"), "list-check_outlined"),
    (("summarize", "analyze", "prepare"), "report_outlined"),
)
```

**工具名 → 展示标题**（`_TOOL_TITLES`，同构：read→"Read"、grep→"Search text"、web_fetch→"Fetch web page"、agent→"Run sub-agent"…）+ `humanize_tool_name`（`-`/`_`→空格 + 首字母大写，`terminal`→"Terminal"）。

**匹配规则**（`resolve_tool_icon` / `tool_display_title` 同款）：normalize 为小写 `-`→`_` 后，`normalized == alias or normalized.startswith(f"{alias}_")`。

### 本地移植方案

1. `git show upstream/github_sync:cardkit/tool_icons.py > cardkit/tool_icons.py`
2. `cardkit/elements.py:629` 的 `"token": step.get("icon", "tool_02")` 改为经 `resolve_tool_icon(step.get("tool"))` 解析（或最小改动：仅把默认值 `tool_02` → `code_outlined`）
3. 可选：面板工具行标题接 `tool_display_title`，通报行接 `split_notice_lead + resolve_notice_icon`
4. 上游经验教训（v2.0.16 用户验收）：terminal 用 `setting_outlined` 齿轮语义不符，统一 `code_outlined`——映射表已按此定稿

### 测试要点

- `resolve_tool_icon("Terminal")` / `("web_search_v2")`（前缀匹配）/ `("unknown_tool")`（回退默认）
- `resolve_notice_icon("⏱️")`（带 U+FE0F）
- 全部函数喂 `None`/空串不抛异常

---

## P2 — 99991400 限频错误分道退避（几行改动）

### 背景

- 本地 `feishu/client.py`：`99991400`（per-API 限频，HTTP 400）与其他瞬态码共用 `_TRANSIENT_RETRY_DELAYS = (0.1, 0.3, 0.6)`。
- 上游 v2.0.32 审计结论：**0.1s 后重试限频等于继续撞墙**——三连快重试耗尽后整回合降级。
- 流式更新有官方 10 QPS/卡豁免，能撞到限频的是 create/batch 突发；秒级退避足够等过窗口。

### 上游实现（`feishu/client.py`）

```python
_RATE_LIMIT_RETRY_DELAYS = (1.0, 3.0, 6.0)   # v2.0.32：限频码分道，秒级退避
_RATE_LIMIT_CODE = 99991400

# _retry_transient 内，重试延迟选择处：
delays = (_RATE_LIMIT_RETRY_DELAYS if e.code == _RATE_LIMIT_CODE
          else _TRANSIENT_RETRY_DELAYS)
delay = delays[attempt]
```

### 本地移植方案

在本地 `_retry_transient`（`feishu/client.py:264`）同位置加同款分道逻辑，加一个测试：mock 抛 99991400 断言 sleep 参数为 1.0/3.0/6.0。

---

## P3 — 私有 hermes import 收敛（`compat/hermes_api.py` 模式，小改动）

### 背景

- hermes 无公开插件 seam，插件直接 import 其私有模块。本地散落至少 12 处（`patching/gateway.py`、`patching/adapter.py`、`patching/hermes_adapter.py`、`__main__.py`），hermes 升级时改锚点要全仓找。
- 上游模式：`compat/hermes_api.py`（41 行）单模块集中全部私有 import，暴露稳定函数签名：

```python
def resolve_gateway_model():      from gateway.run import _resolve_gateway_model
def build_session_key(source):    from gateway.session import build_session_key
def env_cred_class():             from gateway.platform_env import EnvCred
def safe_schedule_threadsafe(...): from agent.async_utils import safe_schedule_threadsafe
```

### 本地移植方案

新建 `compat/hermes_api.py`，把散落的 `SendResult`/`EphemeralReply`/`Platform`/`safe_schedule_threadsafe`/`run_conversation`/`GatewayRunner` 六类 import 收敛进去。函数内延迟 import 保持原语义（找不到时在调用点抛，不影响启动路径）。收益：v1.9.0 式人工适配（hermes v0.22.7 reaction 契约修正）下次只改一个文件。

---

## P4 — `scripts/compat_probe.py` Hermes 新版兼容自动探测（需改造）

### 背景

上游 hermes-release-watch 工作流检测到 hermes-agent 新版本后**自动**做兼容性探测并推送结论（不是通知人工测试）。五步探测，每步产出可读明细，任何一步失败立即定性，退出码 0/1，`--json` 落盘：

| 步骤 | 内容 | 失败含义 |
|---|---|---|
| S1 | PYTHONPATH 前置新版 hermes 源码，import 内置飞书适配器 | 依赖/结构级失配（明细含 ImportError 链） |
| S2 | import 插件 adapter，验证 `_builtin` 绑定真基类 | 继承面断裂，插件 fail-fast 拒启 |
| S3 | `_rebuild_class_with_real_base` + 实例化触发 `_probe_override_surface`（send/edit_message/send_clarify 三方法缺失即 RuntimeError） | 插件第一红线 |
| S4 | 官方表面清单逐项核对（`SURFACE_CHECKLIST`：send、edit_message、send_clarify、delete_message、handle_message、`_handle_message_with_guards`、set_message_handler、`_CARD_ACTION_ROUTES`…） | 该 hermes 版本下部分能力失灵 |
| S5 | 插件 pytest 全套在 hermes 源码于 sys.path 的真实环境下跑 | 新版场景回归 |

**上游踩坑教训（照抄）**：S4 清单曾误把插件自身 FeishuClient 的方法（cardkit_create 等 6 项）列为"基类必须有"，导致 0.23.2 起每次探针误报 S4 失败。清单只放**基类表面**。

### 本地移植方案（改造点）

V1 是 AST patching 架构，S2/S3 不适用，改造为：
- S2'：新版 hermes 源码喂给本地 patch 锚点探测器（本地已有"签名漂移探测"，v1.8.9 引入）——检查 `patching/` 各锚点函数在新版源码 AST 中是否仍可定位、签名是否漂移
- S4'：清单改为本地实际依赖的 hermes 私有表面（`gateway.platforms.base.SendResult/EphemeralReply`、`gateway.config.Platform`、`agent.conversation_loop.run_conversation`、`agent.async_utils.safe_schedule_threadsafe`、`gateway.run.GatewayRunner`）
- S1/S5 基本原样（S5 = PYTHONPATH 前置新版源码跑本地 pytest 全套）

---

## P5 — `plugin/clarify_form.py` 多选/开放题表单卡（需改造，功能增强）

### 背景

本地 clarify 卡是**单选** `select_static` 下拉 + 自定义输入。上游 v2.0.36（2026-11-19，终版前最后功能）用 CardKit 2.0 **form 容器**升级为三种交互：
- 多选题 → `multi_select_static`（提交数组）
- 开放题 → `input`（multiline_text，`auto_resize` 3→6 行）
- 提交/取消 → form 内 `button`（`form_action_type: submit/reset`），callback 携带 `action.form_value`

### 关键实现细节（`plugin/clarify_form.py`，189 行）

```python
# 多选组件（仅 form 容器内合法）
{"tag": "multi_select_static", "element_id": "clarify_multi", "name": "clarify_multi",
 "placeholder": "请选择（可多选）", "width": "fill", "required": True,
 "behaviors": [{"type": "callback",
                "value": {"hermes_clarify_action": "form_component",
                          "clarify_id": clarify_id}}],
 "options": [{"text": choice, "value": str(index)}   # value = 选项下标
              for index, choice in enumerate(choices)]}

# 开放题组件
{"tag": "input", "name": "clarify_text", "input_type": "multiline_text",
 "rows": 3, "max_rows": 6, "auto_resize": True, "width": "fill", "required": True,
 "label": {"tag": "plain_text", "content": question}}

# 提交按钮：form_action_type="submit"，callback value 携带 form_kind("multi"/"text")
{"tag": "button", "type": "primary", "form_action_type": "submit",
 "behaviors": [{"type": "callback",
                "value": {"hermes_clarify_action": "form_submit",
                          "clarify_id": clarify_id, "form_kind": form_kind}}]}
# 取消按钮：form_action_type="reset"（无 callback）
```

**两个易踩的坑（上游注释里的硬知识）**：
1. **200830-safe**：飞书拒绝用 JSON 1.0 卡片更新 JSON 2.0 卡片——提交/过期后的落定卡必须保持 `schema: "2.0"` 结构（`build_settled_form_card`）。
2. **多选答案回填格式**：`extract_form_answer` 把选项下标还原为 labels 后 `json.dumps(labels)`——这是 Hermes multi-select 的线上格式（JSON 数组字符串），不是逗号拼接。

**SDK 兼容提取**：`extract_form_value` 同时支持 SDK 对象和裸 dict（`getattr` + `isinstance` 双路），跨 SDK 版本安全。

### 本地移植方案

- 卡片构建部分（`build_clarify_form_card`/`build_settled_form_card`/`extract_form_value`/`extract_form_answer`）可基本原样放进 `cardkit/special.py` 或新文件——纯 dict 构建，无 V2 依赖
- 改造点：回调路由——上游靠 `hermes_clarify_action` behavior value 分发到 V2 adapter；本地需接入 V1 的 clarify 回调处理链（`cardkit/special.py` 三态卡的既有路由），并把 `retire_clarify_card`（v1.8.8 本地已有）适配到 form 卡落定态
- 前提：确认飞书应用已配置卡片回调（上游 v2.0.23 记录过"该应用尚未配置卡片回调"的坑）

---

## 明确不借鉴清单（及理由）

| 上游能力 | 不借理由 |
|---|---|
| 长文分块三级切点（段落>标题行前>换行 + 围栏/表格双检查） | 本地 v1.8.9 已有**按 `##` 标题切块**方案（`_split_answer_chunks`，切点天然落在渲染安全位、围栏感知），各有取舍，不折腾 |
| 300317 重试 | 本地 preservative seal 已有 2 次重试 + v1.10.3 兜底链（但如果按 P0 接入 `call_with_seq_recovery`，顺手统一） |
| watchdog 拆卡 | 本地 v1.10.0 seal watchdog 三规则是自己的方案（300s 静默拆卡不截断），语义不同 |
| i18n 中英双语表 | 本地已有 `cardkit/i18n.py` |
| native streaming / per-chat 锁 / cron oneshot 卡 / reasoning 面板行 | V2 composer 体系核心，与 V1 patching 架构不兼容，摘出来等于重写 |
| CI/Gitee 镜像/Release 流水线/飞书通知卡 | 只服务上游自己的发布流程 |
| `lark-streaming-platform`（后继项目） | 全新独立平台（LSEP/1 协议），非本仓库演进方向；上游 V2 知识已冻结迁移至其 `docs/design/04-knowledge-freeze.md`，值得**阅读**（CardKit 怪癖表、竞态事故档案），不值得代码移植 |

---

## 建议落地顺序

| 序 | 项 | 工作量 | 风险 | 收益 |
|---|---|---|---|---|
| 1 | P1 图标表 ✅ 已落地 v1.10.4（最小改动方案：不复制 tool_icons.py，直接修本地 tooluse.py 描述符表 + elements.py 默认值） | 半小时（复制+改默认值） | 零 | 修 bug：非官方图标 token |
| 2 | P2 限频分道 ✅ 已落地 v1.10.4 | 10 分钟 | 零 | 减少整回合降级 |
| 3 | P0 200850 自愈 ✅ 已落地 v1.11.0（seq.py 状态字段名按本地 CardSession.sequence 适配） | 半天（seq.py + client 方法 + flush 接线 + 测试） | 低（自愈失败走上抛=现状行为，不会更糟） | 根治长回合 10 分钟必炸 |
| 4 | P3 import 收敛 ✅ 已落地 v1.11.1（compat/hermes_api.py，13 访问器；tools.clarify_gateway 留在 clarify 域） | 1 小时（纯重构） | 低 | 下次 hermes 升级改一处 |
| 5 | P4 兼容探测 ✅ 已落地 v1.11.1（scripts/compat_probe.py，S1/S2'/S4'/S5；含 P3↔P4 清单一致性守卫测试） | 半天（S2/S4 改造） | 低（只读脚本） | hermes 新版自动出结论 |
| 6 | P5 clarify 表单卡 ✅ 已落地 v1.12.0（cardkit/clarify_form.py + adapter 接线；真机验证需飞书应用卡片回调已配置） | 1-2 天（含回调接线+真机验证） | 中（需飞书卡片回调配置） | 多选/开放题交互升级 |

> 顺序 1+2 可以一次提交；3 独立成一个版本（建议 v1.11.0，本地版本线 v1.10.3 → v1.11.0）；4-6 按需。
>
> **上游已停更且 2027-01-01 转私有**——本文档所有上游引用均可在 bundle 中找到：`git clone /Users/dddpeter/myworks/hermes-lark-streaming-upstream-v2.0.36.bundle <目录>`。

# Nacos Toolkit 新功能路线图（2026-10-17）

> 状态：**v0.3 已实现（2026-10-17）**；v0.3.1 及以后为规划。
> 基线版本：v0.3.0（11 个只读 MCP 工具 + v1/v3 双协议栈 + 最小工作台）。
> v0.3 的 F1–F4 已落地（单元测试 78 个、协议冒烟 19 项、真实 Nacos v1/v3 E2E 各 55 项全部通过）；实现细节见 [`v0.3-implementation-2026-10-17.md`](v0.3-implementation-2026-10-17.md)。后续版本仍按本文逐项拆任务。

---

## 0. 摘要

建议把插件从 v0.2 的「7 个只读查询工具 + 状态页」升级为「AI 与人都能用的 Nacos 配置/服务上下文层」，分四步（v0.3 已完成）：

| 版本 | 主题 | 核心内容 |
| --- | --- | --- |
| v0.3 | 只读智能 | 配置正文搜索、环境漂移总览、历史版本对比、连接体检 |
| v0.3.1 | 小步增强 | 环境速览、配置订阅者、TLS/管理员初始化/命名空间名解析 |
| v0.4 | 工作台 2.0 | 配置浏览器、漂移矩阵、服务健康看板、AI 副驾、命令/右键入口 |
| v0.5 | 受控写 | 写模式 + 白名单 + 预览 + CAS + 两阶段确认 + 审计；提升/回滚/导出迁移 |
| v0.6 | 实时与生态 | 变更监听、GitOps 基线对比、Nacos 3.3+ MCP 注册中心、隧道增强、Beta/实例运维 |

v0.3 不碰 UI，全部复用现有 `httptest` 单测、协议冒烟与 Docker E2E 体系；默认只读姿态不变。

---

## 1. 现状盘点

### 1.1 已交付能力

- **MCP 工具（11 个，全部 `readOnlyHint: true`）**：`get_config`、`search_configs`、`diff_config`、`get_config_history`、`list_namespaces`、`list_services`、`list_instances`，以及 v0.3 新增的 `find_in_configs`、`diff_envs`、`diff_history`、`diagnose`。
- **协议栈**：连接时自动探测 v3（Nacos 3.x console readiness）→ 失败回退 v1（1.x/2.x readiness / server state）；支持 `api_version` 手动覆盖。
- **鉴权**：v1 用 `accessToken` 查询参数并自动续期；v3 用 `Authorization: Bearer`；console 接口即使服务端关闭鉴权也要求账号。
- **工作台**：连接状态 + 工具清单（`ui/index.html`）。
- **质量基线**：`backend/main_test.go`（v1/v3 mock、错误路径、安全边界）、`backend/diff_test.go`、`scripts/smoke.py`、`scripts/e2e_docker.py`（2.4.3 与 3.2.4 双链路）。

### 1.2 代码与数据流

```text
AI 客户端 / DBX AI 面板
  │ MCP tools/list, tools/call（connectionId 由宿主注入）
  ▼
DBX 宿主 MCP 桥
  │ mcp/tools, mcp/call（lifecycle.connection 携带连接快照）
  ▼
backend/main.go       生命周期、方法分发
  ├─ mcp.go           工具目录、参数归一化、会话缓存
  ├─ nacos.go         地址解析、协议探测、鉴权、HTTP 传输、读取能力分发
  ├─ v1.go            Nacos 1.x/2.x Open API
  ├─ v3.go            Nacos 3.x client + console API
  └─ diff.go          行级 LCS diff
  ▼
Nacos Server
```

关键入口与现状：

| 文件 | 现状 | 机会 |
| --- | --- | --- |
| `backend/main.go` | `connection/test|connect|disconnect`、`nacos/ping`、`mcp/tools`、`mcp/call` | `runtime.host/port` 未使用；无写模式/动作 |
| `backend/nacos.go` | 客户端、探测、鉴权、2 MiB 上限、分页归一化 | 新能力分发、TLS、命名空间解析 |
| `backend/v1.go` | config / history / namespaces / services / instances | 正文扫描、listener、export/import/clone、beta |
| `backend/v3.go` | config / history / namespaces / services / instances | `searchDetail`、listener、export/import/clone、beta、server state、AI/MCP |
| `backend/mcp.go` | 11 工具、30 s 预算、6 MiB 结果上限、会话缓存 | 写工具隐藏、风险提示 |
| `backend/insights.go` | v0.3：正文搜索、环境漂移、历史对比、连接体检 | 后续能力复用同一层 |
| `backend/diff.go` | 行级 LCS，3000 行上限 | 复用为历史对比/GitOps 基线 |
| `ui/index.html` | 状态 + 工具清单 | 配置浏览器、看板、AI 副驾 |
| `manifest.json` | connection-provider / workbench / mcp | 新字段、command/menus/context-menu、可选权限 |

### 1.3 现有约束

- 只读：无发布、删除、实例变更。
- 响应上限：上游 2 MiB、工具结果 6 MiB、单次调用 30 s（宿主 120 s）。
- 零第三方依赖：Go 标准库 + DBX SDK；diff 为自研。
- 仅桌面端：DBX Web/Docker 后端不提供插件工具（宿主限制）。
- 凭据由宿主注入 sidecar，不落盘；错误信息不回显上游正文与 token。

### 1.4 Nacos 侧可用的新能力

已从 Nacos 2.4.3 源码、3.x 控制台 API 文档与现有调查记录核对：

| 能力 | v1（1.x/2.x） | v3（3.x） | 备注 |
| --- | --- | --- | --- |
| 配置正文搜索 | ❌ 无原生接口 | ✅ `GET /v3/console/cs/config/searchDetail` | v1 只能列表后抓正文扫描 |
| 配置元数据过滤 | `appName` / `config_tags` / `types` | `appName` / `configTags` / `type` | 现有 `search_configs` 未开放 |
| 配置历史 | `GET /v1/cs/history` | `GET /v3/console/cs/history/list`、`/history` | 已有列表 + 单版本 |
| 配置订阅者 | `GET /v1/cs/listener` | `GET /v3/console/cs/config/listener` | 采样/聚合语义需 spike |
| 导出/导入 | `GET/POST export=true` / `import=true` | `GET /v3/console/cs/config/export2` / `POST /import` | ZIP 二进制 |
| 命名空间克隆 | `clone=true`（按配置 ID 复制到目标命名空间） | `POST /v3/console/cs/config/clone`（显式源/目标） | 统一方案建议读+发布 |
| Beta/灰度 | `beta=true` + `betaIps` 头 | `GET/DELETE /v3/console/cs/config/beta` + `betaIps` | 需写权限 |
| 服务端状态 | `GET /v1/console/server/state` | `GET /v3/console/server/state` | v3 含 auth/quota 字段 |
| 集群节点 | 需 spike | `GET /v3/console/core/cluster/nodes` | v3 需管理员 |
| 命名空间 CRUD | 有 | `GET/POST/PUT/DELETE /v3/console/core/namespace` | 暂不开放给 AI |
| 实例运维 | 注册/更新/删除 Open API | `PUT/DELETE /v3/console/ns/instance` | 权重、上下线、持久化实例 |
| AI/MCP 注册中心 | ❌ | `/v3/console/ai/mcp`（3.3+） | 列表端点需 spike |
| 并发保护 | `casMd5` 请求头 | `casMd5` 请求头 | 发布时防覆盖 |

### 1.5 DBX 宿主侧可用的新能力

来自 `docs/reference/plugin-development-full.txt` 与 `manifest.schema.json`：

- **权限**：`host.workbench`、`host.storage`、`host.ai`、`host.events`、`host.binary`、`host.filesystem`。
- **工作台 API**：`host.storage`、`host.ai.openConversation`、`ai.setRecommendations`、`fileTransfer`（pick/beginSave/read/write）、`clipboard`、`floating`。
- **贡献点**：`command`（打开工作台）、`menus`（commandPalette / appToolbar / appSidebar）、`context-menu`（connection/table，可声明 `open-workbench` action）、`workbench.ai.recommendations`。
- **MCP 行为**：非 `readOnlyHint` 工具默认由宿主弹出确认，展示完整参数；用户拒绝/5 分钟超时即失败。插件仍须自己做防护。
- **连接运行时**：生命周期请求可带 `runtime.host/port`（经 SSH/代理转换后的端点）；多端点协议可声明 `proxy_route` 获取 SOCKS5 路由。

### 1.6 关键缺口

1. `mcp/tools` 不读取 `connectionId`，无法按连接隐藏写工具。
2. `connection/test|connect|mcp/call` 忽略 `runtime.host/port`，内网 Nacos 无法经 DBX 隧道访问。
3. 无 TLS 自定义 CA / 自签名处理，企业内网 HTTPS 常卡住。
4. 命名空间只认 ID，不认显示名（`namespaceShowName`），AI 容易填错。
5. 无写模式、无发布预览、无审计、无白名单。
6. 工作台只有状态页，无法浏览配置/服务，也未接入 DBX AI 面板。

---

## 2. 产品目标与原则

**目标**：让 AI 在回答 Nacos 相关问题时先拿到正确的环境上下文，再按需下钻；让人工在 DBX 内完成浏览、对比与受控发布。

**原则**：

1. **只读优先**：默认不引入任何写能力；写必须显式开启并二次确认。
2. **双栈一致**：新工具同时支持 v1/v3；无法对等时明确降级并在输出中标注。
3. **结果可解释**：所有分页/扫描都有 `totalCount`、`truncated` 与下钻提示，不静默截断。
4. **最小暴露**：只返回请求所需字段；错误不带上游正文/凭据；不默认抓全量正文。
5. **零依赖优先**：新增能力优先用标准库与现有实现；确需依赖时单独评审。
6. **宿主能力可降级**：`host.ai/storage/fileTransfer/events` 都是可选能力，旧宿主隐藏入口而不是报错。

---

## 3. 功能候选池（16 项）

| ID | 功能 | 一句话价值 | Nacos/DBX 依据 | 工作量 | 风险 | 建议 |
| --- | --- | --- | --- | --- | --- | --- |
| F1 | 配置正文搜索 `find_in_configs` | 回答「哪个配置里配了 X」，不用先知道 dataId | v3 `searchDetail`；v1 列表+限量抓取 | M | 低 | v0.3 |
| F2 | 环境漂移总览 `diff_envs` | 一次看 dev/test/prod 哪些配置缺失/多出/不一致 | 现有 `searchConfigs` 的 md5 | M | 低 | v0.3 |
| F3 | 历史版本对比 `diff_history` | 当前 vs 历史、历史 vs 历史 | `readConfig` + `getConfigRevision` + `lineDiffs` | S | 低 | v0.3 |
| F4 | 连接体检 `diagnose` | 协议/版本/鉴权/console/admin/权限一次说清 | v1/v3 `server/state` + 健康/登录/探测 | M | 低 | v0.3 |
| F5 | 环境速览 `get_context` | 一次调用给 AI 环境摘要与异常项 | 聚合 F1/F2/F4 + 现有 list | M | 低 | v0.3.1 |
| F6 | 配置订阅者 `list_listeners` | 发布前看谁在监听，评估影响面 | v1 `/v1/cs/listener`；v3 `config/listener` | S/M | 中 | v0.3.1 |
| F7 | 工作台 2.0 | 配置浏览器/漂移矩阵/服务看板/AI 副驾/命令入口 | `host.ai/storage/fileTransfer` + 新 RPC | L | 低 | v0.4 |
| F8 | 受控发布 | 预览、发布、CAS、两阶段确认、写白名单、审计 | `POST config` + `casMd5` + 宿主确认 | L | 高 | v0.5 |
| F9 | 提升/回滚 | 跨命名空间 promote、按 historyId 回滚 | 现有读接口 + 受控发布 | M | 高 | v0.5 |
| F10 | 连接增强 | 自定义 CA、3.x 管理员初始化、命名空间名解析 | `tls.Config`、`/v3/auth/user/admin`、`listNamespaces` | M | 中 | v0.3.1 |
| F11 | 隧道支持 | 通过 DBX SSH/SOCKS5 连接内网 Nacos | `runtime.host/port` + `proxy_route` | L | 中 | v0.5/0.6 |
| F12 | GitOps 基线对比 | 本地 Git 目录 vs Nacos 配置漂移 | `fileTransfer` + `diff` | M | 中 | v0.6 |
| F13 | 实时变更流 | 配置变更实时推送到工作台 | Nacos long-polling + `host.events` | L | 中 | v0.6 |
| F14 | Nacos AI/MCP 注册中心 | 浏览 3.3+ 托管的 MCP Server 与工具 | `/v3/console/ai/mcp`（需 spike） | M | 中 | 实验 |
| F15 | 导出/导入/迁移 | 命名空间备份、恢复、跨环境迁移 | `export2` / `import` / `clone` | M | 中 | v0.5+ |
| F16 | Beta/灰度与实例运维 | betaIps 灰度、实例权重/上下线 | `beta` + `PUT /v3/console/ns/instance` | M | 高 | v0.6 |

---

## 4. 推荐版本节奏

### v0.3 Config Intelligence（只读）— 已实现

- [x] F1 `find_in_configs`：v3 `searchDetail` + v1 限量扫描，返回命中行片段与 `truncated` 信息。
- [x] F2 `diff_envs`：按命名空间对比全部配置的 md5，报告 missing/extra/changed；可选内容 diff。
- [x] F3 `diff_history`：历史版本 vs 当前版本，或两个历史版本，复用 LCS diff。
- [x] F4 `diagnose`：协议/健康/服务端状态/鉴权/admin 初始化/命名空间/数据面逐项检查。
- [x] 工具数 7 → 11；不碰 UI，不改 manifest 结构；新增工具全部 `readOnlyHint: true`。
- [x] Docker E2E 在真实 2.4.3 / 3.2.4 实例上通过（v1 55/55、v3 55/55，包含无鉴权 + 鉴权实例）。
- 发布说明重点：AI 可以直接回答「哪里配了 X」「dev 和 prod 差哪些配置」「这个配置上个月改了什么」「为什么连不上 Nacos 3.x」。

### v0.3.1 小步增强

- F10（TLS / 管理员初始化 / 命名空间显示名）。
- F5、F6 视工具预算取舍；若工具数超过 12，优先把 F5 做成工作台/`diagnose` 的聚合输出，而不是新增工具。

### v0.4 Workbench 2.0

- F7：配置浏览器 + 漂移矩阵 + 服务健康看板 + AI 副驾 + 命令/右键入口。
- F5 作为工作台首页的「环境速览」。
- 依赖：`host.storage`、`host.ai`、`host.fileTransfer`、`host.workbench`；全部按 `capabilities.*` 探测。

### v0.5 Controlled Writes

- F8 + F9 + F15；写模式默认关闭。
- 先做预览与审计，再开放发布；删除配置与删除命名空间不进入 AI 工具面。

### v0.6 Live & Ecosystem

- F11 + F12 + F13 + F16；F14 先做技术验证。
- 实时能力以工作台为主，不新增长阻塞的 MCP 工具。

---

## 5. 重点功能卡

### F1 `find_in_configs`（配置正文搜索）

**问题**：`search_configs` 只匹配 dataId/group；AI 要回答「哪里配置了 `spring.datasource.url`」只能逐个 `get_config`，既慢又容易漏。

**参数**：

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `connectionId` | string | 是 | 宿主注入 |
| `keyword` | string | 是 | 字面量匹配，不做正则以避免 ReDoS |
| `dataId` | string | 否 | 先按 dataId 收窄 |
| `group` | string | 否 | 先按 group 收窄 |
| `namespace` | string | 否 | 缺省用连接命名空间 |
| `maxResults` | integer | 否 | 1–50，默认 20 |
| `scanLimit` | integer | 否 | v1 专用，1–1000，默认 200 |
| `caseSensitive` | boolean | 否 | 默认 false |

**v3 数据流**：

1. `GET /v3/console/cs/config/searchDetail?search=blur&configDetail=<keyword>&dataId=*&groupName=*&namespaceId=...&pageNo=1&pageSize=...`
2. 对返回的匹配元数据取前 `maxResults` 条，逐条 `getConfig`（并发上限 8）取正文。
3. 按行匹配，生成 `snippets`（每配置最多 3 条、每条最多 200 字符）。

**v1 回退**：

1. `GET /v1/cs/configs?search=blur` 分页遍历，按 `scanLimit` 截断候选。
2. 并发抓正文（上限 8），逐行匹配。
3. 输出 `scannedConfigs` / `candidateCount` / `truncated` / `truncatedReason`，让模型知道结果是否完整。

**输出**：

```json
{
  "connection": "Nacos",
  "namespace": "dev",
  "keyword": "spring.datasource.url",
  "matches": [
    {
      "dataId": "order-service.yaml",
      "group": "DEFAULT_GROUP",
      "namespace": "dev",
      "type": "yaml",
      "md5": "…",
      "matchCount": 2,
      "snippets": [{"line": 18, "text": "    url: jdbc:mysql://…"}]
    }
  ],
  "scannedConfigs": 200,
  "candidateCount": 1420,
  "truncated": true,
  "truncatedReason": "scanLimit reached; narrow dataId/group or raise scanLimit"
}
```

**边界/失败模式**：

- v3 console 不可达：返回可操作错误（检查 console 端口/账号），不退化成静默空结果。
- 无匹配：`matches: []` + `hint`，不是 `isError`。
- 超大配置：单条正文超过 2 MiB 由现有传输层拒绝；扫描阶段跳过并在 `truncatedReason` 说明。
- 权限：401/403 翻译为「账号无权限或 token 失效」，不输出上游正文。
- 超时：总预算 30 s；到点返回已完成部分 + `truncatedReason: "time budget"`。

**测试**：

- v3 mock：命中、无命中、console 403、分页。
- v1 mock：多页扫描、`scanLimit` 截断、大小写、超时。
- 输出上限：单条 snippet 截断、`maxResults` 钳制。
- 安全：错误信息不含 token/上游正文。

**工作量/风险**：M / 低。收益最高，建议第一个实现。

---

### F2 `diff_envs`（跨命名空间漂移总览）

**问题**：`diff_config` 需要先知道 dataId；用户真正常问的是「dev 和 prod 有哪些配置不一致」，希望先看总览再下钻。

**参数**：

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `connectionId` | string | 是 | 宿主注入 |
| `namespaces` | string[] | 是 | 2–8 个，首个为基准 |
| `dataId` | string | 否 | 收窄配置范围 |
| `group` | string | 否 | 收窄分组 |
| `includeContentDiff` | boolean | 否 | 默认 false；true 时对前 N 条 changed 生成行级 diff |
| `maxConfigsPerNamespace` | integer | 否 | 默认 1000，硬上限 2000 |
| `maxDifferences` | integer | 否 | 默认 100，硬上限 500 |

**算法**：

1. 每个命名空间分页拉 `searchConfigs`（pageSize 500），建立 `group@@dataId → {md5,type}`。
2. 以首个命名空间为基准，对比其余命名空间：
   - `onlyInBaseline`：基准有、目标没有。
   - `onlyInTarget`：目标有、基准没有。
   - `different`：两边都有但 md5 不同。
   - `identical`：md5 相同。
3. 输出计数 + 限量列表；`includeContentDiff` 为 true 时只对前 `maxDifferences` 条 changed 抓正文生成 diff（可复用 `diff_config` 的逻辑）。

**输出**：

```json
{
  "baseline": "dev",
  "summaries": [
    {"namespace": "prod", "identical": 120, "different": 8, "onlyInBaseline": 2, "onlyInTarget": 1, "truncated": false}
  ],
  "changed": [
    {"namespace": "prod", "dataId": "gateway.yaml", "group": "DEFAULT_GROUP", "status": "different",
     "added": 3, "removed": 1, "diff": "..."}
  ],
  "hint": "Run diff_config with a dataId to see the full line diff."
}
```

**边界/失败模式**：

- 命名空间不存在/无权限：按命名空间返回错误项，不拖垮其他命名空间。
- 配置数超过上限：`truncated: true` + 实际扫描数 + 建议收窄 group/dataId。
- 同 dataId 跨 group：以 `group@@dataId` 为键，不合并。
- 空命名空间：计数为 0，不报错。
- v1/v3 分页字段差异：复用 `normalizePaging` 与现有解析。

**测试**：

- 四类差异（only baseline / only target / different / identical）全覆盖。
- 多页分页（> 500）与 `maxConfigsPerNamespace` 截断。
- 命名空间不存在、空命名空间、跨 group。
- `includeContentDiff` 上限与超大配置跳过。

**工作量/风险**：M / 低。

---

### F3 `diff_history`（历史版本对比）

**问题**：现有 `get_config_history` 能列历史、能读单版本，但不能比较；AI 要自己拿两份大文本心算，容易漏行。

**参数**：

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `connectionId` | string | 是 | 宿主注入 |
| `dataId` / `group` | string | 是/否 | 配置定位；group 默认 `DEFAULT_GROUP` |
| `namespace` | string | 否 | 缺省用连接命名空间 |
| `fromHistoryId` | string | 是 | 基准版本 |
| `toHistoryId` | string | 否 | 缺省表示当前版本 |
| `contextLines` | integer | 否 | 默认 3，硬上限 10 |

**实现**：

- `fromHistoryId` 走 `getConfigRevision`；`toHistoryId` 为空走 `readConfig`，否则再取一个 revision。
- 复用 `lineDiffs` / `renderDiff`；输出两侧的 `operator`、`srcIp`、`opType`、`md5`、`createdTime`。
- 状态：`identical` / `different` / `missing-in-source` / `missing-in-target`。

**边界/失败模式**：

- 配置已删除：当前版本不存在时返回 `missing-in-target`，并附最后历史版本信息。
- revision 不存在/已被清理：明确报错，不返回空 diff。
- 行数超 `maxDiffLines`（3000）：返回可操作错误，建议收窄或只对比关键区段。
- 两侧 md5 相同：直接 `identical`，不生成 diff。

**测试**：当前 vs 历史、历史 vs 历史、缺失、相同、超大。

**工作量/风险**：S / 低。

---

### F4 `diagnose`（连接体检）

**问题**：Nacos 3.x 的 console 端口、强制登录、admin 初始化是已有文档记录的真实坑；当前 `connection/test` 只给一句「reading configs failed」，用户不知道下一步。

**检查项**：

1. 健康端点 + 延迟（v3 console readiness / v1 readiness）。
2. 协议探测结果（auto/v1/v3）与用户覆盖值是否一致。
3. v3 `GET /v3/console/server/state`：版本、`startup_mode`、`auth_enabled`、`auth_admin_request`、`defaultMaxSize`、配额。
4. v1 `GET /v1/console/server/state`：版本。
5. 登录检查（有凭据时）；无凭据时明确「console 工具不可用」。
6. 命名空间列表 + 一次配置读取探测，区分 401/403/404/网络错误。
7. 汇总 `ok/warn/fail` 与修复建议。

**输出**：

```json
{
  "overall": "warn",
  "server": {"version": "3.2.4", "mode": "standalone", "authEnabled": true, "authAdminRequest": false, "protocol": "v3"},
  "checks": [
    {"id": "health", "status": "ok", "detail": "ready: ok (42 ms)"},
    {"id": "console-auth", "status": "warn", "detail": "console APIs require credentials", "remediation": "set username/password"}
  ],
  "suggestions": ["Set username/password to enable search/history/namespaces/services."]
}
```

**边界/失败模式**：

- v3 console 不可达但 client API 可用：`overall: warn`，说明哪些工具可用。
- `auth_admin_request=true`：提示先初始化管理员（指向连接动作，不在诊断里写）。
- 错误密码/无权限：区分「凭据错误」与「ACL 拒绝」。
- v1 无 server state：降级为 readiness + version 未知。
- 全程只读，不发送写请求。

**测试**：v3 正常 / 待初始化 / console 不可达 / 错误密码；v1 正常 / 匿名；超时。

**工作量/风险**：M / 低。与 F1/F2 并列最值得先做。

---

## 6. 工作台 2.0（F7）

**布局**：

- 左侧：连接摘要 + 命名空间选择 + 分组过滤。
- 顶部标签：配置 / 服务 / 漂移 / 概览。
- 主区：列表 + 详情；详情含「正文 / 元数据 / 历史 / 对比」四个视图。

**功能**：

1. **配置浏览器**：dataId/group/正文搜索；列表分页；正文查看（行号、等宽、复制）；历史列表 + 单版本查看 + diff；导出单条配置。
2. **漂移矩阵**：选择基准与目标命名空间，展示 changed/missing/added 矩阵；点击行进入 diff；导出 Markdown。
3. **服务健康看板**：服务列表 → 实例表（健康、权重、集群、metadata）；健康率、离线/不健康统计；复制 `ip:port`。
4. **连接体检卡片**：F4 的可视化；`auth_admin_request` 时显示「初始化 Nacos 3.x 管理员」连接动作（写操作，用户主动点击，二次确认）。
5. **AI 副驾**：`host.ai.setRecommendations` 提供 5 条场景化问题；`ai.openConversation` 带当前配置/命名空间上下文；`mode: "agent"` 可使用本插件工具。
6. **命令/右键入口**：`command` + `menus` + `context-menu`；命令面板「Nacos: 搜索配置 / 环境对比 / 服务实例 / 连接体检」；连接右键打开工作台。
7. **持久化**：`host.storage` 保存最近命名空间、最近配置、面板状态；导出用 `host.fileTransfer`，浏览器端降级 Blob 下载；所有内容 `textContent` 渲染。

**后端支撑**：

- 新增工作台 RPC（避免 UI 解析 MCP 文本结果）：`nacos/overview`、`nacos/configs`、`nacos/config`、`nacos/config/history`、`nacos/config/diff`、`nacos/services`、`nacos/instances`、`nacos/namespaces`、`nacos/diff/envs`。
- 与 MCP 工具共享同一个 service 层，避免逻辑分叉。
- 写操作走独立 RPC + 自定义确认弹窗，不复用 AI 工具入口。

**权限**：新增 `host.workbench`、`host.storage`、`host.ai`；实时流再加 `host.events`。全部按 `capabilities.*` 探测，旧宿主隐藏入口。

**测试**：后端 RPC 单测；UI 手工冒烟（`dbx-plugin dev`）；E2E 不依赖浏览器，直接驱动 sidecar 验证 RPC。

---

## 7. 受控写（F8/F9）

### 7.1 写模式与工具隐藏

- 连接表单新增：
  - `write_enabled`（boolean，默认 false）。
  - `write_allowlist`（text，逗号分隔命名空间，默认空 = 不允许写）。
- `mcp/tools` 读取宿主传入的 `connectionId`，解析缓存连接记录；未开启写或未命中白名单时，写工具不出现在工具列表。
- 即使模型伪造参数，`mcp/call` 也再次校验写模式/白名单。

### 7.2 两阶段发布

1. `preview_config_change`（只读）：
   - 参数：目标 namespace/group/dataId、content（或 `sourceNamespace` + `historyId`）。
   - 返回：当前 md5、目标 md5、行级 diff、大小/类型检查、风险评分、`confirmToken`（5 分钟，绑定连接/目标/内容哈希/当前 md5）。
2. `publish_config`（非只读，宿主会再确认）：
   - 参数：目标、content、`expectedMd5`、`confirmToken`。
   - 服务端使用 `casMd5` 请求头；md5 不匹配返回冲突错误，不覆盖并发修改。
   - v1：`POST /v1/cs/configs`（form：dataId/group/tenant/content/type/appName/src_user）+ `casMd5`。
   - v3：`POST /v3/console/cs/config`（form：dataId/groupName/namespaceId/content/type）+ `casMd5`。
   - 成功后返回新 md5、变更摘要与审计记录 id。

### 7.3 提升与回滚

- `promote_config`：源命名空间 → 目标命名空间；读源内容、生成 diff、走同一套 publish（不依赖 v3 独有的 clone 接口，保持 v1/v3 一致）。
- `rollback_config`：按 `historyId` 读历史内容，生成 diff，再 publish；`expectedMd5` 为当前 md5。
- 批量提升（多 dataId）作为 UI 功能，AI 工具先只做单条，避免一次误改一大片。

### 7.4 风险评分与审计

- 风险因素：目标命名空间（prod 加权）、变更行数、配置是否被监听（F6）、是否修改敏感 key（password/secret/token/url）、是否跨环境。
- 输出 `risk: {level, reasons}`；高风险时要求显式 `confirmRisk: true`。
- 审计：`DBX_PLUGIN_DATA_DIR/audit.jsonl`，记录时间、连接 id、工具、目标、结果、风险等级；不记录正文与凭据。
- 不提供 AI 删除配置/命名空间；删除仅允许工作台内「输入 dataId 确认」的显式操作（可选，最后做）。

---

## 8. 其他功能卡简述

### F5 `get_context`（环境速览）

- 一次返回：命名空间列表、当前命名空间配置数/服务数、最近变更（若可用）、服务健康摘要、诊断异常项。
- 用于 AI 开场定位，避免多轮 list；可缓存 30–60 s。
- 工具预算紧时，优先做成 `diagnose` 的聚合输出或工作台首页。

### F6 `list_listeners`（配置订阅者）

- v1 `GET /v1/cs/listener?dataId&group&tenant&sampleTime`；v3 `GET /v3/console/cs/config/listener`。
- 输出订阅者数量、IP/MD5 列表；注明采样/聚合语义。
- 发布预览的风险评分可复用该能力。

### F10 连接增强

- 自定义 CA：连接字段 `tls_ca`（picker 文件 + `content_field`）与 `tls_skip_verify`（默认 false，UI 明示风险）。
- Nacos 3.x 管理员初始化：连接动作 `POST /v3/auth/user/admin`，仅在 `auth_admin_request=true` 时显示。
- 命名空间显示名解析：`namespace` 与 `namespaces` 参数接受 ID 或 `namespaceShowName`，通过 `listNamespaces` 解析并缓存 60 s；歧义时报错。

### F11 隧道支持

- 使用宿主传入的 `runtime.host/port`；Nacos 3.x 双端口（server + console）需要 SOCKS5 代理时声明 `proxy_route: true` 并使用 `runtime.proxy`。
- 需先验证 DBX 插件连接是否支持配置 SSH/代理传输层；属高价值但需要 spike。

### F12 GitOps 基线对比

- 工作台通过 `host.fileTransfer.pick/onDrop` 读取本地 Git 目录中的配置文件，按 `group/dataId` 约定匹配 Nacos 配置，生成漂移列表与 diff。
- 不写 Nacos；适合发布前检查「Git 里的配置是否已经同步到 Nacos」。
- 注意：文件内容进入 sidecar 后只做比对，不落盘；大目录需分块与上限。

### F13 实时变更流

- 使用 Nacos 长轮询 listener 监听指定 dataId/group；变更时通过 `host.events` 推送到工作台，展示变更 feed。
- 需要连接生命周期管理、后台 goroutine、取消与重连；不暴露为 MCP 长任务。

### F14 Nacos AI/MCP 注册中心（实验）

- Nacos 3.3+ 的 `/v3/console/ai/mcp` 管理 MCP Server、工具、版本与发布状态。
- 可做：列出 MCP Server / 工具 / 版本；展示 `.mcp.json` 片段供复制；不做自动安装、不做写。
- 需先确认列表端点与权限模型。

### F15 导出/导入/迁移

- 导出：v1 `export=true` / v3 `export2`，ZIP 二进制；工作台通过 `fileTransfer` 保存；AI 不直接返回二进制。
- 导入：上传 ZIP，先解析并与目标命名空间做 diff，再走受控写；需写模式 + 确认。
- 迁移：导出 → 导入 + 差异预览；不默认覆盖。

### F16 Beta/灰度与实例运维

- Beta：`betaIps` 头发布、查询/停止 beta；用于小范围灰度，风险高，先做工作台。
- 实例运维：`PUT /v3/console/ns/instance` 更新权重/上下线/健康；建议只做工作台显式操作，暂不开放给 AI。

---

## 9. 安全与隐私设计

| 主题 | 要求 |
| --- | --- |
| 默认只读 | 写工具在连接未开启 `write_enabled` 且命中白名单前不可见/不可调用 |
| 最小暴露 | 工具结果只返回请求字段；错误不带上游正文、token、密码 |
| 响应上限 | 沿用上游 2 MiB / 工具 6 MiB / 30 s 预算；扫描/漂移必须显式 `truncated` |
| 并发保护 | 发布必须带 `casMd5`；预览 token 绑定内容哈希与当前 md5 |
| 宿主确认 | 非 `readOnlyHint` 工具由宿主展示完整参数并人工确认；插件仍做二次校验 |
| 审计 | 写操作写 `audit.jsonl`，不含正文/凭据；保留最近 N 天（可配置） |
| XSS | 工作台只用 `textContent`/`createElement`，不用 `innerHTML` |
| 输入校验 | 不使用用户正则；长度/页码/命名空间/配置键全部白名单化或钳制 |
| 凭据 | 不落盘；sidecar 断开连接即删除会话缓存；日志不打印凭据 |
| 内容片段 | 正文搜索可能返回密钥片段，信任边界与 `get_config` 一致；后续可评估敏感 key 脱敏 |

---

## 10. 测试策略

### 单元测试（`backend/main_test.go`、`backend/diff_test.go`）

- v1/v3 mock 覆盖每个新端点：成功、404、401/403、脏数据、分页、超时。
- 输出断言：字段、上限、`truncated`、错误信息不含敏感内容。
- 写路径：写模式关闭、白名单未命中、token 过期/不匹配、CAS 冲突、成功。

### 协议冒烟（`scripts/smoke.py`）

- 驱动 sidecar 完成 `plugin/initialize → connection/test → connection/connect → mcp/tools → mcp/call`。
- 新增工具至少各一条成功路径 + 一条错误路径；工具数量断言随版本更新。

### 真实 Nacos E2E（`scripts/e2e_docker.py`）

- v1（2.4.3）与 v3（3.2.4）双链路；新工具逐项断言。
- 写路径使用独立容器/命名空间，测试后清理；不碰用户数据。
- 记录检查矩阵数量，更新 `docs/e2e-*.md`。

### 工作台

- 后端 RPC 单测；`dbx-plugin dev` 手工冒烟；不引入前端测试框架（保持零依赖）。
- 检查项：空连接、无权限、超大配置、旧宿主无 `host.ai/storage`、明暗主题。

---

## 11. 待验证 Spike

| # | 问题 | 影响 | 验证方式 |
| --- | --- | --- | --- |
| S1 | v3 `config/listener` 的 `aggregation` 语义与响应形状 | F6 输出 | 3.2.4 实例实测 + 源码 |
| S2 | v3 `server/state` 字段与权限 | F4 | 3.2.4 实测（匿名/登录） |
| S3 | v1 `server/state` 可用字段 | F4 | 2.4.3 实测 |
| S4 | v3 AI/MCP 列表端点（3.3+）与权限 | F14 | 3.3+ 实例 + 官方文档 |
| S5 | DBX 插件连接的 `runtime` / `proxy_route` 隧道行为 | F11 | DBX 宿主实测（SSH/代理） |
| S6 | `host.ai/storage/fileTransfer/events` 在目标 DBX 版本的可用性 | F7/F13 | 宿主 capabilities + 手工冒烟 |
| S7 | Nacos 3.x 管理员初始化参数与错误码 | F10 | 3.2.4 全新实例 |
| S8 | v1/v3 发布接口的 `casMd5` 行为与冲突错误码 | F8 | 2.4.3/3.2.4 实测 |
| S9 | 大 ZIP 导出的流式传输（JSONL 8 MiB 限制） | F15 | 分块/二进制通道验证 |
| S10 | 命名空间显示名解析的缓存与歧义 | F10 | 单测 + 实例 |

---

## 12. 非目标

- 不替代 Nacos 控制台，不覆盖用户/角色/权限管理。
- 不默认开放写；不提供 AI 删除配置/命名空间。
- 不做配置内容解密（Nacos 加密插件）。
- 不引入前端框架或第三方 Go 依赖（除非单独评审）。
- 不承诺 DBX Web/Docker 后端的插件工具支持。
- 不实现跨 Nacos 实例（多连接）diff 的 v0.3 版本；该能力依赖 peer connection 设计，放到后续评估。

---

## 13. 验收清单

### 本轮（文档）

- [ ] 16 项功能候选齐全，含价值、API 依据、代码落点、v1/v3 差异、工作量、风险。
- [ ] F1–F4 达到可实现粒度：参数、输出、边界、测试。
- [ ] 所有 API 标注「已验证」或「待 spike」，不虚构。
- [ ] `README.md` / `README.en.md` / `docs/README.md` 链接到本文。
- [ ] `git diff --stat` 仅文档；`backend/`、`ui/`、`manifest.json` 零改动。
- [ ] `git diff --check` 通过。

### v0.3（代码，已实现 2026-10-17）

- [x] F1：v3 原生搜索 + v1 限量扫描；`truncated` 显式；无匹配不报错。
- [x] F2：四类差异全覆盖；多页与上限测试。
- [x] F3：当前 vs 历史、历史 vs 历史、缺失、超大。
- [x] F4：v3 待初始化/console 不可达/错误密码、v1 匿名（单测覆盖；真实 Docker E2E 待复跑）。
- [x] `go test ./...`、`scripts/smoke.py` 全绿（78 个测试；冒烟 19 项检查）。
- [x] `scripts/e2e_docker.py`（v1 + v3）在真实 Nacos 上通过：v1 55/55、v3 55/55。
- [x] README 工具表、版本号、manifest 与路线图同步。

---

## 14. 参考

- 仓库内：[`README.md`](../README.md)、[`docs/v1-api-verification-2026-10-17.md`](v1-api-verification-2026-10-17.md)、[`docs/e2e-full-regression-2026-10-17.md`](e2e-full-regression-2026-10-17.md)、[`docs/e2e-nacos3-native-2026-10-17.md`](e2e-nacos3-native-2026-10-17.md)、[`docs/code-review-2026-10-16.md`](code-review-2026-10-16.md)。
- Nacos 控制台 API：[`console-api.md`](https://raw.githubusercontent.com/nacos-group/nacos-group.github.io/develop-astro-nacos/src/content/docs/next/zh-cn/manual/admin/console-api.md)（v3 console / server state / AI-MCP）。
- DBX 插件开发文档：[`plugin-development-full.txt`](reference/plugin-development-full.txt)、[`manifest.schema.json`](reference/manifest.schema.json)。
- 背景 issue：[t8y2/dbx#11893](https://github.com/t8y2/dbx/issues/11893)。

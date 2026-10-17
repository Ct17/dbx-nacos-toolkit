# Nacos Toolkit — DBX 插件

在 [DBX](https://github.com/t8y2/dbx) 中连接 Nacos，并向 AI 助手开放**只读 MCP 工具**。

DBX 桌面端内置了 Nacos 配置浏览界面，但内置连接的 MCP 面是关闭的（官方连接类型定义中 `mcpMode: unsupported`，见 [issue #11893](https://github.com/t8y2/dbx/issues/11893)）。本插件通过 DBX 官方插件体系解决这个问题：自带 Nacos 连接类型，并把工具桥接进 DBX MCP 服务器，`Claude Code`、`Cursor`、`Windsurf` 等 AI 客户端即可直接读取 Nacos 配置。

## 功能

**连接表单**（连接树 → 新建连接 → Nacos）

| 字段           | 说明                                      |
| ------------ | --------------------------------------- |
| 服务地址 / 端口    | 支持 `127.0.0.1`、域名、`https://…`，也可直接粘贴 `https://host:8848/nacos`；端口已填则不再追加，默认 8848 |
| Context Path | 默认 `/nacos`，自定义部署时修改；地址里已带路径时以本字段为准 |
| Console 端口     | Nacos 3.x 控制台端口（默认 8080，留空自动探测）；1.x/2.x 可留空 |
| API 版本         | `auto`（默认，推荐）/ `v1` / `v3`；仅在自动探测选错协议栈时手动指定 |
| 命名空间 ID      | 留空为 public                              |
| 用户名 / 密码     | Nacos 1.x/2.x 未开启鉴权时可留空；**Nacos 3.x 必填**（console API 始终要求账号，见下） |

**MCP 工具**（全部只读，声明 `readOnlyHint: true`，AI 调用无需人工确认）

| 工具                | 说明                                                 |
| ----------------- | -------------------------------------------------- |
| `get_config`      | 按 dataId（+ group / namespace）读取配置全文                |
| `search_configs`  | 按关键词搜索配置，返回分页元数据（dataId / group / type / md5，不含正文） |
| `diff_config`     | **跨环境对比**：同一配置在 2–8 个命名空间间逐行 diff（首个为基准），输出带上下文压缩的差异 |
| `get_config_history` | **配置历史**：列出变更记录（时间 / 操作人 / opType / md5），传 `historyId` 可读该历史版本的完整内容 |
| `list_namespaces` | 列出所有命名空间及其配置数量                                     |
| `list_services`   | 列出命名空间中的注册服务名                                      |
| `list_instances`  | 列出某个服务的实例（IP / 端口 / 健康 / 权重 / 集群 / metadata）        |

在 AI 客户端中工具呈现为 `dbx_nacos_toolkit__get_config` 等形式（`dbx_<插件前缀>__<工具名>`，前缀取插件 id 最后一段）；内置 AI 面板中为 `<前缀>__<工具名>`。

**工作台**：显示连接探测结果与当前暴露给 AI 的工具清单。

## 架构

```text
AI 客户端 (Claude Code / Cursor / DBX AI 面板)
        │  MCP (tools/list, tools/call)
        ▼
DBX MCP 服务器 / 宿主        凭据由宿主注入，不经过模型
        │  插件桥接协议 (mcp/tools, mcp/call, lifecycle.connection)
        ▼
本插件 Go Sidecar          纯标准库，自动识别协议栈（v1 / v3）
        │  HTTP（v1：1.x/2.x Open API；v3：3.x 原生 client + console API）
        ▼
Nacos Server
```

- 协议实现：[`dbx-plugin-sdk`](https://github.com/t8y2/dbx/tree/main/plugins/sdk/go)（Sidecar Protocol v1，JSON-RPC 2.0 / JSONL over stdio）
- Nacos 调用：连接时自动探测协议栈——v3（Nacos 3.x 原生，`/v3/client/**` + `/v3/console/**` 双端口）优先，回退 v1（`/v1/cs/configs`、`/v1/console/namespaces`、`/v1/ns/service/list`、`/v1/ns/instance/list` 等）；登录与 token 自动续期（v1 用 `accessToken`，v3 用 `Bearer` 头）
- 无第三方依赖：Go 标准库 + DBX SDK（diff 为自研 LCS 实现）

## 目录结构

```text
├── manifest.json            # 插件契约：连接表单、工作台、mcp 贡献声明
├── dbx-plugin.toml          # 打包配置（Go backend）
├── backend/
│   ├── main.go              # handler：连接生命周期 + 协议分发
│   ├── nacos.go             # 客户端基础设施：地址解析、协议探测、鉴权、传输
│   ├── v1.go                # Nacos 1.x/2.x Open API 协议栈
│   ├── v3.go                # Nacos 3.x 原生 API 协议栈（client + console）
│   ├── mcp.go               # MCP 工具定义与调用
│   ├── diff.go              # 行级 diff（LCS，零依赖）
│   ├── main_test.go         # 集成测试（httptest 模拟 Nacos，v1 + v3 双 mock）
│   └── diff_test.go         # diff 算法单元测试
├── scripts/smoke.py         # 协议级冒烟：模拟宿主驱动 sidecar 全链路（mock Nacos）
├── scripts/e2e_docker.py    # Docker 真实 Nacos 端到端：七工具逐项断言 + 错误路径 + 鉴权链路（v1/v3 双模式）
├── ui/index.html            # 工作台界面（沙箱静态页）
├── third_party/dbx-plugin-sdk/  # Go SDK 本地副本（go.mod replace）
└── docs/reference/          # 开发资料存档（官方文档、schema）
```

## 开发

环境要求：Go 1.22+、Node.js 22+（仅 CLI 需要）、`@dbx-app/plugin-cli`。

```bash
# 单元测试（httptest 模拟 Nacos，离线可跑）
cd backend && go test ./... -v

# 协议级冒烟（编译 sidecar + 起 mock Nacos + 模拟宿主对话）
python3 scripts/smoke.py

# 真实 Nacos 端到端（需本地 Docker；脚本自动灌测试数据，可重复运行）
# v1 链路（Nacos 2.4.3 等）
docker run -d --name nacos-e2e -p 18848:8848 -e MODE=standalone nacos/nacos-server:v2.4.3
python3 scripts/e2e_docker.py --port 18848

# v3 原生链路（Nacos 3.2+，无 adapter；需映射 console 端口 8080）
docker run -d --name nacos3-e2e -p 18868:8848 -p 18088:8080 \
  -e MODE=standalone -e NACOS_AUTH_ENABLE=false \
  -e NACOS_AUTH_TOKEN=<Base64 编码的 ≥32 字节字符串，3.x 镜像启动必需> \
  nacos/nacos-server:v3.2.4
python3 scripts/e2e_docker.py --port 18868 --console-port 18088

# 独立开发宿主（不启动 DBX，浏览器调试真实前后端 + 连接表单）
npx @dbx-app/plugin-cli dev --path . --port 5190

# 打包未签名开发包
npx @dbx-app/plugin-cli package .
```

> `backend/go.mod` 通过 `replace` 指向 `third_party/dbx-plugin-sdk`（SDK 尚无 Go module tag）。如使用 DBX 源码工作区，可改用 `--sdk-root` 或 `DBX_PLUGIN_SDK_ROOT`。

## 在 DBX 中安装与使用

1. 打包：`dbx-plugin package .` → `dist/io.github.ct17.nacos_toolkit-0.2.0-darwin-arm64.dbxp`
2. DBX → 插件中心 → 设置 → 第三方与开发者选项 → 开启「允许安装未签名开发包」
3. 安装 `.dbxp` → 已安装中确认「Nacos Toolkit」
4. 新建连接 → 选择 **Nacos**（插件提供）→ 填写地址与凭据 → 测试连接
5. 打开该连接 → 「内置 AI 工具」中开启本插件 → AI 客户端即可调用工具

**MCP 客户端配置**（`.mcp.json`，与 DBX 常规配置相同）：

```json
{
  "mcpServers": {
    "dbx": { "command": "npx", "args": ["-y", "@dbx-app/mcp-server"] }
  }
}
```

再在 **DBX 设置 → MCP** 中把连接和 `dbx_nacos_toolkit__*` 工具加入白名单即可。

## Nacos 3.x 支持（原生 v3，无需 adapter）

插件在连接时**自动探测协议栈**：先探测 v3 控制台健康端点（Nacos 3.x），失败再回退 v1 健康端点（1.x / 2.x）。Nacos 3.x 直接走原生 v3 API，**不需要安装 legacy adapter**：

| 环境 | 协议栈 | 连接配置 |
| --- | --- | --- |
| Nacos 1.x / 2.x | v1 Open API | 无（用户名/密码仅鉴权开启时填写） |
| Nacos 3.x（原生） | v3 API | 填写 **Console 端口**（默认 8080）+ **用户名/密码** |
| Nacos 3.x + legacy adapter | v3（探测优先） | 同上；adapter 不再是必需项 |

Nacos 3.x 的两点使用提示：

1. **Console 端口**：v3 的 console API（搜索 / 历史 / 命名空间 / 服务列表）运行在独立端口（默认 8080，与 8848 服务端口分离）。Docker 映射改变了端口（如 `-p 18088:8080`）时需要在连接里填写实际端口；直装默认 8080 可留空（自动探测）。
2. **账号是必需的**：v3 的 console API 即使服务端关闭了鉴权开关也强制登录。全新实例先初始化管理员（`POST /v3/auth/user/admin`），再把该账号填进连接。仅使用 `get_config` / `diff_config` / `list_instances`（client API）时可匿名连接。

实测（纯 Nacos 3.2.4，无 adapter）：**43 项端到端检查全部通过 × 3 遍**（双实例：无鉴权 + 鉴权），2.4.3 回归 43/43。细节见 `docs/e2e-nacos3-native-2026-10-17.md`。

## 已知限制

- **只读**：当前版本不提供写配置、发服务等操作（后续版本规划）
- **仅桌面端**：DBX Web/Docker 后端不提供插件工具（官方限制）
- **凭据**：由宿主注入 sidecar，不落盘；sidecar 仅在内存中保留连接快照与活跃会话（Go 字符串无法原地清零，断开连接后随 GC 回收）。健康探测（`nacos/ping`）不携带凭据
- **单次响应上限 2 MiB**：超过上限的响应会显式报错，而不是静默截断后交给 AI
- **AI 可见内容**：工具结果只包含请求结果与 HTTP 状态码，上游响应正文写入插件日志，不进模型上下文
- **Nacos 协议**：1.x / 2.x 走 v1 Open API；3.x 自动切换原生 v3 API（3.2+ 无需 legacy adapter）。v3 的实例列表不返回已下线（`enabled=false`）实例（Nacos 官方语义）；v3 的 console 接口要求账号（见「Nacos 3.x 支持」）

## 性能

- 同一连接的多次工具调用复用同一个 Nacos 客户端：token 与 HTTP 连接池不重复建立（仅首次调用登录一次），连接断开时缓存失效
- 上游调用超时 15 s，单次工具调用总预算 30 s

## Roadmap

- [x] `diff_config`：跨命名空间/环境对比同一配置（对齐 issue #11893 的核心诉求）— v0.1.1
- [x] `list_instances`：服务实例列表 — v0.1.1
- [x] `get_config_history`：配置历史版本（列表 + 单版本内容）— v0.1.1
- [x] Nacos 3.x 原生 v3 协议栈：自动探测 + console 端口/Bearer 鉴权，无需 legacy adapter — v0.2.0
- [x] `list_instances` 支持 cluster 过滤；`diff_config` 并行读取（2–8 命名空间单次往返等待）— v0.2.0
- [ ] 工作台升级为配置浏览器（列表 / 搜索 / 查看）
- [ ] 写工具（发布配置、dryRun 预览 + 两阶段确认，需宿主人工确认）

## 参考资料

- [DBX 插件开发文档](https://dbxio.com/cn/docs/plugin-development)（存档于 `docs/reference/`）
- [DBX 插件快速开始](https://github.com/t8y2/dbx/blob/main/plugins/GETTING_STARTED.zh-CN.md)
- [DBX MCP 集成文档](https://dbxio.com/cn/docs/mcp)

## License

Apache-2.0

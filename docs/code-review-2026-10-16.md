# 代码评审报告 — dbx_plugin_nacos

- 评审对象：`Ct17/dbx-nacos-toolkit`（HEAD = `ba86360`，已与 `origin/main` 同步）
- 评审时间：2026-10-16
- 评审范围：全部已跟踪文件 + git 历史 + 工作区
- 验证方式：`go test ./...`（14 个测试全绿）、`strings` 扫描二进制、两个临时探针实测（已删除，工作区已恢复干净）

---

## 结论摘要

| 级别 | 数量 | 内容 |
| --- | --- | --- |
| P0 | 1 | 8.2 MB 编译产物已推送到公开仓库，且内含本机绝对路径 |
| P1 | 1 | Nacos accessToken 会随传输层错误泄漏给 AI 模型与 UI（已实测复现） |
| P2 | 3 | 配置读取静默截断 4 MB；上游响应体回显进错误；每次工具调用重复登录 |
| P3 | 5 | 分页参数静默降级、host 解析歧义、字节切片截断、缺 LICENSE、CI 未 pin SHA |
| 良好 | 4 | 无凭据入库、`.DS_Store`/`dist/`/签名密钥均已忽略、UI 无 XSS、并发有锁保护 |

---

## A. 不应该 push 到远程仓库的内容

### A1【P0】`backend/dbx-nacos-toolkit` — 8.2 MB 编译产物已推送

**证据**

```text
$ file backend/dbx-nacos-toolkit
backend/dbx-nacos-toolkit: Mach-O 64-bit executable arm64
$ git ls-files | grep dbx-nacos-toolkit
backend/dbx-nacos-toolkit
$ git log --oneline --follow -- backend/dbx-nacos-toolkit
8209dbd feat: Nacos Toolkit DBX plugin with read-only MCP tools   # 已 push
$ git cat-file -s e8ad3b5bfec...        # blob 大小
8600786
```

**为什么要摘掉**

1. **仓库体积失控**：已跟踪文件中它 8.2 MB，第二名是 76 KB 的文档 —— 99% 的仓库体积是这一个产物。每次重新 `go build` 提交一次就多一个 8 MB blob，历史不可回收。
2. **泄漏本机信息**：`strings` 实测二进制里嵌入了构建机的绝对路径 ——
   `/Users/tao/code/dbx_plugin_nacos/backend/{main,mcp,nacos}.go`、
   `/Users/tao/code/dbx_plugin_nacos/third_party/dbx-plugin-sdk/sdk.go`。
   公开仓库里直接暴露本机用户名与目录结构。
3. **无意义且有害**：darwin-arm64 单平台，别人 clone 下来用不了；`dbx-plugin package .` 会按 `dbx-plugin.toml` 自行编译（`binary = "dbx-plugin-nacos-toolkit"`），仓库里这份 `dbx-nacos-toolkit` 只是本地 `go build` 的遗留物，与打包流程无关。

**修复（安全，不改历史）**

```bash
# 1) 停止跟踪，保留本地文件
git rm --cached backend/dbx-nacos-toolkit

# 2) .gitignore 追加
printf '\n# 构建产物\n/backend/dbx-nacos-toolkit\n/backend/dbx-plugin-nacos-toolkit\n' >> .gitignore

# 3) 提交
git add .gitignore && git commit -m "chore: stop tracking the compiled sidecar binary"
```

**可选：抹掉历史里的 8 MB**（`git rm --cached` 只解决 HEAD，历史 blob 仍在；仓库已 push 到公开地址，需重写历史 + force push）

```bash
git filter-repo --path backend/dbx-nacos-toolkit --invert-paths   # 需先 brew install git-filter-repo
git push --force-with-lease origin main
```

个人仓库、无协作者时这是干净的收尾；若已有 fork/协作者，先通知再强推。

### A2 无需处理但已确认干净的项（好消息）

- **无任何凭据入库**：全量 `git grep` + 全历史扫描（`AKIA…` / `ghp_…` / `PRIVATE KEY` / `.env` / `*.pem` / 签名密钥）零命中。测试里的 `nacos/nacos` 是 mock 值。
- `.DS_Store`、`dist/`、`.dbx-repository-signing-key.env`、`*.log` 均已在 `.gitignore`（第 1–5 行）覆盖，`git check-ignore` 验证生效。
- 工作区里的 `.DS_Store`（6 KB）未被跟踪，无需提交。

### A3【P3】README 声明 Apache-2.0，但仓库没有 `LICENSE` 文件

README 第 128 行写了 License，实际 `ls LICENSE*` 无匹配。声明了许可却没有正文 = 别人无法合规使用/分发（GitHub 也不会识别许可）。建议补一份 Apache-2.0 全文，或在 README 去掉许可声明。

### A4【P3】发布流水线引用了可变的上游分支 + `contents: write`

`.github/workflows/plugin-release.yml:12-18` 使用 `t8y2/dbx/.github/workflows/plugin-release-reusable.yml@plugin-sdk-v1`，并授予 `permissions: contents: write`。可复用的第三方 workflow 用**分支名**而不是 commit SHA 固定 —— 上游分支一旦被改写，代码会在你的 release 事件上运行，并持有你仓库的写权限。

```yaml
# 建议：pin 到审计过的 commit SHA
uses: t8y2/dbx/.github/workflows/plugin-release-reusable.yml@<commit-sha>  # plugin-sdk-v1
```

---

## B. 安全隐患

### B1【P1】Nacos accessToken 泄漏进错误文本（已实测复现）

**位置**：`backend/nacos.go:190-192`

```go
resp, err := c.http.Do(req)
if err != nil {
    return nil, 0, &apiError{Message: fmt.Sprintf("request to %s failed: %v", c.baseURL, err)}
}
```

`c.http.Do` 返回的 `*url.Error`，其 `Error()` 实现是 `fmt.Sprintf("%s %q: %s", Op, e.URL, e.Err)` —— **`e.URL` 含完整 query string**，而 Nacos v1 的鉴权 token 正是以 `?accessToken=…` 传递的（`nacos.go:179-181`）。

**实测输出**（临时探针，上游超时场景）

```text
request to http://127.0.0.1:51318/nacos failed:
Get "http://127.0.0.1:51318/nacos/v1/console/health/readiness?accessToken=SUPER-SECRET-TOKEN":
context deadline exceeded (Client.Timeout exceeded while awaiting headers)
```

**影响链**：`getOnce` → `apiError` → `toolError(err.Error())`（`mcp.go:166`、`mcp.go:185`、`mcp.go:198`、`mcp.go:219`）→ 作为 MCP 工具结果交给 AI 模型，并显示在 DBX 面板与 UI（`ui/index.html:168`）。

即：**任何一次上游超时 / DNS 失败 / 连接重置，都会把一枚有效期约 5 小时、可读全部配置的 token 写进模型上下文和对话历史。** 只需 `curl` 一次截断的 URL 就能直接读配置。

**修复**

```go
// nacos.go: 新增（需 import "errors"）
// transportError strips the *url.Error wrapper, whose Error() embeds the full
// request URL — including the accessToken query parameter.
func transportError(err error) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Err != nil {
		return urlErr.Err.Error()
	}
	return err.Error()
}

// getOnce 内改为：
	return nil, 0, &apiError{Message: fmt.Sprintf("request to %s failed: %s", c.baseURL, transportError(err))}
```

`ensureToken` 的 `login request failed: %v`（`nacos.go:120`）同理替换，避免将来支持 header/body 传参时再踩同一个坑。

### B2【P2】配置内容超 4 MB 被静默截断

**位置**：`backend/nacos.go:31` + `nacos.go:195`

```go
maxResponseBytes = 4 << 20
...
body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
```

超过 4 MB 的响应被无声砍断：不报错、不标记，`get_config` 会把**半截 YAML** 当成完整配置返回给 AI。AI 基于残缺配置回答"某个服务为什么这么配"，是会直接误导人的。同类问题在 `ensureToken`（`nacos.go:123`）也存在（登录响应一般很小，可忽略）。

**修复思路**：多读 1 字节用于判定截断，超出时显式报错而不是返回残数据：

```go
body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
if err != nil { /* 原有错误处理 */ }
if len(body) > maxResponseBytes {
	return nil, resp.StatusCode, &apiError{Status: resp.StatusCode,
		Message: fmt.Sprintf("upstream response exceeds %d bytes; refusing to return truncated config content", maxResponseBytes)}
}
```

**附带风险**：SDK 的 `maxJSONBytes = 8 MB`（`third_party/dbx-plugin-sdk/sdk.go:17`）。4 MB 正文经 `json.MarshalIndent`（`mcp.go:303`）转义后可膨胀到 6–8 MB 以上，一旦越界，`emitter.write` 写失败只会把日志打到 stderr（`sdk.go:241`），**宿主永远收不到响应，工具调用挂到 120 s 超时**。建议把上限收敛到 1 MB 并显式报错。

### B3【P2】错误信息回显上游响应体

`nacos.go:129 / 249 / 287 / 319 / 372` 都做了 `truncate(string(body), 200)`，把 Nacos 的原样响应体拼进错误信息并交给模型。多数是 JSON 报错，但 5xx 时可能带堆栈、内部路径等运维信息，属于不必要的信息外泄面。建议默认只保留状态码，把响应体收敛到 stderr 日志。

### B4【P2】凭据常驻内存、生命周期长于连接

`p.connections`（`main.go:34`、`main.go:105-107`）保存明文密码快照，仅在 `connection/disconnect` 时 `delete`。Go 的字符串不可覆写，删除后旧值仍在堆上存活至 GC；sidecar 是常驻进程。README「凭据不落盘、仅内存保留快照」的表述属实，但建议：

- `disconnect` 时把记录字段置空再删除（缩短存活窗口）；
- 文档里明确"凭据存活于 sidecar 进程内存，进程退出即释放"；
- 不要为落地评测/复现把这个快照写进日志（当前代码没有，保持住）。

另注：`handlePing`（`main.go:134`）是纯健康探测，却也会带上用户名密码，并在 401 时触发一次登录 —— 无安全损失，但属多余凭据暴露，去掉更干净。

### B5【P1-低】`newNacosClient` 的 host 解析歧义

`nacos.go:60-94`：只剥离 `http(s)://` 前缀，**不识别 host 里带的路径**。用户在"服务地址"里填 `http://nacos.example.com:8848/nacos`（很常见的复制粘贴结果）时，`contextPath` 默认为 `/nacos` 再拼一次 → `http://nacos.example.com:8848/nacos/nacos`，请求 404，且报错信息不会指出真正原因。

`nacos.go:76` 的 `!strings.Contains(host, ":")` 还会让 IPv6 地址 `[::1]` 跳过端口拼接，连到错误端口。

建议：解析 host 时用 `net/url` 提取 path 作为 contextPath 兜底，并对 `host` 中出现 `/` 的情况给出明确提示；端口判断改用 `strings.Contains(host, "]")` 之外的更严谨方式（如 `net.SplitHostPort`）。

### B6【P3】`truncate` 按字节切分

`nacos.go:412-418` 用 `value[:limit]` 切字符串，会切断 UTF-8 多字节字符，中文报错信息出现乱码。先转 `[]rune` 再截即可。

### B7 已确认安全的项

- **UI 无 XSS**：`ui/index.html` 全程 `textContent` / `createElement`，未使用 `innerHTML`（第 136-152 行、168 行）。工具名、描述、错误信息注入路径都安全。
- **并发有保护**：`plugin.mu` 保护 `connections`（`main.go:105-122`），`nacosClient.mu` 保护 token（`nacos.go:104-152`），SDK 每个请求一个 goroutine，无数据竞争。
- **不信任模型输入**：工具参数全部经 `stringArg`/`intArg` 归一化，`pageSize` 有上限、`dataId` 空值有校验。
- **无 SQL/命令注入面**：纯 HTTP 客户端，构造 URL 走 `url.Values` 编码。

---

## C. 性能问题

### C1【P2】每次工具调用都重新登录 + 新建 HTTP 客户端

**位置**：`backend/mcp.go:249-297`（`sessionFor` 第 290 行 `newNacosClient(...)`）

`sessionFor` 每次调用都新建 `nacosClient`，而 token 缓存挂在 client 实例上（`nacos.go:52-54`）→ **token 缓存完全失效**。同时 `http.Client` 也是新的（`nacos.go:92`）→ 连接池无法复用，每次调用都要重建 TCP（https 还要握手）。

**实测**（临时探针，3 次 `get_config`）

```text
logins after connection/connect: 0; logins after 3 tool calls: 3
```

AI 一轮对话常连调 5–10 次工具，即 5–10 次多余登录 RTT + 同量的连接重建（跨网络时每次几十毫秒起）。

**修复思路**：在 `plugin` 上缓存会话，key 用 connection id，值为 `{client, namespace, name, credentialFingerprint}`；`connection/disconnect` 时删除；凭据指纹变化（payload 里 password/username 与缓存不一致）时重建。缓存后注意 `ensureToken`（`nacos.go:104-105`）持锁发起 HTTP 调用的窗口会阻塞同一 client 的并发调用 —— 可接受，或用 singleflight 收敛。

### C2【P3】`pageSize` 超限被静默降级为 20，而不是钳到 200

**位置**：`nacos.go:259-261`、`nacos.go:354-356`

```go
if pageSize <= 0 || pageSize > 200 { pageSize = 20 }
```

AI 传 `pageSize: 500` 想看全部数据，实际只拿到 20 条且无任何提示，接着可能得出"这个环境只有 20 个配置"的错误结论。建议：

```go
if pageSize <= 0 { pageSize = 20 } else if pageSize > 200 { pageSize = 200 }
```

### C3 其余性能项（可不改）

- `list_namespaces` 走 Nacos 无分页接口，全量拉取属接口限制。
- `search_configs` 用 struct 解码天然丢弃 `content` 字段（`nacos.go:214-221`，有回归测试 `TestSearchConfigsOmitsContentFromUpstream` 覆盖）✓，但服务端仍会传输这些字节；从请求侧无法控制，注释也已说明，属已知代价。
- `resp.Body.Close()` 在所有路径都有 `defer` ✓，无连接泄漏。
- `io.ReadAll` + `MarshalIndent` 对 4 MB 正文会有数倍内存放大，收敛上限（见 B2）后自然缓解。

---

## D. 其他

- `.gitignore` 建议补 `*.dbxp`、`.idea/`、`.vscode/`（`dist/` 已覆盖打包产物）。
- `nacos.go:139-141`：服务端返回 `tokenTtl <= 60s`（含 0）时直接按 1 小时处理，token 提前失效靠 401 重试兜底 —— 行为可接受，但会多一次失败请求，值得加注释说明是故意为之。
- `nacos.go:161-164`：`get` 在收到 403 时会无条件重登一次。若 403 来自 ACL（而非 token 过期，如测试里的 `hidden.yaml`），会白白多一次登录 RTT —— 可只在 `invalidateToken` 后重试 401，或在 403 时先判断响应体是否 `user not found`。

---

## 建议的修复顺序

1. `git rm --cached backend/dbx-nacos-toolkit` + `.gitignore`（P0，5 分钟，需要时再决定是否重写历史）
2. 修 token 泄漏（B1，P1，改动约 10 行，必须做）
3. 修 host/contextPath 解析（B5，直接决定"用户能不能连上"）
4. 截断改为显式报错 + 上限收敛（B2）
5. 会话缓存（C1）、pageSize 钳制（C2）
6. 细节：`truncate` 转 rune、补 LICENSE、CI pin SHA

---

## 修复记录（2026-10-16 同日）

| 编号 | 状态 | 改动 |
| --- | --- | --- |
| A1 | ✅ | `git rm --cached backend/dbx-nacos-toolkit`，本地二进制一并删除；`.gitignore` 增加 `/backend/dbx-nacos-toolkit`、`/backend/dbx-plugin-nacos-toolkit`、`*.dbxp`、`.idea/`、`.vscode/`。历史中的 8 MB blob 仍在（待决定是否重写） |
| A3 | ✅ | 新增 `LICENSE`（Apache-2.0 全文，取自 apache.org） |
| A4 | ⚠️ 部分 | workflow 内加入 pin SHA 的强制说明与 TODO；**无法自动 pin** —— 上游 ref `plugin-sdk-v1` 在公开仓库不存在（无同名分支/tag，main 上也没有该 reusable workflow），需你提供正确的 ref 或 SHA |
| B1 | ✅ | 新增 `transportError()`：`errors.As` 拆掉 `*url.Error` 外壳，错误文本不再携带含 `accessToken` 的 URL；登录路径同步替换 |
| B2 | ✅ | 新增 `readBounded()`（多读 1 字节判定截断），超限显式报错；上限 4 MiB → 2 MiB；`toolJSON` 增加 6 MiB 结果上限，避免 SDK 8 MiB 帧限制导致宿主收不到响应而超时 |
| B3 | ✅ | 错误信息只保留 HTTP 状态码；响应正文改由 `logUpstream()` 写入 stderr（插件日志），不进模型上下文；登录响应正文任何情况下都不外泄 |
| B4 | ✅ | `disconnect` 同时清空 `connections` 与 `sessions`；`nacos/ping` 改为不带凭据探测；README 说明凭据生命周期与 Go 字符串无法清零的事实 |
| B5 | ✅ | `newNacosClient` 改用 `net/url` 解析：支持 `https://host:8848/nacos`、`host/nacos` 裸写法、IPv6 `[::1]`（`net.SplitHostPort` 判端口 + `net.JoinHostPort` 补括号），拒绝非 http(s) scheme |
| B6 | ✅ | `truncate` 改为按 rune 截断 |
| C1 | ✅ | `plugin.sessions` 缓存每连接客户端（连接指纹 sha256 作 key，字段变化即失效，容量 64 后整体丢弃）；实测 3 次调用 1 次登录 |
| C2 | ✅ | 新增 `normalizePaging()`：`pageSize > 200` 钳到 200（原为静默重置 20） |
| C3 备注 | ✅ | `get()` 的 401/403 重试保留并加注释说明取舍（ACL 拒绝时会多付一次登录 RTT） |
| 其他 | ✅ | `tokenTtl` 回退逻辑加注释；测试 14 → 22 个，覆盖 token 泄漏、截断拒绝、上游正文不外泄、分页钳制、会话复用与失效、地址解析、rune 安全 |

验证：`go vet` 无告警、`gofmt` 无差异、`go test ./...` 22/22 通过、`scripts/smoke.py` 全链路通过。

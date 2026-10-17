# Nacos 1.x/2.x「v1 接口」用法核对报告 — dbx_plugin_nacos

- 核对对象：插件 v1 协议栈（`backend/v1.go`，对应改动前 `backend/nacos.go`）
- 核对方法：**Nacos 2.4.3 tag 全量源码逐文件核对**（`/tmp/nacos-src/nacos-2.4.3`，官方 codeload 源码包）＋ 本地 2.4.3 实例（容器 `dbx-nacos-e2e`，`127.0.0.1:18848`）实测交叉验证
- 时间：2026-10-17
- 结论：**7 个读取能力 + 登录共 8 条调用全部用法正确，未发现参数/路径错误**；沉淀 4 项优化（1 项行为修正 + 3 项增强），均已随 v0.2.0 落实

---

## 一、逐接口核对表

| # | 插件调用 | 2.4.3 源码依据 | 判定 |
| --- | --- | --- | --- |
| 1 | `GET /v1/cs/configs?dataId&group&tenant`（读配置） | `ConfigController.getConfig`：`@RequestParam("dataId")` / `("group")` 必填，`tenant` optional 默认空串；`NamespaceUtil.processNamespaceParameter` 把空白转 `public`；返回原始文本，不存在时 404 | ✅ 正确；404 → `config not found` 的映射与源码语义一致 |
| 2 | `GET /v1/cs/configs?search=blur&dataId&group&tenant&pageNo&pageSize`（搜索） | `ConfigController.fuzzySearchConfig`（`@GetMapping(params = "search=blur")`）：dataId/group 必填，`*` 为通配符；返回 `Page<ConfigInfo>`（**含 content**）；分页参数无服务端硬上限 | ✅ 正确；插件结构体不含 content 字段，天然丢弃正文（防大响应） |
| 3 | `GET /v1/cs/history?search=accurate&…`（历史列表） | `HistoryController.listConfigHistory`：`@GetMapping(params = "search=accurate")` | ✅ 正确 |
| 4 | `GET /v1/cs/history?dataId&group&tenant&nid=<id>`（历史详情，**不带 search**） | 同一路径的详情方法靠参数区分：带 `search=accurate` 必走列表分支（`nid` 被忽略）；只有不带 search 才命中详情 | ✅ 正确（v0.1.1 已按此修复，并有防回归单测） |
| 5 | `GET /v1/ns/service/list?pageNo&pageSize&namespaceId&groupName`（服务列表） | `ServiceController.list`（L162-178）：`pageNo`/`pageSize` 必填，`namespaceId` 默认 public，`groupName` 可选（默认 DEFAULT_GROUP，按组过滤）；返回 `{count, doms[]}` | ✅ 正确 |
| 6 | `GET /v1/ns/instance/list?serviceName&namespaceId&groupName`（实例列表） | `InstanceController.list`（L322-342）：读取 `serviceName`/`clusters`/`clientIP`/`udpPort`/`healthyOnly`/`app` —— **控制器本身不读 `groupName`**；组名由 `naming/web/ServiceNameFilter`（L44-83）在过滤器中合并进 serviceName（`"order-service"` → `"BETA_GROUP@@order-service"`，未带 `@@` 时使用 groupName，缺省 DEFAULT_GROUP） | ✅ 正确（见第二节实测：曾疑似参数 bug，双重排除） |
| 7 | `GET /v1/console/namespaces`（命名空间列表） | `console/NamespaceController`（L71-74）：`RestResult<List<Namespace>>`，字段 `namespace/namespaceShowName/configCount/quota/type` | ✅ 正确 |
| 8 | `POST /v1/auth/login`（登录） | 2.x 原生登录端点；`accessToken` 查询参数附带；401/403 表示凭据错误 | ✅ 正确 |

> 补充核对（历史详情响应）：2.4.x 的 `ConfigHistoryInfo.createdTime` 在 derby 存储下是 2010 占位值、真实变更时间在 `lastModifiedTime`；`opType` 为定长字段带尾部空格。插件已做 `TrimSpace` + 时间优先取 `lastModifiedTime`（v0.1.1 修复，防回归单测覆盖）。

---

## 二、实测交叉验证（2.4.3 容器）

### 2.1 实例列表的组名语义（曾疑似 bug，实测排除）

受控实验：在 `BETA_GROUP` 注册 `10.99.7.7:8080` 后查询：

| 调用方式 | 返回 |
| --- | --- |
| `serviceName=order-service&groupName=BETA_GROUP`（插件当前方式） | ✅ 命中 `name=BETA_GROUP@@order-service`，hosts=[10.99.7.7] |
| `serviceName=BETA_GROUP@@order-service`（手工拼接方式） | ✅ 命中同一实例 |

→ 结论：`ServiceNameFilter` 会把 groupName 合并进 serviceName，**插件现有两参数传法是标准调用方式**，非默认组亦正确过滤。

### 2.2 分页上限实测

| 接口 | pageSize=500 | 501 | 1000 |
| --- | --- | --- | --- |
| `/v1/cs/configs`（blur 搜索） | 200 OK | 200 OK | 200 OK |
| `/v1/cs/history` | 200 OK（12 条全量） | — | 200 OK |
| `/v1/ns/service/list` | 200 OK | — | 200 OK |

→ v1 服务端对 pageSize 无硬校验；插件此前自设 200 上限偏保守。

### 2.3 健康端点形态

`/v1/console/health/readiness` 在 2.4.x 返回**纯文本 `OK`**（早期 2.x 版本返回 `{"status":"UP"}` JSON）；1.x 无 readiness，仅有 `/v1/console/server/state`。插件两种形态都兼容并有 fallback。

---

## 三、优化项与落实情况

| # | 优化 | 类型 | 状态 |
| --- | --- | --- | --- |
| 1 | **`valid` 字段诚实化**：实测 2.4.3 与 3.2.4 的实例响应**都没有 `valid` 字段**；旧实现用 `bool` 零值无条件输出 `"valid": false`（伪造数据）。改为指针语义——仅服务端真的提供时才输出 | 行为修正 | ✅ v0.2.0 |
| 2 | **分页上限 200 → 500**：与 Nacos 3.x console 历史接口的服务端上限（`Math.min(500, pageSize)`）对齐，同时实测 v1 接受更大值不报错；工具 schema 同步更新为 1-500 | 增强 | ✅ v0.2.0 |
| 3 | **`diff_config` 并行读取**：2–8 个命名空间的配置读取由串行改为并发（错误按命名空间顺序取首个，保持信息稳定），远端服务器下把 n 次往返压缩为 1 次往返等待 | 性能 | ✅ v0.2.0 |
| 4 | **`list_instances` 新增 `cluster` 过滤**：v1 传 `clusters`、v3 传 `clusterName`（两栈同参） | 增强 | ✅ v0.2.0 |
| 5 | 备选未实施：`search_configs` 开放 `appName` / `config_tags` / `types` 过滤器（v1 与 v3 均支持，工具暂未开放）；`healthyOnly` 仅 v1 支持（v3 控制器硬编码 `false`），为保持双栈一致未开放 | 备注 | 记录 |

---

## 四、核对所用源码文件（tag 2.4.3）

```text
config/src/main/java/com/alibaba/nacos/config/server/controller/ConfigController.java
config/src/main/java/com/alibaba/nacos/config/server/controller/HistoryController.java
naming/src/main/java/com/alibaba/nacos/naming/controllers/ServiceController.java
naming/src/main/java/com/alibaba/nacos/naming/controllers/InstanceController.java
naming/src/main/java/com/alibaba/nacos/naming/core/InstanceOperatorClientImpl.java
naming/src/main/java/com/alibaba/nacos/naming/web/ServiceNameFilter.java
console/src/main/java/com/alibaba/nacos/console/controller/NamespaceController.java
common/src/main/java/com/alibaba/nacos/common/utils/NamespaceUtil.java
```

复现：

```bash
curl -sL -o nacos-2.4.3.tgz https://codeload.github.com/alibaba/nacos/tar.gz/refs/tags/2.4.3 && tar xzf nacos-2.4.3.tgz

# 实测（容器在跑时）
curl "http://127.0.0.1:18848/nacos/v1/ns/instance/list?serviceName=order-service&namespaceId=dev&groupName=DEFAULT_GROUP"
```

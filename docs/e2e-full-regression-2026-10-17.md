# Docker 全量端到端回归验证报告 — dbx_plugin_nacos

- 验证对象：`Ct17/dbx-nacos-toolkit` v0.2.0（基于 `8205074` 的本地改动，含 v3 协议栈）
- 验证时间：2026-10-17（第二轮，全面回归 + 审查驱动补测）
- 验证方式：复用既有 4 容器矩阵，Docker 启动真实 Nacos，`scripts/e2e_docker.py` 驱动 sidecar 二进制走 DBX stdio 协议（JSON-RPC 2.0）逐项断言；单测与协议冒烟先行
- 验证结果：**全部通过，无新增缺陷**——单测 56/56、协议冒烟 14 项、v1 链路 E2E 46/46、v3 原生链路 E2E 46/46、v3 幂等复跑 43/43（补测前口径）

> 检查项口径演进：首轮 v1 回归为 42 项（该轮报告未随仓库发布）；v0.2.0 起阶段 1 新增「connection/test 报告协议栈」检查变为 43 项；本轮经代码审查发现两处验证缺口（连接表单的 `api_version` 手动覆盖、`list_instances` 的 `cluster` 过滤参数——均为 manifest/README 已声明功能但零用例覆盖），在 `scripts/e2e_docker.py` 补齐 3 项检查后矩阵为 **46 项**。

---

## 一、测试环境

| 容器 | 镜像 | 端口映射 | 鉴权 | 用途 |
| --- | --- | --- | --- | --- |
| `dbx-nacos-e2e` | `nacos/nacos-server:v2.4.3` | `18848→8848` | 关闭 | v1 主链路（1.x/2.x Open API） |
| `dbx-nacos-e2e-auth` | `nacos/nacos-server:v2.4.3` | `18948→8848` | 开启 | v1 鉴权链路（accessToken） |
| `dbx-nacos3-pure` | `nacos/nacos-server:v3.2.4` | `18868→8848` / `18088→8080` | 关闭 | v3 原生链路（client + console API，无 adapter） |
| `dbx-nacos3-pure-auth` | `nacos/nacos-server:v3.2.4` | `18968→8848` / `18098→8080` | 开启 | v3 鉴权链路（Bearer） |

容器**首次创建命令**（鉴权实例需要的环境变量一并给出；`NACOS_AUTH_TOKEN` 需自行生成，Docker 版 Nacos 启动时校验其解码后长度 ≥32 字节）：

```bash
TOKEN=$(openssl rand -base64 48)

# v1 主链路 / v1 鉴权链路
docker run -d --name dbx-nacos-e2e -p 18848:8848 \
  -e MODE=standalone -e NACOS_AUTH_ENABLE=false nacos/nacos-server:v2.4.3
docker run -d --name dbx-nacos-e2e-auth -p 18948:8848 \
  -e MODE=standalone -e NACOS_AUTH_ENABLE=true \
  -e NACOS_AUTH_TOKEN="$TOKEN" \
  -e NACOS_AUTH_IDENTITY_KEY=serverIdentity -e NACOS_AUTH_IDENTITY_VALUE=security \
  nacos/nacos-server:v2.4.3

# v3 原生链路 / v3 鉴权链路（console 端口需一并映射）
docker run -d --name dbx-nacos3-pure -p 18868:8848 -p 18088:8080 \
  -e MODE=standalone -e NACOS_AUTH_ENABLE=false \
  -e NACOS_AUTH_TOKEN="$TOKEN" nacos/nacos-server:v3.2.4
docker run -d --name dbx-nacos3-pure-auth -p 18968:8848 -p 18098:8080 \
  -e MODE=standalone -e NACOS_AUTH_ENABLE=true \
  -e NACOS_AUTH_TOKEN="$TOKEN" \
  -e NACOS_AUTH_IDENTITY_KEY=serverIdentity -e NACOS_AUTH_IDENTITY_VALUE=security \
  nacos/nacos-server:v3.2.4
```

以下命令块假设容器已创建，仅负责启停。

```bash
# 启动（复用既有容器，数据跨轮保留；脚本灌数幂等）
docker start dbx-nacos-e2e dbx-nacos-e2e-auth dbx-nacos3-pure dbx-nacos3-pure-auth

# v1 全量 E2E（含鉴权实例）
python3 scripts/e2e_docker.py --port 18848 --auth-port 18948

# v3 原生全量 E2E（含鉴权实例）
python3 scripts/e2e_docker.py --port 18868 --console-port 18088 \
  --auth-port 18968 --auth-console-port 18098

# 验证完成后停止容器
docker stop dbx-nacos-e2e dbx-nacos-e2e-auth dbx-nacos3-pure dbx-nacos3-pure-auth
```

---

## 二、验证结果总览

| 套件 | 规模 | 结果 |
| --- | --- | --- |
| Go 单元测试（`backend`，httptest 模拟 v1/v3 双栈） | 56 项 | **56/56 通过**（0 失败） |
| 协议冒烟（`scripts/smoke.py`，mock Nacos） | 14 项 | **全部通过** |
| v1 链路 E2E（2.4.3，无鉴权 + 鉴权） | 46 项 | **46/46 通过** |
| v3 原生链路 E2E（3.2.4，无鉴权 + 鉴权） | 46 项 | **46/46 通过** |
| v3 原生链路幂等复跑（补测前 43 项口径） | 43 项 | **43/43 通过** |

### E2E 覆盖矩阵（每条链路 4 阶段 46 项）

| 阶段 | 项数 | 内容 |
| --- | --- | --- |
| 1 连接生命周期 | 10 | `plugin/initialize` 协商、插件 id 一致性、`connection/test`（连通 + 协议栈报告 + 不可达端口报错 + **显式 `api_version` 覆盖自动探测**）、`connection/connect`、`mcp/tools`（7 工具 + readOnlyHint）、`nacos/ping`（api 字段） |
| 2 七个只读工具 | 20 | `get_config`（读取 / namespace 覆盖）、`search_configs`（全量 / 关键词过滤无内容泄漏 / 分页）、`diff_config`（identical / different 行级 diff / missing-in-target）、`get_config_history`（列表 ≥2 条 / 字段完整性 / opType 枚举 / 非 2010 占位时间 / historyId 详情）、`list_namespaces`（含 configCount=3）、`list_services`、`list_instances`（实例数 / 健康数 / 字段完整性 / **cluster 过滤命中 DEFAULT 集群 / cluster 过滤无命中返回 0 实例**） |
| 3 错误路径与边界 | 8 | 配置不存在、缺 dataId、diff 少于两命名空间、缺 serviceName、未知工具、无连接调用、未知方法 -32601、disconnect |
| 4 鉴权链路 | 8 | 前置灌数、正确凭据（test/connect/get_config/list_namespaces）、错误密码拒绝、无凭据探测拦截、无凭据 403 |

### 协议栈自动探测确认

- v1 链路：`connection/test` 报告 `API v1`，`nacos/ping.api == "v1"`
- v3 链路：`connection/test` 报告 `API v3`，`nacos/ping.api == "v3"`（console 健康端点探测命中，未安装 adapter）
- 显式 `api_version=v1` / `=v3` 覆盖：跳过自动探测直连指定协议栈，连接测试与协议栈报告均正确（自动探测选错时的恢复手段可用）

---

## 三、结论

1. **v0.2.0 工作区（含 v3 协议栈）在两条协议链路上均全功能可用**：与 `docs/e2e-nacos3-native-2026-10-17.md` 的首轮验证结果一致，本轮回归未发现退化。
2. **连接表单声明功能全量覆盖**：`api_version` 手动覆盖与 `list_instances` 的 `cluster` 过滤（v1 走 `clusters`、v3 走 `clusterName`）经本轮补测验证通过，README 功能面无剩余未验证项。
3. **测试数据幂等性确认**：v3 实例重复运行 E2E（复用容器内既有数据）依旧全部通过，脚本灌数与断言可安全重复执行。
4. **鉴权链路双栈确认**：v1（accessToken 查询参数 + token 复用）与 v3（Bearer 头 + console 强制登录）的登录、token 复用、错误凭据、无凭据拦截全部符合预期；E2E 日志中的 403 输出为错误路径测试的预期产物（sidecar 日志直写 stderr）。
5. 本轮未修改任何产品代码（`backend/` 零改动）；仅扩展了 E2E 脚本检查矩阵（+3 项）并新增本文档。

---

## 四、遗留观察（非缺陷，供后续参考）

- sidecar 在错误路径测试中向 stderr 打印上游 404/403 概要（`[nacos-toolkit] GET … -> HTTP 403: …`），属设计内行为（上游响应写入插件日志、不进模型上下文），但 E2E 输出会混入少量噪声，可考虑在脚本侧过滤或加 `--quiet` 开关。
- `dbx-nacos3-e2e` / `dbx-nacos3-auth` 两个旧容器（3.2.4 adapter 调查期遗留）已停止且未被本轮使用，可在不需要时 `docker rm` 清理。
- 首轮 43 项口径下的幂等复跑未在补测后按 46 项重跑第三遍（补测项本身在两条链路各自通过且幂等性已由前序运行证明）；如需可再执行一轮 v3 全量复跑。

---

## 五、本轮变更文件

| 文件 | 变更 |
| --- | --- |
| `scripts/e2e_docker.py` | 阶段 1 新增「显式 `api_version` 覆盖」检查；阶段 2 新增「cluster 过滤命中 / 无命中」2 项检查（矩阵 43 → 46） |
| `docs/e2e-full-regression-2026-10-17.md` | 新增：本文档（全量回归验证报告） |

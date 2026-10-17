# Nacos 3.x 原生 v3 适配与验证报告（无需 adapter）— dbx_plugin_nacos

- 目标：**不安装 legacy adapter**，让插件在纯 Nacos 3.x 上全功能可用
- 结果：**43/43 项端到端检查全部通过 × 3 遍**（双实例：无鉴权 + 鉴权），2.4.3 回归 43/43，单测 56 项、协议冒烟全绿
- 交付版本：v0.2.0（`manifest.json` / `backend/main.go` 同步）
- 时间：2026-10-17

---

## 一、测试环境

| 容器 | 端口 | 说明 |
| --- | --- | --- |
| `dbx-nacos3-pure` | `18868`（server）/ `18088`（console） | **纯 Nacos 3.2.4**，无 adapter、无兼容开关、无鉴权 |
| `dbx-nacos3-pure-auth` | `18968` / `18098` | 同上 + `NACOS_AUTH_ENABLE=true` |
| `dbx-nacos-e2e` / `-auth` | `18848` / `18948` | 2.4.3 回归对照 |

```bash
# 纯 3.2.4（无 adapter）启动方式
docker run -d --name dbx-nacos3-pure -p 18868:8848 -p 18088:8080 \
  -e MODE=standalone -e NACOS_AUTH_ENABLE=false \
  -e NACOS_AUTH_TOKEN=<Base64 ≥32 字节> \
  nacos/nacos-server:v3.2.4

# 端到端（含鉴权实例）
python3 scripts/e2e_docker.py --port 18868 --console-port 18088 \
  --auth-port 18968 --auth-console-port 18098
# 2.4.3 回归
python3 scripts/e2e_docker.py --port 18848 --auth-port 18948
```

---

## 二、实现要点（均由 3.2.4 源码 + 实测双重确认）

### 2.1 协议栈自动探测

连接时先探测 **v3 控制台健康端点**（`GET {console}/v3/console/health/readiness` → `{"code":0,"data":"ok"}`，匿名可达），命中即用 v3；否则回退 v1 健康端点（1.x/2.x，以及装了 adapter 的 3.x）。连接表单可显式指定 `api_version`（auto/v1/v3）覆盖探测。

### 2.2 端点映射（7 个能力）

| 能力 | v3 端点 | 端口 | 鉴权 |
| --- | --- | --- | --- |
| 健康检查 | `GET /v3/console/health/readiness` | console | 匿名 |
| 读配置 / diff | `GET /v3/client/cs/config?dataId&groupName&namespaceId` | server | 关闭鉴权时匿名 |
| 搜索配置 | `GET /v3/console/cs/config/list?search=blur&…` | console | **强制 Bearer** |
| 历史列表 | `GET /v3/console/cs/history/list?…` | console | **强制 Bearer** |
| 历史详情 | `GET /v3/console/cs/history?nid=&dataId=&groupName=` | console | **强制 Bearer** |
| 命名空间 | `GET /v3/console/core/namespace/list` | console | **强制 Bearer** |
| 服务列表 | `GET /v3/console/ns/service/list?groupNameParam=…` | console | **强制 Bearer** |
| 实例列表 | `GET /v3/client/ns/instance/list?serviceName&groupName` | server | 关闭鉴权时匿名 |
| 登录 | `POST /v3/auth/user/login` | server + console 均可 | 匿名 |

### 2.3 实测得出的关键行为（决定了实现细节）

1. **console 端口独立、无 context path**：v3 console 是独立应用（默认 8080，与 8848 分离），路径不带 `/nacos` 前缀；
2. **console API 与鉴权开关解耦**：`NACOS_AUTH_ENABLE=false` 时 console 接口（除 health）仍返回 403，必须登录；无鉴权实例需先 `POST /v3/auth/user/admin` 初始化管理员；
3. **统一响应封装 `Result<T>`**：成功 `code 0`；鉴权失败 `code 10001`；资源不存在 `code 20004`（HTTP 200）。**失败态 `data` 形状与成功态不同（如字符串）**——因此解码分两段：先解封装头部，成功后再按类型解 `data`；
4. **历史时间字段**：列表的 `createTime` 在 derby 下是 2010 占位值，真实变更时间在 `modifyTime`（映射为展示时间的优先来源）；`opType` 仍为定长带空格；
5. **blur 搜素语义**：`*` 为通配符，未带通配符的关键词不会自动做包含匹配——插件沿用 v1 策略把关键词包裹为 `*kw*`；
6. **命名空间 id**：默认命名空间为 `"public"`（v1 为空串，插件输出层兼容两者）；
7. **实例语义差异（官方）**：v3 的 client 实例列表**不返回 `enabled=false` 的实例**；`valid` 字段 v1/v3 实测均不存在（工具已改为仅在上游提供时输出）。

### 2.4 连接配置（Nacos 3.x）

- **Console 端口**：默认 8080 自动探测；Docker 映射改变时需填写实际端口；
- **用户名/密码**：console 能力必需（无鉴权实例先初始化管理员）；仅用 client 类工具（get_config/diff/list_instances）可匿名。

---

## 三、验证结果

| 套件 | 结果 |
| --- | --- |
| 纯 3.2.4 全量 E2E（含鉴权实例） | **43/43 × 3 遍**（幂等） |
| 2.4.3 回归 E2E | **43/43 × 2 遍** |
| Go 单测（含 11 个 v3 新增用例 + 探测/回退用例） | **56/56** |
| `scripts/smoke.py` 协议冒烟 | 通过 |

覆盖矩阵：连接生命周期 9 项 → 七工具 18 项 → 错误路径 8 项 → 鉴权链路 8 项。v3 新增专项断言：协议栈报告（`ping.api == "v3"`）、console 凭据拦截与错误密码提示、403/20004 错误翻译、历史时间取真实值、v3 实例输出不含伪造 `valid`。

---

## 四、与 adapter 方案的关系

- v0.2.0 起 **3.2+ 无需任何服务端改动**；已部署 legacy adapter 的环境不受影响（探测优先 v3，行为与本次验证一致）；
- 此前 adapter 路线的调查记录已归档，不随仓库发布；v3 端点清单以本文第三节为准。

## 五、变更清单（v0.2.0）

- `backend/nacos.go`：基础设施重构（协议探测 / 双栈鉴权 / 传输层）；`backend/v1.go`、`backend/v3.go` 新增协议栈实现
- `backend/mcp.go`、`backend/main.go`：连接字段（`console_port`/`api_version`）、`list_instances.cluster`、`ping.api`、`valid` 字段诚实化、diff 并行读取、v3 连接测试引导
- `manifest.json`：连接表单新增 Console 端口 / API 版本；版本号 0.1.1 → 0.2.0
- `scripts/e2e_docker.py`：v3 模式（console 数据准备 + Bearer + 心跳）；`scripts/smoke.py`：固定 v1
- `README.md`：3.x 支持章节重写

# v0.3 Docker E2E 验证报告（2026-10-17）

> 状态：**现行**。验证对象：v0.3.0 工作区（F1–F4 四个只读能力）。
> 结论：Nacos 2.4.3（v1）与 Nacos 3.2.4（原生 v3）**各 55/55 项检查全部通过**，含无鉴权与鉴权实例。

---

## 1. 环境矩阵

| 链路 | 服务端 | 容器 | 端口 | 鉴权 |
| --- | --- | --- | --- | --- |
| v1 基础 | Nacos 2.4.3 standalone | `dbx-nacos-e2e` | 18848 | 关闭 |
| v1 鉴权 | Nacos 2.4.3 standalone | `dbx-nacos-e2e-auth` | 18948 | 开启 |
| v3 基础 | Nacos 3.2.4 standalone（无 adapter） | `dbx-nacos3-pure` | 18868 / console 18088 | 关闭 |
| v3 鉴权 | Nacos 3.2.4 standalone（无 adapter） | `dbx-nacos3-pure-auth` | 18968 / console 18098 | 开启 |

- 插件构建方式：`go build` 后由 `scripts/e2e_docker.py` 以 DBX sidecar 协议驱动。
- 测试数据：`prepare_fixtures` / `prepare_fixtures_v3` 幂等写入 dev/test/prod、配置历史、服务实例与心跳。
- 两轮均覆盖 `mcp/tools` 11 个工具目录、四类 v0.3 新能力、原有七工具、错误路径与鉴权链路。

---

## 2. 结果

| 套件 | 检查数 | 结果 |
| --- | --- | --- |
| v1 基础 + 鉴权 | 55 | ✅ 55/55 |
| v3 基础 + 鉴权 | 55 | ✅ 55/55 |

v0.3 新增断言（两条链路各 9 项）：

- `diff_history：历史版本与当前版本对比 -> different`
- `find_in_configs：按正文关键词命中 gateway.yaml`
- `find_in_configs：返回扫描覆盖信息`
- `diff_envs：test 与 dev 的漂移统计正确`
- `diff_envs：prod 与 dev 的漂移统计正确`
- `diff_envs：changed 同时包含 different / only-in-baseline`
- `diff_envs：includeContentDiff 生成 gateway 行级 diff`
- `diagnose：返回协议/版本/检查项`
- `diagnose：健康与数据面检查通过`

原有 46 项（7 工具、连接生命周期、错误路径、鉴权、cluster 过滤等）保持通过。

---

## 3. 首轮失败与修正

首轮 v1 出现 5 项失败、v3 出现 2 项失败，均为真实语义差异，已修正：

| # | 现象 | 根因 | 修正 |
| --- | --- | --- | --- |
| 1 | `diff_envs` 把 test/prod 全部判为 identical | Nacos 2.4.3 的 blur 搜索响应 `md5` 为 `null`（v3 正常返回） | `loadConfigFingerprints` 在 md5 缺失时抓正文计算 MD5（并发 8）；有 md5 的 v3 不产生额外请求 |
| 2 | v3 `find_in_configs` 命中 0 条 | Nacos 3.x `searchDetail` 的 `configDetail` 是 LIKE 模式，必须带 `*` 通配符（如 `*port*`） | `v3SearchConfigsByContent` 用 `blurPattern(keyword)` 包装 |
| 3 | v1/v3 `diff_history` 期望 different 实际 identical | 重复运行后历史列表末尾的版本可能仍是当前内容（成对更新） | E2E 改为从历史中找一条正文与当前不同的版本再断言；产品逻辑无缺陷 |

修正后两条链路均 55/55。

---

## 4. 复跑命令

```bash
# 启动/复用容器（示例，端口与本文一致）
docker start dbx-nacos-e2e dbx-nacos-e2e-auth dbx-nacos3-pure dbx-nacos3-pure-auth

# v1（2.4.3）：无鉴权 + 鉴权
python3 scripts/e2e_docker.py --port 18848 --auth-port 18948

# v3（3.2.4 原生 v3）：无鉴权 + 鉴权
python3 scripts/e2e_docker.py --port 18868 --console-port 18088 \
  --auth-port 18968 --auth-console-port 18098
```

脚本退出码 0 表示全部通过；数据准备幂等，可重复运行。

---

## 5. 备注

- 容器为本地既有测试环境，启动后复用；测试结束未删除容器，便于复跑。
- 本轮同时通过：`go test ./...`（78 个）、`go test -race`、`scripts/smoke.py`（19 项）。
- 实现与边界详见 [`v0.3-implementation-2026-10-17.md`](v0.3-implementation-2026-10-17.md)；功能规划见 [`feature-roadmap-2026-10-17.md`](feature-roadmap-2026-10-17.md)。

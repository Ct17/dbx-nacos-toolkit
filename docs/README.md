# 文档索引

本目录存放开发过程记录。**结论以「状态」列为准**，标为「已被取代」的材料已移出仓库。

| 文档 | 日期 | 内容 | 状态 |
| --- | --- | --- | --- |
| [code-review-2026-10-16.md](code-review-2026-10-16.md) | 2026-10-16 | 代码评审（P0–P3 问题清单与修复结论） | 现行 |
| [e2e-full-regression-2026-10-17.md](e2e-full-regression-2026-10-17.md) | 2026-10-17 | Docker 全量回归：4 容器矩阵、46 项检查、环境重建命令 | 已被 v0.3 口径（55 项）取代 |
| [e2e-nacos3-native-2026-10-17.md](e2e-nacos3-native-2026-10-17.md) | 2026-10-17 | Nacos 3.x 原生 v3 适配：端点清单、鉴权行为、验证结果 | **现行** |
| [v1-api-verification-2026-10-17.md](v1-api-verification-2026-10-17.md) | 2026-10-17 | v1 接口逐条核对（Nacos 2.4.3 源码级） | 现行 |
| [feature-roadmap-2026-10-17.md](feature-roadmap-2026-10-17.md) | 2026-10-17 | 新功能候选池（16 项）、四阶段路线图、重点功能卡与待验证 spike 清单 | **现行（v0.3 已实现；v0.3.1+ 规划）** |
| [v0.3-implementation-2026-10-17.md](v0.3-implementation-2026-10-17.md) | 2026-10-17 | v0.3 四个只读能力的实现记录、边界、代码落点与验证结果 | **现行** |
| [e2e-v0.3-2026-10-17.md](e2e-v0.3-2026-10-17.md) | 2026-10-17 | v0.3 Docker E2E：v1/v3 各 55/55、环境矩阵、首轮失败与修正 | **现行（最新口径）** |
| [verify-v0.3-2026-10-17.md](verify-v0.3-2026-10-17.md) | 2026-10-17 | v0.3 独立复验：77 单测 / 19 冒烟 / 55+55 E2E 复现；P2 数据竞争与 P3 文档口径已修复，P1 提交待办 | **现行** |

## 阅读顺序建议

1. 了解功能与已知限制 → 仓库根目录 `README.md`
2. 复跑端到端验证 → 本目录 `e2e-full-regression` 第一节（容器创建命令）→ `scripts/e2e_docker.py`
3. 了解 v0.3 交付与后续路线 → `feature-roadmap-2026-10-17.md`
4. 确认某个 v1 接口用法是否正确 → `v1-api-verification`

## 未随仓库发布的记录

`docs/archive/` 保留两类已被取代的材料，仅存于本地（`.gitignore` 已忽略）：

- 首轮 v1 回归报告（42 项旧口径，已被 46 项矩阵取代）
- Nacos 3.x legacy adapter 兼容方案调查（v0.2.0 起已被原生 v3 适配取代）

## 参考资料

`docs/reference/` 为纯外部资料存档，不构成本仓库结论：

| 文件 | 来源 |
| --- | --- |
| `go-sdk-readme.md` | DBX Go 插件 SDK 说明 |
| `manifest.schema.json` | 插件 manifest schema |
| `plugin-development-full.txt` | DBX 插件开发文档全文 |
| `plugin-getting-started-zh.md` | 插件快速上手（中文） |

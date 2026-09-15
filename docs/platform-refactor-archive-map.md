# 旧平台重构清单归档映射

> 状态：2026-09-16 已归档。当前平台实施的唯一入口是 [Watchdog KISS Tasklist](watchdog-kiss-refactor-tasklist.md)；Flow 实施状态只看 [Flow Tasklist](flow-module-tasklist.md)。本文只核销旧 [平台重构 Tasklist](platform-refactor-tasklist.md) 中仍显示为未完成的条目，不产生新的实施路线。

## 归档规则

- **KISS 完成**：目标能力仍需要，已由对应 KISS 纵向切片交付并有代码、测试和提交证据。
- **删除旧路线**：条目依赖 PocketBase、多租户、VictoriaMetrics、通用 provider/module 或在线迁移；目标架构明确不再实现。
- **Flow 继续**：属于 Flow 业务数据或地址维度契约，只能在 Flow 清单继续，平台清单不再追踪。

## 未完成项逐项归档

| 旧条目 | 归类 | 唯一去向 / 结论 |
|---|---|---|
| P0 `PB 收缩为认证内核` | 删除旧路线 | KISS-01 改为彻底删除 PB；本地认证、剩余活入口和物理删除均已闭环。 |
| P0 `空库安装、存量迁移、checksum/quarantine、shadow read、切换、回退和生产零调用观察` | KISS 完成 + 删除旧路线 | 空库安装、schema checksum、失败隔离和 PB 零调用由 KISS-01B/E 完成；用户确认无历史数据，backfill/shadow read/在线切换与回退不实施。 |
| P1 `collector enrollment/rotation/revoke/capability/fleet rollout/canary` | KISS 完成 | 由 KISS-04 完成注册、凭证、plan/ACK/LKG 与 rollout；没有真实需求的通用 canary 状态机不再扩建。 |
| P3 `Adjustment policy/version/rule/approval/reconciliation` | KISS 完成 | 由 KISS-07 的三层账单、adjustment/reversal、审批与 reconciliation 交付。 |
| P3 `查询时应用与批量物化边界、冲突优先级、effective time、血缘和回滚` | KISS 完成 | KISS-07 冻结 raw/supplier/customer 查询时计算、generation/provenance 与不可变关闭；不回写原始 Flow。 |
| P3 `修正前后对账、审计、导出视图和租户级权限` | KISS 完成 + 删除旧路线 | KISS-07 完成对账、审计与证据导出；权限改为单域 RBAC，tenant 权限删除。 |
| PLAT-WEB-01 `前后端独立运行边界` | KISS 完成 | KISS-01B/C 完成一个 `API_URL`、直连 Gin、CORS、登录/登出/下载、history fallback 和 API 故障行为；PB OAuth/SSE 与旧单体兼容不保留。 |
| PLAT-04F2 `system-scope job 管理 API` | 删除旧路线 | 其前提是跨租户 platform-admin；单部署域不需要第二套 system-scope 身份。现有 operation jobs 继续按单域 RBAC 管理。 |
| PLAT-IAM-01 `platform-admin/system-role` | 删除旧路线 | 多租户平台级角色不属于目标产品；管理员是单域 RBAC 的普通内建角色，不再引入 owner-tenant 变通。 |
| PLAT-04A2f `scoped publication 通用内核` | 删除旧路线 | 不抽象第二套通用 publication CRUD；全局地址发布保留 KISS-05 已验证实现，VPN 使用自己的 typed adapter。 |
| PLAT-04C4 `运营商发布身份` | Flow 继续 | 运营商稳定 `isp_id`、ASN 集合、发布/回滚与查询标签属于 Flow 地址维度契约，在 Flow tasklist 继续验收。 |

旧清单中已经勾选的条目只作为历史提交证据，不代表其 PB/tenant/VM 架构仍然有效；与 KISS 目标冲突的描述一律以 KISS 架构和本映射为准。

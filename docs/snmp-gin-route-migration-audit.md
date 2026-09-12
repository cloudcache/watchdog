# SNMP / Gin 路由迁移审计

状态：2026-09-12；SNMP 时序兼容面已完成迁移。本文只审计迁移完整性，不把历史存在的功能默认为死功能。

## 结论

当前 404 不是“SNMP agent 没启动”造成的。agent 是否运行只影响是否产生新样本；API 路由是否存在由 Gin 后端决定。

路由混乱来自一次越过迁移边界的切换：历史 API 使用 `/api/v1/network/...` 和 `/api/v1/snmp/...`，新 Gin 又建立了 `/devices`、`/ports`、`/bgp`、`/snmp-profiles` 等第二套路由，并把历史路径标成“legacy alias”。前端随后被零散改向第二套路由，但许多历史子资源并未在第二套路径上注册。迁移也没有以“历史路由、参数、响应 -> Gin 等价实现 -> 原前端无修改运行 -> 契约测试”逐项核销，最终出现 404 和两套不完整契约并存。

从现在起遵守以下门禁：

1. 历史功能只有在产品明确决定下线后才能删除；“新后端尚未迁移”不能作为删除理由。
2. 每个功能按 schema、repository、API、UI、单元、真实 MySQL/ClickHouse 集成和路由契约一起迁移。
3. 历史路径就是兼容契约；前端不因后端迁移而改 URL、参数或响应 DTO。`/devices` 等新路径不得再被当成迁移目标，是否保留以后单独决定。
4. SNMP 管理定义存 MySQL，SNMP 时序、聚合、导出和事件存 ClickHouse；不恢复 VM/VictoriaLogs fallback。

## SNMP 路由逐项核销

| 能力 | 必须保留的历史路由 | Gin 状态 / 处置 |
|---|---|
| 设备 CRUD/列表/summary | `/network/devices...` | 已迁移；继续作为对外契约，不改前端 |
| 设备端口/地址/BGP/传感器/库存/VLAN/LAG | `/network/devices/{id}/...` | 已迁移；分页、搜索、排序、filter 由 Gin 提供 |
| 端口 CRUD | `/network/ports/{id}` | 已迁移；继续作为对外契约 |
| 全局 BGP | `/network/bgp...` | 已迁移；继续作为对外契约 |
| SNMP profile | `/snmp/profiles...` | 已迁移；继续作为对外契约 |
| SNMP metrics | `/metrics/catalog|query|range|realtime|aggregate` | 已迁移到 ClickHouse；同路径、同参数和图表响应 |
| SNMP export | `/exports...` | 已迁移到 ClickHouse reader；同路径 |
| 设备/端口 overview graph schema | `/graph/devices/{id}/overview`、`/graph/ports/{id}/overview` | 本批恢复；同路径，查询改走 ClickHouse |
| 跨设备/端口聚合图 CRUD/列表 | `/aggregate-graphs...` | 本批恢复；同路径/DTO，MySQL 只存定义和成员 |
| 聚合图 series/data/summary | `/aggregate-graphs/{id}/...` | 本批恢复；同参数/响应，时序读取改为 ClickHouse |
| 端口 raw/supplier/customer 修正规则 | `/network/ports/{id}/policy` | 已迁移；无租户 MySQL 定义，原 DTO；metrics/aggregate/aggregate-graph 在聚合前逐端口应用同一确定性规则 |
| provider/customer 默认规则 | `/network/traffic-policy-defaults` | 已迁移；无端口覆盖时按 side 继承全局规则，更新留审计记录 |
| MIB module CRUD | `/snmp/mib-modules...` | 已迁移；无租户 MySQL CRUD、原 PascalCase DTO、管理写审计 |
| 设备事件列表 | `/network/devices/{id}/events` | 已迁移；ClickHouse `snmp_events`，服务端分页/search/sort/column filter，原 VTable envelope 响应体 |
| 设备事件 facets | `/network/devices/{id}/events/facets` | 已迁移；severity/event_type/source 的有界 facet 查询，排除当前列过滤以支持多选 |
| SNMP trap HTTP ingestion | `/snmp/traps` | 已迁移；原 UDP listener 不重写，Bearer/mTLS 接 Agent Registry，仅 `snmp` agent 可写；具备 `device.update` 的 session 保留诊断入口；事件只写 ClickHouse |

## 当前前端仍调用、但 Gin 未闭环的非 Flow 路由

这些不是本批 SNMP aggregate 的扩大实现范围，但必须保留在平台迁移清单中，不能因 404 删除界面：

| 路由族 | 状态 |
|---|---|
| `/api/v1/me/preferences` | MySQL 已有 `user_preferences`，Gin API 未迁移 |
| `/api/v1/audit-logs`、`/api/v1/operation-jobs...` | 已迁移到 Gin/MySQL；不是待办 |
| `/api/v1/dashboards...` | 历史 dashboard CRUD/preview 未迁移 |
| `/api/v1/retention/policies...` | 未迁移；需按 ClickHouse 生命周期重新冻结语义 |
| `/api/v1/modules...` | 历史模块目录未迁移；需明确由 agent capability 取代的部分和仍需保留的 UI 能力 |
| `/api/v1/config`、`/heartbeat/status|test` | 平台状态/配置 UI 仍调用，Gin 未迁移 |
| `/api/v1/containers...`、`/systemd-services...` | system agent 延后，不应伪装为已迁移 |
| `/api/v1/smart-devices...` | 旧 PB 链已删除但 UI 仍存在；需产品决定迁到 device health 或明确下线，不能静默 404 |

## 历史非 Flow API 全量差异

本表以 `internal/watchdog/api_*.go` 的历史注册和当前前端调用为输入，与
`internal/server` 的 Gin 注册逐族比对。它用于防止“删掉旧 handler 就等于迁移完成”。
Flow/Geo 由并行工作负责，不在本次 SNMP 提交中改动。

| 历史路由族 | 当前状态 | 兼容迁移要求 |
|---|---|---|
| `/health/live`、`/health/ready`、`/health/runtime...` | **未迁移** | 保留探针路径和状态码；底层健康项改为 MySQL/ClickHouse |
| `/me/preferences` | **未迁移** | 迁入 MySQL `user_preferences`，原 GET/PUT DTO 不变 |
| `/permissions` PUT/DELETE、`/permissions/effective` | **未迁移** | 映射到单域 RBAC/设备根；不能只留下 GET catalog |
| `/collector-enrollment-secrets...`、`/collectors/enroll`、collector credential/plan/principal/delete-preview | **尚无历史路径适配** | 新 `agents` 数据模型可以承载，但旧 agent/collector 客户端路径和 DTO 必须有明确兼容层或版本退场门禁 |
| `/plan-rollouts...`、`/operation-job-schedules...` | **部分迁移/路径改变** | operation job 已迁；rollout 目前只在 `/agents/plan-rollouts`，旧路径和 schedule CRUD 尚缺 |
| `/dashboards...` | **未迁移** | 保留 CRUD、graph-options、draft/saved preview；管理定义进 MySQL，指标查询进 ClickHouse |
| `/modules...`、`/tenants/{id}/modules` | **未迁移** | 去租户后提供单域兼容适配；不能让仍在使用的模块 UI 直接 404 |
| `/retention/policies...` | **未迁移** | 语义重定向为 ClickHouse 生命周期，但保留 API 参数/响应 |
| `/historical/preview|archive|delete` | **未迁移** | 按 ClickHouse 分区操作重做执行端，保留异步 operation job 契约 |
| `/query`、`/query-policies...` | **未迁移** | 明确 dashboard/非 Flow 查询消费者后迁入；不能用删除 DatasetProvider 代替兼容实现 |
| `/billing/periods/{id}/compute|void` | **路径发生变化** | 当前 Gin 使用 `calculate|close`；需要恢复旧 action 别名及旧 DTO，或先完成有版本的调用方迁移 |
| `/metrics/vmquery` | **已迁移兼容入口** | 原 path、`query/start/end/step` 和 matrix 响应保留；执行改为 CH typed SNMP selector。VM 已裁撤，任意 MetricsQL 不再伪装可执行，非精确 selector 明确返回 400 |
| `/config`、`/heartbeat...` | **未迁移** | 迁入 MySQL/运行时状态；保持现有配置页调用 |
| `/containers...`、`/systemd-services...` | **未迁移（system agent 延后）** | 保留在平台任务，不能返回伪数据或静默删 UI |
| `/smart-devices...` | **未迁移** | 明确归入 device health 或经产品决策下线；在此前视为缺口 |

明确删除而无需兼容的只有多租户管理本身（例如租户 CRUD/tenant ownership）。如果旧路径中
tenant ID 只是作用域参数、而功能本身仍属于目标产品，则应由单域适配器忽略/校验默认域，
而不是把整个功能一起删除。

## 后续迁移顺序

本轮 SNMP 五项已经按 schema、repository、API、进程认证、单元、真实 MySQL/ClickHouse 集成与路由契约闭环。后续仅按“当前前端仍调用”和“历史非 Flow API 全量差异”逐域迁移；每批以历史契约测试为门禁，不再零散修改前端 URL。system/container agent 仍按 KISS-03B 延后，不混入 SNMP 提交。

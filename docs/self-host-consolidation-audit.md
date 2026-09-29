# Watchdog 自托管收敛审计 (Self-Host Consolidation Audit)

> **复核状态（2026-09-28）**：逐条对照当前代码复核——已修复 16 · 部分修复 3 · 未修复 1 · 作废 3（作废 = 被后续设计决策取代，如全局共享 token、不设审批门）。逐条状态与证据见文末「复核状态（2026-09-28）」。

> **历史审计 / 决策已完成：** 本文提出的去 PB、去多租户、MySQL + ClickHouse 方向已经冻结到 [Watchdog KISS 目标架构](watchdog-kiss-architecture.md)。本文不再是待决策或实施入口，仅保留当时的现状证据。

## 0. 目标（用户指令）

1. **去 PB、去多租户、保留 RBAC**：由"可 SaaS"变为 **self-host** 单租户。
2. **存储收敛 · MySQL**：用户权限、固化报表、账单、设备/target。
3. **存储收敛 · ClickHouse**：监控数据（时序）。
4. **无存量数据需迁移**（用户确认）—— 决定性简化。

## 1. 结论先行

可行，且"无存量数据"让它从**在线迁移**变成**推倒重建、一次切换**。三类最吓人的成本直接消失：无 PB→MySQL / VM→CH 数据搬运、无影子对账窗口、无 `systems` 记录 ID 连续性问题、无逐列改表手术。剩下的**全是代码**，且被**一件事**主导：**替换 PB 认证**。其余（遥测落库、HTTP 承载、SSE）都是机械活。

## 2. 现状："架构太复杂"的具体含义

- **hub 本身就是一个 PocketBase 应用**（`internal/hub/hub.go` 内嵌 `core.App`）。PB 同时是：唯一认证权威 + 遥测的 SQLite 存储 + realtime/SSE 引擎 + `/api/v1` 挂靠其上的 HTTP server/router/静态托管。
- **4 个在线存储 + Kafka**：MySQL（~90 表，管理库）、ClickHouse（`watchdog_flow`，flow 事实）、**VictoriaMetrics**（唯一 TSDB，~40 series，全部 SNMP + agent 遥测历史）、PB SQLite（认证 + 遥测）。VLogs 已裁撤（`config.go:441` 硬报错守卫）。
- **多租户**：90 表中 **78 表带 `tenant_id`**（77 条 `ON DELETE CASCADE` 外键指向 `tenants`）；~**1,132** 处 `auth.TenantID`、**418** 处 `WHERE tenant_id`、**93** 个 repo 方法带 `tenantID` 参。但 tenant 由身份投影下发，**无 tenant CRUD/UI**。
- **RBAC**：`roles`/`permissions`/`user_roles`（`user_roles` 已无 tenant），admin = 名为 `admin` 的角色；`RequirePermission` 63 处，`auth.IsAdmin` 直接放行。

## 3. 目标架构

- **单一 Go 服务**（无 PB），self-host，单租户。
- **认证**：极简原生 或 外部 IdP/反代（见决策 1）。保留 `external_subject_id` 桥，`identity_adapter` 授权投影不动。
- **存储两件套**：MySQL（users/RBAC、targets/devices/ports、账单、固化报表/导出）+ ClickHouse（**所有监控时序**：flow + SNMP 指标 + agent 遥测 stats）。**VM 下线**。
- **RBAC 保留**（全局角色，admin=角色）。

## 4. 逐目标差距

### A. 去 PB —— 认证是命门，其余机械

- **认证（难）**：前端深度耦合 PB JS SDK（`pb.authStore` 5 文件/15 处；登录用 `authWithPassword`/`authWithOAuth2`/`authWithOTP`/`requestPasswordReset`/`listAuthMethods`）。后端唯一验证点 `NewPocketBaseIdentityAuthenticator`（`FindAuthRecordByToken`，限 `users` 集合）。凭据在 PB SQLite（bcrypt/token/OTP/MFA/OAuth）。MySQL 已是纯授权投影（`029` 删 `password_hash`），键 `external_subject_id`。
  - **self-host 单租户大幅简化**：自托管一般不需要 OTP/MFA/多 provider OAuth。**极简原生认证**（bcrypt 口令 + session token + 首用户引导）约数百 LOC，而非 PB 全量面的 2–4k；或直接交给**反代/SSO header**（`TRUSTED_AUTH_HEADER` 已存在）。关键是保住 `token → external_subject_id` 桥，让授权投影和全部 `/api/v1` 不动。
  - 前端：去掉 pocketbase SDK（它同时是 transport + authStore + realtime）。`pb.send` 换成带 session token 的 fetch 封装；重写登录表单。
- **遥测集合（机械，无数据搬运）**：`systems`/`system_stats`/`container_stats`/`containers`/`systemd_services`/`system_details`/`smart_devices` → 落 MySQL（清单/最新）+ CH（时序）。`fingerprints`/`universal_tokens`（agent 认证）→ MySQL 表。**`systems` ID 连续性风险因无存量而消失**。
- **Realtime SSE（小）**：仅 3 处 `.subscribe()`（`alerts`/`smart_devices`/`fingerprints`）。self-host 直接**改轮询**即可（图表本就轮询 VM；既有配方已把 subscribe 改 refetch）。
- **HTTP 承载（较易）**：`/api/v1` 已是标准 `http.Handler`，换 chi/std mux 平移。需补：静态 SPA + fallback、CORS/body-limit 中间件、cron 库、把 `OnRecord*` 钩子改成显式调用。

### B. 去多租户，保留 RBAC —— PIN，别 DROP

- **推荐 PIN 单一固定 tenant**：认证适配器改成永远绑定同一个 tenant，seed 一行，删前端 tenant 切换器 + `X-Watchdog-Tenant-ID`。schema/外键/索引/~1,000 调用点**全部不动、原样编译**。RBAC **零结构改动**（roles/permissions 留存，admin 仍是角色）。
- **DROP**（78 表去列）= 干净 schema，但要重建 77 外键 + 各 `uq_*_tenant_*` 复合唯一键，并同时改 ~1,000 处。无存量 → **数据风险为零**，但代码 churn 仍在。**结论：先 PIN；DROP 作为可选后续清理**（常量列无害）。若想借重建顺带上干净 schema，DROP 可行，但预算好那笔机械改动。
- 顺带**溶解 address-library owner-tenant 变通**（今天遮住你 UI 的那套）为普通 admin 门禁。
- **保留** `operation_jobs` 的 tenant/system scope（本质是"归属 vs 平台全局"，单租户仍有用）。

### C. 存储 → MySQL + ClickHouse（下线 VM）

- **VM → CH**：写侧易（2 个 sink：`snmp_raw_writer.go`、`agent_plan.go`，复用 `internal/flowch` 原生 writer）。降精度：复用 `flowch/rollup.go`（CH 比 VM 更适合"raw ~1 年后 downsample"，正是你的保留模型）。**读侧是真成本**：~10 个 VM 耦合文件（~1,500 LOC）在 CH SQL 里重写 PromQL 语义 —— counter `rate()`（含重置检测）、traffic-view/value-mode 修正，且 `billing_metrics.go` 读这些（**正确性=钱**）。无存量 → 用新鲜数据验证、无历史对账，但数学必须对。
- **遥测落点**：`systems`→MySQL（清单）；`smart_devices`→MySQL（最新）+ CH（历史点）；`system_stats`/`container_stats`→CH。
- 复用 `metric_retention_policies`（MySQL）的执行从 `DeleteSeries` 换成 CH TTL/分区 drop。

## 5. 决策状态

1. **认证策略** —— ✅ **已定：极简原生认证**（bcrypt 口令 + session token + 首用户引导；砍 OTP/MFA/OAuth；保住 `token → external_subject_id` 桥，`identity_adapter` 授权投影不动）。
2. **PIN vs DROP** `tenant_id` —— 建议默认 **PIN**（DROP 作可选后续）。*未反对即按此。*
3. **VM 下线**（VM→CH）—— 建议默认 **下线 VM**。*未反对即按此。*
4. **SSE → 轮询** —— 建议默认 **轮询**（self-host 最简）。*未反对即按此。*

## 6. 建议顺序（推倒重建、一次切换）

1. **去多租户 · PIN**：改认证适配器 + seed 一行 + 删前端切换器。最小改动、无 schema 变更、解锁后续。
2. **存储 · VM→CH**：建 CH `metric_samples` + 重指 2 个写 sink + 指标 rollup；在 QueryGateway 后把 VM 读路径用 CH 重写（rate/traffic-view/billing）；下线 VM。
3. **认证 · 去 PB**：极简原生（或 IdP），保住 `external_subject_id` 桥；前端去 SDK、重写登录。
4. **遥测落库**：7 个 PB 集合落 MySQL/CH；agent 认证 `fingerprints`/`universal_tokens` → MySQL。
5. **HTTP 承载**：PB server 换纯 Go（router/static/CORS/cron）；钩子改显式；删 `pocketbase` 依赖。
6. **收尾**：VLogs 部署资产清扫；可选 `tenant_id` 列 DROP。

## 7. 工期/风险

- 最大：**认证替换**（安全关键）+ **VM 读路径/账单正确性**。
- **无存量数据**移除了整层迁移安全工程（checksum/quarantine/shadow-read/回退）。
- **可逆性**：每步都是在重建存储上的代码改动；切换期每个域把旧路径留在开关后，验证后再删。

## 8. 与既有文档的关系

- 本文**重开** `docs/storage-consolidation.md`（ADR-SC-001）的 §1.4/§3.1/§14（不合并 VM/CH）与 PB-auth-kernel 立场。建议决策落定后，把 ADR-SC-001 标注为"被自托管收敛取代"，避免两份冲突方向并存。
- `docs/platform-refactor-tasklist.md` 的 P0"PB 收缩"与"空库安装/存量迁移"两项应按本方向重写（后者因无存量而基本作废）。

---

## 复核状态（2026-09-28）

本节由 2026-09-28 全项目复核生成：每条发现都对照当前代码/迁移/提交核实，以代码为准。汇总：已修复 16 · 部分修复 3 · 未修复 1 · 作废 3（部分未修复项在复核时按组列出，故表格行数可能少于汇总数）。

| 位置 | 发现 | 状态 | 证据 / 说明 |
|---|---|---|---|
| L7 | goals: remove PB and tenancy, keep RBAC, MySQL for management | 已修复 | go.mod has no pocketbase; 0001 has no tenant_id; `rbac.go` \| |
| L9 | goal: all monitoring data in CH | 部分修复 | flow + SNMP in CH；system agent telemetry not built (KISS-03B) |
| L34 | native auth replaces PB | 已修复 | bcrypt + session; frontend has no PB SDK \| |
| L37 | telemetry collections → MySQL/CH | 部分修复 | agent auth moved to the shared token (`aa870f65d`)；no system/container CH tables |
| L38 | SSE → polling | 已修复 | no EventSource or subscribe in frontend \| |
| L39 | HTTP carrier moves to plain Go | 已修复 | Gin `router.go` \| |
| L43 | PIN a tenant rather than DROP | 作废（被后续决策取代） | DROP was chosen (`0001_baseline.sql:9`) \| |
| L45 | address-library owner-tenant workaround | 已修复 | tenancy removed \| |
| L46 | keep operation_jobs scope | 已修复 | 0001 operation_jobs.scope \| |
| L50 | VM → CH (rate / traffic-view / billing) | 已修复 | snmpch; /metrics/vmquery compatibility \| |
| L52 | metric_retention_policies enforced via CH TTL / partition drop | 未修复 | `retention.go` CRUD only; 012/014 have no TTL \| |
| L56 | decisions 1/3/4 (native auth, VM removed, polling) | 已修复 | as above；3 items |
| L57 | decision 2 PIN by default | 作废（被后续决策取代） | DROP adopted \| |
| L63 | step 1 PIN | 作废（被后续决策取代） | DROP adopted \| |
| L64 | steps 2/3/5/6 VM→CH, auth, HTTP, cleanup | 已修复 | as above；4 items |
| L66 | step 4 telemetry storage | 部分修复 | agents/shared token；system telemetry missing |
| L78 | mark ADR-SC-001 superseded / freeze platform tasklist | 已修复 | `storage-consolidation.md:3`; `platform-refactor-tasklist.md:3`；2 items |

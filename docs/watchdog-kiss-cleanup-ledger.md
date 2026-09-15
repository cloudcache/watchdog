# Watchdog KISS 遗留清理台账（待清理清单 + 清理影响评估）

日期：2026-09-15。范围：把 [watchdog-kiss-audit-2026-09-15.md](watchdog-kiss-audit-2026-09-15.md) 的区域级审计下沉到**代码/包/符号、表/字段、接口/路由、配置/依赖**级别，逐项给出精确位置、依赖它的活跃代码、删除影响、前置条件、KISS 归属与执行状态，用于**确认剩余任务（KISS-01E / KISS-08）的执行状态**。方法：只读子代理精确枚举 + 对 live `watchdog` 库（79 表）与 `go build ./...`（exit 0）核验；每条均带 file:line / table.column / symbol 证据。

状态取值：`ready`=无前置可现在做；`blocked-by-X`=须先完成 X；`done`=已收敛；`KEEP`=保留。

---

## 0. 执行状态总览（剩余任务确认）

| KISS 任务 | 门禁项 | 精确阻断/剩余 | 现状 |
|---|---|---|---|
| **KISS-01E** 编码删除 | 移除 PB 源码/依赖/SQLite/volume + PB 时代字段文字 | 活跃路径 PB=0（已核）。剩余仅**非运行时**文字残留：死的 PB-hub Dockerfile+CI（L19a）、init/migration PB 字段与注释（S1/S2、L19c）、GitHub 模板（L19b）、PB 告警表 dump（S10） | **ready**——清完这几项即可勾「编码删除」；「回归/clean-stack 验收」门属运维另计 |
| **KISS-08** 遗留清理 | 删 `internal/watchdog`/VM/tenant/provider/旧 targets/兼容 adapter | **唯一硬阻断链**：`internal/server`（13 文件）import `internal/watchdog`（65 符号=SNMP 域+引擎，L1/L2）。分解为 (a) 抽 SNMP 切片断 import（L2-L4，ready）；(b) 退役 6 遗留 worker（L10-L14，2 个已被 in-server CH 取代）。之后整包 + `robfig/cron` + VM 配置随之删（L1/L6-L9/L15/L17） | **blocked-by-L2 + workers**——但可分步 |
| **危险接线**（属 KISS-08，但可现在拆） | —— | 遗留安装器建不兼容 `watchdog_installation` 会 brick KISS boot（S3/L12）；两超期 worker 查 KISS 不存在的 `collector_agents⋈tenants`（S5/L10-L11） | **ready**——加库守卫或摘 Makefile/scripts/CI 接线 |
| **接口收敛**（KISS-02→08） | 删 `/targets`+`/network/*` compat alias | 35 条 compat 路由（I1-I5）仍被前端 65 处调用；且端口 CRUD/策略、设备 snmp-patch、设备 events **仅在** alias 上，canonical 面不全 | **blocked-by-前端迁移 + 补 canonical** |

**一句话结论**：KISS 运行时（`cmd/watchdog-server`→`internal/server`→`deploy/schema/mysql`）干净自洽、无 blocker。所有遗留物由**一条 import 链**（server→watchdog 的 SNMP 切片）+ **6 个遗留 cmd worker** + **前端 compat 调用**三者钉住；三者互相独立，可分步拆除，全程不动 KISS 运行时。

---

## 1. 代码 / 包 / 符号台账（L1–L19）

`internal/watchdog` = 单一 Go 包（237 非测试 + 180 测试文件）。Go 按包 import，故 `internal/server` 只要 import 就钉住整包——**删任何文件前必须先断这条 import（L2）**。

| ID | 类别 | 精确位置 | 分类 | 依赖它的活跃代码 | 删除影响 | 前置 | 归属 | 状态 |
|---|---|---|---|---|---|---|---|---|
| L1 | package | `internal/watchdog/`（237 文件） | KISS-08 | `internal/server`(13) + 6 worker main | 包级 import，未断前一文件不可删 | L2 + L10-L14 | KISS-08 | blocked-by-L2 |
| L2 | symbol-set | 65 符号 / 19 锚文件 + SNMP 引擎闭包(~18) — `domain.go`/`traffic_policy.go`/`metric_catalog.go`/`metrics_query.go`/`snmp_*` | **EXTRACT-first** | `internal/server` 13 文件（`snmp_*`,`metrics.go`,`aggregate_graphs.go`,`graph_overview.go`,`retention.go`,`handlers_grants.go`） | 不抽则 server 无法脱离 watchdog | 建 `internal/snmpdomain`，迁 `gosmi`/`gosnmp` | KISS-08 | **ready** |
| L3 | file(split) | `config.go:275 SNMPConfig`（余为遗留 BackendConfig） | EXTRACT-first | `snmp_discovery.go:26` | server 少 MIB 注册入参类型 | 从 BackendConfig 拆出 | KISS-08 | ready |
| L4 | file(split) | `repository.go:468 MetricRetentionPolicy` | EXTRACT-first | `retention.go:25,100` | server retention 断 | 从遗留 repo 拆出 | KISS-08 | ready |
| L5 | file-cluster | 旧 HTTP 层 `api_*.go`(53) + `api_router.go`；`runtime.go:413 Router()`,`:535 MetricsScrapeHandler()` | **DEAD-now** | 无（无 binary 调 `.Router()`） | 无——已被 `internal/server` Gin 取代 | 连 `Router()`/`NewAPIV1Router` 一起删 | KISS-08 | **ready** |
| L6 | file-cluster | `victoriametrics.go`,`query_gateway.go`,`query_provider_*.go`,`export_vm.go` | KISS-08 | 仅经 `NewBackendRuntime`（worker） | 断遗留 worker | L10/L11 退役 | KISS-08 | blocked-by-workers |
| L7 | file-cluster | `flow_*.go`(17)，含 `flow_rollup_jobs.go:450-452` (`collector_agents⋈tenants`) | KISS-08 | `NewBackendRuntime`（worker） | 断 aggregate/export worker | L10/L11 | KISS-08 | blocked |
| L8 | file-cluster | `address_*.go`/`dimension_*.go`(19) | KISS-08 | worker 侧 flow-enrichment/query-provider | 已被 `internal/address` 取代 | flow 消费端迁 v2 | KISS-05→08 | blocked |
| L9 | file-cluster | `mysql_*.go`(36),`collector_*`(13),`operation_job*.go`(4),`platform_*`,`billing_*`,`dashboard.go` | KISS-08 | `NewBackendRuntime`（worker） | 断遗留 worker | L10/L11 | KISS-08 | blocked |
| L10 | cmd | `cmd/watchdog-aggregate-rollup` | legacy-guard-then-delete | 无（未发布） | **已被** in-server CH `aggregate_graphs.go`/`snmp_aggregate.go`+`internal/snmpch` 取代；对 KISS 库查 `collector_agents⋈tenants` 即失败 | 加库守卫 / 摘 CI+Makefile | KISS-08 | **ready** |
| L11 | cmd | `cmd/watchdog-export-worker` | legacy-guard-then-delete | 无 | **已被** in-server opjob CH 导出 `exports.go:25`/`snmp_exports.go`/`flow_exports.go` 取代 | 同上 | KISS-08 | **ready** |
| L12 | cmd | `cmd/watchdog-install` + `install/init.sql` + `deploy/migration/mysql/` | legacy-guard-then-delete | 无 | 建 boot-breaking `watchdog_installation`（见 S3） | 确认 HTTP `/install` 唯一；摘 `Makefile:111-112`、`scripts/watchdog-dev-db.sh:24`、`docs/...architecture.md:115` | KISS-08 | **ready** |
| L13 | cmd | `cmd/watchdog-librenms-extract` | legacy-delete | dev 工具 | 无（`ParseLibrenmsDefinitions` 随 L2 迁走） | L2 | KISS-08 | ready |
| L14 | cmd | `cmd/watchdog-snmp-agent` / `cmd/watchdog-system-agent` | legacy-guard | 无（未发布） | KISS-03B/路线图 | 路线图决策 | KISS-08/03B | blocked(路线图) |
| L15 | config | `config.go:971` **必填** `victoriametrics.base_url`；VM/tenant 键 `:80,114-115,232-233,246,258,264,125,199` | KISS-08 | 遗留 worker 经 `LoadBackendConfig` | 断遗留配置加载 | L10-L14 | KISS-08 | blocked |
| L16 | config | `config/watchdog.example.yaml`(`:7,13-16,41,57,65-77`)、`watchdog.dev.yaml`(`:7,10-13,27,37,43,84-90`) VM/tenant/provider 键 | **DEAD-now**(server 忽略) | 无（server 用 `internal/server/config.go`） | 无 | —— | KISS-08 | **ready** |
| L17 | dep | `go.mod robfig/cron/v3` | KISS-08 | 仅 `operation_job_schedule.go:12` | 无（L1 后） | L1 | KISS-08 | blocked-by-L1 |
| L18 | dep | `gosmi` / `gosnmp` | **KEEP(relocate)** | 活跃 SNMP 引擎 | —— | 随 L2 迁 | KISS-08 | ready |
| L19 | PB 残留 | a) `internal/dockerfile_hub:29,31,34` + `.github/workflows/docker-images.yml:18,71`；b) `.github/ISSUE_TEMPLATE/bug_report.yml:124`、`DISCUSSION_TEMPLATE/support.yml:92`（`/_/#/logs`）；c) 见 S1/S2 | **DEAD-now** | 无 | 关闭 KISS-01E「编码删除」 | 无 | KISS-01E | **ready** |

**关键精确性**：`internal/server` 对 watchdog 的 65 符号里 ~53 为 VALUE（域类型/常量/纯 helper）、~12 为 RUNTIME，且 RUNTIME **全是活跃 SNMP 采集引擎**（poller/discovery/query-engine/CH-writer/MIB-registry/trap-dispatcher，KISS-03 代码，也被 `watchdog-snmp-collector` 用）——**零** `NewBackendRuntime`/`QueryGateway`/`DatasetProvider`/`VictoriaMetrics` 引用。故 L2 抽出的是**自足的 SNMP 域+引擎切片**，不牵扯遗留平台运行时。

---

## 2. Schema / 表 / 字段台账（S1–S12，K1–K2）

KISS `deploy/schema/mysql` 30 迁移 = live 79 表；真实 `tenant_id`/PB 列 **0**（14 处 `tenant_id` 全注释）。遗留 `install/init.sql`=89 表/363 real `tenant_id`；`deploy/migration/mysql`=413。

| ID | 对象 | 精确位置 | 分类 | KISS 等价 | 读写它的代码 | 删除/保留影响 | 归属 | 状态 |
|---|---|---|---|---|---|---|---|---|
| S1 | `users.auth_provider`,`external_subject_id` | `install/init.sql:2020-2021,2026,2028` | DEAD-now(PB) | 无 | 无（dump） | 消除最后 PB schema 字段 | KISS-01E | **待删** |
| S2 | "PocketBase" 注释 | `deploy/migration/mysql/030:1` | DEAD-now(PB) | 无 | 无 | 纯文字 | KISS-01E | 待删 |
| S3 | `watchdog_installation`(旧形状 `id VARCHAR 'default'`,无 `schema_version`) | `deploy/migration/mysql/001:6`+`install/init.sql:2032` | DEAD-now(冲突) | `watchdog_installation`(0001:29 `id=1`+`schema_version`) | 写:`internal/watchdog/install.go:86-108` | **不删=KISS server 启动报 Unknown column 'schema_version' 开不了机** | KISS-08(接线现拆) | **阻断** |
| S4 | `watchdog_schema_migrations` | `internal/watchdog/mysql_migrator.go:104` | DEAD-now | `schema_migrations`(0001:19) | `internal/watchdog/{mysql_migrator,install}.go` | 双 ledger；随包删 | KISS-08 | 延后 |
| S5 | `collector_agents`,`tenants` | init 568/1959；migration 018/001 | DEAD-now | `agents`(0003)/无 | **rw `flow_rollup_jobs.go:451-452`**,`mysql_collector_machine_authenticator.go:54`,`mysql_target_repository.go:298` | 超期 worker 查询→对 KISS 库即失败 | KISS-08(worker 现守卫) | **阻断** |
| S6 | collector_* 治理表(9 张) | migration 018/019/052/053 | DEAD-now | 部分折叠入 `agent_*`/`operation_jobs` | 仅 `internal/watchdog` | 无 KISS 读者 | KISS-08 | 延后 |
| S7 | snmp 定义/事件表(6 张) | migration 007 | DEAD-now | 定义→代码引擎；`snmp_events`→ClickHouse | `snmp_events` 仅 CH(`internal/snmpch`)；余仅 watchdog | CH 已承接 events | KISS-08 | 延后 |
| S8 | operation_job 卫星表(4)+`discovery_jobs` | migration 028/044/009 | DEAD-now | `operation_jobs`(0001:368) | 仅 watchdog | 单 job 引擎已收敛 | KISS-08 | 延后 |
| S9 | flow 泛化表(`query_dataset_policies`,`flow_storage_*`,`flow_classification_profiles`,`aggregate_graph_data`) | migration 045/056/059/022 | DEAD-now | CH-side / 无 | watchdog 泛化栈 | 剥 hub 泛化栈后删 | KISS-08(=hub 退役) | 延后 |
| S10 | PB 告警表(`alerts_history`,`notification_channels`,`quiet_hours`) | `install/init.sql:453,1505,1675` | DEAD-now(PB) | 无(KISS-L 在 CH 重建) | 无 | 已无写链 | KISS-01E/L | 待删 |
| S11 | 遗留 device/target/network 表(§见审计) | migration 001/004/008/016 | KISS-equivalent | `devices/ports/sensors/…` | `mysql_target_repository.go` 等 | 已被 `devices` 根取代 | KISS-08 | 延后 |
| S12 | `tenant_modules` | migration 023:4 | DEAD-now | 无 | 仅 watchdog | module registry 已删 | KISS-08 | 延后 |
| K1 | 双定义 billing 表 | `0001:433-557` vs `0020:3-250` | **done**(intra-KISS 收敛) | `0020`(先 DROP 再建) | `internal/server` | 无需动 | — | done |
| K2 | `async_jobs`→`operation_jobs` | `0010`→`0014` RENAME | **done** | `operation_jobs` | server/address | 无需动 | — | done |

整树可删单元：`deploy/migration/mysql/*.sql`(44)+`install/init.sql`(89 表 dump)——唯一应用者是 `cmd/watchdog-install` + 3 个 `NewBackendRuntime` worker + `mysql_migrator.go`；断 L2 + 退役 worker(L10-L14) + 摘接线后整体删。

---

## 3. 接口 / 路由台账（I1–I5）

canonical 面已存在：`/devices`（CRUD + `/devices/:id/{ports,addresses,bgp,sensors,inventory,vlans,lags,snmp/discover}`）、`/device-groups`、`/locations`、`/snmp-profiles`。compat alias 全部委派同一批 device/port handler（无逻辑分叉，见审计 §4）。

| ID | 路由组 | 精确位置 | 条数 | 前端调用 | 删除影响 | 前置 | 归属 | 状态 |
|---|---|---|---|---|---|---|---|---|
| I1 | `/network/devices` | `router.go:186-204` | 19 | ×37 | 委派同 handler | 前端切 `/devices` | KISS-08 | blocked-by-前端 |
| I2 | `/network/ports` | `router.go:207-213` | 6 | ×9 | 委派同 handler；**端口 CRUD/policy 仅在此** | 前端切 + **补 canonical `/ports`** | KISS-08 | blocked |
| I3 | `/network/bgp` | `router.go:214-216` | 2 | ×1 | 委派同 handler | 前端切 `/devices/:id/bgp` | KISS-08 | blocked-by-前端 |
| I4 | `/targets` | `router.go:217-223` | 6 | ×18 | 委派 device handler（`system↔host` DTO 映射） | 前端切 `/devices` | KISS-08 | blocked-by-前端 |
| I5 | `/network/traffic-policy-defaults` + `/network/devices/:id/snmp`、`/:id/events` | `router.go:194,203-206` | 2+ | 含于上 | `patchDeviceSNMP`/设备 events **仅在** alias | 补 canonical 后前端切 | KISS-08 | blocked |

**影响评估**：compat 路由本身零逻辑分叉（安全），但**不是纯冗余**——端口 CRUD/策略、设备 SNMP-patch、设备 events 只在 `/network/*` 上，canonical 面不全。故删 compat 的正确顺序：①补齐 canonical `/ports` 与设备 snmp-patch/events；②前端 65 处调用（`/network/devices`×37、`/network/ports`×9、`/targets`×18、`/network/bgp`×1）迁到 canonical；③删 I1-I5。**注意 UI 契约冻结规则**（tasklist §0：不改导航/IA/交互）——前端迁移须纯换 URL。

---

## 4. 清理影响评估 · 严格删除顺序

三条钉子彼此独立，可并行/分步；全程不动 KISS 运行时。

**Phase A — 现在可做（无前置，ready）**
1. **PB 收尾（关 KISS-01E 代码删除门）**：删 `internal/dockerfile_hub` + 修 CI `docker-images.yml:18,71`（L19a）；改 GitHub 模板 `/_/#/logs`（L19b）；清 `init.sql:2020-2028`、`migration/030:1` PB 字段/注释（S1/S2/L19c）。影响：无（皆非运行时）。
2. **拆危险接线**：给 `cmd/watchdog-install` + `watchdog-aggregate-rollup` + `watchdog-export-worker` 加「检测到 KISS `watchdog_installation`（`id=1`/有 `schema_version`）即拒绝运行」的响亮守卫；或从 `Makefile:111-112`、`scripts/watchdog-dev-db.sh:24`、CI、`docs/...architecture.md:115` 摘除接线（S3/S5/L10-L12）。影响：消除 brick-boot 与超期 worker 误跑风险；不删代码。
3. **删 DEAD-now 死码/死配置**：旧 HTTP 层 `api_*.go`+`Router()`（L5）；example/dev yaml 的 VM/tenant 键（L16）。影响：无（无 binary/无 server 读）。

**Phase B — 断 server→watchdog import（KISS-08 前置，ready）**
4. **抽 SNMP 切片**：19 锚文件 + 引擎闭包 + 拆 `SNMPConfig`/`MetricRetentionPolicy` → 新 `internal/snmpdomain`，迁 `gosmi`/`gosnmp`，repoint 13 个 `internal/server` 文件（L2-L4、L18）。验收：`internal/server` 不再 import `internal/watchdog`；`go build ./...` + SNMP 集成回归。

**Phase C — 退役遗留 worker**
5. L10/L11（已被 in-server CH 取代）停发布 + 删；L13 删（dev 工具）；L12 删（HTTP `/install` 唯一）；L14 按路线图（KISS-03B/Tier2）。

**Phase D — 整包 + 依赖 + schema 树删除（KISS-08 收尾）**
6. 无 importer 后删整 `internal/watchdog`（L1、L6-L9）+ `robfig/cron`（L17）+ 遗留 VM 校验（L15）+ 遗留 schema 树 `deploy/migration/mysql`+`install/init.sql`（S3-S12 载体）。

**Phase E — 接口收敛（独立，可最后）**
7. 补 canonical `/ports`/snmp-patch/events → 前端 65 处迁 URL → 删 compat I1-I5。

---

## 5. 剩余任务执行状态确认

- **KISS-01E 编码删除**：唯一剩余是**非运行时** PB 文字残留（L19、S1/S2/S10）——`ready`，Phase A.1 完成即可勾选该子项；其余门（clean-stack 空库启动、race/lint 全量）属运维验收，非本清理范畴。
- **KISS-08**：不是"未开始"，而是"被一条 import 链 + 6 worker + 前端调用精确钉住"。可分步：Phase B（抽切片）为核心前置；Phase A.2/A.3 可**立即**消除危险与死码。全部完成后整包删除条件满足。
- **无 blocker/major 代码缺陷**：KISS 运行时不依赖以上任何遗留物；本台账所有"阻断"均为**遗留物删除的前置**，非运行时故障。

配套：CH 启动语义（audit 主题 2）、地址库 seed 接线属独立 install 硬化小包，见 [watchdog-kiss-audit-2026-09-15.md](watchdog-kiss-audit-2026-09-15.md) §2/§主题 2。

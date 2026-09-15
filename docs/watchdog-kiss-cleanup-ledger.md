# Watchdog KISS 遗留清理台账（待清理清单 + 清理影响评估）

日期：2026-09-15。范围：把 [watchdog-kiss-audit-2026-09-15.md](watchdog-kiss-audit-2026-09-15.md) 的区域级审计下沉到**代码/包/符号、表/字段、接口/路由、配置/依赖**级别，逐项给出精确位置、依赖它的活跃代码、删除影响、前置条件、KISS 归属与执行状态，用于**确认剩余任务（KISS-01E / KISS-08）的执行状态**。方法：只读子代理精确枚举 + 对 live `watchdog` 库（79 表）与 `go build ./...`（exit 0）核验；每条均带 file:line / table.column / symbol 证据。

状态取值：`ready`=无前置可现在做；`blocked-by-X`=须先完成 X；`done`=已收敛；`KEEP`=保留。

---

## 0. 执行状态总览（剩余任务确认）

| KISS 任务 | 门禁项 | 精确阻断/剩余 | 现状 |
|---|---|---|---|
| **KISS-01E** 编码删除 | 移除 PB 源码/依赖/SQLite/volume + PB 时代字段文字 | 活跃路径 PB=0（已核）。剩余仅**非运行时**文字残留：死的 PB-hub Dockerfile+CI（L19a）、init/migration PB 字段与注释（S1/S2、L19c）、GitHub 模板（L19b）、PB 告警表 dump（S10） | **ready**——清完这几项即可勾「编码删除」；「回归/clean-stack 验收」门属运维另计 |
| **KISS-08** 遗留清理 | 删 `internal/watchdog`/VM/tenant/provider/旧 targets/兼容 adapter | **唯一硬阻断链**：`internal/server`（13 文件）import `internal/watchdog`（65 符号=SNMP 域+引擎，L1/L2）。分解为 (a) 抽 SNMP 切片断 import（L2-L4，ready）；(b) 退役遗留命令（L10-L14，其中 L10/L11 已由 KISS-08B 删除）。之后整包 + `robfig/cron` + VM 配置随之删（L1/L6-L9/L15/L17） | **blocked-by-L2 + remaining commands**——但可分步 |
| **危险接线**（属 KISS-08） | —— | 遗留安装器与两个超期 worker 的可执行入口均已删除；旧 schema/实现只随不可达的 `internal/watchdog` 历史测试暂留 | **done（KISS-08B/C）** |
| **接口收敛**（KISS-02→08） | 删 `/targets`+设备领域 `/network/*` compat alias | canonical 缺口已补、前端调用已迁、I1-I5 路由及专用 target handler/DTO 分支已删除；源码与路由反向测试防回流 | **done（KISS-08A）** |

**一句话结论**：KISS 运行时（`cmd/watchdog-server`→`internal/server`→`deploy/schema/mysql`）干净自洽、无 blocker。设备 API compat 调用与路由已清；剩余遗留物由**一条 import 链**（server→watchdog 的 SNMP 切片）+ **6 个遗留 cmd worker**钉住，两者可分步拆除。

---

## 1. 代码 / 包 / 符号台账（L1–L19）

`internal/watchdog` = 单一 Go 包（237 非测试 + 180 测试文件）。Go 按包 import，故 `internal/server` 只要 import 就钉住整包——**删任何文件前必须先断这条 import（L2）**。

| ID | 类别 | 精确位置 | 分类 | 依赖它的活跃代码 | 删除影响 | 前置 | 归属 | 状态 |
|---|---|---|---|---|---|---|---|---|
| L1 | package | `internal/watchdog/`（237 文件） | KISS-08 | `internal/server`(13) + 1 延期 system-agent command main | 包级 import，未断前一文件不可删 | L2 + L14 | KISS-08 | blocked-by-L2 |
| L2 | symbol-set | 65 符号 / 19 锚文件 + SNMP 引擎闭包(~18) — `domain.go`/`traffic_policy.go`/`metric_catalog.go`/`metrics_query.go`/`snmp_*` | **EXTRACT-first** | `internal/server` 13 文件（`snmp_*`,`metrics.go`,`aggregate_graphs.go`,`graph_overview.go`,`retention.go`,`handlers_grants.go`） | 不抽则 server 无法脱离 watchdog | 建 `internal/snmpdomain`，迁 `gosmi`/`gosnmp` | KISS-08 | **ready** |
| L3 | file(split) | `config.go:275 SNMPConfig`（余为遗留 BackendConfig） | EXTRACT-first | `snmp_discovery.go:26` | server 少 MIB 注册入参类型 | 从 BackendConfig 拆出 | KISS-08 | ready |
| L4 | file(split) | `repository.go:468 MetricRetentionPolicy` | EXTRACT-first | 无（已归属 `internal/server/retention.go`） | 无 | 已完成 | KISS-08F2 | **done** |
| L5 | file-cluster | 旧 HTTP 层 `api_*.go` + `api_router.go`；旧 all-in-one `runtime.go` 已删 | **NOT-clean（2026-09-15 尝试回退，§6）** | api_*.go 还定义共享类型 `FlowGeoConfig`/`SNMPDeviceDiscoverer`/`flowDetailRunner`/`MetricsAggregateRequest`/`metricsSelector` 被全包用；8 个可独立纵向副本与旧 runtime 已删 | 仅等价 Gin 功能已验收的纵向片可先删，其余类型交织旧 handler/test | 与 Phase B/D 类型解耦一起 | KISS-08 | **partial：runtime + 8 副本已删** |
| L6 | file-cluster | `victoriametrics.go`,`query_gateway.go`,`query_provider_*.go`,`export_vm.go` | KISS-08 | 旧 runtime 已删；仅剩包内类型/测试闭包 | 删除遗留 VM query/export | L2 边界类型解耦 | KISS-08 | **ready-for-closure-audit** |
| L7 | file-cluster | `flow_*.go`(17)，含 `flow_rollup_jobs.go:450-452` (`collector_agents⋈tenants`) | KISS-08 | `NewBackendRuntime`（worker） | 断 aggregate/export worker | L10/L11 | KISS-08 | blocked |
| L8 | file-cluster | `address_*.go`/`dimension_*.go`(19) | KISS-08 | worker 侧 flow-enrichment/query-provider | 已被 `internal/address` 取代 | flow 消费端迁 v2 | KISS-05→08 | blocked |
| L9 | file-cluster | `mysql_*.go`,`collector_*`(13),`operation_job*.go`(4),`platform_*`,`billing_*`；旧 Dashboard/Retention/User Preferences/Aggregate Graph repository 纵向副本已删 | KISS-08 | 余项仍由遗留包内部引用 | 已由 Gin/MySQL 单域实现承接的闭包逐项物理删除；余项继续按真实引用拆除 | L2/L14 | KISS-08 | **partial：4 个管理闭包 done** |
| L10 | cmd | `cmd/watchdog-aggregate-rollup` | legacy-delete | 无（未发布） | **已被** in-server CH `aggregate_graphs.go`/`snmp_aggregate.go`+`internal/snmpch` 取代；旧命令对 KISS 库查 `collector_agents⋈tenants` 即失败 | 已满足 | KISS-08B | **done** |
| L11 | cmd | `cmd/watchdog-export-worker` | legacy-delete | 无 | **已被** in-server opjob CH 导出 `exports.go:25`/`snmp_exports.go`/`flow_exports.go` 取代 | 已满足 | KISS-08B | **done** |
| L12 | cmd/schema | `cmd/watchdog-install` + `install/init.sql` + `deploy/migration/mysql/` | legacy-delete | HTTP `/install` + `deploy/schema/mysql` | CLI、Makefile target 与 dev-db 脚本已删除；旧 schema/实现只被 `internal/watchdog` 历史测试引用 | schema 树随 L1 删除 | KISS-08C/D | **entry done; files blocked-by-L1** |
| L13 | cmd | `cmd/watchdog-librenms-extract` | legacy-delete | 无（未发布） | server 已按配置直接加载 `ParseLibrenmsDefinitions`，旧命令只写不存在的多租户 definition 表 | 已满足 | KISS-08D | **done** |
| L14 | cmd | `cmd/watchdog-snmp-agent` / `cmd/watchdog-system-agent` | legacy-guard | trap agent 已有独立最小配置/wire DTO；system agent 仍延期 | trap agent 行为不变且已断旧包；system agent 归后续路线图 | system agent 路线图决策 | KISS-08E/03B | **partial：trap done；system blocked** |
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
| S3 | `watchdog_installation`(旧形状 `id VARCHAR 'default'`,无 `schema_version`) | `deploy/migration/mysql/001:6`+`install/init.sql:2032` | DEAD-now(冲突) | `watchdog_installation`(0001:29 `id=1`+`schema_version`) | 仅 `internal/watchdog` 历史实现/测试 | CLI/Makefile/script 入口已删，不能再污染 KISS 库；文件随 L1 删除 | KISS-08C/D | **runtime-safe; files blocked-by-L1** |
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

canonical 面现为：`/devices`（CRUD、summary、SNMP 设置/发现、`/devices/:id/{ports,addresses,bgp,sensors,inventory,vlans,lags,events}`）、`/ports`（CRUD/policy）、`/bgp`、`/traffic-policy-defaults`、`/device-groups`、`/locations`、`/snmp-profiles`。I1-I5 已在 KISS-08A 删除。

| ID | 路由组 | 精确位置 | 条数 | 前端调用 | 删除影响 | 前置 | 归属 | 状态 |
|---|---|---|---|---|---|---|---|---|
| I1 | `/network/devices` | 历史 `router.go` | 19 | 0 | 已删；network 列表通过 `/devices?kind=network`，summary 通过 `/devices/summary` | 已满足 | KISS-08A | done |
| I2 | `/network/ports` | 历史 `router.go` | 6 | 0 | 已删；CRUD/delete-preview/policy 均由 `/ports` 承接 | 已满足 | KISS-08A | done |
| I3 | `/network/bgp` | 历史 `router.go` | 2 | 0 | 已删；全局 BGP 由 `/bgp` 承接 | 已满足 | KISS-08A | done |
| I4 | `/targets` | 历史 `router.go` | 6 | 0 | 已删；host/network 共用 `/devices`，内部 `host` kind 在 API 返回 `system` | 已满足 | KISS-08A | done |
| I5 | `/network/traffic-policy-defaults` + 设备 SNMP/events | 历史 `router.go` | 2+ | 0 | 已删；分别由 `/traffic-policy-defaults`、`/devices/:id/snmp`、`/devices/:id/events` 承接 | 已满足 | KISS-08A | done |

**完成证据**：先补 canonical 缺口，再机械迁移前端 API URL并给 network 选择器增加服务端 `kind=network`，最后删除 I1-I5 与 target 专用响应分支。路由反向测试禁止旧 alias 挂载，前端源码测试禁止旧 API 字符串回流；未修改浏览器页面路由、导航、布局和视觉。

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
5. L10/L11 已在 KISS-08B 物理删除；L12 的 CLI/Makefile/script 入口已在 KISS-08C 删除；L13 已在 KISS-08D 删除；L14 的 SNMP trap agent 已在 KISS-08E 断开旧包，system agent 按路线图延期（KISS-03B/Tier2）。

**Phase D — 整包 + 依赖 + schema 树删除（KISS-08 收尾）**
6. 无 importer 后删整 `internal/watchdog`（L1、L6-L9）+ `robfig/cron`（L17）+ 遗留 VM 校验（L15）+ 遗留 schema 树 `deploy/migration/mysql`+`install/init.sql`（S3-S12 载体）。

**Phase E — 接口收敛（KISS-08A 已完成）**
7. 已补 canonical `/ports`/snmp-patch/events/defaults，前端设备领域调用已迁到 canonical URL，compat I1-I5 已删除；真实 MySQL device/agent CRUD 与 Go/前端门禁通过。

---

## 5. 剩余任务执行状态确认

- **KISS-01E 编码删除**：唯一剩余是**非运行时** PB 文字残留（L19、S1/S2/S10）——`ready`，Phase A.1 完成即可勾选该子项；其余门（clean-stack 空库启动、race/lint 全量）属运维验收，非本清理范畴。
- **KISS-08**：不是"未开始"，而是"被一条 import 链 + 6 worker + 前端调用精确钉住"。可分步：Phase B（抽切片）为核心前置；Phase A.2/A.3 可**立即**消除危险与死码。全部完成后整包删除条件满足。
- **无 blocker/major 代码缺陷**：KISS 运行时不依赖以上任何遗留物；本台账所有"阻断"均为**遗留物删除的前置**，非运行时故障。

配套：CH 启动语义（audit 主题 2）、地址库 seed 接线属独立 install 硬化小包，见 [watchdog-kiss-audit-2026-09-15.md](watchdog-kiss-audit-2026-09-15.md) §2/§主题 2。

## 6. 执行记录（2026-09-15 Phase A 已做）

| 项 | 动作 | commit |
|---|---|---|
| S3/S5/L10-L12 危险接线 | **遗留迁移器加 KISS 库守卫**：`ApplyMySQLMigrations` 检测到 KISS 标记表 `schema_migrations` 即拒绝（覆盖 `watchdog-install` + `NewBackendRuntime` 的 aggregate-rollup/export-worker），对真实 KISS `watchdog` 库验证拒绝生效——即使遗留安装器/worker 仍在，也**不再能 brick KISS boot** | 4574289d |
| L19a | 删死的 `internal/dockerfile_hub`（建已删的 internal/cmd/hub+PB serve+/watchdog_data），新增 `internal/dockerfile_server` 建 cmd/watchdog-server(8091)，CI 两处 repoint | 28276488 |
| L19b | GitHub 模板 `Hub Logs / PocketBase /_/#/logs` → `Server Logs` | 28276488 |
| S1 | `install/init.sql` 删 PB 列 `auth_provider`/`external_subject_id`+索引（仅存于 init.sql，迁移树无；init↔migration 既有 parity 缺口无关且未动） | 28276488 |
| L16(yaml) | **未做**：example/dev yaml 的 VM/tenant 键——遗留 worker config `LoadBackendConfig` 仍**必填** VM base_url，裸删会断其配置加载；随 L15/worker 退役一并处理 |
| S2(030 注释) | **未做**：改注释会动 `checksums.sha256`+破 checksum 测试，价值极低；随 KISS-08 整树删除 |
| L5(api_*.go 53 文件) | **尝试后回退——分类修正**：并非 DEAD-now 可整批删。删除后 `go build ./internal/watchdog` 断裂：api_*.go **不只是旧 HTTP 层**，还定义了被全包引用的共享类型/接口——`FlowGeoConfig`(config.go)、`SNMPDeviceDiscoverer`(discovery_scheduler.go)、`flowDetailRunner`/`flowOverseasRunner`(runtime.go/export_vm.go/query_provider_flow.go)、`flowVPNFindingExportCreateRequest`、`MetricsAggregateRequest`、`metricsSelector` 等。仅 `Router()`/`NewAPIV1Router` 是 HTTP 死码，但这些类型与运行时/worker 代码交织。→ **不是独立可删项**：须与 Phase B/D 的类型解耦一起做，不能单独 git rm。runtime.go `Router()` 方法本身可删（无调用者），但收益须整包一起。已回退，无净改动。|
| L14(SNMP trap agent) | 命令改为只读取共享 YAML 的 `snmp_trap_agent` section，并继续接受原 `WATCHDOG_SNMP_TRAP_*` 和 CLI 覆盖；本地 wire DTO 保持 `/api/v1/snmp/traps` JSON 不变。源码已无 `internal/watchdog` import；system agent 未动、继续延期。 | KISS-08E |
| L9(Dashboard) | 删除旧 tenant-scoped domain/repository/API/MySQL/test 六文件，从旧 Router/runtime 摘除注册；当前 Gin `/api/v1/dashboards` 全路由、MySQL 单域实现和 UI 不变。共享 JSON EOF guard 移到旧通用 API helper，避免因错误文件归属误删其他 handler 的输入完整性校验。 | KISS-08F1 |
| L4/L9(Retention) | DTO 收口至当前 `internal/server`，删除旧 tenant-scoped repository/API/MySQL/test 纵向副本并摘除旧 Router/runtime 注册；Gin `/api/v1/retention/policies`、MySQL 单域表、JSON 字段和 UI 不变。 | KISS-08F2 |
| L9(User Preferences) | 删除旧 tenant-scoped domain/repository/API/MySQL/test 纵向副本并摘除旧 Router/runtime 注册；Gin `/api/v1/me/preferences`、MySQL 单域表、ETag/CAS wire 语义和 UI 不变。地址修订旧 handler 仍使用同一 quoted row-version parser。 | KISS-08F3 |
| L5(SNMP Profile/MIB HTTP) | 删除旧 `net/http` profile/MIB handler 与其专用测试；Gin `/api/v1/snmp/{profiles,mib-modules}`、MySQL 单域实现和 UI 不变。依然被旧 network/discovery/poller 使用的 repository/engine 未删，共用 fake 迁至中性 test helper。 | KISS-08F4 |
| L5(Audit Log HTTP) | 在 Gin 恢复旧 `actor_id`/`resource_id` 精确过滤后删除旧 tenant-scoped reader/handler/endpoint test；当前 `/api/v1/audit-logs`、服务端分页、用户名 join 和 UI 不变。遗留包内部尚用的 audit repository 未删。 | KISS-08F5 |
| L5/L6/L7/L9(Backend Runtime) | 确认全库生产零调用后删除 816 行 all-in-one runtime 及仅构造它的测试；server/SNMP/Flow 独立 command 不变。旧 test router 的 health response DTO 临时留在 router 边界；由 runtime 假活的 VM/provider/repository 闭包转为可单独审计。 | KISS-08F6 |
| L5(Aggregate Graph HTTP) | 当前 Gin 已承接完整 CRUD、items/ports、series/data/summary 与 CH 查询后，删除旧 tenant-scoped HTTP handler、router 注入和 endpoint test；旧包内 rollup/repository/domain 留待引用闭包后续删除。 | KISS-08F7 |
| L9/L15(Aggregate Graph rollup) | F7 后旧 rollup、tenant repository/DTO、MySQL 实现、graph-series helper 和专属测试仅自循环，整组删除；同时移除无消费者的旧 rollup YAML/env/config，当前 Gin+CH 路径不变。 | KISS-08F8 |
| L5/I4(Permission CRUD) | 删除与单域 RBAC 冲突的 tenant-scoped 任意授权 HTTP handler/test-router 注入和不可达前端 form/route；当前固定 catalog、role ability 与 user resource grants 保持唯一入口。 | KISS-08F9 |
| L5(BGP HTTP) | 当前 Gin canonical BGP API 已承接 v4/v6、分页/搜索/排序/过滤与资源授权后，删除仅服务已禁用 `/network/...` 别名的旧 tenant-scoped HTTP handler/test；BGP 采集和 domain/repository 保留。 | KISS-08F10 |

**Phase A 剩余**：L16(yaml,须先退役 worker)、S2(随整树删)、L5(api——**经证实须解耦，非独立删**)。

**Phase B 已实测边界（2026-09-15，尝试后回退，未落地）**：把 ~35 个 SNMP/domain 文件 + `mibs/` embed 目录移到 `internal/snmpdomain` 并孤立编译，实测出**精确 cut edges**（比初探大）：
- **可移核心**（自足）：domain.go / traffic_policy.go / metric_catalog.go / metrics_query.go / graph_panel.go(**须拆**) / snmp_collector_types.go / snmp_discovery_engine.go + 全 `snmp_discovery_*` 闭包 / snmp_mib.go(+mibs/) / snmp_poll_runner.go / snmp_poller.go / snmp_query_engine.go / snmp_clickhouse_writer.go(经接口用 snmpch，干净) / snmp.go / snmp_definition_parser.go / snmp_collector_registry.go / snmp_trap_dispatcher.go / snmp_trap_handlers.go / snmp_os_detection.go / snmp_definition_yaml.go / snmp_projection.go / snmp_rate_compute.go。
- **不可移（VM/DB 耦合，留 watchdog）**：snmp_raw_writer.go(VM `VictoriaMetricsClient`)、snmp_collector_repository.go 的 MySQL 实现 `mysql_snmp_collector_repository.go`(761 行)、snmp_collector_discovery_import.go(`NetworkRepository`/`prepareDeviceSensors`)、snmp_definition_importer.go(逻辑)。
- **须额外抽取的小定义**（散在遗留文件，被移动文件引用）：`SNMPConfig`(config.go:275，2 字段)、`FixedTimeWindow`(metrics_service.go:27)、`TrafficViewMode`(traffic_view.go:5)、`SNMPCollectorRepository` **接口**(snmp_collector_repository.go，纯) + `SNMPEventFilter`(mysql_snmp_collector_repository.go)、`SNMPDefinitionImport` 结构(snmp_definition_importer.go)。
- **graph_panel.go 须文件内拆分**：`NewNetworkDeviceOverviewDashboard`/`NewNetworkPortOverviewDashboard`(server 需要)留 snmpdomain；`vmResponseToGraphSeries`(+VM 类型 `VMValue`/`VictoriaMetricsResponse`)留 watchdog。dashboard builder 不用 VM helper，可拆。
- ⚠ **真正的坑=未导出符号跨界**：`collectorStableID`(stable_id.go:20，**未导出**)被移动文件引用——Go 拆包后未导出符号不能跨包也不能 alias，必须整体移动且确认无留守引用者。此类未导出跨界符号是拆包的主要风险，须逐个清点。
- **反向成本**：抽出后 watchdog 约 200 文件仍引用被移符号→须在 watchdog 建 ~65 条 re-export 别名(`type X = snmpdomain.X`/const/var)；类型可 alias，未导出符号(如 collectorStableID)不可 alias 只能全移。

**结论**：Phase B 可行但是**真实多文件重构**（移 ~30 文件 + 5 处小类型抽取 + graph_panel 文件拆分 + 未导出符号清点 + ~65 别名 + repoint 13 server 文件 + 全量 build/vet/test），非一次安全推完。已干净回退（`git reset --hard` + 无 snmpdomain 残留，full build 绿）。⚠ **教训：共享 worktree 中 `git reset --hard` 会毁掉并行 session 的未提交改动**（本次瞬时清掉并行 flow-enrichment-publications 的 server.go/router.go 未提交改，并行 session 已重放恢复）——清理未落地改动应用 `git stash`/`git checkout -- <path>` 限定范围，勿用 reset --hard。KISS-01E 代码删除门的 PB 残留（L19/S1）已清；`internal/watchdog` 整包 + init.sql/migration 整树物理删除仍属 KISS-08（须先 Phase B + 退役 worker + 处理 ~20 遗留测试）。

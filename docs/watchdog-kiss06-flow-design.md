# KISS-06 Flow 单域化与 ClickHouse 查询收敛 — 设计冻结

> 本文件是 KISS-06「设计」门禁的产出：冻结「不动」的数据面/算法，精确列出「只删 tenant」的改动点与三处签名/序列化 wire 决策，并固化两切片提交计划。编码只能对照本冻结做机械改动。
> 归属：`docs/watchdog-kiss-refactor-tasklist.md` §KISS-06。原则：复用既有经测试的强代码，只去 PB/tenant/VM，不重写 decoder、不另造存储、不改计数与生命周期语义。

## 0. 运行时路径核查（决定切片顺序，已 grounding）

- 旧 hub `internal/watchdog`（`NewAPIV1Router`/`ln`）**无任何 `cmd/*` 服务** —— `cmd/` 仅有 collector/worker/agent/install/migrate/rollup/export，无 hub main。故 hub 内 `api_flow_*`（11 文件）、`flow_enrichment_delivery.go`、`flow_operator_query.go`、`export_vm.go`、`api_metrics.go` 均为**死代码**，删除/迁移不会打断在运行的服务。
- Enrichment 版本投递：生产者 = 死 hub（`FlowlnService.FetchDesired` → `ln_publications` 表 `WHERE tenant_id=?`，由未被引用的 `registerFlowlnRoutes` 暴露）；消费者 = **活的** `cmd/watchdog-flow-worker`（`flowworker` version-sync / `version_http.go` "ln control-plane" client）。→ 唯一发 tenant 的生产者是死代码，commit 1 去消费端 tenant 不产生静默混读。
- 新 Gin server（`internal/server`）**当前完全无 ClickHouse client**：仅 `config.go` 有 `ClickHouseConfig`（`Database:"watchdog_flow"` 默认）+ 一处 agent 能力串 `flow.write.clickhouse/`。这是 commit 2 的承重结构缺口。
- CH flow schema 只由 `cmd/watchdog-flow-migrate`（`flowch` immutable ledger，embed `deploy/migration/clickhouse/*.sql`）应用；`deploy/schema/clickhouse/0001_baseline.sql`（SNMP v2 baseline，"No tenant dimension"）**尚无任何 Go 应用它**。

**结论：commit 1（数据面，全部被活 cmd 消费，必须可构建可运行）→ commit 2（查询收敛+接线，退役死 hub）。顺序安全。**

## 1. 冻结（不动，逐字复用）

- **解码**：`internal/flowstream` 全量（sFlow v5/NetFlow v5 fast decode、GoFlow2 v9/IPFIX、template/sampling/fallback）。tenant 只在 `flowworker/decode_adapter.go` 之后进入，flowstream 本身 ~0 tenant 耦合。
- **CH 写入机制**：`flowch` native batch insert（`native.go` proto.Col*）、Storage V2 分区/生命周期（`batch.go`）、rollup 物化 SQL 骨架（`rollup.go`）、count/counter 对账（`reconciliation*.go`）、VPN candidate 生成 SQL 骨架（`vpn_candidate.go`）—— **仅删 tenant 列/键，SQL 其余逐字保留**。
- **富化/版本**：`flowworker` enrich 主路径、EnrichmentVersionCatalog 多版本 as-of 选择、LKG/RemoteVersionSync/断网恢复、WADS 热路径查询（`flowdimension`）—— 仅删 tenant 维度。
- **查询语义**：`flowquery` compiler/admission/report composer、cursor/filter/budget、六报表守恒、detail/facet/overseas/address-set 编译 —— 仅删 scope.tenant_id 与 `WHERE tenant_id`。
- **Prometheus**：`flowmetrics` 抓取目标保留（≠ VM 查询接线，勿删）。
- **配置**：Kafka 坐标、`FlowConfig` 保留/下采样策略、CH 连接参数不变。

## 2. 改动点（只删 tenant，逐包，file:line 为编码 checklist）

### 2.1 CH 物理 schema —— 就地编辑迁移文件（与 MySQL 0005/0008/0011/0013 就地去租户一致）

`flowch.LoadMigrations` 要求 001 起连续编号 + 对文件字节校验和；`PlanMigrations` 仅对**已记录状态**比对校验和。无存量→fresh 集群 `recorded` 为空→改文件字节安全。**不新增 rebuild 迁移**（那是「迁移存量」故事，违背「不 re-store」）。就地编辑这 5 个（去 `tenant_id`/`tenant_ids` 列 + 从 ORDER BY/PARTITION 去 tenant，其余逐字不变）：

- `001_flow_schema.sql`：`flow_records` 去 `tenant_id` 列 + `ORDER BY (tenant_id, toStartOfHour(event_time), record_id)` → `ORDER BY (toStartOfHour(event_time), record_id)`；`flow_aggregate_1m/1h` 去 `tenant_id` 列 + ORDER BY 去首键 tenant_id；`flow_vpn_candidates` 去 `tenant_id` + ORDER BY 去首键。（`flow_ingest_batches` 本就无 tenant，不动。）
- `003_flow_vpn_candidate_generation.sql` / `004_flow_ingest_receipt_audit.sql`（`tenant_ids` Array）/ `010_flow_aggregate_reorder.sql`（reorder 副本表 + `SELECT *`，两侧同步去 tenant 列，`SELECT *` 仍对齐）/ `011_flow_storage_v2.sql`。
- 其余 002/005/006/007/008/009 经核不含 tenant，不动。

### 2.2 Go 数据面（commit 1）—— 结构字段 / 参数 / SQL

- `flowworker/record.go:25`、`enrich.go:68,138,258,343,521`（enrichRecord/selectVersions/blocked 去 tenantID 参数，`VersionBlockedError.TenantID` 删）、`version_catalog.go:32,106,188,204`（`byTenant` map→单键；`ClassificationVersion/DimensionVersion` 去 tenantID）、`version_loader.go:43,61`（JSON `tenant_id` 删，见 §3 wire）。
- `flowch/native.go:253,399,496,534,555,572`（flow_records 列去 tenant_id、receipts 去 tenant_ids、proto.Col 构造删）、`batch.go:100-112,157-159`（TenantIDs 聚合删、storagePartition 去 tenantID 键）、`rollup.go:28,126-317,587-688`（RollupRunner.TenantID、Latest/BucketNeedsRepair/DayStorageCounters 去 tenantID 参、全部 rollup SQL 去 `WHERE/GROUP BY/PARTITION tenant_id`）、`vpn_candidate.go:22,73-223`（TenantID、SQL、**conversation_key SHA256 去 tenant_id 参与项**——单域下 hash 身份变更可接受）。
- `flowdimension/snapshot.go:51,150`、`classification.go:39,57,75`、`address_snapshot_binary.go:68`（TenantID + json tag）。
- `flowplan/plan.go:36`、`signature.go:27,40,190`（签名载荷，见 §3 wire）。
- `flowvpn/bundle.go:51,65`、`runner.go:28,381,398`（TenantID + SQL）。
- `flowstream`：仅 1 处，随手清。

### 2.3 Go 查询面（commit 2）

- `flowquery/query.go:85,303,661`、`overseas.go:68,222-300`、`detail.go:358,910,947`、`detail_facet.go:194`、`address_set.go:56,198`：删 `Scope.TenantID` 字段与 `scope.tenant_id` 校验分支，删 SQL `WHERE (source.)tenant_id = {tenant:String}` 与相关 `GROUP BY/JOIN USING(tenant_id,...)`、`{tenant:String}` 参数绑定。

## 3. 三处 wire / 签名格式冻结决策（「旧 tenant wire 明确拒绝，不静默混读」）

1. **`flowplan/signature.go` 导出器绑定计划签名**：从签名载荷删 `tenant_id` 字段，digest 按去 tenant 后的规范重算。**不兼容旧含 tenant 签名**（旧 wire 拒绝）；同步更新测试签名向量。
2. **`flowworker/version_loader.go` enrichment 版本投递载荷**：删 `tenant_id`。commit 1 去消费端；v2 生产者（enrichment delivery 迁 `internal/server` 时，commit 2）发 tenant-free。唯一 tenant-emitting 生产者（死 hub）不构成运行时混读。
3. **`flowvpn/bundle.go` VPN bundle**：删 `TenantID`（含 json tag）；bundle 序列化/校验去 tenant。

## 4. 查询收敛（commit 2）设计

- **删死 hub 泛化栈**（在 `internal/watchdog`，非 `flowquery`）：`export_vm.go`、`api_metrics.go`、`config.go` 的 `lnConfig`(`query_gateway`)/VM 开关、以及 `NewAPIV1Router` 中相关接线。`internal/flowquery` 本已是纯 CH 引擎，无泛化 provider 可删——只去 tenant。
- **单一 `FlowQueryService`**：在 `internal/server` 内新增 CH client（复用 `flowch` 已用的 clickhouse-go native 依赖），构造一个服务，直接调用 `flowquery` 的 typed 编译器（query/detail/facet/overseas/address-set）+ report composer，向 Gin handler 暴露。Explorer/六报表/明细/导出共用 typed contract，唯一查询后端 = ClickHouse。
- **端口迁移**（死 hub net/http → Gin，接新 RBAC）：`api_flow_records/exports/detail_exports/overseas/filters/saved_filters/geo/vpn_management/vpn_rules/storage_lifecycle/enrichment_delivery`。用新 RBAC 权限；列表走 server 侧分页。
- **CH schema 应用缺口**：为 `internal/server` 增 CH 连接后，flow schema 仍由 `cmd/watchdog-flow-migrate` 应用（不变）；server 只做查询连接，不建表。

## 5. 切片计划（两提交，各自贯穿 8 门）

- **Commit 1 — 数据面单域化**：§2.1 CH 迁移就地去 tenant + §2.2 Go 数据面去 tenant + §3.1/§3.3 wire。门禁：`go build ./... && go vet` + `flowworker/flowch/flowplan/flowdimension/flowvpn/flowstream` 单测（去 tenant fixtures）+ 迁移文件 fresh apply（若有 CH 环境）。可构建可运行（flow-worker/collect/rollup/migrate 编译通过）。
- **Commit 2 — CH 查询收敛 + 接线**：§2.3 flowquery 去 tenant + §4（删死 hub 泛化栈、CH client、FlowQueryService、路由迁 Gin）+ §3.2 wire。门禁：server httptest（真实 CH gate，env 控）+ flowquery 单测 + 六页/Explorer/明细/导出契约。

## 5.1 Hub 级联边界（编码中发现，已定）

去 flow-lib tenant 字段/签名后，`internal/watchdog`（经 `NewBackendRuntime` 被 aggregate-rollup 等 cmd 依赖）有 8 个 flow 控制面文件引用被删字段而**编译**中断：`flow_rollup_jobs.go`(84)、`mysql_flow_enrichment_publication.go`(64)、`flow_rollup_reaper.go`(34)、`flow_storage_jobs.go`(19)、`collector_plan(_delivery).go`、`flow_enrichment_publication.go`、`address_dimension_compile.go`（+ `internal/address/dimension_compile.go` 1 处 `SnapshotBundle{TenantID}`）。

**关键事实**：KISS MySQL schema（`deploy/schema/mysql/`，`ApplyMySQLSchema`）**无 `tenants`/`collector_agents` 表**，而 `flow_rollup_jobs.go` 仍 `FROM collector_agents JOIN tenants` —— 该 hub flow 控制面**在我改动前已对 KISS schema 运行时失效**（遗留、待迁移）。故我的 flow-lib 改动只破坏其**编译**，不改其（既有已坏的）运行时 SQL。

**边界决定**：commit 1 对 hub 只做**有界编译修复**（在 break 点删除对已删 flow-lib 字段/签名的引用/接口参数，保留 hub 自身 tenant 脚手架），使 `go build ./...` 通过；hub flow 控制面的运行时去租户属遗留 hub 迁移/退役（KISS-08），不在 KISS-06 范围。无运行时回退（hub SQL 未动）。KISS-06「可运行」针对 flow 数据面（flow 库 + 活跃 flow 路径），其可运行。

## 6. 非目标

不改 decoder；不做存量数据迁移；不动 `flowstream`/`flowmetrics`（Prometheus 抓取目标保留）；不修 `librenms→ln` 重命名遗留；不重写 CH 查询编译器/report composer（复用 `internal/flowquery` 与 hub report 逻辑，仅去 tenant + 去 gateway 信封搬到 v2）。

## 7. 范围扩展：Hub 退役并入 KISS-06（用户 2026-09-09 决策）

用户选择把 `internal/watchdog` hub 的**整体退役**并入本轮，而非仅迁 flow。mapper 核查的关键约束：泛化 `QueryGateway`/`victoriametrics.go` 被 **非 flow** 代码（`snmp_raw_writer`/`aggregate_graph_rollup`/`billing_metrics`/`metrics_service`/`agent_plan`）经 `NewBackendRuntime` 使用，而 `NewBackendRuntime` 仍被**活的** worker（`watchdog-snmp-collector`/`aggregate-rollup`/`export-worker`）构造。故**不能**在迁 flow 时物理删除这些类型（会断活 worker）。退役只能**按依赖顺序分阶段剥离**，最后才删 hub。

**Hub 退役依赖序（每阶段一个可构建提交，`./...` 全程 green）：**
1. **Flow 剥离（KISS-06 本体，进行中）**：flowquery 去 tenant → `internal/server` 加 CH client + `FlowQueryService` + 迁全部 11 条 flow 路由到 Gin → flow 不再依赖 hub。
2. **SNMP→CH（"下线VM"）**：SNMP 时序 + realtime/range/aggregate 从 VictoriaMetrics 迁到 ClickHouse（`snmp_raw_writer`/`metrics_service`/`aggregate_graph_rollup`/`api_metrics`）。
3. **Billing 剥离（KISS-07）**：billing 脱 VM/hub。
4. **Worker 改宿**：`watchdog-snmp-collector`/`aggregate-rollup`/`export-worker` 脱 `NewBackendRuntime`。
5. **删除（KISS-08）**：`internal/watchdog` + `victoriametrics.go` + 泛化 gateway/DatasetProvider 栈，待无 import 后物理删。

### 7.1 Phase 1 切片（flow 剥离）
- **1a（本提交）**：`flowquery` 去 tenant（Scope.TenantID + 6 编译器 `validTenant` 闸 + `"tenant"` 参数 + `_generation`-latest CTE 的 `SELECT/GROUP BY/USING/WHERE tenant_id`）+ 有界编译修复其消费者（hub 7 文件的 `flowquery.Scope{TenantID}` 去字段、flowquery 自测、flowch 集成测试 Scope）。可构建可提交。
- **1b（下一提交）**：`internal/server` 加 `*flowch.NativeInserter`（`ch-go` native pool，其 `.Do` 即 `flowquery.Executor`；`New()` 内构造，仿 `startAddressLibrary`；`ClickHouseConfig` 补 TLS/pool/timeout 字段，读密码走 secret 文件）+ `FlowQueryService`（直接调 `flowquery.New*Runner` + 移植 `ClickHouseFlowQueryProvider`/report composer，去 gateway/DatasetProvider 信封、去 VM）+ 迁查询核心路由（records/facets、overseas、reports、exports/detail-exports）到 Gin。
- **1c**：迁 MySQL 后端管理路由（saved filters、VPN findings/rules、storage lifecycle、geo）+ enrichment delivery（**机器凭证**鉴权，非 session；移植后即可删 KISS-05 遗留 `internal/watchdog/address_*` 重复）。
- **RBAC**：`internal/server/rbac.go` 已有 `flow.view.*`/`flow.export.*`/`flow.device.*`；补 `flow.vpn.*`、saved-filter、storage-lifecycle、geo-admin ability（重铸 hub 的 `Action*` 常量为 `flow.*` 串）。
- 参照接线：`internal/watchdog/runtime.go:217-262`（单 `*flowch.NativeInserter` 喂所有 runner + provider）；`internal/server` 路由/handler/RBAC 模板见 `handlers_address.go` + `router.go:181-188`。

# KISS-03B 设计：系统 agent 去租户 + 系统遥测入 ClickHouse

日期：2026-09-17。范围：兑现 KISS-03 中延后的「system/container agent 切片」。目标 = 把系统/容器遥测的采集端去多租户、并把其时序落地到 ClickHouse，忠实照搬 KISS-03A（SNMP→CH）的分层。**不含新业务功能**，只补齐延后的存储与查询垂直切片。

## 1. 现状（已核实）

- **发行版 agent `internal/cmd/agent/agent.go`（goreleaser `watchdog-agent`）已干净**：0 `tenant`、0 PocketBase、0 VictoriaMetrics。**本包不动它**。
- **系统 agent `cmd/watchdog-system-agent`** 是延后件，已基本自足（`types.go` 定义本地 `main` 包类型，非 import 遗留 `internal/watchdog`）：
  - `systemAgentScope{TenantID, TargetID}`（`types.go:6-9`）——**TenantID 待删**。
  - `systemSampleBatch{TenantID, TargetID, SampledAt, System, Containers, GPUs}`（`types.go:17-24`）——**TenantID 待删**。
  - 采集：`systemResourceSample`(CPU/Mem/Disk/NetIn/NetOut)、`containerSample`(每容器 CPU/Mem/NetTx/Rx)、`gpuSample`。
  - `client.go` 把批次发往 hub；`main.go:232` 用 `plan.Agent.TenantID`。
- **PocketBase 已彻底移除**：agent 代码全域 0 PB 引用 → 「去 PocketBase」为**空操作**。
- **KISS 路径下当前无系统遥测存储**（既非 VM 也非 CH）：`internal/server/agents.go:977` 仅登记 `system.samples/` 能力标签；系统 plan/sample 的入库/查询实现随 KISS-03B 延后，**从未迁到 CH**。即「VM→CH」实为「**在 CH 上实现系统遥测**」，非查改替换。
- **模板已就位**：`internal/snmpch/`（store/write/query/aggregate/rollup）+ `deploy/migration/clickhouse/012_snmp_telemetry.sql`（`snmp_samples` 表）是 KISS-03A 的成熟范本，直接照搬结构。
- `internal/server` 当前工作树干净（并行 session 已提交/暂停），但可能恢复——见 §5 风险。

## 2. 目标分层（对标 KISS-03A）

| 层 | KISS-03A（SNMP，已做） | KISS-03B（系统，本包） |
|---|---|---|
| 采集端 | `watchdog-snmp-collector` | `cmd/watchdog-system-agent`（去 TenantID） |
| CH schema | `snmp_samples`（012） | 新增 `system_samples`（+容器/GPU），模 012 |
| CH store | `internal/snmpch`（store/write/query） | 新增 `internal/systemch`（同结构） |
| 写入 | `ClickHouseSNMPRawWriter` | agent → server ingest → `systemch.WriteSamples` |
| server 入口 | 采集器直写 | `POST /api/v1/agents/:id/system-samples`（agent 鉴权） |
| 查询/展示 | SNMP 图表 CH 查询 | host/target 监控页 CH 查询 |

## 3. 工作分解（WBS）

### A. 去多租户（采集端 + 计划契约）
- 删 `cmd/watchdog-system-agent/types.go` 的 `systemAgentScope.TenantID`、`systemSampleBatch.TenantID`；`main.go:232` 去 `plan.Agent.TenantID`；`client.go` 批次载荷去 tenant 字段。
- 系统 agent **plan 契约**去 tenant：server 下发的系统 agent plan（`internal/agentplan` 签名载荷 / `internal/server` 计划构造）删 `TenantID`（与 KISS-04 已做的其它 agent 一致，只针对 system kind）。
- `cmd/watchdog-system-agent/config*.go`、`agent_plan_test.go` 同步。

### B. 系统遥测 → ClickHouse（垂直切片）
1. **CH schema**：`deploy/migration/clickhouse/NNN_system_telemetry.sql` 建 `system_samples`（`observed_at, device_id/target_id, agent_id, metric, value`——或按 snmp_samples 的宽表列式），另建 `container_samples`、`gpu_samples`（或统一表 + `entity_kind`）。列/编解码/分区照 012。
2. **`internal/systemch` 包**：`store.go`(Ready 校验 schema、WriteSamples)、`query.go`(rate/chart 查询)、`aggregate.go`（按需）——照 `internal/snmpch` 结构，复用 `internal/flowch.NativeInserter`（同一 CH 池）。
3. **server ingest**：`internal/server` 加 `POST /agents/:id/system-samples`（`authenticateAgent` 鉴权，body=去 tenant 的 systemSampleBatch）→ `systemch.WriteSamples`；`router.go` 在 agent 组内注册。
4. **agent client**：`cmd/watchdog-system-agent/client.go` 改 post 到新端点。
5. **查询/展示**：`internal/server` 加 host/target 系统时序查询 handler（读 `systemch`）；前端 host/target 监控页接该查询（当前若为空/占位，此处补齐）。

### C. 去 PocketBase
- 无需处理（已核实 0 引用）。清单保留此结论以核销。

## 4. 验收（8 门，对标 KISS-03A）
- 设计（本文档）；编码（A+B）；单测（去 tenant 归一、systemch write/rate、ingest 鉴权）；真实集成（空库→装管理员→注册 system agent→采样→CH→host 页查询非空）；变更设计/测试；回归（`go build ./...`/vet/race、前端 build）；文档；已提交门（schema+systemch+ingest+agent+前端接线一个可独立构建提交，不夹带并行 flow WIP）。
- 静态门：clean schema 0 `tenant_id`；系统遥测活写/查路径仅 ClickHouse。

## 5. 依赖与风险
- **触及 `internal/server`**——并行 session 正在此区域做 flow-enrichment-publications。**强制**：本切片在**独立 git worktree** 执行，或与并行 session 明确错开文件；**禁用 `git reset --hard`**（曾误清并行未提交改，见 [watchdog-kiss-cleanup-ledger.md](watchdog-kiss-cleanup-ledger.md) §6 教训）。
- CH schema 走 `deploy/migration/clickhouse`（install 时应用）；server 启动依赖 CH（见审计主题 2 的启动语义）。
- `internal/agentplan` 若被多种 agent 共用，去 system 的 TenantID 需确认不影响其它 kind。

## 6. 相关小项（独立，需用户输入）
- **goreleaser/打包去 Beszel/henrygd**（与「已删 LICENSE、按原创口径」一致）：`.goreleaser.yml:134 maintainer: henrygd <hank@henrygd.me>`、`:129-132` Beszel 描述、`:169`、`:191 copyright: 2025 henrygd`，及 `supplemental/debian/copyright`。改品需用户提供**维护者名/邮箱**与**包描述**，不臆造身份。不属 KISS-03B 8 门，可随时单独做。

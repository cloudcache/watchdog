# Watchdog 全项目复核（代码 · 设计 · 文档，2026-09-28）

范围：全部代码、49 份设计/任务/审计文档。方法：10 个只读核验子任务并行（5 个按领域核对任务清单、1 个核对审计发现、2 个核对设计与代码差异、2 个代码审查），**每条结论必须引用提交或 `path:line`，不以文档自述为准**；严重项由主会话再次对照源码复核。生产（.18）事实只采用本轮运维实测过的（event_time 索引约 6× 裁剪、人工 raw 保留、磁盘事故修复、Flow 估算与 sFlow 计数差约 2%）。

## 1. 仓库状态

- 工作区：复核前干净；本次改动只涉及 `docs/` 与 5 个测试文件（见 §2）。
- **未推送**：`main` 领先 `origin/main` **793 个提交**，近期全部工作（5m 分层、agent 简化、文档）只在本地。
- 构建与测试：`go build ./...` 通过；全模块单元测试 30 个包全部通过；`internal/server` 在**真实 MySQL 8** 上整包 **206 通过 / 0 失败 / 5 跳过**（跳过项需 ClickHouse/SNMP 设备/真实二进制）；前端 `tsc`、`biome`（183 文件）、agent-control 单测、lingui 编译均通过。
- `go vet` 仅 1 条既有告警：`internal/flowstream/inlet_test.go:70` 按值复制含 mutex 的 protobuf（测试代码，来自 `1005b5e36`）。

## 2. 复核中修复的代码（均为测试）

| 文件 | 原因 | 验证 |
|---|---|---|
| `internal/server/agent_plan_integration_test.go` | `aa870f65d` 后 `createAgent` 拒绝 `token` 字段，测试未配共享 token（**Stage 2 回归**，因 DSN 门禁未被发现） | 真实 MySQL 8 通过 |
| `internal/server/flow_enrichment_publications_integration_test.go` | 同上；wrong-kind 用例改为依赖 `authenticateFlowWorker` 的 kind 校验 | 真实 MySQL 8 通过 |
| `internal/server/flow_vpn_publications_integration_test.go` | 同上 | 真实 MySQL 8 通过 |
| `internal/server/flow_storage_lifecycle_integration_test.go` | 期望值过期：`00be2cdb5`（09-22）给归档作业加了日级 rollup，单测已改、此集成测试漏改（25→26 次调用），自 09-22 起一直失败 | 真实 MySQL 8 通过 |

## 3. 未修复的代码发现（按严重度）

> 2026-09-29 修复进展：高 1/2/3/4/5/7/9 与中项中的 draining、`hour_lookback`、自注册审计已修（见各条括注）；6/8/10 待决定。验证：`internal/server` 在真实 MySQL 8 上整包 210 通过 / 0 失败 / 5 跳过（跳过项同前）；四个 agent 真实二进制的注册/计划/LKG/吊销集成测试（`TestAgentProcessesRegisterApplyLKGAndRevocation`，另需 `WATCHDOG_AGENT_PROCESS_TEST_MYSQL_DSN`）通过，含旧 enrollment 参数兼容。

**高**

1. **注册可覆写既有 agent 的 kind 与绑定**（`internal/server/agents.go` enrollAgent upsert）：`kind=VALUES(kind)`、绑定字段无条件覆盖。持共享 token 者可把某 flow_worker 改成别的 kind（其 flow-worker 接口随即 401）；agent 重启时若未带 `device_id` 会清掉管理员在 UI 设的绑定；已吊销行的 kind/绑定仍可被改。修：已存在行禁止改 kind（409），未提交的绑定字段保持原值。（2026-09-29 已修：事务内 `SELECT … FOR UPDATE`，改 kind 返回 409 `immutable_kind`；重注册只刷新版本/能力，绑定只补空不覆盖，不改 name/status；新建与补绑定写审计。集成测试 `TestDeviceAndAgentAPI` 覆盖。）
2. **SNMP 运行上报乱序**（`cmd/watchdog-snmp-collector/main.go` 每轮 `go reportPoll` + `recordAgentStatus` 无条件 `last_run_at=?`）：旧的失败上报晚于新的成功上报落库时，health 被错置为 error、`last_run_at` 倒退。修：单一顺序上报协程；服务端 `last_run_at=GREATEST(...)`。（2026-09-29 已修：collector 改为单协程按轮次顺序上报、队列满则丢弃不阻塞轮询、报告携带本轮结束时间；服务端 health/last_error/last_run_at/last_success_at 只随更新的 run 前进，旧报告仍计入 run/failure 计数。集成测试覆盖。）
3. **报表读路径绕过连接池拆分**（`internal/server/flow_archive.go:31` runner 建在 batch 池；读路径 `flow_archive.go:111,143`、`flow_report_endpoints.go:300`）：batch 池默认仅 1 个连接，每个报表面板的覆盖检查都排在后台 rollup/归档/删除作业之后（最长 15 分钟）。修：只读的 marker/coverage 方法走 interactive 池，只有 `Run()` 走 batch 池。（2026-09-29 已修：`s.flowRollup` 只被这 3 处读路径使用，改建在 interactive 池；后台作业仍用 batch runner。）
4. **5m 聚合没有迟到修复**（`internal/server/flow_hot_rollup.go` scanFiveMinute；`BucketNeedsRepair` 从未用于 5m）：1m 被迟到数据修正后 5m 不重建，会少计。潜伏缺陷（目前无读者）；`021` 迁移注释声称 repair 工具"原样适用"并不属实。（2026-09-29 已修：某小时任一 1m 桶的 `generated_at` 晚于该小时 5m 的 `generated_at` 即重派生，新 generation 高于旧值；每轮只读两次 marker（原为每小时两次）；回看上限取 `min(hour_lookback, 48h)` 即 1m TTL。单测覆盖稳定不重写/修复后重派生/回看上限。）
5. **Stage 2 升级陷阱**（均已核实，已写入 `watchdog-release-readiness-2026-09-28.md` §1.3 与安装手册）：生产 agent 的旧 `wda_…` 凭据会被拒 → 401 循环重启；旧 env 中的 `-agent-enrollment-token-file` 会让新二进制启动即以状态 2 退出。建议：保留一个版本的"已废弃、忽略"参数兼容层。（2026-09-29 已修：四个二进制接受并忽略该参数（打印废弃警告）；`agentplan.Register` 把 401/403 映射为 `ErrUnauthorized`，被拒的 token 以状态 0 停止而非循环重启。仍需按 §1.3 顺序换共享 token，否则该 agent 停止采集。）
6. **system agent 采集链断开**：服务端 `/api/v1/system-agents/:id/{plan,samples}` 已在 `71e9bf39f` 删除，system agent 仍在请求，每轮 404——系统遥测完全未入库。（2026-09-29 未修，待决定：旧实现写入已下线的 VictoriaMetrics，恢复需要新的 ClickHouse 表 + 采样/计划接口 + 查询与主机页，属于功能重建而非补丁；另一选项是停止发布 system agent。）
7. **SNMP 计费 5m 桶永久缺口**：`snmp_interface_traffic_5m` 只由 collector 在每轮后重建"上一个"已关闭桶，停机期间的桶永不补算，无回填任务。（2026-09-29 已修：collector 重建自上次以来的每个已关闭桶（旧到新，每轮至多 12 个），失败或超长的轮次不再漏桶；进程启动时从 24h 内最新已发布桶之后续建，补上停机前只采了一半的桶。单测覆盖。）
8. **operator 查询与历史重分类只认 v1 pair**（`internal/server/flow_operator_query.go:74-154`、`flow_reclassification.go:132-179`）：v2 部署时期的事实会被漏计或返回 503；门禁也不读 v2 deployment ACK。（2026-09-29 未修，已出方案待决定：按 worker 复刻"最新已安装且 effective_from ≤ t"规则，v1 pair 与 v2 部署取并集钉住，约 250 行生产代码 + 350 行测试，落在你正在改的 flow 查询文件；重分类先加 409 护栏，完整 v2 重分类需迁移 0051。）
9. **`flow.export.*` 权限未被强制**（`internal/server/flow_exports.go:154,540-543`、`flow_export_vpn_findings.go:51`）：导出只校验 `flow.view.<层>`/`flow.vpn.view`——安全缺口。（2026-09-29 已修：明细/查询/报表导出创建时要求 `flow.export.<层>`，作业执行时再校验一次（入队后被收回权限即终止）；VPN findings 导出含 raw 层端点，额外要求 `flow.export.raw`——默认 operator 角色因此失去该导出，需要时给它加 `flow.export.raw`。前端导出按钮未按权限隐藏，无权限时显示 403 提示。）
10. **许可证冲突（发布阻断）**：readme 声称 MIT，而 Akvorado 派生文件为 AGPL-3.0-only，仓库无根 LICENSE/NOTICE。（2026-09-29 未修，待决定：属于法律选择。）

**中**

- 自注册不写审计（`agents.go` enrollAgent）；`agent_runs` 无保留期（flow 每分钟 1 行/agent，SNMP 最快每 5 秒 1 行）。（2026-09-29：审计已随 §3-1 补上；`agent_runs` 保留期未处理。）
- `authenticateFlowWorker` 拒绝 `draining`，与其余 agent 接口"非 revoked 即可"不一致（另有 3 处同类判断）。（2026-09-29 已修：draining worker 可继续拉取/确认自己的 feed。其余同类判断都是给新工作选目标（部署、设备绑定、发布目标），排除 draining 是正确的，保持不变。）
- SNMP 三张 CH 表无 TTL，`/retention/policies` 只存不执行；SNMP profile secret 明文存储。
- `hour_lookback` 默认 72h 大于 1m TTL 48h：48–72h 前的小时永远补不出 5m。（2026-09-29 已修：5m 扫描回看取 `min(hour_lookback, 48h)`，不再为不可能补出的小时查询。）
- raw 列 codec 只在生产手工 ALTER，未进迁移——新装库拿不到。
- rollup/reconciliation 指标无生产调用方，从未导出；GoFlow2 回落路径丢弃 sFlow counter。
- worker CLI 仍接受仅 mTLS 配置（服务端已不认，必然 401）；`021` 注释称 interface 5m 由 DropArchiveMonth 回收，实际未实现。
- 生产 worker 锁定 v2 后收到 404 不回退 v1（卡在 LKG 11）；地址快照激活按绑定自动发布 v2（偏离"显式选择"设计）；`011` 把旧事实 `fact_schema` 一律写 3。
- 找回密码令牌写入服务端日志；遗留 `internal/cmd/agent`（PB 时代 WebSocket 客户端）仍被 `make build`/goreleaser 构建发布，goreleaser 还引用不存在的 `supplemental/debian/copyright`；`export_tasks`、`idempotency_records` 两表零引用；`config/watchdog.yaml` 的 `definitions_dir` 写死开发机路径。
- 设计取舍（非缺陷，已写入文档）：共享 token 不绑定具体 agent id，"只能读自己"的机器接口只能按 URL id 过滤。

## 4. 任务状态更新（9 份清单：核实 257 条，实际改动 239 条；另 18 条是对已勾选项的确认，无需改动）

| 文档 | 复核前 未完成/已完成 | 复核后 | 说明 |
|---|---|---|---|
| `watchdog-flow-data-lifecycle-review-2026-09-24.md` | 139 / 7 | 86 / 60 | 完成 9；作废 44（多为用户 §11.1 砍掉的范围、§9 被 §11.4 取代）；部分完成 32 |
| `watchdog-flow-perf-audit-2026-09-19.md` | 24 / 20 | 24 / 20 | 部分完成 17；已被后续实现取代 2 |
| `flow-storage-v2-change-plan.md` | 6 / 17 | 6 / 17 | 部分完成 2；已被取代 1 |
| `watchdog-release-readiness-2026-09-28.md` | 11 / 0 | 10 / 1 | 完成 1；部分完成 7；另新增 §1.3（agent 升级顺序） |
| `flow-worker-control-plane-repair-plan.md` | 84 / 6 | 80 / 10 | 作废 4；部分完成 35；已被取代 3 |
| `flow-module-tasklist.md` | 33 / 306 | 30 / 309 | 完成 2；作废 2；**改回待办 1**（operator 查询 v1-only）；部分完成 11；已被取代 7；备注 5 |
| `flow-address-artifact-deployment-design.md` | 25 / 24 | 31 / 18 | 完成 1；**改回待办 7**（6 条测试夸大 + 激活自动发布偏离设计）；部分完成 10；备注 1 |
| `watchdog-kiss-refactor-tasklist.md` | 22 / 174 | 21 / 175 | 作废 1；部分完成 10；已被取代 6 |
| `platform-refactor-tasklist.md` | 21 / 232 | 5 / 248 | 作废 14；完成 2；部分完成 2；整篇加归档横幅（约 88 条 [x] 已被 KISS 删除/替换） |

每条变更都在原行末尾附 `（2026-09-28 复核：…）` 注记与证据。"线上已验证"只用于本轮在 .18 实测过的事实。

## 5. 审计文档复核（9 份，共 518 条发现）

每份审计文档标题下新增一行汇总，文末新增逐条「复核状态（2026-09-28）」表（证据 + 说明）：

| 文档 | 已修复 | 部分 | 未修复 | 作废 |
|---|---|---|---|---|
| `watchdog-agent-lifecycle-audit-2026-09-28.md` | 2 | 4 | 19 | 2 |
| `watchdog-flow-collector-worker-code-review-2026-09-19.md` | 13 | 29 | 57 | 1 |
| `watchdog-flow-remediation-audit-2026-09-20.md` | 44 | 56 | 76 | 3 |
| `watchdog-product-review-2026-09-22.md` | 5 | 4 | 59 | 1 |
| `watchdog-storage-lifecycle-review-2026-09-22.md` | 23 | 11 | 12 | 2 |
| `watchdog-kiss-audit-2026-09-15.md` | 13 | 0 | 3 | 2 |
| `runtime-budget-audit.md` | 2 | 1 | 8 | 0 |
| `snmp-gin-route-migration-audit.md` | 25 | 2 | 2 | 14 |
| `self-host-consolidation-audit.md` | 16 | 3 | 1 | 3 |

## 6. 设计文档与代码对齐（217 处差异）

- **就地改写**（面向操作者、必须准确）：`watchdog-install.md`（共享 token、升级陷阱、同源反代、SNMP 发现归属、collector/worker 职责、SNMP 无保留期、`WATCHDOG_AGENT_SHARED_TOKEN`）、`agent-plan-delivery-design.md`（9 处）、`watchdog-kiss-architecture.md` §5.3（机器 API 清单、health 派生、draining 语义、绑定表、ACK 状态、system agent 断链）、`watchdog-local-dev.md`、`watchdog-release-readiness-2026-09-28.md`。
- **「与代码的差异（2026-09-28 复核）」节**（标题下一行指引 + 文末逐条，**以代码为准**，正文保留作设计历史）：22 份——KISS 架构（23）、flow-module-design（27）、SNMP collector 设计（16）、5m 设计（10）、ix-scale 设计（10）、地址查询计划（9）、生命周期复审（7）、kiss03（7）、reliability remediation（7）、flow-pipeline-adr、direction-requirements、contract-restoration（各 6）、VPN 设计、collector-worker 再设计（各 5）、cleanup-ledger（4）、kiss03b、workbench、PB 删除清单（各 3）、fastpath、scale-baseline、archive-map（各 2）、kiss07（1）。
- **归档横幅**（整篇已被取代）：`watchdog-platform-module-architecture.md`、`snmp-collector-task-list.md`、`collector-fleet-rollout-design.md`、`pocketbase-mysql-fork-design.md`（未采纳）、`storage-consolidation.md`、`watchdog-kiss06-flow-design.md`、`platform-refactor-tasklist.md`。

"代码优于设计"的主要方面：热层 1m/5m/1h + marker 覆盖前缀读聚合、两阶段 generation 发布、v2 定向部署、全局共享 token、运行上报派生健康、WADS 取代 flow-geo、审批制 raw 删除与 tombstone barrier、scale_ppm 校准（估算差约 2%）、单查询方向拆分、SNMP 自动发现。

## 7. 需要你决定

1. **推送**：是否把 793 个本地提交推到 `origin`（或改为以签名 tag/制品发布）。
2. **Flow worker 控制面方向**：`flow-worker-control-plane-repair-plan.md` 的"逐设备 desired"方案与已提交的 composite v2 部署方向相反；新前端契约测试已把 composite 固化，该计划自己的变更冻结（L594）也未被遵守。以哪个为目标？
3. **代码发现是否现在修**：建议顺序——§3 高 1/2/4（均为本轮自己引入）→ 5（升级兼容参数）→ 3（连接池）→ 9（导出权限）→ 8（v2 operator 门禁）→ 6/7（system agent、SNMP 补桶）→ 10（许可证）。
4. **生产运维（需你在主机执行）**：vda3 存储策略、部署 5m 分层与 agent 改动（按 §1.3 顺序）、nginx `/api/` 反代端口（8090 vs 8091）。

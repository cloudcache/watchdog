# Flow Storage V2 重大变更设计与实施计划

状态：**Accepted / guarded implementation complete; destructive cutover locked**
生效范围：Flow Kafka→worker→ClickHouse 写入、审计对账、明细查询、导出、原始数据老化与 downsample。
替代关系：本文是上述范围的唯一生效设计；`flow-pipeline-adr.md`、`flow-module-design.md`、`flow-reliability-remediation.md` 中与 30 天原始 TTL、实时 1m/1h rollup、`record_id`/`ingest_batch_id`/内容 checksum 冲突的段落均按本文解释。001–010 已发布迁移保持逐字节不可变，只允许新增向前迁移。

## 1. 变更目标与边界

本次不是三个互不相关的小优化，而是一次存储契约升级：

1. 原始事实不再由硬编码 `TTL 30 DAY` 删除；数据是否老化由租户生命周期策略和 operation job 决定。
2. 1m/1h 聚合不再在数据到达后立即全量双写；原始在线期直接查询全精度事实，只有达到策略年龄的封闭分区才 downsample。
3. 热写路径不再计算 `sourceID`、`record_id`、`dimension_fingerprint`、insert-block checksum 或 batch ID hash。
4. Kafka 天然坐标成为事实身份；审计保留，但从内容 hash 对账改成逐 Kafka 消息的 count/counter 守恒与 offset 覆盖检查。
5. 明细 cursor、导出快照和 reconciliation 不再依赖 `record_id`。

“去 hash”只约束 Flow 每 datagram、每 record、每查询的热路径。以下 checksum 不在删除范围：ClickHouse part 自校验、迁移文件防篡改、地址库发布物验签、认证/密码、导出文件下载完整性。operation job 的低频幂等键也不属于 Flow 逐记录热路径；是否改成结构化唯一键由平台任务单独迁移，不能顺手削弱全局作业幂等性。

本次不夹带 [flow-address-query-plan.md](flow-address-query-plan.md) 的“停止写分类列/全面改为 `dictGet`”切换。migration 011 为无损换表，保留现有事实列，先解决身份、生命周期和查询源切换；地址字典切换必须以新的 schema/generation 和 raw/archive 同口径守恒单独实施，不能在这次维护窗口同时改变分类算法而失去可对账基线。

## 2. 冻结的 V2 身份契约

### 2.1 source stream

`source_stream_id` 是 Kafka 集群 + topic incarnation 的不可复用管理标识，1–128 个受限 ASCII 字符。它不能只取 topic 名；topic 删除后重建仍须生成新的 stream ID。正常生产启动必须显式配置该值。迁移存量无法恢复 Kafka topic UUID，统一标为 `legacy:<kafka_topic>`；切换时继续消费原 topic 的 worker 必须使用同一个 legacy ID，换 topic 时必须换新 ID。

### 2.2 record identity

一条事实的唯一身份固定为：

```text
(source_stream_id, kafka_partition, kafka_offset, record_index)
```

`record_index` 是一个 RawFlow/Kafka 消息内、解码后从 0 开始且连续的序号。`tenant_id`、`event_time`、target/binding 与派生维度不是身份，但为了查询裁剪可位于 MergeTree 排序键前部；其前提是由已签名 `registry_version` 和 RawFlow 事件确定、重放不可漂移。需要修正事实时必须使用显式更高的 repair generation，不能用修改 identity 的方式“覆盖”。

V2 `flow_records` 的去重键为：

```text
(tenant_id, toStartOfHour(event_time), source_stream_id,
 kafka_partition, kafka_offset, record_index)
```

`ReplacingMergeTree(ingest_generation)` 负责跨 Kafka 至少一次重放的最终收敛。实时精确查询仍使用 `FINAL` 或等价 `argMax`；后台 merge 不是读时去重保证。

### 2.3 insert block 与 receipt

ClickHouse insert block 只是吞吐批次，可随行数/字节预算变化，不是业务身份。自然 dedup token 可直接编码 block 的首尾 Kafka 坐标；它只优化 ClickHouse 有限 dedup window，永久幂等仍由事实排序键保证。

`flow_ingest_batches` V2 实际语义改为 **source-message receipt**：每个允许提交 offset 的 Kafka 消息都必须有且只有一行，唯一键为 `(source_stream_id, kafka_partition, kafka_offset)`。`message_disposition` 区分 `persisted/template_missing/empty/decode_rejected/mapping_rejected`；后四类是计数器为零的 receipt-only 决策，不是假装成丢数。可重试失败不得写拒绝 receipt，也不得提交 offset。事实仍以最多 50,000 行的大块写入；一个块可对应多个 receipt。这样 worker 升级、批大小调整或重放时 receipt 身份不变。

receipt 保存：topic 诊断名、tenant IDs、record count、raw/estimated bytes 与 packets、estimated-valid record count、事件时间范围、worker/receipt schema 和 generation；不保存 batch hash 或内容 checksum。

## 3. 对账能力与保证边界

scanner 以 Kafka committed-next-offset 为右开水位，按 `(stream, partition, offset)` 有界扫描：

- receipt 存在、事实不存在：`missing_records`；
- 事实存在、receipt 不存在：`missing_receipt`；
- receipt/fact topic、record_index 从 0 连续性或 offset 覆盖不一致：`identity_mismatch`；
- record count 不同：`count_mismatch`；
- raw/estimated 字节、包或 valid count 不同：`counter_mismatch`。

scanner 必须逐个枚举有界的 `[next_offset, committed_next_offset)`，不能只遍历查询返回的行；否则 receipt 和事实同时消失的 offset 会成为不可见空洞。count 能发现丢/重记录，counter 能发现总量漂移，连续 offset 覆盖能发现整条消息缺口。删除内容 checksum 后，不再声称能发现“记录数和所有计数总和都不变，但某个非计数字段被错误改写”的语义腐败；ClickHouse part checksum 只覆盖存储介质损坏，也不能替代业务语义校验。字段映射准确性由 decoder 差分 corpus、schema contract、抽样明细回放和版本化 repair 测试承担。

性能上，V1 对每条事实至少做一次完整 SHA-256，并再次逐字段吸收进 batch checksum；V2 两者均为零。代价是 MergeTree 键从 32-byte 不透明摘要改为可追溯坐标，其中 partition/offset/index 为 16 bytes，另含 LowCardinality `source_stream_id`。因此不能把收益描述成“主键恒定缩小一半”：确定收益是 CPU/alloc 降低和可追溯性提高，真实主键压缩、写放大与吞吐必须用同一 corpus 的端到端压测判定。可靠性不依赖哈希碰撞假设，而依赖 Kafka 坐标不复用、source stream incarnation 管理、ReplacingMergeTree generation、逐消息 receipt 和 count/counter 守恒。

## 4. 生命周期与 downsample 状态机

DDL 不再硬编码原始、receipt、1m 或 1h 的 TTL。默认行为是**不自动删除**。每个租户的已发布策略至少包含：

| 字段 | 约束 | 语义 |
|---|---:|---|
| `raw_retention` | > 0 | 原始事实作为查询主源的最短在线时长；不是代码常量 |
| `downsample_resolution` | 固定 1h（V2） | 存储归档粒度；日/周/月只是查询展示步长，避免再建一套 1d 事实 |
| `archive_retention` | 0 或 > raw | 0 表示无限；否则由作业删除归档分区 |
| `late_arrival_window` | >= 0 | 分区封闭后继续接纳迟到数据的时间 |
| `delete_grace` | > 0 | downsample 验证通过到删原始分区的最短保护期 |
| `max_partitions_per_run` | 1..N | 每轮扫描/删除预算 |

一个 UTC 日只有在该日末尾同时越过 `raw_retention` 和 `late_arrival_window` 后才允许排队；等价实现为 `day < utcDate(now - max(raw_retention, late_arrival_window))`。不能因为迟到窗口已过就提前 downsample。每个 `tenant + UTC source partition/day + policy_version` 依次经历：

```text
hot -> sealed -> downsample_written -> reconciled -> delete_eligible -> raw_deleted
```

只有同时满足以下条件才能进入 `raw_deleted`：源分区早于 raw retention 与迟到窗口、目标 generation 已 durable、目标 count/counter 与源守恒、receipt scanner 已越过该分区、delete grace 已到期。失败/取消只保留或重试当前 operation job，不推进水位。删除使用显式分区操作并写审计，不依赖后台 TTL 的不可见时机。

为使上述删除在物理上成立，V2 原始表必须按 `(tenant_id, UTC day)` 分区，1m/1h 表按 `(tenant_id, UTC month)` 分区；只按日/月分区会迫使租户策略走大范围 mutation，不能作为验收方案。writer 的单块分区预算按“不同 tenant-day 数”计算，而不是只数日期。receipt 按写入月保留，独立于事实删除；它承担 Kafka 审计，不能跟随某一 tenant 的原始分区误删。

实时 `flow_rollup` scheduler/reaper 在 V2 切换时关闭；旧聚合仅作迁移期兼容读。新的 aging/downsample handler 复用平台 `operation_jobs` lease/heartbeat/cancel/retry，不在 flow-worker 内增加状态机。

归档 generation 固定编码为 `(policy_version << 32) | repair_attempt`。这保证新策略的首次构建严格高于任何旧策略 repair；迁移 011 会拒绝 legacy generation 已进入高 32-bit 命名空间的数据库。任务幂等键包含 UTC 日、策略版本和 repair attempt，取消、租约接管或失败重试不能复用更旧 generation 覆盖新结果。调度持久化顺序固定为 `enqueue operation job -> bind sealed partition state/job/generation -> advance watermark`；任一步失败重扫都由幂等键收敛，绝不能在分区状态存在前推进水位。被清理的 terminal job 以 `job.id IS NULL` 继续进入 repair 候选，不能把 UTC 日永久搁浅。

`raw_delete_enabled` 当前必须为 false，创建、修改和发布三条路径都会 fail closed。原因不是 downsample 尚不可用，而是物理删除还缺少 durable Kafka committed-offset 日覆盖证明；仅有 ClickHouse receipt 不能证明 Kafka 已提交窗口。`archive_retention` 同样只先保存为策略意图，不启动归档删除作业。两类删除在独立故障注入、审计与恢复门禁完成前不得由 TTL 或人工 SQL 绕过。

## 5. 查询与导出切换

原始在线期的总览、多维、源/目的 IP、境外和 VPN 查询均以 `flow_records FINAL` 为事实源，并按请求时间范围选择稳定步长；不得因移除实时 rollup 把自定义时间范围重新限制为 1m/1h。

标准单维、方向和境外 1h 查询使用管理库已验证的连续 archive boundary：`[from, boundary)` 读最新 generation 的 1h 归档，`[boundary, to)` 读原始事实，两段互斥后统一执行一次 TopN/other，禁止分别截断再相加。1m 查询始终读原始事实。管理库若没有从查询首日开始的连续 `reconciled` 状态，boundary 就是 `from`，不得按年龄猜归档完整。2–4 维联合 tuple 暂时仍读原始事实，因为单维归档无法重建维度相关性；超出原始在线窗口前必须先发布专用联合索引，否则明确拒绝，不能返回伪精确结果。

明细全序和 cursor V2：

```text
(selected_sort_value, event_time, source_stream_id,
 kafka_partition, kafka_offset, record_index)
```

默认不含 `selected_sort_value`。API 返回结构化 `source_coordinate`；旧 record-id cursor 在切换后以“cursor contract expired”拒绝，不能静默从头翻页。同步 Flow query gateway 不再为每次请求 JSON 计算 `query_hash`。异步导出属于低频控制面，仍保存规范化 query JSON、版本、as-of、权限快照及其完整性 hash，用于幂等重试和可复现；导出文件自身 SHA-256 也保留用于下载完整性。

## 6. 当前实现映射

| 契约 | 实现位置 | 当前门禁 |
|---|---|---|
| CH 自然坐标表、逐消息 receipt、无固定 TTL、legacy 保留 | `deploy/migration/clickhouse/011_flow_storage_v2.sql` | 维护窗口执行；worker readiness 拒绝 V1/V2 混用 |
| worker `source_stream_id`、自然坐标写入、无逐记录 hash | `cmd/watchdog-flow-worker`、`internal/flowworker`、`internal/flowch` | production 必须显式提供 stream incarnation ID |
| count/counter reconciliation | `internal/flowch/reconciliation*.go` | 调用方必须提供 Kafka committed-next-offset；扫描有界且不完整不报伪零 |
| 策略、UTC 日状态、水位和 CAS API | MySQL migration 056、`internal/watchdog/*flow_storage*` | `raw_delete_enabled=true` 稳定拒绝 |
| aging/downsample operation job | `internal/watchdog/flow_storage_jobs.go`、`internal/flowch/rollup.go` | 只写 1h archive；守恒不通过进入 failed/repair |
| raw/archive 混合查询 | `internal/flowquery`、`internal/watchdog/query_provider_flow*.go`、`api_flow_overseas.go` | 只采用连续 reconciled boundary；1m/raw 与 1h/hybrid 语义分开 |

开关固定为 `flow_storage.enabled`；legacy `flow_rollup.enabled` 仅供回滚观察窗，两者同时为 true 时配置校验直接失败。迁移本身不自动启动 worker、hub、downsample 或删除作业。

## 7. 迁移和发布顺序

这是维护窗口切换，不声称无停机：

1. **预检**：所有 001–010 migration applied；Kafka 无不可解释 lag；记录各 partition committed-next-offset；禁止在 dirty migration 库上继续。
2. **停写**：停止 flow-worker；冻结 Flow repair/downsample/旧 rollup job；hub 查询可保持只读。
3. **向前迁移**：创建 V2 staging 表；从旧事实按 `legacy:<topic>` 回填；按 Kafka offset 从事实重建 message receipts；核对各 tenant/partition/day 的 count/counter；以单条 multi-table `RENAME` 原子切换正式/legacy 表名；保留 `_legacy_hash_v1` 表，不立即删除。禁止用可重复反转的 `EXCHANGE TABLES`：切换语句收到不确定 ACK 后重试必须 fail closed，由人工核对表 schema/row counters 后修复 migration ledger，不能再交换一次。
4. **部署读侧**：先部署支持自然坐标 cursor/reconciliation 的 hub；readiness 必须验证 V2 schema。
5. **部署写侧**：配置正确 `source_stream_id`，启动 V2 worker；确认 first offset 正好衔接预检水位。
6. **观察**：至少跨一个完整迟到窗口，监控 Kafka lag、receipt coverage、count/counter mismatch、CH insert latency/parts。
7. **启用 aging**：只有独立 downsample 集成门禁通过后才发布生命周期策略；默认继续不删原始。

回滚窗口内：停 V2 worker，记录 V2 新写 Kafka 坐标，交换回 legacy 表并启动旧 worker。V2 新写事实不能直接丢弃；需先以坐标导回或让旧 worker 从切换 offset 重放。禁止 `DROP` legacy 表作为自动迁移步骤。

## 8. 实施任务与验收门禁

### V2-A 契约与迁移

- [x] **变更设计**：冻结自然坐标、message receipt、零热路径 hash、生命周期状态与切换/回滚顺序。
- [x] **编码**：新增 CH forward migration；001–010 不变；schema readiness 拒绝混用 V1 worker/V2 表。
- [x] **单元**：migration canonical set、DDL 列/键/无固定 TTL/无 hash 列断言。
- [x] **集成**：真实 CH 全迁移、存量 backfill、原子 multi-table rename、count/counter 守恒、legacy 保留；普通 statement resume 由既有真实 CH 用例覆盖。
- [ ] **变更测试（DDL 不确定 ACK）**：在 multi-table rename 已提交但客户端未确认处中断；重试必须失败且不得反向切换，人工核对正式/legacy schema 与 counters 后才允许精确修复 ledger。
- [ ] **变更测试**：维护窗口预检、hub-first/worker-second、回滚重放；错误/缺失 source stream 的启动拒绝已有单元覆盖。

### V2-B 写入与审计

- [x] **编码**：source stream 贯穿 decode/enrich/write；移除四类热路径 hash；block 与 receipt 解耦。
- [x] **单元**：同消息重放、跨 block、批预算变化、同 topic 新 stream、不连续 record index、溢出。
- [ ] **集成**：真实四协议 Kafka→worker→CH 和 count/counter receipt 已通过；真实 CH ambiguous ACK 与 `dedup window=0` 后自然坐标收敛已通过。仍缺实际 worker/CH crash 与存活 consumer 间 rebalance 的组合故障，因此本项不提前关闭。
- [ ] **性能**：同一 Apple M2、1024-record enrichment benchmark 各 5 次，V1/V2 中位数分别为 `1.044ms/0.829ms`，V2 CPU 时间约降 20.6%，两者均 `0 alloc`；这只是整批 enrich 而非 hash-only microbenchmark，但仍不能代替真实 Kafka→CH 的 records/s、CPU、CH bytes/row 与固定硬件 soak。

### V2-C 查询、导出与对账

- [x] **编码**：自然坐标 cursor；逐 committed offset 的 message receipt scanner；删除 Flow query 热路径 hash，保留 artifact checksum。
- [x] **单元**：多字段稳定翻页、同毫秒多 partition、cursor 过期、完全空洞和五类 mismatch 优先级。
- [x] **集成（CH）**：真实 CH 验证 1h archive + raw 互斥拼接、全局 TopN、1m raw-only、境外查询和 generation 守恒。
- [ ] **集成（端到端）**：分页无重/漏；receipt coverage 到 Kafka committed-next-offset；导出与在线查询同参数同总量。

### V2-D aging/downsample

- [x] **设计**：冻结管理策略表/API、UTC 分区粒度、目标 schema、守恒证据、generation 命名空间和删除授权。
- [x] **编码（非破坏路径）**：关闭实时 rollup 互斥开关；增加 aging scanner、downsample operation handler、repair、连续 archive boundary 和管理 API。
- [x] **单元**：raw retention/迟到最大窗口、空分区、策略版本、取消/失败 repair、generation、UTC 边界和 raw-delete fail-closed。
- [x] **集成（非破坏路径）**：真实 MySQL policy/lease/state + 真实 CH source→archive→reconcile 与 hybrid read 守恒。
- [ ] **编码/集成（破坏路径）**：接入 durable Kafka 日覆盖证据后实现显式 raw/archive delete handler；故障、取消或证据缺失均不得删除。
- [ ] **回归**：总览/Explorer/六页/custom range/导出、Kafka/CH 故障注入、全库 race/vet/test。
- [x] **已提交门禁**：实现、迁移、测试和本变更计划已进入 `a9fc7622 feat(flow): implement storage v2 lifecycle`；DDL 不确定 ACK、维护窗口/回滚、组合故障、system-scope Kafka 水位和物理删除仍按各自未勾选门禁保持锁定。

## 9. 本次验证证据（2026-09-07）

| 层次 | 门禁 | 结果 |
|---|---|---|
| 静态/全库 | `go test ./...`、`go vet ./...`、`git diff --check` | 通过 |
| 并发 | `go test -race ./internal/flowch ./internal/flowquery ./internal/flowworker ./internal/watchdog` | 通过 |
| 前端契约 | `npm --prefix internal/site test`（33 项）、production build | 通过；仅保留既有大 chunk 警告 |
| 可复现交付 | `a9fc7622 feat(flow): implement storage v2 lifecycle` | 87 个 Storage V2 文件；不包含平台格式化噪声和本地数据库备份 |
| MySQL | policy CRUD/publish、operation job/state/repair、archive boundary、`install/init.sql` parity | 真实 MySQL 通过 |
| ClickHouse 迁移 | 001–010 存量→011、回填、multi-table rename、legacy 保留、24h 1h archive 守恒、hybrid/raw/overseas 查询 | ClickHouse 26.3.29.7 通过 |
| Kafka 数据面 | Akvorado NetFlow v5/v9、IPFIX、sFlow corpus 经 Kafka 4.3.1→worker→Storage V2 CH | 通过 |
| 对账 | clean/count/missing records/整 offset 双空洞/non-persisted/message budget | 真实 ClickHouse 通过 |
| 重放故障 | fact 已提交但 insert ACK 丢失；禁用 CH server dedup 后完整重放 | 自然坐标、facts 与 message receipt 最终收敛 |

这些证据只关闭非破坏路径，不替代仍未通过的维护窗口切换/回滚、真实 worker+CH crash/rebalance soak、Kafka 日覆盖以及物理删除门禁。

# Flow / SNMP ClickHouse 存储结构与查询设计

> 文档日期：2026-09-28  
> 适用仓库：`watchdog`  
> 生产核验：`root@103.83.64.18:15533`，ClickHouse `24.9.2.42`，数据库 `watchdog_flow`  
> 文档性质：当前生产事实、当前仓库契约和目标演进设计的统一说明。本文凡标为“目标”的内容，均不得解释为已经在线上生效。  
> 2026-09-29 复核：已逐节对照 migration 与代码（`path:line`）并做只读生产核验；正文就地更正处标注“（2026-09-29 复核：…）”，一致性结论、缺陷、割裂与未闭环项见 §17。

## 1. 结论与设计边界

Watchdog 的持久化边界固定为：

- MySQL 保存用户、设备、端口、权限、发布、生命周期策略、operation job 和删除审批等管理事实。
- Kafka 只承担 Flow RawFlow 传输与消费位点，不是查询存储，也不是长期归档。
- ClickHouse 是 Flow、sFlow interface counter、SNMP sample、SNMP event 和计量聚合的唯一遥测数据面。
- Flow 原始事实不可就地重写；修正通过 generation、独立 reclassification 表和显式 publication/version 表达。
- 聚合表不是通过物化视图实时维护，而是由受控 runner 先写数据行、再写 generation marker。查询只接受完整发布的 generation。
- Flow raw 删除由 MySQL 生命周期状态机授权，不能由 ClickHouse TTL、5m 表或热聚合单独授权。
- SNMP counter 的 rate、reset、wrap 和 gap 是派生语义；`snmp_samples` 保存累计 counter 源事实。

截至生产核验时，生产 migration 账本只到 `020_flow_hot_rollup_and_skip_indexes.sql`。仓库中的 `021_flow_atomic_5m.sql` 和 `022_flow_records_event_time_index.sql` 尚未进入生产 migration 账本。其中：

- `flow_aggregate_5m`、`flow_interface_traffic_5m` 线上不存在，Flow 5m 仍是目标结构。
- `flow_event_time_minmax` 已经在线上存在，但 migration 022 未入账；`flow_records` 还有若干不在 001–020 DDL 中的附加 column codec。生产物理 schema 与 migration 账本已经发生 drift。发布前必须先对账，不能直接把“索引存在”当作“022 已应用”。（2026-09-29 复核：server 每次启动都会自动应用全部内嵌 migration（`internal/server/server.go:174` → `:219` → `internal/server/install.go:153-185`），没有暂缓开关；部署新二进制即会执行 021/022。二者均为纯元数据语句（021 两条 `CREATE TABLE IF NOT EXISTS`，022 一条 `ADD INDEX IF NOT EXISTS`，无 `MATERIALIZE`/`MODIFY`），生产已存在的索引会让 022 直接入账。真正的 drift 风险在于账本只校验文件 checksum、不核对物理 schema，见 §17.4。）
- 当前 Flow 查询编译器只选择 `1m / 1h / 1d`，Flow billing 和接口对账仍直接扫描 `flow_records`；不能在文档或 API 中宣称 Flow 5m 已闭环。（2026-09-29 复核：仓库中没有任何代码写或读 `flow_interface_traffic_5m`，仅 `internal/flowch/schema_test.go:82` 列出表名。）

## 2. 证据来源与优先级

本文按以下优先级裁定冲突：

1. 生产 `system.tables`、`system.columns`、`system.data_skipping_indices`、`system.projections` 和 `flow_schema_migrations`。
2. `deploy/migration/clickhouse/001..022` 的前向 migration。
3. 实际写入与查询代码：`internal/flowch`、`internal/flowquery`、`internal/snmpch`、`internal/flowlifecycle`。
4. 运行配置：生产 `/etc/watchdog/watchdog.yaml` 和仓库 `config/watchdog.yaml`。
5. Flow Storage V2、5m、生命周期、地址发布、SNMP ClickHouse 等设计文档。

历史文档中与上述事实冲突的 TTL、表数量、VictoriaMetrics 路径、实时 rollup 或“5m 已上线”描述均视为过期。

## 3. 总体数据链路

```text
Flow exporter
  -> watchdog-flow-collect
  -> RawFlow / Kafka
  -> watchdog-flow-worker
  -> flow_records + flow_ingest_receipts
                     + sflow_interface_counters（仅 sFlow counter record）
                     + flow_quarantined_datagrams（命中删除屏障的迟到报文）
  -> hot rollup: raw -> 1m -> 1h
  -> cold lifecycle: raw -> 1h -> 1d -> 对账 -> 审批 -> raw partition drop

SNMP device
  -> watchdog-snmp-collector
  -> snmp_samples
  -> closed 5m counter rollup
  -> snmp_interface_traffic_5m

SNMP trap / derived event
  -> snmp_events

Query/API
  -> Flow Query compiler: aggregate prefix + raw tail / bounded raw detail
  -> SNMP Query: raw gauge/counter correction or published 5m billing evidence
```

Flow 与 SNMP 通过稳定的 `device_id`、端口 `port_id`/`if_index` 和 UTC 5 分钟网格对账，但三类数据保持独立事实源：

- sampled Flow：估算业务流量；
- sFlow generic-interface counter：exporter 的累计接口计数；
- SNMP IF-MIB counter：轮询得到的累计接口计数。

对账只生成证据和比率，不反向修改任何源事实。

## 4. 生产 ClickHouse 现状基线

### 4.1 版本、容量与迁移

生产快照时间约为 `2026-09-28 10:22 UTC`：

| 项目 | 当前值 |
|---|---|
| ClickHouse | `24.9.2.42` |
| 数据库 | `watchdog_flow` |
| migration 账本 | 001–020，共 20 条，全部 `applied` |
| `flow_records` | 约 1.58 亿行，约 10.7 GiB，日分区 |
| `flow_ingest_receipts` | 约 2.03 亿行，约 1.03 GiB，月分区 |
| `flow_aggregate_1m` | 约 847 万行，约 108 MiB，日分区，2 天 TTL |
| `flow_aggregate_1h` | 约 312 万行，约 38 MiB，月分区 |
| `sflow_interface_counters` | 约 29.8 万行，约 6.1 MiB |
| `snmp_samples` | 约 1,686 万行，约 375 MiB |
| `snmp_interface_traffic_5m` | 约 54.1 万行，约 7.9 MiB |
| `snmp_events` | 0 行 |
| ClickHouse 磁盘 | 约 59.45 GiB，总空闲约 29.81 GiB |

2026-09-28 早间热聚合曾因磁盘不足连续报 `NOT_ENOUGH_SPACE`；空间恢复后继续生成。设计上必须监控磁盘水位、聚合覆盖和 batch pool 等待时间，不能仅以进程存活判断数据面健康。

（2026-09-29 复核补充，只读核验于 04:16 UTC：）

- `flow_records` 只剩 `20260927`（2,353 万行）、`20260928`（2.66 亿行）、`20260929`（至 04:00 约 5,039 万行）三个日分区。09-27 之前的 raw 已不存在；按 09-28 全天计，raw 写入量约 2.66 亿行/天。
- MySQL 生命周期表 `flow_retention_policy_revisions`、`flow_retention_partition_states`、`flow_deletion_receipts`、`flow_deletion_approvals` 均为 0 行。09-27 之前的 raw 是人工删除的，没有走 §12 的状态机；冷归档也从未运行，所以 `flow_aggregate_1d` 为 0 行。
- `flow_aggregate_1h` 在 09-21 00–02 缺 3 小时，在 **09-25 07:00 至 09-27 00:00 缺整整 42 小时**；这段时间的 raw 已删，任何层级都无法再补。09-21 另有 6 个小时的 generation 低于生产 `minimum_generation`，不可读。1m 覆盖 09-27 19:00 之后的全部小时。后果见 §9.2 与 §17.3。

### 4.2 生产表目录

| 表 | 角色 | 引擎 / 分区 | 排序键摘要 | TTL | 线上状态 |
|---|---|---|---|---|---|
| `flow_records` | Flow 不可变基础事实 | `ReplacingMergeTree(ingest_generation)` / 日 | 小时、stream、partition、offset、record | 无 | 主写入、主明细源 |
| `flow_ingest_receipts` | 每 Kafka message 的持久化回执 | `ReplacingMergeTree(generation)` / 月 | stream、partition、offset | 无 | 主对账证据 |
| `flow_aggregate_1m` | 近期高维查询缓存 | `ReplacingMergeTree(generation)` / 日 | bucket、dimension kind/value、资源与版本 | 2 天，整 part 删除 | 已运行 |
| `flow_aggregate_1h` | 生命周期冷聚合和中长查询 | `ReplacingMergeTree(generation)` / 月 | 同聚合统一键 | 无 | 已运行 |
| `flow_aggregate_1d` | 由完整 1h 派生的长区间加速层 | `ReplacingMergeTree(generation)` / 月 | 同聚合统一键 | 无 | 表存在，当前 0 行 |
| `flow_vpn_candidates` | 版本化 VPN candidate + marker | `ReplacingMergeTree(generation)` / 日 | window、conversation、rule set、版本身份 | 90 天 | 表存在，当前 0 行 |
| `flow_address_dict_source` | 版本化 IP_TRIE 字典源 | `ReplacingMergeTree` / 无分区 | dict version、prefix | 无 | 表存在，当前 0 行 |
| `flow_quarantined_datagrams` | raw 删除屏障后的迟到报文 | `ReplacingMergeTree(barrier_revision)` / 月 | stream、partition、offset | 无 | 表存在，当前 0 行 |
| `flow_reclassified_records` | 历史重分类的独立事实副本 | `ReplacingMergeTree(ingest_generation)` / 日 | reclassification、generation、小时、Kafka 身份 | 无 | 表存在，当前 0 行 |
| `flow_reclassification_generations` | 重分类完成 marker 与守恒总计 | `ReplacingMergeTree(completed_at)` | reclassification、generation | 无 | 表存在，当前 0 行 |
| `sflow_interface_counters` | sFlow 累计接口计数源事实 | `ReplacingMergeTree(ingest_generation)` / 月 | device、ifIndex、time、Kafka 身份 | 无 | 已运行 |
| `snmp_samples` | SNMP gauge/counter 原始样本 | `ReplacingMergeTree(ingested_at)` / 月 | device、metric、entity、time、poll 身份 | 无 | 已运行 |
| `snmp_interface_traffic_5m` | SNMP 计费级 5m 接口证据 | `ReplacingMergeTree(generation)` / 月 | bucket、row kind、device、port | 无 | 已运行 |
| `snmp_events` | trap/派生事件 | `ReplacingMergeTree(ingested_at)` / 月 | device、occurred time、id | 无 | 表存在，当前 0 行 |
| `flow_schema_migrations` | migration 账本 | `ReplacingMergeTree(generation)` | version | 无 | 001–020 |

线上还保留若干 rollback 表：`flow_records_legacy_hash_v1`、`flow_ingest_batches_legacy_hash_v1`、`flow_aggregate_1m_legacy_ttl_v1`、`flow_aggregate_1h_legacy_ttl_v1` 和 `flow_aggregate_1m_storage_v2_legacy`。它们不是查询源，删除必须走单独的回滚期结束与空间回收审批。

仓库目标 migration 021 另外定义：

| 目标表 | 角色 | 分区 / TTL | 当前差距 |
|---|---|---|---|
| `flow_interface_traffic_5m` | Flow raw/supplier/customer 三层接口计量证据 | 月分区；无无条件 TTL，随 archive 月状态机删除 | 线上不存在；没有生产 writer/reader 切换证据 |
| `flow_aggregate_5m` | 排除高基数 endpoint 的 90 天中期查询层 | 日分区；默认 90 天整 part TTL | 线上不存在；查询 planner 尚不选择 5m |

## 5. Flow 物理存储模型

### 5.1 `flow_records`

`flow_records` 是所有查询和归档的最终基础事实。字段按职责分组如下：

| 字段组 | 关键字段 | 语义 |
|---|---|---|
| 时间 | `event_time`, `received_time` | 设备事件时间与 collector/worker 接收时间；分区使用事件日 |
| 自然身份 | `source_stream_id`, `kafka_topic`, `kafka_partition`, `kafka_offset`, `record_index` | Storage V2 去重身份，不再依赖逐记录 hash |
| 重放次序 | `ingest_generation` | `ReplacingMergeTree` 版本；同一自然身份取最新 generation |
| 来源 | `collector_id`, `exporter_id`, `target_id`, `device_id`, `registry_version`, `exporter_epoch` | 数据源和管理对象快照 |
| 协议 | `flow_protocol`, `observation_domain_id`, `sub_agent_id`, `datagram_sequence`, `agent_ip` | NetFlow/sFlow/IPFIX 解码身份 |
| 接口 | `observation_if_index`, `ingress_if_index`, `egress_if_index`, `observation_direction` | 观测点与业务接口方向 |
| 五元组 | `src_ip`, `dst_ip`, `src_port`, `dst_port`, `ip_protocol`, `tcp_flags` | endpoint/detail 查询基础 |
| 原始计数 | `raw_bytes`, `raw_packets`, `flow_duration_ms` | exporter 报告的原始值 |
| 采样估算 | `sampling_mode`, `sampling_rate`, `sampling_source`, `estimated_valid`, `estimated_bytes`, `estimated_packets`, `estimated_bytes_scale_ppm` | 可审计的采样与静态校准结果 |
| sFlow 质量 | `source_id_*`, `sample_sequence`, `sample_pool`, `exporter_drops`, `sample_index`, `quality_epoch`, `quality_flags` | 丢包、序列和估算质量 |
| 维度版本 | `dimension_snapshot_id`, `dimension_version`, `geo_version`, `classification_version` | 事实生成时使用的不可变发布版本 |
| 业务分类 | `business_direction`, `business`, `category`, `disposition` | customer view 的方向、业务和计数/丢弃决策 |
| 本远端 | `local_ip`, `remote_ip`, `local_port`, `remote_port`, `local_prefix_id`, `remote_prefix_id` | 以业务方向归一后的 endpoint |
| 地址集合 | `local_address_set_ids`, `remote_address_set_ids` | 允许重叠的集合归属；查询时 arrayJoin |
| 地域/ASN | `remote_*`, `remote_geo_*`, `remote_isp_id`, `remote_asn`, `remote_asn_source` | customer 修正后的查询维度 |
| 供应商来源 | `fact_schema`, `supplier_*`, `supplier_geo_version`, `supplier_category` | customer override 前的供应商事实 |
| override 证据 | `customer_geo_override_fields` | 哪些字段被 customer publication 覆盖 |

物理契约：

```sql
ENGINE = ReplacingMergeTree(ingest_generation)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (
  toStartOfHour(event_time), source_stream_id,
  kafka_partition, kafka_offset, record_index
)
SETTINGS index_granularity = 8192,
         deduplicate_merge_projection_mode = 'rebuild'
```

重要影响：

- 日分区适合生命周期按 UTC 日做 `DROP PARTITION`。
- 首排序字段只有小时精度，因此单个 5 分钟 raw 查询仅靠主键会读完整小时；`flow_event_time_minmax` 用来补充小时内裁剪。（2026-09-29 复核：**只对非 FINAL 读取成立。** 生产 `use_skip_indexes_if_final=0`（24.9 默认值）。对 09-28 12:00–12:05 做 `EXPLAIN indexes=1`：非 FINAL 时 minmax 把 1,240 个 granule 裁到 95 个；加 FINAL 后停在主键的 1,240 个，也就是整小时。raw 查询尾段、detail、billing、对账全部走 `flow_records FINAL`（`internal/flowquery/query.go:1002,1115,1137`，`internal/flowch/billing.go:270`，`internal/flowch/interface_reconciliation.go:249`，`internal/flowch/reconciliation_scanner.go:290`），因此这些路径实际上用不到任何跳数索引。见 §17.3-H3。）
- `FINAL` 是当前读取最新 generation 的正确性手段，但它会增加 CPU/内存和 merge 成本。所有查询都必须有时间和资源边界。
- raw 没有 ClickHouse TTL；删除权威在 MySQL 生命周期策略与审批状态机。

### 5.2 `flow_ingest_receipts`

每个 Kafka message，无论产生 0、1 或多条事实，都写一条 receipt：

- 身份：`source_stream_id + kafka_partition + kafka_offset`；
- disposition：`persisted`、`template_missing`、`empty`、`decode_rejected`、`mapping_rejected`、`late_quarantined`；
- 守恒字段：record、counter record、raw/estimated bytes/packets、estimated-valid record 数；
- 事件覆盖：`min_event_time`、`max_event_time`；
- 版本：`worker_schema`、`receipt_schema`、`generation`。

receipt 与 fact/counter 是按顺序耐久写入，不是 ClickHouse 跨表事务。Kafka offset 只有在要求的表写入成功后才能提交。冷对账按 receipt 与 projection/fact counters 校验连续 offset 区间，不使用逐记录 hash。

### 5.3 `sflow_interface_counters`

该表保存 sFlow generic-interface 的累计 counter，不在写入时计算 rate：

- 身份延续 Kafka `source_stream_id / partition / offset / sample_index / record_index`；
- 接口键为 `device_id + if_index`；
- 保存 `if_in_octets / if_out_octets`、packet、discard、error、speed、status 等累计值；
- 查询或对账时按时间排序做 delta；counter 回退视为 reset，不擅自跨 reset 补值。

### 5.4 统一 EAV 聚合表

`flow_aggregate_1m`、`1h`、`1d` 以及目标 `5m` 使用相同列集合：

- 时间：`bucket`；
- 资源：`target_id / device_id / exporter_id`；
- 业务：`business_direction / category / business`；
- 维度：`dimension_kind / dimension_value`；
- 版本：`dimension_snapshot_id / geo_version / classification_version`；
- 守恒值：raw/estimated bytes/packets、received/unknown-sampling/quality records；
- 发布：`generation / generated_at`。

排序键将 `bucket, dimension_kind, dimension_value` 放在最前，原因是每个聚合查询都固定 bucket 范围和 dimension kind，随后才按资源、业务和版本过滤。

当前生产维度包括：

```text
total, business, category, protocol, asn, isp,
geo.continent, geo.region, geo.country, geo.province, geo.city,
local_prefix, remote_prefix, observation_interface,
src_ip, dst_ip, remote_port
```

（2026-09-29 复核：rollup 还物化 `address_set`，取值为本端和远端地址集合 ID 去重合并后的结果（`internal/flowch/rollup.go:1089`），在查询侧标为不可加维度（`internal/flowquery/query.go:306`）。§9.5 所说的“查询时展开”只适用于 raw/joint 路径。）

`_generation` 是内部 marker，不是业务维度。`src_ip / dst_ip / remote_port` 的高基数 `_other` 是物化的候选补集；读取端不能再次把它当普通 TopN 候选。

### 5.5 generation marker 原子发布

每个 bucket 的发布顺序固定为：

1. 写入该 generation 的全部 value rows；
2. 数据 INSERT 成功后写 `dimension_kind='_generation'` marker；
3. 查询先按 bucket 求 `max(generation)` marker；
4. value rows 必须与该 marker generation 相等才可见。

因此：

- 写 value 后进程崩溃不会发布半桶；
- 空 repair 可以只发布新 marker，使旧值失效；
- 多 generation 保留在 parts 中，由 ReplacingMergeTree 后台合并，不要求查询前同步 mutation；（2026-09-29 复核：此条只对首次发布成立。`generation` 不在聚合表排序键里，它是 `ReplacingMergeTree` 的版本列（`deploy/migration/clickhouse/011_flow_storage_v2.sql:231-235`），读取端又用 `FINAL` 加 marker generation 过滤（`internal/flowquery/query.go:866-875`）。所以重建已发布的桶时，如果新 generation 的 value 已写入而 marker 失败（09-28 的 `NOT_ENOUGH_SPACE` 就发生在 marker 阶段），同键的新行会在 FINAL 和合并中遮住仍被 marker 指向的旧行，该桶读数偏低，直到下一次重建成功。热路径的迟到 repair 通常能自愈，窗口外的桶不能。SNMP 5m 表同理（`012_snmp_telemetry.sql:40-42`）。）
- 热聚合 generation 必须小于 `2^32`；生命周期 generation 为 `policy_version << 32 | repair_attempt`，永远高于热缓存，防止缓存覆盖审计归档。

生产快照中，1m 有 900 个 marker bucket，但有业务数据的 dimension bucket 为 602 个；1h 有 133 个 marker bucket，业务数据为 89 个。marker-only 空桶是合法的完整空结果，不是天然数据丢失证据。

## 6. SNMP 物理存储模型

### 6.1 `snmp_samples`

SNMP 采用窄表而非每指标一列：

| 字段组 | 字段 | 说明 |
|---|---|---|
| 时间 | `observed_at`, `ingested_at` | 设备观测时间和 ClickHouse 写入时间 |
| 资源 | `device_id`, `agent_id`, `entity_kind`, `entity_id` | device/port/sensor 等实体 |
| 采集定义 | `recipe_id`, `metric` | 发布的 recipe 与稳定 metric 名称 |
| 值 | `value_kind`, `gauge_value`, `counter_value`, `counter_width` | gauge 或 32/64 位累计 counter |
| 质量 | `interval_ms`, `quality_flags` | 计划间隔和采集质量 |
| 自然身份 | `poll_sequence`, `source_run_id`, `sample_index` | 一个 poll 内可重放的稳定顺序 |

```sql
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (
  device_id, metric, entity_kind, entity_id,
  observed_at, poll_sequence, source_run_id, sample_index
)
```

写入以最多 2,000 行为一块；一块必须属于同一个 `source_run_id + poll_sequence` 且 `sample_index` 连续。`insert_deduplication_token` 使用 poll 与 sample 范围，失败最多指数退避重试 5 次。（2026-09-29 复核，共三点：
1. 实际是最多 5 次尝试、4 次重试，退避 20/40/80/160 ms，无抖动（`internal/snmpch/write.go:65-82`）。
2. 仓库任何 DDL 都没有设置 `non_replicated_deduplication_window`，生产值为 0（见 `docs/watchdog-flow-perf-audit-2026-09-19.md` R2.1），所以非复制表上的 token 不提供块级去重，重试安全依赖 ReplacingMergeTree 加读时 FINAL/argMax。
3. 潜伏缺陷：poller 给 string/MAC/IP 样本也分配 `sample_index`（`internal/snmpdomain/poller.go:97`），writer 随后丢弃这些样本（`internal/snmpdomain/clickhouse_writer.go:29-31`），剩余行的索引就不连续，`writeChunk` 会整块拒写（`internal/snmpch/write.go:45-50`）。生产启用的 recipe 只有 gauge/counter32/counter64/state，暂未触发。）

### 6.2 `snmp_interface_traffic_5m`

这是当前已经生效的 SNMP 计费证据层：

- value 行键：`bucket_start + device_id + port_id + generation`；（2026-09-29 复核：排序键实为 `(bucket_start, row_kind, device_id, port_id)`，`generation` 是 ReplacingMergeTree 版本列（`012_snmp_telemetry.sql:40-42`），同键只保留最高一代；reader 靠 `values.generation = published.generation` 兜底（`internal/snmpch/billing.go:257`）。）
- generation 行：同 bucket 的发布 marker；
- 值：`in/out bytes`、`in/out bps`；
- 质量：`reset_flag`、`gap_flag`、`coverage`；
- 无 TTL，当前按月分区。

构建一个 closed 5m bucket 时向前读取 15 分钟，以获得窗口前 counter：

- counter 单调递增：`delta = current - previous`；
- 32 位 counter 仅在 previous 位于顶部 10%、current 位于底部 10% 时接受 wrap；
- width 变化、其他回退视为 reset；（2026-09-29 复核：`reset_flag` 只统计同 width 的回退（`internal/snmpch/rollup.go:97`）。width 变化的那一段 delta=0，不计入 accepted，不置 reset 或 gap，只会拉低 coverage。）
- 间隔必须大于 0 且不超过 `max(3 * interval_ms, 15m)`；
- rate 为 `sum(delta) * 8000 / sum(accepted_elapsed_ms)`；
- 先写 value，再写 generation marker。

（2026-09-29 复核补充两点语义：一，区间 `(previous_at, observed_at]` 的 delta 整体计入 `observed_at` 所在的桶（`internal/snmpch/rollup.go:100`），所以跨桶边界的一段增量全部落在后一个桶，计费按 5m 桶粒度可以接受，但应写明；二，`coverage = least(sum(accepted_ms)/600000, 1)`（`:108`）把 in、out 两个 metric 的覆盖毫秒相加后除以 2×5 分钟，只有一个方向有数据时 coverage 为 0.5，且上限截断会掩盖跨边界造成的超额覆盖。）

~~当前 server-side collector 每轮 poll 后只尝试最近一个 closed 5m bucket。若 collector 长时间停止，现有运行循环本身不会自动补齐所有历史缺口；需要 repair/backfill 调度或显式运维任务。~~（2026-09-29 复核：已由 `9e5740ed2` 部分修复。collector 现在会重建上次发布之后的每个已关闭桶，按从旧到新、每轮最多 12 个；进程启动时从 24 小时内最新的已发布桶之后接着建（`internal/server/snmp_collector_runtime.go:128-160`）。以下三点仍未覆盖：比最新已发布桶更早的中间空洞不会再扫描；发布之后才到达的样本不会触发重建；没有覆盖率指标。）

### 6.3 `snmp_events`

`snmp_events` 保存 trap 和派生事件：

- 主键局部性：`device_id, occurred_at, id`；
- 可筛选字段：`source, severity, event_type`；
- payload：人类可读 `message` 和完整 `raw_json`；
- 写入使用事件 ID 范围的 dedup token；（2026-09-29 复核：同 §6.1，非复制表上 token 不生效，依赖 ReplacingMergeTree(ingested_at) 加读时 FINAL。）
- 当前无 TTL。

## 7. 索引、projection、codec 与分区设计

### 7.1 生产跳数索引

| 表 | 索引 | 类型 | GRANULARITY | 用途 |
|---|---|---|---:|---|
| `flow_records` | `flow_event_time_minmax` | `minmax(event_time)` | 1 | 小时排序键内的分钟/5m 时间裁剪 |
| `flow_records` | `flow_device_set` | `set(1024)` | 4 | device 范围查询 |
| `flow_records` | `flow_target_set` | `set(1024)` | 4 | target 范围查询 |
| `flow_records` | `flow_exporter_set` | `set(1024)` | 4 | exporter 范围查询 |
| `flow_records` | `flow_direction_set` | `set(16)` | 4 | business direction |
| `flow_records` | `flow_category_set` | `set(32)` | 4 | category |
| `flow_records` | `flow_src_ip_bloom` | `bloom_filter(0.001)` | 4 | 精确 source IP 条件 |
| `flow_records` | `flow_dst_ip_bloom` | `bloom_filter(0.001)` | 4 | 精确 destination IP 条件 |
| `snmp_events` | severity | `set(64)` | 4 | facet/filter |
| `snmp_events` | source | `set(128)` | 4 | facet/filter |
| `snmp_events` | event type | `bloom_filter(0.01)` | 4 | event type filter |

生产 `flow_event_time_minmax` 已占约 167 KiB，src/dst bloom 合计约 225 MiB。Bloom 只适合精确等值/IN，不替代 endpoint group-by，也不能用于任意 CIDR 范围裁剪。

`ADD INDEX` 只保证新 part 带索引；老 part 需要 merge 或受控 `MATERIALIZE INDEX`。上线验收必须通过 `system.data_skipping_indices`、part 覆盖和 `EXPLAIN indexes=1` 验证，不能只检查 DDL 元数据。（2026-09-29 复核：`system.data_skipping_indices` 只有表级信息，没有 per-part 覆盖，part 覆盖要看 `system.parts` 或 `EXPLAIN`。更关键的是，`EXPLAIN` 必须同时跑业务实际使用的 FINAL 形态：24.9 默认 `use_skip_indexes_if_final=0`，FINAL 读取不使用任何跳数索引，见 §5.1 复核注。022 注释里 757→122 granule 的实测是非 FINAL 结果，不代表 raw 查询路径的收益。）

### 7.2 Ingest audit projection

`flow_records.flow_ingest_audit_v2` 是窄 projection：

```text
source_stream_id, kafka_topic, kafka_partition, kafka_offset,
record_index, raw_bytes, raw_packets, estimated_valid,
estimated_bytes, estimated_packets, ingest_generation
ORDER BY source_stream_id, kafka_partition, kafka_offset, record_index
```

它服务 Kafka receipt/fact bounded reconciliation，避免按 offset 对事件时间排序的宽 raw 表做全表扫描。`deduplicate_merge_projection_mode='rebuild'` 是 ReplacingMergeTree 去重后仍保持 projection 正确的必要设置。（2026-09-29 复核：对账扫描器读的是 `flow_records FINAL ... GROUP BY kafka_offset`（`internal/flowch/reconciliation_scanner.go:283-295`），FINAL 查询不会使用 projection。按 `docs/watchdog-flow-perf-audit-2026-09-19.md` 的生产实测，FINAL 路径读 314,037 行，非 FINAL 走 projection 只读 8,192 行。所以这个 projection 目前没有被它声称服务的路径用上。）

### 7.3 Codec

Flow raw/aggregate 对 timestamp 使用 `DoubleDelta + ZSTD(1)`，单调 offset/generation 使用 `DoubleDelta`，计数使用 `T64 + ZSTD(1)`，IP 和高基数字符串使用 ZSTD。sFlow counter 对累计 counter 使用 `Delta + ZSTD(1)`。

生产 `flow_records` 还实际启用了 `observation/ingress/egress_if_index`、`source_id_value`、`sample_pool`、`remote_port` 的 `T64`，`sample_sequence` 的 `DoubleDelta`，以及 `local_ip/remote_ip` 的 ZSTD；这些附加 codec 不在已入账的 001–020 migration 文本中，必须作为 schema drift 纳入 fingerprint 修复，而不能在重放 migration 时意外丢失。

Codec 是压缩和读 I/O 优化，不改变排序、去重或查询语义。migration 中的 `MODIFY COLUMN ... CODEC` 对旧 part 只在后续 merge 时逐步生效。

## 8. 聚合设计与当前运行配置

### 8.1 Flow 热聚合

生产配置：

| 参数 | 生产值 | 语义 |
|---|---:|---|
| enabled | true | 启用非破坏性近期缓存 |
| scan interval | 1m | 一轮结束后等待 1 分钟再扫描 |
| seal delay | 5m | 更新于此之后的 raw tail 不聚合 |
| minute lookback | 6h | 1m 缺口/repair 窗口 |
| hour lookback | 24h | 1h 缺口/repair 窗口；仓库样例为 72h |
| minute late arrival | 30m | 1m 迟到检查（2026-09-29 复核，第二轮更正：1m 的 `through` 取已封小时边界，repair 窗口是 `[through-30m, through)`（`internal/server/flow_hot_rollup.go:187-193,239`）。每小时前 30 分钟的分钟桶发布时已过去 30 分钟以上，本身就满足 30 分钟迟到 SLA，所以这不是缺陷。真正的问题在 1h：它的迟到窗口是 2h，却从 1m 派生。30 分钟到 2 小时之间迟到的记录会让 1h 在窗口内被反复判为需修复，但每次都从同一份 1m 重建，无法收敛。） |
| hour late arrival | 2h | 1h 迟到检查 |
| repair interval | 30m | 同 bucket 最小复核间隔 |
| max minute buckets/run | 60 | 每轮 1m 上限 |
| max hour buckets/run | 1 | 每轮重型 1h 上限 |
| max threads | 4 | ClickHouse statement CPU 上限 |
| max memory | 6 GiB | statement 内存上限，支持 external GROUP BY |
| batch pool | 1 connection | 避免多个重型 rollup 并行争抢 16 GiB 节点 |
| minimum generation | `1790070054` | 在线强制重建的 generation floor |

当前生成顺序：

1. sealed 小时内一次批量补齐最多 60 个 1m bucket；
2. 1h 若 60 个 1m marker 完整，则从 1m 合并；
3. 若 1m 不完整且 raw 有记录，避免错误地发布不完整 1h；raw 确认为空时可 marker-only；
4. 仓库目标代码随后从完整 1m 合并 12 个 5m bucket，但生产表尚不存在，不能启用该阶段。（2026-09-29 复核：该阶段**没有独立开关**，只要 `hot_rollup.enabled` 就无条件执行（`internal/server/flow_hot_rollup.go:95`）；server 启动又会自动应用 021（§1 复核注）。因此部署当前二进制后，生产会立即开始影子写 `flow_aggregate_5m`。这正好是 §13 “5m 发布门禁”的第 2 步，但与本条“不能启用”相矛盾，要关只能关掉整个热聚合。另外，`1bf356c14` 已给 5m 加上迟到修复：某小时任一 1m 的 `generated_at` 晚于该小时 5m 的 `generated_at` 时重新派生。每轮只读两次 marker，回看取 `min(hour_lookback, 48h)`（`:102-141`）。）

### 8.2 Flow 冷生命周期聚合

对一个满足 policy 的 UTC 日：

1. 为 24 个小时分别从 raw 重建 `flow_aggregate_1h`，generation 使用 policy namespace；
2. 从 24 个完整 1h generation 派生 `flow_aggregate_1d`；
3. 对 raw 与 1h 的 record/raw/estimated bytes/packets、estimated-valid records 做守恒；
4. 记录 late check 与 reconciliation；
5. 只有完整删除门禁通过后才能 drop raw 日分区。

`1d` 是查询加速，不是 raw 删除的第二权威；删除守恒始终以 raw 对 1h 为准。

### 8.3 Flow 目标 5m 分层

目标结构必须分为两张表，不能混用：

- `flow_aggregate_5m`：非 endpoint 的 90 天查询缓存；由完整 1m 合并，排除 `src_ip / dst_ip / remote_port`，默认 TTL 可在线调整。
- `flow_interface_traffic_5m`：计费/审计证据；由 raw 按 device/exporter/ifIndex/direction/layer 聚合，保留版本引用和 ingest generation 范围，不由 90 天 TTL 自动删除。

两张表各自需要 generation marker 与覆盖权威。`flow_aggregate_5m` 完整不代表接口证据完整，反之亦然；两者均不能替代 raw→1h 生命周期守恒。

（2026-09-29 复核：两张表的实现进度并不对称。`flow_aggregate_5m` 已有 writer，就是热调度 5m 阶段，但 planner 不读它。`flow_interface_traffic_5m` 则 writer、reader、删除路径都没有：`DropArchiveMonth` 只 DROP 1d 和 1h（`internal/flowch/raw_delete.go:45-80`），021 注释里说的“由归档月状态机回收”尚未实现。）

### 8.4 SNMP 5m 聚合

SNMP 5m 已在线运行。generation 当前使用 rebuild 时刻的 Unix millisecond。计费 reader：

- 生成完整 UTC 5m 时间网格；
- 每个 bucket 先选已发布 marker generation；
- 再按 billing scope 读取 value rows；
- 缺 port、gap 或 coverage < 1 的 bucket 标记为 gap；
- P95 使用 nearest-rank：`ceil(0.95*N)-1`；
- 账单可选择 in、out 或 in+out，但方向策略来自 MySQL billing account，不写入 ClickHouse。

（2026-09-29 复核：
- **两套 P95 语义并存。** `snmpch.ReadBilling` 的 Rate95/Average 按完整网格计算，gap 和未发布桶按 0 计入，N 等于期望桶数（`internal/snmpch/billing.go:168-200`）；HTTP 账单摘要暴露的就是这组数。持久化账期计费则只取 SNMP 与 Flow 三层同时完整的 5m 交集（`internal/billing/service.go:254-301`）。两者对同一账期会给出不同的 P95，文档与 UI 都应标明各自口径。
- **coverage 与采样相位耦合，生产约 29% 的桶被误判为不完整。** 近 24 小时 131,794 行 value 中，38,575 行 coverage≈0.8，但 gap/reset 均为 0。原因：最忙端口的实际采样间隔约 81 秒（配置 60 秒）；§6.2 的整段归属规则使一个 5m 桶有时只收进 3 段（约 243 秒，即 0.81），有时收进 4 段（封顶为 1）。计费阈值是 `coverage < 0.999999` 即算 gap（`internal/snmpch/billing.go:250`），这些桶因此被持久化计费剔除，而它们的 bps 本身是正确的。修法：把跨桶段按边界比例拆分 delta 和 elapsed。
- 方向实为逐端口 `billing_account_ports.direction`（in/out/agg），账期用 `billing_period_ports` 快照。）

## 9. Flow 查询设计

### 9.1 请求与物理 source 分离

客户端给出业务时间窗、显示 step、TopN、维度、metric 和过滤条件；server 独立选择物理 source：

| 显示需求 | 当前物理 source | source bucket 上限 | 备注 |
|---|---|---:|---|
| `< 1h` | `flow_aggregate_1m` + 必要 raw tail | 10,080 | 最多 7 天分钟源桶 |
| `>= 1h` 且 `< 1d` | `flow_aggregate_1h` + 必要 raw tail | 9,600 | 自动 24h 报表至少使用小时源 |
| `>= 1d` | `flow_aggregate_1d` + 必要 raw tail | 400 | 生产 1d 当前无覆盖，需 readiness/fallback（2026-09-29 复核：没有任何 1d→1h 回退。1d 无 marker 时整段走 raw（`internal/server/flow_archive.go:112-124` → `internal/flowch/rollup.go:277-302`）。生产单个 UTC 日 raw 约 2.66 亿行，已超过无 scope 50M、有 scope 250M 两档预算，所以日步长报表必然 fail-closed。1d 只由冷归档写入，而冷归档从未运行。） |
| 明细、任意复杂联合维度 | bounded `flow_records` | 按扫描预算 | 不伪装为聚合查询 |
| 目标中期非 endpoint | `flow_aggregate_5m` | 待 planner 接入 | 021 仅建表，不等于查询切换（2026-09-29 复核：写侧已存在，热调度会写该表；planner 仍只返回 1m/1h/1d（`internal/flowquery/plan.go:88-100`、`internal/flowquery/query.go:627-637`）） |

显示 interval 必须是 source resolution 的整数倍，且不超过 30 天；时间范围采用 `[from,to)`，只读 closed bucket。返回点数按 `points * (TopN + optional other) + metadata` 预估，不得超过 250,000 行。

### 9.2 Storage V2 hybrid 查询

`ArchiveThrough` 是从连续 generation coverage 计算的读取边界，不直接等于 policy retention 时间：

```text
[from, archiveThrough)  -> generation-marked aggregate
[archiveThrough, to)    -> flow_records FINAL
```

两个分支必须：

- 使用互不重叠的半开区间；
- 输出相同字段和版本 identity；
- 在 union 后进行一次全局 TopN 排名；
- 对非整 presentation bucket 使用实际覆盖秒数计算 bps/pps；
- 将 target/device/exporter/direction/category/business 等可下推条件推入 raw scan；
- 将需要统一语义的 dimension value 和版本条件保留在 union 后。

如果 archive coverage 不连续，边界只能停在第一个缺口前，不能跨洞拼接后声称完整。

（2026-09-29 复核：这条规则与“洞永久存在”两件事叠加，是当前最严重的查询问题，见 §17.3-H1。
- **覆盖是从 `from` 起的连续前缀。** 遇到第一个缺口或第一个低于 `minimum_generation` 的 marker 就停止（`internal/flowch/rollup.go:286-297`），之后的所有桶都走 raw，包括洞后完好的聚合。
- **生产现状下的后果。** 1h 缺 09-25 07:00 至 09-27 00:00，raw 只剩 09-27 起约 3.4 亿行。
  - 起点在洞前的 7 天或 30 天报表：raw 段超出预算，同步返回 400 `QUERY_RANGE_LIMIT`，异步终止失败。
  - 跨洞但 raw 尾恰在预算内的窄范围：raw 桶被当作“已覆盖”（`internal/flowquery/query.go:1087-1090`），响应标 `complete=true`，42 小时显示为 0。
- **查询层不知道 raw 已被删除。** 查询代码没有任何路径读取分区状态，热调度还会给没有 raw 的小时发布空 marker（`internal/server/flow_hot_rollup.go:288-297`）。所以人工删除 raw 后，仍在 lookback 内的小时会被固化为“权威 0”。）

### 9.3 TopN、endpoint 与 `_other`

普通聚合读取：

1. 选择每个 bucket 的最新 marker generation；
2. 读取指定 `dimension_kind`；
3. 按 dimension + snapshot/geo/classification version 计算全窗口 rank；
4. 稳定 tie-break；
5. 非 TopN 合并到 `_other`；
6. 保留 mixed-version 元数据，不能跨版本静默合并。

全 raw 的 source/destination IP 高基数查询使用两遍算法：

1. `topKWeighted(top_n * 8)` 产生有界候选；
2. 第二遍对候选计算精确 bucket 值，并把非候选精确守恒到 `_other`；
3. 返回 `approximate=true`，表示候选选择近似，不表示已返回候选的值近似。（2026-09-29 复核，共三点：
   - `Compiled.Approximate` 的判定比这里写的宽：只要 IP/端口查询读到聚合就为 true，因为聚合层已按 top-1000/256 折叠（`internal/flowquery/query.go:609-611`，`internal/flowch/rollup.go:877,882`）。
   - 能传到客户端的只有端点报表主面板的 meta（`internal/server/flow_report_endpoints.go:81,163`）；`/flow/query`、其他面板和导出都不带这个标记。
   - 两遍候选路径要求 `archiveThrough == from`（`query.go:521`）。只要存在一个已覆盖的前缀桶，就会退回对整个 raw 尾做精确 GROUP BY。）

如请求带 residual filter、缺少 identity scope 或无法安全走候选路径，则回到受限 exact GROUP BY，并受更小扫描预算保护。

### 9.4 指标、维度与视图

聚合 metric：raw/estimated bytes、bps、packets、pps、received records。聚合表当前只实现 materialized customer view；raw/supplier 明细和历史重分类使用独立 bounded path，不能把聚合 v1 误当三层通用表。

方向维度不读取单独 `dimension_kind=direction`，而是读取 `total` 行并按已物化的 `business_direction` 分组，避免重复存储。

地址集合可重叠，`arrayJoin` 后结果非加性；只有明确标记 additive 的维度才允许 `_other`。

### 9.5 细节、联合维度、地址与 VPN

- detail：从 `flow_records FINAL` 读取，列、排序和 filter 采用固定 allowlist；分页、facet 和导出均有读预算。
- joint/business×category：当前从 raw 或 reclassified raw 编译联合维度，不依赖单维 EAV 表拼接，保证组合语义。（2026-09-29 复核：只有 `/flow/query` 的多维 `dimensions[]` 是纯 raw，且范围限 24h，本文未写明（`internal/flowquery/joint.go:115-117`）。business×category 和 endpoint×category/business 走 `flow_aggregate_1h` 加 raw 尾（`internal/flowquery/rollup_joint.go`，`internal/server/flow_report_endpoints.go:283-307`）。端点关联面板的聚合段与 raw 段各自排名后拼接，不满足 §16 不变量 6 的“全局 TopN”。）
- address set：基于事实中的 versioned set IDs；查询时展开，限制 5M raw rows / 1 GiB 等独立预算。（2026-09-29 复核：5M/1 GiB 只适用于“集合组合”查询，且该查询限 ≤1h、10 秒、仅同步（`internal/flowquery/address_set.go:79-81,133-139`）。报错提示用户改用异步 job，但代码里没有这个 job。以 `dimension=address_set` 查询时读的是物化的 `address_set` 行，走常规 50M/250M 预算。）
- historical reclassification：只有 MySQL activation 和 ClickHouse generation marker 同时存在时才切换到 `flow_reclassified_records`。
- VPN：candidate 表按 window + conversation + rule/publication versions 读取；generation marker 使空 repair 具有权威性。

### 9.6 同步与异步入口

生产 Flow 配置：

- ClickHouse statement timeout：2 分钟；
- HTTP 同步总时限：25 秒；
- 请求范围大于 1 小时时进入 operation job；
- panel concurrency：3；
- async 结果保留：24 小时；
- batch ClickHouse pool：1，interactive pool：8。

同步/异步只改变执行和结果交付方式，不改变 SQL 正确性、读预算或物理 source 选择。异步任务不能绕过 `max_rows_to_read`、`max_bytes_to_read` 和版本边界。

（2026-09-29 复核：
- **范围。** 25 秒同步时限和“超过 1 小时转异步”只作用于 `POST /flow/reports/query`（`internal/server/flow_reports.go:404-409`，`internal/server/flow_report_jobs.go:42-44`）。`/flow/query` 等入口只有 2 分钟语句超时；detail 和地址集合固定 10 秒；billing 固定 120 秒。
- **结果文件不回收。** “24 小时保留”只是过期后读取返回 410（`flow_report_jobs.go:203`），`data/flow-query-results` 下的结果文件从不删除，在磁盘本已吃紧的节点上无界增长。
- **超时处理。** `classifyExecutionError` 不识别超时（`internal/flowquery/query.go:256-272`），2 分钟超时会变成 503，异步任务还会重试 3 次。
- **连接池。** Flow billing 跑在交互池上（`internal/server/server.go:253`），会与交互查询争用连接。）

### 9.7 Flow 查询保护

| 路径 | 最大读取行 | 最大读取字节 | 其他限制 |
|---|---:|---:|---|
| 普通未 scoped raw | 50M | 4 GiB | memory 4 GiB |
| device/target/exporter scoped raw | 250M | 16 GiB | memory 4 GiB |
| endpoint 两遍候选 | 500M | 32 GiB | `candidate_n=top_n*8` |
| 聚合结果 | 250K | 同上按路径 | external group/sort 阈值各 1 GiB |

所有 aggregate SQL 使用 `do_not_merge_across_partitions_select_final=1`，溢出模式为 `throw`，不得返回静默截断的部分结果。（2026-09-29 复核：表中数值属实（`internal/flowquery/query.go:35-59,582-597`），但有两处遗漏。一是预算档不止这几档：还有 detail/facet/地址集合的 5M/1 GiB、rollup joint 的 50M/4 GiB 加 2 GiB 内存、overseas scoped 的 500M/32 GiB。二是地址集合、Flow billing、接口对账的聚合 SQL 没有设 `do_not_merge_across_partitions_select_final`，其中 Flow billing 连 `max_rows_to_read`/`max_bytes_to_read` 都没有（`internal/flowch/billing.go:129-134`）。）

## 10. SNMP 查询设计

### 10.1 Gauge 与 raw counter

Gauge 查询按 presentation bucket 对 `(observed_at, ingested_at)` 做 `argMax`；累计 counter 的 raw 查询返回 bucket 内最新累计值。所有查询都先按 device、metric、entity 和时间限制 raw。

### 10.2 Counter rate

接口 bps 不直接存入 `snmp_samples`，而是：

1. 向前读取最多 15 分钟；
2. 对相同 device/entity 按 `observed_at` 排序；
3. 用 `lagInFrame` 取得 previous；
4. 校验 counter width、elapsed time、forward/wrap；
5. 计算 delta 和 accepted elapsed；
6. 按请求 step 聚合为 `sum(delta)*8000/sum(elapsed_ms)`。

这保证不把缺测时间当作零，也不把 reset 当作巨大流量。

### 10.3 多资源聚合

授权完成后的 device/port scope 作为 ClickHouse external table 传入，而不是拼接进 SQL。支持 `sum/avg/min/max/count` 固定 allowlist。device-wide scope 会覆盖同 device 的重复 port scope。

当前硬上限：

- 1–1,000 个 scope；
- 查询范围最多 400 天；
- step 为 1 秒到 24 小时；
- 中间/结果最多 250,000 行；
- 生产 statement 15 秒、50M rows、4 GiB read、2 GiB memory。

（2026-09-29 复核：
- **250,000 只是结果行上限。** 引擎侧只设了 `max_result_rows`（`internal/snmpch/store.go:89`），“中间行”只是 handler 的估算。
- **400 天、24h step 的限制只在 Aggregate/scoped 路径成立。** 单设备 `Store.Query` 自身不限，靠 handler 兜底；其 rate SQL 也缺 `entity_kind='port'`（`internal/snmpch/query.go:138`）。
- **15s/50M/4 GiB/2 GiB 是可调默认值。** 硬上限是 2 分钟/500M/64 GiB/8 GiB（`store.go:40-52`）。
- **scoped 与 aggregate 查询不能用主键裁剪。** 它们只靠 `INNER JOIN snmp_scope` 过滤设备，WHERE 里没有 `device_id IN (...)` 预过滤（`internal/snmpch/aggregate.go:258-262,310-314`），排序键首列 `device_id` 用不上。需用 `EXPLAIN indexes=1` 验证后补上谓词。
- **图表与 5m 证据的桶语义不一致。** 图表按 `[start,end)` 归属样本，并丢弃 accepted=0 的桶；5m rollup 按 `(start,end]` 归属，accepted=0 的端口仍写 bps=0 的行。）

### 10.4 Billing

SNMP billing 只能读取已发布的 `snmp_interface_traffic_5m`，范围必须 5m 对齐、closed、最多 400 天。它不会独立从 `snmp_samples` 重算另一套 counter 语义。

Flow billing 当前仍从 `flow_records FINAL` 生成 5m 网格和 raw/supplier/customer 三层值。迁移到 `flow_interface_traffic_5m` 之前，Flow 长周期账单仍有高 raw scan 成本；migration 021 本身不能完成 reader 切换。

（2026-09-29 复核：实际情况比“成本高”更严重，Flow 账单目前算不出来。
- **查询本身跑不完。** `flow_records FINAL` 与外部 scope 表做 CROSS JOIN，设备条件只在 join 中，无法用主键或跳数索引裁剪；没有 `max_rows_to_read`，只有 120 秒、8 线程、1 GiB（`internal/flowch/billing.go:129-134,270-275`）。按生产约 2.66 亿行/天，任何真实账期都会超时。
- **历史账期没有数据。** raw 只保留约 3 天，历史账期根本没有 raw 可读。
- **持久化计费拿不到桶。** 持久化计费要求 SNMP 与 Flow 三层同时完整的 5m 交集，所以对历史账期拿不到任何桶。
- **证据层还没有 writer。** 唯一出路是 `flow_interface_traffic_5m`，但它目前没有 writer。
- **SNMP billing 读取不按端口裁剪。** `values` CTE 读取账期内全部端口的行，之后才与 scope join（`internal/snmpch/billing.go:235-242`）。建议加 `port_id IN (SELECT port_id FROM snmp_billing_scope)`。）

### 10.5 Event 查询

事件查询强制 device scope，分页 limit 1–100、offset 最大 100,000；severity/type/source 每类最多 8 个过滤值；搜索最多 256 字符。facet 仅允许 severity、event_type、source。（2026-09-29 复核：256 是 Go `len()` 算出的字节数，不是字符数，中文约 85 字（`internal/snmpch/events.go:222`）。）

当前自由文本搜索是 `positionCaseInsensitiveUTF8(concat(...))`，不受现有 skip index 加速；事件量增长后应评估 token bloom 或独立搜索方案，但不能提前为 0 行生产表引入复杂索引。

## 11. Flow / sFlow counter / SNMP 三方接口对账

当前接口对账是最多 7 天、最多 1,000 scopes 的只读证据查询：

- Flow：直接从 `flow_records FINAL` 按 5m、device、ifIndex 汇总 estimated bytes；
- sFlow counter：从 `sflow_interface_counters FINAL` 做累计 counter delta；
- SNMP：从已发布的 `snmp_interface_traffic_5m` 读取；
- 输出 Flow/counter/SNMP 的 in/out bytes、coverage、reset/gap 和两两 ratio。

ratio 只有 denominator 非零时才有效。该查询不应用校准、不生成 billing 值、不修改任何数据。

（2026-09-29 复核：**这不是“当前”可用的查询。**
- **未接线。** `ReadInterfaceReconciliation` 没有任何非测试调用方，也没有路由、job 或 UI。
- **两项上限不能同时满足。** 结果行数等于桶数×scope，受默认 50,000 行预算约束（`internal/flowch/interface_reconciliation.go:85-90,113-116`）。7 天有 2,016 个桶，最多只能配 24 个 scope；1,000 个 scope 只够约 4 小时。
- **Flow 分支扫描过宽。** Flow 分支同样只靠 join 过滤设备，读的是 FINAL（`:249-254`），在生产数据量下 100M 行的读预算约合半天。
- **输出不完整。** Flow 分支没有 coverage 字段，counter 分支也没有独立的 gap 字段（`:44-77`）。）

目标状态是在 `flow_interface_traffic_5m` 完成 backfill、coverage、reader 切换后，用 Flow 5m 证据替代 Flow raw 分支；sFlow counter 也应有独立 5m 派生证据，避免每次对账重复窗口计算。

## 12. 生命周期与删除设计

### 12.1 Flow retention policy

Flow retention policy 是 MySQL 中安装级、发布后不可变的版本化策略。关键参数：raw retention、1h archive retention、late window、delete grace、每轮分区数、raw/archive delete 开关和 backup-before-delete。

代码不提供“默认保留 N 天”的破坏性语义；operator 必须显式发布策略。`ArchiveRetentionSeconds=0` 表示无限保留。

（2026-09-29 复核：参数列表漏了必填的 `bootstrap_from`，早于它的日永不归档或删除（`internal/flowlifecycle/lifecycle.go:113`），也漏了各参数的取值域（`:74-81`）。生产至今没有发布过任何策略。**首次发布前必须知道两个陷阱：**

1. **归档不检查 raw 是否存在。** 调度器从 `bootstrap_from` 起逐日排归档 job（`internal/flowlifecycle/archive_scheduler.go:128-162`），handler 从 raw 重建 24 个 1h 和 1d，再比较守恒（`internal/flowlifecycle/archive.go:137-183`），全程不校验 raw。如果 `bootstrap_from` 早于 2026-09-27，09-21 至 09-26 会在空 raw 上完成归档：0=0 通过守恒，而 ≥2^32 的空生命周期 generation 会把这些日仍然存在的热 1h 全部遮住。**`bootstrap_from` 必须不早于 2026-09-27，并先补上“raw 必须存在”的护栏。**
2. **换版会让旧版的日永远卡住。** 删除就绪要求分区状态的策略版本等于当前发布版本（`internal/flowlifecycle/delete_readiness.go:109-112`），但发布新版只会把旧版置为 retired，不迁移分区状态（`internal/flowlifecycle/store.go:222`）。调度器只给没有状态的日排 job，repair 也沿用旧策略（`archive_scheduler.go:167-187`）。因此“v1 只归档、v2 再开删除”这条自然路径，会让所有 v1 日永远卡在 `invalid_generation`。要么首版就定好全部开关，要么先实现状态迁移。）

### 12.2 Raw UTC 日状态机

```text
sealed
  -> archive_written
  -> reconciled
  -> delete_eligible
  -> raw_deleted

任一步失败 -> failed -> repair generation
```

raw 日删除必须同时满足：

- 存在当前 published policy 和同版本 partition state；
- 24 个 1h generation 完整；
- raw 与 1h 六类 counters 守恒；
- late window 和 delete grace 已结束；
- receipt 覆盖的 Kafka offset 已被 stable committed/reconciled watermark 覆盖；
- watermark identity 健康、无 mismatch；
- 如策略要求，存在 checksum 完整且已经 restore-tested 的 backup evidence；
- raw delete feature switch 开启；
- 有未撤销、版本匹配的人工 deletion approval；
- worker 已 ACK delete barrier；
- 没有覆盖该日的 active reclassification。

执行使用明确的日 partition，不使用行级 `DELETE`。命中 barrier 的极晚报文写 `flow_quarantined_datagrams` 并产生 `late_quarantined` receipt，不能重新污染已删除事件日。

（2026-09-29 复核，逐条对照 `internal/flowlifecycle`：

**状态机语义：**
- `sealed` 的含义是归档 job 已入队，而不是这一天已封闭。
- `reconciled` 只表示 raw 与 1h 六类 counter 守恒，Kafka 水位要到 readiness 阶段才校验。
- `reconciled → delete_eligible` 由人工审批完成，审批撤销后可以回退。

**删除条件的实际执行情况：**
- **已强制：** 策略与同版本状态、六类守恒、删除开关、审批、Kafka 覆盖与水位（仅对有记录的日）。
- **只隐式成立：** “24 个 1h generation 完整”，只是靠守恒间接保证。
- **比文档写得更宽松：**
  - 备份证据由操作员自报，登记即 `verified`，不校验备份实体。
  - barrier 只要求 `status='active'` 的 flow worker 确认，一个 active worker 都没有时直接视为就绪（`delete_barrier.go:127-133`），draining worker 不需要确认。
  - active reclassification 只在执行时检查（`raw_delete.go:282-284`），readiness 与审批阶段都不查，所以 UI 可能显示可删、执行时却被拦下。
- **代码强制、文档未写：** 审批时冻结的物理行数、覆盖和备份在执行时必须逐项相等；DROP 之后还会复核 raw=0 且归档未变。

**风险：**
- **late check 与删除存在竞态（子任务读代码推断，未复现）。** late check 不排除已有删除 job 的日（`archive.go:382-403`）。如果它恰好在 DROP 之后运行，会从空 raw 重建一个更高 generation 的空归档，把原归档遮住。
- **数据丢失没人发现。** reconciliation 关闭时，Kafka `ErrDataLoss` 只按可恢复告警处理（`internal/flowstream/consumer.go:266-278`），offset 空洞无人发现。）

### 12.3 当前保留矩阵

| 数据 | 当前删除权威 | 当前问题 |
|---|---|---|
| Flow raw | MySQL state machine + day partition drop | 正确，但 reconciliation 生产配置当前 disabled（2026-09-29 复核：这是设计上的权威。生产实际的权威是**人工 DROP**：生命周期四张表均为 0 行，09-27 之前的 raw 却已不在，没有任何删除回执可追溯，查询层也无从得知。） |
| Flow 1m | ClickHouse 2 天 TTL，整日 part | 只作为近期缓存，正确 |
| Flow 1h / 1d | lifecycle archive 月删除流程 | 无无条件 TTL，正确 |
| Flow 目标 5m query | 90 天 TTL | 尚未部署 |
| Flow 目标 5m interface evidence | archive 月状态机 | 尚未部署/未接 reader |
| Flow receipt | 无 | 2.03 亿行持续增长，必须设计对账水位后的安全清理 |
| sFlow counter | 无 | 持续增长；需先有 5m 派生/对账/恢复门禁 |
| quarantine | 无 | 需纳入事件审计与审批式清理 |
| reclassification | 无 | 需随 publication/hold 生命周期处理 |
| SNMP raw sample | 无 | 目前无限增长；需独立 SNMP retention policy（2026-09-29 复核：仓库已有 `metric_retention_policies` 表（MySQL 0029）、`/retention/policies` API 和前端页，但没有任何消费者，容易被误认为已经生效） |
| SNMP 5m evidence | 无 | 可作为长期 billing evidence，但需 archive/hold 策略 |
| SNMP event | 无 | 需事件审计保留策略 |

不得因为磁盘压力直接给 raw、receipt 或证据表增加短 TTL。先冻结可恢复性、对账和 legal/billing hold，再发布前向 retention migration。

## 13. 生产差距与实施优先级

### P0（2026-09-29 复核新增，优先于下列各项）

依据与证据见 §17.3。

1. **覆盖空洞与前缀语义（H1）**
   - 查询改为按覆盖分段读：聚合段加上仅针对洞的 raw 段；raw 已不存在的洞显式返回 gap，不当作 0。至少要先做 1d→1h 回退，并让 completeness 反映 raw 缺失。
   - 为“1m 小时滑出 `minute_lookback` 仍不完整”增加告警。
   - 决定热 1h 空洞的修复路径：带预算的 raw→1h 修复，或者冷归档。
   - 人工删除 raw 必须登记，使查询能够识别。
2. **首次发布 retention policy 之前（H2）**
   - `bootstrap_from` 不得早于 2026-09-27。
   - 归档前校验 raw 或 receipt 存在。
   - 实现换版时的分区状态迁移。
   - late check 排除正在删除的日。
   - 首版策略就定好全部开关。
3. **FINAL 与跳数索引（H3）**：评估 `use_skip_indexes_if_final=1`，同时对重放时可能改变的列（category、direction）使用 `ignore_data_skipping_indices`。必须用 FINAL 形态的 `EXPLAIN` 加 A/B 验证，不得直接上线。
4. **计费与证据（H4）**
   - 在对外宣称 Flow 计费之前，先实现 `flow_interface_traffic_5m` 的 writer、回填和 reader。
   - 给 Flow billing 补读预算和设备预过滤。
   - 修正 SNMP coverage 的相位问题，否则持久化计费会剔除约 29% 的桶。

### P0：迁移账本与物理 schema 对齐

1. 读取 `system.data_skipping_indices` 确认 `flow_event_time_minmax` 的 part 覆盖。（2026-09-29 复核：这张系统表没有 per-part 信息，应改用 `system.parts` 或 `EXPLAIN`。）
2. 对 `flow_records` 做完整 `SHOW CREATE TABLE`/column codec fingerprint，记录所有 ledger 外变更，而不只核对 022 索引。（2026-09-29 复核：9 列附加 codec 确实不在任何 migration 中。建议加前向 migration 023 用幂等 `MODIFY COLUMN ... CODEC` 收敛（仅改元数据），否则新装库永远与生产不同。）
3. 对 migration 022 采用“已存在即验证”的修复流程，使 ledger 与物理 schema 一致；禁止删除再重建索引。（2026-09-29 复核：代码里没有这个流程。`watchdog-flow-migrate` 只有 inspect/apply/resume/unlock；022 是 `ADD INDEX IF NOT EXISTS`，apply 时会直接入账，不比对已存在索引的定义。）
4. 在对账前不要直接运行完整 021/022 migration 批次。（2026-09-29 复核：**这一条无法执行，也不是真正的风险点。**
   - **部署即迁移。** server 每次启动都会应用全部 pending migration，没有暂缓开关；唯一的“暂停”方法会连带关掉整个 ClickHouse 平面。
   - **021/022 本身安全。** 两者都只改元数据。
   - **真正的风险是：**
     - 迁移只能前进，回滚旧二进制会因 “schema version 021 is newer than the available migration set” 让 ClickHouse 平面起不来（`internal/flowch/migrations.go:144-146`）；
     - 自动迁移用的连接没有传 TLS（`internal/server/install.go:164-168`，对比 `server.go:416-427`），启用 TLS 的部署会迁移失败；
     - 账本只校验文件 checksum，没有物理 schema 指纹。
   - **建议：** 部署前先用新二进制跑只读 `watchdog-flow-migrate -command inspect`；增加 `clickhouse.auto_migrate` 开关或版本上限；把回滚限制写进发布手册。）

### P0：Flow 5m 发布门禁

1. 先应用 021 建表，但保持 reader 不切换。
2. 影子生成 `flow_aggregate_5m` 和 `flow_interface_traffic_5m`。
3. 对 raw/1m/5m/1h 做 bucket coverage 与守恒。
4. 为热 scheduler、冷 backfill、late repair 分别证明 generation 规则。
5. 查询 compiler 增加 5m source 后，先切非 endpoint 报表。
6. Flow billing 和接口对账最后切到 interface evidence 表。
7. 只有所有 reader 都能解释 gap/coverage/version，才扩展 raw 删除 readiness。

### P0：生产 reconciliation

生产配置当前 `flow.reconciliation.enabled=false`，且没有 source stream/bootstrap offsets。raw 删除依赖 Kafka coverage 时必须 fail closed。启用前应从实际 consumer group committed offsets 和 receipt 最小 offset 建立显式 bootstrap，禁止猜 offset 0。

### P1：查询与覆盖一致性

- 1d 表当前无数据，planner/readiness 必须 fallback 或返回明确 incomplete，不能返回静默空报表。
- 所有报表入口、endpoint、detail、joint、export、billing 必须共享同一 coverage 决策，不能各自猜 source。
- 5m 上线后，1m 继续负责最近高基数 endpoint；5m 不存 src/dst IP 和 remote port；1h/1d 的高基数历史策略应重新评估。
- 生产 `hour_lookback=24h` 与仓库样例 `72h` 不同，应作为部署参数记录，不应写死在业务语义中。

### P1：SNMP 生命周期与 repair

- 为 `snmp_samples / snmp_interface_traffic_5m / snmp_events` 定义独立、可审计 retention，而非复用 Flow raw policy。
- 增加 SNMP 5m 缺口扫描、迟到 repair 和 coverage metrics；当前只补最近一个 closed bucket。（2026-09-29 复核：`9e5740ed2` 已补上两项：上次发布以来的所有已关闭桶都会重建（每轮 ≤12 个），启动时从 24h 内最新已发布桶续建。仍缺中间空洞扫描、发布后迟到样本的重建和 coverage 指标；另需按 §8.4 修正 coverage 的相位问题。）
- 清理前先证明 SNMP raw 到 5m 的 counter/coverage 守恒和恢复能力。

### P1：容量与运维

- 告警：磁盘 free、parts 数、merge backlog、TTL lag、projection/index materialization、batch pool wait。
- 指标：各 resolution 最新连续 marker、最大 bucket age、repair/permanent gap、raw/receipt ingest lag。
- 将 `flow_ingest_receipts`、sFlow counter 和 SNMP raw 的增长率纳入容量模型；不能只监控 `flow_records`。
- （2026-09-29 复核新增）
  - `data/flow-query-results` 的异步结果文件从不删除，需要回收任务。
  - 删除证据查询（`DayStorageCounters`、`RawDayPhysicalRecords`、`DayOffsetCoverage`）没有内存、线程、读量上限（`internal/flowch/rollup.go:481-547`，`internal/flowch/retention_coverage.go:46-64`）。它们对 receipts 做 FINAL 全表扫描，且与热聚合共用 1 个 batch 连接；每个待删日每 6 小时要扫两次，receipts 越多越慢。
  - 旧的 `*_legacy_ttl_v1` 回滚表仍带 180/400 天行级 TTL（`deploy/migration/clickhouse/010_flow_aggregate_reorder.sql:57,97`），会自行删除，与 §4.2 “删除须审批”冲突。

## 14. 验收与运维查询

以下只读检查应进入发布门禁；具体数据库名由部署注入。

```sql
-- migration 与物理表是否一致
SELECT version, name, state, completed_statements, updated_at
FROM flow_schema_migrations FINAL
ORDER BY version;

SELECT name, engine_full, total_rows, formatReadableSize(total_bytes)
FROM system.tables
WHERE database = currentDatabase()
ORDER BY name;

-- skip index / projection
SELECT table, name, type_full, expr, granularity, data_compressed_bytes
FROM system.data_skipping_indices
WHERE database = currentDatabase()
ORDER BY table, name;

SELECT table, name, type, sorting_key, query
FROM system.projections
WHERE database = currentDatabase()
ORDER BY table, name;

-- 聚合覆盖：marker 和数据桶分开观察
SELECT dimension_kind, min(bucket), max(bucket), uniqExact(bucket), count()
FROM flow_aggregate_1m
GROUP BY dimension_kind
ORDER BY dimension_kind;

-- SNMP 5m 最新发布 generation
SELECT bucket_start, max(generation)
FROM snmp_interface_traffic_5m FINAL
WHERE row_kind = 'generation'
GROUP BY bucket_start
ORDER BY bucket_start DESC
LIMIT 24;

-- 索引是否真正参与查询，需替换为真实范围和 scope
EXPLAIN indexes = 1
SELECT count()
FROM flow_records FINAL
WHERE event_time >= {from:DateTime64(3, 'UTC')}
  AND event_time < {to:DateTime64(3, 'UTC')}
  AND device_id = {device:String};
```

（2026-09-29 复核补充：
- **资源上限。** 在生产执行上述 SQL 时必须加 `SETTINGS max_memory_usage = 1500000000, max_threads = 2`（2026-09-22 OOM 事故后的约定）；`flow_aggregate_1m` 的全表 GROUP BY 尤其如此。
- **EXPLAIN 两种形态都要跑。** 上面的 `EXPLAIN` 样例带 FINAL，在 24.9 上必然看不到跳数索引，应与非 FINAL 版本并列比较，结论只按业务实际走的形态下。
- **会话库。** `currentDatabase()` 和未限定的表名依赖会话库，`watchdog-flow-migrate` 连的是 `default`。
- **补充以下三条检查。**）

```sql
-- 覆盖空洞：没有 1h marker 的小时（范围按需调整）
SELECT h
FROM (SELECT toDateTime('2026-09-21 00:00:00','UTC') + number*3600 AS h FROM numbers(240))
WHERE h < toStartOfHour(now())
  AND h NOT IN (SELECT bucket FROM flow_aggregate_1h WHERE dimension_kind='_generation')
ORDER BY h
SETTINGS max_memory_usage = 1500000000, max_threads = 2;

-- 可读性：按日统计 marker 与 minimum_generation 以上的可读小时
SELECT toDate(bucket) d, count() hours, countIf(g >= {minimum_generation:UInt64}) readable
FROM (SELECT bucket, max(generation) g FROM flow_aggregate_1h
      WHERE dimension_kind='_generation' GROUP BY bucket)
GROUP BY d ORDER BY d
SETTINGS max_memory_usage = 1500000000, max_threads = 2;

-- 物理 schema 指纹：列 codec（与 migration 期望对比）
SELECT table, name, type, compression_codec
FROM system.columns
WHERE database = currentDatabase() AND table IN ('flow_records','flow_ingest_receipts','sflow_interface_counters')
ORDER BY table, position;
```

发布必须至少验证：

- migration replay/idempotency；
- 空桶 marker 与非空桶守恒；
- late arrival 生成更高 generation；
- raw/aggregate hybrid 边界无重叠无缺口；
- TopN + `_other` 守恒和稳定 tie-break；
- mixed publication/version 不被静默合并；
- SNMP counter forward/wrap/reset/gap；
- Flow/sFlow counter/SNMP 三方 5m 对账；
- 超预算查询 fail closed，无部分结果；
- backup/restore evidence 和删除审批可复现。

## 15. 代码与文档映射

| 主题 | 权威位置 |
|---|---|
| ClickHouse DDL | `deploy/migration/clickhouse/001..022` |
| Flow 写入/receipt | `internal/flowch/writer.go`, `pipeline.go`, `batch.go` |
| Flow rollup/generation | `internal/flowch/rollup.go` |
| Flow 查询编译 | `internal/flowquery/query.go`, `plan.go`, `detail.go`, `joint.go` |
| Flow billing | `internal/flowch/billing.go` |
| 三方接口对账 | `internal/flowch/interface_reconciliation.go` |
| Flow 生命周期 | `internal/flowlifecycle/*` |
| 热聚合调度 | `internal/server/flow_hot_rollup.go` |
| SNMP 样本/查询 | `internal/snmpch/write.go`, `query.go`, `aggregate.go` |
| SNMP 5m/billing | `internal/snmpch/rollup.go`, `billing.go` |
| SNMP event | `internal/snmpch/events.go` |
| 运行预算 | `internal/server/config.go`, `config/watchdog.yaml` |
| Storage V2 | `docs/flow-storage-v2-change-plan.md` |
| 生命周期/5m 复核 | `docs/watchdog-flow-data-lifecycle-review-2026-09-24.md` |
| IX 规模与 P95 | `docs/watchdog-flow-clickhouse-ix-scale-design.md` |
| SNMP ClickHouse | `docs/kiss03-snmp-clickhouse-design.md` |
| 地址发布与历史修正 | `docs/flow-address-query-plan.md` |
| （2026-09-29 复核补充）CH 原生写入与 receipt 行构造 | `internal/flowch/native.go` |
| 覆盖边界（聚合前缀 + raw 尾） | `internal/server/flow_archive.go`（`applyFlowStorageBoundary`）、`internal/server/flow_report_endpoints.go`（`endpointCorrelationRollupPlan`，自带 1h 覆盖判断） |
| migration 账本与自动应用 | `internal/flowch/migrations.go`、`internal/flowch/migrator.go`、`internal/server/install.go`（`applyClickHouseSchema`）、`cmd/watchdog-flow-migrate` |
| raw 日与归档月物理删除 | `internal/flowch/raw_delete.go` |
| SNMP 5m 重建调度 | `internal/server/snmp_collector_runtime.go` |
| 持久化计费（SNMP∩Flow 交集） | `internal/billing/service.go` |

## 16. 最终不变量

1. 原始事实与累计 counter 不就地改写。
2. 同一自然身份只由 generation 决定最新版本。
3. 聚合只有 marker 发布后可读。
4. 时间窗口统一为 UTC 半开区间 `[from,to)`。
5. 查询 source 由连续覆盖决定，不由保留策略或表存在本身决定。
6. TopN 在所有物理 source 合并后全局计算，`_other` 必须守恒。
7. billing 只使用 closed、对齐、可解释 coverage 的 5m bucket。
8. Flow raw 删除只由完整生命周期门禁授权。
9. 5m 查询缓存、5m 接口证据和 1h 生命周期归档是三种不同权威。
10. migration 账本、物理 schema、运行二进制和查询能力必须四方一致，任一 drift 均阻断破坏性变更。

（2026-09-29 复核：以下不变量当前没有被代码或生产满足，均为待闭环项，而不是现状描述。）

| 不变量 | 现状 |
|---|---|
| 3 | 新 generation 的 marker 写入失败时，会遮住已发布的旧 generation（§5.5 复核注）。 |
| 4 | SNMP 5m 按 `(start,end]` 归属样本，与 `[from,to)` 不一致（§6.2 复核注）。 |
| 5 | 只有“聚合与 raw 的分界”由覆盖决定，分辨率由展示间隔决定；joint、detail、billing、对账写死走 raw（§9.2 复核注）。 |
| 6 | 端点关联面板分段排名后拼接（§9.5 复核注）。 |
| 8 | 生产 raw 实际由人工 DROP（§12.3）。 |
| 10 | 没有任何物理 schema 指纹或 drift 阻断代码（§13）。 |

## 17. 复核结论（2026-09-29）

> 方法：按节对照 `deploy/migration/clickhouse/001..022` 与 `internal/flowch`、`internal/flowquery`、`internal/snmpch`、`internal/snmpdomain`、`internal/flowlifecycle`、`internal/server`、`internal/billing` 的现行代码，每条结论给出 `path:line`；严重项由主会话复核源码。生产只读核验于 2026-09-29 04:16 UTC 进行：ClickHouse 查询限 1.5 GB 内存、2 线程，另有 `EXPLAIN`（不执行）和 MySQL `COUNT(*)`。正文已就地加注“（2026-09-29 复核：…）”，本节汇总。

### 17.1 总体判断

- **一致的部分**：物理 schema 描述准确。经 010/011/020 的 staging 加 RENAME 替换后，§4.2 所列各表的引擎、分区、排序键、TTL，§5.1 的 raw 物理契约，§7.1 的跳数索引，§7.2 的 projection，§7.3 的 codec 与 codec 漂移结论，以及 §14 账本列、§15 路径，都与 migration 和代码吻合。
- **主要问题**：文档描述了机制，但没有写出这些机制在生产上叠加后的结果。
  - 覆盖空洞加前缀语义，使长区间报表退化为 raw 全扫描或静默零值（H1）；
  - 生产 raw 删除绕开了本文定义的生命周期，而首次发布策略还有两个会造成数据不可见或永久卡住的陷阱（H2）；
  - `FINAL` 使文中所有 raw 裁剪手段失效（H3）；
  - Flow 计费、三方对账与 5m 证据层之间没有 writer 和调用方，只是纸面闭环（H4）。

### 17.2 生产核验事实（只读）

| 项 | 结果 |
|---|---|
| migration 账本 | 001–020 applied |
| `flow_records` 分区 | 仅 `20260927`（2,353 万行）、`20260928`（2.66 亿行）、`20260929`（至 04:00 约 5,039 万行） |
| 1h 覆盖（有 marker 的小时/可读小时） | 09-21 21/15，09-22~24 各 24/24，**09-25 7/7，09-26 0**，09-27 23/23，09-28 24/24 |
| 缺 1h 的小时 | 09-21 00–02；**09-25 07:00 至 09-27 00:00 共 42 小时** |
| 1m | 09-27 19:00 之后每小时 60/60 可读 |
| `flow_aggregate_1d` | 0 行 |
| MySQL 生命周期四表 | 全为 0 行 |
| `use_skip_indexes_if_final` | 0（默认） |
| 5 分钟窗口 `EXPLAIN indexes=1` | 非 FINAL：主键 1,240 → minmax 95 个 granule；FINAL：停在 1,240 |
| SNMP 5m（近 24h） | 131,794 行中 38,575 行 coverage≈0.8，gap/reset 均为 0；最忙端口实际采样间隔约 81 秒 |
| SNMP recipe 类型 | gauge 976、counter32 1,976、counter64 918、state 953；无 string/ip/mac |
| server 日志（48h） | 无 `TOO_MANY_ROWS/BYTES`；09-28 09:33–09:35 热聚合 marker 阶段 `NOT_ENOUGH_SPACE` |

### 17.3 高优先级：缺陷与未闭环

**H1 覆盖空洞 + 前缀语义 → 长区间报表 fail-closed 或静默零值**

- **空洞一旦产生就永久存在。**
  - 热 1h 只从完整 1m 派生，1m 不完整且 raw 有记录时直接跳过（`internal/server/flow_hot_rollup.go:288-297`）；
  - 1m 只在 `minute_lookback`（生产 6h）内补齐；
  - 冷归档需要已发布的策略，而生产从未发布。
  - 结果：超过 6 小时的停机或磁盘满就会留下永久 1h 空洞，生产已有 42 小时。
- **查询按前缀读取。** `CoveredThroughAtLeast` 遇到第一个洞就停止，其后全部读 raw（`internal/flowch/rollup.go:286-297`，`internal/server/flow_archive.go:111-124`）。于是：
  - 起点早于洞的报表超出读预算而失败；
  - 洞内若 raw 已删，结果为 0，却仍标 `complete=true`（`internal/flowquery/query.go:1087-1090`）；
  - 1d 没有生产者，也没有回退到 1h。
- **放大因素。**
  - 人工删 raw 后，lookback 内没有 raw 的小时会被热调度发布为权威的空 marker；
  - 1m 的迟到修复窗口只覆盖每小时后 30 分钟（§8.1）。
- **修复方向**：分段混合读取并返回显式 gap；1d→1h 回退；在响应和导出中带出覆盖元数据；空洞告警与修复路径；登记人工删除的 raw 日。

**H2 生命周期：生产被绕开，首次发布又有两个陷阱**

- **现状。** 生命周期四表为 0 行，raw 却只剩 3 天：删除权威实际是人工 DROP，查询层无从得知。
- **陷阱 A：归档不校验 raw 是否存在。** 策略的 `bootstrap_from` 若早于 09-27，09-21 至 09-26 会在空 raw 上完成归档（0=0 通过守恒），≥2^32 的空 generation 会遮住这些日仍然存在的热 1h（`internal/flowlifecycle/archive_scheduler.go:128-162`，`internal/flowlifecycle/archive.go:137-183`）。
- **陷阱 B：换版会永久卡住旧版的日。** 发布新版只把旧版置为 retired，不迁移分区状态（`internal/flowlifecycle/store.go:222`），而删除就绪要求版本一致（`delete_readiness.go:109-112`）。
- **其他风险。**
  - late check 可能与删除竞态：候选包含 `delete_eligible` 且 `raw_deleted_at IS NULL` 的日（`archive.go:382-403`）；
  - readiness 不含 barrier 与 reclassification 两项检查；
  - barrier 只要求 active worker 确认，零个 worker 时直接视为就绪；
  - 备份证据由操作员自报；
  - reconciliation 失配后没有豁免或重置出口；
  - reconciliation 关闭时，Kafka `ErrDataLoss` 只当告警处理。

**H3 `FINAL` 使 raw 裁剪全部失效**

- 生产 `use_skip_indexes_if_final=0`，而 raw 查询尾段、detail、billing、对账全部走 FINAL。所以 `flow_event_time_minmax`、device/target/exporter 的 set 索引、IP bloom 在这些路径上都不起作用。
- 对账同样读 FINAL，用不上 `flow_ingest_audit_v2` projection。
- 022 注释中的约 6 倍收益是非 FINAL 实测，不能代表 raw 查询路径。
- **修复方向**：对重放时不变的列（`event_time`、device/target/exporter、src/dst IP）开启 `use_skip_indexes_if_final=1`，并用 `ignore_data_skipping_indices` 排除可变列（category、direction）；以 FINAL 形态 `EXPLAIN` 加 A/B 验证。另一条路是在不需要去重的路径上改用 argMax 或按 generation 过滤，替代 FINAL。

**H4 计费与对账是纸面闭环**

- **证据层缺失。** `flow_interface_traffic_5m` 没有 writer、reader 和删除路径。
- **Flow billing 算不出来。** 它对 raw FINAL 做 CROSS JOIN，没有读预算，跑在交互池上；在约 2.66 亿行/天、raw 仅保留 3 天的条件下，任何真实账期都算不出。
- **持久化计费拿不到桶。** 它要求 SNMP∩Flow 的完整交集。
- **三方对账不可用。** 接口对账函数没有调用方，默认预算下“7 天 × 1,000 scope”不可达。
- **SNMP 侧 coverage 失真。** SNMP 虽有证据层，但 coverage 与采样相位耦合，约 29% 的桶被持久化计费剔除（§8.4）。

### 17.4 中优先级

| # | 问题 | 证据 |
|---|---|---|
| M1 | 自动迁移只能前进：无暂缓开关，回滚旧二进制会使 ClickHouse 平面起不来；自动迁移连接漏传 TLS；`deploy/schema/embed.go:1-3` 仍写“进程启动时从不建表或改表” | `internal/server/server.go:174-219`，`internal/server/install.go:153-185`，`internal/flowch/migrations.go:144-146` |
| M2 | 热聚合 5m 阶段没有独立开关，部署后即开始影子写 | `internal/server/flow_hot_rollup.go:95` |
| M3 | 覆盖决策分散在三处（`applyFlowStorageBoundary`、overseas 复制版、端点关联固定 1h）；1m→1h 时间启发式回退只有报表有；`/flow/query` 多维、detail、joint 恒走 raw | `internal/server/flow_archive.go:98-166`，`internal/server/flow_report_endpoints.go:283-307`，`internal/server/flow_reports.go:474-492` |
| M4 | `approximate` 只在端点报表主面板暴露，其余近似排名对客户端不可见 | `internal/server/flow_report_endpoints.go:81,163` |
| M5 | 非复制表上所有 `insert_deduplication_token` 都不生效（Flow、SNMP、rollup），幂等只靠 ReplacingMergeTree 加 FINAL | 仓库未设 `non_replicated_deduplication_window`；perf 审计 R2.1 |
| M6 | 两套 SNMP P95 口径（HTTP 摘要把缺桶计 0，持久化计费取交集） | `internal/snmpch/billing.go:168-200`，`internal/billing/service.go:254-301` |
| M7 | 1h 的迟到窗口（2h）长于 1m（30m），而 1h 从 1m 派生；迟到 30 分钟到 2 小时的记录会让 1h 反复重建却无法收敛（第二轮更正：1m 窗口本身符合 SLA） | `internal/server/flow_hot_rollup.go:187-193,239,270-285` |
| M8 | 异步报表结果文件从不回收 | `internal/server/flow_report_jobs.go:203` |
| M9 | 删除证据查询没有资源上限，与热聚合共用 1 个 batch 连接 | `internal/flowch/rollup.go:481-547`，`internal/flowch/retention_coverage.go:46-64` |
| M10 | SNMP scoped、aggregate 与 billing 查询只靠 JOIN 过滤设备或端口，无法用主键裁剪 | `internal/snmpch/aggregate.go:258-262,310-314`，`internal/snmpch/billing.go:235-242` |

### 17.5 低优先级与文档准确性

- **潜伏缺陷**：SNMP `sample_index` 空洞会让整块写入失败（生产暂未触发，§6.1）。
- **SNMP 语义**：
  - SNMP 写入是 5 次尝试、4 次重试，不是 5 次重试；
  - width 变化不计 reset；
  - 事件搜索 256 是字节数；
  - 图表与 5m 证据的桶语义不一致；
  - 单设备 rate SQL 缺 `entity_kind='port'`。
- **文档遗漏**：
  - §5.4 漏了 `address_set` 维度；
  - §6.2 把 `generation` 写成了行键；
  - §12 的状态机语义与参数列表不全；
  - §9.6 的 25 秒和异步阈值只适用于报表；
  - §9.7 的预算档与 `do_not_merge` 覆盖面写得过宽。
- **其他**：
  - 超时不被识别为预算错误，会变成 503 并触发异步重试；
  - `*_legacy_ttl_v1` 回滚表仍有行级 TTL；
  - `flow_address_dict_source` 没有生产 writer 或 reader；
  - `metric_retention_policies` API 和页面没有消费者；
  - `require_complete` 被接收但无人读取；查询导出会丢掉 `time_windows`、`direction_split` 和地址集合；
  - `time_windows` 按源桶起点判断，1d 源在非 UTC 时区会剔除全部桶。

### 17.6 建议顺序

1. **立即做运维决策，不改代码**：
   - 首次发布 retention policy 时 `bootstrap_from` 不早于 2026-09-27，且首版就定好全部开关；
   - 人工 DROP raw 前记录日期；
   - 部署新二进制前先跑 `watchdog-flow-migrate -command inspect`，并接受“不可回滚到 021 之前的二进制”。
2. **H1 查询侧**：分段混合读取、显式 gap、1d→1h 回退。**H2 护栏**：raw 存在性校验、换版状态迁移、late check 排除删除中的日。
3. **H3**：以 FINAL 形态验证后，按列启用跳数索引。
4. **H4**：先修正 SNMP coverage（按边界拆分），再实现 `flow_interface_traffic_5m` 的 writer、回填和 reader，给 Flow billing 补读预算，最后把三方对账接到 API。
5. M 类按表内顺序处理，L 类随相关改动顺带修正。

### 17.7 核验范围与未核实项

- **已由主会话复核**：本节全部“生产核验事实”，以及 H1–H4 所引用的源码行。
- **未由主会话逐行复现**：M3、M4、M9、M10 与部分 L 类来自子任务读代码，已给出 `path:line`；late check 与删除的竞态属读代码推断，尚未复现。
- **生产事实只读核验，未做任何写入。** 按项目约定，涉及生产写入的修复（空洞修补、策略发布、索引设置）须由你在主机上执行。

## 18. 审计结论（2026-09-29 第二轮）

### 18.1 结论

Flow 的查询与生命周期**都没有闭环**。本轮沿“界面 → API → 规划器 → 覆盖判断 → SQL → ClickHouse 表”逐段追踪，并用生产失败 job 和只读数据核验。结论如下：

1. **raw 聚合后的自动清理不存在。** 删除链路全靠人工：先建审批，再手动调用删除接口（`internal/server/flow_storage_lifecycle.go:152` → `internal/flowlifecycle/raw_delete.go:115`），没有任何调度器自动删除。生产从未发布策略，raw 靠人工 DROP，且没有任何记录。
2. **不同时间区间的查询没有落到正确的聚合表。** 分辨率选择是对的：24h 以下用 1m，24h 及以上用 1h，约 150 天以上用 1d。但“覆盖前缀”规则让任何在起点之后出现洞的范围都整段退回 raw；1d 没有生产者，也没有回退；5m 表从未被选用。所以生产上**所有起点早于 09-27 01:00 的范围都会失败**。
3. **界面问题有两个独立原因。**
   - 六类卡片和总流量标题显示的是最后一个桶的值，与所选范围无关，所以看起来“仍是当天数据”。
   - 改时间范围不会自动查询，必须点“刷新”。
   - 7 天报错就是上面第 2 点：生产 job `GRY36D7V825SX9ZRD6EKN3D1XG`（09-22 06:59→09-29 06:59，单设备）在 1h 覆盖于 09-25 07:00 中断后，整段读 raw，超出设备级预算（250M 行）而终止。

### 18.2 时间区间路由矩阵

按自动规划、`target_points=300` 计算（`internal/flowquery/plan.go:57-120`）。“本轮修复后”各列需部署后才会在生产生效。

| 界面范围 | 显示间隔 | 物理源 | 修复前（生产现状） | 本轮修复后 |
|---|---|---|---|---|
| 5m–1h | 1m | 1m + 当前小时 raw 尾 | 正常（1m 在小时封闭后发布，当前小时读 raw） | 不变 |
| 3h–12h | 1m 源，1m/5m 间隔 | 1m + raw 尾 | 报表以 6h 调度窗口为界：起点更早就改用 1h，于是 6–48h 前的窗口也被粗化为小时点；`/flow/query`、查询模式、导出不做回退，起点早于 1m 保留期时整段读 raw | 各入口共用 `planFlowAggregate`：以 1m 实际保留期（48h）为界，界内保持 1m/5m 粒度，界外自动步长改用 1h |
| 24h–2d | 1h | 1h + raw 尾 | 起点在洞前时，洞后整段读 raw | 越过旧洞只读聚合，洞计为缺失 |
| 7d | 1h | 1h + raw 尾 | **失败**（生产 job 已证实） | 聚合加上 ≤1h 的 raw 尾，42 小时洞在 completeness 中报告 |
| 30d、3mo | 1h（3h/12h 间隔） | 1h | 起点没有 marker，整段读 raw 而失败 | 读已有聚合，缺失段报告 |
| 6mo、1y | 1d（1d/2d 间隔） | 1d（生产 0 行） | 整段读 raw 而失败 | 1d 覆盖不全时改用 1h 源，保持显示间隔 |
| — | — | 5m | 从未被选用 | 未改（见 O3） |

### 18.3 待修复缺陷清单

状态说明：**已部署（rNN）** 指修复已随该 release 上线生产（部署记录见 18.8）；**已修（r22，待部署）** 指已提交、待上线；其余为待办。

| ID | 优先级 | 缺陷 | 证据 | 修复方案 | 状态 |
|---|---|---|---|---|---|
| D1 | P0 | 覆盖前缀：一个洞就让之后整段读 raw，报表失败；洞内显示 0 却标 `complete=true` | `internal/server/flow_archive.go`（原 `CoveredThroughAtLeast`）、`internal/flowquery/query.go:1087-1090`、生产 job | 边界改为“最后一个可读桶之后”：旧洞计为缺失，3 小时内的新洞仍用 raw 精确补齐；归档侧 SQL 按 `minimum_generation` 过滤 | **已部署（r20）** |
| D2 | P0 | 1d 没有回退，6mo/1y 报表失败 | `internal/flowquery/plan.go:90-91`、生产 1d 为 0 行 | 1d 覆盖不全时改用 1h 源，保持显示间隔 | **已部署（r20）** |
| D3 | P0 | 1m 地平线回退只有报表有，`/flow/query`、查询模式、导出会整段读 raw；报表的回退又以 6h 调度窗口为界，把仍有 1m 数据的 6–48h 窗口粗化为小时点 | `internal/server/handlers_flow.go`、`flow_query_modes.go`、`flow_exports.go`、`flow_reports.go` | 共用 `planFlowAggregate`，以 1m 保留期（48h）为界；显式步长保持不变 | **已部署（r20）** |
| D4 | P0 | business×category 面板在起点无覆盖时，整段走 raw joint | `internal/server/flow_reports.go:746-755` | 随 D1 改为同一边界 | **已部署（r20）** |
| D5 | P0 | 六类卡片与总流量标题显示最后一个桶的值 | `frontend/src/lib/flow-report-model.ts:370-383` | 速率类显示所选范围的平均值，量类显示所选范围的总量，并加标注 | **已部署（r20）** |
| D6 | P1 | 改时间范围或设备后不自动查询 | `frontend/src/components/routes/flow-reports.tsx:514-522` | 范围和设备变化后去抖 400ms 自动刷新；其余筛选仍需手动刷新 | **已部署（r20）** |
| D7 | P1 | 报表面板不返回 completeness，前端的完整度横幅对洞不可见 | `internal/server/flow_reports.go`（面板 meta） | 聚合面板 meta 带上 `complete_ratio`、`partial` 和实际读取的源 | **已部署（r20）** |
| D8 | P0 | raw 删除没有自动闭环（见 18.1 第 1 点） | `internal/flowlifecycle/delete_readiness.go:169`、`raw_delete.go:115` | 按 18.5 的决定：策略由配置文件 `flow.lifecycle` 管理，启动时与已发布版本比对，不同即以系统用户 `system:flow-lifecycle`（disabled、无密码、无角色）发布新版本，策略写接口返回 409；`auto_delete` 由 `AutoDeleter` 每 5 分钟推进：证据齐全即审批 → 发布 delete barrier → worker 全部确认后调度删除 job，审批、barrier、回执都记在系统用户名下。所有人工路径的门禁不变；`require_kafka_coverage: false` 仅豁免 Kafka 位点覆盖（生产未启用 reconciliation） | **已部署（r20；r21/r22 改为约 1 天 raw）**：`internal/server/flow_lifecycle_config.go`、`internal/flowlifecycle/auto_delete.go`、MySQL 迁移 0051；MySQL 集成测试覆盖“配置发布 → 自动审批 → 调度 → 删除 job 完成 → 改配置换版” |
| D9 | P0 | 首次发布策略的陷阱：归档不校验 raw 是否存在；换版后分区状态搁浅；late check 与删除竞态 | §12.1、§12.2 复核注 | 归档前比较 raw 与已发布聚合的记录数，raw 少即以 `RAW_INCOMPLETE` 挂起该日（不重建、不进修复队列，调度器越过它继续推进后续日）；换版后旧版本已对账、未删除的日按新版本重新归档；late check 排除已调度删除的日 | **已部署（r20）**：`internal/flowlifecycle/archive.go`、`archive_scheduler.go`；MySQL 集成测试覆盖挂起、修复队列排除、换版重归档 |
| D10 | P1 | 热 1h 空洞一旦超出 1m 保留窗口就永久存在 | §17.3-H1 | 1h 扫描遇到缺失小时、且该小时已超出 minute lookback 时，在 1m 保留期（48h）内先从 raw 重建该小时的 60 个 1m 桶，再由 1m 派生 1h；超过 48h 的交给冷归档（按天从 raw 重建） | **已部署（r20）**（随 D22）；空洞告警待办 |
| D11 | P1 | 人工删除 raw 没有登记，查询无法区分“无流量”和“数据已删” | §12.3 复核注 | 删除登记，查询把这些日标为缺失 | 待办（随 D8） |
| D12 | P1 | SNMP coverage 与采样相位耦合，约 29% 的桶被计费剔除 | §8.4 复核注 | 跨桶的段按桶边界比例拆分 | 待办 |
| D13 | P1 | Flow 计费：没有读预算，CROSS JOIN 不下推设备条件，跑在交互池上；raw 只保留 3 天，无法出账 | §10.4 复核注 | 加预算和设备预过滤；实现接口级 5m 证据的 writer | 待办 |
| D14 | P1 | 异步报表结果文件从不清理 | `internal/server/flow_report_jobs.go:203` | 增加回收任务 | 待办 |
| D15 | P1 | 自动迁移不传 TLS；无法回滚到 021 之前的二进制 | §13 复核注 | 传入 TLS；发布手册写明先 inspect | 待办 |
| D16 | P2 | 新 generation 的 marker 写失败时，会遮住已发布的旧 generation | §5.5 复核注 | marker 写失败时强制重试；或把 generation 纳入排序键并定期清理旧代 | 待办 |
| D17 | P2 | 1h 与 1m 迟到窗口不一致，1h 反复重建却无法收敛 | §8.1 复核注 | 1h 迟到修复时先从 raw 重建该小时的 1m，再派生 1h | **已部署（r20）**（随 D22） |
| D18 | P2 | 超时未归类为预算错误，返回 503 并被重试 3 次；`approximate` 标记只有端点面板带出 | §9.6、§9.3 复核注 | 补充错误分类；所有响应都带 `approximate` | 待办 |
| D19 | P2 | 删除就绪检查不含 barrier 和 reclassification；barrier 只要求 active worker 确认；`ErrDataLoss` 只告警 | §12.2 复核注 | 并入 readiness；ACK 集合包含 draining worker；数据丢失时阻断删除 | 待办 |
| D20 | P2 | SNMP `sample_index` 空洞会让整块写入失败（生产暂未触发） | §6.1 复核注 | 写入前按保留下来的行重新编号 | 待办 |
| D21 | P2 | flowch 的 ClickHouse 集成测试在 HEAD 就有 4 个失败：`RollupQueryRepair`、`ConcurrentAggregate`、`OverseasKPIAndRepair` 的覆盖为 0；`StorageV2MigrationBackfill` 仍断言旧的 1m 表形态 | 本轮在 dev CH 26.3 上对比 HEAD 与工作区，结果相同 | 按 020/021 之后的 schema 更新断言与测试夹具 | 待办 |
| D22 | P0 | 热 rollup 按墙钟封桶：ingest 停滞后，worker 尚未写入的小时被封成空桶（marker-only）或残缺桶，而 30m/2h 迟到窗口不会再修复；查询随后显示 0 且标 `complete=true` | 生产 1h：09-29 19:00Z–09-30 00:00Z 的 marker（generation 1790731692，09-30 01:28:13Z 生成，worker 5 秒后才恢复）数据行为 0，而 raw 现有约 4,000 万行；13:00Z–18:00Z 无 1m/1h（6h minute lookback 在磁盘满期间过期） | 封桶时刻取墙钟与 ingest 水位（raw 最新 `event_time`）中较早者减去 `seal_delay`；1h 迟到修复先从 raw 重建该小时的 1m；`BucketNeedsRepair` 对 1m/1h 只在 raw 多于聚合时修复（raw 少只意味着已被删除，重建会把正确聚合清空）。生产实测水位探测 37ms、读 318 万行 | **已部署（r20）**：`internal/server/flow_hot_rollup.go`、`internal/flowch/rollup.go`；部署后把 `hour_late_arrival_window` 临时设为 `24h` 可修复 09-29 20Z–09-30 00Z（见 18.7） |
| D23 | P0 | 控制面不可达且没有 LKG 时，flow worker 启动即退出：API 被磁盘写满拖住后，worker 从 09:04 到 09:28（SGT）崩溃重启 42 次，延长了 ingest 中断；collector 同样会在重启时丢弃全部 UDP | `journalctl -u watchdog-flow-worker`：`fetch agent plan … context deadline exceeded` + `no agent plan LKG is available` | 与“没有期望 plan”同样处理：用启动参数运行，心跳恢复后拿到 plan 再重启应用；回退状态下若心跳被拒，退出码非 0 让 systemd 重启，由启动时的同步判定是否被吊销 | **已部署（r20）**：`cmd/watchdog-flow-worker`、`cmd/watchdog-flow-collect`；snmp-collector、system-agent 相同模式待办 |
| D24 | P0 | Kafka `watchdog.flow.raw-v1` 保留 `retention.ms=6h`、`retention.bytes=6GiB`，短于一次故障的时长：worker 停滞期间未消费的数据被 Kafka 删除，永久丢失 | 09-29 18:00Z 仅 186 万行、19:00Z 为 0、20:00Z 仅 212 万行（前一天同时段约 790 万、690 万）；Kafka 在 vda3（64G，仅用 6.5G） | 保留期至少覆盖最长可接受故障（建议 48h；按约 1.3GiB/h 估算需约 60GiB 时改为 24h/32GiB）；worker lag 告警 | 待办（运维，命令见 18.7） |
| D25 | P0 | 单盘容量：vda1（60G）同时承载 ClickHouse、MySQL（含 binlog）、日志。raw 保留 1 天时，第 D 天在 D+48h 才删除，峰值约为 2 天 raw（按 16–18GiB/天约 32–36GiB），还要给 merge 预留空间；ClickHouse system 日志无 TTL（约 4.3GiB）；MySQL binlog 每天约 8GiB | 09-30 凌晨磁盘写满：ClickHouse `NOT_ENOUGH_SPACE`、MySQL 写入阻塞、API 超时 | 把 ClickHouse 数据迁到独立大盘（或扩 vda1）；system 日志设 TTL；binlog 见 D26 | 待办（需你决定） |
| D26 | P1 | `snmp_collection_recipes` 两天被更新 885 万次（约 51 次/秒），ROW 格式 binlog 每天约 8GiB | `performance_schema.table_io_waits_summary_by_table` | 降低调度状态的写频率（批量或只写变化），或 `binlog_row_image=MINIMAL`；单机无复制时可缩短 binlog 保留 | 待办 |
| D27 | P2 | 热 rollup 修复过期小时每 30 分钟才完成一个：`BucketNeedsRepair` 的"需要修复"结论也被缓存，超出单次预算的候选要等满 `repair_interval` 才会再被检查 | 部署 r20 后 09-29 20Z–00Z 依次在 04:04、04:35、05:09、05:40、06:11 修复 | 只缓存"无需修复"的结论；已修复的小时靠新 generation 的 `generated_at` 避开窗口 | **已部署（r21）** |
| D28 | P1 | 冷归档 job 没有资源上限，按 runner 默认每条语句 10GiB、不限线程；生产主机 15GB 内存，ClickHouse 上限约 15.1GB | `internal/flowlifecycle/archive.go` 的 1h/1d rollup 请求未带限额 | 归档复用 `flow.hot_rollup` 的 `max_threads`/`priority`/`max_memory_bytes`（生产 4 线程、6GiB） | **已部署（r21）** |
| D29 | P0 | r21 只放宽了代码里的 `raw_retention` 下限，MySQL 0033 的 CHECK（86400..315576000）仍拒绝 6h 策略；配置同步失败，1 天策略继续生效，且 auto-deleter 只在同步成功后启动，自动删除停摆 | 生产日志 `Check constraint 'flow_retention_policy_revisions_chk_3' is violated`（r21 启动 14:32 SGT） | 迁移 0052 把该 CHECK 换成具名的 1 小时下限；配置集成测试改为经 MySQL 发布 6h 策略（缺迁移时失败） | **已修（r22，待部署）** |

### 18.4 可优化清单

| ID | 优化项 | 收益 | 前提与验证 |
|---|---|---|---|
| O1 | 对重放时不变的列启用 `use_skip_indexes_if_final=1`，并用 `ignore_data_skipping_indices` 排除可变列 | raw 尾段约 13 倍裁剪（`EXPLAIN` 实测：1,240 降到 95 个 granule） | 用 FINAL 形态 `EXPLAIN` 加 A/B 验证正确性 |
| O2 | 把发布节奏从“小时封闭后”改为每 5 分钟 | raw 尾从最长约 65 分钟缩到约 10 分钟；每个报表面板少读约 90% 的 raw | 生命周期复核 §11.2-F |
| O3 | planner 在 `[5m, 1h)` 的非端点查询接入 5m 层 | 6h、12h 图表读行数约降 20 倍 | 表已由热调度写入，只差 planner 和 reader |
| O4 | 冷归档改为“先校验热 1h，再晋升为生命周期 generation”，不再从 raw 重建 | 每天少扫约 2.66 亿行 raw | 守恒校验不变 |
| O5 | 覆盖决策收敛为一个 `resolveCoverage`，覆盖报表、查询、导出、overseas、端点 | 消除三处复制，路由一致 | 本轮已统一边界算法，overseas/端点仍各自调用 |
| O6 | Flow billing 与 SNMP scoped/billing 查询加 device/port `IN` 预过滤 | 主键前缀可以裁剪 | `EXPLAIN indexes=1` |
| O7 | 删除证据查询加资源上限，避开 batch 池；设计 receipts 的清理 | 删除就绪判定不再随 receipts 增长而变慢 | — |
| O8 | 补物理 schema 指纹检查和 codec migration 023；评估非复制表的去重窗口 | 新装库与生产一致；token 去重真正生效 | 生产只读核对 |
| O9 | 报表的多个面板共用同一段 raw 尾扫描 | 每个报表只扫一次 raw 尾 | — |

### 18.5 自动清理的决定（2026-09-30 已定）

1. **raw 保留 1 天**：r20 用 `raw_retention: 24h`，按公式第 D 天在 D+24h+max(24h, late) 归档、再过 `delete_grace` 删除，即每条记录至少保留 1 天，磁盘峰值约 2 天 raw（见 D25）。2026-09-30 修订（r21）：最小值由 24h 降为 1h，生产改为 `raw_retention: "6h"`（与 `late_arrival` 相同），第 D 天结束 6 小时后归档、再过 1 小时删除（约 D+31h），磁盘上约 1–1.3 天 raw。
2. **自动删除，由配置文件而非界面管理**：`flow.lifecycle.auto_delete: true`；策略 API 只读（写操作返回 409 `flow_lifecycle_config_managed`）。
3. **Kafka 对账门禁豁免**：生产未启用 reconciliation，`require_kafka_coverage: false`；守恒、late/grace 窗口、delete barrier、reclassification 检查照常。
4. **不要求删除前备份**：`require_backup_before_delete: false`（生产没有备份证据，也没有备份空间）。
5. **`bootstrap_from: "2026-09-29"`**：09-27、09-28 的 raw 已人工 DROP。09-29 虽缺 18:15Z–20:40Z（D24），但 raw 是这一天仅存的来源；它的冷归档（10-01 00:00Z 起）会按天从 raw 重建 1h/1d，顺带修好 D22 留下的 13:00Z–00:00Z 空桶，之后才删除 raw。
6. 09-28 当天的数据已按你的指令手动删除（`ALTER TABLE … DROP PARTITION 20260928`，同时删了 20260927），这两天只剩 1h 聚合；这次删除没有登记（D11）。

### 18.6 2026-09-30 事故复盘

| 时间（UTC） | 现象 | 根因 |
|---|---|---|
| 09-29 约 13:00 起 | 热 rollup 不再产出 1m/1h marker | vda1 写满前后，ClickHouse 写入失败；6h minute lookback 在恢复前过期，13:00–18:00 的 1m/1h 永久缺失（D10，已修） |
| 约 18:15–20:40 | 这段 flow 永久丢失 | worker 停滞超过 Kafka 保留期（6h/6GiB），未消费数据被删除（D24） |
| 01:04–01:28 | worker 崩溃重启 42 次 | MySQL 与 ClickHouse 同盘写满，API 超时；worker 没有 LKG，拉 plan 超时即退出（D23，已修） |
| 01:28 | 你 DROP 20260927/20260928 后空间释放，rollup 与 worker 同时恢复 | 热 rollup 在 worker 恢复前 5 秒按墙钟把 19:00–00:00 封成空桶（D22，已修） |
| 01:33 起 | 2 天报表 19:00、23:00、00:00 为 0，其余几个小时偏低；7 天报表 09-25 08:00 起到 09-28 全空，耗时 42s | 生产二进制仍按“覆盖前缀”在 09-25 08:00 的第一个洞后整段读 raw（D1，已修未部署），而 raw 只剩 09-29 |

### 18.7 上线步骤

1. **部署** `watchdog-server`、`watchdog-flow-worker`、`watchdog-flow-collect`。MySQL 迁移 0051 在启动时自动执行，没有新的 ClickHouse 迁移。
2. **配置** `/etc/watchdog/watchdog.yaml`：

   ```yaml
   flow:
     hot_rollup:
       hour_late_arrival_window: "24h"   # 部署后第一天用于修复 09-29 20Z–09-30 00Z，之后改回 "2h"
     lifecycle:
       enabled: true
       bootstrap_from: "2026-09-29"
       raw_retention: "24h"
       late_arrival: "6h"
       delete_grace: "1h"
       max_partitions_per_run: 1
       raw_delete: true
       auto_delete: true
       require_backup_before_delete: false
       require_kafka_coverage: false
   ```

   r21 起把 `raw_retention` 改为 `"6h"`，见 18.5 第 1 点。

3. **Kafka 保留期**（D24，vda3 有空间）：

   ```bash
   docker exec watchdog-prod-kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server 127.0.0.1:9092 --alter --entity-type topics --entity-name watchdog.flow.raw-v1 --add-config retention.ms=86400000,retention.bytes=34359738368
   ```

4. **磁盘**（D25/D26）：ClickHouse system 日志设 TTL（如 3 天）；binlog 保留缩短或改 `binlog_row_image=MINIMAL`；中期把 ClickHouse 数据迁到独立大盘。
5. **验证**：
   - 启动日志出现 `Flow lifecycle published policy version 1 from flow.lifecycle`；
   - `GET /api/v1/flow/storage/partitions` 在 10-01 00:00Z 后看到 09-29 进入 `reconciled`，约 1 小时后变为 `raw_deleted`；
   - 7 天报表在数秒内返回，09-27/09-28 有数据，洞在完整度横幅中显示。

### 18.8 部署记录（2026-09-30）

| Release | 提交 | 上线 | 结果 |
|---|---|---|---|
| r20 `20260930-flow-lifecycle-auto-delete-r20` | `4c8398b3d` `209774aae` `ccb158bc8` `2ea288d3c` `d5f47f7d3` `192fd88ba`（自 r19 基线 `bc10958f4` 起共 34 个提交） | 11:33 SGT，用户执行 `deploy.sh` | 四个服务切到 r20；agent 改用共享 token（`agents.shared_token` 经 systemd drop-in 注入）；MySQL 0050/0051、CH 021/022 生效；策略 v1 由 `system:flow-lifecycle` 发布；Kafka 保留改为 24h/32GiB；7 天报表 1.3s、全部读 1h；热 rollup 修复 09-29 13Z–09-30 00Z 与 raw 完全一致 |
| — | — | 14:10 SGT，用户执行 | 删除 09-27 的 22 个空 1h marker（`mutation_368`），这些小时改报缺失 |
| r21 `20260930-flow-raw-one-day-r21` | `e8432cfa2` `f38738bb0` `1629ba436` | 14:32 SGT | 代码与配置改为 `raw_retention: "6h"`，但策略同步被 MySQL CHECK 拒绝（D29），1 天策略继续生效；用户按兜底命令手动 DROP 了 `20260929`（其 1h 已逐小时核对与 raw 一致），磁盘降至 54% |
| r22 `20260930-flow-raw-retention-check-r22` | `bd27e6198` | 待部署 | 迁移 0052 放宽 CHECK；部署后策略 v2（6h）发布、auto-deleter 启动；09-29 会因 raw 已删被挂起为 `RAW_INCOMPLETE`（预期），09-30 于 10-01 06:00Z 归档、约 07:00Z 删除 |

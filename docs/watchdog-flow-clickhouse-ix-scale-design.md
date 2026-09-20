# Flow 数据在 IX / 骨干网规模下的 ClickHouse 存储·索引·查询设计
## —— 从 P95 计费的 5 分钟聚合起步

日期：2026-09-19　复核：2026-09-20　状态：**复核后的设计草案（禁止直接执行本文示例 DDL/SQL）**　范围：`internal/flowch`（DDL/写入/rollup/计费/对账）、`internal/flowquery`、`internal/billing`、`internal/snmpch`（作为模式来源）。
关联：[watchdog-flow-perf-audit-2026-09-19.md](watchdog-flow-perf-audit-2026-09-19.md)（根因 R1–R7，本文按其编号引用）、[flow-storage-v2-change-plan.md](flow-storage-v2-change-plan.md)（生效存储契约）。

> 本文最初基于提交 `cfbb31ce` 及当时工作树撰写。2026-09-20 复核时，`estimated_bytes_scale_ppm` 已进入迁移账本，`interface_reconciliation.go` 仍是未跟踪工作树文件；两者不能继续被笼统描述为“并行会话成果”。本节以下的历史方案仅在通过本次复核门禁后才可实施。

---

## 复核结论（2026-09-20，优先于后文历史方案）

### A. 当前事实基线

| 项 | 当前事实 | 裁决 |
|---|---|---|
| 生产版本 | ClickHouse 24.9.2.42；`flow_records` 约 5773 万行 / 3.96 GiB / 15 active parts；`flow_ingest_receipts` 约 1473 万行；`sflow_interface_counters` 约 1.21 万行 | 单机当前能运行，不等于通过 IX 容量门禁 |
| 原始事实表 | `ReplacingMergeTree(ingest_generation)`，UTC 日分区，排序键为 `(toStartOfHour(event_time), source_stream_id, kafka_partition, kafka_offset, record_index)`，无 TTL、无非复制去重窗口 | 去重仍靠 Kafka 坐标 + 查询 `FINAL`/generation 收敛；不得先删 `FINAL` |
| 生命周期 | 当前对 Flow DDL 写死 TTL 的禁令**仅是** 011/012 的文本匹配加三张活表的 DSN 门控（校正，见 remediation §4.5：`flow_vpn_candidates` 全部迁移应用后仍带 `TTL window_end + INTERVAL 90 DAY`（`001:207`，003/013 仅 ALTER），`*_legacy_*` 表亦然，018/019 无 TTL 只因作者未写）；删除仍须由 retention policy、归档覆盖、水位、保护窗口和审批驱动 | 后文所有固定 `TTL` 建议废止 |
| 热聚合 | 生产未形成可供计费/报表消费的稳定 Flow 5m/1m 热层 | 先冻结语义与重建协议，再建有限聚合目录 |
| 实时正确性 | 172.57.1.2 最近一小时约 119.8 Gbps 原始估算中，约 63.4 Gbps（约 53%）被归为 `transit`；七牛 IPv4/IPv6 CIDR 只保存到了 MySQL，保存链根本没有触发客户边界发布，旧 ACK 表为空 | 当前“Flow 只有 SNMP 一半”主要是发布闭环/分类问题，不是先靠 CH 索引解决 |
| 发布实现状态 | 客户边界 create/update/delete 只修改 `flow_device_customers/flow_customer_source_prefixes` 并写审计，不创建 dirty revision、不排队 deployment；v2 migration/API/loader/UI 仍是未跟踪工作树，生产没有 `flow_worker_deployments` 四表 | 当前根本没有端到端重发布；任何“地址已保存/版本号增加”都不得显示为已生效 |
| 投递 | Kafka topic 12 分区、RF=1、min ISR=1 | 可开发验证；不满足高可用 IX 生产门禁 |
| UDP 接收 | 生产 `rmem_max=212992`，虽请求 32 MiB 但未回读证明生效 | `kernel_drops=0` 不能证明未丢包 |

### B. 原方案中必须废止的结论

1. **固定 TTL 废止。** raw 1–7 天、1m 30 天、1h 13 个月、counter 90 天和 5m 25 个月都不能写进表 DDL。保留期必须是管理面策略，删除前需验证聚合/归档覆盖、Kafka/重建水位、账单关闭状态和删除审批。容量规划可给建议值，不能冒充数据生命周期契约。
2. **接口位置不等于业务方向。** `ingress_if_index` / `egress_if_index` 是一次转发中的入/出接口；`business_direction` 是相对客户源地址边界的流入/流出。把一条记录 `arrayJoin` 成 ingress=`in`、egress=`out` 会在设备总量上双计，也会把设备转发方向误当成本地网络方向。接口计量层和六分类业务层必须分开。
3. **ClickHouse 三表写入不存在跨表事务。** facts、counters、receipts 是三次独立 INSERT。正确顺序是 facts/counters 成功后最后写 receipt，再提交 Kafka offset；崩溃窗口由 Kafka 坐标幂等、回执对账和 repair generation 收敛，不能写成“合并为一次事务”。关键回执不得在未证明 `wait_for_async_insert` 持久语义和崩溃恢复前改用异步确认。
4. **generation 不能只按 bucket。** 重建身份至少包含 `source_kind + view + device_id + if_index + direction + bucket + classification/dimension/correction revision`。只按 bucket 取 `max(generation)` 会让一个设备/接口的新 generation 屏蔽另一个设备/接口的有效行。
5. **P95 算法不可换名替换。** 当前 Go 侧是已冻结测试向量的 nearest-rank 口径；`quantileExactInclusive(0.95)` 是否等价尚未证明。切到 CH 计算前必须用缺桶、重复桶、日 95、时区/DST、上下行合并和关闭账单向量逐项对拍。
6. **动态校准不能补齐未采样接口。** `k = counter/estimated` 仅能用于同设备、同 ifIndex、同方向、同 5m 桶且 counter 无 reset/gap、sample coverage 有效的记录。没有 packet sample 的接口不能用 counter 比例凭空分摊到 IP/地域/运营商维度；账单需冻结 k、来源和 revision，不得回写 raw 或静默重算关闭账单。
7. **物化维度必须白名单化。** 一个包含全部 IP/前缀/ASN/地域/客户组合的“万能聚合表”会产生维度笛卡尔爆炸。只允许基于真实查询模板的有限聚合目录，每项有基数预算、刷新窗口、迟到策略、重建键和回退路径。
8. **IX 性能数字是容量假设，不是验收结果。** 文中的 100k–1M samples/s、50k 行块、查询 SLO 和分片收益必须由固定硬件、固定流量夹具、真实压缩率、merge backlog、Kafka lag 和查询并发压测证明。

### C. 正确的实施顺序与门禁

1. **P0 正确性先行**：客户 CIDR 行本身就是编辑权威；保存事务完成后按规范化内容计算稳定 `source_revision`，响应/UI 只能显示“草稿已保存、需要发布”，无需另造 dirty 状态机。管理员显式选择目标 worker/device 后，operation job 固定该 source revision，异步构建 `WADS + policy + device-boundary` deployment；worker 校验、先持久化 LKG、原子热切换并 ACK `installed`。控制面只有收到目标 worker 对同一 manifest checksum/generation 的 installed ACK 才能显示“已生效”。以同一小时、同一设备、同一端口集合比较 SNMP counter、sFlow counter、packet-sample estimated；六分类之和加明确排除量必须等于 eligible total。当前 `transit≈53%` 未消除前，不做计费切流。
2. **P1 幂等/对账**：以 `(topic, partition, offset, record_index)` 为事实身份；receipt 在事实/counter 后写；按连续 offset 窗口比较 count + raw/estimated bytes/packets。任何“HTTP/ClickHouse 成功返回”不能替代回执对账。
3. **P2 接口 5m 层**：仅承载接口 counter/estimated 计量，不携带六分类；冻结 generation 作用域、迟到窗口、reset/gap、coverage 和 repair 协议。先与 SNMP/sFlow counter 对拍，再接 billing。
4. **P3 有限业务聚合**：六分类、运营商、地域等按已发布 classification/dimension revision 物化；历史重分类写新 generation，不覆盖原始事实。
5. **P4 查询与生命周期**：查询路由由时间范围、过滤条件和可用覆盖选择 raw/5m/1m/1h；生命周期策略只在归档/聚合覆盖和恢复演练通过后执行删除。
6. **发布门禁**：迁移测试、真实 MySQL+CH+Kafka 集成、崩溃点重放、双写差异、账单测试向量、24h 非空产品回归、固定硬件压测全部通过后，才允许切换读路径。

后文若与本节冲突，以本节为准；示例 DDL/SQL 只用于讨论列形状，不能作为迁移文件复制执行。

---

## 0. 现状（代码事实）

| 项 | 现状 | 证据 |
|---|---|---|
| P95 计费（flow） | 每次计算把**整个计费周期**（最长 400 天）的 `flow_records FINAL` 按 `toStartOfFiveMinutes(event_time)` 聚合，`CROSS JOIN` 外部表 `flow_billing_scope(device_id, if_index, direction)`，谓词 `ingress_if_index=… OR egress_if_index=…`（非排序键 → 全扫）；Go 侧 `bytes×8/300` → nearest-rank P95 / 日 95 均值；覆盖策略 `common_complete_5m_intersection` | [flowch/billing.go:251-276](../internal/flowch/billing.go)、[billing/compute.go:38-99](../internal/billing/compute.go)、[billing/service.go:181](../internal/billing/service.go) |
| P95 计费（SNMP） | 读**持久化**的 `snmp_interface_traffic_5m`（`row_kind='value'` + `'generation'` 标记，按已发布 generation join），每桶 in/out bytes、bps、reset/gap、coverage | [snmpch/billing.go:226-248](../internal/snmpch/billing.go) |
| SNMP 5m 生产 | 每个已关闭 5m 桶一个 job：`RebuildClosedInterfaceBucket` 从 `snmp_samples FINAL` 差分（回绕/重置/间隙处理）→ 写值行 + 写 generation 标记 | [snmpch/rollup.go:18-88](../internal/snmpch/rollup.go) |
| flow 5m 接口聚合 | **不存在**；仅计费与对账在读时临时聚合 | grep 无 `flow_interface_*` |
| sFlow 接口 counter | 018：`ReplacingMergeTree(ingest_generation) PARTITION BY toYYYYMM(event_time) ORDER BY (device_id, if_index, event_time, …kafka 坐标)`，无 TTL；每个写入块**第三次**同步 INSERT（records / receipts / counters） | [018](../deploy/migration/clickhouse/018_sflow_interface_counters.sql)、[flowch/native.go:224-229](../internal/flowch/native.go) |
| 三方对账 | 读时：raw 5m 聚合 ∪ counter 差分（窗口函数）∪ SNMP 5m，`LIMIT 250000`，未持久化、未暴露路由 | [interface_reconciliation.go:243-345](../internal/flowch/interface_reconciliation.go) |
| 静态修正 | WIP：绑定级 `estimated_bytes_scale_ppm`（ppm，范围受限，`pre_scaled` 禁用），入库时乘到 `estimated_bytes` | [flowplan/plan.go:48-50,191-195](../internal/flowplan/plan.go)、019 |
| 热窗口聚合 | 无（R1）；1h 归档只在原始保留期过完后生成；维度查询近期全走 raw `GROUP BY … FINAL` | 审计 R1 |
| raw 访问路径 | 排序键 = `(toStartOfHour(event_time), source_stream_id, kafka_partition, kafka_offset, record_index)`；零 skip index（R3）；`FINAL` 处处（R2） | 审计 R2/R3 |

**结论**：计费与对账的 5 分钟口径**已经存在但只存在于查询里**。规模化的第一步不是新算法，而是把这个 5 分钟口径**物化**成表，让计费/对账/校准/报表都从它读。

---

## 1. 规模假设与目标（IX / 骨干）

用参数而不是猜数（`.18` 实测前不写死）：

| 参数 | 记号 | 典型 IX 量级 |
|---|---|---|
| exporter 数 | E | 100–500 台 |
| 接口数 | I | 5k–50k |
| 样本总速率 | S | 100k–1M samples/s（按 §"rate" 建议配置后） |
| raw 行压缩后大小 | b | ~80–150 B/行（104 列，需 `system.parts` 实测） |
| raw 日增量 | S·86400·b | 1M/s × 120 B ≈ **10 TB/日** |
| 5m 接口聚合日增量 | I·288·(in/out×layer)·~64 B | 50k 接口 ≈ **1–3 GB/日** |
| 维度 1m 聚合日增量 | Σkind 基数 × 1440 | 取决于折叠，目标 ≤ raw 的 1/50 |

由此得到容量方向：raw 热数据应受容量预算约束，周期性查询应优先命中经过覆盖验证的聚合层；但在线时长由生命周期策略决定，不能硬编码为 1–7 天。计费切到 5m 表、对账切到 5m 表都必须先通过原始事实双读对拍。

查询 SLO（供压测判据）：dashboard 刷新 < 2 s；单账户月度 P95 计算 < 5 s；三方对账一天一设备 < 3 s；明细搜索仅限 24 h。

---

## 2. 分层聚合模型（自下而上，从 5m 起步）

```
Layer D  raw flow_records                策略保留（热盘） ── 明细 / 重分类 / 5m 重建源
   │  每 5 分钟关闭桶 job（或 MV）
Layer A  flow_interface_traffic_5m       策略保留 ── P95 计费 / 三方对账 / 校准系数（本文核心）
   │  同一 job 顺带
Layer B  flow_aggregate_1m / 1h（维度）  按覆盖与容量策略保留 ── 六类/客户/运营商/国家/ASN 报表
   │  月封存
Layer C  flow_aggregate_1d（可选）       多年 ── 容量规划 / 年报
```

### 2.1 Layer A：`flow_interface_traffic_5m`（计费级接口聚合）

**键与列**（镜像 `snmp_interface_traffic_5m`，使 billing reader 一套代码两个 source）：

```sql
CREATE TABLE flow_interface_traffic_5m (
  bucket_start   DateTime('UTC') CODEC(DoubleDelta, ZSTD(1)),
  row_kind       Enum8('value'=1,'generation'=2),
  device_id      LowCardinality(String),
  if_index       UInt32 CODEC(T64, ZSTD(1)),
  -- 三个口径分别存，避免读时再按 fact_schema/disposition 过滤 raw
  raw_in_bytes UInt64, raw_out_bytes UInt64,
  supplier_in_bytes UInt64, supplier_out_bytes UInt64,
  customer_in_bytes UInt64, customer_out_bytes UInt64,
  in_records UInt64, out_records UInt64,          -- 参与样本数（精度评估 ±196/√c）
  in_unknown UInt64, out_unknown UInt64,          -- estimated_valid=false 的样本数（静默丢失量）
  exporter_drops UInt64,                          -- Σ sFlow 样本头 drops（导出侧丢样证据）
  coverage       Float64,                         -- 桶内有样本的 30s 子区间占比（或 1.0）
  generation     UInt64 CODEC(DoubleDelta, ZSTD(1)),
  generated_at   DateTime64(3,'UTC')
) ENGINE = ReplacingMergeTree(generation)
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (bucket_start, device_id, if_index, row_kind)
PRIMARY KEY (bucket_start, device_id)
SETTINGS index_granularity = 8192;
```

该表不带固定 TTL；生命周期由管理面 policy 驱动。值行和 generation 标记必须使用同一完整 scope，禁止只按 bucket 发布。

**生产方式**：可以复用 SNMP 的“关闭桶 + generation 标记”机制，但 Flow 接口计量必须先冻结“接口位置”和“业务方向”两个不同语义。下列历史 SQL 会把一条转发记录展开到两个接口，**不得用于设备总量或六分类流入/流出**；仅在明确的逐接口转发计量场景中，且同接口、hairpin、未知接口和重复记录规则经过测试后才可采用：

```sql
INSERT INTO flow_interface_traffic_5m
SELECT toDateTime({bucket:DateTime}) bucket_start, 'value',
  device_id, ifIndex,
  sumIf(estimated_bytes, estimated_valid AND side='in')                          raw_in_bytes,  … ,
  sumIf(estimated_bytes, estimated_valid AND side='in' AND fact_schema>=2)       supplier_in_bytes, … ,
  sumIf(estimated_bytes, estimated_valid AND side='in' AND disposition='count')  customer_in_bytes, … ,
  countIf(side='in') in_records, countIf(side='in' AND NOT estimated_valid) in_unknown, … ,
  sum(exporter_drops), 1.0, {generation:UInt64}, now64(3)
FROM (
  SELECT device_id, estimated_bytes, estimated_valid, fact_schema, disposition, exporter_drops,
         arrayJoin([(ingress_if_index,'in'),(egress_if_index,'out')]) AS t,
         t.1 AS ifIndex, t.2 AS side
  FROM flow_records FINAL
  WHERE event_time >= {bucket} AND event_time < {bucket} + INTERVAL 5 MINUTE
    AND (ingress_if_index != 0 OR egress_if_index != 0)
)
GROUP BY device_id, ifIndex;
-- 然后写 generation 标记行（与 snmpch 相同）
```

要点：一个 raw 行可以贡献给 ingress 和 egress 两个**接口位置**，但不能因此同时贡献给业务“流入”和“流出”。账单绑定接口时要明确计量哪一侧；设备/客户总量应按 `business_direction` 只计一次。迟到数据用完整 scope 的 generation 重建，billing reader 只取该 scope 的已发布 generation。

**消费方**：
1. **计费**：`flowch/billing.go` 改读 `flow_interface_traffic_5m`（与 [snmpch/billing.go:226-248](../internal/snmpch/billing.go) 同形），P95 直接在 CH 内算（§4.1）；扫描量从"周期全量 raw"降到 `8640 × 端口数` 行。
2. **对账**：`interface_reconciliation.go` 三方都改读 5m 表（flow 5m ∪ counter 5m ∪ snmp 5m），去掉读时对 raw 的 5m 聚合与 counter 窗口函数。
3. **校准**（§2.3）。

### 2.2 counter 侧：把差分也物化

018 的 `sflow_interface_counters` 排序键 `(device_id, if_index, event_time, …)` 适合按接口时间窗口读取；每次对账都重跑窗口差分在 I=50k 时不可持续。建议：同一个 5m job 顺带把 counter 差分写成 `sflow_interface_traffic_5m`（in/out bytes、bps、reset/gap、coverage、完整 generation scope）。counter 原始表同样由生命周期策略删除，不能写死 90 天 TTL。三种来源可以同形，但必须保留 `source=snmp|sflow_counter|packet_estimate` 和 provenance，不能混成无法追溯的一行。

### 2.3 校准系数（动态）与 `scale_ppm`（静态）的分工

- **静态 `estimated_bytes_scale_ppm`**（并行会话 WIP）：适合**已知、稳定**的系统性偏差（如 L2/L3 帧格式差 ≈ 1–3%），写入时一次乘到 `estimated_bytes`，历史不可追溯修改（设计说明也如此）。
- **动态校准 k**：`k(device, if_index, dir, bucket) = 真值 5m 字节 / 估算 5m 字节`，真值优先 sFlow counter（同一数据报、无 SNMP 依赖），其次 SNMP。存到 `flow_interface_calibration_5m(bucket_start, device_id, if_index, dir, k_counter, k_snmp, truth_source, quality)`（或作为 Layer A 的额外列）。
- **使用**：报表新增「校准」取值口径 = `estimated × k`（与 客户/供应商/原始 并列）；**告警**：k 连续 N 桶落在 [0.9, 1.1] 之外 → 丢样/配错；`in_unknown/in_records` 高 → 倍率未知。
- **不要**把 k 回写进 raw（raw 不可修正，V2 契约）；k 只作用于展示/计费口径选择。

### 2.4 Layer B：维度热层（R1 的解法）

现有 `rollupSQL`（[flowch/rollup.go:656-763](../internal/flowch/rollup.go)）不变，改变**调度**：对当前窗口每个**已关闭** 1m（或 5m）桶持续产出 `generation=1`，1h 从 1m 派生（不再从 raw 17× `ARRAY JOIN`），日封存时策略 generation 覆盖；读侧 `max(generation)` 已兼容。top-N 折叠扩展到 `remote_port/asn/remote_prefix/local_prefix/geo.city/address_set`（R5.2）；月封存后 reap 旧 generation。**是否用 MV-on-insert**：只有在插入去重真实生效（§3.3）后才可考虑 `AggregatingMergeTree` MV（否则 at-least-once 重放会重复计入 MV，而 ReplacingMergeTree 只能收敛原表不能收敛 MV）；在此之前用关闭桶 job。

---

## 3. 表级设计细则（IX 规模）

### 3.1 raw `flow_records`
- **分区**：保持按 UTC 日；raw 在线时间由 retention policy 与容量模型决定。禁止再增加“策略之外”的 DDL TTL，因为它会绕过归档覆盖、账单关闭和删除审批。
- **排序键冲突**：去重身份需要 Kafka 坐标在前，查询裁剪需要 device/接口在前。不改主键，加**投影**：`PROJECTION p_iface (SELECT device_id, ingress_if_index, egress_if_index, toStartOfFiveMinutes(event_time) b, estimated_bytes, estimated_valid, fact_schema, disposition, exporter_drops ORDER BY (device_id, b))` 服务 5m job / 计费重建 / 对账；注意投影对 `FINAL` 无效 → 5m job 内用 `argMax(…, ingest_generation)` 按坐标去重代替 `FINAL`。
- **skip index**（R3.1）：`bloom_filter(0.01)` on `src_ip, dst_ip, local_ip, remote_ip`；`set(256)` on `device_id, exporter_id, business`；`minmax` on `remote_isp_id, remote_asn, remote_geo_country_id`。只对近期分区 `MATERIALIZE`。
- **codec**：补 `local_ip/remote_ip ZSTD`、序列号 `Delta`（R5.6）；宽列瘦身（supplier 孪生列改 delta）。
- **写入**：确定性切块（按 offset 坐标）+ 经崩溃重放证明有效的去重设置（或 Replicated*）。块大小由延迟、内存、part 率和压缩率共同决定，50k 只是上限候选。records/counters/receipts 仍是独立 INSERT：facts/counters 成功后最后写 receipt，全部成功后提交 Kafka；关键路径保持同步持久确认，除非异步确认语义经过故障注入证明。

### 3.2 5m 表（Layer A / counter / SNMP）
- `ReplacingMergeTree(generation)` + 标记行（可重建、可修复、读侧按完整 scope 的已发布 generation join）；在物理去重与 marker 一致性未证明前，不得笼统承诺“不用 FINAL”。
- `ORDER BY (bucket_start, device_id, if_index, direction, source, view, revision, row_kind)` 的具体列序由实测裁剪决定；分区按月；保留期来自 policy，不写死 25 个月。
- 计费查询模式为"时间范围 × 少量接口"，主键前置 bucket 即可裁剪；单账户月度 = 8640 桶 × 端口数。

### 3.3 去重与 `FINAL`（R2）
顺序必须是：**确定性块 → 去重窗口/复制 → 去掉 FINAL**。在此之前所有聚合从 raw `FINAL` 读（现状），但只读 5 分钟窗口，代价可控。

### 3.4 维度聚合表
只为已登记查询模板建立维度聚合；键至少包含 bucket、view、direction、dimension kind/value 与分类/地址/修正 revision。1m/1h 保留期由 policy 决定；旧 generation 只有在新 generation 覆盖完整、恢复点存在且删除保护通过后才能 reap。

---

## 4. 查询形状

### 4.1 P95：先冻结算法，再决定在 Go 或 ClickHouse 计算

旧提案 SQL 已删除，避免被误复制为生产查询。它同时存在值表 `FINAL`、marker 作用域不完整、账单冻结快照未参与严格匹配，以及 ClickHouse quantile 未证明与当前 nearest-rank 等价四个问题，不能靠追加一个 `WHERE` 修复。

正式实现应先由 billing period 冻结 `device/ifIndex/direction/source/view/classification/dimension/correction revision` 完整 scope，reader 再读取每个完整 scope 已发布的 generation。现阶段继续以 Go nearest-rank 为唯一参考算法；只有 ClickHouse 表达式与缺桶、重复桶、时区/DST、日 95、上下行合并及关闭账单测试向量全部对拍后才允许切换。日 95 必须逐日算 P95 再按合同规则汇总，不能对全月点再做一次 P95。

### 4.2 报表 / 对账 / 明细
- 报表：只读 1m/1h（Layer B）；过滤下推进两个 UNION 分支（R4.6）；两段式 top-N（R4.3）；`direction` 维度 + 面板并发（R4.4）；`use_query_cache`（R4.11）。
- 对账：三张 5m 表按 `(bucket, device_id, if_index)` join，无窗口函数、无 raw。
- 明细：raw ≤ 24 h + skip index；超出用聚合或异步导出。

---

## 5. 集群与硬件（超出单机时）

- **分片键 = `device_id`**（`cityHash64(device_id)`）：接口聚合、对账、计费全部设备本地，无跨分片 GROUP BY；Kafka 分区键已是 exporter，让 partition→shard 对齐即可让 worker 直写本地分片。
- `Replicated*MergeTree` 让 `insert_deduplication_token` 天然生效（解决 R2.1）；`Distributed` 只在维度报表跨设备汇总时使用。
- 硬件基线：raw 热盘 NVMe（写入 + merge），聚合层 SSD，冷卷对象存储/HDD；`ZSTD(1)`，`max_parts_in_total`、`parts_to_throw_insert` 按块率设置；`max_partitions_per_insert_block` 与 `MaxPartitionDays` 保持 ≤ 7。

---

## 6. 落地顺序（对应审计编号）

| 阶段 | 内容 | 依赖 |
|---|---|---|
| P0 | `.18` 实测：parts / 行大小 / read_rows / CH 版本 / 去重窗口 | — |
| P1 | 先用 `EXPLAIN indexes=1` 与 read_rows/read_bytes 实测 raw 投影、skip index、codec；验证确定性切块与去重窗口；不增加固定 TTL | P0 |
| **P2** | **`flow_interface_traffic_5m` + 关闭桶 job + billing/reconciliation 切读**（本文核心，等价于 SNMP 已有模式的复制） | P1 |
| P3 | `sflow_interface_traffic_5m`（counter 差分物化）+ 校准系数表 + 「校准」口径 + 偏差告警 | P2 |
| P4 | 维度热层：关闭桶持续 generation=1、1h 从 1m 派生、全 kind 折叠、generation reap | P2 |
| P5 | 去 FINAL（确定性块 + 去重窗口/复制之后）；分片/副本 | P1/P4 |

P2 只有在客户边界发布/ACK 与三方数值对账先通过后才是最小可交付；否则只是把错误分类和漏算更快地物化。切读前必须并行计算原始路径与 5m 路径并逐账期比较。

---

## 7. 与并行会话工作的衔接

- 他们：counters 入库（018）、读时三方对账、静态 `scale_ppm`。
- 本文：把三方都物化到 5m 表、动态 k、计费切读。两者正交；建议由流量会话按 P2→P3 顺序实施，本文档只做设计，不改代码。
- 明确不做：不用 `dictGet` 改历史；不把 k 写回 raw；不在插入去重生效前引入 MV。

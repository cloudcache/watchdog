# Watchdog 5 分钟计量原子层设计 · DDL 与迁移草案 · 存储生命周期审计

> **与代码的差异（2026-09-28 复核）**：本文有 10 处已落后于代码或与代码不一致——以代码为准，逐条见文末「与代码的差异（2026-09-28 复核）」。

日期:2026-09-23
输入:akvorado 源码(`~/Downloads/akvorado-main`)、生产 .18 live 实测(全部查询 `max_memory_usage=1.5G, max_threads=2`)、仓库当前 schema(migration 001–020)、`internal/flowch/{rollup,billing}.go`、`internal/flowlifecycle`、`config/watchdog.yaml`。
关系:承接 `watchdog-storage-lifecycle-review-2026-09-22.md`(含并行开发者的十条修订)与 `watchdog-product-review-2026-09-22.md`;不重复其结论,只在其约束下给出可落地方案。
评估状态:**§8(2026-09-23 追加)已对照生产在跑的 v13→r19 查询优化代码逐项评估**;§3/§5 中被 §8 修正的条目以 §8.2 为准,原文保留不改。

---

## 0. 紧急:根盘 99%(1.1 GB 空闲)

| 项 | live 2026-09-23 |
|---|---|
| `/`(vda1 60G) | **56G used / 1.1G free / 99%** |
| `/var/lib/watchdog/kafka`(vda3 63G) | 6.9G used / **53G free** |
| `/var/lib/clickhouse` | 40G(`flow_records` 33.79 GiB = 431M 行 / 2 天) |
| `/var/lib/mysql` | 7.1G(binlog 1.7G、redo 1.1G、数据 1.4G) |
| raw 日增 | 09-22 = 251.7M 行 / **19.65 GiB**;09-23 至 14:50 = 179.9M / 14.16 GiB |
| 摄入状态 | worker active,p11 lag 209,CH 日志暂无 disk 错误;CH `default` 盘 free 1.01 GiB |
| 09-22 热层覆盖 | 1h marker **24/24**,1m marker **1440/1440** |

ClickHouse 在 merge 时需要接近输出 part 大小的临时空间,1 GB 余量下 merge 会先停,随后插入报 `NO_FREE_DISK`;Kafka retention 6h 是真正的丢数前兜底。**必须在数小时内二选一:**

**方案 A(10 秒,破坏性):DROP 09-22 raw 分区,回收 19.65 GiB。**
```bash
ssh -p 15533 root@103.83.64.18 'CH_PW=$(docker exec watchdog-prod-clickhouse-1 printenv CLICKHOUSE_PASSWORD); docker exec watchdog-prod-clickhouse-1 clickhouse-client --password "$CH_PW" -q "ALTER TABLE watchdog_flow.flow_records DROP PARTITION 20260922"'
```
代价:09-22 的逐流明细、flow 侧计费 5m 证据(`flowch/billing.go` 现在直接扫 raw)、以及从未运行的守恒对账,永久不可得;报表不受影响(1h/1d 与两天内的 1m 仍可读)。计费当前 0 账户,实际损失是审计能力。这是手工运维删除,绕过审批状态机——与 9/19–21 那次同性质,但**这次数据是好的**。

**方案 B(约 15 分钟,无损,推荐):把 ClickHouse 冷分区放到 vda3。** ClickHouse 原生多盘存储策略,在线 `MOVE PARTITION`,不停摄入(只需重启一次 CH 加载配置,~40s,Kafka 缓冲)。
1. 宿主:`mkdir -p /var/lib/watchdog/kafka/clickhouse-data && chown 101:101 /var/lib/watchdog/kafka/clickhouse-data`(容器内 clickhouse uid=101)。
2. `/etc/watchdog/clickhouse/storage.xml`:
```xml
<clickhouse>
  <storage_configuration>
    <disks>
      <vda3><path>/var/lib/clickhouse-vda3/</path></vda3>
    </disks>
    <policies>
      <root_then_vda3>
        <volumes>
          <hot><disk>default</disk></hot>
          <cold><disk>vda3</disk></cold>
        </volumes>
        <move_factor>0.25</move_factor>   <!-- 根盘剩余 <25% 时后台自动把最老 part 挪到 vda3 -->
      </root_then_vda3>
    </policies>
  </storage_configuration>
</clickhouse>
```
3. compose 的 clickhouse 服务加两条挂载:`/var/lib/watchdog/kafka/clickhouse-data:/var/lib/clickhouse-vda3`、`/etc/watchdog/clickhouse/storage.xml:/etc/clickhouse-server/config.d/storage.xml`,`docker compose up -d clickhouse`。
4. 在线切策略并搬走昨天:
```sql
ALTER TABLE watchdog_flow.flow_records MODIFY SETTING storage_policy = 'root_then_vda3';
ALTER TABLE watchdog_flow.flow_records MOVE PARTITION 20260922 TO DISK 'vda3';
```
效果:根盘立即回收 19.65 GiB;此后 `move_factor` 自动把旧 part 溢出到 vda3(53G),raw 可保留 3–4 天。约束:新策略必须包含原 `default` 盘(满足);vda3 与 Kafka 共用分区,Kafka 已封顶 6G。这也正是 §3 分层设计需要的"热在根盘、冷在大盘"的物理基础。

**方案 C(微量):** `PURGE BINARY LOGS`(~1.1 GB)、删除空的 legacy 表(0 字节)——不解决问题,不单独做。
**方案 D:** 原计划的根盘扩容至 124G(需停机 + 快照),仍是根治,但不是今天。

建议:**立刻做 B**;若 B 未完成前根盘先归零,再做 A 兜底。

---

## 1. akvorado 的分辨率与保留期:源码级证据

| 事实 | 位置 | 原文 |
|---|---|---|
| 默认四层:raw 15d、1m 7d、**5m 90d**、**1h 1 年** | `orchestrator/clickhouse/config.go:64-74` | `Resolutions: []ResolutionConfiguration{ {Interval: 0, TTL: 15 * 24 * time.Hour}, // 15 days` / `{Interval: time.Minute, TTL: 7 * 24 * time.Hour}, // 7 days` / `{Interval: 5 * time.Minute, TTL: 3 * 30 * 24 * time.Hour}, // 90 days` / `{Interval: time.Hour, TTL: 12 * 30 * 24 * time.Hour}, // 1 year`;`MaxPartitions: 50` |
| 语义:Interval 0 = raw 表;TTL 0 = 永不过期 | `config.go:49-56` | `type ResolutionConfiguration struct { Interval time.Duration ... // An interval of 0 means no consolidation ... TTL time.Duration // A value of 0 means to never expire` |
| 文档示例与理由(raw 比 1m 留得久,因为 rollup 表没有 IP/端口) | `console/data/docs/50-configuration.md:1215-1242` | `ttl: 360h # 15 days` / `168h # 1 week` / `2160h # 3 months` / `8760h # 1 year`;1218-1222:"consolidated tables do not contain information about source/destination IP addresses and ports by default. That's why you may want to keep the interval-0 table data a bit longer" |
| TTL 与分区宽度由 TTL 派生(≈50 个分区) | `migrations_helpers.go:412-413,434-435` | `partitionInterval := uint64((resolution.TTL / time.Duration(c.config.MaxPartitions)).Seconds())`;`ttl := uint64(resolution.TTL.Seconds())`;`PARTITION BY toYYYYMMDDhhmmss(toStartOfInterval(TimeReceived, INTERVAL partitionInterval second))` + `TTL(ttlExpr)` |
| 整 part 删除 | `migrations_helpers.go:27` | 默认表设置 `"ttl_only_drop_parts": 1` |
| raw 主键以 5 分钟取整打头 | `migrations_helpers.go:437` | `fiveMinutes := sb.Function("toStartOfFiveMinutes", sb.Column("TimeReceived"))` |
| 配置变更时在线改 TTL | `migrations_helpers.go:594-603` | `if !engine.TTL().Matches(ttlExpr) { ... AlterTable(...).ModifyTTL(ttlExpr)` |
| 每个非 raw 分辨率一张表 + 一个 insert-trigger MV,且剔除高基数列 | `migrations_helpers.go:409,725-737` | `tableName = fmt.Sprintf("flows_%s", resolution.Interval)`(→ `flows_5m0s`);`createFlowsConsumerView`:`ClickHouseSkipMainOnlyColumns` |
| console 从表名反解分辨率再路由 | `console/clickhouse.go:55-57` | `strings.HasPrefix(table.Name, "flows_")` → `time.ParseDuration(strings.TrimPrefix(table.Name, "flows_"))` |

一处精确性说明:代码里 1h 的 TTL 是 `12*30*24h = 8640h(360 天)`,注释写 "1 year",文档示例写 `8760h`;"1h 留一年"是设计意图,代码字面是 360 天。

---

## 2. 引擎原生"取整"机制 → 本设计采用什么

| 机制 | ClickHouse 能力 | 采用? | 理由 |
|---|---|---|---|
| 取整排序键/分区键 | `ORDER BY (toStartOfFiveMinutes(t), …)` / `PARTITION BY toYYYYMMDD(t)` | **采用**(5m 表以 `bucket_start` 打头;raw 排序键前缀由 `toStartOfHour` 改为 `toStartOfFiveMinutes` 列入 P3) | 稀疏索引即"取整索引",裁剪粒度随之变细 |
| MV insert-trigger 预聚合 | `MATERIALIZED VIEW … TO summing_table` | **不采用于 raw→5m** | raw 是 ReplacingMergeTree,MV 在去重前消费插入块,重放/修正会被重复累加(存储审查修订 #5);块级 exactly-once(产品审查 U1)未落地前不能开 |
| `TTL … GROUP BY` 按龄降采样 | 同表内老化折叠 | **不采用于计费层**,仅作图表层可选 | 见 §4 方案 A 的三个硬伤 |
| 投影 | `ADD PROJECTION` 自动改写 | 不采用 | 与 `FINAL` 互斥;ReplacingMergeTree 读路径不受益 |
| `VersionedCollapsingMergeTree` 修正 | sign/version 抵消行 | **保留为方案 B,暂不采用** | 见 §4 |
| 查询侧取整 | `toStartOfInterval / toStartOf{FiveMinutes,Hour,Day,Week,Month,Quarter,Year}`(带时区) | **采用** | 请求前后对齐桶边界,选"满足步长的最粗层" |

结论:**取整由引擎函数完成,编排由 Go 完成**(现有 generation-marked 两阶段 rollup),这是在计费对账约束下能拿到的最大引擎红利。

---

## 3. 5 分钟计量原子层设计

### 3.1 目标与不变量
- **计量原子 = 5 分钟桶。** 月 95 / 日 95 是 5m 速率样本的分位数,只能从 5m 行算;1h/1d 只服务图表与长周期,永不作计费输入。
- 计费与图表**共用同一份 5m 事实**,`flowch/billing.go` 不再每账期扫 raw。
- 不变量:① 每桶连续 `_generation` marker(两阶段:数据成功后再发 marker);② 守恒:同一 (日, device, if_index, direction, layer) 的 5m 求和 = raw 求和(容差 0);③ 重放安全:重建=写新 generation,读取取最大代;④ 日历层(日/周/月/季/年)在**账户时区**从 1h 派生,不落物理表(UTC 1d 只作图表加速)。

### 3.2 两张 5m 表

**(a) 计费原子:`flow_interface_traffic_5m`**(与 `snmp_interface_traffic_5m` 同形,便于三方对账)
```sql
CREATE TABLE IF NOT EXISTS watchdog_flow.flow_interface_traffic_5m (
  bucket_start              DateTime('UTC')                 CODEC(DoubleDelta, ZSTD(1)),
  row_kind                  Enum8('value' = 1, 'generation' = 2),
  device_id                 LowCardinality(String),
  exporter_id               LowCardinality(String),
  if_index                  UInt32                          CODEC(T64, ZSTD(1)),
  direction                 Enum8('in' = 1, 'out' = 2),
  layer                     Enum8('raw' = 1, 'supplier' = 2, 'customer' = 3),
  raw_bytes                 UInt64 CODEC(T64, ZSTD(1)),
  raw_packets               UInt64 CODEC(T64, ZSTD(1)),
  estimated_bytes           UInt64 CODEC(T64, ZSTD(1)),
  estimated_packets         UInt64 CODEC(T64, ZSTD(1)),
  received_records          UInt64 CODEC(T64, ZSTD(1)),
  unknown_sampling_records  UInt64 CODEC(T64, ZSTD(1)),
  coverage                  Float32,
  generation                UInt64 CODEC(DoubleDelta, ZSTD(1)),
  generated_at              DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = ReplacingMergeTree(generation)
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (bucket_start, device_id, exporter_id, if_index, direction, layer, row_kind)
TTL bucket_start + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
```
`layer` 对应 `default_layer`:raw = 全部记录;supplier = `fact_schema >= 2`;customer = `disposition = 'count'`(与 `flowBillingQuerySQL` 三层口径一致)。体量:端口数 × 288 × 2 方向 × 3 层 ≈ 当前规模每天 < 30 万行、< 5 MB。

**(b) 图表维度层:`flow_aggregate_5m`**(与 1h 同 EAV 形态,**剔除 `src_ip / dst_ip / remote_port` 三种高基数 kind**——它们只存在于 1m(2 天)与 raw)
```sql
CREATE TABLE IF NOT EXISTS watchdog_flow.flow_aggregate_5m AS watchdog_flow.flow_aggregate_1h
ENGINE = ReplacingMergeTree(generation)
PARTITION BY toYYYYMMDD(bucket)
ORDER BY (bucket, dimension_kind, dimension_value, target_id, device_id, exporter_id,
          business_direction, category, business, dimension_snapshot_id, geo_version, classification_version)
TTL bucket + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
```
体量:剔除端点 kind 后约 5K 行/桶(live 1h 去掉 remote_port 后 ≈ 1M/10.3M 的量级)× 288 ≈ 1.4M 行/天 ≈ 15 MB/天,90 天 ≈ 1.4 GB。

### 3.3 生成、派生与读取
- **生成**:复用 hot rollup 调度器(`seal_delay=5m`):每个封闭小时一次 raw 扫描产出 12 个 5m 桶(与现在"封闭小时一次 raw→60×1m"同一次扫描,顺手多写一张表,**不增加 raw 读取**);两阶段:数据 INSERT 成功 → 独立 INSERT 发 `row_kind='generation'` marker。
- **派生**:1h = Σ 12 个 5m(加法量精确);1d = Σ 24 个 1h;`flow_aggregate_1m` 继续由 raw 生成、2 天 TTL,仅供 <5 分钟细粒度。
- **计费读取**:`flowch/billing.go` 的 `flowBillingQuerySQL` 改读 `flow_interface_traffic_5m`(取每桶最大代的 `value` 行,`FINAL` 仅在该小表上),保留 raw 路径作为 marker 缺口时的回退;95th/日 95/月均在 5m 行上计算,`common-complete-bucket` 策略不变(marker 缺口 = 非完整桶)。
- **查询路由**:`PlanAggregate` 层序列改为 `raw → 1m → 5m → 1h → 1d`;`from/to` 先 `toStartOfInterval` 到目标层桶边界;两端不完整桶用更细一层补齐(而非整桶多算);日历桶用 `toStartOfInterval(ts, INTERVAL 1 MONTH, tz)` 从 1h 派生。
- **守恒**:每日 seal 后,`flow_interface_traffic_5m` 按 (device, if_index, direction, layer=raw) 求和 vs raw 求和,写入既有 `flow_retention_partition_states` 对账;通过后该日 raw 才具备删除资格。

---

## 4. 两个"引擎原生"方案对比:TTL GROUP BY vs Collapsing

**方案 A:单表 `TTL … GROUP BY` 自动降采样**
```sql
CREATE TABLE watchdog_flow.flow_interface_traffic_tiered (
  hour_bucket  DateTime('UTC'),               -- 写入时 = toStartOfHour(bucket_start)
  bucket_start DateTime('UTC'),
  device_id LowCardinality(String), exporter_id LowCardinality(String),
  if_index UInt32, direction Enum8('in'=1,'out'=2), layer Enum8('raw'=1,'supplier'=2,'customer'=3),
  raw_bytes UInt64, raw_packets UInt64, estimated_bytes UInt64, estimated_packets UInt64,
  received_records UInt64, unknown_sampling_records UInt64, coverage Float32
)
ENGINE = SummingMergeTree((raw_bytes, raw_packets, estimated_bytes, estimated_packets, received_records, unknown_sampling_records))
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (hour_bucket, device_id, exporter_id, if_index, direction, layer, bucket_start)
TTL bucket_start + INTERVAL 90 DAY
    GROUP BY hour_bucket, device_id, exporter_id, if_index, direction, layer
    SET raw_bytes = sum(raw_bytes), raw_packets = sum(raw_packets),
        estimated_bytes = sum(estimated_bytes), estimated_packets = sum(estimated_packets),
        received_records = sum(received_records), unknown_sampling_records = sum(unknown_sampling_records),
        coverage = avg(coverage), bucket_start = min(bucket_start)
SETTINGS ttl_only_drop_parts = 0;   -- GROUP BY TTL 必须行级重写,不能整 part 丢弃
```

**方案 B:`VersionedCollapsingMergeTree` 承接修正**
```sql
CREATE TABLE watchdog_flow.flow_interface_traffic_5m_vc (
  bucket_start DateTime('UTC'), device_id LowCardinality(String), exporter_id LowCardinality(String),
  if_index UInt32, direction Enum8('in'=1,'out'=2), layer Enum8('raw'=1,'supplier'=2,'customer'=3),
  raw_bytes UInt64, raw_packets UInt64, estimated_bytes UInt64, estimated_packets UInt64,
  received_records UInt64, unknown_sampling_records UInt64,
  sign Int8, version UInt64
)
ENGINE = VersionedCollapsingMergeTree(sign, version)
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (bucket_start, device_id, exporter_id, if_index, direction, layer)
TTL bucket_start + INTERVAL 90 DAY DELETE SETTINGS ttl_only_drop_parts = 1;
-- 重建一个桶 = 为现存行插入 sign=-1(旧 version)+ 为新值插入 sign=+1(新 version);读:sum(estimated_bytes * sign)
```

| 维度 | A · TTL GROUP BY | B · VersionedCollapsing | C · 现状(generation-marked ReplacingMergeTree) |
|---|---|---|---|
| 编排 | 零(引擎 merge 时折叠) | 写者需先读出旧行再写抵消行 | 写者只写新代 + marker |
| 与重放/修正的关系 | **致命**:GROUP BY 跨 generation 求和,死代行会被加进结果;要求先做到块级 exactly-once 且不能存多代 | 修正可撤销;但重放同一插入块会造成 +1/−1 失衡,同样依赖块级去重 | 天然容忍重放(取最大代),代价是 FINAL + 死代行需定期清理(live:1h 表 ~90% 是旧代行) |
| 5m 粒度保留 | **折叠后 5m 消失**,95th 不可再算 → 只能在计费视界(90d)之后才允许折叠,而那时 1h 表早已存在,收益≈0 | 保留 | 保留 |
| 时点确定性 | 无(后台 merge 触发) | 有(插入即生效) | 有(marker 发布即生效) |
| 非加法量(coverage/flags) | 只能 avg/any,语义模糊 | 抵消后需重算 | 整行重写,语义清晰 |
| 与既有 marker/守恒/对账工具链 | 需重写 | 需重写(sign 语义) | 直接复用 |
| 适用位置 | 仅图表层、且仅在 exactly-once 落地后 | 未来 MV 原生链路(U1 落地后) | **计费层与全部 tier(本方案)** |

**结论:** 计费原子层用 **C(与 1m/1h/1d 同构)**;A 不进入计费路径;B 作为 U1(块级去重)落地后的下一步候选,届时可把 5m→1h 改成 MV 链。同时把 C 的短板补上——**增加"死代清理"作业**(见 §5.3)。

---

## 5. 存储数据生命周期审计(live 2026-09-23)与优化方案

### 5.1 审计发现

| # | 发现 | 数字 | 级别 |
|---|---|---|---|
| L1 | raw 无 TTL、无 published policy,按 ~20 GiB/天增长(251.7M 行/天 @ 84 B/行) | 33.79 GiB / 431M 行 / 2 天 | **P0** |
| L2 | raw **列级浪费**:`remote_ip` 4.46G(无 codec,压缩比 1.4)、`dst_ip` 2.83G、`src_ip` 1.84G、`local_ip` 1.31G(无 codec)、`sample_pool` 1.61G(无 codec)、`sample_sequence` 1.56G(**压缩比 1.0**)、三个 `*_if_index` 各 ~0.97G(无 codec)、`source_id_value` 0.94G、`remote_port` 0.82G(比 1.0) | IP 列 31%;派生列(`remote_ip/local_ip`)17%;采样元数据 9%;if_index 9% | **P1** |
| L3 | `remote_ip / local_ip` 是 `src_ip/dst_ip × business_direction` 的**派生列**,却物理存储 5.8 GiB | 17% | P1 |
| L4 | `flow_aggregate_1h` 死代行:10.3M 行中新代仅 ~1.1M(19.8K/桶 × 57),旧代(remote_port 全量扇出)仍在盘上 | ~90% 行为死代,~90 MiB | P2(读性能) |
| L5 | `flow_ingest_receipts` 180M 行 / 947 MiB,无 TTL;其中 **70.4M 行的 raw 日已被删除**,永远无法对账(含双 incarnation 冲突段) | 39% 不可对账 | P1 |
| L6 | `flow_aggregate_1m` 68.8M 行 / 720 MiB,TTL 2d 生效 | 正常 | — |
| L7 | 5 张 legacy 表(`*_legacy_ttl_v1`、`*_storage_v2_legacy`、`flow_records_legacy_hash_v1`、`flow_ingest_batches_legacy_hash_v1`)0 行 | 仅元数据噪音 | P3 |
| L8 | ClickHouse 只有 `default` 一块盘,无存储策略;根盘与 vda3 容量倒挂 | 60G@99% vs 63G@12% | **P0** |
| L9 | MySQL 7.1G(binlog 1.7G / redo 1.1G / 数据 1.4G),Kafka 6.9G(已封顶) | 稳定 | — |

### 5.2 容量模型(单 exporter,250M 行/天)

| 阶段 | B/行(压缩) | GiB/天 | 2 天 raw | 3 天 raw |
|---|---|---|---|---|
| 现状 | 84 | ~20 | 40 | 60 |
| + P1 codec(L2) | ~65 | ~15.5 | 31 | 46 |
| + 派生列改 ALIAS(L3) | ~50 | ~12 | 24 | 36 |
| 5m/1h/1d 三层合计(90d/1y/∞) | — | ~0.03 | ≈ 1.5 GB 稳态 | — |

根盘扣除 MySQL 7 + 系统/docker ~8 + 聚合层 ~2 后,可给 raw 的约 40 GB → **即便做完全部列优化,3 天 raw 也放不进根盘**;vda3(或扩容)不是可选项,是前提(§0 方案 B)。

### 5.3 优化方案(按阶段,预期收益)

**P0(今天)** §0 方案 B:根盘 -19.65 GiB,raw 冷分区落 vda3;之后 `move_factor` 自动溢出。

**P1(本周,不改语义)**
1. raw 列 codec(仅影响新 part;旧 part 在有余量时按分区 `OPTIMIZE TABLE … PARTITION … FINAL` 重写,或等 TTL/删除自然汰换):
```sql
ALTER TABLE watchdog_flow.flow_records
  MODIFY COLUMN remote_ip IPv6 CODEC(ZSTD(1)),
  MODIFY COLUMN local_ip IPv6 CODEC(ZSTD(1)),
  MODIFY COLUMN sample_sequence UInt32 CODEC(DoubleDelta, ZSTD(1)),   -- 单源单调递增,比 1.0 → 预计 <0.1
  MODIFY COLUMN sample_pool UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN ingress_if_index UInt32 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN egress_if_index UInt32 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN observation_if_index UInt32 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN source_id_value UInt32 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN remote_port UInt16 CODEC(T64, ZSTD(1));
```
   预期 -7~8 GiB/2 天(≈ -23%)。先在 dev 库用 `SELECT name, formatReadableSize(data_compressed_bytes)` 对比一小时数据验证再上线。
2. receipts 裁剪 + 有界:`ALTER TABLE flow_ingest_receipts DELETE WHERE inserted_at < '2026-09-22'`(-70M 行,需先有 §0 余量),再 `MODIFY TTL toDateTime(inserted_at) + INTERVAL 45 DAY`(legacy batches 原有 45d 的口径);双 incarnation 的 stream id 切换(产品审查 P1)同期执行。
3. **死代清理作业**(新):对 1h/1d(及未来 5m)按分区执行 `ALTER TABLE … DELETE WHERE generation < <该桶 marker 最大代>`;或在 rollup 发布新代成功后同步删除旧代行。live 收益 -90 MiB,但读路径少扫 90% 行。
4. `DROP TABLE` 五张 0 行 legacy 表。

**P2(下周,5m 原子层)** migration 021 建 §3.2 两表 → rollup 调度器增加 5m 产出(同一次封闭小时扫描)→ 回填保留期内 raw(2–3 天,每小时 ~4s 级)→ `flowch/billing.go` 切读 5m(raw 回退)→ `PlanAggregate` 加入 5m 层 → 发布 `raw_delete_enabled=false` 的 policy,让守恒对账先跑起来(以 5m 为对账对象)→ 验证 7 天连续覆盖后再开 `raw_delete_enabled`(`raw_retention_seconds` 建议 3 天,`archive_retention_seconds` 1h 层 1 年,`delete_grace` 1 天)。

**P3(之后)** raw 排序键前缀 `toStartOfHour` → `toStartOfFiveMinutes`(需重建表,配合 vda3);`remote_ip/local_ip` 改 ALIAS(先审阅读者:`flowquery/{filter,overseas,detail}.go`、`flowch/vpn_candidate.go`、`server/flow_export_rows.go`、VPN findings 四处——只有 `vpn_candidate` 按 `remote_ip` 分组需要评估无索引代价);U1 块级去重落地后评估方案 B 与 MV 链。

### 5.4 与既有约束的一致性
- 不加无条件 raw TTL(修订 #4);raw 删除仍走 policy → seal → 5m 守恒 → 审批 → `DROP PARTITION`。
- 不用 insert-trigger MV 承接 raw(修订 #5);5m 由 generation-marked 两阶段 rollup 生成(修订 #9)。
- 读取只信连续 marker 与最小可读代(修订 #6/#10);5m 加入 tier 序列后,coverage horizon 逻辑同样适用。

---

## 6. 迁移草案(顺序、回滚)

| 步 | 动作 | 回滚 |
|---|---|---|
| 0 | §0 方案 B(存储策略 + MOVE PARTITION) | `MOVE PARTITION … TO DISK 'default'`;策略保留无害 |
| 1 | P1.1 codec ALTER(元数据操作,秒级);逐分区 OPTIMIZE 在余量 >1.5× 分区大小时进行 | `MODIFY COLUMN … CODEC(...)` 改回;不需回写 |
| 2 | P1.2 receipts DELETE + TTL;P1.4 DROP legacy | DELETE 不可逆(对象本就不可对账);TTL 可 `MODIFY TTL` |
| 3 | migration 021:创建 `flow_interface_traffic_5m`、`flow_aggregate_5m`(空表,零风险) | `DROP TABLE` |
| 4 | rollup:封闭小时产出 12×5m(两阶段 marker);`minimum_generation` 门槛同 1m/1h | 关闭 5m 产出,表留空 |
| 5 | 回填保留期 raw → 5m;校验 Σ5m = Σraw 逐日逐端口 | 清空 5m 表重来 |
| 6 | `flowch/billing.go` 读 5m(marker 完整才用,否则回退 raw);`PlanAggregate` 加 5m | 开关回退到 raw 路径 |
| 7 | 发布 policy(`raw_delete_enabled=false`)→ 对账 7 天 → 开启删除 | 撤销 policy 版本 |
| 8 | P1.3 死代清理作业上线(先 1h/1d,再 5m) | 停作业;数据不受影响 |

每一步都可独立验证:`SELECT tier, uniq(bucket) markers …` 覆盖连续性、`Σ5m vs Σraw` 守恒、`system.parts` 体量、`system.query_log` 的 24h 报表耗时。

---

## 7. 执行状态(2026-09-23 追加)

| 项 | 状态 | 结果 |
|---|---|---|
| §0 止血 | 用户手工 `DROP PARTITION 20260922` | 根盘 99% → **76%**(43G used / 15G free);raw 仅剩 `20260923` |
| §5.3 P1.1 raw 列 codec | **已应用**(metadata-only) | `remote_ip/local_ip → ZSTD(1)`;`sample_sequence → DoubleDelta+ZSTD`;`sample_pool/ingress_if_index/egress_if_index/observation_if_index/source_id_value/remote_port → T64+ZSTD`。仅新 part 生效;未强制重写今日 14 GB 分区(需 ≥15 GB 临时空间),由自然 merge 与分区汰换接管 |
| §5.3 P1.3 死代清理 | **已执行**(1h、1m;1d 为空) | 谓词:`(bucket, generation) IN (… WHERE generation < 该桶 `_generation` marker 最大代)`,同步 mutation。1h **10.34M → 2.19M 行**(99.8 → 24.3 MiB);1m **69.63M → 39.09M 行**(720 → 475 MiB);校验 superseded_left = 0,live 行数与删前一致 |
| §5.3 P1.2 receipts 裁剪 | **已执行**(`inserted_at < 2026-09-23`) | **180.75M → 46.21M 行**(950 → 242 MiB);TTL 45d 尚未加(待与 stream-id 切换一并做) |
| 待办 | — | 死代清理应变成 rollup 发布新代后的常规作业(否则每次回填都会再堆死行);receipts `MODIFY TTL`;5 张空 legacy 表 DROP;§0 方案 B 的 vda3 存储策略仍是 3 天 raw 保留的前提 |

---

## 8. 对接评估:本方案 vs 当前查询优化代码(v13 → r19)

评估日期 2026-09-23。依据:仓库 HEAD `bc10958f4` + 并行开发者未提交 WIP(`rollup_joint.go`、`flow_reports.go`、新文件 `flowch/interface_reconciliation.go`);生产 release `/opt/watchdog/releases/20260923-flow-business-category-hybrid-r19`;生产 live 实测(全部 `max_memory_usage=1.5G, max_threads=2`,只读)。工作树 `go build ./...` 与 flowquery/flowch/server/billing/flowlifecycle 单测在含 WIP 状态下全部通过。

### 8.1 评估基线:生产现在跑的是什么

| 项 | 现状(代码坐标 / live) |
|---|---|
| 层选择 | `PlanAggregate`(`flowquery/plan.go:57-120`)只按时间范围与步长选 **1m / 1h / 1d**;≥24h 自动步长强制 ≥1h;分钟源桶 >10,080 才升 1h。不知道 dimension。 |
| 读边界 | `applyFlowStorageBoundary`(`server/flow_archive.go:91-127`)用 `CoveredThroughAtLeast` 求所选 tier 的**连续 marker 前缀** → `ArchiveThrough`;前缀读 aggregate、尾段读 raw,一条 SQL 内 UNION 后再全局排名(`flowquery/query.go` `storageV2QuerySQL`)。remote_port 显式值永远走 raw。 |
| 报表专用回退 | `planFlowReportAggregate`(`server/flow_reports.go:471-489`):1m 源且 `EffectiveFrom` 早于 1m 覆盖水平线(`seal_delay` + `minute_lookback`)→ 改按 1h 重新 plan。explorer(`handlers_flow.go:255`)、direction split(`flow_query_modes.go:63`)、导出(`flow_exports.go:508`)**没有**这个回退。 |
| 写路径 | hot scheduler(`server/flow_hot_rollup.go`):每个**封闭小时一次** raw→60×1m(数据 INSERT 成功后再独立 INSERT 发 60 个 marker,r17 起两阶段);60 个 1m marker 齐后 1m→1h 派生(`derivedRollupSQL`);raw 为空则 marker-only。1h→1d 只在 lifecycle 归档作业(`flowlifecycle/archive.go:161`),生产 policy 数 = 0,从未运行,1d 表 0 行。 |
| 代际 | hot < 2^32,lifecycle ≥ 2^32(`flowch/rollup.go:24`);生产 `minimum_generation: 1790070054`(旧协议低代 marker 不可读);生产 `hour_lookback: 24h`(仓库 yaml 写 72h)。 |
| live 覆盖 | 1m:09-22 1440/1440、09-23 960(至 16:00);1h:09-22 24/24、09-23 16;raw 仅剩分区 20260923(195M 行 / 15.2 GiB);根盘 64%。 |
| live 24h 成本 | raw→1m 24 次 **avg 144 s / max 215 s**,峰值 3.73 GiB;1m→1h 24 次 avg 3.5 s;marker 48 次 8 ms。交互:hybrid(1h 前缀 + raw 尾)66 次 avg 0.97 s / p90 1.56 s / **max 11.2 s(25.8M 行)**;纯 1m 2,059 次 27 ms;纯 1h 1,467 次 17 ms。 |
| 计费 | `flowch/billing.go:247-288` 仍扫 `flow_records FINAL` 聚 5m。live:**单端口 6 小时 = 7.1 s / 88.2M 行 / 3.12 GiB 读**;30 天账期外推 ≈ 120 × 7.1 s ≈ 14 分钟、>350 GiB 读,远超 `max_execution_time=120s`;且 raw 只留 1–3 天。**结论:当前任何真实账期都 fail-closed**(产品审查 B6 的量化)。 |
| 并行 WIP | `interface_reconciliation.go`:flow / sFlow counter / SNMP 三方 5m 接口证据,flow 腿扫 raw FINAL,默认预算 `MaxRowsToRead=1e8`、范围 ≤7 天;按 live 8M 行/小时,1e8 行 ≈ **12.5 小时**,整天请求即超预算 fail-closed。它和本方案的 `flow_interface_traffic_5m` 是**同一粒度**(5m × device × if_index × direction)。 |

### 8.2 方案逐项裁定

| # | 方案条目 | 裁定 | 依据 / 修正内容 |
|---|---|---|---|
| 1 | 5m = 计量原子层(§3.1) | **保留** | 计费路径唯一可行出路(8.1 计费行);1h/1d 不能算 95th。 |
| 2 | `flow_interface_traffic_5m`(§3.2a) | **保留,DDL 修正** | 需保住 `FlowBillingResult` 的 provenance 契约:加 `dimension_snapshot_ids Array(LowCardinality(String))`、`geo_versions Array(LowCardinality(String))`、`classification_versions Array(UInt32)`、`ingest_generation_min/max UInt64`;`received_records/unknown_sampling_records` 改为 `observed_records`(接口+方向匹配的全部记录,三层同值,对应 `raw_observations`)与 `known_records`(层内 `estimated_valid` 且满足层谓词,对应 `*_known`);层谓词照抄 `flowBillingQuerySQL`:raw = 全部记录、supplier = `fact_schema>=2`、customer = `disposition='count'`;删 `coverage` 列(读时 known/observed,与现在一致)。live 形态实测(一小时,从 raw,无 ARRAY JOIN 扇出):**1.5 s / 8.08M 行读 / 19 MiB 内存 / 540 行产出** → 13K 行/天,90 天 ≈ 1.2M 行、个位数 MB。 |
| 3 | `flow_aggregate_5m`(§3.2b) | **保留,改为从 1m 派生** | 不从 raw 扫:60 个 1m marker 齐后用 `derivedRollupSQL` 变体按 `toStartOfFiveMinutes(source.bucket)` 分组。live 一小时:**0.17 s / 1.38M 行读 / 24,236 行产出**(≈2,020 行/桶)→ 0.58M 行/天,90 天 ≈ 52M 行 ≈ 0.6 GB(§5.2 原估 1.4M 行/天偏高一倍)。剔除 src_ip/dst_ip/remote_port 是前提:昨日 1h 最新代 471,589 行中这三种 kind 占 412,719(87.5%)。 |
| 4 | "与 raw→1m 同一次扫描顺手多写一张表"(§3.3 生成) | **修正** | ClickHouse 一条 INSERT SELECT 只写一张表。interface-5m 是独立的第二次 raw 扫描,但成本 1.5 s vs raw→1m 144 s(+1%),可忽略;5m-EAV 走 1m 派生,零 raw 读取。 |
| 5 | 层序列 `raw→1m→5m→1h→1d`(§3.3 路由) | **修正为"1m 服务不了才用 5m"** | 只在 `interval ∈ [5m, 1h)` 且(分钟源桶 >10,080 **或** `EffectiveFrom` 早于 1m 覆盖水平线)时选 5m,上限 26,000 桶(≈90 天);其余分支不动,`plan_test.go` 现有用例全部保持。5m 表无三种端点 kind,server 层按 Dimension 排除(这三种 kind 在 >2 天范围只能走 1h,与现状一致)。**修正一个现状缺口**:按代码路径,explorer/direction split/导出对 3–7 天、步长 5m–30m 的请求今天选 1m 源,1m 只覆盖 2 天 → `CoveredThroughAtLeast` 返回起点 → 整段读 raw → raw 已删则**空结果**;5m 层直接补上这个洞。 |
| 6 | "两端不完整桶用更细一层补齐"(§3.3) | **放弃** | 当前编译器是"单一 tier + 一个 `archive_through` 切点 + raw 尾段"的两段模型,多 tier 拼接要改 `storageV2QuerySQL` 与 `covered_buckets` 语义,与 r19 的 business×category hybrid 路径正面冲突;`EffectiveFrom/To` 截断已明确交代不完整桶。 |
| 7 | 计费读 5m,"marker 缺口回退 raw"(§3.3 计费) | **保留,回退语义修正** | 缺口桶按现有 common-complete-bucket 策略记 `Observed=false`(与今天"该桶无记录"同构,`billing/service.go:255-315` 不改);raw 只作**迁移期开关**(`flow.billing.evidence_source: raw \| interface_5m`),禁止同一账期混两种证据源。`agg` = in+out 在读时求和,B2 仍是产品决策,与层无关。 |
| 8 | "Σ5m = Σraw 容差 0 通过后 raw 才可删"(§3.1 ②/§3.3 守恒) | **修正:不进删除门禁** | 门禁保持 `DayStorageCounters` raw vs 1h `total`(`archive.go:176-186`)。interface-5m 按接口×方向计数:一条记录 ingress/egress 各计一次、`if_index=0` 不计,与记录守恒不同构。改为 §8.5 的**审计查询**(日 Σ raw 层 in/out 字节 = raw `sumIf`)作计费证据自检。 |
| 9 | 日历层从 1h 派生、不落表(§3.1 ④) | **保留但标注未实现** | 编译器 `output_bucket` 是从 `{from}` 起的定宽桶(`niceIntervals` 最大 30d),月/季/年日历桶是查询编译器改动,与 5m 无关,后置。计费账期由 `billing/periods.go` 自行按账户时区切,不受影响。 |
| 10 | §4 结论(方案 C,generation-marked) | **保留** | r17 两阶段 marker 让 C 的"时点确定性"更强;TTL GROUP BY / Collapsing 结论不变。 |
| 11 | 死代清理作业(§5.3 P1.3) | **保留,加一条** | `minimum_generation` 强制再生会再次制造死代行;作业应在 hot scheduler 同进程、按桶在新 marker 发布后删旧代,而不是定期全表。 |

### 8.3 精确对接点(文件:行 → 变更)

- `internal/flowch/rollup.go`
  - `RollupResolution` 加 `RollupFiveMinute = "5m"`;`rollupTarget`(:956)加 `flow_aggregate_5m`;`rollupStatsTarget`(:783)与 `stats [3]` 扩到 4;`ValidateRollupRequest`(:804-850)允许 `Resolution=5m, SourceResolution=1m`,`BucketEnd` 对 5m 允许小时内 12 桶批;`buildRollupQuery`(:860)为 5m 选新的 `derivedRangeRollupSQL`(`toStartOfInterval(source.bucket, toIntervalSecond({bucket_seconds}))` 分组,其余同 `derivedRollupSQL`);marker SQL(:1010)已支持范围,不改。
  - 新增 interface 表的生成器:`buildInterfaceRollupQuery`(§8.2 #2 形态,层行展开)+ `row_kind='generation'` marker;`GenerationMarkers/CoveredThroughAtLeast/LatestGeneration`(:155-300)按表形分派(EAV 表 `dimension_kind='_generation'`,接口表 `row_kind='generation'`)。
  - `BucketNeedsRepair`(:344)对 5m 的 base 计数改读 1m `total` 行(同 1d 读 1h 的既有模式)。
- `internal/server/flow_hot_rollup.go`:`ScanOnce`(:71)在 1m 之后、1h 之前加 5m 扫描,判定复用 1h 的 `CoveredThroughAtLeast(1m, hour, hour+1h) == hour+1h`;interface-5m 放在 1h 候选循环里作为 raw 派生,`RawBucketHasRecords` 为空则 marker-only,计入 `MaxHourBucketsPerRun` 的重活配额。`FlowHotRollupConfig`(`config.go:115`)**不新增键**,复用 `hour_lookback / hour_late_arrival_window / repair_interval`。
- `internal/flowquery/plan.go`:`BucketFiveMinute`;:94-100 分支先试 5m(§8.2 #5 规则);`internal/flowquery/query.go:626 bucketSpec` 加 `(5m, "flow_aggregate_5m", 26_000)`。
- `internal/server/flow_reports.go:471 planFlowReportAggregate`:水平线回退改为"interval < 1h → 5m,否则 1h"。**该文件当前有并行开发者未提交改动(business×category hybrid),必须等其提交后再动。**
- `internal/server/flow_archive.go:104-110`:Bucket→resolution 映射加 5m;Dimension ∈ {src_ip, dst_ip, remote_port} 且 Bucket=5m 时改按 1h 重新 plan。
- `internal/flowch/billing.go`:`flowBillingQuerySQL` 改读 `flow_interface_traffic_5m`(published-generation join,镜像 `snmpch/billing.go:226-256`),输出列与 `ReadBilling` 结果结构**不变**;加"账期末桶必须有 marker"检查。`billing/service.go` 零改动。
- `internal/flowlifecycle/archive.go:139-166`:归档作业(policy 发布后才会跑)改为 **raw → 5m-EAV(一次 raw 扫描)→ 1h(派生)→ 1d(派生)** + interface-5m(raw),raw 扫描次数不变;守恒门禁不变。原因:冷归档时 1m 可能已过 2 天 TTL,5m 不能再从 1m 派生。
- `internal/flowch/interface_reconciliation.go`(并行 WIP):flow 腿改读 interface-5m,否则 1e8 行预算只够 12.5 小时。
- `deploy/migration/clickhouse/021_flow_five_minute_tiers.sql`:两张空表(§3.2 + §8.2 #2/#3 修正),由 `watchdog-flow-migrate -command apply` 应用(内嵌 FS,不需拷文件)。
- 测试:`plan_test`(5m 用例)、`rollup_test`(Validate/SQL 快照)、`flow_hot_rollup_test`(5m 候选与 marker-only)、`billing_test`(5m 读 + 缺口桶)、`flow_archive` 边界。

### 8.4 顺序与门禁(取代 §5.3 P2 与 §6 步 3–7)

| 步 | 动作 | 门禁 | 冲突面 |
|---|---|---|---|
| 0 | §0 方案 B 存储策略 | — | 无(决定 raw 留几天,与 5m 无关) |
| 1 | migration 021 空表 | `SHOW CREATE` | 无 |
| 2 | hot scheduler:5m-EAV(从 1m)+ interface-5m(从 raw) | 观察 24h:marker 连续;每小时成本 ≈ +0.2 s / +1.5 s;无死代堆积 | `rollup.go`(开发者上一提交刚改过,当前无未提交改动) |
| 3 | 回填:1m 保留期内(2 天)→ 5m-EAV;interface-5m 从现存 raw | Σ5m = Σ1m(逐桶) | 无 |
| 4 | 读路径:plan / bucketSpec / boundary / reports | 同一请求 5m vs 1h `total` 恒等;query_log 读行数下降;3–7 天 5m 步长请求不再为空 | **`flow_reports.go` 等并行提交** |
| 5 | 计费:`evidence_source` 开关 + 影子对比(同账期 raw vs 5m 的 95th,容差 0)→ 切换 | 影子相等 | 无(`service.go` 不改)。**这是唯一让真实账期可算的一步。** |
| 6 | lifecycle:policy 发布(`raw_delete_enabled=false`)后归档改 raw→5m→1h→1d | 守恒门禁不变 | 无(生产未运行) |
| 7 | `interface_reconciliation.go` flow 腿读 interface-5m | 整天请求不再超预算 | 并行 WIP 文件 |

### 8.5 附加发现(评估过程中,非 5m 本身)

1. **raw 子小时读取无裁剪(live 证据)**:14:00–14:05 窗口 683,856 行,实读 **6.93M 行**(整小时 7,177,549 行);排序键前缀 `toStartOfHour` 且没有任何 `event_time` 索引(020 只加了 device/target/exporter/src_ip/dst_ip/direction/category)。这是并行开发者"逐分钟任务各读 929 万行"的根因,也是 hybrid 尾段 11.2 s / 25.8M 行的根因。比 5m 表更便宜的杠杆:
   ```sql
   ALTER TABLE watchdog_flow.flow_records ADD INDEX IF NOT EXISTS flow_event_time_minmax event_time TYPE minmax GRANULARITY 1;
   -- 元数据操作,只对新 part 生效;旧 part 用 MATERIALIZE INDEX ... IN PARTITION 在预算内做
   ```
   part 内按 (hour, stream, partition, offset) 排序,offset 与 event_time 近似单调,minmax 应能把 5 分钟谓词裁到约 1/12。验证:加索引后对新 part `EXPLAIN indexes=1` + query_log `read_rows` 前后对比。这是生产 DDL 变更,需用户/并行开发者拍板,本评估未执行。若成立,"每 5 分钟发布一次 interface-5m"(不等小时封闭)也成为可能。
2. `hour_lookback` 生产 24h、仓库 yaml 72h:5m/1h 自动回填深度按生产值算,更早只能一次性回填。
3. interface-5m 计费证据自检(替代 §3.3 的守恒门禁表述):
   ```sql
   SELECT device_id, if_index,
     sumIf(estimated_bytes, direction='in')  AS in_5m,
     sumIf(estimated_bytes, direction='out') AS out_5m
   FROM flow_interface_traffic_5m FINAL
   WHERE row_kind='value' AND layer='raw' AND toDate(bucket_start)=<day>
   GROUP BY device_id, if_index
   -- 对比 raw:sumIf(estimated_bytes, estimated_valid AND ingress_if_index=if_index) / egress 同理,容差 0
   ```
4. 1h 表 87.5% 行是三种端点 kind:1h 的长期保留也可考虑把这三种 kind 单独 TTL(后置,不在本方案)。

### 8.6 结论

方案核心(5m 计量原子层 + generation-marked 方案 C)与 v13/r19 代码**兼容且互补**,可在不改现有读写协议的前提下作为第四层接入;但 §8.2 有四处必须修正(#4 不是同一次扫描、#5 路由规则收窄、#6 放弃多层拼接、#8 不进删除门禁),两处与并行开发在同一文件(`flow_reports.go`、`rollup.go`),一处比 5m 更便宜的杠杆(`event_time` minmax 索引)应先验证。计费是唯一"必须做"的驱动:live 数据证明现有 raw 路径对任何真实账期都算不出来。实施仍待用户与并行开发者确认,本节不改代码。

---

## 与代码的差异（2026-09-28 复核）

2026-09-28 全项目复核将本文与当前代码/迁移/提交逐条对照，下列各处设计已被实现取代、改名或尚未实现。**以代码为准**；正文保留作设计历史，未逐句改写。

- **§8.4/§8.6（:352-363,388）**：“实施仍待确认、本节不改代码”已过时——`021`/`022`、`RollupFiveMinute`、`scanFiveMinute` 均已落地（`423d820e2`、`c1385cd3b`、`d6c38b62e`），尚未部署生产。
- **§8.5 #1（:365-372）**：event_time minmax 索引已作为 `022` 落地，生产新 part 上 5 分钟窗口 757→122 granules（≈6×）；排序键重建仍属 P3。
- **§8.3 命名（:339,349）**：迁移实际为 `021_flow_atomic_5m.sql` + `022_flow_records_event_time_index.sql`（server 启动也会自动 apply）；常量为 `derivedFiveMinuteRollupSQL`（toStartOfFiveMinutes，排除 src_ip/dst_ip/remote_port），5m 批最长 1 天且 source 必须为 1m。
- **调度顺序（:342）**：scanFiveMinute 在 1m 与 1h 之后执行，复用 hour_lookback/MaxHourBucketsPerRun。
- **§3.2(a) interface DDL（:108-132）**：以 `021` 为准——无 TTL、按月分区（设计由归档月状态机回收，但 DropArchiveMonth 尚未加入该表）；ORDER BY (bucket_start,row_kind,device_id,exporter_id,if_index,direction,layer)；Enum 含 `'na'=0`；列为 observed/known + 版本数组 + ingest_generation_min/max。
- **迟到修复（:341）**：**未实现**——`BucketNeedsRepair` 没有 5m 分支，scanFiveMinute 只补缺失覆盖，1m 被迟到数据修复后 5m 不会重新派生（潜伏缺陷，目前无读者）。
- **未实现（:342-348）**：interface-5m 从 raw 派生、planner/bucketSpec 加 5m、报表回退、计费切读、冷归档 raw→5m、对账读 5m 均未实现（planner 只有 1m/1h/1d，billing 仍扫 `flow_records FINAL`，归档仍 raw→1h→1d）。
- **raw codec（:295）**：“已应用”仅指生产手工 ALTER；`011` 中这些列没有 CODEC，新装库拿不到——需补 forward migration。
- **hour_lookback**：仓库默认 72h 大于 1m TTL（48h），48–72h 前的小时永远补不出 5m（scanFiveMinute 要求 1m 完整）——应限制为 ≤48h 或由冷路径补齐。
- **死代清理（:258,296-298）**：无任何死代 DELETE 或 MODIFY TTL 作业。

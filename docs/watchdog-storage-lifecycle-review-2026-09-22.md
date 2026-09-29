# Watchdog 存储分层 / 生命周期 / 查询效率审查 — 对标 akvorado

> **复核状态（2026-09-28）**：逐条对照当前代码复核——已修复 23 · 部分修复 11 · 未修复 12 · 作废 2（作废 = 被后续设计决策取代，如全局共享 token、不设审批门）。逐条状态与证据见文末「复核状态（2026-09-28）」。

日期:2026-09-22
范围:原始数据 / 处理后聚合 / 报表查询三层的存储、生命周期、Kafka 消息设计;并回答"为什么 akvorado 存储与查询高效,而我们不行"。
证据:live 生产 .18 实测 + 三份代码级 deep-dive(watchdog CH 存储/查询、watchdog Kafka、akvorado 架构)。

---

## 审计修订与实施状态（同日复核）

本文初稿给出的方向基本正确，但代码、生产状态和完整失败 SQL 复核后，有五项必须修订，否则会把性能修复变成数据正确性问题：

1. `ArchiveScheduler` 并非“没有 Start”。服务启动路径已创建 worker/scheduler；生产不工作的直接原因是 `flow_retention_policy_revisions` **没有 published policy（0 行）**。调度器按设计 fail-closed，因此没有创建归档作业。这是控制面未激活，不是 goroutine 漏启动。
2. 不是“每个查询都扫 399M 行”。主键中的时间前缀会裁剪分区/范围；真实失败的 24h endpoint 查询约读 223M raw 行一次，但 candidate + tagged 两段各读一次，累计约 **446M 行 / 18.8GiB**，第三次相同重试再重复施压。核心问题是高基数 endpoint 的两遍 raw 物理扫描，不是单纯延长 timeout 能解决。
3. raw `flow_records` 的 `FINAL` **不能直接删除**。raw 是 `ReplacingMergeTree`，generation CTE 只去重 aggregate generation，不去重 raw 的逻辑版本；删掉 raw `FINAL` 会重复计费。aggregate marker 子查询可去掉 `FINAL`，读取选中 generation 的 source 仍保留 `FINAL`。
4. 不能给 raw 加无条件 `event_time + 14d` backstop TTL。它会绕过“归档覆盖 → 守恒对账 → 删除批准/证据”的生命周期屏障。raw 删除只允许走现有对账和审批状态机；未发布 policy 时宁可告警/扩容，也不能静默丢原始计费事实。
5. insert-trigger SummingMergeTree MV 不能直接承接当前写入语义。MV 在 ReplacingMergeTree 合并去重前消费 insert block；重放、修正和历史重富化会被重复累加。采用 generation-marked、可重建的 rollup cache，才能保持 replay-safe 和现有对账保证。
6. hot `minimum_generation` 不能只驱动后台重建，还必须参与读取授权。否则 raw 被人工删除后，旧 marker 仍可能让旧 aggregate 数据“复活”。当前读取只接受 `generation >= minimum_generation` 的 hot marker；生命周期高位 generation 不受该发布门槛影响。
7. v7 的主要慢点不是线程数：raw 排序键以 `toStartOfHour(event_time)` 起始，单分钟 rollup 也会读取整小时；逐分钟回填会把同一小时重复扫描 60 次。当前分钟层只在小时封闭后用一次范围数据 INSERT 扫描该小时，未封小时由查询走 raw tail。
8. 小时层禁止再 fallback 到 raw EAV 展开：分钟覆盖完整时执行 1m→1h；raw 物理桶确认为空时只写单行空 marker；两者都不满足时暂缓。生产基准从 raw 小时的 134.5 秒 / 3.02GiB 降到 1m→1h 的 4.423 秒 / 361MiB。
9. **同一条 `INSERT SELECT ... UNION ALL marker` 不是原子提交协议。** ClickHouse 可能在语句最终失败前已写入部分 block；简单 marker 分支也可能先于重聚合分支落盘。二次复审已把 rollup 改成两阶段发布：先同步写数据，成功返回后再用独立同步 INSERT 发布一个或一组 `_generation` marker。数据阶段失败绝不发 marker；marker 阶段失败时数据保持不可读，下一次以相同 generation 和分阶段确定性 token 安全重试。该改动当前为 post-v13 待发布补丁，发布时必须提高 hot `minimum_generation`，不能继续信任旧协议写出的低代 marker。
10. **物理 tier 选择必须考虑真实 coverage horizon。** 通用密度规划在 6–24h 自动报表以及较早的短窗口上可能仍选 1m，但生产仅维护配置的 `minute_lookback=6h`；请求起点位于覆盖之前时，连续前缀门禁会正确地把整窗降级到 raw。本轮服务端优化先执行通用计划，只有当它选中 1m 且计划起点早于 scheduler 的实际 1m 覆盖起点（包含 `seal_delay` 与整点封桶语义）时才改选 1h。阈值不硬编码，用户显式请求的细粒度 step 也不被偷偷粗化。

当前实施（已于 2026-09-22 分阶段发布到生产）采用统一的三层读路径：

| 层 | 表 / 分辨率 | 生成方式 | 覆盖证明 | 用途 |
|---|---|---|---|---|
| raw tail | `flow_records` | worker 批写 | 原始事实 | 仅查询尚未 seal/rollup 的尾部，以及 aggregate 不支持的 base-only filter |
| hot | `flow_aggregate_1m`（2d cache）+ `flow_aggregate_1h`（72h bridge） | 封闭小时一次 raw→60×1m；完整 1m→1h；数据成功后发布 marker | 每 bucket `_generation` marker 连续且达到最小可读代 | 近实时 6h/24h 报表；当前小时保留 raw tail |
| cold | `flow_aggregate_1h` + `flow_aggregate_1d` | published lifecycle policy 驱动；1d 从完整 1h generation 派生 | 1h 守恒对账 + 两层 marker | 多日到 400d 查询；raw/归档删除仍由审批状态机控制 |

查询边界只相信目标物理表的连续 `_generation` marker，不能仅凭 MySQL lifecycle 状态或时间推断。任何 gap 都在该点回退 raw，避免“查得快但漏一个 bucket”。

---

## 0. TL;DR / 核心结论

1. **审计时下采样/归档层在生产中从未运行。** `flow_retention_partition_states` = 0 行;`operation_jobs` 里从未出现过任何 archive/downsample 作业;`flow_aggregate_1h` / `flow_aggregate_1m` 均为 **0 行**。根因是没有 published lifecycle policy，scheduler 正确地 fail-closed。现已部署不授权删除的 hot rollup 并开始生成连续 marker；冷生命周期仍保持 fail-closed，等待发布 policy。
2. **异步查询其实已经存在。** `flow.report.query`(operation_jobs 上,即 `20260921-flow-report-async-query`)5 成功 / 6 失败;失败原因正是 5× "query exceeded a ClickHouse resource limit" + 1× 我已修复并部署的 BAD_GET。所以"异步"不是从零建,而是它跑的底层查询打 raw 超预算。
3. **Kafka retention 曾是复发问题**；部署前复核 live 已手工设置 6h+6GiB/partition，但仓库 provisioning 未固化，重建仍会复发。流量继续集中在 partition 11（按 exporter 分区，为 template 顺序的刻意取舍）。
4. **根盘压力因错误数据删除暂时解除。** 用户确认 9 月 19/20/21 数据错误并删除后，`flow_records` 只剩 `20260922` 活动分区；最终回填后根盘为 27G used / 30G free（48%），Kafka 独立分区 53G free。旧 aggregate 物理行不据此直接删除，而是由最小可读 generation 隔离并在线发布空 marker，防止旧数据被查询“复活”。
5. **已选并发布路线:不推倒重建。** 保留对账式生命周期；增加 generation-marked 1m/1h hot cache 和 1d cold tier；分钟层按封闭小时批量生成，小时层只从完整分钟层派生；endpoint 关联在完整小时覆盖后改读 rollup。异步只负责耐久执行，不再拿重试掩盖同一条确定性超限 SQL。

---

## 1. 现状快照与时间线

下表是**初始审计快照**，用于解释当时为何超时，不能再作为当前生产状态读取。当前状态以本节后的“v13 发布后快照”和 §9 为准。

| 项 | 实测 |
|---|---|
| 根分区 | 初查 `/dev/vda1` 60G/6.8G free；部署前复核 **47G used / 9.6G free(84%)**；扩容仍未生效 |
| flow_records | ReplacingMergeTree,**399,055,509 行 / 30 GiB / 49h span**,**无 TTL** |
| flow_aggregate_1h(归档层) | **0 行** |
| flow_aggregate_1m | **0 行**(且无生产写入方,dead schema) |
| flow_retention_partition_states | **0 行**(从无 day 被 seal) |
| operation_jobs 出现过的类型 | address_snapshot_build / flow.enrichment.publish / flow.report.query / flow.worker.deployment.publish —— **无任何 archive/downsample** |
| flow.report.query | 5 succeeded / 6 failed(失败=资源超限×5 + BAD_GET×1) |
| Kafka topic | `watchdog.flow.raw-v1`,12 分区,流量仍集中在 partition 11；Kafka 数据盘已独立为 63G（53G free） |
| Kafka 消息 | protobuf 包一整个**未解码 raw datagram**,~600B 均值,~2.4M/h ≈ **35 GB/day** |
| Kafka retention | 初始审计时无配置；同日部署前复核已为 **6h + 6GiB/partition**（`retention.ms=21600000`,`retention.bytes=6442450944`） |
| 消费者 | watchdog-flow-worker-v1,初查 lag 225；部署前复核 **lag 797（仍跟得上）** |
| flow_ingest_receipts | 394 MiB / 75M 行,**无 TTL 无裁剪** |

发布中间快照（11:52 CST，历史）：migration 020 已登记；`flow_aggregate_1h` 已产生连续的 03:00、04:00 UTC marker，`flow_aggregate_1m` 已开始连续回填。该时点尚未完成 24h 验收。

v13 发布后快照：1h 最近 24/24 bucket、1m 封闭六小时 360/360 bucket 均达到读取 generation floor；同形滚动 24h endpoint 作业 attempt 1 在 2.434 秒完成。MySQL lifecycle/policy 仍为 0，因此 hot coverage 已服务查询，但 cold archive 与 raw 删除仍未获授权。

---

## 2. 数据流全貌与"应然 vs 实然"

```
network → flow-collect(收 UDP + admission + 封 RawFlow)
        → Kafka watchdog.flow.raw-v1  [6h + 6GiB/partition;流量集中 partition 11]
        → flow-worker(解码 + 富化:地址库/ASN/Geo/六维/业务/VPN)
        → flow_records (ReplacingMergeTree, 无 TTL)
        → hot rollup:封闭小时 raw→1m → 1h（不授权删除）
        →〔待激活〕cold archive policy → 1h → 1d → 守恒对账 → 审批删除 raw
查询:连续 aggregate 前缀 + 未覆盖 raw 尾段；base-only filter/address-set 保留受限 raw 路径
```

关键：初始审计时因为归档从未运行，`ArchiveThrough` 等于请求起点，single-dim 与 endpoint 退化为 raw。v13 已用物理 marker 连续前缀接管读路由，不能再用该历史结论描述当前查询；仍然全 raw 的只应是 aggregate 无法表达的 base-only filter/address-set，以及当前未封尾段。

---

## 3. akvorado 为什么高效 —— 4 个可移植模式 + 我们的差距

| # | akvorado 模式 | 为什么高效 | watchdog 现状/差距 |
|---|---|---|---|
| P1 | **MV insert-trigger 下采样 tier**:一张 raw `flows` + 由 config list 声明的 `flows_1m0s/5m0s/1h0m0s`,每张挂 `MATERIALIZED VIEW ... TO ... AS SELECT toStartOfInterval(...)` 从 raw 扇出。聚合成本 O(插入 batch),无 cron、无 staleness。(`migrations_helpers.go:725-767`,`config.go:64-74`) | 宽查询永不碰 raw;新增 tier 只是往 list 加一行 | 出于代际覆盖与可修复性选择了 Go 批 `INSERT SELECT`;现已有 **1m/1h/1d** 三层，hot rollup 已运行，cold lifecycle 的 policy/state/job 仍未闭环 |
| P2 | **SummingMergeTree + 刻意粗化的 sorting key**:rollup 表把 SrcAddr/DstAddr/port 等高基数列(MainOnly)整列剔除,ORDER BY 只留可重复的维度元组;查询恒 `SUM(Bytes*SamplingRate)` → **永不需要 FINAL**。(`definition.go:246-257`,`clickhouse.go:122-143`) | 行真正被折叠;读不依赖"已完全 merge" | ReplacingMergeTree + generation-CTE 保留可重算能力；public/source 数据仍用 `FINAL`，marker 最大代际读取已去掉冗余 `FINAL`，完成标记改为数据成功后独立发布 |
| P3 | **默认 skip index**:IP/port/ASN 上 bloom(0.001),低基数分类列上 minmax/set。(`config.go:122-139`) | 非排序键的点过滤也能跳粒度 | 020 migration 已为新 part 增加 metadata skip index；旧 part 尚未统一 materialize，收益取决于生产 part 年龄与查询过滤条件 |
| P4 | **按分辨率递增的 TTL(倒挂)+ ttl_only_drop_parts**:raw 15d、1m 7d、5m 90d、1h 1y;分区宽度 = TTL/50 自适应。(`config.go:64-74`,`migrations_helpers.go:412`) | 最贵的 raw 留最短、最省的 1h 留最久;删除 O(parts) | raw 按设计无表 TTL；1m 已有 2d 分区 TTL，1h/1d 与 raw 由应用 lifecycle 管理；receipts 等元数据仍缺少有界裁剪 |
| P5 | **console 路由到"满足步长的最粗表"**:`targetInterval=(End-Start)/Points`,贪心爬到仍满足步长的最粗 tier;需要 MainOnly 列时硬回退 raw。(`console/clickhouse.go:283-335`) | 一年查询落 1h 表;明细查询才回 raw | `PlanAggregate` 已支持 **1m/1h/1d/raw**；自动报表额外按配置的 minute coverage horizon 避免 6–24h 请求因 1m 缺口整体回退 raw；显式要求 base-only 字段时仍正确回退 raw |
| (P6) | 采样率进 sorting key,查询时 `Bytes*SamplingRate` 放大,ingest 不膨胀 | 存储与采样率解耦 | watchdog 亦分列存 raw + scale_ppm(方向一致 ✅) |
| (P7) | **Kafka = 短 buffer**:输入 1d、可选输出 1h,配置文件里写明"不是存储" | 只吸收 writer 短暂不可用 | 初始无 retention；live 已设 6h+6GiB/partition，当前工作树的 dev compose 已声明同值，仍需纳入正式生产 provisioning |
| (P8) | outlet **原生批量写 CH**(async_insert + 退避 + 轮询池),不走 CH Kafka 引擎;Null 落地表 + MV 适配 schema 演进 | 写路径无小 part 风暴 | worker 已批写(方向一致);已修 CH 双池 |

> 注:当前 akvorado 已**不用** ClickHouse Kafka 表引擎(outlet 原生写),与旧博客不同 —— 我们不必为"上 Kafka 引擎"纠结。

---

## 4. 根因排名(为什么慢 / 为什么膨胀)

1. **冷归档控制面仍未激活**（无 published policy；operation_jobs 无 archive 作业；partition_states 0 行）→ hot 查询已不再全打 raw，但 raw 仍无法进入受控删除流程。**当前最高生命周期杠杆。**
2. **初始缺少 daily/hot tier，现已补齐 1m/1h/1d。** 当前未完成的是 cold policy 激活和 1d 长窗口生产验收，不再是 schema 缺失。
3. **FINAL 使用边界不清**:marker CTE 上的 `FINAL` 冗余；raw/source rows 上的 `FINAL` 是 ReplacingMergeTree 正确性要求，不能删除。
4. **endpoint 两次 raw 扫描是历史根因，v13 已收敛。** 当前 endpoint/category/business 在覆盖前缀读 aggregate，仅尾段读 raw；同形 24h 作业已从资源超限降至 2.434 秒。base-only filter 仍需单独预算。
5. **geo_version/dimension_snapshot_id/classification_version 进 GROUP BY**:会放大 series 数，但这是 provenance 正确性边界；不能为性能静默合并不同发布版本。UI/查询可显式选择版本，不能在存储层抹掉版本。
6. **Kafka retention 初始缺失已止血，但声明化闭环未验收**：live 已设 6h+6GiB/partition；partition 11 skew 是 exporter/template 顺序策略的结果，容量语义必须以时间上限为主，bytes 只作保险。
7. **receipts / reclassified / reclassification_generations / quarantined 无 TTL 无裁剪**:长期无界增长。

---

## 5. Item 1:查询效率 + 异步查询(方案对比,供决策)

> 同步审查结论：方案已从“供决策”收敛为 generation-marked Hybrid。下列 A/B/C 保留为决策记录；其中“删 raw FINAL”“无条件 raw TTL”“直接 SummingMergeTree MV”均已否决，不能进入实施。

### 5.0 共同前置(无论选哪个都必须先做)—— 见 Phase 1
- **发布 lifecycle policy，而不是修 scheduler Start**：scheduler 已启动；没有 published policy 时必须 fail-closed。先发布 `raw_delete_enabled=false` 的非破坏策略完成归档/对账回填，验证连续覆盖后再单独审批 raw 删除。
- raw **不加 backstop TTL**；磁盘阈值、Kafka retention、扩容和归档积压告警负责止血，不能绕过删除证据链。

### 5.1 Option A — akvorado 式 MV 分辨率 tier(推倒重建)
- **做法**:新增 `flow_agg_5m/1h/1d` = SummingMergeTree,由 `MATERIALIZED VIEW ... TO ...` 从 flow_records 扇出;sorting key 粗化;查询恒 SUM() 去掉 FINAL;重写查询编译器按范围选最粗表;回填历史;退役 Go rollup job。
- **难点**:我们聚合是 EAV"长表"(ARRAY JOIN 17 种 dimension_kind),要在 MV 内做扇出(每插入 block 触发,ingest 成本上升);**对账守恒机制没有等价物**(SummingMergeTree 不保证计费级精确);reclassification(历史重富化)与 MV 冲突难协调。
- **effort 高 / risk 高 / payoff 高**:当天数据也实时可查(无 raw tail),长期最干净。**但牺牲计费对账保证** —— 对一个计费平台是重代价。

### 5.2 Option B — 优化现有 rollup-job 路径(+ 补 tier)
- **做法**:启用归档(前置)→ 加 **daily tier `flow_aggregate_1d`**(同一 rollup 加 `RollupOneDay` + bucketSpec + 路由)→ 仅去 marker `FINAL` → endpoint 关联改读 aggregate → 加 metadata-only bloom/set skip index。
- **难点**:当天 tail 仍是 raw(除非再加 5m,见 C);保留 O(period) rollup 成本。
- **effort 中 / risk 低-中 / payoff 中-高**:全部增量、可 shadow A/B(对齐 perf-audit 指引);**保留对账**;大部分机器已存在。

### 5.3 Option C — Generation-marked Hybrid（已采用）
- **B 的全部** + 连续 **1m 热 cache**（复用 ReplacingMergeTree generation rollup，2 天 TTL）和 **1h 热 bridge**（72h）。它们不是计费权威，也不授权 raw 删除；生命周期的高位 generation 可确定性覆盖 hot generation。
- 每次查询先读目标 tier 的 `_generation` markers，取最大连续覆盖点；前缀读 rollup，尾部读 raw。gap 不会被时间推断掩盖。
- `flow_aggregate_1d` 从最新完整 1h generations 派生，不再读取 raw；400 天查询最多读取 400 个 source buckets。
- hot repair 先比较 base/aggregate 状态并按 `repair_interval` 节流，禁止周期性无条件重写稳定 buckets。

### 5.4 对比表

| 维度 | A(MV 重写) | B(优化现有) | C(Hybrid,推荐) |
|---|---|---|---|
| 宽范围历史查询 | ✅✅ | ✅(靠 1d tier) | ✅✅ |
| 当天近实时查询 | ✅✅ | ⚠️仍 raw | ✅(1m 热层 + 当前小时 raw tail) |
| 去 FINAL | ✅ | ✅ | ✅ |
| 计费对账保证 | ❌ 丢失 | ✅ 保留 | ✅ 保留 |
| 复用已建机器 | ❌ 大改 | ✅✅ | ✅ |
| 相对工作量 | 高 | 中 | 中-高 |
| 相对风险 | 高 | 低-中 | 中 |
| reclassification 兼容 | 难 | 保持 | 保持 |

### 5.5 异步查询建议(recommendation)
- **异步已存在**:`flow.report.query` on `operation_jobs`(submit→poll,带 idempotency/checkpoint/lease)。失败纯粹因为底层查询打 raw 超预算。
- **akvorado 没有异步查询** —— 它靠 tier 让查询同步就够快。**最好的"异步"是"让查询快到能同步"。**
- **实施**:复用 `operation_jobs`。资源上限/请求校验类 `RequestError` 标记为 terminal，禁止用三次相同重试把一次超限放大为三次；连接重置等瞬态故障仍可重试。性能来自 tier 与查询规划，不来自异步排队。

---

## 6. Kafka 消息设计 review + 修复

- **消息格式本身 OK**:一条 = 一个未解码 raw datagram(protobuf 薄封套),富化在 Kafka 之后做,支持重放/重富化 —— 与 akvorado 同源,是**优点**。~600B/条对"每 datagram 含多 sample"而言不算胖。
- **真正问题 = retention 曾经无声明化配置 + skew**。部署前复核确认 topic 已手工设置 6h + 6GiB/partition，当前 lag 797；本次不把它扩大到 24h，以免在 60G 根盘仍紧张时反向增加存储压力。修复:
  1. **时间+空间双界限**：冻结 `retention.ms=21600000` 与 `retention.bytes=6442450944`。时间型保证 skew 分区也按恢复窗口过期，bytes 是磁盘事故保险，不单独作为保留语义。
  2. **代码化 / 声明化**:写进 topic 创建路径与生产 provisioning,而非再一次手工 `kafka-configs`(上次手工修零留痕,3 天后复发)。
  3. **skew**:按 exporter 分区是为 NetFlow/IPFIX template 排序,刻意为之;exporter 少时 skew 固有。**用时间型 retention 即可控盘**,不必冒险重分区打乱 template 顺序。
- 次要:消费者组名在代码里有 3 个不同默认值(config.go:101 / main.go:109 / watchdog.yaml:26),建议统一。

---

## 7. 生命周期 / TTL 分层 review + 修复

对齐 akvorado 的"倒挂 TTL + ttl_only_drop_parts",逐表:

| 表 | 现状 | 建议 |
|---|---|---|
| flow_records(raw) | 无 TTL,靠对账删除(但 policy 未发布) | 保持无硬 TTL；发布 policy、归档守恒后走审批删除 |
| flow_aggregate_1h | 无 TTL | 冷归档权威；由 archive retention 审批按月删除 |
| flow_aggregate_1m | dead | 改成 generation-marked 1m hot cache；按日分区，2d `ttl_only_drop_parts` |
| (新)flow_aggregate_1d | 不存在 | 从完整 1h generation 派生；和同月 1h archive 联动删除 |
| flow_ingest_receipts | 394M,无裁剪 | 先定义“可删水位=已对账 offset/batch”，再裁剪；不得任意 TTL |
| flow_reclassified_records / _generations | 无裁剪 | 先建立 generation 完成/回滚证据，再按 generation 裁剪 |
| flow_quarantined_datagrams | 无裁剪 | 先冻结审计/重放保留要求，再形成独立 policy；不得直接套通用 30d |
| Kafka topic | 已手工设 6h+6GiB/partition，但原代码无声明 | provisioning 固化同一 6h+6GiB；扩大窗口需先完成容量测算 |

---

## 8. 分阶段路线图

- **Phase 0（止血）**:① Kafka topic 声明化 `retention.ms=21600000,retention.bytes=6442450944`（与 live 一致）；② 扩容/快照与磁盘告警；③ 禁止 raw backstop TTL。
- **Phase 1（已发布）**:generation marker coverage；1m hot cache；1h hot bridge；1d tier；coverage-aware raw tail；endpoint category/business aggregate correlation；确定性超限 async terminal；skip index 只加 metadata，不在迁移里 MATERIALIZE 400M 老数据。
- **Phase 2（已完成）**:同版本 ClickHouse 隔离库 migration/SQL、定向测试、Linux build、migration 020 与 server 部署均已完成；分钟范围批处理、空 marker、最小可读 generation、1m→1h 与 24h endpoint 生产基准均已通过。
- **Phase 2.1（代码已完成，待发布）**:① 将 rollup 数据和 marker 从同一 INSERT 拆成两阶段提交；故障测试覆盖“数据失败不发 marker”“marker 失败不推进完成水位”“两阶段使用不同确定性 dedup token”；② 自动报表的计划起点早于 scheduler 实际 1m coverage 起点时选择 1h，既消除 8h/12h/18h 断层，也覆盖“较早但很短”的历史窗口。发布必须设置高于旧 hot 代的 `minimum_generation` 并重新形成连续覆盖，再接受新 marker。
- **Phase 3（冷层激活）**:发布 `raw_delete_enabled=false` 的 lifecycle policy，完成小时/日回填与守恒对账。只有备份验证、连续覆盖和删除审批齐全后，才另行启用 raw 删除。
- **Phase 4（受控维护）**:新 parts 自动带 skip index；旧 400M parts 的 `MATERIALIZE INDEX` 只能按分区、按 I/O budget 逐步执行。receipts/reclassification/quarantine 各自先定义可删证据，再实现 retention。

### 上线验收指标

- 原 24h endpoint 报告不再出现 raw candidate + tagged 两次全窗口扫描；endpoint/category/business 均命中 1h aggregate coverage。
- 6h 查询在 seal tail 之外命中 1m cache；24h 命中 1h；长周期且日步长命中 1d。
- 自动 8h/12h/18h 报表不得因 1m 起点 gap 整窗 raw；当 `minute_lookback=6h` 时应计划到 1h。显式小于 1h 的 step 仍保留精度并接受 raw/资源上限约束。
- API timeout、ClickHouse `max_execution_time` 来自配置；HTTP 30s 只影响同步等待，不决定后台 SQL 上限。
- `flow.report.query` 的资源超限只失败一次并返回可操作错误，不再 attempt 1/2/3 重放确定性失败。
- 任一 marker gap 都在 `ArchiveThrough` 暴露并回退 raw，不允许以 partial aggregate 当完整结果。
- fault injection 在数据 INSERT 失败时必须观察不到新 marker；marker INSERT 失败时新 generation 必须保持不可读且完成水位不推进；相同请求重试的数据/marker token 各自稳定且彼此不同。
- raw 删除前必须同时具备：published policy、小时守恒、连续 coverage、late check、备份证据（若 policy 要求）、显式审批。

## 9. 生产发布与性能证据（2026-09-22）

- migration 020 已在 ClickHouse 24.9 生产执行；新增 1d tier、重建 1m cache 定义并以 metadata-only 方式添加 skip indexes，没有对约 4 亿老行执行一次性 `MATERIALIZE INDEX`。
- server 使用独立 interactive/batch native pool。底层 transport deadline 不再依赖 `flowch` 的隐式 2 分钟默认值：`clickhouse.operation_timeout=2m`，`clickhouse.batch_operation_timeout=15m` 均来自 YAML 并分别接线；`flow.query.execution_timeout=2m` 继续约束报表 SQL，浏览器 25 秒同步等待只决定转异步。
- 首个较轻小时桶：17,817,012 raw rows / 2.59GiB，105.9 秒完成，写入 446,756 aggregate rows。
- 第二小时桶在旧二进制下连续 6 次于约 120 秒取消；修复后同一桶读取 24,050,014 rows / 3.40GiB，170.7 秒完成，峰值约 4.06GiB，写入 498,915 rows 并发布 marker。证明此前是隐藏 transport deadline，不是 ClickHouse resource limit。
- 一轮“1 个小时桶 + 10 个分钟桶”于 11:52:04 完成，`rebuilt=11 err=<nil>`。生产复核进一步发现 `time.Ticker` 在单轮超过间隔时会保留 pending tick，不能保证冷却期；调度已改为每轮完成后再等待配置间隔。随后 query log 证明单分钟任务因 raw 首键为小时而反复读取同一小时（约 929 万行/次），所以已废弃逐分钟稳态生成：现在只对完全 seal 的小时做一次范围数据 INSERT。v13 仍在同一 INSERT 中 UNION marker；二次复审补丁已改成数据成功后独立发布 60 个 marker。`batch_max_conns=1` 保持批任务串行。
- 发布后 API 健康为 `clickhouse=true,mysql=true,runtime_ready=true`；回滚 release 与 `/etc/watchdog/watchdog.yaml.pre-storage-tiers-v2-20260922` 均保留。
- 当前生产 release 为 `/opt/watchdog/releases/20260922-flow-storage-tiers-v13`，server SHA-256 `1f1566686bcb21ef53eb0bb69d600a38a8171f58bf0714ee28efc2acf4855148`；v7–v12 release 与对应配置备份保留作快速回滚。

### 生产基数复核与二次收敛

- MySQL lifecycle 状态仍为 0，但当前读取并非只靠 MySQL `ArchiveThrough`：已部署代码在 `flowRollup` 可用时调用 ClickHouse marker coverage，以达到最小可读 generation 的连续前缀切分 aggregate/raw；只有 rollup runner 不存在时才回退 MySQL lifecycle。endpoint 关联同样按连续前缀拆分，尾段单独读 raw，不会把局部岛当完整结果，也不会因一个尾桶把整窗降级。
- 按最新有效 generation（不是包含旧代的物理总行数）统计，7 个小时桶共有约 3.82M 公共行；其中 `remote_port` 3.687M（约 96.5%），`src_ip+dst_ip` 约 106K。每小时 raw 输入约 16.4M–27.9M、aggregate 输出约 447K–873K，当前实际收缩约 35–57 倍，仍远低于低基数维度应有水平。
- 因此基数主因不是 endpoint top-1000，而是未受限的 `remote_port`。rollup 将 remote-port 每组 top-N 限为 256，长尾守恒折入 `_other`；明确查询某个端口时强制 raw 以保持精确，普通 top-N 报表标记 `approximate=true`。IP 仍保留 1000/组。
- batch 连接池隔离不等于 ClickHouse CPU/I/O 隔离。hot rollup 新增配置化 `max_threads=4`、`priority=10`、`max_memory_bytes=6GiB`，外部 GROUP BY spill 阈值随内存预算下调；仍保持 batch 单连接、每轮完成后冷却。
- 仓库默认 `hour_lookback=72h`，当前生产 override 为 **24h**；文中“72h bridge”描述的是能力上限，不是 live coverage。24h 已验收不代表 36h/72h 已命中 aggregate；在 cold policy 仍为空时，超过生产 hour lookback 的窗口可能退回 raw。扩大 hour lookback 必须先测磁盘、回填 I/O 和交互延迟，不能只改配置。
- `minimum_generation=1790057131` 同时是在线重建门槛和查询读取门槛；低于它的 hot marker 不再授权 aggregate 读取。生命周期高位 generation 仍保持权威。旧物理行后续随 1m TTL 或受控维护回收。
- v7 首个生产小时桶实测读取 12,953,946 raw rows，63.7 秒完成；`system.query_log.Settings` 证实 `max_threads=4,priority=10,max_memory_usage=6442450944`，峰值 2.14GiB。新代仅写 19,769 rows；相邻旧形状桶写 437,022 rows，端口长尾收敛使输出缩小约 22 倍。随后 60 个分钟桶同轮完成，`rebuilt=61 err=<nil>`，API 健康保持全绿。
- v7 仍慢的进一步根因：逐分钟任务各自因小时排序键读取约 929 万 raw rows；某次旧 raw 小时 fallback 读取 11.66M rows、134.5 秒、峰值 3.02GiB。v10 起禁止该 fallback：完整分钟源才派生；物理 raw 为空只写 marker（生产实测 5ms / read 1 / write 1）；非空但分钟不完整则暂缓。
- v11 首轮生产执行 `rebuilt=77 err=<nil>`：60 个分钟桶加已删除区间的空小时一次清理，空 marker 不占重任务预算。1m→1h 首个非空桶实测 4.423 秒、361.34MiB、写 19,638 行，且 SQL 不含 `flow_records`；相对 134.5 秒 raw 小时路径约快 30 倍、峰值内存下降约 8.5 倍。
- v12 将分钟水位对齐到 seal 后的完整 UTC 小时。当前小时始终作为 raw tail，因此稳态每小时只做一次 raw EAV 展开，不再每分钟重复扫描当前小时。发布后 `/api/v1/health` 为 ClickHouse/MySQL/runtime 全绿。
- v13 把 endpoint category/business 关联从“任一尾桶缺 marker 就整窗 raw”改为 aggregate 连续前缀 + raw 尾段，结果在服务端合并。滚动到当前窗口的同一 24h source-endpoint payload 作业 `A07A4EE0CD84D1524ED154241D` attempt 1 成功，端到端 2.434 秒；query log 不再出现约 78.99M read rows 的 raw-only 关联。关联查询读约 676K 1h rows、0.11–0.13 秒；主 endpoint/direction 的 aggregate+raw-tail 查询读约 3.82M–4.26M rows、0.35–0.42 秒。API 健康保持全绿。

---

## 附:关键证据坐标

- ClickHouse 24.9 隔离验证：migration 020 全部语句执行成功；代码生成的 1d `INSERT SELECT` 直接执行成功；验证库随后删除。
- 全新安装兼容性复核发现 released migration 001 的 DateTime64 TTL 在 ClickHouse 24.9 报 `BAD_TTL_EXPRESSION`。不修改 released bytes/checksum；migrator 仅在执行 v001 时把两个临时 TTL 包装为 `toDateTime(...)`，随后 migration 011 会重建并移除这些 legacy TTL。
- 归档未激活:live `flow_retention_policy_revisions` published=0、`flow_retention_partition_states`=0、`operation_jobs` 无 archive 类型;代码 [internal/server/flow_archive.go](../internal/server/flow_archive.go),[internal/flowch/rollup.go](../internal/flowch/rollup.go),[internal/flowlifecycle/archive.go](../internal/flowlifecycle/archive.go)
- 无 TTL(测试强制):[internal/flowch/schema_test.go:110](../internal/flowch/schema_test.go),[internal/flowch/migrations.go:179](../internal/flowch/migrations.go)
- 查询路径:[internal/flowquery/query.go](../internal/flowquery/query.go),[internal/flowquery/joint.go](../internal/flowquery/joint.go),[internal/flowquery/plan.go:81](../internal/flowquery/plan.go)
- Kafka:[internal/flowstream/rawflow.go:85](../internal/flowstream/rawflow.go)(ExporterKey/skew),[internal/flowstream/producer.go](../internal/flowstream/producer.go)；初始仓库无 retention，live 已手工止血，当前 dev compose 工作树已加入 6h+6GiB/partition；复发记录 docs/watchdog-flow-perf-audit-2026-09-19.md:420-424
- 异步已存在:live `operation_jobs.job_type='flow.report.query'`
- akvorado:orchestrator/clickhouse/{config.go:64-74,migrations_helpers.go:401-459,725-767},common/schema/{definition.go,clickhouse.go:122-152},console/clickhouse.go:283-335,config/akvorado.yaml:22-28

---

## 复核状态（2026-09-28）

本节由 2026-09-28 全项目复核生成：每条发现都对照当前代码/迁移/提交核实，以代码为准。汇总：已修复 23 · 部分修复 11 · 未修复 12 · 作废 2（部分未修复项在复核时按组列出，故表格行数可能少于汇总数）。

| 位置 | 发现 | 状态 | 证据 / 说明 |
|---|---|---|---|
| L13 | rev1 no published policy (scheduler fail-closed) | 未修复 | ops has not published a policy；operator did manual raw retention |
| L14 | rev2 endpoint two raw scans | 已修复 | v13 / `00be2cdb5` \| |
| L15 | rev3/4/5 keep raw FINAL, no raw TTL, no insert-trigger MV | 已修复 | constraints upheld: 020–022 add no raw TTL; RMT + generation kept；3 items |
| L18 | rev6 minimum_generation also gates reads | 已修复 | `flow_archive.go:112,161` \| |
| L19 | rev7/8 hour batching / no raw fallback for the hour tier | 已修复 | `00be2cdb5` RollupRequest.BucketEnd, MarkerOnly (`rollup.go:38-46,304`)；2 items |
| L21 | rev9 two-phase marker | 已修复 | `bc10958f4`；deployment unconfirmed |
| L22 | rev10 coverage-aware tier selection | 已修复 | `bc10958f4` `flow_reports.go:486-488` \| |
| L38 | TL;DR1 downsampling/archive never ran | 部分修复 | hot rollup (v13)；cold still fail-closed |
| L39 | TL;DR2 async retries amplify limit failures | 已修复 | `00be2cdb5` `flow_report_jobs.go:117-122` TerminalError \| |
| L40 | TL;DR3 Kafka retention recurrence | 部分修复 | `c5a9b745b` `compose.flow-dev.yml:48`；prod provisioning not codified |
| L41 | TL;DR4 root disk pressure | 作废（被后续决策取代） | snapshot state; disk-full handled operationally \| |
| L42 | TL;DR5 hybrid tiers, no rebuild | 已修复 | `00be2cdb5` \| |
| L91 | §3 P1 tiers | 部分修复 | 1m/1h/1d + 5m (`c1385cd3b`)；cold lifecycle inactive; 5m not routed |
| L92 | §3 P2 FINAL dependence | 已修复 | marker reads drop FINAL (`rollup.go:345`) \| |
| L93 | §3 P3 skip indexes | 部分修复 | 020 + 022 (`423d820e2`)；old parts not materialized |
| L94 | §3 P4 tiered TTL | 部分修复 | 1m 2d (020), 5m 90d (021:67)；metadata tables unbounded |
| L95 | §3 P5 route to coarsest tier | 已修复 | `flow_archive.go:103-116`；5m not routed yet |
| L97 | §3 P7 Kafka as short buffer | 部分修复 | dev compose only \| |
| L98 | §3 P8 CH dual pools | 已修复 | `0ef47e87` \| |
| L106 | §4.1 cold control plane inactive | 未修复 | policy not published \| |
| L107 | §4.2/4.3/4.4 tiers / marker FINAL / endpoint raw scans | 已修复 | `00be2cdb5`, v13；3 items; 1d prod acceptance pending |
| L110 | §4.5 provenance columns in GROUP BY | 未修复 | `query.go/joint.go` by design \| |
| L111 | §4.6 Kafka retention declared | 部分修复 | `c5a9b745b` \| |
| L112 | §4.7 metadata tables unbounded | 未修复 | no TTL or job \| |
| L156 | §5.5 limit errors terminal | 已修复 | `00be2cdb5` \| |
| L164 | §6 retention.ms + bytes / declarative | 部分修复 | `compose.flow-dev.yml:48`；2 items; prod not codified |
| L166 | §6 skew handled by time retention | 作废（被后续决策取代） | design trade-off, no action \| |
| L167 | §6 three different consumer-group defaults | 未修复 | `config.go:101` / `main.go:108` / `watchdog.yaml:30` \| |
| L177 | §7 raw / 1h lifecycle | 未修复 | policy not published；2 items |
| L179 | §7 1m 2d TTL / new 1d tier | 已修复 | 020 migration；2 items |
| L181 | §7 receipts / reclassified / quarantined | 未修复 | unchanged；3 items |
| L184 | §7 Kafka topic | 部分修复 | dev compose \| |
| L190 | Phase 0 retention / disk alerts / no raw TTL | 部分修复 | `c5a9b745b`; no raw TTL upheld；disk handled by ops; no alerting in repo |
| L191 | Phase 1/2/2.1 | 已修复 | `00be2cdb5`, `bc10958f4`；3 items; 2.1 deployment unconfirmed |
| L194 | Phase 3/4 cold activation / old-part index materialization | 未修复 | not done；2 items |

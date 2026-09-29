# Watchdog Flow 数据生命周期复核与 5m 原子层变更计划

> **与代码的差异（2026-09-28 复核）**：本文有 7 处已落后于代码或与代码不一致——以代码为准，逐条见文末「与代码的差异（2026-09-28 复核）」。

> 日期：2026-09-24  
> 基线：`docs/watchdog-5m-atomic-tier-design-2026-09-23.md` 与当前工作树  
> 范围：Flow 从接收、Kafka、解码/分类、ClickHouse 原始事实、聚合、查询、账单、迟到/重放、对账、保留/删除、备份恢复到发布运维的完整生命周期  
> 本文性质：事实审计、变更设计和可勾选任务清单；不包含本轮数据面编码
> 复核:**§11(2026-09-24 追加)已对本文做范围裁定与设计修正**;§4–§10 中与 §11 冲突的条目以 §11 为准,原文保留不改。

## 1. 结论

当前系统已经具备一条可靠的 **Kafka 至原始事实层** 主链，也已有 generation/marker、operation job、分区状态机、删除门禁和外部恢复演练等关键积木；但 `5m` 原子层仍然只存在于设计中，不能按“部分完成”或“已经上线”计算。

核心事实如下：

1. ClickHouse 迁移目前只有 `001`–`020`，不存在设计中的 `021_flow_atomic_5m.sql`；`flow_interface_traffic_5m` 和 `flow_aggregate_5m` 均未创建。
2. 聚合执行器只支持 `1m / 1h / 1d`；查询规划器和物理表路由同样只认识这三层。
3. 账单虽然按 5 分钟网格返回结果，但仍直接扫描 `flow_records FINAL`；它不是“5m 原子证据层”。
4. 冷生命周期仍按 `raw → 1h → 1d` 构建，并以 raw 与 1h 的精确守恒作为原始分区删除权威。
5. sFlow counter sample 已进入 `sflow_interface_counters`，此前“counter 被丢弃”的旧结论已失效；但主对账扫描尚未证明 counter receipt 对应的 counter 行实际存在。
6. raw 表没有 TTL 是有意的 fail-closed 设计；但默认配置中 hot rollup 和 reconciliation 均未启用，且没有发布策略时生命周期不会推进，因此“安全不删”仍可能演变为磁盘无限增长。
7. 当前代码实现了许多局部机制，但缺少一个能回答“今天哪些日分区可查、可计费、可修复、可删除、可恢复”的统一运行状态闭环。

因此，本轮建议不是重写现有数据面，而是在现有可靠主链上增加两张职责分离的 5m 表，并把它们接入既有 generation、operation job、生命周期和查询框架：

- `flow_interface_traffic_5m`：端口/方向/三层取值的账单与接口证据层，从 raw 生成。
- `flow_aggregate_5m`：不含高基数端点的中期交互查询层；热路径从完整 1m 生成，冷补建可从 raw 生成。

### 1.1 两个核心驱动力

此前版本把 5m 主要写成“生命周期正确性补丁”，这是主次倒置。这个变更首先由以下两个工程事实驱动：

1. **采用成熟的多分辨率保留模型。** Akvorado 默认使用 raw 15 天、1m 7 天、5m 90 天、1h 360 天的倒金字塔保留：高基数事实保留较短，低基数汇总保留更久。Watchdog 不应机械复制每个天数，但必须采用同一原则、可配置默认值、约 50 个分区的容量约束和整 part 删除方式。
2. **建立真正的 5m 原子层。** 5m 同时是 95th/平均/总量账单可复算的最小长期证据单位，也是 1m 与 1h 之间的交互查询层。它不是把 1h 图表“画细一点”，而是让中期查询和账单彻底停止反复扫描 raw。

### 1.2 两个首要目标

| 目标 | 设计手段 | 成功判据 |
|---|---|---|
| **存储容量压缩** | raw 只承担高基数端点与重建；5m/1h 排除 IP、端口等高基数列；使用压缩 codec、整 part TTL/受控 DROP PARTITION 和分层保留 | 不以“表已创建”为完成；必须证明 raw 日增、各层稳态容量、part 数和删除速率满足固定磁盘预算 |
| **高效查询** | 查询前对齐桶边界；按维度和步长选择满足语义的最粗完整层；5m 表以 `bucket_start` 为排序前缀；raw 的时间聚簇由小时细化到 5 分钟列入独立 P3 重建 | 24h/7d/30d 非端点查询不得扫描 raw；账单不得按账期扫描 raw；以 `query_log` 的 read rows/bytes、P95/P99 和资源超限率验收 |

因此，5m 项目的完成定义必须同时包含**容量收益**和**查询收益**，不能只验 generation、marker、repair 和删除门禁。

### 1.3 Akvorado 源码证据及可迁移原则

以下证据来自仓库外的本地参考源码 `/Users/chenliliang/Downloads/akvorado-main`，用于解释设计来源，不代表 Watchdog 已经实现：

| 事实 | Akvorado 位置 | 对 Watchdog 的约束 |
|---|---|---|
| 默认 raw 15d、1m 7d、5m 90d、1h `12×30d` | `orchestrator/clickhouse/config.go:48-73` | 给出产品默认层级；代码字面 1h 是 360 天，虽注释写“一年”，不得误写为精确 365 天 |
| `Interval=0` 表示 raw；`TTL=0` 表示永不过期 | `orchestrator/clickhouse/config.go:48-56` | retention 必须是显式配置语义，不用魔法常量 |
| 文档解释 raw 比 1m 留得更久，因为聚合表默认不含源/目的 IP 和端口 | `console/data/docs/50-configuration.md:1215-1242` | 高基数明细和低基数查询层职责分离；不能期待 5m 回答精确端点问题 |
| `MaxPartitions=50`，分区宽度由 `TTL / MaxPartitions` 派生 | `orchestrator/clickhouse/config.go:72`、`migrations_helpers.go:405-435` | 分区数是容量与元数据预算的一部分，不能只固定“按日/按月”而不测算 |
| 默认 `ttl_only_drop_parts=1` | `migrations_helpers.go:24-28` | 查询层过期应整 part 删除，避免 TTL 行级 mutation |
| raw 主键/排序键以 `toStartOfFiveMinutes(TimeReceived)` 开头 | `migrations_helpers.go:436-442` | raw 子小时裁剪应以 5m 时间聚簇为 P3 目标，而不只依赖 skip index |
| 每个非 raw 分辨率独立建表，并去掉 main-only 高基数列 | `migrations_helpers.go:405-445,725-742` | `flow_aggregate_5m` 必须排除端点列，容量收益来自 schema，而不只是 TTL |
| 每个非 raw 分辨率配一个 insert-trigger consumer MV | `migrations_helpers.go:725-742` | 证明 Akvorado 的自动 rollup 机制；Watchdog 因至少一次重放和修正语义不直接复制，改用 generation-marked job |
| TTL 可在线 `MODIFY TTL` | `migrations_helpers.go:594-605` | retention 默认值可运维调整，不应编译进 Go const |
| console 从 `flows_<duration>` 反解分辨率 | `console/clickhouse.go:51-58` | Watchdog planner 同样必须显式返回/展示实际 source tier |

可迁移的是**多层、可配置 TTL、受控分区数、整 part 删除、时间取整排序和按分辨率路由**；不可直接复制的是 Akvorado 的 insert-trigger MV，因为 Watchdog raw 使用 `ReplacingMergeTree`、至少一次重放、generation 修正和账单证据语义。

### 1.4 ClickHouse 原生机制裁定

| 机制 | 采用 | 裁定理由 |
|---|---|---|
| 取整排序键/分区键 | **采用** | 5m 表以 `bucket_start` 开头；raw 的 `toStartOfHour`→`toStartOfFiveMinutes` 作为 P3 重建，利用稀疏索引改善时间裁剪 |
| raw→5m insert-trigger MV | **不采用** | MV 在 `ReplacingMergeTree` 去重前消费插入块，重放和修正会重复累加；块级 exactly-once 未落地前不满足账单准确性 |
| `TTL … GROUP BY` 同表降采样 | **计费层不采用** | 难以表达双 generation、迟到 repair、独立 marker 和账期证据冻结；图表缓存层仅可另行评估 |
| Projection | **不采用** | 当前正确性查询依赖 `FINAL`，投影不能解决该路径且会增加写放大 |
| `VersionedCollapsingMergeTree` | **暂缓** | 可作为未来修正模型，但本轮不为引入 5m 而重写 raw identity 与查询 |
| 查询侧 `toStartOfInterval` | **采用** | Go 负责选择 tier、coverage 和 generation，ClickHouse 负责按请求时区/步长取整 |

结论是：**取整由 ClickHouse 函数完成，生命周期与 generation 编排由 Go 完成。** 这既获得引擎的时间裁剪和聚合能力，又保留 Watchdog 对重放、迟到、账单和删除的可证明性。

## 2. 审计口径

### 2.1 状态定义

| 状态 | 含义 |
|---|---|
| 已实现 | 代码、DDL 和测试入口均存在；不代表生产已启用 |
| 半闭环 | 核心机制存在，但缺少接线、运行门禁、覆盖证明或端到端验收 |
| 仅设计 | 文档存在，当前 DDL/代码/API 尚不存在 |
| WIP | 只在未提交工作树中存在，不计入可交付基线 |

### 2.2 边界与假设

- 本文以当前仓库事实为准；不把历史截图、旧任务勾选或某次服务器运行状态当作当前实现证据。
- 未直接检查生产服务器的 systemd、Kafka retention、ClickHouse system tables 和 MySQL 发布策略，因此运行启用状态必须由后续部署验收确认。
- 现有未提交的 `internal/flowch/interface_reconciliation.go` 等文件按 WIP 记录，不据此宣告功能完成。
- 5m 原子层是新增存储形态，不改变 fast decode、Kafka 坐标身份、Flow 分类快照和原始写入协议。

## 3. 当前生命周期全景

```text
sFlow/NetFlow
    │
    ▼
flow-collect ──RawFlow──► Kafka
                           │ partition order / at-least-once
                           ▼
                     flow-worker
                 decode + enrich + classify
                           │
             ┌─────────────┼──────────────┐
             ▼             ▼              ▼
       flow_records   sflow counters   ingest receipts
          raw fact       cumulative        offsets/counts
             │
       ┌─────┴───────────┐
       ▼                 ▼
    hot 1m            cold 1h ──► 1d
       │                 │
       └──── queries ────┘

设计目标新增：
  complete 1m ──► aggregate 5m ──► 中期多维查询
  raw ──────────► interface 5m ──► 账单/接口证据/Flow-SNMP 对账
```

## 4. 分阶段实现复核

### 4.1 接收与 Kafka 缓冲：已实现，保留

现状：

- `internal/flowstream/consumer.go:225-237` 保证同一 partition 内批次有序；handler 失败时整批不标记为已处理。
- `internal/flowstream/consumer.go:288-305` 只在 durable handler 成功后提交 Kafka offset；提交失败会重放。
- Kafka 坐标仍是记录天然身份，符合至少一次消费下的确定性重试语义。

复核结论：

- 这部分不应因 5m 设计重写。
- 需要补的是运行期证据：Kafka retention 必须覆盖最大故障、迟到窗口和恢复时间，而不能只写在配置示例中。
- `ErrDataLoss` 当前被视为可恢复告警并继续消费；必须把 bootstrap/reset 后形成的不可恢复缺口显式进入水位与生命周期 blocker，不能仅留日志。

### 4.2 Worker 解码、估算与分类：已实现，5m 不介入

现状：

- Worker 对 Kafka partition batch 解码、分类并一次交给持久化 handler。
- flow sample 和 sFlow interface counter 已是两种独立记录；5m 设计不应在采集器中做聚合。
- 分类版本、地址快照、sampling 和 quality/provenance 已进入 raw 字段，是后续账单可复算的基础。

复核结论：

- 继续坚持“采集/Worker 只生产事实，ClickHouse 异步聚合”的边界。
- 5m 不能加入 worker 热路径，否则发布修复、迟到数据和历史重放会产生两套状态机。
- 客户地址或分类版本变化只影响新事实；历史重分类必须通过独立 operation job 生成新 generation，不能原地改 raw。

### 4.3 原始事实、counter 与 receipt 写入：基本可靠，但不是事务

现状：

- `internal/flowch/native.go:232-269` 依次同步写 `flow_records`、`sflow_interface_counters`、`flow_ingest_receipts`。
- `async_insert=0`、`wait_for_async_insert=1`，每个 INSERT 使用确定性 dedup token。
- receipt 最后写，Kafka offset 只在整个 handler 成功后提交。

风险：

- 三次 INSERT 不属于一个 ClickHouse 事务。进程可能在 facts 成功、counter 或 receipt 失败后退出；重放依赖 dedup/ReplacingMergeTree 收敛。
- 这不要求引入分布式事务，但要求 reconciliation 同时覆盖 flow facts 与 counter rows。
- receipt 的 `counter_record_count` 已存在，而当前主扫描允许“counter-only、零 fact 行”通过，却未验证对应 counter 行数；这是准确性缺口。

目标：

- 保留 receipt-last 和 at-least-once。
- 对账分别证明：receipt→flow facts、receipt→sFlow counter rows；两个结果共同推进 reconciled watermark。
- partial durability 必须可观测，不能因最终重放成功而不记录发生次数和恢复延迟。

### 4.4 Raw ClickHouse 物理模型：可用，但中期查询和账单代价过高

现状：

- `011_flow_storage_v2.sql` 中 raw 按日分区，`ReplacingMergeTree`，排序键以小时、Kafka 流和坐标为主。
- raw 无 TTL，由 MySQL 生命周期状态机控制删除。
- `020_flow_hot_rollup_and_skip_indexes.sql` 给常用设备、端点、方向和分类字段增加 skip index；没有 `event_time` minmax index。

风险：

- 端口账单执行 `flow_records FINAL` 并按 5m 分组，长账期会重复扫描大量 raw。
- 小于一小时的查询仍可能读取同一小时内较多 granule；生产基线中 5 分钟窗口曾读取接近整小时数据。增加 `event_time` minmax 只能作为短期验证项，长期 P3 是把 raw 排序前缀从 `toStartOfHour(event_time)` 改为 `toStartOfFiveMinutes(event_time)`；两者都必须由 `EXPLAIN indexes=1` 和 `system.query_log` 验收。
- `FINAL` 是正确性保险但成本高；在 Kafka 坐标和 stable generation 已收敛后，应实测能否用按坐标取最新 generation 的受控子查询替代，而不是全局取消去重。

物理优化顺序：

1. P1：补适合列类型与分布的 codec，先把 live 基线约 84 B/row 降到约 65 B/row。
2. P2：审查 `remote_ip/local_ip` 等确定性派生列的所有读者后再决定是否改 `ALIAS`，目标约 50 B/row；不能为省空间破坏 VPN/端点查询。
3. P3：以影子表重建 raw 的 5m 时间聚簇排序键；这是物理迁移，不与 5m 逻辑表 DDL 捆绑发布。

### 4.5 热聚合：已有 1m/1h；默认未启用，缺 5m

现状：

- `internal/flowch/rollup.go:20-30` 只定义 `1m / 1h / 1d`。
- 数据先写、marker 后写，generation 高低位区分生命周期和热缓存命名空间。
- 1m 表按日分区并有 2 天 TTL；1h/1d 由生命周期控制。
- 默认配置 `FlowHotRollup.Enabled=false`；源码默认值不等于生产关闭，但说明部署必须显式启用。

缺口：

- 没有 `5m` resolution、runner、marker、stats 或 scheduler。
- 没有证明每五个 1m marker 完整后再生成 aggregate 5m 的依赖规则。
- hot scheduler、reconciliation scheduler 和生命周期 policy 的启用状态分散，运维无法从一个接口判断实际生效状态。

### 4.6 5m 原子层：仅设计，且需修正三点

原设计方向正确：接口证据和多维查询必须分表，不能用一张 EAV 表承担账单；但实施前需要修正以下内容。

#### 修正 A：90 天是可配置默认值，图表层与账单证据层采用不同删除门禁

- `flow_aggregate_5m` 采用 Akvorado 式**默认 90 天**保留，并允许通过 published lifecycle policy 在线调整；使用 `ttl_only_drop_parts=1` 整 part 删除。它承担中期查询，必须真正通过 TTL/分区清理控制稳态容量，不能因强调安全而无限保留。
- `flow_interface_traffic_5m` 同样以 90 天作为 policy 默认值，但它是账单证据。首版不配置无条件 ClickHouse TTL，而由生命周期 job 在已关闭账单结果及 provenance 已冻结、未关闭账期/争议期/审计期/legal hold 均满足后整 part 删除。将来只有在冻结证据已独立保存并证明可复算后，才允许改成 CH TTL。
- raw 不复制 Akvorado 的无条件 15 天 TTL。Watchdog raw 仍由 watermark、守恒、正式下游覆盖、备份和批准状态机 fail-closed 删除；15 天只是容量规划参考上限。
- 1h 的参考默认值按 Akvorado 代码字面记为 360 天，并保持可配置；文档可以显示“约一年”，但测试必须按实际 duration。
- 分区宽度要以 retention 与目标 part 数共同决定。优先评估 `TTL / MaxPartitions≈50`；若保留 Watchdog 按日/月分区，必须用 part 数、drop 时延和 merge 压力证明它更合适。
- 结论不是“禁止 TTL”，而是：**查询层用 TTL 获得容量闭环，证据层在业务 hold 满足后才能物理删除，raw 继续走可证明状态机。**

#### 修正 B：两个 5m 层必须拥有独立覆盖权威

- aggregate 5m 的 generation marker 只证明非端点 EAV 聚合完成。
- interface 5m 的 marker 只证明端口证据完成。
- 两者不能共享一个“5m 已完成”标记；查询和账单分别检查自己的连续覆盖。

#### 修正 C：5m 不取代 raw→1h 的删除守恒权威

- 一条 raw flow 可能同时贡献 ingress 和 egress 两条接口记录，interface 5m 的 record_count 与 raw 不相等。
- raw 与 1h 的精确计数器守恒暂时继续作为删除权威。
- 当 5m 上线后，**完整 marker 覆盖**应成为 raw 删除的可用性前置条件；但 Flow/SNMP 比值偏差和 interface record_count 不作为删除守恒条件。

### 4.7 冷聚合与迟到修复：状态机成熟，但当前只构建 1h/1d

现状：

- `internal/flowlifecycle/archive.go:138-185` 对一个 UTC 日逐小时构建 1h，再从 24 个小时构建 1d。
- operation job 每小时 checkpoint，crash/retry 能从 `next_hour` 继续。
- raw 与 1h 的 count/raw/estimated bytes/packets 完全一致后才能 reconciled。
- policy、partition state、generation、late window 和 repair attempt 已存在。

目标：

- 冷任务增加 5m 两层的构建与 checkpoint，但不另造 job 状态机。
- aggregate 5m 热路径优先从完整 1m 生成；若 1m 已因 TTL 消失，冷任务允许从 raw 直接生成。
- interface 5m 始终从 raw 生成，以保留 ifIndex/direction/三层和 provenance。
- repair generation 先写新数据再写 marker，旧 generation 不立即做 mutation 清理。

### 4.8 查询路由：已支持 raw/1m/1h/1d，缺 5m 且各端点行为不统一

现状：

- `internal/flowquery/plan.go:57-119` 只选择 `1m / 1h / 1d`；自动 24h 查询会切到 1h。
- 连续 marker coverage 能约束 archive 使用边界；尾部回落 raw。
- remote port 等精确高基数查询保持 raw。
- report handler 存在从 1m 回退 1h 的局部逻辑，但并非所有 Explorer、六报表和导出入口共用同一规划器。

目标路由：

| 查询类型 | `<5m` | `[5m,1h)` | `>=1h` | 超出可用细粒度范围 |
|---|---|---|---|---|
| 非端点维度 | raw/1m | aggregate 5m | 1h/1d | 使用完整覆盖的粗层 |
| src/dst IP、remote port | raw/1m | raw/1m | 不自动使用 EAV 5m | 明确 unavailable 或提交受限异步任务 |
| 端口账单/对账 | raw（迁移期） | interface 5m | interface 5m | 缺 marker 即 incomplete |

约束：

- 首版只选择一套覆盖完整的物理层，不做多层拼接，避免边界重复/漏算。
- 有缺口时可在预算内完整回退 raw；不能把缺口渲染为 0。
- API 响应必须返回 `source_tier / coverage_from / coverage_to / generation / incomplete_reason`，前端不猜数据来源。

### 4.9 Billing：业务语义较完整，物理证据层未闭环

现状：

- `internal/flowch/billing.go:73-96` 强制 5m 对齐、闭合时间和 bucket budget。
- `internal/flowch/billing.go:247-288` 直接扫描 `flow_records FINAL`，生成 raw/supplier/customer 三层值与版本引用。
- 查询允许最长 400 天并使用固定 120 秒、8 threads、1 GiB 上限；这些是防失控保护，不应被误认为容量设计。

问题：

- 400 天 raw 扫描与 raw 生命周期目标冲突；raw 删除后旧账期不可复算。
- hard-coded 上限只适合安全阀，业务允许的账期、争议期和并发预算应由 policy/runtime config 决定。
- 账期没有冻结 `evidence_source`，迁移期间存在同一账期混用 raw 和 5m 的风险。

目标：

- 新账期冻结 `evidence_source=interface_5m_v1`、覆盖 generation、分类/地址快照引用和 source bucket 范围。
- 旧打开账期保持 `raw_v1` 或执行显式重算迁移；关闭账期不可隐式改来源。
- 账单只消费 complete interface 5m bucket；缺 bucket、unknown sampling 或版本缺失都标 incomplete，不自动补 0。
- 长账期计算与 CSV/Parquet 导出必须走 operation job，在线 API 只提交、查状态和下载结果。

### 4.10 Reconciliation：Flow facts 已有，counter 与 5m 仍缺

现状：

- 主扫描按 Kafka offset 窗口比较 receipt 与 flow facts 的 identity/count/raw/estimated counters，不再依赖逐记录 hash。
- counter-only Kafka message 可以合法没有 flow fact；receipt 已记录 `counter_record_count`。

缺口：

- 尚未把 receipt 的 counter count 与 `sflow_interface_counters` 对账。
- 尚无 raw→interface 5m、1m/raw→aggregate 5m 的覆盖与数值审计。
- 未提交的 interface reconciliation WIP 不计入完成状态。

目标分层：

1. ingest reconciliation：证明 Kafka receipt 对应的 flow facts 与 counter rows 都落盘。
2. rollup reconciliation：证明每个目标 bucket 的 marker、generation 和源计数器匹配。
3. interface audit：比较 Flow 估算、sFlow counter、SNMP counter，输出 coverage/ratio/issues；它用于准确性告警，不自动把账调平，也不作为 raw 删除守恒权威。

### 4.11 Raw 删除：门禁正确，5m 上线后需扩展可用性条件

现状：

- `internal/flowlifecycle/delete_readiness.go:65-177` fail-closed 检查发布策略、分区状态、generation、精确 counters、迟到窗口、Kafka offset/watermark、备份和人工批准。
- 删除通过显式 operation job 执行 `DROP PARTITION`，有 deletion receipt；不是后台 TTL。

变更：

- 在 5m 成为查询/账单正式来源后，raw 删除 readiness 额外要求：该日 aggregate 5m 与 interface 5m marker 连续且已通过各自 build audit。
- interface Flow/SNMP 比值异常不阻塞删除，但缺失 marker、构建失败、版本引用缺失必须阻塞。
- policy 首次发布必须 `raw_delete_enabled=false`；完成 backfill、shadow compare、恢复演练后再单独批准开启。

### 4.12 Receipt、counter 与旧 generation 生命周期：当前没有完整清理闭环

问题：

- receipt 按 `inserted_at` 月分区，而 raw 按 `event_time` 日分区，无法按某一 raw 日直接等价删除 receipt。
- `sflow_interface_counters` 为月分区且无 TTL，会持续增长。
- ReplacingMergeTree 的旧 generation 在逻辑上失效，但物理行不会立刻消失。

设计：

- 新增按 `stream/partition/event_day` 的不可变 receipt summary，保存 offset span、message/fact/counter counts 和总量。
- 仅当一个 receipt 月涉及的 raw 日都已删除、summary 校验完成、水位健康且审计窗口已过，才由 operation job 删除该月 detailed receipts。
- sFlow counter 月仅在对应 interface 5m 全覆盖、对账完成、保留期满足且无 hold 时删除。
- 旧 generation 按 dead/live ratio 和磁盘预算触发分区级 rebuild/compaction；禁止每桶 `ALTER DELETE`。
- 旧表只在无路由引用、无数据或备份验证、回滚窗口结束后删除。

### 4.13 Backup 与 restore：实现较强，证据授权仍需收紧

现状：

- restore drill 能在隔离数据库恢复外部备份、校验 schema、比较 raw/archive/物理行/Kafka coverage 和 manifest SHA，并清理临时库。
- raw 删除门禁可以要求 backup evidence。

风险：

- backup evidence 的写入与“某次成功 restore operation job”之间缺少不可伪造的强绑定。
- 恢复演练主要是手工/CLI 能力，尚未成为定期生命周期任务和 readiness 输入。

目标：

- 只有 restore drill 成功结束的 job handler 可以生成可用于删除门禁的 evidence。
- evidence 固化 backup URI、manifest digest、恢复目标、schema digest、覆盖范围、核验 counters、job ID 和时间。
- 定期恢复演练失败或 evidence 过期时，自动阻塞新的 raw/archive 删除批准。

### 4.14 运维可见性：机制分散，缺统一闭环

需要一个只读 lifecycle readiness API/页面，至少展示：

- schema/migration 是否包含 5m 两表；
- hot scheduler、reconciliation、lifecycle scheduler 的实际启用状态和最近成功时间；
- 当前 published policy 及生效版本；
- raw/1m/5m/1h/1d 的时间覆盖、marker 缺口和 generation；
- Kafka committed/reconciled watermark、lag、data-loss reset；
- receipt/fact/counter mismatch；
- raw、receipt、counter、5m、archive 磁盘占用和增长率；
- backup/restore evidence 年龄；
- 每个分区的 deletion blockers；
- open billing period 和 legal hold 对删除的影响。

## 5. 容量压缩与查询效率目标

### 5.1 已有生产基线

下表来自 2026-09-23 审计快照，用于设定量级和回归方向；正式切流前必须在相同硬件、相同时间窗和相同数据集上重新采集，不得把估算值当最终验收结果。

| 项目 | 已测基线 | 暴露的问题 |
|---|---|---|
| raw 日增 | 约 251.7M 行、19.65 GiB/天，约 84 B/row | 靠 raw 承担中长期查询不可持续 |
| raw codec 目标 | 约 65 B/row、15.5 GiB/天 | 需真实 shadow part 验证压缩率与 CPU |
| raw 派生列 ALIAS 目标 | 约 50 B/row、12 GiB/天 | 必须先审计全部 reader，不能只为省容量删除物理列 |
| interface 5m | 1 小时 raw→5m 约 1.5s、8.08M 输入行、540 输出行；90 天约 1.2M 行、个位数 MB | 账单继续扫 raw 没有容量或性能理由 |
| aggregate 5m | 1 小时 1m→5m 约 0.17s、1.38M 输入行、24,236 输出行；90 天约 52M 行、0.6 GB | 必须排除 `src_ip/dst_ip/remote_port`；这三类曾占最新 1h 行数约 87.5% |
| 聚合层稳态估算 | 5m/1h/1d 合计约 1.5 GB | 多分辨率层不是主要磁盘压力，raw 才是 |
| raw→1m | 24 次平均 144s、最大 215s、峰值 3.73 GiB | 再从 raw 重复构建所有查询层会放大 I/O |
| 在线聚合查询 | 纯 1m 平均 27ms；纯 1h 平均 17ms；hybrid 平均 0.97s、P90 1.56s、最大 11.2s/25.8M 行 | coverage gap 或尾段 raw 会让性能退化两个数量级 |
| 账单 | 单端口 6h raw 扫描 7.1s、88.2M 行、3.12 GiB；30 天外推约 14 分钟、>350 GiB | 当前 120s 查询上限下，真实月账期天然失败 |

### 5.2 目标容量模型

目标不是把所有数据都“永久保留”，而是在语义允许的层级中用最低基数保存最长时间：

| 层 | 产品默认 | 容量策略 | 说明 |
|---|---:|---|---|
| raw | 由磁盘预算和删除门禁计算；Akvorado 15d 仅作参考 | codec/ALIAS/5m 时间聚簇；安全后整日 DROP PARTITION | 保留 IP/端口和重建能力，容量最大 |
| 1m | 当前实现 2d；是否调整到参考 7d 需容量测试 | 短期 TTL、整 part 删除 | 最近实时查询缓存，不是账单证据 |
| aggregate 5m | 90d，可配置 | 排除高基数端点；约 50 个目标分区；整 part TTL | 3–90 天中期交互查询主层 |
| interface 5m | policy 默认 90d，受账期/争议/hold 约束 | 极窄 schema；首版不用无条件 CH TTL，由 lifecycle job 在冻结证据后整 part 删除 | 95th/平均/总量与接口对账 |
| 1h | 360d，可配置 | 低基数长期聚合、整 part TTL/受控删除 | 约一年趋势查询 |
| 1d | policy 决定，可为长期 | 月分区、受控删除 | 长期容量最低 |

必须为每层输出：`rows`、`bytes_on_disk`、压缩 B/row、active/inactive parts、日增长、最老/最新 bucket、预计耗尽天数。任何 retention 修改先做容量预览，再发布 policy；不能靠 Go const 或 DDL 常量静默改变。

### 5.3 查询路由与性能门禁

查询选择遵循“**满足语义且具有连续 coverage 的最粗层**”，而不是“能返回结果就回 raw”：

- `<5m` 或端点明细：raw/1m，且必须有严格时间、设备和行预算。
- `[5m,1h)` 非端点：优先 aggregate 5m；3–90 天范围不得因 1m 过期而整段回 raw。
- `>=1h` 非端点：1h/1d。
- 端口账单/对账：interface 5m；缺 marker 必须返回 incomplete，不能隐式扫 30 天 raw。
- 请求时间先由 Go 对齐层边界，SQL 用 ClickHouse `toStartOfInterval`，响应返回实际 `source_tier`、coverage 与 generation。

量化门禁：

1. 24h/7d/30d 的非端点查询不得从 `flow_records` 读取；`query_log` 必须证明 read rows/bytes 与所选聚合层物理规模同阶。
2. 30 天单端口账单只读 interface 5m，读取行数应与“端口×方向×层×5m 桶”同阶；不得再出现数百 GiB raw 读取。
3. 默认产品请求不得出现 `query exceeded a ClickHouse resource limit`；超预算端点查询应在执行前转异步或拒绝，而不是先扫描再失败。
4. raw 5 分钟时间窗在 P3 后不得读取接近整小时；以同一查询的 `EXPLAIN indexes=1` 和 `read_rows/read_bytes` 前后对比验收。
5. 性能报告同时记录结果正确性、CPU、峰值内存、读放大、并发和 merge backlog；只报接口耗时不算通过。

## 6. 目标架构与不变量

### 6.1 存储职责

| 层 | 来源 | 用途 | 删除权威 |
|---|---|---|---|
| raw `flow_records` | Worker | 精确端点、追溯、重分类、重建 | lifecycle state + raw↔1h 守恒 + 5m coverage + watermark/backup/approval |
| sFlow counters | Worker | 接口真值/覆盖校准 | interface 5m coverage + counter reconciliation + policy |
| 1m | raw | 最近实时多维查询 | 当前 2d 短期 cache TTL；目标默认需容量复核；不可作为账单证据 |
| aggregate 5m | complete 1m；冷补建可用 raw | 5m–1h 的非端点查询 | 默认 90d、可配置、`ttl_only_drop_parts=1`；policy 可覆盖 |
| interface 5m | raw | 账单、端口流量、Flow/SNMP 对账 | policy 默认 90d；无静态 CH TTL，billing/audit/legal hold 满足后由 job 整 part 删除 |
| 1h | raw（首阶段保持） | 中长期查询、raw 删除守恒 | 默认 360d、可配置；lifecycle policy |
| 1d | complete 1h | 长期趋势 | archive month lifecycle |
| receipt detail | ingest | 精确 Kafka 入库审计 | summary + raw deleted + watermark + audit window |
| receipt summary | detail | 长期可证明性 | 更长期 policy/备份 |

### 6.2 必须冻结的不变量

1. Kafka offset 只能在 facts、counter 和 receipt 全部 durable 后提交。
2. 重放同一 Kafka 坐标必须收敛，不生成业务重复。
3. 所有聚合均“数据先写、marker 后写”；查询只读最新完整 generation。
4. aggregate 5m 和 interface 5m 分别拥有 marker，不共享完成状态。
5. 端点查询永不伪装成 5m EAV 精确查询。
6. 缺覆盖显示 incomplete/unavailable，不显示为 0。
7. 同一账期只允许一个 evidence source；关闭账期不可被后台切换。
8. raw 删除仍以 raw↔1h 精确守恒为数值权威，同时要求正式下游层存在完整覆盖。
9. Flow/SNMP 偏差只产生 issue，不自动修改 Flow 数据或账单。
10. retention 的**产品默认值**必须明确且可配置，运行值来自发布的 lifecycle policy；不能散落在 Go const、不可修改 DDL TTL 或前端默认值中。
11. 线程、内存、并发和超时属于部署 runtime budget；保留期、hold 和删除条件属于数据 policy，二者不能混为一套配置。
12. 没有完整恢复证据时宁可不删；磁盘告警不能绕过删除门禁。

## 7. 分阶段发布策略

### Phase 0：冻结契约，不改变读写路径

- 建立 5m DDL、marker、policy、产品默认 retention 和 API 契约。
- 记录当前 raw/1m/1h/1d 的容量、part、压缩率、查询/账单 read rows/bytes 与延迟基线。
- 启用只读 readiness，不开启 raw 删除。

### Phase 1：影子生成

- 创建两张 5m 表。
- hot/cold job 影子生成 5m，但所有在线查询和账单仍走旧路径。
- 按 bucket 比较 raw、1m、aggregate 5m、interface 5m；记录差异，不影响用户结果。

### Phase 2：非端点查询切换

- 只对 marker 连续的范围启用 aggregate 5m。
- endpoint 查询保持 raw/1m。
- 可按 feature flag 一键回退旧 planner；不删除任何 5m 数据。

### Phase 3：账单证据迁移

- 新建账期默认 `interface_5m_v1`。
- 既有打开账期保留 raw 或由管理员显式重算；关闭账期不迁移。
- 通过独立可复算向量验证 95th/average/total、缺桶、方向、三层修正和 provenance。

### Phase 4：删除门禁扩展

- raw delete readiness 加入两套 5m coverage 前置条件。
- 发布 raw-delete-disabled policy，完成 backfill 和恢复演练。
- 独立批准后才允许首个 raw 分区删除；删除后重跑查询、账单和恢复验收。

### Phase 5：生命周期收口

- detailed receipts、sFlow counters、旧 generation 和旧表进入受控清理。
- 是否将 1h 改为从 5m 派生，必须作为后续独立 ADR；本次不顺带修改。

## 8. 可勾选变更任务清单

### 已确认可复用的前置能力

- [x] Kafka partition 内有序批处理，durable handler 成功后才提交 offset。
- [x] Kafka 坐标身份、同步 INSERT、稳定 dedup token 与重放收敛机制。
- [x] raw facts、sFlow cumulative counters 和 ingest receipts 已有独立持久化表。
- [x] 1m/1h/1d 的 generation、数据先写 marker 后写和最新完整 generation 查询原语。
- [x] operation job lease/checkpoint/retry/cancel/takeover 可供 5m hot/cold/repair 复用。
- [x] raw 日分区的 policy、迟到窗口、守恒、水位、备份、批准和删除 receipt 门禁。
- [x] 外部备份隔离恢复、schema/manifest/counters 校验的 restore-drill 实现。
- [ ] **运行确认**：以上能力在目标部署中的 migration、配置、scheduler 和最近成功记录仍须由 readiness 验证；源码存在不等于已启用。（2026-09-28 复核：部分完成 — 线上已证 Kafka lag 0、摄入恢复，hot rollup 已启用；reconciliation 仍 enabled:false，policy 0 条（靠人工 DROP），无 readiness；`config/watchdog.yaml:52,67`）

### FL5M-00 设计冻结与基线

- [ ] **设计**：冻结两张 5m 表职责、字段、主键、row kind、generation/marker、provenance、observed/known、空桶和迟到语义。（2026-09-28 复核：部分完成 — 021 已冻结字段、主键、row_kind、provenance、observed/known；未定义 5m 如何跟随 1m 迟到 repair，也未定义 interface 空桶语义；`423d820e2`; `deploy/migration/clickhouse/021_flow_atomic_5m.sql:13-68`）
- [ ] **设计**：冻结 raw/1m/5m/1h/1d 的产品默认 retention、允许范围、在线变更语义和约 50 个目标分区预算；5m 默认 90 天、1h 默认 360 天均为可配置 policy 值。（2026-09-28 复核：部分完成 — §11.2-B/H 已定 1m 2d、5m 90d TTL、1h 360d=archive_retention_seconds；raw 默认值与允许范围未冻结；`deploy/migration/clickhouse/021_flow_atomic_5m.sql:67-68`; `deploy/schema/mysql/0033_flow_storage_lifecycle.sql:3-16`）
- [x] **设计**：冻结图表查询层 TTL 与账单证据删除的边界；补 billing/audit/dispute/legal hold，保证静态 TTL 不越过证据保护。（2026-09-28 复核：作废 — 被 §11.2-A/B 取代：1m/5m 用 DDL TTL，1h/1d/interface 5m 走归档月状态机，hold/dispute 已删除；`317e5664a`）
- [ ] **设计**：冻结查询路由矩阵、端点禁用规则、fallback 和 incomplete 响应契约。（2026-09-28 复核：部分完成 — 路由矩阵、端点定义、raw 尾段 fallback 已冻结（§4.8/§11.2-C）；incomplete 响应字段在 §4.8 与 §11.1 之间未统一；`317e5664a`）
- [x] **设计**：冻结 raw 删除中“1h 数值守恒权威 + 5m 覆盖前置”的组合规则。（2026-09-28 复核：已完成 — `317e5664a`；规则已冻结（§4.6-C/§11.1 FL5M-10）；five_minute_coverage_missing blocker 的实现见 §11.4 L758（未做））
- [ ] **基线**：采集当前 raw/1m/1h/1d 行数、part、压缩率、query_log read rows/bytes、P50/P95/P99、磁盘日增长和账单耗时。（2026-09-28 复核：部分完成 — 09-23 生产快照已有 raw/1m/1h 行数、B/row、日增与耗时；缺各层 part 数与 P95/P99，切流前重采未做）
- [ ] **容量预算**：用最近 7 天实际 ingest 量计算 raw 天数、5m 90 天、1h 360 天和 backfill 临时空间；输出高水位、预留空间与预计耗尽日期。（2026-09-28 复核：部分完成 — 已有 62 B/row≈15.5 GiB/天、3/7/15 天 raw 容量、聚合层 1–2 GB；缺 7 天实测基数、backfill 临时空间、高水位与耗尽日期；`docs/watchdog-release-readiness-2026-09-28.md:45-55`）
- [x] **验收预算**：冻结 24h/7d/30d 查询与 30 天账单的 source tier、最大 read amplification、P95/P99、内存和并发阈值；固定硬件前先记录相对基线，不伪造绝对 SLA。（2026-09-28 复核：作废 — §11.1 FL5M-00 移出“验收预算冻结”，验收口径改由 §11.4 L763 承担）
- [x] **变更测试**：确认不改变 collector、Kafka payload、Worker decode/classification 和 raw identity。（2026-09-28 复核：已完成 — `423d820e2`, `c1385cd3b`, `d6c38b62e`；三个 5m 提交均未触及 flowstream/flowworker/collector/native.go；022 只加 skip index（diff 核对，无专门测试））
- [x] **已提交门禁**：独立 ADR/设计提交经数据面、账单和运维 reviewer 确认后，方可开始 DDL。（2026-09-28 复核：作废 — §11.1 FL5M-00 移出独立 ADR 评审流程；DDL 已在 423d820e2 落地；`423d820e2`）

### FL5M-01 ClickHouse 5m schema

- [x] **DDL**：新增 `021_flow_atomic_5m.sql`，创建 `flow_interface_traffic_5m` 和 `flow_aggregate_5m`；aggregate 表使用可在线修改的 90 天默认 TTL，interface 表首版不配置无条件 CH TTL，其 90 天默认保留由 lifecycle policy/job 执行。（2026-09-28 复核：已完成 — `423d820e2`; `deploy/migration/clickhouse/021_flow_atomic_5m.sql:25-68`；两表已建，aggregate TTL 90 DAY，interface 无 TTL；保留执行改走 DropArchiveMonth（§11.2-A，尚未扩展）；生产未应用 021）
- [x] **DDL**：interface 表保留 device/exporter/ifIndex/direction/layer、bytes/packets/count、observed/known、sampling/version refs、generation/marker。（2026-09-28 复核：已完成 — `deploy/migration/clickhouse/021_flow_atomic_5m.sql:25-46`；device/exporter/if_index/direction/layer、bytes/packets、observed/known、版本数组、ingest 代区间、generation+row_kind 均在）
- [x] **DDL**：aggregate 表排除 `src_ip / dst_ip / remote_port` 等高基数端点维度，并沿用 EAV generation marker 约定。（2026-09-28 复核：已完成 — `c1385cd3b`; `internal/flowch/rollup.go:1257`；写入端排除 src_ip/dst_ip/remote_port；表 AS 1h EAV，沿用 _generation marker（bucket_seconds=300））
- [x] **DDL**：两表以 `bucket_start` 为 ORDER BY 前缀；aggregate 设置 `ttl_only_drop_parts=1`，interface 的分区同样必须支持 job 整 part 删除；按 `retention / target_partitions` 预览分区宽度，和按日/月候选做 part/merge/drop benchmark 后冻结。（2026-09-28 复核：已完成 — `deploy/migration/clickhouse/021_flow_atomic_5m.sql:48-49,62-68`；两表以 bucket(_start) 开头；ttl_only_drop_parts=1；interface 月分区可整分区 DROP；分区 benchmark 已被 §11.2-H 取消）
- [x] **DDL/编码**：interface 证据表实现 hold-aware lifecycle 删除和冻结账单证据，证明不会删除未关闭账期、争议或 legal hold 所需数据。（2026-09-28 复核：作废 — §11.2-A 删除了 hold/dispute/证据冻结，interface 5m 并入归档月状态机）
- [x] **API**：schema readiness 返回两张表、字段版本和 migration version；缺表时明确 unavailable，不静默回 raw 扫大范围。（2026-09-28 复核：作废 — §11.1 改用 watchdog-flow-migrate -command inspect；注意 native.go 的 Storage V2 schema 校验不含 5m 表；`cmd/watchdog-flow-migrate/main.go:52,134-137`）
- [ ] **单元测试**：DDL 契约、enum/LowCardinality、IPv4/IPv6、空值、marker 唯一性、TTL 默认/覆盖、整 part 删除和证据 hold。（2026-09-28 复核：部分完成 — 只断言表数、字段与 TTL 字符串；缺 enum/LowCardinality、空值、marker 唯一、TTL 覆盖、整 part 删除用例（hold 部分已作废）；`423d820e2`; `internal/flowch/schema_test.go:53,82-84`）
- [ ] **集成测试**：真实 ClickHouse clean install、从 020 升级到 021、重复 migration、回滚只停读写不删数据。（2026-09-28 复核：部分完成 — 通用测试（需环境变量）覆盖 001-021 安装→022 升级→幂等；无 020→021 专测、5m 表断言、回滚测试与运行记录；`internal/flowch/migrator_integration_test.go:20-130`）
- [ ] **变更测试**：旧 server/worker 在 021 已执行但 feature disabled 时仍可运行。
- [ ] **已提交门禁**：migration、schema readiness 和真实 CH 测试同一独立提交；不得只提交 DDL。（2026-09-28 复核：部分完成 — 独立提交含 DDL+单测+install 文档；缺真实 CH 测试（仅 dev 手工验证）；schema readiness 已被 §11.1 移出；`423d820e2`）

### FL5M-02 5m rollup runner 与 marker

- [ ] **编码**：新增 `RollupFiveMinutes`；明确 aggregate 5m 的 1m 来源和 raw fallback，interface 5m 只从 raw 生成。（2026-09-28 复核：部分完成 — RollupFiveMinute 已实现且只能从 1m 派生，raw fallback 显式拒绝；从 raw 生成 interface 5m 的 runner 未做；`c1385cd3b`; `internal/flowch/rollup.go:24,824-826,910-911`）
- [ ] **编码**：实现 interface 5m 专用 runner；不把接口行硬塞进 EAV runner。
- [ ] **编码**：两个 runner 均先写数据再写 marker，重试使用稳定 generation，支持 marker-only 空桶。（2026-09-28 复核：部分完成 — aggregate 5m 已做到数据→marker、确定性 token、空小时由区间 marker 覆盖；interface runner 缺；热路径重试会换新代；`internal/flowch/rollup.go:658-709,952-957,1108-1124`）
- [ ] **编码**：stats/metrics 分别记录扫描行/字节、输出行、耗时、失败、repair、marker gap。（2026-09-28 复核：部分完成 — FiveMinute 只在内存计数，flowmetrics 只导出 1m/1h；无扫描行/字节、输出行、耗时、marker gap 指标；`internal/flowch/rollup.go:120,719,793`; `internal/flowmetrics/rollup.go:41-66`）
- [ ] **单元测试**：五个 1m 完整性、空桶、双方向、一条 raw 贡献两个接口、unknown sampling、版本 refs、稳定重试。（2026-09-28 复核：部分完成 — 只有 SQL 形态、批范围与 1m 完整/不完整两例；缺空桶、双方向、一条 raw 贡献两接口、unknown sampling、版本 refs、重试；`internal/flowch/rollup_test.go:697-754`; `internal/server/flow_hot_rollup_test.go:222-263`）
- [ ] **集成测试**：真实 CH crash-before-marker、marker retry、同 generation takeover、新 generation repair、旧 generation 不可见。
- [ ] **性能测试**：1 小时批量构建 12 个 5m bucket 与逐 5m 构建对比，按 read bytes/CPU/峰值内存选实现。
- [x] **变更测试**：验证现有 1m/1h/1d SQL 和 marker 没有被改变。（2026-09-28 复核：已完成 — `c1385cd3b`; `internal/flowch/rollup_test.go:107-304`；1m/1h/1d 的 SQL 与 marker 常量未改，只加 5m 分支；原有用例未改）
- [ ] **已提交门禁**：runner、指标、单元与真实 CH 集成测试同一纵向切片提交。（2026-09-28 复核：部分完成 — 同一提交含 runner、stats、单测；缺 metrics 导出、真实 CH 集成测试与 interface runner；`c1385cd3b`）

### FL5M-03 热路径调度

- [ ] **设计**：冻结 seal delay、repair window、每轮 bucket 数、并发、优先级和 CH read budget；全部为 runtime config 并有安全上限。（2026-09-28 复核：部分完成 — 按 §11.1 复用既有 hot_rollup 配置键未加新键；但 5m 未用 late window/repair_interval，hour_lookback 无上限（默认 72h，超过 1m TTL 48h）；`d6c38b62e`; `internal/server/flow_hot_rollup.go:103-141`; `internal/server/config.go:365-383`）
- [ ] **编码**：在已有 hot scheduler 中编排 1m 完整后生成 aggregate 5m，并从 closed raw 生成 interface 5m。（2026-09-28 复核：部分完成 — 1m 整小时齐即派生 aggregate 5m（已接入未部署）；interface 5m 缺；1m 迟到 repair 后 5m 不会重建（已覆盖即跳过）；`d6c38b62e`; `internal/server/flow_hot_rollup.go:91-141`）
- [x] **编码**：不得新建第二套 lease/state machine；继续使用 operation job/marker/generation 原语。（2026-09-28 复核：已完成 — `internal/server/flow_hot_rollup.go:103-141`；复用 flowHotRollupScheduler 与 CoveredThroughAtLeast/Run 的 marker/generation 原语，无新 lease 或状态机）
- [ ] **编码**：启动时从 marker gap 恢复，不从“当前时间”盲目跳过历史缺口。（2026-09-28 复核：部分完成 — 每轮在 lookback 内按 5m marker 缺口从最旧补起；超出 lookback 或 1m TTL 的缺口无冷路径兜底（FL5M-04 未做）；`internal/server/flow_hot_rollup.go:104-125`）
- [ ] **单元测试**：时区、桶边界、seal delay、迟到、预算、取消、重启、重复调度。（2026-09-28 复核：部分完成 — 只有“1m 完整→跑一次”与“不完整→跳过”两例；缺时区、seal delay、迟到、预算、取消、重启、重复调度；`internal/server/flow_hot_rollup_test.go:222-263`）
- [ ] **集成测试**：真实 MySQL lease + CH，双 server 抢占、crash/restart、backpressure 下最终收敛。
- [x] **API/UI**：readiness 展示调度器启用状态、最近成功桶、lag、gap 和失败原因。（2026-09-28 复核：作废 — §11.1 FL5M-03 移出 readiness 展示；另 5m stats 未导出到 metrics；`internal/flowmetrics/rollup.go:41-66`）
- [x] **已提交门禁**：默认 feature disabled；完成影子期运行证明后再由部署配置启用。（2026-09-28 复核：作废 — §11.1 改为跟随 hot_rollup.enabled、不设 write_5m；Go 默认 false 但 yaml 为 true，部署后立即开始影子写；`internal/server/config.go:261`; `config/watchdog.yaml:52`）

### FL5M-04 冷构建、迟到与 repair

- [ ] **编码**：扩展现有 `flow_storage_downsample` job payload/checkpoint，使一个 UTC 日包含 aggregate 5m、interface 5m、1h、1d 阶段状态。
- [ ] **编码**：1m 尚在时 aggregate 5m 从 1m 生成；1m 已过期时从 raw 生成，结果语义必须相同。
- [ ] **编码**：interface 5m 从 raw 生成；迟到窗口内按 repair generation 重建受影响 bucket。
- [ ] **编码**：旧 payload 版本明确拒绝或迁移，不进行猜测式兼容。
- [ ] **单元测试**：水位、UTC 日、迟到边界、resume checkpoint、temporary/permanent error、cancel/takeover。
- [ ] **集成测试**：真实 CH/MySQL 注入迟到 flow，验证 5m 新 generation、1h/1d 和 raw 守恒最终收敛。
- [ ] **变更测试**：rolling upgrade 中旧 worker 不领取新 payload；新 worker 不误处理旧 schema。
- [ ] **已提交门禁**：operation job、runner 和迁移测试通过后独立提交，不保留并行临时状态机。

### FL5M-05 查询规划与所有入口统一

- [ ] **编码**：planner 加入 5m source tier；仅对非端点 dimension 且连续 marker 覆盖时选择。
- [ ] **编码**：src/dst IP、remote port 和 raw detail 永不路由到 aggregate 5m。
- [ ] **编码**：Explorer、六报表、source/destination/overseas/VPN、导出共用同一 planner 和 coverage reader。（2026-09-28 复核：部分完成 — 各入口都用 PlanAggregate，但只有报表走覆盖水平线；Explorer、direction split、导出未统一；无 5m；`internal/server/flow_reports.go:474-491`; `internal/server/handlers_flow.go:255`; `internal/server/flow_exports.go:508`）
- [ ] **编码**：首版禁止跨层拼接；选择单一完整层或预算内完整 raw fallback。
- [ ] **API**：响应增加 source tier、coverage、generation、incomplete reason；资源超限返回可操作建议而非伪 0。（2026-09-28 复核：部分完成 — 已有 source、effective_from/to、covered_buckets；Bucket 无 5m 值，无 generation/incomplete_reason；`internal/flowquery/plan.go:20-34`; `internal/flowquery/runner.go:45`; `internal/flowquery/query.go:72-74`）
- [ ] **单元测试**：4m59s/5m/59m/1h 边界、marker gap、endpoint、显式 step、空范围、时区和晚高峰。
- [ ] **集成测试**：同一固定向量在 raw/1m/5m/1h 结果守恒，所有报表入口路由一致。
- [ ] **性能测试**：24h/7d/30d 多维查询的 read rows/bytes、P95/P99、内存、并发和资源超限率；证明非端点请求不读 raw，且读放大与选中 tier 同阶。
- [ ] **性能测试**：30 天单端口账单只读 interface 5m，读取量与端口×方向×层×桶数同阶；与 6 小时 raw 的 88.2M 行/3.12 GiB 基线对比。
- [ ] **回归测试**：前端单位、流入/流出、六分类名称、运营商/地域名称和分页导出不因 source tier 改变。
- [ ] **已提交门禁**：后端 planner、API contract、前端适配和路由扫描测试同一纵向切片提交。

### FL5M-06 Billing 证据迁移

- [ ] **设计**：为账期冻结 `evidence_source`、5m generation/coverage、分类/地址版本、缺桶和 unknown sampling 语义。（2026-09-28 复核：部分完成 — §11.1 FL5M-06 已定账期加 evidence_source（建期冻结）、agg 语义不变；列定义、取值、旧账期语义未写进 DDL 或文档）
- [ ] **DDL**：账期/计算结果增加 evidence source 与覆盖引用；关闭状态不可更新来源。
- [ ] **编码**：实现 interface 5m billing reader；保留 raw reader 仅用于旧账期和 shadow compare。
- [x] **编码**：长账期 calculate/recalculate/export 全部通过 operation job，在线请求不执行 400 天 raw/5m 扫描。（2026-09-28 复核：作废 — §11.2-E 裁定：切到 interface 5m 后同步 API 足够，异步不进本轮）
- [ ] **API/UI**：展示证据来源、完整率、缺桶、版本引用和异步任务状态；禁止把 incomplete 账期标为可批准。
- [ ] **单元测试**：月95、日95、平均、总量、时区、方向、缺桶、counter reset、unknown sampling、三层修正、重复计算和 reversal。（2026-09-28 复核：部分完成 — compute 层与 raw reader 已测 95th、日95、时区、三层、unknown sampling、reversal；interface 5m reader 未实现故无对应用例；`internal/billing/compute_test.go:9,152,197`; `internal/flowch/billing_test.go:45,101`）
- [ ] **集成测试**：真实 CH raw 与 interface 5m shadow compare；同一固定向量可由独立 SQL 复算。
- [ ] **变更测试**：旧打开账期保持 raw 或显式迁移；关闭账期拒绝隐式重算；一账期不得混源。
- [ ] **已提交门禁**：金额/带宽向量由独立 reviewer 复算，之后才允许新账期默认切 5m。

### FL5M-07 Ingest、rollup 与接口对账闭环

- [x] **编码**：主 reconciliation 增加 receipt `counter_record_count` 对 `sflow_interface_counters` 的坐标身份与行数校验；若还需比较累计 counter 总和，先显式扩展 receipt schema，不从现有字段臆算。（2026-09-28 复核：作废 — 未实现（counter-only 消息只放行不校验 counter 行）；§11.1 FL5M-07 已移到运维项目；`internal/flowch/reconciliation_scanner.go:167-174`）
- [x] **编码**：将 committed/reconciled watermark 只有在 fact 与 counter 两路都健康时推进。（2026-09-28 复核：作废 — 未实现；§11.1 FL5M-07 移出“水位双路推进”）
- [x] **编码**：增加 raw→aggregate 5m、raw→interface 5m 的 marker/coverage/build audit。（2026-09-28 复核：作废 — 未实现；§11.1 FL5M-07 只保留一条，5m 覆盖仅以 §11.4 L758 blocker 形式保留）
- [ ] **编码**：实现 Flow 估算、sFlow counter、SNMP 5m 对比 issue；只报告 coverage、ratio 和原因，不自动修正数据。（2026-09-28 复核：部分完成 — 只读返回覆盖、比值、reset/gap；未接 API 或调度，无 issue 原因；flow 腿仍扫 raw FINAL；线上人工核验 Flow/sFlow 差≈2%；`c00e6c646`; `internal/flowch/interface_reconciliation.go:94,249`）
- [x] **API/UI**：按设备/端口/方向展示未采样、倍率异常、丢样、mapping reject、counter-only 和时间错位。（2026-09-28 复核：作废 — 未实现；§11.1 FL5M-07 移出 issue 面板）
- [x] **单元测试**：fact-only、counter-only、partial insert、receipt missing、duplicate replay、counter reset/wrap、零流量与未知值。（2026-09-28 复核：作废 — 针对已移出的 receipt↔counter 对账（§11.1 FL5M-07），未实现）
- [x] **集成测试**：真实 Kafka→worker→CH 故意在三次 INSERT 间 crash，重放后对账收敛且水位不越过缺口。（2026-09-28 复核：作废 — 随 receipt↔counter 与水位双路一起被 §11.1 FL5M-07 移出，未实现）
- [ ] **变更测试**：接口比值异常不阻塞 raw 数值守恒，但 missing 5m marker 阻塞 raw 删除。
- [ ] **已提交门禁**：未提交 WIP 经审查后重做/吸收；不得直接以文件存在宣告完成。（2026-09-28 复核：部分完成 — WIP 已并入标题无关的 c00e6c646，c539d770e 补单测；尚未接线，flow 腿未改读 interface 5m；`c00e6c646`; `c539d770e`）

### FL5M-08 Retention 与物理清理

- [ ] **设计**：扩展 published policy：各层默认/最小/最大保留、目标 part 数、receipt audit、counter raw、billing dispute、legal hold、backup requirement；禁止散落业务 const。
- [ ] **编码**：允许在线修改 aggregate 5m/1h TTL，使用 `materialize_ttl_after_modify=0` 避免发布瞬间触发无预算全表重写；readiness 展示当前和待生效 retention。
- [x] **DDL/编码**：新增 receipt daily summary；详细 receipt 只在整月所有关联 raw 日安全后按月 DROP PARTITION。（2026-09-28 复核：作废 — §11.1 FL5M-08 移出，改为 receipts TTL 45 DAY（同样未做，见 §11.4 L761））
- [x] **编码**：sFlow counter 按月检查 interface 5m 覆盖、对账、保留和 hold 后删除。（2026-09-28 复核：作废 — §11.1 FL5M-08 移出，改为 counters TTL 90 DAY（未做，018 无 TTL）；`deploy/migration/clickhouse/018_sflow_interface_counters.sql`）
- [ ] **编码**：aggregate 5m/1h 查询层允许 CH 整 part TTL；interface 5m、1d 及其他证据数据走 operation job/hold/receipt。任何层都禁止行级无预算 mutation。（2026-09-28 复核：部分完成 — aggregate 5m 整 part TTL 已在 021；DropArchiveMonth 只删 1d/1h，未加 interface 5m 分区；1h TTL 已被 §11.2-B 否决；`deploy/migration/clickhouse/021_flow_atomic_5m.sql:67-68`; `internal/flowch/raw_delete.go:45-80`）
- [x] **编码**：按 dead/live ratio、part count 和 I/O budget 调度旧 generation 分区 rebuild/compaction；禁止逐桶 mutation。（2026-09-28 复核：作废 — §11.1 FL5M-08 移出，改为再生后按分区清理死代（§11.2-H，未实现，见 §11.4 L761））
- [ ] **编码**：列出 legacy 表引用，经过零引用、备份和回滚窗口门禁后再删除。
- [x] **单元测试**：跨月 receipt、迟到事件、open bill、legal hold、policy retirement、并发批准和 stale approval。（2026-09-28 复核：作废 — 对应删除功能（跨月 receipt、hold、open bill）已被 §11.1 FL5M-08 移出）
- [x] **集成测试**：真实 CH DROP PARTITION，验证当前查询、账单、reconciliation、restore 均不依赖已删 detail。（2026-09-28 复核：作废 — detail receipt/counter 的 DROP 作业已被 §11.1 FL5M-08 移出；替代的 TTL 方案需另测）
- [x] **已提交门禁**：先提交只读 readiness，再提交实际 delete handler；默认删除开关关闭。（2026-09-28 复核：作废 — readiness 已被 §11.1 移出；删除开关默认 0 已由 0033 保证；`deploy/schema/mysql/0033_flow_storage_lifecycle.sql:14-16`）

### FL5M-09 Backup、restore 与统一 readiness

- [x] **编码**：backup evidence 只能由成功 restore-drill job 生成，绑定 job ID、manifest/schema digest、覆盖范围和核验 counters。（2026-09-28 复核：作废 — 未实现（evidence 由 API 手工录入）；§11.1 FL5M-09 已移出；`internal/flowlifecycle/backup_evidence.go:67`）
- [x] **编码**：evidence 过期、最近 drill 失败或覆盖不足时阻止新的删除批准。（2026-09-28 复核：作废 — 未实现（只取最新 verified 覆盖证据）；§11.1 FL5M-09 已移出；`internal/flowlifecycle/archive_delete_readiness.go:246-249`）
- [x] **API**：新增统一 lifecycle readiness，汇总 scheduler、policy、tier coverage、watermark、mismatch、disk、backup 和 blockers。（2026-09-28 复核：作废 — 未实现；§11.1 FL5M-09 移出统一 readiness API）
- [x] **UI**：按 UTC 日/月份展示 raw→5m→1h→1d 状态和“为什么不能删”，不提供绕过门禁按钮。（2026-09-28 复核：作废 — 未实现；§11.1 FL5M-09 移出 readiness UI）
- [x] **单元测试**：伪造 evidence、digest mismatch、过期、部分覆盖、并发 drill、失败清理。（2026-09-28 复核：作废 — 未实现；随 §11.1 FL5M-09 一起移出）
- [x] **集成测试**：外部备份恢复到隔离数据库，核对 raw/5m/1h/1d/receipt summary 后生成 evidence。（2026-09-28 复核：作废 — 未实现（drill 仍只核对 raw/archive）；随 §11.1 FL5M-09 移出；`internal/flowch/restore_drill.go:257`）
- [x] **运行测试**：周期性 drill、告警和审计日志在 server 重启后仍可追溯。（2026-09-28 复核：作废 — 未实现；周期演练已被 §11.1 FL5M-09 移出）
- [x] **已提交门禁**：restore drill 真实成功记录作为发布证据，不接受只跑 mock。（2026-09-28 复核：作废 — 随 §11.1 FL5M-09 移出；仓库无真实 drill 成功记录）

### FL5M-10 Backfill、切流与回滚

- [ ] **设计**：冻结 backfill 范围、分区顺序、并发/读预算、暂停/恢复、磁盘高水位和失败重试。（2026-09-28 复核：部分完成 — §11.1 FL5M-10 已定回填范围；热调度只在 hour_lookback（生产 24h）内自动补 5m-EAV；interface 回填、顺序、预算、暂停、高水位未定；`internal/server/flow_hot_rollup.go:104-110`）
- [ ] **编码**：按日 operation job backfill 两套 5m，支持断点、取消、幂等和 generation repair。
- [ ] **编码**：feature flags 分离 `write_5m / read_5m / billing_5m / require_5m_for_delete`，避免一次性切换。
- [ ] **验证**：至少覆盖正常日、空日、迟到日、mapping reject 日、counter-only 日和跨版本日。
- [ ] **切流**：先影子写，再 query read，后 billing read，最后 raw delete gate；每步独立批准。
- [ ] **回滚**：关闭 read flag 即回旧 planner；关闭 write 不删除已生成 5m；账期 evidence source 不随全局 flag 改变。
- [ ] **容量测试**：backfill 与在线 ingest/query 并行时，Kafka lag、CH merge、P99 和磁盘不突破预算。
- [ ] **已提交门禁**：每个 flag 和回滚路径有自动测试，部署 runbook 写明当前阶段。

### FL5M-11 全链回归与发布门禁

- [ ] **单元门禁**：`flowstream / flowworker / flowch / flowquery / flowlifecycle / billing` 全部通过，含 race/vet。
- [ ] **真实基础设施**：Kafka、MySQL、ClickHouse clean install，fast sFlow/NetFlow decode→raw/counter/receipt→5m→query/billing 全链。
- [ ] **可靠性**：Kafka rebalance、worker crash、CH timeout/fake ack、server takeover、迟到、重复重放和磁盘高水位。
- [ ] **准确性**：raw/rollup 守恒、Flow/sFlow/SNMP 对账、IPv4/IPv6、方向、sampling、六分类、三层修正和历史版本。（2026-09-28 复核：部分完成 — Flow 估算与 sFlow counter 差≈2% 已线上验证；5m 守恒、IPv6、方向、六分类、三层、历史版本未验）
- [x] **性能**：固定硬件与数据集下 ingest EPS、Kafka lag、5m build、24h/7d/30d query P95/P99、billing 和 backfill 并发。（2026-09-28 复核：作废 — §11.1 FL5M-11 移出固定硬件 SLA；查询性能验收改由 §11.4 L763 承担）
- [ ] **生命周期**：policy 发布、archive、repair、backup、restore、approve、raw delete、receipt/counter/archive delete 完整演练。
- [ ] **产品回归**：Explorer、六报表、VTable 分页/过滤/导出、账单和 readiness 均显示真实 source/coverage/incomplete。
- [x] **发布门禁**：任务清单、代码、DDL、API、测试报告、运行指标、回滚记录和独立复算证据全部进入独立提交；禁止仅勾文档。（2026-09-28 复核：作废 — §11.1 FL5M-11 移出发布门禁仪式，改为常规测试门禁）

### FL5M-12 Raw 时间聚簇与压缩 P3

- [ ] **设计**：把 raw 排序前缀从 `toStartOfHour(event_time)` 改为 `toStartOfFiveMinutes(event_time)`；冻结 Kafka 坐标去重、`FINAL`、端点过滤和新旧表并存语义。
- [ ] **审计**：逐一审查 `remote_ip/local_ip/sample_*/*_if_index` 的写入和读取者；只有确定性可重算且查询成本可接受的列才改 `ALIAS`，不为压缩率破坏 VPN/明细查询。
- [ ] **实验**：先在新 part/影子表比较 minmax index、5m ORDER BY、codec 和 ALIAS 组合；记录压缩 B/row、5m/1h/24h read rows/bytes、插入吞吐、merge CPU 与 `FINAL` 成本。（2026-09-28 复核：部分完成 — minmax 索引线上 757→122 granules（≈6×）、raw≈62 B/row；5m ORDER BY 影子表、ALIAS、吞吐、merge CPU、FINAL 成本未测；`deploy/migration/clickhouse/022_flow_records_event_time_index.sql:4-14`）
- [ ] **迁移**：采用新版本表和分区级 backfill/校验/切换；不在原表上做不可回滚的大 mutation；空间预算必须容纳新旧分区并存。
- [ ] **准确性测试**：Kafka 坐标去重、重放、新 generation、IPv4/IPv6、端点过滤、VPN 和分类结果在新旧表完全一致。
- [ ] **性能门禁**：同一 5 分钟 raw 查询不再读取接近整小时；codec/ALIAS 达到已冻结的容量收益且 ingest P99 不退化越界。（2026-09-28 复核：部分完成 — 第一句线上已满足（只读 122/757 granules）；codec/ALIAS 目标未冻结（现 62 B/row），ingest P99 未测；`deploy/migration/clickhouse/022_flow_records_event_time_index.sql:4-11`）
- [ ] **回滚**：切回旧表/旧 reader 不删除新表；只有独立备份恢复和零读者证明后才回收旧分区。
- [ ] **已提交门禁**：设计、基准报告、DDL、迁移器、读写切换和回滚测试分阶段提交；不得与 5m 逻辑表首发绑定。（2026-09-28 复核：部分完成 — 022 索引与实测注释已提交（与 021 同提交，§11.1 允许）；排序键重建的设计、基准、迁移器、切换、回滚测试未做；`423d820e2`）

## 9. 完成定义

本生命周期优化包只有同时满足以下条件才可标记完成：

- [x] clean install 和升级安装均存在两张 5m 表，schema readiness 为通过；（2026-09-28 复核：作废 — §9 完成定义已被 §11.4 替代（对应 L756）；schema readiness 已被 §11.1 移出）
- [x] hot、cold、late repair 均能生成并原子发布两套独立 marker；（2026-09-28 复核：作废 — §9 已被 §11.4 替代（对应 L757/L758）；目前无 interface marker、无冷路径，5m 无迟到 repair）
- [x] 查询 planner 实际选择 5m，端点查询明确不误用 5m；（2026-09-28 复核：作废 — §9 已被 §11.4 替代（对应 L759）；planner 仍只选 1m/1h/1d）
- [x] 新账期实际从 interface 5m 读取并冻结 evidence source；（2026-09-28 复核：作废 — §9 已被 §11.4 替代（对应 L760）；无 evidence_source）
- [x] receipt 对 flow facts 和 sFlow counters 两路均可证明无静默丢失；（2026-09-28 复核：作废 — §9 已被 §11.4 替代；§11.1 FL5M-07 移出 receipt↔counter 对账）
- [x] raw 删除仍通过 1h 守恒，并额外确认正式下游 5m 覆盖；（2026-09-28 复核：作废 — §9 已被 §11.4 替代（对应 L758）；blocker 未实现）
- [x] receipt、counter、旧 generation 和 archive 都有可执行、可回滚、可审计的清理路径；（2026-09-28 复核：作废 — §9 已被 §11.4 替代（对应 L761）；TTL、死代清理、legacy DROP 未做）
- [x] 外部恢复演练真实通过，生成的 evidence 能被删除门禁验证；（2026-09-28 复核：作废 — §9 已被 §11.4 替代；§11.1 FL5M-09 已移出）
- [x] aggregate 5m 默认 90 天、1h 默认 360 天且均可在线配置；interface 5m 的 policy 默认 90 天且 hold-aware；整 part 删除能把各层稳定在已批准容量预算内；（2026-09-28 复核：作废 — §9 已被 §11.4 替代（对应 L756/L761）；1h 360d 为 policy 值非 TTL，interface 不做 hold）
- [x] raw codec/派生列/时间聚簇优化有前后基准，达到冻结的 B/row 和时间裁剪目标且不降低 ingest 可靠性；（2026-09-28 复核：作废 — §9 已被 §11.4 替代（对应 L762）；codec 已上线（62 B/row），ALIAS 不在本轮）
- [x] 24h/7d/30d 非端点查询不读 raw，30 天账单不读 raw，query_log 证明 read rows/bytes 与所选 tier 同阶；（2026-09-28 复核：作废 — §9 已被 §11.4 替代（对应 L763），按 §11.2-C 改为“覆盖前缀内零 raw 读取”）
- [x] 24h/7d/30d 查询、账单和 backfill 达到固定性能预算，默认请求不再触发 ClickHouse resource limit；（2026-09-28 复核：作废 — §9 已被 §11.4 替代（对应 L763）；固定性能预算已被 §11.1 FL5M-11 移出）
- [x] 统一 readiness 能准确解释每个分区当前处于可查、可计费、可修复、可删或阻塞的原因；（2026-09-28 复核：作废 — §9 已被 §11.4 替代；§11.1 FL5M-09 移出统一 readiness）
- [x] 相关实现已提交，工作树没有依赖未提交 helper/WIP 文件。（2026-09-28 复核：作废 — §9 已被 §11.4 替代；条件本身目前成立（工作树干净，原 WIP 已在 c00e6c646 提交）；`c00e6c646`）

## 10. 建议执行顺序

严格按以下顺序推进，避免再次出现“表、查询、账单、删除各自做了一半”的状态：

1. `FL5M-00` 设计冻结与基线。
2. `FL5M-01` schema readiness。
3. `FL5M-02` runner/marker。
4. `FL5M-03` 热影子写与 `FL5M-04` 冷补建/repair。
5. `FL5M-07` ingest/counter/rollup 对账。
6. `FL5M-05` 非端点查询切流。
7. `FL5M-06` 账单证据迁移。
8. `FL5M-09` 恢复证据和统一 readiness。
9. `FL5M-10` backfill/切流/回滚。
10. `FL5M-08` 在以上证据成立后启用 retention 清理。
11. `FL5M-12` 作为独立 P3 执行 raw 5m 时间聚簇与压缩重建；其影子基准可在第 1 步后准备，但不得捆绑 5m 逻辑层首发。
12. `FL5M-11` 汇总执行全链、容量、查询性能与发布门禁。

在 `FL5M-11` 完成前，raw 删除策略应保持关闭；在账单迁移验收前，不能把 `flow_interface_traffic_5m` 宣称为账单权威；在真实 marker 覆盖前，查询不能仅因表存在就选择 5m。

---

## 11. 复核裁定(2026-09-24):范围控制、设计修正与四个澄清

复核依据:仓库 HEAD `bc10958f4` + 未提交 WIP;生产 release r19 只读实测(`system.tables` / `query_log`,全部 1.5 GiB / 2 线程);`docs/watchdog-5m-atomic-tier-design-2026-09-23.md` §8。核心目标以用户 2026-09-24 的表述为准:**以已在 TB 级节点运行过的 akvorado 为基础,复用既有 ClickHouse 能力和 akvorado 成熟的多分辨率保留模型,提高查询效率,降低 raw 容量占用(raw 小本身也提高查询效率)。** 本文 §4–§10 的 13 个任务组、131 个勾选项按这个目标裁定。

### 11.1 范围裁定:131 项 → 约 40 项

判据只有一条:该项是否直接服务"多分辨率保留 + 查询效率 + raw 容量"。不服务的不是错,是**另一个项目**。

| 任务组 | 裁定 | 保留内容 | 移出内容(另立项目) |
|---|---|---|---|
| FL5M-00 基线 | **缩减** | 沿用 2026-09-23 已采集基线(§5.1);切流时同口径重采 | 独立 ADR 评审流程、"验收预算冻结"仪式 |
| FL5M-01 schema | **核心** | migration 021 两张表;aggregate 5m 日分区 + `TTL 90 DAY` + `ttl_only_drop_parts=1`;interface 5m 月分区、**无 TTL**、含 provenance 列(见 11.2-A) | "hold-aware lifecycle 删除"、schema readiness API(用 `watchdog-flow-migrate -command inspect` 即可) |
| FL5M-02 runner/marker | **核心** | 两个 runner(EAV 5m 从 1m 派生、interface 5m 从 raw)、两套 marker、marker-only 空桶、stats | — |
| FL5M-03 热调度 | **核心,缩减** | 接入现有 `flowHotRollupScheduler`,复用 `hour_lookback / hour_late_arrival_window / repair_interval`,**不新增配置键**;启用与否跟随 `hot_rollup.enabled` | 第二套 lease/状态机(本来就没有)、readiness 展示 |
| FL5M-04 冷构建 | **核心,缩减** | 归档作业加 5m 阶段:**raw → 5m-EAV(一次 raw 扫描)→ 1h(派生)→ 1d(派生)** + interface 5m(raw);checkpoint 沿用 `next_hour` | "1h 从 5m 派生另立 ADR"(见 11.2-D) |
| FL5M-05 规划统一 | **核心** | planner 加 5m;端点规则;Explorer/报表/direction split/导出共用 `planFlowReportAggregate` 的水平线逻辑;响应带 source/coverage(`AggregatePlan` 与 `covered_buckets` 已有) | "资源超限返回可操作建议"等文案打磨 |
| FL5M-06 账单 | **核心,缩减** | 账期表加一列 `evidence_source`(建期冻结);interface 5m reader;影子对比;`agg` 语义不变 | **长账期改异步 operation job**(见 11.2-E);争议期/legal hold 字段 |
| FL5M-07 对账 | **后置** | 仅一条:WIP `interface_reconciliation.go` 的 flow 腿改读 interface 5m | receipt↔counter 对账、水位双路推进、issue 面板——与本目标无关 |
| FL5M-08 保留/清理 | **缩减** | policy 发布 → `ALTER … MODIFY TTL`(aggregate 5m、1m,`materialize_ttl_after_modify=0`);receipts `TTL 45 DAY`、`sflow_interface_counters` `TTL 90 DAY`(DDL 即可);死代清理作业(按分区、再生后触发);DROP 5 张空 legacy 表 | receipt daily summary 表、按月 receipt/counter 删除作业、dead/live ratio 调度 compaction、hold 字段 |
| FL5M-09 备份/readiness | **后置** | — | backup evidence 与 restore-drill job 绑定、周期演练、统一 readiness API/UI:是运维项目,不是保留模型项目 |
| FL5M-10 回填/切流 | **缩减** | 回填 = 1m 保留期内(2 天)→ 5m-EAV + 现存 raw → interface 5m;**两个开关**:`flow.query.read_5m`、`flow.billing.evidence_source` | `write_5m`(表存在即写,幂等且 1.7 s/小时)、`require_5m_for_delete`(进 policy/readiness blocker,不是 flag) |
| FL5M-11 全链回归 | **缩减** | 正常测试门禁(单测 + 真实 CH 集成 + 影子对比) | "固定硬件 SLA""发布门禁仪式" |
| FL5M-12 raw 时间聚簇 | **提级到 P1** | 见 11.2-F:先 `event_time` minmax 索引实验,再 5 分钟排序键重建 | "不得与 5m 首发捆绑"——改为紧随 021 之后 |

移出项统一记入"生命周期运维项目"待办,不混入本轮。

### 11.2 设计修正

**A. interface 5m 不需要新的"hold-aware"删除机制,并入既有归档月状态机。** 本文 §4.6 修正 A 为 interface 5m 引入账期冻结、争议期、legal hold、job 整 part 删除——这是一套新概念。事实:① 该表极小(live 形态 540 行/小时 ≈ 13K 行/天,一年不到 5M 行、几十 MB),**没有任何容量理由**在 90 天删它;② 1h/1d 的删除本来就走 `archive_delete_enabled` + `ArchiveMonthDeleteReadiness` + 审批 + `DropArchiveMonth`(`flowch/raw_delete.go:45-70` 同时 DROP 1h 与 1d 的月分区)。裁定:interface 5m 用 **月分区**,**无 DDL TTL**,`DropArchiveMonth` 多 DROP 一个分区;保留期 = policy `archive_retention_seconds`(与 1h 相同);证据保护由既有 `require_backup_before_delete` + 审批承担。删除 §4.6-A 与 FL5M-01/08 中的 hold/dispute/evidence-freeze 条目。

**B. "1h 默认 360 天可配置 TTL"会破坏审批状态机。** live 事实(`system.tables`):`flow_records`、`flow_aggregate_1h`、`flow_aggregate_1d`、`flow_ingest_receipts`、`sflow_interface_counters`、`snmp_*` **都没有 DDL TTL**;001 的 1h `400 DAY` 已在 011 重建时去掉;只有 1m 有 `TTL 2 DAY`。给 1h 加 DDL TTL 会让分区在状态机背后消失,`ArchiveMonthPhysicalRecords` 与 readiness 会把它当作异常。裁定:**查询缓存层(1m、aggregate 5m)用 DDL TTL,policy 通过 `MODIFY TTL` 调整——这是 akvorado 模式;证据层(1h、1d、interface 5m)只走状态机。** "1h 360d"是 policy `archive_retention_seconds` 的产品默认值,不是 TTL。

**C. 路由矩阵采纳,但门禁措辞要精确。** §4.8 的 `[5m,1h)` → aggregate 5m 比 §8.2 #5 的"1m 服务不了才用 5m"更优:live 1m 每桶约 9–10K 行、5m 每桶约 2K 行,同一 6 小时图表读行数约差 20 倍。代价是 `plan_test.go` 两个用例(five minutes / six hours)的期望源从 1m 改 5m,可接受。但 §1.2/§5.3/§9 的"24h/7d/30d 非端点查询**不读 raw**"作为验收会永远失败:1m/5m/1h 都在**小时封闭后**发布,封闭后到 marker 发布之间(raw→1m 平均 144 s + 派生)最近那个小时只能读 raw。正确表述:**marker 覆盖前缀内零 raw 读取;raw 尾段仅限最近一个尚未发布的封闭小时与迟到修复;以 `query_log` 中 raw 读行数占比与 P95 验收。** 要真正把尾段压到 ≤10 分钟,前提是 11.2-F(5 分钟发布节奏)。

**D. 冷路径 1h 应从 5m 派生,不另立 ADR。** §4.7 把"1h 是否从 5m 派生"推到独立 ADR,同时要求冷任务从 raw 各建一次 5m 和 1h——冷路径 raw 扫描翻倍(EAV 扇出形态每小时 ~144 s 级)。热路径已经证明 1h 由 1m 派生正确(`derivedRollupSQL`,加法量精确,守恒对账 `DayStorageCounters` 不受影响)。裁定:冷路径 raw → 5m → 1h → 1d,与热路径同构。

**E. 账单不需要改异步。** §4.9 要求长账期计算与导出走 operation job。切读 interface 5m 后,30 天单端口 = 30 × 288 × (端口×方向×层) ≈ 40 万行以内,毫秒级;现有同步 API 与 120 s 上限完全够用。异步只对 raw 证据源(迁移期旧账期)有意义,不进本轮。400 天上限保留。

**F. raw 5 分钟时间聚簇不是"重型 P3",现在就便宜。** §4.4/FL5M-12 把排序键 `toStartOfHour → toStartOfFiveMinutes` 当作影子表大迁移。事实:raw 保留只有 1–3 天(live 一个分区 21 GiB),重建 = 新表 + 复制 1–3 个日分区(小时级)+ 日边界切换;这正是 akvorado 的 raw 主键设计(`migrations_helpers.go:437`),也是 live 证明的最大查询杠杆:5 分钟窗口 684K 行实读 6.93M 行(整小时)。收益链:子小时 explorer 与 hybrid 尾段读放大 ÷12 → hot rollup 可改为**每 5 分钟发布** 5m/1m marker → raw 尾段 ≤10 分钟 → 11.2-C 的门禁才可能成立。顺序:① `ADD INDEX flow_event_time_minmax event_time TYPE minmax GRANULARITY 1`(元数据,新 part 生效,`EXPLAIN indexes=1` 验证);② 若裁剪成立且不足,再做排序键重建。提级到 021 之后的 P1。

**G. §1 第 6 条事实修正。** "默认配置中 hot rollup 未启用"只对 Go 默认值成立(`config.go:252` `Enabled:false`);仓库 `config/watchdog.yaml` 与生产都是 `enabled: true`,生产 1m/1h marker 连续(09-22 1440/1440、24/24)。reconciliation 未启用属实。表述改为"代码默认关闭、部署已启用"。

**H. 其余小项。** §4.12"禁止逐桶 ALTER DELETE":稳态死代行只来自新旧代键集差异(top-N 换手、`minimum_generation` 再生),ReplacingMergeTree 合并会折叠同键旧代;清理就是**每分区一条**带 `generation < 该桶 marker 最大代` 谓词的 DELETE,在再生事件后触发,不是逐桶。§4.4"FINAL 替代实测"移出(存储审查修订 #3)。§5.2"1m 调 7d"不做:有 90 天 5m 后 1m 只服务 ≤2 天的分钟级视图。分区宽度不再 benchmark:aggregate 5m 按日(与 1m 同)、interface 5m 按月(与归档月同)。

### 11.3 四个澄清

**(1)"取整由 ClickHouse 函数完成"指什么?5 分钟内取最大值吗?** 不是最大值。"取整"是**时间戳落桶**:每条 raw 记录的 `event_time` 用 `toStartOfFiveMinutes(event_time)`(1m 用 `toStartOfMinute`,1h 用 `toStartOfHour`,查询侧用 `toStartOfInterval(event_time, INTERVAL n second, 'UTC')`)映射到桶起点,然后 `GROUP BY 桶 + 维度` 对 `raw_bytes / raw_packets / estimated_bytes / estimated_packets / count()` **求和**。5 分钟桶的值 = 桶内所有记录的字节之和;速率 = 和 × 8 ÷ 300(`flowch/billing.go:187`);95th = 账期内全部 5 分钟速率样本排序后取第 ⌈0.95·n⌉ 个(nearest-rank,`billing/compute.go:113`);日 95 = 按账户时区逐日 nearest-rank 再平均(`compute.go:74-96`);月均 = 样本均值。小时 = 12 个 5 分钟桶求和,日 = 24 个小时求和,都是加法量,精确无损。**最大值在任何层都不出现**;整个链路只有 SNMP 侧的 reset/gap 标志用 `max()` 取"是否出现过"。另外查询侧还有一次"取整":请求的 `from/to` 被截到所选层的桶边界(见 (4))。

**(2)核心目标与本文的一致性。** 本文 §1.1–1.4 的原则(倒金字塔保留、可配置 TTL、约 50 分区、整 part 删除、时间取整排序、按分辨率路由;不复制 insert-trigger MV)与目标一致;偏离目标的是 §4.6-A/§4.9/§4.12/§4.13/§4.14 把生命周期运维闭环(hold、receipt summary、备份证据、readiness UI)绑进来——已在 11.1 移出。对"降低 raw 容量"目标,必须说清机制:akvorado 靠 raw 表 TTL 15 天;Watchdog 按已确认的约束**不加无条件 raw TTL**,等价物是**发布 policy(`raw_retention_seconds` 例如 3 天)+ 5m/1h 覆盖 + 守恒 + 审批后 `DROP PARTITION`**——生产 policy 至今为 0 条,所以今天 raw 的实际保留期是"用户手工 DROP"。5m 层落地后发布 policy 是 raw 容量目标的真正落点;列 codec(已应用)、5 分钟排序键(11.2-F)与它叠加。

**(3)什么是端点查询 / 非端点查询?** 按 `flowquery/query.go:95-113` 的维度枚举:**端点维度 = `src_ip`、`dst_ip`、`remote_port`** 三种,基数接近 raw(昨日 1h 最新代 471,589 行中占 412,719 行);聚合层只保留每组 **top-N**(IP 1000、端口 256,`rollup.go:853-858`),长尾折进 `_other`。**非端点维度 = 其余全部**:`total`、`direction`、`category`(六类)、`geo.continent/region/country/province/city`、`isp`、`asn`、`business`、`local_prefix`、`remote_prefix`、`address_set`、`protocol`、`observation_interface`——低基数,每层完整保留,任何范围都能由聚合层精确回答。"端点查询"= 维度或过滤落在三种端点维度上、且需要**精确值或全量排名**的查询:显式 IP/端口过滤(`flow_archive.go:100` remote_port 显式值强制 raw)、源/目的 IP 分析报表的候选阶段(`storageV2RawEndpointQuerySQL` 的 topKWeighted 近似路径,响应标 `approximate`)。注意端点 **top-N 图表**仍可走 1h 聚合(v13 的 `CompileEndpointRollupJoint` 在 1h marker 完整时用 1h 表做端点×分类关联);只有"精确到某个 IP/端口"或"超出保留 top-N"才必须 raw。所以 5m-EAV 表剔除三种端点 kind 不损失非端点查询,也不改变端点查询今天的路径。

**(4)时间段整除与首尾桶。** 结论成立,且比设想的更干净:前端预设窗口(`flow-report-model.ts:285-320`、`flow-explorer-model.ts:189-207`)用 `floorMinute(now)` 作为 `to`,`from = to − 时长`,自定义范围两端也 `floorMinute`;服务端 `PlanAggregate`(`plan.go:105-106`)再把两端**向下截到所选层的桶边界**(`EffectiveFrom = from.Truncate(step)`、`EffectiveTo = to.Truncate(step)`)。于是:预设窗口 = **整体平移到桶边界的完整窗口**(24h 仍是 24h,只是起止各早了同样的秒数);自定义窗口 = 头部多包含最多一个桶宽、尾部少包含最多一个桶宽——桶宽是**所选层**的步长:1m 层差 <1 分钟,5m 层 <5 分钟,**1h 层最多 59 分钟**(例如 09:30–18:45 在 1h 层变成 09:00–18:00)。响应里 `requested_from/to` 与 `effective_from/to` 并列返回,前端可以显示对齐结果。这也是 §8.2 #6 放弃"用更细层补齐首尾桶"的理由:只影响自定义范围,补齐要打破单切点模型,收益是边缘桶,不值得。若产品要求自定义范围尾部"包含最后一个不完整小时",正确做法是把尾部改为读 5m/1m 层的整桶(它们都在 1h 内),而不是拼接 raw——放在 11.2-F 之后评估。

### 11.4 本轮完成定义(替代 §9)

- [x] 021 两表存在;aggregate 5m 有 90 天 TTL、日分区;interface 5m 月分区、无 TTL、含 provenance 列（2026-09-28 复核：已完成 — `423d820e2`; `deploy/migration/clickhouse/021_flow_atomic_5m.sql:25-68`；代码已满足；生产未应用 021（仅手工建了 event_time 索引））
- [ ] 热路径:每个封闭小时产出 5m-EAV(从 1m)与 interface 5m(从 raw),两套 marker 连续;每小时额外成本 ≤ 3 s（2026-09-28 复核：部分完成 — 5m-EAV 热路径已提交未部署；interface 5m 缺；5m 不随 1m 迟到 repair 重建；≤3 s 未线上实测（dev 0.17 s）；`d6c38b62e`; `internal/server/flow_hot_rollup.go:91-141`）
- [ ] 冷路径(policy 发布后):raw → 5m → 1h → 1d,守恒门禁不变;raw 删除 readiness 多一个 blocker `five_minute_coverage_missing`
- [ ] planner:`[5m,1h)` 非端点走 5m,所有入口一致;端点维度不进 5m;响应带 source/coverage
- [ ] 账单:新账期 `evidence_source=interface_5m_v1`,30 天账期读行数与桶数同阶,影子对比 95th 容差 0
- [ ] 保留:policy → MODIFY TTL(1m/5m);receipts 45d、counters 90d TTL;死代清理作业;legacy 表已 DROP
- [ ] raw:`event_time` minmax 索引实验有 EXPLAIN/read_rows 前后对比;5 分钟排序键重建有独立提交（2026-09-28 复核：部分完成 — 索引前后对比已在线上完成（757→122 granules，≈6×）；5 分钟排序键重建未做；`423d820e2`; `deploy/migration/clickhouse/022_flow_records_event_time_index.sql:14`）
- [ ] `query_log`:非端点 24h/7d/30d 报表 raw 读行数占比 <5%,hybrid P95 <1 s;30 天账单不读 raw

---

## 12. 执行进度(loop · 2026-09-25 起)

目标:存得快 / 存得准 / 查得快 / 容量精简。每次迭代交付一个**可验证增量**,不改写线协议、不动生产,直到迭代 0 与并行协调解除。

### 迭代 0(前置,阻塞中):恢复摄入 —— 归属用户
生产根盘 100% 满,摄入停摆,09-24 rollup 卡在 14/24。需先在宿主释放空间(`tune2fs -m 1 /dev/vda1`,或删 `/opt/watchdog/releases` 旧副本)再 DROP 已完整聚合的 09-23。该步为主机写入,安全策略拦截,须用户执行。**四个目标在摄入恢复前都不成立。** 09-24 未补到 24/24 前不得删其 raw。

### 迭代 1(已完成并在 dev CH 验证):FL5M-01 schema + event_time 索引
- 新增 `deploy/migration/clickhouse/021_flow_atomic_5m.sql`:`flow_interface_traffic_5m`(计费/接口证据;月分区;**无 DDL TTL**,走归档月状态机;含 provenance 数组 + observed/known;§11.2-A)+ `flow_aggregate_5m`(非端点查询层;`AS flow_aggregate_1h`;日分区;`TTL 90 DAY` + `ttl_only_drop_parts=1`;§11.2-B)。
- 新增 `022_flow_records_event_time_index.sql`:`flow_event_time_minmax`(§11.2-F;生产实测子小时 6x 裁剪,已手工建过,`IF NOT EXISTS` 幂等)。
- 更新 `migrations_test.go`(20→22)与 `schema_test.go`(表数 14→16 + 锁定新表契约);`go build ./...` 通过,migration/schema 单测通过。
- **dev ClickHouse 26.3 实测**:两表 DDL 正确执行;Enum(value/generation、na/in/out、na/raw/supplier/customer)、Array(snapshot/geo/classification)、value+marker 行、ReplacingMergeTree FINAL、索引注册全部通过;验证后 DROP 清理,dev 保持干净。
- 未落生产:021/022 待迭代 0 解除后随发布应用。

### 后续迭代(有序 + 门禁)
- **迭代 2 · FL5M-02**:5m rollup runner + 双 marker(5m-EAV 从 1m 派生用 `toStartOfFiveMinutes`;interface-5m 从 raw,层展开 + provenance);扩 `RollupResolution`/`rollupTarget`/stats/Validate。dev CH 可验证,不需生产。
- **迭代 3 · FL5M-03**:接入现有 `flowHotRollupScheduler`,复用 `hour_lookback/hour_late_arrival_window/repair_interval`,不新增配置键。
- **迭代 4 · FL5M-05**:`PlanAggregate` 加 5m,`[5m,1h)` 非端点走 5m,端点维度排除;Explorer/报表/direction split/导出统一;响应带 source_tier/coverage。建立在已提交的 `flow_reports.go`/`rollup_joint.go` 上。
- **迭代 5 · FL5M-06**:`flowch/billing.go` 读 `flow_interface_traffic_5m`(镜像 `snmpch/billing.go`);账期冻结 `evidence_source`;影子对比 95th 容差 0。
- **迭代 6 · FL5M-07**:`interface_reconciliation.go` 的 flow 腿改读 interface_5m(否则 1e8 行预算只够 12.5h)。
- **迭代 7 · FL5M-08**:发布 policy(`raw_delete_enabled=false`)+ `MODIFY TTL`(1m/5m)+ 死代清理作业 + receipts 45d/counters 90d TTL + DROP 空 legacy 表;通过后开 raw 删除,raw 删除 readiness 增 `five_minute_coverage_missing` blocker。
- 端到端四目标验收在生产摄入恢复后用 `query_log`(read rows/bytes、P95/P99、资源超限率)量化:24h/7d/30d 非端点覆盖前缀零 raw、30 天账单不扫 raw。

### 迭代 2(已完成,dev CH 端到端验证):FL5M-02 五分钟 rollup runner(EAV 从 1m)
- `internal/flowch/rollup.go`:新增 `RollupFiveMinute`;`rollupTarget`→`flow_aggregate_5m`(5min);`stats[4]`+`RollupStats.FiveMinute`+`rollupStatsTarget`;`ValidateRollupRequest` 要求 5m 必须 `SourceResolution=1m`(禁 5m-from-raw 落入 raw 扫描分支)并支持 5m 批范围(≤1 天、5m 对齐);`buildRollupQuery` 5m 分支→新常量 `derivedFiveMinuteRollupSQL`(从 1m 派生,`toStartOfFiveMinutes` 落桶,marker 门控最新代,**排除 src_ip/dst_ip/remote_port**,嵌套子查询 + 显式 ON join 规避 bucket 别名与 USING 键冲突)。
- 复用既有两阶段:数据 INSERT 成功后 `generationMarkerRollupSQL`(bucket_seconds=300)发 marker。
- 测试:`rollup_test.go` 新增 5m 构建/校验用例;既有 reject 用例满足;`go build ./...` + flowch 单测全绿。
- **dev ClickHouse 26.3 端到端**:seed 两个 1m 桶(total/category/src_ip + marker)→ 跑 5m rollup → `flow_aggregate_5m` 得 total/category 各 raw_bytes=300/received=12(精确求和),src_ip 被排除(端点行=0),5m marker=1;验证后清理。
- 未接调度/未落生产:迭代 3(热调度接入)接线后随发布上线。interface-5m runner 为迭代 2b(端点证据表,从 raw,独立 row_kind marker)。

### 迭代 3(已完成,单测):FL5M-03 5m 接入热调度
- `internal/server/flow_hot_rollup.go`:`ScanOnce` 在 minute→hour 之后新增 `scanFiveMinute`;它对 `[now-seal_delay-hour_lookback, now-seal_delay)` 的每个封闭小时,当该小时 5m 未覆盖且 1m 已完整(两次 `CoveredThroughAtLeast`)时,发一条 `RollupFiveMinute`(source=1m,Bucket=hour,BucketEnd=hour+1h)→ 一次产出 12 个 5m 桶 + marker;**永不扫 raw**(1m 已 TTL 的老小时留给冷生命周期);**不新增配置键**(复用 hour_lookback / MaxHourBucketsPerRun / seal_delay / MinimumGeneration)。
- 生成代:`nextHotRollupGeneration(now,0)`=now-unix(热命名空间),自然 > 任何旧代 → 低于可读 floor 的旧代被取代。
- 测试:`flow_hot_rollup_test.go` 新增两例(1m 完整→发 1 条 5m;1m 不完整→跳过);server/flowch/flowquery 全套 `go test` 通过,build 干净。
- 未落生产:随发布上线;上线后热层将自动为每个封闭小时产出 5m(dev 实测单小时派生 ~0.17s)。**注意**:hour_lookback(生产 24h)须 ≤ 1m TTL(2 天)才能全程从 1m 派生,否则超窗小时的 5m 待冷路径(迭代 4/FL5M-04)。

### 部署文档
- `docs/watchdog-install.md` 新增「ClickHouse storage tiers, capacity and retention」:列出 raw+1m/5m/1h/1d+interface-5m+event_time 索引及其分区/TTL;写明迁移随 `install.go` 自动应用;点明**容量真相**(raw 无自动保留、~15GiB/天、须 vda3 存储策略 + 发布保留策略,否则手工汰换)。

---

## 与代码的差异（2026-09-28 复核）

2026-09-28 全项目复核将本文与当前代码/迁移/提交逐条对照，下列各处设计已被实现取代、改名或尚未实现。**以代码为准**；正文保留作设计历史，未逐句改写。

- **§1/§4.5（:15-16,203）**：“不存在 021、执行器只支持 1m/1h/1d、无 5m runner”已被 §12 迭代 1–3 取代：`021`/`022`、RollupFiveMinute、scanFiveMinute 已接入热调度。
- **hour_lookback（:801）**：仓库默认与示例配置为 72h（> 1m TTL 48h），48–72h 前的小时永远补不出 5m——需统一为 ≤48h 或由冷路径补齐。
- **interface 5m 回收（:728）**：DropArchiveMonth 只删 `flow_aggregate_1d`/`1h`；`021` 注释说已接入，实际未实现。
- **冷路径与 blocker（:734,758）**：归档仍 raw→1h→1d，无 `five_minute_coverage_missing` blocker。
- **未实现（:718,738,759,761）**：planner 对 [5m,1h) 走 5m、每 5 分钟发布、receipts/counters TTL、MODIFY TTL、死代清理均未实现（热层仍按封闭小时发布）。
- **interface-5m runner 与账期 evidence_source（:716,795）**：均未实现，billing 仍扫 `flow_records FINAL`。
- **ErrDataLoss（:136）**：已计数并在 worker run summary 上报 `kafka_data_loss`（可见），但未进入水位/删除 blocker（未阻断）。

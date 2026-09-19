# Flow / Flow-worker 聚合·分维度查询·索引·存储 性能效率审计

日期：2026-09-19　范围：`internal/flowch`（CH DDL/写入/rollup/对账/重分类）、`internal/flowquery`（查询编译）、`internal/flowworker` + `internal/flowdimension`（写路径/分类）、`cmd/watchdog-flow-{collect,worker}`、`internal/server/flow_*`（报表编排）。
方法：只读代码审计（存储 schema / 查询层 / 写路径+聚合）并准备生产只读测量；结论区分“代码已证实”“运行待证实”和“方案待压测”，不把估算写成生产事实。**本审计不修改生产代码、表结构或运行配置。**
对照基线：`docs/flow-storage-v2-change-plan.md`（唯一生效存储契约）、`docs/flow-reliability-remediation.md`（F 系列已修项）、`docs/flow-module-tasklist.md`（已登记未完成项）。

> 生产测量状态（2026-09-19）：SSH 存在间歇超时，但重试后已从 `103.83.64.18:15533` 完成一轮 ClickHouse 只读采样（24.9.2.42），结果见 §8。已取得表/part、查询读取量、插入批、重复坐标、投影和去重窗口证据；Kafka lag、硬件利用率、lifecycle/late-arrival/备份水位仍待补齐。任何 DDL 仍须先完成缺失基线与影子 A/B。

---

## 0. 决定一切的两个事实

1. **启用 lifecycle 时 Storage V2 生效，且只存在 1h 归档。** `applyFlowStorageBoundary`（[flow_archive.go](../internal/server/flow_archive.go)）仅在 `flowLifecycle != nil` 时置 `StorageV2=true`；唯一 rollup 生产者是按**已对账 UTC 日**运行的归档 job（[flowlifecycle/archive.go](../internal/flowlifecycle/archive.go) `RollupOneHour`）。1m 在生产**没有写入方**。后果：
   - 所有被规划为 1m 源的查询（自动规划通常为 `<24h`，以及 `<1h` 的显式步长）**从头到尾读原始 `flow_records`**；自动规划在 `>=24h` 时已强制使用 1h 源，超过 10,080 个分钟源桶也会自动降到 1h；显式且无法承载的分钟步长会被拒绝（[plan.go](../internal/flowquery/plan.go)、[flow_archive.go](../internal/server/flow_archive.go)）；
   - 1h 源计划只对已对账日读 `flow_aggregate_1h`，**今天及未对账日一律读 raw**（`ArchiveThrough`，[flowlifecycle/archive.go:440-482](../internal/flowlifecycle/archive.go)）；
   - 归档日的准入条件是 `day + 24h + max(RawRetention, LateArrival)`（[lifecycle.go:107-121](../internal/flowlifecycle/lifecycle.go)）——即**原始保留期整体过完之后**才聚合。1h 归档是**冷归档，不是查询加速层**。
2. **`flow_records` 没有 data-skipping index。** 当前迁移里 `ADD INDEX` 只用于 `snmp_events`；`flow_records` 只有 Kafka 坐标投影 `flow_ingest_audit_v2`（[011_flow_storage_v2.sql](../deploy/migration/clickhouse/011_flow_storage_v2.sql)）。排序键 `(toStartOfHour(event_time), source_stream_id, kafka_partition, kafka_offset, record_index)` 能裁时间和坐标前缀，但 IP、设备、端口、运营商等谓词没有独立跳数索引。是否“全扫”仍须以 `EXPLAIN indexes=1` 和 `query_log.read_rows` 证明，不能只凭 DDL 下结论。

这两点是下面 7 个根因中至少 4 个的共同上游。

---

## 1. 执行摘要（按优先级）

| # | 根因 | 严重度 | 一句话 |
|---|---|---|---|
| R5.1 | `flow_ingest_receipts` 无生命周期清理，事件日查询与写入月分区不对齐 | **HIGH（长期增长待测）** | 当前压缩磁盘不大，但表会持续增长；删除就绪度查询可能随保留月数增大 |
| R1 | 没有热窗口聚合层：未归档范围从 raw `GROUP BY … FINAL` | **HIGH** | 24h 默认虽已走 1h 计划，但未对账的近期部分仍现场聚合；显式分钟计划可能触发护栏 |
| R3 | `flow_records` 零 skip index + 护栏 fail-closed | **HIGH** | 单 IP 24h 明细一旦窗口 >5M 行直接 `QUERY_RANGE_LIMIT`，功能从"慢"退化成"不可用" |
| R2 | 插入去重闭环未证明；rebalance 回拨百万消息仍进入处理器 | **HIGH（线上已有重复）** | 生产非复制去重窗口为 0；2026-09-17 物理行比 `FINAL` 多 2,393,402 行 |
| R4 | 查询形状把最重扫描乘以 N | **HIGH** | 境外 CTE 多次引用、top-N 全局窗口排序；方向面板通过两条查询合并，端点报表分块追加查询 |
| R6 | fetch 处理与同步写入耦合，块大小受单次 poll 限制 | **HIGH（线上已证实）** | 每分钟约 60 次 facts + 60 次 receipts INSERT；事实块 P95 1,694 行，远低于 50k 上限 |
| R7 | 对账/修复/重分类扫描裁剪不足 | **HIGH/MED** | 实测同一坐标窗 `FINAL` 读 314,037 行，投影路径读 8,192 行；修复整日重算；重分类每页重做全窗 FINAL |
| C1 | **正确性缺陷**：`vpn_candidate.go` 仍读没有生产写入方的 `flow_aggregate_1m` | 功能 | 新装/空表为 0；若残留旧行则为过期值，均不能表示 V2 当前完整度 |

已做对的事见 §7；已登记未完成项（不重复登记）：`异步联合索引`（2–4 维联合 >24h）、固定硬件压测/72h soak（[flow-module-tasklist.md:284,479,488](flow-module-tasklist.md)）。

---

## 2. 发现台账

严重度：**C** critical / **H** high / **M** medium / **L** low。同一问题从多路审计看到时合并并注明来源（S=存储、Q=查询、W=写路径/聚合）。

### R1　热窗口聚合缺失（设计选择的代价，代码忠实实现了它）

| ID | 严 | 发现 | 证据 | 影响 | 建议 |
|---|---|---|---|---|---|
| R1.1 | H | 1m 源查询和 1h 计划中的未归档后缀在 raw 上做 11 键 `GROUP BY` + `FINAL` | [plan.go](../internal/flowquery/plan.go)、[flow_archive.go](../internal/server/flow_archive.go)、`storageV2QuerySQL` [query.go](../internal/flowquery/query.go)、§8 | Explorer/dashboard 的近期部分重复聚合原始事实；线上多类产品查询单次读取 3,500 万～4,740 万行、约 1.2～2.3GiB，最慢一类 P95 已达 10.3s | 先把重复报表查询合并、过滤下推并测量；若仍不达标，再单独设计“热加速层”。不得直接用 `generation=1` 复用归档表：它会改变 Storage V2 的代际、迟到、守恒和删除语义，必须另立 ADR、迁移和 crash/late-arrival 测试 |
| R1.2 | H | 1h 归档只在 `day+24h+max(raw_retention, late_arrival)` 后生成，是生命周期归档而非热查询缓存 | [lifecycle.go](../internal/flowlifecycle/lifecycle.go)、[archive_scheduler.go](../internal/flowlifecycle/archive_scheduler.go) | raw retention 较长时，近期后缀持续从 raw 查询 | 保持归档与加速职责分离；是否新增热层取决于 Phase 0 的 read_rows/P95，而不是先改生命周期 |
| R1.3 | M | 1h 从 raw 算而非从 1m；每行 `ARRAY JOIN` 展开 17 个 (kind,value) 元组 + 地址集合；`row_number() OVER (PARTITION BY 10 列)` 对**全部** kind 求值（`if(kind IN(...))` 只包裹结果不缩小窗口） | [rollup.go:641-650, 693-699, 724-745](../internal/flowch/rollup.go)（S-m12, W-A3） | 整个分组结果在内存排序；只设了 external group-by（4GiB）没设 external sort | 仅对 `src_ip/dst_ip` 分支做排名，非 IP kind 走 `UNION ALL` 不带窗口；补 `max_bytes_before_external_sort` |

### R2　`FINAL` 广泛存在，插入去重闭环尚未证明

| ID | 严 | 发现 | 证据 | 影响 | 建议 |
|---|---|---|---|---|---|
| R2.1 | H | 代码发送 `insert_deduplication_token`，但仓库没有设置非复制 MergeTree 的 dedup window；生产 `non_replicated_deduplication_window=0` | [native.go](../internal/flowch/native.go)、[011_flow_storage_v2.sql](../deploy/migration/clickhouse/011_flow_storage_v2.sql)、§8 | 当前 token 不提供块级去重；线上 2026-09-17 分区已有 2,393,402 个 merge 前物理重复行。即使 window>0，也只覆盖窗口内、token 相同的块 | 先完成确定性块与 retry/rebalance 故障测试。若启用窗口，容量必须按峰值 insert block 率×最长重试时间计算；不能拍脑袋固定为 1000，更不能因此立即删除 `FINAL` |
| R2.2 | H | 去重 token 编码当前块边界 `flow-v2:stream:part:firstOff:firstIdx:lastOff:lastIdx:n:m` | [native.go](../internal/flowch/native.go) | crash/rebalance 后若批边界变化，token 变化，块级去重不能收敛；自然坐标仍可由 ReplacingMergeTree+`FINAL` 收敛 | 先用故障集成测试证明边界是否稳定；若不稳定，按 Kafka offset/record_index 建确定性切块，或保留坐标级读时去重 |
| R2.3 | H | 每次 partition assignment 把 fetch offset 回拨 `TemplateReplayRecords`（默认 1,000,000），**全部记录**照常解码+分类+写入，只是不提交 offset | [consumer.go:141-143, 319, 326-328](../internal/flowstream/consumer.go)、[main.go:111](../cmd/watchdog-flow-worker/main.go)（W-W1） | 每次重启/rebalance 每分区最多重写 1M datagram × records；token 不匹配 → 新 part，直到 merge 前所有查询多付 FINAL | 重放窗口只做 decode（模板是唯一需要的状态），floor 以下跳过 `handleGroup`；或持久化模板状态、把窗口降到数千条 |
| R2.4 | M | 聚合读 `FINAL` + latest-generation JOIN；raw 读普遍使用 `FINAL` | [query.go](../internal/flowquery/query.go)、[detail.go](../internal/flowquery/detail.go)、[joint.go](../internal/flowquery/joint.go)、[overseas.go](../internal/flowquery/overseas.go)、[010_flow_aggregate_reorder.sql](../deploy/migration/clickhouse/010_flow_aggregate_reorder.sql) | 查询 CPU/内存增加，但当前它也是至少一次写入正确性的组成部分 | 只有在确定性块、dedup window 容量、超窗重试、rebalance/crash 测试和重复率监控全部通过后，才可按路径试验去 `FINAL`；`do_not_merge_across_partitions_select_final=1` 可独立基准验证 |

### R3　`flow_records` 无二级访问路径

| ID | 严 | 发现 | 证据 | 影响 | 建议 |
|---|---|---|---|---|---|
| R3.1 | H | 零 skip index。明细 IP、账单设备/端口、运营商和 VPN 候选都过滤非排序键列 | [detail.go](../internal/flowquery/detail.go)、[billing.go](../internal/flowch/billing.go)、[flow_operator_query.go](../internal/server/flow_operator_query.go)、[vpn_candidate.go](../internal/flowch/vpn_candidate.go) | 在高基数、低局部性的列上，盲加 bloom/set 可能几乎不裁 granule却持续增加写入、merge 和磁盘成本 | 先从真实慢查询选 1–2 个候选：IP 等值查询比较 bloom；device+time 比较 projection；用同一生产样本记录 `read_rows/read_bytes/P95/insert rows·s⁻¹/part 大小`。只有读收益显著且写回退达标才迁移和 materialize |
| R3.2 | H | 护栏 fail-closed：明细/facet/地址集合 5M 行，聚合/联合/境外 50M 行 | [detail.go](../internal/flowquery/detail.go)、[detail_facet.go](../internal/flowquery/detail_facet.go)、[address_set.go](../internal/flowquery/address_set.go)、[query.go](../internal/flowquery/query.go)、§8 | 查询会明确失败而不是拖垮 CH；当前用户已实际遇到 resource-limit，且线上多条产品查询已读 3,500 万～4,740 万行，逼近 50M 上限 | 保留护栏；先优化访问路径和异步导出，不以单纯调大常量掩盖问题。同步查询、图表和批量导出必须使用不同预算 |
| R3.3 | L | 列过滤用 `toUInt64(col) IN(...)` / `CAST(col AS String) IN(...)` | [detail_column_filter.go:58-72](../internal/flowquery/detail_column_filter.go)（Q-F19） | Enum/LowCardinality 逐行转 String；未来 skip index 被绕过 | 原生比较：`col IN ({p:UInt16})`、枚举字面量 |
| R3.4 | M | 查询编译器把交互式预算写成字面量：detail/facet/address-set 为 10s、5M rows、1GiB read；query/joint/overseas 为 15s、50M rows、4GiB read/memory；TopN/过滤值上限也为编译期常量 | [detail.go](../internal/flowquery/detail.go)、[detail_facet.go](../internal/flowquery/detail_facet.go)、[address_set.go](../internal/flowquery/address_set.go)、[query.go](../internal/flowquery/query.go)、[joint.go](../internal/flowquery/joint.go)、[overseas.go](../internal/flowquery/overseas.go) | 安全上限本身必要，但无法按硬件/工作负载调优，且交互查询与长时间批量任务没有独立资源类别；直接改常量容易把超时从“可见失败”变成集群争抢 | 保留服务端硬上限，集中为少量命名预算档（interactive/detail/export/maintenance），由受控配置选择且不得由客户端任意放大；长窗口/批量导出走 operation job，记录进度、取消、重试与产物，不占用 HTTP 请求生命周期 |

### R4　查询形状把最重扫描乘 N

| ID | 严 | 发现 | 证据 | 影响 | 建议 |
|---|---|---|---|---|---|
| R4.1 | H（执行倍数待测） | 境外查询的 `selected/geo_source` CTE 被多次引用，存在重复展开/执行风险 | [overseas.go](../internal/flowquery/overseas.go) | 代码形状明确复杂，但“恰好执行 4 次”和“成本恰好 4×”取决于部署版本分析器，尚不能当生产事实 | 先用 `EXPLAIN PIPELINE` 与相同参数的 `query_log.read_rows` 证明；再改为单遍条件聚合或受控中间结果，禁止直接引入临时表状态泄漏 |
| R4.2 | H | 境外 raw 分支 `ARRAY JOIN [src_ip, dst_ip, geo]` 三倍行、按 `toString(src_ip/dst_ip)` 分组到 IP 基数；`remote/local_expanded` 再 ×4；`uniqExact(endpoint_ip)` 按 (bucket,dir,family,versions)；`/flow/overseas/query` 没有额外的产品级窗口限制（查询规划最多允许 9,600 小时源桶） | [overseas.go:280-296, 344-355, 362, 377](../internal/flowquery/overseas.go)、[handlers_flow.go:304-343](../internal/server/handlers_flow.go)（Q-F3） | 内存随 distinct IP、方向/地址族派生组放大，可能触发 4GiB 查询内存护栏；实际峰值待测 | 先按 (bucket,dir,family) 聚合一次，`all/combined` 用 `GROUPING SETS`/对聚合行二次小 GROUP BY 派生；按原生 `IPv6` 分组、外层再 toString。产品若允许近似去重，再以误差测试决定是否把 `uniqExact` 换成近似算法，不能作为纯性能改动直接替换 |
| R4.3 | H | top-N+`_other` 用 `sum() OVER (PARTITION BY dims)` + `dense_rank() OVER (ORDER BY …)` 作用于**全部**分组行；无 external sort | [joint.go:411-429](../internal/flowquery/joint.go)、[query.go:774-792](../internal/flowquery/query.go)（Q-F4, W-A3） | 全局 ORDER BY 窗口 = 单流排序物化 (#桶 × #维值 × #版本) 行后才截 top_n | 两段式：`top AS (SELECT dims … ORDER BY sum(metric) DESC LIMIT top_n)` 再 `IN top` 打标（legacy 模板即此形状），或 `LIMIT top_n BY`；补 `max_bytes_before_external_sort` |
| R4.4 | M/H | 公共联合维度定义不接受 `direction`；服务端通过两条带 `filters.directions=in/out` 的查询合成方向结果，报表面板按顺序执行 | [joint.go](../internal/flowquery/joint.go)、[flow_reports.go](../internal/server/flow_reports.go)、[flow_query_modes.go](../internal/server/flow_query_modes.go) | 每个方向重复相同时间窗扫描；端点和境外面板进一步放大请求数。确切条数随报表面板而异，不能固定写成 28 | 把 `business_direction` 纳入受控联合维度并验证 raw/archive 同口径；随后用 `[direction, X]` 合并扫描。并发只能在单查询降本后加入，且需独立小信号量和请求总 deadline |
| R4.5 | M | 端点报表 N+1：按 `⌊100/TopN⌋` 分块 × 2 方向；TopN=100 → 20 次 ≤24h raw 联合扫；每次 `src_ip IN(…)` 是 20–100 项 OR 链 | [flow_report_endpoints.go:205-245, 277-291](../internal/server/flow_report_endpoints.go)、[filter.go:455-468](../internal/flowquery/filter.go)（Q-F7） | 一次报表最多 22 条 raw 扫描 | 有 `direction` 维后单条 `[src_ip, direction, category]` 联合查询替代 |
| R4.6 | M | V2 模板把调用方过滤（`business_direction/device_id…`）放在 `UNION ALL` **之后**的 `filtered/selected` CTE，而 `raw_rows/raw_selected` 的 GROUP BY 先对全窗执行 | [query.go:764-773](../internal/flowquery/query.go)、[overseas.go:298-306](../internal/flowquery/overseas.go)（Q-F12） | 除非优化器穿透 UNION ALL+GROUP BY（部分键是函数别名如 `toString(business_direction)`），否则设备过滤不缩小 raw 扫描——**这也是"按设备查仍像全量"的一个查询侧因素** | 把条件显式写进两个分支；`EXPLAIN` 验证 |
| R4.7 | M | 每个 facet 字段一条独立 GROUP BY；facet 设置缺 `max_memory_usage`；搜索用 `positionCaseInsensitiveUTF8(CAST(col AS String))` | [detail_facet.go:57-131, 121-127](../internal/flowquery/detail_facet.go)（Q-F9） | 打开 N 个 facet 会对同一受限 IP/时间 scope 发出 N 次扫描 | 一条查询多 facet（`arrayJoin` (field,value) + `LIMIT n BY field` 或每列 `topK`）；补内存护栏；缓存需遵守 R4.11 的完整 key 与失效条件 |
| R4.8 | M | supplier 明细每页先对全 scope 做 `min(fact_schema) OVER ()` 与 `row_number() OVER (ORDER BY …)` 再 cursor/LIMIT；导出循环翻页到尽 | [detail.go:937-947](../internal/flowquery/detail.go)、[flow_exports.go:565-613](../internal/server/flow_exports.go)（Q-F8） | O(页数 × N)，每页全排序 | `min(fact_schema)` 改标量子查询；保证行用 `UNION ALL` 单行 select；去掉 `row_number` |
| R4.9 | M | 峰段过滤逐行 `toTimeZone`+`toDayOfWeek`+`toHour*60+toMinute`，作用于非键列 | [time_window.go:34-66](../internal/flowquery/time_window.go)、[query.go:388-395](../internal/flowquery/query.go)、[joint.go:174-181](../internal/flowquery/joint.go)（Q-F10） | 无法用主键裁剪 | Go 侧展开成 UTC `[from,to)` 区间列表（≤8 窗 × ≤7 天）OR 到键列；聚合源用 `bucket IN(…)` |
| R4.10 | L/M | 先转字符串再聚合：联合按 `[toString(src_ip), toString(category)]`（Array(String) 键）、`CAST(dimension_snapshot_id AS String)` 逐 raw 行 | [joint.go:391-396](../internal/flowquery/joint.go)、[query.go:742-747](../internal/flowquery/query.go)、[overseas.go:263-269](../internal/flowquery/overseas.go)（Q-F16） | 逐行字符串物化、哈希更慢 | 按原生列元组分组，外层再转换 |
| R4.11 | L/M | 应用层没有结果缓存或 singleflight；相同 dashboard 刷新会重复发出相同请求 | [query.go](../internal/flowquery/query.go)、[joint.go](../internal/flowquery/joint.go) | 重复消耗 CH；但结果不一定完全不可变，迟到数据、repair generation 和权限/版本会改变结果 | 优先做 request singleflight；若缓存，key 必须含规范化 filter、view、权限 scope、分类/地址快照、archive boundary 和闭桶上限，并在新 generation 发布后失效；不能只开 CH query cache 即宣称正确 |
| R4.12 | L | `ArchiveThrough` 每个面板/方向/窗口各查一次 MySQL `flow_retention_partition_states` | [flow_archive.go:94](../internal/server/flow_archive.go)、[flowlifecycle/archive.go:450-451](../internal/flowlifecycle/archive.go)（Q-F14） | 总览 4×、端点 ≥3×、observed ⌈桶/12⌉× | 每请求解析一次并透传 |
| R4.13 | L | 护栏缺口：A(V2) 缺 `max_bytes_before_external_group_by`（联合有）；无路径设 `max_bytes_before_external_sort`；facet 缺 `max_memory_usage`；HTTP server 无 read/write timeout，多面板串行查询端到端无统一 deadline | [query.go:456-466](../internal/flowquery/query.go) vs [joint.go:216](../internal/flowquery/joint.go)（Q-F15） | 单条查询受限不等于整个请求有界；请求中断后的资源释放也缺少端到端门禁 | 补齐；给报表请求整体 deadline，并测试客户端取消向 CH 传播 |
| R4.14 | L | 地址集合成员判定 `hasAny(arrayDistinct(arrayConcat(local, remote)), […])` 逐行创建合并数组 | [address_set.go](../internal/flowquery/address_set.go) | 增加逐行 CPU/分配 | 改为两个 `hasAny` 条件组合并做等价测试；是否加数组 bloom 由 EXPLAIN/基准决定。不要直接改成 `Array(LowCardinality(String))`：兼容性、编码收益与迁移成本均未验证 |

### R5　无界增长 / 保留策略缺位

| ID | 严 | 发现 | 证据 | 影响 | 建议 |
|---|---|---|---|---|---|
| R5.1 | H | `flow_ingest_receipts` 每个可提交 Kafka 消息一行；无 TTL、无 DROP。`DayOffsetCoverage` 按事件时间相交查询，但表按 `inserted_at` 月分区，事件时间列无跳数索引 | [011_flow_storage_v2.sql](../deploy/migration/clickhouse/011_flow_storage_v2.sql)、[raw_delete.go](../internal/flowch/raw_delete.go)、[retention_coverage.go](../internal/flowch/retention_coverage.go)、§8 | 线上约 1,116 万行/47.44MiB，行数约为 facts 的 24.6%，但压缩磁盘仅约 1.7%；“第二大表”只对行数成立、不对磁盘成立。当前仅覆盖约 1.5 天，长期增长与删除就绪度成本仍未实测 | receipt 是删除与 Kafka 对账证据，不能用固定 N 天 TTL 直接删除。设计“receipt 月保留”应落成显式生命周期：只有该写入月覆盖的所有事件日均越过 reconciliation、raw-delete/restore 保护与审计保留水位，operation job 才 DROP 月分区；若先试 minmax 索引，必须用真实事件乱序度验证裁剪率 |
| R5.2 | H | 1h 归档基数无界：只有 `src_ip/dst_ip` 做 top-1000/组折叠；`remote_port`（≤65,536/组/小时）、`asn`、`remote_prefix`（地址库 ≈2.66M 段）、`local_prefix`、`geo.city`、`address_set` 按 raw 基数物化，且 top-N 是**按 9 列组**而非按桶；每次 repair 插入整代新 generation，旧代行永不物理删除（只在读时 `_generation` join 过滤） | [rollup.go:693-699, 724-745, 749-752](../internal/flowch/rollup.go)、[query.go:642-657](../internal/flowquery/query.go)（S-F4, W-A4） | 长期归档可逼近 raw 行数；每次读都为陈旧 generation 付 FINAL | 把 top-N 折叠扩展到 `remote_port/asn/remote_prefix/local_prefix/geo.city/address_set`（或归档不存 `remote_port`）；月封存后对 `generation < latest` 做 lightweight delete；先在 .18 按 kind 量行数（§6） |
| R5.3 | H | `flow_reclassified_records` 用 `CREATE … AS SELECT … WHERE 0` 建表，只复制列定义结果而不保证继承原表 codec/default/projection；每个重分类 generation 可复制大窗口，且未见淘汰路径 | [016_flow_historical_reclassification.sql](../deploy/migration/clickhouse/016_flow_historical_reclassification.sql)、[reclassification.go](../internal/flowch/reclassification.go) | 压缩与生命周期均存在明确风险；实际每行字节和代际增长待测 | 用显式 DDL 固定 codec/default；按重分类任务和月份建立可删除边界；任务成功替代/取消/过审计期后由 operation job 清理，不做同步大 mutation |
| R5.4 | H | `flow_records` 无 TTL；`DropRawDay` 只经 policy revision + 对账水位 + 备份证据 + 人工批准链到达；`flowLifecycle==nil` 时 V2 关闭 | [011:139-144](../deploy/migration/clickhouse/011_flow_storage_v2.sql)、[flowlifecycle/raw_delete.go:245,310](../internal/flowlifecycle/raw_delete.go)（S-F6, W-A9） | 未配策略或证据链卡住的部署**静默永久保留** raw；所有 FINAL 读、迟到复核（R7.2）、对账（R7.1）都随之线性变慢 | 保留审计删除；增加“未配置策略/最老分区年龄/水位停滞”告警，并让 operation job 真正驱动 archive/delete。不得再用静态 30/365/400 天 TTL 绕过业务保留、备份与恢复证据 |
| R5.5 | L | 迁移保留了 `*_legacy_*`；`flow_address_dict_source` 未见生产读取；`flow_aggregate_1m` 无生产写入方但仍被 VPN 覆盖率读取 | [011_flow_storage_v2.sql](../deploy/migration/clickhouse/011_flow_storage_v2.sql)、[009_flow_address_dict_source.sql](../deploy/migration/clickhouse/009_flow_address_dict_source.sql)、[flow-address-query-plan.md](flow-address-query-plan.md) | 磁盘与运维复杂度增加，但不能仅凭代码认定线上表存在或可删 | 先做线上引用、行数、备份与回滚窗口清单；C1 修复且回滚窗口关闭后再以独立可审计迁移 DROP |
| R5.6 | L/M（收益待测） | `flow_records` 很宽；部分 IPv6/序列/数组列未显式 codec；VPN/隔离/SNMP 表的 codec/TTL 不统一；单块允许跨 90 个事件日是故障回放保护值 | [011_flow_storage_v2.sql](../deploy/migration/clickhouse/011_flow_storage_v2.sql)、[003_flow_vpn_candidate_generation.sql](../deploy/migration/clickhouse/003_flow_vpn_candidate_generation.sql)、[015_flow_raw_delete_quarantine.sql](../deploy/migration/clickhouse/015_flow_raw_delete_quarantine.sql)、[batch.go](../internal/flowch/batch.go)、[012_snmp_telemetry.sql](../deploy/migration/clickhouse/012_snmp_telemetry.sql) | 可能增加压缩、inode 与 merge 成本，但列文件数量不是独立性能结论 | 用 `system.columns`/`system.parts_columns` 找真实大列，再逐列 A/B codec；90 日预算不能直接降到 7，否则长停机回放可能永久失败。supplier delta 化会改变可追溯事实模型，不属于纯性能改动 |

### R6　写路径

| ID | 严 | 发现 | 证据 | 影响 | 建议 |
|---|---|---|---|---|---|
| R6.1 | H | `processFetches` 每分区起 goroutine 后 `wait.Wait()`：下一次 `PollFetches` 必须等**所有**分区的同步 CH 写（含最多 2 分钟重试）返回；goroutine 数 = 分区数，但 `chpool MaxConns=8` | [consumer.go:306-353](../internal/flowstream/consumer.go)、[writer.go:169, 233-236](../internal/flowch/writer.go)、[main.go:126](../cmd/watchdog-flow-worker/main.go)（W-W2） | 单分区吞吐 ≈ (≤1MiB fetch)/(全体最慢插入延迟)；一个慢分区拖住全部 | 每分区 worker goroutine + 有界 channel（Akvorado 模型），成功后由 worker 标记 offset；revoke 时排空以保 `BlockRebalanceOnPoll` 语义 |
| R6.2 | H | 块不跨 poll；records 与 receipts 分两次同步 INSERT；投影随 part 维护。线上最近一小时两表各 3,594 次 INSERT（约 60/min），facts P95 1,694 行/块、35ms，receipts P95 437 行/块、6ms | [batch.go](../internal/flowch/batch.go)、[native.go](../internal/flowch/native.go)、[011_flow_storage_v2.sql](../deploy/migration/clickhouse/011_flow_storage_v2.sql)、§8 | 当前生产确实形成约 120 个新 part/min，随后由 merge 压到 `flow_records` 25 个、receipts 7 个 active part；50k 是上限而非实际批大小，写放大/merge 工作量已被实测证实 | 增加 worker rows/block、flush reason、lag 指标；仅在有界 per-partition accumulator 能证明 rebalance 排空、offset 提交和内存上限后，才允许跨 poll 合批，并用 parts/min 与 lag 验收 |
| R6.3 | M | 逐记录冗余：6 次地址区间查找可归 2 次（`ClassifyEndpoints` 2 + `ClassifyEndpointsForDirection` 2 + `ResolveAddress`×2）；`EnrichedRecord` 1000B 按值经两层返回再 append（3 次拷贝）；`EnrichBatch` 每 Kafka 消息新分配切片而缓冲复用版 `EnrichBatchInto` 无生产调用；`quarantineDecision` 对每条记录 `Guard.Covers`（拷 `ExceptionDays`+`Validate`+`Format(DateOnly)`）；地址解析两次；`Metadata()` 逐记录/逐二分步拷贝 | [enrich.go:203, 212, 242-244, 256-390](../internal/flowworker/enrich.go)、[address_snapshot_index.go:200-206, 216-217, 245-246](../internal/flowdimension/address_snapshot_index.go)、[pipeline.go:103, 137-144](../internal/flowch/pipeline.go)、[barrier.go:65-69, 173-174](../internal/flowtombstone/barrier.go)（W-W5） | 50k 记录/fetch ≈ 50MB 瞬时垃圾；CPU/GC | 每记录一对查找同时供方向/端点/Geo；barrier 日按 batch 算一次或比较 `Unix()/86400`；切到 `EnrichBatchInto` 每分区复用缓冲；`Record` 存 16 字节数组而非切片 |
| R6.4 | L/M | 每块重建 ~95 列（含 ~30 个 LowCardinality 字典）；`countryCode` 每调用分配（每记录 2 次）；隔离冷路径每 datagram 2 次同步 INSERT + 完整重试且在 fetch 屏障内；`MarkCommitRecords` 逐记录调用（每次取客户端锁）；`DecodeRecord` 每 Kafka 记录取全局互斥锁 | [native.go:250-347, 633-639](../internal/flowch/native.go)、[quarantine.go:74, 84](../internal/flowch/quarantine.go)、[consumer.go:330](../internal/flowstream/consumer.go)、[decoder.go:460-477](../internal/flowstream/decoder.go)（W-W6/7/8） | 跨 raw-delete barrier 的重放（尤其叠加 R2.3 回拨进已删日）变成数千次串行往返 | 列集按分区 worker 常驻 `Reset()`；国家码预编码；隔离行按分区 fetch 合批；`MarkCommitRecords` 传切片 |

### R7　对账 / 修复 / 重分类扫描

| ID | 严 | 发现 | 证据 | 影响 | 建议 |
|---|---|---|---|---|---|
| R7.1 | H | 对账 `readFactCounters` 使用 `FROM flow_records FINAL WHERE source_stream_id=… AND kafka_partition=… AND kafka_offset ∈ […]`，但**没有 `event_time` 范围** | [reconciliation_scanner.go:274-286, 322-327](../internal/flowch/reconciliation_scanner.go)、[ingest_audit_projection_integration_test.go:83, 133-149](../internal/flowch/ingest_audit_projection_integration_test.go)（S-F2, W-A5）、§8 | 24.9 实测：`FINAL` 走主表/通用排除，25 parts 中读 9、6,049 granules 中读 39；同一查询读 314,037 行/5.58MiB/30ms。去掉 `FINAL` 才选择 `flow_ingest_audit_v2`，读 1 granule、8,192 行/104KiB/9ms，约 38× 行数差 | receipt 分页结果携带 `min/max_event_time`，先给事实查询加时间边界；是否去 `FINAL` 服从 R2 的确定性切块、去重容量与故障测试，不能为了投影命中牺牲重复计数正确性 |
| R7.2 | M | 修复整日重算：从 `NextHour=0` 重跑 24 小时即使只变一小时；`BucketNeedsRepair/LatestGeneration/RecordReaperRepair/RecordPermanentGap/RecordTerminalFailure` **无生产调用**（F1/F2 reaper 已回滚）；迟到复核每 6h 对每个 `reconciled` 且未 `raw_deleted` 的日跑 `DayStorageCounters`（整日 raw FINAL 扫 + 归档 join），raw 删除未开启时**永远重扫** | [archive.go:57-82, 138-157, 370-391](../internal/flowlifecycle/archive.go)、[archive_scheduler.go:73-92](../internal/flowlifecycle/archive_scheduler.go)、[rollup.go:124-222, 275-315](../internal/flowch/rollup.go)（W-A2） | 修复成本 24× 必要值；复核成本随保留天数线性增长 | 用 `BucketNeedsRepair` 做逐小时比对与重算；迟到复核在 `LateArrivalSeconds` 过后停止，不等删除 |
| R7.3 | M/H | 重分类每 5k 行页重执行全窗 `FINAL` 查询 + 5 列元组游标（首列 `event_time` 只是键前缀的单调函数）；`CoordinateDiff` 4 次 FINAL 扫 + 2 次 `EXCEPT DISTINCT` 全坐标集驻留内存；证据求和 `toDecimal256` | [reclassification.go:19-22, 145-152, 429-449](../internal/flowch/reclassification.go)（W-A7） | 31 天窗口时代价极高 | 外层按 `toYYYYMMDD` 分区翻页；页大小向 50k 上限提；`EXCEPT DISTINCT` 改逐日 count/hash 比对 |

### C　顺带发现的正确性缺陷

| ID | 发现 | 证据 | 影响 |
|---|---|---|---|
| C1 | `flow_aggregate_1m` 在 V2 无生产写入方，但 `vpn_candidate.go` 仍从它算 `covered_buckets`（`FINAL … dimension_kind='_generation'`）；`flow_vpn_detection.go:62` 已接线。线上 1m/1h 两张聚合表当前均为 0 行 | [vpn_candidate.go:176-182, 232-234](../internal/flowch/vpn_candidate.go)、[flow_vpn_detection.go:62](../internal/server/flow_vpn_detection.go)（S-F7, W-A8）、§8 | 当前生产 `complete_ratio=0`；若未来残留旧 1m 行则会变成过期覆盖率。应改从 receipts/已关闭 raw 分钟计算分钟完整度；不能拿 1h generation 冒充分钟覆盖 |
| C2 | （风险）若在 R2.1/R2.2/R2.3 之前就去掉 `FINAL`，R2.3 的重放重复与重试重复会变成**重复计数** | — | 顺序必须是"先让去重真正生效，再撤 FINAL" |

---

## 3. 设计意图 vs 实现（差距清单）

| Storage V2 / 设计文档说 | 代码实际 | 结论 |
|---|---|---|
| 原始在线期直接查全精度事实；1m 不再持续物化；策略到龄才 1h 归档（storage-v2 §1、design §1.5） | 忠实实现（§0.1） | **线上代价已初步量化**：多类产品查询单次读取 3,500 万～4,740 万行（R1、§8）。先减少重复扫描、下推过滤并完成并发基线；热加速层仍不是既定结论 |
| 事实以最多 50,000 行大块写入（storage-v2 §2.3） | 块受单次 poll、50k 行、64MiB 和 90 事件日预算共同限制；单个 datagram 可展开多条 flow（R6.2） | 50k 是上限，不是批大小承诺；线上 facts P95 仅 1,694 行、两表合计约 120 个新 part/min，跨 poll 有界合批应列为明确优化项 |
| `flow_ingest_audit_v2` 投影服务对账窄扫描（design §3.4） | 对账没有事件时间边界并使用 `FINAL`；24.9 实测 `FINAL` 读主表 314,037 行，非 `FINAL` 走投影读 8,192 行（R7.1、§8） | 投影本身有效，但现有正确性语义阻止对账路径直接使用；先加事件时间边界，再完成去重闭环，最后评估去 `FINAL` |
| receipt 按写入月保留（storage-v2 §2.3） | 无 TTL、无 DROP（R5.1） | 未实现 |
| `ReplacingMergeTree` + insert token 收敛 at-least-once（design §1.4） | token 是否有效取决于生产 `non_replicated_deduplication_window`；当前 token 又依赖批边界（R2.1/R2.2） | 目前可确认自然坐标提供身份、`FINAL` 提供读时收敛；块级去重保证尚未闭环 |
| F7：聚合排序键 `(bucket, dimension_kind, …)` 前置 ✅ | 已落地，`dimension_kind`/`_generation`/`dimension_value IN` 均可裁 granule | 做对了；但 `target/device/exporter/direction/category/business` 排在高基数 `dimension_value` 之后无法裁剪（境外查询尤甚，R4.2） |
| F8：query/detail/address-set/rollup 内存护栏 ✅ | 已落地（§7）；仍缺 external sort、facet 内存、A(V2) external group-by（R4.13） | 部分 |
| F17：codec ✅ | 热列已加；`local/remote_ip`、序列号、`flow_reclassified_records` 整表遗漏（R5.3, R5.6） | 部分 |
| F6：src/dst IP top-N 折叠 ✅ | 仅这两个 kind，其余 kind 无界（R5.2） | 部分 |
| 六类互斥可相加、`unknown/internal/transit/ambiguous` 作残差（design §7） | 查询/rollup 一致 | 正确 |

---

## 4. 分阶段整改建议（先证据、后低风险收敛、再结构性变更）

> 本审计只给执行顺序与门禁，不把未压测的 DDL 直接列为“立即实施”。尤其禁止先批量加索引、固定去重窗口、删除 `FINAL`、添加静态 TTL，或把现有冷归档表兼作热层。

### Phase 0 · 强制基线（生产只读）

首轮 ClickHouse 基线已经执行并固化在 §8：CH 版本与去重设置、表/分区/part 规模、rows/block、parts/min、重复坐标、产品查询读取量以及 `FINAL`/投影对照均已取得。下一次结构性变更前仍须补齐 Kafka lag、worker/CH 主机 CPU/内存/磁盘/merge backlog 时间序列、并发 P95，以及 lifecycle/late-arrival/备份/删除水位。缺项未补齐前，不批准索引 materialize、去重窗口或热层方案。

验收：测量结果附在本文件或独立结果文件；每项候选优化都能指向一条基线查询及目标 SLO。

### Phase 1 · 不改变统计语义的查询修复

1. R4.6：把设备、方向、业务、版本等条件显式下推到 raw/archive 各分支，并以 `EXPLAIN indexes=1` 和 `read_rows` 回归。
2. R4.3：top-N 改两段式，补 external-sort/memory/deadline 护栏。
3. R4.4/R4.5：把方向纳入受控联合查询，一次扫描返回流入/流出；端点报表取消分块 N+1。
4. R4.1/R4.2：境外路径改成单遍聚合、原生 IPv6 分组和有界时间窗；保持 `uniqExact`，除非产品批准近似误差。
5. R4.7/R4.12/R4.13：合并 facet 扫描、请求内只解析一次 archive boundary、补齐同步查询护栏。相同请求先用 singleflight；缓存需另有完整 key/失效设计。
6. R3.4：收敛服务端预算档；交互式查询保持 fail-closed，长窗口/批量工作提交 operation job 并异步导出。
7. C1：分钟完整度从 receipts/已关闭 raw 分钟计算，修复空 1m 表依赖。

验收：结果值与修复前的可完成小窗口逐项一致；24h 默认页面不越过同步预算；限额仍 fail-closed；至少覆盖 raw-only、archive-only、跨边界三种路径。

### Phase 2 · 有证据的访问路径与生命周期

1. 从 Phase 0 最慢查询只选 1–2 个候选做影子表 A/B：IP 等值 bloom 与 device+time projection 分别验证读取收益和写入/merge/磁盘回退，禁止一次给所有维度加索引。
2. 去重窗口按“峰值插入块率 × 最长可恢复重试/rebalance 时间 + 安全余量”计算，并验证确定性块 token；不使用固定 `1000`。
3. receipt 清理由 operation job 按月推进：只有该写入月覆盖的事件日全部越过 reconciliation、raw-delete/restore、备份和审计水位才 DROP PARTITION；不配置静态 TTL。
4. `flow_reclassified_records` 改显式 DDL；确认无引用、备份与回滚期关闭后，再独立删除 legacy/死表。

验收：同一生产样本给出前后 read/write/merge/disk 对照；DDL 可前向部署并可停止扩散；任何 DROP 均有引用扫描、备份和恢复证明。

### Phase 3 · 对账、修复与归档算法

1. R7.1：receipt 页携带事实最小/最大事件时间，对账查询先裁日/小时；在部署版本验证投影。去掉 `FINAL` 必须等待 Phase 2 去重闭环通过。
2. R7.2：修复最小单位降为小时；迟到复核超过 late-arrival 窗口后停止。
3. R7.3：重分类按日/分区翻页，证据比较按日收敛，避免全窗 `EXCEPT DISTINCT` 驻留。
4. R1.3/R5.2：只对需要排名的 kind 做窗口排序；按真实基数决定哪些维度折叠，并清理已封存旧 generation。

验收：crash/retry 后相同 generation 收敛；修复一小时不重算全天；对账/重分类 read_rows 随目标窗口而不是总保留期增长。

### Phase 4 · 写路径吞吐与可靠性

1. 每 Kafka partition 使用有界 worker/channel，保留 revoke 排空和 offset 提交顺序；一个慢分区不阻塞全部分区。
2. 依据实测 rows/block、insert P95 与内存预算调整 MaxRows/MaxBytes/MaxWait；允许有界跨 poll 合批，但不牺牲崩溃恢复边界。
3. token 使用确定性坐标边界；模板回拨区仅恢复 decoder 状态，低于 floor 的数据不再分类/写入。
4. 合并端点查找、复用 enrich/列缓冲、隔离行批量写入，逐项用 allocation/records·s⁻¹ 基准验收。

验收：rebalance、CH 暂停、假 ACK、超窗重试下无静默丢数；count/counter 对账一致；峰值 lag 有界，内存不随运行时间增长。

### Phase 5 · 热加速层（只有 Phase 1–4 后仍未达 SLO 才启动）

热层需独立 ADR，冻结表、代际、迟到覆盖、修复、水位、删除和查询边界。不得直接向当前 `flow_aggregate_1h` 写 `generation=1` 并假定读侧无需变更，因为这会混合冷归档与热缓存的生命周期和正确性语义。

验收：raw/hot/archive 三段守恒；迟到与 repair 可重复执行；关闭热层后仍能回退到正确但较慢的 raw 查询。

### 提交门禁

| 门禁 | 必须证明 |
|---|---|
| 正确性 | raw/archive/跨边界、流入/流出、六分类、IPv4/IPv6、迟到、重复消费结果一致 |
| 性能 | 固定数据量与并发下 P50/P95、read_rows、memory、rows/block、lag 有前后对照 |
| 可靠性 | crash/rebalance/CH 超时/假 ACK/重试不丢、不重计，receipt 与事实 count/counter 收敛 |
| 生命周期 | archive/delete/reclass/receipt 删除均由水位与 operation job 驱动，禁止静态期限绕过证据 |
| 交付 | 每阶段独立提交；迁移、代码、单元、真实 CH 集成、回归和回滚说明齐全 |

---

## 5. 与"按设备查像全量 / ↓0↑0"的关系（承接前一轮）

- R4.6 已证实过滤条件在 SQL 模板中位于 `UNION ALL` 后；优化器能否穿透并下推是**运行待证实**，不能直接等同“必然全扫”。无论优化器当前是否成功，显式下推都更容易审计和建立 EXPLAIN 门禁。
- 当前服务端已经用两条查询分别取流入/流出，所以 `↓0↑0` 不能简单归因于“没有 direction 维度”。它还可能来自 exporter→device 归因、客户源地址边界、分类版本、筛选器或数据窗口。R4.4 解决的是重复扫描和统一查询口径，不替代数据归因排查。

---

## 6. 在 .18 上先量再改（复制即跑）

```sql
-- CH 版本 & 去重窗口（决定 R2.1 要不要做、FINAL 行为）
SELECT version();
SELECT name, value FROM system.merge_tree_settings WHERE name IN ('non_replicated_deduplication_window');
SELECT name, engine_full FROM system.tables WHERE database='watchdog_flow' AND engine LIKE '%MergeTree%';

-- 表规模 / part 数（R5.*、R6.2 too-many-parts）
SELECT table, count() AS parts, sum(rows) AS rows,
       formatReadableSize(sum(bytes_on_disk)) AS disk,
       formatReadableSize(sum(data_uncompressed_bytes)) AS raw
FROM system.parts WHERE database='watchdog_flow' AND active GROUP BY table ORDER BY sum(bytes_on_disk) DESC;
SELECT table, partition, count() AS parts FROM system.parts WHERE database='watchdog_flow' AND active
GROUP BY table, partition ORDER BY parts DESC LIMIT 20;

-- 实际索引与投影定义（不要凭迁移文件猜线上状态）
SELECT table, name, type, expr, granularity
FROM system.data_skipping_indices WHERE database='watchdog_flow' ORDER BY table, name;
SELECT table, name, type, sorting_key
FROM system.projections WHERE database='watchdog_flow' ORDER BY table, name;

-- 事实/receipt 时间边界与增长（R5.1）
SELECT count(), min(event_time), max(event_time), min(received_time), max(received_time) FROM flow_records;
SELECT count(), min(min_event_time), max(max_event_time), min(inserted_at), max(inserted_at)
FROM flow_ingest_receipts;

-- 1h 归档每 kind 基数（R5.2 折叠范围）
SELECT dimension_kind, count() AS rows, uniq(dimension_value) AS values, uniq(generation) AS gens
FROM flow_aggregate_1h WHERE bucket >= now() - INTERVAL 7 DAY GROUP BY dimension_kind ORDER BY rows DESC;

-- 最近一小时 Kafka 坐标重复率（可能较重，先在副本/低峰执行）
SELECT count() AS rows,
       uniqExact(tuple(source_stream_id, kafka_partition, kafka_offset, record_index)) AS coordinates,
       rows - coordinates AS duplicate_rows
FROM flow_records
WHERE event_time >= now() - INTERVAL 1 HOUR;

-- 每条查询真实代价（R1/R3/R4：read_rows 远大于 result_rows = 未裁剪）
SELECT substring(replaceRegexpAll(query, '\\s+', ' '), 1, 90) AS q, count() AS n,
       round(avg(read_rows)) AS avg_read_rows, formatReadableSize(avg(read_bytes)) AS avg_read,
       quantileExact(0.95)(read_rows) AS p95_read_rows,
       formatReadableSize(avg(memory_usage)) AS avg_mem,
       quantileExact(0.95)(query_duration_ms) AS p95_ms,
       round(avg(result_rows)) AS avg_result_rows
FROM system.query_log
WHERE event_time > now() - INTERVAL 1 DAY AND type = 'QueryFinish' AND query LIKE '%flow_%'
GROUP BY q ORDER BY avg_read_rows DESC LIMIT 25;

-- 插入块/part 形成速率（system.part_log 必须已开启）
SELECT table, toStartOfMinute(event_time) AS minute, count() AS new_parts,
       sum(system.part_log.rows) AS inserted_rows,
       round(avg(system.part_log.rows)) AS avg_rows_per_part
FROM system.part_log
WHERE database='watchdog_flow' AND event_type='NewPart' AND event_time >= now() - INTERVAL 1 HOUR
GROUP BY table, minute ORDER BY minute DESC, table;

-- 索引/投影是否命中（R3.1 / R7.1；24.9 用 indexes=1，比较两份计划的 ReadFromMergeTree 表名）
EXPLAIN indexes = 1 SELECT count() FROM flow_records WHERE event_time >= now() - INTERVAL 1 DAY AND src_ip = toIPv6('1.2.3.4');
EXPLAIN indexes = 1 SELECT count() FROM flow_records FINAL WHERE source_stream_id = '<stream>' AND kafka_partition = 0 AND kafka_offset BETWEEN 0 AND 100000;
EXPLAIN indexes = 1 SELECT count() FROM flow_records WHERE source_stream_id = '<stream>' AND kafka_partition = 0 AND kafka_offset BETWEEN 0 AND 100000;
```

这些语句均为只读，但 `uniqExact` 和 EXPLAIN 的真实查询仍会消耗资源，应在低峰、只读账号和明确的查询超时/读取预算下执行。Kafka lag、worker records/block、insert P95 还需从 worker 指标与日志取值，不能由 ClickHouse 表反推。

---

## 7. 已经做对的（保持）

- `flow_records` 键 `(toStartOfHour(event_time), source_stream_id, kafka_partition, kafka_offset, record_index)`：时间范围经单调 `toStartOfHour` 裁剪；Kafka 自然坐标给出稳定身份与 keyset 翻页，配合 ReplacingMergeTree/`FINAL` 可做读时收敛；日分区与 `DROP PARTITION` 生命周期对应；`MaxPartitionDays` 防 `max_partitions_per_insert_block` 重试风暴。
- 核心 Flow 表的热列 codec、LowCardinality/Enum、原生 `IPv6` 与固定国家码已落地（007/008/010/011/015）；具体列类型和压缩收益仍以线上 `system.columns/parts_columns` 为准，不把“全库无 Nullable”等未经全库扫描的描述当保证。
- 聚合键前置 `(bucket, dimension_kind, dimension_value)`（010）；`_generation` 标记与数据原子写入，空修复可信；`external_group_by`/`max_memory_usage` 护栏。
- 核心 query/detail/address-set 路径已有服务端读取/结果/内存护栏，限额错误映射为 typed `limit_exceeded`；但 external sort、facet 内存、A(V2) external group-by 和 HTTP 总 deadline 仍有缺口（R4.13），不能概括成“所有路径完备”。
- 写入真正列式（ch-go native + LZ4），永久/可重试错误分类；解码零拷贝 + 快路径 + poison datagram 隔离；分类无锁无 IO（atomic catalog + 不可变 WADS 二分）。
- 对账水位增量、读预算有界；删除用分区 DROP 不用 mutation、前后守恒证据、restore drill。
- 迁移不可变、校验和、逐语句可恢复、fail-fast 锁；011 单次原子多表 `RENAME`。
- 注册表标识符 + 类型化参数，无注入面；规范化 filter AST 可作缓存键。

## 8. 生产只读基线（2026-09-19）

> 数据来自生产 ClickHouse 24.9.2.42 的一次低峰只读采样。数值会随采集继续增长，应用于判断数量级和证明查询/写入形状，不代表固定容量承诺。

### 8.1 数据量、存储与生命周期现状

| 表/对象 | 活跃 part | 行数 | 压缩磁盘 | 未压缩 | 结论 |
|---|---:|---:|---:|---:|---|
| `flow_records` | 25 | 45,335,952 | 2.66GiB | 13.63GiB | 数据约覆盖 2026-09-17 15:09 至 2026-09-19 00:07 UTC；无 data-skipping index |
| `flow_ingest_receipts` | 7 | 11,130,292 | 47.44MiB | 1.05GiB | 行数约为 facts 的 24.6%，压缩盘约为 facts 的 1.7%；没有 TTL/DROP 生命周期 |
| `flow_aggregate_1m` | — | 0 | — | — | 没有生产写入方，不能作为 VPN 分钟完整度来源 |
| `flow_aggregate_1h` | — | 0 | — | — | 当前没有可供近期查询使用的已归档聚合 |
| `flow_address_dict_source` | — | 0 | — | — | 与设计中的二进制 WADS 路径一致，但遗留表去留仍须引用扫描 |

`flow_records` 只有 Kafka 坐标投影 `flow_ingest_audit_v2`，没有为 IP、设备、端口、运营商建立跳数索引。旧 aggregate 表仍存在静态 TTL，但当前 V2 records、receipts 和现行 aggregate 表均没有对应静态 TTL；这不等于生命周期正确，删除仍应由水位与 operation job 驱动。

### 8.2 写入批、part 形成与重复坐标

最近一小时 `flow_records` 与 receipts 各执行 3,594 次 INSERT，约 60 次/分钟/表：

| 写入 | INSERT/h | 平均行/块 | P95 行/块 | P95 INSERT |
|---|---:|---:|---:|---:|
| facts | 3,594 | 1,400 | 1,694 | 35ms |
| receipts | 3,594 | 360 | 437 | 6ms |

这意味着生产每分钟约形成 60 个 facts part + 60 个 receipts part，随后依靠后台 merge 收敛到 25/7 个活跃 part。50,000 行只是代码上限，实际批次只有约 1,400 行；R6 不是推测，而是已证实的小批高频写入。

生产 `non_replicated_deduplication_window=0`。最近一小时 4,995,730 行与 Kafka 自然坐标一一对应，未见重复；但按日比较物理行与 `FINAL` 后行数，2026-09-17 有 5,581,638 个物理行、3,188,236 个去重行，差 2,393,402 行。该证据证明重复曾经存在，但仅凭表数据不能判定是重启、rebalance、假 ACK 还是旧部署行为；修复必须由确定性重放测试定位，不能只扩大去重窗口。

### 8.3 产品查询与坐标对账读取代价

`system.query_log` 中多类线上产品查询单次读取约 3,500 万～4,740 万行、约 1.2～2.3GiB。多数样本 P95 约 0.7～2.2s，但最慢一类 P95 为 10.313s、平均内存约 3.49GiB。它解释了 24h 多维、源/目的 IP、境外和 VPN 页面逼近或触发 50M/4GiB 护栏的现象；不能通过单纯调大常量解决。

2026-09-19 的进一步故障取证确认了两次真实 `MEMORY_LIMIT_EXCEEDED`：分别读取 15,751,262 行/811.23MiB 与 20,938,826 行/1.05GiB，均在 `AggregatingTransform` 将中间状态扩张到约 4.03/4.08GiB 时触发 4GiB 硬上限。失败 SQL 仍是旧部署形状：先按 `dst_ip` 聚合全量事实，随后才在外层按设备过滤。处置第一步是部署 `a777ac83` 的源扫描过滤下推，并让主查询、联合查询与境外查询在 1GiB 时启用 external group-by/external sort。

过滤下推和落盘仍不足以闭环端点查询：同设备当前 24h 范围包含 66,210,292 条事实和 18,374,764 个不同目标 IP；全库为 69,720,428 条事实和 19,431,102 个不同目标 IP。按小时+IP 精确聚合再窗口排名的只读探针即使启用 external group/sort 仍在 15 秒超时。因此源/目的 IP 的 raw-only Storage V2 路径改为两遍有界算法：第一遍以 `topKWeighted(TopN*8)` 获取固定大小候选，第二遍对候选计算精确桶值，并把非候选及候选中未进入最终 Top N 的事实按版本汇入 `_other`。普通查询仍保留 50M rows 上限；仅该有设备过滤、内存有界的双扫描端点路径使用 150M 总读行上限，4GiB read/memory 与 15s 上限不变。真实 ClickHouse 集成验证 Top N、`_other`、records 和完整度守恒；生产 SLO 仍须部署后记录。

同一个 Kafka offset 窄窗口（11,000,000～11,001,000）返回相同的 3,889 条事实时：

| 查询形状 | 读取行 | 读取字节 | 耗时 | 内存 | 读取路径 |
|---|---:|---:|---:|---:|---|
| `FINAL` | 314,037 | 5.58MiB | 30ms | 约 2.57MiB | 主表；9/25 parts、39/6,049 granules |
| 非 `FINAL` | 8,192 | 104KiB | 9ms | 约 167KiB | `flow_ingest_audit_v2`；1/9 parts、1/5,327 granules |

`FINAL` 路径读取行约为投影路径的 38 倍，但目前它仍承担重复收敛语义。因此正确顺序是：先给对账查询增加 receipt 事件时间边界，再闭环确定性块/去重，最后才评估是否可去掉 `FINAL`；不能为命中投影直接牺牲准确性。

### 8.4 尚未取得、变更前必须补齐

- Kafka consumer lag、分区数、rebalance/重启频率及每 datagram 展开记录数；
- worker 和 ClickHouse 的 CPU、RSS/GC、磁盘吞吐、merge backlog、并发查询 P95/P99；
- 实际 raw retention、late-arrival、archive-through、备份/恢复与删除保护水位；
- 24h 默认报表在固定并发、固定设备过滤下的端到端 SLO；
- 故障注入下 block 边界是否确定、超窗重试是否重复、receipt/fact count/counter 是否最终收敛。

## 9. 执行任务清单（持续更新）

> 执行原则：先冻结同一设备、端口、时间窗和计量口径，再定位差额；不得用未说明的倍率、修正值或扩大 ClickHouse 限额制造“看起来一致”。每个工作包按“设计/证据 → 编码 → 单元 → 真实 MySQL+Kafka+ClickHouse 集成 → 故障/变更测试 → 回归 → 独立提交”闭环。

### PERF-Q1 Storage V2 查询过滤下推

- [x] **设计/编码**：将 `business_direction/category/business/target/device/exporter/classification_version` 显式下推到 archive 与 raw 分支；`dimension_value/snapshot/geo` 等聚合后语义保留在 `UNION ALL` 之后。
- [x] **单元测试**：验证 archive 使用 `source.*`、raw 使用原生列或与既有分组一致的转换；验证聚合后过滤没有被错误下推。
- [x] **真实 ClickHouse 集成**：从 Storage V2 迁移到当前完整 schema 后，覆盖 direction/category/device/snapshot/geo/version 的 raw 查询向量并核对结果。
- [ ] **性能验收**：生产同参数运行 `EXPLAIN indexes=1` 和 `query_log` 前后对照，记录 `read_rows/read_bytes/P95/memory_usage`；无明确收益不得把该项写成性能完成。
- [ ] **提交门禁**：代码、单元、真实 CH 回归与本文证据作为一个独立提交，不夹带无关 UI/架构改动。

### PERF-Q1B 24h 查询聚合内存闭环

- [x] **故障证据**：固定两条生产失败 query，确认不是结果行过多，而是高基数端点聚合在 `AggregatingTransform` 内达到 4GiB；同时确认生产仍使用过滤后置的旧 SQL。
- [x] **编码**：主查询、联合维度与境外查询在 1GiB 中间状态时启用 external group-by/external sort；源/目的 IP 的 raw-only 路径使用 `TopN*8` 有界候选加候选精确桶值，删除千万级 IP 窗口排序。普通路径保留 50M rows 上限；端点双扫描路径单独使用 150M 总读行预算，4GiB read/memory、15s 上限不变。
- [x] **单元测试**：三条查询编译器均断言外部聚合/排序阈值；端点编译器断言两遍扫描均先应用设备过滤、候选数固定、无 `dense_rank` 高基数状态，带显式 IP/version 残余筛选时仍回退精确路径。
- [x] **真实 ClickHouse 集成**：隔离数据库写入三个目标 IP，验证候选查询的 Top 1 精确值、版本化 `_other`、records 和 bucket completeness 守恒。
- [ ] **生产验收**：部署包含 `a777ac83` 与本项修复的统一 server 二进制；以同设备、24h、同 Top-N 重放，核对结果与可完成小窗口等价，并记录 `read_rows/read_bytes/memory_usage/duration`。
- [ ] **回归/提交门禁**：`flowquery/server` 通过后独立提交；生产仍超过 50M/15s 的全设备或高基数查询不得再抬同步上限，应进入两段式 Top-N、热查询层或异步导出工作包。

### PERF-CUST6 七牛 IPv4/IPv6 客户源地址归属闭环

- [x] **设计/本地证据**：以设备绑定为作用域，冻结七牛客户的 IPv4 `120.199.32.128/25`、原生 IPv6 `2409:8728:8ff:1077::/64` 及边界样本；确认生产 MySQL 草稿已有双栈 CIDR，但当前 worker 静态 bootstrap 产物只有 IPv4，且未启用 control-plane 版本拉取。生产 WADS/配对版本/ACK/LKG 仍须发布核对，不把全局 Geo 地址库当客户碎片地址的维护入口。
- [x] **编码与本地回归**：统一地址规范为 16 字节可比较形式；IPv4 与 IPv4-mapped IPv6 只在协议边界显式 `Unmap`，原生 IPv6 不降级、不加/减伪前缀；worker 对源/目的地址做 LPM，最长前缀命中结果供客户归属、方向和六分类使用。`6b6bb622` 已增加 classification、worker enrichment、Gin→worker 配对版本三层双栈回归测试。
- [ ] **写入校验**：在 `flow_records` 核对原始 src/dst、local/remote、device/exporter、business、category、address/dimension snapshot 与 classification version；若旧事实版本不含新客户边界，明确走历史重分类，不允许查询层临时猜归属。
- [ ] **查询校验**：源 IP、目的 IP、多维、六报表和导出均显示“七牛”而非 `_unassigned`；IPv4/IPv6 使用相同客户口径，展示层将 `::ffff:x.x.x.x` 规范为 IPv4 文本但不误改原生 IPv6。
- [ ] **单元测试**：覆盖 IPv4、IPv4-mapped IPv6、原生 IPv6、CIDR 首尾地址、相邻不命中、重叠前缀最长匹配、同客户多 CIDR、同设备多客户、跨设备同 CIDR 隔离。
- [ ] **真实链路集成**：发布包含七牛双栈 CIDR 的 WADS → worker 拉取/验签/ACK → 写入 CH → query/report/export；断言双栈均归属七牛且流入/流出、六分类和计数器守恒。
- [ ] **变更/回归/提交门禁**：旧快照继续由 LKG 可启动；无效 IPv6、重叠冲突、版本回退 fail-closed；跑 flowdimension/flowworker/flowch/flowquery/server 全链后独立提交。

### PERF-RECON1 172.57.1.2 的 SNMP ↔ sFlow 端口守恒对账

- [x] **身份与范围冻结**：设备 `172.57.1.2` 与 `103.83.64.2` 是两台独立设备；前者通过自身接口地址 `100.64.20.2` 向 collector 发送 sFlow。首个生产基线冻结 ifIndex `72..81`（`25GE1/0/1..10`）及 UTC `[2026-09-19 00:00:00,01:00:00)`；Flow 必须按 `egress_if_index` 对账，不能拿 observation ifIndex 或业务方向替代物理端口流出。
- [x] **SNMP 口径与基线**：同窗已按发布 generation 的 `snmp_interface_traffic_5m FINAL` 读取 12 个 5 分钟桶；10 端口流出为 `9.203–9.264 Gbps`，coverage 最低 `0.8052`、reset/gap 均为 0。后续实现仍须按 `ifHCOutOctets` 差分语义识别 reset/wrap、`ifCounterDiscontinuityTime`、缺桶和端口状态变化；不得把瞬时 bps 相加当窗口字节。
- [x] **sFlow 事实基线**：同窗 `flow_records FINAL` 按 device+`egress_if_index` 得到 `8.144–8.218 Gbps`、各端口约 33.8–34.1 万记录、sampling rate 恒为 `8192`，相对 SNMP 约 `88%–89%`。这证明 10 端口并非完全未覆盖，但存在稳定偏差；业务方向/客户归属筛选造成的二次减量必须与物理端口事实分开核对。UDP 接收、sequence gap、collector/kernel drop、Kafka/worker lag、CH reject 仍待补齐。
- [x] **4096 变更复测**：设备在 `2026-09-19 01:55:00 UTC` 切换；切换后的纯 4096 完整桶为 `02:00–02:35`，10 端口每 5 分钟平均 `642,077` 条，切换前稳定 8192 桶 `01:40–01:55` 平均 `322,060` 条，密度为 `1.994x`。同窗 Flow/SNMP 汇总比值分别为 `97.37%` 与 `97.34%`（只取 SNMP coverage=1 的完整桶时分别为 `97.40%` 与 `97.57%`），差异远小于相邻桶自然波动；估算总量未随倍率翻倍或减半，证明采样倍率应用正确。降低采样间隔约可把随机抽样标准误差降为原来的 `1/sqrt(2)`，但当前约 `2.6%` 的稳定系统性差额未被消除，仍须由 counter、方向覆盖、sequence/drop 和端口映射证据分类。最近 15 分钟无 `mapping_rejected`，有 120 个零 flow-record 的 `empty` 回执；counter-only 接入后该类报文应转为带 counter 数量的 `persisted` 回执。
- [ ] **方向与端口映射**：明确“设备端口流出”和“本地网络视角流出”不是天然同义；分别产出 observation-port 守恒表与业务方向守恒表，验证 input/output ifIndex 的选择，不允许因客户 CIDR 未命中而让总量消失。
- [ ] **差额分类**：逐端口计算 `flow_estimated_out_bytes / snmp_ifHCOutOctets_delta`，把差额归入 exporter 未覆盖、ifIndex 错配、采样率未知/变化、序列缺口、collector/kernel drop、Kafka/worker lag、CH reject、查询漏筛或二层同流多端口计数；只报告差异，不自动调平。
- [ ] **实现/API**：复用现有 SNMP CH reader 与 Flow fact reader，提供有界同窗 reconciliation 结果（设备、端口、SNMP、Flow、比率、证据/质量标志）；长窗口和批量端口通过 operation job 渐进执行、可取消/重试/导出，不占 HTTP 生命周期。
- [ ] **单元测试**：10 端口、不同采样率、sampling unknown、sequence gap、counter reset/wrap、缺桶、端口 down、IPv4/IPv6、重复 Kafka 消费与同一流经多端口场景。
- [ ] **真实集成**：真实 SNMP CH + Flow CH 同窗对账；核对 receipts 的 count/counter 与事实一致；注入丢包/缺采样/错 ifIndex 后只产生对应问题，不修改原始数据。
- [ ] **性能/回归/提交门禁**：对账必须按 device+ifIndex+time 裁剪，记录 read_rows/P95；跑 snmpch/flowch/flowquery/billing/server 以及真实 CH 回归后独立提交。容差由生产基线和采样统计确定，不在代码中拍脑袋写死。

### PERF-SFC1 sFlow 接口 counter 真值、覆盖与校准闭环

> 约束：counter sample 与 packet flow sample 是两条独立遥测通道。丢弃 counter 不会直接降低 `estimated_bytes`，但会丢失同源接口真值和“哪些端口/方向实际启用了包采样”的证据。任何校准结果必须是带版本和 provenance 的派生口径，绝不覆盖 raw/estimated 事实。

- [x] **设计冻结**：支持标准/expanded counter sample 的 generic interface counter；冻结 agent/sub-agent/datagram sequence、sample source/sequence/index、record index、ifIndex/ifSpeed/ifDirection/ifStatus、in/out octets、单播/组播/广播包、discard/error/unknown-protocol 字段及 Kafka 坐标身份。
- [x] **解码**：fast sFlow 解码器已读取 generic interface counter；未知的合法 counter record 只跳过该 record，并覆盖标准/expanded、counter-only、多 record 和截断测试。继续保留 flow sample 原有零拷贝路径。
- [x] **可靠传输/回执**：CounterRecord 随原 datagram 经 Kafka→worker；counter-only 数据报由 `empty` 改为 `persisted`，回执分别记录 flow fact count 与 counter record count，重放仍以 Kafka 坐标幂等。单元测试覆盖 decoder→worker 的 counter-only 持久化语义以及 worker→CH 的独立 counter 回执。
- [x] **ClickHouse 存储**：迁移 `018_sflow_interface_counters.sql` 新增累计量表 `sflow_interface_counters`，以 device/exporter+ifIndex+event time 查询，以 Kafka 坐标去重；累计量不混写 SNMP 表。差分层的 wrap、reset、乱序、迟到和 discontinuity 仍归入下一项对账实现。
- [ ] **对账/告警**：按 `(device,ifIndex,direction,5m)` 计算 counter delta、Flow estimated 与 `k=counter/estimated`；分别输出“有 counter 无 flow 样本”的覆盖问题、持续漂移、序列缺口、未知倍率、绑定/ifIndex 错误，不自动调平。
- [ ] **派生校准口径**：raw/estimated 永久不变；仅在质量门禁通过时生成 versioned calibrated 派生值，保存 counter 窗口、倍率、版本和证据。供应商/客户视角及 billing 是否采用必须显式配置，缺 counter/覆盖不足时 fail-closed，不回退为隐式倍率。
- [ ] **单元测试**：已完成标准/expanded、counter-only、混合 sample、多 record、未知 record、截断以及 counter-only Kafka→worker→CH 回执；尚需随对账层完成 IPv4/IPv6 agent、差分、wrap/reset、迟到/重复、单/双向覆盖、零 estimated、质量阈值和 raw 不可变。
- [ ] **真实集成/性能**：真实 Huawei CE datagram→Kafka→worker→CH；与 SNMP 同窗 10 端口核对，验证 counter 与 SNMP、packet estimate 三方差异；记录 decoder records/s、worker P95、CH read_rows/bytes，72h soak 无回执漂移后独立提交。

### PERF-Q2 查询、聚类与存储后续闭环

- [ ] **查询**：完成 Phase 1 的两段式 top-N、direction 联合扫描、端点 N+1 消除、境外单遍聚合、facet 合并、预算档与异步导出；每项保持小窗口数值等价。
- [ ] **聚类/维度**：验证六分类互斥+残差守恒，客户/Geo/运营商 LPM 只在 worker 内存快照中完成；不得在逐 flow 写路径查 MySQL/ClickHouse，也不得在查询时重新发明归属。
- [ ] **存储**：只依据真实慢查询影子 A/B 候选 projection/index；闭环确定性重放与去重后再评估去 `FINAL`；receipt/archive/delete 均由水位和 operation job 驱动。
- [ ] **可观测性**：补 collector datagrams/records/reject/drop、Kafka lag、worker enrich/write、unknown sampling/address miss、CH rows/block/insert/merge、reconciliation drift 指标及告警。
- [ ] **回归矩阵**：raw-only、archive-only、跨边界；IPv4/IPv6；流入/流出；六分类；客户/运营商/Geo；重复/迟到/rebalance/CH timeout；报表、明细、分页、导出与计费。
- [ ] **发布门禁**：固定硬件并发压测和 72h soak；达到准确性、性能、可靠性和生命周期门禁后才允许生产发布。

## 10. 审计结论

生产基线把优先级收敛为四个闭环：**过滤下推、合并重复扫描并修复空 1m 依赖（R4/C1）**；**让跨 poll 有界合批真正接近设计批量（R6）**；**补对账事件时间边界并保持 `FINAL` 直到去重闭环通过（R7/R2）**；**把 receipt、archive、delete 纳入水位驱动生命周期（R5）**。这些工作都应先保持统计、计费和六分类语义不变，再用 §8 基线做前后对照。

以下方案在补齐 Phase 0 缺项并完成影子 A/B 前明确禁止直接实施：批量 materialize 多组 skip index、固定 `non_replicated_deduplication_window=1000`、提前删除 `FINAL`、给 receipts/raw 配静态 TTL、向冷归档表写“早期 generation”充当热缓存、把 `uniqExact` 静默替换为近似算法。它们可能改善某一张图，却会把写放大、重复计数、审计证据或统计精度风险带入生产。

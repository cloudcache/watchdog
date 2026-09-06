# Flow 数据面性能与可靠性修复清单

> 来源：2026-09 Flow 模块复盘（采集→Kafka→分类→ClickHouse→查询/汇总，5 路并行子系统审查 + 交叉复核）。
> 完整分级报告见 artifact：https://claude.ai/code/artifact/84cfbec6-aad8-4a2f-9888-5f709426b104
> 结论：无 Critical；数据面按 Akvorado 派生模型构建且实现与设计吻合；可靠性风险集中在**汇总层**与几处**已实现未接线的原语**。

## 执行规则

- 每片：编码 → 单元/gated 测试 → 提交 → 勾选。CH/Kafka 本地已就绪（CH `127.0.0.1:9000`，Kafka `127.0.0.1:9092`，integration 由 `WATCHDOG_FLOW_*_INTEGRATION` 开关）。
- 优先独立 flow 包（`flowstream`/`flowch`/`flowquery`/`flowworker`/`flowdimension`/`flowvpn`）——不与 `internal/watchdog`（当前 address/geo 改动区）抢构建。
- 标 **【需决策】** 的项含产品/架构取舍，先给推荐方案，落地前点一下用户。
- 标 **【CH 迁移】** 的项动 `deploy/migration/clickhouse/`（与 MySQL 迁移序列无关，但排序键类改动须重建表，谨慎）。

---

## P0 · 静默丢数与授权（先止血）

- [x] **F8 给最重的查询与 rollup 补内存护栏** ✅ commit a9a17901 — query.go +max_bytes_to_read/max_memory_usage(4GiB)；detail/address-set +max_memory_usage(2GiB)；rollup +max_bytes_before_external_group_by(4GiB spill)/max_memory_usage(10GiB)。四处 settings 断言扩展 + gated CH（rollup/query/detail/并发隔离）绿。 — `query.go`（两次 FINAL + 半连接，读至 50M 行）是四个查询构造器里唯一无 `max_memory_usage`/`max_bytes_to_read` 的；rollup 查询也无 external-group-by/内存上限。→ 给 query/detail/address-set/rollup 补 `max_memory_usage` + `max_bytes_before_external_group_by`（`overseas.go` 已是样板）。这是挡住 F1 那类可重试 OOM 的先决止血。文件：`flowquery/query.go:324`、`flowquery/detail.go`、`flowquery/address_set.go`、`flowch/rollup.go:248`。验证：单测断言生成 SQL 含护栏 + gated CH 查询仍绿。
- [ ] **F18-obs 汇总空洞可观测性** — `latestCompletedBucketUnix` 是滚动 max（掩盖中间空洞）；terminal `failed` 只增可重试计数器、不增 permanent（天然告警漏 F1）。→ 出专门的 terminal-failed 计数器 + per-tenant「最高连续完成桶」gauge。文件：`flowmetrics/rollup.go`、`flowch/rollup.go:143`。（先于 F1，让空洞可见）
- [ ] **F1 汇总水位改「已完成」语义** 【需决策】 — 水位在 job 入队时推进；job 若 terminal `failed`（`MEMORY_LIMIT`/`TIMEOUT` 均可重试、重型租户确定性失败）则该桶永久静默缺失、无自愈。→ 推荐：另设完成水位（最高连续成功桶）+ reaper 重驱 enqueued-but-not-succeeded 的桶 + 允许对无 generation 的桶重排 gen-1。文件：`internal/watchdog/flow_rollup_jobs.go`（注意与 address 改动同包）。
- [ ] **F2 接上迟到修复（`EnqueueFlowRollupRepair` 目前死代码）** 【需决策】 — 桶仅凭墙钟 `now−5min` 放行，迟到数据永不重算。→ 推荐：用 per-partition ingest 低水位放行桶 + 自动对最近关闭的一批桶跑 gen N+1（读侧已支持多 generation）。文件：`internal/watchdog/flow_rollup_jobs.go`。
- [x] **F3 数据层（raw/supplier）授权执行点**（flowquery 侧）✅ commit 14b1e8fc（PLAT-04H flowquery 半）— `Scope` 加 `AllowedViews`（fail-closed：nil=仅 customer 最低层，raw/supplier 需显式授予）；`CompileDetail` 按请求 view、customer-locked 的 aggregate/overseas/address-set 按 ViewCustomer 强制 `allowsView`；新 `ErrorPermissionDenied`。RBAC 单测 + gated CH（customer）绿。**余项（宿主，internal/watchdog）**：网关按主体权限构造 `Scope.AllowedViews`、module/dataset 注册、CH pool 注入、readiness/限流/error envelope。

## P1 · 可用性与规模（放大 P0 的引擎）

- [x] **F4 GoFlow2 解码加 `recover()`** ✅ commit ae1ebd71 — `recoverDecoderPanic` 包住 pipe.DecodeFlow，panic→decode error→`handleRejected(decode_invalid)` 跳过并推进 offset（已追踪 caller）；err 路径重置 sink+metadata。单测证 panic→error/passthrough/nil。 — 单个畸形 UDP 包 panic → worker 崩溃循环（可远程触发 DoS）。→ 每记录解码包 recover，转 `decode_invalid` 计数以推进 offset。文件：`flowstream/decoder.go:113`、`flowstream/consumer.go:310`。验证：构造 panic 输入 → 被 recover + 计数 + offset 前进。
- [x] **F5 采集端优雅关闭死锁** ✅ commit b09a47bc — 关闭时并发 `closeProducer()`（sync.Once 共享 defer 与 shutdown 两路）与 receiver join，Close 内 Flush 释放阻塞的 Produce，等 join+close 双完成再返回。build/vet 绿；backpressure 下 graceful-shutdown 集成测试归入容量/soak 验收。 — `Produce` 用 `context.Background()`，缓冲满时阻塞；关闭先 join receiver 再 `producer.Close()` → 死锁到 SIGKILL。→ 关闭时先并发 `producer.Close(closeCtx)`（Flush 后取消 client ctx）再 join。文件：`flowstream/producer.go:124`、`cmd/watchdog-flow-collect/main.go:102,176`。
- [~] **F10 卡住的 partition/块无死信**（无需决策的一半 ✅ commit a2ea5aaf；带设计的一半留后）— `insertWithRetry` 无限重试且 `RunPartitionBatches` 每轮等所有 partition goroutine + 一个 partition 失败会 `cancel()` 掉其它 → 一个卡住拖垮全 worker、且阻塞 rebalance。
  - ✅ 已做(ADR 兼容,不丢不死信,失败记录仍走 unmarked→重放):`Writer.RetryMaxElapsed`(默认 2m)给每块一个总重试预算,超时把块作为错误抛出让 barrier 排空、rebalance 能推进;`processFetches` 不再因一个 partition 失败取消健康 partition(健康的跑完并提交进度);新增 stuck 信号 `RetryingNow` gauge + `BudgetExceeded` counter → `watchdog_flow_clickhouse_blocks_retrying` / `..._insert_budget_exceeded_total`。文件:`flowch/writer.go`、`flowstream/consumer.go:319`、`flowmetrics/metrics.go`。
  - ⏳ 留后(带设计):真正的「会话内 per-partition 解耦」(健康 partition 不靠重启就持续前进)需要改提交模型做 per-partition offset 回退,是 F10 的设计决策部分。
- [x] **F6 `src_ip`/`dst_ip` 维度基数爆炸** ✅ commit aa12f214（决策=rollup 期 top-N + _other）— rollup 查询包一层 rank-and-refold：按 estimated_bytes 对每组 src_ip/dst_ip 排名，留 top `rollupIPTopN`(默认1000)，长尾折进单个 `_other`（流量保留不丢）；非 IP 维 ip_rank=0 原样透传，`_generation` marker 不动。**注意**：存量桶需 repair（新 generation）才采纳。gated CH 证明 top-2 保留 + 尾部折 _other（含求和）。 — 以近乎原始基数物化进 180/400 天汇总表。→ 推荐：rollup 期对 IP 维度做 top-N + `_other`，或移独立短 TTL 表。文件：`flowch/rollup.go:336`、`001_flow_schema.sql:99`。
- [ ] **F7 汇总表排序键把 `dimension_kind` 排第 9 位** 【CH 迁移·重建表】 — 强制过滤无法裁剪，每查扫全部维度类。→ 重排为 `(tenant_id, bucket, dimension_kind, dimension_value, …)`（列不变，去重语义不变）。注意 MergeTree 不能 ALTER ORDER BY，须新表 + 迁移。文件：`001_flow_schema.sql:125`。
- [ ] **F9 1 分钟汇总 60× 重扫 tenant-hour** 【CH 迁移·重建表】 — `flow_records` 排序键截到小时，分钟桶无法跳 granule。→ 排序键加 `toStartOfFiveMinutes(event_time)` 分量，或用 MV 派生 1m。文件：`001_flow_schema.sql:95`、`rollup.go:321`。

## P2 · 正确性边角 · 资源 · 卫生

- [x] **F11 内网向流 address-set 归属恒空** ✅ commit 8747ef85（决策=in/out 并集）— 编译期预计算 `internal` 并集字段（每匹配集恰一次），classify 时零拷贝 alias（保零分配）；per-record 展开上限计入 2*maxLocalInternal。单测断言内网流 local 端携带集归属。 — `endpoint()` switch 无 `DirectionInternal` 分支 → 内网流量丢客户归属。→ 补分支（推荐 in/out 并集）；与产品确认语义。文件：`flowdimension/classify.go:102`。验证：内网 10.x↔10.x 流命中 `both` 集合。
- [~] **F12 历史 GeoIndex 版本不回收（`RetainVersions` 无调用方）** — 长期运行内存缓慢泄漏。
  - ✅ **泄漏已封顶**(commit afa5ae64,自包含、不需 hub 接线):`GeoCatalog.load()` 装入版本后调 `capHistoricalGeoVersions`,保留 active + 按 EffectiveFrom 最近的若干版本(默认 128,geo 发布稀少故足够宽),裁掉更老的;早于保留窗口的事件经 `Select` 返回 `ErrNoGeoIndex`,不会选到错版本。单测覆盖保最近/回滚保 active/未超限 no-op。
  - ⏳ **待补(hub 侧,可选)**:按「事件时间重放窗口」的精确保留仍是 `RetainVersions` 的活,等 hub `FlowGeoService.Reload` 接线时接上;当前 cap 只是防泄漏兜底。文件:`flowdimension/geo.go`、`internal/watchdog/flow_geo.go`。
  - 精确机制（复核后）：`GeoCatalog.load()` 每装入一个新 version 就把它加进 `byVersion` 且从不裁剪。两个 caller 里——**worker**（`cmd/watchdog-flow-worker/main.go:344` 一次性按 CLI bundle 列表装载，**有界、不泄漏**）；**hub** `FlowGeoService.Reload()`（`internal/watchdog/flow_geo.go`，每次 geo 重发布调用一次，**会随发布次数无界累积** = 真正泄漏路径）。
  - `RetainVersions` 本身已有基础单测（geo_test.go:423/429），逻辑正确、只是没 caller。修复=在 hub 的周期重载点用「active.EffectiveFrom − 事件重放窗口」算出需保留版本集并调用 `RetainVersions`。该点在 `internal/watchdog`（当前热区，且与正在重做的地址/分类发布链路相关），留给该链路定稿时一并接。
- [x] **F16 VPN 打分器每规则预分配 signals 切片** ✅ commit（本轮）— `make([]string,0,12)` 改 `var signals []string`，首检失败即零分配。AllocsPerRun 断言早退零分配。 — 每窗口约 5000 万次废弃分配。→ signals 延迟到首个成功信号才分配。文件：`flowvpn/scorer.go:401`。验证：alloc 基准显著下降。
- [x] **F15 解码热路径多一次 proto marshal→unmarshal + 分配**（已做真·zero-copy）— 全系统最高频操作。去掉 format+transport 往返，直接引用池化 `FlowMessage`。文件：`flowstream/decoder.go`、`flowstream/sflow_metadata.go`。
  - **修正 + 落地(2026-09-06)**:先前"判定不做"的前提**是错的**——它假设快路径仍须 `proto.Clone` 深拷贝。实际不需要:下游 `flowworker/decode_adapter.go` 的 `mapFlowMessage` **当场消费** `FlowMessage`——标量按值拷、地址经 `canonicalAddress16`（`netip.AddrFromSlice`→`As16()`→`append` 全新 `[]byte`）深拷,`Map` 返回后 `Record` 与池化 buffer 完全独立。所以 `FlowMessage` 无需跨 `Decode` 存活,zero-copy 可行。
  - **实现**:pipe 传 `Format:nil`（`formatSend` 内 `if p.format!=nil` 守卫 → marshal/transport 全跳过）；`metadataProducer` 在 `Produce` 后持有池化消息、`Commit`**延迟回收**（存 `pending`,不立即 `delegate.Commit`）；`Decode` 开头 `recyclePending()` 把**上一批**还给池,再直接取 `&ppm.FlowMessage` 建 batch。删掉 `captureTransport`（unmarshal 到新对象那半）。
  - **契约**:batch 里的指针指向池化 buffer,仅在**下一次 `Decode` 前**有效——调用方必须先 map 完再 decode 下一条。partition worker 天然满足（每条 record 先 `adapter.Map` 再 decode 下一条）。并发解码共享全局 `sync.Pool` 也安全:延迟回收使"在用"消息始终 checked-out、对其他 decoder 不可见。
  - **实测 A/B `BenchmarkDecodeNetFlowV5`（30 rec/datagram）**:41,400ns→8,780ns/op(**4.7× / −79%**)、470→110 allocs/op(**−77%**)、37,020→5,309 B/op(**−86%**)、吞吐 725K→3.42M rec/s。远超先前"~10%"的保守误判。
  - **门禁**:四协议 corpus(`TestDecoderHandlesSFlowAndNetFlowV5`+v9/IPFIX)、新增 `TestDecoderReuseKeepsBatchesCorrectAcrossDecodes`（40×2 记录顺序复用、逐批 map-before-next 不串批）、并发 `TestPartitionDecodersProcessPartitionsConcurrently`（8 分区读取记录内容 + `-race`,已用"改回立即回收即 FAIL"反证其有效）。`flowstream`+`flowworker` 全绿 `-race`。
- [x] **F15b 解码热路径 step 2:NetFlow v5 定长快解码器**（commit 93fa3e92）— zero-copy(F15)去掉了 marshal 往返,但 profile 显示 GoFlow2 仍有 ~40% CPU 花在**反射式二进制读**(`utils.BinaryDecoder`/`intDataSize`/`bytes.Buffer`),105 分配/包大头在 `ConvertNetFlowLegacyRecord`(先解析成中间 struct 再转 protobuf,两段)+ 每包 `netip.AddrPort.String` 路由键。
  - **落地**:NetFlow v5 是定长报文(24B 头 + N×48B 记录),`flowstream/netflow_v5_fast.go` 按固定偏移直接解、零反射、无中间 struct、无逐记录分配(地址切 payload、复用 backing 数组)。`Decode` 对 `NETFLOW_V5` 走快路径(`d.fastNetFlowV5` 默认开),其余(v9/IPFIX/sFlow)仍回落 GoFlow2。
  - **正确性门禁**:`TestNetFlowV5FastMatchesGoFlow2` 用 corpus(single/many/时间 wrap/饱和/空)两条路径各解一遍、逐记录 `proto.Equal` + batch 身份断言——**逐字段等价**才过(它抓出并修正了 `ConvertNetFlowLegacyRecord` 漏设、由 pipe `ProduceArgs` 补的 `TimeReceivedNs`/`SamplerAddress`)。截断/恶意 count 时快路径只解实际存在的记录(GoFlow2 会吐幻影零记录),`TestNetFlowV5FastClampsTruncatedCount` 锁定这个**更安全的有意分歧**。
  - **实测 A/B(同 30 rec/包,两路输出 proto 完全一致)**:8,800ns→**1,145ns/op(7.7×)**、110→**7 allocs/op(−94%,逐记录分配清零)**、5,309→1,984 B/op(−63%)、吞吐 3.4M→**26M rec/s/核**。正好落在预估 5–15× 区间(先前 116ns/72× 是"只解 12 字段扁平 struct"的地板,非等价对比)。`flowstream`+`flowworker` 全绿 `-race`。
- [x] **F15c 解码热路径 step 3:zero-copy Kafka 信封解析**（commit 1cc27c90）— v5 快路径落地后,信封 `proto.Unmarshal` 成了剩余大头:它拷贝每个 bytes 字段,尤其**整个 flow payload**(30 记录约 1.5KB)+ 两个身份串 + 每次一个 struct。
  - **落地**:`flowstream/rawflow_fast.go` 的 `parseRawFlowInto` 直接走 protobuf wire:`Payload`/`SourceAddress` **切 value buffer 不拷**(batch 生命周期内有效,worker map 完才复用),只拷小的 `CollectorId`/`ListenerId`,目标 `RawFlow` 跨调用复用。未知字段(6/7/8,解码器不读)按 wire type 跳过,前向兼容。
  - **门禁**:`TestRawFlowZeroCopyParseMatchesProto`(corpus:真实 NetFlow/sFlow 信封、带跳过字段的、饱和标量,两路解断言 decoder 相关字段相等)+ `TestRawFlowZeroCopyParseRejectsTruncated`(截断报错不 panic)+ **所有既有解码器测试现在都走 `parseRawFlowInto`**。
  - **实测**:信封 A/B(30 记录/~1.5KB)344→**83ns(4×)**、1,744→**16 B/op(−99%)**、5→2 allocs。**NetFlow v5 端到端(v5 快路径 + zero-copy 信封)对比原始 GoFlow2 基线**:8,800→**773ns/op(11.4×)**、110→**4 allocs/op(−96%)**、5,309→**261 B/op(−95%)**、3.4M→**~39M rec/s/核**。`flowstream`+`flowworker` 全绿 `-race`。
- [ ] **F15d 解码热路径 step 4:sFlow v5 快解码器**（进行中）— 已测 baseline:30 SampledIPv4 样本 **13,757ns / 232 allocs / 2.18M rec/s**(比 v5 更重,且这还是**不含抓包头解析**的简单情形)。机会大。
  - **难点**:sFlow 不是定长——是 TLV(datagram 头 → 变长 samples → 变长 records),且最常见的 `SampledHeader` 记录内含**被采样报文的以太网/IP/TCP 头**,需解析**不可信嵌套报文**(安全敏感)。不能像 v5 那样纯定长直解。
  - **计划**:(1) 手写 sFlow **framing** 解析(datagram/sample/record 的 TLV 导航,零反射,严格 bounds check)替掉 GoFlow2 的 35 处反射调用;(2) `SampledHeader` 的抓包头解析**复用 GoFlow2 已加固的 `ParseSampledHeader`**(不重写以太网/IP 解析,降风险);(3) 复刻 sample 元数据提取(SubAgentID/SourceId/SampleSequence/SamplePool/Drops/SampleIndex);(4) `TestSFlowFastMatchesGoFlow2` 差分门禁(两路 `proto.Equal` + 元数据相等,覆盖 SampledHeader/SampledIPv4/IPv6/ExtendedRouter/Gateway/Switch 各记录类型)。因在**攻击面最大的不可信路径**上,按差分门禁逐步验证、不图快。
- [x] **F14 汇总租户公平性游标按「列出」推进** ✅ commit 1616b153 — 追赶期头几个租户耗尽预算、其余饥饿。`ScanClosedBuckets` 现返回 `StoppedEarly` + `LastProcessedTenant`;`ScanOnce` 只在整页处理完才把游标推到 `next`,预算中途截断则续在最后一个已处理租户之后(或原地保持,若连第一个都没跑完)。新测试证明单租户预算下游标 a→b 走位、三租户都被调度(修复前 b/c 饿死)。文件:`internal/watchdog/flow_rollup_jobs.go`。
- [x] **F17 全表无压缩 CODEC** 【CH 迁移】 ✅ commit cfc2cef8 — migration 007 给 flow_records 关键列加 CODEC：时间/单调 offset→DoubleDelta,ZSTD；计数→T64,ZSTD；IPv6→ZSTD。MODIFY COLUMN 元数据级、在线。gated CH 迁移链应用绿；loader/canonical-set 断言升到 7。聚合表 CODEC 已由 migration 008 补齐（flow_aggregate_1m/1h，180/400 天保留，时间/generation→DoubleDelta,ZSTD；计数→T64,ZSTD；dimension_value→ZSTD；LowCardinality 列保留；断言升到 8，commit 2998ce26）。 — 时间/计数/IPv6 列均落默认 LZ4。→ 只进 `MODIFY COLUMN … CODEC(...)` 迁移（Delta/DoubleDelta/T64/Gorilla/ZSTD）。文件：`deploy/migration/clickhouse/`。
- [x] **F18-mig 迁移 006 会在有数据集群超时** 【CH 运维】✅ commit 2af4cb71（走「配大 timeout」;「轮询 system.mutations」不可行,见下）— `MATERIALIZE PROJECTION … mutations_sync=2` 阻塞全表 mutation > 默认 5min。
  - ⛔ **async+poll 方案不可行**:006 是 immutable(checksum drift 门禁,改不了 `mutations_sync=0`),且它排在最前(任何新迁移都在它之后,救不了它),且 executor 对每条语句都套连接级 `OperationTimeout`(没法单条延长)。所以只剩「配大 timeout」这个杠杆。
  - ✅ 已做:`watchdog-flow-migrate` 的 ctx 是 signal-only(无 deadline),per-op timeout 是唯一时间边界。把默认 `-clickhouse-operation-timeout` 从 5min 提到 **1h**(一次性、有人值守、可 Ctrl-C 的工具,宁可等久也不误杀正常长迁移),help 说明按最大 mutation 规模设定、可调小求快失败。文件:`cmd/watchdog-flow-migrate/main.go`。
  - 备注:真正的 async 物化只能靠**未来新迁移从一开始就写成 async + migrator 加 poll 支持**;不为 006 追溯。
- [x] **F18-block 回放块跨 >100 event-time 日触分区上限卡死** ✅ commit 21fa59d7 — PrepareBlocks 跟踪当前块 distinct partition-day，超 `MaxPartitionDays`(默认90,硬顶100) 前先 flush，数据保留（尾部起新块）。单测证 4 天在 2 天上限下分 2 块。 — `max_partitions_per_insert_block` 未归类 permanent → 无限重试。→ 按分区日切块 + event_time 合理性窗口。文件：`flowch/batch.go:90`。
- [x] **F18-est 行数估计忽略 version 元组乘数** ✅ commit 2c156d94 — 采「翻译 overflow」方案(比预估版本多重性更稳):`classifyExecutionError` 把 CH 资源上限异常(`TOO_MANY_ROWS`/`TOO_MANY_BYTES`/`TOO_MANY_ROWS_OR_BYTES`/`MEMORY_LIMIT_EXCEEDED`/`SET_SIZE_LIMIT_EXCEEDED`)统一映射成 `from/to` 的 `ErrorLimitExceeded`,四个 runner(aggregate/detail/overseas/address-set)共用;非上限错误原样透传。unit 验限额码→typed、非限额码/普通错误不误判。 — 混版本长范围抛原始 CH overflow 而非 typed 错误。→ 把版本多重性纳入估计，或翻译 overflow 回 `ErrorLimitExceeded`。文件：`flowquery/query.go`、`runner.go`、`detail_runner.go`、`overseas_runner.go`、`address_set_runner.go`。
- [x] **F13 遗留 `watchdog-sflow-collector` 下线** ✅ commit 52b9d3f2（用户拍板下线，不走"保留并加固"分支）— 逐包 DB I/O、静默丢包、关闭挂起、60s 丢数窗口。直接删除该独立原型(cmd + `internal/watchdog/sflow_collector.go` + config.go 的 `SFlowCollectorConfig`/`WATCHDOG_SFLOW_*`/校验 + config_test + yaml/install 文档/gitignore)；其角色由 RawFlow 数据面(flow-collect→Kafka→flow-worker，GoFlow2 解 sFlow v5)取代。无自有 MySQL 表/迁移(原写 VM),goflow2 依赖保留(新管线在用),`go build`/`go vet`/config 测试通过;新管线 sFlow 处理(protocol "sflow5")不动。

## 设计事实（非缺陷，须告知运维）

- 不存在历史「重分类」worker：分类在 ingest 时按事件时间不可变物化，generation/repair 只能重**聚合**不能重**分类**。一次定义修复不回溯纠正历史聚合。

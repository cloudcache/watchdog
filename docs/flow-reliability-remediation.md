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
- [ ] **F3 数据层（raw/supplier）授权执行点** 【需决策·依赖查询网关】 — `CompileDetail` 直接接受客户端 `view`，`raw` 还移除可见性过滤。查询包当前无 HTTP 调用方。→ 推荐：给 `CompileDetail` 加 `allowedViews` 参数把执行点收进包内；建查询网关（PLAT-04H）时做「主体→允许 view 集合」映射。文件：`flowquery/detail.go:334`。

## P1 · 可用性与规模（放大 P0 的引擎）

- [x] **F4 GoFlow2 解码加 `recover()`** ✅ commit ae1ebd71 — `recoverDecoderPanic` 包住 pipe.DecodeFlow，panic→decode error→`handleRejected(decode_invalid)` 跳过并推进 offset（已追踪 caller）；err 路径重置 sink+metadata。单测证 panic→error/passthrough/nil。 — 单个畸形 UDP 包 panic → worker 崩溃循环（可远程触发 DoS）。→ 每记录解码包 recover，转 `decode_invalid` 计数以推进 offset。文件：`flowstream/decoder.go:113`、`flowstream/consumer.go:310`。验证：构造 panic 输入 → 被 recover + 计数 + offset 前进。
- [x] **F5 采集端优雅关闭死锁** ✅ commit b09a47bc — 关闭时并发 `closeProducer()`（sync.Once 共享 defer 与 shutdown 两路）与 receiver join，Close 内 Flush 释放阻塞的 Produce，等 join+close 双完成再返回。build/vet 绿；backpressure 下 graceful-shutdown 集成测试归入容量/soak 验收。 — `Produce` 用 `context.Background()`，缓冲满时阻塞；关闭先 join receiver 再 `producer.Close()` → 死锁到 SIGKILL。→ 关闭时先并发 `producer.Close(closeCtx)`（Flush 后取消 client ctx）再 join。文件：`flowstream/producer.go:124`、`cmd/watchdog-flow-collect/main.go:102,176`。
- [ ] **F10 卡住的 partition/块无死信** — `insertWithRetry` 无限重试且 `RunPartitionBatches` 每轮等所有 partition goroutine → 一个卡住拖垮全 worker。→ 每块有界尝试/时间预算，超限 park/死信 + 出「stuck」指标。文件：`flowch/writer.go:107`、`flowstream/consumer.go:319`。
- [x] **F6 `src_ip`/`dst_ip` 维度基数爆炸** ✅ commit aa12f214（决策=rollup 期 top-N + _other）— rollup 查询包一层 rank-and-refold：按 estimated_bytes 对每组 src_ip/dst_ip 排名，留 top `rollupIPTopN`(默认1000)，长尾折进单个 `_other`（流量保留不丢）；非 IP 维 ip_rank=0 原样透传，`_generation` marker 不动。**注意**：存量桶需 repair（新 generation）才采纳。gated CH 证明 top-2 保留 + 尾部折 _other（含求和）。 — 以近乎原始基数物化进 180/400 天汇总表。→ 推荐：rollup 期对 IP 维度做 top-N + `_other`，或移独立短 TTL 表。文件：`flowch/rollup.go:336`、`001_flow_schema.sql:99`。
- [ ] **F7 汇总表排序键把 `dimension_kind` 排第 9 位** 【CH 迁移·重建表】 — 强制过滤无法裁剪，每查扫全部维度类。→ 重排为 `(tenant_id, bucket, dimension_kind, dimension_value, …)`（列不变，去重语义不变）。注意 MergeTree 不能 ALTER ORDER BY，须新表 + 迁移。文件：`001_flow_schema.sql:125`。
- [ ] **F9 1 分钟汇总 60× 重扫 tenant-hour** 【CH 迁移·重建表】 — `flow_records` 排序键截到小时，分钟桶无法跳 granule。→ 排序键加 `toStartOfFiveMinutes(event_time)` 分量，或用 MV 派生 1m。文件：`001_flow_schema.sql:95`、`rollup.go:321`。

## P2 · 正确性边角 · 资源 · 卫生

- [x] **F11 内网向流 address-set 归属恒空** ✅ commit 8747ef85（决策=in/out 并集）— 编译期预计算 `internal` 并集字段（每匹配集恰一次），classify 时零拷贝 alias（保零分配）；per-record 展开上限计入 2*maxLocalInternal。单测断言内网流 local 端携带集归属。 — `endpoint()` switch 无 `DirectionInternal` 分支 → 内网流量丢客户归属。→ 补分支（推荐 in/out 并集）；与产品确认语义。文件：`flowdimension/classify.go:102`。验证：内网 10.x↔10.x 流命中 `both` 集合。
- [ ] **F12 历史 GeoIndex 版本不回收（`RetainVersions` 无调用方）** — 长期运行内存缓慢泄漏。→ 把 `RetainVersions` 接进 reload/保留策略（只留事件时间重放窗口内版本）。文件：`flowdimension/geo.go:427`、`internal/watchdog/flow_geo.go`。
- [x] **F16 VPN 打分器每规则预分配 signals 切片** ✅ commit（本轮）— `make([]string,0,12)` 改 `var signals []string`，首检失败即零分配。AllocsPerRun 断言早退零分配。 — 每窗口约 5000 万次废弃分配。→ signals 延迟到首个成功信号才分配。文件：`flowvpn/scorer.go:401`。验证：alloc 基准显著下降。
- [ ] **F15 解码热路径多一次 proto marshal→unmarshal + 分配** — 全系统最高频操作。→ 直接捕获内嵌 `FlowMessage`，去 format+transport 往返。文件：`flowstream/decoder.go:56`。验证：alloc 基准 + 解码正确性回归。
- [ ] **F14 汇总租户公平性游标按「列出」推进** — 追赶期头几个租户耗尽预算、其余饥饿。→ 游标只推进到实际处理的最后一个租户。文件：`internal/watchdog/flow_rollup_jobs.go:139`。
- [ ] **F17 全表无压缩 CODEC** 【CH 迁移】 — 时间/计数/IPv6 列均落默认 LZ4。→ 只进 `MODIFY COLUMN … CODEC(...)` 迁移（Delta/DoubleDelta/T64/Gorilla/ZSTD）。文件：`deploy/migration/clickhouse/`。
- [ ] **F18-mig 迁移 006 会在有数据集群超时** 【CH 运维】 — `MATERIALIZE PROJECTION … mutations_sync=2` 阻塞全表 mutation > 默认 5min。→ 为 006 配大 timeout 或轮询 `system.mutations`。
- [ ] **F18-block 回放块跨 >100 event-time 日触分区上限卡死** — `max_partitions_per_insert_block` 未归类 permanent → 无限重试。→ 按分区日切块 + event_time 合理性窗口。文件：`flowch/batch.go:90`。
- [ ] **F18-est 行数估计忽略 version 元组乘数** — 混版本长范围抛原始 CH overflow 而非 typed 错误。→ 把版本多重性纳入估计，或翻译 overflow 回 `ErrorLimitExceeded`。文件：`flowquery/query.go:277`。
- [ ] **F13 遗留 `watchdog-sflow-collector` 下线** 【需决策】 — 逐包 DB I/O、静默丢包、关闭挂起、60s 丢数窗口。→ 推荐以 flowstream Receiver 取代后删除；若须保留则内存缓存 device/port + `SetReadBuffer` + 丢包指标 + ctx 关 conn + 退出前 flush。文件：`internal/watchdog/sflow_collector.go`。

## 设计事实（非缺陷，须告知运维）

- 不存在历史「重分类」worker：分类在 ingest 时按事件时间不可变物化，generation/repair 只能重**聚合**不能重**分类**。一次定义修复不回溯纠正历史聚合。

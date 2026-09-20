# flow-collector / flow-worker 代码级审查（正确性·可靠性·安全·协议·高性能）

日期：2026-09-19　复核：2026-09-20　方法：代码审查 + 当前 Git + 生产进程/Kafka/ClickHouse/MySQL 只读证据。本文原始审查**未修改任何代码**；2026-09-20 复核用于校正其结论和任务状态。
原审查基点：`HEAD cfbb31ce fix(flow): split endpoint scan budget`。当前复核基点：`HEAD 313a0ac0`，工作树仍含未提交/未跟踪的发布、对账和部署代码；这些文件不能被当作已交付生产能力。
关联：[watchdog-flow-perf-audit-2026-09-19.md](watchdog-flow-perf-audit-2026-09-19.md)（性能根因 R1–R7；其 §8 已含 `.18` 生产实测）、[watchdog-flow-clickhouse-ix-scale-design.md](watchdog-flow-clickhouse-ix-scale-design.md)（IX 规模存储设计）。

严重度：**C** critical / **H** high / **M** medium / **L** low。标记：✅ = 本人已按 file:line 复核；◐ = 审查者报告、待复核（文件当时被另一路审查占用）；— = 设计/运维事实。

---

## 2026-09-20 深入复核（本节优先于原审查）

### 复核摘要

原审查对“接收缓冲、topic 校验、worker 毒丸、模板重放、幂等窗口、连接池竞争、热路径分配”大方向判断成立，但把部分性能推测写成确定事实，并给出若干会降低可靠性的修复建议。当前最紧急问题不是再做微优化，而是补齐**客户边界发布/ACK**：生产七牛 IPv4/IPv6 CIDR 只写入了 MySQL，保存链根本没有创建发布修订或发布任务，更谈不上 worker 安装；最近一小时约 119.8 Gbps 原始估算中约 63.4 Gbps 被标为 `transit`，六分类报表因此只显示约一半。这个正确性缺口未关闭前，任何吞吐提升都只会更快地产生错误分类。

### 当前生产证据

| 证据 | 结果 | 含义 |
|---|---|---|
| 进程 | server、flow-collect、flow-worker、snmp-collector 均 active | 当前 500 不是服务未启动；此前 500 是发布目录缺少 `frontend/index.html` 导致 Nginx SPA fallback 循环，补齐静态构建后 `/` 与 `/flow?range=24h` 均为 200 |
| collector | 约 1475 万 sFlow datagram → 同数 Kafka record；约 19.5 GB；显式 publish/reject/oversize/invalid 计数为 0 | 应用层未报告丢失，但不能证明 UDP 内核或网卡未丢 |
| UDP 缓冲 | `net.core.rmem_max=212992`，代码请求 32 MiB 但未回读 | 生产实际风险验证了原 H1；`kernel_drops=0` 只表示已收到的 cmsg 未报告溢出 |
| Kafka | 12 partitions、RF=1、min ISR=1 | 单 broker 可开发运行，不满足 IX 高可用 |
| ClickHouse | `flow_records` 约 5773 万行 / 3.96 GiB；receipts 约 1473 万行；无 Flow 固定 TTL、无非复制去重窗口 | 原始写入已运行；物理幂等与生命周期仍未闭环 |
| 客户边界 | 172.57.1.2 已绑定七牛 `120.199.32.128/25`、`2409:8728:8ff:1077::/64`；旧 enrichment ACK 表为空，新 deployment 表尚未进入生产 schema | 这次只发生了保存，没有发生发布；worker 继续运行旧客户边界，大量 `transit` 是必然结果 |
| 边界保存链 | 当前 create/update/delete 只修改客户边界两张表并写审计；没有 dirty revision、没有 operation job、没有 deployment/ACK 状态回写 | 这不是“重发布失败”，而是根本没有触发重发布；UI 不得把保存结果表达为运行时已更新 |
| v2 工作树 | migration 0049、deployment API/job/UI、worker loader/LKG/dual-sync 均未跟踪、未进入生产 schema/二进制 | 纵向代码形状已出现，但在 migration、真实 MySQL+worker ACK、升级/回滚和独立提交门禁前一律按未完成处理 |
| 数值对账 | packet sample 估算约 119.8 Gbps，其中 in 约 13.2、out 约 43.2、transit 约 63.4；sFlow counter 12 个接口 in 合计约 101 Gbps、out 合计约 17.7 Gbps | 总估算与 counter 总量已接近同量级；报表只剩一半主要是 transit 排除，而不是先继续放大 sampling scale |

### 原审查逐项校准

| 原结论 | 当前状态 | 复核裁决 |
|---|---|---|
| GoFlow2 零字段模板可无界循环 | **仍成立、Critical** | 必须在进入上游 decoder 前验证/拒绝零进展模板，或维护已修补 fork。只用 goroutine/context 超时不能停止一个不合作的死循环，原 Phase 0.1 的“200ms watchdog”不足 |
| 数据形状错误退出整个 worker | **仍成立、Critical/High** | 永久数据错误写拒绝回执并推进；版本暂时缺失只暂停对应 Kafka partition，不能退出全进程 |
| `SO_RCVBUF` 可能被钳制 | **已由生产证实** | 首选部署 sysctl + 启动回读 + readiness/指标；不应默认给进程 `CAP_NET_ADMIN`/`SO_RCVBUFFORCE`，能力提升只能是受控备选 |
| topic 只 Ping broker、不验 topic | **仍成立** | 启动 metadata 校验并 fail-fast；原建议 `UnknownTopicRetries(-1)` 会无限阻塞并制造“进程活着但不工作”，废止 |
| 同步 Send 早返回未计数、无 franz-go logger、无 MaxBufferedBytes | **仍成立** | 补恒等式、有限字节预算和限速日志；不得以无界重试掩盖错误 |
| `TryProduce` 满时丢包 | **原修复建议错误** | 产品目标不是主动丢弃。采用有界内存 + 明确 backpressure/拥塞状态；只有经过产品决策的 load-shedding 才能丢，并须记录连续 offset/时间窗口 |
| QinQ 必然整 datagram `mapping_rejected` | **已证明机制、暴露面待测**（remediation §7.1 复核后修正） | 机制在代码里可完整判定：外层 `0x88a8` → goflow2 `parserNone` → 空地址 → 整 datagram `mapping_rejected`（并非"证据不足"）；运行时未知的只是**暴露面**（发生频率）。`0x8100` 单标签是否离开快路径取决于 fallback；仍以真实 pcap 差分固化 fast/fallback 的地址/端口/VLAN/长度一致性 |
| sFlow counter 被丢弃 | **已修复** | generic interface counter 已进入 `sflow_interface_counters`；仍需 reset/wrap/gap/coverage 的 5m 对账层 |
| `estimated_bytes_scale_ppm` schema/write path | **部分修复** | 迁移账本、生产列和 writer 已存在；但 `NativeInserter.Ready()` 仍只核验 5 个身份列，缺列时会错误通过 readiness。它也不能修复客户边界未发布、未采样接口或错误方向分类 |
| `interface_reconciliation.go` 已交付 | **未交付** | 当前仍为未跟踪工作树文件，不能列为生产能力或门禁已通过 |
| LKG 用当前时间验签导致旧 key 退役后无法恢复 | **仍成立** | LKG 应验证“签发时有效 + 内容/签名可信”，再受本地过期/撤销策略约束；不能用当前信任窗口否定曾合法安装的最后已知好版本 |
| MaxConns 应 ≥ partitions | **原建议过度简化** | 连接数由 CH 并发容量、块延迟、内存和 partition pipeline 共同决定；盲目等于分区数会把排队转移到 CH merge/内存 |
| loopback 一律关闭 LZ4 | **未经证明** | 必须 benchmark CPU、拷贝、网络和 CH 解压；不能从地址是 127.0.0.1 直接推导关闭压缩更快 |
| 默认 sockets=CPU、`recvmmsg`、原地 protobuf | **性能候选，不是 P0** | 先修可证明的丢数和正确性，再以 pprof/alloc/pps 基准决定。`SO_REUSEPORT` 还需验证 exporter 源端口变化与模板状态归属 |
| 200–280k records/s/core、1M dps | **推理值，未验证** | 只能作为压测假设，禁止用于容量承诺 |

### 修正后的优先级

1. **P0 分类发布闭环**：客户 CIDR 行本身作为编辑权威，保存事务后计算规范化 `source_revision`，响应必须明确“草稿已保存、需要发布”，不新增第二套 dirty 状态机；管理员显式选择目标 worker/device；operation job 固定 source revision 后异步构建 `WADS + policy + device-boundary`；worker 下载、校验、先持久化 LKG 再原子热切换；ACK `downloaded/verified/installed/failed`；UI 以目标 worker 对同一 generation/checksum 的 `installed` ACK 为唯一“已生效”依据。发布后按新 `classification_version` 验证 `transit` 占比和七牛 v4/v6 命中。
2. **P0 采集正确性**：topic metadata、有效 socket buffer、publish 恒等式、序列 gap、Kafka lag、receipt reconciliation；任何一项缺失都不能宣称“零丢失”。
3. **P0 worker 毒丸**：零字段模板前置拒绝；永久/暂时错误分类；按 partition 暂停；进程级夹具证明一条毒报文后其它分区继续。
4. **P1 幂等/恢复**：确定性 Kafka 坐标块、去重窗口/复制表、facts→counters→receipt→offset 顺序、每个崩溃点重放；LKG 密钥退役恢复演练。
5. **P2 性能**：在 correctness gates 通过后再做 pipeline、列构建器复用、`recvmmsg`、socket 数、连接池拆分；每项必须有前后基准和回退开关。

### 必须新增的回归门禁

- 同一设备/端口/5m 桶的 SNMP counter、sFlow counter、packet estimate 三方表；reset/gap/方向/采样 coverage 均有明确状态。
- 一份包含 IPv4、原生 IPv6、IPv4-mapped IPv6 的客户边界发布到指定 worker，ACK installed 后，新事件分别分类为 in/out/internal，不得 transit；未选 worker 时 UI 必须显示“未发布”，不能显示成功。
- 六分类总量 + 明确排除项（internal/transit/ambiguous/drop/invalid）= eligible total；分类版本切换前后可按版本解释。
- collector 在 topic 缺失、broker 不可写、socket buffer 不足时 readiness 失败或明显降级；received、published、failed、buffered 计数守恒。
- worker 对零字段模板、未知 ODID、未来时钟、版本暂缺和大 datagram 分别验证“拒绝并推进”或“仅暂停该 partition”，不得 OOM/整进程循环重启。

后文保留为 2026-09-19 原始审查记录；与本节冲突时，以本节为准。

---

## 0. 客观基线

| 项 | 结果 |
|---|---|
| 构建 / vet | `watchdog-flow-collect`、`watchdog-flow-worker` 均 BUILD OK；`go vet ./internal/flowstream/ ./internal/flowworker/` 无告警 |
| 单测覆盖率 | flowstream **67.7%**、flowworker **76.3%**、flowch **65.1%**、flowplan **65.5%**（集成测试无 DSN 自动跳过） |
| race | `go test -race` flowstream + flowworker：**clean** |
| fuzz | 仅 3 个目标：`FuzzInspectFlowProtocolNeverPanics`（协议探测）、`FuzzDecodeAdapterNeverPanics` / `FuzzDecodeAdapterCounterConservation`（adapter `Map`）；**手写 sFlow v5 / NetFlow v5 / RawFlow 快路径解析器无任何 fuzz 目标，无提交语料** |
| 依赖 | goflow2 v3（2026-05-28 伪版本）、franz-go 1.21.3、ch-go 0.72.0、bart 0.28.0、x/sys 0.47.0；本机未安装 `govulncheck` / `gosec` / `staticcheck` |
| 生产实测（引自 perf-audit §8，CH 24.9.2.42） | `flow_records` 45.3M 行 / 2.66GiB / 25 parts；receipts 11.1M 行无 TTL；1m/1h 聚合 **0 行**；facts/receipts 各 ≈60 INSERT/分钟，**平均 1,400 行/块（P95 1,694）**，50k 上限从未达到；`non_replicated_deduplication_window=0`，2026-09-17 分区物理行比 `FINAL` 多 **2,393,402**；产品查询单次读 3,500–4,740 万行 / 1.2–2.3GiB，最慢类 P95 **10.3s**；两次真实 `MEMORY_LIMIT_EXCEEDED`；单设备 24h = 66.2M 事实 / 18.4M 不同目的 IP；对账 `FINAL` 读 314,037 行 vs 投影 8,192 行（**38×**） |

---

## 1. 执行摘要

**总体判断：** 投递核心（同步持久化后才提交 offset、回执契约、`(stream,partition,offset,record_index)` 身份、ed25519 信任链、零拷贝契约）设计正确且有测试；问题集中在**四类**，按"今天就会咬人"到"IX 规模才会咬人"排序：

1. **三个进程级崩溃/毒丸路径（2 Critical + 1 High）**
   - §4 C1：一个 v9/IPFIX **零字段模板**（RFC 7011 模板撤回 + UDP 乱序即可非恶意触发）让 GoFlow2 `DecodeDataSet` 死循环并无界 `append` → worker **OOM → 重启 → 重放 → 再 OOM**，永久毒丸。无解码超时、无 `MemoryMax`。
   - §3 C1：worker 把**数据形状错误**（事件时间 >5min 未来偏差、早于首版本、采集器按前缀准入但 worker 要求 ODID 匹配、>1024 记录）当作"可重试"→ **退出整进程**，5s 崩溃循环，rebalance 后下一个 worker 在同一 offset 再死。
   - §2 H2：采集器签名源计划**只在启动加载**，到期后任何重启 = 崩溃循环 + 100% UDP 静默丢失（未跟踪的 `cmd/flow-plan-resign/` 证明已经发生过）。
2. **静默丢失 / 无法证明"零丢失"（6 High/Medium）**：Kafka topic 缺失不校验（§2 H3，100% 丢且进程"健康"）；QinQ `0x88a8` 已证实会离开快路径，但是否最终 `mapping_rejected` 必须由真实 pcap 的 fallback 结果证明（§4 H1 + §3 M8）；同步 `Send` 失败 4 处不计数、`OnPublishError` 未接线（§2 M3）；模板重放默认改 0 后每次重启把模板刷新前的 NetFlow **已提交地**记为 `template_missing`（§3 M6，设计文档要求为正）；倍率未知曾使 `estimated_bytes=0` 静默排除，但生产当前约一半差异已经定位为客户边界未完成重发布、约 53% 被归为 `transit`，不能继续把它归因于倍率；BGP 属性/MPLS 在 `DecodedRecord` 边界丢弃（§4 H2）。
3. **幂等/一致性假设不成立（4 High）**：模板重放对 floor 以下记录**重跑持久化处理器**并以更大 generation 的 `template_missing` 回执压过 `persisted`（§3 H1，现有集成测试把该行为固化）；去重 token 编码块边界不可复现 + `non_replicated_deduplication_window=0`（§3 H2/§5 CH2，生产 09-17 分区 2,393,402 物理重复，所有读者付 `FINAL`）；版本选择无 valid-until + `effective_from` 可回溯（§3 H4）；LKG 恢复用 `now` 验签 → 密钥退役让所有 worker 无法启动（§3 M4）。
4. **IX 规模吞吐天花板（结构性，非 bug）**：采集器单 socket 一次 `recvmsg`/datagram + 11–13 次分配 + 5 次拷贝 + franz-go 全局锁 → ≈200–250k pps/socket，`-sockets` 默认 1；`SO_RCVBUF` 被 `rmem_max` 钳到 ≈200KB（§2 H1）；每 exporter 钉死一个分区、dev topic 12 分区（§2 M11）；worker 分区内解码→丰富化→构建→插入**零重叠** + 每 fetch 屏障 + 每 poll 同步提交（§3 H3/§5 HP1）；每块 2–3 次 `Do` + ~100 列对象与 30 个 LC 字典每块重建（§5 CH3/CH4，估 30–45% CPU）；每 flow 记录 3 次堆分配 + ≈4KB 按值结构体搬运，`EnrichBatchInto` 已写好但无生产调用（§5 HP3/HP4）；hub 一个 8 连接池同时服务交互查询与 5 个长作业、排队等待藏在 2 分钟超时里且无指标（§5 CH1）。

**历史数量（不可作为当前完成度）：** 原审查统计 Critical 2、High 11（去重后）、Medium ≈31、Low ≈27；标 ✅ 只表示当时按 file:line 看过，不表示已经修复或提交。当前工作树与生产部署已漂移，完成度必须逐项用 Git、迁移账本、运行时 schema、ACK 和测试结果重新核销。三个手写线格式解析器仍缺完整 fuzz/pcap 门禁。

**建议顺序**（详见本次复核结论与 §6）：先完成客户边界 dirty → 定向 deployment → installed ACK 的产品正确性闭环；同时修会导致进程崩溃或静默丢失的 Phase 0 小项；随后修幂等/一致性，最后才进入 IX 吞吐工程。**不要**用扩大倍率、纳入 `transit` 或调大 `blockMaxRows` 掩盖当前分类错误，**不要**在补零拷贝回归测试之前启用 franz-go `WithPools`。

---

## 2. flow-collector（`cmd/watchdog-flow-collect` + `internal/flowstream` 接收/发布路径）

### 2.1 发现

| # | 严 | 位置 | 发现 | 影响 | 修复 | 复核 |
|---|---|---|---|---|---|---|
| H1 | H | [receiver.go:60-63](../internal/flowstream/receiver.go)、[main.go:63](../cmd/watchdog-flow-collect/main.go)、systemd unit | `SetReadBuffer(32MiB)` 只发 `SO_RCVBUF`，Linux 静默钳到 `net.core.rmem_max`（生产已证实为 212992B）；无 `getsockopt` 回读、无日志、仓库零处 `rmem_max` | 实际队列 ≈140 个 1500B 报文（100k dps 下 ≈1.4ms）；任何 GC 停顿或 Produce 阻塞即丢包，而运维以为有 32MiB | 回读有效 `SO_RCVBUF`，不足则 readiness/指标告警；部署提供并核验 `sysctl.d`。`SO_RCVBUFFORCE`/`CAP_NET_ADMIN` 仅作受控环境备选，不默认提权 | ✅ |
| H2 | H | [main.go:133](../cmd/watchdog-flow-collect/main.go)、[flowplan/plan.go:104-111](../internal/flowplan/plan.go)、unit `Restart=on-failure/5s` | 签名源计划只在启动加载一次：运行中不检查到期、不重载、无源计划 LKG（agentplan 的 LKG 只管进程参数）。运行中 fail-open，重启时 fail-closed。未跟踪的 `cmd/flow-plan-resign/`（把 `ExpiresAt` 延 365 天）说明已经踩过 | 到期后任何重启（SIGTERM、计划变更、崩溃）= 每 5s 崩溃循环、100% UDP 丢失且**不计数**；新增来源也要重启 | ticker/inotify 重载进 `atomic.Pointer[*Registry]`，逐报文打 `RegistryVersion`；重载失败保留上一有效注册表；导出 `plan_expires_at_seconds`、提前告警 | ✅ |
| H3 | H | [producer.go:60-72](../internal/flowstream/producer.go)、[main.go:167](../cmd/watchdog-flow-collect/main.go) `Ping` 仅 broker 元数据 | 从不校验 topic 存在/分区数（自动建 topic 关闭，topic 名自动加 `-v1` 后缀）；franz-go 默认 4 次未知 topic 重试后记录失败 | topic 缺失或拼错 → **静默 100% 丢失**，进程看起来健康，只有 `kafka_publish_errors_total` 变化 | 启动发 Metadata 校验 topic、分区数与关键配置并 fail-fast；运行中用健康状态暴露不可写。禁止用无限 UnknownTopic 重试伪装健康 | ✅ |
| M1 | M | [producer.go:61](../internal/flowstream/producer.go)、[rawflow.go:102-109](../internal/flowstream/rawflow.go) | 只按记录数限制缓冲（`MaxBufferedRecords`），无 `MaxBufferedBytes`；编码器池保留增长后的缓冲 | broker 故障 + 大报文时最坏 65536×65KiB ≈ 4.3GiB → OOM（unit 无 `MemoryMax`） | 加 `kgo.MaxBufferedBytes` + unit `MemoryMax`/`GOMEMLIMIT` | ◐ |
| M2 | M | [producer.go:127](../internal/flowstream/producer.go) `Produce(context.Background())`；franz-go 全局缓冲计数 | 跨 exporter 队头阻塞：一个分区不可写填满全局缓冲后**所有**接收 goroutine 停在 Produce，所有监听都停止排空内核队列 | 一个坏分区 → 上百无关 exporter 内核丢包 | 加字节上限、拥塞/停收指标和隔离评估；必要时按 listener/client 隔离。`TryProduce` 主动丢弃不是默认修复，除非产品明确接受并记录丢失窗口 | ◐ |
| M3 | M | [main.go:212-227](../cmd/watchdog-flow-collect/main.go) 未接 `OnPublishError`；[producer.go:100-130](../internal/flowstream/producer.go) 四处早返回只 `finish(err)` 不计数 | 同步 `Send` 失败**无处计数**（关闭竞争窗口、`NewRawFlow` 校验失败） | `received = kafka_records + publish_errors + buffered` 恒等式不成立，无法用计数器证明"无丢失" | `Send` 每条失败路径 `stats.errors.Add(1)`（或接线 `OnPublishError`）+ 恒等式测试 | ✅ |
| M4 | M | [main.go:156-164](../cmd/watchdog-flow-collect/main.go)、[producer.go:161-165](../internal/flowstream/producer.go) | 关机时 30s flush 超时后丢弃最多 `queueSize` 条，只进 `stats.errors`，既不打日志也已不可抓取 | 未确认窗口事后不可知；ADR 承诺的"明确未确认区间"未实现 | `Close` 末尾记录 `producer.Stats()`；`-shutdown-flush-timeout` | ◐ |
| M5 | M | [main.go:108-117, 261-265](../cmd/watchdog-flow-collect/main.go) | 任何 agent 计划变更 → `ErrPlanChanged` → exit 1 → 5s 重启；端口未绑定期间报文被 ICMP 拒绝且**无任何计数** | 每次控制面变更在 IX 速率下丢 0.5–5M 报文且不可见；叠加 H2 可能变成崩溃循环 | 进程内应用变更（`SO_REUSEPORT` 起新收器再停旧）；至少该路径 `RestartSec=0` + `restarts_total` | ◐ |
| M6 | M | [flowplan/plan.go:326-348](../internal/flowplan/plan.go) | 准入是 O(bindings) 线性扫描，NetFlow 端口对被拒报文 ×3 | 上千绑定时每报文微秒级；未准入洪泛的廉价 CPU 放大点 | 按协议编译前缀树（LPM）/精确主机 map | ◐ |
| M7 | M | [rawflow.go:62-99](../internal/flowstream/rawflow.go)、[producer.go:91-127](../internal/flowstream/producer.go)、[receiver.go:73-141](../internal/flowstream/receiver.go) | 每报文 ≈8 次堆分配 + 2 次 key 构造（`inlet.go:56` 与 `Marshal` 各算一次）+ 1 次 `recvmsg`（无 `recvmmsg`）；64KiB 池缓冲每次 GC 被丢弃重建 | 500k–1M dps 下每秒数百万分配与接收循环争 CPU | 每 goroutine 复用接收缓冲/`RawFlow`/key 定长数组；去掉每报文 `sync.Once`；`recvmmsg` | ◐（详见 §5） |
| M8 | M | [producer.go:60-64](../internal/flowstream/producer.go) 无 `kgo.WithLogger` | franz-go 完全静默：断连、重试、`NOT_ENOUGH_REPLICAS`、未知 topic、阻塞 Produce 零日志 | 内核丢包尖峰无法与 Kafka 事件关联 | `kgo.WithLogger(BasicLogger(Warn))` 限速 | ◐ |
| M9 | M | [receiver.go:82-88](../internal/flowstream/receiver.go)、[flowmetrics/metrics.go:97](../internal/flowmetrics/metrics.go)、`receiver_rxq_other.go` | 丢包数只在**下一次成功读取**的 cmsg 里出现（Produce 阻塞期间不可见）；单一无标签 `kernel_drops_total`（分不清 sFlow/NetFlow）；只覆盖 socket 队列溢出；非 Linux 恒为 0 且无警告 | "证明无丢失"需外部 `nstat`/网卡计数；macOS/BSD 不可能 | 加 `listener` 标签；独立轮询 `/proc/net/udp` drops；非 Linux 启动警告 | ◐ |
| M10 | M | [main.go:213](../cmd/watchdog-flow-collect/main.go) 取 `plan.CollectorID`，不与 `-agent-id` 比对；[signature.go:158](../internal/flowplan/signature.go) 接受 v1 信封 | 把 B 的计划文件放到主机 A，A 就以 B 身份发布（同 Kafka key、同 registry 命名空间） | 数据归错采集器、跨主机重复身份 | 启动要求 `plan.CollectorID == agentID`；生产拒绝 v1 信封 | ✅ |
| M11 | M（设计/IX） | [rawflow.go:75-91](../internal/flowstream/rawflow.go) key=`collector‖0‖exporter_ip`；dev topic 12 分区；ADR 禁止原地扩分区 | 每个 exporter 钉死一个分区，大 exporter 撞分区近乎必然；worker 单分区串行 → 热分区封顶解码吞吐 | 无法事后修复（需 topic 版本升级） | `-v1` 预留 ≥64 分区；sFlow（无模板状态）可按 `sub_agent_id` 子键 | — |
| M12 | M | [config.go:140-151, 258-263](../internal/flowstream/config.go) | SASL `plain` 在 `TLS.Enabled=false` 时被接受 | Kafka 链路明文凭据 | 拒绝无 TLS 的 PLAIN | ✅ |
| L1–L10 | L | 见审查原文 | `-check` 有副作用（注册/写 LKG/消费 enrollment）；生产不做 4 字节版本检查、`0x00000005` 歧义；同 UID 可加入 reuseport 组劫持数据；包非 Linux 不可构建/`-sockets>1` 在 BSD 不负载均衡；关机丢内核已排队报文不计数；第二个信号被吞；**文档漂移**（install 文档称采集器"解码"、设计文档写 YAML `sockets: 8`/`max_buffered_records: 262144`，二进制实为纯 flag，默认 `-sockets 1`/队列 65536）；unit 缺 `Wants=network-online`/`LimitNOFILE`/`MemoryMax`/`CPUAffinity`、metrics 默认 9090 与 Prometheus 冲突；每次抓取 STW `ReadMemStats`；TrustStore 未接线 | | | L7/L8 ✅ 其余 ◐ |

### 2.2 IX 规模视角（估算，未实测）
单 socket goroutine 每报文 ≈ 1 `recvmsg`（1–2µs）+ 准入扫描 + 信封/序列化（~0.3–0.5µs）+ `Produce`（全局锁 + 分区锁 ~0.5–1µs）+ ~8 次分配 → **~150–300k dps/socket 上限**，实际 100–200k；单 exporter 固定四元组永远落一个 socket → 单 exporter 封顶同量级（sFlow 1:8192 下每 exporter 10–20k dps 足够，未采样/1:100 NetFlow 不够）。要到 1M dps：先修 H1 与 M7，`-sockets` ≥ 核数、`recvmmsg`、NIC RSS/IRQ 亲和 + NUMA 绑定、每监听器独立 franz-go client、`MaxBufferedBytes` 按秒级摄入定容、分区 ≥64、LPM 准入——之后瓶颈转到 worker 热分区解码。

### 2.3 做对的
热路径严格 = 接收→前缀准入→信封→Kafka（无解码/DB/HTTP/文件）；缓冲所有权契约正确（同步拷贝、池缓冲由 Kafka promise 归还）；投递与接收上下文解耦；F5 关机死锁修复正确；`SO_RXQ_OVFL` 回绕安全；准入 fail-closed、Ed25519 v2 信封绑定采集器/版本/哈希/有效期；Kafka 默认安全（acks=all、幂等、无限重试、murmur2 按 key）；secrets 只走文件；四元组 `SO_REUSEPORT` 保序保住 NetFlow/IPFIX 模板安全；指标固定基数并由测试锁定。

---

## 3. flow-worker（消费 → 解码 → 分类 → 成块 → 写入）

> 并行会话进行中的两组改动已纳入：① 投递重写（`AutoCommitMarks`+1s 自动提交 → `DisableAutoCommit` + 每次 poll 显式 `CommitRecords`，分块提交，**模板重放默认从 1,000,000 改为 0**）；② `EstimatedBytesScalePPM` 端到端接线（但 `Ready()` 未加新列，见 M3）。

### 3.1 发现

| # | 严 | 位置 | 发现 | 影响 | 修复 | 复核 |
|---|---|---|---|---|---|---|
| C1 | **C** | [consumer.go:284-288](../internal/flowstream/consumer.go) → [main.go:307-317,146-148](../cmd/watchdog-flow-worker/main.go)；unit `Restart=on-failure` | 任何"可重试"处理错误**退出整个进程**，没有按分区暂停/跳过（`enrich.go:135-137` 承诺 `VersionBlockedError` "暂停分区"，无实现）。数据相关触发：(a) 事件时间 > 接收+5min（`defaultMaxFutureSkew=5m`，`enrich_test` 已把 +6min 固化为 "future skew" 错误）——NetFlow 用 exporter 时钟；(b) 事件时间早于首个已安装版本（无过去偏差上界）；(c) **观测域不匹配**：采集器只按源前缀准入（`AdmitSource` 不看 ODID），worker `Admit` 在绑定钉了 ODID 时拒绝 → `ErrBindingUnavailable` → 可重试；RawFlow 钉死 `registry_version`，发布修正计划也救不了已入队报文；(d) `--bootstrap-plan` 文件缺对应 revision（无运行时计划同步）；(e) 单报文 >1024 条记录 | 一条坏报文（6 分钟时钟偏差、未登记 ODID、巨型报文）→ 5s 崩溃循环，本 worker 全部分区停滞；rebalance 后下一个 worker 在同一 offset 再死。(c)/(d) 不手工跳 offset 不可恢复（= 丢数据）。每次重启重做 LKG 验签 + WADS 编译（最大 512MiB） | (a)(c)(e)：改为回执 `mapping_rejected`（带原因）或钳制 + quality 位，**数据形状错误永远不进可重试类**；(b)(d)：实现真正的按分区暂停（返回 `(0,nil)` 保持其它分区流动，`paused_partitions` 指标）；加对称的过去偏差上界；采集器与 worker 用同一 `Admit`（含 ODID），未绑定报文不进 Kafka | ✅（触发 (a)(c) 与退出路径 `consumer.go:284-288 → main.go:307-317` 均核对；`processor.go:155-156` 非 `ErrDecodedFlowInvalid` 的映射错误全部计入 retryable 并上抛） |
| H1 | H | [consumer.go:336,343](../internal/flowstream/consumer.go)、[processor.go:138-140,162-169](../internal/flowworker/processor.go)、[native.go:701-704](../internal/flowch/native.go)、[decoder.go:296](../internal/flowstream/decoder.go)、DDL 011:199-201 | 模板重放对 floor 以下记录**重跑持久化处理器**（只是不提交）。重解码时模板已不在窗口内 → 变成 `template_missing` 回执，其 generation = Kafka `record.Timestamp`（生产者毫秒）**≥** 已持久化回执的 generation（`RawFlow.TimeReceived` 整秒）→ `ReplacingMergeTree(generation)` 保留 `template_missing` 行。`kafka_replay_integration_test.go:54-58` 断言重放后 offset [1,3] 再次被解码，把该行为固化 | 每次启用重放的重启：事实物理重复（只靠 `FINAL` 收敛），**已持久化消息的回执被翻成零计数**→ 对账 `count_mismatch`/`missing_receipt` 假阳性；违反 V2 §2.3"有且只有一行" | floor 以下只解码不 `handleGroup`（无回执无插入）；回执 generation 与持久化行同源（`DecodeRecord` 在 `ErrorTemplateNotFound` 时返回 `ReceivedAt`），且回执型 disposition 永远不得压过 `persisted` | ✅（`consumer.go:336` 对全部记录跑处理器、`:343` 才按 floor 过滤提交；`processor.go:138-140` 模板缺失回执传 `time.Time{}` → `:164-165` 取 Kafka `record.Timestamp`；`native.go:701-704` generation 取该毫秒值） |
| H2 | H | [native.go:244-247,256](../internal/flowch/native.go)、[batch.go:173,239](../internal/flowch/batch.go)、DDL 011/018 | 去重 token 编码块边界（取决于 fetch 大小、分块位置、字节预算——均可由计划改变），重放几乎不可复现；且无表设置 `non_replicated_deduplication_window`（生产实测 = 0，09-17 分区 2,393,402 物理重复） | 块级去重是空操作；重复行直到 merge 都真实存在，所有读者付 `FINAL`；V2 "首尾坐标 token" 假设不成立 | 按确定性 offset 桶切块（token=`(stream,partition,bucket)`），再按峰值块/秒×最长重试定容窗口；否则去掉 token 并把 `FINAL` 明文写成唯一收敛机制 | ✅（perf-audit R2） |
| H3 | H | [consumer.go:327-370](../internal/flowstream/consumer.go)、[writer.go:168-197](../internal/flowch/writer.go)、[native.go:66-68](../internal/flowch/native.go)、franz-go `RebalanceTimeout` 60s、[enrich.go:84-133](../internal/flowworker/enrich.go)、[pipeline.go:103](../internal/flowch/pipeline.go) | 每次 fetch 屏障：每分区一 goroutine，`wait.Wait()` 后才提交/下一次 poll；分区内解码→分类→成块→插入全串行；重试预算 2min × 操作超时 2min 可让屏障持续 ~4min，`BlockRebalanceOnPoll` 期间被踢出组（60s）→ 后续 `CommitRecords` 失败 → 退出 + 重复。`chpool MaxConns=8` 被 N 个分区共享；`EnrichedRecord` ≈1KB 按值传递、`EnrichBatch` 每 datagram 分配，`EnrichBatchInto` 无生产调用 | **IX 下先崩的顺序**：单分区串行上限（一个大 exporter ≈50–150k rec/s，与核数无关）→ 屏障 = 最慢分区 → GC（每分区每块 25–160MB 瞬时）→ `MaxConns=8` 封顶插入并发 → part 率（生产 P95 1,694 行/块，~120 part/min，远低于 50k 上限）。要到 1M/s，分区数 ≥ 核数 ≥ ~10 且完美均衡 | 每分区 worker goroutine + 有界队列，解码/分类与插入流水线化，按分区提交，revoke 时排空；`MaxConns ≥ 分区数`；切到 `EnrichBatchInto`；热 exporter 按 ODID/接口分片；重试预算 < `RebalanceTimeout` | ✅（perf-audit R6） |
| H4 | H | [version_catalog.go:224-230](../internal/flowworker/version_catalog.go)、[flow_enrichment_publications.go:673](../internal/server/flow_enrichment_publications.go)、[native.go:410](../internal/flowch/native.go) | 版本选择 = `effective_from ≤ event_time` 的最新者，**无 valid-until**；控制面只要求 `effective_from` 晚于上一版（允许相对 now 回溯）；同步失败时 worker 继续用陈旧 LKG | 回溯发布前已处理的事件用版本 N，安装后重放得到 N+1 但 `ingest_generation` 相同 → merge 后赢家随机；控制面不可达期间静默误分类；违反 V2 §2.2"重放不可漂移" | 控制面：`effective_from ≥ now + 激活延迟 ≥ 最大 worker 滞后`；worker：同步失败超过陈旧上界后对越界事件时间 fail-closed（暂停）；或把 `classification_version` 折进 `ingest_generation` | ✅ |
| M1 | M | [barrier.go:65-79,170-176](../internal/flowtombstone/barrier.go)、[pipeline.go:137-144](../internal/flowch/pipeline.go) | 装了 barrier 后**每条记录**跑 `Guard.Covers`：拷贝 barrier（含 `ExceptionDays` 切片）+ `Validate()` + `Format(DateOnly)`；包文档却写"一个 atomic 指针 + UTC 日期比较" | raw 删除一开启即热路径 CPU/分配爆炸（潜伏） | `Install` 时预计算 `deletedThroughDay int` + 排序 `[]int`；比较 `EventTimeUnixMS/86_400_000`；按批用 min/max 事件时间判一次 | ✅ |
| M2 | M | [pipeline.go:92-101](../internal/flowch/pipeline.go)、[quarantine.go:74,84](../internal/flowch/quarantine.go)、[main.go:225-256](../cmd/watchdog-flow-worker/main.go) | 一条被覆盖记录隔离**整个 datagram**（非删除日的记录也进不了 `flow_records`）；每个隔离 datagram = 2 次同步单行插入；文件引导（无 `--control-plane-url`）的 worker **根本没有 barrier** 且从不 ACK | 跨 barrier 重放 = part 风暴 + 相邻日事实丢失；文件模式 worker 静默重插已删除日 | 按记录隔离；隔离行合批；CH 有 raw-delete 证据时无 barrier 拒绝启动 | ✅ 文件引导部分（`main.go:225-230` barrier 仅在 `versionSync != nil` 时加载，`:252-256` 否则 `NewPipeline` 无守卫）；整 datagram 隔离 ◐ |
| M3 | M | [native.go:148-151,191 vs 523](../internal/flowch/native.go) | `Ready()` 仍只要求 5 个记录列，而插入已写 `estimated_bytes_scale_ppm`（未跟踪迁移 019） | worker 先于 019 升级：readiness 通过，随后每次插入永久 `NO_SUCH_COLUMN_IN_TABLE` → 崩溃循环而非干净的 readiness 失败 | readiness 列表加该列 + 更新 `native_test.go:97` | ✅（`native.go:149-151` 仅 5 列、`:191` 断言 `==5`；`:523` 插入已含 `estimated_bytes_scale_ppm`） |
| M4 | M | [version_lkg.go:316](../internal/flowworker/version_lkg.go)、[version_signature.go:92](../internal/flowworker/version_signature.go)、[trust.go:207-208](../internal/flowplan/trust.go) | LKG 恢复用 `now` 验签历史发布；退役中的密钥过 `trust_until` 即失效；任一旧发布用过期密钥签 → `Restore` 整体中止 → worker 拒绝启动；清 LKG 从 cursor 0 重拉得到同样结果；无发布重签工具 | 签名密钥退役可让**所有** worker 无法启动直到控制面重签历史 | 历史信封按 `SignedAtUnixMilli` 验签（仍尊重吊销），或控制面轮换时重签发布 | ✅ |
| M5 | M | [consumer.go:261-269](../internal/flowstream/consumer.go) | 任何 `fetches.Errors()` 致命退出；franz-go 文档称"typed errors 通常可重试" | 瞬时 broker 事件 → 重启风暴 + 重引导 | `kerr.IsRetriable` 的分区错误继续（日志+指标）；仅 auth/未知 topic/数据丢失类退出 | ✅（`consumer.go:261-269` 任何 fetch 错误 `return errors.Join(...)`） |
| M6 | M | [main.go:115](../cmd/watchdog-flow-worker/main.go)、[config.go:108-111](../internal/flowstream/config.go)、TTL 30m | 重放默认已改为 **0**：每次重启/rebalance 后，NetFlow v9/IPFIX 在下一次模板刷新前的数据被提交为 `template_missing`（回执型）；模板刷新慢于 30min 的 exporter 也丢。设计文档 [flow-module-design.md:210](flow-module-design.md) 明确要求该值"必须为正且有硬上限"；仓库无任何部署/文档设置该 flag | 静默、已提交的丢失，只在 `template_missing_total` 可见 | 持久化每分区模板状态，或对 NetFlow 绑定强制该 flag 并对 `template_missing_total` 速率告警 | ✅ |
| M7 | M | [main.go:313-317](../cmd/watchdog-flow-worker/main.go)、[native.go:215-240](../internal/flowch/native.go)、[consumer.go:272-277](../internal/flowstream/consumer.go) | SIGTERM 用同一取消 ctx 中止进行中的插入（只有提交有新 10s ctx）；records→counters→receipts 三次非原子插入 | 关机中断块：记录无回执直到重放（`missing_receipt` 假窗口）且重启必重复插入 | 给进行中处理器一个 ≤`TimeoutStopSec` 的宽限 ctx | ◐ |
| M8 | M | [main.go:260](../cmd/watchdog-flow-worker/main.go) `nil` observer、[processor.go:196-200](../internal/flowworker/processor.go) | 被拒报文只计数，不记 offset/原因 | 解码/映射拒绝风暴无法诊断（H1 的 QinQ 100% 丢失就只会表现为一个数字） | 限速 observer 日志（topic/partition/offset/原因） | ✅（`main.go:260` 传 `nil`；`processor.go:196-201` 仅 `rejected.Add(1)`） |
| M9 | M | [decode_adapter.go:281-288](../internal/flowworker/decode_adapter.go)、[batch.go:196-206](../internal/flowch/batch.go) | 倍率未知 → `estimated_valid=false`、`estimated_bytes=0`，`sum(estimated_bytes)` 静默排除 | 除非每个查询都显示有效覆盖率，否则系统性低报（与"估算远低于 SNMP"直接相关） | 查询层必须报覆盖率；估算列考虑 `NULL` | ✅ |
| L1–L7 | L | 见审查原文 | 入/出接口都绑定时 `observation_if_index=0`；WorkerID 上限 http 26 vs loader 128（✅）；`committable` 每记录取锁；被撤销分区的解码器不驱逐（✅ `decoder.go:491-521` 只增不删、`:523-544` 仅 `Close()` 删除；`consumer.go:112-117` revoke/lost 钩子只更新 stats——模板存储/pipe goroutine 随 rebalance 累积）；回执 `inserted_at` 秒级 vs 毫秒级可能跨月分区导致 `FINAL` 不合并；unit 无 `TimeoutStopSec`/`StartLimit*`/`LimitNOFILE`（✅） | | | |

### 3.2 做对的
投递核心正确：`DisableAutoCommit` + 同步持久化后才 `CommitRecords`，健康分区在同伴失败时仍提交，分块提交持久化前缀，`BlockRebalanceOnPoll` 直到提交完成，关机提交用新 10s ctx；回执契约（每消息一条、与切块无关、回执型零计数并在 IO 前校验）；身份契约（`(stream,partition,offset,record_index)` 连续、`--source-stream-id` 必填、`ingest_generation` 取自 RawFlow 接收时间使重试字节相同）；解码层毒报文处理（goflow2 panic 捕获，malformed → `decode_rejected`，不可映射 → `mapping_rejected`，都不停分区）；版本信任链（canonical JSON + ed25519 + 单调 trust bundle，页内严格递增，原子 pair 安装、先持久化后发布，LKG 全有或全无，拒绝重定向、流式 SHA-256 + 大小上界 + 内容寻址硬链接，事件时间选版从不回退"当前"）；barrier 单调 revision + 原子 fsync LKG + 先安装后 ACK；IPv4-mapped IPv6 在 adapter/enricher/两个分类器/plan admit 全部 `Unmap()`；启动 exactly-one-of token/mTLS、明文仅 loopback、TLS ≥1.2；计数器算术全程防溢出；已有测试覆盖部分提交、同伴隔离、提交失败、floor 不回退、版本阻塞不提交（真实 Kafka）、重启重放（真实 Kafka）、writer 重试/预算/永久错误、块确定性、LKG 全有或全无、ACK 重试不重下载、重定向拒绝。

### 3.3 测试缺口
两成员 rebalance/revoke 中途 + 重复计数；records 与 receipts 插入之间崩溃 + `FINAL` 收敛；floor 以下重放的回执结果（现有测试断言的是有害行为）；C1 各毒类在进程级；SIGTERM 中途插入；去重 token 对设了窗口的 CH 是否有效；barrier 热路径分配；每 fetch 内存上界；端到端吞吐（只有 `BenchmarkEnrichBatch1024RecordsReused`）。

---

## 4. 解码器（sFlow v5 / NetFlow v5 快路径 + GoFlow2 回落）

> 生产路径 = 手写快路径 `netflow_v5_fast.go` / `sflow_v5_fast.go` / `rawflow_fast.go`；NetFlow v9/IPFIX、sFlow ExtendedGateway 及未处理构造回落到 goflow2 `v3.0.0-20260528…6dee964c38ee`（模块缓存，未 vendor）。解码器测试 `-race` 全过。

### 4.1 发现

| # | 严 | 位置 | 发现 | 影响 | 修复 | 复核 |
|---|---|---|---|---|---|---|
| C1 | **C** | goflow2 [netflow.go:169-173](file:///Users/chenliliang/go/pkg/mod/github.com/netsampler/goflow2/v3@v3.0.0-20260528232550-6dee964c38ee/decoders/netflow/netflow.go)（`int(uint16)<0` 死检查，`FieldCount=0` 被接受）→ [netflow.go:345-391](file:///Users/chenliliang/go/pkg/mod/github.com/netsampler/goflow2/v3@v3.0.0-20260528232550-6dee964c38ee/decoders/netflow/netflow.go) `AddTemplate` → `utils/store/templates/store.go:164-180`（**无校验直接 `Set`**）→ [netflow.go:278-295](file:///Users/chenliliang/go/pkg/mod/github.com/netsampler/goflow2/v3@v3.0.0-20260528232550-6dee964c38ee/decoders/netflow/netflow.go) `DecodeDataSet`：`listFieldsSize==0` 时 `for payload.Len() >= 0` 永真、每轮消耗 0 字节、`records = append(...)` 无界增长；`DecodeOptionsDataSet:258` 同形。worker 侧 [decoder.go:277-284](../internal/flowstream/decoder.go) `recoverDecoderPanic` **只捕获 panic**，[decoder.go:336-346](../internal/flowstream/decoder.go) 无解码超时 | 一个 v9/IPFIX **零字段模板**（RFC 7011 §8.1 模板撤回就是 `FieldCount=0` 的合法报文；所有字段 `Length=0` 的模板同样触发）后跟同 ID 的数据集 → 该分区 goroutine 死循环 + 堆无界增长 → **整进程 OOM**；unit 无 `MemoryMax`，被 OOM-kill 后重启，datagram 未提交 → 重放 → 再死：**永久毒丸，且 UDP 乱序（撤回先于最后一个数据集到达）即可非恶意触发**。v9/IPFIX 永远走 GoFlow2 回落，模板内容不校验，故可达 | 三道保险任选其二：(1) 数据集解码前拒绝 `GetTemplateSize==0` 且无变长字段的模板（本地包装 `TemplateStore.AddTemplate`，不必改上游）；(2) 要求前进：`payload.Len()` 未减少即 break（上游补丁 + 本地 fork/replace 直到合入）；(3) GoFlow2 解码跑在 watchdog 超时下（如 200ms），超时归入 `decode_rejected` 推进 offset。同时 unit 加 `MemoryMax` 把 OOM 变成可见的快速失败 | ✅ 全链路 |
| H1 | M（性能）/H（待证正确性） | [sflow_v5_fast.go:512-514](../internal/flowstream/sflow_v5_fast.go)（`default → false` 回落）；goflow2 `producer/proto/producer_packet.go:284-300` | 带 VLAN/QinQ 的样本会离开手写快路径；旧审查据此进一步断言整个 datagram 必然 `mapping_rejected`，但没有真实 pcap/回落结果证明 | 已证实快路径命中率下降；是否丢记录取决于 fallback 能否得到 L3/L4，不得写成 100% 丢失 | 增加 0x8100/0x88a8 单/双标签真实夹具，比较 fast/fallback 的地址、端口、VLAN、长度与拒绝回执；测试证明后再定严重度 | ◐ |
| H2 | H（IX） | [decoder.go:84-130](../internal/flowstream/decoder.go) `DecodedRecord`/`recordFromFlowMessage`；goflow2 `producer_sf.go:100-118`、`producer_nf.go:474-508` | sFlow ExtendedGateway 回落后 GoFlow2 产出 `AsPath`/`BgpCommunities`/`BgpNextHop`/`NextHopAs`，`DecodedRecord` **无对应字段**，只剩 `SrcAs/DstAs`；`NextHop` 为空（GoFlow2 写的是 `BgpNextHop`）。v9/IPFIX 的 MPLS 标签同样在此边界丢弃 | 按对等体/AS-path/community 的 IX 分析（peering 报表、路由泄露归因）不可能；只有源/目的 ASN | 若 per-peer 分析在范围内：`DecodedRecord` + 事实表增加 as_path/communities/bgp_next_hop(+mpls) 并在回落路径映射；否则在文档明示丢失 | ◐ |
| M1 | M | [sflow_v5_fast.go:466-471](../internal/flowstream/sflow_v5_fast.go) `copyPacketFields`；goflow2 `producer_packet.go:515` | 0x8100 帧回落后只拷 ExtendedSwitch 的 `SrcVlan/DstVlan`，GoFlow2 写在 `VlanId` 的 802.1Q 标签不进 `DecodedRecord` | 采样帧本身的 VLAN 丢失（IX 上 VLAN = 客户/peer 端口的重要键） | 携带 `VlanId`，或 ExtendedSwitch 缺失时以 `m.VlanId` 填 `SrcVlan` | ◐ |
| M2 | M | 三个快路径解析器 + `rawflow_fast.go`；仅 `decoder_test.go:383` 一个版本探测 fuzz | 对抗覆盖只有前缀截断（`sflow_v5_fast_test.go:201` 单语料、`netflow_v5_fast_test.go:65`）；`(*[N]byte)(c.buf[c.off:])` 批量读（`sflow_v5_fast.go:114,303,320,406,422`）的安全**完全依赖前置 `remaining()`**，无变异 fuzz 守卫 | 今天安全（逐处核对了 5 个 guard），但任何削弱 guard 的改动会在对抗输入上 panic 而无测试拦截；且 NetFlow v5 快路径 [decoder.go:309-316](../internal/flowstream/decoder.go) **不在** `recoverDecoderPanic` 内 | 为两个快解码器与信封解析器加 `go test` fuzz 目标，以现有夹具做种；v5 快路径也包进恢复 | ✅（基线已列） |
| L1 | L | [sflow_v5_fast.go:154-223](../internal/flowstream/sflow_v5_fast.go) vs `docs/flow-decode-fastpath.md:124`；`sflow_v5_fast_test.go:53-60` | 快路径已提取 generic-interface counters（`cf8b55eb`），文档仍写"跳过"；差分测试 `TestSFlowFastMatchesGoFlow2` 不比 `CounterRecords`（GoFlow2 producer 不映射 counters，无参考实现），counter 正确性只靠一份手写期望；Ethernet/VLAN/CPU counters 与 drop samples（format 5）仍丢 | counter 路径保证弱于 flow 记录；文档陈旧 | 更新文档；加独立 counter 参考比对（如用 sflowtool 输出做夹具） | ◐ |
| L2 | L | [sflow_v5_fast.go:311-312,328-329](../internal/flowstream/sflow_v5_fast.go)；goflow2 `producer_sf.go:55-61` | 接口字段按裸值读取，忽略 **format 位**（standard 高 2 位 / expanded 的 `inputIfFormat`）；format 1(discard)/2(multiple) 值被当作 ifIndex | 可能与真实 ifIndex 假匹配进入 `observation()`（[decode_adapter.go:242-261](../internal/flowworker/decode_adapter.go)）→ 接口归属错；与 GoFlow2 行为一致 | 解释 format：非 0 格式清零 ifIndex 并置 quality 位 | ◐ |
| L3 | L | goflow2 `utils/pipe.go:125` + [decoder.go:294-295](../internal/flowstream/decoder.go)；`netflow_sampling.go:55` | GoFlow2 模板/采样状态键 `RouterKey = Src.String()` **含 UDP 源端口**，而分区/`ExporterKey` 只用源 IP | exporter 在模板与数据报文间变换源端口 → 模板永远找不到（`template_missing`）；罕见，继承自 GoFlow2/Akvorado | 若观察到则把 RouterKey 归一为 exporter IP | ◐ |
| L4 | L | [decoder.go:104,150-152](../internal/flowstream/decoder.go)、`flowch/native.go:430`、`enrich.go:241` | `SequenceNum`/`DatagramSequence` 正确解出并落库，但**没有任何 gap/丢包检测**；fast-path 文档的"SequenceNum（丢包检测）"只是意图 | 无解码缺陷；exporter→collector 的丢包不可见（与"sFlow 估算偏低"排查直接相关） | 查询层按 (exporter, sub_agent) 做序列 gap 统计并出指标 | ◐ |
| I1 | 信息 | `sflow_v5_fast.go:137,140,350,353` | `int(uint32)` 在 32 位构建上可为负；`c.take` 拒绝 `n<0`，64 位目标完全安全 | 无 | 无（可移植性备注） | — |

### 4.2 做对的
快路径每一次读取都有前置长度守卫（逐处核对 `remaining()`/长度检查与 `(*[N]byte)` 配对：`:111-114,300-303,317-320,403-406,419-422`），`samplesCount`/`recordsCount` 钳 1000 与 GoFlow2 DDoS 守卫一致；NetFlow v5 计数钳制（`netflow_v5_fast.go:49-51`）比 GoFlow2 的幻影零记录更安全，且有测试锁定；字段映射与 GoFlow2 位级一致（v5 采样 `&0x3FFF`、uint32 时间戳回绕、IPv6 traffic-class、sFlow `SamplerAddress`=agent IP、`TimeFlowStart/End/Received`=接收时间）；零拷贝契约端到端成立（记录/地址只在下一次 `Decode` 前别名有效，worker 经 `canonicalAddress16` 深拷贝、标量按值拷贝；`recyclePending` 延迟归还；每次解码整体覆写记录避免旧字段泄漏）；分区隔离（每分区独立 `Decoder`/模板存储/采样存储，`entry.mu` 单 goroutine，`TestPartitionDecodersDoNotShareTemplateState`）；两个不可信解析路径的 panic 收敛为可跳过错误；信封解析器拒绝截断的长度前缀字段、身份驻留上限 1024；多采样器正确性（按 `(router,version,domain,samplerId)` 后处理 + 防溢出包间隔公式 + TTL 有界存储）优于 GoFlow2 的最后写入者胜。

### 4.3 代码无法判定
- `TestDecoderWithAkvoradoDeviceCaptureFixtures`（含 `4000/2000` 多采样器断言、sFlow expanded 夹具）依赖外部 Akvorado pcap，**本 checkout 缺失 → 全部 skip**（`pcap_fixture_test.go:132-136`），其保证在本环境未验证。
- 9000B jumbo datagram 结构上没问题（`maxPayloadSize=65535`，长度驱动），无专门测试。
- franz-go 缓冲生命周期：零拷贝文档假设 Kafka `value` 可能被 franz-go 回收，但仓库未配置 `WithPools`/`Record.Recycle`（零处使用）——今天 `record.Value` 是每记录新分配；若日后启用池化，该契约变成承重墙，需回归测试。
- 生产 exporter 的 UDP 源端口是否稳定（关系 L3）。

---

## 5. ClickHouse 连接层与高性能路径（zero-copy / 连接池 / 多线程 / 连接复用 / 最短路径）

### 5.1 连接层事实（全部为 ch-go **native TCP**，树内无 HTTP/clickhouse-go；每进程恰好一个 `chpool`）

| 进程 | 构造 | MaxConns/MinConns | Dial / 单包 Read / 单操作超时 | TLS | 压缩 |
|---|---|---|---|---|---|
| flow-worker | [main.go:236](../cmd/watchdog-flow-worker/main.go) → [native.go:102](../internal/flowch/native.go) | flag，默认 **8/1** | 3s / 30s / **2m** | flag 可配 | LZ4 |
| hub（watchdog-server） | [server.go:382-386](../internal/server/server.go) **硬编码 `MaxConns: 8, MinConns: 1`** | 8/1 | ch-go 默认 **1s** / 3s / 2m（`native.go:26`） | **无**（未传 `TLS`） | LZ4 |
| snmp-collector | `snmp_collector_runtime.go:58-62` | 8/1 | 同 hub | 无 | LZ4 |

- 池：puddle；`MaxConnLifetime 1h / MaxConnIdleTime 30m / HealthCheckPeriod 1m`（ch-go `chpool/pool.go:36-38`）；`Pool.Do` = 每查询 `Acquire → Do → Release`，连接跨查询复用但**每次 `Do` 重新获取**（一块的 2–3 次 INSERT 不钉连接）。
- [native.go:65-69,115](../internal/flowch/native.go) `operationTimeoutExecutor` 包在 `pool.Do` 外 → **2 分钟预算包含 Acquire 排队等待**。
- hub 上 `s.clickHouse` 一个池被 6 个文件共用：flow API 查询、rollup、重分类、对账扫描、VPN 物化+查询、计费、SNMP 读（`server.go`、`flow_query.go`、`flow_archive.go`、`flow_reclassification.go`、`flow_reconciliation.go`、`flow_vpn_detection.go`）；5 个单循环 opjob worker 可同时占住 8 连接中的 5 条跑长作业（rollup `max_memory_usage` 10GiB / 外部 GROUP BY 4GiB，`rollup.go:634-635`）。摄入 INSERT 在 worker 进程 → 摄入与用户查询之间**没有池级队头阻塞**（只共享 CH 服务器）。
- 设置：每条查询自带 `[]ch.Setting{Important:true}`，native 协议按查询下发 → **无会话变异、无跨查询泄漏**。`async_insert=0` + `wait_for_async_insert=1` + 每块 `insert_deduplication_token`（[native.go:249-259](../internal/flowch/native.go)；snmpch/rollup/vpn 同模式）。
- 块格式：列式 `proto.Input`，每次 `Do` 一个块（无 `OnInput` 流式）；编码 → LZ4 进 `compressor.Data` → 追加回 `buf`（ch-go `query.go:296-322`，三次过每块）。查询结果经 `OnResult` 逐块流式，受 `max_result_rows`+`result_overflow_mode=throw` 约束。
- 取消：ctx 取消 → ch-go 发 `ClientCodeCancel`（1s 期限）后 **`c.Close()`** → chpool 在 Release 时销毁该连接；`QueryID` 每次 `Do` 随机 UUID（仅 `raw_delete.go` 自设）；无 `KILL QUERY` 路径。

### 5.2 连接层发现

| # | 严 | 位置 | 发现 | 影响 | 修复 | 复核 |
|---|---|---|---|---|---|---|
| CH1 | H | [server.go:385](../internal/server/server.go)；[native.go:115](../internal/flowch/native.go)；`flow_archive.go:44-59`、`flow_reclassification.go:104`、`flow_reconciliation.go:112` | 一个 8 连接池同时服务交互式 API 与最多 ~5 个并发长作业（rollup INSERT-SELECT 达 10GiB、重分类、对账、VPN、计费）；`Acquire` 静默阻塞、等待藏在 2m 超时里；`pool.Stat()` 从不导出 | rollup 突发期间用户查询延迟尖峰 / 2 分钟停顿；队列深度零可观测 | 拆两个池：交互（8，操作超时 30–60s）+ 批处理（2–3）；导出 `Stat()`（idle/acquired/waiting）；`MaxConns` 进配置 | ✅ |
| CH2 | M | [native.go:256](../internal/flowch/native.go)；DDL 011:139-144 | 非复制 MergeTree 且迁移中无 `non_replicated_deduplication_window`（grep 为零；生产实测 = 0） | 超时后实际已落盘的块重试 = 重复行；正确性全压在 `ReplacingMergeTree+FINAL`，重复 part 加重 merge | 与 §3 H2 同一修复：确定性 token + `ALTER … MODIFY SETTING non_replicated_deduplication_window` | ✅（perf-audit R2） |
| CH3 | M | [native.go:220,229,237](../internal/flowch/native.go)；ch-go `query.go:603` | 每块 **2–3 次顺序 `Do`**（records / counters / receipts），每次 = Acquire + `uuid.New()`（getrandom 系统调用）+ errgroup 3 goroutine + 完整往返；receipts 块 = 每 Kafka 消息一行（≤4096）→ 每块一个小 part | 高 RTT 下每块延迟 ×2–3；额外小 part → merge 压力（生产 ~60 INSERT/分钟/表 与此直接相关） | 整个 `Writer.Write` 只 Acquire 一个 `*chpool.Client`；receipts 按 chunk（一次 `HandleRecords`）合成一个 INSERT 而非按块 | ✅ |
| CH4 | M | [native.go:265-363](../internal/flowch/native.go)（`var (…)` 每次调用零值新建）+ `Append` 增长；ch-go `col_low_cardinality.go:323-339` `Prepare` 重建 `kv` map | ~100 个列对象每块重建、50k 行各扩容 ~16 次（≈1.6k 次 realloc、≈2× 字节拷贝）；~30 个 LC 列每块重建 `map[string]int` | 块构建是 worker 每记录最大单项 CPU（估 30–45%）；GC 抖动 | 每分区保留一份 `recordColumns`，块间 `Reset()`（LC `Prepare` 对非 nil `kv` 做 `clear`），按 `len(block.Records)` 预置容量 | ✅ |
| CH5 | L | ch-go `query.go:46` 取消即 `Close()`；chpool `client.go:26-27` | 每个被取消/超时的查询都丢弃 TCP 连接 → 下次使用重新拨号+Hello | 查询超时下池抖动；`MinConns 1` 反复拨号 | 接受 ch-go 设计；但 `QueryID` 设为 `<service>:<request-id>` 让 `system.query_log`/KILL 可用；hub `MinConns` 提到 2–4 | ◐ |
| CH6 | L | [server.go:382-386](../internal/server/server.go) vs [main.go:132-139](../cmd/watchdog-flow-worker/main.go) | hub/snmp-collector 依赖 ch-go 1s 拨号默认且**无法用 TLS**；worker 可以 | 慢链路 1s 拨号失败；hub→CH 明文凭据 | 把 worker 的 `TLSConfig`/超时接进 hub 配置 | ✅ |
| CH7 | L | `native.go:765-771` `countryCode`：`strings.ToUpper(strings.TrimSpace(v))` + `[]byte(v)` 每行 ×2 | 热构建循环每行 2–4 次小分配 | 50k 块 ≈ 10–20 万次分配 | 国家码存 `[2]byte`，`append(code[:])` | ◐ |
| CH8 | L | [native.go:106](../internal/flowch/native.go) 恒 `CompressionLZ4`，默认地址 `127.0.0.1:9000`（`:24`） | loopback 上 LZ4 纯烧 CPU（每块多 3 次拷贝，`query.go:296-322`） | ≈0.3–0.5µs/行 | 地址为 loopback/unix 时 `CompressionNone`，远程保留 LZ4 | ✅ |
| CH9 | 信息 | ch-go 每连接 `buf`/`compressor` 保留增长后的容量 | 8 连接 ×（50k 行编码块 ≈22MB + 压缩副本）≈ 数百 MB 常驻（估算） | worker 内存足迹 | `blockMaxRows` 降到 20–25k 或减少每 worker 连接数 | ◐ |

**每块（50k 行）插入成本估算：** Acquire/Release 2–3 次；`uuid.New()`+errgroup+3 goroutine 各 2–3 次；行→列拷贝 1 次（≈450B/行 + LC 键 4B/行/列）；编码→写缓冲→LZ4→追加回 3 次过 ≈22MB（压后 5–8MB）；系统调用每块数次（多 MB 写）；LC 字典 ~30 个 map 重建。

### 5.3 采集器热路径：每 datagram 成本核算（[receiver.go:72-142](../internal/flowstream/receiver.go) → [inlet.go:48-65](../internal/flowstream/inlet.go) → [rawflow.go:39-110](../internal/flowstream/rawflow.go) → [producer.go:90-138](../internal/flowstream/producer.go) → franz-go）

| 阶段 | 拷贝（payload 字节） | 堆分配 | 系统调用 | 锁 |
|---|---|---|---|---|
| `getBuffer` + `ReadMsgUDPAddrPort`（`receiver.go:73-74`） | 1 内核→用户 | 0（池） | 1 `recvmsg`（空闲时 +≈1 EAGAIN/epoll） | 池 Get |
| `Admit → AdmitSourceFamily → AdmitSource` 线性扫描（`plan.go:326-348`） | 0 | 0 | 0 | 0；O(bindings×≤3 协议) |
| `NewRawFlow`（`rawflow.go:62-63` `AsSlice()` + `&flowpb.RawFlow{}`） | 0 | 2 | | |
| `ExporterKey` **调用两次**：`inlet.go:56` 与 `Marshal` 内 `rawflow.go:99`（结果丢弃） | 0 | 4（2 次浪费） | | |
| `Marshal` → `MarshalAppend((*buffer)[:0], flow)`（`rawflow.go:103`） | 1 payload→信封 | 0 摊销（池） | | 池 Get |
| `complete` 闭包 + `completed sync.Once` 逃逸（`receiver.go:121-128`） | | 1–2 | | |
| `Producer.Send`：`finish` 闭包、`&kgo.Record{}`、promise 闭包（`producer.go:91,122,127`） | 0（Value 别名信封） | 3 | | |
| franz-go `produce`：`p.mu.Lock()`（全局）+ `recBuf.mu.Lock()` | 0 | ~1 | | 2（1 全局） |
| promise 完成 `p.mu.Lock()` → `finish` → 池 Put | | | | 1 全局 + 池 Put |
| sink（按批摊销）：`appendTo` 拷进请求、LZ4 进池缓冲、`copy(dst[recordsAt:], compressed)`、`write()` | 3（+1 内核） | | ≈0.002 write | |

**合计每 datagram ≈ 5 次用户态拷贝 + 1 次内核（6）、11–13 次分配、1 次系统调用（+1 空闲）、3 次互斥（2 次在 franz-go 全局 `producer.mu`）+ 4 次 sync.Pool 操作。** payload 被**拷贝**（非别名）进记录；信封缓冲由 `kgo.Record.Value` 别名、仅在 promise 中释放（正确）。批处理：franz-go 默认 linger 10ms、批 ≤1,000,012B、幂等下每 broker 5 in-flight；`MaxBufferedRecords(65536)` 满则 `Produce(context.Background())` 阻塞 → 接收 goroutine 停 → 内核丢包（`SO_RXQ_OVFL` 计数）。压缩在每 broker 的 sink goroutine。goroutine：每 socket 1 个，`-sockets` 默认 **1**（`main.go:62`），仅 `>1` 时 `SO_REUSEPORT`；仓库**无 `recvmmsg`**。池有效性：接收池 ≈ 每 goroutine 1 缓冲；编码池 in-flight ≈ 速率×(linger+RTT)（100k pps × 20ms ≈ 2k × 1.6KB）；增长后容量经 `*buffer = encoded[:0]` 保留。

### 5.4 worker 热路径：每 Kafka 记录（datagram）/ 每 flow 记录 / 每块

| 阶段 | 拷贝 | 分配 | 锁 |
|---|---|---|---|
| franz-go fetch：socket 读；LZ4 解压进池缓冲后 **`slices.Clone`**（`compression.go:363-375,397-402`）；`Record.Value` 别名克隆 | 1 内核 + 1 解压 + 1 克隆（按批） | 按批 | |
| `replayWatermarks.committable` **replay 关闭仍每记录调用**（`consumer.go:96` 恒构造；`:343` → `:172-177` `w.mu.Lock()`） | 0 | 0 | 1 |
| `PartitionDecoders.DecodeRecord`（`decoder.go:495-516`）全局 `d.mu` + `entry.mu` 每 datagram，虽然 `validatePartitionRecords`（`processor.go:181-194`）已保证一 chunk 一分区 | 0 | 0 | 2 |
| `parseRawFlowInto`（`rawflow_fast.go:68-70` 零拷贝）+ 驻留 ID（`decoder.go:239-254`） | 0 | 0 | |
| NetFlow v5 / sFlow 快路径（别名 payload；`growRecords` 复用） | 0 | 0 | |
| GoFlow2 慢路径：`metadata := append([]DecodedRecordMetadata(nil), …)`（`decoder.go:364`） | 0 | 1 | |
| `Catalog.Resolve`（`catalog.go:73-89`，atomic + map + `registry.Admit`）→ `SourceBinding` **按值** | ≈150B | 0 | atomic |
| `DecodeAdapter.Map`（`decode_adapter.go:60-140`）：`&RecordBatch{}`、2 切片、`canonicalAddressBytes`（+AgentIP） | `DecodedBatch` 按值 | 4–5/datagram | |
| `mapFlowMessage` 每记录：`binding` 按值 ×3（`:157,:174,:176→:242,:263`）+ `DecodedBatch` 按值 ×1；`canonicalAddress16` ×2（`append([]byte(nil), encoded[:]...)` `:348-355`）；`&Record{}`（`:192`） | ≈600B memmove + 1 记录拷贝 | **3/记录** | |
| `Pipeline.Handle` 用 **`EnrichBatch`**（`pipeline.go:103`）而非 `EnrichBatchInto`（`enrich.go:212-215` 注释即为"让 worker 每分区复用一个记录缓冲"）→ `&EnrichedBatch{}` + `make([]EnrichedRecord,0,n)` | | 2–3/datagram | |
| `enrichRecordWithSnapshots`：`EnrichedRecord`（≈1KB：2×`GeoInfo`≈184B、`ClassifiedEndpoints`≈250B）按值构建/返回/追加（`enrich.go:264,379-401`）；`GeoInfo` 按值传 6 次；`AddressSnapshotResolution`（≈400B）按值返回 ×2；`selectVersions` 每记录二分 | **≈3–4KB memmove/记录** | 0 | atomic |
| `validateBatch` 重做 adapter 刚做过的事：`parseAddress16` ×2（`:509-510`）、`scaledCountersMatch` 128 位乘除（`:543-552`） | 0 | 0 | |
| `PrepareBlocks` `RecordRef{Batch,Record}`（`batch.go:215`） | 0 | ≈1/块 | |
| `buildRecordInput`（`native.go:261-547`）+ `countryCode` | 1（行→列 ≈450B） | ~100 列 + ~1.6k 扩容 + 30 LC map/块；2–4/行 | |
| ch-go `Do` ×2–3 每块（编码、LZ4、追加回、写） | 3 + 内核 | uuid/errgroup 每 Do | Acquire ×2–3 |

**每 flow 记录 ≈ 7 次用户态字节拷贝（Record、EnrichedRecord ×2、列、编码、LZ4、追加回）+ ≈4KB 按值结构体搬运 + 3 次堆分配；每 datagram ≈ 7–8 次分配 + 3 次互斥；每块 ≈1.7k 次分配。一个 30 记录的 NetFlow v5 datagram ≈ 100 次分配。**

goroutine/分区模型：`processFetches` 每次 poll 每分区起一个 goroutine 并 `wait.Wait()`（`consumer.go:327-370`）→ 按最慢分区的**每 fetch 屏障**；随后每次 poll **同步** `CommitRecords` 往返（`:278`，未提交改动新增）；再 `PollFetches`。分区内：≤4096 消息一 chunk（`:380-405`）→ 全部解码（`processor.go:115-121`）→ 全部丰富化（`pipeline.go:88-121`）→ `PrepareBlocks` → 逐块顺序 `insertWithRetry`（`writer.go:190-197`）。**分区内 CPU（解码/丰富化/构建）与插入等待零重叠**；跨分区并行封顶 `MaxConns=8`（超出即 `Acquire` 排队，且计入 2m 操作超时）。

块与 fetch 尺寸：`FetchMaxPartitionBytes` 16MiB（`config.go:102`）≈ 11k 条 1.5KB 消息 → 3 个 4096 chunk；NetFlow v5（≤30 记录/消息）→ ≈122k 记录/chunk → 3 块 50k → 6 次顺序 `Do`；sFlow（5–8 记录/消息）→ ≈30k 记录/chunk → 1 块 → 2 次 `Do`；receipts 每块 ≤4096 行。生产实测平均 1,400 行/块说明当前流量远未填满块——每块固定开销（CH3/CH4）占比反而更高。

每 in-flight fetch 内存（估算）：`FetchMaxBytes` 64MiB 压缩/broker（×broker 数；`maxConcurrentFetches` 无界）+ 2–3× 解压克隆（≈130–200MiB）+ 每个密集 NetFlow chunk 122k ×（Record 250B + 地址 32B + EnrichedRecord ≈1KB + 列 ≈450B）≈ 200MiB，× 最多 8 并发分区 ≈ **1.6GiB 瞬时最坏**——unit 无 `MemoryMax`。

### 5.5 热路径发现（与 §2/§3 重叠者只给指针）

| # | 严 | 位置 | 发现 | 影响 | 修复 | 复核 |
|---|---|---|---|---|---|---|
| HP1 | H | `consumer.go:327-370` + `writer.go:190-197` + `pipeline.go:88-126` | = §3 H3：分区内解码→丰富化→构建→插入严格串行 + 每 fetch 屏障 + 每 poll 同步提交；每次 CH 往返期间 CPU 空转 | 分区吞吐 ≈ 1/(t_cpu + t_insert)，估 1.5–2× 白白丢掉 | 每分区两级流水线：prepare goroutine 产 `PreparedBlock` 进深度 1–2 的 channel，writer goroutine 插入；按 chunk 提交最后一条 durable 记录（已在跟踪） | ✅ |
| HP2 | H | `native.go:265-363`；ch-go LC `Prepare` | = CH4 | 估 30–45% worker 每记录 CPU | 复用 + `Reset()` + 预置容量 | ✅ |
| HP3 | M | `pipeline.go:103`；`enrich.go:264,379-401,370/377/397/398`；`address_snapshot_index.go:200-206` | 生产路径每 datagram 新分配 `EnrichedBatch` 并把每条 ≈1KB 记录拷两次；≈3–4KB 结构体搬运/记录；而 `EnrichBatchInto` 已实现却无生产调用 | GC 压力 ∝ 记录数；估 10–15% CPU | 每分区 `[]EnrichedRecord` arena 按 chunk 定容，原地构建（`&records[i]`）；`*GeoInfo`/`*AddressSnapshotResolution` 传指针；`RecordRef` 指向 arena | ✅ |
| HP4 | M | `decode_adapter.go:192,348-355`；`record.go:105-106` `SourceIP []byte` | 每 flow 记录 3 次堆分配只为装 32 字节地址 + 250B 结构体 | 每 NetFlow v5 datagram ≈90 次分配 | 地址改 `[16]byte`；`RecordBatch.Records []Record` 值切片每分区复用 | ◐ |
| HP5 | M | `receiver.go:60-63` | = §2 H1（`SO_RCVBUF` 被 `rmem_max` 钳制、无回读） | 突发吸收 ≈0.4MB 而非 32MB | 首选 sysctl + 回读/readiness；仅在受控例外环境评估 `SO_RCVBUFFORCE` | ✅ |
| HP6 | M | `receiver.go:74` 每 datagram 一次 `ReadMsgUDPAddrPort`；仓库无 `recvmmsg` | 单接收 goroutine 硬地板 1 系统调用/datagram（≈1.5–2.5µs） | 每 socket 上限 ≈200–250k pps | Linux `unix.Recvmmsg`（16–64 条）经 `RawConn.Read`；保留 `SO_RXQ_OVFL` 解析 | ✅ |
| HP7 | M | `inlet.go:56` + `rawflow.go:99`（结果丢弃）；`rawflow.go:62-63`；`producer.go:91/122/127`；`receiver.go:121-128` | 11–13 次分配/datagram，≥5 次可免（重复 key、RawFlow 结构体、3 个闭包、key 不按源缓存） | 估 5–10% 采集器 CPU | 删第二次 `ExporterKey`；每接收器复用一个 `flowpb.RawFlow`；按 `AddrPort` 缓存 key（同 `decoder.go:261-269` 的 last-source 缓存）；闭包换池化 `inflight` 结构 | ✅ |
| HP8 | M | `rawflow.go:103` `MarshalAppend` 拷全 payload；sink 再 3 次 | 每 payload 字节 5 次用户态拷贝 | 内存带宽；估 10–15% 采集器 CPU | 原地编码信封：datagram 读进池缓冲的偏移处，前置 field-2 tag/len，标量字段追加在 payload 之后（protobuf 字段顺序自由），整缓冲交给 Kafka、promise 归还 | ◐ |
| HP9 | L | `decoder.go:495-515` 全局 `d.mu` + `entry.mu`；`consumer.go:96/343` replay 关闭仍取锁 | 每 datagram 3 次互斥，其中一次跨分区 goroutine 争用 | ≈100–300ns/datagram，纯浪费 | 每 `HandleRecords` chunk 解析一次解码器；`TemplateReplayRecords==0` 时跳过 `committable` | ✅ |
| HP10 | L | `decode_adapter.go:157,174,176,242,263`（`SourceBinding`/`DecodedBatch` 按值 ×4） | ≈600B memmove/记录 | ≈50–100ns/记录 | 传 `*SourceBinding`、`*DecodedBatch` | ◐ |
| HP11 | L | `enrich.go:429-552` `validateBatch` | 重解析地址、重算 `scaleEstimatedBytes` | ≈100–200ns/记录 | 在 adapter 边界校验一次，或把复核放到 debug flag 后 | ◐ |
| HP12 | L | `plan.go:326-336`（采集器 `Admit`；worker `registry.Admit` `catalog.go:85` 疑似同形） | = §2 M6 | 1000 绑定时 ≈10–30µs/datagram | 每协议前缀表 / `netipx.IPSet` | ✅ |
| HP13 | L | `sflow_v5_fast.go:513` `default: return false // VLAN (0x8100)` | 任何 802.1Q 采样帧离开零分配快路径进 GoFlow2 `ParseSampledHeader`（`:389-390` 注释自述"该路径 51% CPU 与 100% 分配"） | sFlow 吞吐取决于打不打 tag 而非快路径（与 §4 H1 QinQ 同根） | 固定偏移处理 1–2 层 VLAN tag（含 0x88a8） | ✅ |
| HP14 | 信息 | `consumer.go:278` `CommitRecords`（未提交改动） | 同步提交在屏障路径上每 poll 加一次 Kafka RTT（原为异步 `AutoCommitMarks`） | ≈1–5ms/poll | 保持同步语义但与下一次 `PollFetches` 重叠（发出 poll 后提交上一轮），或接受 | ✅ |

### 5.6 吞吐上限（推理估算，未实测）
- **采集器，每 socket goroutine（≈1 核）：** ≈4–5µs/datagram（recvmsg+netpoll ≈2µs、信封 ≈0.5µs、franz-go produce 3 锁/分配 ≈1.5µs、GC）→ **≈200–250k datagram/s/socket**；`recvmmsg` + 原地编码后 ≈400–600k/s/核。`-sockets N` 近线性扩展，直到 franz-go 全局 `producer.mu`（2 次/datagram，≈3–5M/s）或每 broker sink goroutine（LZ4 ≈0.5–1GB/s ≈ 350–700k 个 1.5KB datagram/s/broker 连接）。
- **worker，每核：** ≈3.5–5µs CPU/flow 记录（解码 0.05–0.1 v5 快路径、映射 ≈0.2、丰富化 ≈1–1.5、块构建 ≈1.5–2、ch-go 编码+LZ4 ≈0.3–0.5）→ 插入完全重叠时 **≈200–280k 记录/s/核**。**每分区**含串行插入等待（假设 50k 行块服务端 0.1–0.3s）：**≈80–150k 记录/s**；每 worker ≈ min(分区数, 8 连接) × 该值，然后受 CH 服务端限制。每分区每 poll：16MiB ≈ 11k 消息；周期 = 最慢分区 + 提交 RTT。

### 5.7 按预期收益排序的前五项改动

| # | 改动 | 位置 | 预期收益（估） |
|---|---|---|---|
| 1 | worker：每分区 prepare（解码/丰富化/构建）与插入重叠；receipts 按 chunk 合并；每次 `Write` 一个池化客户端 | `consumer.go`/`pipeline.go`/`writer.go`/`native.go:220-240` | 插入延迟≈CPU 时间时分区吞吐 1.5–2× |
| 2 | worker：块间复用/预置列构建器与 LC map | `native.go:261-363` | worker 每记录 CPU −30–45%，每块 −1.7k 分配 |
| 3 | worker：原地 `EnrichedRecord` arena（`EnrichBatchInto`）、geo 结构传指针、`[16]byte` 地址、值 `Record` 切片 | `pipeline.go:103`、`enrich.go:264/379`、`decode_adapter.go:192/348` | 每记录 −3 分配、−4–5KB memmove；估 −15–25% CPU，GC 停顿显著下降 |
| 4 | 采集器：`recvmmsg` + 原地信封编码 + 去掉重复 `ExporterKey`/闭包 | `receiver.go:74`、`rawflow.go:99/103`、`producer.go:91-137` | 每 socket pps 1.5–2.5×（系统调用地板），−1 拷贝、−5 分配/datagram |
| 5 | 采集器运维：`-sockets` 默认 >1 + `SO_REUSEPORT`、修 `SO_RCVBUF` 钳制、sFlow 快路径处理 VLAN/QinQ | `main.go:62/216`、`receiver.go:60-63`、`sflow_v5_fast.go:513` | 随核数线性扩展；突发容忍；快路径命中率 |

### 5.8 做对的
零拷贝信封解析 + 驻留身份；固定偏移 NetFlow v5/sFlow 框架解码器（边界检查、包级哨兵错误、复用 backing 数组）；每源 sampler 地址缓存；GoFlow2 池延迟回收；接收/编码池缓冲只在 Kafka ack 后释放；`SO_RXQ_OVFL` 内核丢包计数；`SO_REUSEPORT` 支持；生产者背压而非无界内存；每分区解码器隔离；`RecordRef` 指针块；有界 chunk 让部分进度可提交；同伴分区不因一个失败被取消；有界重试预算使卡住的分区不能挂死屏障；`EnrichBatchInto` API 已为复用设计；native + 列式 `proto.Input`；有健康检查的有界 puddle 池；显式 `async_insert=0` 使 Kafka 提交 ⇔ 持久化；每个读查询都带 `max_execution_time/max_rows_to_read/max_bytes_to_read/max_result_rows` + `throw`；permanent/retryable 分类 + 有界 `RetryMaxElapsed`；块按分区日切分避免 `max_partitions_per_insert_block`；计费 scope 用 `ExternalData` 而非巨型 IN；单一 `Executor` 接口无第二套客户端栈。

### 5.9 代码无法判定
CH 服务端对 100 列 × 50k 行块的插入延迟（worker 吞吐的主导项）；`non_replicated_deduplication_window` 是否曾带外设置（生产实测 0 → 未设）；hub 池实际饱和度（无指标）；broker/CH 主机拓扑（loopback 还是远程）；ch-go 缓冲实际常驻；目标主机 `rmem_max`；每 worker 分区数与 broker 数；协议混合与 sFlow 快路径命中率（VLAN 打 tag 比例）；`GOGC`/`GOMEMLIMIT`；worker 侧 `flowplan.Registry.Admit` 是否与 `AdmitSource` 同为线性扫描。

**未提交改动（热路径包）观察：** `go build` flowstream/flowworker/flowch/flowquery/两个 cmd 通过。改动自洽：worker fetch/分区批 flag（`main.go:111-114,334-387`）、消费者从 `AutoCommitMarks` 改为每 poll 显式 `CommitRecords` 且 `TemplateReplayRecords` 默认 1M→0（`consumer.go:107,119-123,278`；`config.go:95-112`）、`estimated_bytes_scale_ppm` 贯穿 `record.go`/`decode_adapter.go:178-191`/`enrich.go`/`native.go:304,450,523` + 迁移 019（在树，未验证是否已应用）、新 `interface_reconciliation.go`（未跟踪，可编译）。一处遗留：replay 关闭时 `replayWatermarks` 仍每记录分配+加锁（`consumer.go:96,343`）。

---

## 6. 分阶段修复计划

### Phase 0 — 止血（本周；每项 <50 行；无 schema 变更；可各自独立提交）

| # | 改动 | 位置 | 验证 |
|---|---|---|---|
| 0.1 | GoFlow2 模板守卫：在模板进入 store 前拒绝零字段/零前进模板；数据集循环必须验证每轮实际消费字节。普通 goroutine timeout 不能停止已死循环的解码器，只有在可取消边界成立或把解码隔离到可终止进程后才可把 timeout 作为第三道保险 | `internal/flowstream/decoder.go:196-216,337-346`、上游 fork/replace | 单测：零字段模板 + 数据集、全零长度字段、合法 template withdrawal 均有界返回；进程级测试证明不会继续占用 CPU/内存 |
| 0.2 | worker 错误分类：未来偏差 / 早于首版本 / ODID 不匹配 / >1024 记录 → `mapping_rejected` 回执（带原因）+ quality 位，**不再**进 `retryableErrors`；`fetches.Errors()` 中 `kerr.IsRetriable` 的继续 | `internal/flowworker/processor.go:149-157`、`decode_adapter.go`、`enrich.go:135-137`、`consumer.go:261-269` | `enrich_test` +6min 用例改为回执断言；新增进程级测试：一条毒报文后分区继续前进 |
| 0.3 | 采集器启动校验 topic 存在、分区数和关键 durability 配置，缺失时 fail-fast；运行中重试必须有界并进入不可写/降级健康状态，禁止 `UnknownTopicRetries(-1)` | `internal/flowstream/producer.go:60-72`、`cmd/watchdog-flow-collect/main.go:167` | 不存在 topic → 启动失败并给出 topic 名；运行中删 topic → 有界时间内 readiness 失败且不假装健康 |
| 0.4 | `SO_RCVBUF` 回读：有效值小于请求值即 WARN + 指标 `receive_buffer_effective_bytes`；部署优先设置并核验 `net.core.rmem_max`。只有受控环境确实无法设置 sysctl 时才评估 `SO_RCVBUFFORCE`/`CAP_NET_ADMIN`，不把高权限设为默认 | `internal/flowstream/receiver.go:60-63`、部署 sysctl | 在 `rmem_max=212992` 的主机启动可见告警；调整 sysctl 后 readiness/指标显示目标值 |
| 0.5 | `Producer.Send` 四处早返回计入 `stats.errors`；接线 `OnPublishError`；`Close` 末尾记录最终 `Stats()`；`received = kafka_records + publish_errors + buffered` 恒等式测试 | `internal/flowstream/producer.go:100-130,161-165`、`main.go:212-227` | 恒等式单测 |
| 0.6 | 采集器要求 `plan.CollectorID == -agent-id`；生产拒绝 v1 信封；SASL PLAIN 无 TLS 拒绝 | `main.go:213`、`flowplan/signature.go:158`、`flowstream/config.go:140-151` | 单测 |
| 0.7 | 先增加 `0x8100/0x88a8` 单/双标签真实 pcap 差分测试，证明 fallback 丢了什么；只有测试证明存在正确性缺口时才扩快路径，并保持 fast/fallback 的地址、端口、VLAN、长度完全一致 | `internal/flowstream/sflow_v5_fast.go:466-471,512-514` | QinQ 夹具覆盖 fast hit、fallback success、明确 rejection 三种结果 |
| 0.8 | `Ready()` 增加 `estimated_bytes_scale_ppm` schema 检查，并收口 rejection observer 的限速结构化日志（topic/partition/offset/reason）；migration/writer 已落地，但 readiness 尚未闭环 | `internal/flowch/native.go:141-180`、`cmd/watchdog-flow-worker/main.go:260` | readiness 缺列失败测试；日志抽样和拒绝风暴限速测试 |
| 0.9 | systemd 增加 `LimitNOFILE`、`TimeoutStopSec`、restart policy、`Wants=network-online.target`、采集器 metrics 端口和 sysctl 核验；`MemoryMax/GOMEMLIMIT` 必须由压测峰值、fetch 上限和安全余量计算，不能直接写死“≥2GiB” | `deploy/systemd/*.service`、worker `main.go:313-317` | 固定流量压测后核验无 OOM、无 swap 抖动、限制值和配置预算一致 |

**模板重放默认值（M6）需要决策：**要么恢复正默认（如 200,000）并配合 0.2 之后 H1 的修复，要么持久化每分区模板状态。在 H1 修复前把默认值改回正数会放大回执翻转；因此建议顺序 = Phase 1.1 先落，再改默认。

### Phase 1 — 幂等与一致性（2 周；与 perf-audit Phase 1 DDL 同批）

| # | 改动 | 位置 |
|---|---|---|
| 1.1 | 模板重放 floor 以下只解码不 `handleGroup`（无回执无插入）；`DecodeRecord` 在 `ErrorTemplateNotFound` 时返回 `ReceivedAt` 使回执 generation 与持久化行同源；回执型 disposition 永远不得压过 `persisted`（DDL 或写入侧守卫）；修正 `kafka_replay_integration_test.go:54-58` 的断言 | `consumer.go:336-343`、`processor.go:138-140`、`native.go:701-704` |
| 1.2 | 去重：按确定性 offset 桶切块（token = `(stream,partition,bucket)`），`ALTER TABLE flow_records/flow_ingest_receipts/sflow_interface_counters MODIFY SETTING non_replicated_deduplication_window = N`（N ≥ 峰值块/秒 × 最长重试秒数 × 3 表）；完成后 perf-audit R2 的"去 `FINAL`"才可能 | `native.go:244-247`、`batch.go:173,239`、CH 迁移 |
| 1.3 | 版本时间线：控制面强制 `effective_from ≥ now + 激活延迟（≥ 最大 worker 滞后）`；worker 同步失败超过陈旧上界后对越界事件时间暂停分区（真正的按分区暂停：返回 `(0,nil)` 保持其它分区流动 + `paused_partitions` 指标）——这也是 §3 C1 (b)(d) 的正解 | `flow_enrichment_publications.go:673`、`version_catalog.go:224-230`、`consumer.go:284-288` |
| 1.4 | LKG 恢复按 `SignedAtUnixMilli` 验签（仍尊重吊销）；或控制面密钥轮换时重签历史发布 | `version_lkg.go:316`、`version_signature.go:92`、`flowplan/trust.go:207` |
| 1.5 | 采集器源计划热重载（ticker/inotify → `atomic.Pointer[*Registry]`，逐报文打 `RegistryVersion`；失败保留上一有效注册表）；`plan_expires_at_seconds` 指标 + 提前告警；agent 计划变更进程内应用（`SO_REUSEPORT` 新收器替换旧收器） | `cmd/watchdog-flow-collect/main.go:108-133,261-265`、`flowplan/plan.go:104-111` |
| 1.6 | 采集器与 worker 共用同一 `Admit`（含 ODID）；未绑定报文不进 Kafka 而计入 `admission_rejected{reason}` | `flowplan/plan.go:326-348`、`cmd/watchdog-flow-collect/main.go` |
| 1.7 | 隔离按记录而非整 datagram；隔离行合批；barrier `Covers` 预计算为整数日比较（`Install` 时算 `deletedThroughDay` + 排序切片）；无 barrier 的文件引导 worker 在 CH 有 raw-delete 证据时拒绝启动 | `pipeline.go:92-101,137-144`、`quarantine.go:74,84`、`barrier.go:65-79` |
| 1.8 | hub CH 池拆分：交互池（8，操作超时 30–60s）+ 批处理池（2–3）；导出 `pool.Stat()`；`MaxConns`/TLS/超时进 hub 配置；`QueryID = <service>:<request-id>` | `internal/server/server.go:382-386`、`internal/flowch/native.go:74-118` |

### Phase 2 — IX 规模吞吐（1–2 个月；按 §5.7 收益排序）

| # | 改动 | 预期 |
|---|---|---|
| 2.1 | worker 每分区两级流水线（prepare → 有界 channel → writer），按 chunk 提交，revoke 时排空；连接池大小由 CH insert 延迟、merge backlog、内存和分区并发压测决定，不要求机械等于分区数；重试预算 < `RebalanceTimeout` | 验收以端到端吞吐、Kafka lag、CH part/merge 和内存为准，不预写倍数 |
| 2.2 | 每分区复用列构建器 + LC 字典（`Reset()`，按 `len(block.Records)` 预置）；每次 `Write` 一个池化客户端；receipts 按 chunk 合并；LZ4 与无压缩只在 loopback 固定夹具上对比；国家码 `[2]byte` | 验收以 CPU/row、bytes/row、insert latency、part rate 实测为准 |
| 2.3 | `EnrichBatchInto` arena + 指针传递 geo/resolution + `[16]byte` 地址 + 值 `Record` 切片；`SourceBinding`/`DecodedBatch` 传指针；`validateBatch` 不重做 adapter 的工作；replay 关闭时跳过 `committable`；每 chunk 解析一次解码器 | 每记录 −3 分配、−4–5KB memmove；CPU −15–25% |
| 2.4 | 采集器 `recvmmsg`（16–64 条）+ 原地信封编码 + 去重复 `ExporterKey` + 按源缓存 key + 池化 `inflight` 替代闭包；`-sockets` 默认 = 核数 + `SO_REUSEPORT`；NIC RSS/IRQ 亲和与 NUMA 绑定文档；`MaxBufferedBytes` 按秒级摄入定容；每监听器独立 franz-go client（隔离 sFlow/NetFlow 队头阻塞）；`kgo.WithLogger` 限速；`kernel_drops_total` 加 `listener` 标签 + 独立轮询 `/proc/net/udp` | 每 socket 1.5–2.5×，随核数线性 |
| 2.5 | 前缀树/LPM 准入；`-v1` topic 版本升级时预留 ≥64 分区，sFlow 可按 `sub_agent_id` 子键 | 千级绑定 + 热 exporter 不再封顶单分区 |
| 2.6 | 若按对等体分析在范围内：`DecodedRecord` + 事实表增加 as_path/communities/bgp_next_hop/mpls_label 并在回落路径映射；sFlow 接口 format 位解释；序列 gap 统计指标 | IX peering 报表可行；exporter→collector 丢包可见 |
| 2.7 | 解码器每分区驱逐（revoke 钩子 → `Close` 该分区 decoder）；被撤销分区模板状态释放 | 长运行 worker 内存不随 rebalance 累积 |

### Phase 3 — 测试与文档债（穿插进行）
- fuzz：`decodeSFlowV5Fast`、`decodeNetFlowV5Fast`、`parseRawFlowInto`（以现有夹具做种）；NetFlow v5 快路径包进 `recoverDecoderPanic`。
- 集成：两成员 rebalance/revoke 中途 + 重复计数；records 与 receipts 之间崩溃 + `FINAL` 收敛；floor 以下重放的回执结果；C1 各毒类进程级；SIGTERM 中途插入；去重 token 对设了窗口的 CH 是否生效；barrier 热路径分配；每 fetch 内存上界；端到端吞吐基准（目前只有 `BenchmarkEnrichBatch1024RecordsReused`）；counter 记录独立参考比对；9000B jumbo；恢复 Akvorado pcap 夹具或替换为自有夹具。
- 文档：install 文档"采集器解码"、设计文档 YAML `sockets: 8`/`max_buffered_records: 262144`（二进制实为 flag，默认 `-sockets 1`/65536）、`flow-decode-fastpath.md` counters 已提取、`flow-module-design.md:210` 模板重放要求与默认值 0 的矛盾、包文档"atomic 指针 + 日期比较"与 barrier 实现不符。

## 7. 代码无法判定、需实测
生产 `net.core.rmem_max`/内核版本/网卡 RSS；生产 Kafka 分区数、副本、`min.insync.replicas`、`message.max.bytes`、ACL；`collector-plan.json` 如何到达主机与刷新；exporter 源端口固定与否；`/etc/watchdog/flow/worker.env`（重放窗口、`MaxConns`、批大小）；franz-go 在屏障期间失去分区时 `CommitRecords` 的确切结果；控制面 raw-delete ACK 门是否覆盖文件引导 worker；`effective_from` 的运维策略；真实吞吐/延迟（无接收循环基准）。

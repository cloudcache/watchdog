# Flow 模块详细设计

> **Flow Storage V2 重大变更（唯一生效契约）**：原始事实取消硬编码 30 天 TTL、实时 rollup 改为策略驱动 aging/downsample、记录和回执改用 Kafka 自然坐标且移除 Flow 热路径 hash。详细切换、回滚、准确性边界和任务门禁见 [flow-storage-v2-change-plan.md](flow-storage-v2-change-plan.md)。本文后续与该设计冲突的旧 DDL、TTL、`record_id`、batch checksum、实时 1m/1h 描述均为历史基线，不可再作为实现依据；001–010 迁移仍保持不可修改。

> 状态：现行。本文只定义当前实现契约；需求口径见 [flow-direction-requirements.md](flow-direction-requirements.md)，架构取舍和废弃方案见 [flow-pipeline-adr.md](flow-pipeline-adr.md)，唯一执行状态见 [flow-module-tasklist.md](flow-module-tasklist.md)。完成历史不得回填到本文。

> **修订(2026-09)——保留与降精度模型**：原始/全精度是在线主查询面，保留时长完全来自安装级 Storage V2 policy，不写死 30 天或 1 年。只有整个 UTC 日越过 `max(raw_retention, late_arrival_window)` 后才生成 1h archive；1m 不再持续物化。固定 TTL、实时双 rollup 和 hash 身份的后续旧段落仅保留为 V1 历史。

> **修订(2026-09)——自研 v5 快解码器**:**NetFlow v5 与 sFlow v5 已由自研定长/TLV 快解码器处理**(零反射、零分配、对 GoFlow2 逐字段差分验证);GoFlow2 仅剩 **NetFlow v9 / IPFIX** 与 sFlow 未覆盖记录的回落。因此本文 §1.1 图的 decode 步、以及 §「采样与旁带元数据」中"sFlow 的 sub-agent/source-id/sample-pool/drop 靠 GoFlow2 producer 同序旁带保留"仅对 **GoFlow2 路径(v9/IPFIX)** 成立;**sFlow v5 快路径直接在自研解码器内复刻这些字段**。NetFlow v9/IPFIX 的 GoFlow2 template/sampling store 语义不变。详见 [flow-decode-fastpath.md](flow-decode-fastpath.md)。

## 1. 冻结边界

### 1.1 唯一数据链路

```text
router/switch
  -> watchdog-flow-collect
       UDP receive -> source-prefix admission -> RawFlow protobuf -> Kafka
  -> watchdog.flow.raw-v1
  -> watchdog-flow-worker
       partition-ordered decode (自研 NetFlow v5/sFlow v5 快路径 + GoFlow2 v9/IPFIX) -> sampling -> dimensions -> CH batches
  -> ClickHouse raw facts + policy-aged 1h archive
  -> authenticated query/export API
  -> six product views
```

只有一个 Flow 数据 topic。Kafka 是唯一队列和短期重放边界。`flow-collect` 不解码、不查管理库、不归类、不聚合、不写本地 WAL；`flow-worker` 不在接收线程工作；ClickHouse 是唯一 Flow 分析事实库；VictoriaMetrics（VM）只保存低基数运行指标；MySQL 只保存配置、权限、任务和操作状态。

### 1.2 进程职责

| 进程 | 必须做 | 禁止做 |
|---|---|---|
| `watchdog` | 登录/RBAC、collector/exporter 管理、签名 plan、查询网关、修正/导出/probe job | 接收 UDP、逐 flow 计算、浏览器直连 CH |
| `watchdog-flow-collect` | sFlow/NetFlow/IPFIX UDP、多 socket、来源准入、RawFlow、Kafka 异步发送、flush | GoFlow2 decode、MySQL/CH/VM、Geo、采样放大、业务维度 |
| `watchdog-flow-worker` | partition 内有序 decode、模板状态、采样一次、event-time 快照、批写 CH、offset | 逐 flow HTTP/MySQL、无界 goroutine、同步地址库 RPC |
| `watchdog-vpn-probe-agent` | 消费获批任务、能力协商、限速探测、结构化证据回传 | 自主扩大目标、修改 Flow 事实、上传默认原始 payload |

### 1.3 复用平台能力

复用平台已落地的 `users/roles/permissions`、tenant scope、target/device/port、collector registry、签名 plan、`address_prefixes/address_sets`、`operation_jobs` handler registry、export、visualization 和 audit。PLAT-04A 已落地人工 prefix/set 的 immutable preview/publish 基座与 migration 040，但 active MMDB/IPDB base overlay、签名/approve、retire/rollback、worker install ack 和引用保留仍是平台前置；PLAT-04B 的剩余项是通用 per-tenant 周期触发和跨类型扫描背压，不得把已经存在的 lease/heartbeat/cancel/retry/typed payload registry 描述成未实现。Flow 不复制通用 CRUD、身份或任务引擎；平台缺陷登记在 [platform-refactor-tasklist.md](platform-refactor-tasklist.md)，除非阻断当前 Flow 切片，否则不得顺手重构平台。

### 1.4 正确性不变量

1. 同一 UDP datagram 只进入一个 RawFlow topic；同一 `collector_id + exporter_ip` 固定到同一 partition。
2. worker 只有在 ClickHouse base 批次验证成功后才提交 Kafka offset。
3. `raw_bytes/raw_packets` 永不改写；`estimated_*` 每条记录最多放大一次。
4. 业务方向只由事件时间有效的本地前缀决定；观察接口只用于定位和冲突标志。
5. 每条事实固定 `registry/dimension/geo/classification/rule` 版本，可重放、可解释。
6. 主前缀最长匹配且互斥；address set 是可多归属标签，跨 set 不得相加当总量。
7. 六类 + `internal/transit/unknown` 与 accepted base fact 守恒。
8. 故障显示 incomplete/lag/loss interval，禁止伪装为 0 流量。

### 1.5 Storage V2 生效架构

| 层 | 唯一生效契约 |
|---|---|
| 事实身份 | `(source_stream_id, kafka_partition, kafka_offset, record_index)`；`source_stream_id` 表示 Kafka cluster/topic incarnation，topic 重建后禁止复用 |
| 写入 | 大 columnar block 写事实，逐 Kafka message 写 receipt；无 `record_id`、`ingest_batch_id`、dimension fingerprint 或内容 checksum 热路径计算 |
| 对账 | 由 Kafka `committed_next_offset` 给出闭合右边界；逐 offset 检查 receipt、连续 record index、count 及 raw/estimated bytes/packets |
| 生命周期 | MySQL 全局 immutable policy + UTC 日状态；候选日必须越过 `max(raw_retention, late_arrival_window)`，再由 `operation_jobs` 写 24 个 1h bucket 并核对守恒；每 6h 轮转复核迟到数据，差异进入下一 repair generation |
| generation | `(policy_version << 32) | repair_attempt`；新策略和 repair 单调前进，不与 legacy generation 冲突 |
| 查询 | 连续 reconciled 日之前读 1h archive，之后读 raw；两段互斥且 union 后再做全局 TopN。1m 和未物化的联合维度读 raw |
| 删除 | 当前 fail closed；`raw_delete_enabled=true` 不可发布。L5A 已提供 Kafka 日覆盖/水位/counter/restore evidence 的只读就绪度；只有批准、真实恢复和故障门禁完成后才新增显式 delete handler |

管理 API 固定为 `GET/POST /api/v1/flow/storage/policies`、`GET/PATCH/DELETE /api/v1/flow/storage/policies/{id}`、`POST .../{id}/actions/publish`、`GET /api/v1/flow/storage/partitions` 和只读 `GET /api/v1/flow/storage/partitions/{YYYY-MM-DD}/delete-readiness`。修改/删除 draft 与发布均使用 row-version/`If-Match`，整个安装同时只能有一个 published policy。server 内的固定扫描预算只限制单轮工作量，不定义保留时长；不存在第二套 `flow_storage` 天数配置。归档 worker 复用 `operation_jobs` 的 lease/heartbeat/cancel/retry/checkpoint，失败后从 `next_hour` 续跑，同 generation 重建幂等。MySQL 只把连续 `reconciled/delete_eligible/raw_deleted` UTC 日暴露为 archive boundary；首个缺口、运行中或失败日立即停止边界，查询从该处读取 raw，禁止按年龄猜测归档完整。物理删除仍未启动。

删除就绪度读取从 `flow_ingest_receipts FINAL` 聚合所有 `record_count>0` 且 `[min_event_time,max_event_time]` 与目标 UTC 日相交的消息，得到每个 `(source_stream_id,kafka_topic,kafka_partition)` 的 `[min(offset),max(offset)+1)`；跨午夜消息会被保守纳入两日。每个范围必须匹配 MySQL 同 stream/partition 的唯一水位，topic/consumer group 不漂移、`bootstrap_offset<=first_offset`、状态 healthy、mismatch 为零、committed snapshot 与 last verified 时间非空，且 committed/reconciled next-offset 都越过范围末端。随后重新读取 raw/1h archive 六项 counter 并与持久分区状态一致，按策略要求选择覆盖整日且已 restore-tested 的 backup evidence。API 分开返回 `evidence_ready` 和 `deletion_ready`：前者不包含删除开关与人工批准，后者必须通过完整 fail-closed guard；读取不会把 `reconciled` 推成 `delete_eligible`，不会创建 deletion receipt，也不会执行 ClickHouse DDL。

## 2. RawFlow、Kafka 与 collector

### 2.1 RawFlow v1

物理 topic 为 `watchdog.flow.raw-v1`。value 是 protobuf，最小字段如下：

| 字段 | 类型 | 语义 |
|---|---|---|
| `time_received` | int64 | collector 收包 Unix 秒 |
| `payload` | bytes | 原始 UDP datagram |
| `source_address/source_port` | bytes/uint32 | exporter UDP 来源；端口只诊断 |
| `decoder` | enum | `SFLOW` 或 `NETFLOW`，后者覆盖 v5/v9/IPFIX |
| `collector_id/listener_id` | string | 采集实例和监听器身份 |
| `registry_version` | uint64 | 接收时签名 plan 版本 |
| `timestamp_source/decapsulation_protocol/rate_limit` | enum/string/uint64 | 保留 Akvorado RawFlow 兼容语义 |

Kafka key 为 `collector_id || 0x00 || canonical_exporter_ip`，不得包含 UDP 源端口。topic 不原地增加 partition；扩分区使用新 topic schema 版本并灰度切换，避免模板流改变 partition。

### 2.2 来源准入

collector 启动必须加载有效签名 plan。sFlow listener 允许 `sflow5` binding；共享 NetFlow listener 允许 `netflow5/netflow9/ipfix` 任一 binding。接收阶段只按来源前缀判断，observation domain 和 sampler 在 worker 解码后核验。未知来源只增加低基数计数，不保存 payload，不自动创建 exporter。

plan 到期且没有新版本时 fail closed；控制面短暂不可用时，在有效期内使用本地 last-known-good。plan 文件以临时文件、`fsync`、rename、目录 `fsync` 原子替换。

### 2.3 Producer 参数和故障语义

- `franz-go` 异步 `Produce`、默认幂等 producer、有界 `MaxBufferedRecords`、LZ4/Zstd 批压缩；
- Kafka broker/topic/TLS/SASL/ACL 由部署系统创建，collector 只拥有目标 topic 的 write/describe 权限；
- callback 后才能复用 payload/protobuf buffer；最终失败计数并标记 degraded；
- 队列满形成接收背压，最终 socket/kernel drop 是明确的 UDP 服务语义；
- 调用方 context 在所有权转交前已取消则拒绝；一旦 `Produce` 接受，记录使用 producer 生命周期，停止 Receiver 不得取消已入 franz-go buffer 的记录；
- 关闭时先停止收包，再在有界时间内 flush；超时记录未确认时间窗。禁止把 Receiver 的 cancel context 留在已接收记录上，否则正常关停会把在途 flow 误报为 produce error 并丢弃。

Kafka 不可用不会触发第二套 WAL、retry 数据库或 ACK 状态机。容量必须通过 Kafka buffer、socket buffer、N+1 collector 和告警窗口保证，而不是宣称绝对零丢失。

producer/consumer 共用 franz-go 的连接恢复和 backoff，不在 Flow 外包一层状态机。两个生产进程都在启动时以有界 `Ping` 验证 broker/TLS/SASL；启动成功后启用 `AlwaysRetryEOF`，因此 broker restart 或高负载切断替换连接时的首请求 EOF 继续由 franz-go 恢复，不会被误判为 TLS 配置错误并终止 worker。生产门禁必须在已提交 offset 后实际 restart 保留数据的隔离 broker，确认同一进程存活、offset 不回退且后续 flow 继续落库；仅 pause/unpause 或另起 worker 不能替代。

### 2.4 Collector 配置

```yaml
flow_collect:
  plan_file: /var/lib/watchdog-flow/plan.json
  plan_public_key_file: /etc/watchdog/flow-plan.pub
  listeners:
    sflow: { address: ":6343", sockets: 8 }
    netflow: { address: ":2055", sockets: 8 }
  udp:
    receive_buffer_bytes: 33554432
    max_datagram_bytes: 65535
  kafka:
    brokers: ["kafka-1:9093", "kafka-2:9093", "kafka-3:9093"]
    topic: watchdog.flow.raw
    client_id: watchdog-flow-collect
    max_buffered_records: 262144
    compression: lz4
    tls: { enabled: true, ca_file: /etc/watchdog/ca.pem }
    sasl: { mechanism: scram-sha-512, secret_file: /run/secrets/kafka }
```

配置解析必须拒绝未知 key、明文 secret 和非法范围。CLI/env/YAML 最终映射到同一 typed config；优先级固定为 CLI > env > YAML > default，启动日志只输出脱敏后的 effective config。

## 3. Worker、解码和批次提交

### 3.1 Partition 执行模型

每个 Kafka partition 同时只由一个 goroutine 有序处理；不同 partition 并行。GoFlow2 decoder/template/sampling state 属于 partition worker。禁止逐 datagram 或逐 record 创建 goroutine。rebalance 时停止领取新 record，等待当前有界 batch，成功则标记 offset，失败则放弃并由新 owner 重放。

NetFlow/IPFIX 模板缺失不能阻塞 partition 等待未来模板，否则未来模板永远无法处理。该 datagram 记录 `template_missing` 后完成；后续模板到达即恢复。损坏或永久不支持的 datagram 按 Akvorado/GoFlow2 的成熟语义计低基数错误并完成 offset；可选诊断只保存脱敏摘要和有界样本，不新增 DLQ、attempt 或隔离状态机。

worker 将验签后的 plan revision 装入只增的 immutable catalog；每条 RawFlow 只能按 `collector_id + registry_version` 精确取 binding，缺版本即停止处理且不标记 offset，绝不回退到“当前版本”。过期 plan 可以用于解释其有效期内已进入 Kafka 的历史记录，但不能再用于 collector 来源准入；只有 Kafka retention、consumer watermark 和最长故障窗口都证明不再引用时，才可回收旧 revision。

### 3.2 内部 Record

worker 内存结构不是 Kafka schema，不承诺跨版本持久兼容。必须包含：

- source identity：topic/partition/offset/record index、collector/listener、exporter、target/device、observation domain；
- time：event、export、received；
- endpoints：src/dst IPv4/IPv6、ports、protocol、TCP flags、ingress/egress ifIndex；
- counters：raw bytes/packets、sampling mode/rate/source、estimated bytes/packets；
- enrichment：business direction、local/remote endpoint、Geo/ASN/ISP、category、business、primary prefixes/address sets；
- provenance：registry/dimension/geo/classification/rule versions、quality flags。

稳定 `record_id = SHA256(topic, partition, offset, record_index)`；稳定 `ingest_batch_id = SHA256(topic, partition, first_offset, last_offset, content_checksum)`。同一输入和版本必须产生相同 ID、分类和计数；block 因 rows/bytes 上限切分时，边界也必须确定。

### 3.3 计数与采样

precedence 固定为：协议记录明确语义/采样字段 → 精确 data-source/sampler override → exporter default → unknown。

| 输入 | 输出 |
|---|---|
| sampled，rate=N | `estimated = raw * N`，防溢出 |
| pre-scaled | `estimated = raw` |
| unknown | 保留 raw，estimated 标为不可用；不得猜 rate=1 |
| override 与报文冲突 | 以报文明确语义为准并置 `sampling_conflict` |

sFlow 的 sampling rate/pool 一般在 flow sample/data source 中；NetFlow/IPFIX 可由 sampling options/template 或 exporter 配置提供。GoFlow2 通用 `FlowMessage` 不包含 sFlow 的 `sub-agent/source-id/sample-pool/drop`，worker 通过 GoFlow2 producer 的同序旁带元数据保留这些字段，不二次解码报文。

GoFlow2 内建 NetFlow sampling store 的键只有 `(exporter, version, observation-domain)`，同一 domain 存在多个 sampler/selector 时会把最后一个 rate 套给所有记录。worker 因此在同一个 producer 回调内读取 GoFlow2 **已经解码**的 options/data fields，以有界 TTL FlowStore 保存 `(exporter, version, domain, sampler-id) -> rate`，逐记录修正 `SamplingRate`。这不是第二次协议解码，也不增加 topic、队列或数据库；真实 Akvorado 多采样器 pcap 必须得到 `4000/2000`，partition owner 重建而尚未重放 options data 时必须是 unknown，禁止继承旧 owner 状态。

plan 的 `default_sampling_rate` 只作为 exporter 最后兜底；pre-scaled 禁止配置该倍率。CLI 能对不同端口配置不同 rate，所以平台不能统一套一个采样率。每次配置变化生成新 registry version，历史事件仍按接收版本解释。

### 3.4 ClickHouse 提交协议（V1 历史基线，已由 §1.5/Storage V2 替代）

> 本节至 3.4.1 记录迁移 001–010 的旧行为，仅用于理解 legacy 表和回滚。当前实现不得再生成 stable record hash、block receipt 或内容 checksum。

1. consumer 把一次 fetch 按 partition 分组；partition 内顺序解码/富化，不跨 partition 混批；
2. 一个 partition fetch 只调用一次 durable handler；snapshot/binding 不可用时整批不写、不标 offset；
3. handler 按固定 rows/bytes 上限切 columnar block，以 Kafka 自然坐标 `(source_stream_id,partition,offset,record_index)` 和 insert token 同步写 `flow_records`；网络歧义重试同一 block；
4. 每个 Kafka message 的事实写成功后写一条 receipt，供 Kafka offset/row/count/counter 对账；热路径不预查 receipt，也不逐批执行 `FINAL`；
5. 全部 block 和 receipt 成功后 handler 返回，consumer 才标记该 partition fetch 的 offsets；
6. ClickHouse 是 at-least-once sink：重放可能短暂产生物理重复，自然坐标 + `ReplacingMergeTree` 收敛；面向用户的 base 查询和异步 rollup 必须按自然坐标去重；
7. 1m/1h rollup 只重建已关闭 event-time bucket，以 generation 原子替换；失败从 base 重算，不回写 Kafka。

这条链路没有两阶段提交、分布式事务或每批 read-before-write。Kafka offset 是消费进度，自然坐标是事实幂等键，receipt 只做审计/对账，三者职责不可混用。

崩溃发生在 records 与 receipt 之间时，重放直接使用相同自然坐标、generation 和 insert token，不做写前查询；物理重复由自然坐标 + `ReplacingMergeTree` 收敛。目标 ClickHouse 版本必须实测 insert token、ReplacingMergeTree、`FINAL` 和 replicated/distributed 变体；在这些证据完成前只承诺 at-least-once，不宣称 exactly-once。

#### 3.4.1 Ingest receipt 对账契约

Storage V2 的 receipt 是**每个允许提交的 Kafka message 一条回执**，不是 CH block 摘要，也不是 tenant 资源。权威身份为 `(source_stream_id, kafka_partition, kafka_offset)`；topic 只作诊断，`source_stream_id` 表示不可复用的 Kafka cluster/topic incarnation。每个 message 无论产生事实、模板缺失、零行或被确定性拒绝，都必须写入相应 disposition 的 receipt 后才允许提交 offset。事实以 `(source_stream_id, partition, offset, record_index)` 自然坐标和最大 `ingest_generation` 收敛，不再生成 `record_id`、`ingest_batch_id` 或内容 checksum。

对账右边界只能由配置的 worker consumer group 向**真实 Kafka broker**读取 `committed_next_offset`；禁止以 CH 最大 offset、receipt 最大 offset、worker 内存 lag 或 SNMP counter 冒充。`CommittedOffsetReader` 不加入消费组、不消费也不提交记录，使用稳定 OffsetFetch 并要求 exact topic metadata 存在且至少有一个 partition；未提交的 partition（offset `-1`）没有闭合窗口，不纳入当次扫描。

每次全局 job 第一次 attempt 原子冻结所有已提交 partition 的 close offset，之后 crash/retry 只能继续同一 checkpoint，不能刷新右边界造成移动目标。左边界优先读取 `flow_reconciliation_watermarks`；不存在时必须由 `flow.reconciliation.bootstrap_offsets` 显式给出每个已提交 partition 的上线 cutover。缺 cutover、partition 负数或 committed offset 小于既有水位均终态失败，绝不从 CH min/max 推断，也不静默把当前 committed offset 当起点。topic 重建必须换新的 `source_stream_id`。

scanner 逐一枚举连续的 `[next_offset, committed_next_offset)`，即使 receipt 与事实同时消失也能发现空洞。每个 chunk 同时受 message 数、事实行数和 CH read bytes 限制，cursor 不前进立即终态失败，避免预算过小造成死循环。比较口径如下：

| 项目 | receipt 权威值 | `flow_records` 最新 generation 复算 |
|---|---|---|
| message 身份 | stream/partition/offset/topic/disposition | topic 唯一；`record_index` 从 0 连续且无重复 |
| 规模 | `record_count` | generation 去重后的记录数 |
| 计数 | raw bytes/packets、estimated bytes/packets、estimated-valid records | 对应 `sum`；estimated 三项只统计 `estimated_valid` |

固定且互斥的主 reason 为 `missing_receipt > missing_records > identity_mismatch > count_mismatch > counter_mismatch`。删除 checksum 后明确不再承诺发现“行数和所有计数总和不变、但非计数字段被改写”的业务语义腐败；介质损坏由 CH part checksum 处理，字段映射准确性由 decoder 差分 corpus、schema contract、抽样明细回放和版本化 repair 测试承担。这一取舍让 Flow 写路径逐记录 hash 清零，而丢消息、丢/重事实和总量漂移仍可检测。

执行复用平台 `operation_jobs` 唯一状态机：每个 UTC interval bucket 使用 request hash + bucket 的唯一 idempotency key，公共 worker 提供 lease/heartbeat/cancel/takeover/retry；Flow handler 只冻结 Kafka 快照、分块调用 scanner、通过 attempt-scoped reporter 保存完整 checkpoint，并在 partition 完成后推进全局水位。持久化顺序固定为“先 report 完成 partition 的 checkpoint，再 advance watermark”；若两步间崩溃，takeover 从 checkpoint 重放幂等 advance，不会跳过未保存的扫描。不同进程或相邻 bucket 重叠完成时，MySQL 单调水位拒绝回退，已被更新窗口覆盖的旧完成为 no-op。关闭配置后不启动 scheduler/worker，也不执行遗留 queued job。

运行证据以 `operation_jobs` checkpoint/result 和 `flow_reconciliation_watermarks` 为权威，管理页读取后者展示 committed/reconciled next-offset、状态和 mismatch 总数。Prometheus reconciliation 指标属于后续可观测性切片；在真正接入唯一 metrics listener 前不得宣称已发布，也不得从不完整扫描产生伪零。

migration 011 的 `flow_ingest_audit_v2` projection 按 `(source_stream_id,kafka_partition,kafka_offset,record_index)` 服务这条窄扫描路径；001–010 的 block receipt、hash 列和 `flow_ingest_audit_v1` 只存在于 `_legacy_hash_v1` 回滚表。当前 system scanner 的落地只关闭“真实 Kafka 水位→CH 对账”门禁，**不会自动解锁 raw physical delete**：生命周期删除还必须另有按 UTC 日持久化的完整、无 mismatch 覆盖证明、delete grace、审计和故障恢复测试。

NetFlow v9/IPFIX 模板状态位于 worker 内存，而模板 record 可能已提交。每次 partition assignment 因此从原 committed offset 向前回放固定数量的 RawFlow record：旧窗口参与解码和幂等 CH 写入以重建模板，但提交水位绝不能低于 assignment 前的 committed offset；到达旧水位后才正常前进。`template_replay_records` 必须为正且有硬上限，并按“单 partition 在 exporter 最大模板刷新间隔内的 record 数 + 裕量”定容。exporter 必须周期刷新模板；未满足此前置条件时显示 template-missing/partial，不能声称完整，也不能用猜测字段解码。

单节点真实 Kafka 恢复门禁固定检查 committed-next-offset，而不是仅看 handler 调用次数：同一 partition 先写入 v9/IPFIX 的 template+data 并提交到 4，再只追加无模板的 data。新的冷 processor assignment 后向前回放 4 条；当新 data 的 durable handler 被注入失败时 group offset 必须仍为 4，第三个冷 processor 再次接管后必须从回放窗口恢复两套模板、解码旧 data `1/3` 和新 data `4/5`，最终提交到 6。该门禁证明有界回放、水位不倒退和依赖恢复后的可重放性；它不模拟 broker 断网、`kill -9`、两个存活成员间的 partition 转移或 lag 时间序列，这些仍是独立故障门禁。

### 3.5 FLOW-04B 实时关闭桶调度（V1 回滚兼容，生产禁与 Storage V2 并启）

> 下述 `flow_rollup` payload、水位与逐 1m/1h 关闭桶调度是 legacy rollback contract。Storage V2 使用 `flow_storage_downsample`、UTC 日、policy-version generation 和 §1.5 的 aging 条件；两者共享 CH rebuild primitive，不共享调度身份。

rollup 不在 `flow-worker` 内执行。hub 复用平台 `operation_jobs` 的 handler registry，以 `flow_rollup` 类型领取任务；通用 worker 继续负责 lease、heartbeat、cancel、takeover、retry/backoff 和终态，Flow 只提供版本化 payload、关闭桶入队和 ClickHouse handler。这样 CH 聚合失败只形成 rollup stale，不阻塞 Kafka 消费或 base fact 写入。

v1 payload 冻结为：

```json
{
  "schema_version": 1,
  "payload": {
    "resolution": "1m",
    "bucket_unix": 1788611640,
    "generation": 1
  }
}
```

tenant 只取 `operation_jobs.tenant_id`，不在 payload 复制；`generated_at` 取 job 的稳定 `created_at`，不在重复入队时使用当前时间。只接受 `1m/1h`、Unix epoch 之后的 UTC 对齐桶和正 generation。未知 schema、裸旧 payload、畸形字段、非法桶以及 CH schema/权限类永久错误直接进入 terminal failure；网络/超时类暂时错误使用公共 retry budget。

这里必须区分三个身份/水位：

| 名称 | 定义 | 用途 |
|---|---|---|
| 逻辑桶 | `(tenant, resolution, bucket)` | 表示同一个待汇总事实区间 |
| 执行幂等键 | `flow_rollup:v1:{resolution}:{bucket_unix_20}:g{generation_20}`，tenant 由唯一索引外层限定 | 同 generation 重试/并发入队收敛；repair 不与旧 payload 冲突 |
| 调度水位 | 通用 `operation_job_watermarks` 中 `(tenant, flow_rollup, v1:{resolution}) -> bucket_unix`，每租户/分辨率仅一行 | `operation_jobs` 终态会清理，不能承担长期水位；成功入队后才用 `GREATEST` 单调推进 |

调度水位不是 Kafka consumer offset，也不是用户查询的数据完整性水位。查询完整性仍由 CH `_generation` marker、覆盖桶数、ingest receipt 与 job 状态共同判定；只因任务已经入队绝不能报告 rollup complete。

关闭判定固定为 `eligible_end = truncate_UTC(now - late_arrival_window, resolution)`，只入队 `bucket < eligible_end`。空库/首次启用从 `ceil_UTC(eligible_end - bootstrap_lookback, resolution)` 开始，单次受 `max_buckets_per_series_scan` 和全局 `max_buckets_per_scan` 双预算限制；预算耗尽返回显式状态，下次依据持久调度水位接续，不能无界追赶。写顺序只能是 operation job durable enqueue → watermark advance；两步之间崩溃会重复入队同一键并收敛，反向顺序会永久跳桶，禁止使用。多实例并发扫描允许重复尝试，最终由 MySQL `(tenant_id, job_type, idempotency_key)` 唯一约束和水位 `GREATEST` 收敛。

正常关闭桶使用 generation 1。迟到窗口之后仍到达的 base fact、receipt 对账不一致或人工修正只触发 forward repair：从 CH `_generation` marker 读取该逻辑桶权威最大 generation，创建 `generation+1` 的新 operation job，整桶从 `flow_records FINAL` 重建；不能从有 14 天清理周期的 job 历史猜 generation，禁止 UPDATE 聚合行或复用旧 generation。读取同一旧 marker 的并发 repair 调用得到相同的新键并由 MySQL 收敛；新 marker 已可见后的下一次 repair 合法进入再下一代。重复执行使用相同 CH dedup token；代数耗尽必须人工处置。旧 payload 不迁就解析，rolling upgrade 必须先部署能读取 v1 的 handler，再启用 v1 producer；未来 v2 也遵循“先读后写”。

当前代码边界是：`internal/watchdog/flow_rollup_jobs.go` 实现 v1 contract、持久水位、有界关闭桶/tenant 分页扫描、repair 和 handler；`internal/flowch/rollup.go` 是唯一 CH rebuild/generation reader；migration 028 增加通用、常数行规模的 `operation_job_watermarks`，不增加 Flow 管理域表。hub 已完成默认关闭的生产装配，只有显式 `flow_rollup.enabled=true` 才建立专用 CH pool、注册 worker 并启动扫描，不在 `flow-worker` 增加旁路循环。所有运行时 INSERT/rollup/query SQL 使用未限定表名，以 CH native 连接选定的 `clickhouse_database` 为唯一数据库上下文；DDL 中的 `watchdog_flow` 是默认部署名，自定义数据库必须在迁移阶段一致替换，禁止运行时一部分写默认库、一部分查配置库。

运行配置只暴露一组必要预算：`scan_interval/late_arrival_window/bootstrap_lookback`、`max_tenants_per_scan/max_buckets_per_series_scan/max_buckets_per_scan`、`worker_concurrency/lease_for/max_attempts/retry_base` 以及 CH native endpoint/pool/timeout/TLS。默认关闭；密码只接受 `clickhouse_password_file`，不接受 YAML/env 明文。tenant 枚举按 active tenant 下未删除的 `flow_collect` collector 去重并使用稳定 keyset cursor；cursor 只负责轮询公平性，可在重启后丢失，真正的 bucket 水位必须持久化。

## 4. 地址、Geo、ASN 与分类

### 4.1 外部地址包与 Geo 树契约

watchdog 不连接 EdgeManager 数据库。当前 loader 可读取 EdgeManager 或其他供应方导出的 `flow-geo-v1`/`flow-geo-v2` 目录：

```text
flow-geo-v1/
  manifest.json
  ipv4.csv.zst
  ipv6.csv.zst
  operators.json
  geo_dict.json
```

导出器只读来源侧 `geo_base_v4/geo_base_v6/geo_subnets/isp_operators/geo_dict`，先把已批准的 subnet 修正展平到 IPv4/IPv6 不重叠区间，再输出以上四个规范化文件；watchdog 不连接也不回写 EdgeManager。`manifest.json` 的严格字段固定为 `schema/version/generated_at/effective_from/admin_code_system/unknown_country/files`，其中每个 file 只有 `sha256/rows`；未知字段直接拒绝。`effective_from` 是 UTC 分钟边界，导出时可显式指定，未指定则取当前 UTC 分钟。文件字段：

| 文件 | 必需字段 |
|---|---|
| `ipv4.csv.zst` / `ipv6.csv.zst` | v1：ip_start、ip_end、country、admin_code、subdivision、city、isp_id、asn；v2：再增加必需的 geo_leaf_code |
| `operators.json` | id、name、short_name、category、enabled |
| `geo_dict.json` | kind、code、name、parent_code、enabled |

运营商的候选 Flow 身份是 tenant 内稳定的 `UInt16`，`0` 永远保留为 unknown。管理面的 `isp_operators.flow_isp_id` 只读且不可修改；migration 051 用 `isp_operator_flow_id_sequences` 单调分配，用 `isp_operator_flow_ids` 保存不可复用的 allocation ledger。删除运营商只删除业务行，不删除 ledger；名称、code、ASN 可变，稳定 ID 不变；既有数据按 `created_at,id` 确定性回填。新增运营商的序列分配、ledger 和业务行必须在同一事务完成，并发创建不得重复。C4a 只建立管理身份，人工地址段此时仍只编译 `operator.id/code/category`，不得提前写 `flow.geo.isp_id`。

这只完成管理身份（PLAT-04C4a），不等于发布绑定完成。dimension bundle schema v2（PLAT-04C4b1）把 tenant operator 的管理 ID、稳定 Flow ID、显示字段、ASN 集合和启用状态纳入同一个 immutable/checksummed/signed 小型 definition object；发布前做规范排序并拒绝 0、重复 ID、重复 Flow ID、ASN 0/重复/乱序，以及同一 ASN 同时归属多个 enabled operator。ISP 是单值可加维度；需要交叠、层级或组合展示时使用 address set，不能让一个 ASN 随查询随机落入多个 ISP。旧 schema v1 对象继续可读，但不能夹带 v2 operators。该对象协议升级不增加管理表字段，所以不制造空 migration；新发布的 `entry_count` 是 prefix + set + operator 定义总数，preview 另返回 `operator_count`。

AddressSnap 构建前将 definition bundle 升为 schema v3（PLAT-04C4b1a）：v2 只携带 prefix label、set 规则和 operator，缺少人工 Geo 的名称/父子关系以及 set 名称，若直接构建会使历史展示依赖当前 MySQL。v3 增加规范排序的 `geo_nodes(id/kind/code/name/parent_id/enabled)` 和 `address_sets.name`，全部进入 draft digest、不可变对象与审批签名；编译拒绝缺父、同级/逆级父子、启用子挂禁用父、重复 kind/code 和空集合名。v1/v2 reader 保留，旧 schema 不得夹带新字段。该变化复用现有 JSON/object/计数字段，`entry_count` 更新为 prefix + set + operator + Geo node，preview 增加 `geo_node_count`，不新增 MySQL migration。

供应商 Geo bundle 与全局 customer operator 都使用 UInt16，但属于两个独立命名空间；不能直接覆盖数字 ID，否则会把同一个数字的不同运营商合并。C4b2 AddressSnap 分开保存 supplier ISP 与 customer ISP：人工 prefix 的 `operator_id` 直接解析为稳定 Flow ID；base range 只按非零 ASN 精确命中唯一 enabled operator，缺 ASN/未配置时 customer ISP 为 0，禁止按可变 name/code 模糊猜测。平台的异步 builder 消费 pinned source manifest + schema v3 definition object，合成为同一签名、不可变二进制 generation，并验证 `1..65535`、唯一性和所有引用完整；activation/rollback/consumer ACK 固定同一 snapshot/checksum。worker 只拉取该对象并离线构建内存索引，不连接 MySQL/CH。现行 CH migration 009 的单一 `isp_id UInt32` 和 IP_TRIE 仅为未接入生产的历史实验，不回改，也不再以前向 migration 扩展。当前便捷查询已经在覆盖时间窗的 enrichment publication 被全部 active worker 留下 installed milestone 后，按稳定 `remote_isp_id` 查询；不满足门禁即显式 incomplete，ASN fallback 已删除。

builder 的管理面分页顺序固定为 `(tenant_id,import_id,family,ip_start,prefix_length,id)`；migration 060 为该完整 keyset 建索引，不能用 offset pagination 代替。有序且互不重叠的 MMDB/IPDB generation 单遍验证并合并相邻同值 range；发现嵌套时自动退回通用 longest-prefix sweep。中间行只引用去重 Geo 值，prepared layer 只引用不可变 source/manual 值，避免为百万行复制空 Geo、map 和 slice。该优化不改变 WADS wire、签名、source row-count 校验或嵌套语义。

供应商原始 `operator_name` 没有可直接复用的数值身份，因此 migration 058 新增 tenant-scoped `address_supplier_operators` 与单调 allocation sequence：key 只做 trim/空白折叠/小写规范化，不做别名、相似度或 customer 对齐；同 key 首次分配 `1..65535`，删除 publication/导入时不回收。MMDB 的 `isp`/`organization` 可作为 supplier operator 来源，但 `autonomous_system_organization` 只是 ASN 的显示名称，严禁映射为 ISP；否则 GeoLite2-ASN 的 81,399 个不同 ASN 会同时造成语义污染和 UInt16 空间溢出。ASN 数值独立保存，后续如需 AS 名称展示应发布独立 ASN-name dictionary，不能借用 ISP ID。每个 AddressSnap 仅携带本 generation 实际引用的 supplier operator、当前显示名和 ASN evidence；customer operator 继续来自 `isp_operators.flow_isp_id`。同一个数字可同时存在于 supplier/customer namespace，事实字段必须带其语义列而不能跨 namespace join。

v1 的 range 行只有扁平 country/admin/city，不能完整表达“洲 → 区域 → 国家 → 省 → 市”。`flow-geo-v2` 保持四个文件和 manifest 校验机制，只给 range CSV 增加必需的 `geo_leaf_code`。每个展平后的不重叠 IP 区间只引用一个最具体 Geo 节点；`geo_dict.json` 保存邻接树，允许的产品层级固定为 `continent/region/country/province/city`。每个 code 在一个 bundle 内全局唯一、至多一个 parent；loader 拒绝缺父、环、同一路径重复 kind、逆序层级、禁用叶/祖先和超过五层的路径。节点名称只用于显示，事实和关系都引用稳定 code。

加载时由 `geo_leaf_code` 沿 `parent_code` 一次性预编译五级路径，并校验 range 的 country/admin 字段与路径不冲突；热路径只做一次地址区间查找，不逐 flow 追树或查库。ASN 和 ISP 均为 `0=unknown` 的可选属性，不是 range、Geo 节点或地址组的主键，也不影响无 ASN 地址的发布。找不到某一层时该层为 `_unassigned`，不得用上级或名称猜测。

同一不可变 `GeoIndex` 也是查询面的树语义权威：按稳定 code 返回节点和 root-to-node breadcrumb，按 parent 返回已启用直属 children，按 ancestor + 目标 kind 展开排序去重且有硬上限的 descendant codes。展开只允许 `flow-geo-v2`，一次请求固定一个 `geo_version`；不得跨版本按名称拼树，也不得由 hub 复制一套 loader/遍历算法。

loader 流程：在临时目录验证 manifest/schema/size/checksum/row count → 检查区间、外键、枚举、Geo 树和 IPv4/IPv6 → 构建不可变 range index + 预编译路径 → 原子交换指针。失败保留当前版本并告警。至少保留覆盖 Kafka retention、base 重分类窗口的旧版本；event time 选择 `effective_from <= event_time` 的最新版本。v2 loader、EdgeManager 导出脚本、worker 事实字段和 CH 002 向前迁移已经完成。Hub 始终挂载 Geo catalog 路由：先加载 `flow_geo.path` 的 active bundle，再加载 `flow_geo.historical_paths`；路径为空或 active bundle 未就绪时返回明确的 `503 service_unavailable`，不能表现为路由不存在，也不能连带清空独立加载的运营商/地址集合。查询目录和结果标签都委托同一个 `flowdimension.GeoCatalog`，按事实自身 `geo_version` 解析，缺失历史版本时显示稳定 ID 并明确缺少显示元数据，绝不回退 active 名称。查询结果的 source 徽标必须显示物理来源（`flow_records`、`1m` 或 `1h`），display 徽标才显示 bucket 步长，禁止用 `source_seconds` 把 base-fact 查询伪装成 rollup。

### 4.2 三种地址归属语义

| 语义 | 存储与命中 | 可否相加 |
|---|---|---|
| 主地址段 `primary_prefix` | `address_prefixes` 每个 canonical CIDR 一行；endpoint 最长前缀只命中一个稳定 ID，未命中为 `_unassigned` | 同一 endpoint role 下互斥；含 `_unassigned` 时可与总量对账 |
| Geo 层级 | range 只存一个 leaf，加载时得到 continent/region/country/province/city 各零或一个稳定 ID | 同一 level 互斥可加；不同 level 是同一字节的不同观察角度，严禁跨层相加 |
| 地址组 `address_set` | 独立的组 ID/组名 + label selector/显式 CIDR/组引用及排除；一个 endpoint 可命中任意多个组，数组排序去重并有硬上限 | 组间可能重叠，固定 `additive=false` |

`address_prefixes` 不要求 ASN。相同 CIDR 不为每个组或 Geo 层重复建行；需要多个组时让一行携带多个可选择 label，由多个 `address_sets` 分别匹配。需要 CIDR 逐级细化时允许嵌套行：发布编译从覆盖范围最大的父段到最具体子段继承 label，同 key 由子段覆盖，不同 key（如 continent/region/country/province/city/business）全部保留；运行时仍只做一次 LPM。主前缀 ID 永远取最具体行，继承不会产生多个 primary prefix。

MySQL 管理态保存 CIDR、labels、组 ID/名称和 selector；immutable publication 将名称之外的稳定 ID/规则编译给 worker。名称可改但 ID 不变，历史显示按记录引用的 dimension/Geo version 解析，不能用当前名称篡改历史语义。

base fact 一条 flow 只写一行：保存唯一 `local_prefix_id/remote_prefix_id`、去重后的 `*_address_set_ids`，以及 `remote_geo_continent_id/region_id/country_id/province_id/city_id`。不为五级 Geo 复制五条 base fact。异步 rollup 才将同一计数展开为五种独立 `dimension_kind`；每次查询必须选定一个 kind。地址段流量归类必定异步：collector 只送 raw datagram；worker 使用已加载内存快照做 LPM/Geo range lookup；CH rollup 从 base 的稳定 ID 汇总。禁止 UDP 热路径访问 MySQL、文件或 HTTP。

这里的“异步”指分类位于 Kafka 后的 worker，而不是推迟到查询：ingest 分类当前已经是 BART LPM + Geo 二分的纯内存实现，每条 flow 零 DB 查询。需要替换的是索引装载来源。正式发布物为 `AddressSnap`：平台 operation job 将钉住的 Geo/ASN import generation 与人工 prefix/set/operator 定义合成一个 dictionary-coded、v4/v6 有序区间的 zstd 二进制对象；object SHA 被审批签名覆盖，对象内部另有固定 header、section 边界和 CRC32C。worker 有界下载、验证、离线建 catalog 后一次 atomic swap，失败保留 last-known-good 并 ACK failed。磁盘 LKG 以 SHA 内容寻址保存对象、按 classification version 保存不可变签名 manifest，trust bundle 只允许 generation 单调替换；manifest 在 catalog 可见前持久化，冷启动在隔离 catalog 完整恢复后才一次发布，任一损坏不能产生半恢复。第一版继续使用现有 BART/二分；约 64 MiB 一级表的 DIR-24-8 只有在固定硬件基准证明必要且多租户共享基库容量成立时才可引入，禁止直接按租户复制。

远端同步不另造 publication 状态机：worker 使用平台既有 `agents/agent_credentials` 中 `kind=flow_worker` 的 token 或 mTLS 身份请求 trust、单调 desired page 和 publication-owned objects，先验签 envelope 再允许 object ref 绑定。HTTP redirect 被禁用；trust generation/checksum header、page cursor、object length/checksum header 与正文 SHA 均须一致。下载/校验失败不发布 manifest，installed ACK 超时则保留已经 durable/active 的本地版本，下一轮按同一 classification version 幂等重试且不重复传输已校验对象。downloaded/failed/installed 写 KISS migration 0031 的 ACK 里程碑，worker 不引入第二张状态表。旧 Hub migration 059/collector registry 只作为 wire 与失败语义的来源，不再是运行时、双写方或 schema 依赖。

`watchdog-flow-worker` 的 remote-version 模式与历史 `bootstrap-version-publication/geo-bundle` 模式互斥；地址版本不再混合两个权威。remote 模式以 token-file 或 mTLS 二选一认证，明文控制面只允许 loopback。启动必须按 LKG restore → initial sync → 至少一个 version 可选 → CH/Kafka 的顺序；控制面故障只有在完整 LKG 已恢复时才能降级启动。周期刷新串行执行并沿用最后成功 cursor，失败只记录并保留 catalog；shutdown 取消 HTTP context 并等待刷新协程。`--check` 仍是无网络的配置/本地制品检查，不能产生“检查即登录/请求”的副作用。

平台 writer 使用 `address_snapshot_build` job，API 线程只入队。job ID 也是不可变 snapshot/build ID；handler 以 `(family,ip_start,prefix_length,id)` keyset 分页读取 preview digest 所固定的 import generations，核对 durable v4/v6 原始行数并验证 CIDR 与二进制 start/end 一致。同 source 的嵌套 CIDR 是合法输入，必须先按最长前缀语义展平成互斥区间，不能要求“原始行数 = 输出区间数”。构建在长事务外进行，最终短事务锁全局地址库并再次核对 draft/source/version 后才插入 approval pending publication；版本竞争重试，草稿变化终止，二者都不能改 activation。导入是低频后台资料装载，允许分钟级完成但必须 checkpoint/状态/结果可追溯；发布是平台管理员每次编辑修订后的常态操作，不能重新解析原始 MMDB/IPDB，必须直接读取已就绪 generation，且 operation job 的状态、进度、错误和结果引用均可查询。旧 active 在新对象 build、审批或 worker 安装失败时持续服务。AddressSnap 是平台级单例，不归属于用户或 tenant；只有拥有全局 `address.manage/address.publish` 的管理员能上传、编辑、preview、build、审批及发布，其他角色最多只读，禁止复制 WADS。KISS migration 0013 保存 WADS format/version/builder/build-job 血缘，0031 的签名 pair 同时绑定 snapshot/version/ref/checksum 与 classification 对象；旧 JSON reader 只为滚动升级兼容，不能再被新 writer 生成。

KISS AuthContext 只有本机安装域的 user/role/permission，不再存在 tenant membership、owner tenant、tenant header 或“当前租户”切换。`GET /api/v1/me` 返回同一全局 RBAC 投影；前端据此隐藏维护入口，但每个 Gin API 仍分别强制 `address.view/manage/publish`。classification profile 是 `id=1` 的全局 CAS 编辑态，publication timeline 全局单调；0031 直接以 `dimension_snapshot_id` 外键引用唯一地址库快照，并让所有 worker catalog 共享同一份已编译 WADS。API 严格拒绝客户端夹带 `tenant_id`，从 schema、DTO、查询键、签名 envelope 到对象路径都不再保留 tenant 兼容列。

默认查询使用 fact 已存的派生稳定 ID 与其 snapshot/classification version，原始 `src_ip/dst_ip` 始终保留。按新地址定义重看历史通过 operation job 加载明确 AddressSnap 版本并从 raw fact 写新的派生 generation；以 Kafka 自然坐标、record count、raw/estimated byte/packet counter 守恒后才切换。查询、rollup 和修正均不使用 CH `dictGet`，也不以当前名称覆盖历史显示。完整格式、发布、迁移和验收见 [Flow 地址发布与查询计划](flow-address-query-plan.md)。

便捷运营商查询的 wire 不是 ASN fallback：浏览器只发送 `operator_selection.operator_id`。Flow provider preparer 以 repeatable-read 控制面快照解析全局 immutable `flow_isp_id` 和覆盖 `[from,to)` 的 publication timeline，并要求每个 active `flow_worker` 对每个所需 pair 都曾 ACK installed；`installed_at` 是不可逆里程碑，最新 attempt 后来 failed 不能抹掉已安装事实。通过后把 typed `isp = UInt16`、精确 dimension snapshot/classification version 集合和 schema-v1 prepared selection 写回 canonical parameters。它们同时成为延迟导出 query hash、响应 `meta.versions` 和 `query.executed` audit 的 provenance；prepared 字段、typed predicate、版本集合任一被客户端改写都 fail closed。该门禁每次查询准备只读一次 MySQL，不在 worker 入库热路径执行，且复用 migration 0031 与唯一 agent registry，不增加状态机。

### 4.3 KISS 单域发布与投递契约

发布链只迁移旧 Hub 已验证的行为，不重写数据面内核。WADS import/build 仍是可追溯 operation job；classification profile 是小型管理配置，使用 `If-Match` 同步 CAS；pair publish 只选择已经 active+approved 的 WADS、编译一个有界 classification JSON、生成 Ed25519 envelope，并在短事务内写 metadata/audit。worker 随后异步 pull，在 Kafka 消费热路径之外完成下载、验签、CRC/SHA、离线编译、LKG 持久化和 atomic catalog swap。实际每条 flow 的六分类仍是 Kafka 后 worker 内的同步内存 lookup，不访问 MySQL、ClickHouse、HTTP 或文件；查询和报表读取已经写入 CH 的 category/version，不按当前配置现场重分类。

| 生命周期 | 执行位置与时序 | 权威存储 | 失败语义 |
|---|---|---|---|
| MMDB/IPDB 导入、AddressSnap build | 平台异步 operation job，低频 | MySQL 状态/血缘 + object store WADS | 可重试且全程可查；旧 active 不变 |
| classification profile 编辑 | Gin 同步 CAS，低频 | MySQL singleton | stale `If-Match` 拒绝，不产生半更新 |
| pair publish | Gin 同步短事务，常态但低频 | MySQL immutable metadata + object store JSON | WADS/profile/version 任一变化即拒绝；孤儿对象补偿删除 |
| worker distribution/install | worker 周期异步 pull | 磁盘 LKG + 进程内 catalog | 任一校验/编译失败保留 LKG，不暴露半版本并 ACK failed |
| 单条 flow 分类 | flow worker 同步执行，逐记录 | 只读进程内 immutable snapshot | event time 无可用版本时暂停分区，绝不回退当前版本 |
| 查询/报表 | Gin QueryGateway → ClickHouse | CH fact/版本列，MySQL 只做一次版本/ACK 门禁 | 版本不完整显式 incomplete/fail closed，不实时扫地址库 |

| API | 鉴权 | 语义 |
|---|---|---|
| `GET/PUT /api/v1/flow/classification-profile` | `address.view/manage` | 读全局编辑态；写必须 `If-Match`，严格拒绝未知字段 |
| `GET/POST /api/v1/flow/enrichment-publications` | `address.view/publish` | 服务端分页/search/sort/filter；创建不可变 WADS+classification pair |
| `GET .../enrichment-publications/facets` | `address.view` | publication VTable 动态列候选 |
| `GET .../{publication_id}/acks[(/facets)]` | `address.view` | worker 安装证据的服务端 VTable/facets |
| `GET /api/v1/flow-workers/{id}/trust-bundle` | active `flow_worker` machine credential | 单调 trust generation，拒绝其他 agent kind |
| `GET /api/v1/flow-workers/{id}/enrichment-publications` | active `flow_worker` machine credential | 按 classification version 升序 cursor 拉取 canonical signed envelope |
| `GET .../{publication_id}/objects/{dimension\|classification}` | active `flow_worker` machine credential | publication-owned ref、有界大小、checksum/ETag、immutable cache |
| `POST .../{publication_id}/ack` | active `flow_worker` machine credential | body 只回显版本/校验和和结果；身份、worker、publication 均取路径/凭据并在锁内复核 |

真实 CH 五级守恒门禁使用独立数据库顺序执行 001..005，同一闭合 1m 桶只写三条 base fact，分别落到两个国家、两个省和三个城市；一次 rollup 后分别查询 `geo.continent/region/country/province/city`。每一级都必须保持 600 raw bytes、3 received records，同一级稳定 ID 唯一，父级值等于其叶节点之和；这只证明“每条 fact 每级一次”，协议输入链路由下述 Kafka 四协议 corpus 门禁独立证明。

四协议正常链路门禁已独立补齐：固定 Akvorado sFlow v5、NetFlow v5/v9、IPFIX pcap 通过真实回环 UDP Receiver、来源准入和自动 decoder 进入唯一 RawFlow topic，再由真实 consumer group、GoFlow2、immutable version catalog 和 native CH writer 落隔离数据库。验收从每条 CH fact 按 block 原顺序重算 receipt SHA-256，同时核对 record count、raw/estimated bytes/packets 和 valid-estimate count；再对所有 event-time 分钟桶执行 production rollup，continent/region/country/province/city 每一级的 raw bytes/records 都必须等于 base。测试用唯一 topic/database，成功或失败均清理，不改开发数据；该正常链路不能替代 broker/rebalance/worker/CH 故障矩阵、版本混跑或容量测试。

例如地址区间 `203.0.113.0/24` 的 range 只保存 `geo_leaf_code=330100`、`asn=0`；字典使用 EdgeManager 的稳定编码保存 `Asia <- EastAsia <- CN <- 330000 <- 330100`。worker 一次 lookup 后在同一 fact 得到五个 ID。若它还属于“重点客户”和“教育网”两个组，`remote_address_set_ids=[set-education,set-key-customer]`；不会复制成七条 flow，也不要求先分配 ASN。

### 4.3 地址集合代数与匹配

参考 EdgeManager 的 [`geo/setops.rs`](../../EdgeManager/backend/src/geo/setops.rs)、[`geo/build.rs`](../../EdgeManager/backend/src/geo/build.rs) 和 [`dns/engine.rs`](../../EdgeManager/backend/src/dns/engine.rs)，但 Flow 只复用集合语义，不形成运行时依赖。每个 address set 的确定公式为：

```text
membership = (selector ∪ explicit_members ∪ include_sets)
             − (explicit_exclude_members ∪ exclude_sets)
```

- `selector` 的同一字段多值为 OR，不同字段为 AND；所有字段均空时不匹配，不能意外成为全集。
- `explicit_members/exclude_members` 接受 canonical CIDR；任意 start-end 只允许导入端使用，发布前归一成最小 CIDR 集。IPv4/IPv6 是两个独立全集。
- `include_sets/exclude_sets` 只引用同租户稳定 ID；发布必须验证引用存在、依赖无环、深度/节点数/展开量有界；排除最终生效，因此同时 include/exclude 时 exclude 胜。
- 发布器汇总主前缀、成员和排除 CIDR 的全部边界。CIDR 的交叠必然是包含关系，因此不把 `/0` 展开成海量叶子区间，而是在第二棵不可变 LPM 中为每个边界预计算完整 membership；排除后为空的边界也必须写入，用空值截断父前缀。编译阶段可用 dense bool/bitmap 求闭包，LPM 节点保存排序去重的稳定 ID 数组；运行时只做主前缀 LPM + membership LPM，不扫描组、不追依赖。超过每 endpoint 或一条 in/out fact 的组数上限必须拒绝 publication，禁止静默截断。
- 地址组允许互相重叠，没有唯一胜者。EdgeManager DNS 的“显式 priority → 覆盖范围更小 → 稳定 ID”只适用于必须挑一条答案线路的策略；Flow 统计保留全部 membership。若将来某个策略必须挑唯一组，必须单独声明 winner policy，不能改变分析事实。

地址集合查询使用已入库的事件时间 membership，默认语义是“当时有效规则”。过滤规范固定为：

```json
"address_set_filter": {
  "include_any": ["set-a", "set-b"],
  "include_all": ["set-c"],
  "exclude_any": ["set-d"]
}
```

即 `(A ∪ B) ∩ C − D`；空数组不增加条件。`include_any` 用数组相交非空，`include_all` 用请求集合是 fact membership 的子集，`exclude_any` 要求交集为空。A∪B 的总流量必须从 base fact 以该谓词一次求和，不能把 address-set rollup 的 A 与 B 相加；A∩B、A−B 同理。任意集合表达式的大时间窗查询超出 base 扫描预算时转异步 job，不以错误近似值代替。

Geo 树也按集合化简：同层节点互斥；祖先包含后代。`parent ∪ child = parent`、`parent ∩ child = child`，父子同时选择时先归一为最小 antichain，避免重复。补集必须显式给出有限 universe（例如 tenant local prefixes 或某 Geo 父节点），禁止把默认补集解释为整个 IPv4+IPv6 空间。

### 4.4 业务方向

| src local | dst local | 结果 | remote endpoint |
|---|---|---|---|
| yes | no | `out` | dst |
| no | yes | `in` | src |
| yes | yes | `internal` | none |
| no | no | `transit` | none |

观察接口方向仅保存为 `observation_direction`。若与业务方向冲突，置 `direction_conflict`；不得翻转 endpoint。

### 4.5 六类规则

只对 `in/out` 分类，优先级不可配置：

| 优先级 | category | 条件 |
|---:|---|---|
| 1 | `overseas` | remote 不属于中国口径 |
| 2 | `on_net_local_city` | 本网、同城市 |
| 3 | `on_net_cross_city` | 本网、同省不同城市 |
| 4 | `on_net_cross_province` | 本网、外省 |
| 5 | `off_net_in_province` | 异网、同省 |
| 6 | `off_net_cross_province` | 异网、外省 |

Geo 不足得到 `unknown`。港澳台口径由 snapshot 固定。`home_isp_ids/home_asns` 分别匹配结构化 ISP ID/ASN，任一命中即为“本网”；只有远端具有与已配置集合可比较的身份时才判定异网，否则为 unknown，不以 ASN 名称字符串猜测。规则实现为纯函数，输入 record + immutable snapshot，输出 direction/category/provenance；property test 验证守恒和确定性。

### 4.6 修正视图

原始事实不可变。用户可选择：

- `raw`：导出协议直接携带的 src/dst IP、端口、ASN、接口和原始/估算计数；不附会 Geo、ISP、地址组或业务归属。方向未知时仍保留 src/dst，不为得到 local/remote 而猜测客户地址库；
- `supplier`：事件时间命中的只读供应商 Geo bundle 基线，以及“供应商 ASN 优先、缺失时回退 exporter ASN”的明确来源；方向、local/remote 和 category 仍使用同一客户 publication 的网络边界/分类规则，但不得包含客户 Geo/ASN/ISP override；
- `customer`：同一 supplier 基线上叠加租户版本化 prefix、Geo/ASN/ISP、address set 和 business override 后的生产视图。

`flow_records` 只保存一份原始 tuple/计数，同时保存 supplier 基线和 customer 最终值：`source_asn/destination_asn` 属于 raw；`supplier_remote_* / supplier_category / supplier_geo_version` 属于 supplier；现有 `remote_* / category / dimension_snapshot_id / classification_version` 属于 customer。`customer_geo_override_fields` 是稳定 bitset（country/admin/subdivision/city/isp/asn 分别为 bit 0..5），用于解释两个视图为何不同。`fact_schema=1` 的旧事实没有 supplier 基线，supplier 查询必须报告 unavailable/incomplete，禁止拿 customer 值冒充；migration 005 后由 worker 显式写 `fact_schema=2`。

规则含 reason、actor、审批、`effective_from/expires_at`、row version。修正只产生新 snapshot；新流量按事件时间选择已发布版本。历史修正必须复用 `operation_jobs`，输入固定为 tenant、时间窗、源/目标 publication、目标 view 和 generation；只从仍在线的原始事实，或具有连续 Kafka 坐标与版本证据的授权重放源重算，不能从单维 1h archive 伪造记录级重分类。作业先写隔离的新 generation，再按自然 Kafka 坐标覆盖、record count 和 raw/estimated counters 守恒，最后原子切换可见 generation；失败、取消或校验不通过继续读取旧 generation。禁止 `ALTER/UPDATE flow_records`、禁止复用 `ingest_generation` 表达分类版本，也禁止在请求线程扫描原始事实做大范围修正。原始事实已经按策略销毁且没有可验证重放源时稳定拒绝，不能产生“部分已修正”的结果。

本节冻结的是语义和事实 provenance。历史修正的派生投影/aggregate 写入契约必须在 FLOW-06B 通过真实 CH 容量测试后选定；在此之前不新增第六张高基数表，也不开放 supplier/raw aggregate API。这样 migration 005 是后续任何实现都必需的无损基线，不预埋未经验证的 overlay 状态机。

## 5. 管理和操作流程

### 5.1 开通向导

1. 检查 Kafka topic/TLS/ACL、CH schema、Geo bundle、VM scrape、UDP 端口和时钟；
2. 注册 collector/service identity，生成短期 enrollment，再换取可轮换凭据；
3. 录入 exporter 的 source IP/prefix、协议、target/device、可选 observation domain 和 sampling policy；
4. 发布签名 plan，collector ACK 实际 revision；
5. 发送测试 flow，核验 RawFlow、decoded records、CH base、分类守恒和页面完整性；
6. 才允许 exporter 从 pending 转 active。

未知来源不得自动激活。自动识别只生成候选：协议、source IP、domain、可能 target/interface、采样证据；管理员确认后才入 plan。

### 5.2 状态机

`flow_exporters`: `pending -> active <-> suspended -> retired -> deleted`。删除先从 plan 移除并确认 collector 已应用，再软删管理记录；CH/Kafka 按 retention 到期，不同步扫描历史。

collector 健康由平台根据 heartbeat 派生：plan/LKG、Kafka buffer/failure、UDP drops、版本和 last seen。agent 不能自报 healthy。配置、凭据、plan、exporter、规则和 job 操作都写 audit。

### 5.3 VPN 异步证据链

1. CH 关闭窗口后按境内外、端口/协议、ASN/高危 prefix、时长、流量比、单向性、QUIC/TCP/TLS hint 计算 candidate；
2. versioned rules 评分，命中 allow/suppress 则保留评价证据但不探测；
3. 达阈值后创建平台 `operation_job`；调度时再次检查租户授权范围、配额、cooldown 和 kill switch；
4. probe agent 根据 capabilities 执行 passive/handshake 插件；
5. 回传 analyzer/version/vantage/stop reason/confidence/脱敏 fingerprint；
6. finding 合并机器 verdict，人工 disposition 独立更新，二者不可互相覆盖。

FLOW-07A 的被动评分内核是纯函数边界：输入一个已关闭窗口的 normalized candidate 与一个已验证、不可变的 rule-set snapshot，输出可解释 `score/level/verdict/evidence` 和 `probe_recommended`；它不读 MySQL/CH/Kafka、不创建 job，也没有授权探测的能力。`probe_recommended=true` 仅表示评分和完整度达到门槛，平台调度仍必须重新检查 RBAC、目标范围、配额、cooldown 和 kill switch。

candidate v1 字段固定覆盖 window/conversation key、local/remote IP、主协议和端口、双向 bytes、flow record/active bucket 数、最大时长、remote ASN/country/prefix、显式 transport hints、窗口完整度，以及 dimension snapshot/Geo/classification 三个事实版本。输入约束是：conversation key 为 canonical 32-byte hex，窗口递增且不超过 24h，IP 无 zone，record/bucket 数为正，ratio 在 `[0,1]`，country 为大写 ISO2，ID 使用稳定编码，classification version 大于零。评分结果原样携带三个事实版本和 rule-set version，finding 写入前不得丢失或改写 provenance。双向行为定义为：

```text
symmetry = min(local_to_remote_bytes, remote_to_local_bytes)
           / max(local_to_remote_bytes, remote_to_local_bytes)
dominance = 1 - symmetry
```

两向都为 0 时两者均为 0；因此近似对称和明显单向是两个独立信号，不能互相替代。规则 schema v1 的固定 match registry 为 `remote_ports/protocols/remote_asns/remote_prefix_ids/remote_countries/transport_hints/min_duration_ms/min_total_bytes/min_flow_records/min_active_buckets/min_symmetry_ratio/min_dominance_ratio`：同一数组内 OR，不同非空字段间 AND。数组先排序去重且单信号最多 256，规则最多 1,000；空 match、未知 enum/hint、0 端口/协议/ASN、非法比例和重复 rule ID 全部拒绝。

`transport_hints` 只接受 `tcp/tls/quic` 的上游显式证据。端口 443 不会自动生成 TLS hint，UDP/443 也不会自动生成 QUIC hint；当前 base flow schema 没有 DPI/L7 证据，若无受支持 exporter/analyzer 就保持缺失并只使用端口/协议规则。这样可避免把普通 HTTPS/UDP 流量包装成协议识别结论。

规则 effect 为 `score/allow/suppress`。score weight 为 1..100，所有命中贡献求和并在 100 封顶，同时返回 `score_capped`；风险阈值满足 `0 < medium < high < critical <= 100`。allow/suppress 是不触发 probe 的 terminal rule，仍保留其它评分证据；多条 terminal 同时命中时按 priority DESC、同优先级 `suppress > allow`、再按 rule ID ASC 选唯一 decision rule。无 terminal 时，完整度不足返回 `incomplete_window`，分数不足返回 `below_threshold`，只有两项都通过才给出 `probe_candidate + probe_recommended`。证据按 rule ID 稳定排序，规则输入顺序不影响结果；历史 finding 永远记录 rule-set version。

FLOW-07A2 只在已关闭、UTC 分钟对齐且不超过 24 小时的窗口运行。materializer 从 `flow_records FINAL` 读取 `disposition=count`、方向为 `in/out` 且 local/remote endpoint 有效的事实，按 `tenant + local_ip + remote_ip + dimension_snapshot_id + geo_version + classification_version` 分组；会话 key 是 tenant 与规范化 endpoint 对的 SHA-256，版本不混入会话身份，而是进入存储替换键。主协议、端口、remote ASN/country/prefix 必须由同一条事实的稳定 `argMax(estimated_valid, estimated_bytes, raw_bytes, event_time, record_id)` tuple 产生，禁止分别取 max 后拼成从未出现过的组合。

双向 bytes 只累加 `estimated_valid=true` 的 `estimated_bytes`：`out` 为 local→remote，`in` 为 remote→local；未知采样事实仍计入 `flow_record_count/active_bucket_count` 和 evidence，但不把 raw bytes 混进估算口径。完整度为 `min(窗口内已有 1m _generation marker 数 / 预期分钟数, estimated_valid 事实数 / 全部事实数)`；quality record 数单列进入 versioned JSON evidence，不因 fallback 等非致命 flag 自动篡改完整度。当前 base fact 没有 DPI/L7 证据，materializer 只能由明确 IP protocol 6 生成 `tcp` hint，绝不由 443 端口猜 TLS，也不由 UDP/443 猜 QUIC；TLS/QUIC 只能由未来受支持 analyzer 的显式证据加入。

顺序 migration 003 向 001 的已发布基线前向增加 `remote_prefix_id/geo_version/classification_version/row_kind`。ClickHouse 不允许直接把已存在列追加到排序键，因此同一个可重放 `ALTER` 再新增由 writer 显式赋值的 `key_row_kind/key_dimension_snapshot_id/key_geo_version/key_classification_version`，并在保留原排序键完整前缀的基础上把这四列加入 replacement key；禁止用字符串拼接或散列替代精确 tuple 身份。旧行的 key 列为零/空且没有 generation marker，不属于新读取契约。每次 materialize 用一个同步 `INSERT SELECT ... UNION ALL` 原子写入全部 candidate 和内部 `_generation` marker，同时逐项复制四个 identity key；即使窗口为空也写 marker。相同请求使用稳定 dedup token；迟到或规则重算使用更大 generation。读取方必须先按 `tenant + window + rule_set_version + row_kind=_generation` 取得最新 generation，再只读该 generation 的 candidate，不能按剩余 candidate 求 max，否则空修复或候选消失后会泄漏旧行。001/002 不回改；旧行通过 `row_kind='candidate'` 默认值保持可读，但在没有对应 marker 时不属于新读取契约。

FLOW-07A3 reader/scorer bridge 用同一个参数化 CH 查询同时选择 marker 和该 marker 的 candidate，避免“两次查询先读 generation、再读数据”之间发生 repair 竞态。读取上限由请求给出且不得超过 50,000，SQL 用 `limit+1`，runner 再做独立硬限；tenant、窗口、rule-set version 和 limit 全是 typed parameter。CH 的 UNION/block 返回顺序不构成协议，metadata 可先于或后于数据；runner 只在整次执行结束后验证恰有一个 marker、generation 大于零且所有 candidate generation 一致。列数/列长、重复候选键、IPv4-mapped 还原、四类版本、canonical conversation key、显式 hint、evidence schema/counter、complete ratio 和 scorer 输入逐项验证；缺 marker、坏行、部分响应、执行错误或超限全部 fail-closed，不返回部分评分。空窗口只有合法 marker 时返回空 candidate 集与对应 generation，不回看旧行。该 bridge 仍无 MySQL 写入和 probe/job 副作用。

VPN 规则升级/回滚不修改事实或旧候选：同一 conversation 在不同 `dimension_snapshot_id + geo_version + classification_version` 下必须生成不同 candidate key；同一窗口的 rule-set v1/v2 也各有自己的 marker/generation 空间。发布 v2 后查询 v1 必须仍得到原 generation/result；回滚仅表示管理面重新选择不可变 v1，并以更大 v1 generation 重新 materialize，不能删除 v2 或覆写旧 v1。真实 CH 变更门禁同时保留两组事实版本，执行 v1→v2→v1 并逐项核对版本、generation 和 score；它只证明数据面隔离，typed publication CRUD/审批/If-Match/audit 和投递确认仍由平台承担。

真实 CH candidate 门禁在独立数据库顺序执行 001..005，写入同一 local/remote IPv6 tuple 的 out、反向 in 和 unknown-sampling 三条事实，先生成两个 1m coverage marker，再执行 generation 1 materialize/read/score。它必须得到一个稳定 SHA-256 conversation key、双向 `1000/900` bytes、3 条事实/2 个 active bucket、`2/3` 完整度和质量计数；追加迟到 out 事实并修复对应 rollup 后，generation 2 必须收敛为 `1700/900`、4 条事实、`3/4` 完整度，评分才可从完整度阻断转为 probe recommendation。最后用同 record ID 的较新 drop 事实生成 generation 3，reader 只能返回权威空集，不能泄漏旧 candidate。该门禁验证 candidate 数据面与纯评分边界，不替代 rule publication、finding 持久化、RBAC 或主动探测授权。

## 6. MySQL 管理契约

Flow exporter 不是 collector，也不是第二份设备。KISS 管理面以全局 `devices` 作为唯一设备根；SNMP 是设备配置，Flow 则由一对多 `flow_exporter_bindings` 从表表达。`agents(kind=flow_collect)` 表示接收进程，binding 才表示设备发流来源、协议、observation domain、采样和 collector 归属。`/api/v1/flow/devices` 是该关联的管理视图，不创建独立设备身份；只开 Flow 的设备同样先有 `devices` 行，SNMP 字段为空。旧 `collector_agents` 和 plan 中的 tenant 字段仅是 KISS-06 尚待移除的兼容数据面契约，不能反向污染新管理库。

| 表 | 必需字段 | 唯一身份与索引 | 生命周期 |
|---|---|---|---|
| flow_exporter_bindings | device、可空 flow-collect agent、规范 source prefix、protocol、可空 observation domain、sampling mode/rules、observations、enabled、ownership epoch、row/published version | 稳定 binding ID；protocol+source prefix+domain 唯一；device/collector 索引 | 新增/编辑只改变 desired row；`published_row_version == row_version` 才表示对应配置已发布，enabled=false 且撤销版本已发布后才可物理删除 |
| flow_classification_profiles | singleton id、home province/city、home ISP/ASN、港澳台口径、internal/transit policy、definition digest、row version | `id=1` 主键与 CHECK | 全局单行 CAS 编辑态；数组规范排序/去重，发布时由统一 compiler 再校验 |
| flow_enrichment_publications | classification version/effective time/profile version、WADS snapshot/version/effective/ref/checksum、classification ref/checksum、Ed25519 key/signature | classification version 与 effective time 分别全局唯一 | metadata/version pair 不可变；大对象不进 MySQL；新 signing key 只签新版本，不回写旧 pair |
| flow_enrichment_publication_acks | publication、worker、boot/software、download/install milestone、最后 attempt/error、row version | publication+worker 主键；按 worker/state 查询 | downloaded -> installed；后续失败保留既有成功 milestone，不把失败尝试伪装成卸载 |
| flow_vpn_rules | kind、versioned selectors/behavior/intelligence/probe policy、weight、status、row version | 活跃名称按 tenant 唯一；按 tenant+status 查询 | draft -> active <-> suspended -> retired -> deleted |
| flow_vpn_findings | window/conversation、双向 bytes、协议/端口、score/level/verdict/disposition、evidence/rule version、probe result、snapshot/geo version、expiry | tenant + window + conversation + rule_set_version 唯一；按 level/probe status 查询 | 机器 verdict 与人工 disposition 独立；TTL 后可验证销毁 |

统一约束：

- ID 使用平台稳定 ID；名称只展示，不参与引用；IP 用 16-byte binary；时间为 UTC millisecond。
- JSON 必须有 schema version，API 拒绝未知 key、非法枚举、过宽 CIDR/端口和超量数组。
- 创建/action 要求 Idempotency-Key；PATCH/DELETE 要求强 If-Match，成功递增 row_version。
- agent、device、用户、snapshot 和 publication 的外键只能引用当时真实存在且长度一致的 KISS 表；禁止恢复 tenant/collector-registry 兼容外键或先在设计中虚构外键。
- immutable dimension publication 是平台前置项 PLAT-04A；落地前 worker 仅使用验签静态 publication，绝不逐 flow 查询 MySQL。

## 7. ClickHouse 5 张表

完整、可执行且唯一权威的单节点 DDL 是 [`deploy/migration/clickhouse/`](../deploy/migration/clickhouse/) 下按文件名顺序执行的 migration：001 建立基线，002 向前增加 Geo v2 五级稳定 ID 和 ASN 来源枚举，003 完成 VPN candidate provenance 与 generation marker，004 增加 ingest receipt v2 审计元数据，005 前向保留 supplier/customer 双层事实 provenance，006 增加可同步物化、按 Kafka offset 排序的窄 ingest-audit projection。设计文档不再复制一份会漂移的 SQL。生产集群只允许由后续 migration 生成 Replicated/Distributed 变体，不在运行时拼 DDL。

CH migration 文件名固定为连续的 `NNN_lower_snake.sql`，单文件不超过 4 MiB、必须是无 BOM/NUL 的 UTF-8；SHA-256 覆盖精确文件字节，已发布文件连注释和空白都禁止修改。loader 只在引号/反引号/行注释/块注释之外按分号拆 statement，避免把 enum 默认值中的分号误拆。planner 将本地清单与 CH 最新状态按 version/name/checksum 对齐：缺号、重复、数据库超前、checksum 漂移、未知状态全部 fail-closed；`applying/failed` 是 dirty，普通 apply 禁止隐式重放，只有显式 resume 且 dirty 是最后一条记录时才从该版本继续。08A1 只冻结纯 loader/planner；状态表、并发锁、inspect/apply/resume CLI 和真实 CH 故障恢复由 08A2 完成，worker/hub 启动永不隐式改 schema。

08A2 将 migration 生命周期收口到专用 `watchdog-flow-migrate`，并把 001..current 精确嵌入二进制；运行时不接受任意 SQL 目录，因而旧二进制面对更高数据库版本会 fail-closed，新二进制才能按其内置 checksum 集合前进。命令固定为 `inspect/apply/resume/unlock`：inspect 只读 `system.tables` 和 ledger，不建库、不建表；apply 只接受 clean history；resume 是处理 dirty 的唯一入口；unlock 必须提交锁创建时输出的精确 owner token。连接固定从 `default` 数据库进入并执行 canonical `watchdog_flow` DDL，密码只从 secret file 读取，TLS 文件在 TLS 未启用时一律拒绝。自定义数据库仍必须在发布阶段生成一致的 migration 制品，不允许运行时字符串替换部分 DDL。

`watchdog_flow.flow_schema_migrations` 由执行器 bootstrap，采用 `ReplacingMergeTree(generation) ORDER BY version`，每个最新状态保存 version/name/exact-byte checksum、`applying|applied|failed`、已完成 statement 数、attempt/lock owner、截断到 2048 bytes 且保持 UTF-8 的诊断、单调 generation 和更新时间。执行顺序固定为：取得持久 fail-fast 锁 → 读 `FINAL` ledger → 规划 → 写 applying → 串行执行一条 statement → 同步写 checkpoint → 全部完成后写 applied。所有 migration statement 与 ledger INSERT 显式关闭 async insert。若 DDL 已成功而 checkpoint 应答失败，failed 状态能写入时记录该 statement 已完成；若连 failed 状态也不可写，resume 最多重放最后一条未确认 statement，因此所有发布 migration 必须保持 forward-only、可重入，禁止破坏性回滚。

并发锁使用 `watchdog_flow.flow_schema_migration_lock` 的原子 `CREATE TABLE` 竞争，不轮询、不排队；owner 保存在持久 table metadata comment，而不是重启即丢的 Memory 行。已有表即返回当前 owner/创建时间；正常退出在无视调用方 cancel 的 5 秒清理上下文中校验 owner 后 drop。ClickHouse 或迁移进程异常留下的锁不会按时间自动接管，因为平台无法区分慢 DDL 与死亡进程；只能由操作者确认旧进程已退出后执行 exact-owner unlock。空/损坏 metadata、owner 不匹配或锁释放失败均 fail-closed 并保留诊断。

本地真实门禁使用 `deploy/compose.flow-dev.yml`：固定 Apache Kafka KRaft 与 ClickHouse LTS 完整版本、所有宿主端口只绑定 `127.0.0.1`、Kafka 关闭 auto-create，并由一次性 init 服务幂等创建 12 分区的 `watchdog.flow.raw-v1`。Compose 是单节点功能/故障恢复环境，不冒充生产 HA 或吞吐验收；worker/hub 仍不得因容器存在而自动迁移。

08A3 的版本兼容规则只允许“相同不可变前缀 + 新 migration 后缀”：旧 migration 集先应用后，新二进制必须只看到待执行后缀；数据库版本高于二进制内置集合时，旧二进制必须拒绝；任何已记录 version 的 name/checksum 变化，无论状态 clean/dirty，都必须拒绝，系统不提供 adopt-checksum、跳过或自动改 ledger 的逃生口。预发布 migration 只有在尚未通过外部发布门禁且没有生产 ledger 时才可修正；一旦制品发布，修复只能新增下一版本 migration。进程 cancel/超时后不再执行新 statement，但锁释放使用独立的短清理上下文；若 ClickHouse 不可达或进程被强杀，持久锁跨服务重启保留，必须在确认旧进程死亡后 exact-owner unlock。

单节点故障门禁至少覆盖：旧集合 001..N → 新集合 001..N+1、旧集合读新库拒绝、checksum drift 拒绝、最后一条未确认 statement 重放、调用方 cancel/deadline 后锁释放与新连接池 resume、锁跨 ClickHouse restart 保留、Kafka `acks=all` 记录跨 broker restart 可读。它只能证明本地持久化和 fail-closed 控制面；不能证明副本故障、controller quorum、ISR 收缩、Replicated/Distributed DDL、N+1 容量或 RPO/RTO。

ClickHouse 时限必须拆成三类，不得把配置名当成库行为推断：`DialTimeout` 约束 TCP 建连；ch-go `ReadTimeout` 仅给一次 packet read 设置 deadline，库捕获该网络超时后继续轮询，因此它既不是静默链路的失败时限，也不会因有/无 progress packet 决定整条查询何时结束；`OperationTimeout` 才是一次 pool 建立或完整 `Do` 的硬上限，并与调用方 context 取更早者。worker 默认 operation 2m，migration CLI 与 hub rollup 默认 5m；预计更长的 DDL/rollup 必须显式调大，不能设成无界。透明 TCP 代理门禁在成功 control query 后仅丢弃 server→client 响应，验证内部 operation deadline、单连接无隐式 DDL retry，以及关闭旧池后新池恢复。第二条门禁按协议时序放行 INSERT column metadata 和客户端 block 后丢弃最终响应，必须先观察到“facts 已提交、receipt 缺失”的模糊状态，再用完全相同的 `PreparedBlock`/dedup token 重放并以 `FINAL` fact count、raw counters、单条 receipt/checksum 收敛为成功；禁止把“首次根本没提交”误报成幂等恢复。第三条门禁在下一服务端响应的确定 byte 边界截断 TCP，要求立即返回 transport/decode error、结果对象保持空、连接层零重试且新池可恢复；它与完整 packet 后静默丢弃是两类不同故障，不能互相替代。幂等性还必须脱离 ClickHouse 短期 dedup cache 验证：隔离环境把 `non_replicated_deduplication_window` 设为 0 并暂停 merge，两次相同 block 应出现两份物理 rows，而 `FINAL` 依赖稳定 record/batch identity 仍精确收敛；若物理层没有重复，不能证明 dedup window 外语义。操作者仍必须先 inspect，再按 clean/dirty 状态选择 apply/resume。

| 表 | 角色 | 幂等/查询规则 |
|---|---|---|
| `flow_records` | 完整 enriched base fact | Kafka 自然坐标 + `ingest_generation` 收敛；按 tenant/UTC 日分区；无固定 TTL |
| `flow_aggregate_1m` | migration 兼容表 | Storage V2 不持续写；1m 查询直接读 raw |
| `flow_aggregate_1h` | 策略老化后的归档 | 每个 tenant/UTC 日一次 operation job 写 24 桶；按 policy/repair generation 取最新 |
| `flow_ingest_receipts` | 每个可提交 Kafka message 的回执 | 自然坐标唯一；保存 disposition/count/counter，不保存 batch hash/content checksum |
| `flow_vpn_candidates` | 异步 VPN 候选 | 事实三版本 + 规则版本隔离；marker 选最新 generation，证据有 TTL |

DDL 使用代码里的准确枚举名（如 `on_net_local_city`、`off_net_in_province`），`quality_flags` 保存 UInt64 bitset，`estimated_valid` 与零值显式分离；这些字段由 migration contract test 按顺序合成最终 schema 后与 Go encoder/物化 SQL 核对。001–010 保持已发布的 V1 基线且禁止改写，011 在维护窗口换表到自然坐标/逐消息 receipt 并保留 legacy 表；以后只允许新增 forward migration。

`dimension_kind` 的公开 registry 固定为 `total/category/geo.continent/geo.region/geo.country/geo.province/geo.city/isp/asn/business/local_prefix/remote_prefix/address_set/src_ip/dst_ip/remote_port/protocol/observation_interface`；`_generation` 是不可查询的内部 marker。每次查询必须选一个公开 kind。`primary_prefix` 和每个单独 Geo level 在包含 `_unassigned` 时可与 `total` 对账；`address_set` 是重叠标签统计，不能与 `total` 对账；不同 Geo level 也不能彼此相加。1m/1h rollup 由异步 job 对单个 `tenant + 已关闭 bucket` 发出一次原子 `INSERT SELECT`，同一 generation 同时生成全部维度和 marker；迟到/修正以更大 generation 完整重建。查询先按 `tenant+bucket`（包括 marker）求最新 generation，再只读该 generation 的公开 kind，不能逐 key `argMax`，否则新版本已消失的旧 key 会残留。IPv4 写 IPv4-mapped IPv6，API 还原文本。

## 8. VM 指标与 SNMP 对账

两个数据面进程各自提供 Prometheus 文本格式 `GET /metrics`，由 VM pull/scrape；进程不主动写 VM。collector 默认监听 `127.0.0.1:9090`，worker 默认监听 `127.0.0.1:9091`，`-metrics-listen ''` 明确禁用。地址必须是裸 `host:port`，不接受 URL/路径或 0 端口；绑定失败使进程启动失败，运行中 metrics server 异常退出也会终止主进程，避免“数据面活着但监控永久消失”。端点只读、无业务数据，默认仅 loopback；跨主机抓取必须由部署层提供网络 ACL，TLS/mTLS 通过同机反向代理或 sidecar 终止，不在两个数据面进程中复制证书生命周期。

真实 VictoriaMetrics wire 门禁使用 production collector/worker handler 启动回环 HTTP server，读取未经改写的 Prometheus text，经 VM Prometheus import 后用 instant query 核对 durable Kafka record 与 worker lag，并枚举 series 拒绝 tenant/exporter/ASN/prefix/IP/topic/partition 高基数标签。测试用唯一 `integration_run`/process 外部抓取标签隔离并在退出时精确删除。该门禁只证明 exposition→VM storage/query 兼容；VM/vmagent 的 pull discovery、两个真实进程、网络 ACL/TLS、scrape `up` 和故障恢复仍必须用部署配置验收，不能因为 import 成功而标记完成。

指标契约如下；除表内固定枚举外不允许增加 label：

| 进程/层 | metric | 类型/单位 | 语义 |
|---|---|---|---|
| collector | `watchdog_flow_collector_datagrams_received_total{decoder="sflow|netflow"}` | counter/datagram | 已通过 source plan 准入且识别 decoder 的 UDP 报文 |
| collector | `watchdog_flow_collector_datagrams_rejected_total` / `invalid_total` / `oversize_total` / `kernel_drops_total` | counter/datagram | 来源拒绝、空/不支持、截断和内核 RXQ drop；互不混算 |
| collector/Kafka | `watchdog_flow_collector_kafka_records_total` / `bytes_total` / `publish_errors_total` | counter/record,byte | producer callback 确认的成功、编码后字节和最终失败 |
| collector/Kafka | `watchdog_flow_collector_kafka_buffered_records` | gauge/record | franz-go 当前有界 producer 队列 |
| collector/Kafka | `watchdog_flow_collector_kafka_produce_duration_seconds_total` | counter/second | Produce 调用到 acknowledgement callback 的累计时长；以 `(success+error)` 作次数计算均值 |
| collector/plan | `watchdog_flow_collector_plan_revision` / `plan_age_seconds` | gauge/revision,second | 当前签名 plan 版本和本次加载年龄 |
| worker/Kafka | `watchdog_flow_worker_kafka_records_total` / `bytes_total` / `errors_total` | counter/record,byte,error | durable handler 成功后可提交的 record/byte，以及 fetch/handler 失败 |
| worker/Kafka | `watchdog_flow_worker_kafka_polls_total` / `poll_duration_seconds_total` | counter/poll,second | 已完成 poll 次数（包括关闭时取消的末次 poll）和累计阻塞时长 |
| worker/Kafka | `watchdog_flow_worker_kafka_rebalances_total` / `lost_partitions_total` | counter/event,partition | 完成 assignment 的 rebalance 次数和未安全 revoke 的丢失分区数 |
| worker/Kafka | `watchdog_flow_worker_kafka_assigned_partitions` / `lag_known_partitions` / `lag_records` | gauge/partition,partition,record | 当前 assignment；有 high-watermark 证据的分区数；这些分区最新 durable/committable offset 到 high-watermark 的总差值 |
| worker | `watchdog_flow_worker_datagrams_decoded_total` / `records_persisted_total` | counter/datagram,record | 成功 decode 的 datagram 和 CH durable 后的 base records |
| worker | `watchdog_flow_worker_template_missing_total` / `rejected_total` / `retryable_errors_total` | counter/event | 模板缺失、永久拒绝和需 Kafka 重放的失败 |
| worker | `watchdog_flow_worker_sampling_unknown_records_total` / `sampling_conflict_records_total` / `snapshot_miss_total` | counter/record,event | 仅在 durable 后计采样质量；snapshot 缺失按被阻塞 enrich 尝试计 |
| CH | `watchdog_flow_clickhouse_insert_attempts_total` / `insert_errors_total{class="retryable|permanent"}` / `insert_retries_total` | counter/attempt | 每次真实 insert 调用、固定两类失败和实际 backoff 后的重试 |
| CH | `watchdog_flow_clickhouse_blocks_total` / `rows_total` / `insert_duration_seconds_total` | counter/block,row,second | records+receipt durable block、行数和所有 insert attempt 累计耗时 |
| reconciliation | `watchdog_flow_ingest_reconciliation_mismatches{reason}` / `last_success_timestamp_seconds` / `scan_complete` | gauge/window,timestamp,bool | 最后一次完整 Kafka committed-offset 快照的五类守恒结果；不完整时保留数值并置 complete=0 |
| rollup | `watchdog_flow_rollup_attempts_total` / `success_total` / `errors_total{resolution,class}` | counter/attempt | 真实 CH rebuild 调用、成功和固定 retryable/permanent 失败；resolution 仅 `1m/1h` |
| rollup | `watchdog_flow_rollup_rebuilds_total{resolution,kind}` | counter/rebuild | 成功的 initial/repair；`generation=1` 为 initial，更大 generation 为 repair |
| rollup | `watchdog_flow_rollup_last_success_timestamp_seconds` / `completed_bucket_known` / `completed_bucket_age_seconds` | gauge | 当前 hub 进程最近成功时间与完成的最大 bucket end；无成功样本以 known=0 表示，age 零值不可单独解释 |
| process | `watchdog_flow_process_goroutines` / `heap_alloc_bytes` / `gc_cycles_total` / `start_time_seconds` | gauge,gauge,counter,gauge | Go runtime 状态 |
| process | `watchdog_flow_process_stats_up` / `cpu_seconds_total` / `resident_memory_bytes` / `open_fds` | gauge,counter,gauge,gauge | OS 进程统计；读取任一失败时 `stats_up=0`，其余值归零；Prometheus 自身 `up` 仍表示 scrape 可达性 |

counter 只用 atomic 单调累加；gauge 来自单次一致快照。worker lag 在 assignment 后、首次 high-watermark fetch 前是 unknown，必须用 `lag_known_partitions < assigned_partitions` 表达不完整，禁止填成零。模板有界 replay 期间，低于原 committed floor 的 record 不更新 lag；到达原水位后才更新。当前 consumer 没有 pause/resume 状态，CH/dimension 暂时失败直接不提交并退出等待编排重启，因此不暴露恒为零的伪 pause 指标。rollup 指标在真正调用 CH 的 runner 内计 attempt/error/success，repair 只按已验证 request 的 generation 判定；latest completed bucket 取本进程成功集合的最大 bucket end，旧桶 repair 不使 age 倒退，也不声称代表每个 tenant 的覆盖率。

禁止用 tenant、IP、ASN、prefix、port、topic、partition、bucket、job ID、generation 或 exporter 原始地址作 metric label。高基数分析只进 CH。rollup 与 reconciliation provider 均组合进 hub 现有 runtime Prometheus 文本；统一 machine scrape/TLS/ACL 使用平台 PLAT-04E 的同一 listener，Flow 不另起第三个 HTTP server。ingest receipt mismatch 只由真实 Kafka OffsetFetch + CH scanner 产生，不能在 worker 猜测或为了指标另建巡检状态机。

告警至少同时判断 scrape `up`、`watchdog_flow_process_stats_up`、UDP kernel drop 增量、Kafka publish error/buffer、`lag_known_partitions/assigned_partitions`、lag 增长、template/snapshot miss、CH retryable/permanent error。任何未知覆盖率或缺失分区必须显示 incomplete，不得按零流量处理。

SNMP 对账不是分类前置条件。按同 device + ifIndex + observation direction + window 比较：

```text
flow_bytes = sum(valid estimated_bytes)
snmp_bytes = delta(ifHCInOctets or ifHCOutOctets)
coverage   = valid_duration / window_duration
deviation  = abs(flow_bytes - snmp_bytes) / max(snmp_bytes, 1)
```

窗口必须排除 counter reset/wrap、设备重启、接口映射变化和 incomplete Flow。结果只报警和辅助修 sampling/exporter/观察点配置，绝不按比例改写 Flow。

## 9. 查询契约与关键 SQL

### 9.1 统一 QueryRequest

```json
{
  "dataset": "flow.traffic",
  "from": "2026-09-05T00:00:00Z",
  "to": "2026-09-12T00:00:00Z",
  "step_seconds": 0,
  "limit": 250000,
  "value_layer": "customer",
  "parameters": {
    "metric": "estimated_bps",
    "dimension": "geo.city",
    "filters": {"directions":["in","out"],"dimension_values":["330100","330200"]},
    "top_n": 20,
    "include_other": true,
    "target_points": 300,
    "timezone": "Asia/Shanghai"
  }
}
```

tenant 不属于客户端 QueryRequest，由已认证 scope 单独注入 compiler；请求里的任意 ID、名称和时间都只能成为 typed ClickHouse parameter，不能进入 SQL 标识符或表达式。`from/to` 是用户业务时间窗，`target_points` 是期望图表密度，`step_seconds=0` 表示自动；非零值是显式**展示步长**，不是 CH 表名。当前 provider registry 固定为：

- 时间/分辨率 planner：时间窗支持多预设和任意自定义；默认 `target_points=300`（允许 5..2000），按 `ceil(range/points)` 选择不小于目标的稳定人类可读步长（1m/5m/15m/30m/1h/2h/3h/6h/12h/1d/2d/7d/30d）。再选择不粗于展示步长的最粗可用源：近期细粒度读 `flow_aggregate_1m`，小时以上读 `flow_aggregate_1h`；1m 源最多 10,080 个闭桶，1h 源最多 9,600 个闭桶。响应必须返回 `requested/effective range + source/source_seconds + step_seconds + target_points`，不能让前端猜源表。
- `from/to` 统一换算 UTC、左闭右开；provider 只查询已关闭 source bucket，并明确返回对齐后的 effective range。展示 bucket 以 effective-from 为锚点，可由多个 source bucket 汇聚；自定义范围末尾不足一个展示步长时，bps/pps 必须除以该末桶实际覆盖秒数，不能按完整展示步长低估。
- metric：`raw_bytes/raw_bps/raw_packets/raw_pps/estimated_bytes/estimated_bps/estimated_packets/estimated_pps/received_records`；速率以当前**展示桶实际秒数**计算，TopN 仍按对应可加计数排序；
- dimension：第 7 节公开 registry 的 18 个值；只有 `address_set` 可多归属且 `additive=false`，禁止 `include_other`，其他单值维度都通过 `_unassigned` 保持与 total 可对账；
- filter：`directions/categories/businesses/target_ids/device_ids/exporter_ids/dimension_values/dimension_snapshot_ids/geo_versions/classification_versions`。枚举严格校验，值排序去重，单字段最多 2,048、总计最多 4,096；API 将 `geo_ancestor_ids` 用同一 Geo version 展开后才填 `dimension_values`；
- TopN 为 1..100，稳定顺序是值 DESC，再按 dimension ID 和三个版本字段 ASC；`total` 要求 TopN=1。点数乘 `(TopN + other)` 超过 250,000 时在查询前拒绝，CH 同时设置 15 秒、25 万结果行、5,000 万扫描行硬限并以 `throw` 结束，不能静默截断；
- `timezone` 只控制 API/前端显示，CH bucket 始终 UTC。

Flow Explorer 的查询状态包含 range/custom start-end、metric、1–4 个 ordered dimensions、typed filter、TopN/Other、target points、graph type 和 value layer，并编码进 URL。单维查询读 1m/1h aggregate，支持折线/堆叠/热力/表格；2–4 维查询由独立 joint compiler/runner 从同一 `flow_records FINAL` 事实生成真实有序 tuple，支持同样的时间序列和桑基，绝不把多次单维 TopN 在浏览器拼接。同步 joint v1 固定为 customer/count、UTC 分钟边界、最长 24h、TopN 1..100、最多 4 维和 5,000 万扫描行/4 GiB 扫描/4 GiB 内存/15 秒；`total` 只允许作为唯一 base 结果维度，用于“总流量 + 跨维过滤”，加入多维 tuple 时因无信息增益而拒绝；`address_set` 的重叠多归属必须由显式异步索引定义。长于 24h 或需要 address-set path 的查询必须命中未来的预配置异步联合索引，否则稳定拒绝，不能静默换成单维口径。joint 响应声明 `source=flow_records`，并因 base 尚无独立闭桶 coverage marker 而返回 partial warning，不能伪报端到端完整。其新增仅为查询代码，不新增状态或表，故本切片没有伪造一个空 migration；联合索引落地时必须用独立 forward-only migration。

生产入口默认展示同一 Flow query API 上的便捷分析层，而不是先要求用户编写表达式：国家→省→市按 `geo_dict.parent_id` 级联选择，查询谓词必须使用发布到 Flow snapshot 的稳定 `code`，管理库 ULID 只用于界面选中态；运营商选择只发送全局 `operator_id`，Gin 在 repeatable-read 快照内解析稳定 `flow_isp_id` 与覆盖时间窗的 migration 0031 publication pair，并在全部 active worker 存在 `installed_at` 后同时固定 snapshot/classification versions，禁止 ASN fallback 或把 ULID/code 冒充 `remote_isp_id`。流向视图在一次受鉴权、资源授权、并发和 timeout 约束的请求内执行 `dimension=total + directions=in/out` 两个查询，按最差 completeness 合并，只展示入/出两条真实总量序列；浏览器不得再分别发起请求后拼接。国家/省/市/运营商等跨维谓词仍将两个方向一起切换到最长 24 小时的 bounded base-fact 路径，不得为服务端分页退化成错误的 rollup 交集。协议、TOP 源/目的 IP、TOP 本地/远端网段直接选择公开 dimension。时间预设、自定义时间、metric、TopN、图形和筛选状态进入 URL；后台导出冻结同一份规范化请求和版本 provenance，去掉交互 VTable 当前页 projection 后通过平台 operation job 执行，并受相同数据集策略的范围、行数、超时和并发限制。原 1–4 维、typed filter、address set、device、Sankey 能力折叠为高级 Flow Explorer，仍遵守跨维最长 24h、权限、预算和异步联合索引前置条件。

Flow 查询参数可选携带 `table={search,sort_by,sort_direction,limit,offset,filters}`，由同一 provider 在已受 TopN/扫描预算限制的完整查询结果上生成统计投影。允许排序和筛选的固定字段只有 `dimension/last/average/p95/maximum/minimum/total/records/unknown/quality`；页大小 1..100、offset 最大 10,000、搜索和单值最大 256 字节、每列最多 100 个选择，总选择最多 1,000，未知字段/方向/重复筛选值 fail closed。provider 必须在分页前按 `path + dimension_snapshot_id + geo_version + classification_version` 分组，按 effective range 零填缺桶，并使用和图表一致的末桶覆盖秒数计算 bps/pps total、区间 average、exact-nearest-rank p95、last/min/max、记录数和质量比例；然后才执行全局搜索、AND-across-columns/OR-within-column 筛选、稳定排序和分页。`filter_options` 从完整 TopN 结果而非当前页生成，UI 所有十列均进入 VTable server mode；搜索、筛选、排序和页大小变化回到第一页，陈旧响应按请求序号丢弃。该投影无持久状态且不新增数据库对象，所以不伪造空 migration；未携带 `table` 的旧客户端响应保持不变，旧 hub 对新字段按 strict JSON 明确拒绝，发布必须保证 hub 先于新 UI。

过滤表达式由前端递归下降 parser 生成 AST，再经 authenticated `POST /api/v1/flow/filters/validate` 返回规范 AST；查询只接受该规范形态，使参数哈希、缓存键、审计和 URL 重放一致。逻辑节点固定为 `and/or/not/predicate`，最多 8 层、64 节点、单 predicate 2,048 值、总计 4,096 值；比较符固定为 `eq/ne/in/not_in/gt/gte/lt/lte`。field registry 包含 direction/category/business/target/device/exporter、src/dst/local/remote IP 或 CIDR、remote ASN/ISP、五级 Geo、local/remote prefix、local/remote port、IP protocol 和 observation interface。字符串、枚举、无符号整数、协议别名、IP/CIDR 各自规范化，AND/OR 子树和值排序去重；字段与操作符只能来自 registry，值只进入 ch-go typed parameter，绝不拼进 SQL。`GET /api/v1/flow/filters/catalog` 和 `POST /api/v1/flow/filters/complete` 提供同一 registry 的字段/操作符/枚举补全。普通用户的 target/device/exporter 仍必须走既有 resource selector 和逐资源授权；复杂 AST 中出现资源字段时 fail closed，管理员才可直接使用。

只引用 direction/category/business/target/device/exporter 的 AST 可直接命中 1m/1h rollup；引用 IP/CIDR/ASN/Geo/ISP/prefix/port/protocol/interface 的 AST，即使只选择一个结果维度，也必须路由到最长 24h 的 `flow_records FINAL` base-fact compiler，不能在独立聚合结果之间伪造交集。base compiler 支持 1–4 个有序结果维度；旧 `dimension_values` 只在单维时有确定含义，多维时稳定拒绝。ClickHouse 用 IPv6 列存 IPv4-mapped 地址，因此 IPv4 CIDR 参数在执行层转换为等价 `::ffff:0:0/96+prefix`，规范 AST 和界面仍保留用户熟悉的 IPv4 表达。更长时间窗和 address-set/常用跨维过滤由预配置异步联合索引承接；索引未发布时显式拒绝或标 degraded，不能静默改口径。保存/共享过滤器及其 RBAC 是下一项管理面 migration，不在无状态 validate/compile 路径中另建存储。

aggregate schema v1 只物化 `customer` view；`raw/supplier` 请求必须返回稳定的 `unsupported` 错误，不能把 customer 结果换个标签返回。FLOW-06 只有在 base/rollup 增加可验证的并行 provenance 或 versioned reclass generation 后才能开放另外两个 view。

每个 bucket 的读取先仅从 `_generation` marker 求最新 generation，再用 `(tenant_id,bucket,generation)` 回连公开维度；禁止对每个 dimension key 单独 `argMax`。TopN key 固定包含 `dimension_value + dimension_snapshot_id + geo_version + classification_version`，所以跨版本不会按名称或裸 ID 静默合并。`other` 也按版本分别返回；结果行硬限处理版本爆炸。每次查询固定追加一条 `is_metadata=1` 的内部 sentinel，`covered_buckets=count(latest generation marker)`；provider 删除该行并与请求期望桶数比较。即使没有任何公开维度行，也能区分“完整的零结果”和“rollup 未完成”，不得由 HTTP 层猜测。compiler 的 `RequestError` 固定提供 `field + code(required/invalid/unsupported/limit_exceeded/incomplete_range) + message`，API 只负责映射统一错误 envelope。

每个维度结果至少返回 `id/name/kind/parent_id/path/additive/completeness/dimension_version/geo_version`。compiler 固定返回每行 `received_records/unknown_sampling_records/quality_records/generated_at`，API 由此计算 sampling completeness 和质量提示；未知不能当 0。Geo 同层 `additive=true`，缺失归 `_unassigned`；address set 固定 `additive=false`。时间范围跨多个名称或树结构版本时，响应必须按版本拆分或返回 `mixed_versions` 警告，不能把同名当同 ID，也不能把不同 ID 静默合并。

provider result runner 必须消费 CH 的多个 data block，校验所有列等长、结果行硬限、bucket 范围/对齐、非负有限值、版本身份、quality counters 和点唯一键。metadata sentinel 必须恰好一条且除 `covered_buckets` 外为固定零值；缺失、重复、畸形或覆盖数大于期望桶数都使整个查询失败，不能返回部分结果。对每个公开点返回 `sampling_completeness=(received-unknown_sampling)/received` 和 `quality_record_ratio=quality/received`；分母为零时分别标记 `Known=false`，不返回虚假的 100%。结果总体返回 `expected_buckets/covered_buckets/ratio/complete` 以及 `mixed_versions/version_count`；CH 在任一 block 后失败时丢弃已累积点，不泄漏部分成功。

ch-go native 查询有三条必须由真实 ClickHouse 门禁锁定、不能只靠 SQL 字符串单测的协议约束：`proto.Parameter.Value` 是 custom setting 的 ClickHouse Field dump，数字也必须按 `'1'` 形式编码，再由 `{name:UInt*}` 占位符做类型转换；带别名读取 `ReplacingMergeTree` 时固定写成 `FROM table AS source FINAL`；TopN/other 聚合的输出别名不得与输入列 `dimension_value` 同名，必须先使用 `grouped_dimension_value`，仅在最外层投影恢复 API 列名。否则会分别触发 native parameter restore、`FINAL` 语法或 alias substitution 聚合错误。外部集成测试使用独立临时数据库，按 001→005 顺序执行真实 DDL，经 native writer 写入 facts，并覆盖 1m/1h、同 generation 重放、下一 generation repair、空桶 sentinel、强制多 result block 以及首 block 后取消且零部分结果；不得复用或清空开发数据库来完成测试。

版本与预算的真实 CH 门禁另构造三个 rank 完全相同的 Geo 序列，其中同一 city ID 分属两个 snapshot/Geo/classification 版本。`TopN=2 + other` 必须按固定 tuple tie-break 选择两个版本化 city 行，第三行按自身版本进入 `_other`，最终返回 `mixed_versions=true/version_count=2` 且 bytes/records 守恒。将 `max_rows_to_read` 或 `max_result_rows` 压到 1 时，ClickHouse 必须抛错且 provider 返回零部分点。静默传输超时、服务端长查询 deadline 与集群容量仍由 12.2/外部故障门禁完成，不能用客户端已过期 context 冒充。

查询并发状态只能存在于单次 `Run` 栈内：共享 runner 只持有并发安全的 executor，每次调用必须新建 result columns、去重集合、version 集合和 metadata counter，不得缓存或复用 decoder。单节点门禁在同一个 4-connection production pool 上交错提交 64 个 `geo.city TopN+other` 与 `total` 请求，逐请求核对点、records、完整桶和总量；race 与多轮结果隔离通过只证明并发正确性，不代表固定硬件 QPS、N+1 或集群容量。

查询能力也必须由同一编译器 registry 导出，而不是让 hub 或前端复制白名单：aggregate v1 只发布 customer view 及现有 metric/dimension registry；detail 按 raw/supplier/customer 分别发布允许字段、默认字段和 filters。能力返回值是稳定排序的副本，调用方修改不得污染进程内 registry；未知 view 必须明确拒绝。编译器的接受/拒绝测试遍历同一 capability，确保能力声明与实际 SQL 校验不会漂移。

```json
{
  "dimension": "geo.city",
  "scope": {"id":"330000","name":"浙江省","path":["亚洲","东亚","中国","浙江省"]},
  "series": [{"id":"330100","name":"杭州市","parent_id":"330000","bytes":123456,"additive":true}],
  "unassigned_bytes": 0,
  "completeness": 1.0,
  "geo_version": "2026-09-05.1"
}
```

“查浙江省”直接查 `dimension_kind=geo.province AND dimension_value=330000`；“看浙江下属城市”查 `dimension_kind=geo.city`，API 用同版本字典限制为该省的直属/后代 city IDs。父级总量使用已经直接生成的 province rollup，不依赖临时相加城市；只有产品需要核验覆盖率时才比较“父级直接值 = 子级值之和 + `_unassigned`”。

### 9.2 六类趋势

```sql
SELECT bucket, category, business_direction,
       sum(estimated_bytes) * 8 / 60 AS bps
FROM watchdog_flow.flow_aggregate_1m FINAL
INNER JOIN (
  SELECT tenant_id, bucket, max(generation) AS generation
  FROM watchdog_flow.flow_aggregate_1m FINAL
  WHERE tenant_id = {tenant:String}
    AND bucket >= {from:DateTime} AND bucket < {to:DateTime}
    AND dimension_kind = '_generation'
  GROUP BY tenant_id, bucket
) AS latest USING (tenant_id, bucket, generation)
WHERE tenant_id = {tenant:String}
  AND bucket >= {from:DateTime} AND bucket < {to:DateTime}
  AND dimension_kind = 'category'
GROUP BY bucket, category, business_direction
ORDER BY bucket, category, business_direction;
```

### 9.3 ASN/地址段/地域 TopN

```sql
SELECT dimension_value, sum(estimated_bytes) AS bytes
FROM watchdog_flow.flow_aggregate_1m FINAL
INNER JOIN (
  SELECT tenant_id, bucket, max(generation) AS generation
  FROM watchdog_flow.flow_aggregate_1m FINAL
  WHERE tenant_id = {tenant:String}
    AND bucket >= {from:DateTime} AND bucket < {to:DateTime}
    AND dimension_kind = '_generation'
  GROUP BY tenant_id, bucket
) AS latest USING (tenant_id, bucket, generation)
WHERE tenant_id = {tenant:String}
  AND bucket >= {from:DateTime} AND bucket < {to:DateTime}
  AND dimension_kind = {dimension:String}
  AND business_direction = {direction:String}
GROUP BY dimension_value
ORDER BY bytes DESC, dimension_value ASC
LIMIT {limit:UInt16};
```

### 9.4 源/目的 IP 明细

IP 明细是 base fact 搜索，不复用 rollup，也不把 ASN 当作地址段身份。v2 请求契约固定为：

- tenant 只从 authenticated scope 注入；`ip` 接受 IPv4/IPv6（拒绝 zone），内部规范化并与 IPv4-mapped IPv6 存储比较；`endpoint` 只能是 `source/destination/either`；
- `view=customer` 读取已入库的客户视图并固定过滤 `disposition='count'`；`view=raw` 读取协议 tuple，不应用 customer disposition，因此被客户规则标记 drop 的事实仍可由具备 raw 权限的用户审计；`view=supplier` 读取 migration 005 的 `supplier_*` 基线列并使用 supplier category，同样遵守 count disposition；
- `from/to` 是 UTC 左闭右开、毫秒精度，单次最多 24 小时且 `to` 不得在未来；`limit` 为 1..500，CH 实际读取 `limit+1`；
- 默认排序为 `(event_time DESC, source_stream_id DESC, kafka_partition DESC, kafka_offset DESC, record_index DESC)`。任意展示字段排序使用 cursor v3，payload 绑定 `field/direction/typed field value/event_time/source coordinate`；主字段相等时统一以同方向的 event time 与自然坐标形成严格全序。next cursor 只取已返回页最后一行；
- cursor 版本、canonical base64/JSON、字段类型、source stream、时间范围和排序身份均严格检查。旧 record-id cursor 明确过期并拒绝，不能静默从头翻页。String/UInt64/Bool/DateTime64 值分别按 canonical wire value 解析；客户端改变 IP、endpoint、时间、filter、sort 或 page size 必须清空 cursor 链；
- 可选字段只能来自 provider 固定 registry；`event_time/source_coordinate` 永远返回，内部还读取 src/dst IP 复核结果。raw 只开放协议/采样/质量/target/device/exporter/observation 字段，不开放 business/category/local/remote/Geo/ISP/customer snapshot 等派生字段。supplier 开放 raw 字段、direction/local/remote、supplier Geo/ASN/ISP/category 及其 dimension/classification provenance；不开放 customer business、prefix 或 override 值。地址集合数组不进入 field mask：重叠集合的并/交/差必须走 base membership 谓词和去重聚合，不能由明细数组在 UI 侧相加；
- customer filters 开放 `directions/categories/businesses/target_ids/device_ids/exporter_ids`；supplier 的 category 映射 `supplier_category`，但拒绝 customer business；raw 只开放后三种资源过滤器，若携带方向/category/business 立即稳定拒绝，而不是暗中按 customer 字段筛 raw。VTable 另用 `column_filters=[{field,values}]` 表达固定字段 registry 上的精确 OR、跨字段 AND；String/IP/UInt64/Bool/DateTime64 分别规范化并参数化，IP 用原生 IPv6 列与 `toIPv6` 比较，禁止以显示字符串比较 IPv4-mapped 地址。单字段最多 100 个、总值最多 256 个、字段最多 32 个；重复字段、与旧 typed filter 重复约束及空 values fail closed；
- supplier 查询在 cursor 之前对完整过滤范围计算 `min(fact_schema) OVER ()`；每个数据行携带相同证据。若 cursor 排除了全部数据，查询仍用全范围第一行返回一个 `_scope_match=false` 的 metadata-only 行，runner 校验后丢弃。任何命中事实的 minimum 小于 2、跨 block 不一致或 metadata 列缺失，都返回 `ErrSupplierProvenanceUnavailable`/malformed 且整页清空；空范围合法返回 complete=true、minimum=0。这样 limit、旧 customer/raw cursor 或伪造 cursor 都不能绕过迁移完整性；
- 查询使用 `flow_records FINAL` 收敛 at-least-once 物理重复，并设置 10 秒、`limit+1` 结果行、500 万扫描行、1 GiB 扫描字节硬限，所有 overflow mode 为 `throw`。超限/超时返回错误，不返回静默截断页。

```sql
SELECT event_time,
       CAST(source_stream_id AS String) AS source_stream_id,
       kafka_partition, kafka_offset, record_index,
       toString(src_ip) AS _source_ip,
       toString(dst_ip) AS _destination_ip,
       /* fixed field-registry expressions */
FROM flow_records FINAL
WHERE tenant_id = {tenant:String}
  AND event_time >= {from:DateTime64(3,'UTC')} AND event_time < {to:DateTime64(3,'UTC')}
  /* customer only: AND disposition = 'count' */
  AND (src_ip = toIPv6({ip:String}) OR dst_ip = toIPv6({ip:String}))
  AND (event_time, source_stream_id, kafka_partition, kafka_offset, record_index)
      < ({cursor_time:DateTime64(3,'UTC')}, {cursor_source_stream_id:String},
         {cursor_kafka_partition:UInt32}, {cursor_kafka_offset:UInt64},
         {cursor_record_index:UInt32})
ORDER BY event_time DESC, source_stream_id DESC, kafka_partition DESC,
         kafka_offset DESC, record_index DESC
LIMIT {fetch_limit:UInt16}; -- requested limit + 1
```

provider 使用与 field mask 对应的 typed ch-go columns 消费任意多个 data block，校验列等长、总行数不超过 `limit+1`、时间范围/毫秒精度、source coordinate 合法、IP 端点命中、游标边界以及跨 block 全局严格排序/唯一。任一结果畸形、取消或 CH 执行失败都丢弃累积行，响应全有或全无；只有真实读到额外一行才返回 `has_more=true + next_cursor`。

生产 HTTP 适配器固定为 `POST /api/v1/flow/records/search`，以 `POST /api/v1/flow/records/facets` 提供同范围的懒加载列值/计数，并以 `GET /api/v1/flow/records/capabilities` 暴露同一字段/视图 registry。适配器不得接受 tenant 字段；它从登录上下文注入 tenant，把 raw/supplier/customer 映射到平台 `view_raw/view_supplier/view_customer` grant，并用聚合查询相同的 target/device/exporter 资源授权器（包括 column filter 中的资源字段）。search 响应为 `{data: DetailResult, meta: {sort, page_size}}`，meta 返回实际主键和稳定 tie-break；客户端不得把当前页重排后冒充服务端排序。facet 限 1..100 个值、搜索限 128 字节、沿用 24h/500 万行/1 GiB/10 秒扫描预算，排除当前字段自身条件但保留其他条件；customer/raw 可用，supplier 在能证明全范围 provenance 前明确拒绝，不得把不完整基线聚合成候选值。成功查询都记录低基数敏感访问审计，不写 IP、cursor、搜索词或完整过滤值。

源 IP、目的 IP 页的明细 VTable 只消费上述 API：点击 Top IP 或输入精确 IPv4/IPv6 后发起查询；换页沿服务端 opaque cursor 链前进/后退，修改页大小、时间、端点、column filter 或 sort 必须清空 cursor 链并回到第一页；新请求必须取消旧请求，旧响应不得覆盖新状态。13 个展示列全部声明稳定服务端排序和懒加载 facet；打开列头筛选时按相同 scope/其他列条件查询，输入搜索 200ms 防抖并取消旧 facet 请求，禁止用当前页值冒充全集。全不选表示清除；远程候选的“全选当前结果”不是全局全集，只有未修改的 unconstrained 状态可规范化为无约束。

通用 VTable 的筛选浮层固定 portal 到 `document.body`，使用 viewport fixed 坐标；打开时做上下/左右 collision clamp，宽高受 viewport 限制、值列表内部滚动，页面/表格外部滚动、resize、Escape 或外部点击均关闭。server contract 同时支持静态候选和 `field+search+AbortSignal` 懒加载；关闭、换列或继续输入必须取消旧请求，旧响应不能覆盖新候选。这个 UI 基础能力不等于所有列表已经具备服务端筛选：每个调用点只有在其 list API 提供 typed filter、稳定 server sort 和 total/cursor pagination 后才能切到 server mode。

ClickHouse 会在同一 SELECT 内做全局 alias substitution，因此 field mask 中的 `src_ip AS src_ip` 不能反向改变 WHERE 中的 IPv6 列类型。detail registry 只保存固定物理列名和结果类型，compiler 统一生成 `source.<column>` 及必要的 `CAST(... AS String)/toUInt64` 转换；`flow_records` 固定写成 `AS source FINAL`，tenant、时间、disposition、endpoint、filter、supplier evidence 和 cursor 全部显式引用 `source.*`。新增任意字段都必须通过同一路径，禁止重新放入任意 SQL expression。真实 CH 门禁必须同时验证 IPv4-mapped 规范化、相同毫秒 record ID 降序翻页、较新 ingest generation 覆盖旧物理行、首 block 取消和 deadline/扫描预算错误时零部分结果。

所有 detail 动态字符串字段都显式 `CAST(source.<column> AS String)`，因为 `toString(LowCardinality(String))` 仍可能在 native wire 上保持 LowCardinality；supplier 内部 `_scope_match` 也必须 `CAST(... AS Bool)`，不能把比较表达式的 `UInt8` 交给 `ColBool`。真实 provenance 门禁包含人工写入的 `fact_schema=1` 和当前 writer 的 schema 2：raw 必须同时显示 count/drop 和两代事实，customer 必须排除 drop，supplier 在完整窗口含 schema 1 时返回 `ErrSupplierProvenanceUnavailable` 且零行；schema 2 子窗口、cursor 续页和合法空窗口分别返回完整证据。权限判定仍由宿主 RBAC 完成，不由数据测试冒充。

### 9.5 重叠地址集合统计

地址集合的单组 rollup 只能回答“每个组各有多少”，不能把 A、B 两组的结果相加来回答 `A∪B`，因为同一 fact 可同时属于 A/B。任意并/交/差必须读取 base fact 的事件时间 membership 数组并在聚合前执行一次布尔谓词；查询没有 `ARRAY JOIN`，命中的一行 fact 无论含多少选中组都只进入一次 `sum/count`。

请求复用固定 metric registry、`customer/count`、closed 1m/1h bucket 及通用 directions/categories/businesses/target/device/exporter filters，并增加：

- `endpoint=local/remote/either` 分别选择 `local_address_set_ids`、`remote_address_set_ids` 或二者 `arrayDistinct(arrayConcat(...))`；这是业务方向归一后的端点，不等同于原始 source/destination；
- `address_set_filter.include_any/include_all/exclude_any` 先由 `flowdimension.CompileAddressSetFilter` 统一验证、排序去重，三组原始 ID 合计最多 256；必须至少有一个正选择器（include_any 或 include_all），拒绝把 exclude-only/全空请求解释成无边界全集；
- 同步 base 扫描最长 1 小时，继续使用 500 万行、1 GiB、10 秒硬限。超过窗口返回稳定 `limit_exceeded` 并提示转平台 `operation_jobs`；query provider 不创建第二套 job 状态机；
- 输出按 `bucket + dimension_snapshot_id + geo_version + classification_version` 分组，跨版本不合并，返回 `mixed_versions/version_count`。每个点包含 value、received/unknown-sampling/quality records 及已知标志；空结果是合法零点结果，不伪造 rollup completeness；
- typed runner 校验多 block 列长度、硬行限、bucket 范围/对齐、版本身份、非负有限值、质量计数和全局稳定顺序；CH 失败或任何畸形均返回零结果。

```sql
SELECT toDateTime(toStartOfMinute(event_time), 'UTC') AS bucket,
       CAST(dimension_snapshot_id AS String) AS dimension_snapshot_id,
       CAST(geo_version AS String) AS geo_version,
       classification_version,
       sum(estimated_bytes) * 8 / 60 AS value,
       count() AS received_records,
       countIf(NOT estimated_valid) AS unknown_sampling_records,
       countIf(quality_flags != 0) AS quality_records
FROM flow_records FINAL
WHERE tenant_id = {tenant:String}
  AND event_time >= {from:DateTime('UTC')} AND event_time < {to:DateTime('UTC')}
  AND disposition = 'count'
  AND hasAny(arrayDistinct(arrayConcat(local_address_set_ids, remote_address_set_ids)),
             [{include_any_0:String}, {include_any_1:String}])
  AND hasAll(arrayDistinct(arrayConcat(local_address_set_ids, remote_address_set_ids)),
             [{include_all_0:String}])
  AND NOT hasAny(arrayDistinct(arrayConcat(local_address_set_ids, remote_address_set_ids)),
                 [{exclude_any_0:String}])
GROUP BY bucket, dimension_snapshot_id, geo_version, classification_version
ORDER BY bucket, dimension_snapshot_id, geo_version, classification_version;
```

CH native result contract 不接受把 `LowCardinality(String)` 隐式当作 `String`；`dimension_snapshot_id/geo_version` 必须在查询投影中显式 `CAST(... AS String)`，与 runner 的 `ColStr` 一致。真实集成数据必须至少包含同时属于 A/B、只属于 A、均不属于三类 fact，以及同 record ID 的较新 generation；分别核对 A∪B、A∩B、A−B，证明重叠 membership 不展开、不重复计数，raw/estimated counter、unknown sampling 和 quality record 同源守恒，并验证 deadline/扫描预算拒绝时不返回部分点。

生产查询接线复用 `POST /api/v1/flow/query` 的认证、tenant/value-layer、并发和审计边界。地址集合组合请求固定 `dimension=address_set`、`top_n=1`、`include_other=false`，增加 `address_set_endpoint` 与上述 `address_set_filter`；当前同步路径只接受闭合且 UTC 分钟对齐的最长 1 小时窗口，并直接读取 `flow_records FINAL`。组合查询不能同时携带 joint dimensions、direction split、typed filter 或 aggregate dimension/version filter；超窗或不兼容组合稳定拒绝，等待异步联合索引，不能静默退回逐组 rollup 相加。响应回显 canonical filter/endpoint，`dimension.additive=false`、`plan.source=flow_records`，并把 base 去重准确性与尚未独立证明的端到端 ingest completeness 分开报告；空结果是成功的零点集合，不伪造 rollup coverage。

Geo 选择目录为 `GET /api/v1/flow/geo/catalog?level=<country|province|city>&parent=<stable-code>&version=<immutable-version>&limit=<1..5000>`。一次响应只属于一个 version 和一个精确 level，parent 只取直属子级，返回稳定 `id/name/parent_id/path/additive`；未知版本/parent、重复或未知参数、超量结果全部 fail closed。Flow query 的 `dimension_labels` 用 `geo_version:dimension_value` 作键，携带同一结构与 version；事实 `dimension_value` 仍保持稳定 ID。便捷页从 active 目录选择 Geo 时必须同时提交该 `geo_version` 过滤条件，不能让 active code 无提示地命中其它 publication 的历史事实。

95th 必须先生成等长 bucket 的 bps，再使用 `quantileExact(0.95)`；平均是 bucket 平均，不是不同 bucket 宽度混算。当前值必须标明最新完整 bucket，不能使用未关闭 bucket 冒充完整数据。

### 9.6 境外 KPI 与 country/region 查询

境外专题只读 1m/1h aggregate 的最新 generation，不同步扫描 `flow_records`，也不增加第二张境外事实表。`category=overseas` 是数据面按事件时间 classification snapshot 生成的唯一境外权威；查询层不得再次用国家名称、ISO 字符串或当前港澳台设置重判历史。`geo_level` 首发只接受 `country/region`，分别固定读取 `geo.country/geo.region` rollup；Geo 名称和 breadcrumb 由结果行自己的 `geo_version` 在共享 `flowdimension.GeoCatalog` 中解析，不能跨版本按名称合并。

境外页将通用 Explorer 与专题 aggregate 明确分层：专题区展示入/出 KPI 与趋势、观测境外 IP/本地主机、country/region TopN、采样未知比例、闭桶覆盖率和版本混用告警；查询跨度不超过 7 天时使用闭合 UTC `1m` 桶，更长跨度使用闭合 UTC `1h` 桶。专题 schema v1 只能下推 direction/business/resource，因此设备筛选可直接生效，而国家/省/市/运营商的交叉条件必须交给下方 bounded Explorer；界面必须提示这一口径差异，不得把分别查询的边际分布拼成联合结果。Geo 展示键为 `geo_version:geo_value`，后端在同版本发布目录解析名称和 breadcrumb，解析失败时显示稳定 ID，绝不回退到当前 active version。

同一个 latest-generation 查询返回两类行：

- `kind=kpi`：对 `category=overseas` 的流量返回 `in/out/combined × ipv4/ipv6/unknown/all`。业务方向决定远端和本地端：入向的远端/本地分别是 `src_ip/dst_ip`，出向分别是 `dst_ip/src_ip`；复用现有 endpoint rollup，不新增冗余的 local/remote IP 高基数维度。查询同时聚合远端和本地两条镜像计数并校验 metric、received、unknown-sampling、quality 四组总数完全一致，不一致时 provider 整体失败，禁止只显示其中一侧。
- `kind=geo`：已分配稳定 Geo ID 且 `category=overseas` 的行进入 `geo_scope=overseas` TopN；所选 country/region 层为 `_unassigned` 的所有入/出向流量进入独立 `geo_scope=unknown_geo` 行，先判 unknown 再判 overseas。unknown 不进入境外 TopN/other，也不能因为地址看起来像公网地址而补猜境外。`other` 只合并当前版本内非 TopN 的已知境外 ID。

IPv4 在 aggregate endpoint 中仍是 `::ffff:a.b.c.d` 的 IPv4-mapped IPv6；family 只在这一规范表示上判定，并显式保留 `::`/异常为 `unknown`，绝不把 sentinel 计作 IPv6。每个 family 和 direction 组合还生成服务端精确的 `all/combined` 行：流量可加，唯一 IP 数必须由 `uniqExact` 对原始 endpoint ID 集合计算，客户端不得把入/出向去重数相加。

KPI 字段命名为 `observed_remote_ips/observed_local_hosts`：它们是在已接收 Flow 事实中的精确去重数量，不应用 sampling rate，也不声称等于未采样全网的真实唯一主机数。bytes/packets 按既有 raw/estimated metric 口径计算；每行同时返回 `received_records/unknown_sampling_records/quality_records` 和已知标志，页面必须把 observed 与 sampling completeness 一起展示。

请求继续只允许 `customer` view、闭桶 UTC 范围、固定 metric registry、TopN 1..100，以及 `in/out/business/target/device/exporter` typed filters；tenant 仅来自 authenticated scope。结果预算按每桶最多 `12 + 3 × (TopN + other + unknown)` 行预拒绝，CH 设置 15 秒、25 万结果、5,000 万扫描行、4 GiB 扫描字节和 4 GiB 查询内存硬限，且固定 `join_use_nulls=0` 让缺失的 endpoint 镜像产生 `endpoint_consistent=0`，而不是受集群默认值影响。结果保留 `dimension_snapshot_id/geo_version/classification_version`，返回 mixed-version 和 generation coverage；唯一 metadata sentinel、typed multi-block 校验及任一错误全有或全无的规则与 9.1 相同。真实 CH 对 endpoint 镜像、IPv4-mapped 输出、repair generation 和 TopN/unknown 守恒的执行证据是独立集成门禁，本地 SQL contract test 不能替代。

真实 CH 集成门禁使用独立数据库顺序执行 001..005，并写入同一闭合 1m 桶的入向 IPv4、出向 IPv4、出向 IPv6 和双端未知地址事实：generation 1 同时校验 `in/out/combined × ipv4/ipv6/unknown/all`、远端/本地镜像计数、country/region `TopN=1 + other + unknown_geo` 与总量守恒；随后写入迟到 IPv6 事实并以 generation 2 重建，查询只能看到新 generation，TopN 排序和 other 必须随之收敛。测试库退出时清理。该门禁验证的是 Flow 数据面与查询契约，不替代 hub API、tenant/RBAC 和 UI 验收。

### 9.7 VPN findings 异步导出

VPN finding 是 MySQL 中的低容量管理/处置对象，不经 ClickHouse aggregate compiler。`POST /api/v1/flow/vpn/findings/exports` 只创建任务：请求冻结 `from/to`、全文搜索、typed column filters、`sort_by/sort_direction`、行数上限和格式，tenant/actor 只取认证上下文；创建同时要求 `vpn_view + vpn_export`，执行、下载和重试继续复核当前 `vpn_export`。时间窗最多 90 天，默认/硬上限 250,000 行。

执行复用已有 `export_tasks + operation_jobs`。provider 对冻结条件执行一条 tenant-scoped MySQL 查询并读取 `limit+1`；多出一行就整体失败，禁止产生无提示截断文件。CSV 与 Parquet 共用独立的 finding export row schema；不得复用 Flow aggregate schema，也不得新增导出状态机、消息队列或表。产物保留窗口、两端 IP/端口、双向计数、ASN/国家/prefix、score/risk/verdict、规则和事实版本、人工 disposition、probe status 以及 `evidence_present/probe_result_present`，但明确排除 `tenant_id`、`conversation_key`、完整 `evidence/probe_result`、`disposition_note/disposition_by`。CSV 文本字段继续做 spreadsheet formula 防护。

该表格型 dataset 的 query step 固定为 0；通用 export normalizer 不允许把它改成 5 分钟。`avg_5m` 仅作为既有非空 aggregation 存储字段的兼容占位，不参与查询或制品计算。旧 `contract_version=0` VictoriaMetrics 导出仍走显式 fallback；QueryGateway 未启用不阻断 VPN findings 从 MySQL 导出。

### 9.8 raw/supplier Flow 明细异步导出

`flow.records` 是独立的明细导出 dataset，不复用 `flow.traffic` 聚合行结构。创建请求只接受 `raw` 或 `supplier` detail query，并冻结精确 IP、source/destination/either 端点、最长 24 小时时间窗、compiler-resolved fields、typed/column filters、稳定 sort、每页 500 行以及最多 250,000 行的任务总上限；客户端 cursor 必须为空。创建、执行、重试和下载 raw 任务同时要求 `view_raw + export_raw`，supplier 同时要求 `view_supplier + export_supplier`。target/device/exporter selector 仍走统一资源授权，tenant 只取认证上下文。

worker 使用已冻结的 detail schema 和 Kafka 坐标 cursor 逐页读取，不构造 offset page，也不走 aggregate QueryGateway row schema。每页必须返回相同 view/fields；`has_more=true` 必须携带前进的新 cursor；达到总上限仍有下一页、字段缺失、类型漂移、取消或 supplier 的全窗 `fact_schema >= 2` provenance 证据不完整，均整体失败且不产生截断制品。结果在内存中压成字段顺序一致的 value slice，避免为每个累计行保留 map；operation job context 在每页前检查取消。该上限是有界完整导出，不是无限 dump。

CSV/Parquet 都输出 `event_time + source_stream_id/kafka_partition/kafka_offset/record_index + 冻结字段`。动态 Parquet schema 保留 string/uint64/bool/timestamp 类型；CSV 对所有字符串继续做 spreadsheet formula 防护。任务、lease、heartbeat、retry/cancel、产物 checksum/size/row_count、下载鉴权、保留期和销毁完全复用平台 `export_tasks + operation_jobs`，没有新 migration、MQ 或状态机。源/目的 IP 页面默认仍为 customer 明细，但可切换 raw/supplier，并从同一筛选/排序状态创建后台 CSV 导出。

## 10. API

所有路径在 `/api/v1` 下，统一 tenant/RBAC、cursor/page、sort、filter、field mask、ETag、idempotency、audit 和错误 envelope。

| Method/path | 权限 | 语义 |
|---|---|---|
| `GET/POST /flow/exporters` | view/configure | 列表/创建；source identity 唯一 |
| `GET/PATCH/DELETE /flow/exporters/{id}` | view/configure | 查看、乐观锁修改、退役删除 |
| `POST /flow/exporters/{id}/actions/activate` | configure | 验证 plan ACK 和测试流后激活 |
| `POST /flow/exporters/actions/discover` | configure | 创建候选，不自动激活 |
| `GET/PATCH /flow/settings` | view/configure | 首页、本网和口径设置 |
| `POST /flow/dimensions/actions/validate` | configure | 校验 bundle/snapshot，不发布 |
| `POST /flow/dimensions/actions/publish` | approve | 原子发布新 snapshot |
| `GET /flow/dimensions/lookup` | view | 按 IP + event time 返回唯一 prefix、Geo 完整 path、全部 address sets 及命中依据 |
| `POST /flow/dimensions/actions/evaluate` | configure | 预览组并/交/差/有限补集、规范化 CIDR、重叠、依赖 DAG 和最坏展开量；不写入 |
| `GET /flow/geo/catalog` | view | 按 immutable version + exact level + parent 返回稳定 ID、直属节点和完整 path；拒绝跨版本/重复参数 |
| `POST /flow/query` | view | 统一趋势/TopN/统计查询；地址集合组合从 base 去重、同步窗口最多 1h |
| `GET /flow/reports/capabilities` | view | 返回固定运营报表 kind、筛选、统计、panel 与导出能力；来自服务端同一 registry |
| `POST /flow/reports/query` | view；VPN 同时要求 vpn_view | 一次冻结并组合 overview/dimensions/endpoints/overseas/vpn 固定报表；响应为 kind-specific typed union |
| `POST /flow/overseas/query` | view | 境外 KPI、country/region TopN 与采样完整性；只读 aggregate |
| `POST /flow/records/search` | sensitive_view | 分页源/目的明细 |
| `GET/POST /flow/vpn/rules` | vpn_view/configure_adjustment | tenant-scoped 服务端列表/创建 draft |
| `GET/PATCH/DELETE /flow/vpn/rules/{id}` | vpn_view/configure_adjustment | 查看、If-Match 修改/软删 draft |
| `POST /flow/vpn/rules/{id}/actions/preview` | configure | 候选量、成本、误报预览 |
| `GET /flow/vpn/findings` | vpn_view | 分页/筛选/搜索 |
| `GET /flow/vpn/findings/facets` | vpn_view | 指定展示列的有界远程候选；继承时间/搜索/其它列筛选并排除本列自身条件 |
| `GET /flow/vpn/findings/{id}` | vpn_view | 完整证据链 |
| `POST /flow/vpn/findings/{id}/actions/disposition` | vpn_triage | 人工处置，要求 If-Match |
| `POST /flow/vpn/findings/exports` | vpn_view + vpn_export | 冻结当前服务端筛选/排序，创建有界脱敏 CSV/Parquet 异步导出 |
| `POST /flow/vpn/findings/{id}/actions/probe` | probe | 创建受控异步 job |
| `POST /flow/reclass-jobs` | configure | 复用 operation job 创建回算 |
| `GET /flow/reclass-jobs/{id}` | view | 进度、范围、版本、校验 |
| `POST /flow/records/exports` | view_raw + export_raw 或 view_supplier + export_supplier | 冻结 detail schema/filter/sort，按 Kafka 坐标 cursor 创建有界完整 CSV/Parquet 异步导出 |
| `GET /flow-workers/{worker_id}/trust-bundle` | flow_worker machine credential | 拉取与 collector plan 共用的单调 Ed25519 trust bundle；collector 凭据拒绝 |
| `GET /flow-workers/{worker_id}/enrichment-publications` | flow_worker machine credential | 按 `after_version` 升序分页拉取 signed dimension+classification envelope；tenant 只取机器 identity |
| `GET /flow-workers/{worker_id}/enrichment-publications/{publication_id}/objects/{kind}` | flow_worker machine credential | 由服务端按 publication 与 `dimension/classification` 解析 immutable object；有界 Range 下载，禁止客户端 object ref |
| `POST /flow-workers/{worker_id}/enrichment-publications/{publication_id}/ack` | flow_worker machine credential | 上报 downloaded/installed/failed；pair metadata 必须精确一致，里程碑不因后续失败回退 |
| `POST /flow/exports` | customer query + customer export | 创建策略有界的异步 CSV/Parquet 完整查询导出；冻结 query/policy/auth 快照 |
| `GET /exports/{id}` | export task owner/admin | 复用平台导出状态、取消、失败原因和过期时间 |
| `GET /exports/{id}/download` | export task owner/admin | 下载非空且校验通过的制品；过期后拒绝 |
| `GET /flow/health` | operate | 分层 readiness/lag/completeness |

输入错误 400，未认证 401，权限/范围 403，不存在 404，ETag/幂等冲突 409/412，限流 429，依赖不可用 503。响应必须区分 `complete/partial/unavailable`，包含 data watermark、版本和警告；依赖故障不能返回成功的空数组。

## 11. 固定运营报表与六个界面

Flow 有两个并列而非互相替代的使用面：**固定运营报表**用于值班、汇报和导出，选择时间与少量业务条件即可得到稳定的一组 KPI、趋势、排名和表格；**Advanced Flow Explorer**用于临时选择 1–4 个维度、typed filter 和折线/堆叠/热力/桑基/数据表。固定报表不能只是给 Explorer 改默认参数，也不能复制一套 ClickHouse 查询实现；二者复用同一个 compiler/runner、QueryGateway、版本目录、资源授权和 export job。

页面稳定入口分别为 `/flow`、`/flow/dimensions`、`/flow/source`、`/flow/destination`、`/flow/overseas`、`/flow/vpn`；`/traffic-matrix` 只作为总览的发布窗口兼容别名。已有六个 route/preset 只证明入口存在，不代表下列固定报表已经交付。

### 11.1 固定报表的一致性与统计契约

`POST /api/v1/flow/reports/query` 在顶层接收公共查询字段，并用 `report={schema_version,kind,...}` 作为 discriminated union；`report.kind=overview|dimensions|endpoints|overseas|vpn`，其中 `endpoints` 还必须指定 `report.side=source|destination`。公共请求只包含 `from/to/timezone`、固定 metric、显示模式、峰段、TopN/目标点、业务/Geo/运营商以及已经过资源授权的 target/device/exporter selector。tenant、角色、可见资源和敏感 value layer 只能来自认证上下文。kind-specific 字段对其它 kind 必须拒绝，禁止“忽略未知字段后猜默认”。`report.tables` 只承载固定报表 VTable 的服务端 search/sort/filter/page 投影；导出准备阶段必须去掉该交互页投影并冻结 panel selector，保证导出是同条件全量而不是当前页。

报表服务在一次 QueryGateway admission 内冻结并回传：requested/effective range、source/display resolution、最新闭合桶、AddressSnap/classification/Geo 版本集合、数据 watermark、资源授权快照、查询计划和 completeness。一个报表的多个 panel 可由服务端执行多个 typed 子查询，但必须共享上述冻结上下文；前端不得分别发起若干普通 `/flow/query` 后把不同时刻、不同 publication 或不同水位的响应拼成一张报表。必需 panel 任一失败则整体失败且零部分数据；设计为可选的 VPN/probe 等 panel 可返回显式 `unavailable{code,reason}`，不能返回成功的空数组。

统计口径固定如下：

- `in/out` 始终站在已配置本网地址集合一侧观察，分别表示流入本网/流出本网，不使用设备 ingress/egress 字段直接冒充业务方向；
- “当前”是最新**完整** source bucket；平均值是等宽展示桶平均；95th 是等宽展示桶 bps/pps 的 `quantileExact(0.95)`；峰值是同一桶序列最大值，禁止对未闭桶或不同宽度桶混算；
- `display_mode=value` 返回原值，`share` 返回当前 series 占同 bucket、同方向、同 metric 权威总量的比例，`difference` 返回带符号的 `in - out`；零分母返回 unknown 而不是 0% 或 100%；
- 六类业务分类是 `on_net_local_city/on_net_cross_city/on_net_cross_province/off_net_in_province/off_net_cross_province/overseas`，互斥且可相加；`unknown/internal/transit/ambiguous` 不得挪入六类，必须以残差和 `classified_coverage` 单独展示。六类卡片比例的分母是包含残差的同方向总量，因此六类之和允许小于 100%；占比图若只绘六类，旁边必须同时显示覆盖率；
- 峰段是带时区的可复用本地墙钟窗口（工作日/星期集合、`start_local/end_local`），可跨午夜。服务端先按 timezone 判定桶是否入窗，再计算统计；UI 的“午高峰/晚高峰”只是命名配置，不能变成浏览器端删点；
- TopN 永远带稳定 tie-break 和 `_other`；Geo 结果按事实自身的 immutable version/code 展示，运营商按发布后的稳定 operator ID；跨版本不得按显示名称合并；
- 图、KPI、VTable 和导出都来自同一规范请求与同一 count/counter。报表导出冻结 report kind、请求、版本、水位和 panel selector，复用 `/flow/exports`、`export_tasks + operation_jobs` 输出 CSV/Parquet；全量导出不受当前页影响，超过同步预算创建可追溯任务而不截断。

外层继续使用 QueryGateway 的 `{data,meta}` envelope；`data` 内的固定报表公共 metadata 至少为：

```json
{
  "schema_version": 1,
  "kind": "overview",
  "display_mode": "value",
  "range": {"requested_from": "...", "requested_to": "...", "effective_from": "...", "effective_to": "...", "timezone": "Asia/Shanghai"},
  "plan": {"source": "1m", "source_seconds": 60, "display_seconds": 300},
  "watermark": {"latest_complete_bucket": "...", "generated_at": "..."},
  "versions": {"dimension_snapshot_id": "...", "geo_version": "...", "classification_version": "1"},
  "completeness": {"complete_ratio": 1, "late_ratio": 0, "unknown_ratio": 0.003, "partial": false},
  "panels": [{"id": "total", "status": "ready", "data": {}, "meta": {"unit": "bps", "step_seconds": 300, "as_of": "...", "versions": {}, "completeness": {"complete_ratio": 1, "late_ratio": 0, "unknown_ratio": 0, "partial": false}}}],
  "warnings": []
}
```

### 11.2 六套固定报表

1. **Flow 总览**：总入/出与趋势；六类卡片分别显示入/出、sparkline 和占总量比例；六类占比；业务 × 总量/六类的入/出/占比矩阵；采样、质量、分类残差和版本完整性。卡片、图和矩阵必须逐方向与总量对账。
2. **多维报表**：提供“默认六类、本网按省、异网运营商、境外、VPN”固定分组，分别绘制上行/下行趋势，并支持 value/share/signed `in-out`、当前/95th/峰值/平均。国家→省→市、运营商、业务和峰段是便捷筛选；任意 1–4 维、地址集合、热力和桑基保留在 Advanced Explorer。
3. **源 IP 分析**：服务端 Top IP 表显示总入/出及 sparkline、六类入/出/占比、业务标签和明细入口；分页、搜索、排序、column filter 全部作用于完整服务端结果。管理员“修正归属地”只深链到地址库 draft 并预填 IP、时间、旧版本和命中证据，发布仍走地址库异步 job，报表不得直接改历史事实或 active snapshot。
4. **目的 IP 分析**：与源 IP 使用同一 typed endpoint report，仅 `side` 不同；独立 URL、权限审计、详情状态和导出，禁止复制 SQL、字段 registry 或页面表格逻辑。
5. **境外流量**：在现有入/出 KPI 与趋势、观测境外 IP/本地主机、country/region TopN 基础上，补齐 ASN、远端端口+协议、业务/运营商筛选、VPN 流量比例和明细入口。observed 唯一数不按采样率放大；unknown Geo、unknown sampling 与 mixed version 分开显示，不能猜测。
6. **VPN 运营报表**：上层展示疑似主机、VPN 总流量/占比、活跃端口、高风险主机、入/出趋势、端口分布和 VPN 类型分布；下层才是 candidate/finding、score、证据、probe timeline 和人工处置。流量统计来自 Flow facts/candidate generation，findings 来自 MySQL 管理对象，二者以冻结窗口和规则/事实版本关联，不能把 findings 行数冒充流量或主机总数。

所有报表 VTable 都必须具备服务端分页、搜索、稳定排序和 typed column filter；filter popover 使用 body portal、collision clamp、viewport max-height 和内部滚动。页面在 1280×720 和 1920×1080 下不得出现图例、tooltip 或筛选浮层溢出；查询条件进入 URL，切换条件取消旧请求，旧响应不得覆盖新结果。VPN findings 读取/筛选/处置和异步 CSV 导出已经挂载，但这只算 VPN 管理面片段；规则 publication、关闭窗口写入和 probe 编排不可用时必须逐 panel 显示 unavailable，禁止请求不存在的接口或用样例数据伪装成功。

VPN 规则管理分为“可编辑 draft”和“不可变 rule-set publication”两层。draft API 只暴露评分器 schema v1 已实际消费的 `name/kind/match/effect/weight/priority/status`；`match` 由 `flowvpn.NormalizeRule` 与数据面共用同一 canonical validator，数组排序去重后持久化。migration 055 中预留但尚无执行语义的 `behavior_json/intelligence_json/probe_policy_json` 不进入 API，直到对应 analyzer/probe contract 冻结，避免接受不会生效的配置。创建、编辑、软删要求 tenant `configure_adjustment`，GET 要求 `vpn_view`；PATCH/DELETE 强制 quoted `If-Match`，name 冲突返回 409，row-version 冲突返回 412，所有 mutation 写审计。draft 的 `active` 仅表示下一次 rule-set 编译的纳入候选，不是 worker 已安装证明；只有后续签名 publication 完成 activation 且 worker 回报 installed ACK，页面才能显示该 rule-set 可用。

### 11.3 VPN rule-set publication 契约

VPN 发布不是另一套配置 CRUD 或状态机。它复用平台 `dimension_snapshots`、`dimension_snapshot_activations`、`dimension_snapshot_acks`、`dimension_snapshot_references` 和 `operation_jobs`，固定 scope 为 `(module_key='flow', dimension_key='vpn_rule_set')`；地址库继续使用 `flow/address`。平台仓储和生命周期必须先提取为显式 scope 参数的内部通用内核，地址和 VPN API 只做 typed adapter，任何按 ID 的读、审批、激活、ACK、引用和 GC 都必须同时匹配 tenant/module/dimension。现有四张表已能承载该类型：VPN 行使用 `source_manifest_version=0`、`source_manifest=[]`，`entry_count=rule_count`，地址专属的三个 count 均为 0，因此不创建空 migration；只有发现真实新持久字段时才从当前 migration head 领取编号。

不可变 object 的唯一 schema v1 为：

| 字段 | 约束与含义 |
| --- | --- |
| `schema_version` | 固定 `1`；未知版本 fail closed |
| `snapshot_id/tenant_id/version` | snapshot ID 是 finding 中的 `rule_set_version`；version 只是同租户发布序号 |
| `effective_from` | UTC 分钟边界，并与 snapshot/activation 元数据完全相同 |
| `medium/high/critical_threshold` | `0 < medium < high < critical <= 100` |
| `probe_threshold/minimum_completeness` | probe 为 1..100；完整度为有限数且在 `[0,1]` |
| `rules[]` | 只含发布时 `status=active AND deleted_at IS NULL` 的规则，按稳定 ID 排序，1..1000 条 |
| `rules[].id/name/kind` | ID 唯一；name trim 后 1..190；kind 固定 `passive/intelligence/probe`，用于历史解释 |
| `rules[].effect/weight/priority/match` | 与 scorer schema v1 同一 validator；列表排序去重，禁止未知字段 |

encoder 只生成无额外空白的 canonical JSON；decoder 先限制 4 MiB，再校验对象级 `sha256:<lowerhex>`、严格 JSON/EOF/schema/identity/time/rule budget，重新编码后要求逐字节相等，最后才构造不可变 scorer。名称和类别进入 object/checksum/signature，以便历史 finding 从产生它的 bundle 还原解释；评分器只接收执行字段。这里的 SHA-256 每次小型管理对象发布计算一次，用于下载完整性和审批签名，不在 Kafka/ClickHouse 逐 flow 写路径上，不恢复已废除的 per-record hash。

preview 输入为 `effective_from` 及五个 threshold，响应包含 canonical `draft_digest`、rule count、estimated object bytes 和具体 validation error。digest 覆盖 schema、threshold 及所有 active published rule 字段，但不包含尚未分配的 snapshot ID/version；inactive/suspended/retired/deleted draft 不影响它。publish job payload v1 只保存 effective time、thresholds 和 preview digest，不复制规则 JSON。handler 在同一 repeatable-read 事务中锁 tenant，重新读取 active rows、规范化并复算 digest；不一致终态返回 draft-changed，避免 preview 后静默发布另一批规则。

发布成功顺序固定为：分配该 scope 的下一 version/snapshot ID → 编码对象 → 保存对象并核对 checksum → 插入 pending snapshot 与 audit → 提交。失败时清除尚未被 snapshot 引用的对象。重试先按 `(tenant, scope, effective_from)` 查询：digest、threshold/object checksum 都相同则返回原 snapshot；同一生效分钟但内容不同返回 conflict，保证“DB 已提交但 job 结果未写”重试不会创建第二版本或把成功任务误报失败。operation job idempotency key 绑定 tenant、scope、effective minute 和 preview digest；lease/retry/cancel 沿用平台实现。

HTTP 固定为 `POST /api/v1/flow/vpn/rule-sets/preview`、`POST .../publish`、`GET /api/v1/flow/vpn/rule-sets`、`GET .../{snapshot_id}` 以及 `approve/reject/activate/rollback/retire` actions。preview 要求 `configure_adjustment`，publish/lifecycle 要求 tenant `operate`，list/get 要求 `vpn_view`；所有状态修改使用 quoted `If-Match`。规则页面只表示 draft，publication 页面显示 pending/approved/rejected、event-time activation、worker downloaded/installed/failed、drift、引用和对象回收状态，不能把 snapshot 创建或 activation 冒充 worker ready。

审批继续使用 Ed25519 且签名覆盖 tenant/scope/snapshot/version/effective time/object ref+checksum/draft digest/schema。trusted-key resolver 应提升为平台 publication 公共能力；部署键使用 `publication.trusted_keys[]`，旧 `address_library.trusted_keys[]` 仅作为 `flow/address` 的一个发布窗口兼容输入，VPN 不借用 EdgeManager/address 命名。私钥仍在外部审批方。激活和回滚只追加 event-time timeline，不修改历史 finding；worker 依据事件时间选择版本，下载/校验成功后原子替换 catalog 并 ACK，失败继续使用上一已安装 generation。

finding 落库时必须同时延长相应 rule-set reference，consumer key 使用有界的 `vpn_findings:<UTC-day>` 聚合而非每 finding 一行，`retain_until` 至少覆盖该日 findings 的最大 `expires_at`。关闭窗口写 finding 与 reference 必须同一 MySQL 事务，失败整体回滚；这样 retire/GC 不会删除仍可查询 finding 的解释对象。升级门禁覆盖旧 worker 拒绝未知 schema、新旧 worker 并存、publish commit 后 crash/retry、坏对象/签名、ACK 失败保持旧 scorer、同分钟冲突、跨 tenant/scope ID、回滚、引用与 GC 竞态。C4-P 只交付 publication；安装/ACK 属 C4-W，主动探测授权仍属 FLOW-07B。

所有 VTable 都必须具备服务端分页、搜索、排序和 column filter；filter popover 使用 portal、collision detection、viewport max-height 和滚动，不得溢出或错位。公共 VTable 只给 list API 明确声明的 typed filter/sort 列显示入口，未声明列不得退化为当前页本地筛选或排序；筛选、排序、搜索或页大小变化必须回到第一页，并丢弃已发出的旧响应。Geo 表每行显示当前层名称、完整路径和稳定 ID tooltip，并可进入 children；图例只包含当前 level。地址组用多值 chips/独立 TopN，明确重叠口径。页面保留 query state 到 URL，支持取消过期请求；大数据只显示 TopN + other，不渲染无限序列。每张图支持创建/修改/复制/删除保存视图，保存的是 versioned QueryRequest，不保存 SQL。

## 12. 性能、容量与故障

### 12.1 容量模型

带宽不能直接换算 PPS。容量输入至少包括 exporter 数、datagrams/s、datagram size、records/datagram、协议/模板比例、Kafka record bytes、CH rows/batch 和查询并发。100G、2×100G、12×10G 只描述链路上限；采样率、平均包长、聚合方式和 exporter 配置决定真实负载。

性能验收在固定硬件上分别使用 64/256/1024-byte datagram 和真实混合 corpus：

- 持续目标峰值 2× 30 分钟，突发 3× 5 分钟，72h soak；
- 报告 received/published/decoded/committed、kernel drop、Kafka lag、CH rows、CPU/RSS/GC/alloc；
- N+1 后单实例接管仍满足 SLO；
- 任何瓶颈必须以 profile 和容量数据定位，不预设 GoFlow2 或 Kafka 足够/不足。

### 12.2 故障矩阵

| 故障 | 行为 | 数据状态 |
|---|---|---|
| Kafka 中断 | producer 有界重试，满后背压/socket drop | 明确 loss interval |
| collector 崩溃 | VIP/exporter 切到 N+1 | Kafka ACK 前 UDP 可能丢；sequence/drop 证明 |
| worker 崩溃/rebalance | assignment 有界回放模板窗口，未提交 offset 重放 | 不回退 committed watermark；deterministic ID + Replacing 收敛 |
| 模板缺失 | 标记并跳过，不阻塞未来模板 | partial，后续模板恢复 |
| snapshot 缺失 | pause partition，不用最新版本猜 | Kafka lag |
| CH 不可用 | pause，不提交 offset | Kafka lag |
| rollup 失败 | base 继续写，查询显示 rollup stale | 从 base 修复 |
| VM 不可用 | 不影响 Flow facts | 可观测性 degraded |
| SNMP 不可用 | 不影响分类 | reconciliation unavailable |

### 12.3 查询与安全限制

默认限制时间范围、最大 points/series/TopN、扫描分区/字节、并发和超时。超限返回可操作建议，不自动发起昂贵查询。IP 明细、VPN 证据和导出单独授权；日志/metric 不记录原始 payload、secret 或完整高基数查询值。主动探测默认关闭，必须有租户范围、审批、配额、deadline 和全局 kill switch。

## 13. 生命周期

| 对象 | 创建/生效 | 保留 | 终止 |
|---|---|---|---|
| RawFlow Kafka | broker ACK 后 durable | 覆盖最大 worker/CH 故障窗口 | retention 自动删除 |
| CH records | Kafka message receipt durable 后可提交 offset | tenant policy 的 raw online window；默认无限 | 当前禁止自动删除；未来须经 downsample 守恒、Kafka committed-offset 日覆盖和 delete grace 后由显式 job 删除 |
| 1h archive | UTC 日越过 raw/late aging window 后生成 | tenant policy；0 表示无限 | 当前禁止自动删除；未来按月分区和显式 operation job 删除 |
| 1m aggregate | V1 兼容表 | 不再新增 Storage V2 数据 | 回滚观察窗结束后只允许 forward migration 退役 |
| Geo/dimension snapshot | validate + approve + publish | 不短于引用事实/Kafka | 无引用后清理 |
| VPN finding/evidence | 规则窗口关闭/探测回传 | 风险策略 TTL | 审计后到期删除 |
| export | 异步完成 | 短期、签名下载 | 到期删除并记录 |
| exporter/collector | enrollment + activation | 生命周期内 | revoke plan/credential，软删后 purge |

tenant purge 固定为：冻结新写入 → 撤销 collector/probe 凭据 → 终止 jobs/exports → 删除 CH/VM 范围数据 → purge MySQL Flow 配置 → 验证 provider 查询为空 → 生成 destruction receipt。任一步失败保持可重试状态，不报告成功。

## 14. 分期与闭环验收

### P1：RawFlow 到六类总览

collector、单 topic、worker/GoFlow2、采样、Geo/dimension、CH records/1m、VM pipeline metrics、总览；真实 Kafka/CH、四协议 fixture、守恒、故障和容量测试全部通过。

### P2：明细、修正、导出与对账

源/目的 IP、地址段/address set、raw/supplier/customer、reclass job、异步导出、SNMP reconciliation；权限、分页/filter、回算边界和导出审计通过。

### P3：境外与 VPN

境外专题、规则评分、candidate/finding、可插拔 probe agent；只观察/canary/kill switch、安全范围和误报处置通过。

### P4：HA、运营和灾备

N+1、多 broker/replicated CH、容量预测、备份恢复、跨故障组合、tenant purge 和 72h soak 通过。

每个 P 都必须完成：设计冻结 → 编码 → 单元 → 集成 → 变更设计 → 变更测试 → race/vet/全量回归 → 灰度/回退证据。具体 checkbox 和自动循环顺序只维护在 tasklist，本文不复制执行状态。

## 15. 复杂度守门

以下变更必须新 ADR、容量/故障数据和删除计划：第二 Flow 数据 topic、collector 本地 WAL、独立状态数据库、另一套 retry/ACK 状态机、Flow 双写 VM、同步地址 RPC、逐 flow HTTP/日志、浏览器直连 Kafka/CH、通用工作流引擎。优先使用 Kafka offset、deterministic batch、immutable snapshot、CH base 重建和平台现有 CRUD/job/RBAC。

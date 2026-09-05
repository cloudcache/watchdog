# Flow 模块详细设计

> 状态：现行。本文只定义当前实现契约；需求口径见 [flow-direction-requirements.md](flow-direction-requirements.md)，架构取舍和废弃方案见 [flow-pipeline-adr.md](flow-pipeline-adr.md)，唯一执行状态见 [flow-module-tasklist.md](flow-module-tasklist.md)。完成历史不得回填到本文。

## 1. 冻结边界

### 1.1 唯一数据链路

```text
router/switch
  -> watchdog-flow-collect
       UDP receive -> source-prefix admission -> RawFlow protobuf -> Kafka
  -> watchdog.flow.raw-v1
  -> watchdog-flow-worker
       partition-ordered GoFlow2 decode -> sampling -> dimensions -> CH batches
  -> ClickHouse base + asynchronous rollups
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

复用平台已落地的 `users/roles/permissions`、tenant scope、target/device/port、collector registry、签名 plan、`address_prefixes/address_sets`、`operation_jobs` handler registry、export、visualization 和 audit。immutable dimension publication 仍是平台待办 PLAT-04A；PLAT-04B 的剩余项是通用 per-tenant 周期触发和跨类型扫描背压，不得把已经存在的 lease/heartbeat/cancel/retry/typed payload registry 描述成未实现。Flow 不复制通用 CRUD、身份或任务引擎；平台缺陷登记在 [platform-refactor-tasklist.md](platform-refactor-tasklist.md)，除非阻断当前 Flow 切片，否则不得顺手重构平台。

### 1.4 正确性不变量

1. 同一 UDP datagram 只进入一个 RawFlow topic；同一 `collector_id + exporter_ip` 固定到同一 partition。
2. worker 只有在 ClickHouse base 批次验证成功后才提交 Kafka offset。
3. `raw_bytes/raw_packets` 永不改写；`estimated_*` 每条记录最多放大一次。
4. 业务方向只由事件时间有效的本地前缀决定；观察接口只用于定位和冲突标志。
5. 每条事实固定 `registry/dimension/geo/classification/rule` 版本，可重放、可解释。
6. 主前缀最长匹配且互斥；address set 是可多归属标签，跨 set 不得相加当总量。
7. 六类 + `internal/transit/unknown` 与 accepted base fact 守恒。
8. 故障显示 incomplete/lag/loss interval，禁止伪装为 0 流量。

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
- 关闭时先停止收包，再在有界时间内 flush；超时记录未确认时间窗。

Kafka 不可用不会触发第二套 WAL、retry 数据库或 ACK 状态机。容量必须通过 Kafka buffer、socket buffer、N+1 collector 和告警窗口保证，而不是宣称绝对零丢失。

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

### 3.4 ClickHouse 提交协议

1. consumer 把一次 fetch 按 partition 分组；partition 内顺序解码/富化，不跨 partition 混批；
2. 一个 partition fetch 只调用一次 durable handler；snapshot/binding 不可用时整批不写、不标 offset；
3. handler 按固定 rows/bytes 上限切 columnar block，以稳定 record ID 和 block token 同步写 `flow_records`；网络歧义重试同一 block；
4. base 写成功后追加一条 batch receipt，供 Kafka offset/row/checksum 对账；热路径不预查 receipt，也不逐批执行 `FINAL`；
5. 全部 block 和 receipt 成功后 handler 返回，consumer 才标记该 partition fetch 的 offsets；
6. ClickHouse 是 at-least-once sink：重放可能短暂产生物理重复，`record_id + ReplacingMergeTree` 收敛；面向用户的 base 查询和异步 rollup 必须按稳定 ID 去重；
7. 1m/1h rollup 只重建已关闭 event-time bucket，以 generation 原子替换；失败从 base 重算，不回写 Kafka。

这条链路没有两阶段提交、分布式事务或每批 read-before-write。Kafka offset 是消费进度，稳定 record ID 是事实幂等键，receipt 只做审计/对账，三者职责不可混用。

崩溃发生在 records 与 receipt 之间时，重放直接使用相同 records、generation 和 insert token，不做写前查询；物理重复由稳定 `record_id + ReplacingMergeTree` 收敛。目标 ClickHouse 版本必须实测 insert token、ReplacingMergeTree、`FINAL` 和 replicated/distributed 变体；在这些证据完成前只承诺 at-least-once，不宣称 exactly-once。

NetFlow v9/IPFIX 模板状态位于 worker 内存，而模板 record 可能已提交。每次 partition assignment 因此从原 committed offset 向前回放固定数量的 RawFlow record：旧窗口参与解码和幂等 CH 写入以重建模板，但提交水位绝不能低于 assignment 前的 committed offset；到达旧水位后才正常前进。`template_replay_records` 必须为正且有硬上限，并按“单 partition 在 exporter 最大模板刷新间隔内的 record 数 + 裕量”定容。exporter 必须周期刷新模板；未满足此前置条件时显示 template-missing/partial，不能声称完整，也不能用猜测字段解码。

### 3.5 FLOW-04B 关闭桶调度与 repair

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

v1 的 range 行只有扁平 country/admin/city，不能完整表达“洲 → 区域 → 国家 → 省 → 市”。`flow-geo-v2` 保持四个文件和 manifest 校验机制，只给 range CSV 增加必需的 `geo_leaf_code`。每个展平后的不重叠 IP 区间只引用一个最具体 Geo 节点；`geo_dict.json` 保存邻接树，允许的产品层级固定为 `continent/region/country/province/city`。每个 code 在一个 bundle 内全局唯一、至多一个 parent；loader 拒绝缺父、环、同一路径重复 kind、逆序层级、禁用叶/祖先和超过五层的路径。节点名称只用于显示，事实和关系都引用稳定 code。

加载时由 `geo_leaf_code` 沿 `parent_code` 一次性预编译五级路径，并校验 range 的 country/admin 字段与路径不冲突；热路径只做一次地址区间查找，不逐 flow 追树或查库。ASN 和 ISP 均为 `0=unknown` 的可选属性，不是 range、Geo 节点或地址组的主键，也不影响无 ASN 地址的发布。找不到某一层时该层为 `_unassigned`，不得用上级或名称猜测。

同一不可变 `GeoIndex` 也是查询面的树语义权威：按稳定 code 返回节点和 root-to-node breadcrumb，按 parent 返回已启用直属 children，按 ancestor + 目标 kind 展开排序去重且有硬上限的 descendant codes。展开只允许 `flow-geo-v2`，一次请求固定一个 `geo_version`；不得跨版本按名称拼树，也不得由 hub 复制一套 loader/遍历算法。

loader 流程：在临时目录验证 manifest/schema/size/checksum/row count → 检查区间、外键、枚举、Geo 树和 IPv4/IPv6 → 构建不可变 range index + 预编译路径 → 原子交换指针。失败保留当前版本并告警。至少保留覆盖 Kafka retention、base 重分类窗口的旧版本；event time 选择 `effective_from <= event_time` 的最新版本。v2 loader、EdgeManager 导出脚本、worker 事实字段和 CH 002 向前迁移已经完成；查询 API 仍须按 `geo_version` 解析名称和树，完成前 UI 不得自行拼接 continent/region。

### 4.2 三种地址归属语义

| 语义 | 存储与命中 | 可否相加 |
|---|---|---|
| 主地址段 `primary_prefix` | `address_prefixes` 每个 canonical CIDR 一行；endpoint 最长前缀只命中一个稳定 ID，未命中为 `_unassigned` | 同一 endpoint role 下互斥；含 `_unassigned` 时可与总量对账 |
| Geo 层级 | range 只存一个 leaf，加载时得到 continent/region/country/province/city 各零或一个稳定 ID | 同一 level 互斥可加；不同 level 是同一字节的不同观察角度，严禁跨层相加 |
| 地址组 `address_set` | 独立的组 ID/组名 + label selector/显式 CIDR/组引用及排除；一个 endpoint 可命中任意多个组，数组排序去重并有硬上限 | 组间可能重叠，固定 `additive=false` |

`address_prefixes` 不要求 ASN。相同 CIDR 不为每个组或 Geo 层重复建行；需要多个组时让一行携带多个可选择 label，由多个 `address_sets` 分别匹配。需要 CIDR 逐级细化时允许嵌套行：发布编译从覆盖范围最大的父段到最具体子段继承 label，同 key 由子段覆盖，不同 key（如 continent/region/country/province/city/business）全部保留；运行时仍只做一次 LPM。主前缀 ID 永远取最具体行，继承不会产生多个 primary prefix。

MySQL 管理态保存 CIDR、labels、组 ID/名称和 selector；immutable publication 将名称之外的稳定 ID/规则编译给 worker。名称可改但 ID 不变，历史显示按记录引用的 dimension/Geo version 解析，不能用当前名称篡改历史语义。

base fact 一条 flow 只写一行：保存唯一 `local_prefix_id/remote_prefix_id`、去重后的 `*_address_set_ids`，以及 `remote_geo_continent_id/region_id/country_id/province_id/city_id`。不为五级 Geo 复制五条 base fact。异步 rollup 才将同一计数展开为五种独立 `dimension_kind`；每次查询必须选定一个 kind。地址段流量归类必定异步：collector 只送 raw datagram；worker 使用已加载内存快照做 LPM/Geo range lookup；CH rollup 从 base 的稳定 ID 汇总。禁止 UDP 热路径访问 MySQL、文件或 HTTP。

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
| 2 | `onnet_local_city` | 本网、同城市 |
| 3 | `onnet_cross_city` | 本网、同省不同城市 |
| 4 | `onnet_other_province` | 本网、外省 |
| 5 | `offnet_same_province` | 异网、同省 |
| 6 | `offnet_other_province` | 异网、外省 |

Geo 不足得到 `unknown`。港澳台口径由 snapshot 固定。`home_isp_ids/home_asns` 分别匹配结构化 ISP ID/ASN，任一命中即为“本网”；只有远端具有与已配置集合可比较的身份时才判定异网，否则为 unknown，不以 ASN 名称字符串猜测。规则实现为纯函数，输入 record + immutable snapshot，输出 direction/category/provenance；property test 验证守恒和确定性。

### 4.6 修正视图

原始事实不可变。用户可选择：

- `raw`：导出包/协议原始值；
- `supplier`：供应商 Geo/ASN/ISP 及其版本；
- `customer`：租户版本化 prefix/Geo/business override。

规则含 reason、actor、审批、`effective_from/expires_at`、row version。修正只产生新 snapshot；在线查询按事件时间选版本。大范围历史修正使用 `operation_jobs` 异步回算，在新 generation 守恒验证后切换，不原地 UPDATE CH facts。

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

顺序 migration 003 向 001 的已发布基线前向增加 `remote_prefix_id/geo_version/classification_version/row_kind`，并在保留原排序键完整前缀的基础上加入 row kind 与三个事实版本。每次 materialize 用一个同步 `INSERT SELECT ... UNION ALL` 原子写入全部 candidate 和内部 `_generation` marker；即使窗口为空也写 marker。相同请求使用稳定 dedup token；迟到或规则重算使用更大 generation。读取方必须先按 `tenant + window + rule_set_version + row_kind=_generation` 取得最新 generation，再只读该 generation 的 candidate，不能按剩余 candidate 求 max，否则空修复或候选消失后会泄漏旧行。001/002 不回改；旧行通过 `row_kind='candidate'` 默认值保持可读，但在没有对应 marker 时不属于新读取契约。

FLOW-07A3 reader/scorer bridge 用同一个参数化 CH 查询同时选择 marker 和该 marker 的 candidate，避免“两次查询先读 generation、再读数据”之间发生 repair 竞态。读取上限由请求给出且不得超过 50,000，SQL 用 `limit+1`，runner 再做独立硬限；tenant、窗口、rule-set version 和 limit 全是 typed parameter。CH 的 UNION/block 返回顺序不构成协议，metadata 可先于或后于数据；runner 只在整次执行结束后验证恰有一个 marker、generation 大于零且所有 candidate generation 一致。列数/列长、重复候选键、IPv4-mapped 还原、四类版本、canonical conversation key、显式 hint、evidence schema/counter、complete ratio 和 scorer 输入逐项验证；缺 marker、坏行、部分响应、执行错误或超限全部 fail-closed，不返回部分评分。空窗口只有合法 marker 时返回空 candidate 集与对应 generation，不回看旧行。该 bridge 仍无 MySQL 写入和 probe/job 副作用。

## 6. MySQL 管理契约

Flow 管理面最终只拥有四张域表；reclass/probe/export 复用平台 operation_jobs，地址规则复用 address_prefixes/address_sets，审计复用 audit_logs。四表当前尚未进入 migration，不能把本文当作已部署 schema；实施时只在新的顺序 migration 中建表，并同步 fresh-install、repository、API、迁移和回滚测试。

| 表 | 必需字段 | 唯一身份与索引 | 生命周期 |
|---|---|---|---|
| flow_exporters | tenant/collector/target/device、source IP、protocol、observation domain、counter/sampling/reconcile policy、generation/observed generation、row version | 活跃记录按 tenant + protocol + source_ip + domain 唯一；按 collector/target+status 查询 | pending -> active <-> suspended -> retired -> deleted；软删后异步清理 |
| flow_settings | home country/province/city、home ISP/ASN、港澳台口径、internal/transit policy、active publication、classification version、row version | tenant 主键 | publication 验证通过后原子切换；历史版本保留到无事实引用 |
| flow_vpn_rules | kind、versioned selectors/behavior/intelligence/probe policy、weight、status、row version | 活跃名称按 tenant 唯一；按 tenant+status 查询 | draft -> active <-> suspended -> retired -> deleted |
| flow_vpn_findings | window/conversation、双向 bytes、协议/端口、score/level/verdict/disposition、evidence/rule version、probe result、snapshot/geo version、expiry | tenant + window + conversation + rule_set_version 唯一；按 level/probe status 查询 | 机器 verdict 与人工 disposition 独立；TTL 后可验证销毁 |

统一约束：

- ID 使用平台稳定 ID；名称只展示，不参与引用；IP 用 16-byte binary；时间为 UTC millisecond。
- JSON 必须有 schema version，API 拒绝未知 key、非法枚举、过宽 CIDR/端口和超量数组。
- 创建/action 要求 Idempotency-Key；PATCH/DELETE 要求强 If-Match，成功递增 row_version。
- tenant、collector、target、device、用户和 publication 的外键只能引用当时真实存在且长度一致的表；禁止先在设计中虚构外键。
- immutable dimension publication 是平台前置项 PLAT-04A；落地前 worker 仅使用验签静态 publication，绝不逐 flow 查询 MySQL。

## 7. ClickHouse 5 张表

完整、可执行且唯一权威的单节点 DDL 是 [`deploy/migration/clickhouse/`](../deploy/migration/clickhouse/) 下按文件名顺序执行的 migration：001 建立基线，002 向前增加 Geo v2 五级稳定 ID 和 ASN 来源枚举，003 完成 VPN candidate provenance 与 generation marker 契约。设计文档不再复制一份会漂移的 SQL。生产集群只允许由后续 migration 生成 Replicated/Distributed 变体，不在运行时拼 DDL。

| 表 | 角色 | 幂等/查询规则 |
|---|---|---|
| `flow_records` | 完整 enriched base fact | `record_id=32-byte SHA-256`；Replacing 收敛；base 查询去重 |
| `flow_aggregate_1m` | 近期趋势/TopN | 关闭 bucket 异步重建；按 `generation` 取最新 |
| `flow_aggregate_1h` | 长期趋势/TopN | 与 1m 同 schema，不从未关闭 1m 增量拼接 |
| `flow_ingest_batches` | 每次 durable insert receipt | 不参与写前判断；只做 offset/count/checksum 审计 |
| `flow_vpn_candidates` | 异步 VPN 候选 | 事实三版本 + 规则版本隔离；marker 选最新 generation，证据有 TTL |

DDL 使用代码里的准确枚举名（如 `on_net_local_city`、`off_net_in_province`），`record_id/batch_id` 保存原始 32 bytes 而不是 64 字节十六进制文本，`quality_flags` 保存 UInt64 bitset，`estimated_valid` 与零值显式分离；这些字段由 migration contract test 按顺序合成最终 schema 后与 Go encoder/物化 SQL 核对。001 仍保持已发布的 v1 基线，002 增加五级稳定 ID，003 只前向完成 candidate；禁止原地篡改已部署 migration。

`dimension_kind` 的公开 registry 固定为 `total/category/geo.continent/geo.region/geo.country/geo.province/geo.city/isp/asn/business/local_prefix/remote_prefix/address_set/src_ip/dst_ip/remote_port/protocol/observation_interface`；`_generation` 是不可查询的内部 marker。每次查询必须选一个公开 kind。`primary_prefix` 和每个单独 Geo level 在包含 `_unassigned` 时可与 `total` 对账；`address_set` 是重叠标签统计，不能与 `total` 对账；不同 Geo level 也不能彼此相加。1m/1h rollup 由异步 job 对单个 `tenant + 已关闭 bucket` 发出一次原子 `INSERT SELECT`，同一 generation 同时生成全部维度和 marker；迟到/修正以更大 generation 完整重建。查询先按 `tenant+bucket`（包括 marker）求最新 generation，再只读该 generation 的公开 kind，不能逐 key `argMax`，否则新版本已消失的旧 key 会残留。IPv4 写 IPv4-mapped IPv6，API 还原文本。

## 8. VM 指标与 SNMP 对账

两个数据面进程各自提供 Prometheus 文本格式 `GET /metrics`，由 VM pull/scrape；进程不主动写 VM。collector 默认监听 `127.0.0.1:9090`，worker 默认监听 `127.0.0.1:9091`，`-metrics-listen ''` 明确禁用。地址必须是裸 `host:port`，不接受 URL/路径或 0 端口；绑定失败使进程启动失败，运行中 metrics server 异常退出也会终止主进程，避免“数据面活着但监控永久消失”。端点只读、无业务数据，默认仅 loopback；跨主机抓取必须由部署层提供网络 ACL，TLS/mTLS 通过同机反向代理或 sidecar 终止，不在两个数据面进程中复制证书生命周期。

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
| process | `watchdog_flow_process_goroutines` / `heap_alloc_bytes` / `gc_cycles_total` / `start_time_seconds` | gauge,gauge,counter,gauge | Go runtime 状态 |
| process | `watchdog_flow_process_stats_up` / `cpu_seconds_total` / `resident_memory_bytes` / `open_fds` | gauge,counter,gauge,gauge | OS 进程统计；读取任一失败时 `stats_up=0`，其余值归零；Prometheus 自身 `up` 仍表示 scrape 可达性 |

counter 只用 atomic 单调累加；gauge 来自单次一致快照。worker lag 在 assignment 后、首次 high-watermark fetch 前是 unknown，必须用 `lag_known_partitions < assigned_partitions` 表达不完整，禁止填成零。模板有界 replay 期间，低于原 committed floor 的 record 不更新 lag；到达原水位后才更新。当前 consumer 没有 pause/resume 状态，CH/dimension 暂时失败直接不提交并退出等待编排重启，因此不暴露恒为零的伪 pause 指标。

禁止用 tenant、IP、ASN、prefix、port、topic、partition 或 exporter 原始地址作 metric label。高基数分析只进 CH。ingest receipt mismatch、rollup age/repair 只有在 FLOW-04B 复用平台 operation job 调度器后才能由真实 reconciliation/rollup runner 产生，不能在当前 worker 猜测或另建巡检状态机。

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
  "from": "2026-09-05T00:00:00Z",
  "to": "2026-09-05T01:00:00Z",
  "bucket": "1m",
  "metric": "estimated_bps",
  "dimension": "geo.city",
  "filters": {"directions":["in","out"],"dimension_values":["330100","330200"],"target_ids":[]},
  "view": "customer",
  "top_n": 20,
  "include_other": true,
  "timezone": "Asia/Shanghai"
}
```

tenant 不属于客户端 QueryRequest，由已认证 scope 单独注入 compiler；请求里的任意 ID、名称和时间都只能成为 typed ClickHouse parameter，不能进入 SQL 标识符或表达式。当前 provider registry 固定为：

- bucket：`1m` 读 `flow_aggregate_1m`，最多 10,080 点；`1h` 读 `flow_aggregate_1h`，最多 9,600 点；`from/to` 统一换算 UTC、左闭右开、必须桶对齐且 `to` 不得包含未关闭桶；
- metric：`raw_bytes/raw_bps/raw_packets/raw_pps/estimated_bytes/estimated_bps/estimated_packets/estimated_pps/received_records`；速率以当前桶秒数计算，TopN 仍按对应可加计数排序；
- dimension：第 7 节公开 registry 的 18 个值；只有 `address_set` 可多归属且 `additive=false`，禁止 `include_other`，其他单值维度都通过 `_unassigned` 保持与 total 可对账；
- filter：`directions/categories/businesses/target_ids/device_ids/exporter_ids/dimension_values/dimension_snapshot_ids/geo_versions/classification_versions`。枚举严格校验，值排序去重，单字段最多 2,048、总计最多 4,096；API 将 `geo_ancestor_ids` 用同一 Geo version 展开后才填 `dimension_values`；
- TopN 为 1..100，稳定顺序是值 DESC，再按 dimension ID 和三个版本字段 ASC；`total` 要求 TopN=1。点数乘 `(TopN + other)` 超过 250,000 时在查询前拒绝，CH 同时设置 15 秒、25 万结果行、5,000 万扫描行硬限并以 `throw` 结束，不能静默截断；
- `timezone` 只控制 API/前端显示，CH bucket 始终 UTC。

aggregate schema v1 只物化 `customer` view；`raw/supplier` 请求必须返回稳定的 `unsupported` 错误，不能把 customer 结果换个标签返回。FLOW-06 只有在 base/rollup 增加可验证的并行 provenance 或 versioned reclass generation 后才能开放另外两个 view。

每个 bucket 的读取先仅从 `_generation` marker 求最新 generation，再用 `(tenant_id,bucket,generation)` 回连公开维度；禁止对每个 dimension key 单独 `argMax`。TopN key 固定包含 `dimension_value + dimension_snapshot_id + geo_version + classification_version`，所以跨版本不会按名称或裸 ID 静默合并。`other` 也按版本分别返回；结果行硬限处理版本爆炸。每次查询固定追加一条 `is_metadata=1` 的内部 sentinel，`covered_buckets=count(latest generation marker)`；provider 删除该行并与请求期望桶数比较。即使没有任何公开维度行，也能区分“完整的零结果”和“rollup 未完成”，不得由 HTTP 层猜测。compiler 的 `RequestError` 固定提供 `field + code(required/invalid/unsupported/limit_exceeded/incomplete_range) + message`，API 只负责映射统一错误 envelope。

每个维度结果至少返回 `id/name/kind/parent_id/path/additive/completeness/dimension_version/geo_version`。compiler 固定返回每行 `received_records/unknown_sampling_records/quality_records/generated_at`，API 由此计算 sampling completeness 和质量提示；未知不能当 0。Geo 同层 `additive=true`，缺失归 `_unassigned`；address set 固定 `additive=false`。时间范围跨多个名称或树结构版本时，响应必须按版本拆分或返回 `mixed_versions` 警告，不能把同名当同 ID，也不能把不同 ID 静默合并。

provider result runner 必须消费 CH 的多个 data block，校验所有列等长、结果行硬限、bucket 范围/对齐、非负有限值、版本身份、quality counters 和点唯一键。metadata sentinel 必须恰好一条且除 `covered_buckets` 外为固定零值；缺失、重复、畸形或覆盖数大于期望桶数都使整个查询失败，不能返回部分结果。对每个公开点返回 `sampling_completeness=(received-unknown_sampling)/received` 和 `quality_record_ratio=quality/received`；分母为零时分别标记 `Known=false`，不返回虚假的 100%。结果总体返回 `expected_buckets/covered_buckets/ratio/complete` 以及 `mixed_versions/version_count`；CH 在任一 block 后失败时丢弃已累积点，不泄漏部分成功。

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

IP 明细是 base fact 搜索，不复用 rollup，也不把 ASN 当作地址段身份。v1 请求契约固定为：

- tenant 只从 authenticated scope 注入；`ip` 接受 IPv4/IPv6（拒绝 zone），内部规范化并与 IPv4-mapped IPv6 存储比较；`endpoint` 只能是 `source/destination/either`；
- `view` 只接受 `customer`，并固定过滤 `disposition='count'`。在 FLOW-06 建成事实 provenance 前，`raw/supplier` 返回 `unsupported`，不能改标签冒充；
- `from/to` 是 UTC 左闭右开、毫秒精度，单次最多 24 小时且 `to` 不得在未来；`limit` 为 1..500，CH 实际读取 `limit+1`；
- 排序键固定为 `(event_time DESC, record_id DESC)`。opaque cursor v1 由 8 字节大端 Unix 毫秒和 32 字节 record ID 组成；续页谓词是 `(event_time < cursor_time) OR (event_time = cursor_time AND record_id < cursor_record_id)`。next cursor 取已返回页最后一行而不是探测出的额外行，因此同一毫秒也不重不漏；修改 field mask 不改变 cursor 含义；
- cursor 版本、长度、base64 canonical encoding、时间范围均严格检查。首发只有 v1；未来不兼容升级必须以新前缀发布、旧 decoder 保留一个发布窗口，未知版本明确拒绝，不能猜测；客户端续页时必须原样保持 IP、endpoint、时间和 filters；
- 可选字段只能来自 provider 固定 registry；`event_time/record_id` 永远返回，内部还读取 src/dst IP 复核结果。地址集合数组不进入 v1 field mask：重叠集合的并/交/差必须走 base membership 谓词和去重聚合，不能由明细数组在 UI 侧相加；
- filters 只开放 `directions/categories/businesses/target_ids/device_ids/exporter_ids`，单字段最多 100 个、原始总数最多 256 个，先限量再排序去重。所有值都是 typed parameter，只有固定 registry 中的表达式进入 SQL；
- 查询使用 `flow_records FINAL` 收敛 at-least-once 物理重复，并设置 10 秒、`limit+1` 结果行、500 万扫描行、1 GiB 扫描字节硬限，所有 overflow mode 为 `throw`。超限/超时返回错误，不返回静默截断页。

```sql
SELECT event_time,
       lower(hex(record_id)) AS record_id,
       toString(src_ip) AS _source_ip,
       toString(dst_ip) AS _destination_ip,
       /* fixed field-registry expressions */
FROM flow_records FINAL
WHERE tenant_id = {tenant:String}
  AND event_time >= {from:DateTime64(3,'UTC')} AND event_time < {to:DateTime64(3,'UTC')}
  AND disposition = 'count'
  AND (src_ip = toIPv6({ip:String}) OR dst_ip = toIPv6({ip:String}))
  AND (event_time < {cursor_time:DateTime64(3,'UTC')}
       OR (event_time = {cursor_time:DateTime64(3,'UTC')}
           AND record_id < unhex({cursor_record_id:String})))
ORDER BY event_time DESC, record_id DESC
LIMIT {fetch_limit:UInt16}; -- requested limit + 1
```

provider 使用与 field mask 对应的 typed ch-go columns 消费任意多个 data block，校验列等长、总行数不超过 `limit+1`、时间范围/毫秒精度、record ID canonical hex、IP 端点命中、游标边界以及跨 block 全局严格降序/唯一。任一结果畸形、取消或 CH 执行失败都丢弃累积行，响应全有或全无；只有真实读到额外一行才返回 `has_more=true + next_cursor`。

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
       dimension_snapshot_id, geo_version, classification_version,
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

95th 必须先生成等长 bucket 的 bps，再使用 `quantileExact(0.95)`；平均是 bucket 平均，不是不同 bucket 宽度混算。当前值必须标明最新完整 bucket，不能使用未关闭 bucket 冒充完整数据。

### 9.6 境外 KPI 与 country/region 查询

境外专题只读 1m/1h aggregate 的最新 generation，不同步扫描 `flow_records`，也不增加第二张境外事实表。`category=overseas` 是数据面按事件时间 classification snapshot 生成的唯一境外权威；查询层不得再次用国家名称、ISO 字符串或当前港澳台设置重判历史。`geo_level` 首发只接受 `country/region`，分别固定读取 `geo.country/geo.region` rollup；Geo 名称和 breadcrumb 由结果行自己的 `geo_version` 在共享 `flowdimension.GeoCatalog` 中解析，不能跨版本按名称合并。

同一个 latest-generation 查询返回两类行：

- `kind=kpi`：对 `category=overseas` 的流量返回 `in/out/combined × ipv4/ipv6/unknown/all`。业务方向决定远端和本地端：入向的远端/本地分别是 `src_ip/dst_ip`，出向分别是 `dst_ip/src_ip`；复用现有 endpoint rollup，不新增冗余的 local/remote IP 高基数维度。查询同时聚合远端和本地两条镜像计数并校验 metric、received、unknown-sampling、quality 四组总数完全一致，不一致时 provider 整体失败，禁止只显示其中一侧。
- `kind=geo`：已分配稳定 Geo ID 且 `category=overseas` 的行进入 `geo_scope=overseas` TopN；所选 country/region 层为 `_unassigned` 的所有入/出向流量进入独立 `geo_scope=unknown_geo` 行，先判 unknown 再判 overseas。unknown 不进入境外 TopN/other，也不能因为地址看起来像公网地址而补猜境外。`other` 只合并当前版本内非 TopN 的已知境外 ID。

IPv4 在 aggregate endpoint 中仍是 `::ffff:a.b.c.d` 的 IPv4-mapped IPv6；family 只在这一规范表示上判定，并显式保留 `::`/异常为 `unknown`，绝不把 sentinel 计作 IPv6。每个 family 和 direction 组合还生成服务端精确的 `all/combined` 行：流量可加，唯一 IP 数必须由 `uniqExact` 对原始 endpoint ID 集合计算，客户端不得把入/出向去重数相加。

KPI 字段命名为 `observed_remote_ips/observed_local_hosts`：它们是在已接收 Flow 事实中的精确去重数量，不应用 sampling rate，也不声称等于未采样全网的真实唯一主机数。bytes/packets 按既有 raw/estimated metric 口径计算；每行同时返回 `received_records/unknown_sampling_records/quality_records` 和已知标志，页面必须把 observed 与 sampling completeness 一起展示。

请求继续只允许 `customer` view、闭桶 UTC 范围、固定 metric registry、TopN 1..100，以及 `in/out/business/target/device/exporter` typed filters；tenant 仅来自 authenticated scope。结果预算按每桶最多 `12 + 3 × (TopN + other + unknown)` 行预拒绝，CH 设置 15 秒、25 万结果、5,000 万扫描行、4 GiB 扫描字节和 4 GiB 查询内存硬限，且固定 `join_use_nulls=0` 让缺失的 endpoint 镜像产生 `endpoint_consistent=0`，而不是受集群默认值影响。结果保留 `dimension_snapshot_id/geo_version/classification_version`，返回 mixed-version 和 generation coverage；唯一 metadata sentinel、typed multi-block 校验及任一错误全有或全无的规则与 9.1 相同。真实 CH 对 endpoint 镜像、IPv4-mapped 输出、repair generation 和 TopN/unknown 守恒的执行证据是独立集成门禁，本地 SQL contract test 不能替代。

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
| `GET /flow/dimensions/geo/{version}/children` | view | 按 parent ID 分页返回直属子节点和 breadcrumb，拒绝跨版本引用 |
| `POST /flow/query` | view | 统一趋势/TopN/统计查询 |
| `POST /flow/records/search` | sensitive_view | 分页源/目的明细 |
| `GET/POST /flow/vpn/rules` | view/configure | 规则列表/创建 |
| `GET/PATCH/DELETE /flow/vpn/rules/{id}` | view/configure | 版本化修改/退役 |
| `POST /flow/vpn/rules/{id}/actions/preview` | configure | 候选量、成本、误报预览 |
| `GET /flow/vpn/findings` | vpn_view | 分页/筛选/搜索 |
| `GET /flow/vpn/findings/{id}` | vpn_view | 完整证据链 |
| `POST /flow/vpn/findings/{id}/actions/disposition` | vpn_triage | 人工处置，要求 If-Match |
| `POST /flow/vpn/findings/{id}/actions/probe` | probe | 创建受控异步 job |
| `POST /flow/reclass-jobs` | configure | 复用 operation job 创建回算 |
| `GET /flow/reclass-jobs/{id}` | view | 进度、范围、版本、校验 |
| `POST /flow/exports` | export | 创建异步 CSV/Parquet 导出 |
| `GET /flow/exports/{id}` | export | 状态、过期下载引用 |
| `GET /flow/health` | operate | 分层 readiness/lag/completeness |

输入错误 400，未认证 401，权限/范围 403，不存在 404，ETag/幂等冲突 409/412，限流 429，依赖不可用 503。响应必须区分 `complete/partial/unavailable`，包含 data watermark、版本和警告；依赖故障不能返回成功的空数组。

## 11. 六个界面

1. **Flow 总览**：总流量、上下行、六类卡片/占比/趋势、业务表、完整性。
2. **多维分析**：流量值/占比/差值，六类/Geo 层级/ISP/ASN/业务/地址段/地址组/端口，95th/峰值/平均；Geo 用面包屑 + 当前节点直属子级下钻，地址组单独显示“可重叠，不可相加”。
3. **源 IP 分析**：TopN、六类拆分、趋势、详情、修正入口。
4. **目的 IP 分析**：与源 IP 同契约，独立权限和导出。
5. **境外流量**：流入/流出、境外 IP、本地主机、地区/ASN/端口/协议。
6. **VPN 风险**：candidate/finding、score、证据、probe timeline、处置。

所有 VTable 都必须具备服务端分页、搜索、排序和 column filter；filter popover 使用 portal、collision detection、viewport max-height 和滚动，不得溢出或错位。Geo 表每行显示当前层名称、完整路径和稳定 ID tooltip，并可进入 children；图例只包含当前 level。地址组用多值 chips/独立 TopN，明确重叠口径。页面保留 query state 到 URL，支持取消过期请求；大数据只显示 TopN + other，不渲染无限序列。每张图支持创建/修改/复制/删除保存视图，保存的是 versioned QueryRequest，不保存 SQL。

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
| CH records | committed batch | 至少覆盖在线重分类 | TTL；删除前 rollup/备份验证 |
| 1m/1h rollup | 关闭迟到窗口后生成 | 产品查询窗口 | TTL，可由 base 重建的范围明确 |
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

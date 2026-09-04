# 流向分析模块详细设计

本文把 [flow-direction-requirements.md](flow-direction-requirements.md) 的六维分类和 F0–F9 落到 watchdog 当前代码栈。总体前提是先按 [watchdog-platform-module-architecture.md](watchdog-platform-module-architecture.md) 整理宿主的身份/RBAC、module/collector/target、统一查询、图表、导出和三层修正契约；flow 再作为模块注册业务资源、数据集、API、worker 和页面。生产数据面冻结为独立 `flow-collect`（UDP 接收 + GoFlow2 解码 + 采样归一 + 本地 raw WAL）→ Kafka normalized → dimension/aggregate worker → ClickHouse。VictoriaMetrics 继续保存 SNMP/宿主/pipeline 的低基数运行指标，不作为 Flow 业务事实的必经写入。地址段、address set、Geo/ASN、业务和六维统计全部异步，不进入采集成功路径。已有 SNMP 能力只是可选总量对账和计费输入，不是 flow 分类依赖。可执行工作项和完成证据统一在 [flow-module-tasklist.md](flow-module-tasklist.md) 跟踪。

核心边界：EdgeManager 只是离线地址库编辑/导出工具。watchdog 读取自包含导出文件，不连接、不依赖、不回写 EdgeManager 数据库。

## 0. 冻结基线与完成定义

本设计是编码基线，不允许实现阶段重新选择数据链路。部署规模和阈值可以按压测填写，以下结构性决定不得作为“优化”反复改动：

1. 只有两个数据面进程角色：`flow-collect` 与 `flow-dimension-worker`；GoFlow2 在 flow-collect 内运行；
2. 只有一个正式事实 topic：`watchdog.flow.normalized.v1`；原始 UDP 由本地 raw WAL 保护；
3. dimension worker 只提交一个 ClickHouse enriched base batch；地址段、endpoint、Geo、端口和低基数序列都是可重建派生；
4. 配置以不可变 snapshot/version 发布，事实按 event time 选版；raw、supplier、customer 三层值都不能覆盖 persisted raw；
5. Flow P1 上线前必须同时完成 N+1、容量、告警、runbook、备份恢复、安全和测试，不能把基础可靠性留给 P4。

每个实现工作包的 Definition of Done：

| 类别 | 必交付物 | 完成证据 |
|---|---|---|
| 数据 | protobuf/DDL/migration、ID/版本/TTL/不变量、备份与重建 | schema contract、空库/升级执行记录、守恒与重放报告 |
| 操作 | 配置、dashboard、alert、runbook、扩缩容、轮换、回滚 | 故障注入、N+1 切换、备份恢复和 on-call 演练 |
| 性能 | 输入容量表、单组件证书、端到端/soak/突发报告 | 2×持续 30 分钟、3×突发 5 分钟、72h soak |
| 功能 | API、权限、页面、导出、审计、错误/空态 | API contract、浏览器 E2E、查询/导出一致性 |
| 测试 | 单元、集成、变更设计、变更测试、回归、安全、性能、恢复 | CI 链接、fixture/pcap checksum、缺陷关闭记录 |

任何 schema、topic、分类或 API 变更必须先更新 ADR 和兼容矩阵，再实现双读/迁移/forward-fix；禁止上线后靠人工改表或修改历史事实补救。

## 1. 架构决策

### ADR-01：flow-collect 与 dimension worker 独立运行

将现有 `watchdog-sflow-collector` 演进为两个独立于 hub/API 的数据面角色：

- `watchdog-flow-collect` 负责 UDP 接收、来源准入、本地 raw WAL、GoFlow2 解码、模板/采样状态、exporter/观察点准入、采样归一和 normalized Kafka producer；
- `watchdog-flow-dimension-worker` 负责版本化地址段/address set、业务方向、Geo/ASN、六维、分钟汇聚和 sink；
- UDP/协议解码与统计维度、高基数存储和查询故障互相隔离；两个角色可按各自瓶颈扩容、绑核和滚动升级；
- 与当前 [`cmd/watchdog-sflow-collector`](../cmd/watchdog-sflow-collector/main.go) 部署形态一致。

`flow-collect` 禁止连接业务 MySQL、ClickHouse 和 VictoriaMetrics。它用平台 enrollment 取得 `collector_id`、租户/模块 plan、允许来源和 Kafka 凭据，已有有效 plan 时控制面短暂不可用不停止收数。GoFlow2 是进程内 adapter，不再部署第二个独立解码服务。

### ADR-02：Kafka-compatible MQ 是强制边界

Flow P1 即部署 MQ，不再把它推迟到 HA 阶段：

- `watchdog.flow.normalized.v1` 是唯一正式事实 topic，保存已解码、已采样归一但尚未做地址/Geo/业务归类的 versioned batch，支持 dimension worker 独立积压、扩容和重分类；
- 完整 UDP datagram 先进入 `flow-collect` 本地 append-only raw WAL；normalized batch 获得 Kafka ack 后才推进 WAL checkpoint，避免为了协议重放再建设一套 raw Kafka/独立解码服务；
- normalized 使用冻结的 4096 个 virtual shards：输入为 `tenant_id || 0x00 || src_ip_16 || dst_ip_16`（IPv4 使用 v4-mapped 16B），算法为 xxHash64 seed=0，`virtual_shard=hash & 4095`，Kafka key 为稳定 `normalized_batch_id`；物理 partition 不依赖客户端默认 hash，而由签名 plan 中版本化 `PartitionMap[virtual_shard]` 指定。扩分区发布只增不减的新 map 并从分钟边界生效，WAL 子批次固化原 `partition_map_version/physical_partition`，旧数据 replay 仍投原 partition；NetFlow/IPFIX 的 exporter/observation-domain 模板亲和性在进入 Kafka 前由 flow-collect listener/worker ownership 保证；
- producer 启用 idempotence、`acks=all`、压缩和重试；生产建议 replication factor 3、`min.insync.replicas=2`；
- raw WAL 有容量和期限上限，MQ 恢复后按接收顺序重新解码/发布；溢出必须生成明确 data-loss window；
- dimension consumer 采用 at-least-once：ClickHouse enriched base batch 成功并能由 `ingest_batch_id` 证明后才提交 normalized offset；派生 rollup 可从 base fact 回补。

topic、envelope、WAL、TLS/ACL、retention 和去重细节以宿主平台文档 §12 为准。不能以“单实例当前够快”为由让 flow-collect 直写分析存储，因为 MQ 解决的还包括重放、升级、背压和职责隔离。

### ADR-03：GoFlow2 v3 在 flow-collect 内接收和解码

使用仓库已有 `github.com/netsampler/goflow2/v3`：

- sFlow v5 复用现有 decoder；
- NetFlow v5/v9、IPFIX 使用 v3 collector/decoder 和模板缓存；
- 使用多 socket、固定 decode worker 和统一 `NormalizedRecordBatch`；不使用 stdout、JSON 或逐条 HTTP transport；
- 当前依赖固定在 [`go.mod`](../go.mod) 的精确 v3 commit；升级只通过协议 fixture、兼容和容量变更测试，不因名称“GoFlow2”误降到 v2 主版本。

Akvorado 用作入口、Kafka、classifier、network attribute、多分辨率 ClickHouse 的设计参考，不复制其 AGPL 实现。raw datagram 只保留在 flow-collect WAL 和受限 decode-DLQ；dimension worker 不维护 NetFlow/IPFIX template，从 normalized topic 重放即可重新归类。

### ADR-04：地址段与所有统计维度异步归类

地址段归类绝不放在 flow-collect 的接收/解码热路径：

- flow-collect 的成功边界是把确定性 `normalized_id=datagram_id/record_index` 写入 Kafka normalized topic 并推进本地 WAL checkpoint；
- dimension worker 按 record `event_time` 选择不可变 `dimension_snapshot`，而非消费时最新配置；
- 对 src/dst 和 local/remote 分别做最长前缀 `primary_prefix`，一个 role 内互斥，未命中 `_unassigned`，确保 prefix 汇总守恒；
- `address_set` 是 selector tag，可一条记录命中多个 set；不同 set 不可相加，查询元数据标记 `additive=false`；
- dimension/CH 失败只增加 normalized lag，offset 不提交即可重放，不占 flow-collect decode queue；Kafka 生产故障由本地 WAL 承接；
- 地址库或规则变更发布新 snapshot，历史重分类由异步 replay/backfill job 完成，不能阻塞在线采集。

### ADR-05：flow 通过宿主模块契约接入

flow module 采用 Go 编译期 registry，不使用脆弱的运行时 `.so`：

- 注册 `flow_exporter` 资源及其 tenant/target 父子关系；
- 注册 `flow.category`、`flow.address_dimension`、`flow.endpoint`、`flow.remote`、`flow.port`、`flow.vpn` datasets 和 CH provider；宿主现有 SNMP/system dataset 继续使用 VM provider；
- 注册 `/api/v1/modules/flow/*` 路由、dimension/rollup/评分 worker、migration、health check 和六个前端入口；
- 复用平台 collector enrollment/binding、target CRUD、QueryGateway、visualization、export、adjustment 和 audit，不复制用户/角色/权限表；
- 模块不得解析 PocketBase/OIDC token、信任客户端 tenant、暴露任意 SQL/PromQL 或让浏览器直连 CH/VM。

现有 `/api/v1/flow/*` 保留一个兼容版本并转发到模块 service；新功能只增加到 canonical 模块路由。

### ADR-06：四种数据基础设施各负其责

| 存储 | 角色 | 不承担 |
|---|---|---|
| Kafka-compatible MQ | normalized 事实可靠传输、短期保留、分区、有序重放 | UDP 原始包长期归档、长期分析查询、配置 CRUD |
| ClickHouse | 高基数分析事实源：分钟 pair、地址段/address set、endpoint、Geo、端口 | 接口计费真值、配置 CRUD |
| VictoriaMetrics | 现有 SNMP/系统指标和 collect/dimension/Kafka 自监控；可选的 Flow 低基数 recording cache | Flow 业务事实、per-IP、端口和机构明细 |
| MySQL | exporter、home profile、VPN 规则/结果；现有 CIDR 标签和审计 | flow 明细 |

VictoriaLogs 不再是任何部署 profile 的组成部分。解码诊断只允许写有大小/TTL 上限的本地 capture 或 Kafka DLQ，并且默认关闭；它们不是在线查询存储。

AS、IP 段、地域、运营商和六类结果完全由 flow 与离线地址库计算。已有计费场景的 SNMP 接口计数仍是计费真值，并可作为 flow 总量的独立对账基准；未部署 SNMP 时 flow 模块仍可运行，只是没有独立总量校验。sFlow counter sample 也可提供接口累计量作为同通道基准，但不能替代 SNMP 的独立性。任何 flow 估算或校准值都禁止覆盖账单原始 counter。

#### VM 能否替代 ClickHouse

**完整需求下不能。** VictoriaMetrics 的每个 metric name + label set 都形成独立 time series；IP、CIDR、ASN、端口、城市、机构和 address set 进入 label 后，活跃序列数和 churn 会随通信组合增长。VM 适合“已知、有限、预聚合”的时间序列，不适合作为任意明细行、任意维度组合和历史版本重算的事实库。VictoriaMetrics 官方也明确说明每个唯一 label value 会产生新序列，高基数会增加资源消耗；ClickHouse 的列式模型则面向 append-mostly、大范围聚合和高基数 `GROUP BY`。依据分别见 [VictoriaMetrics key concepts](https://docs.victoriametrics.com/victoriametrics/keyconcepts/) 与 [ClickHouse columnar workload guidance](https://clickhouse.com/resources/engineering/when-to-use-columnar-database)。

部署 profile 冻结如下：

| Profile | Flow 业务存储 | 能力边界 | 结论 |
|---|---|---|---|
| `full` | CH 承接全部 Flow 业务事实与查询；现有 VM 只保存 SNMP/宿主/pipeline 指标 | F1–F9、IP/地址段/ASN/Geo/端口、Top-N、历史重归类、明细导出、派生重建 | **本项目唯一 GA 基线；单一 Flow 事实源** |
| `full+recording-cache` | 在 `full` 上把少量六类/业务曲线从 CH 异步复制到 VM | 只优化已证明的高频总览；可随时删除并从 CH 重建 | 非基线；只有 CH 查询压测不达标且收益报告获批才启用 |
| `vm-only-lite` | 只保留预聚合六类/业务/方向曲线 | 不支持 per-IP、CIDR/地址段、ASN、Geo、端口、任意组合 Top-N、历史重归类和逐行导出 | 仅另立“精简产品”时可选，**不满足本文验收** |

Kafka 和 raw WAL 是可靠接收/解耦/重放边界，VM 无论是否扩容都不能替代它们。

默认不向 VM 写 Flow 业务序列。若 `full+recording-cache` 经压测批准，Flow label 白名单固定为 `tenant_id/device_id/business_id/category/direction`；pipeline 自监控可另用有界的 `collector_id/exporter_id/component/result`。禁止把 `src_ip/dst_ip/prefix_id/address_set_id/asn/country/province/city/organization/port/ingest_batch_id/snapshot_id/version/job_id` 写成 VM label。`business_id`、device/exporter 数必须来自受管资源且计入租户 cardinality budget；版本和完整性通过 checkpoint/查询 envelope 返回，不用滚动 version label 制造新序列。

Flow 序列预算按下面的上界在开通时预估，并用 VictoriaMetrics cardinality explorer/vmestimator 持续告警：

```text
flow_series_upper_bound = active_tenants
                        × active_devices_per_tenant
                        × approved_business_values
                        × category_values
                        × direction_values
                        × metric_names
```

基线下所有 Flow 业务查询都走 CH。可选 VM recording cache 只能服务 checkpoint 证明目标时间段 `complete`、版本与 CH active overlay 一致的六类/业务趋势；其他查询仍路由 CH。CH 不可用时，基线返回 `503 provider_unavailable`；启用缓存时仅上述 summary 可带 `degraded=true/details_available=false` 返回，明细、任意分组、重归类、精确导出仍返回 503。VM 丢失时从 CH base/derived 回填；CH base 丢失不能从 VM 反向恢复。

### ADR-07：离线 Geo 文件、原子热加载

EdgeManager 或其他工具把编辑完成的地址库导出为 `flow-geo-v1` 目录。watchdog 仅：

1. 读取 manifest；
2. 校验 schema、SHA-256、行数和区间；
3. 在旁路构建内存索引；
4. 一次原子指针切换；
5. 失败时继续使用旧版本并告警。

不存在运行时 Geo DSN、远程查询或回写 API。租户临时修正复用 watchdog `address_prefixes` 作为最高优先级覆盖。

### ADR-08：方向由本地网段决定，接口只限定观察点

业务上/下行按 src/dst 是否匹配 `labels.flow=local` 决定。`in_if/out_if` 用于 exporter 接口过滤、定位上联和对账，不直接替代业务方向。

P1 不实现通用跨设备去重。开通时必须用 exporter/interface allowlist 选择唯一计量观察点；检测到可能重复时告警而不是静默“去重”。

### ADR-09：VPN 采用异步多阶段证据链

VPN 主流程固定为 `flow candidate/score → probe job → evidence merge → finding verdict`：

- dimension/评分 worker 只使用已入 CH 的 src/dst 境内外、端口/protocol、可靠 duration、fanout、周期、多 transport、local↔remote bytes ratio、ASN/address-set 情报产生候选，不阻塞 normalized 消费；
- 旁路被动观察和主动握手探测都复用平台 CollectorRegistry/plan/job/result 契约，不新增同步 RPC 到 flow-collect，也不改变 CH base 提交条件；agent API 不可用时使用本地有界 result spool；
- `passive_observe` 只通过镜像/TAP 观察候选**后续**握手；候选产生前已经结束的握手不能重建，等待期无新连接则为 inconclusive。`active_handshake` 会主动连接目标，必须独立 action、审批、授权 scope、并发/速率/超时和 kill switch。二者在字段、页面和审计中绝不混称“被动探测”；
- flow-only 结果为 `heuristic`；DPI/指纹吻合但非唯一为 `corroborated`；只有协议特异、可复现握手响应才为 `confirmed_protocol`。confirmed 只确认协议存在，不确认恶意或违规；
- SOCKS/HTTP CONNECT/WebSocket 等可有明确握手；Trojan、Shadowsocks/SS、VMess 等加密/伪装协议通常无法无凭据通用确认，禁止爆破或发送利用 payload，只能结合被动指纹/时序/情报给 `corroborated/inconclusive`；
- nDPI 可作为 `passive_observe` analyzer，提供协议、TLS/QUIC metadata/fingerprint 和 flow risks；它不进入基础 collector，普通五元组也不能被事后还原成 DPI。

CN2/ASN/CIDR/机构/route class 是管理员维护的 versioned intelligence selector，不是内置价值判断。规则可 `score`、`probe_trigger`、`allow` 或 `suppress`，必须带 source/reason/owner/effective/expiry；allow/suppress 先于正向加分执行。

## 2. 组件与数据流

```text
设备 UDP ──▶ watchdog-flow-collect
               │ admit → raw WAL → GoFlow2 decode/template/sampling
               │ platform collector plan
               └── normalized protobuf batch ──▶ Kafka normalized topic
                                                        │ independent lag/replay
                                                        ▼
                                           watchdog-flow-dimension-worker
flow-geo-v1 文件 ──版本索引────────────────▶ direction/geo/classify
地址段/set snapshot ──event-time 选择───────▶ prefix/set/business/six-class
                                                        │ bounded async aggregate
                                                        ▼
                                             ClickHouse enriched base
                                                        │ rebuildable rollup
                                                        ▼
                                             ClickHouse derived/query
                                                        │
                                                        ▼
watchdog core ──module/resource/dataset registry──▶ flow module API/worker
身份/RBAC、collector/target、QueryGateway、visualization/export/adjustment
                                                        │
                                                        ▼
                                                   6 个前端页面

sFlow/SNMP/system 与 pipeline `/metrics` ─────────▶ 现有 VictoriaMetrics
sFlow counter samples ────────────────────────────▶ 可选同通道接口总量对账
SNMP counters（可选）─────────────────────────────▶ 独立总量对账 + 现有计费
```

目标包边界：

```text
cmd/watchdog-flow-collect/      UDP、admission、raw WAL、GoFlow2、sampling、normalized producer
cmd/watchdog-flow-dimension-worker/ normalized consumer、dimension snapshot、aggregate/sink
internal/platform/              module/resource/dataset/collector/query contracts
internal/modules/flow/
  wal/            raw datagram segment、group commit、checkpoint、replay
  envelope/       normalized protobuf batch、schema 与 partition key
  decoder/        GoFlow2 adapter、template/sampling state
  registry/       exporter + target + ifIndex 版本快照
  dimension/      snapshot loader、event-time version、prefix/set 异步归类
  prefix/         local/business/tenant Geo override LPM
  geo/            flow-geo-v1 loader + v4/v6 range index
  classify/       direction + 六维纯函数
  aggregate/      分片分钟 map、base batch sink、offset/dedup、rollup repair
  clickhouse/     module migrations、insert、query provider
  metrics/        pipeline Prometheus 指标；Flow 业务事实不直接写 VM
  api/            模块路由、DTO 与服务层
  module.go       descriptor/register
```

现有 [`sflow_collector.go`](../internal/watchdog/sflow_collector.go) 不在热路径上继续堆补丁。逐 flow VictoriaLogs 写入和浏览器 FlowSearch 按存储收敛 S0 先行禁用/删除，不等待新管线；旧 sFlow aggregate metric 只在 flow-collect→normalized→dimension worker 验收后退役，并保留一版兼容迁移说明。

## 3. 地址库导出与读取契约

### 3.1 `flow-geo-v1` 目录

推荐用版本目录 + `current` 符号链接原子发布：

```text
/var/lib/watchdog/flow-geo/
  2026-09-02T120000Z/
    manifest.json
    ipv4.csv.zst
    ipv6.csv.zst
    operators.json
    geo_dict.json
  current -> 2026-09-02T120000Z
```

`manifest.json`：

```json
{
  "schema": "flow-geo-v1",
  "version": "2026-09-02T120000Z-a13f9c2e",
  "generated_at": "2026-09-02T12:00:00Z",
  "admin_code_system": "GB/T2260-6",
  "unknown_country": "ZZ",
  "files": {
    "ipv4.csv.zst": {"sha256": "...", "rows": 123456},
    "ipv6.csv.zst": {"sha256": "...", "rows": 23456},
    "operators.json": {"sha256": "...", "rows": 30},
    "geo_dict.json": {"sha256": "...", "rows": 500}
  }
}
```

`ipv4.csv.zst` / `ipv6.csv.zst` 是已解析、非重叠、按 `ip_start` 升序的最终区间：

```csv
ip_start,ip_end,country,admin_code,subdivision,city,isp_id,asn
1.0.1.0,1.0.3.255,CN,350100,350000,福州市,3,4134
```

约束：

- 区间闭合 `[ip_start, ip_end]`、同 family 不重叠、起点严格递增；
- `country` 为 ISO 3166-1 alpha-2，未知为 `ZZ`；
- 中国 `admin_code` 为 6 位 GB/T 2260，允许空；省=`前2位+0000`，市=`前4位+00`；
- 境外可用 `subdivision/city` 展示，`admin_code` 为空；
- 港澳台必须规范化为 `country=CN` 且 `admin_code` 以 `71/81/82` 开头（GB/T 2260：`710000/810000/820000`）；导出器负责把上游 `HK/MO/TW` 国家码折算为该表示，同一版本内不得两种表示混存，否则 `overseas_includes_hmt` 无法确定性实现（分类函数仍做入口规范化以兼容旧导出）；
- `isp_id` 是导出物内稳定 UInt16，0=未知；
- ASN 为 UInt32，0=未知；
- `operators.json` 至少含 `{id,name,short_name,category,enabled}`；
- `geo_dict.json` 至少含 `{kind,code,name,parent_code,enabled}`；
- 文件可由 `.zst` 流式解压，watchdog 不把压缩文本整体读入内存。

### 3.2 与 EdgeManager 四类表的最小关系

这只是**导出器内部**的来源说明，watchdog 不访问这些表：

| 来源 | 导出器动作 | watchdog 动作 |
|---|---|---|
| `geo_base_v4/geo_base_v6` | 读取未删除 base 区间和 `admin_code` | 无数据库访问 |
| `geo_subnets` | 应用已审核 set/del、最具体覆盖优先，输出最终无重叠区间 | 无写回 |
| `isp_operators` | 把名称/映射固化为稳定 `isp_id`，导出字典 | 只读 `operators.json` |
| `geo_dict` | 导出省市/国家显示字典 | 只读 `geo_dict.json` |

watchdog 不关心导出器如何审核和发布，也不携带 EdgeManager 凭据。只要其他工具能生成同样的 `flow-geo-v1`，即可替换来源。

### 3.3 Loader 和内存索引

1. poll `current/manifest.json`，version 未变直接返回；
2. 校验 manifest 大小上限、schema 和所有 SHA-256；
3. 流式解析并验证 IP、区间、排序、重叠、字典外键；
4. IPv4 转 `uint32 start/end` 有序数组；IPv6 转两个 `uint64` 或 `uint128` 等价结构有序数组；
5. 各用二分查找，构建期间不阻塞旧索引；同时把版本对应的 operator/Geo 字典登记到只读 `GeoCatalog`；
6. 验证抽样 lookup 后 `atomic.Pointer[GeoIndex].Store(new)`；
7. 更新 `watchdog_flow_geo_version_info` 和 reload 指标。

加载失败不得把旧索引置空。初次启动无有效地址库时 flow-collect 仍可把 normalized records 写入 Kafka，但 dimension worker 必须暂停受影响的 tenant/partition 且不提交 offset，直到有效 `GeoIndex` 就绪；`/flow` 页面同时显示阻断级健康告警。只有显式发布、带版本号的“空 Geo snapshot”才允许把 Geo 字段归为 unknown，防止安装错误被静默固化。

### 3.4 watchdog 租户级修正

修正不改导出文件，复用现有 `address_prefixes`：

```json
{
  "cidr": "203.0.113.8/32",
  "source": "flow_geo_override",
  "labels": {
    "flow.geo.country": "CN",
    "flow.geo.admin_code": "330100",
    "flow.geo.isp_id": "3",
    "flow.geo.asn": "4134",
    "flow.geo.reason": "运营商确认"
  }
}
```

lookup 优先级：租户 override LPM > `flow-geo-v1` 区间 > unknown。若同 CIDR 已有 `flow=local` 或 `business` 标签，服务端必须 merge `flow.geo.*`，不能覆盖整张 labels。创建、修改或删除 override 时，同一服务事务必须递增 `flow_settings.classification_version` 并写审计；删除修正也只删除 `flow.geo.*`，剩余 labels 非空时保留行。

可提供“导出修正清单”CSV 给地址库维护人员离线导入，但主流程不依赖此动作。

## 4. 管理流程

### 4.1 开通向导

1. **宿主与依赖检查**：生产身份/API、flow module、MySQL、Kafka topic/TLS/ACL、CH、VM、Geo 文件、UDP 端口、时钟和 flow-collect WAL 磁盘水位；
2. **collector 注册**：平台创建 `module_key=flow` 的 collector，生成只显示一次的 enrollment secret；agent exchange 后取得长期 token/mTLS、collector ID 和签名 plan；
3. **collector binding**：把 collector 绑定到一个或多个 target/exporter 范围；plan 只包含该实例应接收的 listener、source allowlist、tenant/module identity、Kafka topic/principal 和配置版本；
4. **设备导出**：按 vendor 显示示例，但最终配置以具体型号/固件手册为准；
5. **exporter 绑定**：未知来源只生成限速 quarantine metadata，不入分析；管理员绑定 target、collector、协议、observation domain、接口 allowlist、计数语义和按 data source/sampler 的采样 override；
6. **本地/业务网段**：编辑 `address_prefixes/address_sets` draft，运行重叠、空洞、selector 和预计 row-expansion 检查；发布不可变 dimension snapshot，并等待 dimension workers 全部 ack；
7. **Home profile**：自动识别候选，管理员确认本省、本市和一个或多个本运营商；
8. **校验**：观察至少 30 分钟，确认 Kafka lag/DLQ、unknown、采样质量和重复观察；若设备输出 sFlow counter sample 或已配置 SNMP，再确认同范围 counter 偏差。没有 counter 基准不阻塞 flow 分析开通，但向导必须标记“未配置独立总量对账”。

### 4.2 自动识别

- 读取近 24h 的本地端流量；
- 对本地 IP 做 Geo lookup，按 `estimated_bytes` 加权；
- 生成省、市、ISP Top 候选及 share，保存 `flow_settings.auto_detect_json`；
- ISP share ≥15% 默认勾选，但不自动应用；
- 点击“应用”才更新 `home_*` 并递增 `classification_version`；
- 自动识别使用的数据范围、Geo version、unknown share 必须回显。

自动识别只能辅助配置，不能把动态流量众数当地址权威。例如 CDN/代理流量可能使对端众数与本地归属无关。

### 4.3 修正归属地

1. IP 行点击“修正归属地”，`GET /flow/geo/lookup?ip=` 回显文件结果和已有租户 override；
2. 填 CIDR（默认该 IP `/32` 或 `/128`）、国家、省市、机构、ASN 和原因；
3. 服务端验证 CIDR 范围、字典值和权限，事务性 merge 写入 `address_prefixes`、递增 `classification_version` 并追加 `audit_logs`；
4. 发布新的 dimension snapshot；dimension worker 旁路加载并校验后 ack，按 `effective_from` 和 record event time 选择版本，目标 60 秒内就绪；
5. 历史聚合不自动改写；P4 可按 Geo/classification version 发起回算。

### 4.4 Collector 与 exporter 状态机

平台 collector 生命周期：

```text
pending ──enroll──▶ active ↔ suspended ──revoke──▶ revoked
                      ↘ stale（派生健康态，凭据状态仍 active）
```

- collector 是独立服务身份，凭据、heartbeat、plan version、软件版本和 WAL/MQ 健康由平台管理；
- binding 是 M:N 关系，解绑 exporter/target 不等于吊销整个 collector；
- revoke 后不再下发 plan 且 Kafka principal 应同步撤销；stale 只表示超出 heartbeat 阈值；
- collector 的接入 API、表结构和权限按宿主平台文档 §6 实现，不在 flow 模块重复建表。

flow exporter 生命周期：

```text
desired status: unknown packet → pending → active ↔ suspended → retired → deleted
observed health:                    warming → healthy ↔ degraded ↔ stale
```

- pending：记录限速 source/agent/protocol/observation domain/last seen metadata，完整 payload 不进入分析 topic；
- active：必须有 tenant、collector binding、target 和有效 observation policy；NetFlow/IPFIX 的 `counter_mode` 不能为 `auto`；
- suspended：管理员显式停用，报文丢弃并计数；`retired` 停止新数据但保留配置和历史引用；`deleted` 是可恢复期内 tombstone，purge 才物理销毁；
- warming/healthy/degraded/stale 是独立 `observed_health`，不改变 desired status。warming 表示仍等模板/sampler/首个完整窗口；degraded 表示 sequence gap、模板、完整率或 sink 问题；stale 为 `now-last_seen > 3 × expected_datagram_interval_seconds`，恢复收包自动转 healthy；
- exporter identity 变更不自动迁移模板或计数。

### 4.5 VPN 情报、规则与探测操作流程

1. 安全管理员从现有 address set/CIDR 管理导入或维护 ASN/机构/route-class 情报，填写 source、reason、owner、effective/expiry；系统生成 intelligence version。CN2/ASN 只是 selector，必须由管理员明确选择 `score/allow/suppress/probe_trigger`；
2. 创建 draft rule，使用结构化 builder 组合 src/dst 境内外、方向、端口/transport、行为和情报；preview 在冻结窗口执行，展示候选规模、预计 probe 数、Top prefix/ASN、成本和脱敏样本，不写 finding；
3. 规则审批并 activate，生成 immutable rule-set version。评分 worker 只处理关闭迟到窗口的数据，先应用 allow/suppress，再按 family cap 评分并 upsert heuristic finding；
4. probe_trigger 达标时创建幂等 job。passive plan 等待后续匹配握手；active plan 只有 `probe_active` 权限、批准 scope 且全局 kill switch 解除时才可领取；
5. agent 提交签名、限长证据，服务端验证 job/scope/analyzer/version 后合并 verdict。结果冲突保留各 attempt，不使用 last-write-wins 覆盖；
6. 具备 `dispose_finding` 的用户可 acknowledge、标记 investigating/authorized/false-positive/resolved、添加不改变 evidence 的 annotation，或从 false-positive 样本创建新的 suppress rule；不能直接编辑 score/verdict。规则/情报到期只影响新 finding，历史 finding 保留当时版本直至 TTL。

## 5. 数据面 7 步流程

### 步骤 1：flow-collect 接收、准入并写本地 raw WAL

- sFlow `:6343/udp`，NetFlow/IPFIX `:2055/udp`；`SO_REUSEPORT`/socket count、OS receive buffer、固定 reader/decode worker 和有界队列，禁止每包 goroutine；
- 依据签名 plan 做 listener/source/tenant 准入，不从报文接受 tenant；未知来源只产生无 tenant、无 raw payload 的 quarantine protobuf metadata，不把未归属 payload 混入 WAL/正式 topic。quarantine 使用固定异步队列、每 source/全局每秒双限流；Kafka 慢或不可用不得反压 UDP/WAL 主链，队列满、限流和发布失败分别计数；
- datagram 封装 `datagram_id/tenant/collector/boot/protocol/recv_ts/source/domain_hint/payload/crc32`，批量追加本地 segment WAL；`datagram_id=hash(collector_id,boot_id,wal_segment_sequence,record_offset,payload_crc32)`，在 append 时生成并随 WAL 持久化，replay/进程重启直接复用，绝不按重放时间重新生成；
- UDP reader 只负责准入和 append，不直接投递 decode queue；单一增量 WAL cursor 是唯一 decode dispatch source。这样 live datagram 也先越过 durable barrier，避免 live/replay 双入口造成同一 exporter 的模板与 data set 乱序；
- WAL 采用单写者 append、group commit、segment checksum、启动扫描和已确认 watermark。默认 `fsync_interval=10ms`；主机掉电 RPO 上限等于该间隔，进程崩溃不得丢已 fsync segment；
- 磁盘高水位先告警并停止非必要诊断；达到 hard limit 后按 plan 停止 socket/显式记录 data-loss interval，禁止静默覆盖未确认 segment。

### 步骤 2：同进程 GoFlow2 解码、模板与质量状态

flow-collect 将 WAL record 以 `hash(protocol, UDP transport source IP, observation_domain_id)` 固定分派到 decode worker；同一 exporter/domain 只有一个 owner queue，模板、sampler options 和 data set 不跨 worker 竞态。GoFlow2 v3 解码四类协议；sFlow 保留 `subAgentId/sequence/sourceId/sampleSequence/samplingRate/samplePool/drops`，NetFlow/IPFIX 使用显式注入的 template store 与 sampling-rate store，状态严格按 exporter/domain/sampler 隔离。template-pending 可以释放当前 record，让 WAL 中后到的模板先建立状态，再启动下一轮重放。确定性的 decode/normalize 拒绝按 `retry_initial` 指数退避至 `retry_max`，达到 `decode_max_attempts` 后发布稳定 ID 的受限 decode-DLQ；只有 Kafka `acks=all` 后才确认原 WAL record。plan history 缺失、状态/Kafka/WAL 失败属于基础设施或控制面失败，禁止伪装成坏包进入 DLQ，继续背压/重试。由此既不会让毒包永久卡住 affinity worker，也不会因 Kafka 故障误丢事实。

内存 exporter registry 绑定 tenant/target/device，验证唯一 observation interface 和 allowlist，记录 sequence gap、restart、template wait、decode error、drops 和 quality flags。pending/suspended/retired/deleted、health=warming、无模板或无有效 sampling 语义的记录不伪造统计值：可恢复的 template wait 留在 WAL 等待重放，不可恢复坏包进入受限 decode-DLQ。

GoFlow2 状态变化必须先形成 collect-state 安全点，顺序冻结为：

```text
raw WAL durable
  -> GoFlow2 decode 更新 template/sampling store
  -> 只快照当前 exporter/domain
  -> 本地 .state 临时文件 + file fsync + atomic rename + directory fsync
  -> Kafka compacted collect-state 按 state_key 发布并等待 acks=all
  -> ACK2 child 0
  -> 发布 normalized data children 并分别 ACK2
```

CollectState v2 把“业务身份”和“写入 fencing”拆开，禁止把可变的当前 owner 混入业务身份：

```text
state_identity_key = SHA-256(length-prefix(
  "watchdog.flow.collect-state.identity.v2",
  tenant_id, exporter_id, protocol, source_ip_16, observation_domain_id))
state_key          = SHA-256(length-prefix(
  "watchdog.flow.collect-state.partition.v2",
  state_identity_key, ownership_epoch))
state_id           = SHA-256(datagram_id, state_key)
restore_order      = (ownership_epoch, state_generation)
```

`state_identity_key` 跨 collector ownership 稳定；`state_key` 是 Kafka compacted key，并包含由控制面按 exporter binding 单调递增的 `ownership_epoch`。因此失联旧 owner 的迟到写只会更新旧 epoch key，不能把新 owner 的状态从 compacted topic 中覆盖掉；D3B 在恢复窗口结束后才可对旧 epoch 发 tombstone。plan payload schema v2 必须逐 source 明确给出正数 `ownership_epoch`；schema v1 仅为滚动兼容而隐式视为 epoch 1，不允许跨 collector 恢复。相同 epoch 出现不同 `collector_id` 视为控制面 split-brain 并 fail closed，不能用 Kafka “最后写入者获胜”掩盖。

`state_generation` 来自同一 exporter/domain decoder store 的单调 revision；恢复模板/option sampler 时一并恢复 revision，后续 checkpoint 必须继续递增。恢复端只接收当前签名 plan 能准入且 `tenant_id/exporter_id` 精确匹配的状态，拒绝高于当前 plan 的 epoch；低于当前 epoch 的状态可作为新 owner 冷启动基线。同一 identity 选择最大 `(ownership_epoch,state_generation)`；同一二元组如 payload digest 不同即判定 split-brain/corruption，Kafka partition/offset 只用于审计，绝不作为消解冲突的正确性依据。

本地 frame 使用 magic/length/CRC32，protobuf 内再保存 payload SHA-256；任何损坏、身份错配、文件名/key 不一致均启动失败，不静默回退为空状态。checkpoint 只承载 GoFlow2 的有界 JSON snapshot，属于低频控制状态，不是 per-flow 业务 JSON；超过可配置 `decoder_state_ttl`（默认 30 分钟）不恢复，以免把过期模板应用到新 exporter session。CollectState v1 本地文件只允许原 `collector_id` 恢复，下一次状态变化自动写 v2。若一个报文先更新模板再发生 decode/normalize 拒绝，状态安全点仍须先于 DLQ，不能因回收毒包丢失后续数据依赖的模板。

D3B 启动顺序冻结为：加载并验证 active/LKG plan 与 history（允许新 active 暂时形成 `max+1`）→ 打开并恢复 WAL、扫描 pending `registry_version`、保留 active 与所有 WAL 引用并回收其余 history → 捕获 collect-state 各 partition high watermark → 扫描到该一致边界并按上述规则选择 → 与本地 checkpoint 合并并恢复 decoder revision/template/sampler → 仅从仍 pending 的 WAL ID 恢复 attempt/quality metadata → 最后开放 UDP listener/WAL dispatch。这里“打开 WAL”只做本地恢复和加锁，不启动 listener/dispatch。滚动升级先 drain listener 并发布安全点；新 owner 必须完成该恢复闸门，不能先收包再依赖进程内缓存。

D3B1 已把 Kafka 回读接入该前置闸门：启动时先冻结每个 partition 的 `[log_start_offset, high_watermark)`，各 partition 并行读取且只接受边界内严格递增的消息；protobuf value 上限、Kafka key/`state_key` 一致性、跨 partition 重复 key、payload SHA/identity、当前签名 registry 与 epoch 全部校验，tombstone 删除同 key 候选。读取有全局 `collect_state_restore_timeout`，内存只保留当前 plan 可接收的最新 key，且受 `collect_state_restore_max_candidates` 硬上限保护。Kafka 边界读完后再次校验 plan 尚未过期，再把 Kafka 与本地候选在写入 decoder **之前一次性**按 `(ownership_epoch,state_generation)` 合并，避免“先恢复本地新状态、再被远端旧状态覆盖”；成功后才打开 WAL/quality/publisher/runner/listener。恢复数量与耗时由低基数 gauge 暴露。

D3B2A 已持久化签名 plan history：首次接受的 revision 以 `fsync(file) → rename → fsync(directory)` 原子安装，revision/payload 不可变、collector identity 不可变且拒绝 revision 回退；active 文件损坏、缺失或不可用时，只能选择 history 中“签名有效且当前仍在有效期”的最高 revision 作为 LKG。历史 plan 可以过期但仍保留，旧 WAL 必须按 record 上的 `registry_version` 精确解析当时的 source binding、采样规则和 `partition_map_version/physical_partition`，并校验 record 的 `received_at` 落在该 plan 有效期内，绝不套用当前 plan。编译后的 registry 深拷贝签名 payload 中的 slice/map/pointer，调用方不能通过返回值改变内存中的签名事实。history 默认最多 128 个 revision且至少需要 2 个槽位；新 active 可先原子落盘形成一个受控的 `max+1` 启动窗口，随后扫描 durable WAL，只保留 active、历史最高 revision（防回退水位）和所有 pending record 引用的 revision并 `fsync(directory)`。引用缺失或实际必需 revision 超过上限时启动失败，绝不猜测删除。

collect-state 恢复先服从当前 plan 的 ownership fence；仅当 active plan 已完全不再准入该 exporter/domain 元组时，才允许同一 collector 使用 state 自带 `registry_version` 对应的历史 plan 恢复旧 WAL 所需模板。若 active plan 已把相同元组改绑给其他 tenant/exporter，则历史状态拒绝进入共享 decoder，避免跨租户/跨出口模板污染。跨 collector 接管仍只能由当前 plan 中严格递增的 `ownership_epoch` 授权，不能借历史 plan 绕过。

D3B2B 使用独立、定长、有界的 attempt journal 保存失败路径，避免给正常高吞吐流量增加逐报文状态写放大。首次处理的 `replay_generation=0` 是隐式值；某次处理返回失败后，必须先追加 `(datagram_id,next_generation)`，等 group-fsync durability barrier 完成，才允许按该 generation 重试或发布 DLQ。文件头绑定 `collector_id` 的 SHA-256，record 带 magic/version/CRC32；同 ID generation 只能逐一递增，完整损坏 fail closed，仅尾部 torn-write 可截断。启动恢复只保留 durable WAL 中未终态确认的 ID；运行时即使 Kafka 已成功，也不立即删除内存 generation，周期 checkpoint 会先同步 WAL/ACK，再仅回收已 durable terminal ACK 的 ID并原子重写 journal，从而避免“ACK 尚未落盘而 attempt 已删除”的崩溃窗口。默认 group-fsync 10ms、checkpoint 5m、上限 256MiB；满时先压缩，真实 pending 集合仍放不下则拒绝推进重试并使 readiness fail closed。journal bytes/tracked/appends/failures/restores/checkpoints 与 plan history entries/pruned 均为低基数指标。

D3B3A 已把 Kafka topic 从部署约定提升为启动硬契约。签名 plan/history 完成 WAL 引用回收后、读取 collect-state 之前，flow-collect 使用关闭 auto-create 的只读 admin client 校验四个 topic：存在且名称不复用、partition 从 0 连续、leader 可用、无 offline replica、每 partition 的 RF/ISR 达标、`cleanup.policy` 精确、有效 `min.insync.replicas/max.message.bytes/retention.ms/delete.retention.ms/unclean.leader.election.enable` 达标；缺少 `DESCRIBE_CONFIGS` ACL 或任一值不安全均拒绝开放 UDP。normalized 的 partition 下限取配置值与**所有保留 plan** 中最大 physical partition 加一的较大者，旧 WAL 不会因扩分区失去原 partition；collect-state 因当前 keyed hash 会在扩分区后把同一 key 漂移到不同 partition，所以 partition 数必须精确等于配置且服务期内不可原地扩容，扩容只能新建版本 topic并做有界迁移。所有 topic 禁止 unclean leader election；normalized/DLQ/quarantine 必须是 `delete`，collect-state 必须是仅 `compact`，禁止叠加会按普通 retention 删除仍有效模板状态的 `delete`。producer 的 message 上限同步提高到 collect-state 本地硬上限，避免 broker 配够而客户端仍按 Sarama 默认约 1MB 拒绝。

TLS 使用 TLS 1.2 下限；未配置 `tls_ca_file` 时使用系统 trust store，配置后则只信任该私有 CA 文件，并可用成对的 `tls_cert_file/tls_key_file` 提供每 collector 独立 mTLS 身份；不允许 `InsecureSkipVerify`，`tls_server_name` 只用于显式 SNI/主机名校验。runtime principal 只授予四 topic 的 `DESCRIBE`、`DESCRIBE_CONFIGS`、`WRITE`，collect-state 的 `READ`，以及 idempotent producer 所需的 cluster `IDEMPOTENT_WRITE`；它没有 `CREATE/ALTER/DELETE/CREATE_ACLS`。topic 必须由独立 IaC/admin principal 预创建，采集器不会因拼错名称偷偷建出默认单副本 topic。对 WRITE/READ 的最终授权仍由 collect-state 启动读取及每 topic 首次真实 publish 的 broker ack 验证，`DescribeAcls` 不授予 runtime principal。

旧 epoch tombstone 不是 owner 自己的“顺手清理”，而是 watchdog 内 reconciler 的带审计状态机：`revoke_old_plan_and_unique_principal → wait old plan expiry + clock skew and ACL propagation → new owner restore → observe new epoch state at a frozen Kafka high-watermark and require generation advance → tombstone exact old state_key → verify tombstone at a later frozen boundary`。只有同时满足旧 owner 已 drain/失去 WRITE、新 plan 的 epoch 严格增加、replacement identity 相同且新 key 已 Kafka durable，才能清理；超时保持旧 key而不影响正确性。回滚也必须再分配更高 epoch，绝不复用已 tombstone 的 epoch。tombstone 由 reconciler 专用 principal 写入，flow-collect runtime API 不暴露任意 key 删除；`delete.retention.ms` 至少覆盖最大恢复/消费者中断窗口，并记录 old/new collector、identity、epoch、generation、partition/offset 和审批审计。

当前仍未完成的是 D3B3B 的 IaC 实际创建、reconciler tombstone 编码，以及 template/options corpus、真实多 broker/ACL/TLS 故障、进程重启、kill -9 和滚动 owner 切换故障注入；这些完成前不得宣称跨节点闭环。

### 步骤 3：采样归一、批量发布并推进 WAL

```text
source_key = (agent_ip, sub_agent_id, observation_domain_id, source_type, source_id, if_index)
effective_rate = most_specific_sampling_rule[source_key] ?? record.sampling_rate
sampled:    estimated_bytes = checked_mul(record.bytes, effective_rate)
            estimated_packets = checked_mul(record.packets, effective_rate)
pre_scaled: estimated_bytes = record.bytes
            estimated_packets = record.packets
```

`sflow5` 逐 sample 使用自带 rate；rate 缺失/零且无精确 source override 时拒绝，绝不默认为 1；pre-scaled 不再乘倍率。flow-collect 维护 sample epoch/pool 质量，但不静默覆盖 nominal raw。

每条解码记录产生确定性 `normalized_id=datagram_id/record_index`，包含 event/receive time、tenant/collector/exporter/target/device、observation、src/dst IP/port/ASN、protocol、原始计数字段、sampling mode/rate、estimated bytes/packets、duration、sFlow source/sample 状态、quality 和 registry version，**不包含地址段、address set、Geo、业务或六维结果**。同一 datagram 的 records 按冻结的 `virtual_shard` 分组；每个非空 shard 形成确定性子批次 `normalized_batch_id=SHA-256(datagram_id,virtual_shard,chunk_index)`，一个子批次不能再跨 Kafka partition。WAL confirmed record 保存全部子批次的 ack bitmap，只有全部确认后 datagram 才可回收；重启只重发未确认子批次。

```text
batch_schema_version, normalized_batch_id, datagram_id, virtual_shard,
partition_map_version, physical_partition, replay_generation,
tenant_id, collector_id, exporter_id, registry_version,
received_at, protocol, source_ip, observation_domain_id,
sub_agent_id, datagram_sequence, agent_ip, exporter_epoch,
records[] {
  record_index, normalized_id, event_time,
  target_id, device_id, observation_if_index, observation_direction, in_if, out_if,
  src_ip, dst_ip, src_port, dst_port, ip_proto, tcp_flags,
  raw_bytes, raw_packets, sampling_mode, sampling_rate,
  estimated_bytes, estimated_packets, flow_duration_ms, quality_flags,
  src_as, dst_as, source_id_type, source_id_value,
  sample_sequence, sample_pool, exporter_drops, sample_index, quality_epoch
}
```

上述结构冻结为 `NormalizedRecordBatch` protobuf v1 的字段号基线（只追加、不复用、不改语义；`normalized_id` 由 `datagram_id/record_index` 推导，不单独占字段）：

```proto
syntax = "proto3";
package watchdog.flow.v1;

message NormalizedRecordBatch {
  uint32 batch_schema_version   = 1;  // 当前 = 1
  bytes  normalized_batch_id    = 2;  // 32B hash
  bytes  datagram_id            = 3;  // 32B hash
  uint32 virtual_shard          = 4;  // 0..4095
  uint32 partition_map_version  = 5;
  uint32 physical_partition     = 6;
  uint32 replay_generation      = 7;
  string tenant_id              = 8;
  string collector_id           = 9;
  string exporter_id            = 10;
  uint64 registry_version       = 11;
  int64  received_at_unix_ms    = 12;
  uint32 protocol               = 13; // 1=sflow5 2=netflow5 3=netflow9 4=ipfix
  bytes  source_ip              = 14; // 16B，IPv4 用 v4-mapped
  uint64 observation_domain_id  = 15;
  repeated NormalizedRecord records = 16;
  reserved 17 to 31;
  uint32 sub_agent_id          = 32;
  uint32 datagram_sequence     = 33;
  bytes  agent_ip              = 34; // sFlow header agent address；非 sFlow 为空
  uint64 exporter_epoch        = 35; // collector 观察到的 exporter session epoch
}

message NormalizedRecord {
  uint32 record_index           = 1;
  int64  event_time_unix_ms     = 2;
  string target_id              = 3;
  string device_id              = 4;
  uint32 observation_if_index   = 5;
  uint32 observation_direction  = 6;  // 0=unknown 1=ingress 2=egress
  uint32 in_if                  = 7;
  uint32 out_if                 = 8;
  bytes  src_ip                 = 9;  // 16B
  bytes  dst_ip                 = 10; // 16B
  uint32 src_port               = 11;
  uint32 dst_port               = 12;
  uint32 ip_proto               = 13;
  uint32 tcp_flags              = 14;
  uint64 raw_bytes              = 15;
  uint64 raw_packets            = 16;
  uint32 sampling_mode          = 17; // 1=sampled 2=pre_scaled
  uint64 sampling_rate          = 18;
  uint64 estimated_bytes        = 19;
  uint64 estimated_packets      = 20;
  uint64 flow_duration_ms       = 21;
  uint64 quality_flags          = 22; // 位集，注册表见下
  reserved 23 to 31;
  uint32 src_as                 = 32;
  uint32 dst_as                 = 33;
  uint32 source_id_type         = 34;
  uint32 source_id_value        = 35;
  uint32 sample_sequence        = 36;
  uint64 sample_pool            = 37;
  uint64 exporter_drops         = 38;
  uint32 sample_index           = 39; // datagram 内 sample 序号；同 sample 多 record 相同
  uint64 quality_epoch          = 40; // high32=exporter epoch，low32=source/sampler epoch
}

message CollectState {
  uint32 state_schema_version  = 1;  // 当前 = 1
  bytes  state_id              = 2;  // 32B，datagram/state key 的稳定 hash
  bytes  state_key             = 3;  // 32B，compacted topic key
  bytes  datagram_id           = 4;  // 32B
  string tenant_id             = 5;
  string collector_id          = 6;
  string exporter_id           = 7;
  uint64 registry_version      = 8;
  int64  received_at_unix_ms   = 9;
  uint32 protocol              = 10; // 仅 netflow9/ipfix
  bytes  source_ip             = 11; // 16B，UDP transport source
  uint64 observation_domain_id = 12;
  bytes  templates_json        = 13; // GoFlow2 snapshot，非业务记录
  bytes  sampling_rates_json   = 14; // GoFlow2 snapshot，非业务记录
  bytes  payload_sha256        = 15; // 除本字段外的确定性 protobuf 摘要
}

message DecodeFailure {
  uint32 failure_schema_version = 1;
  bytes  event_id               = 2;  // hash(datagram_id,error_code)，重试稳定
  bytes  datagram_id            = 3;
  string tenant_id              = 4;
  string collector_id           = 5;
  string exporter_id            = 6;
  uint64 registry_version       = 7;
  int64  received_at_unix_ms    = 8;
  uint32 protocol               = 9;
  bytes  source_ip              = 10;
  uint64 observation_domain_id  = 11;
  string error_code             = 12; // 稳定枚举，不按 message 分支
  string error_summary          = 13; // 去控制字符，最多 512 rune
  uint32 attempts               = 14;
  bytes  payload_sha256         = 15;
  bytes  payload                = 16; // 默认空；仅受限 ACL/TTL 下显式开启
  bool   payload_truncated      = 17;
}

message QuarantineEvent {
  uint32 event_schema_version   = 1;
  bytes  event_id               = 2;
  string collector_id           = 3;
  int64  received_at_unix_ms    = 4;
  uint32 protocol               = 5;
  bytes  source_ip              = 6;
  uint64 observation_domain_id  = 7;
  string reason_code            = 8;  // FLOW_EXPORTER_UNKNOWN
  uint32 payload_bytes          = 9;
  bytes  payload_sha256         = 10; // 仅摘要，绝不带 raw payload
}
```

字段 32 之后用于补齐 GoFlow2 已能提供、且做 ASN/采样质量分析不可缺失的事实。`agent_ip/sub_agent_id/datagram_sequence/exporter_epoch` 属于 datagram，其中 `source_ip` 始终是 UDP transport source，不能拿 relay 地址覆盖 sFlow header 的 `agent_ip`；`source_id/sample_sequence/sample_pool/drops/sample_index/quality_epoch` 属于单个 sFlow sample 或 record，因此必须放在 record，不能错误提升为 batch。`sample_index` 防止同一 sFlow sample 解出多个 flow record 时重复推进 sample sequence；字段号只追加，不回填 17–31 的预留区。

`quality_flags` 位注册表随 schema 版本冻结、只增不改：bit0 `template_recently_learned`、bit1 `sampling_rate_overridden`、bit2 `duration_unreliable`、bit3 `sequence_gap_window`、bit4 `sample_pool_reset_window`、bit5 `clock_skew_suspected`、bit6 `truncated_header`、bit7 `sampling_rate_change_window`、bit8 `exporter_restart_window`、bit9 `sequence_out_of_order_window`、bit10 `quality_state_saturated`。新增位必须先登记语义与消费方处理方式；消费端忽略未注册位但原样保留。所有质量位只注释事实，不得修改 raw/nominal estimated counters。CI 用旧 fixture 解析新 writer 输出验证前向兼容。

sequence unit 按协议冻结：sFlow datagram 和每 source sample 均 `+1`；NetFlow v5 按本报文 flow-record count；NetFlow v9 按 export packet `+1`；IPFIX 按 Data Record count（包含 Options Data Record）。NetFlow v5 用 engine type/id、v9/IPFIX 用 observation domain、sFlow 用 agent/sub-agent/source 隔离。只有带 uptime 的协议且 uptime 明确回退、同时排除 uint32 自然回绕时才建立 restart epoch；IPFIX sequence 回退只标乱序，不猜测重启。gap/rate-change/pool-reset/restart/out-of-order 在可配置窗口内投射到后续 record。

进程内 tracker 在 downstream 失败后按 datagram ID 缓存首次判定直到 Kafka/WAL terminal ack，正常成功路径不分配 retry cache；publish/normalize retry 复用该判定，因此不会把自身重试计作乱序。tracker 使用 `quality.state_ttl/max_exporters/max_data_sources` 硬边界；容量耗尽不丢 flow，而置 bit10 并增加低基数计数。

E2B1 的单 owner 持久化顺序固定为 `raw WAL durable → Observe → quality journal append → normalized/state Kafka publish → quality journal durability barrier → terminal WAL ACK`。quality journal 每个 **datagram** 追加一次 exporter/source 的绝对 post-state 和该 datagram 的 quality decision，不按 flow record 写、不写 Kafka；同一 datagram ID 的追加幂等，重试只等待原 durability target。journal frame 为 `magic + uint32 length + CRC32 + protobuf`，protobuf 再含 SHA-256、原 WAL segment/offset 和 schema version。Kafka 等待时间与 journal group-fsync 并行隐藏本地落盘延迟；journal 尚未 durable 时允许发送稳定 ID 的 Kafka 消息，但绝不允许写入 terminal WAL ACK，崩溃最多造成可去重的重放而不会丢状态。append 失败、durability barrier 失败或达到硬上限时，raw WAL 仍未确认，不能跳过质量状态继续确认数据。

周期 checkpoint 通过进程内 RW barrier 取得一致切面：先同步 WAL/ACK 和 quality journal，再把 exporter/source 状态及仅对应“尚未 durable ACK”的 retry decision 原子写入 `quality.snapshot`（临时文件 fsync、rename、目录 fsync），最后才 truncate/fsync journal。启动先扫描 raw WAL 的 durable recovery view，并严格按 snapshot→journal 恢复；只有“WAL 中仍存在且没有 durable terminal ACK”的 ID 才恢复 retry decision，查不到的 ID 不能一律当未确认，因为其 segment 可能已成功回收；已确认记录的 sequence state 仍保留。仅不完整的尾 frame 可截断，完整 frame 的 magic/CRC/SHA/collector/schema/容量任一不符均 fail closed。这样 kill/restart 后本机 epoch 连续，同时不产生 per-datagram Kafka 同步写放大。`journal_fsync_interval` 是并行 worker 的本地 group commit 窗口，`checkpoint_interval` 控制压缩频率，`journal_max_bytes` 是保护磁盘和恢复时间的硬界；三者必须纳入目标 datagrams/s、NVMe fsync p95 和最长 Kafka 故障窗口的容量验收。

E2B1 只解决同一 collector identity 和本地 state volume 的进程重启。新 owner/跨节点接管仍须 E2B2：把 coalesced quality snapshot 关联到固定 exporter shard ownership 与 Kafka compacted checkpoint，连同 FLOW-02D3 的 plan history/attempt metadata 一起恢复；在 owner 切换、旧 WAL 和 kill -9 矩阵完成前，不宣称跨节点 epoch 连续。

单个 datagram/shard 子批次达到 `max_records` 或 `max_bytes` 时按稳定 `chunk_index` 切分；Kafka producer request 再按 `max_wait` 合并多个 protobuf message，减少网络 syscall，但不改变 message ID。存在 collect-state 时 child 0 固定留给状态消息，data child 从 1 开始；否则 data child 从 0 开始。Kafka idempotent producer 获得 `acks=all` 后，追加对应的 `ACK2(datagram_id,child_index,child_count,crc32)`；ack journal 与 raw WAL 使用同周期 group fsync。进程内 pending ack 立即防重，崩溃前尚未 fsync 的 ack 只会使稳定 ID 重放，不会漏数；segment 回收只看 durable ack，并在回收时原子压缩 ack journal。恢复时重建逻辑 ack bitmap，只有全部 child confirmed 的关闭 WAL segment 才能回收。进程内失败重发递增 `replay_generation` 并保持 batch/record ID 和原 partition map；跨进程/跨节点连续 generation 仍须 FLOW-02D3 的持久化 attempt metadata/plan history 才能保证，消费端去重不能以 generation 代替稳定 ID。dimension 对 replay batch 使用 base `source_batch_ids` 精确核验，不使用可能误删数据的 Bloom-only 判定。sFlow counter sample 走独立 counter adapter，不进入 normalized flow topic，也不乘 sampling rate。

### 步骤 4：dimension worker 选择版本并归类地址段

- 消费 `watchdog.flow.normalized.v1`，按 record `event_time` 选择当时 active 的 `dimension_snapshot`；消费延迟不得改变版本；
- 用版本化 LPM 判断 local/remote 和业务方向，再分别对 local/remote（并保留 src/dst side）做最长前缀；
- `primary_prefix` 每个 address role 只产生一个互斥成员，未命中写 `_unassigned`；每个 role 的 prefix 汇总必须守恒；
- 根据 prefix labels 运行 address set selector；一个 record 可命中多个 set，写 `membership_mode=tag`，API 标记非加和；
- snapshot 发布前预估匹配扩张并限制 `max_address_sets_per_record`；超限配置禁止发布，不靠运行时静默截断。

### 步骤 5：Geo/ASN、业务和六维富化

- tenant Geo override LPM 优先，否则查询与 event time 对齐的 `GeoIndex`；
- 生成 `GeoInfo{country,admin_code,subdivision,city,isp_id,asn,version,source}` 并调用六维纯函数；
- 固化 `dimension_snapshot_id`、`geo_version` 和 `classification_version`；
- `internal` 记录（两端皆本地）的 business 取 src 侧本地标签作为确定性选择，只进入 internal 质量口径，不进入业务 × 六类矩阵；
- 配置加载失败继续旧版本并告警；没有可用 event-time 版本时暂停该 tenant/partition，不把记录归入错误的新版本。

### 步骤 6：异步分片分钟汇聚

- normalized partition 由固定 consumer worker 独占，record 再 hash 到固定 dimension shard；当前分钟和迟到宽限窗口并存，禁止逐 flow 启动 goroutine 或同步查询 MySQL；
- 同一 enriched record 同时更新 pair/category/endpoint/Geo/port 和 address dimension 聚合；address 以 `address_role + dimension_kind + dimension_id + snapshot` 为键；
- 聚合状态达到内存水位时提前 flush 或 spill，继续超限则暂停 Kafka partition 形成 lag，不丢 pair 明细；
- normalized lag 达到容量高水位时先告警/扩容并停止非必要 backfill；Kafka producer 背压由 flow-collect WAL 承接；
- 只有 normalized retention 与 WAL 联合保护窗口被耗尽才形成不可恢复 loss，并记录精确 tenant/partition/event-time 窗口；
- address set row expansion、unassigned ratio、snapshot skew 和 dimension lag 必须可观测。

### 步骤 7：单一 base insert、提交 offset 与派生修复

- enriched record 先在 worker 内聚合为 `flow_pair_1m` base batch；base row 已携带 prefix/set/snapshot/Geo/业务/六维字段，dimension worker 不再同时直写 address 表；
- `ingest_batch_id=hash(topic,partition,first_offset,last_offset,worker_schema)`，insert 前在本地 manifest 固化 offset range、输入 normalized batch IDs、row count、checksum 和 snapshot versions；worker 对 `replay_generation>0` 的 batch 先用 base `source_batch_ids` 精确查询并剔除已提交 ID；
- 生产 base 表使用 ReplicatedSummingMergeTree insert dedup token；重试前还可按持久化 `ingest_batch_id` 检查完整 base batch，避免 dedup window 外重复；
- base insert 成功后即可提交 normalized offset。其余五张 CH 表和可选 VM recording cache 都是可重建派生数据：每个 partition/derived-target 在 compacted checkpoint 中推进连续 watermark，空洞保存为有界 missing batch ID 列表；不得为每个 batch 创建永久 compacted key。attached MV 必须忽略 view error 并记录日志，或改用互斥的进程内 rollup loop。失败只记录 divergence，由 base fact 用同一 `(ingest_batch_id,target)` 幂等补写，不反向丢弃已确认事实；
- 重分类 job 使用独立 consumer group/run ID 和目标 snapshot，受 retention/TTL 限制，可暂停、重试和审计，不阻塞在线 group；
- 有界本地 debug capture/Kafka DLQ 仅用于短期诊断，不承载统计事实；VictoriaLogs 按存储收敛 ADR 裁撤。

### 5.1 端到端不变量

以下不变量同时进入代码断言、指标和测试，违反任一项即阻断发布：

| 不变量 | 约束 |
|---|---|
| ID 可追溯 | `datagram_id → normalized_batch_id/normalized_id → ingest_batch_id` 可双向定位，WAL/Kafka/CH/日志不生成随机替代 ID |
| 接收确认 | Kafka ack 前不回收对应 WAL；WAL hard limit 不覆盖未确认数据 |
| 采样原值 | raw bytes/packets、sampling mode/rate 与 estimated 值同时保留；估算只计算一次 |
| 单一事实提交 | normalized offset 只以 enriched base batch 成功为条件，不依赖 VM/派生表 |
| Prefix 守恒 | 同一 tenant/time/address_role 的全部 primary prefix（含 `_unassigned`）之和等于 base total |
| Set 非加和 | address set 可以多重命中，API/图表/导出固定 `additive=false` |
| 时态一致 | dimension/Geo/classification/adjustment 都回显版本；异步延迟不能改变 event-time 选版 |
| 派生可修复 | base TTL 窗口内删除任一派生分区后可从 base 重建且结果一致；超出 base TTL 的派生历史依赖派生表自身备份，不承诺从 base 重建 |
| 无静默丢失 | kernel/WAL/Kafka/CH 任一容量耗尽必须生成 tenant/exporter/time 范围明确的 data-loss/degraded interval |

### 5.2 状态与检查点

```text
WAL:       appended → fsynced → decoded → kafka_acked → reclaimable
Kafka:     produced → consumed → base_inserted → offset_committed
Derived:   pending → complete | divergent → rebuilding → complete
Snapshot:  draft → validated → published → workers_acked → effective → retired
Exporter desired: pending → active ↔ suspended → retired → deleted
Exporter health:  unknown → warming → healthy ↔ degraded ↔ stale
Reclass:   pending → running ↔ paused → validated → active → retired
                         └──────────────→ failed | canceled
VPN find:  heuristic → probe_queued → probing/observing
                                  ├→ corroborated | confirmed_protocol
                                  └→ inconclusive | expired
```

状态迁移必须幂等且带原因、时间、actor/version。`warming` 期间 NetFlow/IPFIX 不伪造无模板记录；`divergent` 时查询可回退 base 或标记 partial，不能静默返回不完整派生数据。

## 6. 六维分类规则

### 6.1 纯函数输入输出

```go
type HomeProfile struct {
    Province           string
    City               string
    ISPIDs             map[uint16]struct{}
    OverseasIncludesHMT bool
    Version             uint32
}

type Category string
```

规则按序命中。分类函数入口先做 HMT 规范化：`country ∈ {HK, MO, TW}` 的记录改写为 `country=CN` 加对应 GB/T 2260 省级码（`810000/820000/710000`，原 admin_code 已更细时保留原值），此后 `is_hmt = admin_code 前两位 ∈ {71, 81, 82}`：

| 序号 | 条件 | category |
|---:|---|---|
| 1 | country 未知/空 | `unknown` |
| 2 | country 非 CN（规范化后仍非 CN） | `overseas` |
| 3 | is_hmt 且 `overseas_includes_hmt=true` | `overseas` |
| 4 | 无省级码，或 ISP=0 | `unknown` |
| 5 | ISP∈home 且省=home 且有市码且市=home | `on_net_local_city` |
| 6 | ISP∈home 且省=home 且有市码且市≠home | `on_net_cross_city` |
| 7 | ISP∈home 且省=home 且（无市码，或 home 未配置本市） | `unknown` |
| 8 | ISP∈home 且省≠home | `on_net_cross_province` |
| 9 | ISP∉home 且省=home | `off_net_in_province` |
| 10 | ISP∉home 且省≠home | `off_net_cross_province` |

规则 7 是显式补位：本网同省但市级信息不足时不得猜测本市/跨市，且任何输入组合都必须命中表中一行——纯函数以“落表外即 bug”做穷尽性单测。`overseas_includes_hmt=false` 时港澳台按规范化后的省码继续走 4–10（通常落入异网外省），分类结果不再受上游 country 码表示差异影响。

`internal/transit/ambiguous` 在方向步骤直接产生同名非六类 category。Home profile 不完整时所有中国记录进入 unknown，并在健康页显示“未应用归属配置”。

### 6.2 配置变化

- 应用新 home profile 时 `classification_version + 1`；
- Geo 文件切换只改变 `geo_version`；
- 正在写的旧分钟桶继续使用其首次创建时的版本，下一分钟统一切新版本，避免同 key 混版本；
- 租户 Geo override version 计入 `classification_version`；
- QueryGateway 按分钟解析 live/active overlay，只选择一套 dimension/Geo/classification version；跨时间片可以拼接，不得把同一分钟的多个版本直接相加；详情和导出必须返回版本分布。

## 7. MySQL：flow 模块 5 张新表

沿用 watchdog MySQL 8、`CHAR(26)` ID、tenant FK 和 `utf8mb4` 约定。本节只列 flow 模块拥有的五张表；用户/角色/权限、`collector_agents/collector_bindings`、target、`dimension_snapshots`、visualization、export、adjustment policy 属于宿主 core，DDL 见平台架构文档。本地/业务/Geo override 复用 `address_prefixes`，审计复用 `audit_logs`，不另建表。

```sql
CREATE TABLE IF NOT EXISTS flow_exporters (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  collector_id CHAR(26) NOT NULL,
  target_id CHAR(26) NOT NULL,
  device_id CHAR(26) NULL,
  source_ip VARBINARY(16) NOT NULL,
  agent_ip VARBINARY(16) NULL,
  protocol VARCHAR(16) NOT NULL,
  observation_domain_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
  counter_mode VARCHAR(16) NOT NULL DEFAULT 'auto',
  sampling_overrides_json JSON NULL COMMENT
    'exact sFlow source or NetFlow sampler keys mapped to positive integer rates',
  reconcile_counter_source VARCHAR(16) NOT NULL DEFAULT 'none',
  allowed_in_if_indexes JSON NULL,
  allowed_out_if_indexes JSON NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'pending',
  expected_datagram_interval_seconds INT UNSIGNED NOT NULL DEFAULT 60,
  last_seen_at DATETIME(3) NULL,
  datagrams_received BIGINT UNSIGNED NOT NULL DEFAULT 0,
  records_decoded BIGINT UNSIGNED NOT NULL DEFAULT 0,
  sequence_gaps BIGINT UNSIGNED NOT NULL DEFAULT 0,
  generation BIGINT UNSIGNED NOT NULL DEFAULT 1,
  observed_generation BIGINT UNSIGNED NOT NULL DEFAULT 0,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NULL,
  updated_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  retired_at DATETIME(3) NULL,
  deleted_at DATETIME(3) NULL,
  purge_after DATETIME(3) NULL,
  live_identity TINYINT GENERATED ALWAYS AS
    (IF(deleted_at IS NULL, 1, NULL)) STORED,
  UNIQUE KEY uq_flow_exporter_source
    (tenant_id, protocol, source_ip, observation_domain_id, live_identity),
  KEY idx_flow_exporter_collector (tenant_id, collector_id),
  KEY idx_flow_exporter_target (tenant_id, target_id),
  KEY idx_flow_exporter_device (tenant_id, device_id),
  KEY idx_flow_exporter_health (tenant_id, status, last_seen_at),
  CONSTRAINT fk_flow_exporter_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_flow_exporter_collector
    FOREIGN KEY (collector_id) REFERENCES collector_agents(id) ON DELETE RESTRICT,
  CONSTRAINT fk_flow_exporter_target
    FOREIGN KEY (target_id) REFERENCES targets(id) ON DELETE RESTRICT,
  CONSTRAINT fk_flow_exporter_device
    FOREIGN KEY (device_id) REFERENCES network_devices(id) ON DELETE SET NULL,
  CHECK (protocol IN ('sflow5','netflow5','netflow9','ipfix')),
  CHECK (counter_mode IN ('auto','sampled','pre_scaled')),
  CHECK (status IN ('pending','active','suspended','retired','deleted')),
  CHECK (reconcile_counter_source IN ('none','sflow','snmp'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS flow_settings (
  tenant_id CHAR(26) PRIMARY KEY,
  home_province CHAR(6) NULL,
  home_city CHAR(6) NULL,
  home_isp_ids JSON NULL,
  overseas_includes_hmt TINYINT(1) NOT NULL DEFAULT 1,
  internal_policy VARCHAR(16) NOT NULL DEFAULT 'count',
  transit_policy VARCHAR(16) NOT NULL DEFAULT 'drop',
  auto_detect_json JSON NULL,
  classification_version INT UNSIGNED NOT NULL DEFAULT 1,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  updated_by CHAR(26) NULL,
  applied_at DATETIME(3) NULL,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  CONSTRAINT fk_flow_settings_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (internal_policy IN ('drop','count')),
  CHECK (transit_policy IN ('drop','count'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS flow_vpn_rules (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  name VARCHAR(190) NOT NULL,
  kind VARCHAR(32) NOT NULL,
  rule_schema_version SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  selector_json JSON NOT NULL COMMENT
    'src/dst geo, direction, protocol/transport, local/remote port selectors',
  behavior_json JSON NULL COMMENT
    'duration, fanout, periodicity, transport set, bytes/packets/flows and ratio thresholds',
  intelligence_json JSON NULL COMMENT
    'versioned prefix/address-set/ASN/org/route-class selectors with source/reason/expiry',
  probe_policy_json JSON NULL COMMENT
    'none/passive_observe/active_handshake profiles, threshold, scope and limits',
  weight SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  status VARCHAR(16) NOT NULL DEFAULT 'draft',
  rule_version INT UNSIGNED NOT NULL DEFAULT 1,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NULL,
  updated_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  retired_at DATETIME(3) NULL,
  deleted_at DATETIME(3) NULL,
  purge_after DATETIME(3) NULL,
  live_identity TINYINT GENERATED ALWAYS AS
    (IF(deleted_at IS NULL, 1, NULL)) STORED,
  UNIQUE KEY uq_flow_vpn_rule_name (tenant_id, name, live_identity),
  KEY idx_flow_vpn_rule_status (tenant_id, status),
  CONSTRAINT fk_flow_vpn_rule_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (kind IN ('score','probe_trigger','allow','suppress')),
  CHECK (status IN ('draft','active','suspended','retired','deleted'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS flow_vpn_findings (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  window_start DATETIME(3) NOT NULL,
  window_end DATETIME(3) NOT NULL,
  conversation_key BINARY(16) NOT NULL,
  local_ip VARBINARY(16) NOT NULL,
  remote_ip VARBINARY(16) NOT NULL,
  primary_local_port SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  primary_remote_port SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  primary_ip_protocol TINYINT UNSIGNED NOT NULL,
  local_to_remote_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
  remote_to_local_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
  balanced_ratio DECIMAL(7,6) NOT NULL DEFAULT 0,
  flow_record_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
  max_duration_ms BIGINT UNSIGNED NOT NULL DEFAULT 0,
  remote_fanout INT UNSIGNED NOT NULL DEFAULT 1,
  active_bucket_count INT UNSIGNED NOT NULL DEFAULT 0,
  periodicity_score DECIMAL(7,6) NULL,
  transport_set JSON NOT NULL,
  directionality VARCHAR(16) NOT NULL,
  score SMALLINT UNSIGNED NOT NULL,
  level VARCHAR(16) NOT NULL,
  verdict VARCHAR(32) NOT NULL DEFAULT 'heuristic',
  disposition VARCHAR(24) NOT NULL DEFAULT 'open',
  disposition_note VARCHAR(1024) NULL,
  disposition_by CHAR(26) NULL,
  disposition_at DATETIME(3) NULL,
  matched_rule_ids JSON NOT NULL,
  heuristic_evidence JSON NOT NULL,
  rule_set_version VARCHAR(128) NOT NULL,
  intelligence_version VARCHAR(128) NULL,
  latest_probe_job_id CHAR(26) NULL,
  probe_mode VARCHAR(24) NOT NULL DEFAULT 'none',
  probe_status VARCHAR(24) NOT NULL DEFAULT 'not_requested',
  detected_protocols JSON NULL,
  probe_confidence DECIMAL(5,4) NULL,
  probe_analyzer VARCHAR(64) NULL,
  probe_analyzer_version VARCHAR(64) NULL,
  probe_evidence JSON NULL,
  probed_at DATETIME(3) NULL,
  dimension_snapshot_id CHAR(26) NOT NULL,
  geo_version VARCHAR(128) NOT NULL,
  classification_version INT UNSIGNED NOT NULL,
  complete_ratio DECIMAL(7,6) NOT NULL DEFAULT 1.000000,
  expires_at DATETIME(3) NOT NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_flow_vpn_finding
    (tenant_id, window_start, window_end, conversation_key, rule_set_version),
  KEY idx_flow_vpn_finding_level (tenant_id, window_end, level, score),
  KEY idx_flow_vpn_finding_probe (tenant_id, probe_status, window_end),
  KEY idx_flow_vpn_finding_disposition
    (tenant_id, disposition, window_end),
  CONSTRAINT fk_flow_vpn_finding_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (window_end > window_start),
  CHECK (balanced_ratio >= 0 AND balanced_ratio <= 1),
  CHECK (remote_fanout >= 1),
  CHECK (periodicity_score IS NULL OR
    (periodicity_score >= 0 AND periodicity_score <= 1)),
  CHECK (complete_ratio >= 0 AND complete_ratio <= 1),
  CHECK (level IN ('suspect','high_risk')),
  CHECK (directionality IN ('balanced','unidirectional','asymmetric','unknown')),
  CHECK (verdict IN ('heuristic','corroborated','confirmed_protocol','inconclusive')),
  CHECK (disposition IN (
    'open','acknowledged','investigating','authorized','false_positive','resolved'
  )),
  CHECK (probe_mode IN ('none','passive_observe','active_handshake')),
  CHECK (probe_status IN (
    'not_requested','queued','running','succeeded','failed','canceled','expired'
  )),
  CHECK (probe_confidence IS NULL OR
    (probe_confidence >= 0 AND probe_confidence <= 1))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS flow_reclassification_jobs (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  requested_by CHAR(26) NOT NULL,
  idempotency_key VARCHAR(128) NOT NULL,
  request_hash CHAR(64) NOT NULL,
  source_kind VARCHAR(16) NOT NULL,
  range_start DATETIME(3) NOT NULL,
  range_end DATETIME(3) NOT NULL,
  source_dimension_snapshot_id CHAR(26) NULL,
  target_dimension_snapshot_id CHAR(26) NOT NULL,
  target_geo_version VARCHAR(128) NOT NULL,
  target_classification_version INT UNSIGNED NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'pending',
  checkpoint_json JSON NULL,
  input_records BIGINT UNSIGNED NOT NULL DEFAULT 0,
  output_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,
  progress_total BIGINT UNSIGNED NOT NULL DEFAULT 0,
  progress_done BIGINT UNSIGNED NOT NULL DEFAULT 0,
  validation_json JSON NULL,
  result_ref VARCHAR(512) NULL,
  error_code VARCHAR(64) NULL,
  error_message VARCHAR(1024) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  started_at DATETIME(3) NULL,
  heartbeat_at DATETIME(3) NULL,
  lease_owner VARCHAR(128) NULL,
  lease_expires_at DATETIME(3) NULL,
  cancel_requested_at DATETIME(3) NULL,
  finished_at DATETIME(3) NULL,
  activated_at DATETIME(3) NULL,
  retired_at DATETIME(3) NULL,
  expires_at DATETIME(3) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_flow_reclass_idempotency (tenant_id, idempotency_key),
  KEY idx_flow_reclass_status (tenant_id, status, created_at),
  KEY idx_flow_reclass_range (tenant_id, range_start, range_end, status),
  CONSTRAINT fk_flow_reclass_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_flow_reclass_user
    FOREIGN KEY (requested_by) REFERENCES users(id) ON DELETE RESTRICT,
  CONSTRAINT fk_flow_reclass_target_snapshot
    FOREIGN KEY (target_dimension_snapshot_id) REFERENCES dimension_snapshots(id)
      ON DELETE RESTRICT,
  CHECK (source_kind IN ('normalized','base')),
  CHECK (status IN (
    'pending','running','paused','validated','active','retired','failed','canceled'
  )),
  CHECK (progress_done <= progress_total OR progress_total = 0),
  CHECK (range_end > range_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

`collector_id` 指向平台服务身份，`target_id` 是必填资源根，`device_id` 只在已发现具体网元时填写。exporter 的协议身份唯一键**不含 `collector_id`**：`collector_id` 只是“当前 decode owner”，N+1 按 exporter affinity 切换 VIP 时由 reconciler 依据新 owner 的 heartbeat/plan 更新该列，不得因此产生第二行 exporter，也不使原 identity 回到 pending。准入路径固定为：签名 plan 判定实例是否被授权接收该 source → 按 `(tenant, protocol, source_ip, observation_domain)` 查唯一 exporter 行 → 校验其 status；owner 列短暂滞后只影响健康展示，不影响准入与数据接收。应用层还必须校验 exporter 与 collector binding、target、tenant 一致，不能只依赖单列 FK。`sampling_overrides_json` 示例为 `{"sflow":{"0/0/12":1000,"0/0/13":10000},"netflow":{"7":2000}}`。API 必须验证 key 是精确 data source/sampler、rate 为正整数并限制条目数；不支持 `*` 或设备级默认 key，避免一个 override 覆盖不同端口。`reconcile_counter_source=none` 是合法 active 状态。

`created_by/updated_by/requested_by/disposition_by` 是逻辑 actor ID，可代表用户、service account 或 system reconciler；除 `requested_by` 需要保留当前用户 FK 外，公共 actor 字段不强制 FK，避免用户停用/删除破坏历史审计。`audit_logs` 保存 actor type、subject、request/correlation ID 和前后值，是权威审计链。service 层必须保证 `status=deleted` 与 `deleted_at` 同步、所有 PATCH 递增 `row_version`、同一 `Idempotency-Key + request_hash` 返回原结果、不同 request hash 返回 409。

VPN rule 的四个 JSON 字段不是任意透传对象，必须按 `rule_schema_version` 使用固定 JSON Schema，拒绝未知 key。v1 逻辑结构如下：

```json
{
  "selector_json": {
    "src_geo": ["domestic", "overseas", "unknown"],
    "dst_geo": ["domestic", "overseas", "unknown"],
    "business_directions": ["out", "in"],
    "ip_protocols": [6, 17],
    "transport_hints": ["tcp", "udp", "tls_candidate", "quic_candidate"],
    "local_ports": [{"start": 1, "end": 65535}],
    "remote_ports": [{"start": 443, "end": 443}]
  },
  "behavior_json": {
    "min_bytes": 10485760,
    "min_packets": 100,
    "min_flow_records": 3,
    "min_duration_ms": 600000,
    "min_remote_fanout": 1,
    "min_active_buckets": 4,
    "periodicity_score_gte": 0.75,
    "transport_all": ["udp", "tcp"],
    "balanced_ratio_gte": 0.70,
    "unidirectional_ratio_lte": 0.05,
    "min_complete_ratio": 0.95
  },
  "intelligence_json": {
    "remote_address_set_ids": ["01..."],
    "remote_asns": [4809],
    "remote_organization_ids": [],
    "remote_route_classes": ["cn2"],
    "source": "tenant-intelligence",
    "reason": "approved investigation scope",
    "owner": "security-team",
    "effective_from": "2026-09-01T00:00:00Z",
    "expires_at": "2026-10-01T00:00:00Z"
  },
  "probe_policy_json": {
    "mode": "passive_observe",
    "min_score": 4,
    "profiles": ["socks5", "http_connect", "websocket", "tls_vpn", "encrypted_proxy_fingerprint"],
    "scope_policy_id": "01...",
    "timeout_ms": 3000,
    "max_attempts": 1,
    "cooldown_seconds": 86400
  }
}
```

数组为空表示该维度不限制，不允许用字符串 `*`。同一数组内取 OR，不同非空字段及 `selector/behavior/intelligence` 三组之间取 AND；多条正向规则之间取 OR，再按 family cap 合分。`tls_candidate/quic_candidate` 仅由端口/传输层推断；`tls_observed/quic_observed` 只能来自旁路 analyzer。`remote_route_classes` 是租户地址情报标签，系统不预置 CN2/ASN 风险结论。`allow` 表达已批准的合法用途，`suppress` 表达已验证的检测器误报；两者均要求 owner/reason/expiry，命中后停止正向评分和 probe，但只记录 evaluation/audit，不删除既有 finding。active rule 不原地修改 JSON；PATCH 创建新 `rule_version`，preview 返回候选数、预计 probe 数、Top ASN/prefix 和成本，审批后切换新的 immutable `rule_set_version`。

VPN finding 以 canonical peer conversation 聚合：local/remote 方向固定，`conversation_key=hash(tenant,local_ip,remote_ip)`，时间窗口由唯一键另行限定；primary port/protocol 取窗口内估算字节最大的传输组合，完整组合保存在 `transport_set`，因此能表达同一 peer 同时出现 UDP/443、TCP/443 和其他 transport。`balanced_ratio=min(local_to_remote_bytes,remote_to_local_bytes)/max(local_to_remote_bytes,remote_to_local_bytes,1)`；`remote_fanout` 是同一规则 selector、窗口和本地主机下的 distinct remote peer 数，不能在单个 peer 内计算。活动时间按固定子桶去重，`periodicity_score=max_frequency(consecutive_active_bucket_gap)/gap_count`；少于 4 个活动桶时为 NULL，不得把稀疏样本判为周期连接。只有窗口完整、唯一观察点、最小 bytes/packets/flow-record count 达标时才判 `balanced/unidirectional`，否则为 `unknown`。`flow_record_count` 是实际收到并聚合的 flow/sample record 数，不按 sampling rate 放大。finding 更新 probe 字段不改 CH raw/base；`latest_probe_job_id` 只作列表快捷引用，完整 job/attempt timeline 仍从平台 job 按 `finding_id` 查询。每次结果合并写 audit，并保留原 `heuristic_evidence`。`verdict` 是机器证据结论，`disposition` 是人工处置结论，二者不能互相覆盖：acknowledge/调查/授权/误报/解决只更新 disposition；annotation 作为有界 audit event 追加，不能编辑或删除原证据。评分、probe merge 和人工处置均以 `row_version` 乐观锁更新，冲突时重读后按字段所有权合并，不使用整行 last-write-wins。

Probe 复用平台异步 job/collector plan，不再新增 flow probe 表：job 固化 `finding_id/conversation/target/mode/profiles/scope_policy/analyzer_version/deadline`，幂等键为 `hash(finding_id,mode,profiles,analyzer_version)`。agent 返回签名的 attempt、时间、network vantage、request/response fingerprint、protocol、confidence、stop reason 和截断/脱敏证据；原始 payload 默认不上传。被动 plan 只下发限时 BPF/五元组观察条件，主动 plan 必须在调度前再次检查授权 CIDR、端口、tenant quota 和 kill switch。

历史重分类只处理已经关闭迟到窗口的完整分钟。可回算范围受事实保留硬限制：`source_kind=normalized` ≤ normalized retention（默认 72h），`source_kind=base` ≤ base TTL（默认 7d）；更早历史只能保持原分类版本，产品与 API 均不承诺超窗回算，创建请求超窗时拒绝并返回 `available_from`。job 先写目标 classification/Geo/dimension version 的新 base rows；其 `ingest_batch_id=hash(job_id,chunk_index,worker_schema)`，checkpoint 必须持久化 source range、chunk、row count 和 checksum。完成 base/derived/prefix 守恒验证后才能从 `validated` 原子激活；同 tenant 的 active range 禁止重叠。QueryGateway 把查询时间切成 active overlay 与原 live version 区间，每个分钟只能选择一套 classification version，避免新旧版本相加。退役 overlay 只改变查询指针，不删除 raw/base；物理清理由 TTL 统一完成。

实现迁移时以版本 migration 为唯一 schema 来源，`install/init.sql` 由 migration 生成或由 CI 验证等价，不再要求开发者手工双写。flow migration 必须声明依赖 platform collector migration，并提供旧 `target_agents`/原型 exporter 数据的迁移或明确不可迁移报告。

## 8. ClickHouse：6 张表与物化视图

六张物理表分别覆盖 enriched pair base、六类、源/目的 endpoint、对端 Geo、端口和异步地址统计维度。dimension worker 的 normalized 消费主循环只写 `flow_pair_1m` 一处；prefix/set/snapshot 结果已经作为 base 字段写入，五张派生表由 MV/rollup 从 base 生成。这样 normalized offset 的成功条件只有一个事实 sink，不存在 pair 成功而 address 表失败的跨表部分提交。每张派生表都保留 `ingest_batch_id`，查询跨 batch 求和；rollup repair 以 `(ingest_batch_id, derived_target)` 为最小幂等单元。

下列 `SummingMergeTree` 是单节点开发/CI 可执行基线。生产 migration 必须生成 `ReplicatedSummingMergeTree`（以及按部署需要的 Distributed 表），并验证 insert dedup window 覆盖 Kafka 最大重放期；如果目标 ClickHouse 拓扑不能提供等价批次去重，则先实现 offset-range ingest ledger，不能仅靠 `SummingMergeTree` 宣称重复消费安全。生产 base insert 必须设置 `materialized_views_ignore_errors=1` 并开启 view query log，使派生失败不能反向破坏事实提交；rollup loop 根据 base manifest、`system.query_views_log` 和 compacted dimension checkpoint 检测缺失 target 并用同一 SELECT 补写。若目标版本不能证明该隔离语义，则 production migration 不 attach MV，改由 dimension 进程内独立 rollup loop 执行同样的 INSERT SELECT；两种模式不得同时启用。

### 8.1 建库和主表

```sql
CREATE DATABASE IF NOT EXISTS watchdog_flow;

CREATE TABLE IF NOT EXISTS watchdog_flow.flow_pair_1m (
  ts DateTime('UTC'),
  ingest_batch_id FixedString(32),
  source_batch_ids Array(FixedString(32)),
  tenant_id LowCardinality(String),
  device_id LowCardinality(String),
  exporter_id LowCardinality(String),
  observation_if_index UInt32,
  observation_direction Enum8('unknown' = 0, 'ingress' = 1, 'egress' = 2),
  business_direction Enum8(
    'in' = 1, 'out' = 2, 'internal' = 3,
    'transit' = 4, 'ambiguous' = 5
  ),
  category Enum8(
    'on_net_local_city' = 1,
    'on_net_cross_city' = 2,
    'on_net_cross_province' = 3,
    'off_net_in_province' = 4,
    'off_net_cross_province' = 5,
    'overseas' = 6,
    'unknown' = 7,
    'internal' = 8,
    'transit' = 9,
    'ambiguous' = 10
  ),
  business LowCardinality(String),
  src_ip IPv6,
  dst_ip IPv6,
  src_port UInt16,
  dst_port UInt16,
  local_ip IPv6,
  remote_ip IPv6,
  local_port UInt16,
  remote_port UInt16,
  ip_proto UInt8,
  tcp_flags UInt8,
  remote_country FixedString(2),
  remote_admin_code LowCardinality(String),
  remote_subdivision LowCardinality(String),
  remote_city LowCardinality(String),
  remote_isp_id UInt16,
  remote_asn UInt32,
  dimension_snapshot_id LowCardinality(String),
  local_endpoint_side Enum8('none' = 0, 'src' = 1, 'dst' = 2),
  remote_endpoint_side Enum8('none' = 0, 'src' = 1, 'dst' = 2),
  local_prefix_id LowCardinality(String),
  local_prefix_cidr LowCardinality(String),
  remote_prefix_id LowCardinality(String),
  remote_prefix_cidr LowCardinality(String),
  local_address_set_ids Array(String),
  remote_address_set_ids Array(String),
  dimension_fingerprint UInt64,
  geo_version LowCardinality(String),
  classification_version UInt32,
  estimated_bytes UInt64,
  estimated_packets UInt64,
  sample_count UInt64,
  flow_duration_ms_sum UInt64,
  flow_duration_ms_max UInt64,
  INDEX idx_source_batch_ids source_batch_ids TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = SummingMergeTree((
  estimated_bytes, estimated_packets, sample_count, flow_duration_ms_sum
))
PARTITION BY toDate(ts)
ORDER BY (
  tenant_id, ts, ingest_batch_id, device_id, exporter_id, observation_if_index,
  observation_direction,
  business_direction, category, business,
  src_ip, dst_ip, src_port, dst_port, ip_proto, tcp_flags,
  remote_country, remote_admin_code, remote_subdivision, remote_city,
  remote_isp_id, remote_asn, dimension_snapshot_id,
  local_prefix_id, remote_prefix_id, dimension_fingerprint,
  geo_version, classification_version
)
TTL ts + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;
```

IPv4 在写入端映射为 IPv4-mapped IPv6，API 输出时还原为 IPv4 文本。`ts` 必须是 UTC 分钟起点。没有业务 local/remote endpoint 时 IP 写 `::` 且对应 side 必须为 `none`，查询不得把 sentinel 当真实 IP；无法判定观察方向时写 `unknown`，不得猜测 ingress/egress。address-set ID 数组必须排序、去重并受 `max_address_sets_per_record` 限制；`dimension_fingerprint` 是 prefix/set/endpoint-side 组合的稳定 hash，防止不同集合成员的 base row 错误合并。`flow_duration_ms_sum` 用于均值，`flow_duration_ms_max` 由 worker 对当前 base key 的可靠非负 duration 求最大值；它不列入 SummingMergeTree 的求和列，且同一 `ingest_batch_id + base key` 只能输出一个确定性值，重放必须相同。没有可靠 duration 时二者均为 0 并设置 quality flag，评分不得把 0 当短连接。`ingest_batch_id` 使用 32 字节十六进制 hash，并保留到 base TTL 结束，用于 dedup window 外的完整批次核验；`source_batch_ids` 保存该 base row 吸收的稳定 normalized batch IDs，按 batch manifest 有界切分，不允许无限数组。

WAL replay 去重查询固定限定 tenant、事件日期和 incoming IDs，再用 `has(source_batch_ids, id)` 精确确认；Bloom index 只用于跳过无关 granule，不能把 Bloom 命中直接当已存在。online Kafka offset replay 优先使用相同 `ingest_batch_id`/insert token，不对每条正常 record 额外查询 ClickHouse。

### 8.2 六类/业务聚合表

```sql
CREATE TABLE IF NOT EXISTS watchdog_flow.flow_category_1m (
  ts DateTime('UTC'),
  ingest_batch_id FixedString(32),
  tenant_id LowCardinality(String),
  device_id LowCardinality(String),
  business_direction Enum8('in' = 1, 'out' = 2),
  category Enum8(
    'on_net_local_city' = 1,
    'on_net_cross_city' = 2,
    'on_net_cross_province' = 3,
    'off_net_in_province' = 4,
    'off_net_cross_province' = 5,
    'overseas' = 6,
    'unknown' = 7
  ),
  business LowCardinality(String),
  dimension_snapshot_id LowCardinality(String),
  geo_version LowCardinality(String),
  classification_version UInt32,
  estimated_bytes UInt64,
  estimated_packets UInt64,
  sample_count UInt64
)
ENGINE = SummingMergeTree((estimated_bytes, estimated_packets, sample_count))
PARTITION BY toYYYYMM(ts)
ORDER BY (
  tenant_id, ts, ingest_batch_id, device_id, business_direction,
  category, business, dimension_snapshot_id, geo_version, classification_version
)
TTL ts + INTERVAL 400 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS watchdog_flow.mv_flow_category_1m
TO watchdog_flow.flow_category_1m
AS
SELECT
  ts, ingest_batch_id, tenant_id, device_id, business_direction, category, business,
  dimension_snapshot_id, geo_version, classification_version,
  sum(estimated_bytes) AS estimated_bytes,
  sum(estimated_packets) AS estimated_packets,
  sum(sample_count) AS sample_count
FROM watchdog_flow.flow_pair_1m
WHERE business_direction IN ('in', 'out')
  AND category IN (
    'on_net_local_city', 'on_net_cross_city', 'on_net_cross_province',
    'off_net_in_province', 'off_net_cross_province', 'overseas', 'unknown'
  )
GROUP BY
  ts, ingest_batch_id, tenant_id, device_id, business_direction, category, business,
  dimension_snapshot_id, geo_version, classification_version;
```

### 8.3 源/目的 endpoint 表

```sql
CREATE TABLE IF NOT EXISTS watchdog_flow.flow_endpoint_1m (
  ts DateTime('UTC'),
  ingest_batch_id FixedString(32),
  tenant_id LowCardinality(String),
  device_id LowCardinality(String),
  endpoint_side Enum8('src' = 1, 'dst' = 2),
  endpoint_ip IPv6,
  business_direction Enum8('in' = 1, 'out' = 2),
  category Enum8(
    'on_net_local_city' = 1,
    'on_net_cross_city' = 2,
    'on_net_cross_province' = 3,
    'off_net_in_province' = 4,
    'off_net_cross_province' = 5,
    'overseas' = 6,
    'unknown' = 7
  ),
  business LowCardinality(String),
  dimension_snapshot_id LowCardinality(String),
  geo_version LowCardinality(String),
  classification_version UInt32,
  estimated_bytes UInt64,
  estimated_packets UInt64,
  sample_count UInt64
)
ENGINE = SummingMergeTree((estimated_bytes, estimated_packets, sample_count))
PARTITION BY toDate(ts)
ORDER BY (
  tenant_id, ts, ingest_batch_id, endpoint_side, endpoint_ip,
  device_id, business_direction, category, business,
  dimension_snapshot_id, geo_version, classification_version
)
TTL ts + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS watchdog_flow.mv_flow_endpoint_src_1m
TO watchdog_flow.flow_endpoint_1m
AS
SELECT
  ts, ingest_batch_id, tenant_id, device_id, 'src' AS endpoint_side,
  src_ip AS endpoint_ip, business_direction, category, business,
  dimension_snapshot_id, geo_version, classification_version,
  sum(estimated_bytes) AS estimated_bytes,
  sum(estimated_packets) AS estimated_packets,
  sum(sample_count) AS sample_count
FROM watchdog_flow.flow_pair_1m
WHERE business_direction IN ('in', 'out') AND category NOT IN ('internal','transit','ambiguous')
GROUP BY
  ts, ingest_batch_id, tenant_id, device_id, src_ip, business_direction, category, business,
  dimension_snapshot_id, geo_version, classification_version;

CREATE MATERIALIZED VIEW IF NOT EXISTS watchdog_flow.mv_flow_endpoint_dst_1m
TO watchdog_flow.flow_endpoint_1m
AS
SELECT
  ts, ingest_batch_id, tenant_id, device_id, 'dst' AS endpoint_side,
  dst_ip AS endpoint_ip, business_direction, category, business,
  dimension_snapshot_id, geo_version, classification_version,
  sum(estimated_bytes) AS estimated_bytes,
  sum(estimated_packets) AS estimated_packets,
  sum(sample_count) AS sample_count
FROM watchdog_flow.flow_pair_1m
WHERE business_direction IN ('in', 'out') AND category NOT IN ('internal','transit','ambiguous')
GROUP BY
  ts, ingest_batch_id, tenant_id, device_id, dst_ip, business_direction, category, business,
  dimension_snapshot_id, geo_version, classification_version;
```

### 8.4 对端 Geo 表

```sql
CREATE TABLE IF NOT EXISTS watchdog_flow.flow_remote_geo_1m (
  ts DateTime('UTC'),
  ingest_batch_id FixedString(32),
  tenant_id LowCardinality(String),
  device_id LowCardinality(String),
  business_direction Enum8('in' = 1, 'out' = 2),
  category Enum8(
    'on_net_local_city' = 1,
    'on_net_cross_city' = 2,
    'on_net_cross_province' = 3,
    'off_net_in_province' = 4,
    'off_net_cross_province' = 5,
    'overseas' = 6,
    'unknown' = 7
  ),
  business LowCardinality(String),
  dimension_snapshot_id LowCardinality(String),
  geo_version LowCardinality(String),
  classification_version UInt32,
  remote_country FixedString(2),
  remote_admin_code LowCardinality(String),
  remote_subdivision LowCardinality(String),
  remote_city LowCardinality(String),
  remote_isp_id UInt16,
  remote_asn UInt32,
  estimated_bytes UInt64,
  estimated_packets UInt64,
  sample_count UInt64
)
ENGINE = SummingMergeTree((estimated_bytes, estimated_packets, sample_count))
PARTITION BY toDate(ts)
ORDER BY (
  tenant_id, ts, ingest_batch_id, device_id, business_direction, category, business,
  dimension_snapshot_id, geo_version, classification_version,
  remote_country, remote_admin_code, remote_subdivision, remote_city,
  remote_isp_id, remote_asn
)
TTL ts + INTERVAL 180 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS watchdog_flow.mv_flow_remote_geo_1m
TO watchdog_flow.flow_remote_geo_1m
AS
SELECT
  ts, ingest_batch_id, tenant_id, device_id, business_direction, category, business,
  dimension_snapshot_id, geo_version, classification_version,
  remote_country, remote_admin_code, remote_subdivision, remote_city,
  remote_isp_id, remote_asn,
  sum(estimated_bytes) AS estimated_bytes,
  sum(estimated_packets) AS estimated_packets,
  sum(sample_count) AS sample_count
FROM watchdog_flow.flow_pair_1m
WHERE business_direction IN ('in', 'out') AND category NOT IN ('internal','transit','ambiguous')
GROUP BY
  ts, ingest_batch_id, tenant_id, device_id, business_direction, category, business,
  dimension_snapshot_id, geo_version, classification_version,
  remote_country, remote_admin_code, remote_subdivision, remote_city,
  remote_isp_id, remote_asn;
```

### 8.5 端口/协议表

```sql
CREATE TABLE IF NOT EXISTS watchdog_flow.flow_port_1m (
  ts DateTime('UTC'),
  ingest_batch_id FixedString(32),
  tenant_id LowCardinality(String),
  device_id LowCardinality(String),
  business_direction Enum8('in' = 1, 'out' = 2),
  category Enum8(
    'on_net_local_city' = 1,
    'on_net_cross_city' = 2,
    'on_net_cross_province' = 3,
    'off_net_in_province' = 4,
    'off_net_cross_province' = 5,
    'overseas' = 6,
    'unknown' = 7
  ),
  business LowCardinality(String),
  dimension_snapshot_id LowCardinality(String),
  geo_version LowCardinality(String),
  classification_version UInt32,
  local_ip IPv6,
  port_side Enum8('local' = 1, 'remote' = 2),
  port UInt16,
  ip_proto UInt8,
  estimated_bytes UInt64,
  estimated_packets UInt64,
  sample_count UInt64
)
ENGINE = SummingMergeTree((estimated_bytes, estimated_packets, sample_count))
PARTITION BY toDate(ts)
ORDER BY (
  tenant_id, ts, ingest_batch_id, local_ip, port_side, port, ip_proto,
  device_id, business_direction, category, business,
  dimension_snapshot_id, geo_version, classification_version
)
TTL ts + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS watchdog_flow.mv_flow_port_local_1m
TO watchdog_flow.flow_port_1m
AS
SELECT
  ts, ingest_batch_id, tenant_id, device_id, business_direction, category, business,
  dimension_snapshot_id, geo_version, classification_version, local_ip,
  'local' AS port_side,
  local_port AS port, ip_proto,
  sum(estimated_bytes) AS estimated_bytes,
  sum(estimated_packets) AS estimated_packets,
  sum(sample_count) AS sample_count
FROM watchdog_flow.flow_pair_1m
WHERE business_direction IN ('in', 'out')
GROUP BY
  ts, ingest_batch_id, tenant_id, device_id, business_direction, category, business,
  dimension_snapshot_id, geo_version, classification_version, local_ip, local_port, ip_proto;

CREATE MATERIALIZED VIEW IF NOT EXISTS watchdog_flow.mv_flow_port_remote_1m
TO watchdog_flow.flow_port_1m
AS
SELECT
  ts, ingest_batch_id, tenant_id, device_id, business_direction, category, business,
  dimension_snapshot_id, geo_version, classification_version, local_ip,
  'remote' AS port_side,
  remote_port AS port, ip_proto,
  sum(estimated_bytes) AS estimated_bytes,
  sum(estimated_packets) AS estimated_packets,
  sum(sample_count) AS sample_count
FROM watchdog_flow.flow_pair_1m
WHERE business_direction IN ('in', 'out')
GROUP BY
  ts, ingest_batch_id, tenant_id, device_id, business_direction, category, business,
  dimension_snapshot_id, geo_version, classification_version, local_ip, remote_port, ip_proto;
```

### 8.6 地址段与 Address Set 派生汇聚表

```sql
CREATE TABLE IF NOT EXISTS watchdog_flow.flow_address_dimension_1m (
  ts DateTime('UTC'),
  ingest_batch_id FixedString(32),
  tenant_id LowCardinality(String),
  device_id LowCardinality(String),
  exporter_id LowCardinality(String),
  address_role Enum8('local' = 1, 'remote' = 2),
  endpoint_side Enum8('src' = 1, 'dst' = 2),
  dimension_kind Enum8('primary_prefix' = 1, 'address_set' = 2),
  dimension_id LowCardinality(String),
  prefix_cidr LowCardinality(String),
  membership_mode Enum8('exclusive' = 1, 'tag' = 2),
  dimension_snapshot_id LowCardinality(String),
  business_direction Enum8('in' = 1, 'out' = 2),
  category Enum8(
    'on_net_local_city' = 1,
    'on_net_cross_city' = 2,
    'on_net_cross_province' = 3,
    'off_net_in_province' = 4,
    'off_net_cross_province' = 5,
    'overseas' = 6,
    'unknown' = 7
  ),
  business LowCardinality(String),
  geo_version LowCardinality(String),
  classification_version UInt32,
  estimated_bytes UInt64,
  estimated_packets UInt64,
  sample_count UInt64
)
ENGINE = SummingMergeTree((estimated_bytes, estimated_packets, sample_count))
PARTITION BY toDate(ts)
ORDER BY (
  tenant_id, ts, ingest_batch_id, address_role, dimension_kind, dimension_id,
  dimension_snapshot_id, device_id, exporter_id, endpoint_side,
  business_direction, category, business,
  geo_version, classification_version, prefix_cidr, membership_mode
)
TTL ts + INTERVAL 180 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS watchdog_flow.mv_flow_address_local_prefix_1m
TO watchdog_flow.flow_address_dimension_1m
AS SELECT
  ts, ingest_batch_id, tenant_id, device_id, exporter_id,
  'local' AS address_role, local_endpoint_side AS endpoint_side,
  'primary_prefix' AS dimension_kind, local_prefix_id AS dimension_id,
  local_prefix_cidr AS prefix_cidr, 'exclusive' AS membership_mode,
  dimension_snapshot_id, business_direction, category, business,
  geo_version, classification_version,
  sum(estimated_bytes) AS estimated_bytes,
  sum(estimated_packets) AS estimated_packets,
  sum(sample_count) AS sample_count
FROM watchdog_flow.flow_pair_1m
WHERE business_direction IN ('in', 'out')
GROUP BY ts, ingest_batch_id, tenant_id, device_id, exporter_id, local_endpoint_side,
  local_prefix_id, local_prefix_cidr, dimension_snapshot_id,
  business_direction, category, business, geo_version, classification_version;

CREATE MATERIALIZED VIEW IF NOT EXISTS watchdog_flow.mv_flow_address_remote_prefix_1m
TO watchdog_flow.flow_address_dimension_1m
AS SELECT
  ts, ingest_batch_id, tenant_id, device_id, exporter_id,
  'remote' AS address_role, remote_endpoint_side AS endpoint_side,
  'primary_prefix' AS dimension_kind, remote_prefix_id AS dimension_id,
  remote_prefix_cidr AS prefix_cidr, 'exclusive' AS membership_mode,
  dimension_snapshot_id, business_direction, category, business,
  geo_version, classification_version,
  sum(estimated_bytes) AS estimated_bytes,
  sum(estimated_packets) AS estimated_packets,
  sum(sample_count) AS sample_count
FROM watchdog_flow.flow_pair_1m
WHERE business_direction IN ('in', 'out')
GROUP BY ts, ingest_batch_id, tenant_id, device_id, exporter_id, remote_endpoint_side,
  remote_prefix_id, remote_prefix_cidr, dimension_snapshot_id,
  business_direction, category, business, geo_version, classification_version;

CREATE MATERIALIZED VIEW IF NOT EXISTS watchdog_flow.mv_flow_address_local_set_1m
TO watchdog_flow.flow_address_dimension_1m
AS SELECT
  ts, ingest_batch_id, tenant_id, device_id, exporter_id,
  'local' AS address_role, local_endpoint_side AS endpoint_side,
  'address_set' AS dimension_kind, dimension_id,
  '' AS prefix_cidr, 'tag' AS membership_mode,
  dimension_snapshot_id, business_direction, category, business,
  geo_version, classification_version,
  sum(estimated_bytes) AS estimated_bytes,
  sum(estimated_packets) AS estimated_packets,
  sum(sample_count) AS sample_count
FROM watchdog_flow.flow_pair_1m
ARRAY JOIN local_address_set_ids AS dimension_id
WHERE business_direction IN ('in', 'out')
GROUP BY ts, ingest_batch_id, tenant_id, device_id, exporter_id, local_endpoint_side,
  dimension_id, dimension_snapshot_id, business_direction, category,
  business, geo_version, classification_version;

CREATE MATERIALIZED VIEW IF NOT EXISTS watchdog_flow.mv_flow_address_remote_set_1m
TO watchdog_flow.flow_address_dimension_1m
AS SELECT
  ts, ingest_batch_id, tenant_id, device_id, exporter_id,
  'remote' AS address_role, remote_endpoint_side AS endpoint_side,
  'address_set' AS dimension_kind, dimension_id,
  '' AS prefix_cidr, 'tag' AS membership_mode,
  dimension_snapshot_id, business_direction, category, business,
  geo_version, classification_version,
  sum(estimated_bytes) AS estimated_bytes,
  sum(estimated_packets) AS estimated_packets,
  sum(sample_count) AS sample_count
FROM watchdog_flow.flow_pair_1m
ARRAY JOIN remote_address_set_ids AS dimension_id
WHERE business_direction IN ('in', 'out')
GROUP BY ts, ingest_batch_id, tenant_id, device_id, exporter_id, remote_endpoint_side,
  dimension_id, dimension_snapshot_id, business_direction, category,
  business, geo_version, classification_version;
```

写入规则：

- dimension worker 只把 local/remote prefix、endpoint side 和排序后的 set IDs 写进 `flow_pair_1m`；四个 address MV/rollup 分别派生 local/remote × prefix/set，不在 normalized 消费主循环复制事实；
- 每条 `in/out` flow 对 local 和 remote role 各产生一个 `primary_prefix` 聚合成员；`dimension_id` 为稳定 prefix ID，未命中为 `_unassigned`，`membership_mode=exclusive`；
- 每个命中的 address set 派生 `dimension_kind=address_set`、set ID、`membership_mode=tag`；`prefix_cidr` 为空；
- 查询必须显式过滤单一 `address_role`。`primary_prefix` 之间可相加并与该 role 总量对账；address set 之间不可相加；
- 名称、labels 和 selector 不复制进事实表，按 `dimension_snapshot_id+dimension_id` 从归档 bundle 解析，避免名称修改污染历史。

DDL 必须在目标 ClickHouse 版本的 CI 容器中真实执行，并验证六表、全部 MV、base→derived 完整性、primary-prefix 守恒、address-set 非加和、同一 normalized offset batch 重试不重计、派生表删除后可从 base 重建以及 retention 期内重放；仅靠静态 review 不算通过。生产必须由 migration 层按环境生成 Replicated/Distributed 变体，不在基础 DDL 中硬编码 Keeper 路径。

## 9. VictoriaMetrics：SNMP/系统与 Flow 管线运行指标

### 9.1 为什么 SNMP 适合 VM、Flow 明细不适合

SNMP 的主体是有限且稳定的资源时间序列：`tenant/device/port/direction/metric`。新增端口才新增序列，通信对象变化不会创建新 series；查询也以速率、P95、峰值和告警为主，所以现有 SNMP counter 存 VM 是正确边界。

Flow 的主体是动态通信事实。若把 src/dst IP、端口、ASN、地域和地址集合编码成 label，每个新通信组合都会创建序列，且很难在保留历史的同时改变分类版本。Flow 因而只把 pipeline 自监控写 VM，业务分钟事实和图表统一查 CH，避免双写和两套完整性判断。

### 9.2 可选 Flow recording cache（默认关闭）

| metric | labels | 类型/说明 |
|---|---|---|
| `watchdog_flow_in_bps` | tenant_id,device_id,category,business | 1 分钟估算 bps gauge |
| `watchdog_flow_out_bps` | 同上 | 1 分钟估算 bps gauge |
| `watchdog_flow_unknown_ratio` | tenant_id,device_id | unknown / 全部业务方向 flow |
| `watchdog_flow_counter_coverage_ratio` | tenant_id,device_id,port_id,direction,counter_source | `flow_estimated_bytes / counter_delta_bytes`；source 仅 `sflow\|snmp` |
| `watchdog_flow_counter_deviation_ratio` | tenant_id,device_id,port_id,direction,counter_source | flow 与接口 counter 同桶绝对偏差；无 counter 时不产生序列，不影响 flow 查询 |

上述五项不是 P1 必交付物。只有 CH 常用查询压测未达 SLO、ADR 记录收益/基数/一致性成本并启用 `full+recording-cache` 时才创建；数据必须从 CH 异步生成，不能加入 normalized offset 的成功条件。`business` 空值统一写 `_unassigned`。禁止把 IP、端口、ASN、城市或 error message 放入 VM label。

### 9.3 Flow-Collect、Kafka、dimension 与派生完整性自监控

flow-collect 和 dimension worker 都暴露 Prometheus `/metrics`。flow-collect 至少包括：

- `watchdog_flow_datagrams_received_total{protocol}`；
- `watchdog_flow_udp_kernel_drops_total{listener}`；
- `watchdog_flow_decode_queue_depth{listener}`；
- `watchdog_flow_kafka_produce_total{topic,result}`；
- `watchdog_flow_kafka_produce_latency_seconds{topic}`；
- `watchdog_flow_wal_bytes`、`watchdog_flow_wal_oldest_age_seconds`；
- `watchdog_flow_wal_fsync_latency_seconds`；
- `watchdog_flow_wal_dropped_total{reason}`；

- `watchdog_flow_records_decoded_total{protocol}`；
- `watchdog_flow_sampling_rate_changes_total{protocol}`；
- `watchdog_flow_sample_pool_resets_total{protocol}`；
- `watchdog_flow_samples_missing_rate_total{protocol}`；
- `watchdog_flow_sflow_exporter_drops_total{protocol}`；
- `watchdog_flow_decode_errors_total{protocol,reason}`（reason 固定枚举）；
- `watchdog_flow_sequence_gaps_total{protocol,scope}`（scope 仅 `datagram|sample`）；
- `watchdog_flow_normalized_records_total{protocol,result}`；
- `watchdog_flow_normalized_batch_bytes`、`watchdog_flow_normalized_batch_records`；

Kafka consumer、dimension worker 与存储至少包括：

- `watchdog_flow_kafka_consumer_lag{topic,partition}`；
- `watchdog_flow_kafka_offset{topic,partition,kind}`（kind=`consumed|committed`）；
- `watchdog_flow_kafka_rebalances_total{result}`；
- `watchdog_flow_dlq_total{reason}`；
- `watchdog_flow_late_records_total`；
- `watchdog_flow_sink_batches_total{sink,result}`；
- `watchdog_flow_base_derived_divergence{derived_table}`；
- `watchdog_flow_derived_rebuild_total{table,result}`；
- `watchdog_flow_sink_queue_depth{sink}`；
- `watchdog_flow_pairs_current{shard}`；
- `watchdog_flow_dimension_records_total{result}`；
- `watchdog_flow_dimension_unassigned_ratio{address_role}`；
- `watchdog_flow_dimension_set_expansion_ratio`；
- `watchdog_flow_dimension_snapshot_info{version}`；
- `watchdog_flow_dimension_snapshot_ack{worker_id,version}`；
- `watchdog_flow_degraded_intervals_total{reason}`；
- `watchdog_flow_geo_reload_total{result}`；
- `watchdog_flow_geo_version_info{version}`（值恒 1，仅一个 active）。

VPN 评分/probe 至少包括：

- `watchdog_flow_vpn_candidates_total{result}`、`watchdog_flow_vpn_findings_total{verdict,level,disposition}`；
- `watchdog_flow_vpn_evaluation_duration_seconds{result}`、`watchdog_flow_vpn_rule_preview_candidates`；
- `watchdog_flow_vpn_probe_jobs{mode,status}`、`watchdog_flow_vpn_probe_queue_oldest_age_seconds{mode}`；
- `watchdog_flow_vpn_probe_attempts_total{mode,profile,result}`、`watchdog_flow_vpn_probe_inflight{mode}`；
- `watchdog_flow_vpn_probe_scope_rejected_total{reason}`、`watchdog_flow_vpn_probe_result_rejected_total{reason}`；
- `watchdog_flow_vpn_active_probe_kill_switch`（1 表示停止主动派发）。

exporter_id/worker_id 基数受 registry 限制；未知 source IP、message ID、prefix ID、address set ID 和 offset range 不作为 VM label，避免高基数维度污染时序。partition 数量必须有平台上限；更细的 lag、snapshot 和 dimension/debug 信息进健康 API 或日志。

#### 9.3.1 flow-collect 指标与健康契约

`watchdog-flow-collect` 使用独立的 `flow_collect.observability.listen`，固定提供 `GET|HEAD /metrics`、`/health/live` 和 `/health/ready`。默认只监听 `127.0.0.1:9464`；跨主机 vmagent/Prometheus 抓取必须显式改监听地址并由管理网 ACL/mTLS sidecar 限制，不能把该端点直接暴露到业务网。HTTP 设置 header/write/idle/shutdown 超时和 16 KiB header 上限；这些端点不查询 MySQL、CH 或控制面，也不在 scrape 时扫描 WAL 全量记录。

启动恢复另暴露 `watchdog_flow_collect_state_restore_candidates`、`watchdog_flow_collect_state_restored` 和 `watchdog_flow_collect_state_restore_duration_seconds` 三个无 label gauge；前者是当前签名 plan 可接收且 tombstone/coalesce 后的 Kafka key 数，后者是本地与远端合并后真正恢复的 exporter/domain identity 数。恢复失败时 listener 根本不会开放，进程退出并由启动日志保留 partition/offset 诊断，因此不能把“服务尚未启动、指标不可抓”误判成健康的零值。

标签集合在代码中固定分配，不使用 map 动态创建 series：`protocol=unknown|sflow5|netflow5|netflow9|ipfix`，`listener=sflow|netflow|shared|workers`，`topic=normalized|collect_state|decode_dlq|quarantine`，Kafka `result=success|failure`，decode `reason=invalid_datagram|template_pending|decode_rejected|normalize_rejected`，sequence `scope=datagram|sample`，quality `operation=journal|checkpoint`。collector/exporter/source/tenant/target/device/IP/datagram/message/error text 均不进入该进程的 label；按实例归属由 vmagent 的静态 scrape target/relabel 提供。Kafka latency 和 WAL fsync latency使用固定 1ms–10s + `+Inf` Prometheus histogram；VM 以 `histogram_quantile` 计算 P95/P99，热路径只更新固定原子 bucket，不创建动态 series。

`/health/live` 仅证明 HTTP 进程仍响应。`/health/ready` 是数据接纳闸门：runner 未运行、WAL 不可写或发生 hard admission stop、quality journal 不可写/满、最近一次 collect-state 本地持久化失败，或任一固定 Kafka topic 最近一次 produce 失败时返回 `503 unavailable`；同一组件后续成功才恢复，其他 topic 的成功不能掩盖失败。WAL 到 soft watermark 或 quality journal 达 80% 时仍返回 200，但状态为 `degraded`，用于先告警和排空；不能把历史累计错误计数直接当成永久不健康。WAL `oldest_age` 是最老**保留 segment** 内首条记录的年龄上界，已 ACK 但尚未 rotation/reclaim 的 active segment 仍会计入，不能替代 Kafka consumer lag。

Linux listener 启用 `SO_RXQ_OVFL`，从每个 socket 的累计 cmsg 计算含 uint32 回绕的 delta；多 `SO_REUSEPORT` socket 只对 delta 求和。非 Linux 构建必须通过 `watchdog_flow_udp_kernel_drop_telemetry_supported=0` 明示“不支持”，不能把恒为 0 解释成无丢包。建议告警门槛如下：

| 信号 | warning | critical / 动作 |
|---|---|---|
| kernel/application UDP drop increase | 任一 1 分钟增量 > 0 | 连续 3 分钟 > 0；核对 socket buffer、CPU affinity、采样/PPS 与扩容 |
| receive/decode/quarantine queue | 连续 5 分钟 depth/capacity > 0.70 | 连续 1 分钟 > 0.90；禁止先扩大无界队列掩盖下游不足 |
| WAL | soft watermark 或 oldest age > 10 分钟 | hard watermark/不可写立即摘除 readiness；不得覆盖未 ACK segment |
| Kafka | failure 增量 > 0 或 produce P99 > 1 秒 | `watchdog_flow_kafka_ready=0` 或失败持续 1 分钟；保留 WAL 并排查 broker/ACL/TLS |
| decode/template | 5 分钟错误率 > accepted datagram 的 0.1% | > 1% 或单协议持续 template pending；检查 exporter/domain affinity 与模板生命周期 |
| quality journal | usage > 0.80 或 checkpoint failure > 0 | 不可写/满使 readiness=503；不得越过 quality durability barrier ACK WAL |

阈值是初始运维基线，容量验收后按设备采样配置和目标硬件修订；修订只改告警规则，不改变 counter 语义。抓取目标自身 `up=0` 由 vmagent/Prometheus 产生，不能由已不可达的 flow-collect 自报。

### 9.4 接口 counter 与对账

对账能力分三档，均不改变 AS/IP/地域/六类的计算：

| 档位 | counter 来源 | 能力与限制 |
|---|---|---|
| flow-only | 无 | 可完成全部流向维度和估算总量；页面标记“无独立总量基准” |
| sFlow in-band | sFlow generic interface counter sample | 无需 SNMP 凭据；能校验 flow sample 估算与设备接口累计量，但与 flow 共用 exporter/UDP 路径，独立性较弱 |
| SNMP independent | `ifHCInOctets/ifHCOutOctets`，缺失时退回 32 位 counter | 独立采集路径，最适合发现 sFlow exporter、网络传输或 collector 漏失；沿用现有计费口径 |

同一接口同一窗口只选择一个 `reconcile_counter_source`，sFlow counter 与 SNMP counter 绝不相加。sFlow counter adapter 输出：

```text
watchdog_sflow_if_in_octets_total{tenant_id,device_id,port_id}
watchdog_sflow_if_out_octets_total{tenant_id,device_id,port_id}
```

SNMP 复用已有 `watchdog_snmp_if_in_octets_total` 和 `watchdog_snmp_if_out_octets_total`。两者都是累计 counter，按相邻样本的**实际时间戳**计算 delta/rate；counter sample 不乘 flow sampling rate，CLI counter polling interval 也不进入放大公式。设备重启、`sysUpTime` 回退、无法解释的 counter 回绕、端口重新发现或桶不完整时，该窗口不产出 deviation。

对账基于观察方向，而非业务方向名称：

```text
observation_direction=ingress → 对比同 ifIndex 的 ifInOctets delta
observation_direction=egress  → 对比同 ifIndex 的 ifOutOctets delta
```

若产品要显示业务上/下行对账，必须先配置边界角色：外网侧端口通常 `business out→egress`、`business in→ingress`，内网侧端口通常相反。LAG 只允许选择逻辑口或成员口之和中的一个口径。完整 5 分钟窗计算：

```text
flow_bytes     = sum(estimated_bytes WHERE observation_if_index=i
                     AND observation_direction=d)
counter_bytes  = counter(t1) - counter(t0)
coverage       = flow_bytes / max(counter_bytes, 1)
signed_delta   = (flow_bytes - counter_bytes) / max(counter_bytes, 1)
deviation      = abs(signed_delta)
```

允许比较的共同范围默认是可解析并准入的 IP 流量。接口上的 ARP、STP、非 IP、设备自产/终结流量以及不同 L2/L3 字节口径会形成预期偏差，API 必须返回 `scope_notes`。对账只产生质量结论，不把 counter 总量按比例强行摊给各 AS/地域。

## 10. 关键查询

所有参数由驱动绑定；示例中的 `{name:Type}` 是 ClickHouse named parameter，不得字符串拼接。

以下 SQL 是**单一版本片段**的查询模板。执行前 QueryGateway 必须读取 active reclassification overlay，将 `[start,end)` 切成不重叠的分钟级片段，并为每个片段注入精确谓词：

```sql
(ts >= {segment_start:DateTime} AND ts < {segment_end:DateTime}
 AND dimension_snapshot_id = {snapshot_id:String}
 AND geo_version = {geo_version:String}
 AND classification_version = {classification_version:UInt32})
```

无 overlay 的片段使用当时 live version。多个片段分别查询后按时间拼接；不得省略版本谓词后直接汇总所有版本，否则历史重分类会与原 live rows 重复计量。基线查询只走 CH。

### 10.1 六类趋势

可选 VM recording cache 查询（默认关闭，不能用于 overlay/导出）：

```promql
sum by (category) (
  watchdog_flow_out_bps{tenant_id="tenant_dev", business=~".*"}
)
```

CH 基线查询：

```sql
SELECT
  toStartOfInterval(ts, toIntervalSecond({step:UInt32})) AS t,
  category,
  sum(estimated_bytes) * 8 / {step:UInt32} AS bps
FROM watchdog_flow.flow_category_1m
WHERE tenant_id = {tenant:String}
  AND ts >= {start:DateTime} AND ts < {end:DateTime}
  AND business_direction = {direction:String}
GROUP BY t, category
ORDER BY t, category;
```

占比在同一查询结果中按每个 `t` 的六类总和计算；unknown 不进六类分母，另返回质量比。

### 10.2 按省/机构/境外地区

```sql
SELECT
  toStartOfInterval(ts, toIntervalSecond({step:UInt32})) AS t,
  concat(substring(remote_admin_code, 1, 2), '0000') AS province,
  sum(estimated_bytes) * 8 / {step:UInt32} AS bps
FROM watchdog_flow.flow_remote_geo_1m
WHERE tenant_id = {tenant:String}
  AND ts >= {start:DateTime} AND ts < {end:DateTime}
  AND business_direction = {direction:String}
  AND category IN (
    'on_net_local_city','on_net_cross_city','on_net_cross_province'
  )
GROUP BY t, province
ORDER BY t, province;
```

- 异网机构：category 取两个 off-net，group by `geo_version,remote_isp_id`，服务端用对应版本的 `operators.json` 补名称后再合并同名展示项；
- 境外地区：category=`overseas`，group by `remote_country,remote_subdivision,remote_city`；
- 所有查询必须带 business/device filter，不能在聚合表缺维度后假装支持筛选。

### 10.3 主地址段与 Address Set

主地址段是互斥口径：

```sql
SELECT
  toStartOfInterval(ts, toIntervalSecond({step:UInt32})) AS t,
  dimension_id,
  any(prefix_cidr) AS prefix_cidr,
  sum(estimated_bytes) * 8 / {step:UInt32} AS bps
FROM watchdog_flow.flow_address_dimension_1m
WHERE tenant_id = {tenant:String}
  AND ts >= {start:DateTime} AND ts < {end:DateTime}
  AND address_role = {address_role:String}
  AND dimension_kind = 'primary_prefix'
GROUP BY t, dimension_id
ORDER BY t, bps DESC;
```

API 必须要求 `address_role=local|remote`，并在 meta 返回 `additive=true`、`dimension_snapshot_ids` 和 `_unassigned` 占比。同一 role、相同过滤条件下，所有 primary prefix（含 `_unassigned`）之和必须等于 category/base total。

address set 使用同一表过滤 `dimension_kind='address_set'`。一个 flow 可进入多个 set，响应固定 `additive=false` 和 `membership_mode='tag'`；前端不得显示“所有集合总计”或把集合占比画成总和 100% 的饼图。跨 snapshot 查询按稳定 set ID 合并，同时回显版本分布；审计/导出可用 `dimension_version` 固定复现。

### 10.4 源/目的 Top IP

```sql
SELECT
  endpoint_ip,
  sumIf(estimated_bytes, business_direction = 'out') * 8 / {range_s:UInt32} AS out_bps,
  sumIf(estimated_bytes, business_direction = 'in') * 8 / {range_s:UInt32} AS in_bps,
  sumIf(estimated_bytes, category = 'on_net_local_city') AS b_local,
  sumIf(estimated_bytes, category = 'on_net_cross_city') AS b_cross_city,
  sumIf(estimated_bytes, category = 'on_net_cross_province') AS b_cross_province,
  sumIf(estimated_bytes, category = 'off_net_in_province') AS b_off_in,
  sumIf(estimated_bytes, category = 'off_net_cross_province') AS b_off_cross,
  sumIf(estimated_bytes, category = 'overseas') AS b_overseas
FROM watchdog_flow.flow_endpoint_1m
WHERE tenant_id = {tenant:String}
  AND ts >= {start:DateTime} AND ts < {end:DateTime}
  AND endpoint_side = {side:String}
GROUP BY endpoint_ip
ORDER BY greatest(out_bps, in_bps) DESC, endpoint_ip
LIMIT {limit:UInt16};
```

先取 Top-N IP，再用 `endpoint_ip IN (...)` 批量取 sparkline，避免 N+1 查询。精确 CIDR 搜索应转 IPv6 范围条件；禁止对 IP 文本 `LIKE '%...%'`。

### 10.5 P95/峰值/平均

```sql
WITH buckets AS (
  SELECT
    toStartOfFiveMinutes(ts) AS t5,
    sum(estimated_bytes) * 8 / 300 AS bps,
    countDistinct(ts) AS minute_count
  FROM watchdog_flow.flow_category_1m
  WHERE tenant_id = {tenant:String}
    AND ts >= {start:DateTime} AND ts < {end:DateTime}
    AND business_direction = {direction:String}
  GROUP BY t5
)
SELECT
  quantileExact(0.95)(bps) AS p95,
  max(bps) AS peak,
  avg(bps) AS average,
  argMax(bps, t5) AS current,
  countIf(minute_count = 5) / count() AS complete_ratio
FROM buckets
WHERE minute_count = 5;
```

自定义时间边界不足 5 分钟的首尾桶排除。返回 `complete_ratio`，低于 0.95 时 UI 给数据不完整提示。

`quantileExact` 族的取位语义与现有 Go `Percentile(..., 95)`（nearest-rank）存在变体差异：实现必须用同一组 golden fixture 锁定 CH 函数选型——nearest-rank 对应 `quantileExactHigh` 一族，禁止使用 Excel 插值型 `quantileExactInclusive/Exclusive`——两端逐值一致后才冻结选型。`current` 取最后一个完整 5 分钟桶，与图表实时端点的差异在 UI 注明，不混用两种口径。

### 10.6 境外 KPI

```sql
SELECT
  sum(estimated_bytes) AS total_bytes,
  total_bytes * 8 / {range_s:UInt32} AS average_bps,
  uniqCombined64(remote_ip) AS distinct_remote_ips,
  uniqCombined64(local_ip) AS local_hosts
FROM watchdog_flow.flow_pair_1m
WHERE tenant_id = {tenant:String}
  AND ts >= {start:DateTime} AND ts < {end:DateTime}
  AND category = 'overseas';
```

7 天以上范围不能查询已过 TTL 的 pair 表；API 要么拒绝，要么只返回 remote Geo 聚合可计算的 KPI，并在 response 标记 `partial=true`。

### 10.7 VPN 评分

每 10 分钟对已关闭迟到窗口生成 canonical peer conversation。下面只计算可由 Flow 证明的行为特征，不把端口候选写成已确认协议：

```sql
WITH per_transport_bucket AS (
  SELECT
    local_ip,
    remote_ip,
    local_port,
    remote_port,
    ip_proto,
    toStartOfInterval(ts, INTERVAL 5 MINUTE) AS activity_bucket,
    sumIf(estimated_bytes, business_direction = 'out') AS local_to_remote_bytes,
    sumIf(estimated_bytes, business_direction = 'in') AS remote_to_local_bytes,
    sum(estimated_packets) AS packets,
    sum(sample_count) AS flow_record_count,
    max(flow_duration_ms_max) AS max_duration_ms,
    any(remote_country) AS remote_country,
    any(remote_asn) AS remote_asn,
    sum(estimated_bytes) AS transport_bytes
  FROM watchdog_flow.flow_pair_1m
  WHERE tenant_id = {tenant:String}
    AND ts >= {start:DateTime} AND ts < {end:DateTime}
    AND business_direction IN ('in', 'out')
  GROUP BY
    local_ip, remote_ip, local_port, remote_port, ip_proto, activity_bucket
), per_transport AS (
  SELECT
    local_ip,
    remote_ip,
    local_port,
    remote_port,
    ip_proto,
    sum(local_to_remote_bytes) AS local_to_remote_bytes,
    sum(remote_to_local_bytes) AS remote_to_local_bytes,
    sum(packets) AS packets,
    sum(flow_record_count) AS flow_record_count,
    max(max_duration_ms) AS max_duration_ms,
    any(remote_country) AS remote_country,
    any(remote_asn) AS remote_asn,
    sum(transport_bytes) AS transport_bytes,
    groupUniqArray(activity_bucket) AS active_buckets
  FROM per_transport_bucket
  GROUP BY local_ip, remote_ip, local_port, remote_port, ip_proto
), conversations AS (
  SELECT
    local_ip,
    remote_ip,
    argMax(
      tuple(local_port, remote_port, ip_proto),
      tuple(transport_bytes, ip_proto, remote_port, local_port)
    ) AS primary_transport,
    tupleElement(primary_transport, 1) AS primary_local_port,
    tupleElement(primary_transport, 2) AS primary_remote_port,
    tupleElement(primary_transport, 3) AS primary_ip_protocol,
    groupUniqArray((ip_proto, local_port, remote_port)) AS transport_set,
    sum(local_to_remote_bytes) AS local_to_remote_bytes,
    sum(remote_to_local_bytes) AS remote_to_local_bytes,
    sum(packets) AS packets,
    sum(flow_record_count) AS flow_record_count,
    max(max_duration_ms) AS max_duration_ms,
    any(remote_country) AS remote_country,
    any(remote_asn) AS remote_asn,
    arraySort(arrayDistinct(arrayFlatten(groupArray(active_buckets))))
      AS active_buckets,
    length(active_buckets) AS active_bucket_count,
    greatest(local_to_remote_bytes, remote_to_local_bytes) AS max_direction_bytes,
    least(local_to_remote_bytes, remote_to_local_bytes)
      / greatest(max_direction_bytes, 1) AS balanced_ratio
  FROM per_transport
  GROUP BY local_ip, remote_ip
), local_behavior AS (
  SELECT local_ip, uniqExact(remote_ip) AS remote_fanout
  FROM conversations
  GROUP BY local_ip
)
SELECT
  c.*,
  b.remote_fanout,
  arrayExists(t -> tupleElement(t, 1) = 17
    AND tupleElement(t, 3) = 443, transport_set) AS quic_candidate,
  arrayExists(t -> tupleElement(t, 1) = 6
    AND tupleElement(t, 3) = 443, transport_set) AS tls_candidate,
  multiIf(
    balanced_ratio <= {unidirectional_ratio:Float64}, 'unidirectional',
    balanced_ratio >= {balanced_ratio_threshold:Float64}, 'balanced',
    'asymmetric'
  ) AS directionality
FROM conversations AS c
INNER JOIN local_behavior AS b USING (local_ip)
WHERE max_direction_bytes >= {min_bytes:UInt64}
  AND packets >= {min_packets:UInt64}
  AND flow_record_count >= {min_flow_records:UInt64};
```

SQL 返回的有序 `active_buckets` 由 versioned 纯函数计算 gap 与 `periodicity_score`，并随 evidence 保存输入桶和算法版本；不足最小桶数输出 NULL。生产实现把 active rule-set 编译成有界 selector group，按 tenant/window 批量扫描并共享 feature rows，禁止每条规则单独扫描 ClickHouse；CIDR/ASN/route-class 命中在 versioned 内存索引完成。若不同规则的 selector scope 不同，`remote_fanout` 必须在各自 selector group 内计算，不能复用全租户 fanout。

评分顺序固定为 allow/suppress → selector → behavior → intelligence → probe trigger。相关信号按 `geo/transport/behavior/intelligence` family 取该 family 的最大或封顶分，不把 `TCP+443`、`tls_candidate`、`境外` 等相关条件无限重复加分；最终 score/level 阈值版本化。sFlow duration 不可靠或为 0 时不命中长连接规则。`balanced` 与 `unidirectional` 均可按规则加分，但单独命中不得超过 suspect 门槛。

候选先 upsert `flow_vpn_findings(verdict=heuristic)`；命中 `probe_trigger` 且未处于 cooldown 才创建异步 job。被动/主动结果按 analyzer version 与 finding window 合并：明确 SOCKS/HTTP CONNECT/WebSocket 等握手可升为 `confirmed_protocol`；TLS/QUIC 指纹、加密代理形态或情报交叉印证升为 `corroborated`；超时/拒绝/证据冲突为 `inconclusive`。probe 失败不删除 heuristic finding，也不反向修改 CH base。

### 10.8 raw、supplier、customer 三层值语义

CH 中保存的 flow 事实是 `raw`：按协议声明和每条样本 sampling rate 得到的 nominal 估算，任何页面、导出或修正规则都不能覆盖它。可选 VM recording cache 只是可重建副本，不是事实源。`sample_pool` 推导出的 transport-loss 候选属于采集质量诊断，不等于 supplier/customer 商务修正；如未来正式采用，必须先成为另一个显式事实字段/质量版本，不能偷偷改 raw。

flow DatasetProvider 先按相同过滤条件得到 raw bucket，再调用宿主 AdjustmentService：

```text
raw      = persisted_nominal_bucket
supplier = Apply(raw, approved supplier policy/version)
customer = Apply(raw, approved customer policy/version)
```

supplier 与 customer 都直接以 raw 为输入，第一阶段禁止 `customer=Apply(supplier, ...)` 的隐式级联。操作只允许确定性 decimal `scale/add/clamp`，规则按 tenant/dataset/resource/dimension filter 生效；响应、图表定义和导出 snapshot 必须携带 `value_layer`、`policy_id/policy_version`。查询 raw、supplier、customer 与配置/审批规则分别使用平台定义的分层 action；没有对应权限不能通过“同时返回三层”间接读取。

## 11. API 全表

canonical 路由挂 `/api/v1/modules/flow`；旧 `/api/v1/flow` 仅保留一个版本的兼容转发。时间参数统一：`time_mode=fixed|custom`、`window=1h|6h|12h|24h|7d|30d` 或 RFC3339 `start/end`、`step`、`timezone`，并接受 `value_layer=raw|supplier|customer`（默认 raw）。服务端从 AuthContext 强制 tenant，不接受客户端 tenant_id。

本节只列 flow 模块业务 API。用户/角色/permission、collector enrollment/binding、target、visualization、query 和 export task 的通用 API 由宿主平台提供，flow 通过 descriptor/provider 接入，不复制同名 endpoint。

VPN 权限在通用 action 上增加模块级 `vpn_rules_configure`、`vpn_evidence_read_sensitive`，并复用 `dispose_finding`、`probe_passive`、`probe_active`。主动探测不得由普通 `operate/admin` 隐式获得；规则配置、finding 人工处置、创建 job、探针领取、结果提交、查看完整 IP/指纹和导出分别鉴权。所有 probe 响应返回 `mode/scope_policy_id/analyzer/version/verdict_definition`，不能只返回一个模糊 `detected=true`。

### 11.1 查询 API（ActionView）

| Method/route | 关键参数 | 返回 |
|---|---|---|
| GET `/health` | 无 | flow-collect/WAL、normalized lag、dimension workers、snapshot ack、CH base/derived、pipeline VM/Geo/exporter freshness、active versions、degraded intervals |
| GET `/summary` | 时间、business、device | flow total、可选 counter baseline/source/deviation、六类卡片、unknown、active exporters；无 counter 时仍返回 200 |
| GET `/reconciliation` | 时间、device、port、counter_source | 完整窗口的 flow/counter bytes、coverage、signed/absolute deviation、有效性和 scope notes |
| GET `/series` | `group_by=category\|primary_prefix\|address_set\|business\|province\|org\|overseas_region\|vpn_type`、address role、direction、mode、peak/business/device | series、stats、completeness、source、dimension versions、additive |
| GET `/address-dimensions` | `kind=primary_prefix\|address_set`、`address_role=local\|remote`、version、cursor/limit、时间/筛选 | 地址段/集合流量、unassigned、snapshot、`additive`、next cursor |
| GET `/address-dimensions/status` | 无 | active/draft snapshot、effective time、worker ack、normalized lag、可回算范围 |
| GET `/business-matrix` | 时间、device | 业务 × 六类值/占比 |
| GET `/endpoints` | `side=src\|dst`、cursor、limit、q、business、category、direction | Top IP 行、总估计、next_cursor |
| GET `/endpoints/{ip}` | side、时间 | 时序、六类、Top 对端、Top 端口、当前 Geo |
| GET `/overseas/summary` | 时间/筛选 | total/avg/distinct remote/local hosts/vpn share |
| GET `/overseas/series` | direction、时间 | 出入境趋势 |
| GET `/overseas/regions` | direction、level、limit | 国家/省州/城市分布 |
| GET `/overseas/ports` | direction、port_side、limit | 端口/协议和 VPN rule 标记 |
| GET `/vpn/summary` | window、verdict、disposition、probe_status | KPI、heuristic/corroborated/confirmed_protocol 与人工处置分布、趋势和 probe backlog |
| GET `/vpn/hosts` | `level/verdict/disposition/directionality`、`remote_fanout_gte`、`periodicity_score_gte`、src/dst geo、ASN/address set、probe_status、cursor/limit | finding、双向 ratio、duration/fanout/periodicity、transport set、score/verdict/disposition 和证据摘要 |
| GET `/vpn/findings/{id}` | `include_sensitive` | conversation、版本、全部规则/行为/情报/probe 证据、attempt timeline；敏感字段单独鉴权 |
| GET `/vpn/ports` | direction、transport、limit | 候选/已观察协议、端口/transport/类型分布，明确 candidate 与 confirmed |
| GET `/vpn/probe-jobs` | mode/status/finding/cursor | job 列表、queue/attempt/result/expiry，不返回原始 payload |
| GET `/vpn/probe-jobs/{id}` | 无 | 固化 scope/profile/analyzer、checkpoint、签名结果和错误码 |
| GET `/geo/lookup` | ip | 文件结果、租户 override、effective、version |
| GET `/geo/status` | 无 | path/schema/version/generated_at/checksum/load time/rows/error |

`difference` 固定输出 `out-in` signed bps，response 始终返回 `definition:"out_minus_in"`，禁止客户端自行猜测。

`group_by=vpn_type` 不来自 CH 事实表：由 finding snapshot（MySQL）按 verdict/level 聚合，分辨率为 finding 窗口，`meta.source="mysql_findings"`，不支持任意 step 重采样；其余 group_by 均由 CH provider 服务。

### 11.2 管理 API（ActionConfigure）

| Method/route | 说明 |
|---|---|
| GET/POST `/exporters` | 列表/手工创建；POST 校验 collector binding、target、tenant 和唯一 identity |
| GET/PATCH/DELETE `/exporters/{id}` | 绑定 collector/target/device、status、按 data source/sampler 的采样 override、接口 allowlist、counter source；PATCH 要求 `If-Match`，DELETE 仅 retired/suspended 且先做影响预览，默认 tombstone |
| POST `/exporters/{id}:validate\|activate\|suspend\|resume\|retire\|restore\|purge` | 显式状态动作；activate 必须通过绑定/采样/观察点验证，purge 需审批、恢复期届满和 destruction receipt |
| GET/PUT `/settings` | home profile、HMT、internal/transit policy |
| POST `/settings/auto-detect` | 生成候选，不改有效配置 |
| POST `/settings/apply` | 应用指定候选并递增 classification version |
| POST `/geo/reload` | 立即重读 `current`；不能上传任意服务器路径 |
| GET/POST `/geo-overrides` | 过滤/merge 写 `address_prefixes` 的 `flow.geo.*`，事务性递增 classification version |
| DELETE `/geo-overrides/{prefix_id}` | 只删除 Geo labels，保留同 CIDR 其他 labels，并递增 classification version |
| GET/POST `/reclassifications` | 列表/创建历史重分类；创建时冻结时间、source、目标 dimension/Geo/classification version |
| GET `/reclassifications/{id}` | 状态、checkpoint、输入/输出、验证、错误和可激活性 |
| POST `/reclassifications/{id}/pause`、`resume`、`cancel` | 幂等控制任务；已经 active 的 run 不能 cancel |
| POST `/reclassifications/{id}/activate`、`retire` | 只有 validated 可激活；同 tenant 的 active range 不得重叠；全程审计 |
| GET/POST/PATCH/DELETE `/vpn-rules` | 规则 CRUD，PATCH 要求 `If-Match`；内置规则以 seed 标识但可 suspend，DELETE 默认 tombstone |
| POST `/vpn-rules/{id}:activate\|suspend\|retire\|restore\|purge` | 规则显式状态动作；activate 生成不可变 rule-set version，purge 受 finding/审计保留约束 |
| POST `/vpn-rules/{id}:preview` | 对冻结时间窗 dry-run，返回候选/预计 probe/Top ASN/prefix/成本和 false-positive sample，不写 finding |
| POST `/vpn/evaluate` | 手动触发一次有并发锁、版本和 checkpoint 的评分任务 |
| POST `/vpn/findings/{id}:probe` | `mode/profile/reason`；创建幂等 probe job。active_handshake 额外校验 `probe_active` action、审批、scope policy、quota 和 kill switch |
| POST `/vpn/findings/{id}:acknowledge\|annotate` | acknowledge 幂等更新 disposition；annotation 作为不可变、有界 audit event 追加；均不修改 heuristic/probe evidence、score 或 verdict |
| POST `/vpn/findings/{id}:set-disposition` | `authorized\|false_positive\|investigating\|resolved` + reason；要求 `If-Match`、`dispose_finding` 并审计，不能删除历史 finding |
| POST `/vpn/findings/{id}:create-suppress-rule` | 从 finding 生成 draft suppress rule，必须 preview/审批/activate，不能直接隐藏历史 finding |
| POST `/vpn/probe-jobs/{id}:cancel` | 幂等取消 queued/running probe；agent 已发出的单次握手按 deadline 停止 |
| POST `/vpn/probe-results` | collector mTLS/签名专用；提交 attempt/analyzer/version/fingerprint/confidence/stop reason，拒绝浏览器和普通用户 token |

地址前缀原 CRUD 继续存在，但流向设置 UI 应调用模块的 `/geo-overrides` 服务层，避免前端错误覆盖 labels。

### 11.3 导出 API（ActionExport）

flow 向平台 DatasetRegistry 注册 `flow.series/endpoints/overseas/vpn_findings`，复用 `/api/v1/exports`：

- `dataset=flow.series|flow.endpoints|flow.overseas|flow.vpn_findings`；
- `query_snapshot` 固化筛选、value layer、Geo/classification/adjustment version；
- P1 先支持 CSV，P3 再加 XLSX；当前代码只接受 CSV，不能把截图“Excel”当已复用；
- 平台导出 worker 通过同一 DatasetProvider 读取，不复制查询 SQL；
- 文件下载继续走 tenant/创建者、分层 export action 和审计约束。

### 11.4 响应约定

```json
{
  "data": {
    "series": [{"key":"overseas","name":"境外流量","points":[[1788336000,508100000]]}],
    "stats": {"current":508100000,"p95":520000000,"peak":610000000,"average":490000000}
  },
  "meta": {
    "unit":"bps",
    "step":60,
    "source":"clickhouse",
    "geo_versions":["2026-09-02T120000Z-a13f9c2e"],
    "classification_versions":[3],
    "dimension_snapshot_ids":["01..."],
    "additive":true,
    "value_layer":"supplier",
    "adjustment":{"policy_id":"01...","version":4},
    "complete_ratio":0.998,
    "partial":false
  }
}
```

错误沿用 `APIError{code,message,details}`。CH 不可用时 Flow 业务查询返回 503（仅获批 recording cache 的有限 summary 可显式降级）；VM 不可用不影响 CH Flow 查询，但会影响 SNMP/系统指标和 pipeline 监控；参数、范围和序列上限返回 400；无权限返回 403。

flow 模块的稳定错误码（进入 API contract，客户端按 code 分支，不解析 message）：

| code | HTTP | 场景 |
|---|---:|---|
| `FLOW_PROVIDER_UNAVAILABLE` | 503 | CH 不可用且无获批降级路径 |
| `FLOW_GEO_SNAPSHOT_UNAVAILABLE` | 503 | 无有效 GeoIndex（初装未发布，或加载失败且无旧版本可用） |
| `FLOW_DIMENSION_VERSION_PENDING` | 503 | 查询窗口存在尚未全部 worker ack 的 snapshot 版本且调用方要求 `require_complete=true` |
| `FLOW_RANGE_OUTSIDE_RETENTION` | 400 | 请求范围超出对应表 TTL；details 携带 `available_from/available_to` |
| `FLOW_ADDRESS_SET_NOT_ADDITIVE` | 400 | 客户端对多个 address set 请求求和类聚合 |
| `FLOW_QUERY_LIMIT_EXCEEDED` | 400 | series/points/range/cost 超 §14.5 上限；details 回显限额 |
| `FLOW_EXPORTER_IDENTITY_CONFLICT` | 409 | 创建/恢复 exporter 与既有 live identity 冲突 |
| `FLOW_SAMPLING_MODE_UNCONFIRMED` | 409 | NetFlow/IPFIX exporter 在 `counter_mode=auto` 下尝试 activate |
| `FLOW_RECLASS_RANGE_OVERLAP` | 409 | 重分类 range 与同租户 active overlay 重叠 |
| `FLOW_PROBE_KILL_SWITCH_ENGAGED` | 409 | 任一 kill switch 生效期间请求主动探测 |

新增错误码只能追加，不得复用或改语义；每个 code 的 details 字段结构随 API contract 冻结。

## 12. 六个界面

| 路由 | 页面 | 布局与行为 |
|---|---|---|
| `/flow` | 流向总览 | flow 总上/下行+趋势；六类 3×2 卡片；占比饼图；unknown；有 counter 时显示对账、无 counter 时显示“估算值/未配置独立基准”；业务×六类表；active exporter 状态 |
| `/flow/charts` | 分析图表 | 时间/模式/六类/主地址段/address set/省/机构分组；role/版本/高峰/业务/设备筛选；set 非加和提示；上下行双图和导出 |
| `/flow/ips` | 源/目的 IP | side tab、搜索、Top-N、业务/类别筛选；速率与六类 sparkline；详情抽屉；修正归属地 |
| `/flow/overseas` | 境外专题 | 四 KPI、出入境趋势、端口/协议、地区、业务/设备筛选；疑似 VPN 标记 |
| `/flow/vpn` | VPN 风险 | KPI、趋势、主机/peer finding、境内外/ASN/address-set/port/transport 筛选、双向 ratio、heuristic/corroborated/confirmed_protocol 与人工 disposition 分层、证据 timeline、probe queue/触发/取消、处置和导出；“确认协议”不展示为“确认恶意 VPN” |
| `/settings/flow` | 流向设置 | 开通向导、WAL/normalized lag、flow-collect/dimension/base-derived 健康、exporter/采样、counter 对账、prefix/set draft-preview-publish、worker ack、Geo/home、VPN rule builder/preview、情报版本、probe agent capability/软件版本/plan diff/ack/LKG/expiry/canary/rollback、被动/主动 scope/quota/双 kill switch |

前端要求：

- 使用现有 `pb.send`/backend base URL，禁止硬编码 CH/VM/VLogs；
- 六类颜色 token 全局固定；unknown 使用警告色但不混入六类饼图；
- `formatBitsPerSecond/formatBytes` 统一单位；share y 轴 0–100%，difference 允许负轴；
- 大表用 vtable/cursor，不一次加载所有 IP；
- legend 开关后统计条只计算已选序列；
- 所有图表空态区分“无流量”“依赖不可用”“筛选后为空”“数据不完整”。

## 13. 配置

宿主模块、flow-collect 和 dimension worker 分开配置；tenant/exporter binding 由 enrollment plan 下发，不再在 flow-collect YAML 固定 `tenant_id`：

```yaml
modules:
  flow:
    enabled: true

flow_collect:
  control_plane_url: "https://watchdog.example.com"
  state_dir: "/var/lib/watchdog-flow-collect"
  plan_file: "/var/lib/watchdog-flow-collect/plan.json"
  plan_public_key_file: "/etc/watchdog/flow-plan-ed25519.pub"
  sflow_listen: ":6343"
  netflow_listen: ":2055"
  socket_count: 8
  decode_workers: 16
  decode_queue_datagrams: 262144
  receive_buffer_bytes: 33554432
  max_datagram_bytes: 65535
  plan_refresh_interval: 30s
  plan_history_max_entries: 128
  decoder_state_ttl: 30m
  wal:
    max_bytes: 1073741824000
    max_age: 24h
    segment_bytes: 134217728
    fsync_interval: 10ms
    soft_watermark: 0.70
    hard_watermark: 0.90
  kafka:
    brokers: ["kafka-1:9093", "kafka-2:9093", "kafka-3:9093"]
    normalized_topic: "watchdog.flow.normalized.v1"
    collect_state_topic: "watchdog.flow.collect-state.v1"
    collect_state_restore_timeout: 2m
    collect_state_restore_max_candidates: 262144
    decode_dlq_topic: "watchdog.flow.decode-dlq.v1"
    quarantine_topic: "watchdog.flow.quarantine.v1"
    acks: "all"
    compression: "zstd"
    tls: true
    tls_ca_file: "/etc/watchdog/kafka/ca.crt"
    tls_cert_file: "/etc/watchdog/kafka/flow-collect.crt"
    tls_key_file: "/etc/watchdog/kafka/flow-collect.key"
    tls_server_name: "kafka.watchdog.internal"
    topic_contract:
      check_timeout: 10s
      normalized_partitions: 96
      collect_state_partitions: 32 # 精确值；当前 compacted key hash topic 禁止原地扩容
      decode_dlq_partitions: 12
      quarantine_partitions: 12
      min_replication_factor: 3
      min_in_sync_replicas: 2
      normalized_min_retention: 168h
      collect_state_delete_retention: 24h
      decode_dlq_min_retention: 720h
      quarantine_min_retention: 168h
  normalized_batch:
    max_records: 1024
    max_bytes: 1048576
    max_wait: 5ms
  diagnostics:
    decode_max_attempts: 3
    retry_initial: 250ms
    retry_max: 5s
    attempt_journal_fsync_interval: 10ms
    attempt_checkpoint_interval: 5m
    attempt_journal_max_bytes: 268435456
    quarantine_queue_events: 4096
    quarantine_max_events_per_second: 100
    quarantine_max_events_per_source_second: 2
    dlq_payload_max_bytes: 0 # 默认不把 raw payload 写入 DLQ
  quality:
    state_ttl: 1h
    anomaly_window: 1m
    journal_fsync_interval: 10ms
    checkpoint_interval: 5m
    journal_max_bytes: 536870912
    max_exporters: 65536
    max_data_sources: 262144
  observability:
    listen: "127.0.0.1:9464"
    read_header_timeout: 2s
    write_timeout: 10s
    idle_timeout: 30s
    shutdown_timeout: 5s
  exporter_refresh_interval: 30s

flow_dimension:
  kafka:
    brokers: ["kafka-1:9093", "kafka-2:9093", "kafka-3:9093"]
    normalized_topic: "watchdog.flow.normalized.v1"
    checkpoint_topic: "watchdog.flow.dimension-checkpoint.v1"
    consumer_group: "watchdog-flow-dimension-v1"
    tls: true
  workers: 8
  max_poll_records: 10000
  snapshot_refresh_interval: 30s
  max_address_sets_per_record: 32

  geo:
    path: "/var/lib/watchdog/flow-geo/current"
    schema: "flow-geo-v1"
    poll_interval: 5m
    max_uncompressed_bytes: 2147483648

  clickhouse:
    dsn: "clickhouse://127.0.0.1:9000/watchdog_flow"
    max_batch_rows: 10000
    max_batch_wait: 1s
    derived_mode: "attached_mv" # attached_mv 或 worker，禁止同时启用
    materialized_views_ignore_errors: true
    derived_repair_interval: 30s
    derived_max_missing_batches: 10000

  aggregate:
    bucket: 1m
    late_tolerance: 2m
    shards: 16
    max_pairs_per_minute: 2000000

  debug_capture:
    enabled: false
    sample_ratio: 0.0001
    directory: "/var/lib/watchdog/flow-debug"
    max_bytes: 1073741824
    ttl: 24h

flow_vpn:
  evaluation_interval: 10m
  finding_window: 24h
  activity_bucket: 5m
  min_periodicity_buckets: 4
  balanced_ratio_threshold: 0.70
  unidirectional_ratio_threshold: 0.05
  min_complete_ratio: 0.95
  max_selector_groups_per_run: 64
  max_candidates_per_tenant_per_run: 10000
  probes:
    passive_observe_enabled: false
    active_handshake_enabled: false
    max_queued_per_tenant: 100
    max_concurrency_per_tenant: 2
    max_attempts: 1
    timeout: 3s
    result_max_bytes: 65536
    active_global_kill_switch: true
    require_scope_policy: true
    require_approval: true
    allowed_profiles:
      ["socks4", "socks5", "http_connect", "websocket", "tls_vpn",
       "encrypted_proxy_fingerprint"]

flow_probe_agent:
  control_plane_url: "https://watchdog.example.com"
  state_dir: "/var/lib/watchdog-flow-probe"
  identity_cert_file: "/etc/watchdog-flow-probe/agent.crt"
  identity_key_file: "/etc/watchdog-flow-probe/agent.key"
  plan_trust_bundle_file: "/etc/watchdog-flow-probe/plan-signing-ca.pem"
  plan_refresh_interval: 30s
  local_emergency_kill_switch: true
  result_spool:
    max_bytes: 1073741824
    max_age: 24h
  passive:
    local_enabled: false
    capture_interfaces: []
    max_snaplen: 512
    max_concurrency: 8
  active:
    local_enabled: false
    egress_interface: ""
    allowed_cidrs: []
    max_concurrency: 2
    max_attempts_per_job: 1
    max_timeout: 3s
  analyzers:
    builtin_handshake:
      enabled: true
    ndpi:
      enabled: false
      required_version: ""
```

四个 topic 的生产基线如下；表中 partition 是默认容量起点，不替代目标环境压测。`max.message.bytes` 的校验值包含 64KiB record/batch envelope 余量，实际 topic 可以更大但不能更小：

| topic 角色 | partition | cleanup | retention | RF / min ISR | 最小 message bytes |
|---|---:|---|---|---:|---:|
| normalized | ≥96 且覆盖所有 retained plan map | `delete` | ≥7d | 3 / 2 | `normalized_batch.max_bytes + 65536` |
| collect-state | =32，服务期内不可变 | `compact` | tombstone `delete.retention.ms` ≥24h | 3 / 2 | `64MiB + 65536` |
| decode-DLQ | ≥12 | `delete` | ≥30d | 3 / 2 | `max_datagram_bytes + 65536` |
| quarantine | ≥12 | `delete` | ≥7d | 3 / 2 | `max_datagram_bytes + 65536` |

所有 topic 还必须显式或通过有效默认值满足 `unclean.leader.election.enable=false`。首次部署用只存在于受控运维环境的 admin 证书执行 `watchdog-flow-kafka-bootstrap --config /path/to/admin.yaml --apply`；它只创建缺失 topic，发现已有 topic 时绝不 alter/扩分区/降 retention，随后用同一运行时契约复核。日常可去掉 `--apply` 做只读检查。admin YAML 复用相同 topic/plan 参数，但证书必须是短期 provisioning principal；长期 flow-collect YAML 随后换回 runtime mTLS 证书。任何既有不一致走显式变更单和新 topic 迁移，不能让 bootstrap 自动“修复”生产数据结构。

首次 enrollment secret 通过仅 root 可读文件或进程 secret 注入，只用于 exchange，成功后删除；不能放进长期 YAML。Kafka principal/证书由 plan/secret store 引用，文档示例不包含密码。环境变量分别采用 `WATCHDOG_FLOW_COLLECT_*` 和 `WATCHDOG_FLOW_DIMENSION_*` 前缀。旧 `sflow_collector` 配置保留一个版本并在启动日志提示迁移；新旧 collector 不得同时监听同一端口。示例 worker/WAL 数值只用于说明字段，生产值必须由容量表和压测报告生成配置变更，不允许照抄。

VPN rule/情报/scope/阈值的租户值存 MySQL 并版本化，不允许租户通过 YAML 绕过审批。全局 YAML 和 agent 本机配置只定义硬安全上限；服务端 `active_global_kill_switch=true`、agent `local_emergency_kill_switch=true` 或 `active.local_enabled=false` 任一成立都禁止主动握手。`allowed_cidrs=[]` 表示本机不允许任何主动目标，不是 unrestricted。probe agent 通过 CollectorRegistry 声明 `passive_observe`、`active_handshake` 和 analyzer/profile capabilities，plan 只下发它支持且被批准的 profile。

### 13.1 Probe agent 的配置、维护与扩展

probe agent 复用平台文档 §6 的 collector/plan revision，不建立模块私有注册表。边界固定为：watchdog 负责候选、授权、调度、最终 verdict 和审计；agent 只执行已签名 job、生成受限观察证据并可靠回传。

#### 13.1.1 配置模型与原子应用

有效参数取以下交集，任何一层都只能进一步收紧：

```text
effective = local hard limits ∩ signed active plan ∩ immutable job
```

签名 plan 至少包含 `plan_schema_version/config_version/spec_hash/not_before/expires_at`、tenant/binding、network vantage、允许的 mode/analyzer/profile/version、scope CIDR/port、quota、evidence policy 和 emergency flags。profile 只携带 analyzer schema 允许的安全参数；目标 IP/端口只存在于 job，不能写进长期 profile。agent 应用流程为 `download → signature/hash → schema/compatibility → scope/resource validation → analyzer self-test → fsync staging → atomic rename → heartbeat ack`。任一步失败保留 last-known-good，并上报 stable error code、失败阶段和 rejected version。

本地状态目录只包含：当前/LKG plan、身份引用、已领取 job 的有限 checkpoint 和有界 result spool。禁止缓存租户规则全集、明文 secret 或默认保存 payload。plan expiry 后：passive plan 停止新观察；active plan 不领取新 job；inflight job 只运行到固化 deadline。scope 收缩、revoke 或任一 kill switch 立即停止领取并对不再合法的 job 发出 cancel。

#### 13.1.2 Analyzer 与 profile 扩展点

共享 SDK 使用平台 §6.3 的 `ProbeAnalyzer` 契约。内置 `builtin_handshake` 实现 SOCKS/CONNECT/WebSocket 等受控握手；nDPI 作为可选 passive analyzer。v1 只允许构建期静态注册或版本固定的 cgo adapter，不支持从控制面下载 `.so`、Go plugin、脚本或任意发包 DSL。确有非 Go 依赖时，可使用同机 Unix socket sidecar，但必须：

- 以非特权用户运行，固定制品 digest/SBOM/签名和 adapter API；
- 不持有 watchdog 用户 token、MySQL/Kafka/CH 凭据，只接受单 job 的最小输入；
- 有独立 CPU/RSS/FD/time/result-size limit，退出或超时只使该 attempt 失败；
- passive 抓包 privilege 放在最小 capture helper，整个 agent 不以 root 运行；active analyzer 不获取 capture privilege。

profile 使用严格 JSON Schema，拒绝未知 key，并固定 `profile_key/profile_version/analyzer_version/result_schema_version`。可配置 timeout、snaplen、证据字段、有限协议选项；握手字节、状态机和 response parser 只能随已签名代码版本升级。agent 返回事实、fingerprint、confidence 和 `verdict_candidate`，不能直接写 finding 或输出“恶意”结论。

扩展一个 analyzer 的完成定义为：descriptor/manifest、配置 schema、golden pcap/handshake、普通业务误报集、无凭据/超时/取消 fixture、资源上限、兼容矩阵、SBOM/签名和 runbook 全部通过。发布走 `experimental → canary → GA → deprecated → removed`；job 固化版本，所以升级不能改变已领取 job 的解释。

#### 13.1.3 Job、崩溃恢复和幂等

agent 在任何网络动作前 durable 写入 `attempt_id/job_id/plan_version/profile_version/state=prepared`。主动 socket 首次发包前切为 `network_started`；若进程在该状态崩溃且没有结果，重启后提交 `unknown_after_crash`，不得自动重放主动握手，只有服务端在 quota/cooldown 内批准新 attempt 才可再次连接。passive observation 可从 checkpoint 恢复到原 deadline，但结果必须标记 observation gap。

结果使用 `job_id + attempt_id` 幂等提交，签名并携带 agent boot ID、plan/analyzer/profile/result schema version、started/finished、network vantage、stop reason、truncated 和 evidence hash。控制面不可达时进入有界 spool；spool 达软水位停止低优先级 passive job，达硬水位停止领取所有新 job，不能丢旧结果后继续制造新结果。`max_age` 是停止接单和告警阈值，不是删除未确认结果的 TTL；只有服务端幂等确认或明确 destruction policy 才能回收 spool record。

#### 13.1.4 维护、灰度和回滚

watchdog 管理 plan 与兼容性，不承担二进制分发；制品由现有 systemd/Kubernetes/配置管理发布。升级顺序为：停止领取 → active inflight 到 deadline/取消 → flush spool → 升级 → analyzer self-test/golden smoke → heartbeat 新 capability → canary plan → 扩大 rollout。平台只在 agent API、plan schema、analyzer/profile 和 result schema 交集兼容时派发 job。

canary 由 collector label/版本筛选，公共 rollout job 记录目标数、已应用、拒绝、健康和回滚阈值。回滚不是降低 config version，而是复制旧 spec 形成新的递增 revision。数据库/API/结果 reader 至少兼容一个发布窗口内的旧 agent/result schema；移除旧版本前必须证明无存量 job、spool 或在线 agent 使用。

Collector 详情增加 `能力/配置版本/版本 diff/灰度/Job/诊断` tabs。允许 preview、publish、activate、rollback、suspend/revoke；不提供“在线编辑 agent 文件”或“上传插件”。诊断包只含版本、hash、指标、限长日志和脱敏错误，不含私钥、payload、完整远端 IP 或 tenant 数据。

#### 13.1.5 指标与告警

- `watchdog_probe_agent_config_version`、`watchdog_probe_agent_ack_config_version`、`watchdog_probe_agent_plan_apply_total{result,stage}`、plan age/expiry；
- `watchdog_probe_agent_capability_info{analyzer,version,mode}`、capability drift/unsupported job；
- job queued/prepared/network_started/inflight/completed、duration、cancel、deadline、`unknown_after_crash`；
- analyzer execution/result/error/timeout、evidence truncated、scope/profile/result signature rejection；
- spool bytes/oldest age/dropped（目标恒为 0）、CPU/RSS/FD、capture drops 和 active socket 数；
- active 本机/服务端 kill switch、plan expiry 和 scope rejection 必须有独立告警，不能合并成普通 probe failure。

analyzer/profile/version 属受控低基数 catalog；job/finding/IP 不进入 VM label。健康页同时显示 desired config、ack/LKG version、capability hash、软件版本、plan expiry、spool 和最近 self-test。

### 13.2 生产拓扑与 N+1

```text
Exporter ──UDP VIP/ECMP（exporter affinity）──▶ flow-collect × N+1
                                                   │ RAID1/NVMe raw WAL
                                                   ▼
                                       Kafka brokers ≥3, RF=3
                                                   ▼
                                      dimension workers × M+1
                                                   ▼
                         ClickHouse replicated base + derived
                                                   │
                         existing VM ◀── SNMP/system/pipeline metrics
```

- `N/M` 由 §14.1 的认证单实例吞吐公式计算，任何单实例退出后剩余实例仍必须覆盖批准峰值；
- sFlow 可以按 exporter/sub-agent 分片；NetFlow v9/IPFIX 必须按 exporter + observation-domain 保持 affinity；
- template/sampler/sequence checkpoint 在本地持久化并发布 compacted `collect-state`，新 owner 先恢复状态再处理其 WAL/live queue；
- WAL 使用独立 RAID1/NVMe、预留空间和文件系统告警，不与日志、CH 或容器 overlay 共盘；
- 滚动升级顺序：移出 VIP → 停止新 datagram → WAL/Kafka pending 清零 → 发布 collect-state → 停进程 → 升级 → fixture self-check → 加回 VIP；
- 强制故障无法 drain 时，VIP 切换后新 owner 从 collect-state 恢复；旧节点未 Kafka ack 且不可访问的 WAL 构成明确 RPO 风险窗口，不得宣称 UDP 绝对零丢失。

VPN 探测是旁路低容量支线，不进入上述接收主链：

```text
CH candidate query → watchdog async job/plan → registered probe agent
                                            ├─ passive_observe（镜像/TAP）
                                            └─ active_handshake（默认 kill switch）
                         signed result spool → finding evidence merge/MySQL
```

候选/探针/结果服务全部不可用时，flow 收数、CH base、六维和 flow-only heuristic 继续运行；只把 probe backlog/verdict freshness 标为 degraded。

### 13.3 操作手册与恢复目标

| 事件 | 自动行为 | 人工操作 | 恢复验证 |
|---|---|---|---|
| flow-collect 进程失败 | supervisor 重启、WAL/template 恢复 | 超 60s 切 VIP/隔离节点 | ID 连续、WAL pending 归零、无新增 gap |
| 单 collector 节点失败 | affinity 转移到 N+1 owner | 检查旧 WAL 可恢复性和 data-loss interval | exporter active、template warm、records 恢复 |
| Kafka 单 broker 失败 | RF/minISR 继续服务 | 修复 broker，禁止降 `acks` | ISR 恢复、WAL oldest age 回落 |
| Kafka 集群不可写 | flow-collect 持续写 WAL | 扩 WAL/修复 Kafka，禁止清 WAL | 顺序 replay、checksum/ID/总量一致 |
| dimension/CH 失败 | normalized lag 上升、停止 backfill | 修复/扩 worker 或 CH | lag 清零、base batch 无重复 |
| 派生表 divergence | 标 partial 或查询 base，按缺失 `(ingest_batch_id,target)` 排队 repair | 幂等补 batch；批量损坏时 staging + `REPLACE PARTITION` | base=derived、checkpoint complete |
| Geo/snapshot 发布失败 | 保持旧 active version | 修复 bundle 后重发，不原地改版本 | worker ack、event-time 边界正确 |
| probe plan 应用失败/过期 | 保持 LKG；active 过期后停止新 job | 查看 version diff、兼容/签名/scope/self-test 错误，修复或发布递增 rollback revision | ack=LKG/新 version、无越权 job、告警清除 |
| analyzer crash/capability drift | 隔离该 analyzer，attempt 失败，其他 capability 继续 | 核对制品 digest/SBOM、版本矩阵和资源限制；必要时 canary rollback | self-test 通过、capability hash 稳定、误报集不回退 |
| probe result spool 高水位 | 停低优先级 passive，硬水位停止领取新 job | 恢复控制面/扩容受控目录，禁止删旧结果腾空间继续执行 | oldest age/bytes 回落、dropped=0、结果幂等归并 |
| passive probe 无结果/积压 | heuristic 保留，probe 标 degraded/expired | 检查 TAP/BPF、agent capability、scope 和 result spool | backlog 回落、attempt 有明确 stop reason |
| active probe 异常/误配置 | 全局 kill switch 停止派发，运行 job cancel_requested | 撤销 scope/凭据，核查目标和审计，必要时通知安全负责人 | 无新连接、inflight 归零、旧计划不可领取 |
| 凭据泄露/轮换 | 撤销 principal/plan version | 发新证书并滚动 collector | 旧凭据失败、新凭据最小 ACL |
| 误发布/升级失败 | feature flag/forward-fix | 按兼容矩阵回退二进制，不回退已消费 schema | 新旧 reader、migration、WAL 均兼容 |

Flow P1 目标：fsynced WAL 的进程崩溃 RPO=0；对**已经从 UDP socket 读取并进入 WAL group-commit buffer** 的 datagram，主机掉电 RPO≤`fsync_interval`；NIC/kernel queue 中尚未被应用读取的数据不做零丢失承诺，必须由 exporter sequence/sample-pool gap 显示。单实例故障 RTO≤60s；Kafka/CH 故障只要未超过 WAL/retention 容量则业务事实 RPO=0；常用查询可用性 99.9%。备份范围包括 MySQL、dimension/Geo bundle、flow 配置、ClickHouse base 与 migration，以及**保留期超过 base TTL 的派生表**——五张派生表的 TTL（category 400d、remote_geo/address 180d、endpoint 90d、port 30d）均长于 base 的 7d，超窗历史无法从 base 重建，丢失即永久，因此派生表按各自 TTL 纳入备份；可选 VM Flow cache 不要求独立备份。恢复演练必须验证两条路径：base TTL 窗口内从 base 重建派生、超窗历史从派生备份恢复且与备份 checksum 一致。现有 VM 中的 SNMP/系统指标按平台原备份/retention 策略处理，不属于 Flow base 重建范围。

## 14. 非功能与故障策略

### 14.1 容量与背压

容量不能由链路带宽直接推断，也不能把 GoFlow2 的公开性能描述当作本项目容量承诺。sFlow 首先按端口计算：

```text
line_pps_i        = line_bps_i / (wire_frame_bytes_i * 8)
sample_records_s  = Σ(line_pps_i * sampled_directions_i / sampling_rate_i)
test_sustained_s  = sample_records_s * 2
test_burst_s      = sample_records_s * 3
kafka_payload_Bps = normalized_records_s * p95_encoded_record_bytes
```

`sampled_directions_i` 只能取设备实际启用的 ingress/egress 数，不能用业务“上下行”替代。sFlow datagrams/s 还取决于厂商每个 datagram 的 sample 打包数，必须从 pcap 和 exporter counter 实测；NetFlow/IPFIX 直接以 records/s、datagrams/s、template/options 频率建模。

当前已知端口 `2×100G + 12×10G = 320G`。按最小 64B Ethernet frame 在链路上约占 84B 计算：

| 采样率 | 单向 samples/s | 双向 samples/s | 2×双向持续目标 | 3×双向 5 分钟突发 |
|---|---:|---:|---:|---:|
| 1:512 | 930,060 | 1,860,119 | 3,720,238 | 5,580,357 |
| 1:1000 | 476,190 | 952,381 | 1,904,762 | 2,857,143 |
| 1:2000 | 238,095 | 476,190 | 952,381 | 1,428,571 |
| 1:4096 | 116,257 | 232,515 | 465,030 | 697,545 |
| 1:10000 | 47,619 | 95,238 | 190,476 | 285,714 |

所以“每秒数十万条”只在采样率较低、平均包较大或只采单向时可能足够；1:1000、最小包、双向场景必须按接近 `1M records/s` 基线和 `1.9M records/s` 持续目标设计。若单实例实测达不到，先增加 GoFlow2 socket/worker 并消除 JSON、逐条日志和同步下游；仍不足则按 exporter/sub-agent 水平分片 collector，NetFlow/IPFIX 必须保持 exporter/observation-domain 的模板亲和性。

完整容量基线还包括：raw WAL bytes/s、normalized bytes/s/partition lag、dimension lookup/s、address-set row expansion、minute unique pair/prefix、CH compressed bytes/day 和 WAL 增长速度。Kafka 分区、broker 磁盘/网络、retention 和 WAL 必须用测得的 p95 raw datagram/Protobuf batch 大小计算，不能只验证 decoder CPU。Flow P1 压测至少覆盖预期峰值 2 倍持续 30 分钟、5 分钟突发 3 倍和 72 小时 soak。

```text
wal_required_bytes   = peak_raw_WAL_Bps * kafka_outage_seconds / target_utilization
kafka_retention_bytes = normalized_Bps * retention_seconds * replication_factor / target_utilization
collector_instances  = ceil(test_burst_records_s / certified_instance_records_s) + 1  # N+1
dimension_instances  = ceil(test_burst_records_s / certified_worker_records_s) + 1    # N+1
```

冻结的 Flow P1 性能门：

| 路径 | 正常目标 | 发布阻断条件 |
|---|---:|---|
| UDP→WAL | kernel/queue 可观测 drop=0（实验室批准负载），fsync P99≤20ms | 2×持续或3×突发出现无法解释 drop/OOM |
| WAL→Kafka | produce ack P99≤100ms，健康时 WAL oldest≤5s | WAL 连续增长、ack error 未告警、hard watermark |
| Kafka→base | normalized oldest age≤60s，分钟 base 在迟到窗口后≤60s 可查 | lag 超软水位无扩容，retention remaining≤6h |
| Dimension | P99 lookup/record≤50µs，set expansion≤配置上限 | CPU/RSS 无界、静默截断 set、snapshot skew |
| Base→derived | divergence 正常为0，修复 RTO≤30min | derived partial 未标记或不能从 base 重建 |
| 查询 | 1h P95≤2s，24h P95≤5s，错误率<0.1% | 绕过 limits/RBAC，超时无 partial/completeness |
| VPN score/probe | 评分在下个 10m 周期完成；候选/tenant/run≤上限；active 并发/速率不超 scope policy；queue age 可见 | 评分抢占在线 CH、probe 无界排队/连接、scope 越界或 kill switch 无效 |
| 配置 | plan/snapshot/Geo 发布后≤60s workers ack | 新旧版本混用且未回显、失败覆盖旧 active |

单组件 benchmark、端到端压测、72h soak 必须使用相同 schema、压缩、TLS、地址库规模和 address-set expansion；不得用关闭 WAL、TLS、Geo 或派生写入的“空跑”结果签署生产容量。

降级顺序：

1. 停止有界 debug capture/DLQ sample；
2. 停止新的 active probe、缩短 passive observation plan 并暂停 VPN backfill/低优先级评分；已有 finding 保留；
3. dimension worker 提前 flush 或 spill 聚合状态，继续超水位则暂停 normalized partition，让 lag 吸收维度/CH 背压；
4. 停止历史回算和非必要 rollup，优先在线 consumer；
5. Kafka 不可用或 producer 背压时 flow-collect 从 raw WAL 积压，WAL soft watermark 立即扩容/告警；
6. 只有 normalized retention 或 WAL hard limit 耗尽才允许按已声明策略停收/丢数据，并记录精确 data-loss window；不得为保吞吐静默丢 base 或地址维度事实。

dimension lag 以 oldest-event-age 和 retention remaining 设置软/硬水位：软水位扩容并告警，硬水位停止非必要回算并启动上述逐级背压；任何一级不得靠丢弃 normalized record 自愈。

### 14.2 正确性

- sFlow 必须逐 sample 使用其 rate，并按 `(transport source,agent,sub-agent,source type,source value)` 隔离 pool/sequence；sampled 模式的 rate 缺失不得默认为 1；pre-scaled 模式不得再次乘 rate；override 必须精确到 sFlow source type/value（NetFlow/IPFIX 精确到 domain/sampler/interface），禁止设备级通配，override 和 rate 变更必须审计；
- 每次 flush 只确认一个 enriched base batch；category/address/endpoint/Geo/port 和可选 VM recording cache 都必须能从 base 重建；
- 所有分钟桶带 dimension/Geo/classification version，版本按 event time 选择；
- WAL→normalized 和 normalized→base 的 replay、DLQ、offset commit 必须用 fixture 证明；
- flow-collect 输出重试、CH 重试和 WAL/Kafka 重放必须用 stable datagram/record/ingest batch ID 证明幂等；
- 每个 address role 的 primary prefix（含 `_unassigned`）必须与 base total 守恒；address set 固定为非加和；
- 对账按同一观察接口/观察方向/完整时间桶进行；counter 缺失不得阻塞 flow 分类，sFlow/SNMP counter 不得相加，业务方向映射必须显式；
- P95 只用完整 5 分钟桶并返回完整率；
- 地址维度配置先生成 immutable snapshot，worker 全部 ack 后按 event-time 分钟边界生效；
- raw 不可变，supplier/customer 直接从同一个 raw bucket 经过已批准规则派生，查询和导出返回版本。

### 14.3 可用性

- Geo reload 失败：继续旧版本；
- Kafka 失败：flow-collect 继续写 raw WAL 并告警；恢复后有序重解码/发布；WAL 接近上限升级告警；
- flow-collect 重启：从模板 checkpoint 前的 WAL 安全点恢复，同一 datagram/record ID 不变；N+1 VIP 按 exporter affinity 切换；
- dimension worker/snapshot 失败：Kafka 保留 normalized，恢复后按 event-time 版本归类；不影响 flow-collect；
- CH 失败：暂停 normalized 消费形成 dimension lag，恢复后从已提交 offset 继续；超过 retention 才记录丢失窗口；
- VM 失败：CH Flow 事实与业务查询不受影响；SNMP/系统曲线和 pipeline 可观测性进入明确降级；若启用了 Flow recording cache，则暂停并在恢复后从 CH 回填；
- VPN scoring 失败：不删除旧 finding，不重复激活过期情报，记录 verdict freshness；恢复后从 CH 完整窗口按 rule-set version 幂等补算；
- passive/active probe 不可用：heuristic finding 和主管线继续；job 保持 queued/failed/expired 并回显 stop reason。active kill switch、scope service 或审计不可用时 fail closed，禁止派发主动连接；
- MySQL 短故障：flow-collect 沿用最后签名 exporter plan，dimension worker 沿用已校验 immutable snapshot；禁止未知 exporter 自动 active；
- NetFlow/IPFIX 模板 checkpoint、WAL 安全重放点和滚动 drain 是 Flow P1 必做，不得推迟；warming-up 期间不伪造记录；
- dimension rebalance/重启：从 normalized committed offset 和对应 snapshot 恢复，不重复做协议解码；
- 系统时钟偏差 >5s 告警，bucket 仍以 collector recv time 为准。

### 14.4 安全与隐私

- UDP 只绑定管理/采集网，网络 ACL + source allowlist；
- pending exporter 限速且未知 source 不进入高基数 labels；
- Kafka 使用 TLS/ACL；flow-collect 只能 produce normalized/decode-DLQ/quarantine 并读写 collect-state，dimension worker 只能 consume normalized group、读写 checkpoint，并写 CH base/获批派生 target；
- ClickHouse 用户分 read/write，API 只持 read，dimension worker 只持 base insert/repair 所需最小权限；flow-collect 无 CH/VM 业务凭据；
- Geo 文件目录只读，reload 不接受请求提供的路径；
- IP 查询、导出、Geo 修正全审计；
- 默认 TTL：pair 7d、endpoint 90d、remote/address dimension 180d、port 30d、category 400d，可按隐私策略缩短；
- 至少在 remote Geo TTL 内保留已被数据引用的版本 manifest/operator/Geo 字典；缺失时显示稳定 ID 和“字典已归档”，不得套用当前名称；
- 被动镜像/TAP probe 需单独授权、限时 BPF、最小 snaplen、payload 默认不落盘；证据只保存 hash/握手字段和 analyzer version；
- active handshake 使用独立 `probe_active` 权限、scope policy、审批、专用 egress、DNS/IP pinning、并发/速率/超时/cooldown、全局 kill switch 和全量审计；禁止扫描未授权范围、凭据猜测、协议爆破、漏洞利用、跨重定向扩大 scope 或长期保持连接；
- probe agent 只能领取其 tenant/binding/capability 匹配的签名 job，结果大小受限且签名校验；完整 IP/指纹和导出受 `vpn_evidence_read_sensitive` 保护；
- nDPI/其他 analyzer 版本、模型/指纹版本和误报集必须记录；情报和证据到期后 finding 按 TTL 清理，不能永久保存远端握手材料。

### 14.5 查询限制

- 1h/24h 常用查询最大 50 series、2,000 points/series；
- 自定义时间最长 400d，但 endpoint/port/pair/address dimension 受各自 TTL 限制；
- Top IP limit 10–100，cursor 分页；
- 查询超时默认 10s，导出走异步 worker；
- 对超范围请求返回明确 `available_from/available_to`，不静默返回残缺数据。

## 15. 分期与验收标准

### Platform P0–P3：先完成宿主基座

按平台架构文档分四个小阶段完成：P0 生产身份/API 和 migration 基线；P1 module/resource registry 与通用 collector enrollment/binding；P2 QueryGateway、visualization、provider-neutral export 和 target kind；P3 raw/supplier/customer 修正策略。完成 Platform P0–P2 后才能上线 Flow P1；任何 supplier/customer 查询或导出必须等 Platform P3。各阶段迁移现有 SNMP/system 能力并保持兼容，避免一次性重写。

前置门还包括 flow-collect WAL、Kafka normalized/collect-state/DLQ/quarantine/checkpoint topic、TLS/ACL/retention/容量与 WAL/Kafka/base replay/dedup 演练，以及设备/观察点、本地 CIDR/address set、dimension snapshot 发布、Home profile、`flow-geo-v1`、CH 容量和隐私策略均有负责人确认。

### Flow P1：采集、Kafka、六维与总览

交付：

- 新独立 flow-collect 和 dimension worker；本地 raw WAL、Kafka normalized/collect-state/decode-DLQ/quarantine/checkpoint topic、WAL/Kafka/base replay/dedup；sFlow v5 + NetFlow v5/v9/IPFIX fixture；
- flow module descriptor：资源、datasets、API、worker、health 和六个 feature-gated route manifest；P1 启用总览、分析图表和设置，IP/境外/VPN 页面分别随 P2/P3 开启；
- exporter registry、模板/采样、观察接口 allowlist；
- immutable dimension snapshot、地址段/address set、Geo loader、方向/六维/业务分类；
- 5 张 flow MySQL 表、平台 dimension snapshot 表、6 张 CH 表/MV；VM 只交付 SNMP/系统和 pipeline 自监控，Flow recording cache 默认关闭；
- `/flow`、`/flow/charts` 的六类/业务/地址段分组；
- flow-collect/WAL/normalized lag/dimension/base-derived divergence/Geo/exporter 健康。

验收：

- 方向、六维、重叠前缀、逐样本采样率和双端口 1:1000/1:10000 fixture 100% 通过；
- 真实设备连续 72h，无不可解释 flow-collect crash、kernel drop 或数据缺口；
- Kafka 30 分钟中断由 raw WAL 承接且恢复后无不可解释缺口；flow-collect kill -9 从 WAL/模板安全点恢复，dimension/CH 中断形成 normalized lag，恢复重放后 base 分钟总量不重复；
- 容量报告列出每端口采样率/方向、实际 datagrams/s、decoded records/s、p95 Protobuf bytes、Kafka/维度/CH 吞吐；若按 `2×100G+12×10G`、最小 64B Ethernet frame（84B on-wire）、双向、1:1000 作为批准的最坏场景，则持续 30 分钟达到约 `1.90M records/s`、5 分钟突发达到约 `2.86M records/s`，且无不可解释 UDP drop/OOM/数据缺口；
- 同一 address role 的 primary prefix 加 `_unassigned` 与 base total 完全一致；address set API 明确非加和，维度延迟不改变 event-time snapshot；
- 选定唯一观察点后无 2 倍重复特征；若启用 sFlow/SNMP counter，对账完整窗口达到双方确认阈值；未启用时页面与验收报告明确标记 flow-only，不阻塞 AS/IP/地域/六类功能验收；
- Geo 热切换不丢旧索引，60s 内新桶使用新 version；
- 常用 1h 查询 P95 达成压测冻结的 SLO；
- flow-collect N+1、dimension M+1、Kafka broker 和 CH replica 故障切换达到 §13.3 RPO/RTO；MySQL/Geo/dimension bundle/CH base 备份恢复后派生可重建；
- API tenant 隔离、最小 Kafka/CH ACL、凭据轮换、敏感日志和浏览器不直连存储检查通过。

### Flow P2：多维明细、IP 与归属修正

交付：

- 按省/异网机构分组；
- 地址段/address set Top、趋势、版本和积压状态；
- 源/目的 Top、详情、sparkline；
- watchdog 租户 Geo override merge/删除/审计；
- flow/counter 对账页（sFlow counter 或 SNMP）；
- CSV 导出接入平台 DatasetProvider/export worker；raw/supplier/customer 取值和权限一致。

验收：

- category 表总量与 pair/MV 在同一过滤条件下相等；
- primary-prefix 守恒、address-set 非加和和 snapshot 版本查询通过；
- Top-N 主查询与 sparkline 汇总误差为 0；
- override 不覆盖同 CIDR 的 local/business labels，60s 内只影响新桶；
- 90d endpoint 查询按 TTL 边界正确返回；
- CH 或 VM 短故障的独立降级行为符合 §14.3；VM 故障不影响 CH Flow 查询。

### Flow P3：境外与 VPN 风险

交付：

- 境外 KPI、趋势、地区、端口/协议；
- VPN rule JSON Schema/builder/preview/version，覆盖 src/dst 境内外、方向、端口/transport、duration、fanout、双向 ratio/单向性和多 transport；
- versioned address-set/CIDR/ASN/机构/route-class 情报 selector、source/reason/owner/expiry 和 suppress/allow 规则；
- candidate 评分、finding、probe job/collector capability、passive observe、受控 active handshake、签名结果与 evidence merge；
- probe agent 本机硬上限∩签名 plan∩job、不可变 plan revision/preview/ack/LKG/canary/rollback、静态 analyzer/安全 profile contract、版本兼容与有界 result spool；
- SOCKS/HTTP CONNECT/WebSocket/TLS-VPN 明确握手 profile，以及 encrypted proxy fingerprint 的非确认性 profile；
- VPN 专题页的 heuristic/corroborated/confirmed_protocol/inconclusive、独立人工 disposition、ratio、transport set、probe timeline/queue/kill switch；
- XLSX 导出；
- 数据完整率和 unknown 告警。

验收：

- 已知 fixture 对每条规则输出可解释 evidence；境内↔境外、境内↔境内、balanced、unidirectional、长连接、多 transport 和 intelligence/allow/suppress 真值表全部通过；人工 disposition 不改变机器 verdict/evidence；
- UDP/443、TCP/443、单向备份/视频/CDN/遥测 fixture 不被标为确认协议；只有协议特异握手才输出 confirmed_protocol；
- passive observe 不主动发包，active handshake 只触达授权 CIDR/端口且 kill switch、quota、cancel、timeout、DNS/IP pinning 和审计通过；Trojan/SS 等无凭据场景不爆破、不误报确认；
- probe agent/API/结果存储故障只形成 backlog/degraded，主管线和 heuristic finding 不丢；恢复后 job 幂等且 attempt 不重复失控；
- plan 错误保持 LKG，active 授权过期 fail closed；新旧 agent/analyzer/profile/result schema canary/rollback 通过，主动 attempt 在 `network_started` 后崩溃不会自动重放；
- KPI 与底层查询交叉核对一致；
- 24h VPN 评分在容量数据集上按时完成且不影响在线查询；大候选租户不能挤占其他租户或 Flow 收数。

### Flow P4：运营增强与容量扩展

交付候选：

- 流向异常告警接现有事件通道；
- 按 Geo/classification version 的受控历史回算；
- 在不改变 normalized/base 契约的前提下扩 Kafka 分区和 dimension consumer，增加跨机房复制与灾备切换；Kafka、容量和单机房 N+1 不是此阶段才引入；
- 扩展/升级 nDPI 或其他 analyzer 的 app/risk/fingerprint 插件，但不改变 P3 probe job/result 契约；
- 更长周期多分辨率 rollup、备份恢复演练。

验收：

- 回算任务可暂停、可审计、不会重复计量；
- HA 切换不混淆 NetFlow/IPFIX 模板 ownership；
- nDPI 标签缺失时主管线和六维结果完全不受影响；
- 恢复演练证明配置、CH schema/数据和 Geo version 可重建。

## 16. 必须新增的测试

| 层 | 测试 |
|---|---|
| platform contract | production AuthContext/tenant、module enable/disable、resource inheritance、collector enrollment/rotation/revoke/delete/restore/purge、M:N binding、plan revision/preview/ack/LKG/canary/rollback、dimension snapshot、dataset/visualization/export provider、raw/supplier/customer action isolation |
| flow-collect/WAL | source admission、datagram/batch/record ID、WAL schema/checksum/fsync/checkpoint/crash/overflow、template safe point、quarantine、滚动 drain、N+1 affinity |
| GoFlow2/Kafka | 四协议 decode/sampling、normalized protobuf compatibility/batching/partition、TLS/ACL、produce retry、ack→WAL reclaim、decode-DLQ/quarantine/normalized/checkpoint retention |
| dimension/replay | event-time snapshot、LPM、address-set expansion、normalized rebalance/lag、batch manifest/ingest ID、CH base 故障、offset commit、同 offset 与跨 session 重复幂等 |
| decoder | sFlow v5 的 IPv4/IPv6、sub-agent/source/sample-pool/drops、逐样本 rate、双 source 不同 rate、counter sample；NetFlow v5/v9、IPFIX template/options、缺模板、乱序、sampled/pre-scaled、sampling override |
| registry | exporter identity、pending/active/suspended/retired/deleted desired state、warming/healthy/degraded/stale observed health、非法迁移、source spoof、ifIndex allowlist、recovery |
| direction | local→remote、remote→local、双 local、双 remote、重叠前缀、IPv6 |
| Geo | manifest/checksum、zstd 流式解析、重叠/乱序拒绝、v4/v6 边界、失败保旧、override merge |
| classify | 六类真值表、规则表穷尽性（任意输入组合必命中且唯一）、HMT 入口规范化与 `overseas_includes_hmt` 两态、未知字段、缺市码/home 未配市、双 home ISP、版本切换 |
| aggregate | 分片一致性、迟到窗口、flush/spill/backpressure、primary-prefix 守恒、address-set 非加和、normalized offset-range batch retry 幂等 |
| reconcile | observation ingress/egress 与 ifIn/ifOut 映射、实际 counter 时间差、reset/wrap、LAG 单一口径、无 counter 降级、sFlow/SNMP 不相加 |
| VPN heuristic | src/dst 境内外、local/remote port、TCP/UDP、duration 可信度、fanout、periodic/multi-transport、balanced/unidirectional ratio、最小样本/完整率、情报有效期、allow/suppress 优先、family score cap、verdict/disposition 正交、普通 443/CDN/备份/视频误报集 |
| VPN probe | 本机∩plan∩job、签名/expiry/atomic apply/LKG/capability drift、analyzer/profile/result schema、passive 零主动连接、active scope/审批/DNS-IP pinning/rate/concurrency/timeout/cooldown/双 kill switch/cancel、SOCKS/CONNECT/WebSocket 明确握手、TLS-VPN 指纹、Trojan/SS 不确定、prepared/network_started crash、签名 result spool、job 幂等/evidence TTL/敏感权限 |
| MySQL | 平台表 migration 依赖、flow 五表 upsert/FK/tenant/binding isolation、reclassification range/status、init/migration parity |
| ClickHouse | 六表 DDL 实执行、生产 Replicated 变体、single base sink、每个 MV/互斥 rollup、view error isolation、`ingest_batch_id`、重复 batch、派生重建、TTL、base=aggregate、Top/P95 SQL |
| API | canonical/compat route、参数上限、tenant 强制、value-layer RBAC、存储 503、partial/completeness、cursor |
| UI | 六页空态/错误态、share/difference、legend stats、Geo override 不覆盖 labels |
| 性能 | 2×峰值持续、3×突发、CH 慢写、VM 监控失败、Geo reload、内存上限；可选 recording cache 另做基数/一致性压测 |

## 17. 部署参数冻结表

以下是上线前必须填写的环境输入，不再作为修改架构或 schema 的理由。默认产品语义已经冻结：`difference=out-in`；港澳台默认随 `overseas_includes_hmt=true` 计入境外且可由租户配置；supplier/customer 均直接基于 raw、禁止隐式级联；address set 非加和且每 record 最多 32 个；`flow-geo-v1` 使用版本目录 + current 原子切换；历史回算使用独立 run，不覆盖 raw。

| 参数组 | 必填内容 | 负责人/证据 |
|---|---|---|
| 设备 | 型号、固件、协议、enterprise fields、采样方向/率、模板/options 周期、export timeout、NAT 可见性 | 网络负责人 + 配置备份 + pcap checksum |
| 容量 | 每端口 line rate/帧长、datagrams/records/s、p95 raw/protobuf bytes、row expansion、单实例证书 | 网络/研发/SRE + 容量报告 |
| Kafka/WAL | 产品版本、broker/partition/RF/retention、ACL、WAL 介质/容量/fsync/RPO、N+1/VIP | 数据平台/SRE + 故障演练 |
| ClickHouse | 版本/cluster/Keeper、TTL、dedup、磁盘/备份、派生重建、查询资源组 | DBA + migration/恢复报告 |
| 口径 | home 省市/ISP、观察接口、counter scope/窗口/偏差、unknown/completeness 告警阈值 | 产品/网络/业务签字 |
| 地址库 | 导出负责人、目录、版本保留、checksum、admin code/机构完整率、发布窗口 | 地址库负责人 + manifest |
| 安全 | IP TTL、导出审批、Kafka/CH/文件权限、审计保留、DPI 授权 | 安全/合规评审 |
| VPN | score/level 阈值、风险文案、规则 owner、误报抽验集 | 产品/安全签字 |

参数变更只产生版本化 plan/config/snapshot 和容量复验，不改变本设计的组件边界、topic、ID、base/derived 或 API 语义。

## 18. Flow 全生命周期闭环与最小复杂度约束

全面设计的目标是让一条简单主链在创建、运行、变更、故障和销毁时都有确定行为，不是增加微服务。实现不得超出以下运行组件：现有 watchdog/MySQL/VM、`flow-collect`、Kafka、`flow-dimension-worker` 和 ClickHouse；reconciler、job runner、query/export 都是 watchdog 或现有 worker 内的职责。Flow 只有一个业务事实源 CH，只有一个 normalized topic，只有一个 base 成功提交条件。

### 18.1 数据生命周期

| 数据 | 产生与 durable 边界 | 在线用途 | 过期/删除 | 恢复或证明 |
|---|---|---|---|---|
| UDP raw WAL | datagram group fsync | Kafka 故障承接、模板安全重放 | Kafka 全部 child batch ack 后按 segment 回收；hard limit 形成 loss interval | segment checksum、连续 checkpoint、crash replay |
| normalized Kafka | producer `acks=all` | 异步维度归类和 backpressure | 由 retention 删除；不得早于最大故障/回放窗口 | partition/offset、stable record ID、consumer lag |
| CH `flow_pair_1m` base | batch insert 可由 `ingest_batch_id` 证明 | Flow 事实、重分类输入、派生重建 | TTL/tenant purge；禁止随 exporter/target tombstone 立即删除 | batch checksum、offset manifest、备份恢复 |
| 五张 CH derived | 从 base 按 target 幂等生成 | 查询加速；超 base TTL 后即历史聚合的唯一持有者 | TTL 或整分区替换；base TTL 窗口内可先删后重建 | base=derived 守恒、checkpoint/divergence；超窗部分靠派生备份 |
| VM SNMP/系统/pipeline | remote-write accepted | 端口曲线、P95、告警、运行健康 | 现有 VM retention | 与资源目录/抓取状态核验；不恢复 Flow 明细 |
| 可选 VM Flow cache | CH derived 异步复制 | 仅有限 summary | 可随时整批删除 | 从 CH 重建并逐桶对值 |
| MySQL 配置/job | transaction commit | 管理、版本和操作状态 | tombstone→恢复期→审批 purge | audit log、row version、job checkpoint |
| VPN finding/probe evidence | 关闭窗口评分事务或签名 probe result 合并 | 风险解释、人工处置、误报反馈 | finding/evidence TTL 与 legal hold；规则删除不级联删除历史 | rule/intelligence/analyzer version、job attempts、verdict/disposition audit |
| 导出文件 | worker 写完并校验 checksum | 授权下载 | `expires_at` 后删除 | query snapshot、row count、checksum、destruction receipt |

retention owner、默认 TTL、最小/最大值和合法 hold 必须在部署表中落值。任何 TTL 缩短都先 dry-run 影响行数/时间范围并审批；删除管理对象只阻止新数据，事实按 retention 保留。tenant purge 才执行跨 MySQL、Kafka/WAL 可定位数据、CH、VM cache、对象存储和凭据的有序销毁，并逐项出具计数/checksum receipt。

### 18.2 输入、处理与输出质量契约

| 边界 | 接受条件 | 拒绝/降级行为 | 对外证据 |
|---|---|---|---|
| UDP→WAL | source/listener/plan 合法且 WAL 可 durable | 未知 source 只进限速 metadata；磁盘 hard limit 停收并开 loss interval | accepted/fsynced/dropped counters |
| WAL→normalized | schema 可解、模板/sampler 完整、计数语义确定 | template wait 可重放；坏包进受限 DLQ；缺 sampling 不伪造 rate=1 | protocol、quality flags、stable IDs |
| normalized→base | snapshot 可按 event time 解析、整数计算无溢出、batch 可幂等插入 | 形成 lag/partial，不提交 offset | snapshot/version、batch checksum、complete ratio |
| base→derived | target checkpoint 连续且守恒校验通过 | divergence 并从 base repair，不回滚 base | derived watermark、missing batches |
| query/export | AuthContext、dataset/filter/group/range/cost 合法 | 400/403/409/429/503 有稳定 code；不静默截断或跨版本相加 | source、versions、partial、complete ratio、limits |

`empty` 表示完整查询确实无行；`partial` 表示时间段或派生不完整；`degraded` 表示使用批准的受限降级路径；`error` 表示没有可被正确解释的结果。四者在 API、UI、导出和告警中不得混用。

### 18.3 Flow 管理对象操作闭环

| 对象 | 创建/列表/查看/修改 | 状态动作 | 删除/恢复/销毁 |
|---|---|---|---|
| exporter | cursor/filter/sort；创建幂等；PATCH `If-Match` | validate/activate/suspend/resume/retire | delete-preview→tombstone→restore；purge 需停止收数、撤绑定影响确认和审批 |
| flow settings | tenant singleton GET/PUT，PUT 乐观锁 | auto-detect 只产候选，apply 才生成版本 | 不单独删除；随 tenant 生命周期销毁 |
| Geo override / dimension snapshot | draft CRUD、preview、publish | publish/retire；worker ack 后 effective | 旧 bundle 在可重放窗口内不可 purge |
| VPN rule | draft CRUD、版本化参数 | activate/suspend/retire | tombstone/restore；findings 保留期内 purge 受限 |
| reclassification | POST 固化 request/hash/version/range；GET/list | pause/resume/cancel/validate/activate/retire | 未 active run 可过期清理；active overlay 先 retire，事实由 TTL 清理 |
| finding | list/get/export；annotation 只追加 | probe/取消 probe；系统合并 signed evidence；具备 `dispose_finding` 的用户可 acknowledge/调查/授权/误报/解决 | 按隐私 TTL 过期；不允许用户改写 heuristic/probe evidence，规则删除不级联 finding |

Action 权限按 `view/configure/operate/delete/restore/purge/approve/export/read_sensitive/dispose_finding/probe_passive/probe_active` 分开；list 的每一行和 bulk 的每一个 ID 都执行资源范围过滤。任何状态动作、finding 处置、probe、导出、IP 明细查看、Geo 修正、重分类激活和 purge 都写 audit log；后台 reconciler 的 actor 也不能省略。

### 18.4 变更与功能生命周期

schema、protobuf、topic、Geo bundle、dimension snapshot、规则和 DatasetDescriptor 都先产生新版本，再走 validate→canary→active；旧 reader/writer 的兼容窗口和使用量归零是移除前提。数据库 migration 只 forward、幂等并带 actual/desired 检查；破坏性变更用 shadow table/双读验证/原子切换，不把 down migration 当生产恢复方案。

feature flag 只能控制入口和 worker 调度，不能绕过 migration、鉴权或事实保留。experimental 功能必须有 owner、期限和 telemetry；deprecated API 先返回 deprecation metadata，兼容窗口结束且调用量为零后才由变更 job 移除。

### 18.5 复杂度守门

以下变化必须单独 ADR 和压测证据，不能以“架构完整”为理由直接加入：第二个 Flow 业务事实库、第二个 normalized/raw topic、独立生命周期/权限/导出微服务、浏览器直连存储、Flow 业务默认双写 VM、同步地址归类、逐 flow HTTP/日志写入、通用工作流引擎。评审优先检查能否用现有 watchdog contract、Kafka backlog 和 CH base 重建解决；若可以，不增加组件。

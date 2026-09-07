# Flow 地址发布、入库分类与查询计划

状态：**Accepted，替代旧 ClickHouse `IP_TRIE/dictGet` 方案**。本文件是地址分类发布、热路径和历史修正的当前唯一口径。

## 1. 已核实的现状

- `flow-collect` 只接收 UDP 并写 Kafka，不做地址分类。
- `flow-worker` 已在 Kafka 后使用 `gaissmai/bart` LPM 与 Geo 有序区间二分完成分类；`snapshot.go` 的 lookup 和 `enrich.go` 的 enrichment 每条 flow **不访问 MySQL、ClickHouse 或 HTTP**。
- worker 当前仍从 MySQL 发布记录指向的小型人工 definition object，以及独立 Geo 文件装载运行时索引。也就是说，热路径已经是纯内存，但还没有一个把 pinned source manifest、base Geo/ASN 和人工定义合成在一起的紧凑、不可变二进制发布物。
- CH migration 009 的 `flow_address_dict_source` 和 `IP_TRIE` 集成测试证明过 ClickHouse 字典能力，但没有接入生产 worker/rollup/query。它是历史实验，不是继续演进的架构基础。

因此本次修正不再“把分类从 worker 搬到 ClickHouse”，而是把**索引的发布来源**从运行时批量装载管理数据，改成经审批、可校验、可回滚的二进制 `AddressSnap`。

## 2. 决策

```text
MySQL 管理态（import generation + draft prefix/set/operator）
  -> address_snapshot_build operation job
  -> 固定 source manifest，分页读取不可变输入
  -> 合成/校验/字典编码 AddressSnap
  -> 临时对象 round-trip 校验
  -> immutable object + dimension snapshot metadata
  -> approve/sign -> activate
  -> worker 拉取、双重校验、离线建索引、原子切换、ACK
  -> flow_records 同时保存 raw endpoint 与 ingest 时派生维度/版本
```

1. **MySQL 是唯一编辑态和血缘权威，不是数据面依赖。** API 请求只修改 draft 或投递构建 job；不得在 HTTP 事务中展开百万条地址段。
2. **AddressSnap 是唯一运行时地址发布物。** worker 不再从 MySQL 批量拼装快照，也不使用 CH `dictGet` 分类。
3. **入库继续纯内存分类。** 每条 flow 做固定次数 LPM/区间 lookup，并把 raw IP、供应商值、客户修正值、分类版本一起写入事实。
4. **默认查询读取事实中已固定的维度。** 这保证查询快、结果可复现，并避免查询时对每行执行字典函数。
5. **按新定义查看历史是异步 reclassification。** job 读取 raw fact 与明确的 AddressSnap 版本，写新的派生 generation/索引并先做 count/counter 守恒；不改 base、不静默套用当前定义。
6. **预配置地址组/常用联合维度异步建索引。** ad-hoc IP/CIDR 只允许有界 raw 查询；大范围或长时间查询必须使用已就绪索引或明确拒绝/degraded。

## 3. AddressSnap v1 格式

### 3.1 容器

格式必须由 Watchdog 自己定义并提供 Go golden test，不能把 Rust `bincode` 内存布局直接当跨版本协议：

| 字段 | 约束 |
|---|---|
| magic | 固定 `WADS` |
| format_version | v1；未知版本 fail closed |
| flags/compression | v1 仅允许 zstd；未知位拒绝 |
| header/payload length | 固定 endian；在分配前检查配置上限 |
| tenant/snapshot/version/effective_from | 与外层 publication metadata 完全一致 |
| section counts/boundaries | v1 按固定顺序连续编码；计数必须能由剩余字节完整承载，解码结束禁止尾随字节 |
| payload_crc32c | 快速发现传输/磁盘损坏，不作为信任依据 |
| object SHA-256 | 由 object metadata 保存，并被平台审批签名覆盖 |

下载时先校验对象大小和 SHA-256，再做有上限的 zstd 解压，随后校验 header、CRC、section 边界和声明计数。审批签名覆盖 tenant/scope/version/effective time/object ref/SHA/source manifest；CRC 只负责廉价损坏检测，不能替代签名或 SHA。

### 3.2 逻辑 section

- metadata：schema、source generations、构建器版本、计数和合成规则版本；
- string/value dictionary：稳定 Geo code/path、supplier ISP、customer ISP、ASN、primary prefix、business 和 address-set membership 只存一次；名称是显示元数据，事实身份只用稳定 ID；
- IPv4 ranges：按 start 严格递增且互不重叠，固定宽度 start/end/value-index；
- IPv6 ranges：16-byte start/end/value-index，同样有序且不重叠；
- address-set/operator tables：排序去重的稳定 ID 与 bitmap/offset list；`supplier_isp_id` 和 tenant `customer_isp_id` 分属两个命名空间，`0=unknown`；
- optional diagnostics：构建统计，不进入热路径语义。

单个地址可同时拥有 continent/region/country/province/city 五级路径、一个 primary prefix 和多个非互斥 address sets。范围行引用一个字典值，不复制五行 Geo，也不要求存在 ASN。

### 3.3 确定性与限制

同一组 pinned inputs 和 builder schema 必须 byte-for-byte 相同。builder 固定规范排序、字符串编码、空值、时间精度与压缩参数；拒绝重叠输出、悬空字典引用、重复稳定 ID、超限 set membership、计数不符和非规范地址。

默认预算由配置给出但存在硬上限：compressed/uncompressed bytes、v4/v6 range count、dictionary entries、strings bytes、sets、每 endpoint membership、zstd window 和构建峰值 RSS。任何超限整体失败，不截断后发布。

## 4. 异步构建与发布生命周期

1. preview 在一致性事务中固定 active import slot、row version、artifact checksum/row count 与 draft digest。
2. publish 只创建 `address_snapshot_build` operation job；payload 固定 tenant、draft revision/digest、source manifest、target format/builder version 和幂等 generation。
3. builder 按主键分页读取 pinned `import_id`，分别校验文件/行 checksum 与计数；使用 `combined -> geo/asn 字段域 -> manual explicit fields` 的既定优先级合成不重叠区间。
4. builder 写临时文件，重新从文件 decode 并做全量 invariant/parity sample；成功后才原子保存 object，并创建 pending snapshot。失败不能修改当前 active snapshot。
5. approve 的 Ed25519 签名绑定 object SHA 和 source manifest；activate 只改变 event-time timeline。
6. consumer 分别上报 `downloaded/installed/failed`。只有 `installed` ACK 才表示该 worker 可使用目标版本；平台不能用“快照已创建”冒充数据面就绪。
7. rollback 只切 activation 指针。旧 object 按事实/修复引用、未来 activation、worker LKG 和显式 retention 共同保护后再经 operation job GC。

构建 job 复用平台唯一 `operation_jobs` lease/heartbeat/cancel/retry/checkpoint；不得在 Flow worker 内再造任务状态机。checkpoint 只能落在确定分页边界，重试相同 generation 必须生成相同 bytes/checksum。

## 5. 分发与 worker 加载

- 第一阶段由 worker 通过认证 API 获取“本 scope 的 desired activation metadata”，使用 object ref 拉取 AddressSnap；Kafka 不承载大对象。
- 本地以 `tenant/snapshot/checksum` 保存 last-known-good 文件：下载到临时文件，`fsync + atomic rename` 后才可成为 LKG。启动时可从 LKG 恢复，但仍要校验签名、SHA、header/CRC 和 metadata。
- 解码、索引构建在热路径之外完成；新 catalog 完全构建成功后以单次 atomic swap 可见。失败继续使用旧版本并上报稳定错误码，不允许空表替换。
- 同一 event time 必须选 `effective_from <= event_time` 的最新已安装版本；缺版本暂停对应 Kafka partition，不回退到当前版本误分类。
- 现阶段继续使用已验证的 BART LPM + Geo 有序区间二分。EdgeManager 的 DIR-24-8 是候选优化，不是默认实现：IPv4 一级表约 64 MiB，若按 tenant 复制会线性放大。只有固定硬件上 BART 不达 SLA，且“共享 supplier 基库 + tenant 稀疏 overlay”容量模型成立时才可切换。

gossip 可作为后续低延迟提示，只传播 `(scope,version,checksum,control endpoint)`；权威 metadata、认证下载和 ACK 仍在平台。它不是 v1 的前置条件。

## 6. 写入、查询与历史修正

### 6.1 写入

worker 从已安装的 event-time AddressSnap 得到方向、business、primary prefix、sets、Geo、supplier/customer ISP/ASN 和分类；同时保留 `src_ip/dst_ip`、协议、端口、exporter 字段、raw/estimated counters 以及 snapshot/classification version。写入不访问数据库，不执行逐记录 hash。

### 6.2 默认查询

- 单维与常用预聚合按 fact 已存稳定 ID/version 汇总；显示名称按相同历史版本解析。
- `primary_prefix` 在每个 endpoint role 内互斥可加；`address_set` 可重叠，响应必须标 `additive=false`。
- 明细返回原始 endpoint 和 ingest-time 派生值/版本，不能用当前地址库覆盖历史字段。

### 6.3 指定版本/as-of

- “按当时口径”直接查询事实自身版本。
- “用新口径重算旧时间窗”创建 reclassification operation job。job 固定 raw window、source/target AddressSnap、view、generation 和授权快照；读取 raw facts，使用目标二进制快照离线分类，写可丢弃派生层。
- 新 generation 只有在自然 Kafka 坐标 identity、record count、raw/estimated bytes/packets 守恒并完成 marker 后才可见；失败/取消保留旧 generation。
- raw 已销毁且 Kafka 也不再可验证重放时必须拒绝，不能从旧派生字段猜回原始归属。

### 6.4 地址过滤

- 精确 IP/小 CIDR/短窗口可走受预算约束的 raw 查询。
- 地址组可由 AddressSnap 展开为有界 canonical ranges；表达式超过 SQL/query budget 时转异步索引，不生成无界 OR。
- 常用国家、省、市、运营商、prefix/set 与联合维度由配置驱动的异步索引服务长周期查询。索引 ACK 未 ready 时显式返回 unavailable/degraded，不静默扫描无限 raw 数据。

## 7. 迁移与兼容

1. 先发布 AddressSnap v1 codec/builder 和双读 worker；旧 JSON dimension + Geo 目录仍可启动。
2. 用同一真实 corpus 做旧 loader 与 AddressSnap lookup 全字段 parity，覆盖 v4/v6 边界、嵌套 override、多组、无 ASN 和 supplier/customer ISP 分离。
3. 部署全部 reader 后才允许平台写 AddressSnap；worker ACK 达标后切 activation。
4. 停止 worker 的 MySQL/目录装载入口并保留 LKG/rollback 窗口。
5. migration 009 和 `address_dict_integration_test.go` 标注为历史 CH 字典实验；不回改历史 migration，也不新增“拆 IP_TRIE 字段”的 migration。待兼容窗口结束再以前向清理移除未使用对象。

本变更不删除 `flow_records` 已有派生列，也不恢复固定 30 天 TTL、持续 1m rollup 或逐记录 hash。Storage V2 的原始保留、日归档和 Kafka 坐标对账契约保持不变。

## 8. 验收门禁

- codec：deterministic golden、round-trip、未知版本/flag、截断/尾随、CRC/SHA 篡改、section 越界、zstd bomb/window、计数和资源上限；
- builder：真实 MMDB/IPDB generation + manual overlay，全量 row/count/checksum、重叠/优先级、crash/resume、相同 generation bytes 一致；
- parity：旧内存 loader 与 AddressSnap 对固定 v4/v6 corpus 的完整分类结果一致；
- worker：无 MySQL/CH 连接也能 cold start/LKG restore，下载中断、坏签名、坏对象、构建 OOM 预算、ACK 失败、回滚和原子可见性；
- performance：固定硬件记录 build time、object size、peak RSS、单核/multicore lookup throughput、p95/p99、atomic swap pause，并覆盖多 tenant；BART 与 DIR-24-8 只用同 corpus 决策；
- integration：管理 draft → build job → approve/sign → activate → worker download/install/ACK → Kafka 四协议 → CH fact version → query；失败路径证明旧版本持续服务；
- reclassification：源/目标版本、重试/takeover/cancel、count/counter 守恒、generation 原子切换和 raw 不可用拒绝；
- regression：Flow/Watchdog 全库 test/race/vet/build、真实 MySQL/CH/Kafka 组合门禁和 rolling upgrade。

# Flow 地址发布、入库分类与查询计划

状态：**Accepted，替代旧 ClickHouse `IP_TRIE/dictGet` 方案**。本文件是地址分类发布、热路径和历史修正的当前唯一口径。

## 1. 已核实的现状

- `flow-collect` 只接收 UDP 并写 Kafka，不做地址分类。
- `flow-worker` 已在 Kafka 后使用 `gaissmai/bart` LPM 与 Geo 有序区间二分完成分类；`snapshot.go` 的 lookup 和 `enrich.go` 的 enrichment 每条 flow **不访问 MySQL、ClickHouse 或 HTTP**。
- 平台已经能把 pinned source manifest、base Geo/ASN 与人工 definition 异步编译为 WADS；worker reader 已能校验外部 SHA、WADS header/CRC/zstd/引用/驻留内存预算并直接构建不可变二分索引。WADS-only 本地启动不再要求独立 Geo bundle，旧 JSON publication 仍走原 GeoCatalog。平台已完成 classification profile CAS、不可变 classification object、dimension+classification pair 元数据、复用全局 trust bundle 的 Ed25519 envelope，以及 `flow_worker` 专属认证的 desired/object/ACK HTTP；worker 已完成内容寻址 LKG、严格 HTTP client/sync，并接入生产命令的冷启动、首次同步、周期刷新和退出取消。production router、进程恢复、坏对象保留 LKG、Kafka 缺 event-time version 暂停及 installed-ACK 查询切换均已有真实依赖门禁。
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

AddressSnap 的构建输入使用 dimension definition bundle schema v3。v2 只冻结 tenant operator，仍会丢失人工 Geo 的名称/父子关系和 address-set 名称，不能独立重现历史显示；v3 因此把规范排序的 `geo_nodes(id/kind/code/name/parent_id/enabled)` 与 `address_sets.name` 一并纳入 immutable/checksummed/signed definition。reader 继续接受 v1/v2，但 v3 有地址组时名称必填，Geo 图必须满足 ID 与 kind/code 唯一、父节点存在且层级向下、启用节点不得挂在禁用父节点下。该升级复用已有对象字段，不增加 MySQL migration。

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
| object SHA-256 | 由 object metadata 保存，并与 object format/version、builder version、build job ID 一起被平台审批签名 v3 覆盖；旧 JSON publication 继续使用兼容的签名 v2 wire |

下载时先校验对象大小和 SHA-256，再做有上限的 zstd 解压，随后校验 header、CRC、section 边界和声明计数。审批签名覆盖 tenant/scope/version/effective time/object ref/SHA/source manifest；CRC 只负责廉价损坏检测，不能替代签名或 SHA。

### 3.2 逻辑 section

- metadata：schema、source generations、构建器版本、计数和合成规则版本；
- string/value dictionary：稳定 Geo code/path、supplier ISP、customer ISP、ASN、primary prefix、business 和 address-set membership 只存一次；名称是显示元数据，事实身份只用稳定 ID；
- IPv4 ranges：按 start 严格递增且互不重叠，固定宽度 start/end/value-index；
- IPv6 ranges：16-byte start/end/value-index，同样有序且不重叠；
- Geo/address-set/operator tables：Geo 同时保存 namespace、稳定 `id/parent_id` 与可变 `kind/code/name`；身份与关系只按 namespaced ID，code 不作为全局键，允许不同分支/层级以及 supplier/customer 两个命名空间重用 code/ID。集合保存稳定 ID、name、enabled，运营商保存稳定 ID 与数值 ID；`supplier_isp_id` 和 tenant `customer_isp_id` 分属两个命名空间，`0=unknown`。供应商只按 pinned source 中的精确规范化名称分配独立、单调且永不复用的 UInt16 ID，不与 tenant 运营商模糊对齐；tenant/customer ISP 仍只按非零 ASN 精确映射或人工稳定 operator ID 覆盖；
- optional diagnostics：构建统计，不进入热路径语义。

单个地址可同时拥有 continent/region/country/province/city 五级路径、一个 primary prefix 和多个非互斥 address sets。范围行引用一个字典值，不复制五行 Geo，也不要求存在 ASN。

### 3.3 确定性与限制

同一组 pinned inputs 和 builder schema 必须 byte-for-byte 相同。builder 固定规范排序、字符串编码、空值、时间精度与压缩参数；拒绝重叠输出、悬空字典引用、重复稳定 ID、超限 set membership、计数不符和非规范地址。

编码/reader 预算由配置给出且存在硬上限：compressed/uncompressed bytes、v4/v6 range count、dictionary entries、strings bytes、sets、每 endpoint membership、zstd window 和解码常驻内存估算；任何超限整体失败，不截断后发布。builder 峰值由固定生产 corpus 回归和部署进程内存限制共同约束，Go 进程内不能伪造可靠的 RSS hard-limit。触发 OOM/取消/超预算时 operation job 失败且不得改变 active snapshot。

## 4. 异步构建与发布生命周期

1. preview 在一致性事务中固定 active import slot、row version、artifact checksum/row count 与 schema v3 draft digest；该 digest 已覆盖 operator、人工 Geo 节点及 address-set 显示元数据。preview 返回的 `definition_bytes` 只表示该小型定义对象的规范编码大小；最终 WADS 包含 pinned source，构建前不能伪装成精确 bundle 大小。
2. publish 只创建 `address_snapshot_build` operation job；tenant 来自认证上下文，payload 固定 effective minute 与覆盖 source manifest 的 draft digest，operation job ID 同时作为幂等 snapshot/build identity；format/builder version 由服务端常量拥有，客户端不得注入。
3. builder 按 `(family,ip_start,prefix_length,id)` keyset 分页读取 pinned `import_id`，核对实际读取 v4/v6 原始行数与 manifest，并逐行验证 CIDR、family、prefix length、持久化 start/end 一致；同一 source 内合法嵌套 CIDR 先按最长前缀语义展平为不重叠区间，再使用 `combined -> geo/asn 字段域 -> manual explicit fields` 的既定优先级合成。source artifact SHA 是 immutable generation 身份，不冒充数据库逐行 checksum。
4. builder 写临时文件，重新从文件 decode 并做全量 invariant/parity sample；成功后才原子保存 object，并创建 pending snapshot。失败不能修改当前 active snapshot。object 路径由 job/snapshot ID 确定且内容不可覆盖；commit 失败时旧 attempt 不得删除该路径（新 owner 可能已复用/提交），无引用对象统一交给 fenced orphan-GC。
5. approve 的 Ed25519 签名绑定 object SHA 和 source manifest；activate 只改变 event-time timeline。
6. consumer 分别上报 `downloaded/installed/failed`。只有 `installed` ACK 才表示该 worker 可使用目标版本；平台不能用“快照已创建”冒充数据面就绪。
7. rollback 只切 activation 指针。旧 object 按事实/修复引用、未来 activation、worker LKG 和显式 retention 共同保护后再经 operation job GC。

构建 job 复用平台唯一 `operation_jobs` lease/heartbeat/cancel/retry；分页边界上报单调 progress，job payload 始终保留为不可变 checkpoint，重试/接管从 pinned generation 重新分页读取并以同一 job/snapshot ID 生成相同 bytes/checksum，不另存第二套 builder 状态。读取和两遍合成周期检查取消；zstd 单次压缩完成后再次检查并由 lease fence 决定是否可提交。

合成 core 不把 CIDR 展开为地址：repository adapter 将 raw rows 以最长前缀语义规范化为按 family/start 排序且内部不重叠的 inclusive ranges，builder 再以多路边界 sweep 叠加 combined、Geo、ASN 与已编译 manual range。第一遍只收集去重 string/value dictionary，第二遍输出 value index 并合并相邻同值范围，额外内存随“输入 prefix、边界、不同值和输出范围”增长而不是随地址空间增长。source 声明的 v4/v6 row count 必须与实际读取 durable rows 精确相等；原始行数不能错误地等同于规范化 range 数（嵌套可能拆分或合并）。任一持久化边界不一致、规范化后重叠、悬空 Geo/operator/set 引用或预算超限整体失败。

2026-09-07 在 darwin/arm64、MySQL 9.6 上以真实 `GeoLite2-ASN.mmdb`（12,297,156 bytes，SHA-256 `dca4ada7f85870805b6033f9282561be375bb7ad02a7958fb3061ca3b2ee200a`）执行了完整门禁：1,130,749 条网络（v4 671,052 / v6 459,697）、81,399 个不同 ASN，在 100,000 durable rows 注入失败后由第二 attempt 从 checkpoint 恢复；含 migration 060 发布索引维护的导入为 2m13.259s、8,485 rows/s、峰值 Go heap 15.0 MiB。WADS 为 4,291,551 bytes，合并成 v4 402,248 / v6 106,099 ranges 和 81,400 values；worker decode+compile 82ms、峰值增量 75.6 MiB，100 万次 lookup 约 10.25M/s，p95 292ns、p99 500ns。supplier operator 为 0：`autonomous_system_organization` 是 ASN 显示名称，不是 ISP，不能挤入 supplier UInt16 身份；ASN 数值仍完整进入 AddressSnap。

同一语料最初暴露的管理面成本是 builder 9m04.618s、峰值增量 heap 1,630.7 MiB。修正分为三处：有序无重叠 MMDB/IPDB 单遍合并（嵌套输入仍走 LPM sweep）；prepared layer 只引用不可变 source/manual 值；repository 中间行以共享 Geo 指针保存并按实际合并数精确分配输出。migration 060 再把 keyset cursor 的完整 `(tenant_id,import_id,family,ip_start,prefix_length,id)` 顺序建成索引。最终独立重跑 builder 为 28.292s/457.5 MiB，时间下降约 94.8%、峰值下降约 71.9%，产物 range/value、抽样 ASN 和 lookup 结果不变。AddressSnap 是平台单例发布物，禁止按业务 tenant 重复构建或重复安装；当前错误的 1/4/16 tenant 复制容量模型已撤销。单份已安装 WADS 的 retained heap 约 48.0 MiB；并发查找期间 catalog pointer swap 为 17.791µs，最大 lookup 64.25µs。运行时产物、热路径、builder 和 atomic swap 均已达到 C4b2 性能门禁。

导入与发布是两个不同 SLA。MMDB/IPDB 导入是低频、通常一次性的 operation job，允许分钟级后台运行；验收重点是持久状态、页级 checkpoint、失败原因、重试接管、行数/校验和与最终 generation 可追溯。发布是平台管理员每次地址、地域、运营商或集合编辑后的常态 operation job；API 只入队，job 状态、进度、错误和结果引用可查询，旧 active publication 在 build/审批/安装任一阶段失败时持续服务。preview digest 钉住本次草稿，编辑会使旧 preview 终止；重新 preview 后构建新的不可变对象，只有签名审批、activation 和 worker installed ACK 完整收敛后才切换。导入耗时不作为发布响应 SLA，也不能成为发布时重复解析原始文件的理由。

地址库是安装级全局数据，不归属于用户或 tenant。KISS 身份体系只保留本机 user/role/permission：拥有 `address.manage/address.publish` 的管理员才可上传并激活 import，编辑 prefix/set/Geo/operator/line，执行集合/合并 preview，以及 build/publish/approve/activate/rollback/retire/GC AddressSnap；其他已认证用户按 `address.view` 只读。`GET /api/v1/me` 与每个写 API 使用同一全局 RBAC 投影，浏览器能力仅改善 UX，后端始终是授权边界。MySQL schema、DTO、对象路径和签名 payload 均无 tenant/owner 字段；客户端夹带 `tenant_id` 会被严格 JSON/query 校验拒绝。

## 5. 分发与 worker 加载

- 第一阶段由 worker 通过认证 API 获取 desired activation metadata，使用 publication ID + 服务端拥有的 object kind 拉取 AddressSnap；Kafka 不承载大对象。机器路由固定为 `GET /api/v1/flow-workers/{worker_id}/trust-bundle`、`GET /api/v1/flow-workers/{worker_id}/enrichment-publications?after_version=&limit=`、`GET /api/v1/flow-workers/{worker_id}/enrichment-publications/{publication_id}/objects/{dimension|classification}` 和 `POST .../{publication_id}/ack`。全部路由使用独立 `flow_worker/pull` authenticator；`flow_collect/listen` 凭据不能调用，object ref 也不接受客户端输入。desired 按 classification version 升序、最多 100 个 signed envelope 分页；对象 GET 有服务端大小上限、checksum/ETag、Range 和 immutable private cache header。
- 本地 LKG 固定为 `trust-bundle.json`、`objects/{sha256}` 和 `publications/{classification_version}.json` 三部分。对象有界流式写临时文件并校验 SHA，随后 `fsync + immutable link + directory fsync`；签名 envelope manifest 只有在两对象已可读取、完整 pair 已编译后才以不可变文件落盘，并且必须在 catalog pointer 发布前成功。trust bundle 允许更高 generation 原子替换，拒绝回退和同代异文。冷启动把全部 manifest 在隔离 catalog 中按版本校验签名、SHA、header/CRC、metadata 和单调性，全部成功后一次发布；任一对象或版本损坏都不能暴露半恢复 catalog。HTTP 拉取层仍需复用此 LKG，不得另造缓存状态机。
- worker HTTP client 只接受无 userinfo/query/fragment 的 `http(s)` control-plane URL，禁止 redirect 以免跨 origin 转发 agent token；trust payload 必须与 generation/checksum headers 一致，desired page 必须严格 JSON 且 cursor 单调。每个已验签 pair 才能绑定 object ref；对象 response 的长度/checksum header 和正文 SHA 都要匹配。下载中断只留下随后删除的临时文件，坏签名在任何 object/ACK 前拒绝。两对象落盘后上报 downloaded，完整编译和 manifest 持久化后 atomic install 再上报 installed；installed ACK 不确定时保留本地版本且下一轮不重下对象，只重试同一 pair/ACK。失败按 transport/verify/compile/persist/activate/ack 稳定 stage 上报，任何远端错误都不能替换上一 catalog generation。
- 生产命令的 remote 和静态 bootstrap version 模式互斥。remote 模式要求 `-control-plane-url`、`-version-lkg-dir`，认证使用 `-agent-token-file` 或 `-control-plane-tls-cert/-control-plane-tls-key` 二选一；可用 `-control-plane-tls-ca`、`-control-plane-tls-server-name` 和 `-control-plane-timeout` 收紧 TLS/超时。明文 URL 只允许 loopback。启动先恢复全部 LKG，再尝试首次同步；控制面不可用但 LKG 完整时降级继续，两者都无可用版本则在连接 CH/Kafka 前失败。`-version-refresh-interval` 驱动单一串行刷新循环，进程退出取消并等待在途同步；`-check` 只校验配置和已有本地制品，禁止网络请求。
- 解码、索引构建在热路径之外完成；新 catalog 完全构建成功后以单次 atomic swap 可见。失败继续使用旧版本并上报稳定错误码，不允许空表替换。
- ACK 请求必须回显 publication 内的 dimension snapshot/version/checksum 与 classification version/checksum，服务端在锁内与不可变 pair 逐字段核对，并再次确认 registry 行仍为 active `flow_worker`。`downloaded_at`、`installed_at` 是不可逆里程碑：后续 failed 可以成为最新 attempt state，但不得清空此前的 downloaded/installed 时间。`attempted_at` 由服务端生成并在并发/时钟回退时单调推进。`boot_id` 记录哪次进程尝试，不宣称进程 lease fencing；在 worker heartbeat/session contract 落地前，查询切换只能按明确的 installed milestone 与部署策略判断，不能把“最新 state 非 failed”误当额外保证。
- reader 双读已经落地：`object_format` 为空或 `json/0` 时保留旧 definition + GeoCatalog 语义，`wads/1` 时 Geo、supplier/customer ISP/ASN、prefix、business 和 sets 全部来自同一对象；未知组合在读取对象前即拒绝。WADS value/range/string 引用在安装前预解析，逐 flow 只做 v4/v6 二分和不可变切片读取；WADS-only 构造器允许 GeoCatalog 为空，若事件时间实际选中旧 JSON 版本则以 `dependency=geo` 暂停而非误用当前值。
- 同一 event time 必须选 `effective_from <= event_time` 的最新已安装版本；缺版本暂停对应 Kafka partition，不回退到当前版本误分类。
- 现阶段继续使用已验证的 BART LPM + Geo 有序区间二分。EdgeManager 的 DIR-24-8 是候选优化，不是默认实现：IPv4 一级表约 64 MiB。只有固定硬件上 BART 不达 SLA 时才可切换；无论采用何种索引，进程内都只安装一份全局 AddressSnap，禁止按 tenant 复制。

gossip 可作为后续低延迟提示，只传播 `(scope,version,checksum,control endpoint)`；权威 metadata、认证下载和 ACK 仍在平台。它不是 v1 的前置条件。

KISS 控制面版本对由 migration 0031 保存：`flow_classification_profiles` 是 `id=1` 的全局 CAS 编辑态，`flow_enrichment_publications` 是全局单调且不可变的 event-time pair，`flow_enrichment_publication_acks` 保存每个 `agents(kind=flow_worker)` 的 downloaded/installed/failed 里程碑。`dimension_snapshot_id` 直接引用唯一全局 AddressSnap；MySQL 只保存 profile、ref/checksum、版本、签名与 ACK，不保存 WADS/classification 大对象。publish 选择 `effective_from <= 请求时间` 的最新全局 address activation，且只接受已审批、active、未删除的 WADS/1；classification version/effective time 严格单调。分类对象和完整 pair 使用同一个 active agent-plan Ed25519 key 签名，worker 复用其单调 trust bundle，不建立第二套 key 表。旧 Hub migration 059 的 tenant 版本只保留为迁移来源，不再运行、双写或约束新 wire。该管理操作不在 UDP/Kafka ingest 热路径上，也没有给事实或发布物增加固定 TTL。

## 6. 写入、查询与历史修正

### 6.1 写入

worker 从已安装的 event-time AddressSnap 得到方向、business、primary prefix、sets、Geo、supplier/customer ISP/ASN 和分类；同时保留 `src_ip/dst_ip`、协议、端口、exporter 字段、raw/estimated counters 以及 snapshot/classification version。写入不访问数据库，不执行逐记录 hash。

### 6.2 默认查询

- 单维与常用预聚合按 fact 已存稳定 ID/version 汇总；显示名称按相同历史版本解析。
- `primary_prefix` 在每个 endpoint role 内互斥可加；`address_set` 可重叠，响应必须标 `additive=false`。
- 明细返回原始 endpoint 和 ingest-time 派生值/版本，不能用当前地址库覆盖历史字段。
- 便捷运营商筛选只提交全局稳定 `operator_id`，不再把当前运营商的 ASN 列表展开到请求。Gin Flow preparer 在一个 MySQL repeatable-read 快照内解析只读 `flow_isp_id`、覆盖 `[from,to)` 的全部 enrichment publication，以及 active `flow_worker` 注册集合；每个 publication 必须对每个 active worker 存在不可逆 `installed_at` 里程碑。无 worker、时间窗早于首个 publication、任一 ACK 缺失或运营商禁用均显式返回 `flow_operator_unavailable/invalid_request`，绝不回退 ASN。
- 门禁通过后，服务端把 `isp = flow_isp_id`、精确 `dimension_snapshot_ids` 和 `classification_versions` 注入 canonical provider parameters。`schema_version=1` 的 prepared `operator_selection` 保存 operator、UInt16 identity、publication IDs 和版本集合；交互执行、延迟导出 query hash、响应 provenance 与查询审计使用同一份参数。客户端伪造或修改任一 prepared 字段、ISP predicate 或版本集合都会拒绝。跨版本仍以稳定 UInt16 过滤，并由事实自身 snapshot/classification version 分行/展示，不用当前名称改写历史。
- 这里的 expected fleet 是唯一 collector registry 中状态为 active 的 `flow/flow_worker/pull` 身份，不是 ACK 中“曾观察到的 worker”集合。后续 failed attempt 不清除此前 installed milestone；新注册 active worker 会暂时关闭门禁，直到它补装时间窗所需的历史版本。该控制面检查只在用户查询准备/导出创建时执行，不进入 Kafka 消费或逐 flow 热路径，也不新增表。

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
4. 旧 Hub 的 058/059/060 已作为行为与数据约束来源移植进 KISS schema：0011/0013 保存稳定运营商身份和 AddressSnap 发布血缘，0031 保存全局 classification profile、不可变版本对 metadata 与 `agents` worker ACK。MySQL 仍是管理/血缘库，不成为 worker 运行时依赖。`max_snapshot_bytes`/`WATCHDOG_ADDRESS_LIBRARY_MAX_SNAPSHOT_BYTES` 限制落盘对象，默认 512 MiB。
5. 停止 worker 的 MySQL/目录装载入口并保留 LKG/rollback 窗口。
6. migration 009 和 `address_dict_integration_test.go` 标注为历史 CH 字典实验；不回改历史 migration，也不新增“拆 IP_TRIE 字段”的 migration。待兼容窗口结束再以前向清理移除未使用对象。

本变更不删除 `flow_records` 已有派生列，也不恢复固定 30 天 TTL、持续 1m rollup 或逐记录 hash。Storage V2 的原始保留、日归档和 Kafka 坐标对账契约保持不变。

## 8. 验收门禁

- codec：deterministic golden、round-trip、未知版本/flag、截断/尾随、CRC/SHA 篡改、section 越界、zstd bomb/window、计数和资源上限；
- builder：真实 MMDB/IPDB generation + manual overlay，全量 row/count/checksum、重叠/优先级、crash/resume、相同 generation bytes 一致；
- parity：旧内存 loader 与 AddressSnap 对固定 v4/v6 corpus 的完整分类结果一致；
- worker：无 MySQL/CH 连接也能 cold start/LKG restore，下载中断、坏签名、坏对象、构建 OOM 预算、ACK 失败、回滚和原子可见性；
- performance：固定硬件记录 build time、object size、peak RSS、单核/multicore lookup throughput、p95/p99、atomic swap pause；一个进程无论服务多少 tenant 都只能保留一个 AddressSnap 索引，BART 与 DIR-24-8 只用同 corpus 决策；
- repeated publication：已激活 v1 后编辑地址草稿，旧 preview 必须失败；v2 异步构建期间 v1 仍 active，v2 checksum 必须变化，审批/激活后才能进入时间线。真实生产语料的发布构建、对象编译和 catalog swap 分开计时，禁止用一次性导入耗时替代发布 SLA；
- integration：管理 draft → build job → approve/sign → activate → worker download/install/ACK → Kafka 四协议 → CH fact version → query；失败路径证明旧版本持续服务；
- lifecycle fault gate：真实 MySQL 上模拟 builder 在 object/snapshot 已提交、operation job 终态未写时崩溃，过期 lease 由第二 owner 以同一 job/snapshot identity 收敛；同一链路继续验证 WADS approve/activate、retire/rollback 清除 retention，以及回切版本经 GC job 删除对象并只写一份 destruction receipt；
- reclassification：源/目标版本、重试/takeover/cancel、count/counter 守恒、generation 原子切换和 raw 不可用拒绝；
- regression：Flow/Watchdog 全库 test/race/vet/build、真实 MySQL/CH/Kafka 组合门禁和 rolling upgrade。

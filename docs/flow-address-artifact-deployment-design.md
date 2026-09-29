# Flow 地址制品与 Worker 部署模型变更设计

状态：**Approved design / implementation WIP / production not enabled**
日期：2026-09-19
范围：地址库、客户源地址边界、Flow 分类策略、Worker 分发/ACK/LKG/热切换。
不在本次设计范围：sFlow/NetFlow 解码、Kafka 数据通道、ClickHouse 查询优化、SNMP 数据面。

## 1. 结论

当前实现已经使用两种不同格式，但在发布生命周期上仍被强制绑成一个版本对：

| 数据 | 当前管理态 | 当前不可变制品 | Worker 内存结构 | 结论 |
|---|---|---|---|---|
| 全局地址库（Geo/ASN/运营商/集合） | MySQL 规范化表 | `WADS v1` 二进制 | IPv4/IPv6 有序范围索引 | 格式和热路径保留 |
| 设备客户源地址边界 | MySQL `device → customer → CIDR` 行 | classification JSON v2 的 `device_profiles` | 每设备 BART LPM trie | 从 classification 中拆出，改为独立小制品 |
| 六分类行为策略 | MySQL singleton profile | 与客户边界混在同一 classification JSON | classification snapshot | 改为独立小策略制品，禁止继续承载地址数据 |

目标不是把所有内容改成 YAML，也不是重做 WADS，而是明确分成三层：

1. **编辑层**：MySQL 是权威；UI 可用多行 CIDR，API 可导入 JSON/YAML/纯文本。
2. **不可变制品层**：大地址库使用 WADS；每设备客户边界使用规范 JSON；六分类策略使用规范 JSON。
3. **部署层**：一个很小的、签名的 Worker deployment manifest 引用上述独立制品，并保证整组原子安装。

这样：

- 修改客户 CIDR 不重编、不重传 WADS；
- 修改 Geo/运营商不重编客户边界；
- 修改策略不重编两类地址；
- 发布时显式选择 Worker；Worker 是否已有设备绑定只用于推荐，**不能阻止管理员选中、下载或安装**；
- Worker 只有在所有变更制品校验、编译、LKG 落盘成功后，才一次原子切换运行时视图。

## 2. 当前代码事实

> 2026-09-20 复核：客户源地址 create/update/delete 当前只提交 MySQL 编辑态和审计记录，未写待发布修订、未创建 operation job、未生成 deployment，也没有 worker ACK。工作树中的 migration 0049、deployment API/UI 和 worker loader 尚未提交、尚未进入生产 schema/二进制。下文的 deployment v2 是目标设计和在途实现，不能描述成当前生产能力。

### 2.1 WADS 地址库

`internal/flowdimension/address_snapshot_binary.go` 定义 `WADS v1`：

- 固定 magic/version/flags；
- zstd 压缩；
- 解压前后长度边界；
- payload CRC32C；
- 外层 object SHA-256；
- publication Ed25519 签名；
- string/value dictionary、Geo 节点、运营商/ASN、地址集合、IPv4/IPv6 非重叠范围。

`internal/flowdimension/address_snapshot_index.go` 将其编译成不可变有序范围索引，查询用二分；逐 Flow 无 MySQL、ClickHouse、文件或网络访问。

现有真实语料门禁已经证明当前格式足够快：

- 1,130,749 个源网络编译为约 4.29 MiB WADS；
- Worker decode+compile 约 82 ms；
- retained heap 约 48 MiB；
- 100 万次查找约 10.25 M lookup/s；
- p95 约 292 ns，p99 约 500 ns；
- catalog pointer swap 约 17.8 µs。

因此本次不将 WADS 改成 YAML/JSON，也不盲目替换为 DIR-24-8。WADS 的**传输格式**和 Worker 的**内存索引实现**保持解耦；只有固定生产硬件基准证明 DIR-24-8 有必要时，才单独替换 loader 的 IPv4 索引。

### 2.2 客户源地址边界

当前 MySQL 已经是正确的规范化关系：

```text
devices
  └─ flow_device_customers
       └─ flow_customer_source_prefixes (一行一个规范 IPv4/IPv6 CIDR)
```

当前不可变对象则在 `internal/flowdimension/classification.go` 中编码为 JSON v2：

```json
{
  "schema_version": 2,
  "version": 12,
  "dimension_snapshot_id": "...",
  "device_profiles": [
    {
      "device_id": "...",
      "source_prefixes": [
        {
          "id": "...",
          "cidr": "203.0.113.0/24",
          "customer_id": "...",
          "customer_name": "七牛"
        }
      ]
    }
  ]
}
```

Worker 将每个设备的 CIDR 编译为 `gaissmai/bart` LPM trie，所以客户边界查询同样是纯内存的。问题不是查询结构，而是这个 JSON 同时包含策略、客户边界和 `dimension_snapshot_id`，导致它不能独立发布。

这里的“客户源地址边界”不是每条 Flow 的原始 `src_ip`。原始 Flow 的 `src_ip/dst_ip` 仍作为 ClickHouse `IPv6` 列保存，以统一容纳双栈；IPv4 在驱动层会成为 IPv4-mapped IPv6。API、查询维度和 UI 输出必须统一 `Unmap()` 成普通 IPv4 文本，不能把 `::ffff:` 当成客户边界数据格式或用户可见身份。

### 2.3 当前强耦合点

以下现有契约共同形成强耦合：

- `flow_enrichment_publications` 每行同时保存 WADS 与 classification object；
- `EnrichmentVersionPublication` 同时要求 dimension 和 classification；
- `VersionLoader.Install()` 按一个 pair 下载、编译、持久化和 ACK；
- classification JSON 内保存 `dimension_snapshot_id`；
- Worker catalog 只对完整 pair 做 atomic install。

它保证了“一致组合”，但错误地把“一致性”实现成“共同构建和共同版本”。一致性应该由部署清单保证，而不是让两个制品互相嵌入版本号。

## 3. 可借鉴项目与边界

### 3.1 Akvorado

Akvorado 的 [`outlet.yaml`](https://github.com/akvorado/akvorado/blob/main/config/outlet.yaml) 允许用 CIDR 直接维护 network name/role 等属性；其[配置文档](https://github.com/akvorado/akvorado/blob/main/console/data/docs/02-configuration.md)也定义了 `network-sources`，可从 HTTP 获取外部数据并转换为 prefix attributes。这个模式适合作为**人工输入和外部数据源适配层**：简单、易读、适合 Git 管理。

但 YAML 不适合作为 Watchdog 的最终签名运行时制品：

- YAML 存在隐式类型、别名、重复 key 和多种等价编码；
- canonical signing、跨语言严格解码和向后兼容更难；
- 它没有解决定向 Worker 授权、desired/installed 差异、ACK、LKG 与原子组合切换。

因此只吸收其“CIDR 输入简单、可从远端源导入”的体验，不直接用 YAML 替代 WADS 或部署协议。

### 3.2 EdgeManager

EdgeManager 当前实现具备：

- GeoSnap（magic/version + gzip(bincode) + CRC）；
- IPv4 DIR-24-8、IPv6 有序区间；
- manifest/control pull 主路径与 checksum 感知；
- 后台构建索引、`ArcSwap` 原子切换；
- 本地快照持久化与冷启动；
- 节点状态携带已安装 checksum，失败保留旧版本。

Watchdog 已继承这一模式的大部分机制，而且签名和定向授权更严格：

- agent token 或 mTLS 的 HTTPS pull；
- Ed25519 签名 envelope；
- 内容寻址 LKG；
- downloaded/installed/failed ACK；
- atomic pointer install。

仍缺的不是底层热加载能力，而是**独立制品生命周期和完整的 desired/installed 状态投影**。

## 4. 目标领域模型

### 4.1 Address Catalog（全局地址目录）

- 全安装共享一套；无 tenant。
- 内容：Geo 层级、ASN、运营商、地址集合及地址属性。
- 编辑态：现有 MySQL 表和导入流程。
- 制品：继续使用 `application/vnd.watchdog.wads;version=1`。
- 版本：全局单调 `address_catalog_version`。
- 变更触发：基础地址、Geo、运营商、ASN/集合的新增、修改、删除或重新导入。

### 4.2 Device Customer Boundary（设备客户边界）

- 一个设备可有多个客户；一个客户可有多段 IPv4/IPv6 CIDR。
- 只表达“该地址是否属于这台观测设备上的哪个客户”，不复制 Geo、ASN、运营商属性。
- 编辑态继续使用规范化 MySQL 行；UI 提供每客户一个多行 CIDR 文本框。
- 每个设备生成一个独立规范 JSON 制品；不含 Worker ID，不含 WADS 版本。
- 版本按设备单调 `boundary_revision`。

建议 wire schema：

```json
{
  "schema_version": 1,
  "device_id": "01K...",
  "revision": 17,
  "effective_from": "2026-09-19T10:30:00Z",
  "customers": [
    {
      "customer_id": "01K...",
      "customer_name": "七牛",
      "prefixes": [
        {"id": "01K...", "cidr": "203.0.113.0/24"},
        {"id": "01K...", "cidr": "2409:8000::/32"}
      ]
    }
  ]
}
```

规范化规则：

- IPv4 必须 `Unmap()`，禁止把 `::ffff:x.x.x.x` 当 IPv6 保存；
- CIDR 必须 masked；
- customer 按稳定 ID 排序；prefix 按 family、network bytes、prefix length、ID 排序；
- 同一设备跨客户重叠直接拒绝，不按插入顺序猜优先级；
- JSON 严格解码，未知字段拒绝；
- SHA-256 基于规范 JSON bytes；
- customer name 只是当时显示快照，身份使用 customer ID。

### 4.3 Flow Classification Policy（分类策略）

策略从客户边界中拆出，保持很小：

```json
{
  "schema_version": 1,
  "revision": 6,
  "algorithm": "six-category-v2",
  "overseas_includes_hmt": true,
  "unmatched_endpoint_policy": "unknown",
  "effective_from": "2026-09-19T10:30:00Z"
}
```

策略中不再保存手工 home ASN、home operator ID 或 home province/city。地域和运营商来自 WADS；流入/流出的本地边界来自设备客户 CIDR。`internal/transit` 只作为诊断原因，不再是可以把流量静默排除在六分类总量外的管理开关。

## 5. Worker Deployment Manifest

制品独立后，用一个签名 manifest 表达 Worker 的期望组合：

```json
{
  "schema_version": 1,
  "deployment_id": "01K...",
  "generation": 42,
  "worker_id": "01K...",
  "effective_from": "2026-09-19T10:35:00Z",
  "address_catalog": {
    "artifact_id": "01K...",
    "version": 18,
    "format": "wads",
    "format_version": 1,
    "checksum": "sha256:..."
  },
  "classification_policy": {
    "artifact_id": "01K...",
    "version": 6,
    "format": "json",
    "format_version": 1,
    "checksum": "sha256:..."
  },
  "device_boundaries": [
    {
      "device_id": "01K...",
      "artifact_id": "01K...",
      "revision": 17,
      "checksum": "sha256:..."
    }
  ],
  "signing_key_id": "watchdog-agent-plan-v1",
  "signed_at": "2026-09-19T10:31:00Z",
  "signature": "base64..."
}
```

关键约束：

- manifest 是唯一原子部署单位，但不是数据合并单位；
- artifact 内容寻址，相同 checksum 不重复下载；
- 客户 CIDR 修改只生成一个新的 device boundary 和一个新的 manifest；
- WADS 修改只生成一个新的 address catalog 和引用它的 manifest；
- 未变化的已编译对象复用同一内存指针；
- 回滚不是修改旧行，而是生成更高 generation、重新引用旧 artifact 的新 manifest；
- `effective_from` 必须是 UTC 分钟边界；同一 Worker generation 单调且不可复用。

## 6. 发布选择与设备绑定

发布 UI 必须把“选择 Worker”作为显式步骤：

1. 保存地址库、客户边界或策略草稿；
2. 预览本次变更和受影响设备；
3. 选择一个或多个 active `flow_worker`；
4. 平台为每个所选 Worker 计算新的 desired manifest；
5. 异步 build/sign/publish；
6. Worker 拉取并 ACK。

`flow_worker_device_bindings` 只能用于：

- 默认勾选推荐 Worker；
- 显示拓扑和遗漏警告；
- 计算某 Worker 通常需要的设备边界集合。

它**不能**用于：

- 隐藏未绑定 Worker；
- 拒绝管理员把制品发布给未绑定 Worker；
- 在 object GET 时替代 publication target 授权；
- 唯一 Worker 未绑定时阻断首次启动。

真正的下载授权来自 `(deployment_id, worker_id)` target。管理员显式选择后，该 Worker 立即能看到 manifest 和对象；是否随后保存设备绑定是单独操作。

## 7. 分发协议

### 7.1 决策

继续使用 **HTTPS pull + agent token/mTLS + Ed25519 签名**，不使用 Kafka 传配置，也不引入 gossip：

- Kafka 只承载 Flow 数据，避免大对象、重放和消费组语义污染配置发布；
- Worker 常在 NAT/ACL 后，主动 pull 更简单；
- HTTP 容易实现 Range、ETag、大小限制、审计、重试和定向授权；
- 当前 agent registry、trust bundle、LKG 和 HTTP client 均可复用。

### 7.2 v2 API

```text
GET  /api/v1/flow-workers/{worker_id}/deployments/desired
     ?after_generation={n}&limit={n}&wait_seconds={0..30}

GET  /api/v1/flow-workers/{worker_id}/deployments/{deployment_id}

GET  /api/v1/flow-workers/{worker_id}/deployments/{deployment_id}/objects/{artifact_id}

POST /api/v1/flow-workers/{worker_id}/deployments/{deployment_id}/acks
```

要求：

- worker path ID 必须与认证 agent 身份相同；
- 只有 deployment target 中的 Worker 能读取 manifest/object；
- object ref 不接受客户端自由路径；
- manifest 使用严格 JSON、Ed25519 验签；
- object 同时核验 Content-Length、ETag/checksum header 与正文 SHA-256；
- 支持 Range/断点续传，禁止 redirect 转发 token；
- ACK 幂等；同一 boot ID 的状态只能前进，失败不清空已完成里程碑；
- long poll 是可选优化，首版可沿用固定间隔拉取。

ACK payload 至少包含：

```json
{
  "state": "downloaded|verified|installed|failed",
  "boot_id": "...",
  "software_version": "...",
  "generation": 42,
  "manifest_checksum": "sha256:...",
  "installed_artifacts": [
    {"artifact_id": "...", "checksum": "sha256:..."}
  ],
  "failure_stage": "fetch|verify|compile|persist|activate|ack",
  "failure_code": "...",
  "failure_message": "..."
}
```

## 8. Worker 加载、状态感知与热 reload

### 8.1 安装流程

```text
poll desired manifest
  → 验签与 generation 检查
  → 比较本地 content-addressed objects
  → 只下载变化对象
  → 校验长度/SHA/WADS CRC/schema/resource budget
  → 后台编译 WADS、device BART、policy
  → fsync object 与 manifest LKG
  → 组装 RuntimeSnapshot
  → 单次 atomic pointer swap
  → installed ACK + heartbeat status
```

`RuntimeSnapshot` 逻辑结构：

```go
type RuntimeSnapshot struct {
    DeploymentGeneration uint64
    AddressCatalog       *AddressSnapshotIndex
    Policy               *ClassificationPolicy
    DeviceBoundaries     map[DeviceID]*BoundaryTrie
    Provenance           ArtifactVersions
}
```

热路径只读取一个 atomic pointer；不逐条 Flow 读锁、不查数据库、不访问磁盘和网络。未变化对象直接复用指针。任一新对象失败，整个新 manifest 不可见，旧 LKG 继续服务。

### 8.2 event-time 与迟到数据

Worker 保留按 `effective_from` 排序的 deployment catalog。每条 Flow 用 event time 选择不晚于它的最新已安装 deployment；禁止用当前版本替代缺失历史版本。迟到窗口内引用的 WADS/boundary/policy 必须保留在内存或可从 LKG 恢复。

新事实至少记录：

- `deployment_generation`；
- `address_catalog_version` / snapshot ID；
- `boundary_revision`（按记录所属 device）；
- `policy_revision`；
- classification result/quality flags。

旧 `dimension_snapshot_id + classification_version` 在兼容期继续写，v2 列通过 CH `ADD COLUMN` 增量加入；不回写旧事实，旧行明确标记 legacy provenance。

### 8.3 状态感知

平台 UI 不再只显示“发布成功”，而显示每个 Worker 的收敛矩阵：

| Worker | 进程 | Desired | Downloaded | Verified | Installed | 地址版本 | 边界版本数 | 最后 ACK | 漂移/错误 |
|---|---|---:|---:|---:|---:|---:|---:|---|---|

状态来自两个已有权威的 join：

- agent registry heartbeat/run：在线、boot ID、软件版本、最后心跳；
- deployment ACK：desired/installed generation、artifact checksums、阶段错误。

不另造第三套 worker 状态机。heartbeat 增加 installed generation/checksum 摘要；详细制品列表仍由 deployment ACK 查询。

冷启动：

- LKG 完整：先恢复服务，再后台追 desired；
- LKG 损坏但控制面可用：重新下载，安装前 readiness=false；
- LKG 和控制面都不可用：fail closed，不以空地址表启动；
- 已运行时拉取失败：保持旧 snapshot，health=degraded，不清空分类能力。

## 9. 管理库契约

不继续扩展 pair 专用表。v2 使用通用但有界的四表：

### 9.1 `flow_artifacts`

| 字段 | 语义 |
|---|---|
| id | 不可变 artifact ID |
| kind | `address_catalog` / `device_boundary` / `classification_policy` |
| scope_id | `global` 或 device ID |
| version | kind+scope 内单调版本 |
| format / format_version | `wads/1` 或 `json/1` |
| checksum / size_bytes / object_ref | 内容身份与有界存储 |
| source_revision | 构建时固定的 MySQL row/draft revision |
| effective_from | 最早可被 manifest 引用的时间 |
| created_by / created_at | 审计 |

唯一键：`(kind, scope_id, version)`、`checksum` 可建普通索引用于内容复用；object 不可覆盖。

### 9.2 `flow_worker_deployments`

| 字段 | 语义 |
|---|---|
| id / worker_id / generation | 每 Worker 不可变单调部署 |
| effective_from | event-time 激活点 |
| manifest_checksum / object_ref | 规范签名 manifest |
| signing_key_id / signature / signed_at | 信任链 |
| state | `desired` / `superseded` / `cancelled`；安装状态不写这里 |
| operation_job_id | build/publish 追溯 |

唯一键：`(worker_id,generation)` 和 `(worker_id,effective_from)`。

### 9.3 `flow_worker_deployment_artifacts`

deployment 到 artifact 的不可变引用；主键 `(deployment_id, kind, scope_id)`。一个 deployment 只能有一个 address catalog、一个 policy，并可有多条 device boundary。

### 9.4 `flow_worker_deployment_acks`

保留 `attempted/downloaded/verified/installed` 时间、最新 state、boot/software version、manifest checksum、失败 stage/code/message。installed 里程碑不可被后续失败清空。

现有 `flow_enrichment_publications`、targets、acks 进入只读兼容期；v2 收敛后删除，不双写为永久架构。

## 10. 管理 API 与 UI

### 10.1 客户地址编辑

- 设备 → 多客户；每个客户一个多行 CIDR 输入框；
- 每行只接受 IPv4、IPv6 或 CIDR；单 IP 自动规范为 `/32`、`/128`；
- 粘贴后立即显示规范化结果、重复、覆盖和跨客户冲突；
- 保存只修改 MySQL draft，不自动伪装成已发布；
- 支持 `.txt`、JSON、YAML 导入/导出，但服务端统一解析成相同 DTO/行模型。

### 10.2 发布页

- 显示 draft 与当前 installed 的 diff；
- 显示 WADS 是否变化、哪些 device boundary 变化、policy 是否变化；
- Worker 列表包含所有 active flow workers；已绑定的预选，未绑定的仍可选；
- 发布是 operation job，页面显示 queued/running/progress/failed/succeeded；
- 成功只表示 desired manifest 产生；必须单独显示各 Worker installed ACK；
- 提供 retry、重新选择 Worker、rollback（生成新 generation）和查看错误；
- 禁止“发布后自动给所有 Worker”，除非管理员显式全选并确认。

## 11. 性能与容量门禁

### 11.1 保留 WADS 的依据

当前 WADS 约 10.25M lookup/s。即使按 1M flow records/s、每条两个 endpoint lookup 估算，地址查询约 2M lookup/s，单看现有基准仍有约 5 倍余量。这个结论只证明“无需现在换格式”，不替代生产硬件整链 benchmark。

### 11.2 新增门禁

- WADS 当前认证语料：decode+compile ≤ 1s、p99 lookup ≤ 1µs；
- device boundary：10 万 CIDR 编译有明确内存上限，单次 lookup 0 alloc、p99 ≤ 1µs；
- manifest 验签和 diff 不扫描 WADS 内容；
- boundary-only 发布下载字节中不得出现 WADS object；
- WADS-only 发布不得重建未变化 boundary object；
- atomic install ≤ 10ms，查询线程无全局锁；
- 任何 object 超过软预算时 job 失败并给出估算，硬上限只用于防内存/解压炸弹，不作为产品分页阈值；
- 固定硬件压测必须覆盖 1M records/s、双栈、多个设备和至少一次热 reload。

## 12. 可靠性、安全与一致性

- 构建 job 固定 source revision、所选 Worker IDs、effective time 和 operation job ID；重试必须生成相同 checksum；
- 所有对象先临时写、校验、fsync，再进入内容寻址 LKG；
- 签名覆盖 manifest 中每个 artifact 的 ID/version/format/checksum；
- WADS CRC 只检测损坏，SHA+Ed25519 才是内容身份和信任；
- JSON/YAML 上传有字节、行数、CIDR 数、嵌套深度和解析时间预算；
- Worker 拒绝 generation 回退和同代异文；
- manifest 安装全有或全无；
- ACK 网络失败只重试 ACK，不重复下载和编译；
- GC 前检查 desired/installed deployment、event-time 迟到窗口、repair/reclassification 和 LKG 引用；
- 操作审计记录谁修改、谁选择 Worker、谁发布、谁回滚，不记录 agent secret。

## 13. 兼容迁移顺序

### Phase A：冻结契约，不改数据面

- [x] 冻结三种 artifact schema、manifest v1、ACK 和错误码；
- [x] 冻结 v1 pair → v2 artifact/deployment 的 reader-first 映射与单向切换规则；
- [x] 对当前 WADS、classification JSON、HTTP/LKG 做 golden baseline；
- [ ] 记录生产硬件性能基线。（2026-09-28 复核：改回待办（原勾选不成立/夸大） — 基线均在 darwin/arm64 开发机测得，没有生产硬件基线；`docs/flow-address-library-scale-baseline.md:21`）

### Phase B：新增 v2 管理面

- [x] 新增四张 v2 表和持久化实现；
- [x] WADS builder 输出注册为 address artifact，不改 WADS bytes；
- [x] 将当前 per-device customer rows 编译为独立 boundary JSON；
- [x] 将剩余策略编译为独立 policy JSON；
- [ ] 发布 operation job 接受显式 Worker 选择，binding 只作推荐；（2026-09-28 复核：改回待办（原勾选不成立/夸大） — 地址激活按 binding 自动给已绑定 worker 发 v2，无显式选择与确认，违背 §6/§10.2；`28014a8be`; `internal/server/flow_worker_deployments.go:258-320`; `internal/server/handlers_address_dimension.go:242-251`）
- [x] API/UI 展示编辑态、desired、installed、worker health 和失败状态。

### Phase C：Worker 双读

- [x] Worker 支持旧 pair v1 和 deployment manifest v2；
- [x] v2 loader 按 checksum 只下载变化对象；
- [x] 独立编译后组装 composite runtime snapshot 并 atomic swap；
- [x] LKG 保存 v2 manifest 与对象图并支持冷启动恢复；
- [ ] heartbeat/ACK 暴露 desired/installed drift，installed 里程碑不可被迟到 ACK 倒退。（2026-09-28 复核：改回待办（原勾选不成立/夸大） — ACK 侧成立；heartbeat 没有 installed generation/checksum 摘要；`internal/server/flow_worker_deployments.go:857-868`）

### Phase D：事实版本与查询 provenance

- [ ] ClickHouse 增加 deployment/address/boundary/policy 版本列；
- [ ] 新 Flow 写入四个 provenance 字段；
- [ ] 查询/报表显示版本并能定位 Worker deployment；
- [ ] 旧行明确显示 legacy，不伪造 v2 版本。

### Phase E：迁移和拆旧

- [ ] 由当前 active pair 生成首个 v2 artifacts + manifests；（2026-09-28 复核：部分完成 — 首代 generation 接在 v1 最大值之后；制品取自 active WADS 加当前 profile/边界而非 pair 对象；无迁移工具；`28014a8be`; `internal/server/flow_worker_deployments.go:626-642`）
- [ ] 所选 Worker v2 installed ACK 后将新发布入口切到 v2；（2026-09-28 复核：部分完成 — UI 入口已切到 v2，但不以 installed ACK 为前提；激活、绑定、profile 变更仍自动写 v1；`552d9752b`, `c33b1c652`）
- [ ] 观察至少一个迟到窗口和一次回滚演练；
- [ ] 停止创建 pair v1；
- [ ] 删除旧 pair routes/loader/table 前验证全库无引用。

## 14. 测试清单

### 单元测试

- [x] WADS golden/CRC/SHA/未知版本/截断/zstd bomb；
- [ ] boundary JSON 确定性、严格字段、IPv4 unmap、IPv6、掩码、重复和跨客户重叠；（2026-09-28 复核：改回待办（原勾选不成立/夸大） — 未测未知字段、重复 ID、非 canonical 的拒绝（代码在 deployment_artifacts.go:82-97,216-230）；`internal/flowdimension/deployment_artifacts_test.go:9-88`）
- [ ] policy schema 与算法版本；（2026-09-28 复核：改回待办（原勾选不成立/夸大） — 只测 round-trip，无 schema/算法版本的拒绝测试；`internal/flowdimension/deployment_artifacts_test.go:90-109`）
- [ ] manifest canonical bytes、签名篡改、generation 回退/同代异文；（2026-09-28 复核：改回待办（原勾选不成立/夸大） — canonical 与篡改有测试；generation 回退、同代不同内容只有代码无测试；`internal/flowworker/deployment_manifest_test.go:14-75`）
- [x] BART v4/v6 LPM、无匹配、同设备客户归属；
- [x] runtime composite swap 全有或全无。

### 集成测试

- [x] 只改客户 CIDR：不构建、不下载 WADS；
- [ ] 只改 WADS：不重建 boundary；（2026-09-28 复核：部分完成 — 服务端按 source_revision 复用、worker 按 checksum 不重复下载；但每次重编全部 BART；无测试；`internal/server/flow_worker_deployments.go:495-508`; `internal/flowworker/deployment_loader.go:127-150`）
- [x] 未绑定但被显式选择的 Worker 可以 list/download/ACK；未选择 Worker 404；（2026-09-28 复核：复核备注 — 显式选择链路测试已按共享 token 修复并于 2026-09-28 重跑通过；未选 worker 的 404 未测；共享 token 下 path ID 不绑定身份；`internal/server/flow_enrichment_publications_integration_test.go`）
- [ ] 多 Worker 使用不同 device boundary 集合；（2026-09-28 复核：部分完成 — 一个 job 给所选 worker 发同一设备集；不同集合需分次发布或靠激活按 binding；无测试；`internal/server/flow_worker_deployments.go:264-320,405-422`）
- [ ] 下载中断、坏 SHA、坏签名、编译超限、fsync 失败均保留旧 LKG；（2026-09-28 复核：部分完成 — 单元测了坏 checksum、落盘失败、签名篡改；v2 下载中断、编译超限、fsync 失败与集成测试未做；`internal/flowworker/deployment_loader_test.go:102-141`）
- [ ] installed ACK 丢包后只重试 ACK；（2026-09-28 复核：部分完成 — 代码上重入时只 persist 加 installed ACK，对象直接命中 LKG；无 v2 测试（v1 有）；`internal/flowworker/deployment_loader.go:85-93`）
- [ ] Worker 离线后从 LKG 冷启，恢复相同 generation/checksums；（2026-09-28 复核：改回待办（原勾选不成立/夸大） — v2 冷启动只有单元测试且未断言 checksum；集成测试只验了 v1 Restore；`internal/flowworker/version_lkg_test.go:250-295`）
- [ ] event-time 在两个 deployment 间正确选择，缺历史 fail closed；（2026-09-28 复核：部分完成 — v2 复用 catalog 的事件时间选择（v1 已测）；无 v2 两 deployment 间选择与缺历史的测试；`internal/flowworker/version_catalog.go:212-228`）
- [ ] rollback 生成更高 generation 并引用旧 artifacts。（2026-09-28 复核：部分完成 — 无显式 rollback；源回退后重发会复用旧 artifact 且 generation+1，但未测；地址回滚不会自动下发；`internal/server/flow_worker_deployments.go:439,463,503,626-639`）

### 变更与回归测试

- [ ] v1/v2 双读 rolling upgrade；旧 Worker 不能收到不支持的 v2；（2026-09-28 复核：部分完成 — 只有单元测试，无真实滚动升级；服务端不按 capability 拦截旧 worker；`internal/flowworker/version_dual_sync_test.go:43-82`）
- [ ] 从 active pair 自动生成首个 v2 deployment，结果分类逐条 parity；
- [ ] sFlow/NetFlow fast decode、Kafka offset、CH 写入吞吐不回退；
- [ ] 六分类总量、客户 v4/v6、Geo/运营商名称与旧正确结果对账；
- [ ] agent registry、token/mTLS、RBAC、audit、operation job 回归；
- [ ] race/vet/test/build、真实 MySQL+ClickHouse、固定硬件性能门禁。

### 已提交门禁

- [ ] 每一阶段按纵向切片独立提交；（2026-09-28 复核：部分完成 — Phase B/C 已提交但非干净纵向切片（c00e6c646 夹带对账代码）；D/E 未开始；`c00e6c646`, `28014a8be`, `1270eb196`, `552d9752b`）
- [x] 设计、migration、server、worker、UI、测试不得长期堆在一个未提交工作区；（2026-09-28 复核：已完成 — `c00e6c646`, `28014a8be`, `1270eb196`, `552d9752b`, `8e7ec860e`；现已全部提交（此前约 4–8 天为未提交 WIP））
- [ ] pair v1 只在 v2 真实 installed/LKG/rollback 闭环后删除；
- [ ] 删除旧表和路由必须有静态引用扫描与 fresh-install schema 门禁。

## 15. 验收标准

1. 管理员在设备客户页粘贴多行 v4/v6 CIDR，保存后能选择当前唯一或任意 active Worker 发布。
2. Worker 即使事前没有 device binding，只要被本次 deployment 选中，就能下载、安装和 ACK。
3. 修改一个客户 CIDR时，WADS artifact ID/checksum 和下载次数保持不变。
4. 修改运营商/Geo 时，所有 boundary artifact ID/checksum 保持不变。
5. Worker 页面能看到 process health、desired、installed、版本差异、错误阶段和最后成功 LKG。
6. 热切换失败不影响旧版本继续分类；重启可从 LKG 恢复。
7. 逐 Flow 热路径仍为内存 WADS lookup + 每设备 BART lookup，零 DB/file/network。
8. 新事实同时记录 address/boundary/policy/deployment provenance，查询结果可追溯。

# Flow 模块执行清单

> 这是 Flow 唯一执行状态。需求见 [flow-direction-requirements.md](flow-direction-requirements.md)，现行设计见 [flow-module-design.md](flow-module-design.md)，数据面取舍见 [flow-pipeline-adr.md](flow-pipeline-adr.md)。平台通用缺陷只登记到 [platform-refactor-tasklist.md](platform-refactor-tasklist.md)。

## 1. 自动循环协议

每次“继续”以及一个切片完成后，都执行同一状态机：

1. 读取本页的“活动切片”，核对依赖和完成条件；
2. 只修改该切片拥有的目录；既有平台问题不顺手重构；
3. 依次完成设计、编码、单元、集成、变更设计、变更测试、回归和已提交；
4. 把命令或制品写入证据栏，再勾选；不得用“代码看起来完成”代替测试；
5. 本地验收通过后只暂存当前切片拥有的文件并创建提交；以 `git show --stat --oneline <hash>` 可见、`git status --short` 不再显示该切片文件、从该提交检出后能复现构建/测试为“已提交”门禁，未提交代码不得标记交付完成；
6. 若外部环境阻塞，只登记门禁并选择下一个无依赖切片；若平台能力阻塞，登记平台清单后继续 Flow 独立切片；
7. 切片八类任务全部关闭后，原子地更新活动切片并立即开始下一轮，不等待用户再次输入“继续”。

任何第二 Flow 数据 topic、本地 WAL、新状态库、新任务引擎或常驻服务，都必须先有 ADR、容量/故障证据和删除方案。

## 2. 当前状态

**活动切片：FLOW-05F — Flow Explorer 查询规划与前后端闭环。** 用户时间窗、目标点数、展示步长和 1m/1h 物理源已解除错误绑定；2–4 维短窗已从同一 base fact 返回真实 tuple 和桑基；服务端 typed filter validate/complete/canonical、跨维字段的 24h base 路由、Explorer 表达式和生产页面增量验收已完成，当前继续保存/共享过滤器与预配置异步联合索引。此前 FLOW-04C3B2B scanner 未取消，作为下一无依赖数据面切片保留；不同切片的文件不得混入同一提交。

FLOW-04B 原“平台依赖未解除”的判断已经复核修正：handler registry、分类型并发 worker、lease/heartbeat/cancel/takeover/retry 和版本化 payload 已存在；immutable dimension publication 不阻断对已富化 base facts 的 rollup。平台仍缺通用 per-tenant cron/跨类型扫描背压，Flow 本切片只实现有界的域调度适配，通用化仍留在 PLAT-04B。

| 阶段 | 设计 | 编码 | 单元 | 集成 | 变更设计 | 变更测试 | 回归 | 已提交 | 状态 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| FLOW-01 RawFlow collector | [x] | [x] | [x] | [ ] | [x] | [x] | [x] | [x] | 外部集成门未过 |
| FLOW-02 Kafka worker/GoFlow2 | [x] | [x] | [x] | [ ] | [ ] | [ ] | [x] | [x] | 外部集成门未过 |
| FLOW-03 sampling/dimension | [x] | [x] | [x] | [ ] | [x] | [x] | [x] | [x] | 数据面完成；API 接线/外部集成待办 |
| FLOW-04 CH/rollup/metrics | [x] | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | **进行中** |
| FLOW-05 query/API/UI | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | 待办 |
| FLOW-06 correction/reclass/export | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | 待办 |
| FLOW-07 overseas/VPN | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | 待办 |
| FLOW-08 HA/lifecycle/release | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | 待办 |

“已提交”只在该阶段当前声明为完成的交付范围全部进入可复现提交后勾选；阶段仍有后续切片时，不得因为其中一部分已经提交而提前勾选整行。

### 2.1 本轮提交与可复现证据

- 代码提交：`886ccb2b feat(flow): complete raw pipeline dimensions and rollup lifecycle`；包含 027/028、旧 collector state-restore/cleanup 删除、分层 Geo/address set、worker/CH/query 契约及 rollup production handler/store/runtime。
- 静态与本地回归：`go build ./...`、`go test ./...`、`go vet ./...`、Flow 定向 `go test -race`、EdgeManager Geo 导出 Python unit/compile、`git diff --check`。
- 真实 MySQL：隔离空库执行 001→028 和二次幂等检查；验证 027 后旧表不存在；重复执行 028 可从已有 v1 job 回填最大 bucket 且不会覆盖更高水位；真实 operation-job worker 完成 initial + repair 两个 generation。测试临时库执行后已删除。
- FLOW-03B 真实 CH：`66453df3 test(flow): verify Geo rollups on ClickHouse`；同一组三条 base fact 在 continent/region/country/province/city 五级分别保持 600 raw bytes/3 records，同层稳定 ID 无重复；七组真实 CH 数据回归、Flow race、全库 test/vet 与 diff check 通过。
- FLOW-03B Kafka corpus：`66c28373 test(flow): replay four protocols through Kafka` 与 `282e2a22 fix(flow): flush accepted datagrams on shutdown`；Akvorado sFlow v5、NetFlow v5/v9、IPFIX pcap 经真实 UDP Receiver/source admission/自动协议识别 → RawFlow → Kafka 4.3.1 → GoFlow2 worker → ClickHouse 26.3，逐 block 从 fact 重算 receipt checksum 并核对 count/counter，五级 Geo rollup 守恒。测试同时复现并修复 Receiver 先停时取消 Kafka 在途记录的问题：已接收记录由 producer 生命周期持有，`Close` flush 后 6/6 到达。隔离 topic/database 均已清理。
- FLOW-05A 真实 CH：`c876e974 test(flow): verify versioned aggregate limits`；跨版本同名维度不合并、等值 TopN tuple tie-break、版本化 `_other`、mixed-version metadata、bytes/records 守恒，以及 `max_rows_to_read/max_result_rows` 拒绝且零部分结果已验证。服务端长查询/静默传输的单节点时限证据由 FLOW-08A3 承载。
- FLOW-05A 并发：`2b45d579 test(flow): verify concurrent query isolation`；共享一个 production runner/4-connection native pool，64 个 city TopN+other 与 total 请求交错执行，连续 5 轮共 320 次均保持 decoder/result/completeness 隔离且数值守恒，race 通过。固定硬件吞吐和集群容量仍未验证。
- FLOW-07B：查询核心为 `f8beffaf feat(flow): add overseas KPI query core`；真实 ClickHouse 数据门禁为 `54ac6173 test(flow): verify overseas queries on ClickHouse`，覆盖 IPv4/IPv6/双端 unknown、in/out 端点镜像、country/region TopN+other+unknown、流量守恒和 generation 2 迟到修复；五组真实 CH 数据回归、Flow race、全库 test/vet 与 `git diff --check` 通过。
- FLOW-07A2：`2e9d9474 feat(flow): materialize versioned VPN candidates`；003 前向 migration、原子 candidate+marker materializer、稳定 replay token、版本 provenance 已进入独立提交；Flow 全范围 race、`go test ./...`、`go vet ./...` 和文档 diff check 通过。003 已在 ClickHouse 26.3 LTS 空库及 statement replay 上通过，candidate 数据聚合/迟到 generation 仍保留外部门禁。
- FLOW-07A3：`ad4c2c6e feat(flow): score authoritative VPN candidate generations`；单查询 latest-marker reader、50,000 条硬上限、严格 evidence/ratio/provenance 校验、空 generation 和 all-or-nothing scorer bridge 已提交；真实 CH 执行证据由后述 `55a3b166` 独立承载。
- FLOW-07A 真实 CH：`55a3b166 test(flow): verify VPN candidates on ClickHouse`；覆盖双向会话归一、稳定 conversation SHA-256、coverage/record/quality 完整度、迟到 generation 2、评分 gate 和 generation 3 权威空修复；六组真实 CH 数据回归、Flow race、全库 test/vet 与 diff check 通过。
- FLOW-07A 版本切换：`a7ea3f2d test(flow): verify VPN rule rollback isolation`；同一会话的 dimension snapshot/Geo/classification v1/v2 物化为独立候选；规则集 v1→v2→回切 v1 时得分分别保持 20/30/20，回切只推进 v1 generation，v2 历史 generation 不变。真实 CH race 连续 5 次通过；平台 publication 投递仍是独立门禁。
- FLOW-04C2：`041eebf4 feat(flow): expose rollup lifecycle metrics`；CH runner 原子统计 1m/1h attempt/success/retryable/permanent、initial/repair、last success 与最大完成 bucket，hub runtime 组合低基数 provider；定向 race/vet 与 `go test ./...` 通过。VM 可抓取的 hub machine endpoint 仍由 PLAT-04E 承担。
- FLOW-04C VM 兼容门禁：`c61bb658 test(flow): verify VictoriaMetrics round trip`；生产 collector/worker metrics handler 经真实 loopback HTTP server 输出 Prometheus text，写入现有 VictoriaMetrics 后分别核对 Kafka durable record 和 lag 值，并检查落库 series 不含 tenant/exporter/ASN/prefix/IP/topic/partition 标签。真实 race 连续 2 次通过；每轮唯一测试 series 均精确删除。当前 VM 容器未配置 `promscrape.config`，因此这不冒充 vmagent pull 或两个生产命令联调。
- FLOW-04C production process 基线：`0ce9075d test(flow): run production pipeline end to end`；测试实际编译并启动 `watchdog-flow-collect` 与 `watchdog-flow-worker`，隔离 topic/database 中 5 个 NetFlow-family + 1 个 sFlow datagram 经 Kafka 4.3.1 和 ClickHouse 26.3 收敛为 38 条事实、四种协议和 durable receipt；两个真实进程的 Kafka 指标为 6/6，template missing/rejected/retryable 均为 0，并经现有 VM import/query 验证。测试发现并修复 worker 在空 Geo catalog 上先调用 `LoadHistorical` 而必然启动失败的问题；Geo bootstrap 现在明确按 oldest→newest 输入、末项 active。进程、topic、database 和测试 series 成功/失败均清理；本项不冒充尚未执行的 promscrape pull 与故障矩阵。
- FLOW-04C production VM pull：`c2845406 test(flow): scrape production process metrics`；同一 production harness 临时启动已有 `victoriametrics/victoria-metrics` 镜像，使用 250ms `promscrape.config` 从容器内主动抓取 collector/worker；两 target 的 `up=1`，Kafka durable/committed records 均为 6。VM 使用动态 loopback 映射和临时存储，结束后删除；现有 `victoriametrics:8428` 不重建、不改配置、不写测试 series。
- FLOW-04C UDP safety：`95bb583f test(flow): reject unsafe UDP inputs`；production collector 监听同一真实 UDP socket，从本机非 loopback IPv4 注入格式正确但 plan 未授权的 NetFlow，`rejected=1`；再注入 4097-byte datagram 命中 4096-byte 上限，`oversize=1`。两类输入之后 Kafka records 仍为 0，正常四协议随后仍收敛为 6 个 datagram、38 条事实；测试不以协议解析失败冒充 source admission，也不以 oversize 冒充 RXQ overflow。
- FLOW-04C ClickHouse recovery：`1f2d4762 test(flow): recover production worker after ClickHouse fault`；worker 经透明 TCP 代理连接真实 CH，握手后丢弃服务端响应。collector 已将首条 NetFlow durable 写入 Kafka 时，CH retryable error 增长且 worker committed records 保持 0；恢复代理后同一 worker 自动重试到 committed=1，再完成四协议 6/6、38 条事实和 receipt。共享 CH 容器未停止，本项验证的是生产 backpressure/replay，不以进程重启掩盖失败。
- FLOW-04C Kafka recovery：`3e5c6abf test(flow): recover production pipeline after Kafka pause`；测试启动无卷、随机 loopback 端口的隔离 Kafka 4.3.1 KRaft broker，pause 后 v9 template 停留在 collector buffer，durable records 保持 0；unpause 后 collector ack=1、worker committed=1，随后经 CH 故障恢复和完整 corpus 收敛至 6/6、lag=0、template missing=0。整个 NetFlow corpus 复用一个稳定 UDP exporter session；现有开发 Kafka 未暂停。
- FLOW-04C worker rebalance：`013f3f9f test(flow): verify production worker rebalance`；同 group 的两个真实 worker 从 A 独占 4 个 partition 收敛为 2+2；追加流量后 broker group committed total=end=7、lag=0。优雅停止 A 后 B 第二次 assignment 并接管 4 个 partition，再追加流量后 committed=end=8、lag=0；两端 template missing/rejected 为 0，CH `FINAL` 保持四协议事实收敛。累计进程指标不作为 offset 权威，验收直接读取 broker group。
- FLOW-04C worker strong-kill：`7cb8ef41 test(flow): recover after production worker kill`；A/B 先稳定为 2+2，随后对 A 执行 `Process.Kill`，不提供 revoke/commit 机会；Kafka session 失效后 B 第二次 assignment 并接管 4 个 partition，新 flow 后 broker committed=end=8、lag=0，template missing/rejected=0，CH 事实继续收敛。被杀进程内存已不存在，因此不伪造其 `lost_partitions_total`，以 broker ownership/offset 和存活 worker assignment 为权威。
- FLOW-04C broker restart：`0b141b3b test(flow): recover production worker after broker restart`；四协议 production harness 在 committed-next-offset=1 后实际 restart 隔离 Kafka，保留 topic/group 数据，原 worker 不退出且 offset 不回退，随后继续通过 CH 中断恢复、四协议收敛、2+2 rebalance 与 SIGKILL takeover。测试同时修正公共 franz-go 配置遗漏的 `AlwaysRetryEOF`：启动 Ping 已先验证配置，后续替换连接的首请求 EOF 按 broker restart/load 恢复，不误报 TLS 并终止 worker。
- FLOW-05F typed filter：`1ffb3939 feat(flow): add typed filter lifecycle`；authenticated catalog/validate/complete、canonical AST、前端 AND/OR/NOT parser、rollup/base 自动路由、单维 filtered total、资源字段 fail-closed 权限和 IPv4-mapped CIDR 已提交。真实 CH 验证 city/ASN/CIDR 与 filtered total，真实 HTTP 验证 validate→canonical query→base runner；8090 生产包浏览器验证 5m..1y/自定义预设及 `asn>=AS4134 AND src_ip IN (10.0.0.9/8)` 显示 `flow_records source`。
- FLOW-04C3B1 ingest-audit projection：`3f3501d8 feat(flow): add bounded ingest audit projection`；migration 006 保留 base tenant/time 排序，新增 offset-ordered narrow projection 并以 `rebuild` 维护 ReplacingMergeTree 一致性，存量同步 materialize。真实 200 万行前后结果一致；read rows `2,000,000→16,384`、read bytes `183,630,373→552,673`，EXPLAIN 命中 projection；表空间 `175,078,419→264,954,749`。Bloom probe 即使只查单 batch 仍读 434,176 rows/17,309,576 bytes，已排除。全套 CH 数据集成测试在 001..006 上通过；这不冒充固定硬件容量/N+1。
- FLOW-04C3B2A compare core：`1f00d551 feat(flow): compare ingest audit evidence`；typed receipt/fact/counter/mismatch 结构和无状态 comparator 已提交，固定六类单一 reason 优先级、writer 同字节序 checksum、invalid estimated 计数语义、batch/fact 硬上限、重复 identity 与溢出拒绝。单元直接从 production `PrepareBlocks` 生成证据，证明 comparator checksum 与写入 receipt 一致；全库 test/vet 与 flowch race 通过。
- FLOW-04C3 边界审计：receipt 是同 partition、可跨 tenant 的 block 摘要；不能复用 tenant rollup 水位。已冻结 Kafka committed-next-offset 闭合规则、`FINAL` 去重、count/counter/checksum 对账、固定 mismatch reason、有界 keyset 扫描和不完整时保留上次 gauge 快照。发现 legacy `inserted_at` 实为 source received time，不是落盘/cursor 时间；全局 operation job 登记为 PLAT-04F。
- FLOW-04C3A：`79400cc6 feat(flow): version ingest receipt audit metadata`；migration 004、receipt schema v2、跨 tenant/时间/packet 元数据和 native contract 已提交，Flow race/vet 与全库 test/vet 通过；scanner/全局 job/真实 CH 访问路径仍属 FLOW-04C3B。
- FLOW-06A：`3d63a5a7 feat(flow): preserve supplier fact provenance`；migration 005、worker schema 3、supplier baseline/customer override bitset、native exact-column contract 已提交；Flow race/vet、全库 test/vet 与 diff check 通过，005 已在 ClickHouse 26.3 LTS 空库执行，mixed worker/cutover 数据门禁仍保留。
- FLOW-06A2：`d00b620a feat(flow): expose raw fact detail view`；raw 明细不受 customer disposition 影响，只开放协议/采样/资源/observation 字段和资源过滤，view 进入 typed result；Flow race/vet 与全库 test/vet 通过。
- FLOW-06A3：`d56bc3f6 feat(flow): expose complete supplier detail view`；supplier 明细映射冻结的 supplier 基线，完整过滤窗在 cursor 之前计算 `fact_schema` 证据，旧事实、缺失或矛盾 evidence 均全页失败；Flow race/vet、全库 test/vet 与 diff check 通过。
- FLOW-05C1：`8a4a9b24 feat(flow): publish query capability registry`；aggregate/customer 与 detail 三层能力从 compiler 的同一 registry 导出，逐项验证声明与接受/拒绝一致且返回副本不可污染；`internal/flowquery` race/vet、全库 test/vet 与 diff check 通过。
- FLOW-08A1：`24bd111b feat(flow): validate ClickHouse migration lifecycle`；真实 001..005 loader、精确字节 checksum、quote/comment-aware statement splitter 和 fail-closed recorded-state planner 已提交；Flow race/vet、全库 test/vet 与 diff check 通过。
- FLOW-08A2：`5182f28e feat(flow): operationalize ClickHouse migrations`；embedded migration CLI、持久锁/statement checkpoint、固定 Kafka/ClickHouse Compose 和真实生命周期测试已提交；ClickHouse 26.3 空库/重放/dirty resume 与 Kafka 4.3.1 `acks=all` 生产消费通过。
- FLOW-08A3（单节点范围）：`d67f08aa test(flow): prove migration restart compatibility`、`5d7f9761 test(flow): recover migrations after statement deadline` 与 `f13b57d3 fix(flow): bound silent ClickHouse operations`；旧/新 migration set、drift fail-closed、CH/Kafka restart、调用方 deadline、内部 operation deadline、无隐式 DDL retry 和新连接池恢复已验证。集群和容量门禁未关闭。
- FLOW-08A3 ambiguous INSERT：`21a74da8 test(flow): recover lost ClickHouse insert ack`；透明代理放行 column metadata 与客户端数据块后静默丢弃最终响应，证明超时返回时事实已经提交但 receipt 尚未写入；同一 `PreparedBlock`/dedup token 经新连接重放后，`FINAL` fact count、raw bytes/packets、单条 receipt 与 checksum 精确收敛。真实 CH race 连续 5 次通过，隔离数据库逐轮删除。
- FLOW-08A3 partial response：`ffcc64e3 test(flow): reject partial ClickHouse responses`；透明代理在服务端响应第 64 byte 截断 TCP，production executor 必须立即返回 transport/decode error，不等内部 operation deadline、不暴露半条 result、不在连接层重试；新连接池随后恢复。三种代理故障组合真实 CH race 连续 5 次通过。
- FLOW-08A3 dedup-window-independent replay：`33c3f95f test(flow): verify replay beyond dedup window`；隔离表显式设置 `non_replicated_deduplication_window=0` 并停止 merge，同一 block 完整写两次后物理层为 4 facts/2 receipts，证明未借助短期 server dedup；`FINAL` 仍收敛为 2 facts/1 receipt、900 raw bytes/2 packets 和精确 checksum。四类 CH 故障/幂等门禁组合 race 连续 5 次通过。
- FLOW-08A3 Kafka worker 恢复：`11cd81b0 test(flow): verify Kafka template replay recovery`；同一真实 consumer group 先提交 v9/IPFIX 模板和数据至 offset 4，新数据不带模板；全新 worker 经 assignment 有界回放后能解码新数据，注入 durable failure 时 committed offset 保持 4，下一全新 worker 再次接管并精确推进至 6。真实 Kafka 连续 5 次及正常 corpus 组合 race 2 次通过，隔离 topic 已清理。
- 尚未具备的证据：Linux `SO_RXQ_OVFL` 压力、实际 worker/CH restart 与组合故障、版本混跑、集群 DDL、固定硬件压测和 72h soak，继续保留在 §5 外部门禁，不能由本轮单节点证据替代。

## 3. 已完成实现与证据

### FLOW-01 RawFlow collector

- [x] `UDP -> RawFlow protobuf -> watchdog.flow.raw-v1`；Kafka 是唯一队列/重放边界。
- [x] 来源前缀准入、签名 plan、franz-go 有界 producer、TLS/mTLS、PLAIN/SCRAM secret-file、flush。
- [x] 删除旧 `internal/flowcollect`、Kafka bootstrap、WAL/collect-state/attempt/state-restore 管理链；migration 027 删除遗留表。
- [x] 证据：`internal/flowstream/*_test.go`、`cmd/watchdog-flow-collect/main_test.go`、MySQL 001→028 前向迁移与 027 精确删除测试、Flow race/vet。
- [x] **已提交**：`886ccb2b`；提交中不包含工作区原有 `dbbak-origin-20260824-035506.sql`。

### FLOW-02 Kafka worker 与 GoFlow2

- [x] partition 内串行、跨 partition 并行；成功 durable handler 后才 mark offset。
- [x] sFlow v5、NetFlow v5/v9、IPFIX；模板/采样状态按 owner 隔离；assignment 有界回放且不倒退 committed watermark。
- [x] 生产入口装配签名历史 plan、checksummed dimension/classification publication、Geo bundle、Kafka、GoFlow2、CH。
- [x] 证据：Akvorado 四协议 pcap、多 sampler `4000/2000`、fresh processor deterministic replay、decode/adapter fuzz、Flow race/vet；真实 UDP→Kafka→worker→CH 正常链路见 `66c28373`/`282e2a22`，真实 v9/IPFIX assignment 回放与 committed offset 故障恢复见 `11cd81b0`。
- [x] **已提交**：`886ccb2b`。

### FLOW-03 采样、方向和六维

- [x] `raw` 永久保留；`estimated` 只放大一次；协议字段 > sampler/data-source rule > exporter default > unknown。
- [x] event-time immutable dimension/classification/Geo catalog、LPM、address set、ISP/ASN、六类和守恒。
- [x] EdgeManager 导出严格 `flow-geo-v1`/`flow-geo-v2` bundle，watchdog 只读；不直连或回写 EdgeManager。v1 保留扁平 country/admin/city 兼容，v2 用稳定 code 表达五级路径。
- [x] 证据：counter conservation fuzz、classification property、catalog race、Geo manifest Python tests。
- [x] **已提交**：`886ccb2b`。

### FLOW-04 已完成部分

- [x] ClickHouse 五表权威 migration 和 Go encoder schema contract test。
- [x] stable record/block ID、records→receipt 同步原生 ch-go LZ4/chpool 写入、retry/error 分类和 Kafka offset durable bridge。
- [x] 单 tenant + 已关闭 bucket 的 1m/1h 原子 rebuild；一次生成所有维度和 `_generation` marker，repair 用更高 generation。
- [x] 证据：`internal/flowch/*_test.go`、Flow 全包 `go test -race` 和 `go vet`。

## 4. 下一队列

### FLOW-03B 地址与 Geo 层级（数据面已完成，剩余项受门禁）

- [x] **设计修正**：冻结三种不同口径：唯一主前缀、单路径 Geo 层级、可重叠 address set；ASN/ISP 都是可空属性，不是地址段或组的身份。集合采用 `(selector ∪ members ∪ includes) − (exclude members ∪ excludes)`，查询并/交/差从 base membership 去重求值。
- [x] **编码（地址快照）**：嵌套 CIDR 在发布时由根到叶合并 labels，同 key 由子段覆盖；运行时仍只做一次 LPM；无 ASN 地址可命中多个 address set。
- [x] **单元（地址快照）**：覆盖父段 Geo 标签继承、子段业务覆盖、唯一主前缀、多组命中和无 ASN。
- [x] **本轮证据**：`go test`、`go test -race` 与 `go vet` 已覆盖 flowdimension/flowworker/flowch/collector/worker；集合设计已对照 EdgeManager `geo/setops.rs`、`geo/build.rs`、`dns/engine.rs`。
- [x] **编码（publication 集合编译）**：扩展 members/exclude_members/include_set_ids/exclude_set_ids；完成 v4/v6 canonical CIDR、引用 DAG、排除优先、边界 LPM、不可变 membership 和 endpoint/fact 展开量拒绝。热路径固定两次 LPM，不逐 flow 扫组或追引用。
- [x] **单元（publication 集合编译）**：覆盖同字段 OR/跨字段 AND、include/exclude、排除优先、依赖环/悬空或禁用引用、v4/v6 边界、有限全集 `/0`、无主前缀成员、重叠组及最大展开量。
- [x] **管理面集合运算/预览**：平台共享内核和 API 已实现 v4/v6 并、交、差、显式 universe 的有限补集、严格 normalize/merge、重叠检测、128-bit 计数及显式 cover 扩大量（commit `f44d1978`）；管理页工作台已提交 `9c613c10`。平台 PLAT-04C 已通过 migration 041 + `acbdd4a7` 落地 prefix batch revision 的 preview digest/原子删旧建新/逐操作审计，Batch Apply 管理页已提交 `c241358a`；Flow 数据面不复制 CRUD。
- [x] **编码/单元（查询谓词）**：`include_any/include_all/exclude_any` 编译为有界不可变谓词；已验证 A∪B 中同时属于 A/B 的 fact 只计一次、交集/排除和非法/超量 ID。
- [x] **编码/单元（Geo 包）**：`flow-geo-v2` 每个不重叠地址区间引用一个 `geo_leaf_code`；loader 从 `geo_dict.parent_code` 预编译 continent/region/country/province/city 稳定 ID，并拒绝缺父、环、重复层、非法层序、禁用祖先及 range/path 冲突；v1 继续可读。
- [x] **编码/单元（事实/汇总）**：base fact 一行保存五级 Geo ID、唯一 primary prefix ID 和 address-set ID 数组；顺序 migration 002、native encoder/schema contract 和 rollup 已增加 `geo.continent` 至 `geo.city`，缺失层进入 `_unassigned`，不复制 base flow；ASN 来源保留 v1/v2 provenance。
- [x] **编码/单元（查询原语）**：共享 `GeoIndex` 按单一 bundle 版本提供稳定 code 节点、breadcrumb、启用的直属 children，以及有硬上限的目标层级 descendant code 展开；不让 hub/API 再实现第二套树遍历。
- [ ] **查询 API/显示**：一次只选一个 Geo level，返回 id/name/parent/path/additive/completeness/version；地址组支持 include_any/include_all/exclude_any，组合值从 base 去重计算，单组 rollup 明确 `additive=false`。平台现存重复 v1 loader 的收敛登记在 PLAT-04D，本切片不直接改 hub。
- [x] **集成（进程内）**：真实压缩 v4/v6 bundle、原子热加载、事件时间旧版本选择、Geo v2 → worker → CH native input/rollup 字段契约已覆盖；同一 base flow 只携带一组五级 ID。
- [x] **集成（真实 CH）**：独立数据库顺序 migration、native writer、1m rollup 和五级 query 串联；同一组三条 base fact 在 continent/region/country/province/city 每级均保持 600 raw bytes/3 records，同层稳定 ID 无重复，父级与叶节点求和一致。证据提交 `66453df3`。
- [x] **集成（Kafka corpus 重放）**：真实 sFlow v5、NetFlow v5/v9、IPFIX corpus 已经 production UDP Receiver、source admission、自动 decoder、RawFlow/Inlet、隔离 Kafka topic、partition worker 和 native CH writer；四协议均有 durable fact，template missing/reject/retry 为零；逐 receipt 重算 checksum 并核对 count/raw/estimated bytes/packets，真实 1m rollup 五级均与 base 守恒。测试由显式 `WATCHDOG_FLOW_KAFKA_CLICKHOUSE_INTEGRATION=1` 开启，只创建并清理隔离 topic/database。
- [x] **变更设计/契约测试（数据面）**：v1/v2 bundle 并存读取，worker schema v2，CH 002 只向前增加字段/枚举且 migration contract 以顺序执行后的有效 schema 为准；客户 Geo 修正会清除不兼容的供应商路径 ID；导出版本 hash 覆盖 v4/v6、运营商和字典全部语义输入。
- [ ] **变更设计/测试（查询面）**：旧 worker 对不兼容 publication 的拒绝/滚动升级、双版本查询窗口、回滚只切 publication 不改 base。
- [x] **回归（本地）**：Flow 定向包/命令 race、vet、test，Geo 导出脚本/py_compile 与 CH schema contract 通过。
- [x] **已提交（数据面范围）**：`886ccb2b`；管理面集合预览和查询 API/显示仍未提交，不因此关闭 FLOW-03B 剩余门禁。
- [x] **外部 CH 证据已提交**：五级 Geo 直计数与守恒门禁进入 `66453df3`；测试只创建并清理 `watchdog_flow_it_geo_hierarchy`，不修改已有开发数据。
- [x] **外部 Kafka→CH 证据已提交**：四协议正常链路进入 `66c28373`，真实 UDP collector 与关停 flush 修复进入 `282e2a22`；连续执行及与其余八项真实 CH 数据门禁组合回归通过，测试结束只保留部署固定 topic `watchdog.flow.raw-v1`。

### FLOW-04B Rollup operation job

- [x] **设计**：冻结 v1 payload、逻辑桶与含 generation 的执行幂等键、独立于有限 job retention 的通用持久水位、关闭/迟到窗口、双扫描预算和从 CH marker 前进的 repair generation；明确调度水位不等于查询完整性水位。
- [x] **编码**：v1 encode/decode、稳定 hash/key、关闭 bucket 有界 enqueue、migration 028 通用水位、活跃 Flow tenant 公平分页、repair `generation+1`、CH handler 永久/暂时错误映射、默认关闭的 hub CH/registry/周期扫描生产装配已完成。lease/heartbeat/cancel/retry 继续只由平台 worker 承担，`flow-worker` 未增加状态机。
- [x] **单元**：覆盖 payload/key、UTC/桶对齐、关闭/迟到边界、bootstrap/持久水位、enqueue→watermark crash gap、每 series/全局预算、tenant cursor、repair/CH generation marker、旧 payload、取消传播和永久/暂时错误；takeover/cancel/retry 状态机继续引用平台公共测试，不复制实现。
- [x] **集成**：真实 MySQL lease/handler/repair 已通过；隔离数据库按 001→005 执行真实 CH DDL，经 native writer 写入 facts，完成 1m/1h rebuild、新 runner 模拟确认前重启后同 generation 重放收敛、下一 generation repair 且旧 key 消失、空桶 marker 查询。
- [ ] **变更设计/测试**：先读后写 rolling upgrade、旧 payload 拒绝、repair forward-fix、空库/bootstrap 和 migration 028 存量 job forward-fill 已形成契约；真实 MySQL 已验证 forward-fill 幂等且水位不回退，余项是旧/新进程版本混跑。
- [x] **回归（本地）**：`go test ./...`、`go test -race ./internal/watchdog ./internal/flow... ./cmd/watchdog-flow-collect ./cmd/watchdog-flow-worker`、`go vet ./...`、`git diff --check` 通过。
- [x] **已提交（本地与 MySQL 范围）**：`886ccb2b`；包含曾仅留在工作区的 production handler/store/runtime，`flow_rollup_jobs_test.go` 已使用 enqueue 返回的持久化 job ID，不再依靠 `_test.go` 隐式补契约。
- [x] **外部门禁（单节点）**：真实 MySQL 已完成 001→028、027 删除、028 存量 job forward-fill/单调不回退及 lease/handler/repair；ClickHouse 26.3 native 已验证 rebuild、同 generation 重放及 repair。跨 dedup window、replicated CH 和故障注入继续属于 FLOW-08 生产门禁，不以本项代替。

### FLOW-04C 低基数运行指标（核心已完成，剩余项受门禁）

- [x] **设计**：已冻结 metric 名、类型、单位、固定 label、lag unknown 和 scrape/process 健康语义；IP/ASN/prefix/port/tenant/topic/partition 禁作 label。无实际 pause 状态时禁止输出伪零；receipt mismatch、rollup age/repair 等待 FLOW-04B 的真实执行者。
- [x] **编码（数据面核心）**：collector/worker 分别提供只读 `/metrics`；atomic receiver/producer/consumer/processor/pipeline/writer 统计覆盖 UDP、Kafka buffer/ack latency/poll/assignment/lag/rebalance、decode/template、sampling/snapshot、CH attempt/error/retry/durable row 和 process。Flow facts 不写 VM；高频失败不逐报文刷日志。
- [x] **单元**：固定 exposition contract、并发计数、lag known/assignment/revoke/lost、retry/permanent/durable 计数、GET-only、禁止高基数 label 和 server shutdown 已覆盖。
- [ ] **集成（外部服务）**：启动两个生产命令并由 VM 抓取；注入未知 source、UDP truncate/RXQ overflow、Kafka/CH 失败与恢复、rebalance，核对 broker lag 和 shutdown 末值。production baseline、VM pull、Kafka pause/restart、CH response failure、rebalance 与强杀均已关闭；本项只因 Linux RXQ overflow 和实际 worker/CH restart 组合故障保持未勾选。
- [x] **集成（生产进程基线）**：实际编译并启动两个 production command；真实 UDP 注入 NetFlow v5/v9、IPFIX、sFlow v5，collector Kafka durable ack 6/6，worker commit 6/6，ClickHouse `FINAL` 中 38 条事实覆盖四协议且 receipt 非空；真实进程指标写入现有 VM 后查询一致。使用唯一 topic/database/series，成功和失败路径均清理；证据 `0ce9075d`。这只关闭无故障基线，不代替 promscrape、故障恢复、rebalance、RXQ overflow。
- [x] **集成（VM promscrape）**：独立临时 VM 从 Docker 内主动 pull 两个 production metrics endpoint，两个 `up` 和 Kafka records 精确核对；随机宿主端口、临时存储，cleanup 后零容器/series 残留且不修改现有 VM。证据 `c2845406`。
- [x] **集成（source admission / UDP truncate）**：真实非 loopback source 命中签名 plan 拒绝，超过配置读取边界的 datagram 命中 `MSG_TRUNC`；分别核对 rejected/oversize 且 Kafka 保持 0，再运行正常 corpus 证明监听器没有被异常输入污染。证据 `95bb583f`；RXQ overflow 仍未关闭。
- [x] **集成（ClickHouse 中断恢复）**：透明代理静默丢弃 CH response；Kafka durable record 已存在但 worker offset 不提交，writer retryable 指标增长。代理恢复后同一进程重试并提交，事实/receipt/四协议最终收敛。证据 `1f2d4762`。
- [x] **集成（Kafka 中断恢复）**：隔离 Kafka broker pause 时 collector 只缓冲、不产生 durable ack；unpause 后 producer/consumer 自动恢复，offset 与 broker high watermark 收敛且模板状态保持。证据 `3e5c6abf`；共享开发 broker 未受影响。
- [x] **集成（重叠 worker rebalance）**：同 group 两个 production worker 稳定为 2+2 assignment；追加 flow 后 broker committed=end。优雅退出 A 后 B 接管全部 4 partition，再次追加后 committed=end、lag=0，template state 与 CH 事实收敛。证据 `013f3f9f`。
- [x] **集成（worker 强杀 takeover）**：A 被 SIGKILL 后无 revoke/commit，B 等 broker session 失效并接管 4 partition；新 flow 后 committed=end、lag=0，模板与事实收敛。证据 `7cb8ef41`；死亡进程内存指标不作为持久证据。
- [x] **集成（broker 保留数据重启）**：committed-next-offset=1 后实际 restart 隔离 Kafka；同一 collector/worker 自动重连，topic/group offset 不丢失也不回退，后续 CH recovery、完整 corpus、rebalance 与强杀链路继续通过。公共 Kafka 配置启用 franz-go `AlwaysRetryEOF`，不自造 retry 状态机；证据 `0b141b3b`。
- [ ] **集成（Linux RXQ overflow）**：在提供 `SO_RXQ_OVFL` 的 Linux 固定压力环境制造 socket receive queue drop，核对 `kernel_drops_total` 与 exporter sequence gap；Darwin 不提供该 socket control message，不能用 oversize/truncate 冒充。
- [x] **集成（真实 VM 存储/查询子门禁）**：production collector/worker handler 通过真实 HTTP server 输出，VictoriaMetrics Prometheus import 后 instant query 精确得到 collector Kafka records 和 worker lag；落库 label 复核无高基数维度，唯一 `integration_run` series 在成功/失败 cleanup 中删除。显式环境变量开启，证据提交 `c61bb658`。该测试只证明 wire/storage/query 兼容，不替代上项的 promscrape/生产进程/故障恢复。
- [x] **变更设计**：新增独立 `-metrics-listen`，默认 loopback `9090/9091`、空值禁用；远程 TLS/mTLS 归部署层反代/sidecar，不复制证书生命周期。
- [x] **变更测试**：裸 host:port/IPv6/端口范围校验、URL 拒绝、禁用不 bind、metrics server shutdown 和异常退出联动由配置/生命周期单元覆盖。
- [x] **FLOW-04C2 rollup 指标**：runner 在真实 CH 调用点原子记录 1m/1h attempt/success、retryable/permanent、initial/repair、last success/最大完成 bucket；provider 只使用固定 resolution/class/kind 标签，unknown age 有独立 known gauge，旧桶 repair 不倒退。
- [x] **FLOW-04C2 单元/变更/回归/已提交**：覆盖 resolution、失败分类、repair、未来 age clamp、从未成功 unknown、高基数标签禁止和 hub provider 组合；定向 race/vet 与全库 test 通过，提交 `041eebf4`。
- [x] **FLOW-04C3 设计/边界审计**：冻结可跨 tenant 的 batch 权威键、Kafka committed-next-offset 闭合规则、`FINAL` 去重后 count/counter/checksum 比较、固定 mismatch reason、有界 keyset 扫描和不完整 gauge 快照语义；禁止在 metrics renderer 猜值或为指标另建状态机。
- [x] **FLOW-04C3A 编码/单元**：migration 004 前向增加 receipt schema v2、排序去重 tenant IDs、min/max event time、raw/estimated packets 和 valid-estimate record count；native encoder 从同一 PreparedBlock 确定性产生，覆盖跨 tenant、时间边界、无效 estimated 排除、byte/packet 溢出与 DDL 列契约。
- [x] **FLOW-04C3A 变更设计/测试**：001 不改，旧行 `receipt_schema=1`，新行显式为 2；发布顺序为 004 → 全 worker v2 → 记录 per-partition cutover offset → 启用对账，不对旧 receipt 猜缺失字段。schema contract 已锁定 migration 顺序和 v2 native input。
- [x] **FLOW-04C3B1 访问路径设计/编码**：migration 006 增加唯一 `flow_ingest_audit_v1` narrow projection；显式 `deduplicate_merge_projection_mode='rebuild'`，存量 `MATERIALIZE ... mutations_sync=2`，审计用 offset window + `argMax(..., ingest_generation)` 去重，禁止会退回 base scan 的 `FINAL`。证据 `3f3501d8`。
- [x] **FLOW-04C3B1 单元/集成/变更/回归**：schema/embedded migration 锁定 001..006；真实 200 万存量事实先基线再应用 006，验证结果一致、EXPLAIN 选中 projection、rows 至少 100×/bytes 至少 50× 裁剪并记录磁盘增量；全部 CH data integration、Flow race/vet、全库 test/vet 通过。上线需预留 projection+merge 空间并调 migration timeout；失败 inspect/resume，撤销只允许新 forward migration。
- [x] **FLOW-04C3B2A comparator 编码/单元/已提交**：无状态 typed comparator 拒绝超限、重复/空 identity 和计数溢出；固定 `missing_receipt→missing_records→identity→count→counter→checksum` 单一主因，并以 production block 验证 checksum/estimated 语义。证据 `1f00d551`。
- [~] **FLOW-04C3B2B CH scanner 编码/单元/集成**（核心已提交 commit 0948b45a；两处预算待补）：固定 receipt cursor、Kafka close watermark、TTL eligibility、partitions/offset span/batch/fact rows/read bytes/wall-time 预算，先 counters 后 checksum；只向 comparator 提供 generation-deduplicated facts，不完整扫描不发布伪零。
  - ✅ 已做：`ReconciliationScanner`（flowch）——per (topic,partition) receipt cursor；close watermark（`last_offset < CloseOffset`，不对账在途 offset）；batch/fact-rows/read-bytes 预算；phase1 服务端聚合 counters 分类 missing/identity/count/counter（不取行、镜像 comparator 优先级），phase2 只对 counter-clean 候选拉 record-level facts 交 comparator 定 checksum；`argMax(ingest_generation)` 去重；batch 预算截断进 cursor 一步、fact 预算截断不进 cursor，`Complete=false` 不发伪零。unit 验分类优先级/校验；gated CH 验 clean 完成、counter 不取行即报、missing_records、budget 截断逐 batch 前进。
  - ⏳ 待补：**TTL eligibility**（只对账 base TTL 窗口内的 batch，避免把已过期数据当缺失）；显式 **offset-span / wall-time 预算**（当前由 MaxBatches + ctx/OperationTimeout 间接约束，partitions 由调用方逐 partition 迭代）。这些补齐后再连 FLOW-04C3B3 job 接线。
- [ ] **FLOW-04C3B3 job/指标接线**：复用平台 global/system-scope `operation_jobs` 运行 scanner、持久 checkpoint 与完整快照，受 PLAT-04F 阻塞；Flow 不伪造 tenant、不另建状态机。
- [ ] **FLOW-04C3 已提交**：只有审计元数据、runner、指标、job 接线和对应测试都进入可复现提交后才可勾选；仅文档审计不冒充功能完成。
- [x] **回归**：`go test -race ./internal/flow... ./cmd/watchdog-flow-collect ./cmd/watchdog-flow-worker` 与同范围 `go vet` 通过。

### FLOW-05 Query/API/UI

- [x] **FLOW-05A 设计**：冻结 authenticated tenant scope 与客户端请求分离、1m/1h 闭桶、metric/dimension/filter registry、TopN/other、版本拆分、completeness 字段、稳定错误码和查询预算。aggregate v1 只支持 customer view；禁止伪装 raw/supplier。
- [x] **FLOW-05A 编码**：新增独立 `internal/flowquery` compiler；SQL 结构只来自固定 registry，全部请求值 typed parameter；按 `_generation` marker 选每桶最新 generation，TopN key 包含三个版本字段，固定超时/结果/扫描硬限；内部 metadata sentinel 即使空结果也返回 rollup 覆盖桶数。
- [x] **FLOW-05A 单元**：覆盖 registry/additive、确定性、参数转义且不插入 SQL、filter 排序去重、latest generation/coverage sentinel、TopN tie-break、other、UTC/闭桶、结果预算、非法 enum/view/timezone/scope。
- [x] **FLOW-05A 集成（单节点核心）**：在真实 CH 顺序 migration 上执行 1m/1h 查询，覆盖空 generation、repair 后旧 key 消失、TopN/other 守恒；同时锁定 native 数字 parameter Field dump、`AS source FINAL` 和聚合别名规则。
- [x] **FLOW-05A 集成（版本/预算）**：真实 CH 已验证两个版本的同名维度不合并、等值 TopN 按 `dimension + snapshot + geo + classification` 稳定排序、版本化 `_other` 与总量守恒；`max_rows_to_read/max_result_rows` 压限均抛错且零部分结果。证据提交 `c876e974`。
- [ ] **FLOW-05A 集成（超时/集群容量）**：active query deadline、静默传输 operation timeout、partial packet 和单节点 4-connection/64-request 并发隔离已由真实 CH 门禁证明；集群与固定硬件吞吐/容量仍未完成，不得以单节点正确性替代。
- [x] **FLOW-05A 变更设计**：aggregate v1 只开放 customer view；raw/supplier 保持稳定 unsupported，等待 FLOW-06 的事实 provenance/reclass schema。修正未知单值维度统一 `_unassigned` 的前向语义，不改已发布 CH DDL。
- [x] **FLOW-05A 变更测试**：rollup contract test 覆盖 ASN/ISP/business/local/remote prefix/remote port/observation interface 的 `_unassigned`，防止再次破坏单值维度可加性；旧请求缺 view 明确拒绝而非猜默认。
- [x] **FLOW-05A 回归**：`go test -race ./internal/flow... ./cmd/watchdog-flow-collect ./cmd/watchdog-flow-worker` 与同范围 `go vet` 通过。
- [x] **FLOW-05B 设计**：冻结 Point/Result/RollupCompleteness、单 sentinel、多 block、点唯一键、版本混合和全有或全无结果语义。
- [x] **FLOW-05B 编码**：typed ch-go result columns、有界累积、sentinel 剥离、bucket/sampling/quality completeness 与 mixed-version 已实现。
- [x] **FLOW-05B 单元**：fake executor 覆盖跨 block、空结果但 rollup incomplete、采样/质量比、版本混合、缺失/重复/畸形 sentinel、覆盖溢出、NaN/Inf、越界 bucket、重复点和硬行限。
- [x] **FLOW-05B 集成**：真实 CH 执行并核对 native result 类型、强制 `max_block_size=1` 的多 block、首个数据 block 后 context 取消时零部分结果，以及有 marker 无公开行的完整空结果。服务端长查询/静默传输超时仍由 FLOW-08 覆盖。
- [x] **FLOW-05B 变更设计**：provider 覆盖传入 query 的 result callback，但保留 compiler body/parameter/setting；CH 任一 block 后失败时丢弃已累积点，不允许部分成功响应。
- [x] **FLOW-05B 变更测试**：执行中断、未初始化 runner、malformed/重复 sentinel 和超限结果均返回错误且零结果。
- [x] **FLOW-05B 回归**：Flow 全范围 race/vet/test 通过。
- [x] **FLOW-04B/05A/05B 外部证据已提交**：真实 CH 数据链路、查询修复与隔离集成测试进入提交 `27278350`；测试只创建并清理 `watchdog_flow_it_rollup_query`，不修改现有开发库。
- [x] **FLOW-05A 版本/预算证据已提交**：跨版本、tie-break、服务端扫描/结果预算门禁进入 `c876e974`；测试只创建并清理 `watchdog_flow_it_query_versions`。
- [x] **FLOW-05C 前置审计**：确认 compiler/runner 已隔离 tenant scope 和 typed 参数，但宿主尚无可执行 QueryGateway；Flow module/dataset 未注册，value-layer action 缺失，CH pool 私绑 rollup enablement，Flow readiness/限流/错误映射未装配。平台缺口已登记 PLAT-04H。
- [ ] **FLOW-05C（平台依赖解除后）**：接入 hub tenant/RBAC、共享 Geo catalog、API envelope、限流/超时/审计；不得复制 Geo loader、身份逻辑、CH pool 或 rate limiter。
- [x] **FLOW-05C1 设计/编码**：从 Flow 固定 registry 导出 aggregate customer view 与 detail raw/supplier/customer 的允许字段、默认字段和过滤器；compiler/runner 验证改为消费同一 view registry，hub/UI 不再需要复制白名单。
- [x] **FLOW-05C1 单元/变更测试**：逐 view 验证全部已声明字段/filter 可编译、全部未声明项被拒绝、顺序稳定、默认字段属于允许集合、返回切片修改不污染 registry、未知 view fail-closed；aggregate 仍只声明 customer。
- [x] **FLOW-05C1 回归**：`internal/flowquery` race/vet、全库 test/vet 与 diff check 全过。
- [x] **FLOW-05C1 已提交**：能力契约、共享 registry 改造、测试与设计已进入独立提交 `8a4a9b24`；工作区不再残留该切片生产文件。
- [x] **FLOW-05D 设计**：冻结 source/destination/either IP、customer/count view、24h 毫秒时间范围、稳定 `(event_time,record_id)` cursor、固定 field mask registry、limit+1、base `FINAL` 去重、500 万行/1 GiB/10s 扫描预算和全有或全无结果。
- [x] **FLOW-05D 编码**：实现参数化 `flow_records FINAL` compiler、有界 multi-block typed result runner、结果一致性复核和 next cursor；不接线 hub。
- [x] **FLOW-05D 单元**：覆盖 IPv4/IPv6、三种 endpoint side、同毫秒 cursor 边界/全局顺序、field mask、参数注入、重复/非法/列错位/越界结果、执行中断和 limit+1。
- [x] **FLOW-05D 集成**：真实 CH 顺序迁移/native 写入已覆盖较新 generation 的 `FINAL` 重放去重、同毫秒 record-ID 两页不重不漏、IPv4-mapped source/destination/either、首 block 取消、过期 deadline 和 `max_rows_to_read` 拒绝；所有失败均返回零部分结果。
- [x] **FLOW-05D 变更设计**：字段增加只通过 registry；cursor 与 field mask 解耦；不兼容 cursor 使用新前缀，未知版本明确拒绝，旧 decoder 保留一个发布窗口。
- [x] **FLOW-05D 变更测试**：固定 v1 golden cursor、未知字段/版本/畸形 payload、field mask 改变后旧 cursor 语义不变均已覆盖。
- [x] **FLOW-05D 回归**：`go test -race ./internal/flow... ./cmd/watchdog-flow-collect ./cmd/watchdog-flow-worker` 与同范围 `go vet` 通过。
- [x] **FLOW-05D 已提交**：字段 registry 收敛为物理列+结果类型、`source.*` alias 隔离、编译器契约测试与真实 CH 门禁进入提交 `eacc1ec5`；测试库退出后清理，不修改现有开发数据。
- [x] **FLOW-05E 设计**：冻结 local/remote/either base membership、`include_any/include_all/exclude_any`、事件时间 snapshot、无 `ARRAY JOIN` 的 fact 去重、customer/count、1h 同步预算与异步升级边界。
- [x] **FLOW-05E 编码**：复用唯一 `flowdimension.CompiledAddressSetFilter` canonical 语义实现参数化 `flow_records FINAL` 统计 compiler 和 multi-block typed runner；未新增集合数学或任务引擎。
- [x] **FLOW-05E 单元**：覆盖 A∪B、A∩B、A−B SQL，重叠 fact 无展开、三种 endpoint、metric/bucket、空/排除-only、非法/总量超限 ID、跨版本、采样未知、顺序/重复/列错位/执行失败。
- [x] **FLOW-05E 集成**：真实 CH 顺序迁移/native 写入已覆盖 A∪B、A∩B、A−B，重叠 membership 单 fact 只计一次、较新 generation 经 `FINAL` 替换旧行、raw/estimated/received/unknown-sampling/quality 守恒，以及过期 deadline、`max_rows_to_read` 拒绝时零部分结果。
- [x] **FLOW-05E 变更设计**：集合规则只影响新 snapshot；历史事实按事件时间 snapshot 分组；超预算返回稳定错误并由平台异步 job 承接，不在 provider 内复制状态机。
- [ ] **FLOW-05E 变更测试**：本地已覆盖 snapshot 跨版本不合并与 immutable canonical filter；真实规则增删/回滚、旧事实可解释、同步→异步协议兼容等待 publication/job 平台门禁。
- [x] **FLOW-05E 回归**：`go test -race ./internal/flow... ./cmd/watchdog-flow-collect ./cmd/watchdog-flow-worker` 与同范围 `go vet` 通过。
- [x] **FLOW-05E 已提交**：真实 CH 集成门禁及 LowCardinality wire type 显式 String cast 修复进入提交 `c89e8f33`；测试使用独立数据库并在退出时清理。
- [ ] 实现总览、多维、源 IP、目的 IP、境外、VPN 六页和 query/search/export API。
- [ ] 所有 VTable 统一服务端分页/搜索/排序/column filter；popover portal + collision，禁止溢出错位。
- [ ] 完成参数/RBAC/统计精度单元，API→CH/页面/导出集成，API 版本/灰度/回退变更测试和前后端回归。

#### FLOW-05F Flow Explorer（Akvorado 查询模型对齐）

- [x] **设计**：冻结 `range ≠ display step ≠ source resolution`；`target_points` 驱动自动 planner，显式 step 也只控制展示；响应回传 requested/effective range、source、source/display seconds。冻结折线/堆叠/热力/表格先消费单维序列，桑基只能消费真实联合维度 tuple，禁止多次单维查询拼接。
- [x] **编码（自动 planner/单维查询）**：`PlanAggregate` 支持 5..2000 目标点、1m..30d 显式展示步长、1m/1h 自动选源和各自扫描预算；aggregate compiler 以 effective-from 为锚点二次汇聚 source bucket，provider 回传实际 step/source；QueryGateway metadata 使用 provider 实际 step。
- [x] **编码（首个 Explorer UI）**：时间预设扩展至 5m..1y + 自定义；增加 metric、单维 registry、TopN/Other、目标点数、device/address-set、typed filter、折线/堆叠/热力/表格、last/avg/95th/min/max/total/质量表和 URL 状态；所有结果表复用带搜索/列 filter/分页的 PagedVTable。
- [x] **编码（便捷分析层）**：同页默认提供国家→省→市、运营商、时间、metric 和分析类型选择；覆盖真实入/出方向、协议、TOP 源/目的 IP、TOP 本地/远端网段，运营商严格按已配置 ASN 集合过滤；高级 Explorer 折叠保留且不复制查询后端。
- [x] **当前结果导出**：CSV 导出当前查询返回的 bucket、序列、原值、单位、min/max/last/avg/p95/total 和 sampling/quality 计数，包含 UTF-8 BOM、标准引号转义与公式注入防护；明确不冒充 FLOW-06C 全量异步导出。
- [x] **单元**：planner 覆盖 5m/1h/6h/24h/7d/30d/1y、显式 15m、未来/非法密度/两类源扫描超限；compiler/runner 覆盖 source completeness 与展示桶对齐；前端覆盖预设/自定义、typed filter、末桶 total 和三种 chart spec。
- [x] **集成（真实 CH）**：独立库在两个完整 1m source bucket 上以 15m 展示步长查询，末桶仅覆盖 120 秒；bps 使用实际 120 秒且 completeness 仍为 2/2 source marker。既有 repair/多 block/cancel 门禁同测通过。
- [x] **联合维度/桑基（有界 base）**：独立 joint compiler/runner 从同一 `flow_records FINAL` 事实生成 2–4 维有序 tuple、稳定 tuple TopN/Other、折线/堆叠/热力/表格和桑基；同步范围限 24h，固定 typed expression registry 和 CH 扫描/时间/内存硬限，拒绝歧义 `dimension_values`、重复维度及重叠 address-set；runner 多 block 全有或全无。没有新增表，因此本项不伪造空 migration。
- [x] **联合维度集成（真实 CH）**：隔离库写入两组 `geo.city × ASN` 事实，以 `TopN=1 + Other` 验证 `geo-city-b/4837=550`、other=300；真实 HTTP gateway 同时覆盖空结果 array wire type、自动步长、`source=flow_records` 和 coverage warning。
- [ ] **异步联合索引**：冻结 publication/config/index generation 和 operation-job payload；常用组合及 address-set path 走异步索引，使 >24h 查询可用。索引缺失/过期只能显式拒绝或标 degraded，不能回退成伪联合单维结果。
- [ ] **地址字典生产 index-builder**：消费平台同一已签名 address snapshot 的 source manifest 与 schema v2 operator definition object，校验 source checksum/row count 和 tenant `flow_isp_id`；追加 CH forward migration（不得修改 009）将过渡的单一 `isp_id UInt32` 拆为 supplier ISP 与 `customer_isp_id UInt16`。人工 prefix 按 operator ID 精确绑定，base range 只按非零 ASN 命中唯一 enabled tenant operator；缺失/未配置为 0，不按 name/code 猜测，交叠组合走 address set。生成同一 `dict_version` 的运营商表与 IPv4/IPv6 IP_TRIE source，复用 `operation_jobs` 的 lease/checkpoint/retry/cancel；构建失败保持当前 generation，ACK 固定 snapshot/checksum/dict generation，旧 generation 保留到事实/账单解析窗口结束。平台 C4b1 只提供确定输入，不替代本项。
- [x] **过滤生命周期（无状态查询）**：服务端 catalog/validate/complete/canonical AST 与前端 AND/OR/NOT/括号 parser 已覆盖 IP/CIDR/ASN/Geo/ISP/prefix/端口/协议/interface 和 typed 操作符；只含 rollup 字段时保持 1m/1h，跨维字段强制最长 24h base-fact path。IPv4-mapped CIDR 已由真实 CH 门禁验证；字段/操作符只读 registry、值只走 typed parameter。
- [x] **过滤权限/一致性**：查询只接受 validate 返回的 canonical AST，保障 hash/cache/audit/URL 重放稳定；非管理员不得用复杂 AST 绕过 target/device/exporter resource selector，未知字段/JSON、非规范 AST、预算超限均 fail closed。
- [ ] **保存/共享过滤器**：新增唯一管理库 migration，冻结 owner/share scope/If-Match/软删除/audit/RBAC/引用保护和 CRUD/list/filter；不得把保存状态放入 Flow worker/CH 或再造管理库。
- [ ] **集成（生产 HTTP/UI）**：登录 tenant/RBAC → `/api/v1/query` → shared CH pool → Explorer 四视图；覆盖自动 step metadata、取消/超时/partial/空结果/版本混合、URL 重放和 filter 错误。
- [x] **集成（typed filter 增量）**：真实 HTTP 覆盖 validate/规范 AST/base-fact 空结果及 source/step/partial metadata；生产 8090 浏览器覆盖完整时间预设、CIDR+ASN 表达式、24h 提示、`flow_records source` 和空结果，无 `Failed to fetch`。其余四视图/RBAC/故障组合仍由上一项承载。
- [ ] **集成（便捷分析增量）**：生产 8090 登录态验证默认便捷筛选、高级区折叠、方向双查询、协议/TOP 切换、URL 重放、级联地域/运营商映射、空结果/错误和 CSV 下载；完成后记录制品与提交证据。
- [x] **回归（便捷分析增量）**：前端 28 项 model/chart test、定向 Biome、Vite production build 和全库 `go test ./...` 通过；8090 最终制品验证默认便捷层、方向空结果、协议/TOP 选择和高级区展开，无前端异常。本地库无 Flow 点且 Geo/operator 字典为空，因此真实非空 CSV 下载、地域级联和运营商 ASN 选择仍留在上一集成门禁，未冒充完成。
- [ ] **变更设计/测试**：旧显式 `60/3600` 请求保持兼容；新客户端默认 0/auto；滚动升级时旧 hub 对 auto 请求明确拒绝而非误查。联合索引缺失/过期回落必须显示 source/degraded，不静默换口径。
- [x] **回归（联合维度增量）**：Flow/Watchdog 定向 race、全库 test/vet、前端 25 项 model/chart test + production build、真实 CH aggregate/joint data integration 和 gateway integration 均通过；登录 tenant/RBAC 浏览器验收仍由上一项单独保留，未冒充完成。
- [x] **已提交（本切片范围）**：自动 planner、单维 provider/UI 进入 `157b070d`；真实联合维度、桑基和真实 CH 集成进入 `0751551a`；无状态 typed filter 生命周期、跨维 base 路由及生产页面增量验收进入 `1ffb3939`；默认便捷分析、方向/协议/TOP 查询和当前结果安全 CSV 进入 `33687c3a`。异步联合索引、保存/共享 filter、完整生产 HTTP/RBAC、带数据便捷分析集成和滚动升级门禁仍保持未完成。

### FLOW-06 Correction/Reclass/Export

- [x] **FLOW-06A 设计**：冻结 raw 协议 tuple、supplier bundle 基线、customer override 最终值三层边界；supplier ASN 明确保留 Geo 优先/exporter fallback 来源，customer override 用稳定 bitset 解释；旧 `fact_schema=1` 不冒充 supplier。
- [x] **FLOW-06A 编码/单元**：migration 005 前向增加 `fact_schema=2`、supplier Geo/ASN/category/version 和 customer override fields；worker 在覆盖前复制 supplier 基线，native writer 明确写出；覆盖无覆盖等值、覆盖分歧、ASN fallback、Geo v2 hierarchy、稳定 bit 和非法 provenance。
- [x] **FLOW-06A 变更设计/契约测试**：001..004 不修改；顺序固定为 005 → 全 worker schema 3 → 记录时间/partition cutover → 才开放 supplier。migration 顺序/native exact columns 已锁定，混跑/旧事实禁止回填猜测值，customer rollup/query SQL 未改变。
- [x] **FLOW-06A 回归/已提交**：提交 `3d63a5a7`；Flow race/vet、全库 test/vet、migration/native contract 与 diff check 全过，工作区不再残留该切片生产文件。
- [ ] **FLOW-06A 外部门禁**：真实 CH 已完成隔离库 001→005 顺序执行和当前 schema 3 writer/supplier completeness；剩余 v2/v3 worker 实际制品混跑、partition cutover 记录与切换后全窗口 completeness，不能由手工 schema 1 fixture 代替。
- [x] **FLOW-06A2 raw detail 设计**：raw 对全部 fact schema 可用，只包含协议 tuple/采样/质量/资源/observation 字段，不应用 customer disposition；customer-derived 字段及 direction/category/business filters 在 raw view 稳定拒绝，supplier 继续关闭。
- [x] **FLOW-06A2 编码/单元/变更测试**：detail schema v2 增加显式 raw/customer view；view 进入 compiled/result contract，raw 使用独立默认/允许字段集和资源过滤器；覆盖参数化 SQL、无 customer/supplier 泄漏、drop fact 可审计、非法字段/过滤器/view 及 runner fail-closed。
- [x] **FLOW-06A2 集成（数据语义）**：真实 CH 用含 count/drop 和 fact schema 1/2 的数据核对 raw/customer 行集及同毫秒翻页；raw 保留 drop/旧事实，customer 只返回 count，均不重不漏。
- [ ] **FLOW-06A2 集成（平台接线）**：raw/customer 权限、审计和 HTTP envelope 仍随 FLOW-05C 宿主 QueryGateway 接入完成；不在 Flow provider 内复制身份/RBAC。
- [x] **FLOW-06A2 回归/已提交**：提交 `d00b620a`；Flow race/vet、全库 test/vet 与 diff check 全过，工作区不再残留该切片生产文件。
- [x] **FLOW-06A3 设计/编码**：固定 supplier 字段映射、direction/supplier-category/resource filters，并用 cursor 前的 full-scope window evidence 证明 `min(fact_schema)>=2`；metadata-only 行覆盖 cursor 排除全部数据的情况，不双扫 base。
- [x] **FLOW-06A3 单元/变更测试**：覆盖 supplier Geo/ASN/ISP/category/version 映射、customer business/prefix 拒绝、typed filters/default fields、schema 1 unavailable、metadata-only、缺失/跨 block 矛盾 evidence 和 all-or-nothing runner。
- [x] **FLOW-06A3 集成（数据语义）**：真实 CH 已验证 window/FINAL/IPv4-mapped、schema 1 全窗口 fail-closed、schema 2 cursor 前后页、空范围 complete/min=0，以及 `max_rows_to_read/max_bytes_to_read` 拒绝且零部分结果。
- [x] **FLOW-06A3 回归**：Flow race/vet、全库 test/vet 与 diff check 全过；真实 CH 集成仍按上一项保留外部门禁。
- [x] **FLOW-06A3 已提交**：生产代码、测试与契约文档已进入独立提交 `d56bc3f6`；工作区不再残留该切片生产文件。
- [x] **FLOW-06A2/06A3 外部证据已提交**：真实 provenance 数据门禁、String/Bool wire type 修复和对应单元契约进入提交 `b2c07af9`；平台权限项未被错误勾选。
- [ ] **FLOW-06B 历史重分类**：冻结 tenant/window/source+target publication/view/generation payload；真实 CH 容量测试后选择唯一派生投影路径，复用 operation_jobs 扫描/lease/retry/cancel，不修改 base、不复用 ingest generation、不新增 Flow 状态机。
- [ ] **FLOW-06B1 平台前置**：审计确认现有 handler 运行期间不能受租约保护地更新 progress/checkpoint，worker heartbeat 会写回静态旧进度；已登记 PLAT-04G。解除前不实现整窗 scanner/runner，避免崩溃后整窗重跑或 Flow 自建状态机。
- [ ] **FLOW-06B 守恒/切换**：新 generation 隔离写入，record count、raw/estimated counters、record-ID checksum 全通过后原子可见；失败/取消保留旧 generation。覆盖重叠规则、事件时间、幂等、base TTL/archive 边界、失败续跑和回退。
- [ ] **FLOW-06C 管理/导出**：复用平台 immutable publication、typed CRUD/审批/If-Match/audit 和 operation_jobs；异步导出执行权限/脱敏/配额/过期销毁，raw/supplier/customer 分权，不复制地址库 CRUD。

### FLOW-07 Overseas/VPN

- [x] **FLOW-07A 设计**：冻结 conversation/window 身份、双向计数、完整度、端口/协议/ASN/prefix/country/行为 evidence、versioned rule、score/level/verdict、terminal tie-break；被动建议不等于探测授权。
- [x] **FLOW-07A 编码**：新增纯函数 candidate normalizer/scorer、schema v1 固定 match/effect registry、immutable canonical rule set 和 explainable evidence；增加关闭窗口 CH materializer，但未接 MySQL、hub、Kafka 或 probe job。
- [x] **FLOW-07A 单元**：覆盖对称/单向、长连接/流量/复发、显式 TLS hint 且不由 443 猜测、ASN/prefix、allow/suppress 优先级、完整度 gate、阈值/封顶、输入变异隔离和非法 schema/candidate。
- [x] **FLOW-07A 集成（数据面）**：真实 CH 顺序 migration、base writer、两个 1m rollup、candidate materializer 和 scorer 串联；验证反向原始流归一为同一 local/remote 会话、稳定 SHA-256 key、count/quality/coverage 完整度、迟到 generation 2 和 generation 3 权威空修复。finding/MySQL/probe 编排仍属平台侧。
- [x] **FLOW-07A 变更设计**：candidate/评分结果携带 dimension snapshot、Geo、classification 和 rule-set 四类版本；规则升级生成新 generation/result，不改历史机器 verdict；terminal 决策规则单列，人工 disposition 仍由管理面独立维护。
- [x] **FLOW-07A 数据面变更测试**：确定性 evidence/score、未知 schema、规则顺序/输入修改、dimension/Geo/classification 跨版本 CH 物化，以及 immutable rule-set v1→v2→回切 v1 的 generation/result 隔离均已覆盖；证据提交 `a7ea3f2d`。
- [ ] **FLOW-07A 管理面 publication 门禁**：平台 migrations 042/043 与 `8b86d829` 已提供签名审批 proof、If-Match、event-time activate/rollback、retire、ACK/reference 和审计仓储；仍缺 trusted-key/RBAC API、真实 worker 下载/安装 ACK 与安全 object GC。上述能力继续由 PLAT-04A/04C 承载；数据面直接选取已编译 immutable rule set 的测试不能替代。
- [x] **FLOW-07A2 schema 门禁**：003 前向增加 `remote_prefix_id/geo_version/classification_version/row_kind` 并扩展 replacement key；001/002 未回改，旧行默认 candidate，新读取契约只接受有 marker 的 generation。
- [x] **FLOW-07A2 materializer**：单条同步 `INSERT SELECT ... UNION ALL` 写候选和 `_generation`；空修复可推进 generation，版本不互相覆盖，同请求 dedup token 稳定；未知采样不混 raw/estimated，443 不推断 TLS/QUIC。
- [x] **FLOW-07A2 单元/变更测试**：覆盖 UTC/闭窗/1m..24h、安全标识符、原子 marker、稳定主 tuple、双向计数、rollup/sampling 完整度、四类版本、永久/暂时 CH 错误和 authoritative generation read。
- [x] **FLOW-07A2 已提交**：`2e9d9474`；代码和 migration 可由提交复现，本清单更新不把 fake executor 冒充真实 CH 集成。
- [x] **FLOW-07A3 设计/编码**：同一 CH 查询选 marker 与 latest-generation candidate；metadata block 顺序无关，typed 参数/列、`limit+1` 与 50,000 硬上限；规范化后调用 immutable scorer，无 finding 写入或 probe/job 副作用。
- [x] **FLOW-07A3 单元/变更测试**：覆盖 marker 在前、跨 block、合法空 generation、缺/重复 marker、混 generation、重复 key、超限、列错位、未知 evidence 字段、ratio 不一致、非法 hint、执行失败、非法 scope/window/rules/limit；任一失败均无部分结果。
- [x] **FLOW-07A3 回归/已提交**：`ad4c2c6e`；Flow 全范围 race、全库 test/vet 和 diff check 通过；真实 CH query/repair 数据门禁已由 `55a3b166` 补齐。
- [x] **FLOW-07A 回归**：`go test -race ./internal/flow... ./cmd/watchdog-flow-collect ./cmd/watchdog-flow-worker`、同范围 `go vet` 与 `git diff --check` 通过。
- [x] **FLOW-07A 外部证据已提交**：真实 CH candidate/materializer/reader/scorer 门禁进入 `55a3b166`；测试只创建并清理 `watchdog_flow_it_vpn_candidate`。跨 rule-set publication 增删/回滚仍保留为变更测试，未因数据面通过而错误勾选。
- [x] **FLOW-07B 设计**：冻结 classification `category=overseas` 权威、country/region 两级、所选层 `_unassigned` 独立 unknown Geo、方向确定 local/remote endpoint、IPv4-mapped family、TopN/other、版本和完整性口径；不由查询层按当前国家/HMT 设置重判历史。
- [x] **FLOW-07B 编码**：新增独立 `CompileOverseas`/`OverseasRunner`；只读 latest-generation 1m/1h aggregate，复用 src/dst endpoint rollup生成 in/out/combined 与 ipv4/ipv6/unknown/all KPI，返回 country/region TopN 和 unknown Geo；不接 UI/hub、不扫 base、不新增表或 job。
- [x] **FLOW-07B 单元**：覆盖 typed tenant/filter、闭桶/范围/结果预算、country/region、rate/count、境外/unknown 分离、方向端点映射、IPv4-mapped family、TopN/other、镜像一致性、metadata coverage、mixed versions、多 block、畸形/重复/越界/取消全失败。
- [x] **FLOW-07B 集成**：独立真实 CH 数据库顺序执行 migration + rollup；验证 IPv4/IPv6/双端 unknown、in/out local/remote 镜像、country/region、TopN+other+unknown 和流量守恒，并在同桶写入迟到事实后以 generation 2 重建，旧 generation 不再被查询读取。
- [x] **FLOW-07B 变更设计**：唯一 IP 明确为 `observed_remote_ips/observed_local_hosts`，即收到的 Flow 事实精确去重，不按 sampling rate 放大；历史 classification/Geo/version 不重判，结果跨版本拆分或告警。
- [x] **FLOW-07B 变更测试**：customer-only、未知 level/direction/metric、请求值不插 SQL、IP/Geo unknown 不冒充、endpoint 汇总不一致 fail-closed、结果硬限与 sentinel 错误均已固定为契约测试。
- [x] **FLOW-07B 回归**：境外单项及 rollup/detail/address-set/provenance/overseas 五组真实 CH 数据测试通过；Flow 全范围 race、`go test ./...`、`go vet ./...` 与 `git diff --check` 通过。
- [x] **FLOW-07B 已提交**：`f8beffaf feat(flow): add overseas KPI query core`；代码、测试和设计可由提交复现，工作区不再残留该切片生产文件。
- [x] **FLOW-07B 外部证据已提交**：真实 CH 境外数据门禁进入 `54ac6173`；测试仅创建并清理 `watchdog_flow_it_overseas`，不修改已有开发数据；hub API/RBAC/UI 仍归平台侧，未在此错误关闭。
- [ ] **FLOW-07C（平台依赖解除后）**：复用 operation_jobs 实现授权/配额/cooldown/kill-switch probe 编排与可插拔 agent；不得把主动握手放进 Flow 热路径。

### FLOW-08 HA/Lifecycle/Release

- [x] **FLOW-08A1 设计/编码**：loader 固定连续 `NNN_lower_snake.sql`、4 MiB/UTF-8/no-BOM/no-NUL、精确字节 SHA-256 和 quote/comment-aware statement 拆分；planner 对 version/name/checksum/state 做 fail-closed 对齐，dirty 只允许显式 resume。
- [x] **FLOW-08A1 单元/变更测试**：读取真实 001..005；覆盖内容微改 checksum、空/非法名/缺号/超大/坏 UTF-8/NUL/BOM/未闭合 SQL、数据库 gap/duplicate/ahead/name/checksum/unknown-state/dirty 及从 dirty 精确续跑。
- [x] **FLOW-08A1 回归**：Flow race/vet、全库 test/vet 与 diff check 全过；并行工作区的告警域变更也未破坏本轮全库验证。
- [x] **FLOW-08A1 已提交**：loader/planner、单元测试与设计已进入独立提交 `24bd111b`；工作区不再残留该切片生产文件。
- [x] **FLOW-08A2 设计/变更设计**：冻结 embedded migration binary、statement checkpoint、dirty/resume、单调 generation、同步状态写、持久 metadata owner 锁、无超时 takeover、exact-owner unlock 和 worker/hub 禁止自动迁移；DDL 成功但 checkpoint 未确认时最多重放最后一条幂等 statement。
- [x] **FLOW-08A2 编码**：实现 CH ledger bootstrap、fail-fast acquire、cancel-independent release、inspect/apply/resume/unlock、错误诊断与 JSON CLI；连接从 `default` 进入 canonical `watchdog_flow`，密码只读 secret file，TLS 配置 fail-closed。
- [x] **FLOW-08A2 单元/变更测试**：覆盖逐 statement 状态、同步写、首次/续跑 generation、并发 owner、失败 checkpoint、精确解锁、只读 inspect、非法 token、UTF-8 诊断截断、embedded 001..005 和 CLI 生命周期映射；race/vet 已通过。
- [x] **FLOW-08A2 本地环境**：固定单节点 Kafka KRaft + ClickHouse LTS Compose，回环端口、持久卷、healthcheck、Kafka 禁止 auto-create、一次性创建 12 分区 `watchdog.flow.raw-v1`；明确不作为 HA/容量结果。
- [x] **FLOW-08A2 集成测试**：ClickHouse `26.3.29.7` 无持久卷空库通过 001..005、二次 apply、003 sorting-key statement 重放、并发拒绝、wrong-owner、dirty 普通拒绝及 exact checkpoint resume；实测修正了 ch-go 空 metadata block 处理和 003 不合法的旧 ORDER BY 扩展。Kafka `4.3.1` health/topic metadata、`acks=all` 单条生产及 partition/offset 消费通过，隔离测试 topic 已删除，12 分区 raw topic 保留。
- [x] **FLOW-08A2 回归**：Flow 全范围 race/vet、全库 test/vet、compose config 与 diff check 全过。
- [x] **FLOW-08A2 已提交**：代码、环境、文档和真实测试证据已进入独立提交 `5182f28e`；工作区只保留其他平台切片和用户备份，不残留本切片生产/测试文件。
- [x] **FLOW-08A3 设计**：冻结“不可变相同前缀 + 新 migration 后缀”的唯一升级路径；旧二进制面对更高数据库版本、任意 checksum/name drift 均 fail-closed，不提供 adopt/skip/自动改 ledger。
- [x] **FLOW-08A3 编码（故障测试能力）**：复用同一 production migrator 执行旧/新 migration 子集与真实 CH，不新增测试专用迁移状态机；fake executor 只增加 cancel 注入和清理 context 观测。
- [x] **FLOW-08A3 单元**：增加调用方 cancel 后锁释放上下文仍有效的测试；原有 planner 覆盖 checksum/name/gap/ahead/dirty。
- [x] **FLOW-08A3 变更设计/测试**：cancel 停止新 statement，独立清理上下文释放锁；强杀/CH 不可达则保留持久锁供人工 exact-owner unlock。真实集成覆盖旧集合→新集合、旧集合读新库与 checksum drift 拒绝。
- [x] **FLOW-08A3 CH 单节点集成**：ClickHouse `26.3.29.7` 无持久卷空库先应用 001..004，再由新集合只应用 005；旧集合随后拒绝新库，篡改本地 checksum 拒绝；003 statement 连续 replay、dirty 005 resume 通过。持久开发实例创建空 metadata lock 后重启 ClickHouse，owner 保留；错误 owner 解锁失败，production CLI exact-owner 解锁成功。
- [x] **FLOW-08A3 Kafka 单节点集成**：Kafka `4.3.1` 隔离 topic 以 `acks=all` 写入一条，broker restart 后从原 partition/offset 读回；测试 topic 已删除，`watchdog.flow.raw-v1` 12 个 partition 的 leader/ISR 均恢复为 1。
- [x] **FLOW-08A3 Kafka worker failure/assignment replay 集成**：真实单 partition topic/group 先提交 v9/IPFIX template+data 至 committed-next-offset 4，再只写 data；全新 processor 必须从 4 向前回放 4 条才能解码新 data。注入 durable handler failure 后 offset 仍为 4，下一全新 processor 接管后同时看到 replay data `1/3` 与新 data `4/5`，offset 收敛为 6，template missing/reject 为零；测试不新建状态库/topic 类型，隔离 topic 自动清理。证据提交 `11cd81b0`。
- [x] **FLOW-08A3 active-statement deadline 集成**：真实无结果 migration statement 运行中由 100ms context deadline 中断；旧连接池关闭后，新连接池 inspect 到 dirty statement 0、锁已由独立 cleanup context 释放，显式 resume 后收敛为 applied。测试语句写入 `null()` table function，无业务副作用。
- [x] **FLOW-08A3 transport timeout/断包集成**：查明 ch-go `ReadTimeout` 只是单次 packet read 的轮询间隔，超时后库内继续读取，不能充当操作截止时间；生产 native executor 增加独立 `OperationTimeout`，且保留更早的调用方 deadline。透明 TCP 代理先通过 control query，再静默丢弃 server→client 响应；真实 CH race 连续 5 次均由约 250ms 的内部 operation deadline 结束，连接数保持 1（无隐式 DDL retry），新连接池恢复成功。证据提交 `f13b57d3`。
- [ ] **FLOW-08A3 集群/发布门禁**：旧/新实际制品滚动、Replicated/Distributed/ON CLUSTER DDL、Kafka controller/broker quorum、ISR 收缩、N+1 和 RPO/RTO 演练；单节点 Compose 不勾选。
- [x] **FLOW-08A3 回归**：Flow 全范围 race/vet、全库 test/vet、compose config 与 diff check 全过。
- [x] **FLOW-08A3 已提交（单节点兼容范围）**：版本/restart 证据进入 `d67f08aa`，active-statement deadline/resume 进入 `5d7f9761`，静默断包与生产操作时限进入 `f13b57d3`；集群/发布门禁继续保持未完成。
- [ ] 完成 retention/repair/backup、健康告警、容量预测、tenant purge、版本信息、RPO/RTO、N+1/AZ 和恢复演练。
- [ ] 固定硬件执行 2× 峰值 30m、3× 突发 5m、72h soak；报告 UDP drop、Kafka lag、CH count、CPU/RSS/GC。
- [ ] 完成 Kafka/CH/Geo/VM 组合故障、备份恢复、许可证/NOTICE/源码提供、canary/rollback/forward-fix。

## 5. 外部门禁

- [x] 真实 Kafka/CH 正常链路：四协议 UDP RawFlow、按 exporter key 保持模板/数据顺序、关闭 flush、四协议 durable fact、receipt count/counter/checksum 和五级 rollup 守恒。
- [x] 真实 Kafka 单节点故障链路：v9/IPFIX durable failure → assignment 回放 → committed offset `4→4→6` 由 `11cd81b0` 证明；production broker pause/unpause 与 lag 归零由 `3e5c6abf` 证明；重叠成员 2+2 与正常 takeover 由 `013f3f9f` 证明；实际 worker 强杀后的 broker takeover 由 `7cb8ef41` 证明；四协议 production 链路内 broker 保留数据 restart 与同进程恢复由 `0b141b3b` 证明。多 broker quorum/ISR 仍归集群发布门禁。
- [ ] 真实 ClickHouse 故障链路：静默 timeout、“fact 已提交/receipt 响应丢失→同 block 重放→count/counter/checksum 收敛”、任意 packet 中途截断及禁用 server dedup 后逻辑收敛已由 `f13b57d3`/`21a74da8`/`ffcc64e3`/`33c3f95f` 证明；仍需实际 worker/CH restart 与组合故障，正常链路或单一代理故障不能替代。
- [ ] ClickHouse 发布：实际旧/新制品、Replicated/Distributed/ON CLUSTER DDL 与集群回滚/forward-fix。
- [ ] 性能环境：固定硬件、真实混合 corpus、N+1、容量和 72h soak。
- [ ] 发布许可证：Akvorado 派生文件 SPDX/来源、根许可证、NOTICE、依赖/制品声明。

门禁未满足时不得勾选对应集成/发布项，但不得阻塞无依赖代码切片的自动循环。

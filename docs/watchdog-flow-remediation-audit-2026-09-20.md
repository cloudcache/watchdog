# Flow 三份文档的修复/优化落地复审（代码级）

日期：2026-09-20　对象：[watchdog-flow-perf-audit-2026-09-19.md](watchdog-flow-perf-audit-2026-09-19.md)、[watchdog-flow-clickhouse-ix-scale-design.md](watchdog-flow-clickhouse-ix-scale-design.md)、[watchdog-flow-collector-worker-code-review-2026-09-19.md](watchdog-flow-collector-worker-code-review-2026-09-19.md)（三者均含 2026-09-20"复核"节）。
基点：`HEAD b80d88ae`（= 流量侧最后一次提交 `313a0ac0 feat(flow): target enrichment publications to selected workers` + 3 个前端提交）。工作树另有**未提交/未跟踪**的发布闭环代码（`0049_flow_worker_deployments.sql`、`flow_worker_deployments.go`、`deployment_*.go`、`version_dual_sync.go`、`version_http.go`/`version_lkg.go` 改动、`interface_reconciliation.go`），本文一律按**未交付**处理并单独标注。
方法：四路只读复审（查询层 / 采集器·worker·解码器 / 存储·CH·生命周期 / 发布闭环）+ 本人对每个高风险结论按 file:line 复核；`go build ./...`、`go vet`、无外部依赖的 `go test` 均在当前工作树执行。**未修改任何代码。**
标记：✅ 本人已按 file:line 复核；◐ 复审者报告、未逐行复核；— 无法从代码判定（需生产/实测）。

---

## 1. 结论摘要

1. **落地面很窄，且不在文档标的 P0 上。** 三份文档共列出约 130 个整改项；HEAD 真正落地的是：查询层过滤下推（R4.6）、外部聚合/排序护栏（R4.13 的一部分）、端点两遍算法（R4.3 的端点子集）、境外全 raw 单扫、身份受限预算档、sFlow counter 入库（018）、静态倍率校准（019）、发布定向（`313a0ac0`，v1 配对协议）。code-review 的 **Phase 0 九项止血与 Phase 1 八项：0 项落地、1 项部分**；perf-audit 的 R1.3/R3.1/R4.4/R4.5/R4.7–R4.12/R4.14/R5.x/R6.x/R7.x/C1 全部未动；IX 设计 P1–P5 全部未动（§2–§4）。
2. **已落地的查询修复引入了 4 个新风险（§6）**：端点报表第二段把 Top-N 地址塞回 `DimensionValues`，绕过两遍算法重新进入 §8.3 判定为内存故障根源的精确全基数聚合；`topKWeighted` 候选集是近似算法却无任何标注/开关，触碰文档自己"近似须产品批准"的门禁；预算档（250M/16GiB/30s、500M/32GiB）由**请求过滤条件**选择而非受控配置，且 hub 无 HTTP 超时；境外全 raw 路径写死 `endpoint_consistent=1`、按远端地址族分组等三处行为变化未记录。
3. **已落地的写路径改动引入 2 个新回归（§3.2/§4.1）**：counter-only 回执（`persisted`、`record_count=0`）让未改动的对账扫描器报 `missing_records`、钉死 reconciled 水位并**永久阻塞 raw 日删除**；回执与块汇总从此携带校准后字节且无 ppm 列，`estimated_bytes_scale_ppm` 在同一列里用 0 与 1,000,000 两种编码表示"恒等"。`Ready()` 仍不校验 019 列，未升级库上是崩溃循环而非 readiness 失败。
4. **文档中 [x] 与 HEAD 不符的地方有 8 处（§2.1）**，集中在"真实 ClickHouse 集成"用例的覆盖面被夸大（无 device 过滤、无候选漏掉、无多版本）、"三条链统一预算"（境外用的是端点档）、"只投影当前指标"（联合查询未做）、"独立提交"（`045d7b3a` 一次 28 文件）。
5. **P0 发布闭环的判断需要拆开看（§5）**：HEAD 的保存链**确实**自动入队定向发布且制品含设备边界；"保存链根本没有触发重发布"对**已部署的二进制**（`cfbb31ce`/`afa7859a`，均 `+dirty`）和**工作树 WIP**（删掉了 HEAD 的自动发布）成立。53% `transit` 的直接原因是已部署服务端无自动发布 + worker 处于静态引导模式；而 WIP 的 v2 通道未跟踪且与 v1 形成两个活写者、一个只认 v1 的查询门禁，还会因共用 uint32 generation 卡死 worker。**当前没有任何一个可交付版本能闭环。**
6. **复核裁决再裁决（§7）**：同意 TryProduce/UnknownTopicRetries/SO_RCVBUFFORCE/MaxConns/LZ4/性能数字六条；不同意 QinQ"证据不足"——机制在代码里可完整判定（外层 `0x88a8` → goflow2 `parserNone` → 空地址 → 整 datagram `mapping_rejected`），运行时未知的只是暴露面；同时修正原审查的机制描述（VLAN 帧不走 `errSFlowFallback`，而是快路径内联调用 goflow2 报文解析器），并指出同一机制对任何非 IP 采样帧（ARP/LLDP/STP）在 IX 二层网里是稳态事件。
7. **两项生产操作（Kafka 低水位推进、按 `registry_version<4` 删事实与回执）没有仓库可见记录，与三份文档同日写下的生命周期/证据规则直接冲突（§8）**；之后的"24h 验收 150–170ms"是在 1.09M 行上测的，不能代表 66M/18M 规模。
8. **基线隐藏缺陷**：审查基点 `cfbb31ce` 的 `Ready()` 因 Results 顺序与 SELECT 不一致，在真实 ClickHouse 上必然失败而单元 fake 通过（`045d7b3a` 顺手修复）；`0dca0d2b` 之后的 worker 拒绝在未跑 018 的库上启动——两条部署顺序约束都无处记载（§3.2/§4.5）。

---

## 2. perf-audit（R1–R7 / C1 / §9 任务清单）逐项状态

> 基线说明：文档写于 `023ccd02`（纯文档提交）。清单里 15 个"相关提交"中 8 个（`ab97991f … 95e24b58`）是审计**之前**的状态——R4.3 的 `dense_rank`、R4.5 的分块 N+1、R4.4 的两条方向查询正是它们写出来的；审计之后真正触及查询层的只有 7 个提交：`a777ac83 edfcb51e 786733c1 28672dde 4a433d3a cfbb31ce 045d7b3a`。

| 项 | HEAD 状态 | 提交 | 证据（当前树） | 判定 |
|---|---|---|---|---|
| R1.1 热窗口 raw `GROUP BY…FINAL` | 部分（仅缓解） | `a777ac83`、`4a433d3a` | raw 11 键 GROUP BY + FINAL 未变 `query.go:895-918`；过滤下推到 `:914`；只汇总请求指标 `:906`；≥24h 自动走 1h `plan.go:78-80` | 语义保持；热层按计划未启动（Phase 5） |
| R1.3 rollup 窗口对全部 kind / 无 external sort | 未开始 | — | `rollup.go:634-635`（仅 external group-by 4GiB）、`:694-699` | — |
| R3.2 fail-closed 护栏 | **改了但方向相反** | `786733c1`→`045d7b3a` | 仍 `read_overflow_mode=throw`；身份受限查询上限升到 250M 行/16GiB/30s `query.go:35-36,61-66`，端点两遍与**境外** 500M/32GiB `query.go:41-42`、`overseas.go:219-224`；全局 50M/4GiB/15s `query.go:54` | ✅ 审计写的是"不以单纯调大常量掩盖问题"，实际做法是把常量提高 5–8×，门槛是**请求里带不带 device/target/exporter**（见 §6.3） |
| R3.3 `toUInt64(col) IN`/`CAST AS String` | 未开始（略有扩大） | — | `detail.go:725-735`；新下推也用 `toString(business_direction)` `query.go:709` | 结果正确，热扫描逐行枚举→字符串 |
| R3.4 命名预算档 | 部分/不同 | `4a433d3a`、`cfbb31ce`、`045d7b3a` | 字面量→常量 + 选择函数 `query.go:31-66`、`overseas.go:219-224`；detail/facet/address-set 仍字面量 `detail.go:480-489`、`detail_facet.go:121-126`、`address_set.go:133-139` | 不是配置驱动、无 interactive/detail/export/maintenance 档、长窗口无 operation job；档位由**请求过滤条件**决定 |
| R4.1 境外 CTE 多次引用 | 部分 | `045d7b3a` | 全 raw 计划改为单次 `FROM flow_records FINAL`（`overseas.go:319`，模板 `:303-410`）；混合 archive+raw 计划仍引用 `selected` 3 次（`:510,:523,:579`） | 仓库无 `EXPLAIN PIPELINE`/`read_rows` 证据 |
| R4.2 境外 ARRAY JOIN×3 / `toString(ip)` 分组 / 无窗口上限 | 部分 | `045d7b3a` | 全 raw：一次 `ARRAY JOIN arrayFilter` ≤6 元组 `:353-368`，无逐 IP GROUP BY，`uniqExactIf(toString(remote_ip))` `:346-347`；混合路径未变 `:471-485`；explorer 窗口无上限 `handlers_flow.go:304-343` | `uniqExact` 保留 ✓；**语义变化两处**：`endpoint_consistent` 写死 `1`（`:392`），按 family 的 `observed_local_hosts` 改按**远端**地址族分组（§6.4） |
| R4.3 top-N `sum() OVER`/`dense_rank` | 部分 | `edfcb51e`（护栏）、`786733c1`（仅端点） | 通用 V2 与联合路径仍窗口排名 `query.go:929-947`、`joint.go:420-438`；加了 external sort `query.go:564`、`joint.go:218`；两段式只在端点候选路径 `query.go:1055-1066` | 护栏正确；形状改动只覆盖身份受限的全 raw src/dst IP |
| R4.4 `direction` 进联合维度 | 未开始 | — | `joint.go:337-355` 无 direction；`flow_reports.go:651-679,696-717` 仍两条查询；`flow_query_modes.go:47,142` | — |
| R4.5 端点报表 N+1 | 未开始（审计后） | （审计前 `d589d067`） | `flow_report_endpoints.go:97-112,205-245`；IN 地址→`= toIPv6()` OR 链 `filter.go:453-463` | TopN=20 每页 **10** 条查询、TopN=100 **26** 条（§6.2） |
| R4.6 过滤在 `UNION ALL` 之后 | **已修**（聚合 V2 + 境外 V2） | `a777ac83`、`045d7b3a` | `query.go:692-727`（archive `:893`、raw `:914`）；`overseas.go:259-273`（archive `:434`、raw `:448`、单扫 `:323`） | ✅ 等价性成立（§6.1）；`dimension_value`/snapshot/geo/时间窗仍留在 UNION 后 `query.go:919-927`——`dimension_value` 本可下推，其缺席正是端点报表第二段回退的原因（§6.2） |
| R4.7 facet 逐字段扫描 / 无内存护栏 | 未开始 | — | `detail_facet.go:104,121-126,197` | — |
| R4.8 supplier 明细每页全窗排序 | 未开始 | — | `detail.go:939-940` | — |
| R4.9 峰段逐行 `toTimeZone` | 未开始 | — | `time_window.go:34-36` | — |
| R4.10 先转字符串再聚合 | 未开始 | — | `joint.go:400-405`、`query.go:902-904`、端点 `:1005,1021` | — |
| R4.11 singleflight/缓存 | 未开始 | — | grep 无 | — |
| R4.12 `ArchiveThrough` 每面板查一次 MySQL | 未开始 | — | `flow_reports.go:618,704,773`、`flow_report_endpoints.go:174` | — |
| R4.13 护栏缺口 / HTTP 总 deadline | 部分 | `edfcb51e` | 三个编译器都设了 external group-by+sort `query.go:563-564`、`joint.go:217-218`、`overseas.go:201-202`；facet 仍无 `max_memory_usage`；hub 用 `s.engine.Run` 无超时 `server.go:316`；报表用裸 `c.Request.Context()` `flow_reports.go:395` | 无端到端 deadline；现在是每条受限查询 30s × 一页最多 26 条 |
| R4.14 `hasAny(arrayDistinct(arrayConcat))` | 未开始 | — | `address_set.go:110` | — |
| R7.1 对账无 `event_time` 边界 | 未开始 | — | `reconciliation_scanner.go:274-286`（WHERE `:281-283`）；全文件无 `event_time` | ✅ |
| R7.2 整日重算 / 迟到复核重扫 | 未开始 | — | `flowlifecycle/archive.go:138-157`、`archive_scheduler.go:73-92` | — |
| R7.3 重分类分页 / `EXCEPT DISTINCT` | 未开始 | （`045d7b3a` 只加了一列） | `reclassification.go:19-22,429-448,145-151` | — |
| C1 VPN 覆盖率读空 `flow_aggregate_1m` | 未开始 | — | `vpn_candidate.go:176-182`（`:178`） | ✅ `complete_ratio` 仍来自无写入方的表 |
| §9 PERF-Q1 | 编码 ✓、单测 ✓（仅 SQL 形状）、真实 CH ◐、性能验收 ✗ | `a777ac83` | §6.1 | — |
| §9 PERF-Q1B | 编码 ✓（有缺口）、单测 ✓、真实 CH ◐、生产 ✗ | `edfcb51e`…`045d7b3a` | §6.2/§6.3 | "只投影当前指标"未覆盖联合查询；Top-N 成员近似且未标注 |
| §9 PERF-OPS1 | 运维操作，非代码 | — | — | "24h 验收 150–170ms"是在同一节把 `flow_records` 删到 1,086,644 行之后测的，不代表 §8.3 的 66M 事实/18M IP 规模（§8） |

### 2.1 文档打了 [x] 但 HEAD 不成立

1. PERF-Q1 "单元测试：验证 archive 使用 `source.*`…"——测试只断言 `source.device_id`、`source.classification_version` 两个谓词（`query_test.go:278-344`），另外五个未断言，且从不比较结果值。
2. PERF-Q1 "真实 ClickHouse 集成…覆盖 direction/category/**device**/snapshot/geo/version"——集成用例（`storage_v2_migration_integration_test.go:177-197`）无 device/target/exporter 过滤、全 raw（archive 下推未覆盖）、单条事实、只有正向匹配。
3. PERF-Q1B "真实 ClickHouse 集成…验证**版本化** `_other`"——用例 3 个端点 TopN=1 → candidate_n=8 ≥ 基数，topK 天然精确；无候选漏掉、无多版本、无多桶、无 `include_other=false`。
4. §8.3 "三条链已统一身份受限预算" / §9 "普通单遍 250M/16GiB"——境外用的是 500M/32GiB（`overseas.go:219-224`）。
5. §8.3 "通用聚合仅投影当前指标"对 `query.go` 成立，联合编译器仍汇总四套计数器（`joint.go:406-409`），境外混合路径亦然（`overseas.go:462-465`）。
6. §4 Phase 1.2 "R4.3 top-N 改两段式"——只有端点候选路径。
7. PERF-Q1 "独立提交，不夹带无关改动"——`045d7b3a` 一次提交 28 个文件：境外重写 + 预算 + worker 倍率校准 + consumer/config 改动。
8. PERF-OPS1 "24h 真实验收不再触发 resource limit"——在删表到 1.09M 行之后测得，不能证明 66M/18M 规模下的行为。

### 2.2 HEAD 做了但文档没记

境外全 raw 单扫模板 + runner 侧 Top-N（PERF-Q2 仍列为待办）；境外混合路径谓词下推到 `raw_base`/`archive_selected`；境外身份受限预算 = 端点档 500M/32GiB/30s；境外全 raw 路径的三处行为变化（`endpoint_consistent` 常量、按远端族分组、Top-N 前返回整个 geo 目录）；联合查询在身份受限时 `max_execution_time=30`。

---

## 3. code-review（采集器 / worker / 解码器 + Phase 0/1）逐项状态

> 来源说明：自审查基点 `cfbb31ce` 到 HEAD，触及 worker/collector Go 代码的**只有一个提交 `045d7b3a`**（consumer.go / config.go / worker main.go / decode_adapter.go / enrich.go / record.go / plan.go / native.go +6−2 / reclassification.go / 019）。`313a0ac0` 在 `internal/flowworker` 与 `cmd/watchdog-flow-worker` **零 hunk**——所有 `version_*.go`/`deployment_*.go`/worker `main.go` 改动都只在工作树。`cf8b55eb`/`0dca0d2b`（counters）是基点的祖先，原审查已看过。采集器热路径（`receiver.go`/`producer.go`/collect `main.go`）自 9 月 5–18 日起未动，所以采集器全部发现按构造即为"未开始"。

| ID | 原结论（缩写） | HEAD | 工作树 | 证据（当前树） | 判定 |
|---|---|---|---|---|---|
| **采集器** | | | | | |
| H1 | `SO_RCVBUF` 被 `rmem_max` 钳制、无回读 | 未开始 | 同 | `receiver.go:60-64` 仅 `SetReadBuffer`；`deploy/` 无 sysctl | ✅ 复核已用生产 `rmem_max=212992` 证实，代码仍未动 |
| H2 | 计划只加载一次、到期即崩溃循环 | 未开始 | 同 | collect `main.go:133` 单次 `LoadSignedPlan`；`plan.go:104-111` | ✅ |
| H3 | 只 Ping broker 不验 topic | 未开始 | 同 | `main.go:166-171`；`producer.go:60-72`；grep `kadm\|RequestMetadata` 空 | ✅ |
| M1 | 无 `MaxBufferedBytes` | 未开始 | 同 | `producer.go:61` | ◐ |
| M2 | 全局 Produce 阻塞所有接收器 | 未开始 | 同 | `producer.go:127` | ◐ |
| M3 | `Send` 早返回不计数、`OnPublishError` 未接线 | 未开始 | 同 | `producer.go:99-120` 四处 `finish(err)`；`main.go:212-227` | ✅ |
| M5 | 计划变更→退出→5s 端口空窗 | 未开始 | 同 | `main.go:108-117,261-268`；unit `RestartSec=5` | ◐ |
| M8 | franz-go 无 logger | 未开始 | 同 | grep `WithLogger` 空 | ◐ |
| M9 | `kernel_drops` 无标签、仅 cmsg | 未开始 | 同 | `flowmetrics/metrics.go:97`；`receiver.go:82-88` | ◐ |
| M10 | `collector_id` 不与 `-agent-id` 绑定；v1 信封被接受 | 未开始 | 同 | `main.go:213`；`signature.go:131,158-161` | ✅ |
| M12 | SASL PLAIN 无 TLS 也接受 | 未开始 | 同 | `config.go:140-151` | ✅ |
| M4/M6/M7/M11、L1–L10 | 关机丢弃计数、线性准入、分配、分区、unit | 未开始 | 同 | 文件未变 | — |
| **worker** | | | | | |
| C1 | 数据形状错误退出整进程 | 未开始 | 同 | `processor.go:149-157,122-125` → `consumer.go:284-288` → `main.go:309-331` → `log.Fatal :146-148`；无任何分区暂停 | ✅ 触发面按协议细化：`enrich.go:467` 的 1024 上限现在计 Records+CounterRecords，对 v9/IPFIX 可达（64KiB 报文小模板），对 sFlow（≤1000 样本钳制）与 v5（≤30）不可达 |
| H1 | floor 以下重放重跑持久化处理器；回执 generation 取 Kafka 时间戳 | 未开始（潜伏） | 同 | `consumer.go:336` 对全部记录跑处理器 vs `:343` `committable`；`processor.go:140,164-165`；`native.go:701-704`；`decoder.go:296`；`kafka_replay_integration_test.go:85-87` 断言 `[1,3,4,5]`（floor 以下 1、3 再次进处理器） | ✅ 仅因默认 replay=0（`consumer.go:119-123`）而潜伏 |
| H2 | 去重 token 不确定；无去重窗口 | 未开始 | 同 | `native.go:244-247`；无迁移设窗口（只有 `transport_timeout_integration_test.go:211` 设成 **0**） | ✅ |
| H3 | 每 fetch 屏障、分区串行、同步提交 | 未开始（`045d7b3a` 只改了形状） | 同 | `consumer.go:326-370` + `wait.Wait()`，`:278` 每 poll 同步 `CommitRecords`；`writer.go:190-197` | ✅ 分块（4096）让 durable 前缀可提交，屏障仍在 |
| H4 | 无 valid-until；`effective_from` 可回溯；陈旧 LKG 静默 | 部分（`313a0ac0` 控制面） | WIP 加 v2 规则 | `version_catalog.go:215-231` 无 valid-until；`flow_enrichment_publications.go:659-670`：**自动**路径强制 ≥ now+1min，**手动**只要求 `> latest`；worker `main.go:734-740` 只记日志。WIP `flow_worker_deployments.go:147` 拒绝早于当前分钟（未跟踪） | ◐ 仅控制面部分 |
| M1 | `Guard.Covers` 每记录拷贝/校验/格式化 | 未开始 | 同 | `pipeline.go:137-144` → `barrier.go:178-184,65-79` | ✅ |
| M2 | 整 datagram 隔离；文件引导无 barrier | 未开始 | 同 | `pipeline.go:92-101`；`main.go:223-230,252-256` | ✅ |
| M3 | `Ready()` 缺 `estimated_bytes_scale_ppm` | 未开始 | 同 | `native.go:149-151` 五列、`:191 != 5`；`native_test.go:97` `requiredRecords: 5`；插入写 `:450,:523` | ✅ 未跑 019 的库：readiness 通过 → 每次插入 `NO_SUCH_COLUMN_IN_TABLE`（永久错误，`writer.go:845-860` 不重试）→ 退出循环 |
| M4 | LKG 用 `now` 验签 | 未开始 | **WIP 复制了同一缺陷** | `version_lkg.go:429`、`main.go:595` `time.Now()`、`version_signature.go:92`、`trust.go:205-208`；WIP `RestoreDeployments`（`version_lkg.go:367`）同样用 `now` | ✅ |
| M5 | 任何 `fetches.Errors()` 致命 | 未开始 | 同 | `consumer.go:261-269` | ✅ |
| M6 | 重放默认 0 与设计矛盾 | 未变（`045d7b3a` 设 0） | 同 | `main.go:115`、`config.go:111`；`flow-module-design.md`"必须为正且有硬上限" | ✅ 决策仍悬而未决 |
| M7 | SIGTERM 取消进行中插入 | 未开始 | 同 | `main.go:313-317`；`consumer.go:322`；`writer.go:227-228,244-248`；仅提交有新 10s ctx `consumer.go:274-277` | ◐ |
| M8 | nil rejection observer | 未开始 | 同 | `main.go:260`；`processor.go:196-201` | ✅ |
| M9 | 倍率未知 → `estimated_bytes=0` | 未变 | 同 | `decode_adapter.go:281-288`；`batch.go:196` | ✅ |
| L1–L7 | 解码器不随 revoke 驱逐等 | 未开始 | 同 | `decoder.go:523-544`；`consumer.go:112-117` | ✅ |
| **解码器** | | | | | |
| C1 | 零字段模板无界循环/OOM | 未开始 | 同 | `decoder.go:196-199,215` 原样把 goflow2 store 交给 pipe；grep `AddTemplate\|FieldCount\|GetTemplateSize\|WithTimeout` 空；`recoverDecoderPanic :277-284` 只接 panic | ✅ 循环内无取消点，超时无法中止；只有 OOM-kill，随后未提交 offset 重放 |
| H1 | QinQ `0x88a8` 整 datagram 被拒 | 未开始 | 同 | 机制链见 §7.1 | ✅ 机制在代码里可判定；暴露面需运行时数据 |
| H2 | BGP 属性在 `DecodedRecord` 边界丢弃 | 未开始 | 同 | `decoder.go:84-111,116-130` | ✅ |
| M1 | 802.1Q `VlanId` 不拷贝 | 未开始 | 同 | `sflow_v5_fast.go:466-471` | ◐ |
| M2 | 手写解析器无 fuzz；v5 快路径在 recover 之外 | 未开始 | 同 | `go test -list Fuzz`：flowstream 只有 `FuzzInspectFlowProtocolNeverPanics`；`decoder.go:309-316` | ✅ |
| L1–L4 | 文档漂移、if-format 位、RouterKey 含端口、无序列 gap | 未开始 | 同 | `flow-decode-fastpath.md:124,144`；`sflow_v5_fast.go:311-312,328-329`；`decoder.go:336` | ◐ |

### 3.1 Phase 0（九项止血）与 Phase 1 落地情况

| 项 | 状态 | 证据 |
|---|---|---|
| 0.1 模板守卫 | 未开始 | = 解码器 C1 |
| 0.2 数据形状错误改回执；可重试 fetch 错误继续 | 未开始 | `enrich_test.go:397-399` 仍断言 "future skew" 为错误；`consumer.go:261-269` |
| 0.3 topic 元数据 fail-fast | 未开始 | = H3 |
| 0.4 `SO_RCVBUF` 回读 + sysctl | 未开始 | = H1 |
| 0.5 `Send` 计数恒等式 | 未开始 | = M3 |
| 0.6 collector_id==agent-id；拒 v1；PLAIN 需 TLS | 未开始 | = M10/M12 |
| 0.7 `0x8100/0x88a8` pcap 差分测试 | 未开始 | 无夹具；Akvorado 夹具全部 SKIP |
| 0.8 `Ready()` 加列；observer 日志 | 未开始 | = M3/M8 |
| 0.9 systemd `LimitNOFILE`/`TimeoutStopSec`/`MemoryMax`/`Wants=` | 未开始 | 两个 unit 自 `db6f5a6d` 未变：仅 `Restart=on-failure`、`RestartSec=5`、`After=network-online.target`，采集器 metrics 仍 `127.0.0.1:9090` |
| 1.1 floor 以下只解码；回执 generation 同源 | 未开始 | = H1 |
| 1.2 确定性 token + 去重窗口 | 未开始 | = H2 |
| 1.3 `effective_from ≥ now+延迟`；真正的分区暂停 | 部分（`313a0ac0` 自动路径） | = H4 |
| 1.4 LKG 按签发时间验签 | 未开始（WIP 复制缺陷） | = M4 |
| 1.5 计划热重载 | 未开始 | = H2 |
| 1.6 采集器与 worker 共用含 ODID 的 `Admit` | 未开始 | collect `main.go:217-219` `AdmitSourceFamily`（仅前缀，`plan.go:326-336`）vs worker `catalog.go:85-87` `registry.Admit`（含 ODID，`plan.go:307-321`） |
| 1.7 按记录隔离；整数日 barrier | 未开始 | = M1/M2 |
| 1.8 hub 池拆分 | 未开始 | `server.go:395` |

**结论：Phase 0 九项与 Phase 1 八项，HEAD 落地 0 项、部分 1 项（1.3）。**

### 3.2 `045d7b3a` 落地部分的正确性复核

**消费者提交语义（当前 `consumer.go`）** ✅：`DisableAutoCommit()`（`:107`）+ `BlockRebalanceOnPoll()`（`:108`）；`AutoCommitMarks`/`MarkCommitRecords`/`CommitMarkedOffsets` 已删；revoke/lost 钩子只更新 stats（`:112-117`）；`Close` 不再提交（`:482-490`）。每次 poll：按分区分组、每分区一个 goroutine、`processPartitionChunks`（`:336`）按 `partitionBatchRecords`（默认 4096，`config.go:107`）切块串行调处理器；`RunPartitionBatches` 把处理器包装为成功 `(len,nil)`/失败 `(0,err)`（`:230-235`），**块级全有或全无**；`processed` = 完整成功块之和（`:396-398`）；`records[:processed]` 过 `committable` 后记每分区最后一条（`:341-353`）；`wait.Wait()` 后每 poll **一次同步 `CommitRecords`**（`:270-283`），再返回 `processErr` → `main.go:309` → `log.Fatal`。分区中途失败：该分区只提交 durable 前缀，同伴各提交自己的前缀，然后**整进程退出**；`TestConsumerPartitionBatchCommitsDurableChunksBeforeLaterFailure`（`consumer_test.go:238-272`）锁定。块内部分块已插入后失败：块返回 `(0,err)` → 该块**一条不提交** → 重放用相同 token/generation 重插已成功的块 → 无去重窗口即物理重复，仅靠 `ReplacingMergeTree+FINAL` 收敛。**未发现任何"offset 先于 facts+counters+receipts 持久化就提交"的路径**：提交要求处理器返回 nil，`pipeline.Handle` 只在 `writer.Write` 返回 nil 后返回 nil（`pipeline.go:122-126`），后者要求每块三次 `Do` 都成功（`native.go:220-240`，`async_insert=0, wait_for_async_insert=1` `:253-256`）；隔离路径同样要求两次插入成功（`quarantine.go:74-86`）。遗留：replay 关闭时 `replayWatermarks` 仍分配并每记录取锁（`:96,:343,:176`）。

**插入顺序** ✅：facts（`:220`）→ counters（`:229`）→ receipts（`:237`），回执最后，与复核 B.3 一致；三个 token `token`/`token+":counters"`/`token+":receipts"`（`:244-247`）。counters 失败而 facts 已成功：可重试错误上抛到 `insertWithRetry`（`writer.go:213-259`）**整块重调 `InsertFlowBlock`**，facts 以同 token 重插 → 无窗口即重复；三次插入共用一个 `RetryMaxElapsed`（默认 2min，`writer.go:168-170`）；永久 CH 错误（`:845-860`，含 `NO_SUCH_COLUMN_IN_TABLE`）不重试、直接致命。

**倍率端到端** ✅：`plan.go:135-140` 非零须在 `[500_000, 2_000_000]`，`pre_scaled` 绑定有效 ppm 必须 = 1,000,000；`EffectiveEstimatedBytesScalePPM()` 0→1,000,000（`:191-196`）。`decode_adapter.go:178-191` 仅 `SamplingSampled` 时应用；估算无效时仍记录 ppm；`scaleEstimatedBytes`（`:212-224`）= `(bytes×ppm+500_000)/1_000_000`，`bits.Mul64/Add64/Div64` 128 位、四舍五入；`hi ≥ 1e6` 溢出 → bytes=packets=0、valid=false、`QualityCounterOverflow`；非恒等置 `QualityEstimateCalibrated`（`:38`）。**packets 不缩放**（`enrich.go:551` 校验）。未知/`pre_scaled` 记录存的 ppm 是 **0**，而 019 给历史行的默认是 1,000,000——同一列两种"恒等"编码，任何 `WHERE estimated_bytes_scale_ppm != 1000000` 都会误判。`flow_ingest_receipts` **无** ppm 列，但回执与块汇总（`batch.go:366-372,196-206`、`quarantine.go:46-52`）用的是 adapter 已缩放后的 `EstimatedBytes`——回执从此携带**校准后**字节而无每条来源，与 `raw_bytes×sampling_rate` 的对账会相差 ppm。测试：`decode_adapter_test.go:204-244`（1,027,000 用例 + 2,000,000 溢出用例）。

**sFlow counters** ✅：counter-only datagram → `persisted` 且 `RecordCount=0, CounterRecordCount=n`（`processor.go:146-147`、`decode_adapter.go:122-138`、`batch.go:352-358`、`native.go:728`）；`Ready()` 含 `sflow_interface_counters` 9 列（`native.go:157-160,191`）与 `counter_record_count`（`:155`）；测试 `processor_test.go:57-100`、`sflow_v5_fast_test.go:65-138`。**基点上的隐藏缺陷**（`045d7b3a` 顺手修复）：`cfbb31ce` 的 `Ready()` SELECT 顺序为 records/receipts/**counters**/quarantine，而 `Results` 顺序为 records/receipts/**quarantine**/counters（本人已核 `git show cfbb31ce:internal/flowch/native.go`）；ch-go 按位置绑定并在列名不符时报错（`proto/results.go DecodeResult`，◐）——即基点二进制在真实 ClickHouse 上 readiness 必然失败，单元 fake 按位置追加不校验列名所以测试通过；`045d7b3a` 调整了顺序并让 fake 校验列名（`native_test.go:39-52`）。这正是 M3 同类的"readiness 测试替身弱于真实客户端"缺口，而 `native_test.go` 至今仍不含 `estimated_bytes_scale_ppm`。

**配置变化** ◐：`FetchMaxPartitionBytes` 1→16MiB、`FetchMaxBytes` 64MiB/broker，配合每分区 goroutine 且 unit 无 `MemoryMax`，in-flight 内存上界仍是估算值。

### 3.3 工作树 WIP（未交付，不作完成度计）

意图：在旧 publication 通道旁增加 v2"按 worker 部署"通道。`version_http.go` 增 `FetchDesiredDeployments`（404→`ErrDeploymentRemoteUnsupported`）、`OpenDeploymentArtifact`（要求 `X-Watchdog-Object-Checksum` 等于清单校验和且 `Content-Length` 精确）、`AcknowledgeDeployment`（`downloaded/verified/installed/failed`）、`RemoteDeploymentSync.SyncOnce`（trust bundle → 验签清单 → 登记 LKG → 逐制品缓存或下载并 sha256 校验 → `downloaded` ACK → `DeploymentLoader.Install` → `installed` ACK；任一失败 → `failed` ACK）。`version_lkg.go` 增 `RegisterVerifiedDeployment`/`PersistDeployment`（不可变 `deployments/%020d.json`）/`RestoreDeployments`。`deployment_manifest.go` 签名清单；`deployment_loader.go` 把地址目录 + policy + 每设备边界编译成一个 catalog 条目（先持久化后发布）。`version_dual_sync.go`：v2 优先，仅在 `!v2Active` 且 v2 端点 404 时回落 v1，一旦 v2 安装/恢复过就永不再装 v1。`main.go` 接 `RestoreDeployments`、`NewDualRemoteVersionSync`，cursor = max(旧版本, 部署 generation)。
风险：① 陈旧边界 fail-open **原样继承**——`runVersionSyncLoop`（`main.go:734-740`）任何同步错误只记日志继续，有 LKG 时初始同步失败也继续（`:628-635`）；WIP 只加了服务端 `failed` ACK 可见性，worker 无指标/暂停/readiness 变化。② 未验证制品安装：未发现（清单签名 → 制品校验和 → `StoreObject`/`objectAvailable`/`Fetch` 全部重哈希 `version_lkg.go:175-236,240-268,270-291`；loader 复核大小 `deployment_loader.go:160-172`）。③ LKG 语义变化：旧 `Restore` 先在新 catalog 里暂存再原子发布（`version_lkg.go:408-447`），`RestoreDeployments` **逐 generation 直接装进活 catalog**（`:350,:377`），启动出错仍中止（`main.go:599-602`），"全有或全无"从 catalog 级降为进程级；M4 的 `now` 验签被复制（`:367`）。④ 共用 uint32 游标：generation ≤ cursor 的部署永远不会被拉取、只留一行日志（`version_dual_sync.go:9-12` 注释要求服务端分配高于旧版本，未在此验证）。⑤ 服务端 v2 `effective_from` 仅要求 ≥ 当前 UTC 分钟（`flow_worker_deployments.go:147`），比旧手动路径严，但仍不是"now + 激活延迟 ≥ worker 滞后"。

---

## 4. 存储 / ClickHouse / 生命周期 / IX 设计逐项状态

> 来源说明：自 `023ccd02` 起触及 `internal/flowch`、`flowlifecycle`、`billing`、`snmpch`、`deploy/migration/clickhouse` 的提交只有 `0dca0d2b`（018 + counter writer）、`045d7b3a`（019 + scale_ppm writer + reclassification 3 行）、`313a0ac0`（server.go +5 启动 enrichment opjob worker）。这些目录在工作树与 HEAD 逐字节一致，例外是未跟踪的 `interface_reconciliation.go(+_test)`。

| 项 | 状态 | 证据 | 判定 |
|---|---|---|---|
| R2.1 去重窗口 | 未开始 | 无迁移设 `non_replicated_deduplication_window`；只有 `transport_timeout_integration_test.go:211` 在故障测试里设为 **0**；`native.go:256` 仍发 token | ✅ token 在非复制表上仍是空操作 |
| R2.2 确定性 token | 未开始（`0dca0d2b` 改了格式） | `native.go:244-247` token = stream/partition/首尾 offset+index/三个计数；边界由 `batch.go:136,173,239`（MaxRows/ApproxBytes/MaxPartitionDays/poll chunk）决定 | 仍编码块边界；格式变化未记录；`FirstRecordIndex` 可能取自 counter 的 `record_index`（`batch.go:246`，另一索引空间） |
| R2.4 `FINAL` 处处 | 未开始、且方向为负 | 未跟踪 `interface_reconciliation.go:249,257,286,292` 再加四次 FINAL 读 | — |
| R5.1 receipts 生命周期 | 未开始 | 018 只加了 `counter_record_count DEFAULT 0`（`018:56-57`）；无 TTL/DROP 作业；带外删除见 §8 | — |
| R5.2 1h 归档基数 | 未开始 | `rollup.go:605,675,693-699` 未变 | — |
| R5.3 `flow_reclassified_records` 显式 DDL | 未开始（019 对其 ALTER） | 仍是 016 的 CTAS 派生表 | — |
| R5.4 raw 删除链 / 无 TTL | 未开始 | `flowch/raw_delete.go:30` `DROP PARTITION` 唯一路径 | — |
| R5.6 codec | 未开始（018/019 自带 codec） | `018:5-46` 每列有 codec；`019:5` `T64,ZSTD(1)`；既有宽列未动 | — |
| R6.1 每分区流水线 | 未开始 | `consumer.go:293-370` 每分区 goroutine + `wait.Wait()`（`:370`） | ✅ |
| R6.2 跨 poll 合批 | 部分（只加旋钮） | `config.go` 新增 `FetchMaxBytes/FetchMaxPartitionBytes/PartitionBatchRecords`；worker `main.go:110-114`；无跨 poll 累加器；018 让每块多了**第三次**同步 INSERT（`native.go:224-231`） | part 率只会上升（推断，未测） |
| R6.3 / R6.4 / CH3 / CH4 / CH7 / HP1–HP4 | 未开始 | `pipeline.go:103` 仍 `EnrichBatch`；`native.go:265+` 每次调用重建列；`quarantine.go:74,84`；`record.go:105-106` `[]byte` 地址 | ✅ |
| R7.1 / R7.2 / R7.3 | 未开始 | `reconciliation_scanner.go:281-283` 无 `event_time`；`archive.go:138-157` 从 `NextHour`（新作业为 0）重跑；`archive_scheduler.go:73-86`；`reclassification.go:19-22,438-449` 未变，`045d7b3a` 仅在 `:406,:423,:434` 加列 | ✅ |
| CH1 / CH5 / CH6 hub 池拆分、`QueryID`、TLS/超时 | 未开始 | `server.go:387-391`（工作树 `:395`）`MaxConns: 8, MinConns: 1`，无 `TLS`、无超时、无 `.Stat()`；`313a0ac0` 只 +5 行起 opjob worker | ✅ |
| CH8 loopback LZ4 | 未开始 | `native.go:106` 常量 `CompressionLZ4`（文档自述未验证） | — |
| Phase 1.2 / 1.7 / 1.8、Phase 2.1–2.3 | 未开始 | — | — |
| IX P0 `.18` 实测 | 部分（仅文档） | perf-audit §8；§8.4 列出 Kafka lag/硬件/水位仍缺 | — |
| IX P1 投影/skip index/codec/确定性块/去重窗口 | 未开始 | grep 无 `p_iface`、`flow_records` 无 `ADD INDEX`/`bloom_filter`（仅 `014_snmp_events.sql`）、006 之外无 `ADD PROJECTION` | — |
| IX P2 `flow_interface_traffic_5m` + 关闭桶 job + 计费/对账切读 | 未开始 | grep 无此表；`flowch/billing.go:247-285` 仍 raw `FINAL CROSS JOIN`；`billing/service.go:133` 读它 | — |
| IX P3 `sflow_interface_traffic_5m` + 校准表 + 动态 k | 未开始（落地的是静态 ppm） | 无 `sflow_interface_traffic_5m`/`flow_interface_calibration*`；读时差分只在未跟踪文件 | 静态 ppm ≠ 动态 k |
| IX P4 维度热层 | 未开始 | `vpn_candidate.go:176-182` 仍读 `flow_aggregate_1m`（C1） | ✅ |
| IX P5 去 FINAL / 分片 | 未开始 | — | — |
| 018 `sflow_interface_counters` | **已落地** | §4.2 | DDL 正确；回执语义使扫描器回归（§4.1） |
| 019 `estimated_bytes_scale_ppm` | **已落地**（writer/账本）；readiness 缺口开放 | §4.3 | 崩溃循环模式 |

### 4.1 HIGH 回归：counter-only 回执让未改动的对账扫描器报 `missing_records` ✅

`0dca0d2b` 之后 counter-only sFlow datagram 的回执为 `persisted` 且 `record_count=0`（`processor.go:146-147`、`batch.go:352-358`）。对账扫描器 `readReceipts` 只选 `record_count` 等列、**不读 `counter_record_count`**（`reconciliation_scanner.go:219-225`，本人已核）；分类逻辑对 `persisted` 且无事实行的 offset 直接给 `MismatchMissingRecords`（`:162-167`，本人已核 switch 分支 `case !hasFacts`）；`classifyMessageCounters`（`:189-199`）也不比较 counter 计数；扫描器从不读 `sflow_interface_counters`。后果：`flowlifecycle/reconciliation.go:170-184` 把 reconciled 水位钉在第一个这样的 offset 并持久化 `mismatchCount>0`；`delete_readiness.go:143-146` 要求 healthy/0 mismatch/水位越过才允许删 raw 日 → **凡出现过 counter-only 报文的分区，raw 日删除被永久阻塞**。此外 `DayOffsetCoverage` 过滤 `record_count > 0`（`retention_coverage.go:50`），counter-only 回执对覆盖率没有贡献。仓库无 scanner+counters 测试（只有 `native_test.go`/`schema_test.go` 提到 `counter_record_count`）。PERF-SFC1 自述 15 分钟内曾有 120 个 `empty` 回执，说明该类报文并非罕见（文档数字，未测）。

### 4.2 018 复核 ◐

`ReplacingMergeTree(ingest_generation) PARTITION BY toYYYYMM(event_time) ORDER BY (device_id, if_index, event_time, source_stream_id, kafka_partition, kafka_offset, sample_index, record_index)`，每列显式 codec，无 TTL/投影/去重窗口。重放身份：排序键前缀 `device_id, if_index, event_time` 在重放时稳定（`event_time` = 信封里的 `ReceivedAt`，`ingest_generation` 同源毫秒，`device_id/if_index` 由信封钉住的 registry 版本决定）→ 同一消息折叠为一行；但若在**不同绑定内容**下重解码会产生第二行（不同 `device_id`）而非替换，`target_id` 不在键内。无 TTL 只是"没写"，**不是测试保证**：`schema_test.go:110-116,122` 只对 011/012 文本匹配 `TTL event_time/inserted_at/observed_at/bucket_start`；DSN 门控的 `storage_v2_migration_integration_test.go:283-286,303-306` 只检查 `flow_records`/`flow_aggregate_1m`/`1h` 的活表 DDL；018/019 无人看守。`counter_record_count DEFAULT 0` 只在写入侧校验（`validatePreparedReceipts`：persisted 需 `RecordCount+CounterRecordCount>0`）。

### 4.3 019 复核 ✅

DDL：对 `flow_records` 与 `flow_reclassified_records` `ADD COLUMN IF NOT EXISTS estimated_bytes_scale_ppm UInt32 DEFAULT 1000000 CODEC(T64, ZSTD(1)) AFTER sampling_source`。账本/测试：`migrations_test.go:20` 期望 19 个迁移；`schema_test.go:31,79`。查询层：`internal/flowquery`、`flowch/billing.go`、`billing`、`server` 无任何 CH 查询引用该列（grep 命中仅 MySQL 绑定 CRUD），0 与 1,000,000 对报表/计费不可见；唯一 CH 读者是 `reclassification.go:406,423,434`（复制进重分类表）。三态：1,000,000（019 前的行与未校准采样行）/ 校准值 / **0**（019 后写入的 pre-scaled 或倍率未知行），019 头注释未描述 0 的情况。未跑 019 时的失败模式（代码追踪，未运行）：worker `main.go:242` `Ready` 通过 → 首个 `InsertFlowBlock` 含该列（`native.go:523`）→ `NO_SUCH_COLUMN_IN_TABLE` → `classifyClickHouseError`（`:850-858`）判为 Permanent → `writer.go:221-225` 不重试 → `consumer.go:284-287` → `main.go:307-330` 退出、未提交 offset → 重启重取同批消息：**崩溃循环而非 readiness 失败**。hub 的重分类作业同形（`reclassification.go:237,249` 复用 `buildRecordInput`，自身 readiness `:84-86` 只查六个身份列）；hub 启动时 `server.go:405` 的 readiness 是 `snmpch` 的，不是 flowch 的。

### 4.4 未跟踪的 `interface_reconciliation.go` 代码复核 ◐（按未交付处理，但若原样提交将引入以下问题）

编译/vet 通过，两个单测用 fake executor 只断言 SQL 子串与比率算术；无集成测试、无 HTTP 路由或调用者——从任何二进制都不可达。缺陷：① `FROM flow_records FINAL INNER JOIN scope ON records.device_id=scope.device_id AND (records.ingress_if_index=scope.if_index OR records.egress_if_index=scope.if_index)`（`:249-251`）——flowch 里同一谓词已证明的形式是 `CROSS JOIN … WHERE`（`billing.go:273-278`），ClickHouse 24.9 是否接受 JOIN ON 里 AND 下嵌套 OR 无任何测试证明；② 无 wrap 处理（64 位 sFlow 计数器可容忍），任何回退都按 reset 丢弃 delta；③ counter 侧无 gap 标志，>15 分钟空洞静默变成 coverage 0；④ 序列作用域过粗：分区键缺 `sub_agent_id`/`source_id_type/value`/`exporter_id`，两个来源上报同一 `(device_id, if_index)` 会交错出假 reset；⑤ 时间基准是采集器接收时间而非 agent uptime（`decode_adapter.go:125`），同 datagram 的 counter 记录 `event_time` 相同（elapsed 0 → 忽略）；⑥ 区间 `(prev, cur]` 整体归入含 `cur` 的桶不按比例分摊（与 snmpch 相同），且 flow `[from,to)`、counter `(from,to]`、SNMP `[from,to)` 边界不对称；⑦ 纯读时、无 generation、无关闭桶守卫（`to` 可等于 now），结果随到达漂移；⑧ 四次 FINAL 跨最多 7 天在同步路径上，默认预算 100M 行/8GiB/2GiB/30s、上限 500M/64GiB/8GiB/2min（`:85-90,213-217`）——大于 R3.4 批评的交互预算；⑨ LIMIT 语义安全（`LIMIT MaxResultRows+1` + `result_overflow_mode=throw` + 预检，超限报错不静默截断，硬上限 250,000）；⑩ scope 按 `(device_id, if_index)` 去重（`:193-197`）但 SNMP 按 `(device_id, port_id)` join（`:294`），共享 `port_id` 的两个 scope 会各拿同一份 SNMP 字节；⑪ 方向语义正确落在接口位置层（复核 B.2 满足，无 `arrayJoin` 双计）；⑫ SNMP generation 作用域是从 `snmpch/billing.go:227-233` 逐字复制的桶级 marker（复核 B.4 未被违反，因为没有新 5m 表，但继承了粗粒度）。

### 4.5 设计文档一致性 ✅

- "三表写入不存在跨表事务"——成立：`native.go:215-240` 三次 `Do`、回执最后；counters 与 receipts 之间崩溃留下无回执的 facts+counters（`missing_receipt` 直到重放），重放靠同键同 generation 收敛。
- "当前代码和 schema 测试明确禁止在 Flow DDL 写死 TTL"——**过宽**。禁令只是对 011/012 的文本匹配加三张活表的 DSN 门控检查；仍含 TTL 的迁移：`001:95,127,158,178,207`、`010:57,97`；全部迁移应用后仍带固定 TTL 的活表：**`flow_vpn_candidates`**（`001:207` `TTL window_end + INTERVAL 90 DAY`，003/013 仅 ALTER），以及 `*_legacy_*` 表；018/019 无 TTL 仅因作者没写。
- HEAD 有而文档未记：`0dca0d2b` 把 `receiptSchemaVersion` 4→5（`native.go:31`）且 `Ready()` 要求 12 个回执列 + 9 个 counter 列（`:191`）——**0dca0d2b 及之后的 worker 二进制拒绝在未跑 018 的 schema 上启动**，这条部署顺序约束无处记载；`WorkerSchemaVersion = 6`（`batch.go:17`）；token 格式变化；被跟踪的 perf-audit 自身前后不一致（§8 基线 45.3M 事实/11.1M 回执写在 §9 PERF-OPS1 删表之前）。

### 4.6 测试与构建

`go build ./...`、`go vet ./internal/flowch/ ./internal/flowlifecycle/` 通过；`go test`：flowch **80 过/33 skip/0 败**、flowlifecycle 36/0/0、billing 13/2/0、snmpch 16/1/0；skip 全部由环境变量门控（`WATCHDOG_FLOW_CLICKHOUSE_DATA_INTEGRATION` 21 个、`_FAULT_INTEGRATION` 5 个、`WATCHDOG_FLOW_KAFKA_CLICKHOUSE_INTEGRATION` 3 个等）。

---

## 5. 客户边界发布闭环（P0）HEAD vs 工作树

### 5.1 三份文档的核心事实判断需要按"看的是哪一份代码"拆开 ✅

| 说法 | 对 HEAD `313a0ac0` | 对已构建/已部署的二进制 | 对工作树 WIP |
|---|---|---|---|
| "保存链根本没有触发重发布" | **不成立**：create/update/delete 在同一事务里重同步单例 profile 并入队 `flow.enrichment.publish` 作业，返回 `202 {id, publication_job}`（`flow_customer_boundaries.go` HEAD `:225-244,305-323,371-390`，本人已核） | **成立**：树内 `watchdog-flow-worker`/`watchdog-flow-collect` 二进制戳为 `cfbb31ce +dirty`，`.codex-deploy-afa7859a/release/bin/*` 为 `afa7859a +dirty`（`go version -m`，本人已核）；`cfbb31ce` 的保存处理器只调 `syncFlowClassificationProfile` 并返回 201，无入队（`:213,227,280,318`） | **成立**：未提交 diff 删掉了正是那 119 行，改为返回 `{"source_revision", "runtime_state":"draft", "publication_required":true}`（工作树 `:231,:298,:363`，本人已核）；`flow-module-tasklist.md` 的 diff 写明这是有意为之（"G4…已由 H 取代写路径…binding 只作推荐且绝不自动补写"） |
| "旧 ACK 表为空、worker 继续跑旧边界" | HEAD 已发布的制品**包含**设备边界内容：`resolveFlowClassificationDeviceProfiles` 把 `flow_customer_source_prefixes` 的 `cidr, customer_id, name` 读进分类 JSON（`flow_enrichment_publications.go:450-457`），worker 每设备编译一棵 `bart.Table`（`flowdimension/classification.go:229-244`）并经 `DeviceDirectionAttribution` 分类（`enrich.go:310`） | worker 只有带 `-control-plane-url` 启动才轮询控制面（worker `main.go:518-527`），否则加载静态 `-bootstrap-version-publication` 且 `versionSync == nil`（`:531-549,301-304`）；PERF-CUST6 首条自述生产用静态产物——**与服务端代码无关，MySQL 里保存的任何东西都到不了这个 worker** | — |

因此 53% `transit` 的直接原因是：**已部署的服务端二进制没有自动发布 + worker 处于静态引导模式**，而不是 HEAD 缺少发布链。HEAD 未部署；WIP 又把 HEAD 的自动发布删掉、而 v2 链未跟踪——**当前没有任何一个可交付版本能闭环**。

### 5.2 HEAD 能力矩阵（`313a0ac0`）◐（本人复核了保存链与制品内容两行）

| 能力 | HEAD | 工作树 WIP（未交付） |
|---|---|---|
| 保存边界 | 同事务：upsert `flow_worker_device_bindings`（`:211`→`flow_worker_bindings.go:156-200`）、写两张边界表（`:216,:221`）、重建 profile `device_profiles` 并 `row_version+1`（`:492-534`）、入队作业（`:229`）、审计（`:234`）；`202 {id, publication_job}` | 只写两张边界表 + `INSERT IGNORE` 默认 policy 行（`ensureDefaultFlowClassificationPolicy` `:465-470`）+ 审计；`201/200 {source_revision, runtime_state:"draft", publication_required:true}`；不写 binding、不入队 |
| 修订号 | profile `row_version`+`definition_digest` 每次保存递增（`:528-532`），钉进作业 payload（`flow_enrichment_jobs.go:30-67`） | `source_revision` = 规范化边界 JSON 的 sha256（`flow_worker_deployments.go:403-440`） |
| 建作业 | 保存/profile PUT/地址激活/服务启动自动（`flow_enrichment_publications.go:213,376`；`handlers_address_dimension.go:224`）；key `flow-enrichment:<row_version>:<snapshot>`（`flow_enrichment_jobs.go:64`） | 显式 `POST /api/v1/flow/deployments`（address.publish）→ `flow.worker.deployment.publish`（`:91-130`）；key **不含边界摘要**（`:115-116`） |
| 制品含设备边界 | 一个分类 JSON 含全部设备的 `source_prefixes`（CIDR+客户），与活动 WADS 配对，Ed25519 信封（`:608-781`） | 三类制品：WADS 按校验和复用、policy JSON、每设备边界 JSON；每 worker 签名清单（`:483-553`） |
| 目标 worker | 所有 profile 设备绑定 worker 的并集；唯一活跃 worker 自动绑定/修复；否则 `422 flow_worker_required`（`:795-886,1514`）；目标持久化在 `flow_enrichment_publication_targets`（`:762`）并在 list/object/ack 强制（`:1225,1301,1380-1388`） | 显式 `worker_ids`+`device_ids`（`:137-141`）；绑定只作推荐（`:191-206`） |
| worker 下载/验签/LKG/切换 | `RemoteVersionSync.SyncOnce`（`version_http.go:442-509`）：**先验签**（`:472`）→ 版本严格递增（`:477`）→ 对象 SHA（`:483-488`）→ `downloaded` ACK（`:489`）→ `VersionLoader.Install`（`version_loader.go:149-224`）解码校验、LKG 先落盘再原子切换（`:215-217`）→ `installed` ACK（`:223,236-250`） | `RemoteDeploymentSync` + `DeploymentLoader.Install`（`deployment_loader.go:72-158`）；v2 LKG 恢复 |
| ACK | `downloaded/installed/failed`（`:1452-1454`）存 `flow_enrichment_publication_acks`；`installed_at` 只由 worker POST 设置（`:1409-1411`）；worker 身份 = 路径 agent 的凭据（`:1439-1450`→`agents.go:611-626`） | `downloaded/verified/installed/failed`（0049）；ACK 绑定 `(generation, manifest_checksum)`（`:706-709`） |
| UI"已生效" | 边界卡只说 "Customer source ranges saved"（`flow-customer-boundaries.tsx:203`）、worker 选择必填（`:185-186,430`）；激活指南按 `state==="installed"` 计数（`flow-activation-guide.tsx:197-198,234-237`）；运营商查询门禁要求**每个**目标 worker 都有 `installed_at`（`flow_operator_query.go:146-181`） | 边界卡改为"草稿，需发布"；`flow-worker-deployments.tsx:186,369-371`"Only Installed confirms…" |
| 迁移 | `0048_flow_worker_targeted_publications.sql`：两表 + 唯一活跃 worker 的 `INSERT IGNORE` 回填；`ApplyMySQLSchema` 自动应用 | `0049`：四表，**无 v1→v2 回填** |
| 测试 | `TestFlowEnrichmentPublicationGinWorkerIntegration`（DSN 门控：202+队列目标、job→v1、自动绑定/修复、双 worker 无选择→无目标、非目标 worker 拿空列表与 404、编译对象含双栈 CIDR 与客户名、删除守卫、真实 `SyncOnce`→`installed`、损坏升级→`failed` 且 LKG 完好、离线 LKG 恢复）+ `TestResolveFlowOperatorQueryBinding`（DSN）+ 无 DSN 的路由/ACK 校验单测 | 同一集成测试被**改写为 v2 并删除上述断言**（diff） |

HEAD 的 fail-open 复核：未验证制品安装——无；服务端未收 worker ACK 就标 installed——无；**同步失败后 worker 静默沿用旧边界——有**（`runVersionSyncLoop` `main.go:714-719` 记日志继续；`EnrichmentVersionCatalog.Select` 取 `EffectiveFrom ≤ eventTime` 的最新版本，`version_catalog.go:215-231`，新 `effective_from` 之后的事件继续用旧分类且无标记）。RBAC：边界 CRUD 与绑定 PUT 在 `address.manage` 下触发发布，而手动发布路由要求 `address.publish`（`router.go:244-245`）——发布门禁被绕过。耦合：入队在保存事务内，**无活动 WADS 激活**（`sql.ErrNoRows`→412，`:1516`）或 ≥2 worker 时**任一**profile 设备无绑定（`flowEnrichmentTargets` 遍历全部 `DeviceProfiles`，`:795-863`→422）都会**回滚一次无关的边界保存**。

### 5.3 PERF-CUST6 的 [x]"定向发布生命周期"在 HEAD 基本成立（v1 配对协议），但

(i) 全部 MySQL 测试无 DSN 即 skip，本机无法执行；(ii) "多 worker 必须显式选择"只在 `flowEnrichmentTargets` 函数级测试，未经 HTTP 保存路径；(iii) 无"更新边界→第二次定向发布→worker 升级"用例、无"非目标 worker 的 ACK 被拒"用例；(iv) WIP 改写并删除了这些断言。

### 5.4 WIP v2 设计的正确性风险（≤8 条，◐）

1. **两个活写者、一个只认 v1 的查询门禁**：v1 自动发布仍由 profile PUT（`flow_enrichment_publications.go:376`）、地址激活（`handlers_address_dimension.go:224`）、服务启动（`:213`）触发；v2 部署另成一套；`resolveFlowOperatorQueryBinding` 只读 `flow_enrichment_publications`/targets/acks（`:74-85,146-159`），而 worker 把 `classification_version = v2 generation` 写进 ClickHouse（`enrich.go:400`；`flowch/native.go:494,536`）→ 运营商范围查询将 503 或静默排除 v2 分类的事实。
2. **共用 uint32 命名空间可卡死 worker**：v2 首次部署从 v1 max 播种（`:496-504`），之后 v1 发布可能复用同一号；v2 页为空时 worker 回落 v1（`version_dual_sync.go:58-66`）装了 v1 N，随后 v2 generation N 在 `matchInstalledDeployment` 永远失败（"generation is already installed with different content"，`deployment_loader.go:213-223`）——fail-closed 但无人被提示分配新 generation。
3. **幂等键不含内容、同分钟发布冲突**：key = snapshot+profile+workers+devices+effective_from（`:115-116`），CIDR 变了其余相同 → `ErrHashMismatch` 或映射到旧作业（`opjob.go:213-222`）；`uq_flow_worker_deployment_effective (worker_id, effective_from)`（0049 `:50`）使同一分钟第二次发布变成重复键错误并被 opjob 重试到耗尽。
4. **0049 无回填、无切换**：既有 v1 配对不会投影进 `flow_artifacts`/`flow_worker_deployments`，每个 worker 第一次 v2 都要手动发布；`CHECK … REGEXP` 依赖 MySQL ≥ 8.0.16。
5. **v2 LKG 恢复非全有或全无且仍用 `now` 验签**（§3.3 ③）。
6. **陈旧分类静默持续**：与 HEAD 相同的循环行为，desired-but-not-installed 只在服务端可见。
7. **RBAC/状态不一致**：v2 接受 `registered` worker 为目标（`:163,:488`）而 v1 要求 `active`；profile PUT 在 `address.manage` 下仍自动发布 v1。
8. **制品复用边界**：以更早生效时间重发未变的边界/policy 会报错而非新建制品（`:333-335,373-375`）；policy 的 `source_revision` 与旧 profile PUT 会修改的 `definition_digest` 是同一个值。

### 5.5 HEAD 下让 172.57.1.2 的七牛前缀真正到达 worker 需要的步骤（本人复核了 3/5）

1. 存在活动且已批准的 `flow/address` 激活（`dimension_snapshot_activations`），否则保存 412 回滚。
2. 恰好一个活跃 `flow_worker` agent，或 `PUT /api/v1/flow/worker-device-bindings/{device_id} {"worker_id"}`（address.manage；worker 变化本身也会入队发布）。
3. `POST /api/v1/flow/customer-bindings` 或 `PATCH …/{id}`（If-Match）→ `202 {publication_job}`；行已存在且没跑过作业时：`POST /api/v1/flow/enrichment-publications {"effective_from": 下一 UTC 分钟}`（address.publish）或重启 `watchdog-server`（启动自动入队）。
4. 轮询 `GET /api/v1/operation-jobs/{id}` 到 `succeeded`（`result_ref = flow-enrichment:<publication_id>`）。
5. worker 以 `-control-plane-url … -agent-token-file … -version-lkg-dir …` 启动且**不带** `-bootstrap-version-publication`（互斥，`main.go:518-521`），重启；`GET /api/v1/flow/enrichment-publications/{id}/acks` 出现 `installed`。`event_time ≥ effective_from` 的新事实得到新 `classification_version`；已入库事实需 FLOW-06B 历史重分类 generation。

---

## 6. 已落地修复的正确性复核

### 6.1 过滤下推（`a777ac83`）— 等价性成立，但测试没证明

`compileStorageV2Filters`（`query.go:692-727`）把 directions/categories/businesses/target/device/exporter/classification_versions 下推，`dimension_values/dimension_snapshot_ids/geo_versions` 及时间窗/typed filter 留在 UNION 后（`:491,:919-927`）。archive 分支用 `source.*`（`:702-707`→`:893`），raw 分支用 `toString(business_direction)`/`toString(category)`/`business`/`target_id`/`device_id`/`exporter_id`/`classification_version`（`:708-713`→`:914`）。
等价性：七个下推列全部是 `raw_rows` 的 GROUP BY 键（`:915-917`），先过滤与后过滤严格等价；archive 列为 `LowCardinality(String)`（`010:30-34`）由 rollup 从 raw 枚举名填充（`rollup.go:657-700`），raw 为 `Enum8`（`011:79,104`）经 `toString` 比较同一字符串集，`normalizeStrings` 限制取值（`query.go:303-311`）；`archive_latest`（`:874-880`）与 `covered_buckets`（`:981-984`）从 `_generation` 行计算、不受下推影响。**测试缺口**：无前后结果相等断言（见 §2.1 第 1–2 条）。

### 6.2 端点两遍算法（`786733c1`）— 候选集近似、未标注，且报表第二段绕过了它 ✅

- 候选集用 `topKWeighted({candidate_n})`（`query.go:1004`，`candidate_n = TopN×8`，`:43,:501-502`），即 ClickHouse 的 Filtered Space-Saving 重击者草图（近似；多线程/多 part 的中间状态合并进一步削弱保证）。第二遍对候选算**精确**桶值、其余并入版本化 `_other`（`:1036-1054`）。
- **可能出错的是成员而非数值**：真实 Top-N 端点若不在 ≤8×TopN 候选内，其全部流量进入 `_other`，位置被次级端点顶替；每桶守恒仍精确。混合版本窗口还有一个边界：第一遍按端点（忽略版本）排名、第二遍按 (端点, snapshot, geo, version) 序列排名（`:1022-1027,:1056-1059`）。
- API/UI **没有任何近似标记或退出开关**（`internal/flowquery`、`internal/server`、`frontend/src` grep 无 approximate/topK）。审计 §10 / §4 Phase 1.4 明确"保持 `uniqExact`，近似须产品批准"——`uniqExact` 确实未动，但排名近似同样是未经批准的近似。
- **报表第二段回退到精确全基数聚合**：`flow_report_endpoints.go:100` 对每个方向把 Top-N 地址塞进 `filters.DimensionValues`；而 `endpointCandidateQuery` 要求 `len(residualFilters)==0`（`query.go:499`），`DimensionValues` 是 residual → 该阶段编译成 §8.3 判定为内存故障根源的精确 raw `GROUP BY`（`query.go:895-927`），预算 250M/16GiB/30s；能否避免全基数聚合完全取决于 ClickHouse 优化器是否把 `dimension_value IN (…)` 穿透 UNION ALL+GROUP BY 下推（代码不可判定）。`dimension_value` 是 raw 分组键且 API 值已规范化为存储形式（`ip_dimension.go`），本可以直接下推。
- 每页查询数：2（in/out 总量）+1（Top-N）+ 每方向 ×2 [1 条逐端点聚合 + ⌈10/⌊100/TopN⌋⌉ 条分类分块] + 1（business）= **TopN=20 时 10 条、TopN=100 时 26 条**，每条分块的地址约束是最多 100 项 `col = toIPv6()` 的 OR 链（`filter.go:453-463`）。

### 6.3 预算档由请求内容决定，且无端到端 deadline ✅

`hasIdentityScope` = 任一 device/target/exporter 过滤非空（`query.go:46-48`）；`rawScanBudgets` 250M/16GiB，端点/境外 500M/32GiB，执行时间 30s vs 15s（`:50-66`）。授权只检查主体是否可看这些设备，`device.viewAll` 直接 `return true`（`flow_query.go:91-94`）；单个过滤最多 2,048 个 ID（`query.go:28`）且无基数上限——列出全部设备的查询在语义上就是全局查询，却享受 5–8× 预算。hub 无 HTTP 超时（`server.go:316` `engine.Run`），报表最多 26 条 30s 查询串行跑在 8 连接池上。

### 6.4 境外全 raw 单扫（`045d7b3a`）— 三处行为变化未记录

`endpoint_consistent` 常量 `1`（`overseas.go:392`，混合路径 `:641` 仍真实计算）→ runner 的一致性拒绝（`overseas_runner.go:444-447`）在全 raw 路径永远不触发；按 family 的 `observed_local_hosts`/本地计数改按**远端** IP 族分组（`:346-347,:354-357`，旧 `local_endpoints` 按本地 IP 族 `:518`），v4↔v6 混合流会不同；全 raw 计划先返回整个 geo 目录再在 Go 侧做 Top-N，`EstimatedRows`（`:110`）对长窗口低估。另：`overseasScanBudgets` 注释称 `max_rows_to_read` 按 ARRAY JOIN 展开后计数（`:213-218`），与该设置的文档语义（存储读出行）不符，代码无法证实。

### 6.5 静态倍率 `estimated_bytes_scale_ppm`（`045d7b3a`）

> 待 B 路报告补充链路细节。已复核：019 对 `flow_records`/`flow_reclassified_records` 加 `UInt32 DEFAULT 1000000`（旧行恒等）；`plan.go:135-139` 限定范围并禁止 `pre_scaled`；`decode_adapter.go:212-216` 用 `bits.Mul64` 防溢出并置 `QualityEstimateCalibrated`（`:187-188`）。**未闭合**：`Ready()` 未校验该列（§3）。与文档自身规则的张力：perf-audit §9 原则"不得用未说明的倍率…制造看起来一致"，而 PERF-SFC1 已按 4096 切换后的 2.6% 差额把 1,027,000 ppm 发到生产 plan v4，差额分类（counter/方向/序列 gap/端口映射）那一项仍是 [ ]。

### 6.6 测试与构建（当前工作树）

`go build ./...` OK；`go vet ./internal/flowquery/ ./internal/server/` OK；`go test ./internal/flowquery/` 92 个顶层测试全过（含子测试 251）；`go test ./internal/server/ -run Flow` 70 过 / 14 skip（MySQL/CH 集成）；PERF-Q1/Q1B 的两个"真实 ClickHouse 集成"用例在无 DSN 下 **skip**，本机无法执行。

---

## 7. 对 2026-09-20"复核裁决"的再裁决

### 7.1 QinQ："证据不足，需降级" — **不同意（机制可在代码里判定），同时修正原审查的机制描述** ✅

原审查写的"回落到 GoFlow2"在机制上不准确：`parseSampledPacket`（`sflow_v5_fast.go:480-515`）对外层 EtherType 非 0x0800/0x86dd（含 0x8100、0x88a8）走 `default: return false`（`:512-513`）；`:391-400` 收到 `false` 后**并不**返回 `errSFlowFallback`，而是留在快路径框架里内联调用 goflow2 `ParseSampledHeader`（`:397`）再 `copyPacketFields`（`:466-471`）——`errSFlowFallback` 只在未知样本格式（`:161`）和 ExtendedGateway（`:455`）产生。之后的链条完全由代码决定：goflow2 `producer_sf.go:26-44` → `ParsePacket`（`producer_packet.go:405-470`）→ `ParseEthernet`（`:472-500`）→ `innerNextParserEtype`（`:277-300`）：`0x8100 → parser8021Q`（读内层 EtherType继续，`:503-527`），**无 `0x88a8` 分支 → `parserNone`**，其 `Parser` 为 nil（`:29-31`），循环条件 `nextParser.Parser != nil`（`:421`）无错误退出，`SrcAddr/DstAddr` 保持 nil；仓库未注册自定义 EtherType（grep `RegisterEtype` 空，`DefaultEnvironment` `producer_sf.go:36`）；`copyPacketFields` 拷 nil 地址 → worker `decode_adapter.go:161-168` `canonicalAddress16(nil)` 失败 → `Map` 对整个 batch 包装 `ErrDecodedFlowInvalid`（`:116-119`）→ `processor.go:151-153` → **整个 datagram**（含未打 tag 的样本）`mapping_rejected`。
**代码已证明**：单层 0x8100 正确解码（只是更慢）；外层 0x88a8 确定性地丢整个 datagram。**运行时才知道**：exporter 是否/多频繁把 0x88a8 外层 tag 放进采样头（暴露面）、同一 datagram 里 tag/非 tag 样本的混合比例（放大倍数）。pcap 测试补的是暴露面度量，不是机制。复核的表述"会离开快路径…取决于 GoFlow2 fallback"同样不准确。正确的严重度写法是"**已证明的丢失机制，暴露面未知**"。更进一步：同一机制对**任何非 IP 采样帧**（ARP、LLDP、STP…）在快路径与 GoFlow2 慢路径上都成立——IX 二层网里这是稳态事件，与 QinQ 无关，而 rejection observer 为 nil（M8），只剩计数。

### 7.2 `TryProduce` 满时丢包："原修复建议错误" — **同意结论，补充现状** 

产品目标是不主动丢弃，正确方向是有界内存 + 明确背压/拥塞状态。但要如实记录：当前代码在 `MaxBufferedRecords` 满时 `Produce(context.Background())` 阻塞接收 goroutine（`producer.go:127`），内核 socket 队列随即溢出——**丢包已经在发生，只是发生在内核、由 `SO_RXQ_OVFL` 计数**（且在 Produce 阻塞期间该计数不可见，M9）。"不丢"在今天并不成立；修复必须同时给出应用层背压状态与有界内存（M1），否则只是把丢包从可见变成不可见。

### 7.3 `UnknownTopicRetries(-1)`："废止" — **同意**。无限重试制造"进程活着但不工作"。启动 Metadata 校验 + fail-fast + 运行期健康状态是正确的（H3 仍未做）。

### 7.4 `SO_RCVBUFFORCE`/`CAP_NET_ADMIN`："不应默认" — **同意**。首选 sysctl + 启动回读 + readiness/指标；能力提升作为受控备选。H1 仍未做。

### 7.5 `MaxConns ≥ partitions`："过度简化" — **同意**。连接数应由 CH 并发容量、块延迟、内存与分区流水线共同决定；原文的意图是"不要让 `Acquire` 排队计入 2 分钟操作超时而不可见"，这一点仍成立（CH1）。

### 7.6 loopback 关闭 LZ4："未经证明" — **同意**，须以 CPU/拷贝/CH 解压基准决定；原文已标注为估算。

### 7.7 "性能数字是推理值" — **同意**；原文均标"估算，未实测"。

### 7.8 复核新增的判断中需要再校准的两条

- "`estimated_bytes_scale_ppm` 部分修复…`Ready()` 仍只核验 5 列" — **成立**（§3）；补充：回执携带的是校准后字节且无 ppm 列（§3.2），这是复核未提到的第二个未闭合点。
- "`interface_reconciliation.go` 未交付" — **成立**（仍未跟踪）；其代码本身的问题见 §4。

---

## 8. 生产操作（PERF-OPS1）与仓库自定规则的冲突 ◐（规则引用本人已核；生产后果为代码推演）

PERF-OPS1 记录了两项生产操作：(a) 将 Kafka partition 11 的 low watermark 推进到 `40262989`；(b) 删除 `registry_version < 4` 的 `flow_records` 行及"对应旧 receipts"（`flow_records` 14,065,259 → 1,086,644 行）。

**生效规则**：perf-audit §4 Phase 2.3（receipts 只能按写入月由 operation job 在全部事件日越过对账/raw-delete/restore/备份/审计水位后 DROP）、§4 门禁"生命周期：archive/delete/reclass/receipt 删除均由水位与 operation job 驱动，禁止静态期限绕过证据"、§10 禁止清单；`flow-storage-v2-change-plan.md` §2.3（每条可提交消息恰好一条回执）、§3（扫描器必须枚举 `[next, committed)` 的每个 offset，双缺失不得变成不可见空洞）、§4（"删除使用显式分区操作并写审计"；"receipt 按写入月保留，独立于事实删除"）、§8 V2-D（raw 日删除需审批、备份证据、tombstone ACK，DDL 为按 UTC 日 `DROP PARTITION`）。
**仓库中存在的删除路径**只有 `ALTER TABLE flow_records DROP PARTITION <yyyymmdd>`（`flowch/raw_delete.go:30`）与 `flow_aggregate_1h DROP PARTITION <yyyymm>`（`:57`），由 `flowlifecycle/raw_delete.go:115 ScheduleRawDayDelete` 驱动并落 MySQL `flow_deletion_receipts`（0033）、审批与备份证据。grep **不存在**按 `registry_version` 删事实、删 receipts 或推进 Kafka 水位的任何代码路径；两项操作都没有留下仓库可见的记录（无 deletion receipt、无 operation job、无分区状态迁移、无回执侧标记），唯一痕迹是那一行文档。

读 `flow_ingest_receipts`/对账代码的人会看到的具体后果：
1. **回执/事实守恒**：被删回执的每个 offset，扫描器一律报 `missing_receipt`（`reconciliation_scanner.go:152-161`，"双缺失"按设计可见）；回执在而事实被删 → `missing_records`（`:166-167`）；部分删除 → `count_mismatch/counter_mismatch`。`flowlifecycle/reconciliation.go:165-190` 在第一个坏 offset 冻结 `verifiedNext` 并持久化 `mismatchCount>0`，`delete_readiness.go:143-146` 随即阻塞 raw 日删除。若 reconciled 水位在删除前已越过这些 offset，则永远不会被报告——证据就此消失。
2. **partition 11 低水位 → 40262989**：其下 offset 不可再消费，任何 `missing_records` 无法用重放修复；`flow_reconciliation_watermarks` 中低于它的 bootstrap/committed 指针指向不可读 offset（`reconciliation.go:126` 的 `ErrOffsetRegression/ErrBootstrapRequired` 处理针对回退而非保留截断，行为未验证）。
3. **日→offset 覆盖**：`DayOffsetCoverage`（`retention_coverage.go:47-54`）只从 `record_count>0` 的存活回执推 span；被清空的日 span 缩小或消失；`delete_readiness.go:120-121` 仅在 `Source.RecordCount>0 && len(spans)==0` 时报 `kafka_coverage_missing`——带外清空的日是零记录零 span，该代码不视为阻塞。
4. **raw 不可变 / 分区粒度**：行按谓词在分区内被删除（机制未记录——mutation 或 lightweight delete，作用在带 `deduplicate_merge_projection_mode='rebuild'` 与 `flow_ingest_audit_v2` 投影的表上），2026-09-17..19 分区部分填充；此前抓取的 `flow_retention_partition_states` 计数已陈旧。
5. **归档覆盖**：§8.1 时 `flow_aggregate_1h` 为 0 行，故未破坏既有归档守恒；未来对这些日的归档将以缩减后的 raw 守恒，静默认可删除。
6. **计费历史**：flow 计费在查询时读 raw（`billing.go:247-285`），跨这些日的账期失去全部 v4 前 flow 字节而 SNMP 完好，`common_complete_5m_intersection` 覆盖率坍缩且 CH 内无任何标记说明原因。
7. **counter 回执**：回执没有 `registry_version`（011/018 DDL），"对应旧 receipts"只能按 offset/时间挑选；受影响 offset 上保留在 `sflow_interface_counters` 的 counter 行如今没有回执，扫描器（从不读 counters）报 `missing_receipt`。
8. **审计链**：`flow_deletion_receipts`、operation job、审批、备份证据都不引用这两项操作。未跟踪的 ix-scale 复核 A 写 `flow_records ≈ 57.73M` 与 PERF-OPS1 的删后 1,086,644 互不一致，均不可从仓库验证。

结论：这两项操作与三份文档自己在同一天写下的规则直接冲突（"不得以未说明方式制造看起来一致"、"删除必须由 operation job 与水位驱动并留证据"）。技术上它们把"重复率/守恒/覆盖率"这些评价指标建立在已经被手工修剪过的数据上——之后任何"24h 验收通过"都不能反推到修剪前的规模。

---

## 9. 真正未关闭的项（按门禁排序）

### 实施进度（2026-09-20，本会话）

A 组止损与相邻项已逐个提交（`main`，`Co-Authored-By: Claude Opus 4.8`）：

| 项 | 状态 | 提交 | 备注 |
|---|---|---|---|
| A.1 counter-only 回执 | ✅ 已修 | `bb7490a4` | scanner + 内存比较器 + 校验器；加测试 |
| A.2 `Ready()` 加 scale_ppm | ✅ 已修 | `44465c17` | worker records 5→6、hub 重分类 6→7；未跑 019 现在 readiness 失败而非崩溃循环 |
| A.3 端点 IP 维度值下推 | ✅ 已修 | `37e6333f` | 冗余谓词，post-UNION residual 作 backstop；**读取量收益仍需生产 EXPLAIN** |
| A.4 Top-N 近似披露 | ✅ 后端已修 | `7acad9d6` | `approximate` 进 panel meta；前端徽章待路由重写；候选漏掉测试需 DSN |
| A.5 报表端到端 deadline | ◐ 部分 | `a877277d` | 120s 请求上限已加；hub Read/Write 超时 + 配置化预算档在 `server.go`（并行会话占用）未做 |
| A.6 境外语义记录 | ✅ 已记录 | `59c614d8` | `endpoint_consistent`/family keying 说明为有意；非缺陷 |
| A.7-0.1 模板守卫 | ✅ 已修 | `5e822705` | 零进展模板拒绝，Critical 关闭 |
| A.7-0.2 数据形状错误→回执 | ◐ 部分 | `02177a20` | 永久 enrich 错误改回执+推进（future skew/>限/畸形）；`ErrBindingUnavailable` 的 ODID 永久态与真正的分区 pause 仍待 resolver 拆分 |
| A.7-0.3 topic fail-fast | ✅ 已修 | `bbb3451e` | `VerifyTopic`（kmsg，无新依赖） |
| A.7-0.4 `SO_RCVBUF` 回读 | ✅ 已修 | `a4efa5cc` | + `deploy/sysctl.d/90-watchdog-flow.conf` |
| A.7-0.5 `Send` 计数恒等式 | ✅ 已修 | `6d606807` | 四处早返回计入 errors |
| A.7-0.6 PLAIN 需 TLS | ✅ 已修 | `17a4042c` | |
| A.7-0.8 默认限速 observer | ✅ 已修 | `5cffd9dd` | nil observer 装默认（不改 worker main.go） |
| A.7-0.9 systemd 加固 | ✅ 已修 | `63411d8f` | `MemoryMax`（worker 2G/collector 1G）/`LimitNOFILE`/`TimeoutStopSec`/`Wants=` |
| A.8 receipts 加 scale_ppm | ⛔ 撤销（非缺陷） | — | scanner 比对回执与事实均为**校准后**值（like-to-like），无 raw×rate 对比；`§3.2` 的"漂移"判断过度，不新增迁移/`receiptSchemaVersion` bump |

B/C/D/E 组（发布闭环、幂等一致性、性能、文档）均**未做**：或受 `server.go`/worker `main.go` 并行占用阻塞，或需真实 Kafka/CH/registry 与压测门禁（见 §7 复核、§8）。所有 ✅ 项 build/vet/`-race`/单测通过；标"生产 EXPLAIN/DSN/压测"者仅完成正确性，量化收益仍需线上验证。

---

**A. 先止损（都是 <50 行、无 schema 变更，可各自独立提交）**
1. 对账扫描器读 `counter_record_count`，counter-only 回执不再判 `missing_records`（§4.1）；加 scanner+counters 测试。
2. `Ready()`（worker 与 hub 重分类）加 `estimated_bytes_scale_ppm`；`native_test.go` 夹具同步；记录"0dca0d2b 起需先跑 018/019"的部署顺序（§3.2/§4.3）。
3. 端点报表第二段：把 `dimension_value IN (…)` 下推进 raw 分支（它是分组键、值已规范化），或给该阶段单独走候选路径（§6.2）。
4. 端点 Top-N 近似性：要么 API 返回 `approximate: true` 并在 UI 标注，要么按文档门禁走产品批准；补候选漏掉/多版本测试。
5. 预算档改由服务端配置（interactive/detail/export）决定，不由 `device_ids` 是否非空决定；给报表请求端到端 deadline；hub `ReadTimeout/WriteTimeout`（§6.3）。
6. 境外全 raw：恢复 `endpoint_consistent` 计算、按本地地址族分组或明确记录语义变化（§6.4）。
7. code-review Phase 0 的 0.1（模板守卫：`AddTemplate` 前拒绝 `GetTemplateSize==0` 且无变长字段的模板；超时不是替代）、0.2（数据形状错误 → 回执；`fetches.Errors()` 可重试的继续）、0.3（topic Metadata fail-fast）、0.4（`SO_RCVBUF` 回读 + `sysctl.d`）、0.5（`Send` 恒等式）、0.6、0.8（observer 日志）、0.9（unit `MemoryMax`/`LimitNOFILE`/`TimeoutStopSec`）。
8. `flow_ingest_receipts` 加 `estimated_bytes_scale_ppm`（或同时存未校准值），否则回执对账口径已漂移（§3.2）。

**B. P0 发布闭环（决定分类正确性）**
9. 先做决定：HEAD 的 v1 自动定向发布 **部署并启用 worker 远程模式**（§5.5 五步）可以立刻关闭 53% `transit`；WIP v2 必须先解决"两个活写者"（v1 自动发布路径全部停用或桥接到 v2）、共用 generation 命名空间、幂等键含内容、0049 回填、`registered` worker 目标、LKG 按签发时间验签，再谈提交（§5.4）。
10. 无论 v1/v2：worker 同步失败必须有指标 + readiness 降级，不能只记日志（§5.2）。

**C. P1 幂等/一致性（与 perf-audit Phase 2 同批）**
11. 确定性 offset 桶切块 + `non_replicated_deduplication_window`（按峰值块率×最长重试定容）；之后才谈去 `FINAL`（§4 R2）。
12. 重放 floor 以下只解码不 `handleGroup`；回执 generation 与持久化行同源；模板重放默认值给出决策（M6 vs H1）。
13. LKG 按 `SignedAt` 验签（v1 与 WIP v2 一并）；`effective_from ≥ now + 激活延迟`；真正的按分区暂停替代进程退出（C1）。
14. R7.1 对账加 `event_time` 边界；C1 VPN 覆盖率改从回执/已关闭 raw 分钟计算。
15. 把 PERF-OPS1 两项操作补记为审计事件（哪些日/offset/表、机制、行数前后），并让删除就绪度/覆盖率代码把"带外清空的日"识别为阻塞而非 0/0（§8）。

**D. 性能（correctness gates 之后）**
16. R6.1/HP1 每分区流水线、CH3/CH4 列构建器复用与 receipts 合批、`EnrichBatchInto`、`recvmmsg`——每项带前后基准与回退开关；先把每块第三次 INSERT 对 part 率的影响量出来（§4 R6.2）。
17. IX P2 的 `flow_interface_traffic_5m` 只有在 §4.4 列出的对账读者缺陷（JOIN 形状、序列作用域、gap、边界对称）修完并通过双读对拍后才有意义。

**E. 文档**
18. 三份文档把 §2.1 列出的 8 处 [x] 改回真实状态；perf-audit §8 基线与 §9 PERF-OPS1 的数据规模需分别标注采样时刻；把"schema 测试禁止 TTL"改为准确表述（§4.5）；把 QinQ 条目改为"已证明机制、暴露面待测"（§7.1）。

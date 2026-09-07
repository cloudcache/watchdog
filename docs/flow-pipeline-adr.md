# Flow 数据面收敛 ADR：Akvorado RawFlow + GoFlow2

> **Storage V2 superseding amendment**：数据身份、回执对账、TTL/downsample、明细 cursor 和去热路径 hash 的唯一生效契约见 [flow-storage-v2-change-plan.md](flow-storage-v2-change-plan.md)。本文中 30 天 TTL、实时 1m/1h rollup、hash 派生 ingest id 的旧描述仅保留为历史背景。

状态：**Accepted，核心传输与解码已实施**
生效日期：2026-09-05
关系：本文记录数据面决策；`flow-module-design.md` 和 `flow-module-tasklist.md` 已同步为同一架构，不再保留冲突方案。

> **修订(2026-09)——分类模型**:分类从"ingest 时用 flowdimension 把六维烘进 `flow_records`"改为"`flow_records` 只存原始字段;分类改成一份**版本化的 ClickHouse `IP_TRIE` 字典**,写入不算分类,rollup 和查询都 `dictGet` 现导;定义改了 = 换字典版本 + 重跑受影响的聚合(重分类 = 重跑 rollup),不再单独建重新分类子系统。地址组本阶段由**管理员基于已就绪 geo 规划**(不开放用户自定义地址段)。详见 [flow-address-query-plan.md](flow-address-query-plan.md)。本文下述"地址库/六维在 Kafka 后烘进 CH base"的段落(§1 流水线图 dimensions、§4 维度与事实提交)按该方案修订;**传输 / Kafka / GoFlow2 解码 / RawFlow 契约决策不变**。

> **修订(2026-09)——保留与降精度(downsample)模型**：原始/全精度是在线主查询面，时长由 tenant policy 决定，不固化 30 天或 1 年。整个 UTC 日越过 `max(raw_retention, late_arrival_window)` 后才生成 1h archive；1m 查询继续读 raw，不再持续物化 1m。标准 1h 查询按已核验的连续 archive boundary 读取 `archive + raw` 互斥区间并在 union 后统一 TopN；联合维度在专用异步索引发布前仍读 raw。事实与 receipt 使用 Kafka 自然坐标，详情见 Storage V2 设计。
>
> **不变**:传输 / Kafka / GoFlow2 解码 / RawFlow 契约 / 分类字典(IP_TRIE `dictGet`)模型。

## 1. 决策

数据面固定为两个可独立扩容的进程角色：

```text
network device
  -> flow-collect: UDP receive + source admission + RawFlow encode
  -> Kafka watchdog.flow.raw-v1
  -> flow-worker: GoFlow2 decode + sampling normalization + dimensions
  -> ClickHouse enriched base + rebuildable rollups
```

采用 Akvorado 的 raw-flow-before-decode 分层和 Kafka producer/consumer 生命周期；Kafka 客户端使用 Akvorado 当前采用的 `franz-go`。**NetFlow v5 与 sFlow v5 已改为自研定长/TLV 快解码器**(零反射、零分配、对 GoFlow2 逐字段差分验证;sFlow 抓包头复用 GoFlow2 已加固的 `ParseSampledHeader`,ExtendedGateway/未知记录格式回落 GoFlow2);GoFlow2 仅作为 **NetFlow v9 / IPFIX** 解码器及 sFlow 未覆盖记录的回落,并继续提供 template/sampling store。详见 [flow-decode-fastpath.md](flow-decode-fastpath.md)。地址库、ASN/Geo、六维、业务和 VPN 规则全部在 Kafka 后异步执行。

`flow-collect` 不连接 MySQL、ClickHouse、VictoriaMetrics，不维护本地 raw WAL，不做协议解码、采样放大、维度查询、分钟聚合或跨节点模板状态恢复。Kafka 是唯一排队和短期重放边界。

## 2. RawFlow 契约

topic base 为 `watchdog.flow.raw`，schema v1 的物理 topic 为 `watchdog.flow.raw-v1`。value 使用 protobuf `RawFlow`，保留 Akvorado 字段并增加 watchdog 采集身份：

| 字段 | 语义 |
|---|---|
| `time_received` | UDP 接收 Unix 秒 |
| `payload` | 完整 UDP datagram |
| `source_address/source_port` | exporter 源地址；端口仅诊断，不参与身份 |
| `decoder` | `NETFLOW` 或 `SFLOW` |
| `timestamp_source/decapsulation_protocol/rate_limit` | 与 Akvorado 输入语义一致 |
| `collector_id/listener_id` | 采集实例和监听配置身份，解决不同网络重复私网地址 |
| `registry_version` | 接收时配置版本；下游按此版本解析 exporter/tenant/target binding |

Kafka key 固定为 `collector_id || 0x00 || exporter_ip`，不含 UDP 源端口。同一 collector/exporter 的模板和数据报进入同一 partition，并保持 Kafka partition 内顺序。生产 topic 不原地扩 partition；需要改变 partition 数时创建新 schema/topic 版本并滚动切换，否则 key 到 partition 的映射变化会产生 NetFlow/IPFIX 模板冷启动窗口。

## 3. Kafka 行为

### Producer

- 使用 `franz-go` 异步 `Produce`、有界 `MaxBufferedRecords`、批压缩和 Akvorado 的 `UniformBytesPartitioner(..., keys=true, ...)`；
- 保留客户端默认幂等 producer 和 broker 重试，不在外层实现第二套 retry/WAL/ACK 状态机；
- Kafka 拥塞时 `Produce` 对有界队列形成背压，最终由 UDP socket drop 指标暴露容量不足；UDP 不承诺绝对零丢失；
- completion callback 无论成功或最终失败都释放 protobuf/payload buffer；失败计入低基数 counter，并进入 collector degraded 状态；
- Receiver context 只控制转交前准入；一旦记录进入 franz-go buffer，其 delivery context 归 producer 所有，不能随 Receiver 停止而取消；
- 正常关闭先停止 UDP Receiver，再在有界期限内 `Flush`，最后关闭 client；超时作为明确未确认区间记录。

### Consumer

- 使用 consumer group、`BlockRebalanceOnPoll` 和 marked offset；
- 只有 handler 完成 GoFlow2 解码、维度处理且 ClickHouse base batch 成功后，才标记对应 record offset；
- rebalance/shutdown 提交已标记 offset；未完成 record 重放，ClickHouse 以 topic/partition/offset 派生的 ingest id 去重；
- protobuf 损坏或永久不支持的 datagram 计低基数错误后跳过；可选诊断只保留脱敏摘要和有界样本，不新增 DLQ/attempt 状态机；
- NetFlow/IPFIX 缺模板不能阻塞同 partition 等待未来模板，否则未来模板永远无法被消费；该 datagram 计入 `template_missing` 并跳过，收到后续模板后自动恢复；
- 缺失 `registry_version` 属于配置发布故障，暂停 partition 并告警，不用最新配置猜测历史归属。

不建设 collect-state、quality-checkpoint、attempt、quarantine、state tombstone 等 Kafka topic。运行质量由 sequence/sample-pool/drop 指标和 CH/Kafka 对账体现，不把诊断状态提升为另一套业务事实。

## 4. 解码、维度与事实提交

每个 consumer partition 由一个有序 worker 处理，worker 内持有 GoFlow2 template/sampling store。sFlow/NetFlow 计数语义、采样率和原始字节/包计数沿用 `flow-direction-requirements.md`；采样归一只做一次。

partition 接管/进程重启会丢失内存模板，因此 consumer 从原 committed offset 向前回放一个有界 record 窗口。回放数据照常走 deterministic CH 幂等路径，但低于原水位的 record 不得提交，避免 group offset 倒退。窗口必须覆盖 exporter 最大模板刷新间隔对应的单 partition record 数；模板周期刷新是 exporter 开通前置条件。

本地真实 Kafka 门禁已经验证 v9/IPFIX 冷 worker 的 `committed-next-offset 4 → durable failure 后仍为 4 → 下一冷 worker 恢复后为 6`；新 data 本身不带模板，只有 assignment 回放成功才能被解码。该证据只关闭 handler failure、worker restart、模板重建和 offset 收敛子场景，broker restart、实际进程强杀、存活成员间 rebalance/lost partition 与 lag 恢复仍按任务清单单独验收。

下游从验签后的 immutable catalog 按 `collector_id + registry_version` 精确加载 collector/exporter binding，不回退到最新 revision；过期 revision 只用于解释已进入 Kafka 的历史记录。地址/Geo/分类 snapshot 按事件时间选择。ClickHouse enriched base 同时保存 raw endpoint/接口/ASN/计数和分类版本，派生统计可以从 base 重建。分类版本短暂不可用时 Kafka lag 承接，不允许 flow-collect 同步查询或丢弃已进入 Kafka 的原始报文。

## 5. 容量和可观测性

容量以 `datagrams/s`、`decoded records/s` 和平均 datagram records 数为主，不直接用链路 Gbps 代替处理量。至少记录：

- UDP received、kernel/socket drops、oversize/invalid source；
- Kafka buffered records/bytes、produce success/error、produce latency、consumer lag；
- raw decode success/error、template missing、records/datagram、sampling mode；
- dimension snapshot missing、ClickHouse batch success/error/latency；
- 进程 CPU、RSS、GC、goroutine 和每核 datagrams/records 吞吐。

P1 容量验收仍覆盖批准峰值的 2 倍持续 30 分钟、3 倍突发 5 分钟和 72 小时 soak；报告必须同时给出 UDP drop、Kafka lag、CPU/RSS 和 CH 可见总量。RawFlow protobuf 编码不是主要瓶颈，但必须保留独立 benchmark 防止回归。

## 6. 故障和数据生命周期

| 故障 | 行为 | 恢复 |
|---|---|---|
| Kafka broker/网络中断 | franz-go 在有界内存中重试，队列满后接收链背压并由 socket drop 暴露 | broker 恢复后自动发送；记录影响时间窗 |
| flow-collect 崩溃 | 尚未进入 Kafka 的 UDP 报文可能丢失 | exporter sequence/sample-pool 检出；进程重启，无 WAL replay |
| flow-worker 崩溃 | 未标记 offset 重放 | consumer group 接管，CH ingest id 去重 |
| 模板缺失/重平衡 | 数据报计数并跳过，不阻塞未来模板 | 后续模板到达后恢复 |
| dimension/CH 故障 | partition 不标记，Kafka lag 增长 | 依赖恢复后重放 |

Kafka raw retention 覆盖约定的最大 worker/CH 故障窗口；ClickHouse 原始事实默认无 TTL，由 Storage V2 policy 和显式 operation job 管理。原始/归档删除在 Kafka committed-offset 覆盖和销毁审计门禁完成前保持关闭。Kafka 不是长期分析库，受控诊断也不是业务事实源。

## 7. 迁移边界

旧 `internal/flowcollect` 共 68 个文件、20,420 行，因仓库内无运行时引用，已于 2026-09-05 删除。新传输实现位于 `internal/flowstream`，签名 plan 位于 `internal/flowplan`，轻量运行入口为 `cmd/watchdog-flow-collect`；不得重新引入旧 WAL、collect-state、attempt 或 state-cleanup API。worker/GoFlow2 和真实 Kafka 性能验收仍按 tasklist 推进，不能因入口已存在就宣称链路完成。

迁移顺序：

1. raw envelope、producer/consumer、GoFlow2 worker 单元和 race 测试；
2. 使用脱敏 pcap/UDP fixture 验证四协议和采样语义；
3. 使用固定脱敏 corpus、旧版本 golden 输出和守恒指标比较 datagram、record、raw bytes/packets、采样后 bytes/packets 和六维结果；不恢复或继续维护旧源码包；
4. 目标硬件压测 Kafka 中断、worker rebalance、CH 中断和 soak；
5. 切换 UDP VIP/设备 exporter 目标，观察一个 retention 窗口；
6. 确认历史部署不存在旧 topic/WAL 后，按删除清单移除遗留配置与运行制品。

不能在旁路验收前原地重写旧主链，也不能让新旧 consumer 同时写同一 CH fact identity。

## 8. 来源与许可证门

`internal/flowstream` 的 RawFlow 和 Kafka 生命周期来自 Akvorado，派生文件保留 Free Mobile 版权与 `SPDX-License-Identifier: AGPL-3.0-only`；`franz-go` 和 GoFlow2 保留各自许可证。按项目决定，测试阶段允许 MIT 与 AGPL 文件并存；对外发布前必须完成根许可证、NOTICE、源码提供入口、依赖清单和制品声明切换。许可证门不改变数据面测试标准。

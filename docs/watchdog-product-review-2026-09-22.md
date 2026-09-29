# Watchdog 产品设计与实现审查 — Flow · 账单 · RBAC/用户

> **复核状态（2026-09-28）**：逐条对照当前代码复核——已修复 5 · 部分修复 4 · 未修复 59 · 作废 1（作废 = 被后续设计决策取代，如全局共享 token、不设审批门）。逐条状态与证据见文末「复核状态（2026-09-28）」。

日期:2026-09-22(同日晚于 `watchdog-storage-lifecycle-review-2026-09-22.md`,本文引用而不重复其内容)
目标:需求一致性、完整性、准确性;核心瓶颈(collector / worker / 聚合与查询 / 存储分层 / 生命周期)的效率与**唯一性**(exactly-once / 去重 / 对账基线)。
证据:三份代码级 deep-dive(flow 数据面、billing、RBAC)+ 生产 .18 live 实测(内存受限查询)。所有断言附 `file:line` 或 live 数字。

---

## 二次复审修订（v13 发布后）

本文包含 2026-09-22 初始事故现场数据。二次复审把“历史根因”“当前生产状态”“本轮待发布代码”分开，避免已修复问题继续占据 P1 队列：

| 项 | 初始结论 | 二次复审状态 |
|---|---|---|
| 24h 查询 | endpoint 两遍 raw，128–170s/资源超限 | **v13 已修复并生产验证**：aggregate 连续前缀 + raw 尾段，同形作业 2.434s、attempt 1 |
| 聚合基数 | 约 700K rows/hour，误以为主要由 IP top-1000 导致 | **已修复并验证**：实测 96.5% 来自 remote_port；top-256 + `_other` 后新代约 19.8K rows/hour，约再缩 22× |
| 聚合速度 | 单分钟重复扫描整小时；小时 fallback raw EAV | **v12/v13 已修复**：封闭小时一次 raw→1m，1m→1h 实测 4.423s/361MiB，禁止非空小时 fallback raw |
| marker 完整性 | data 与 marker 同一流式 INSERT，存在先 marker 后失败窗口（U5） | **本轮代码已修复、待发布**：data 成功后独立 marker INSERT；失败注入测试覆盖两阶段门禁 |
| 自动报表物理层选择 | 文档未识别；自动规划可能选 1m，但 1m 连续覆盖只有配置的 6h，较早的短窗口也会踩空 | **本轮代码已修复、待发布**：计划起点早于 scheduler 实际 1m coverage 起点时选 1h；边界来自 `minute_lookback`、`seal_delay` 和封桶规则，显式细粒度查询不变 |
| 冷生命周期 | 无 published policy | **仍未完成**：hot tier 只加速读取，不授权 raw 删除；Phase 3 仍是当前存储 P1 |

因此，本文下文出现的“全部查询扫 raw”“700K/hour”“每个方向各发一条查询”均是**初始快照**，若与本节和存储 review §9 冲突，以较新的生产证据为准。仍未解决的最高风险是 Kafka incarnation、摄入去重/重放语义、worker 全进程退出、cold lifecycle、RBAC 越权与账单产品定义。

---

## 0. TL;DR —— 按优先级

| 级别 | 发现 | 域 |
|---|---|---|
| **P1** | **Kafka incarnation 复用污染了 receipts 键空间。** worker 自己的 flag 文档写着"never reuse after topic recreation",但 Kafka 抹掉重建后 `-source-stream-id` 未变;partition 11 上旧 incarnation 的 43.3M 条 receipt 与新 incarnation 键冲突、13.8M 条成孤儿。对账/守恒/"已摄入"判定都以 `(stream,partition,offset)` 为键 —— 今天的对账基线在双 incarnation 键空间上推理。 | Flow 唯一性 |
| **P1** | **块级去重无效**(`insert_deduplication_token` 在无 `non_replicated_deduplication_window` 的非 Replicated 表上是空操作),重试产生内容相同的物理重复行;09-19 曾测单日 239 万行。FINAL 承重,是所有读路径的放大器。 | Flow 唯一性 |
| **P1** | **重放会静默改写历史桶语义且对账看不见**:富化快照按 event_time 每次重选,一次过去生效的发布使任何重放对旧 offset 算出不同分类/方向,同键、代更晚、FINAL 胜出;对账只比计数/字节,从不比快照/分类。 | Flow 唯一性 |
| **P1** | **quarantine-on-replay 以 ≥2^63 的代压过已 persisted receipt**,事实行未动 → 永久假阳性 `MismatchIdentity`,receipt 失去"此 offset 是否有数据"的真值资格。 | Flow 唯一性 |
| **P1→已修复待发布** | **rollup `_generation` marker 后置。** 数据和 marker 已拆为两个同步 INSERT；数据失败不发 marker，marker 失败不推进完成水位，重试使用相互独立且稳定的 token。发布时仍需提高 hot generation floor，隔离旧协议 marker。 | Flow 聚合 |
| **P1** | **单分区重试耗尽会杀死整个 worker**,无人值守时所有 exporter 停消费,Kafka retention(现 6h)成为全量丢失兜底。 | Flow 可用性 |
| **P1** | **ClickHouse 内存治理缺失导致宿主 OOM 杀进程。** `max_server_memory_usage_to_ram_ratio=0.9`(15G 共享机上允许 CH 用 14G)+ profile 默认 `max_memory_usage=0`(单查询无限)且不落盘。任意一条临时 `clickhouse-client` 查询就能打死生产 CH(今日已发生一次,~40s 恢复,Kafka 缓冲无丢数)。 | 运维 |
| **P1** | **冷归档控制面未激活 = raw 无法受控删除。** v13 hot marker 已使常规 6h/24h 查询不再全扫 raw，但无 published lifecycle policy，cold archive/对账/删除链仍 fail-closed。 | Flow 存储 |
| **P1** | **RBAC 越权:`role.update/role.create` 持有者可给自己所在角色授予全部能力。** `IsAdmin` 只按角色**名字**"administrator"字符串判断,无"不能授予自己没有的能力"规则。 | RBAC |
| **P1** | **密码重置/修改后端齐全、前端零入口;唯一的找回说明指向不存在的 CLI。** `POST /users/:id/password`、`POST /me/password` 存在,`users.tsx`/`user-form.tsx` 没有任何调用;`forgot-pass-form.tsx` 让人跑 `./watchdog superuser upsert`(PocketBase 残留,`cmd/*` 无子命令)。 | 用户 |
| **P1** | **账单不算钱。** KISS-07 设计明确"不引入阶梯、税率、折扣和浮点金额";代码中**没有任何路径把 `unit_price` 乘以任何量**。当前是计量/证据/对账账本,不是出账引擎;表单上的"单价"是装饰性字段。"按 G 计费""每 M/每 G 单价"缺失只是其症状。 | 账单 |
| **P1** | **审批通过的调整可能静默不改变账单**(层不匹配时不重算 `Used/Overuse`,UI 默认层写死 `customer`);**`agg` 方向 = in+out 之和**且设计文档未定义(IX 常见约定是 max),对称流量最多高估 2×。 | 账单 |
| **P2** | 登录无限流/锁定;审计缺登录/登出/改密与账单资金动作;导出下载不复核当前权限;覆盖率缺口只"warning"不阻断审批;`quality_records` KPI 100% 饱和(信息位与错误位混算);`flow.export.*` 能力从未被检查;"启用用户"无一键入口。 | 各域 |

**live 背景**:账单在生产**从未使用**(0 账户、0 账期)→ 模型可在首用前修正、无迁移负担;用户 2 个(`admin`,`test`)、5 个内建角色、49 项能力 + 6 张按资源的用户级授权表;审计日志从未记录过改密/改角色。

---

## 1. 范围与方法

- **Flow 数据面**:collector → Kafka → worker → `flow_records` → rollup → `flow_aggregate_1h` → 查询;生命周期 seal/对账/删除。重点:唯一性、瓶颈、需求偏差。存储分层/TTL/MV 结论见存储审查修订版。
- **账单**:`internal/billing/*`、`internal/server/billing*.go`、`internal/flowch/billing.go`、`internal/snmpch/billing.go`、`docs/kiss07-billing-design.md`、前端 `billing-*.tsx`。
- **RBAC/用户**:`internal/server/{rbac,auth,handlers_session,handlers_admin,handlers_grants,router,install}.go`、schema `0001/0002/0024`、前端 `users.tsx / user-form.tsx / login/*`。
- **live 实测**(全部 `max_memory_usage=1.5G, max_threads=2`):今日分区行数/键数/代数、receipts 对比、incarnation 键区间、quality_flags 分布、CH 内存参数、MySQL 账单/用户/审计状态。

---

## 2. 需求-实现偏差总表(跨域)

| # | 需求(运营者视角) | 实际实现 | 证据 | 影响 |
|---|---|---|---|---|
| F1 | 总流量卡展示流入与流出 | **初始生产 WIP** 曾只返回 outbound，尽管 raw/rollup 均有 inbound。当前仓库已改为一次查询按 `business_direction` 分组，并有 SQL 编译测试；v13 发布证据证明 direction 查询走 aggregate，但尚缺一条同时断言 inbound/outbound 非零的生产响应验收 | `flow_query_modes.go`;`flow_reports.go`;`query_test.go:TestCompileDirectionGroupsTotalRowsInOneScan` | 代码路径已收敛，产品验收仍待补，不能继续把初始 `0 bps` 当当前事实 |
| F2 | "从本网视角看的流入与流出"语义 | 以 `business_direction` 富化结果拆分,但 UI/报表/账单三处对"方向"约定各异(见 §6) | `flow_query_modes.go:28-31`;`flowch/billing.go:254-260` | 同一词在三处含义不同 |
| F3 | 六类流量分类对双向都适用 | **已验证成立**:inbound 13.17 TiB 仍分布于六类(on_net_cross_province 6.04T…),六类筛选不是流入为 0 的原因 | live §3.1 | 排除一个误判,保留分类设计 |
| F4 | 数据质量 KPI 能指出问题 | `countIf(quality_flags!=0)` 把信息位(PlanFallback/EventTimeFallback/**EstimateCalibrated**)与错误位(Conflict/Overflow/Ambiguous/SelectorUnavailable)混算;今日 100% 记录为 `64=EstimateCalibrated` | `decode_adapter.go:32-40`;`query.go:999`,`joint.go:416`,`rollup.go:988` | KPI 恒 100%,失去告警能力(账单不依赖它,已验证) |
| B1 | 按流量 GB 计费、可配每 GB 单价 | traffic 计量强制 `package_port`;无"每 GB"标签;无 volume×单价路径 | `types.go:277-279`;`billing-account-form.tsx:433-441,489-492,560-566` | 无法售卖按量转接 |
| B2 | 平台输出应付金额 | 从不计算金额;`unit_price/currency` 仅进 `provenance_json` | `kiss07-billing-design.md:17`;`billing_exports.go:39-92,103-142` | 出账须外部系统自行推导公式 |
| B3 | 超出承诺带宽/配额按超用计费 | 只算 `Overuse` 证据,不计价 | `periods.go:143-152`;`state.go:94-98` | 无 commit+overage 产品 |
| B4 | agg 方向按 IX 惯例 | 恒为 in+out 之和,设计文档未定义 | `snmpch/billing.go:245`;`flowch/billing.go:254-260`;设计文档无 agg 定义 | 可能系统性高估 |
| B5 | 审批的更正应改变账单 | 仅当调整层 = 账户 `default_layer` 才重算;UI 默认层 `customer` | `state.go:468`;`billing-account-detail.tsx:156,860-872` | 操作员以为改了、实际没改 |
| B6 | 数据质量问题应阻断审批(最好双人) | 只有 `missing_sampling` 为 critical;缺桶/reset/gap 仅 warning;默认 `billing` 角色集 calculate+reconcile+approve 于一身 | `compute.go:124-138`;`state.go:165-175`;`rbac.go:53-57` | 70% 覆盖率的账期可被一人关闭 |
| U1 | 管理员可重置他人密码 | 后端有、前端无 | `router.go:70`,`handlers_admin.go:303-319` vs `users.tsx:219-249`,`user-form.tsx:378-391` | 运营者找不到入口(你的观察正确) |
| U2 | 用户可自行改密码 | 后端有、前端无(无 Profile 页) | `router.go:59`,`handlers_session.go:97-123`;前端零引用 | 同上 |
| U3 | 忘记密码有可用找回路径 | 对话框指向不存在的 CLI;重置 token 仅写日志 | `forgot-pass-form.tsx:80-106`;`cmd/watchdog-server/main.go:13-14`;`handlers_admin.go:15-19` | 单管理员失锁即无路可走 |
| U4 | 导出权限独立于查看 | `flow.export.customer/supplier/raw` 定义了、分配了、导航用了,后端从不检查 | `rbac.go:20-21,48-49`;`flow_query.go:68-84`;`navbar.tsx:215` | 能力目录与执行脱节 |
| U5 | 启用用户一键操作 | 列表只有禁用图标;启用需进编辑改状态 | `users.tsx:230-238`;`user-form.tsx:392-405` | 可用性 |
| U6 | 邮箱唯一性 | 报错文案暗示唯一,DB 无唯一索引(设计上 username 才是身份) | `0001_baseline.sql:60,72`;`handlers_admin.go:115` | 误导性错误分支 |

---

## 3. Flow 数据面

### 3.1 唯一性 / exactly-once(live 证据)

**今日分区(20260922)**

| 指标 | 数值 | 解读 |
|---|---|---|
| 行数 / 近似去重键数 | 68.12M / 68.42M(`uniq`) | 重放重复率 ≈ 0;`ingest_generation` 为毫秒时间戳(1.79e12),22,682 个代 |
| receipts vs 记录中的 datagram | 17.54M vs 17.57M | 对账基线一致(≈0.2% 为估算误差) |
| `estimated_valid=false` / `disposition≠count` / quarantined | 0% / 0% / 0 | 采样标定与准入干净 |
| `quality_flags` | 100% = 64(`QualityEstimateCalibrated`) | KPI 饱和(§2 F4) |
| `source_stream_id` | 单一 `watchdog-prod-kafka-watchdog-flow-raw-v1` | 见下 |

**P1 —— Kafka incarnation 复用(partition 11,同一 stream id)**

| incarnation | offset 区间 | receipts | 时间 |
|---|---|---|---|
| A(抹盘前) | 0 → 68,911,483 | 56.99M | 09-19 11:53 → 09-21 16:49 |
| B(当前) | 24,068,757 → 55,200,774 | 31.13M | 09-21 16:50 → 现在 |

- B 的区间**完全落在** A 内。`flow_ingest_receipts`(无 TTL,`ORDER BY (source_stream_id,kafka_partition,kafka_offset)`,ReplacingMergeTree(generation))中:**43,282,696** 条 A-receipt 与 B 键冲突;**13,781,649** 条 A-receipt 为孤儿(offset 高于当前 log end);冲突区 42.75M 活跃行 vs 31.11M 键 → 11.6M 键当前同时持有两代未合并的行,合并后一代静默覆盖另一代。
- `flow_records` 今日之所以干净,只因 A 期分区被手工 DROP;对账扫描器 `reconciliation_scanner.go:150,230,291` 以 `SourceMessageKey{stream,partition,offset}` 为键,仍会把 A-receipt 配到 B-记录。
- 规则只存在于 flag 帮助文本:`cmd/watchdog-flow-worker/main.go:107` "stable Kafka cluster/topic incarnation ID (required; never reuse after topic recreation)";生产 `worker.env` 未曾改过。
- **修法**:① 在 UTC 日边界切换新 stream id(保证每个 sealed day 单 incarnation);② 清理 A 期 receipts(孤儿可直接删,冲突段需按 inserted_at 分代处理);③ 代码强制:worker 启动时读取 Kafka topic ID(`tIC4zs1O…`)并与 stream id 绑定持久化,不一致拒绝启动。

**代码级唯一性发现(flow agent,静态追踪)**

| ID | 位置 | 失效场景 | 级别 | 修法 |
|---|---|---|---|---|
| U1 | `flowch/native.go:271-284`;无任何 migration 设置 `non_replicated_deduplication_window` | 每次 insert 都发 `insert_deduplication_token`,但在非 Replicated 的 ReplacingMergeTree 上窗口默认 0 → 完全无效。超时/瞬时错误重试同一 `PreparedBlock`(代在解码时固定,`native.go:436-442`)产生**内容相同的物理重复行**。09-19 审计曾测到单日分区 2,393,402 行重复;今日 live 重复率≈0(重放压力低),但机制未修。只要 FINAL 不被删就不是逻辑双计,是所有 FINAL 读的放大器。 | **P1**(若 FINAL 被删即 P0) | 确定性 token(stream,partition,bucket)+ 设窗口 = 峰值块/秒 × 最大重试时长(perf-audit R2.1/H2/CH2 已写方案,未落地) |
| U2 | `native.go:436-442`(`ingest_generation = received.UnixMilli()`);`consumer.go:110,296-307` | 代 = 写者进程墙钟,非逻辑计数。僵尸旧 owner 与新 owner 同写同分区时,**本地时钟更晚者赢**,与合法归属无关。`BlockRebalanceOnPoll`+延迟 `AllowRebalance` 是真实防护,但其安全窗受 `RetryMaxElapsed`(最长 1h,`writer.go:174`)限制,与 consumer group session/rebalance timeout 无代码级交叉校验。 | P2 | 启动断言 `RetryMaxElapsed` < group timeout;代值编入 worker/boot 身份作 tiebreaker |
| U3 | `consumer.go:121-125,174-190`;`pipeline.go:152-165`;`quarantine.go:35`(`Generation = 1<<63 \| revision`);`reconciliation.go:164-172`;`reconciliation_scanner.go:162-165` | 模板重放会**完整重跑**持久化管道(水位只抑制重提交,不抑制重写)。若 raw-delete barrier 已在原持久化与重放之间推进,同一 offset 被改投 `flow_quarantined_datagrams`,receipt 代被强制 >2^63,**必定压过**原 `persisted` receipt;原 `flow_records` 行原封不动 → 对账永久看到 `late_quarantined` receipt 覆盖真实事实 → 永久假阳性 `MismatchIdentity`。 | **P1**(审计链腐蚀;今日不错账,因只比计数/字节) | quarantine-on-replay 对已 `persisted` receipt 幂等(比较而非盲目出代),或对账特判 `persisted→late_quarantined` 且事实仍在 |
| U4 | `flowdimension/snapshot.go:688-712`(`Select` 为 event_time 的纯函数);`reconciliation.go:93-96` | 富化快照在**每次处理尝试时重新选择**,不"钉住首次处理时刻"。一次 `EffectiveFrom` 在过去的发布(正常回填/修正)会让**任何**后续重放(worker 重启重跑未提交尾部、模板重放、rebalance)对旧 offset 算出不同的 `dimension_snapshot_id/business/business_direction/category`,同键不同内容、重放代更晚经 FINAL 胜出。这是 `ReclassificationRunner` 的刻意机制(有证据相等 + `CoordinateDiff` 审计),但**对普通 Kafka 重放同样触发且无审计**;`CompareIngestAudit`/scanner 只比 count/bytes/packets,从不比快照/分类 → 此类漂移在对账里显示"健康"。 | **P1**(历史桶静默语义漂移,唯一守恒检查看不见) | 把实际使用的快照 ID 钉进 receipt 供审计;或对被重处理的 offset 增加"snapshot/classification_version 是否变化"的低成本检查 |
| U5 | `rollup.go` 的 `Run` / `buildRollupMarkerQuery`;`rollup_test.go` 两阶段故障测试 | **已修复待发布。** 数据 INSERT 不再 UNION marker；只有数据成功返回才执行独立 marker INSERT。数据失败时执行器只收到一条 SQL；marker 失败时 generation 不可读、成功计数和完成水位均不推进。data/marker 使用不同、各自确定性的 dedup token。 | **关闭前置条件**：发布时提高 `minimum_generation`，重建 hot 连续前缀；否则旧二进制产生的低代 marker 仍不满足新协议 | 生产故障注入或受控 kill 验证后关闭 U5 |
| U6 | `main.go:107`;`record.go:82-94`(仅字符集校验) | stream id 仅由运营者保证不复用,无时间唯一性校验 —— **live 已发生**(上表 A/B incarnation) | agent 评 P2 → **live 升 P1** | 服务端持久化"topic incarnation"事实(cluster ID + topic ID),不一致拒启 |
| U7 | `flowstream/receiver.go:92-98,99-110,111-117`;`flow-collect/main.go:254-262` | 内核 `SO_RXQ_OVFL` 丢包、准入拒绝、超长/无效三类丢失只在 Prometheus 计数,**结构上不可能**进入 receipts(从未成为 Kafka 消息);Kafka 反压阻塞收包循环 → 转化为更多内核丢包。 | P2(UDP 固有;告警缺口) | 直接对 `flow_collect` 计数器告警,不能指望 CH 侧对账看见此类丢失 |
| U8 | `consumer.go:302-306` → `flow-worker/main.go:308-316,146-149`(`log.Fatal`) | 单分区重试预算耗尽会**杀死整个 worker 进程**(09-19 review 已列 Critical)。新增后果:无人值守 crash-loop 下**所有** exporter 停消费,Kafka retention(现 6h)成为**全部** exporter 的真实丢失兜底。 | **P1**(超过 retention 即真丢) | 隔离持续失败分区(pause/skip+告警),不整体退出 |

### 3.2 性能瓶颈排名

按当前状态重新排名：
1. **冷生命周期未激活**：不再意味着常规 6h/24h 查询全 raw，但意味着 1d cold tier 无连续覆盖、raw 无法删除，多日查询仍可能退化。这是当前存储 P1。
2. **自动规划覆盖断层（本轮已修复待发布）**：自动密度原会选择 1m，而生产只维护 `minute_lookback=6h`；6–24h 报表和较早的短窗口都可能因请求起点早于 1m coverage 而整窗回退 raw。服务端现在仅对自动报表按 scheduler 的实际覆盖起点（`minute_lookback` + `seal_delay` + 整点封桶）选择 1h，显式 step 不变。
3. **剩余高基数路径**：remote_port 聚合已从约 437K/hour 降至约 19.8K/hour；普通 endpoint/category/business 已命中 rollup。仍需处理的是 base-only filter、address-set 和任意多维 joint，而不是再次扩大公共聚合基数。
4. **全局内存治理仍缺失**：hot rollup 已有 6GiB/4 threads/priority 10 且 batch 单连接，但 ClickHouse server profile 与临时运维查询仍可能争抢宿主内存。
5. **partition 11 单分区**：按 exporter 保 template 顺序是刻意约束，当前 lag 数百尚能跟上，但单 exporter 无横向扩展余地。
6. **flow 账单路径仍扫 raw**：`flowch/billing.go` 聚 5m，长账期仍可能在 120s/1GiB 限额下 fail-closed；不能把报表 rollup 直接当计费证据替代品。

代码级排名(flow agent):
1. **块级去重无效是所有其他项的放大器** —— `native.go:271-284`,无 migration 设 `non_replicated_deduplication_window`(仅一个故障注入测试设为 0)。每一处 FINAL 读(rollup `latest` join、`query.go`/`joint.go`、`reconciliation_scanner.go`、`interface_reconciliation.go`、`billing.go`)都在比逻辑行更多的物理行上付合并代价。单点杠杆最高。
2. **通用 `joint.go` 仍缺 heavy-hitter 候选捷径** —— endpoint 的 category/business 关联已经由 `rollup_joint.go` 在 aggregate 前缀上解决；但任意多维 joint（如 `src_ip+remote_port`）仍在 raw 上执行 `grouped→scored→ranked`，高基数组合要先形成完整 GROUP BY 状态再 top-N。后续优化必须限定支持的维度组合，不能把近似候选静默套到所有 joint。
3. **raw→1m 的 EAV 展开仍是每个封闭小时一次重任务** —— 已消除“同小时扫描 60 次”和“1h 再扫 raw”，也把 remote_port 限为 256；剩余成本是一次 ARRAY JOIN 约 17 种 dimension + IP/port 窗口排名。当前用 batch 单连接、4 threads、6GiB 与 spill 控制，不能再与交互池并发争抢；两阶段 marker 只修正确性，不减少该数据阶段成本。
4. **provenance 列固定在展示层 GROUP BY** —— `query.go` `series_rows`(~938-940,~1071-1072)保留 `dimension_snapshot_id, geo_version, classification_version`;窗口内任一版本切换即按 `dimension_value` 倍增输出行,且使预检 `estimatedRows = points*series+1`(`query.go:405-408`)**低估**,编译期看似安全的请求运行期仍可能 `result_overflow_mode=throw`。
5. **对账扫描成本** —— `reconciliation_scanner.go:282-295`(`flow_records FINAL … GROUP BY kafka_offset`)与 `interface_reconciliation.go:243-313`(三张表各一次 FINAL 扫描再 join 外部 scope,上限 7 天/5 万行)。有上限,但每次都付第 1 项的 FINAL 膨胀,且 receipts 无 TTL → 基线成本单调增长。
6. **分区内写路径无流水线**(印证既有"写路径无流水线")—— `consumer.go:398-423` 逐 chunk 同步调 handler;`processor.HandleRecords→pipeline.Handle→writer.Write`(`processor.go:139-165`,`pipeline.go:81-150`,`writer.go:182-199`)全同步,下一块解码从不与当前块的 CH 写入+重试重叠。叠加单 exporter 单分区,吞吐上限 = decode+enrich+insert+retry 延迟的串行和。

### 3.3 存储分层与生命周期

见 `watchdog-storage-lifecycle-review-2026-09-22.md`。当前 correctness 约束为：archive fail-closed 需 published policy；raw `FINAL` 承重不可删；raw 不加无条件 TTL；generation-marked cache 不授权删除；查询边界只信物理表连续 marker；marker 必须在数据 INSERT 成功后单独发布。本文补充：**receipts / reclassified / reclassification_generations / quarantined 四表无 TTL 无裁剪**，其中 receipts 已因 §3.1 双 incarnation 而不可信，清理时应一并纳入生命周期策略。

### 3.4 Flow 需求偏差(F1–F4)

- **F1 流入=0 是初始生产 WIP 发现，不是当前已复现事实**：当时 raw ✓ → 六类 ✓ → rollup ✓ → aggregate total ✓，但投影只剩 outbound。当前代码的 aggregate 与 joint fallback 都以一个查询按 `business_direction` 分组，再统一映射 Inbound/Outbound；需要用 v13 `/flow/reports/query` 响应补一次双向非零验收后才能正式关闭。
- **F2/§6**:方向语义需统一契约。
- **静态 vs live 对照**:flow agent 静态追踪三处方向代码(`query.go:421-431,444-448,864-960`;`flow_query_modes.go:26-139`;`flow_reports.go:314-345,695-761`)均对 in/out **对称**,并提出"`DirectionIn` 要求**目的**命中 `flow=local` 前缀、`DirectionOut` 要求**源**命中(`classify.go:97-101`),本网地址标记不对称即可复现 out≠0/in=0"的假说。**live 数据否定该假说**:`business_direction='in'` 在 raw/rollup/aggregate 各层均有 13–133 TiB。因此分类正确产出了 inbound,丢失点在生产 WIP 二进制的方向查询/投影层(与仓库不一致)。
- **方向查询已由 N×2 收敛为单次分组**：当前 headline 方向面板不再分别发 in/out 两条 SQL；这既消除双扫描，也避免两次查询的 coverage/版本水位不一致。
- **死代码已删除**：旧的 `mergeFlowDirectionResults` 及其仅自测引用已移除，方向结果只剩单查询标注路径。
- **采样比例一致(正确)**:`estimated_bytes_scale_ppm` 仅在富化时应用一次并写入列(`decode_adapter.go:176-190`),rollup/query/joint/billing 都读同一预缩放值;小缺口:账期内重标定无可查询的"scale generation"维度,跨标定的小时桶会静默混合两套标定。
- **账单三层口径差异**:raw/supplier 层**不**过滤 `disposition='count'`,customer 层过滤(`flowch/billing.go:250-270`)—— 刻意的三层设计,但 API/UI 未标注,同端口同窗口 raw≠customer 会被当作"对不上"。
- **Top-N 行数契约**:`series_rows` 的 GROUP BY 含 provenance 三列,版本切换窗口内每桶行数超过 `top_n+1`,与预检预算不符(见 §3.2 第 4 项)。

### 3.5 Flow 保留项(不得回退)
- 聚合读的 generation-marker 门控:`latest` CTE(`max(generation)` from `_generation`)+ `INNER JOIN USING(bucket,generation)`(`query.go:864-877,962-984`)正确绕过 ReplacingMergeTree "消失的键永不删除"的限制;不可退化为纯 FINAL 读。
- `kgo.BlockRebalanceOnPoll()` + 延迟 `AllowRebalance()`(`consumer.go:110,296-307`)是防僵尸 owner 竞写的正确惯用法;只需补 U2 的超时断言。
- 分区级故障隔离与精确部分进度记账(`consumer.go:311-396,398-423`):一个分区失败不影响兄弟分区已落盘写入。
- 单条坏报文转 `mapping_rejected` receipt 并前进(`pipeline.go:105-127`),而非阻塞整个分区组。
- Reclassification 隔离:`flow_reclassified_records` `ORDER BY (reclassification_id, reclassification_generation, …)`(`016:12-15`),`joint.go:84-97` 服务端选表,`CoordinateDiff` + 字节/计数证据相等门控(`reclassification.go:123-180,257-281`);`query.go`/`joint.go` 默认路径不存在与 `flow_records` 双计的通路。
- 多重删除门禁:日级守恒必须精确相等才能离开 `sealed/archive_written`(`archive.go:313-352`),再需 `DeletionApproval` 物理计数匹配(`archive_delete.go:39-45`),再原子可重试 `DROP PARTITION`(`raw_delete.go`)。

---

## 4. 账单(billing)

### 4.1 定价模型矩阵

| 模型 | 状态 | 证据 | 缺口 |
|---|---|---|---|
| 端口套餐(package_port) | **仅标签** | `types.go:51`;`form.tsx:561-562` | 单价从不参与计算 |
| 月 95 / 日 95 / 月均 × 每 Mbps | **计量已实现,计价未实现** | `compute.go:37-54,72-97,99-115`;`types.go:97-98` | 无 `money=unit_price×Mbps` |
| 流量 × 每 GB | **缺失** | `types.go:277-279`;`form.tsx:433-441,489-492` | traffic 锁死 package_port;`AlgorithmTotal` 只与 `traffic_allowance_bytes` 比出"超用"数字 |
| 承诺 + 超用 | **仅证据** | `periods.go:143-152`;`state.go:94-98,477-481`;设计 §17 | `Overuse` 不计价,无超用费率字段 |
| 阶梯 / 税 / 发票号 | **缺失(设计明示不做)** | `kiss07-billing-design.md:17` | — |
| 保底(bandwidth) | 已实现为 floor | `periods.go:143-152`;`types.go:280-285` | 亦不计价 |
| 多币种 | 仅字段 | `types.go:59,97` | 无换算 |

**结论**:`unit_price` 的每一个消费点(`accounts.go:14-16,45-50,77-84,212-213`;`server/billing.go:319-320,363-368`)都不做乘法;导出行 schema(`billing_exports.go:39-92`)没有 `unit_price/currency/amount` 列。

### 4.2 正确性

| ID | 位置 | 场景 → 错账 | 级别 | 修法 |
|---|---|---|---|---|
| B1 | `state.go:468`;`billing-account-detail.tsx:156,860-872` | 调整层≠账户默认层 → 状态"approved"、导出有值、`period.Used` 不变、无提示 | **P1** | 调整层默认取账户 `default_layer`;非默认层需显式确认或拒绝 |
| B2 | `snmpch/billing.go:245,247`;`flowch/billing.go:254,257,260` | `agg` = 和;对称流量较 max 约定最多 2× | **P1(需求决策)** | 合同层面定义,或增加 per-account `max` 模式 |
| B3 | `compute.go:124-138`;`state.go:165-175`;`detail.tsx:780` | 缺桶/reset/gap 仅 warning,Approve 只看 status | P2 | 覆盖率阈值 critical 或审批页强制确认未决 issue |
| B4 | `state.go:500-544`;`rbac.go:53-57` | 一人可算、可消 critical issue、可批可关 | P2 | 拆默认角色(operator/approver)或二人复核 |
| B5 | `form.tsx:575` vs `types.go:283-285` | package_port 隐藏 minimum_percent 控件,历史非零值无法自助修 | P2 | 加载时强制归零 |
| B6 | `flowch/billing.go:270` | 扫 raw 聚 5m,120s/1GiB 预算,超限 fail-closed | P2(可用性) | 5m 物化计划 |
| B7 | `flowch/billing.go:20-24`;`snmpch/billing.go:19-22`;`periods.go:161-167` | `(device_id,if_index)` 键,if_index 重编号后证据指向别的电路 | P3 | 快照加 ifDescr/MAC 交叉校验 |

### 4.3 保留项(不得回退)
追加式反号调整账本(`state.go:335-378`);CAS + approved/closed 不可变状态机(`state.go:60-62,162-164,204-209`);建期快照账户/party/端口/计价(`periods.go:99-176`);FK `RESTRICT` 防账单证据孤儿(`0001_baseline.sql:254`,`0020:82`);SI 单位全链一致(`billing-units.ts:1-2`);共同完整桶 95th 策略(`flowch/billing.go:181-199`,`service.go:254-315`);采样比例仅在摄入时应用一次(`flowch/billing.go:252-260`);导出 SHA-256 命名 + 原子写(`billing_exports.go:330-374`)。

---

## 5. RBAC / 用户

### 5.1 用户生命周期能力矩阵

| 能力 | 后端 | 前端 | 权限 |
|---|---|---|---|
| 创建用户 / 初始密码 | `handlers_admin.go:50-134` | `user-form.tsx`(仅创建态显示密码) | `user.create`(+`user.manage`) |
| **自改密码** | `handlers_session.go:97-123`,`router.go:59` | **缺** | — |
| **管理员重置他人密码** | `handlers_admin.go:303-319`,`router.go:70`(重置后撤销全部会话+审计) | **缺** | `user.manage` |
| 禁用 | `handlers_admin.go:154-265`(即时撤会话) | `users.tsx:230-238` | `user.update` |
| 启用 | 同上 | 仅编辑表单状态下拉 | `user.update` |
| 删除 | `handlers_admin.go:267-300`(禁自删、禁删末位管理员) | `users.tsx:106-118` | `user.delete` |
| 角色分配 | `handlers_admin.go:118,240-244,321-345` | `user-form.tsx:407-437` | `user.manage` |
| 独立强制登出 / 会话列表 | **缺**(仅作为改密/禁用副作用) | **缺** | — |
| 最近登录 | **缺**(`sessions.last_active_at` 未上卷) | **缺** | — |
| 登录限流/锁定 | **缺** | **缺** | — |
| 密码策略 | 仅长度 8–72(`auth.go:20-21,35-38`);bcrypt cost 10 | 同 | — |
| 忘记密码 | token 1h 单次 SHA-256 存储,**仅写日志**投递(`handlers_admin.go:15-19`) | `forgot-pass-form.tsx`(指向不存在 CLI) | — |

### 5.2 安全发现

| ID | 位置 | 问题 | 级别 | 修法 |
|---|---|---|---|---|
| C1 | `handlers_admin.go:411-449,451-503`;`rbac.go:71-76,182-190` | `role.update/create` 可把 `abilityCatalog` 全量授予自己所在角色;`IsAdmin` 与末位管理员保护都按角色名字符串判断,越权者永远不"是"管理员,可逐个禁用真管理员 | **P1** | "不能授予自己没有的能力";禁止修改自己所属角色的能力集;`IsAdmin` 按能力覆盖而非名字 |
| C2 | `forgot-pass-form.tsx:80-106`;`cmd/watchdog-server/main.go:13-14` | 找回说明指向不存在的 `superuser upsert` | P2 | 删除/替换;把已有重置端点接进 UI;文档化直接 SQL 恢复 |
| C3 | `handlers_session.go:33-55` | 登录无限流/锁定 | P2 | 按用户名+IP 计数/退避 |
| C4 | `flow_exports.go:943-955`;`snmp_exports.go:430-453` | 导出下载只看所有权,不复核当前能力;撤权后仍可下载到保留期结束 | P2 | 下载时按作业冻结的 view 复核当前能力 |
| C5 | `0001_baseline.sql:415-426`;`handlers_session.go`;`billing.go:649-849` | 登录/登出/自改密/找回全不审计;账单 create/approve/reverse adjustment、approve/close period、import external、resolve issue 全不审计;表无 DB 级不可变 | P2 | 补 `s.audit`;对应用 DB 角色 `REVOKE UPDATE,DELETE ON audit_logs` |
| C6 | `install.go:47-73,102-127` | 首装抢注竞争(自托管常见) | P3 | 文档/反代限 loopback |

### 5.3 路由门禁
`POST /billing/jobs/:id/cancel`(`billing.go:60`)与 `/exports*`(`exports.go:32-38`)无路由级 `requirePermission`,靠处理器内检查(P3,风格不一致);`POST /flow/exports`、`/flow/records/exports` 只要 `flow.view.<view>`,`flow.export.<view>` 从未被检查(P2,U4)。

### 5.4 保留项
会话 token 仅存 SHA-256、bcrypt、双提交 CSRF(有测试);**`requestIsSecure` 已正确处理反代 `X-Forwarded-Proto`(仅信 loopback 对端,`auth.go:99-116`,`auth_cookie_test.go`)—— 项目笔记里"cookie Secure 不看 X-Forwarded-Proto"一项已过时**;改密/禁用即时撤会话并逐请求复核;末位管理员保护;忘记密码防枚举;管理端点无 IDOR;资源级授权(设备/端口/账户/图/指标)与角色能力两层正确叠加;view/manage/publish 分权阶梯贯穿地址库、富化、VPN、存储删除、账单;租户残留已清干净;agent/worker 认证与用户会话隔离;前端 `can()` 只反映服务端返回。

---

## 6. 跨域一致性主题

1. **方向(direction)三套语义**:报表 `business_direction` in/out(+transit/ambiguous)拆分;账单 account/port direction in/out/agg,`agg`=和;UI "本网视角流入/流出"。需要一份方向契约(枚举 + 语义 + 每处消费者的映射),否则三处各自解读将持续制造"曲解"。
2. **单位**:账单 SI 一致;报表 `estimated_bps` vs `estimated_bytes` 由 metric 定义;bytes→GB 仅在账单 UI(1e9)。若引入每 GB 计价须复用 `billing-units.ts` 常量。
3. **质量信号**:`quality_flags` 信息位/错误位需分离后再进 KPI 与任何门禁。
4. **审计覆盖**:安全动作(认证族)与资金动作(账单族)是两块空白;其余域覆盖良好。
5. **配置即约束的地方要变成代码约束**:stream id 不可复用、Kafka retention、CH 内存上限 —— 三者都曾/正以"靠人记住"运行。

---

## 7. 修复路线(按优先级,给最小改动)

**P1(先做,均不碰计费权威路径)**
1. Kafka incarnation:UTC 日边界切换 `-source-stream-id`;清理 A 期 receipts;worker 绑定 topic ID 拒启。
2. CH 内存治理:`max_server_memory_usage_to_ram_ratio≈0.55`;默认 profile `max_memory_usage≈4G`、`max_bytes_before_external_group_by≈2G`;回填并发限 1。
3. 归档控制面:按存储审查修订版发布 `raw_delete_enabled=false` 的非破坏策略,先回填/对账,再单独审批删除。
4. RBAC C1:授予受限于自身能力;禁改自属角色;`IsAdmin` 按能力。
5. 用户:接通已有重置/自改密端点的 UI;修正找回文案;补认证审计。
6. 账单:B1(调整层默认与校验)、B2(agg 语义决策,建议 per-account 模式)、把 `unit_price` 要么接入计算(含每 GB volume method)要么明确标注"仅参考";补账单审计。
7. 块级去重生效(U1):确定性 token + `non_replicated_deduplication_window`(perf-audit 已有方案);**先于**任何 FINAL 相关改动。
8. 重放语义与审计链(U3/U4):receipt 记录实际使用的快照 ID;quarantine-on-replay 对 persisted 幂等;对账增加快照/分类变更检查。
9. ~~rollup marker 后置(U5)~~ **代码已完成，待带新 generation floor 发布与故障注入验收**；worker 分区隔离(U8)仍未完成：持续失败分区应 pause+告警，而非整体 Fatal。

**P2**
登录限流;导出下载复核能力;覆盖率门禁;quality KPI 掩码;`flow.export.*` 二选一(执行或删除);启用用户一键;误导性邮箱唯一报错清理;receipts/reclassified/quarantined 纳入 TTL 策略;`RetryMaxElapsed` < group timeout 启动断言(U2);collector 丢包/拒绝计数器直接告警(U7);joint 路径对明确支持的高基数组合补候选→精确两遍 top-N;宽窗口 provenance 输出契约与预检预算对齐;分区内写路径流水线化。

### 7.1 可执行发布批次与门禁

原 P1 清单混合了查询、数据身份、安全和商业规则，不能作为一个大版本一起上线。建议拆成以下可独立回滚的批次：

| 批次 | 内容 | 上线门禁 | 明确不包含 |
|---|---|---|---|
| R14 查询正确性 | 两阶段 marker；配置驱动的 1m→1h 自动规划；删除方向双查询死代码 | 新 generation floor；marker fault injection；6h/12h/24h source 与耗时；API health | lifecycle policy、raw 删除、FINAL 删除 |
| R15 cold lifecycle | 发布 `raw_delete_enabled=false` policy；1h/1d 回填；守恒与 gap 告警 | 连续日 coverage；raw/archive 精确相等；备份演练 | 开启任何物理删除 |
| R16 摄入身份 | topic incarnation 绑定；确定性切块；dedup window 定容；重放 provenance 审计 | rebalance/timeout/跨窗口测试；物理重复率看板；旧 receipts 隔离方案 | 去掉 raw `FINAL` |
| S1 安全 | RBAC 不可提权；登录限流；认证/资金审计；导出复核权限 | 越权回归矩阵；末位管理员和撤权场景 | 账单算法变化 |
| B1 账单定义 | 明确平台是“计量账本”还是“出账引擎”；确定 agg=sum/max、金额精度、按 GB、超用规则 | 产品签字的方向/金额契约；金样账期 | 在需求未定时补猜测性乘法 |

R14 可以直接降低查询风险；R15/R16 涉及证据链，只能 fail-closed 推进；B1 存在真实业务选择，不能由代码 review 代替产品决策。

---

## 8. 附:方法学备注

- 本次 live 排查曾以一条未受限的 4 列精确 `GROUP BY`(68M 行)叠加回填负载触发宿主 OOM;这本身即 §0 P1 内存治理项的实证。此后所有查询均加 `SETTINGS max_memory_usage=1500000000, max_threads=2`。
- 生产 flow 查询二进制为并行 rewrite 的 WIP,与仓库不一致;涉及查询投影的定位(F1)以生产响应为准,不以仓库代码推断。

---

## 复核状态（2026-09-28）

本节由 2026-09-28 全项目复核生成：每条发现都对照当前代码/迁移/提交核实，以代码为准。汇总：已修复 5 · 部分修复 4 · 未修复 59 · 作废 1（部分未修复项在复核时按组列出，故表格行数可能少于汇总数）。

| 位置 | 发现 | 状态 | 证据 / 说明 |
|---|---|---|---|
| L30 | P1 Kafka incarnation reuse | 未修复 | worker `main.go:106` only flag help text; no topic-ID binding (grep empty) \| |
| L31 | P1 block-level dedup ineffective | 未修复 | `native.go:283`; no window \| |
| L32 | P1 replay silently rewrites historical semantics | 未修复 | receipts have no snapshot ID; reconciliation compares counts only \| |
| L33 | P1 quarantine-on-replay generation ≥2^63 | 未修复 | `quarantine.go:35` \| |
| L34 | P1 rollup marker published before data | 已修复 | `bc10958f4` `rollup.go:658-669` two-phase；deployment unconfirmed; needs new generation floor |
| L35 | P1 one partition's retry exhaustion kills the worker | 未修复 | `consumer.go:302-305` → `main.go:145-146` log.Fatal \| |
| L36 | P1 ClickHouse memory governance | 未修复 | deploy/clickhouse/config.d has only `backup.xml`; hot rollup capped at 6GiB；server profile not in repo |
| L37 | P1 cold archive control plane inactive | 未修复 | no published policy (ops)；operator used manual raw retention instead |
| L38 | P1 RBAC self-escalation | 未修复 | `handlers_admin.go:411-501` has no "cannot grant what you lack" check; IsAdmin by name `rbac.go:28,75` \| |
| L39 | P1 no password UI; recovery points to a missing CLI | 未修复 | no password calls in `user-form.tsx`; `forgot-pass-form.tsx:101-104` \| |
| L40 | P1 billing never computes money | 未修复 | unit_price is stored/read only (`accounts.go`)；needs a product decision |
| L41 | P1 adjustment layer mismatch / agg=sum | 未修复 | no billing commits since 09-22 \| |
| L42 | P2 bundle (login limit / audit / export recheck / coverage gate / KPI / flow.export / enable user) | 未修复 | see C3/C5/C4/B3/F4/U4/U5 \| |
| L61 | F1 total-traffic card shows only outbound | 部分修复 | one scan grouped by direction (`00be2cdb5`, `query_test.go:108`)；no prod acceptance showing both directions non-zero |
| L62 | F2 three meanings of "direction" | 未修复 | no direction contract \| |
| L63 | F3 six categories apply both ways | 作废（被后续决策取代） | verified correct; not a defect \| |
| L64 | F4 quality KPI saturated | 未修复 | `query.go:999,1155`; `joint.go:416`; `rollup.go:1065` \| |
| L65 | §2 B1–B6 billing requirement gaps | 未修复 | billing unchanged；6 items |
| L71 | §2 U1–U6 (admin reset UI / self password UI / recovery / flow.export unchecked / enable user / email error) | 未修复 | `router.go:59,70` endpoints but no UI; `rbac.go:21,49`; `flow_exports.go:154`; `users.tsx:230-238`; `handlers_admin.go:115`；6 items |
| L110 | §3.1 U1–U4 dedup / wall-clock generation / quarantine / snapshot drift | 未修复 | `native.go:283`; `writer.go:168-174` no group-timeout assertion; `quarantine.go:35`；4 items |
| L114 | §3.1 U5 marker ordering | 已修复 | `bc10958f4` \| |
| L115 | §3.1 U6 stream-id uniqueness | 未修复 | `record.go` only checks the charset \| |
| L116 | §3.1 U7 collector losses only visible in Prometheus | 部分修复 | `fae7eee2c` summary carries kernel_drops/rejected/invalid/oversize；no alerting; not deployed |
| L117 | §3.1 U8 whole-worker Fatal | 未修复 | same as :35 \| |
| L122 | §3.2 cold lifecycle inactive | 未修复 | policy not published \| |
| L123 | §3.2 auto-planning coverage gap | 已修复 | `bc10958f4` `flow_reports.go:486-488` \| |
| L124 | §3.2 high-cardinality paths / memory governance / partition 11 | 未修复 | unchanged；3 items |
| L127 | §3.2 flow billing scans raw | 部分修复 | 021 flow_interface_traffic_5m (`423d820e2`)；no writer; flowch/billing.go:270 still raw FINAL |
| L130 | §3.2 code-level items 1–6 | 未修复 | dedup / joint shortcut / EAV / provenance GROUP BY / scanner cost / write pipeline all unchanged；6 items |
| L139 | receipts/reclassified/quarantined have no TTL | 未修复 | no retention job \| |
| L146 | direction N×2 queries → one grouped query | 已修复 | `00be2cdb5` \| |
| L147 | dead code mergeFlowDirectionResults | 已修复 | `bc10958f4` (grep empty) \| |
| L148 | scale generation / billing layer labels / Top-N row contract | 未修复 | unchanged；3 items |
| L182 | §4.2 B1–B5, B7 | 未修复 | billing unchanged since 09-22；6 items |
| L187 | §4.2 B6 flow billing via raw 5m | 部分修复 | 021 schema only；reader not switched |
| L218 | §5.2 C1–C6 | 未修复 | C4: `flow_exports.go:946-957` checks ownership only; C5: no session/billing audit；6 items |
| L226 | §5.3 route-level gates | 未修复 | `exports.go:32-36` has no requirePermission \| |

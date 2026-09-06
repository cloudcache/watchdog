# 流量流向分析：需求与选型基线

> 状态：现行；2026-09-05 收敛。详细实现见 [flow-module-design.md](flow-module-design.md)，架构取舍见 [flow-pipeline-adr.md](flow-pipeline-adr.md)，执行状态见 [flow-module-tasklist.md](flow-module-tasklist.md)。旧的 collector 解码、WAL、normalized topic 和 collect-state 方案已废止。

## 1. 目标、范围与证据

目标是回答“哪些本地地址，通过哪个观察点，以多大流量，流向哪个地址段、地域、运营商、ASN、业务或风险类别”，支持总览、趋势、TopN、明细、修正、导出和质量核验。

产品证据来自用户提供的七张界面截图，核心能力包括：

- 上下行总量与六类流向卡片、占比、趋势、当前值、95th、峰值和平均值；
- 按本省城市/外省、异网运营商、境外地区、业务筛选；
- 源/目的 IP TopN、每个 IP 的分类拆分、详情和归属修正；
- 境外流量的端口/协议、地区、内网主机和疑似 VPN；
- VPN 端口、类型、主机、上下行和风险等级。

本模块包含 sFlow v5、NetFlow v5/v9、IPFIX；SNMP 是独立的设备/接口指标和可选对账来源；VPN 主动探测是异步扩展。Packet capture、全量 L7 payload 留存、IDS/IPS 和同步逐流探测不在范围内。

## 2. 不可混用的方向语义

### 2.1 观察方向

`ingress/egress` 描述报文相对 exporter 接口的观察方向，来自 sFlow input/output、NetFlow/IPFIX ingress/egress interface 和 exporter 配置。它用于观察点定位、接口映射和 SNMP 对账，不能直接等同于业务流入/流出。

### 2.2 业务方向

业务方向只由事件时间有效的本地地址快照计算：

| src 是否本地 | dst 是否本地 | 方向 | 处理 |
|---|---|---|---|
| 是 | 否 | `egress` / 流出 | 对端是 dst |
| 否 | 是 | `ingress` / 流入 | 对端是 src |
| 是 | 是 | `internal` | 单独统计或按策略丢弃 |
| 否 | 否 | `transit` | 单独统计或按策略丢弃 |

接口方向只用于冲突标记。若接口角色与地址判定冲突，保留原始事实并置质量标志，不偷偷翻转方向。

### 2.3 计数与采样

每条记录必须同时保留：

- `raw_bytes/raw_packets`：解码器读到的计数；
- `sampling_mode/sampling_rate/sampling_source`：计数语义及来源；
- `estimated_bytes/estimated_packets`：统一统计值；
- `quality_flags`：缺采样率、回退配置、序列缺口、模板缺失等。

采样率不是平台统一常量。设备可按端口、sampler、sFlow data source、NetFlow observation domain 或 exporter 配不同采样率，且协议字段可能缺失、延迟或表达“已放大计数”。规则按“报文明确字段 → data-source 规则 → exporter 默认 → 未知”解析；同一条记录只允许放大一次。未知不得默认为 1，也不得静默套用全局采样率。

业务速率统一为 `sum(estimated_bytes) * 8 / bucket_seconds`。事件时间、接收时间、Kafka partition/offset 都要保留，迟到按事件时间落桶。

## 3. 六维分类学

六类只对 `ingress/egress` 的本地—对端流量计算，优先级固定：

| 优先级 | 类别 | 判定 |
|---:|---|---|
| 1 | `overseas` 境外 | 对端 country 不属于中国口径；港澳台是否归境外由版本配置决定 |
| 2 | `onnet_local_city` 本网本市 | 对端属于本网，且 city = 本地城市 |
| 3 | `onnet_cross_city` 本网跨市 | 对端属于本网、同省、非本市 |
| 4 | `onnet_other_province` 本网外省 | 对端属于本网、非本省 |
| 5 | `offnet_same_province` 异网本省 | 对端不属于本网、同省 |
| 6 | `offnet_other_province` 异网外省 | 对端不属于本网、非本省 |

`internal`、`transit`、`unknown` 是处理状态，不塞进六类；六类和这些状态的计数之和必须与已接受 base fact 守恒。

分类输入必须带版本：本地前缀、本网运营商/ASN、Geo、业务/address set、分类规则。最长前缀匹配决定主地址段；address set 可多归属，用于标签和查询，不改变主分类守恒。

地址归属必须区分三种口径：`primary_prefix` 是每个 endpoint 唯一的最长前缀；Geo 是 continent → region → country → province → city 的单路径层级，同一 flow 在每个 level 各归一次；address set 是任意多个可重叠组。ASN/ISP 是可空属性，不是地址段或组的必填身份。相同 CIDR 只存一行，嵌套 CIDR 继承父级不同 key 的标签、子级覆盖同 key；组名独立于 CIDR 和 ASN。主前缀与 Geo 同层含 `_unassigned` 时可加，address set 组间及不同 Geo level 之间不可相加。

address set 的集合公式固定为 `(selector ∪ explicit CIDRs ∪ included sets) − (excluded CIDRs ∪ excluded sets)`；同字段多值 OR、不同字段 AND，排除最终生效。引用必须同租户且无环，IPv4/IPv6 分开归一，发布期切分重叠边界并预计算 membership。查询的并/交/差必须对 base fact 的 membership 去重求值，禁止通过相加重叠组的 rollup 得到“并集”。补集必须指定有限 universe。

## 4. F1–F9 功能需求

### F1 采集、准入、解码与质量

- 独立 `watchdog-flow-collect` 只做 UDP 接收、来源前缀准入、RawFlow envelope 和 Kafka 异步发送；不连接 MySQL、CH、VM，不解码 Flow，不落本地 WAL。
- Kafka 后的 `watchdog-flow-worker` 按 partition 有序使用 GoFlow2 解码 sFlow/NetFlow/IPFIX并维护模板状态。
- 未登记来源只计低基数拒绝指标，不保存 payload、不进入分析库。
- 记录 datagram、exporter、listener、协议、event/receive time、Kafka position、模板/采样/序列质量。
- Kafka 是唯一缓冲与短期重放边界；worker 只有在 CH base 成功后提交 offset。

验收：四类协议 fixture 可形成 base fact；同一 partition 重放不重复计数；未知来源、模板缺失、Kafka/CH 故障和 UDP drop 均可观测。

### F2 地址、Geo、ASN、运营商和修正

- 读取外部生成的 `flow-geo-v1`/`flow-geo-v2` 文件包；watchdog 不读写 EdgeManager 数据库，v1 仅作扁平字段兼容，v2 作为完整层级契约。
- 支持 IPv4/IPv6 最长前缀匹配、country/admin/city/ISP/ASN，文件校验后原子热加载。
- 升级后的地址包必须让每个不重叠 range 引用一个 `geo_leaf_code`，由 `geo_dict.parent_code` 预编译完整五级路径；事实存稳定 ID，名称按事件时间版本解析。
- 无 ASN/ISP 的 CIDR 仍可定义独立组名、层级标签和多个 address set；缺失值显式为 unknown，不拒绝发布或猜测归属。
- address set 支持 selector、显式成员/排除 CIDR、include/exclude 组；发布前做引用 DAG、集合边界、最坏展开量和冲突预览，超限拒绝而非截断。
- 支持租户级前缀、业务、provider/customer 标签和 Geo override；配置按事件时间版本化。
- 原始值不可变；supplier/customer 修正作为版本化规则在查询或派生层应用，带 reason、actor、审批和有效期。

### F3 六类总览

- 总量、流入/流出、六类当前值与占比；至少提供 5m/15m/30m/1h/3h/6h/12h/24h/2d/7d/30d/3月/6月/1年预设及任意自定义起止。预设时间窗不是存储 resolution 的别名。
- 趋势、95th、峰值、平均值和业务表；所有值显示口径、单位、bucket 和完整性状态。
- 六类 + internal/transit/unknown 与 base 守恒，禁止不同卡片使用不同过滤口径。

### F4 多维分析

- 分组：六类、Geo 各级、运营商、ASN、地址段、address set、业务、端口/协议、观察接口；一次查询只能选择一个 Geo level。
- 筛选：租户、target/device/exporter、业务、运营商、时间、高峰段、方向、IP 族和修正视图。
- 模式：流量值、占比、差值；TopN 必须有稳定 tie-breaker 和 `other` 桶。
- 查询器必须把时间窗、目标点数/显式展示步长、底层聚合 resolution、维度、过滤、TopN 和图表类型分别建模；后端按时间窗与目标点数选择数据源和展示步长，前端不得把 `1m/1h` 当作仅有的时间范围。
- 至少支持折线、堆叠、热力、数据表和桑基。折线/堆叠/热力可使用单维时间序列；桑基必须由同一事实或联合维度索引返回真实有序维度 tuple，禁止把多个互不相关的单维 TopN 在浏览器中拼成虚假链路。
- 过滤必须进入可验证的 typed grammar/AST 并可通过 URL 分享；不得把用户表达式直接拼进 SQL。数据表至少显示 last/average/95th/min/max/total、采样完整性、质量和版本，且使用统一 VTable 搜索/列过滤/分页。
- Geo 返回 breadcrumb/children/path/version/completeness；同层可加。address set 返回 `additive=false`，不得显示一个会误导为总占比的组间合计。
- 多组过滤明确支持 `include_any/include_all/exclude_any`；组合总量从 base membership 一次求和，大范围组合查询异步执行。

### F5 业务和地址段

- 业务由版本化 address set、前缀和标签定义；主地址段用最长前缀匹配，重叠段不重复计入主维度。
- 相同 CIDR 不因多个组或 Geo 层级重复建行；嵌套 CIDR 的 label 在 snapshot 发布时由父到子合并，运行时仍只查一次 LPM。
- 管理端提供 IPv4/IPv6 集合并、交、差、有限补集、重叠检测和发布预览；所有 128-bit 地址数学在后端完成。
- 地址段汇聚必须在 worker/CH 异步完成，绝不在 UDP 接收线程逐条访问管理库。
- 支持按地址段/ASN统计流量：方向确定后选择 remote endpoint，再按匹配前缀或 ASN 分组汇总 `estimated_*`。

### F6 源/目的 IP

- 源/目的 TopN、速率、包数、六类拆分、ASN/Geo/业务标签、端口/协议和趋势。
- 明细查询默认分页、搜索、VTable filter、排序和导出限制；敏感 IP 的查看/导出单独授权和审计。
- “修正归属”只生成新版本规则，不修改历史 base fact。

### F7 境外专题

- 境外总量、流入/流出、境外 IP 数、本地主机数、地区、ASN、端口/协议和趋势。
- country/region 口径显式显示；IPv4/IPv6 一致；未知 Geo 单列，不错误归为境外。

### F8 VPN 风险识别

VPN 是风险评分而非单端口结论。可配置证据包括：

- src/dst 境内外组合、端口/协议、CN2/ASN/高危地址段；
- 长连接、QUIC、TCP/TLS 行为、上下行近似对称或明显单向；
- 主机、对端、时间窗、会话规模和复发频率；
- 达阈值后异步、限速、授权地触发握手探测，识别 SOCKS/WebSocket/SSL VPN/Trojan/SS 等协议族。

探测 agent 必须可配置目标范围、协议插件、超时、并发、速率、租户配额和 kill switch；只消费任务并回传结构化证据，不修改分类事实。页面展示 score、证据、规则版本、探测结果和误报处置。

### F9 权限、生命周期与非功能

- 配置、查看、导出、修正、探测、审批分别授权；tenant/resource scope 继承平台 RBAC。
- 查询限制时间范围、点数、序列数、TopN、扫描字节和并发；导出异步执行。
- 数据生命周期覆盖 RawFlow Kafka、CH base/rollup、Geo/规则快照、导出和审计；引用版本的保留期不短于事实。
- collector/worker/CH/Kafka/Geo/VM 每层独立健康；故障不能伪装为 0 流量。
- P1 性能门槛由真实 datagram 大小、采样率和设备数量压测确定，不用链路带宽直接假设 records/s。

## 5. sFlow、NetFlow/IPFIX、SNMP 的角色

| 能力 | sFlow | NetFlow/IPFIX | SNMP |
|---|---|---|---|
| 五元组/端点/端口/协议 | 样本 | flow record | 无 |
| ASN/Geo/地址段流向 | 由 IP + 地址库推导 | 字段或 IP + 地址库 | 无 |
| 流入/流出 | 本地端点 + 观察接口 | 本地端点 + 观察接口 | 只有接口 in/out 总量 |
| 采样 | datagram/data source 通常携带 rate/pool | exporter/domain 的 sampling option 或配置 | 不提供 Flow 采样率 |
| 接口总字节 | 估算 | 估算/导出计数 | 设备 64-bit counter 权威 |
| 设备健康/接口速率 | 弱 | 弱 | 强 |

因此，无 SNMP 也能计算不同 ASN、IP 段、地域、运营商和六类流向；SNMP 不是 Flow 分类前置条件。SNMP 对账是可选质量校验：同设备、同接口、同方向、同时间窗比较 `sum(flow estimated bytes)` 与 `ifHCIn/OutOctets delta`，输出覆盖率和偏差。它可发现采样配置错、漏 exporter、观察点重复、接口映射错或设备 counter reset，但不得按比例偷偷改写 Flow 数据。

## 6. 方案对照与选型

| 方案 | 优点 | 不采用/采用方式 |
|---|---|---|
| Akvorado | RawFlow→Kafka→outlet 架构成熟，Kafka/CH 运维实践完整 | 采用其职责切分、franz-go 配置和故障语义；按 AGPL 来源保留文件头与归属 |
| GoFlow2 v3 | sFlow/NetFlow/IPFIX 解码成熟、吞吐高 | 作为 worker 解码库；不把 GoFlow2 放在 collector 热路径 |
| ntop/nProbe/nDPI | L7/行为识别和风险线索丰富 | nDPI 作为可选 analyzer/probe 插件，不成为 P1 基础链路 |
| SNMP | 接口总量和设备状态权威 | 继续写 VM，作为独立监控及可选 reconciliation |

最终链路只有一条：

```text
router UDP
  -> watchdog-flow-collect (source admission + RawFlow)
  -> watchdog.flow.raw-v1
  -> watchdog-flow-worker (GoFlow2 + sampling + dimensions)
  -> ClickHouse base + rollups
  -> authenticated API/query/export
```

不建设第二个 Flow 数据 topic、本地 raw WAL、collector 解码队列、collect-state、quality checkpoint、attempt journal、tombstone reconciler 或 VictoriaLogs Flow 存储。

## 7. watchdog 映射与前置条件

现有可复用能力：MySQL 管理库、tenant/RBAC、collector registry/plan、target/device/port、VM 指标、Geo bundle loader、Flow dimension/worker 基础。当前实现进度以 tasklist 为准，不能把设计存在等同于运行时完成。

上线前必须明确：

- Kafka 版本、broker/partition/replication/retention、TLS/SASL/ACL 和容量；topic 由 IaC/admin 创建，collector 无管理权限；
- exporter→collector 的源前缀准入和唯一归属；worker group 的 partition 亲和与 template 状态恢复测试；
- 各设备/端口/domain/data source 的计数语义和采样规则；
- Geo bundle 生成、校验、发布、回滚和事件时间版本窗口；
- CH base 去重键、TTL、rollup、迟到、回算和查询配额；
- 目标峰值 PPS 的实测报告、CPU/RSS/GC、Kafka buffer、socket drop 和 N+1 容量。

## 8. 参考实现

- [Akvorado inlet/outlet 架构](../akvorado/console/data/docs/01-install.md)
- [Akvorado 配置与采样/富化](../akvorado/console/data/docs/02-configuration.md)
- [Akvorado 运维与容量](../akvorado/console/data/docs/04-operations.md)
- [GoFlow2](https://github.com/netsampler/goflow2)
- [nDPI](https://github.com/ntop/nDPI)

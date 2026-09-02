# 流量流向分析：需求归纳与方案选型

本文依据用户提供的 7 张“流量监测分析系统 V2.1.10”截图、watchdog 当前代码、仓库内置 Akvorado 源码以及 goflow2、ntop/nDPI 官方资料，定义流向分析模块的产品边界、六维分类、F1–F9 功能需求、数据源分工、方案选型和实施前置条件。详细实现见 [flow-module-design.md](flow-module-design.md)，宿主平台整理与模块契约见 [watchdog-platform-module-architecture.md](watchdog-platform-module-architecture.md)，落地跟踪见 [flow-module-tasklist.md](flow-module-tasklist.md)。

本文是需求与选型基线，不代表截图产品的内部实现。截图看不到的技术细节一律标记为“推断”或“目标设计”，不把推断写成既成事实。

## 1. 证据范围与阅读约定

### 1.1 证据等级

| 等级 | 含义 | 使用方式 |
|---|---|---|
| E1 | 截图直接可见 | 可作为界面和功能验收依据 |
| E2 | 由界面行为或领域逻辑推断 | 必须在实施前由产品或样本数据确认 |
| E3 | watchdog 目标设计 | 是本方案的选择，不声称参考产品已经如此实现 |

### 1.2 截图功能映射

| 编号 | 截图直接证据（E1） |
|---|---|
| S1 流量总览 | 总上/下行、六类卡片、迷你趋势、占比饼图、业务 × 六类表、1h/6h/12h/今天/周/月/自定义时间档 |
| S2 默认分析图 | 流量值/占比/差值、默认/本网按省/异网运营商/境外/VPN 分组、高峰时段、业务/运营商筛选、上下行双图、当前/95th/峰值/平均、导出 |
| S3 本网按省 | 按省多序列趋势和 tooltip 排名 |
| S4 异网运营商 | 移动、联通、教育科研网、腾讯、清华大学等机构级分组，说明它不只是三大运营商 |
| S5 境外专题 | 总量、境外 IP 数、涉外内网主机数、疑似 VPN 占比、出入境趋势、端口/协议、境外地区 |
| S6 VPN 专题 | 疑似主机、VPN 总量、活跃端口、高风险主机、趋势、端口分布、类型分布、Excel 导出 |
| S7 源 IP 分析 | Top IP、上/下行速率、六类分解、sparkline、搜索、业务筛选、详情、修正归属地；左侧菜单另有目的 IP 分析 |

截图未直接展示设备导出配置、本地网段配置、采样算法、IP 库来源、VPN 评分算法、告警规则和后端技术栈。这些均不能仅凭截图断言。

### 1.3 范围

本期范围是“基于 flow 元数据的流向、归属和结算分析”：

- 接收 sFlow v5、NetFlow v5/v9、IPFIX 和兼容的 NetStream v9；
- 以独立 `flow-collect` 完成 UDP 接收、GoFlow2 解码、采样归一和本地 raw WAL，再把 normalized batch 写 Kafka 交给可横向扩展的 dimension workers；
- 识别业务上行/下行、本地端与对端；
- 用中国省市、运营商/机构、ASN 数据做六维分类；
- 提供总览、趋势、Top IP、境外和启发式 VPN 分析；
- 作为 watchdog 可插拔业务模块，复用宿主的用户/RBAC、collector/target、统一查询、图表 CRUD、统计导出、三层修正和审计；
- 与现有 SNMP 接口计数和 95 计费体系可选协作。

不在 P1–P3 基线内：全包留存、阻断/封禁、精确会话重放、NAT 前后地址关联、跨采集点通用 flow 去重、基于载荷的强制 L7 识别。这些必须有额外探针或设备字段才能实现。

## 2. 方向模型与术语

### 2.1 两种“方向”不能混用

1. **观察方向**：flow 报文中的 `in_if/out_if`，表示报文从采集设备哪个接口进入、从哪个接口离开。它用于定位观察点、上联和防止重复统计。
2. **业务方向**：相对配置的本地网段而言，`src` 为本地、`dst` 非本地是 `out`（上行/出向）；反之是 `in`（下行/入向）。六维分类和截图中的上下行使用这一口径。

watchdog 当前 sFlow 原型只实现了观察方向，目标模块必须新增业务方向；两个字段都要保留，不能用一个覆盖另一个。

### 2.2 本地端/对端归一化

| 源 IP | 目的 IP | `business_direction` | `local_ip` | `remote_ip` | 处理 |
|---|---|---|---|---|---|
| local | non-local | `out` | src | dst | 进入六维分类 |
| non-local | local | `in` | dst | src | 进入六维分类 |
| local | local | `internal` | src | dst | 默认入库计数（`internal_policy=count`），不进入六类；可配置 drop |
| non-local | non-local | `transit` | 无 | 无 | 默认仅计指标不入明细（`transit_policy=drop`，边界设备上双非本地流量可能极大），不进入六类；可配置 count 入库 |
| 无法解析/地址缺失 | 任意 | `ambiguous` | 无 | 无 | 丢弃明细并记录质量指标 |

本地网段由租户的 `address_prefixes.labels.flow="local"` 最长前缀匹配决定。业务标签由本地 IP 的 `labels.business` 决定。重叠前缀以最长前缀为准。

### 2.3 统计单位

- sFlow flow sample 使用**该条样本携带的** `sampling_rate` 归一，不能使用设备级或平台级统一倍率；NetFlow/IPFIX 按记录/采样器 option 和已确认的 counter 语义归一；
- 速率统一为 `bps = estimated_bytes × 8 / bucket_seconds`；
- 总量单位为 bytes/TB，速率单位为 bps/Gbps/Tbps；
- 95th 使用完整 5 分钟桶的 nearest-rank P95，与 watchdog 现有 `Percentile(..., 95)` 语义对齐；缺失桶不补零，完整率必须同时展示；
- AS、IP 段、地域、运营商和六类流向均由 flow 独立计算，不依赖 SNMP；已有计费场景仍以 SNMP 接口计数为总量真值，flow 估算只用于结构拆分和可选对账。

### 2.4 sFlow 采样与计数语义

sFlow v5 的 flow sample 包含 `sampling_rate`、`sample_pool`、`drops`、sample sequence、data source 和 `in_if/out_if`。设备可以让不同 data source/端口使用不同采样率，因此平台状态和质量统计至少按 `(agent_address, sub_agent_id, source_id)` 隔离，不能把 CLI 读取的设备级采样率应用到所有样本。

基础估算逐条执行：

```text
estimated_bytes_i   = sampled_frame_bytes_i × sampling_rate_i
estimated_packets_i = sampled_packets_i × sampling_rate_i
```

`sample_pool` 是该 data source 已观察的数据包累计量。样本池单调、没有重启/回绕且窗口样本数足够时，可计算窗口有效倍率 `pool_delta / received_flow_samples`，用来识别随机采样波动、采样率切换和 UDP 样本丢失；任何 corrected 结果都必须与 nominal 原始估算并存，不得静默覆盖。发生 reset、wrap、source 或 rate 变化时关闭当前 epoch，退回逐样本 `sampling_rate`。sequence gap 单独标记质量；若 gap 前后 rate 不变且 pool 连续，可生成带 `transport_loss_compensated` 标记的 corrected 候选，否则该窗口不校正。

sFlow counter sample 与 flow sample 是两条独立语义：counter sample 是接口累计 octets/packets，不乘 sampling rate；CLI 的 polling `interval` 只决定 counter sample 大致多久发送一次，也不是 flow 字节的倍率。线速按相邻 counter 的实际时间差计算，不能假定每次实际间隔都等于 CLI 配置值。

## 3. 六维分类学

### 3.1 分类输入

六维分类只看归一化后的 `remote_ip`，输入为：

- `remote_country`：ISO 3166-1，两位国家码；
- `remote_admin_code`：中国地址使用 6 位 GB/T 2260，尽量到市；
- `remote_isp_id`：地址库导出物中的稳定运营商/机构 ID，0 表示未知；
- `home_province`、`home_city`：租户确认的本省/本市；
- `home_isp_ids`：本运营商/本机构 ID 集合，允许多选以支持双上联；
- `overseas_includes_hmt`：港澳台是否按境外统计的租户策略。

上行按目的 IP 归属、下行按源 IP 归属，是“对端归属”规则在两个方向上的同一表达。

### 3.2 有序判定规则

| 优先级 | 条件 | `category` | 中文名 |
|---:|---|---|---|
| 1 | 非中国，或命中租户配置的港澳台境外口径 | `overseas` | 境外流量 |
| 2 | 本网且对端省=本省且市=本市 | `on_net_local_city` | 本网本市 |
| 3 | 本网且对端省=本省且市≠本市 | `on_net_cross_city` | 本网跨市 |
| 4 | 本网且对端省≠本省 | `on_net_cross_province` | 本网外省 |
| 5 | 异网且对端省=本省 | `off_net_in_province` | 异网本省 |
| 6 | 异网且对端省≠本省 | `off_net_cross_province` | 异网外省 |

无法完成判定的记录进入 `unknown`，不强塞进六类：

- 国家未知时无法判断境内/境外；
- 中国地址缺省级码时无法判断本省/外省；
- 对端运营商未知时无法判断本网/异网；
- 本网且同省但缺市级码时无法判断本市/跨市。

`internal`、`transit`、`ambiguous` 和 `unknown` 是质量/拓扑维度，不是六维分类的第七类。六类占比的分母是六类可分类字节之和；unknown 占全部业务方向 flow 的比例单独展示。已配置独立 counter 基准时，总览同时给出 flow 与 counter 的偏差，避免“六类相加正确”掩盖漏采或重复采集；未配置时明确标记“无独立总量对账”，但不阻塞 AS/IP/地域/六维分析。

### 3.3 机构口径

截图 S4 中同时出现运营商、教育科研网、云/CDN 和组织名称，因此“异网运营商”应实现为 `ISP/AS organization` 维度，而不是固定三大运营商枚举：

- 一级使用地址库导出物中的稳定 `isp_id`；
- 展示名取导出物 `operators.json` 中的简称/名称；
- 未归入机构但 ASN 已知时回退为 `AS{n} · ASN 名称`；
- ASN 和机构都未知时使用“未知机构”，并纳入数据质量统计。

### 3.4 地址段统计维度与汇聚口径

地址段归类是统计计算，不得放进 UDP 接收、GoFlow2 解码或采样归一的成功条件。可靠解码和采样归一后的每条记录先进入 Kafka normalized topic，独立 dimension worker 再异步执行前缀匹配、address set、Geo/ASN、业务和六维归类，最后写分钟汇聚。这里的“异步”是 Kafka partition + 固定 worker + 有界聚合状态，不是逐条启动 goroutine。dimension worker 变慢或重启时先增加 normalized lag；Kafka 生产故障由 flow-collect 本地 raw WAL 承接。恢复后按 offset/WAL 重放，只有 normalized retention 与 WAL 的联合保护窗口被耗尽时才允许形成带精确范围的数据丢失事件。

统计必须区分两种维度：

- **主地址段 `primary_prefix`**：对 src/dst 或 local/remote endpoint 做最长前缀匹配，每个 address role 最多命中一个 prefix；未命中记 `_unassigned`。同一 role 下各 prefix 互斥，因此汇总必须等于该 role 原始总量；
- **地址集合 `address_set`**：现有 selector 可让一个 prefix 同时命中多个集合，属于 tag 口径。单个集合内部可汇总，不同集合之间不可相加，API 必须返回 `additive=false`，避免重复计量。

prefixes、labels、sets 和 selector 需发布为不可变 `dimension_version`。排队记录按 flow `event_time` 选择当时生效版本，不能因为异步延迟套用消费时的最新配置。响应和导出必须带 dimension version；变更后的重分类通过 normalized topic 或仍在 TTL 内的 base fact 异步回放完成。

## 4. F1–F9 功能需求

### F0 宿主平台与模块化前置

F1–F9 是 flow 业务能力，不应再各自建设一套用户、agent、target、图表和导出系统。进入 Flow P1 前，watchdog 至少满足：

| ID | 要求 | 验收要点 |
|---|---|---|
| F0.1 | 生产 API 使用同一身份源，具备租户、用户、角色和授权 CRUD | hub 生产路由实际挂载 watchdog API；禁用用户/角色变更立即生效 |
| F0.2 | module/resource/dataset registry 统一注册后端路由、worker、资源父子关系、查询 provider 和前端入口 | flow 禁用后停止入口与 worker 但不删数据；模块不能绕过 tenant/RBAC |
| F0.3 | 通用 collector registry 支持一次性 enrollment、token/mTLS、轮换、heartbeat、plan version 和 M:N resource binding | 一个 flow collector 可绑定多个 exporter/target；撤销后旧凭据不能继续取 plan |
| F0.4 | target kind、网元/target CRUD、列表、详情和关联影响预览由宿主提供 | flow exporter 引用平台 target；删除 target 前列出 binding/图表/导出影响 |
| F0.5 | QueryGateway 和 DatasetRegistry 支持 VM/CH provider，图表定义支持 CRUD、数据集查询和资源绑定 | 浏览器不直连存储；保存的 query 经过字段/RBAC/上限校验 |
| F0.6 | 统计和导出任务按 dataset/query snapshot 工作，不固定为 target/port/VM/CSV | flow 可复用 CSV/XLSX 异步导出，重试保持同一查询和版本 |
| F0.7 | 明确 `raw/supplier/customer` 三个平行数据层及分层查看/导出/修正权限 | raw 不可覆盖；两类修正规则可版本化、审批、审计和复现，禁止随机修正 |
| F0.8 | schema 以版本 migration 为唯一来源，init 与 migration 有 CI 一致性检查 | 新旧 agent/graph/export 数据迁移可回滚，不长期保留双写 |
| F0.9 | 所有管理对象采用统一资源生命周期、乐观锁、幂等键、审计和删除预览 | create/list/get/update/activate/suspend/retire/restore/purge 及重复请求、版本冲突、依赖占用均有确定响应 |
| F0.10 | 所有异步操作采用统一 job lifecycle、checkpoint、取消/重试和结果证据 | API 超时不等于任务失败；重试不重复执行；调用方可判断 queued/running/succeeded/failed/canceled |
| F0.11 | 建立 desired-state reconciler，持续检查 Kafka topic、数据库 schema、模块/collector plan 和派生完整性 | 漂移可检测、可告警、可安全修复；不得自动缩分区、降副本或 drop 数据 |
| F0.12 | 数据、配置、导出、秘密和审计分别定义保留、归档、冻结、删除与验证流程 | tenant/target/exporter 删除不会留下无主凭据或不可解释事实，也不会误删仍在保留期的数据 |
| F0.13 | API 和数据集统一参数、错误码、分页、排序、ETag/If-Match、Idempotency-Key、dry-run 和 completeness envelope | 客户端不靠猜测区分空数据、无权限、版本冲突、依赖不可用或部分结果 |
| F0.14 | 功能从 experimental/canary/GA 到 deprecated/removed 有兼容窗口、遥测和回滚门 | feature flag 不能绕过 migration/RBAC；废弃 API/schema 有使用量证据和删除测试 |

F0 的完整改造边界、表结构和分期见宿主平台文档。它是 flow 的实施门，不要求一次大重写：先完成生产身份/API、registry 和通用 collector，再逐步兼容迁移现有 SNMP/system 能力。

### F1 采集、观察点与方向判定

| ID | 要求 | 验收要点 |
|---|---|---|
| F1.1 | 独立 flow-collect 使用 GoFlow2 接收/解码 sFlow v5、NetFlow v5/v9、IPFIX；做来源准入、raw WAL、模板/采样状态和 normalized Protobuf batch；NetStream 只承诺兼容标准 v9/IPFIX 字段 | flow-collect 不连接 CH/VM/业务 MySQL；四类 fixture 可进入 normalized topic；Kafka 不可用时 WAL 承接且数据损失可见 |
| F1.2 | 平台 collector 身份/凭据与 flow exporter 协议身份分离；exporter 绑定 target、collector、协议/observation domain、last seen、采样率和健康状态 | 未登记来源只进限速 quarantine metadata，不入分析库；collector 和 exporter 均可独立停用 |
| F1.3 | sFlow 按每条 flow sample 的 rate 归一，并按 agent/sub-agent/source 隔离 sample pool/sequence 状态；NetFlow/IPFIX 正确处理 sampling option，并确认 sampled 或设备已 pre-scaled；override 仅用于明确的异常 exporter/data source | 两端口 1:1000/1:10000 fixture 不会被统一倍率；不重复放大 pre-scaled 计数；缺失/零 rate 不默认为 1；配置变更有审计 |
| F1.4 | 记录 sFlow sample-pool reset/rate change、序列缺口、exporter drops、模板等待、解码失败、Kafka produce/lag、WAL、DLQ、CH sink 和 VM pipeline 监控失败 | 每类故障有低基数指标和告警门槛；异常窗口可定位到 collector/exporter/data source/topic partition |
| F1.5 | 本地网段最长前缀决定业务方向；观察接口方向另存 | 四种 src/dst 本地组合单测全部通过 |
| F1.6 | observation interface allowlist/role 防止同一流经多设备、多接口重复计量 | 双观察点测试不会把总量翻倍 |
| F1.7 | IPv4/IPv6 全链路支持 | IPv4、IPv6 fixture 均可分类和查询 |
| F1.8 | flow-collect 对同一 exporter/observation domain 保持稳定 decode ownership；同一 datagram 拆出的全部 normalized child batches 获得 Kafka ack 后才推进 WAL checkpoint | 模板/采样状态不跨 worker 错配；重启从模板安全点重放；子批次可独立重试且不会漏 record，最终完整收敛；跨 session ID 稳定 |
| F1.9 | 独立 dimension worker 消费 normalized topic，按 event-time 版本异步完成方向、地址段/address set、Geo/ASN、业务、六维和分钟汇聚；enriched base 写入后才提交 normalized offset | 归类/CH 故障只形成 lag、不丢 flow；重复消费不重复分钟计量；派生表可从 base 重建；支持按 dimension version 重放 |

NetFlow/IPFIX 模板状态必须按 exporter + observation domain 隔离。`counter_mode=auto` 只可用于 pending/探测；NetFlow/IPFIX 在确认 `sampled|pre_scaled` 前不得 active。NAT 场景若设备未导出原始/转换后地址，P1 不承诺还原；开通时需确认本地地址在所选观察点可见。

### F2 归属富化、自动识别与修正

| ID | 要求 | 验收要点 |
|---|---|---|
| F2.1 | 对端 IP 得到国家、省、市、ASN、运营商/机构和地址库版本 | 同一版本下查询可复现 |
| F2.2 | 读取离线导出的版本化地址库；文件切换失败时保留旧版本 | checksum、格式、区间校验通过后原子热加载 |
| F2.3 | 自动识别本省、本市和本运营商候选，按近 24h 可分类流量加权 | 只生成候选；管理员确认后才改变分类 |
| F2.4 | 本运营商支持多选 | 双上联候选可以同时应用 |
| F2.5 | IP 行可发起 watchdog 租户级归属覆盖；显示生效范围、修改前后、提交人和原因 | 覆盖写入本地 `address_prefixes` 并审计，不回写外部地址库 |
| F2.6 | 未识别归属可搜索、导出和按占比告警 | 总览显示 unknown 比例和钻取入口 |
| F2.7 | address prefixes/sets 发布不可变维度快照，展示 version、effective time、worker ack、积压和可回算范围 | 未经 publish 的 draft 不影响统计；异步记录按 event time 命中正确版本；发布失败保留旧版本 |

地址库由外部工具编辑并导出，watchdog 不连接其数据库。导出物必须已经扁平化 base 与修正层，并携带 6 位 `admin_code`、机构字典、版本和 checksum；缺少市级行政码时只能完成省级和境外分类，不能验收“本市/跨市”。

### F3 六维总览

- 总上/下行当前速率和趋势；
- 六类 3×2 卡片：上/下行、sparkline、六类占比；
- 六类占比饼图、unknown 数据质量条；
- 业务 × 六类交叉表；
- 时间档：1h、6h、12h、今天、本周、本月、上月、自定义；
- 展示活跃 exporter 数和数据新鲜度；
- 始终展示 flow 估算总量；配置了可比 SNMP 边界端口时，再展示 SNMP 基准和偏差百分比，SNMP 不得成为页面或分类计算的硬依赖。

### F4 多维分析图表

- 分组：六类、主地址段、地址集合、本网按省、异网机构、境外地区、VPN 类型；
- 模式：流量值、占比、差值；
- 筛选：业务、设备/exporter、上联机构、方向、午/晚高峰和时间范围；
- 上/下行独立多序列图、brush 缩放、图例联动、tooltip 排名；
- 当前、P95、峰值、平均和数据完整率；
- 图表定义、查询 JSON、资源绑定和布局复用平台 visualization CRUD，而不是 flow 自建第二套图表元数据；
- 图表图片导出和数据 CSV/XLSX 导出。

“差值”在截图中可见但口径不可见。目标设计暂定为同时间桶 `out_bps - in_bps`，API 返回 signed bps；产品验收前必须确认它是否实际指环比/同比差值。

### F5 业务维度

- IP/CIDR → 业务标签，支持 IPv4/IPv6 和最长前缀；
- 全局业务筛选贯穿总览、图表、IP、境外和导出；
- 业务 × 六类交叉表提供流量值与行内占比；
- 未标记本地 IP 归入“未分配业务”，不丢弃；
- 标签变更只影响新数据，历史回算属于 P4。

### F6 源/目的 IP 分析

- 源、目的两个 endpoint side 独立切换，而不只是“本地 IP”；
- Top-N、精确 IP/CIDR 搜索、业务/六类/方向筛选、总条数；
- 每行显示上/下行、sparkline、六类值和占比；
- 详情显示时序、Top 对端、Top 端口/协议、当前归属及其版本；
- 支持 CSV/XLSX 导出；
- 有权限的管理员可以从任一公共 IP 发起归属修正。

### F7 境外专题

- KPI：境外估算总量、平均速率、独立境外 IP、涉外本地主机、疑似 VPN 占比；
- 出境/入境趋势，合并/上行/下行切换；
- 国家/省州/城市（地址库有城市时）分布；
- 端口/协议分布，命中 VPN 规则的端口标红；
- 支持业务、设备、方向和 Top-N 筛选。

### F8 VPN 识别

VPN 能力采用“候选 → 异步取证 → 证据归并”三阶段，不让探测阻塞 flow 收数、归类或查询：

1. **Flow 候选与评分**：组合 src/dst 境内外、业务方向、本地/对端端口、TCP/UDP、长连接、周期性、远端离散度、多 transport 组合、流入/流出比例和单向性；只有 flow 携带可靠起止时间时才使用 duration。`UDP/443` 只能产生 `quic_candidate`，`TCP/443` 只能产生 `tls_candidate`，端口本身不能确认应用协议。
2. **地址情报条件**：管理员可按 address set/CIDR、ASN、机构、route class（包括自定义 `cn2` 标签）配置加分、合法用途 allow、误报 suppress 或触发探测，规则必须包含来源、原因、owner、版本、有效期和命中范围。CN2、某 ASN 或境外地址本身不被平台内置为高危。
3. **异步取证**：高分候选可触发旁路被动观察或经审批的主动握手探测。被动观察只在镜像/TAP 上捕获后续匹配会话的有限握手；主动探测会从受控探针连接目标，只允许授权 CIDR/端口、低速率、无凭据爆破、无漏洞利用并在握手后停止。两种模式必须在 UI/API 中明确区分。

方向行为按相同 local/remote conversation 和完整窗口计算：

```text
balanced_ratio = min(local_to_remote_bytes, remote_to_local_bytes)
                 / max(local_to_remote_bytes, remote_to_local_bytes, 1)
unidirectional  = balanced_ratio <= configured_unidirectional_ratio
balanced        = balanced_ratio >= configured_balanced_ratio
```

阈值不硬编码；必须同时满足最小 bytes/packets/flow count、唯一观察点和完整率要求。接近 1 的双向流、接近 0 的单向流都只是行为特征，备份、视频、CDN、遥测、扫描、被采样漏掉反向包等均可能产生相同形态，不能单独判 VPN。

证据等级分开：

- `heuristic`：只有 Flow/地址情报特征，统一显示“疑似”；
- `corroborated`：被动 DPI/指纹或主动探测得到相符但非唯一证据；
- `confirmed_protocol`：得到可复现、协议特异的握手响应，只说明检测到 SOCKS/HTTP CONNECT/WebSocket/OpenVPN 等协议，不等于恶意；
- `inconclusive`：超时、拒绝、证据冲突或加密协议无法无凭据确认。

SOCKS4/5、HTTP CONNECT、WebSocket Upgrade 等可以用明确握手响应确认。TLS 证书、ALPN 或普通 banner 只能作为 `corroborated`，只有厂商/协议特异且可复现的响应才能提升为 `confirmed_protocol`。Trojan、Shadowsocks/SS、VMess 等加密/伪装协议通常没有通用的无凭据确认握手，主动探测不能靠猜密码或异常 payload 强行“确认”，应结合被动 TLS/QUIC 指纹、包长/时序和情报只给 `corroborated/inconclusive`。nDPI 可提供 SOCKS、WebSocket、OpenVPN、WireGuard、QUIC 及 TLS/QUIC 风险/指纹等旁路证据，但普通五元组 flow 无法补回载荷；官方列出的 `RISKY_ASN` 也只是由上层应用填充的占位风险，而非 nDPI 自动判断。

每个 finding 返回 conversation、窗口、双向 bytes/ratio、duration/flow count、远端 fanout、周期性、地理/ASN/地址情报、score/level、机器 `verdict`、人工 `disposition`、命中规则、probe job/status、analyzer/version 和脱敏证据。KPI、趋势、主机表、端口/transport/类型分布及导出均使用相同 finding snapshot；规则、finding 处置、探测、完整 IP 证据和导出分别鉴权并审计。

### F9 系统、权限和非功能

- flow 通过 module/resource/dataset registry 注册，不在总路由、导航栏和存储客户端中逐项硬编码；
- RBAC：查询和导出按 `raw/supplier/customer` 分层 action 校验；exporter/Geo/修正规则分别使用配置权限，修正规则另需审批权限；
- 所有查询强制 tenant filter，浏览器不得直连 VictoriaLogs/ClickHouse；
- collector enrollment/heartbeat/plan、exporter、地址库、raw WAL、normalized Kafka lag/DLQ、dimension version/worker ack、ClickHouse base/derived、VM、VPN score/probe queue/result freshness/kill switch 和最后数据时间有健康页；
- UDP 输入仅内网开放，支持源地址 allowlist 和速率限制；
- 明细/聚合 TTL、导出和 IP 数据访问均可审计；
- 每种事实、配置、snapshot、导出、DLQ、WAL、凭据和审计都必须有 owner、retention、archive/purge 条件、恢复方法和销毁证明；引用字典/snapshot 的保留期不得短于事实表；
- raw 是不可变事实；supplier/customer 是查询/导出时按已批准版本派生的两个平行视图，不能相互隐式级联；
- 输入保存 schema/version、原始计数、质量标记和稳定 ID；输出统一返回版本、as-of、完整率、degraded intervals、warnings 和可用时间范围，禁止把 partial 当 complete；
- 管理 CRUD 采用稳定 cursor、字段/排序白名单、ETag/If-Match、Idempotency-Key、删除预览、软删除/恢复和受控 purge；并发更新不能最后写入者静默覆盖；
- 权限最少区分 list/get/create/update/delete/restore/purge/operate/approve/export/read-sensitive/admin，列表计数、错误信息、导出文件和审计同样不能泄露跨租户对象；
- 每租户、collector、查询和导出都有限额/公平队列；降级必须优先牺牲 debug、backfill、长查询和低优先级导出，不能让单租户拖垮在线收数；
- passive/active probe 分 capability、action 和 audit；主动探测默认关闭，必须有授权 scope、专用 egress、DNS/IP pinning、速率/并发/超时/cooldown、全局 kill switch，且探测故障不得阻塞 flow 主链；
- probe agent 使用本机硬上限、签名 immutable plan、immutable job 三层配置交集；plan 有 revision/hash/expiry/ack/LKG、preview/canary/rollback 和兼容矩阵。配置不得远程下发脚本、动态库或任意握手 payload，analyzer/profile 通过版本化受控接口扩展；
- 高基数保护：Kafka 分区/lag/WAL、分钟 pair 上限、Top-K 降级、查询时间/点数/序列数上限；
- 可用性：dimension worker、CH 或 VM 暂时失败不能拖死 UDP 接收；由 normalized lag 隔离，Kafka 不可用时 flow-collect 有界 WAL 落盘，恢复后重放并报告溢出丢失范围；
- 性能指标不预先引用“单核可达某固定 flows/s”，必须按每个端口的线速、线上帧长、采样方向和采样率计算 sample records/s，再用真实 datagram 打包方式、Protobuf 大小和维度扩张压测。容量判断使用 records/s 和 bytes/s，不能用总带宽或开源项目的笼统 benchmark 代替。

本项目已知端口规模 `2×100G + 12×10G = 320G`。按 64B Ethernet frame 在链路上约占 84B 的保守场景，单向约 `476.2 Mpps`；若所有端口双向都采样，约 `952.4 Mpps`。因此 1:1000 时约为 `476k/952k samples/s`，Flow P1 的 2×持续压测目标应达到约 `0.95M/1.90M normalized records/s`；1:4096 时相应约为 `116k/233k samples/s`。最终目标需以实际每端口配置和 pcap/exporter 统计复核。

建议 SLO：批准实验室负载下 collector kernel/内部队列可观测 drop=0；生产任何非零 drop 都告警并记录受影响窗口，不以低于某百分比掩盖持续丢包。1h 常用查询 P95 <2s、24h <5s，配置热更新 <60s；部署实例数、磁盘和阈值在容量输入确认后冻结，但不得降低详细设计中的 2×持续、3×突发和 72h soak 发布门。

## 5. sFlow、NetFlow/IPFIX 与 SNMP 的角色

三条通路回答不同问题，是互补关系。

| 维度 | sFlow v5 | NetFlow v5/v9 / IPFIX / NetStream | watchdog SNMP 接口计数 |
|---|---|---|---|
| 数据模型 | 1:N 包样本，通常带截取包头和 in/out ifIndex；协议也定义 counter sample | 设备流缓存或采样记录；v9/IPFIX 用模板定义字段 | 周期轮询接口累计 octets/packets |
| 状态 | 解码较简单，但 sample pool/sequence 质量状态必须按 agent/sub-agent/source 隔离 | v9/IPFIX 必须维护 exporter/observation-domain/sampler 模板与采样状态 | 维护上次计数，处理回绕/重启 |
| 时延 | 通常秒级 | 受 active/inactive timeout 和模板到达影响 | 取决于轮询周期，当前通常 1 分钟 |
| L3/L4“谁到谁” | 有，取决于采样头 | 有，字段通常更丰富 | 无 |
| L7/载荷 | 仅当样本截取长度足够时有有限可能 | 普通五元组 flow 没有；IPFIX enterprise 字段可能携带探针结果 | 无 |
| 总量精度 | 估算，采样误差对小流明显 | 未采样时较准，采样时仍是估算 | 接口总量真值，但无法拆到 IP/地区/业务 |
| 设备负担 | 通常较低，适合交换机 | 流缓存和模板导出负担通常更高 | 低 |
| 本方案角色 | P1 优先接入的明细结构源，可独立完成 AS/IP/地域/六类分析 | 同一归一化模型的补充输入，可独立完成上述分析 | 现有计费总量真值和可选 flow 对账基准，不参与维度分类 |

SNMP 指标继续存 VictoriaMetrics，并不意味着 Flow 明细也应存 VM。SNMP 的序列集合由受管设备、端口、方向和固定 counter 名决定，规模可预估且 churn 低；Flow 的 IP × 对端 × 端口 × ASN × Geo 组合随通信动态变化，高基数且需要任意分组、明细导出和历史重归类。完整产品因此以 ClickHouse 为唯一 Flow 业务事实源；VM 只保存 SNMP/系统/pipeline 指标。少量 Flow 六类曲线即使未来复制到 VM，也只是压测证明必要后启用的可重建 recording cache，不属于采集成功条件。

### 5.1 无 SNMP 时能计算什么

只要 flow 记录包含可解析的源/目的 IP、可靠采样率并命中本地地址库，即可独立得到：业务上/下行、源/目的 IP 与 CIDR、源/目的 ASN、国家/省/市、运营商、六维分类以及这些维度的估算 bytes/bps/P95。SNMP 不含五元组或地址归属，不能生成这些分组，也不参与 Geo/ASN 分类。

因此 SNMP 不是 flow 模块开通的硬前置条件。没有 SNMP 时，页面显示 flow 估算值及采样质量，但总量缺少独立校验基准，不能仅凭“六类相加一致”证明没有漏采或重复采集。

### 5.2 SNMP 对账口径

“对账”不是拿 SNMP 给 AS/地域分摊，而是比较**同一观察范围的总量**。平台先把 sFlow 的 observation interface 与 watchdog 端口 `ifIndex` 绑定，再按同一设备、同一物理/LAG 边界、同一完整时间窗和同一观察方向比较：

- 样本的 `in_if=i` 估算量只与接口 `i` 的 SNMP `ifHCInOctets` 增量比较；
- 样本的 `out_if=i` 估算量只与接口 `i` 的 SNMP `ifHCOutOctets` 增量比较；
- 业务 `in/out` 与 SNMP `ifIn/ifOut` 不能仅凭名称直接对应；外网侧边界口通常同向，内网侧边界口通常相反，必须由观察点角色显式映射；
- LAG 必须选择逻辑聚合口或成员口之和中的一种，禁止两者相加；只比较共同覆盖的 IP 流量，非 IP/L2、设备自产流量和被过滤流量需从口径中排除或作为预期差异说明。

SNMP 原始累计 counter 按实际采集时间做差，不使用固定 polling interval：

```text
snmp_delta_bytes = counter(t1) - counter(t0)
snmp_bps         = snmp_delta_bytes × 8 / (t1 - t0)
coverage_ratio   = flow_estimated_bytes / max(snmp_delta_bytes, 1)
signed_deviation = (flow_estimated_bytes - snmp_delta_bytes) / max(snmp_delta_bytes, 1)
absolute_deviation = abs(signed_deviation)
```

只有 counter 连续、无设备重启/回绕歧义、时间窗完整且 observation mapping 唯一的桶才进入对账。对账至少使用完整 5 分钟窗，开通验收观察不少于 30 分钟；1 分钟值可展示，但不宜因随机采样波动直接告警。

偏差的主要诊断含义是：接近 `-100%` 常见于采样率缺失或采集丢失；接近 `+100%`（flow 约为 SNMP 两倍）常见于重复观察；持续固定比例偏差常见于倍率或 L2/L3 字节口径不同；只在单接口异常常见于 ifIndex/LAG 映射错误。

基础对账公式为：

```text
deviation = abs(flow_estimated_bytes - snmp_delta_bytes) / max(snmp_delta_bytes, 1)
```

偏差不能自动通过一个全局系数“修正”所有 AS、地域或六类。采样误差不一定在各分组中等比例，强行按 SNMP 总量摊平会制造看似精确的错误数据。若产品确需展示校准值，必须保留 nominal 原始估算、单独标记 corrected 值和适用窗口，计费仍不使用分类后的 flow 校准值。

## 6. 方案对照

### 6.1 Akvorado

仓库内置版本为 `v2.4.0-43-g02f33374`。其架构把快速接收和重处理分开：inlet 收 NetFlow/IPFIX/sFlow 后写 Kafka，outlet 用 GoFlow2 解码、通过 SNMP/静态数据/BMP 富化和分类，再批量写 ClickHouse；console 提供时序和 Sankey。它的 exporter/interface classifier、network attributes、可配置多分辨率 TTL 很适合作为 watchdog 的设计参考。

其 AS 流量计算链路不依赖 SNMP counter：sFlow decoder 把每条 flow sample 的 `SamplingRate` 和 `InIf/OutIf` 写入统一消息（[`decode.go`](../akvorado/outlet/flow/decoder/sflow/decode.go)）；ASN 来自 flow 自带字段，缺失时由网络前缀/BMP 属性富化；查询按 `SrcAS` 或 `DstAS` 过滤/分组，并以 `SUM(Bytes*SamplingRate*8)` 计算 L3 bits（[`clickhouse.go`](../akvorado/console/clickhouse.go)）。enricher 只在显式 override/default 时替换或补 rate，仍缺失就拒绝记录而不是按 1 计算（[`enricher.go`](../akvorado/outlet/core/enricher.go)）。因此某 AS/地址段/地域流量的本质都是“匹配该维度的样本估算量之和”；SNMP 只提供接口元数据或独立总量校验。

优势：成熟的多协议、模板状态、高吞吐分层、SNMP/BMP 富化、ClickHouse schema/migration、查询 UI。缺口：中国 GB/T 省市/机构结算六类、watchdog 业务/计费/RBAC 集成、截图式专题页。Akvorado 是 AGPL-3.0，直接复制或深度链接其实现必须先完成许可证评审。

### 6.2 goflow2 v3 + watchdog 自研富化

watchdog 已依赖 `github.com/netsampler/goflow2/v3`，当前原型使用其 sFlow decoder。GoFlow2 的定位是采集、解码和统一序列化，支持 NetFlow v5/v9、IPFIX、sFlow v5；存储、富化、图表和告警需上层实现。

优势：BSD-3-Clause、与现有 Go 代码和部署方式一致、可以精确实现六维口径。代价：模板状态、批处理、背压、CH schema、Geo 版本、API 和全部专题页都由 watchdog 负责。

### 6.3 ntopng + nProbe + nDPI

优势：包可见时提供成熟 L7、应用/VPN 和 flow risk；ntopng 也有 ClickHouse 历史 flow 浏览器和 SNMP 设备能力。限制：nDPI 是包分析库，普通 flow 五元组不能等价于 DPI；nProbe/ntopng 的高阶 SNMP、ClickHouse/历史分析和规模能力受版本/许可证约束；自定义中国六维和 watchdog RBAC/计费仍需二次集成。

适合定位：重点链路上的可选 L7 side sensor，将 `app_protocol/risk` 作为额外字段送入主线；不作为 P1 主管线。

### 6.4 从成熟项目吸收的工程闭环

本方案不只学习 Akvorado 的 flow 算法，还吸收其生命周期工程方法：

- orchestrator 对 Kafka topic 的存在性、分区数和配置做 desired/actual 对账，只允许安全扩分区并报告副本漂移（[`orchestrator/kafka/root.go`](../akvorado/orchestrator/kafka/root.go)）；
- ClickHouse 启动先校验服务版本，逐步比较实际 schema 与目标 schema，迁移失败指数退避重试，并区分单机/cluster 引擎（[`migrations.go`](../akvorado/orchestrator/clickhouse/migrations.go)、[`migrations_helpers.go`](../akvorado/orchestrator/clickhouse/migrations_helpers.go)）；
- raw、分钟和长周期表使用不同 resolution/TTL，运维文档包含空间诊断、TTL 物化和逐 partition 迁移，而不是只给一条建表 SQL（[`04-operations.md`](../akvorado/console/data/docs/04-operations.md)）；
- 配置加载拒绝未知 key、执行类型校验并为旧字段提供迁移 fixture；查询 filter 只有经 schema parser 验证后才能生成 SQL（[`config.go`](../akvorado/cmd/akvorado/config.go)、[`filter.go`](../akvorado/console/query/filter.go)）；
- ClickHouse 写入以 size/time 双阈值 batch，shutdown 有 grace period/final flush，并暴露 batch/overload 指标（[`outlet/clickhouse/config.go`](../akvorado/outlet/clickhouse/config.go)、[`worker.go`](../akvorado/outlet/core/worker.go)）。

watchdog 还必须补上 Akvorado 不是重点解决的多租户 CRUD、细粒度 RBAC、审批、软删除/恢复、幂等 API、任务取消和隐私 purge。学习原则是复用机制与测试方法，不复制 AGPL 代码，也不把“成熟项目没有”误解为“不需要”。

### 6.5 对照矩阵

| 方案 | 多 flow 协议 | 中国六维可定制 | L7/VPN | 与 watchdog 复用 | 新基础设施 | 许可证/商业约束 | 结论 |
|---|---|---:|---:|---:|---:|---|---|
| 完整部署 Akvorado | 强 | 中，需要改 schema/UI | 弱 | 中 | Kafka + CH + Akvorado 服务 | AGPL-3.0 | 参考/可做独立系统，不作为本次主实现 |
| goflow2 v3 + watchdog | 强 | 强 | Flow 启发式；可接异步 probe 证据 | 强 | Kafka + CH；VM/MySQL 已有，SNMP 可选 | BSD-3-Clause | **推荐主管线** |
| ntopng/nProbe/nDPI | 强 | 中 | **包可见时强** | 低/中 | 探针、ntop 组件、CH | GPL/LGPL + 版本许可证 | 可选 L7 传感器 |
| 维持当前 sFlow + VLogs | 仅 sFlow 子集 | 弱 | 弱 | 已存在 | VLogs | 无新增 | 仅原型/故障排查，不满足目标 |

## 7. watchdog 代码库现状审阅

### 7.1 可复用能力

| 能力 | 代码位置 | 现状 |
|---|---|---|
| sFlow v5 解码 | [`sflow_collector.go`](../internal/watchdog/sflow_collector.go) | 已解 IPv4/IPv6、五元组、采样率、in/out ifIndex |
| 独立 collector 进程 | [`watchdog-sflow-collector`](../cmd/watchdog-sflow-collector/main.go) | 独立于 API/hub，故障隔离方向正确 |
| 前缀最长匹配 | [`sflow_prefix_matcher.go`](../internal/watchdog/sflow_prefix_matcher.go) | BART 支持 IPv4/IPv6 LPM；可复用本地/业务标签 |
| 地址前缀/集合 | [`010_address_sets.sql`](../deploy/migration/mysql/010_address_sets.sql)、[`api_address_sets.go`](../internal/watchdog/api_address_sets.go) | 已有租户化 CIDR+labels CRUD |
| SNMP 口计数 | [`snmp-collector-design.md`](snmp-collector-design.md) 及 `internal/watchdog/snmp_*` | 接口库存、计数率、设备/端口映射较完整 |
| VM 查询和图表 | [`victoriametrics.go`](../internal/watchdog/victoriametrics.go)、[`aggregate-charts.tsx`](../internal/site/src/components/routes/aggregate-charts.tsx) | 时间范围、聚合、图表组件可复用 |
| P95/计费/导出 | [`export.go`](../internal/watchdog/export.go)、[`billing_metrics.go`](../internal/watchdog/billing_metrics.go) | 有 5 分钟 P95、账期、CSV；目前资源范围仍以 target/port 为主 |
| RBAC/审计 | [`api.go`](../internal/watchdog/api.go)、`audit_logs` | 可复用 Action/tenant 约束和审计表 |

### 7.2 当前原型的关键缺口与风险

| 级别 | 现状 | 影响/要求 |
|---|---|---|
| P0 | 仅注册 sFlow socket，没有 NetFlow/IPFIX 模板解码 | F1.1 不满足；需 goflow2 v3 统一接收层 |
| P0 | `findDevice()` 每包列出设备并以 `SysName/SysDescr == agentIP` 猜测；单设备时无条件兜底 | 会错绑 exporter，且每包访问 DB；改为内存 exporter registry |
| P0 | `resolvePort()` 每 sample 查询全部端口，忽略错误；有 in/out 时偏向 out | 热路径不可扩展，观察方向也可能失真；改为版本化内存接口索引 |
| P0 | `writeFlowToVLogs()` 每条 flow 启 goroutine 发 HTTP，未关闭响应体、无有界队列/重试 | 高流量下会耗尽 goroutine、连接和内存；必须删除这种逐条异步写法 |
| P0 | [`flow-search.tsx`](../internal/site/src/components/routes/flow-search.tsx) 硬编码浏览器直连 `127.0.0.1:9428`，没有后端 tenant filter | 远程不可用且存在租户隔离/安全风险；所有查询必须经 watchdog API |
| P0 | hub 生产 server 尚未形成“统一身份适配后挂载 watchdog backend API”的闭环，`APIV1RouterConfig` 也需手工注入每个 repository | 先完成生产 AuthContext adapter 和 module registry，否则 flow 只能在开发 runtime 工作或继续扩张硬编码 |
| P0 | `target_agents` 强制一个 target，agent type 只允许 snmp/system | 不能表达独立 flow collector 服务多个 exporter/target；迁移为通用 collector identity + M:N binding |
| P1 | 当前方向只由 in/out ifIndex 推出，不看本地网段 | 不能生成业务上/下行和六类 |
| P1 | VM 聚合只保留第一个 address set，未执行 `match_direction` | 多标签和方向语义丢失 |
| P1 | flush 前先清空内存；VM 写失败即永久丢失 | 需有界批队列、失败策略和丢失区间指标 |
| P1 | 指标名 `watchdog_sflow_traffic_bytes` 实际是每 flush 区间字节 gauge | 语义易误用；目标改为明确的 bps gauge 和自监控 counter |
| P1 | collector 由静态配置固定一个 tenant，未知 exporter 无法安全自动归属租户 | 由宿主 enrollment 下发 tenant/module/binding plan；collector 不接受报文自报 tenant，未知来源只进限速 quarantine metadata |
| P1 | sFlow flow sample 已解出 rate，但 `sampling_rate=0` 会在 [`sflow_collector.go`](../internal/watchdog/sflow_collector.go) 静默改为 1；未保留 sub-agent/source/sample-pool/drops，counter sample 也未接入 | 会严重低估异常样本，也无法识别每端口不同 rate、样本池变化或使用 sFlow 自带接口 counter；新管线必须拒绝/降级并补状态字段 |
| P1 | 没有 sFlow collector 单测、丢包/序列/双端口不同采样率/方向 fixture | 新管线必须先补测试再替换 |
| P1 | 当前指标 API 要求 target/port，export task 只支持 target/port+CSV | 只能复用底层 VM client/P95/格式化，不能声称 flow API/导出“直接可用” |
| P1 | aggregate graph 只绑定 port，metric catalog/查询固定 VM；没有 dataset/visualization provider | flow 高基数 CH 图表无法作为插件接入；先完成 QueryGateway、DatasetRegistry 和通用 visualization CRUD |
| P1 | 当前修正只有 `raw/corrected`，supplier/customer 主要改变查询步长，且算法可做确定性随机增减 | 不满足三种业务口径；迁移为 raw/supplier/customer 平行层和确定性、版本化、可审批规则 |
| P2 | VictoriaLogs 是逐条调试存储，没有 ClickHouse 高基数聚合、Geo/ASN/六类 | 保留为可选短期诊断，不作为分析真值 |

因此，本项目不是从零开始，但当前 sFlow 代码只能算原型。最值得复用的是 goflow2 v3 依赖、独立进程形态、前缀 LPM、SNMP/VM/P95/RBAC 基础；热路径和分析存储需要重做。

## 8. 选型结论与 watchdog 映射

```text
设备 sFlow / NetFlow / IPFIX
  → watchdog-flow-collect（UDP、来源准入、raw WAL、GoFlow2 v3 解码、模板/采样、归一）
  → Kafka normalized topic（可靠事实边界，维度计算可独立积压/重放）
  → watchdog-flow-dimension-worker（event-time 版本的地址段/set/Geo/ASN/业务/六维）
  → ClickHouse enriched base → 可重建的地址段/Top IP/地区/端口派生表
  → watchdog flow module：向宿主注册资源、数据集、API、worker、页面
  → watchdog core：统一身份/RBAC、collector/target、查询、图表、导出、三层修正、审计
  → 6 个页面

sFlow counter sample ────────────────────→ 可选同通道接口总量对账
SNMP 接口 counter（可选）───────────────→ 独立总量对账与现有计费真值
SNMP/system/pipeline metrics ────────────→ 现有 VictoriaMetrics
注册 flow_probe agent ── passive nDPI/其他 analyzer 或受控 active handshake
                       ──→ P3 finding 证据归并，不改变主管线提交条件
```

决策：

1. 延续独立 flow-collect，而不是把 UDP/高基数数据面塞进 hub/API 进程；
2. Kafka-compatible MQ 是 flow-collect 与 dimension worker 之间的强制边界，不允许 flow-collect 直写统计存储；raw 重放由本地 WAL 负责，不再建设第二个 raw topic 和独立解码服务；
3. 使用仓库现有 GoFlow2 v3 在 flow-collect 内接收/解码；统计维度归类必须异步消费 normalized topic；
4. flow 是 watchdog 可插拔模块，先按 [宿主平台契约](watchdog-platform-module-architecture.md) 整理用户权限、collector、target、query/visualization/export 和三层修正；
5. ClickHouse 是唯一 Flow 分析事实源和默认查询源；VM 继续承担 SNMP/系统/pipeline 的稳定低基数序列，不默认双写 Flow 业务曲线；SNMP 是现有计费事实源和可选独立总量校验，不是流向分类依赖；
6. Akvorado 只借鉴 inlet→Kafka→outlet、富化/分类/多分辨率思路，不复制 AGPL 实现；
7. VPN P3 同时冻结 Flow 候选、异步 probe job/result 和证据分级契约；没有镜像或主动授权时仍可运行 flow-only heuristic。nDPI 只是可选的被动 analyzer，受控主动握手由独立 capability、权限和 scope 管理，二者都不进入 Flow 收数热路径；P4 只升级 analyzer/指纹能力，不重做 P3 契约。

## 9. 实施前置条件与决策门

| 门槛 | 必须确认/完成 | 未完成影响 |
|---|---|---|
| GP 宿主平台 | 完成生产身份/API 闭环、module/resource/dataset registry、通用 collector/target、统一 query/visualization/export；三层修正最迟在相关查询/导出开放前完成 | flow 会复制管理框架或绕过 RBAC，不能作为插件验收 |
| GMQ Kafka/WAL | 确认 Kafka-compatible 产品/版本、normalized/collect-state/DLQ/quarantine/checkpoint topic、broker/分区/副本/retention、TLS/ACL、WAL 介质/容量、监控和灾备；完成模板 failover、WAL replay、normalized lag 与 base dedup 压测 | 独立 flow-collect 和异步维度汇聚无可靠解耦、重放与背压边界，Flow P1 不得上线 |
| G0 设备能力 | 具体型号/固件支持的 sFlow/NetStream/IPFIX、采样率字段、模板、导出超时、IPv6 和观察接口配置 | 不能承诺协议、精度和时延 |
| G1 观察点 | 明确只统计哪些 border/interface，验证无重复观察；确认 NAT 前后哪个地址可见 | 总量可能翻倍或方向无法判断 |
| G2 本地/业务网段 | 完整 CIDR、重叠优先级、IPv6、业务标签、address set selector、dimension snapshot 发布和未命中策略 | 方向、地址段汇聚和业务维度不可用 |
| G3 Home profile | 本省、本市、一个或多个本运营商由管理员确认 | 六维不可计算 |
| G4 地址库导出 | EdgeManager 或其他工具产出自包含、保留 `admin_code` 的版本化 `flow-geo-v1` 文件；watchdog 只有文件读取权限 | 本市/跨市不可验收，数据面不得依赖外部数据库 |
| G5 ClickHouse | 确认版本、容量、备份、TTL、磁盘水位和迁移责任；用真实采样压测 | 高基数明细不可上线 |
| G6 许可证 | 记录 goflow2 BSD、Akvorado AGPL 参考边界、nDPI/ntop 组件及商业版本选择 | 不得发布或引入相关组件 |
| G7 隐私安全 | IP 数据保留期、导出权限、审计、镜像/DPI 合法授权 | F6/F7/F8 不得开放 |
| G8 产品待确认 | “差值”口径、港澳台口径、VPN 风险阈值、95 完整率、unknown 目标值 | 对应验收项暂不能冻结 |
| G9 VPN probe | 被动模式确认 TAP/镜像位置、候选后的观察窗口和数据最小化；主动模式确认目标 CIDR/端口、网络出口、审批、独立权限、并发/速率/超时、DNS-IP pinning 和全局 kill switch | 未满足部分保持关闭；只降级为 flow-only heuristic，不阻塞六维流向与主管线 |

SNMP 未列为前置门：没有 SNMP 仍可上线 flow 的 AS/IP/地域/六类分析，但验收必须明确标注“未配置独立总量对账”，且不得宣称 flow 估算等同接口计费值。

## 10. 官方参考

- [Akvorado README](https://github.com/akvorado/akvorado)；[配置与 classifier/network 属性](https://github.com/akvorado/akvorado/blob/main/console/data/docs/02-configuration.md)
- [GoFlow2 README](https://github.com/netsampler/goflow2/blob/main/README.md)；[协议字段映射](https://github.com/netsampler/goflow2/blob/main/docs/protocols.md)
- [nDPI FAQ（包数、VPN、风险能力边界）](https://github.com/ntop/nDPI/blob/dev/doc/FAQ.rst)；[nDPI flow risks](https://www.ntop.org/guides/nDPI/flow_risks.html)；[nDPI protocol IDs（SOCKS/WebSocket/OpenVPN/QUIC/WireGuard 等）](https://github.com/ntop/nDPI/blob/dev/src/include/ndpi_protocol_ids.h)
- [nProbe 采包/flow collector 定位](https://www.ntop.org/guides/nprobe/introduction.html)；[nProbe nDPI 选项](https://www.ntop.org/guides/nprobe/cli_options.html)
- [ntopng ClickHouse 历史 flow](https://www.ntop.org/guides/ntopng/flow_dump/clickhouse/index.html)；[ntopng 版本与许可证](https://www.ntop.org/guides/ntopng/versions_and_licensing.html)
- [sFlow v5 sample 结构](https://sflow.org/developers/diagrams/sFlowV5Sample.pdf)
- [sFlow v5 协议规范](https://sflow.org/sflow_version_5.txt)；[sample pool 与有效采样率说明](https://sflow.org/discussion/sflow-discussion/0036.html)；[counter sample 时间差计算说明](https://sflow.org/discussion/sflow-discussion/0008.html)
- [Apache Kafka producer configs](https://kafka.apache.org/43/configuration/producer-configs/)；[Apache Kafka topic configs](https://kafka.apache.org/40/generated/topic_config.html)
- [VictoriaMetrics data model 与 cardinality](https://docs.victoriametrics.com/victoriametrics/keyconcepts/)；[VictoriaMetrics cardinality explorer](https://docs.victoriametrics.com/victoriametrics/#cardinality-explorer)
- [ClickHouse 列式分析适用场景](https://clickhouse.com/resources/engineering/when-to-use-columnar-database)

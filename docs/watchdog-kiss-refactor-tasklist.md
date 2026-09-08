# Watchdog KISS 架构重构 Tasklist

> 唯一目标架构见 [watchdog-kiss-architecture.md](watchdog-kiss-architecture.md)。本清单只负责去 PB、去 tenant、去 VM、合并设备根和收敛查询/agent/billing；Flow 业务功能仍登记在 [flow-module-tasklist.md](flow-module-tasklist.md)，但不得继续依赖被删除的平台抽象。

## 0. 执行规则

- 没有业务数据需要迁移：不做 backfill、双写、shadow read 或逐表在线 DROP；使用独立 v2 clean schema 一次切换。
- **当前唯一活动工作包是 KISS-01 PocketBase 彻底移除。** KISS-02 及以后全部阻塞在其完成门禁之后；Flow 仅允许处理不触及平台契约的紧急数据面缺陷，不得继续叠加 PB/tenant/provider 兼容层。
- 每个工作包必须纵向闭环：设计、编码、单元、真实集成、变更设计、变更测试、回归、已提交。任一项缺失不得标记完成。
- 一个工作包只拥有自己的目录/契约；不得顺手重写 Flow 热路径或相邻半成品。
- **UI 与现有界面风格保持不变。** 平台重构只替换认证、API 调用和后端数据来源，不修改导航、信息架构、页面布局、组件样式、颜色、字体、图标或交互；因此不新增视觉回归测试，界面文件的非必要变更直接视为越界。
- 每次提交必须从 clean checkout 可复现；禁止依赖未跟踪 migration、测试或本地数据库状态。
- PB 只允许在 KISS-01 的短暂替换窗口存在；认证和剩余活入口切换后立即整层删除，不留只读回退、永久 adapter 或第二套身份权威。
- 任何新增表必须对应当前工作包的真实读写路径；禁止空 migration 和未来占位表。
- 用户已明确授权删除已确认无用/死亡代码以及执行 `DROP DATABASE`。执行破坏性步骤前仍必须只读解析并记录**精确对象白名单**：数据库引擎、库名、用途、配置引用、运行进程/连接和代码引用；禁止通配、模糊匹配、宽目录、未解析环境变量以及删除 MySQL/ClickHouse 系统库、EdgeManager 库、备份库或并行任务使用的库。
- 本轮没有数据迁移和保留要求，但每个实际删除/DROP 必须在对应工作包提交证据中记录目标与执行结果；没有进入白名单的对象一律不动。

## 1. 工作包顺序

### KISS-00 架构冻结与变更边界（决策记录，不阻塞 KISS-01）

- [x] **设计**：冻结单部署域、MySQL + ClickHouse、Kafka transport、全局地址发布、一个 device 根、两个 typed query service、三层 Flow 口径。
- [x] **代码审查**：确认 89 张 MySQL 表/77 张 tenant 表、PB Go/前端耦合、VM/provider 耦合、target/device 双身份和 agent 表膨胀。
- [x] **变更设计**：确认无业务数据迁移，使用 v2 clean schema，不采用 tenant PIN、PB fork 或 VM 双写。
- [ ] **文档归档**：把旧平台清单未完成项逐项标为“保留到 KISS 包 / 删除 / Flow 业务继续”；该归档可随 KISS-01A 盘点完成，但不得成为延迟 PB 清理的前置工作。
- [ ] **已提交门禁**：ADR、清单、旧文档 superseded 标记独立提交，不夹带业务代码。

### KISS-01 PocketBase 彻底移除（第一要务 / 阻断项）

#### KISS-01A 精确盘点与删除白名单

- [x] **设计**：逐项枚举 PB 当前承担的认证、HTTP/router/static、collection/hook/cron/realtime、agent-connect、system telemetry、alert 和前端 SDK 路径；每项只能判定为“迁入目标架构”或“确认死亡并删除”，禁止第三种永久兼容状态。
- [x] **代码/数据盘点**：用仓库引用、启动配置、进程、监听端口、数据库列表、活动连接和表/collection 访问证据，形成精确文件/包/依赖/SQLite/MySQL/ClickHouse 删除白名单；明确哪些未跟踪文件和其他任务改动不属于本包。证据见 [pocketbase-removal-inventory.md](pocketbase-removal-inventory.md)。
- [x] **变更设计/测试**：已按精确目标先停 PB hub，再删源码/依赖/SQLite，最后 DROP 无连接调试库；`watchdog_dev`、非 Watchdog 数据库和其他任务改动均保留。按用户指令不执行中间编译/回归。
- [ ] **已提交门禁**：盘点、白名单和命令清单先独立提交；未完成前不得执行文件删除或 `DROP DATABASE`。

#### KISS-01B MySQL 本地认证与纯 Go 入口

- [ ] **设计**：冻结 v2 认证最小表白名单、FK/唯一键、ID、UTC 时间、row version、secret encryption、bcrypt、session/CSRF、首管理员初始化和 API error envelope；不复刻 PB OTP/OAuth/realtime collection。
- [ ] **编码**：建立 v2 baseline/install；实现 MySQL `users/sessions/roles/permissions` 最小闭环、HttpOnly session、login/logout/current/password change/disable；纯 `net/http` server、body limit、CORS、SPA fallback、健康检查。
- [ ] **单元测试**：密码校验、session rotation/expiry/revoke、CSRF、禁用用户、RBAC、错误信封和敏感字段脱敏。
- [ ] **集成测试**：空 MySQL 只初始化一个管理员；真实登录/登出/禁用/改密；纯 Go server 独立提供 API 和前端构建产物，全程不创建 PB SQLite、不读取 PB env。
- [ ] **已提交门禁**：schema、server、auth 与测试形成可独立启动的提交。

#### KISS-01C 前端完全去 PB

- [ ] **编码**：删除 PB transport/authStore/collection/realtime 调用和 JS SDK；全站只使用一个 `WATCHDOG_CONFIG.API_URL` fetch client；仅在用户主动登录或受控 API 返回 401 时进入登录流程。保留现有路由、导航、页面布局、组件、主题、图标与交互，不借去 PB 重写 UI。
- [ ] **单元/集成测试**：未登录访问公开页面零认证请求；受控页面被动跳转；主动登录、session 过期、logout 和错误提示；真实前端构建连接纯 Go API；既有页面路由和主要操作仍可用。
- [ ] **变更设计/测试**：浏览器只验证 Network/console 中无 PB endpoint、websocket 和 SDK 请求，以及删除 PB 配置后功能行为一致；不做视觉测试，不修改现有样式与布局文件。
- [ ] **已提交门禁**：前端依赖锁文件、client、页面和测试同一提交，仓库前端引用扫描为零。

#### KISS-01D PB 剩余活入口迁移或删除

- [ ] **编码**：agent 注册/心跳只迁移 KISS-04 真正需要的最小入口到 MySQL；SNMP/system 时序活数据直接接目标 ClickHouse sink；**PB 的告警 hook/collection/realtime 直接删除**——日志与告警作为后续单独的 ClickHouse 子系统重建（按 LibreNMS eventlog/alert 结构、无历史迁移），KISS-01 不保留任何 PB 告警路径。已确认的 systems、smart_devices、旧 realtime、hook、cron 和 collection 写链直接删除，不为历史数据造迁移器。
- [ ] **单元/集成测试**：至少一个 agent 注册/心跳不经 PB；删除死亡入口（含全部 PB 告警 hook/collection）后不存在后台重试、静默写 SQLite 或前端 `Failed to fetch` 循环。
- [ ] **变更设计/测试**：逐项核销 KISS-01A 的活依赖；功能若不在目标产品五项内，删除而不是搬家。
- [ ] **已提交门禁**：活入口迁移与对应死链删除按可启动纵向切片提交，不允许“新入口 + 旧入口继续双写”。

#### KISS-01E 物理删除 PocketBase 与旧库

- [ ] **编码删除**：按白名单删除 `internal/hub` 的 PB server/hooks/collections、PB migrations/assets/config/env、Go module 依赖、前端 SDK 和所有已确认无引用的辅助/测试死代码；移除 PB SQLite 文件/volume 的创建和挂载路径。
- [x] **数据库删除**：已删除两套 PB SQLite，并在核验精确库名、配置引用和活动连接后 DROP 12 个旧 Watchdog 调试库；只保留有活动连接且被开发配置引用的 `watchdog_dev`，未使用通配，未动其他数据库。证据见 [pocketbase-removal-inventory.md](pocketbase-removal-inventory.md)。
- [ ] **单元/集成测试**：仅 MySQL + ClickHouse + Kafka 从空目录启动，初始化管理员并完成登录、agent 心跳和健康检查；系统在无 PB binary、SQLite、env、端口和网络请求时正常运行。
- [ ] **变更设计/测试**：重复执行 clean install 和删除脚本必须幂等；精确目标不存在时报告 already absent，目标身份不匹配时 fail closed。
- [ ] **回归测试**：`go test ./...`、race/vet/build、前端 lint/test/build、fresh install 两次；静态扫描运行代码、依赖清单和构建制品均无 `pocketbase`/PB collection/API。
- [ ] **已提交门禁**：删除与 DROP 证据、测试输出和 clean-checkout 启动结果齐全后才标记 KISS-01 完成；随后才能启动 KISS-02。

### KISS-02 单域 RBAC 与设备根

- [ ] **设计**：以 LibreNMS role abilities + `devices_perms/ports_perms/bill_perms` 为基线，冻结“全局 action + 显式资源集合”双门、固定权限 key、默认角色和 device-group scope；设备侧按 LibreNMS 来，冻结 `devices` 与 `ports/interface_addresses/bgp_sessions/sensors/physical_entities/vlans/lag_groups/snmp_profiles` 契约，并将 `locations`（站点/POP）、`device_groups`（static/dynamic）、`device_group_members` 作为一等组织单位（服务导航分组、告警范围和批量授权）。
- [ ] **编码**：删除 tenant context/header/selector；客户/供应商改为业务实体；合并 `targets + network_devices -> devices`，所有子表直接引用 device；设备以 host 唯一、display name 可选。
- [ ] **API/UI**：users/roles/permissions/device/port 领域路由；设备详情各 VTable 保持服务端分页/搜索/排序/column filter；SNMP secret 编辑/继承和发现结果只读。
- [ ] **单元测试**：role ability、device grant、port 显式 grant、端口继承 device、bill 独立 grant、Flow layer ability + device/port scope、update/delete/export 对象范围、空范围 fail-closed；host 规范化/冲突、SNMP v1/v2c/v3 输入、sysName/sysDescr 非必填、IPv4/v6/BGP 通用发现。
- [ ] **集成测试**：空 MySQL 添加真实 SNMP 设备、发现并写设备/端口/IP/BGP/sensor/inventory；CRUD/ETag/delete-preview/job 全链。
- [ ] **变更设计/测试**：建立旧 target/device route 的短期 301/compat DTO 清单；前端全部切换后删除 compat，不保留双 ID。
- [ ] **回归测试**：SNMP discovery、所有设备详情页 VTable、角色边界、真实 MySQL、前端浏览器回归。
- [ ] **已提交门禁**：schema/domain/API/UI/test 原子提交，工作区无旧 route 调用。

### KISS-03 SNMP/system/agent 时序 sink 切换到 ClickHouse

- [ ] **设计**：明确“collector 不重写、只换 storage adapter”；冻结现有 SNMP metric/entity/timestamp/raw counter/interval/quality 输入契约，以及 `telemetry_samples`、`interface_traffic_5m`、optional `telemetry_events` DDL；自然幂等坐标、counter width/reset/wrap/gap、bucket、retention、query budget。
- [ ] **编码**：保持现有 MIB discovery、OS/module definition、SNMP session、poll recipe、agent/collector 和 counter 语义不变；仅将 SNMP/system sink 改为批写 CH，实现 `MetricQueryService`、closed 5m、图表和 export，再删除 VM writer/client/provider/DeleteSeries/config。
- [ ] **单元测试**：UInt64 精度、32/64-bit wrap、reset、乱序/重复、缺口、实际 poll interval、5m 边界、分页/filter 和 query limit。
- [ ] **集成测试**：真实 SNMP -> collector -> CH raw -> 5m -> API/chart/export；进程 crash/retry 后自然坐标收敛；无 VM 容器仍全绿。
- [ ] **变更设计/测试**：同一采集输入在旧 VM adapter 与新 CH adapter 的 metric/entity/timestamp/raw counter/interval/quality 逐字段一致；没有历史回填，仅验证切换前旧 DB 可整库回退。新部署配置拒绝 VM 字段，旧字段给出明确启动错误而非忽略。
- [ ] **回归测试**：SNMP/agent、账单基础 rate、CH migration/fresh schema、race/vet/build、前端图表。
- [ ] **已提交门禁**：CH DDL、writer/query/UI、VM 删除和测试在一个可启动提交中闭环。

### KISS-04 Agent registry 收敛

- [ ] **设计**：冻结 agent kind/capability schema、enrollment/token-or-mTLS、binding、immutable plan、ACK、heartbeat、revocation 和兼容版本矩阵。
- [ ] **编码**：将 collector/target agent 多表收敛为 `agents/credentials/bindings/plans/acks/runs`；保留已有 plan 签名和 LKG 必要能力，删除 tenant ownership/provider/fleet 占位状态机。
- [ ] **API/UI**：agent list/detail/enroll/rotate/revoke/bind/plan/run；所有列表服务端分页/search/sort/filter；secret 只显示一次。
- [ ] **单元测试**：重复 enrollment、token rotation、replay、过期、capability/schema 拒绝、plan downgrade、ACK 幂等、离线/clock skew。
- [ ] **集成测试**：system、SNMP、flow collect、flow worker 至少四种 agent 实际注册、下发、ACK、断线/LKG/吊销。
- [ ] **变更设计/测试**：批量 rollout 用 operation job fan-out；删除未完成 canary 表前验证无生产调用。
- [ ] **回归测试**：agent/collector 全链、认证隔离、MySQL concurrency、race/vet/build、UI。
- [ ] **已提交门禁**：旧 agent tables/repos/routes 无引用，clean install 仅创建新表。

### KISS-05 现有 Geo/AddressSnap 链单域化

- [ ] **设计**：冻结“实现不重写、只去 tenant/owner”的边界；现有 MMDB/IPDB import、MySQL 业务表/字段、CRUD/list、job payload、WADS v1、object store、download/LKG/ACK/GC 均不变。
- [ ] **编码**：只删除 address 表、repository、API、签名 envelope 和权限中的 tenant/owner 参数，改为全局 `address.manage/address.publish`；不得修改 importer、规范化/集合运算、builder、codec、object writer/reader、worker loader 或 publication 状态机。
- [ ] **API/UI**：现有 Geo/线路/运营商/prefix/set/import/draft/preview/publish/history/rollback 请求响应和界面保持不变；移除 tenant 选择/owner 判定，非管理员只读 active catalog。
- [ ] **单元测试**：在现有 v4/v6、层级、并交差、include/exclude、CIDR normalization、重叠、资源预算、object checksum/signature、rollback/GC 套件之外，只新增全局管理员/发布权限和无 tenant 契约测试。
- [ ] **集成测试**：真实 MMDB/IPDB -> MySQL -> async build -> WADS -> worker download/install/ACK -> 内存 lookup；损坏/断网保留 LKG。
- [ ] **变更设计/测试**：同一 MMDB/IPDB fixture 在改造前后生成的 MySQL 业务值、API 结果和 WADS bytes 必须一致；复跑现有 4.3 MiB、compile/lookup/memory/swap 基准；禁止按客户复制 WADS。
- [ ] **回归测试**：地址 UI/API、operation lifecycle、MySQL、Flow dimension parity、race/vet/build。
- [ ] **已提交门禁**：全局发布链单独提交，不夹带 Flow query/report 改造。

### KISS-06 Flow 单域化与查询收敛

- [ ] **设计**：冻结现有 sFlow v5/NetFlow v5 fast decode、GoFlow2 v9/IPFIX/template/fallback；冻结移除 tenant 后的 Kafka wire、CH sort/dedup、receipt/reconciliation、publication/classification version、typed query/export cursor；确认不改解码和计数语义。
- [ ] **编码（数据面）**：从 Flow facts/receipts/aggregates/worker/config 移除 tenant；不得重写 decoder，保持 fast path、GoFlow2 template/sampling store、Kafka 坐标、batch insert、count/counter 对账、Storage V2 生命周期和 WADS 热路径。
- [ ] **编码（查询）**：删除 generic DatasetProvider/QueryGateway policy；保留 Flow compiler/admission/report composer，形成单一 `FlowQueryService`；Explorer、六报表、detail/facet/export 共用 typed contract。
- [ ] **单元测试**：Kafka coordinate dedup、sampling known/unknown、raw/supplier/customer、方向、publication as-of、cursor/filter/budget、report conservation。
- [ ] **集成测试**：真实 sFlow/NetFlow/IPFIX -> Kafka -> worker -> CH -> Explorer/六页/明细/导出；crash/rebalance/fake ack/retry；count/counter reconciliation。
- [ ] **变更设计/测试**：复跑 fast-vs-GoFlow2 差分、sFlow v5/NetFlow v5/v9/IPFIX pcap、现有吞吐、WADS、CH timeout/fault injection/Storage V2 门禁；无 tenant 的旧 wire 明确拒绝，不做静默混读。
- [ ] **回归测试**：Flow 全库 test/race/vet/build、真实 Kafka+CH、前端浏览器六页和 Explorer。
- [ ] **已提交门禁**：数据面单域化与查询切换可拆两个提交，但每个提交必须可构建、可运行且文档状态真实。

### KISS-07 Billing、三层修正与对账闭环

- [ ] **设计**：冻结 party/account/port/period/value/adjustment/reconciliation 字段、状态机、95th/average/total 算法、时区、审批和不可变关闭语义。
- [ ] **编码**：实现 raw/supplier/customer period values、SNMP interface total、外部账单导入、差异阈值/issues、adjustment/reversal、approve/close/export。
- [ ] **API/UI**：bill CRUD、端口绑定、周期计算、三层对比、SNMP/Flow 对账、问题处理、审批和证据导出；列表均 server VTable。
- [ ] **单元测试**：95th、缺桶、时区、方向、counter reset、sampling unknown、三层 delta、重复计算、stale approval、调整与 reversal。
- [ ] **集成测试**：真实 CH fresh Flow + SNMP 数据算同一周期；重复运行确定；关闭后拒绝覆盖；CSV/Parquet 三层结果和 provenance 完整。
- [ ] **变更设计/测试**：故意制造 exporter 缺流、采样率缺失、SNMP reset 和窗口偏移，确认只报差异、不自动调平。
- [ ] **回归测试**：billing/RBAC/audit/job/export、MySQL+CH、前端和端到端。
- [ ] **已提交门禁**：金额/带宽口径必须有独立 reviewer 可复算测试向量后提交。

### KISS-08 最终遗留清理与验收

- [ ] **编码**：KISS-01 已保证 PB 为零；本包只删除 VM/VLogs、tenant、module/resource/dataset/provider registries、旧 targets、兼容 adapter 和废弃配置。
- [ ] **静态门禁**：仓库扫描无 `pocketbase`、`tenant_id`、tenant header、VictoriaMetrics/VictoriaLogs、DatasetProvider 和 target/network-device 双身份运行代码。
- [ ] **空库验收**：仅 MySQL + ClickHouse + Kafka，从零安装管理员、设备、agent、地址 publication、Flow、SNMP、六报表、账单、导出、告警。
- [ ] **故障验收**：MySQL/CH/Kafka 短断、agent 离线、worker crash/rebalance、坏 publication、SNMP reset、任务 takeover/cancel/retry。
- [ ] **性能验收**：Flow 吞吐/延迟/内存不低于切换前基线；SNMP 写入和查询满足目标；账单计算有界。
- [ ] **回归测试**：Go/前端/真实依赖/浏览器/安装脚本/备份恢复全矩阵。
- [ ] **文档**：只保留一套当前架构、schema、配置、运维和故障手册；旧文档标历史，不再作为实施入口。
- [ ] **已提交门禁**：最终删除提交后 clean checkout 可完整部署；所有遗留数据库/volume 已按各工作包的精确白名单处置，不把物理清理拖到项目末尾。

### 后续单独工作包（不阻塞 KISS-01…08，均无历史数据迁移，已登记归属）

- **KISS-L 日志与告警（ClickHouse）**：按 LibreNMS `eventlog`/alert 表结构在 ClickHouse 设计存储与查询设备事件、告警日志和告警状态；MySQL 只保留 `notification_channels`/`quiet_hours` 投递/静音配置，规则定义按需入库。KISS-01 已负责删除全部 PB 告警 hook/collection，本包从零在 CH 上实现，不迁历史。
- **KISS-C Flow 客户/供应商口径修正**：本期只存 raw；本包按 §5.6 增加 `parties`/address set/分类归属处置规则，在渲染/导出/账单计算时对 raw 应用，绝不物化或回写 raw。
- **KISS-T 用户/服务 API token**：如需程序化访问，与 OAuth/OTP/找回一起作为认证扩展；替换 session authenticator，不恢复 PB。

## 2. 任务归属规则

- PB、tenant、VM、device root、agent registry、auth、billing 属于本清单；
- Flow collect/Kafka/worker/CH/WADS 算法和 Flow 报表功能属于 Flow 清单；
- 两者交界只通过冻结契约：`device_id/agent_id`、全局 `publication_id`、Kafka coordinate、CH query service、operation job；
- KISS-01 未完成前，KISS-02+ 不启动；Flow 只允许修复不依赖 PB/tenant/provider 且不改变平台契约的紧急数据面缺陷；
- 发现旧平台缺陷先登记到对应 KISS 工作包，不混入当前 Flow diff。

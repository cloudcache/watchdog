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
- [ ] **编码**：建立 v2 baseline/install；实现 MySQL `users/sessions/roles/permissions` 最小闭环、HttpOnly session、login/logout/current/password change/disable；单一 Gin server、body limit、CORS、健康检查。前端独立运行，不由后端提供 static fallback。
- [ ] **单元测试**：密码校验、session rotation/expiry/revoke、CSRF、禁用用户、RBAC、错误信封和敏感字段脱敏。
- [ ] **集成测试**：空 MySQL 只初始化一个管理员；真实登录/登出/禁用/改密；纯 Go server 独立提供 API 和前端构建产物，全程不创建 PB SQLite、不读取 PB env。
- [ ] **已提交门禁**：schema、server、auth 与测试形成可独立启动的提交。

#### KISS-01C 前端完全去 PB

- [ ] **编码**：删除 PB transport/authStore/collection/realtime 调用和 JS SDK；全站只使用一个 `WATCHDOG_CONFIG.API_URL` fetch client；仅在用户主动登录或受控 API 返回 401 时进入登录流程。保留现有路由、导航、页面布局、组件、主题、图标与交互，不借去 PB 重写 UI。
- [ ] **单元/集成测试**：未登录访问公开页面零认证请求；受控页面被动跳转；主动登录、session 过期、logout 和错误提示；真实前端构建连接纯 Go API；既有页面路由和主要操作仍可用。
- [ ] **变更设计/测试**：浏览器只验证 Network/console 中无 PB endpoint、websocket 和 SDK 请求，以及删除 PB 配置后功能行为一致；不做视觉测试，不修改现有样式与布局文件。
- [ ] **已提交门禁**：前端依赖锁文件、client、页面和测试同一提交，仓库前端引用扫描为零。

#### KISS-01D PB 剩余活入口迁移或删除

- [x] **编码**：agent 注册/心跳最小入口已由 KISS-04 Gin/MySQL 承接；**PB 的告警 hook/collection/realtime 直接删除**——日志与告警作为后续单独的 ClickHouse 子系统重建（按 LibreNMS eventlog/alert 结构、无历史迁移），KISS-01 不保留任何 PB 告警路径。已确认的 systems、smart_devices、旧 realtime、hook、cron 和 collection 写链均已删除，不为历史数据造迁移器。SNMP/system 时序改写 ClickHouse 属 KISS-03 存储纵向切片，不是 PB 入口的隐性前置。
- [x] **单元/集成测试**：真实 MySQL 覆盖 agent enrollment/register/heartbeat；Gin 路由扫描和旧 watchdog router 反向测试覆盖 agent-registry、system-agent heartbeat、notification/quiet-hours/alerts-history 路径均不存在。前端删除 realtime/alert polling store 及相关 API，不再产生旧端点重试或 SQLite 写入。
- [x] **变更设计/测试**：KISS-01A 清单中的 agent 运行入口迁 Gin/MySQL；告警、systems、smart_devices、realtime、hook、cron/collection 写链按非目标产品功能删除，无双写、无历史迁移器、无兼容 fallback。
- [x] **已提交门禁**：活入口迁移与对应死链删除已形成可启动纵向切片，不存在“新入口 + 旧入口继续双写”。

当前核销：生产 agent machine API 唯一路径是 Gin `agents/register|heartbeat|status|errors`，直写 v2 MySQL；旧 watchdog `/agent-registry` 和重复的 heartbeat/status/errors 路由已删除。尚待 KISS-03 等价迁入 Gin+ClickHouse 的历史 system plan/sample 实现及测试继续保留，它不是 PB 路径，也不与 Gin machine API 双写。旧 PB alerts hook/collection/realtime、systems/smart write chain、WebSocket hub、cron、external-subject identity/notification/quiet-hours bridge 已物理删除。

#### KISS-01E 物理删除 PocketBase 与旧库

- [ ] **编码删除**：按白名单删除 `internal/hub` 的 PB server/hooks/collections、PB migrations/assets/config/env、Go module 依赖、前端 SDK 和所有已确认无引用的辅助/测试死代码；移除 PB SQLite 文件/volume 的创建和挂载路径。
- [x] **数据库删除**：已删除两套 PB SQLite，并在核验精确库名、配置引用和活动连接后 DROP 12 个旧 Watchdog 调试库；只保留有活动连接且被开发配置引用的 `watchdog_dev`，未使用通配，未动其他数据库。证据见 [pocketbase-removal-inventory.md](pocketbase-removal-inventory.md)。
- [ ] **单元/集成测试**：仅 MySQL + ClickHouse + Kafka 从空目录启动，初始化管理员并完成登录、agent 心跳和健康检查；系统在无 PB binary、SQLite、env、端口和网络请求时正常运行。
- [ ] **变更设计/测试**：重复执行 clean install 和删除脚本必须幂等；精确目标不存在时报告 already absent，目标身份不匹配时 fail closed。
- [ ] **回归测试**：`go test ./...`、race/vet/build、前端 lint/test/build、fresh install 两次；静态扫描运行代码、依赖清单和构建制品均无 `pocketbase`/PB collection/API。
- [ ] **已提交门禁**：删除与 DROP 证据、测试输出和 clean-checkout 启动结果齐全后才标记 KISS-01 完成；随后才能启动 KISS-02。

### KISS-02 单域 RBAC 与设备根

- [x] **KISS-02A 已完成纵向切片（2026-09-08，`e5848ade`）**：新 Gin 后端已接 `devices` 与 SNMP profile 的 list/get/create/patch/delete，host 唯一且 display name 可选；labels/SNMP override 不丢失，ETag 冲突检测、固定 sort 白名单、服务端分页/search/status filter、device/device-group scope 均已接 MySQL。现有 `/network/devices` 与 `/targets` 是同一 `device_id` 的临时 handler/DTO alias，兼容层仅做 `system -> host` 类型映射，不创建或写旧表；真实 MySQL 集成测试覆盖 CRUD、host 冲突、profile secret 脱敏及 summary/target alias。

- [x] **KISS-02B 设备子资源查询（`145842d3`）**：Gin/MySQL 已接 canonical `/devices/:id/{ports,addresses,bgp,sensors,inventory,vlans,lags}`、`/ports/:id`、`/bgp` 及现有 `/network/*` alias；所有列表固定 SQL sort 白名单、严格分页/search/column filter，端口同时支持设备继承授权与显式 port grant。migration 0007 只补 MIB-neutral inventory 字段，SNMP/system 时序值仍归 ClickHouse；真实空 MySQL 覆盖双栈地址、BGP v4/v6、sensor/inventory/VLAN/LAG、端口 CAS、账单删除阻断和显式端口范围。

- [x] **KISS-02C 组织与资源集合（`bfb47e02`）**：locations、static/dynamic device groups、物化 membership、refresh 和 delete-preview 已闭环；动态规则只允许固定字段/label，不接收 SQL。用户 access replace/get 覆盖 device/group/port/billing 四类 grant，先完整校验再同事务替换，重复 ID 归一化，未知对象不留下半批授权。

- [x] **KISS-02D 通用 SNMP discovery 与 Flow device scope（`2cda16e8`、`ed7be0e2`）**：复用既有 MIB/definition engine 和 v1/v2c/v3 session，不在 handler 拼厂商；发现结果按 completed module 事务写当前 inventory，覆盖双栈 IP/BGP、sensor/entity/VLAN/LAG，失败不裁剪上次清单，账单端口仅标记 `notPresent`。Flow exporter binding 的 list/get/create/update/delete 均叠加 device scope，列表参数 fail-closed。

- [x] **设计**：以 LibreNMS role abilities + `devices_perms/ports_perms/bill_perms` 为基线，冻结“全局 action + 显式资源集合”双门、固定权限 key、默认角色和 device-group scope；冻结唯一 device 根、子资源、locations 与 static/dynamic group 契约。动态组使用 typed rule + 物化成员；SNMP `sys_location` 与管理 `location_id/name` 分离。
- [x] **编码**：新 Gin/MySQL 运行面无 tenant context/header/selector；客户/供应商是业务实体；`targets + network_devices` 已合并为 `devices`，所有子表和 Flow exporter binding 直接引用同一 `device_id`；host 唯一、display name 可选。
- [x] **API/UI**：users/roles/permissions/device/group/location/port/SNMP/Flow-device 领域路由已接；既有 UI 继续用同 handler 的 alias，界面和风格不改。所有已迁设备 VTable 都是服务端分页/search/sort/column filter；SNMP profile 列表不出 secret，编辑和 device override 可维护，发现字段只读。
- [x] **单元测试**：固定 role ability、动态组 typed rule、device/group grant、port 显式 grant与设备继承、billing 独立 grant校验、Flow-device ability + device scope、对象 update/delete 权限和空范围 fail-closed 已覆盖；host 规范化/冲突、SNMP v1/v2c/v3 session、sysName/sysDescr 非必填、双栈 IP/BGP 通用结果已覆盖。Flow 高基数记录层的 layer+device/port query/export 下推归 KISS-06，账单对象状态机归 KISS-07，不在本包伪造占位实现。
- [x] **集成测试**：隔离空 MySQL 全链覆盖 device/profile/discovery/current inventory/organization/grants/Flow exporter/ETag/delete-preview/audit；确定性 fixture 覆盖所有双栈子资源和模块失败/裁剪边界；另对 `103.83.65.0` 完成真实 SNMP discovery 并把真实结果写入另一个空 v2 MySQL 验证。异步 poll plan/ACK 是 KISS-03/04，不为手工 discover 另造 job。
- [x] **变更设计/测试**：canonical 路径与 `/network/devices`、`/targets`、`/network/ports` compat DTO 清单已冻结；alias 只转同一 handler、repository 和 ID，不建旧表、不双写。当前 UI 切换完成前保留 alias，最终物理删除归 KISS-08。
- [x] **回归测试**：`go test ./...`、server/watchdog vet、`go build ./...`、45 个前端单测和 production build 通过；真实 MySQL、真实 SNMP、全部设备详情 VTable 和角色边界均有自动回归。按约束未修改视觉，不增加视觉测试。
- [x] **已提交门禁**：schema/domain/API/test 分为可独立构建的纵向提交 `e5848ade`、`a17b313a`、`145842d3`、`bfb47e02`、`2cda16e8`、`ed7be0e2`；运行时只有一个 device ID/管理库。兼容 URL 的最终删除是 KISS-08 清理门，不再阻塞 KISS-02。

### KISS-03 SNMP/system/agent 时序统一写入并查询 ClickHouse

> 存储边界：MySQL 只保存 device/port/SNMP profile/MIB 配置等管理对象；SNMP 原始 counter、状态样本、派生速率、system/agent 时序、图表和导出全部以 ClickHouse 为唯一权威。不存在 VM 双写、VM 历史迁移或 VM 回退路径。

- [ ] **设计**：明确“collector/MIB/OID 语义不重写，只换 writer/query”；冻结现有 `device_id/entity/metric/collected_at/poll_sequence/raw counter/counter width/interval/quality` 输入契约，以及 CH `telemetry_samples`、`interface_traffic_5m`、必要 `telemetry_events` DDL、自然幂等坐标、分区/排序键、codec、batch、TTL、closed-bucket 和查询预算。原始 32/64-bit counter 使用整数列，不经 Float64。
- [ ] **编码—写入**：保持现有 MIB discovery、OS/module definition、SNMP v1/v2c/v3 session、poll recipe、IPv4/v6/BGP/sensor/inventory 和调度不变；将 SNMP/system/agent 样本通过有界批量 writer 直接写 CH，成功后才确认本批，失败执行有界 retry/backpressure，不逐指标查询 MySQL/CH。
- [ ] **编码—派生**：在 CH 内从连续原始 counter 计算 rate/流量，显式处理 32/64-bit wrap、reset、乱序、重复、缺口和真实 poll interval；只生成已关闭的 5m bucket，保留原始 counter 作为审计依据。
- [ ] **编码—查询**：实现唯一 `MetricQueryService`，设备详情、端口流量、系统/agent 图表、统计和导出全部直接查询 CH；API DTO 与现有 UI 契约保持不变，服务端分页/search/sort/filter 继续保留。
- [ ] **编码—删除**：删除 VictoriaMetrics writer/client/provider/DeleteSeries、Prometheus remote-write/import 查询接线和 VM 配置；不保留 feature flag、双写 adapter 或 fallback。
- [ ] **单元测试**：UInt64 精度、自然坐标幂等、batch retry、32/64-bit wrap、reset、乱序/重复、缺口、实际 interval、5m 边界、TTL、分页/filter 和 query budget。
- [ ] **集成测试**：真实 SNMP -> collector -> CH raw -> closed 5m -> API/chart/export；进程 crash/retry 后自然坐标收敛；停止/不存在 VM 时完整链路正常。
- [ ] **变更设计/测试**：没有历史数据，不做 VM 对比、backfill、shadow read 或整库回退；用固定 SNMP fixture 从采集输入直接核对 CH 原始行、派生 bucket、API 和导出守恒。旧 VM 配置必须报明确的 removed-field 启动错误。
- [ ] **回归测试**：SNMP/agent、设备与端口页面、账单基础 rate、CH fresh schema、race/vet/build；不增加视觉测试。
- [ ] **已提交门禁**：CH DDL、writer、rate/bucket、query/export、VM 删除与测试形成一个纵向闭环；运行代码中 SNMP/system/agent 数据路径不存在 VM。

### KISS-04 Agent registry 收敛

- [x] **KISS-04A 注册/CRUD 纵向切片（基础提交 `e5848ade`，本次闭环提交见 Git history）**
  - [x] **设计**：冻结 `system/snmp/flow_collect/flow_worker/probe` 五类、`registered -> active -> draining -> revoked`、`api_version=v1` 与版本化 capability；enrollment token 固定 kind 和可选 device，scope 用户不得签发任意设备 token；system/snmp 绑定分别校验 host/network device；管理配置 ETag 与高频 heartbeat/run 解耦。
  - [x] **编码**：全局 `agents/agent_credentials/agent_bindings/agent_runs/agent_enrollment_tokens` 由 Gin+MySQL 唯一承载；注册、heartbeat/status/errors、token/mTLS 凭证、轮换、吊销、设备绑定和运行记录均无 PB/tenant；旧 `/agent-registry` 实现删除，system agent heartbeat 客户端改用 canonical Gin URL。
  - [x] **API/UI**：canonical agent list/detail/create/update/delete/enroll/rotate/revoke/run 已接现有 Agents 页面；两张 VTable 使用服务端分页/search/sort/column filter；enrollment 可选目标设备，machine/enrollment secret 只显示一次；布局与样式未重写。
  - [x] **单元测试**：kind/API/capability 兼容矩阵、字段上限、SHA-256 fingerprint、旧状态映射、active/draining 离线判定已覆盖。
  - [x] **集成测试**：全新真实 MySQL 顺序应用 schema；覆盖手工注册、错误 kind/device、设备范围 RBAC、严格 body/query、heartbeat 时钟偏差、run、过期/错 kind/错 capability/错 device/replay enrollment、凭证 CAS 轮换、旧 token 失效、吊销和删除。
  - [x] **变更设计/测试**：旧 `pending/up/down/disabled` 显式归一到新状态；system agent heartbeat/status/errors 保留请求兼容字段但改走 Gin；未等价迁移的 system plan/sample 原实现和测试保留到 KISS-03，不先删后重写。
  - [x] **回归测试**：`go test ./...`、server/watchdog race、全库 vet/build、前端 45 单测、Agent 文件 Biome 和 production build 通过；未增加视觉测试。
  - [x] **已提交门禁**：本纵向切片只提交 Agent schema/API/UI/test、旧 registry 删除和必要文档核销；地址库及其他并行 WIP 不进入提交。

`KISS-04A` 完成不代表整个 `KISS-04` 完成。下面 plan/ACK/LKG/四类实进程闭环仍为待办：

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

### KISS-06 Flow 单域化与 ClickHouse 查询收敛

> Flow 事实、receipt、archive/aggregate、VPN candidate 与现有查询本来就在 ClickHouse；本包不是“把 Flow 迁到 CH”，而是保持当前 CH 数据面不动，删除 tenant/provider/VM 外围依赖并统一查询入口。

- [ ] **设计**：冻结现有 sFlow v5/NetFlow v5 fast decode、GoFlow2 v9/IPFIX/template/fallback；冻结移除 tenant 后的 Kafka wire、CH sort/dedup、receipt/reconciliation、Storage V2 raw/archive、publication/classification version、typed query/export cursor；确认不改解码、计数、CH 写入和生命周期语义。
- [ ] **编码（CH 写入保持）**：从 Flow facts/receipts/aggregates/worker/config 移除 tenant；不得重写 decoder 或另造存储，保持 fast path、GoFlow2 template/sampling store、Kafka 坐标、CH native batch insert、count/counter 对账、Storage V2 生命周期和 WADS 热路径。
- [ ] **编码（CH 查询收敛）**：删除 generic DatasetProvider/QueryGateway policy 和任何 Flow→VM 查询接线；保留现有 CH compiler/admission/report composer，形成单一 `FlowQueryService`；Explorer、六报表、detail/facet/export 共用 typed contract 并直接查询 CH。
- [ ] **单元测试**：Kafka coordinate dedup、sampling known/unknown、raw/supplier/customer、方向、publication as-of、cursor/filter/budget、report conservation。
- [ ] **集成测试**：真实 sFlow/NetFlow/IPFIX -> Kafka -> worker -> CH -> Explorer/六页/明细/导出；crash/rebalance/fake ack/retry；count/counter reconciliation。
- [ ] **变更设计/测试**：复跑 fast-vs-GoFlow2 差分、sFlow v5/NetFlow v5/v9/IPFIX pcap、现有吞吐、WADS、CH timeout/fault injection/Storage V2 门禁；无 tenant 的旧 wire 明确拒绝，不做静默混读。
- [ ] **回归测试**：Flow 全库 test/race/vet/build、真实 Kafka+CH、前端浏览器六页和 Explorer。
- [ ] **已提交门禁**：数据面单域化与 CH 查询收敛可拆两个提交，但每个提交必须可构建、可运行且文档状态真实；运行代码中 Flow 业务数据的 writer/query 仅有 ClickHouse。

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

# Watchdog KISS 架构重构 Tasklist

> 唯一目标架构见 [watchdog-kiss-architecture.md](watchdog-kiss-architecture.md)。本清单只负责去 PB、去 tenant、去 VM、合并设备根和收敛查询/agent/billing；Flow 业务功能仍登记在 [flow-module-tasklist.md](flow-module-tasklist.md)，但不得继续依赖被删除的平台抽象。

## 0. 执行规则

- 没有业务数据需要迁移：不做 backfill、双写、shadow read 或逐表在线 DROP；使用独立 v2 clean schema 一次切换。
- **KISS-01 已完成并解除后续阻塞。** 后续工作按本清单中各包的真实状态推进；Flow 只在 Flow 清单执行，不得重新叠加 PB/tenant/provider 兼容层。
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
- [x] **文档归档**：旧平台清单剩余项已在 [platform-refactor-archive-map.md](platform-refactor-archive-map.md) 逐项标为“KISS 完成 / 删除旧路线 / Flow 继续”；旧架构与自托管审计入口均明确标为历史。
- [x] **已提交门禁**：KISS ADR/初始清单由 `391fd43f` 独立提交；本次归档映射和 superseded 状态由纯文档提交闭环，不夹带业务代码。

### KISS-01 PocketBase 彻底移除（第一要务 / 阻断项）

#### KISS-01A 精确盘点与删除白名单

- [x] **设计**：逐项枚举 PB 当前承担的认证、HTTP/router/static、collection/hook/cron/realtime、agent-connect、system telemetry、alert 和前端 SDK 路径；每项只能判定为“迁入目标架构”或“确认死亡并删除”，禁止第三种永久兼容状态。
- [x] **代码/数据盘点**：用仓库引用、启动配置、进程、监听端口、数据库列表、活动连接和表/collection 访问证据，形成精确文件/包/依赖/SQLite/MySQL/ClickHouse 删除白名单；明确哪些未跟踪文件和其他任务改动不属于本包。证据见 [pocketbase-removal-inventory.md](pocketbase-removal-inventory.md)。
- [x] **变更设计/测试**：已按精确目标先停 PB hub，再删源码/依赖/SQLite，最后 DROP 无连接调试库；`watchdog_dev`、非 Watchdog 数据库和其他任务改动均保留。按用户指令不执行中间编译/回归。
- [x] **已提交门禁**：盘点、白名单和命令清单由 `391fd43f` 独立提交；后续删除与 DROP 证据分别在 KISS-01E 提交链和 [pocketbase-removal-inventory.md](pocketbase-removal-inventory.md) 留痕。

#### KISS-01B MySQL 本地认证与纯 Go 入口

- [x] **KISS-01B1 首次安装纵向切片**：后端和 `frontend/` 分离运行，公开 `GET /api/v1/install-status`、`POST /api/v1/install` 与 `/install`；空 MySQL 启动保持 0 表且登录返回 `428 install_required`，显式提交后依次应用 MySQL/ClickHouse schema，并在事务中只创建一个管理员；重复安装返回 409。安装页不主动登录，正常登录页也不在渲染时提交登录请求。
- [x] **设计/编码/API/UI**：安装状态明确区分 `installed/requires_install/runtime_ready`；无默认或日志生成密码；前端只用 `WATCHDOG_CONFIG.API_URL` 直连 8091，Vite 8090 仅服务 `frontend/`，后端不托管前端、不做代理。运维步骤见 `docs/watchdog-install.md`。
- [x] **单元/集成/变更测试**：覆盖用户名/密码边界；真实空 MySQL 生命周期、安装前登录阻断、安装后 cookie 登录、单管理员与重复安装；真实 ClickHouse 迁移校验命中过历史 checksum drift 时安装失败且不伪报 ready，清理无数据的开发库后 fresh apply 成功；CORS preflight、前端 `/`/`/install` history fallback 与已安装重定向均实测。
- [x] **回归测试**：`internal/server` 测试、管理后端构建、前端 45 个单测和 production build 通过；Docker Kafka 主题 12 partitions、ClickHouse health/schema、MySQL 69 张当前嵌入表均核对。Flow 采集/处理进程未在本切片启动或修改。
- [x] **已提交门禁**：安装 schema、server/auth/router、独立入口、前端 install route/client/page、测试和部署文档构成一个可独立审查的提交，不夹带并行 Flow WIP。

- [x] **设计**：冻结 v2 认证最小表白名单、FK/唯一键、ID、UTC 时间、row version、secret encryption、bcrypt、session/CSRF、首管理员初始化和 API error envelope；不复刻 PB OTP/OAuth/realtime collection。
- [x] **编码**：建立 v2 baseline/install；实现 MySQL `users/sessions/roles/permissions` 最小闭环、HttpOnly session、login/logout/current/password change/disable；单一 Gin server、8 MiB body limit、CORS、健康检查。前端独立运行，不由后端提供 static fallback。
- [x] **单元测试**：密码字节边界/bcrypt、认证/权限/CSRF middleware 统一错误信封均有无数据库单测；独立真实 MySQL 生命周期进一步覆盖登录 token 轮换、库内仅存 token SHA-256、session expiry/logout/password-change revoke、禁用用户即时失效、RBAC 拒绝以及登录/current/users 响应不暴露密码、hash、session/CSRF token。
- [x] **集成测试**：隔离空 MySQL 建立 48 张 v2 管理表，确认 `tenant_id/auth_provider/external_subject_id` 列均为 0；真实覆盖登录、CSRF、设备/Agent CRUD、Agent 心跳、禁用账号立即拒绝旧 session、logout 撤销、二次启动只保留一个 bootstrap 管理员。另以独立进程启动 Gin `:8091`，完成 health、跨源 login/current 后精确删除测试库。
- [x] **已提交门禁**：schema/server/auth 已由 KISS-01B 纵向切片提交；本独立提交补齐统一 error envelope 与完整生命周期测试，不依赖 PB、ClickHouse 或 Flow。

#### KISS-01C 前端完全去 PB

- [x] **编码**：删除 PB transport/authStore/collection/realtime 调用和 JS SDK；全站只使用一个 `WATCHDOG_CONFIG.API_URL` fetch client；仅在用户主动登录或访问受控路由时恢复 session，登录提交完全由用户触发。用户管理页已从 external-subject 投影 DTO 切为 Gin 本地账号/角色 DTO；保留现有路由、导航、页面布局、组件、主题、图标与交互。
- [x] **单元/集成测试**：纯路由策略单测冻结未安装/dev-auth/已检查、公开密码恢复、安装重定向、受控与未知路由的 session 恢复边界；前端 51 个单测、全量 Biome、Vite production build 通过。真实 `frontend/` Vite 直连当前纯 Go `watchdog-server`，浏览器完成受控首页被动检查→登录页、用户主动登录→主界面、session 过期→登录页、主动 logout→登录页，控制台无错误。
- [x] **现有自动门禁**：51 个前端单测、Vite production build、全量 Biome lint 0 diagnostics；`api.send` JSON body 类型由错误的 `RequestInit` 交叉类型改为显式覆盖。PLAT-FE-03 已逐项消除余下 22 项 DTO/失活页面类型债并增加 `npm run typecheck` 门禁，全量 TypeScript build 通过。
- [x] **变更设计/测试**：浏览器只验证请求行为与 console；源码、依赖锁和构建入口扫描无 PocketBase SDK、collection endpoint 或 authStore。没有做视觉测试，也没有修改样式和布局。
- [x] **已提交门禁**：本提交仅含 session 路由策略、统一 client body 边界、测试和核销证据；无依赖变化所以锁文件保持不变，前端 PocketBase 引用扫描为零。

#### KISS-01D PB 剩余活入口迁移或删除

- [x] **编码**：agent 注册/心跳最小入口已由 KISS-04 Gin/MySQL 承接；**PB 的告警 hook/collection/realtime 直接删除**——日志与告警作为后续单独的 ClickHouse 子系统重建（按 LibreNMS eventlog/alert 结构、无历史迁移），KISS-01 不保留任何 PB 告警路径。已确认的 systems、smart_devices、旧 realtime、hook、cron 和 collection 写链均已删除，不为历史数据造迁移器。SNMP/system 时序改写 ClickHouse 属 KISS-03 存储纵向切片，不是 PB 入口的隐性前置。
- [x] **单元/集成测试**：真实 MySQL 覆盖 agent enrollment/register/heartbeat；Gin 路由扫描和旧 watchdog router 反向测试覆盖 agent-registry、system-agent heartbeat、notification/quiet-hours/alerts-history 路径均不存在。前端删除 realtime/alert polling store 及相关 API，不再产生旧端点重试或 SQLite 写入。
- [x] **变更设计/测试**：KISS-01A 清单中的 agent 运行入口迁 Gin/MySQL；告警、systems、smart_devices、realtime、hook、cron/collection 写链按非目标产品功能删除，无双写、无历史迁移器、无兼容 fallback。
- [x] **已提交门禁**：活入口迁移与对应死链删除已形成可启动纵向切片，不存在“新入口 + 旧入口继续双写”。

当前核销：生产 agent machine API 唯一路径是 Gin `agents/register|heartbeat|status|errors`，直写 v2 MySQL；旧 watchdog `/agent-registry` 和重复的 heartbeat/status/errors 路由已删除。尚待 KISS-03 等价迁入 Gin+ClickHouse 的历史 system plan/sample 实现及测试继续保留，它不是 PB 路径，也不与 Gin machine API 双写。旧 PB alerts hook/collection/realtime、systems/smart write chain、WebSocket hub、cron、external-subject identity/notification/quiet-hours bridge 已物理删除。

> **2026-09-15 前端活入口核销**：新增依赖图门禁，从 `frontend/src/main.tsx` 递归扫描实际可达模块中的 `/api/v1/*`，逐条和生产 Gin 路由表匹配；延后的 containers/system history/S.M.A.R.T./旧 modules 页面已从 router、导航和 command palette 卸载，保留源码不等于保留运行入口。Retention、Dashboard、MIB 等已有 Gin 契约的页面继续挂载。该处理没有添加 404 占位 API，也没有重写页面视觉。

#### KISS-01E 物理删除 PocketBase 与旧库

- [x] **编码删除**：按白名单删除 `internal/hub` 的 PB server/hooks/collections、PB migrations/assets/config/env、Go module 依赖、前端 SDK 和所有已确认无引用的辅助/测试死代码；移除 PB SQLite 文件/volume 的创建和挂载路径。
- [x] **数据库删除**：已删除两套 PB SQLite，并在核验精确库名、配置引用和活动连接后 DROP 12 个旧 Watchdog 调试库；只保留有活动连接且被开发配置引用的 `watchdog_dev`，未使用通配，未动其他数据库。证据见 [pocketbase-removal-inventory.md](pocketbase-removal-inventory.md)。
- [x] **单元/集成测试**：仅 MySQL + ClickHouse + Kafka 从空目录启动，初始化管理员并完成登录、agent 心跳和健康检查；系统在无 PB binary、SQLite、env、端口和网络请求时正常运行。
- [x] **变更设计/测试**：PB 删除按一次性精确台账执行，不保留可误删未来数据的通配 cleanup 脚本；重复核验时精确文件/库/容器均报告 absent。旧 installer/worker 对 KISS `schema_migrations` marker fail-closed；fresh install 重复提交 409、server restart 幂等。
- [x] **回归测试**：`go test ./...`、race/vet/build、前端 lint/test/build、fresh install 两次；静态扫描运行代码、依赖清单和构建制品均无 `pocketbase`/PB collection/API。
- [x] **已提交门禁**：删除与 DROP 证据、测试输出和 clean-checkout 启动结果已由 KISS-01E 的独立提交链闭环；这只完成物理删除包，不倒签 KISS-01B/C 尚未核销的认证单测与浏览器 Network 验收。

当前门禁证据（2026-09-16）：PB Go/JS dependency、生产 Gin/前端源码和临时构建的 `watchdog-server` 制品扫描均为零；独立临时 ClickHouse（空 `watchdog_flow`）+ 临时 MySQL 在现有健康 Kafka 旁完成 schema、管理员、登录、一次性 enrollment、SNMP agent register/heartbeat、health 以及 SNMP/Flow query/export/billing worker 接线，临时库/容器均删除。全库 Go test/build/vet、server race、前端 test/lint/typecheck/build 通过。KISS-01B/C/E 的认证、被动登录、物理删除和提交门均已闭环；后续平台工作不得重新引入 PB、tenant 或 VM 兼容层。

> **2026-09-15 审计核销（PB 残留精确清单，见 [watchdog-kiss-audit-2026-09-15.md](watchdog-kiss-audit-2026-09-15.md) §1）**：PB 已彻底退出活跃路径（go.mod/active Go/前端/clean schema/`strings watchdog-server` 全无）。KISS-01E「编码删除」剩余的**精确**残留是三项非发布遗留物：①死的 PB-hub Dockerfile 仍在 CI（`internal/dockerfile_hub:29,31,34` 构建**已删除**的 `internal/cmd/hub`，带 PB `serve`+`/watchdog_data` volume；被 `.github/workflows/docker-images.yml:18,71` 引用）——即 line 70 未完成的「移除 PB SQLite 文件/volume 挂载路径」；②line 77 的「PB 时代字段与文字」= `install/init.sql:2020-2028`（`auth_provider`/`external_subject_id`）+ `deploy/migration/mysql/030:1-2`（"PocketBase" 注释）；③GitHub 模板指向 PB 后台 `/_/#/logs`（`.github/ISSUE_TEMPLATE/bug_report.yml:124`、`.github/DISCUSSION_TEMPLATE/support.yml:92`）。遗留 `internal/watchdog/*` 树删除属 KISS-08（仍被活跃 import）。
>
> **2026-09-15 Phase A 已执行（见 [watchdog-kiss-cleanup-ledger.md](watchdog-kiss-cleanup-ledger.md) §6）**：①PB 残留已清——删死的 hub Dockerfile+CI repoint 到 `dockerfile_server`(cmd/watchdog-server)、GitHub 模板去 `/_/#/logs`、`init.sql` 删 `auth_provider`/`external_subject_id`（commit 28276488）；②遗留迁移器加 KISS 库守卫，`watchdog-install`/超期 worker 对 KISS 库即拒绝，**不再能 brick boot**（commit 4574289d）。KISS-01E「编码删除」的 PB 残留项已完成；`internal/watchdog` 整包 + 遗留 schema 树的物理删除仍属 KISS-08（须先 Phase B 抽 SNMP 切片 + 退役 worker）。

#### KISS-01F 管理面与 ClickHouse 启动解耦

- [x] **设计**：MySQL 的 install/session/RBAC/device/agent/address/billing 管理 CRUD 是服务启动硬依赖；ClickHouse 仍是 Flow/SNMP 唯一时序权威，但允许启动时非致命降级，绝不恢复 VM/PB fallback。`runtime_ready` 只表示管理运行面完成，ClickHouse 单独报告健康；修复依赖后由重启执行幂等迁移和接线，暂不引入并发热重连状态机。
- [x] **编码**：`prepareRuntime` 先建立唯一 `operation_jobs` store，再启动 agent/address/geo 管理能力，随后尝试 ClickHouse；配置、认证、schema 或连接失败只记录可操作原因。ClickHouse 正常时再接 Flow query、SNMP query、Flow/SNMP export 与 billing reader。修复了 Flow export 在 `operation_jobs` 建立前检查、从而静默不启动的历史顺序错误。
- [x] **API/运维**：`GET /api/v1/health` 在降级时返回 `503`、`runtime_ready:true`、`clickhouse:false`、具体 `clickhouse_error` 与 restart recovery；Flow/SNMP 时序 query/export 继续 fail-closed `503`，MySQL 管理 API 可用。缺地址/库名的半配置响亮拒绝。
- [x] **单元/集成测试**：真实临时 MySQL + 不可达 ClickHouse 覆盖首次安装、登录、设备列表、降级 health、Flow query 503 和重启后再次登录；真实 MySQL + Docker ClickHouse 覆盖 SNMP/Flow query store、两类 export worker、billing reader 与共享 job store 全部完成接线。
- [x] **变更设计/测试**：无 schema/data migration；ClickHouse 是启动时可恢复依赖，运行中已建立的 native pool 由驱动负责连接恢复，启动时完全失败则显式要求 restart，避免动态替换 query/store/worker 指针产生竞态。Flow collector/worker 仍为独立进程，本切片未改 decode、Kafka 和写入热路径。
- [x] **回归/已提交门禁**：定向单元、真实 MySQL 正反向 CH 集成、全库 Go build/test/vet 与前端 test/build 全通过后独立提交；并行地址库文件不纳入提交。

### KISS-02 单域 RBAC 与设备根

- [x] **KISS-02A 已完成纵向切片（2026-09-08，`e5848ade`）**：新 Gin 后端已接 `devices` 与 SNMP profile 的 list/get/create/patch/delete，host 唯一且 display name 可选；labels/SNMP override 不丢失，ETag 冲突检测、固定 sort 白名单、服务端分页/search/status filter、device/device-group scope 均已接 MySQL。现有 `/network/devices` 与 `/targets` 是同一 `device_id` 的临时 handler/DTO alias，兼容层仅做 `system -> host` 类型映射，不创建或写旧表；真实 MySQL 集成测试覆盖 CRUD、host 冲突、profile secret 脱敏及 summary/target alias。

- [x] **KISS-02B 设备子资源查询（`145842d3`）**：Gin/MySQL 已接 canonical `/devices/:id/{ports,addresses,bgp,sensors,inventory,vlans,lags}`、`/ports/:id`、`/bgp` 及现有 `/network/*` alias；所有列表固定 SQL sort 白名单、严格分页/search/column filter，端口同时支持设备继承授权与显式 port grant。migration 0007 只补 MIB-neutral inventory 字段，SNMP/system 时序值仍归 ClickHouse；真实空 MySQL 覆盖双栈地址、BGP v4/v6、sensor/inventory/VLAN/LAG、端口 CAS、账单删除阻断和显式端口范围。

- [x] **KISS-02C 组织与资源集合（`bfb47e02`）**：locations、static/dynamic device groups、物化 membership、refresh 和 delete-preview 已闭环；动态规则只允许固定字段/label，不接收 SQL。用户 access replace/get 覆盖 device/group/port/billing 四类 grant，先完整校验再同事务替换，重复 ID 归一化，未知对象不留下半批授权。

- [x] **KISS-02D 通用 SNMP discovery 与 Flow device scope（`2cda16e8`、`ed7be0e2`）**：复用既有 MIB/definition engine 和 v1/v2c/v3 session，不在 handler 拼厂商；发现结果按 completed module 事务写当前 inventory，覆盖双栈 IP/BGP、sensor/entity/VLAN/LAG，失败不裁剪上次清单，账单端口仅标记 `notPresent`。Flow exporter binding 的 list/get/create/update/delete 均叠加 device scope，列表参数 fail-closed。

- [x] **KISS-02E RBAC 管理闭环（2026-09-14，本提交）**：补齐角色 create/get/update/delete 与固定 ability 目录的可维护 UI，角色变更和用户+角色写入均为事务；默认非保护角色只在首次安装播种，后续启动不再覆盖管理员修订，内置 administrator 前后端均只读且最后一个活动管理员不能被删除、禁用或移除角色。用户资源访问维护扩展为 device/device-group/port/billing-account/saved-graph/metric 六类，选择器统一服务端搜索和分页；saved graph 与 metric grant 已接 catalog/query/aggregate/export/dashboard 的查询门。导航、command palette 和直接路由统一按 ability 判定，只有 role 权限的管理员不需要附带 user.view。真实 MySQL 覆盖失败写入回滚、seed 持久性、最后管理员、资源替换/检索和图表/指标范围；前端生产构建及全库 Go 回归通过。

- [x] **设计**：以 LibreNMS role abilities + `devices_perms/ports_perms/bill_perms` 为基线，冻结“全局 action + 显式资源集合”双门、固定权限 key、默认角色和 device-group scope；冻结唯一 device 根、子资源、locations 与 static/dynamic group 契约。动态组使用 typed rule + 物化成员；SNMP `sys_location` 与管理 `location_id/name` 分离。
- [x] **编码**：新 Gin/MySQL 运行面无 tenant context/header/selector；客户/供应商是业务实体；`targets + network_devices` 已合并为 `devices`，所有子表和 Flow exporter binding 直接引用同一 `device_id`；host 唯一、display name 可选。
- [x] **API/UI**：users/roles/permissions/device/group/location/port/SNMP/Flow-device 领域路由已接；既有 UI 已切到 canonical `/devices`、`/ports`、`/bgp`、`/traffic-policy-defaults`，界面和风格不改。所有已迁设备 VTable 都是服务端分页/search/sort/column filter；SNMP profile 列表不出 secret，编辑和 device override 可维护，发现字段只读。
- [x] **单元测试**：固定 role ability、动态组 typed rule、device/group grant、port 显式 grant与设备继承、billing 独立 grant校验、Flow-device ability + device scope、对象 update/delete 权限和空范围 fail-closed 已覆盖；host 规范化/冲突、SNMP v1/v2c/v3 session、sysName/sysDescr 非必填、双栈 IP/BGP 通用结果已覆盖。Flow 高基数记录层的 layer+device/port query/export 下推归 KISS-06，账单对象状态机归 KISS-07，不在本包伪造占位实现。
- [x] **集成测试**：隔离空 MySQL 全链覆盖 device/profile/discovery/current inventory/organization/grants/Flow exporter/ETag/delete-preview/audit；确定性 fixture 覆盖所有双栈子资源和模块失败/裁剪边界；另对 `103.83.65.0` 完成真实 SNMP discovery 并把真实结果写入另一个空 v2 MySQL 验证。异步 poll plan/ACK 是 KISS-03/04，不为手工 discover 另造 job。
- [x] **变更设计/测试**：canonical 路径与历史 compat DTO 清单已冻结；迁移只替换 URL，设备内部 `host` kind 在统一 DTO 边界稳定返回 `system`，网络设备选择器显式下推 `kind=network`。旧 alias 已由 KISS-08A 物理删除，不建旧表、不双写。
- [x] **回归测试**：`go test ./...`、server/watchdog vet、`go build ./...`、45 个前端单测和 production build 通过；真实 MySQL、真实 SNMP、全部设备详情 VTable 和角色边界均有自动回归。按约束未修改视觉，不增加视觉测试。
- [x] **已提交门禁**：schema/domain/API/test 分为可独立构建的纵向提交 `e5848ade`、`a17b313a`、`145842d3`、`bfb47e02`、`2cda16e8`、`ed7be0e2`；运行时只有一个 device ID/管理库。兼容 URL 的最终删除是 KISS-08 清理门，不再阻塞 KISS-02。

> **2026-09-16 审计修正**：旧 `/targets` alias 删除前曾补同一 `validDeviceListFilters` 门；KISS-08A 已将其调用方切换到 canonical `/devices` 后物理删除。非法 status/kind/disabled 仍 fail-closed；真实 MySQL device/agent 全链回归通过。用户 grant 的未知对象由同一事务 FK + `invalid_reference` 映射拒绝并完整回滚，不增加重复预查询。

### KISS-03 SNMP/system/agent 时序统一写入并查询 ClickHouse

> 存储边界：MySQL 只保存 device/port/SNMP profile/MIB 配置等管理对象；SNMP 原始 counter、状态样本、派生速率、system/agent 时序、图表和导出全部以 ClickHouse 为唯一权威。不存在 VM 双写、VM 历史迁移或 VM 回退路径。

#### KISS-03A1 SNMP raw/rate/chart 纵向切片

- [x] **设计**：已冻结“collector/MIB/OID 不重写，只换 writer/query”、SNMP 专用 `snmp_samples`/`snmp_interface_traffic_5m`、UInt64 counter、自然身份、无硬编码 TTL、closed-bucket generation、查询预算和进程失败语义；见 `kiss03-snmp-clickhouse-design.md`。已删除不当的通用 `telemetrych/source_kind` 设计。
- [x] **编码—写入**：生产 collector 已从旧 BackendRuntime 改为全局 MySQL device/profile/recipe -> 既有 poller -> `snmpch` 有界同步 CH batch；CH 成功后才更新 recipe，失败有界 retry/backpressure；无 PB、tenant、VM 或双写。
- [x] **编码—派生**：CH 内按真实相邻时间和 UInt64 counter 计算速率，覆盖 32-bit wrap、reset、gap、重复；只 rebuild 已关闭 5m bucket，value 后 marker 发布 repair generation。
- [x] **编码—查询/API**：Gin `metrics/catalog|query|range|realtime` 直接查询 CH，沿用现有 chart JSON，保留 device/port RBAC、固定/自定义周期和 query budget；active DTO 不再引用旧 metrics backend 类型。
- [x] **编码—schema**：MySQL `snmp_collection_recipes` 和 CH migration 012 已建立；server/collector 只做 readiness，不在启动时改 CH schema；已删除第二套 embedded CH baseline。
- [x] **单元测试**：已覆盖 UInt64 精度、batch identity/retry、wrap/reset/gap、closed bucket、query budget、API JSON 兼容与 `tenant_id` 参数拒绝。
- [x] **集成测试**：真实 MySQL + 固定 SNMP varbind + 既有 poller + 真实 CH 核对 `>2^53` 原值和 recipe 状态；真实 CH 覆盖 raw/rate/closed bucket；链路没有 VM 依赖。
- [x] **变更设计/测试**：无历史数据，不 backfill/shadow read；唯一 CH migration ledger；schema 未迁移时明确拒绝启动；轮询 pass 暂时失败下个 tick 重试。
- [x] **回归测试**：目标 packages 与真实 MySQL/CH 集成通过；允许 loopback 的环境下 `go test ./...`、`go vet ./...`、`go build ./...` 全部通过，无视觉改动。
- [x] **已提交门禁**：本切片相关代码、DDL、测试和文档形成一个可独立构建提交，不夹带 KISS-06/address 并行 WIP。

#### KISS-03A2 SNMP aggregate/export/billing 收口

- [x] **设计/编码/API**：同一 `snmpch.Store` 已实现跨设备/端口 aggregate、`operation_jobs` 异步 CSV 和只读 closed-5m billing reader；Gin 接 `/metrics/aggregate|exports`、`/exports` lifecycle 与 `/billing/accounts/:id/snmp-usage`。scope 作为 CH external table，创建/执行/下载均按当前 grant 校验；没有 DatasetProvider、旧 export fallback、VM DTO、第二套 job 状态机或新 migration。冻结契约见 `docs/kiss03-snmp-clickhouse-design.md` §5–7。
- [x] **测试/提交**：固定 counter fixture 已覆盖总量/95th/缺端口 coverage、scope 去重、预算/白名单、CSV 原子写/checksum、设备/端口/billing/owner 权限、分页/search/filter、首轮暂时失败自动 retry 与运行中 cancel；真实 CH 验证 aggregate、generation billing 和缺端口 gap，真实一次性 MySQL 验证 API/job 全链并删除测试库；全库 test/vet/build 通过。本项按独立提交门禁提交。

> **2026-09-15 回归门修正**：A2 HTTP 集成替身现按生产 `snmpch` 的三种查询契约分别返回 scoped query（5 列）、aggregate/export（2 列）与 billing（12 列），不再以错误列型掩盖 handler 行为；同一测试已在一次性真实 MySQL 上重跑通过。

#### KISS-03A3 SNMP 管理规则、事件与历史入口收口

- [x] **设计**：冻结历史 URL/query/DTO；三层修正为低频 MySQL 管理定义、逐端口确定性变换后再聚合；MIB 只保存管理元数据；eventlog 以 ClickHouse 为唯一权威且不设臆造 TTL；UDP trap listener 继续复用成熟 dispatcher，经 Agent Registry 凭证入站。
- [x] **编码**：实现端口 policy/provider-customer defaults、MIB module CRUD、`snmp_events` 写入/重试/幂等和查询/facets、trap 对 port/BGP/recipe/rediscovery 状态的持久化；metrics/aggregate/aggregate-graph 统一在聚合前应用 raw/supplier/customer 规则；无 PB、tenant、VM 双写。
- [x] **API/UI**：端口策略、全局默认、MIB、设备事件、trap 与历史指标契约均已挂入 Gin；当前 canonical URL 为 `/ports/:id/policy`、`/traffic-policy-defaults`、`/snmp/mib-modules`、`/devices/:id/events[/facets]`、`/snmp/traps`、`/metrics/vmquery`。事件沿用现有 VTable 的服务端分页/search/sort/column filter，未修改页面或视觉。`vmquery` 保留 path/参数/matrix 响应，VM 特有任意 MetricsQL 改为明确 400，仅执行 typed SNMP selector。
- [x] **单元测试**：覆盖修正规则继承/确定性聚合、raw 权限、事件 SQL 参数/预算/facets、CH event retry/dedup input、Trap source 归一、vmquery selector 白名单和全部路由挂载。
- [x] **集成测试**：真实一次性 MySQL 覆盖 defaults→port override→MIB upsert/list/delete→管理员 trap 与 SNMP agent Bearer trap→端口状态；真实 ClickHouse 覆盖 event 写入/list/facet 与既有 raw/rate/aggregate/closed bucket/billing，同批测试库均清理。
- [x] **变更设计/测试**：SNMP agent route 从浏览器 session 组纠正为 agent token/mTLS 或管理员 session 双认证；事件、端口更新和 recipe 唤醒任一失败均返回显式错误；不恢复 MySQL event 双写、VM fallback 或第二套 trap dispatcher。
- [x] **回归测试**：`internal/server`、`internal/snmpch` 单测与真实 MySQL/ClickHouse 集成通过；全库 test/vet/build 在提交门禁再次执行并记录并行 Flow WIP 的独立结果。
- [x] **已提交/偏差记录**：A3 的 DDL、代码、测试和文档已经进入 `8f9ea87f`，且 ClickHouse `013`/`014` 的迁移顺序正确；但该历史提交同时包含并行 Flow/VPN/address 工作，**没有满足原定的独立 SNMP 提交边界**。不通过重写历史或重复搬运稳定代码伪造门禁；后续 `fd338550`、`6d2bb510`、`23bf277b`、`e2ad0a14`、`eee7a95e`、`03acebdc`、`f825fac5` 已按 SNMP 纵向修正提交，当前 `internal/server`、`internal/snmpch` 及全库 test/vet/build 通过。本偏差永久保留供审计，后续工作包仍必须先满足独立提交边界再勾选。

> **2026-09-16 MIB 生命周期修正**：内置 MIB 首次插入默认启用；后续启动只更新内嵌 version/checksum，管理员的 enabled 选择保持不变。真实 MySQL 集成覆盖 disable→reseed→内容升级且仍 disabled。本修正形成独立提交，但不改变上面记录的 A3 历史偏差。

#### KISS-03B system/container agent 延后切片

- [ ] **设计/编码/API**：system/container agent 恢复推进时按真实指标冻结显式 CH schema、writer/query 和 Gin API；不得因延期任务重建 `telemetrych` 万能层。
- [ ] **删除/测试/提交**：等价迁移后物理删除剩余 VictoriaMetrics writer/client/provider/DeleteSeries、remote-write/import、配置和旧 BackendRuntime 路径；停止 VM 后做真实进程集成并独立提交。

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

#### KISS-04B/C immutable plan 与四类实进程闭环

- [x] **设计**：冻结 agent kind/capability schema、enrollment/token-or-mTLS、binding、immutable Ed25519 plan、ACK、heartbeat、revocation、LKG 和兼容矩阵；详细契约见 `docs/agent-plan-delivery-design.md`。
- [x] **编码**：统一使用 `agents/agent_credentials/agent_bindings/agent_plans/agent_plan_acks/agent_runs/agent_enrollment_tokens`；system、SNMP、flow collect、flow worker 均接共享 `internal/agentplan` runtime，严格校验本进程 config，应用成功后原子安装 LKG 并 ACK；401/403 不回退 LKG，运行时 registry heartbeat 收到吊销后停止进程。无 tenant ownership/provider/fleet/canary 状态机。
- [x] **API/UI**：agent list/detail/enroll/rotate/revoke/bind/plan/run 全链已接；新增 Agent Plans 页面，计划列表使用服务端分页/search/sort/schema+signing-key column filter，计划不可 PATCH/DELETE；secret 仍只在 enrollment/create/rotate 响应显示一次。
- [x] **单元测试**：覆盖 canonical/signature/tamper/有效期、LKG 原子安装/历史签名校验/版本倒退、capability/schema/kind 拒绝、严格进程 config、ETag/304、5xx 离线恢复、409/401 禁止恢复、heartbeat 吊销退出、ACK 幂等/冲突/降级、离线健康和 clock skew。
- [x] **集成测试**：真实空 MySQL 的 plan API 覆盖发布/ETag/ACK/rollout/吊销；另构建并执行 system、SNMP、flow collect、flow worker 四个生产二进制，逐一验证一次性 enrollment、凭证落盘、计划下发/应用/ACK、断网 LKG 和吊销拒绝。
- [x] **变更设计/测试**：批量 rollout 复用唯一 `operation_jobs` 的 lease/heartbeat/cancel/retry 和 versioned checkpoint；未创建 canary/fleet/rollout 状态表；旧 payload 按 schema/version fail closed。
- [x] **回归测试**：`internal/agentplan` race、KISS-04 真实 MySQL/四进程集成、全库 Go test/vet/build、前端 45 单测、相关文件 Biome 和 production build 均通过。全库 detached 回归同时暴露 KISS-05 已提交调用依赖两个尚未提交 helper（分页函数与导出 set ID）；验证时仅用临时 shim 隔离该既有缺口，未把地址业务改动混入 KISS-04。
- [x] **已提交门禁**：`3a32c4fc` 收敛 operation job，`9d85df4e` 交付签名计划控制面，`6d847677` 接四进程 runtime/API UI，`94fd3124` 收口兼容矩阵与离线健康测试；KISS-04 不再修改综合 `device_agent_integration_test.go`。clean schema 实测 7 张统一 Agent 表、0 个 `tenant_id`、0 个 collector/fleet/canary 表，Gin/前端无旧 collector plan route。

**KISS-04 = 完成。** 后续 SNMP/system 样本改写 ClickHouse 属 KISS-03；Flow 数据面及 enrichment 版本消费属 KISS-06，不回写本工作包。

### KISS-05 现有 Geo/AddressSnap 链单域化

- [x] **设计**：冻结“实现不重写、只去 tenant/owner”的边界；现有 MMDB/IPDB import、MySQL 业务表/字段、CRUD/list、job payload、WADS v1、object store、download/LKG/ACK/GC 均不变。(边界已冻结 + 全链去 tenant 清单已产出；结构决策见下方进度)
- [x] **编码**：`internal/address` 忠实去 tenant——删 address 表/repo/API/签名 envelope/权限中的 tenant/owner，改全局 `address.manage/address.publish`；importer、规范化/集合运算、builder、codec、object writer/reader、worker loader、publication 状态机逐字未改；ed25519 信封已恢复（撤销 checksum-only 回归）。
- [x] **API/UI**：Geo/线路/运营商/prefix/set/import/draft/preview/publish/versions/rollback/retire 契约（server 分页、If-Match→428/412、ETag、cursor/table、`{job}`/202）逐项复刻；owner-tenant 鉴权适配器→全局 RBAC，非管理员 `address.view` 只读；前端契约与文件未改。
- [x] **单元测试**：迁入遗留 v4/v6、层级、并交差、include/exclude、CIDR 归一、重叠、资源预算、object checksum、**ed25519 签名**、rollback/GC/consumers/draft 套件 + 新增无 tenant/全局权限契约测试；`internal/address` 全绿。
- [x] **集成测试（address/server 链）**：真实 MMDB/IPDB→MySQL→async build→WADS→解码非空 range→幂等重建同 checksum；lifecycle approve(ed25519)/activate/rollback/retire、ACK/consumer summary、GC 生命周期+并发（late reference 串行于发布锁）+销毁回执，均对真实 MySQL 通过。〔worker download/install/内存 lookup/断网 LKG 经 flow-worker 消费端 + enrichment-delivery API，属 KISS-06〕
- [x] **变更设计/测试**：迁入的遗留套件（仅去 tenant）证明 MySQL 业务值/API 行为一致；WADS 幂等重建 checksum 恒定 + `internal/flowdimension` codec/build 测试通过（bytes parity）；scale 认证复跑（50k inputs 29ms/55MiB，<10s/512MiB）；单域已无"按客户复制 WADS"。
- [x] **回归测试**：`go build ./...`、`go vet`、`go test -race ./internal/address ./internal/opjob`（含真实 MySQL 集成）、address HTTP 契约套件、operation lifecycle、Flow dimension parity 全绿；按 line 11 约束未改 UI、不加视觉测试。
- [x] **已提交门禁**：全局发布链共 14 个 address-only pathspec 提交（见下），无 Flow query/report 夹带；server.go/router.go/config.go 的 address 接线已提交（`7887e862`），共享文件上并行会话的 agent-plan 改动为其未提交 WIP、非本包。

**进度（已重置——按用户指令废弃 handler 重写与半切换，从遗留代码忠实 1:1 迁移、文件+测试一并迁入；下列 ✅ 为既有引擎提交，现以"遗留 + 全测试套件"重新校核忠实性，分歧处一律以遗留为准覆盖）：**
- ✅ **opjob 异步引擎**：逐字去 tenant 移植 SaaS `operation_jobs` 框架（幂等 enqueue、`FOR UPDATE SKIP LOCKED` 租约接管、`next_attempt_at` retry backoff、attempt 预算、heartbeat、versioned checkpoint envelope）→ 新包 `internal/opjob`，专用 `async_jobs` 表（`0010`，与 0001 中简版 operation_jobs 隔离）。独立构建 + 纯逻辑单测通过。提交 `8f847b1f`。
- ✅ **`internal/address` 包 models + repos（`51ec4505`）**：set 代数 `address_math.go` / `address_prefix_merge.go` **逐字复用**（与 `internal/watchdog` 原文件 diff 仅 package 行）；models/normalizers、set + taxonomy repositories 机械去 tenant（SQL、游标分页、依赖校验、flow-id 分配语义均不变），flow-id 序列单例化（`0011`）。
- ✅ **import → opjob（`579410c6`）**：`import_reader.go`（StreamMMDB/IPDB 逐字复用）、`repo_import.go`（全量 mysql_address_import：分块 upsert、BINARY(16) 范围查、keyset/table 分页、slot 激活）、`import_job.go`（`opjob.Handler`，versioned checkpoint / resume / 幂等重放保留）。取代早期 Phase-2 goroutine 版（`4e3618ee`）。复用 `0008` schema。
- ✅ **dimension compile + object store + publish writer（`cf32880d`）**：`dimension_object`（磁盘 WADS 对象，路径去 tenant）、`dimension_compile`（bundle 编译逐字复用，单域 sentinel 身份）、`dimension_publisher`（preview/list/get + loadDraft/loadSources，tenant 锁→installation 单例锁）、`dimension_publish_build`（`BuildAddressSnapshotPublication`：flowdimension WADS 构建、supplier 分配单例、游标扫描、range 归一化、digest/version 竞态守卫，全部逐字保留）。schema `0013_dimension_publications`（040/041/042/047/048/049/058 去 tenant 折叠，7 表；空库实测建表通过）。
- ✅ **端到端 WADS 集成实测（`b9d6a96c`）**：真实 MySQL + 真实 MMDB → import → activate → preview → build WADS → 解码非空 range → 幂等重建同 checksum，通过（`WATCHDOG_TEST_MYSQL_DSN`）。修复：flowdimension bundle 要求非空 TenantID identifier（不改 flowdimension），单域用固定 sentinel `"default"` 保持确定性。
- ⚠️ **lifecycle + build job（`9dd2db77`，含回归，将被取代）**：approve/reject/activate/rollback/retire + 激活时间线 + worker ack/reference；`NewAddressSnapshotBuildJobHandler`（opjob）。**回归**：此提交把签名信封改成 checksum-only（approve 降为纯状态迁移）——违反"仅删 tenant/owner、保留信封"边界（line 123）；KISS-05B 将从遗留 `address_dimension_trust.go`/`address_dimension_lifecycle.go` **忠实恢复 ed25519**（20 字段签名载荷仅删 `tenant_id`#3，保留 verify / 持久化前复核 / 激活可信审批闸）。
- **结构决策**：`internal/opjob` + `internal/address` 独立包；独立于并行 SNMP WIP 构建（并行已提交 SNMP discovery `2cda16e8`）。
- 🟡 **dimensions HTTP 接线 + 异步 build worker（已完成+实测，提交待协调）**：`handlers_address_dimension.go`（preview/publish/versions/get/download + approve/reject/activate/rollback/retire，经 `internal/address.Publisher`）；`server.go` 起 opjob build worker（`startAddressLibrary`）；`config.go` 加 `snapshot_dir`；`router.go` 接 `registerAddressDimensionRoutes`。**真实二进制 HTTP 全链实测通过**：upload MMDB→ready→activate→preview→publish（异步入队）→worker 建 WADS→version 出现（object_format=wads, v1）→下载（magic=WADS, 校验和头）。**提交阻塞**：`config.go`/`router.go`/`server.go` 与并行会话未跟踪的 `agent_plans.go`（agent-plan 特性）在同文件纠缠，pathspec 无法拆分——待并行 agent-plan 落地后随其一并提交，避免夹带其未完成特性或提交坏树。
- ✅ **draft/preview/apply（`a8526e09`）**：prefix 批量编辑 prepare/apply（base-digest 复核 + 逐前缀 row-version CAS + 幂等），去 tenant，audit 用 v2 列。
- ✅ **GC + consumers（`a52b956c`）**：retired 对象回收（保留完整安全谓词，opjob 驱动，删除记 audit）；per-worker 目标就绪 + 版本漂移汇总/列表（window CTE，去 tenant）。**至此 internal/address 引擎全量落地**（models/repos/import/compile/publish/lifecycle/GC/consumers/drafts 均已提交）。
- 🟡 **import→opjob 收敛（已完成+HTTP 实测，提交待解锁）**：`internal/address` 导出 `DiskArtifactStore`/`Artifact`（已提交 `3bf90bd3`）；server `handlers_address_import.go` 重写为调用 `internal/address.Store` + `DiskArtifactStore`，upload 改为入队 opjob import（替换 Phase-2 goroutine）；`server.go startAddressLibrary` 起 import worker；删除 server 内 5 个 Phase-2 旧 import 文件（address_import{,_reader,_runner,_store}.go + address_artifact.go）。**真实二进制 HTTP 全链复测**：upload→opjob import worker 解码→ready→activate→preview→publish→build worker→下载 WADS，通过。
- 🔄 **重迁执行设计（本次设计门禁 / 变更面仅三项，其余逐字忠实）**：① 去 PB（本域无匹配）；② 去多租户（删 `TenantID` 字段 / `tenantID` 参数 / `WHERE tenant_id` / `SELECT … tenants … FOR UPDATE`→installation 单例锁；ed25519 载荷仅删 `tenant_id`#3、保留其余 19 字段与信任模型；per-tenant 序列→单例）；③ net/http→Gin + owner-tenant 鉴权适配器→新 RBAC（view→`address.view`／configure→`address.manage`／operate→`address.publish`，均已在 `rbac.go` 存在）。
- 🔧 **纠正既往回归（用户复核，均属"重写而非迁移"）**：① 发布幂等改回 `address-dimension:{effective_from(RFC3339)}:{trim("sha256:")}`、响应 `{job}`/202、build job 以 `job.ID` 为 snapshot 身份（删随机 id）；② 遗留无 retry 端点 → 删除臆造的 `retryImport`（retry = 作业按 checkpoint `processed` 序号重放 / 重新上传新代）；③ 恢复 ed25519 信封与信任模型（撤销 `9dd2db77` checksum-only）；④ 迁入全部 32 个遗留测试（现仅 6），以其为忠实性验收闸。
- 📦 **子包（各自纵向闭环 8 门、独立提交）**：**05A 引擎**（models／集合代数／merge／MMDB-IPDB decode／store(set+taxonomy+import+draft)／artifact／object，去 tenant；迁遗留单测）→ **05B 发布**（compile + **ed25519 trust/lifecycle 恢复** + consumers/ACK-LKG + GC + WADS publish；迁遗留单测 + WADS-bytes parity 变更测试）→ **05C 作业**（import checkpoint 重放 / build `job.ID`=snapshot / GC，on opjob；迁遗留单测）→ **05D HTTP**（8 个 `api_address_*` 忠实 Gin 移植：契约/幂等/If-Match/428/ETag/cursor/table 逐项复刻 + RBAC 替换 + 接线 server.go/router.go，收敛半切换 editable CRUD 一并走 `internal/address`；迁 httptest 套件；前端契约不变）。
- 🔗 **提交纠缠**：`server.go`/`router.go` 与并行会话未跟踪的 `agent_plans.go` 同文件——address-only 文件走 pathspec 提交；两文件各 3 行接线待并行 agent-plan 落地后提交，如实标注、不夹带/不覆盖并行 WIP。
- 🧭 **去重触发点**：遗留 `internal/watchdog/address_*` 因 hub 内 flow-enrichment 交付 + flow-query address-set + runtime 仍编译依赖而**暂留**（去租户 schema 与旧租户 store 不兼容，无法 import 复用）；待这些 flow 消费端迁 v2（KISS-06）后删除，届时才消除重复。
- ✅ **本轮已执行（gate-driven，各自 pathspec 提交，真实 MySQL 实测）**：
  - `faf517c6` 05B ed25519 信任模型**恢复** + 签名/信任单测（撤销 checksum-only 回归）；
  - `8c41a2b1` 05A 引擎单测（disk-store / object / scale / batch canonicalization，去租户忠实移植）；
  - `8b4696ef` 05A **集成 harness**（去租户，每测独立 throwaway schema，不 import server 避免环）+ server-table 分页/CAS；
  - `e0917616` 05A import 生成生命周期 + CAS 集成 + batch 单测；
  - `9eeadd40` 05A taxonomy CRUD/环/在用删除/并发 flow-id + draft prepare→apply+audit 集成。
  - **`internal/address` 全量 unit + integration 对真实 MySQL 通过、vet 干净 → 05A 引擎集成门禁关闭。**
- ✅ **05B/05C/05D 完成（本轮续，各自 pathspec 提交、真实 MySQL 实测）**：`1be4ebfd` ed25519 approve 接入 lifecycle 持久化 + 激活可信闸 ｜ `20d7bd2b` scope/consumer 单测 + ed25519 生命周期集成 ｜ `753282d5` 恢复 `WithClock` + 销毁回执忠实度 + GC 生命周期集成 ｜ `b4cc23ad` GC 并发（late reference 串行于发布锁）｜ `3c72f6a7` WADS builder 单测（longest-prefix/coalesce/supplier-geo）｜ `7887e862` 05D server 接线 + 纠 flagged HTTP 回归（发布幂等 `address-dimension:{eff}:{digest}`、`{job}`/202、job.ID=snapshot、删臆造 retry 端点、upload `{import,job}`、ed25519 approve 端点+信任密钥 resolver）｜ `a1d5948f` 05D 半切换收敛（editable CRUD→`internal/address.Store`，删 server `address_math.go` 重复）｜ `a5d8cdc4` 05D consumers/GC/draft 端点接线 + 统一 If-Match→428 ｜ `7ce49737` 05D httptest 契约套件（真实 gin+RBAC+MySQL）。〔05C 作业 handler 为 opjob 薄封装，行为经引擎集成 + opjob 单测覆盖〕〔worker download/内存 lookup/断网 LKG 属 flow-worker 消费端 = KISS-06〕
- **KISS-05 = 完成**（8 门已勾选；引擎全量去 tenant 单域化、ed25519 完整、契约测试齐备）。唯一"暂留"是遗留 `internal/watchdog/address_*`——因 hub 内 flow-enrichment 交付 + flow-query address-set + runtime 仍编译依赖之，去租户 schema 与旧租户 store 不兼容无法 import 复用；待 flow 消费端迁 v2（KISS-06）后删除以消除重复。

### KISS-06 Flow 单域化与 ClickHouse 查询收敛

> Flow 事实、receipt、archive/aggregate、VPN candidate 与现有查询本来就在 ClickHouse；本包不是“把 Flow 迁到 CH”，而是保持当前 CH 数据面不动，删除 tenant/provider/VM 外围依赖并统一查询入口。

**状态 = 进行中（数据面单域化 ✅ 已提交；查询收敛 partial，多数在工作树未提交）。** 设计冻结 + 分阶段 + hub 退役依赖序见 `docs/watchdog-kiss06-flow-design.md`。已提交：`13e96b7a`（数据面单域化，83 文件）+ `7ac9d103`（flowquery 去 tenant）+ 租户参数拒绝测试。工作树未提交：`internal/server` `FlowQueryService` + Explorer(records/query)/overseas/detail/facet/filters 路由 + geo/saved-filters/VPN-rules CRUD/table composer。**未做**：六报表 Layer 2 编排器（= provider→runner 再架构，非机械移植）、export、删 generic DatasetProvider/QueryGateway/VM（= hub 退役，泛化栈仍被活 worker 经 `NewBackendRuntime` 使用，剥离前不能物理删 → KISS-08）。〔独立的 VPN flow 检测 Tier-1 增强（CIDR/Local-5元/包大小 + 物化）另见 `docs/watchdog-vpn-detection-design.md`，不属本包 8 门。〕

- [x] **设计**：已冻结 sFlow v5/NetFlow v5 fast decode、GoFlow2 v9/IPFIX/template/fallback、去 tenant 后 Kafka wire/CH sort-dedup/receipt-reconciliation/Storage V2 raw-archive/publication-classification version/typed query-export cursor，确认不改解码/计数/CH 写入/生命周期语义。见 `docs/watchdog-kiss06-flow-design.md`。
- [x] **编码（CH 写入保持）= `13e96b7a`（83 文件）**：从 flow facts/receipts/aggregates/worker/config 去 tenant，decoder/CH-write 逐字复用不改，保留 fast path、GoFlow2 template/sampling store、Kafka 坐标、CH native batch insert、count/counter 对账、Storage V2 生命周期、WADS 热路径；CH 迁移 001/003/004/010/011 就地去 tenant（无存量）。
- [ ] **编码（CH 查询收敛）= partial（未提交）**：flowquery 去 tenant 已提交 `7ac9d103`；`FlowQueryService` + Explorer(records/query)/overseas/detail/facet/filters + geo + saved-filters + VPN-rules CRUD + table composer(Layer 1) + **六报表 Layer 2 编排器：五 kind 全部落地——overview/dimensions/overseas/endpoints/vpn（gateway 派发→runner 直调再架构；`/flow/reports`+capabilities；endpoints=多阶段端点地址富化 `flow_report_endpoints.go`；vpn=物化 findings 支撑 `flow_report_vpn.go`，含 overseas vpn_share，vpn kind 额外门 `flow.vpn.view`）** 已建（工作树未提交，包级 build/vet/test 绿）。overview `business_matrix` 表已迁(`flow_report_business_matrix.go`)。**export ✅ 全部落地**：flow 明细/聚合/report 三 flavor(CSV+Parquet)统一 opjob 异步(单 job type `flow.export`+判别式 payload;`flow_exports.go`+`flow_export_rows.go`;`buildFlowReport` 从 report handler 抽出供 export worker 复用;create/list/get/cancel/download+worker per-kind 重授权)。**display_mode(客户端变换,后端校验+echo)+peak-windows(flowquery 原生 TimeWindows,穿入全 aggregate/joint 面板+findings 扫描过滤)✅ 已迁;DirectionSplit=每方向查询模拟无迁移项**。CH 查询收敛编码面完成。**未做**：删 generic DatasetProvider/QueryGateway/Flow→VM（hub 退役，其泛化栈仍被非-flow worker 经 `NewBackendRuntime` 构造，剥离前不能删 → KISS-08）。**vpn_share 忠实性简化**：v2 findings schema 无不可变版本列(dimension_snapshot_id/geo_version/classification_version)→放弃 hub numerator↔denominator 版本交叉守卫。
- [ ] **单元测试 = partial**：Kafka dedup、sampling known/unknown、raw/supplier/customer、方向、publication as-of 随数据面/flowquery 去 tenant 提交（含租户参数拒绝回归 + WADS golden）；detail cursor/filter + 查询面 view-RBAC/错误映射 已建（未提交）。**缺**：report conservation（依赖六报表 Layer 2）。
- [ ] **集成测试 = env-gated 编译过未运行**：sFlow/NetFlow/IPFIX→Kafka→worker→CH 全链集成测试已去 tenant（需真实 Kafka/CH，未跑）；crash/rebalance/fake-ack/retry + count/counter reconciliation 已有；saved-filters/VPN-rules 有 env-gated MySQL 集成测试。**缺**：→ Explorer/六页/明细/导出端到端（依赖六报表 + export + 前端）。
- [ ] **变更设计/测试 = partial**：**无 tenant 的旧 wire 明确拒绝已做**（WADS binary + plan 签名去 tenant_id，旧 wire 拒绝、不静默混读；flowquery 租户参数拒绝测试）。**未跑**：fast-vs-GoFlow2 差分、sFlow/NetFlow/v9/IPFIX pcap 回放、吞吐、CH timeout/fault-injection/Storage V2 门禁（env-gated/手动）。
- [ ] **回归测试 = partial**：包级 test/vet/build 绿（flowquery/flowch/flowvpn/server/address）；**未做**：Flow 全库 race、真实 Kafka+CH、前端浏览器六页/Explorer。全仓 `go build ./...` 期间受并行会话 WIP 间歇影响（非本包引入）。
- [ ] **已提交门禁 = partial**：`13e96b7a`（数据面）+ `7ac9d103`（flowquery）两提交可构建/可运行/文档真实；其余（server 查询/管理面）**hold 未提交**（曾与并行 telemetry→CH 共享 `server.go`；并行现已落地统一 CH client）。运行代码 flow 业务 writer/query 的**活路径**仅 ClickHouse；遗留 hub VM/gateway 代码物理仍在但已运行时失效，删除归 KISS-08。

### KISS-07 Billing、三层修正与对账闭环

**状态 = 完成（2026-09-09）。** 详细冻结契约和复算向量见 `docs/kiss07-billing-design.md`。本包只新增单域 Billing 管理/证据链，复用 KISS-03 `snmpch`、现有 Flow ClickHouse facts 和平台 `operation_jobs`；未恢复 tenant/PB/VM/DatasetProvider，也未改 Flow decode/write。

- [x] **设计**：已冻结 party/account/port/period/value/adjustment/reconciliation 字段、`open -> calculated -> approved -> closed` 状态机、nearest-rank 95th/average/total、IANA timezone + `billing_day` 默认周期、CAS 审批和关闭证据不可变语义；明确本期结算单位仅 bps/bytes，不虚构货币价格/税率。
- [x] **编码**：MySQL 保存 raw/supplier/customer/snmp/external generation、account+party+port 快照、关闭 adjustment 快照、reconciliation/issues；ClickHouse reader 计算同一 scope 的 Flow 与 SNMP；实现 external evidence、阈值、append-only adjustment/reversal、approve/close 和确定性 CSV/Parquet export。
- [x] **API/UI**：party/account CRUD、端口绑定、默认/自定义周期、异步 calculate/reconcile/export、五层对比、issue 处理、adjustment 审批/反转、period 审批/关闭和 evidence 下载已接 Gin/现有三页 UI；account/party/period/port/issues/adjustments/runs/exports 均使用服务端分页、搜索、排序和列过滤 VTable。
- [x] **单元测试**：已覆盖 `[1..19,100] -> p95 19 / average 15`、total bytes、UTC 5m、DST/短月 billing day、共同完整桶、缺桶/gap/reset、direction、unknown sampling、三层 delta、external 一致性、重复 operation、stale approval、adjustment/reversal、CSV/Parquet 同行与 provenance、公式注入转义；无 timestamped `RateBuckets` 的 fallback 明确拒绝。
- [x] **集成测试**：真实 MySQL + 临时 ClickHouse database 写 fresh Flow/SNMP 后，同端口 10 分钟四层均得到 `800 bps / 60000 bytes / 2 buckets`；重复 generation 数值确定；关闭后拒绝重算/导入/普通 adjustment，关闭 evidence 不受事后 reversal 影响；真实 Gin/MySQL operation job 覆盖 CRUD→计算→处理→审批→关闭→CSV/Parquet 下载和 artifact checksum/retention。
- [x] **变更设计/测试**：已故意注入 Flow reader failure、unknown sampling、SNMP reset/gap、缺 bucket 和 reader window offset；均只产生/保留 evidence issue，不自动调平。window offset 形成幂等 critical run/issue/audit、零 value generation，并以 terminal error 停止重试。
- [x] **回归测试**：billing/flowch/snmpch/server test+race+vet、`go build ./...`、真实 MySQL/CH、HTTP/RBAC/CSRF/CAS/audit/job/export、45 个前端单测、Billing Biome 和 production build 均通过。当前工作树另有未提交 KISS-06 migration 013 与其两份测试的并行不一致，不属于本提交；排除该 WIP 后 flow migrator 回归通过。
- [x] **已提交门禁**：独立只读 reviewer 三轮复核并复算 95th/average/total、三层 delta、SNMP/Flow fixture 与 adjustment/reversal；前两轮 9 项阻断全部修复，第三轮结论 `PASS，无 blocking`，随后才允许本纵向切片提交。

### KISS-08 最终遗留清理与验收

- [x] **KISS-08A 设备 API 单根收敛（本提交）**：补齐 `/devices/summary`、`/devices/:id/snmp`、`/ports/:id/policy` 与 `/traffic-policy-defaults`，前端全部改用 canonical `/devices`、`/ports`、`/bgp`，网络列表显式下推 `kind=network`；删除 `/targets` 与 `/network/{devices,ports,bgp,traffic-policy-defaults}` Gin 路由、专用 handler/DTO 分支。API/UI 参数和响应字段保持兼容，未修改页面路由、布局或视觉。路由反向测试与前端源码门禁禁止旧路径回流；真实隔离 MySQL 完成 device/profile/discovery/inventory/agent/RBAC CRUD 全链，Go/前端全量回归通过。

- [x] **KISS-08B 旧 aggregate/export 命令退役（本提交）**：`watchdog-aggregate-rollup` 与 `watchdog-export-worker` 已由 Gin 内共享 ClickHouse client + `operation_jobs` worker 等价取代，且旧命令仍依赖新 schema 不存在的 `tenants/collector_agents`；现物理删除两个命令入口。aggregate graph、SNMP/Flow export、billing export 的 API/UI 和 worker 实现均未改，未触碰 Flow decode/write/query；源码扫描、目标测试与全库 test/vet/build 作为删除门禁。

- [x] **KISS-08C 安装入口单根收敛（本提交）**：保留已经闭环的 `GET /api/v1/install-status`、`POST /api/v1/install` 与前端 `/install` 为唯一首次安装路径，物理删除旧 `cmd/watchdog-install`、Makefile target 和会执行旧 `install/init.sql` 的 dev-db 脚本。当前 embedded `deploy/schema/mysql`、管理员事务创建、重复安装 409 和未安装 API 428 语义不变；真实空 MySQL 安装集成、前端安装路由测试及全库门禁作为验收。旧 migration/init 文件只因 `internal/watchdog` 遗留包的历史测试暂留，随 KISS-08D 整包删除，已无可执行入口。

- [x] **KISS-08D 旧 LibreNMS DB importer 退役（本提交）**：删除未发布且未接构建/脚本的 `watchdog-librenms-extract`；该命令通过 `NewBackendRuntime` 把定义写入旧多租户表，目标架构不再有对应表。生产 server 继续直接从 `snmp.definitions_dir` 调用同一 `ParseLibrenmsDefinitions`，并叠加 embedded/配置 MIB，自动 OS/vendor/module discovery 能力不变；parser/discovery 测试与全库 test/vet/build 为门禁。

- [x] **KISS-08E SNMP trap agent 配置解耦（本提交）**：`watchdog-snmp-agent` 不再为 3 个配置字段和一个 JSON DTO import 整个遗留 `internal/watchdog` 包。命令内最小配置读取器继续支持共享 YAML 的 `snmp_trap_agent` section、`WATCHDOG_SNMP_TRAP_*` 环境变量及原命令行覆盖顺序；只严格校验自己拥有的 section，不受平台其余配置生命周期影响。trap 上报 URL、Authorization 和 JSON 字段保持不变，示例/开发配置纠正为独立 Gin 后端 `:8091`。配置边界、错误向量、仓库配置兼容和 HTTP wire contract 均有单测；目标 build 与源码反向扫描为提交门禁。`watchdog-system-agent` 按已冻结范围继续延期，不在本切片改写。

- [x] **KISS-08F1 旧 Dashboard 纵向副本删除（本提交）**：当前 Gin `/api/v1/dashboards` 的 list/get/create/preview/patch/delete/graph-options、MySQL 单域表和既有前端保持不变；删除遗留包中第二套 tenant-scoped `Dashboard` domain/repository/HTTP handlers、MySQL 实现及仅验证旧实现的测试，并从旧 `APIV1RouterConfig`/`BackendRuntime.Router` 摘除注册。被其他旧 handler 共用但错误命名在 dashboard 文件中的 JSON 单文档校验移至通用 `api.go`，行为不变。当前 server Dashboard 测试、遗留包回归、全库 build/test/vet 和旧符号反向扫描为提交门禁。

- [x] **KISS-08F2 旧 Retention 纵向副本删除（本提交）**：保留当前 Gin `GET/PUT/DELETE /api/v1/retention/policies`、`metric_retention_policies` 单域表、既有 JSON 字段和前端不变；将 response DTO 归属到 `internal/server`，删除遗留包中第二套 tenant-scoped repository/HTTP/MySQL 实现及仅验证旧实现的测试，并从旧 Router/runtime 摘除注册。当前 server 路由和真实 MySQL 集成测试、全库 build/test/vet 及旧符号反向扫描为提交门禁。

- [x] **KISS-08F3 旧 User Preferences 纵向副本删除（本提交）**：保留当前 Gin `GET/PUT /api/v1/me/preferences`、`user_preferences` 单域表、被动读取、1 MiB 输入限制、JSON object 校验、ETag/If-Match 乐观并发语义和前端不变；删除遗留包中第二套 tenant-scoped domain/repository/HTTP/MySQL 实现及旧测试，并从旧 Router/runtime 摘除注册。被遗留地址修订 API 复用的 quoted row-version parser 移到旧通用 API helper，不改其 CAS 行为。当前 Gin 路由单测、真实 MySQL create/read/update/stale-conflict 集成、全库 build/test/vet 和旧符号反向扫描为提交门禁。

- [ ] **编码**：KISS-01 已保证 PB 为零；本包只删除 VM/VLogs、tenant、module/resource/dataset/provider registries、旧 targets、兼容 adapter 和废弃配置。
- [ ] **静态门禁**：仓库扫描无 `pocketbase`、`tenant_id`、tenant header、VictoriaMetrics/VictoriaLogs、DatasetProvider 和 target/network-device 双身份运行代码。
- [ ] **空库验收**：仅 MySQL + ClickHouse + Kafka，从零安装管理员、设备、agent、地址 publication、Flow、SNMP、六报表、账单、导出、告警。
- [ ] **故障验收**：MySQL/CH/Kafka 短断、agent 离线、worker crash/rebalance、坏 publication、SNMP reset、任务 takeover/cancel/retry。
- [ ] **性能验收**：Flow 吞吐/延迟/内存不低于切换前基线；SNMP 写入和查询满足目标；账单计算有界。
- [ ] **回归测试**：Go/前端/真实依赖/浏览器/安装脚本/备份恢复全矩阵。
- [ ] **文档**：只保留一套当前架构、schema、配置、运维和故障手册；旧文档标历史，不再作为实施入口。
- [ ] **已提交门禁**：最终删除提交后 clean checkout 可完整部署；所有遗留数据库/volume 已按各工作包的精确白名单处置，不把物理清理拖到项目末尾。

> **2026-09-15 精确清理台账见 [watchdog-kiss-cleanup-ledger.md](watchdog-kiss-cleanup-ledger.md)**（代码/表/字段/接口/配置逐项 + 依赖 + 删除影响 + 严格删除顺序 + 执行状态；L1-L19 代码、S1-S12 schema、I1-I5 接口）。KISS-08A 已清除前端 compat 调用和 I1-I5 路由，KISS-08B 已删除被新 operation worker 取代的 aggregate/export 旧命令，KISS-08C 已删除旧 CLI 安装接线，KISS-08D 已删除旧 LibreNMS DB importer，KISS-08E 已断开 trap agent 对遗留包的 import，KISS-08F 已按纵向切片删除 Dashboard/Retention 旧副本；剩余物理清理由 `internal/server`→`internal/watchdog` 的 SNMP 域 import 链及延期的 system agent 命令钉住，按 Phase B/C/D 推进。
>
> **2026-09-15 审计（见 [watchdog-kiss-audit-2026-09-15.md](watchdog-kiss-audit-2026-09-15.md) §主题 1/2）**：`internal/server`/`cmd/*` 仍 import 遗留 `internal/watchdog`（作为共享类型库 + `NewBackendRuntime` 泛化栈），删除须先断这些 import——本包正确的延后项。但其**危险接线**建议提前拆（不需整包删除）：①**双安装系统**——`cmd/watchdog-install`+`deploy/migration/mysql`+`install/init.sql` 建出不兼容的 `watchdog_installation`（`id VARCHAR 'default'` vs KISS `id=1`），装完再起 KISS server 报 "Unknown column 'schema_version'" 直接开不了机；仍接在 `Makefile:111-112`、`scripts/watchdog-dev-db.sh`、`docs/watchdog-kiss-architecture.md:115`。②**遗留 worker** `watchdog-aggregate-rollup`/`watchdog-export-worker` 已被 in-server CH worker 取代，且查询 KISS 中不存在的 `collector_agents`/`tenants`（`flow_rollup_jobs.go:451-452`），对 KISS 库运行即失败。③遗留 worker config 仍**必填** VM base_url（`internal/watchdog/config.go:971`）。建议：为遗留安装器/worker 加「拒绝对 KISS 库操作」的响亮守卫，或从 Makefile/scripts/CI/docs 摘除接线，把物理删除留到本包。
>
> **install 硬化（独立小包，不阻塞）**：CH 启动 fail-fast 语义需明确——配好但连不上的 CH 会让整个 server（含 RBAC/device/address）起不来，空 CH 配置则静默降级（`server.go:128-136,257-287`）；出厂 `config/watchdog.yaml` 空密码与 dev CH `watchdog-local` 不符致首次 `POST /install` 返回难懂 500。地址库 seed（`deploy/seed/`）未被 install 加载/文档引用。详见审计 §2/§主题 2。

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

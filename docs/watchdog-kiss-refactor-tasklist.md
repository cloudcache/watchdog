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

> **2026-09-16 三视角语义修正**：`customer/supplier/raw` 固定读取同一端口集合；前两者分别使用 customer/provider policy 的步进、1000/1024 显示基数及每端口每桶可复现的区间修正，raw 不修正。删除把 `side_type` 当端口归属过滤器的错误实现，并以同端口三结果、快速切换取消、中文标签和真实设备 62 端口门禁锁定。

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

**状态 = 平台单域化与 ClickHouse 查询收敛已完成。** 数据面由 `13e96b7a` 去 tenant，`flowquery` 由 `7ac9d103` 收敛；Gin `FlowQueryService`、Explorer、明细、Geo、saved filters、VPN rules、固定报表和异步导出已在 `8f9ea87f` 进入提交，并由后续生命周期提交继续复用。KISS-08 已删除 `internal/watchdog`、`NewBackendRuntime`、DatasetProvider/QueryGateway 和 VM provider 活实现。剩余历史重分类、VPN publication/worker ACK、主动探测及集群/性能发布门只在 Flow 清单跟踪，不再把它们误写成 KISS-06 平台迁移未完成。

- [x] **设计**：已冻结 sFlow v5/NetFlow v5 fast decode、GoFlow2 v9/IPFIX/template/fallback、去 tenant 后 Kafka wire/CH sort-dedup/receipt-reconciliation/Storage V2 raw-archive/publication-classification version/typed query-export cursor，确认不改解码/计数/CH 写入/生命周期语义。见 `docs/watchdog-kiss06-flow-design.md`。
- [x] **编码（CH 写入保持）= `13e96b7a`（83 文件）**：从 flow facts/receipts/aggregates/worker/config 去 tenant，decoder/CH-write 逐字复用不改，保留 fast path、GoFlow2 template/sampling store、Kafka 坐标、CH native batch insert、count/counter 对账、Storage V2 生命周期、WADS 热路径；CH 迁移 001/003/004/010/011 就地去 tenant（无存量）。
- [x] **编码（CH 查询收敛）**：Gin `FlowQueryService` 直接组合 `flowquery` runners；Explorer、records/detail、overseas、Geo、saved filters、VPN rules、固定报表和 CSV/Parquet operation-job export 均已提交。运行代码只读 ClickHouse；没有 DatasetProvider/QueryGateway/VM fallback。
- [x] **单元测试**：覆盖 Kafka 坐标去重、sampling、raw/supplier/customer、方向、publication as-of、detail cursor/filter、查询 RBAC/错误映射和固定报表守恒。
- [x] **集成测试**：真实 Kafka/ClickHouse 四协议数据链及真实 ClickHouse 查询已覆盖；Gin 安装链覆盖 Flow query/overview report，固定报表 HTTP 与导出有真实依赖/确定性 fixture 证据。产品级非空六页与组合故障继续归 Flow 清单，不重复作为 KISS-06 门禁。
- [x] **变更设计/测试**：旧 tenant wire/参数明确拒绝；KISS-08 删除旧 Hub/provider 后，生产源码不再存在双查询栈。Storage V2、滚动版本和固定硬件门禁由 Flow 清单独立追踪。
- [x] **回归测试**：Flow/Server 目标测试、全库 test/vet/build、前端 test/typecheck/build 已在提交门禁执行；六页视觉和生产数据验收仍按 Flow 产品发布门执行。
- [x] **已提交门禁**：`13e96b7a`、`7ac9d103`、`8f9ea87f` 及 KISS-08 删除提交构成可复现提交链；当前工作树的 Storage V2 L5 改动不属于 KISS-06。

### KISS-07 Billing、三层修正与对账闭环

**状态 = 完成（2026-09-09）。** 详细冻结契约和复算向量见 `docs/kiss07-billing-design.md`。本包只新增单域 Billing 管理/证据链，复用 KISS-03 `snmpch`、现有 Flow ClickHouse facts 和平台 `operation_jobs`；未恢复 tenant/PB/VM/DatasetProvider，也未改 Flow decode/write。

- [x] **设计**：已冻结 party/account/port/period/value/adjustment/reconciliation 字段、`open -> calculated -> approved -> closed` 状态机、月 95/日 95/月平均/总量算法、IANA timezone + `billing_day` 默认周期、CAS 审批和关闭证据不可变语义。2026-09-17 产品修正后明确拆分 `measurement_type=bandwidth|traffic` 与 `billing_method=package_port|monthly_95th|daily_95th|monthly_average`；带宽保底直接使用端口标称带宽合计值的百分比并冻结到账期，不再使用 `cdr` 或固定 Mbps“承诺带宽”。
- [x] **编码**：MySQL 保存 raw/supplier/customer/snmp/external generation、account+party+port 快照、关闭 adjustment 快照、reconciliation/issues；ClickHouse reader 计算同一 scope 的 Flow 与 SNMP；实现 external evidence、阈值、append-only adjustment/reversal、approve/close 和确定性 CSV/Parquet export。
- [x] **API/UI**：party/account CRUD、端口绑定、默认/自定义周期、异步 calculate/reconcile/export、五层对比、issue 处理、adjustment 审批/反转、period 审批/关闭和 evidence 下载已接 Gin/现有三页 UI；`/billing/new` 与编辑页按设备名称/IP 搜索后选择/全选端口，可跨设备绑定并为每个端口指定 in/out/agg，账户字段与完整端口集合原子提交，同时配置 raw/supplier/customer 取值策略、带宽/流量计量、包端口/月 95/日 95/月平均和百分比保底；account/party/period/port/issues/adjustments/runs/exports 均使用服务端分页、搜索、排序和列过滤 VTable。
- [x] **单元测试**：已覆盖 `[1..19,100] -> p95 19 / average 15`、逐自然日日 95 后取平均、total bytes、百分比保底与端口 capacity 快照、UTC 5m、DST/短月 billing day、共同完整桶、缺桶/gap/reset、direction、unknown sampling、三层 delta、external 一致性、重复 operation、stale approval、adjustment/reversal、CSV/Parquet 同行与 provenance、公式注入转义；无 timestamped `RateBuckets` 的 fallback 明确拒绝。
- [x] **集成测试**：真实 MySQL + 临时 ClickHouse database 写 fresh Flow/SNMP 后，同端口 10 分钟四层均得到 `800 bps / 60000 bytes / 2 buckets`；重复 generation 数值确定；关闭后拒绝重算/导入/普通 adjustment，关闭 evidence 不受事后 reversal 影响；真实 Gin/MySQL operation job 覆盖 CRUD→计算→处理→审批→关闭→CSV/Parquet 下载和 artifact checksum/retention。
- [x] **变更设计/测试**：已故意注入 Flow reader failure、unknown sampling、SNMP reset/gap、缺 bucket 和 reader window offset；均只产生/保留 evidence issue，不自动调平。window offset 形成幂等 critical run/issue/audit、零 value generation，并以 terminal error 停止重试。
- [x] **回归测试**：billing/flowch/snmpch/server test+race+vet、`go build ./...`、真实 MySQL/CH、HTTP/RBAC/CSRF/CAS/audit/job/export、57 个前端单测、Billing Biome 和 production build 均通过。
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

- [x] **KISS-08F4 旧 SNMP Profile/MIB HTTP 副本删除（本提交）**：保留 Gin `/api/v1/snmp/profiles` 五个 CRUD 路由与 `/api/v1/snmp/mib-modules` 三个 CRUD 路由、请求/响应字段、ETag/CAS、密钥只在单记录读写返回、built-in MIB 保护和前端不变；只删除遗留 `net/http` profile/MIB handlers 及其专用测试。旧 network/discovery/poll runner 尚需的 `SNMPRepository`、MySQL 读取与 SNMP 引擎全部保留；其他遗留测试共用的 fake 移到中性 test helper，不以已删 HTTP 测试文件承载。Gin 全路由单测、真实 MySQL profile/MIB 集成、全库 build/test/vet 及旧 handler 符号反向扫描为提交门禁。

- [x] **KISS-08F5 旧 Audit Log HTTP 副本删除（本提交）**：保留 Gin `GET /api/v1/audit-logs`、`/api/v1/audit` alias、RBAC、服务端分页、`resource_type/action/q/cursor/limit/offset` 和 actor username join；迁移审查发现 Gin 遗漏的旧 `resource_id`/`actor_id` 精确过滤参数后，再删除旧 tenant-scoped `net/http` reader/handler 及它的专用 endpoint 测试。旧包内尚被 destruction receipt 等测试使用的 audit write/list repository 未删。当前 Gin 路由单测、真实 MySQL 用户名+两个精确过滤集成、全库 build/test/vet 和旧 handler 符号反向扫描为提交门禁。

- [x] **KISS-08F6 旧 all-in-one Backend Runtime 删除（本提交）**：全库生产代码已无 `NewBackendRuntime`/`BackendRuntime` 调用；当前 `watchdog-server`、`watchdog-snmp-collector`、SNMP trap agent 和 Flow 进程均由独立入口组装。因此物理删除这个同时组装 tenant/PB 后遗留 management API、VM query/export、SNMP poll、Flow rollup、aggregate rollup、remote provider 和后台 goroutine 的 816 行死 runtime，并删除仅构造该死 runtime 的测试。旧 test router 暂需的 runtime-health response DTO 留在 router 边界，不再拥有运行时实现。当前 server/SNMP/Flow 独立 command build、目标包测试、全库 test/vet 和旧 runtime 符号零引用为提交门禁。

- [x] **KISS-08F7 旧 Aggregate Graph HTTP 副本删除（本提交）**：当前 Gin `/api/v1/aggregate-graphs` 已完整承接 CRUD、items/ports 绑定、series/data/summary、两门 RBAC、服务端约束和 ClickHouse SNMP 查询；删除旧 tenant-scoped `net/http` handler、router 注入点和专属 endpoint test。仍被旧包内 rollup 测试闭包使用的 tenant repository/domain 暂不混入本切片，后续按真实引用闭包删除。当前 Gin aggregate graph 单元、真实 MySQL + CH query executor 契约集成、全库 test/vet/build 与旧 handler 符号零引用为提交门禁。

- [x] **KISS-08F8 旧 Aggregate Graph rollup/repository 闭包删除（本提交）**：F7 摘除最后生产入口后，反向引用确认旧 `AggregateGraphRollup`、tenant repository/DTO、MySQL 实现和 graph-series builder 只在自身测试内成环；物理删除该闭包以及已经无消费者的 `aggregate_graph.rollup_interval` YAML/env/config。当前 Gin 单域 `aggregate_graphs/items/ports` 表、请求响应、ClickHouse 即时查询和页面不变；仍被旧 VM query 使用的通用 `aggregatePortSeries` 保留。当前 Gin aggregate 单元/真实 MySQL 契约、配置测试、全库 test/vet/build 和旧符号零引用为提交门禁。

- [x] **KISS-08F9 旧 Permission CRUD 副本删除（本提交）**：目标模型已经冻结为固定 ability catalog、角色绑定 ability、用户绑定 device/group/port/billing/graph/metric 资源；当前 Gin 保留 `GET /api/v1/permissions`、roles CRUD 与 users access CRUD/picker。删除旧 tenant-scoped 任意 `subject/resource/action` Permission PUT/DELETE/effective handler、test-router 注入与专属测试；同时删除已不可达且调用不存在 PUT API 的前端 permission form 和两个旧路由 alias，活跃 `/permissions` catalog 页面与视觉不变。Gin RBAC 单元/真实 MySQL 管理集成、前端路由/test/lint/typecheck/build、全库 test/vet/build 和旧 handler/URL 零引用为提交门禁。

- [x] **KISS-08F10 旧 BGP HTTP 副本删除（本提交）**：当前 Gin 已完整承接 canonical `/api/v1/bgp`、`/api/v1/devices/:id/bgp`，包含 v4/v6 afi/safi、服务端分页/搜索/排序/过滤和设备资源权限；删除只注册已禁用 `/api/v1/network/...` 别名的旧 tenant-scoped `net/http` handler 与专属测试。BGP domain/repository、SNMP discovery 写入与 MySQL 表不变。Gin 路由、IPv6/分页集成、前端 canonical API 扫描、全库 test/vet/build 和旧 handler 零引用为提交门禁。

- [x] **KISS-08F11 旧 Graph Overview HTTP 副本删除（本提交）**：当前 Gin 已在单一 device root 上承接 `/api/v1/graph/devices/:id/overview`、`/api/v1/graph/ports/:port_id/overview`，仅返回可授权设备/端口的面板查询，并由 canonical ClickHouse metrics API 解析。将旧 handler 用例的 signed 进出流量、BGP 和 sensor 面板契约转入现行真实 MySQL 集成测试后，删除旧 tenant-scoped `net/http` handler/test-router 注入与专属测试；共享 graph panel builder 保留。真实 MySQL、全库 test/vet/build 和旧 handler 零引用为提交门禁。

- [x] **KISS-08F12 旧 SNMP Trap HTTP 副本删除（本提交）**：当前 Gin 保留原 URL `/api/v1/snmp/traps`，已承接管理员会话与 SNMP agent token/mTLS、agent-device binding、trap dispatcher、ClickHouse 事件写入、端口/BGP 状态更新和立即采集触发。删除仍经 tenant repository 持久化的旧 `net/http` handler 及 router config 注入；SNMP trap domain/dispatcher/MIB handlers 保留。现行管理员与 agent 认证集成、CH sink 错误传播、全库 test/vet/build 和旧 handler 零引用为提交门禁。

- [x] **KISS-08F13 旧 Module Center HTTP 删除（本提交）**：单域固定产品不存在 tenant module enable/disable 或 runtime target-kind registry 管理面；全库扫描确认前端和 Gin 无 `/api/v1/modules`、`/api/v1/tenants/:id/modules` 消费者。删除旧 `net/http` module handler、router 注册和只验证该旧面的测试；暂留仍被旧 QueryGateway/Flow 测试引用的底层 registry，不在本切片越界修改 Flow。全库 test/vet/build、前端零 URL 和旧 handler 零引用为提交门禁。

- [x] **KISS-08F14 旧 Tenant discovery HTTP 删除（本提交）**：当前单域 Gin、前端和 agent 均无租户选择/切换；运行态 `/api/v1/tenants` 与 `/api/v1/me/tenants` 已是 404。删除遗留 test-router 的两条 tenant discovery 路由及 `APIV1RouterConfig.TenantDiscovery/Tenants` 注入点，以 404 反向测试冻结边界。仍被旧 Flow rollup 内部扫描引用的 `TenantRepository` 暂留，未修改 Flow 数据面、rollup 或 worker；目标包测试、全库 test/vet/build 和旧 router 注入零引用为提交门禁。

- [x] **KISS-08G1 SNMP 端口策略域抽取（`3f04ec97`）**：新增无 tenant 字段的 `internal/snmpdomain`，成为 supplier/customer/raw 端口策略、1024/1000 billing base、1m/5m step、上下修正和确定性逐点修正的唯一实现；Gin 的策略 CRUD、SNMP 查询、aggregate graph 均直接使用新域。`internal/watchdog` 只保留旧类型到新类型的显式转换，避免尚未拆除的历史测试/代码复制算法。有效 API 字段、MySQL `port_policies/traffic_policy_defaults` 字段和 ClickHouse 原始数据均未改变；响应/前端 DTO 中最后一个始终为空的 `TenantID` 兼容字段已删除，不改视觉。新域单元、兼容回归、真实 MySQL 策略/MIB/trap 生命周期及生产源码反向扫描通过。

- [x] **KISS-08G2 公共指标目录抽取（本提交）**：新增 `internal/metricdomain`，集中管理 system/container/SNMP/BGP 指标定义、scope/unit/value-mode 和自动查询步进；Gin metric catalog、用户指标授权 picker、聚合图与时序查询已直接使用新包，旧包仅保留同名兼容 alias/wrapper。指标名、返回字段、步进集合和权限判断不变，目录与 1h/24h/30d 自动步进均有独立测试。完成后生产 Gin 只剩 SNMP 设备/图表、发现/轮询、MIB、事件/trap 闭包依赖旧包；G3 再迁这些类型与引擎。

- [x] **KISS-08G3a SNMP MIB registry 抽取（本提交）**：将 28 个内置 MIB、gosmi 全局 registry、OID 正反解析、状态枚举和 built-in MIB inventory 迁入 `internal/snmpdomain`；Gin MIB CRUD/启动 seed 直接使用新包，旧 SNMP discovery 仅经薄 wrapper 共用同一 registry，不复制 parser 或内置资源。`mib_modules` 表、API 字段、built-in 保护和外部 `mib_dirs/mib_load` 语义不变；目标包、真实 MySQL MIB 生命周期、全库 test/vet/build 与 race 为门禁。G3b 继续迁设备/发现/轮询公共类型和引擎。

- [x] **KISS-08G3b SNMP 事件/图表契约抽取（本提交）**：把 ClickHouse 事件响应 DTO 与设备/端口 dashboard schema builder 迁入无租户、无存储依赖的 `internal/snmpdomain`；Gin 事件和 overview 路由不再经旧 `internal/watchdog` 的 tenant 类型。图表 builder 只接收生成 query schema 所需的最小设备/端口/传感器字段和 BGP capability，不复制凭证或完整 inventory；指标名来自 `internal/metricdomain`。既有事件 PascalCase wire 字段、dashboard snake_case 字段、signed 流量、虚拟接口排除、BGP/光模块能力面板与 URL 保持不变；目标单元、Gin 设备集成、全库 test/vet/build 与 race 为门禁。G3c 继续抽 trap dispatcher，G3d 再收 discovery/poller 闭包。

- [x] **KISS-08G3c SNMP trap 决策域抽取（本提交）**：将标准 link up/down、cold/warm start、authentication failure、BGP backward transition 与未知 trap 的解析/分派/事件决策迁入 `internal/snmpdomain`；Gin 只负责 agent/admin 认证、设备/recipe 装载以及 MySQL+ClickHouse 原子持久化，不再把 tenant/full device/credential 对象传入 dispatcher。保留 `/api/v1/snmp/traps`、lowercase 请求体、历史事件 ID 算法、Raw varbind 字段、端口/BGP 状态、立即轮询和 rediscovery 语义；新域覆盖 link/BGP/unknown/rediscovery/status normalization，真实 MySQL trap 生命周期、ClickHouse writer 契约与全库 test/vet/build/race 为门禁。完成后生产 Gin 对旧包只剩 discovery handler 和 collector runtime 两个闭包；G3d 继续收拢 discovery/poller。

- [x] **KISS-08G3d1 SNMP poll/query/CH writer 抽取（本提交）**：将生产 `SNMPCollectorRuntime` 使用的 GoSNMP v1/v2c/v3 query engine、配方调度、poller、精确 UInt64 counter 和 ClickHouse writer 迁入无 tenant 参数的 `internal/snmpdomain`。MySQL repository 接口从 `(tenantID, deviceID)` 收敛为单域 `deviceID`，运行时不再向旧 runner 传空 tenant；设备/profile override、SNMP 分块 GET、部分 chunk 容错、缺失 recipe 标记、轮询状态和 CH schema/列保持不变。旧 discovery 暂由边界显式转换接入，下一切片 G3d2 搬迁 discovery/MIB definition engine 后删除转换与最后一个生产旧包 import。独立测试冻结 64 位 counter 大于 `2^53` 时不经浮点写入、单域 runner 契约和 IPv4/IPv6 endpoint 解析；真实 SNMP MySQL→CH 集成、目标包 test/vet/race、全库门禁为提交条件。

- [x] **KISS-08G3d2a SNMP discovery 契约抽取（本提交）**：先把 discovery request/result、设备、端口、IPv4/IPv6 地址、BGP、sensor、inventory、VLAN、LAG、recipe 和 event DTO 收敛到无 tenant 字段的 `internal/snmpdomain`；Gin handler、MySQL import 和独立 collector runtime 只依赖新契约，成熟 discovery/LibreNMS definition 算法仍原样封装在唯一 `legacySNMPDiscoveryRunner` 适配器中。抽取审查补回了不得丢失的 recipe `poller_type/user_func/state_map_id/unit/last_seen_at` 非租户语义；逐字段反射测试规定旧 DTO 除 `TenantID` 外必须在新契约中存在同名等值字段，防止后续搬算法时静默降级。HTTP 路由、请求/响应、MySQL/ClickHouse 表与采集行为不变。G3d2b 只负责迁移 discovery/definition 算法并删除该适配器及生产最后一个旧包 import。

- [x] **KISS-08G3d2b1 LibreNMS definition/parser 与 OS detection 迁移（本提交）**：原样移动 definition YAML 解析、OS detection 顺序/负条件/active-probe 规则、state translation 和 trap handler 解析到 `internal/snmpdomain`；生产 discovery adapter 已直接从新域加载 definitions。旧 repository/历史测试只保留类型 alias 与薄函数 wrapper，不再拥有算法副本；原 parser 测试随算法迁入新包，继续覆盖 scalar/list、主动探测条件、坏 YAML 和 PHP trap 映射。stable ID 算法、definition 字段、文件查找顺序及 LibreNMS 首个完整规则命中语义均不变。G3d2b2 继续机械迁 discovery modules/engine，完成后删除唯一 legacy adapter。

- [x] **KISS-08G3d2b2 SNMP discovery modules/engine 抽取（本提交）**：将 ports、IPv4/IPv6 address、BGP v4/v6 provider、sensor、processor、memory、storage、ENTITY inventory、VLAN、LAG、candidate 与 definition-driven discovery 原样迁入 `internal/snmpdomain`；Gin server 直接构造新域 `DiscoveryEngine`，已删除 `legacySNMPDiscoveryRunner` 和生产源码最后一个 `internal/watchdog` import。单域稳定 ID 保留历史空 tenant seed 的分隔符，避免既有 port/recipe identity 漂移；旧包仅为尚未物理删除的历史内部调用与测试保留显式 DTO/query/module compatibility wrapper，不进入 server/collector 生产路径。IPv4/IPv6 IP-MIB、完整 Huawei VRP/Junos 版本、默认 module registry、definition parser、精确 uint64 poll 与真实 MySQL→SNMP→ClickHouse 均有门禁；全库 test/vet/build 和三包 race 通过。G3 后续只清理遗留 repository/import/runtime 闭包，不再搬动 discovery 算法。

- [x] **KISS-08G4 system agent 解耦（本提交）**：延期的 `watchdog-system-agent` 只把自身 `agent` YAML section、`WATCHDOG_AGENT_*` 覆盖、HTTP client 和 wire DTO 收口到命令包，不再为这些字段加载包含 VM/tenant/provider 的旧全平台配置，也不再 import `internal/watchdog`。原 `/api/v1/system-agents/:id/{plan,samples}` 与 `/api/v1/agents/:id/{heartbeat,status,errors}` URL、header、JSON 字段和 plan/LKG 流程保持不变；共享 YAML 的其他 section 不由该独立进程解释。配置/URL/wire 单元测试及真实四进程 register→plan→ACK→LKG→revoke 测试通过。至此全库生产源码对 `internal/watchdog` import 为零，后续可按闭包物理删除旧包而不搬运算法。

- [x] **KISS-08H 遗留 package 物理删除（本提交）**：在 A–G4 已逐域迁移/删除且全库生产 import 为零后，删除 `internal/watchdog` 剩余 366 个自循环文件（旧 PB/tenant/VM/provider/module/query gateway、安装/rollup/export worker、旧地址/SNMP/Flow 管理副本及其历史测试）。当前 Gin、`internal/address`、`internal/snmpdomain`、`internal/flow*` 和所有独立进程均未改；源码反向扫描、全库 test/vet/build 通过。旧 `deploy/migration/mysql`、`install/init.sql`、legacy YAML/依赖仍作为下一独立删除域，不混入本提交。

- [x] **KISS-08I 遗留 schema/config/dependency 删除（本提交）**：在 H 已删除最后消费者后，物理删除与现行 `deploy/schema/mysql` 冲突的旧 `deploy/migration/mysql`、`install/init.sql` 和 tenant seed `dev/seed.sql`；删除含 VM/query-gateway/tenant/provider/旧 rollup 的第二套 example/dev YAML，仅保留 `config/watchdog.yaml` 作为 server、collector、worker 与可选独立 agent 的共享配置。SNMP trap/system agent 继续只解释各自 section，secret 为空并由 enrollment/env 注入；删除零引用 `robfig/cron/v3`。现行 schema 连续性、唯一配置加载、目标 agent 测试和全库 test/vet/build 为门禁。

> **2026-09-16 门禁修复包已闭环**：Saved Filter 的持久化/API 只接受 `private/shared`，旧 scope 有独立迁移测试；`agent_process_integration_test.go` 从 KISS embedded schema 构建并启动 `system/snmp/flow_collect/flow_worker` 四个真实二进制，覆盖 register→plan→ACK→LKG→revoke；`flow_enrichment_publications_integration_test.go` 另以 production Gin、真实空 MySQL 和真实 `flowworker.RemoteVersionSync` 覆盖 WADS/classification 下载、校验、ACK、坏版本拒绝、LKG 冷恢复与 server restart，并反向断言 clean schema 不含 `tenants/collector_agents`。原 `cmd/watchdog-flow-worker/remote_integration_test.go` 只启动旧 `internal/watchdog` test Router，现已删除，不能再冒充生产进程证据；server 集成测试中的 SNMP 指标常量也只依赖 `metricdomain`。SNMP A2 替身按 scoped/aggregate/billing 三种真实列契约执行；`frontend_route_contract_test.go` 从 `main.tsx` 递归扫描所有可达前端 `/api/v1` 调用并与 Gin production route table 对照，禁止再次产生“页面已迁、后端 404”。四项均已在真实隔离 MySQL 上通过。
>
> **2026-09-16 运行闭环包已闭环（产品五进程）**：`make build-runtime` 是 frontend、server、SNMP collector、Flow collector、Flow worker 的唯一批量构建入口，产物固定在 `frontend/dist` 与 `build/watchdog-*`；开发启动固定为 `make dev-frontend`、`make dev-server`、`make dev-snmp-collector`，Flow 按签名计划启动固定 build 产物，不再使用 task-specific `/private/tmp/watchdog-server-*` 或 `go run`。本地 CH secret 固定为忽略提交且 mode 0600 的 `data/secrets/clickhouse-password`，`make flow-dev-up` 与全部进程读取同一值。真实空 MySQL opt-in 测试已完成 install→login→agent register/heartbeat→device create→SNMP CH write/query→Flow CH write/closed rollup→Gin Flow query；同一 harness 后续补入六类非空 Flow 事实，验证总览/多维/源/目的/境外/VPN 六报表、源/目的服务端分页和异步 CSV 导出，并按唯一 device/source/database 清理验收数据。该证据关闭单节点运行与 Flow 产品数据回归，不倒签地址 publication、账单/告警、浏览器或集群/性能全量空库验收。

> **2026-09-16 生命周期包 L1–L5 已闭环**：L1 新增单域 `0033_flow_storage_lifecycle.sql` 与 fail-closed 守卫并删除未生效的静态天数字段；L2 完成全局 policy/API/repository；L3 接通真实 Kafka stable committed-next-offset、ClickHouse 连续 count/counter scanner、`operation_jobs` checkpoint/retry/cancel/lease 和 MySQL 单调水位；L4 复用现有 CH rebuild primitive，按 UTC 日异步生成 24 个 1h bucket、核对 count/counter、轮转迟到复核并按 repair generation 重建，同时把标准单维、方向、境外、固定报表和导出接到连续 archive boundary。L5A–L5B3 完成 raw 日 Kafka 坐标/计数器证据、backup evidence、批准、worker tombstone ACK、极晚 datagram 隔离、稳定 QueryID 删除及真实外部恢复；L5B4 以 `0039` 唯一活动批准约束、完整 UTC 月日状态/计数器/物理行/恢复证据、人工批准和 canonical operation job 完成 archive 月删除。raw/archive 开关均可显式发布，但每个分区仍逐项 fail closed。

生命周期包后续按纵向切片推进：

- [x] **L1 契约/Schema/静态假象删除**：单域表、纯函数守卫、clean install、旧配置拒绝。
- [x] **L2 管理面**：全局 policy draft/publish/retire、状态/水位/回执查询 API 与现有 Retention 页面 Flow 区域；`job.view/job.manage` 权限、强制 `If-Match` CAS、审计和服务端分页/search/sort/status-filter VTable。真实空 MySQL 已验证草稿更新、过期版本拒绝、单 published revision、上一版本自动 retired、删除开关 fail-closed 和审计条数；前端路由扫描、TypeScript 与 production build 通过。物理删除动作未暴露。
- [x] **L3 Kafka 对账**：稳定 committed-next-offset snapshot、显式 bootstrap、连续 receipt/fact count+counter scanner、checkpoint/heartbeat/cancel/retry 和持久水位。默认关闭且不猜 offset 0；启用配置按 interval bucket 幂等入队。真实 Kafka/CH 验证 clean、count mismatch、missing receipt/records 与预算切分；真实 MySQL operation job 首次 CH 暂时错误后 attempt 2 只读取一次 Kafka 快照并从 frozen checkpoint 收敛，水位推进到 103。
- [x] **L4 归档**：单域 `flow_storage_downsample` job 使用 `(UTC day, policy_version, repair_attempt)` 幂等身份；24 个 1h bucket 每小时 checkpoint，暂时错误可从 `next_hour` 续跑，永久错误/守恒差异终止并进入 repair；每 6h 有界轮转复核仍保留 raw 的 reconciled 日，迟到差异以新 generation 重建。真实 MySQL 证明第 4 个 bucket 暂时失败后 attempt 2 只补余下 21 个 bucket，真实 CH 证明全天 source/archive 六项计数守恒以及 1h hybrid、1m raw-only、境外查询一致；查询只采用无缺口的连续归档前缀。
- [x] **L5 删除保护**：raw 按日、archive 按月；每次删除引用对应 Kafka/计数器证据、有效备份与 restore-tested evidence，并写不可变回执。真实 MySQL/ClickHouse、模糊 ACK 接管和外部恢复演练均通过。
  - [x] **L5A 只读证据链**：从 `flow_ingest_receipts FINAL` 按 UTC 日事件时间交集聚合 `(source_stream_id,topic,partition,min offset,max offset+1)`；逐项匹配 MySQL watermark 的显式 bootstrap、consumer group、健康状态、零 mismatch、committed snapshot/verified 时间和 next-offset，再核对实时 raw/archive 六项 counters 与 restore-tested backup。API 只解释 blockers，不推进 `delete_eligible`、不写 deletion receipt、不执行 CH DDL。真实 CH 跨午夜 message 与真实 MySQL 水位落后/不变性测试通过。
  - [x] **L5B1 备份恢复证据管理**：`GET/POST /api/v1/flow/storage/backup-evidence` 与详情/撤销 action 使用 `job.view/job.manage`；记录创建后不可修改、不可删除，撤销强制 ETag CAS，所有变更审计。覆盖范围、对象引用、SHA-256、恢复演练时间/引用缺一即拒绝；迁移前无 `restore_test_ref` 的旧行 fail closed。真实空 MySQL 已覆盖创建、分页搜索、详情、stale revoke、成功撤销及 readiness；这只关闭批准前置证据管理，不代表完成恢复演练或允许物理删除。
  - [x] **L5B2 raw 日删除批准**：readiness 返回分区 ETag；`POST /partitions/:date/actions/approve-delete` 在同一事务锁定分区/策略/备份证据，冻结 policy/generation、Kafka offset coverage、raw/archive 六项 counters 与 backup evidence，随后 CAS 推进 `reconciled -> delete_eligible`。批准记录可分页/查询但不可编辑/删除；带批准 ETag 的 revoke 在尚无 delete job 时原子回退 `reconciled`。真实空 MySQL 覆盖完整快照、路由、stale CAS、撤销和审计；策略开关仍关闭，L5B3a DDL handler 即使已接线也不可由正常管理面启用。
  - [x] **L5B3 raw 日删除执行/恢复**：
    - [x] **L5B3a executor/receipt（本提交）**：新增 `0037`，冻结所有逻辑物理行数；批准、operation job、requested receipt 与 partition binding 单事务提交。worker 每次执行重新校验 policy/approval/backup/Kafka coverage/raw+archive 六项 counters/物理行数，稳定 QueryID `DROP PARTITION` 后要求 raw=0、物理行=0、archive 不变才完成；永久 CH 错误终止，暂时错误重试，ACK 丢失由接管 attempt 的 post-state 收敛。调度后 cancel 返回 409，调度前只能 revoke approval。真实 MySQL 和 ClickHouse 26.3 通过。
    - [x] **L5B3b ingest tombstone publication/ACK**：复用现有 flow worker HTTP publication/LKG、`operation_jobs`、Kafka 坐标和 receipt；全局单调 barrier 与逐 worker ACK 存 MySQL，worker 原子安装磁盘 LKG 后 ACK，数据面只读原子 Guard。越过 tombstone 的极晚完整 Kafka datagram 先写 CH quarantine 再写 `late_quarantined` durable receipt，重放依靠天然 Kafka 坐标与稳定 insert token 幂等；receipt generation 替换旧 generation，scanner 明确把隔离回执视为零事实且不误报。真实 MySQL/ClickHouse、全库 test/vet/build 与相关 race 通过。
    - [x] **L5B3c 外部 backup/restore drill**：新增一次性 `watchdog-flow-restore-drill`，只接受结构化 named disk/name 和 `watchdog_restore_*` 新库，不接受原始 SQL；读取并哈希真实 `.backup` manifest，执行原生 RESTORE，核对源库恢复前/后与隔离库的 Kafka 坐标、物理行数、raw/archive 六项 counters，默认清理隔离库。独立外置 volume 上的非空 Flow 真实 BACKUP→RESTORE 已通过；管理 evidence 继续只保存结果引用，不冒充演练。
  - [x] **L5B4 archive 月批准/删除/恢复**：新增 `0039`，同一 storage partition 只允许一个活动批准；readiness 必须证明完整 UTC 月每天均为 `raw_deleted`、MySQL 日 archive 总和等于 CH 最新完整 generation、物理行非零、保留期已过且 restore-tested `archive/all` backup 覆盖整月。独立 API 冻结月证据，canonical `flow_storage_archive_delete` job 用稳定 QueryID 执行 `DROP PARTITION YYYYMM`，执行前后复核并禁止取消；模糊 ACK 只在 takeover attempt 且物理分区已空时收敛。真实 MySQL 审批/job/receipt 与真实 ClickHouse 非空月分区删除、整月 BACKUP→RESTORE counters/物理行复算均通过。

- [x] **编码**：KISS-08A–I 已迁出仍使用能力并物理删除 `internal/watchdog`、旧 Hub runtime、VM/VLogs provider、tenant/module/resource/dataset registries、旧 targets、兼容 adapter、旧 schema/config 和零引用依赖；当前生产入口只保留 Gin、MySQL、ClickHouse、Kafka 与独立采集/worker 进程。
- [ ] **静态门禁（接近完成）**：生产依赖和可执行实现已无 PocketBase、`internal/watchdog`、`NewBackendRuntime`、DatasetProvider、VictoriaMetrics/VictoriaLogs client 或 target/device 双写；仍需清理少量历史兼容文字/注释以及 `snmp_vmquery` 对旧 `tenant_id` 标签的显式忽略，完成后再勾选字面零命中门禁。
- [ ] **空库验收**：仅 MySQL + ClickHouse + Kafka，从零安装管理员、设备、agent、地址 publication、Flow、SNMP、六报表、账单、导出、告警。
- [ ] **故障验收**：MySQL/CH/Kafka 短断、agent 离线、worker crash/rebalance、坏 publication、SNMP reset、任务 takeover/cancel/retry。
- [ ] **性能验收**：Flow 吞吐/延迟/内存不低于切换前基线；SNMP 写入和查询满足目标；账单计算有界。
- [ ] **回归测试**：Go/前端/真实依赖/浏览器/安装脚本/备份恢复全矩阵。
- [ ] **文档**：只保留一套当前架构、schema、配置、运维和故障手册；旧文档标历史，不再作为实施入口。
- [ ] **已提交门禁**：最终删除提交后 clean checkout 可完整部署；所有遗留数据库/volume 已按各工作包的精确白名单处置，不把物理清理拖到项目末尾。

> **2026-09-16 精确清理台账见 [watchdog-kiss-cleanup-ledger.md](watchdog-kiss-cleanup-ledger.md)**（代码/表/字段/接口/配置逐项 + 依赖 + 删除影响 + 严格删除顺序 + 执行状态；L1-L19 代码、S1-S12 schema、I1-I5 接口）。KISS-08A–G4 已逐域迁出当前能力，H 物理删除 `internal/watchdog`，I 删除冲突 schema 树、tenant seed、legacy YAML 和零引用 cron 依赖。运行代码、schema 与配置均已收敛；剩余是历史文档标记/删减以及完整空库、故障和性能发布门禁。
>
> **2026-09-15 审计是历史快照，不再作为当前实施入口**：其中记录的 `internal/watchdog` import、双安装系统、遗留 worker 和 VM 必填配置已分别在 KISS-08B–I 删除。当前唯一新装路径为 Gin `/install` + embedded `deploy/schema/mysql`，唯一共享配置为 `config/watchdog.yaml`。
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

# Watchdog 平台架构重构 Tasklist

> 平台通用工作与 Flow 分离。Flow 只依赖这里已经落地的模块、身份、collector、target、query、export 与 correction 契约；非阻断平台缺陷不得混入 Flow 数据面 diff。

## 0. 执行规则

每个切片执行：设计 → 编码 → 单元测试 → 集成测试 → 变更设计 → 变更测试 → 回归测试。完成后自动进入下一个；发现 Flow 专属问题则登记回 [flow-module-tasklist.md](flow-module-tasklist.md)。

## P0 生产入口、身份与存储收敛

- [x] Hub 挂载 `/api/v1`，前端同源访问；登录只在主动登录或访问受控数据时被动触发。
- [x] Beszel/`BESZEL_*` 对外命名收敛到 Watchdog/`WATCHDOG_*`。
- [x] VictoriaLogs Flow 原型和浏览器直连删除；MySQL 为唯一管理库，VM/CH 各自保留指标/Flow 事实角色。
- [ ] PB 收缩为认证内核：迁 user settings、quiet hours、alerts、smart devices；僵尸 system collections 停写并归档。
  - 细化（版图与顺序）：前端直连 PB collection 计 6 处——`users`(认证保留)、`user_settings`(4)、`quiet_hours`(4)、`smart_devices`(2)、`alerts_history`(2)、`fingerprints`(agent 认证保留)。后端 agent-connect 依赖 `systems`/`fingerprints`（load-bearing，不可简单停写）。逐域按 MySQL 表 → repo/API → 前端切换 → PB collection 只读归档 → 生产 backfill 推进。
  - [x] **user_settings（UI 偏好）→ MySQL user_preferences**：migration 030（每用户一行、row_version 乐观锁），repo（读无行返默认不写、写 create/If-Match）、GET/PUT `/api/v1/me/preferences`（仅本人、quoted ETag）。gated MySQL + API 测试（commit a6b5ca65）。**修正**（commit ae937705）：初版误把 emails/webhooks 也迁 MySQL，导致告警投递（读 PB user_settings，且缺行即报错）split-brain——已改为正确拆分：MySQL 只存 UI 偏好（chartTime/units/colors/hourFormat/layoutWidth，前端 PUT 前 strip 通知字段），emails/webhooks 留 PB（notifications 页专用 saveNotificationSettings），alert 投递容忍无行（视为无渠道跳过而非报错）。
  - [x] **notification_channels → MySQL + 告警投递重定向**（A）：migration 031 `notification_channels`（每渠道一行、tenant+user 域），repo（整体替换 + `NotificationChannelsForExternalSubject` 按 PB id=external_subject 解析）、GET/PUT `/api/v1/me/notification-channels`（仅本人 + email/webhook 校验轻 SSRF）；AlertManager 注入可选 reader（backend 起后 wire，读 MySQL，未 wire 回退 PB），缺渠道=no-op；前端 notifications 页与设置加载切 MySQL。id 空间桥接：告警 data.UserID=PB id=MySQL external_subject_id。webhook URL 仍明文（secret store 后续）。gated MySQL+API+校验测试，alerts/hub 套件通过（commit f5181db6）。
  - 余项：② quiet_hours / alerts_history / smart_devices 逐域迁移（同 A 模式：MySQL 表 + reader 注入 + 前端切换）；③ 生产 PB→MySQL 一次性 backfill（dev 无需）；④ PB user_settings collection 停写+归档；⑤ webhook secret store；⑥ 僵尸 telemetry collection 待 agent-connect 解耦。
- [x] 消除 PB/MySQL 双身份权威，验证登录、OTP、找回、session、禁用和 tenant projection。（认证=PB 唯一权威、授权=MySQL 投影、password_hash 列已删，见下）
  - 现状核查：认证=PB 唯一权威（前端 `pb.authStore`/`requestPasswordReset`/OTP/session；服务端 `FindAuthRecordByToken` 验 token，非 `users` collection 拒绝 → PB superuser 被挡在平台 API 外，`platform_backend_test.go` 已测），授权=MySQL 投影（按 `external_subject_id`=PB record id，`identity_adapter.go` 多租户 discovery 分离、disabled fail-closed，`identity_adapter_test.go` 已测）。禁用为设计内单边：MySQL `status=disabled` 即时断业务 API，PB session 清理为独立动作（不影响 fail-closed）。
  - [x] 消除代码层凭据双写：CreateUser 移除最后一处 `password_hash`（写 NULL）引用，identity 代码 100% 无 password；gated MySQL 锁死"create/update 永不写凭据材料"+"disabled 投影 fail-closed，PB 链接不动"（commit e599ab81）。
  - [x] `password_hash` 空壳列 drop：并行会话 027/028 落库后，migration 029 幂等删列（conditional-DDL），init.sql 再生成、迁移测试断言列已不存在、identity 测试改断言列不存在、dev seed 去 password_hash。MySQL 现在**完全无凭据材料**，PB 是唯一认证权威，MySQL 身份纯授权投影（commit fd8889f9）。**双身份权威已彻底消除。**
- [ ] 完成空库安装、存量迁移、checksum/quarantine、shadow read、切换、回退和生产零调用观察。

## P1 模块、Collector/Agent 与 Target

- [x] Module/Resource/Dataset registry 基座与 tenant module 开关。
- [x] collector agent registry、plan revision/signature/ack、credential/enrollment 基础。
- [ ] collector enrollment 一次性 secret、rotation/revoke、capability negotiation、fleet rollout/canary 完整闭环。
  - [x] 一次性 enrollment secret：migration 026 + 无认证兑换端点（行锁单次焚毁、错值不烧、同形拒答），初始 token 一次性签发即可机器认证；gated MySQL 实跑（commit cb226ccf）。
  - [x] token/mTLS 双窗口 rotation/commit/abort/revoke：migration 025 staged 凭据 + If-Match row_version 守卫 + 审计，认证器双窗口，UTC 过期比较；gated MySQL 实跑（commit 89b7a062）。凭据唯一写权先行切到 registry，legacy 投影只同步非凭据字段（commit c981a8e5）。
  - [x] capability negotiation：enroll 可携带声明（software/agent_api/plan_schema 区间/规范化能力集，与心跳同一校验），入册即落列，首个 plan 直接按真实 schema 区间校验；声明先于 secret 校验（400 不烧不探）。gated MySQL + API 测试（commit b2d48eed）。plan 创建端 FOR UPDATE 校验 schema 区间与心跳能力/健康联动此前已在（C1/021）。
  - 余项：fleet rollout/canary（preview/canary/rollback 复制旧 spec 生成更高 version、expiry/kill switch）。
- [x] target 名称与 host 身份分离；网络 target 以 `(tenant, kind, host)` 唯一，display name 可选。
- [x] SNMP profile/community 在新建与编辑可配置，sysName/sysDescr 为采集结果而非输入必填。
- [ ] Target/Network Device/Port/BGP/Inventory/Event/Alert 全 CRUD、分页、搜索、VTable filter 和稳定 ETag/If-Match。
  - [x] 平台 ETag/If-Match 契约（PLAT-00A）：弱 ETag + 412 version_conflict + Idempotency-Key 重放（migration 024，commit a8f43017），已挂 users/roles（a8f43017）、targets（bb304d26）、network device/port GET/PATCH/DELETE（00a8d4f5，struct 补 UpdatedAt+scan updated_at）；error envelope 补 retryable。
  - [x] device events keyset 分页 + severity/event_type 筛选（B）：`ListSNMPEventsPaged`（occurred_at,id 游标复用 audit helper），`/network/devices/{id}/events` 加 cursor/severity/event_type 返 next_cursor（向后兼容），前端事件表 load-more。选 events 而非 target/device 列表：后者被多处当全量下拉源用,分页会破坏消费者；events 是真表格且无界增长、权限单次检查无逐行问题。gated MySQL 分页+双筛选（commit d7b2e2f5）。
  - 余项：BGP/inventory ETag、target/device 列表分页需权限下推（消费者多，需先改为可选分页）。
- [ ] 所有删除实现 preview→异步 job→审计→可验证销毁；禁止 handler 内同步级联大删除。
  - [x] target delete-preview：GET `/targets/{id}/delete-preview` 按随删/脱钩分列依赖（device/ports/agents/投影/历史/留存/VM series vs 聚合图/export），DeleteTarget 同事务清理投影孤儿且不触碰 registry 行；前端删除确认对话框展示影响清单；gated MySQL 全扇出实跑（commits 3fcaece8、753a9fc6）。
  - [x] 异步 operation job 运行时（017 表首次落地执行面）：幂等入队（tenant/type/key + request hash 冲突检测）、SKIP LOCKED 租约认领、过期接管（死 owner token 失效）、心跳续租+取消传播、退避重试/终态四路完成，全程 token 守卫；GET/cancel API；target DELETE 配置 job repo 时改为 202+job（重复 DELETE 复用同 job），worker 在 StartBackground 启动执行级联+VM 清理，前端轮询 job 至终态。gated MySQL：生命周期/竞争/接管/取消/worker 端到端（commit 2446a37b）。
  - [x] job 管理面：`GET /api/v1/operation-jobs` keyset 游标分页 + job_type/status 筛选；/jobs 管理页（类型/状态/尝试次数/错误详情、取消排队或运行中的 job、加载更多）。gated MySQL 分页+双筛选、API 参数映射测试（commit 250bd2ff）。
  - [x] 可验证销毁（destruction receipt）：target_delete payload 升 v2 携带 preview 影响摘要（入队捕获，不进幂等 hash），handler 成功后写 `target.destroyed` 审计回执（target id/actor=CreatedBy/影响计数/VM series matcher，ID 由 jobID 派生 + ON DUPLICATE 幂等，非 users actor 保留于 detail.actor），经 `/api/v1/audit-logs` 可查。gated MySQL 证明重试幂等/可查/内容/系统 actor 保留（commit 3b179535）。
  - [x] device 删除推广：GET `/network/devices/{id}/delete-preview`（级联 11 表：ports/sensors/bgp/vlan/lag/物理实体/接口地址/snmp recipe·module/discovery job，脱钩聚合图），DELETE 配 job repo 时改 202+device_delete job（payload v1 携带影响），worker 跑级联+清 `{device_id=...}` series+写 `network_device.destroyed` 回执（jobID 派生幂等），前端影响对话框+轮询;复用现有 registry/信封/回执,零迁移。gated MySQL 预览计数+异步级联+series 清理+回执（commit ad2e89b0）。
  - [x] port 删除推广：GET `/network/ports/{id}/delete-preview`（随删 interface_address/transceiver/policy，脱钩聚合图/账单/export_task），DELETE 配 job repo 时 202+port_delete job，worker 跑级联+清 `{port_id=...}` series+写 `network_port.destroyed` 回执,前端影响对话框+轮询;零迁移。gated MySQL 全程实跑（commit 60a52ac7）。
  - [x] collector 删除推广：GET `/collectors/{id}/delete-preview`（随删 bindings/plan_revisions，**阻塞** service_principals/ownership_transfers=RESTRICT 证据），DELETE 阻塞返 409 否则 202+collector_delete job（DeleteCollector 事务内复检阻塞返 ErrCollectorDeleteBlocked→terminal 不重试），worker 级联+写 `collector.destroyed` 回执;零迁移。gated MySQL 证明有 principal 不可删（预览 flag/409/terminal）、干净 collector 异步删+回执（commit 5e5df5eb）。**删除生命周期 4 类资源（target/device/port/collector）全覆盖。**

## P2 Query、图表、统计与导出

- [ ] Provider-neutral QueryRequest/QueryResult，统一 VM/CH 的 tenant scope、时间、bucket、limit、cancel 和 completeness。
- [ ] Visualization CRUD、版本/owner、series、布局、预览和 dashboard 引用。
- [ ] 所有 VTable 统一 server pagination/search/sort/filter；popover portal + collision handling。
- [ ] Export job 统一 CSV/Parquet、快照、权限复核、checksum、TTL、下载审计和失败重试。

## P3 raw/supplier/customer 修正

- [ ] Adjustment policy/version/rule/approval/reconciliation 数据契约。
- [ ] 查询时应用与批量物化边界、冲突优先级、effective time、血缘和回滚。
- [ ] 修正前后对账、审计、导出视图和租户级权限。

- [x] 审计读取面：`GET /api/v1/audit-logs`（tenant admin）keyset 游标分页 + resource/actor/action 前缀筛选；/audit-logs 管理页（筛选+加载更多）。同时修复两类静默丢失的审计写入：无 ID 记录被拒（api 层全部 recordAudit）、非 users 表 actor 触发 FK 丢弃（system:enrollment 等，现保留于 detail.actor）。gated MySQL + API 测试（commit 578a4061）。

## 平台缺陷登记

- [ ] **PLAT-04F global/system-scope operation job**：现有 `operation_jobs`/`operation_job_watermarks` 强制真实 `tenant_id` 外键，无法承载 Kafka partition、全局保留、跨 tenant receipt 等平台级作业。需冻结显式 `scope_type=tenant|system`、system actor/audit、幂等键命名域、查询权限、并发预算、水位和销毁语义；禁止伪造“系统 tenant”、去掉外键或让 Flow 复制 lease/retry 状态机。FLOW-04C3 ingest reconciliation 在该原语落地前只实现可独立验证的 receipt 契约。
- [ ] **PLAT-04E hub metrics scrape 契约**：Flow rollup 运行在 hub，现有 collector/worker `/metrics` 无法承载；hub 的 Prometheus 文本只挂在需交互登录 session 的 `/api/v1/health/runtime/metrics`，不适合作为稳定 VM scrape target。平台需统一决定独立内网 `/metrics` 或 machine-auth scrape、TLS/ACL、provider registry、启动失败语义和部署发现；Flow 只提供无 tenant/bucket/job label 的可组合 provider，不另起第三个 HTTP server。
- [ ] **PLAT-04A immutable dimension publication**：`dimension_snapshots` 目前只存在于平台设计文字，MySQL migration、repository、publish/rollback API 和引用保留均未实现；Flow worker 只能使用静态验签 bootstrap。完成前不得让 Flow 热路径回退查询 `address_prefixes/address_sets`，也不得把设计中的外键当成已存在 schema。
- [ ] **PLAT-04B operation job registry/scheduler**：受控 handler registry（重复/空 handler 启动即拒）、每类并发预算调度器（每 worker 独立 lease owner `base/type/index`）、typed payload（各 handler 自行 JSON 反序列化）已实现，hub 通过 registry 注册 `target_delete`（并发 2）。gated MySQL 多类型排水 + 单测（commit 18fde3a3）。周期维护 reaper（idempotency_records/未用过期 enrollment secret/终态 operation_jobs 14 天保留，批量 drain 每批 1000，boot 后 1 分钟首跑再按 1h/1h/6h）已落地——全局按过期删除不走租约机械，gated MySQL 证明只删过期行（已用 secret/近期与运行中 job 保留）+ 单测（commit 75a258a6）。payload schema version 显式化已落地：`{schema_version,payload}` 信封，未知版本/畸形/缺字段为 TerminalJobError，worker 立即失败（code TERMINAL，attempt 1）不耗重试预算，target_delete 两端走 v1 信封；gated MySQL + 单测（commit 43b6675a）。migration 028 新增通用 `(tenant,job_type,partition_key)` 单调 watermark，解决终态 job 清理后周期任务丢水位的问题，Flow 只消费该通用原语。**平台余项**：可复用的 per-tenant cron trigger、公平分页游标和跨 job type 扫描背压；FLOW-04B 的域内 tenant/bucket adapter 不冒充通用 cron，也未进入 `flow-worker`。
- [ ] **PLAT-04C address-prefix/set 管理闭环**：现有 API 只存任意 `selector JSON`，没有 canonical CIDR/IPv6 校验、members/exclude/include DAG、集合并交差/有限补集预览、冲突/展开量检查、publication 引用、分页/filter、ETag/审计；POST/PATCH 还会无条件把 `enabled=true`。平台侧需补 typed schema、validate/preview/publish 生命周期和 VTable 管理面；Flow 侧只实现 immutable 编译与事实 membership，不在数据面复制 CRUD。
- [x] **PLAT-04D Geo lookup 收敛**：hub 的 434 行重复 flow-geo-v1 loader（FlowGeoService/FlowGeoIndex/LoadFlowGeoBundle/二分区间）已删，FlowGeoService 收敛为 ~80 行薄适配器委托 `flowdimension.GeoCatalog`（Reload 委托并保留失败前索引、Lookup 查 active、Status 取 metadata）。`/api/v1/flow/geo/*` 形状不变（前端无消费者），loader 校验现只在 flowdimension 测一次。确认无其他 hub 代码依赖被删类型（sflow prefix matcher 用 bart 树非 geo）。适配器测试用 flowdimension 导出格式建 bundle 验 reload/lookup/status + 失败保留（commit c7681f6d）。
- [ ] 删除历史 migration 不能改 checksum；废弃对象必须用后续 migration 删除并同步 fresh-install schema。本轮 Flow cleanup 已由 migration 027 示范。
- [x] `watchdog-platform-module-architecture.md` 的旧 Flow WAL/normalized/restore 章节已收敛为平台边界并链接 Flow ADR，不再复制数据面设计。
- [ ] 旧 `sflow_collector` VM 聚合原型仍在平台配置/命令中；待新 Flow P1 验收后独立迁移/下线，不与 RawFlow collector 共端口。
- [x] 默认 `go vet ./...` 会编译 `internal/hub_test`，但 `GetHubWithUser` 只在 `testing` tag 可见；已按既有约定给 `api_test.go`/`platform_backend_test.go` 补 `//go:build testing`，默认与 `-tags=testing` 两种 vet 均通过，tagged hub 套件通过（commit a549a602）。

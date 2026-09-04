# Watchdog 平台化与 Flow 模块实施 Tasklist

本清单把 [平台化与可插拔模块架构](watchdog-platform-module-architecture.md)、[存储收敛详细设计](storage-consolidation.md)、[流向需求与选型](flow-direction-requirements.md) 和 [Flow 详细设计](flow-module-design.md) 拆成可勾选工作包。除明确标注的现有组件前置修复外，任务初始为未完成；勾选时应在同一行或关联 issue/PR 中补充负责人和证据链接。

## 1. 使用规则

- [ ] 已为本轮迭代填写负责人、目标版本、开始/完成日期和关联 issue/PR。
- [ ] 每个工作包的“设计”通过评审后才开始编码。
- [ ] “编码完成”不代表工作包完成；单元、集成、变更和回归测试必须分别留证据。
- [ ] “变更设计”必须记录实际实现与已批准设计的差异；没有差异时也要记录“无设计偏差”。
- [ ] “变更测试”专门验证 schema/API/config/升级/回滚/兼容变化，不得用普通单测代替。
- [ ] 工作包所有子项完成、验收证据归档后，才能勾选阶段出口。
- [ ] 未解决的阻断项已登记负责人、影响范围和解除条件，不能以“后续处理”直接关闭。
- [ ] 每个工作包同时交付代码、migration/schema、配置、指标/告警、runbook 和测试；不允许先上线裸功能再把性能、HA、安全或可运维性推到后续阶段。
- [ ] 所有性能任务先提交容量输入表和计算结果，再提交压测；所有数据任务同时提交不变量、幂等键、版本、TTL、备份和重建方法。

建议证据格式：`负责人：___；PR/commit：___；测试报告：___；设计评审：___；完成日期：___`。

### 1.1 已完成的现有组件配置前置修复

以下项目是进入 PLAT-03 前的基线修复，不表示 collector plan、LKG、灰度或 Platform P0/P1 已完成：

- [x] **设计/编码**：现有进程统一 `CLI > env > YAML > default`；严格 YAML、环境变量 fail-fast、URL/ID/listen/path/MIB 规范化、组件级身份校验和完整示例配置已落地。证据：[`config.go`](../internal/watchdog/config.go)、[`watchdog.example.yaml`](../config/watchdog.example.yaml)、[`watchdog-install.md`](watchdog-install.md)。
- [x] **准确性修复**：补齐 sFlow agg/prefix sync、SNMP poll/discovery、aggregate rollup、trap/system agent 覆盖项；MySQL idle=0 不再被默认值吞掉；SNMP polling-only 不再错误要求 device。
- [x] **Agent 一致性修复**：registry 的 type/mode/status/endpoint 在 API 与 repository 共用规范化/校验；PATCH 保留省略字段；未知 JSON、system+pull、非法 run status 和 disabled agent 运行被拒绝；成功上报不再因缺省 status 被记成 failure；旧 agent 的显式 URL/token CLI 真正高于 prefixed env。
- [x] **旧 hub 配置修复**：`config.yml` 严格解码并在写入前完成 name/host/user/port 规范化、重复检查和用户引用校验；现有 system 改用 `(name,host,port)` 元组匹配，消除字符串拼接碰撞。
- [x] **单元/变更测试**：覆盖未知/多文档 YAML、显式零值、非法 env、优先级覆盖、路径/URL 规范化、示例配置 schema、Agent 规范化/部分 PATCH/禁用/状态语义、旧 hub system 配置和旧 agent CLI 优先级。证据：[`config_test.go`](../internal/watchdog/config_test.go)、[`api_agents_test.go`](../internal/watchdog/api_agents_test.go)、[`system_local_collector_test.go`](../internal/watchdog/system_local_collector_test.go)、[`config_parse_test.go`](../internal/hub/config/config_parse_test.go)。
- [ ] **集成/回归测试**：在真实 MySQL、VM 和 UDP listener 环境完成各二进制启动、env/YAML/CLI 组合、trap 转发、SNMP poll/discovery、sFlow ingest、export/rollup 冒烟；VictoriaLogs 不再是目标环境，旧 VLogs 路径只验证“零请求/已禁用”；归档日志和配置脱敏报告。

### 1.2 产品命名与导航收敛

- [x] **设计/变更设计**：产品代码命名空间、环境变量、API、协议头、制品、服务、数据目录和发布仓库统一为 `watchdog` / `WATCHDOG_*`；不保留改名前的产品别名；第三方包名和许可证作者信息保持原始来源。
- [x] **编码**：Go module/package/import、Agent/Hub 配置、前后端接口、脚本、容器、Helm、发布元数据、注释、测试及文档完成统一；在线升级默认直连 `cloudcache/watchdog`，镜像必须通过 `--github-mirror` 显式配置；安装命令改为仓库内脚本的 raw URL。
- [x] **导航编码**：桌面端和移动端统一为“概览 / 资源 / 分析 / 数据 / 设置与管理”；Targets、Network Targets、All Containers、Disk Health 归入资源，Aggregate Charts、Saved Graphs 归入分析，主题和语言归入常规设置。
- [x] **单元/变更测试**：升级 URL、Agent 配置优先级、API/Agent 协议、Hub 配置和领域层定向测试通过；Shell 安装脚本通过语法检查，Windows Agent 完成交叉编译。
- [x] **集成测试**：前端 production build 通过；桌面端与 390px 移动端完成实际浏览器菜单、路由和设置页视觉回归。
- [ ] **全量回归测试**：`go test -count=1 -tags=testing ./...`、前端 production build 与本次改动文件的 Biome check 已通过；GPU helper、Hub system worker 生命周期和 WebSocket 状态收敛问题已修复。全库 Biome 仍有既存格式、`any` 类型和无效 suppression 债务，真实登录浏览器 E2E、race、升级/回滚与 soak 尚无发布证据，因此父项保持未完成。

## 2. 阶段依赖与总览

- [ ] `Platform P0`：生产身份/API 与 migration 基线完成。
- [ ] `Platform P1`：module/resource/dataset 与 collector/target 基座完成。
- [ ] `Platform P2`：统一查询、图表和导出完成。
- [ ] `Platform P3`：raw/supplier/customer 三层修正完成。
- [ ] `Storage consolidation`：生产路由/身份闭环、PB 业务域迁移、legacy Agent 适配和 VLogs 裁撤完成；PocketBase 只剩 auth 应用集合。
- [ ] `Flow P1`：flow-collect（raw WAL + GoFlow2）→normalized→dimension worker、enriched base/派生表、地址段/六维和总览完成；依赖 Platform P0–P2。
- [ ] `Flow P2`：多维明细、IP、归属修正和对账完成；三层导出依赖 Platform P3。
- [ ] `Flow P3`：境外与 VPN 风险完成。
- [ ] `Flow P4`：在 P1 已具备容量/HA/备份基线之上完成跨机房、历史回算、长期 rollup 和可选 nDPI；不得补做 P1 遗漏的基础可靠性。
- [ ] `Release`：全量回归、升级/回滚演练、安全与发布验收完成。

### 2.1 主工作分解与依赖

| 工作包 | 前置 | 必交付物 | 阶段出口证据 |
|---|---|---|---|
| PLAT-00 | 无 | 现状基线、migration runner、init parity | 空库/升级 schema 报告 |
| PLAT-00A | PLAT-00 | 公共生命周期、CRUD/action、job、query envelope 契约 | 幂等/并发/删除/恢复/漂移闭环报告 |
| PLAT-01 | PLAT-00/00A | IdentityAdapter、用户/角色/tenant/RBAC | 生产登录与越权报告 |
| PLAT-02 | PLAT-00A/01 | module/resource/target-kind/dataset registry | SNMP/system 先行注册验证 |
| PLAT-03 | PLAT-01/02 | collector identity、M:N binding、enrollment、plan revision/rollout | 注册/轮换/撤销/兼容/LKG/N+1 测试 |
| PLAT-04 | PLAT-02 | target/device/port 资源树与 CRUD | 删除影响和 tenant 隔离 |
| PLAT-04A | PLAT-02 | dimension snapshot/publish/bundle/ack | event-time 选版与回放 |
| STORE-00 | PLAT-00 | 存储权威基线、VLogs 止血、迁移 manifest 契约 | 读写矩阵与零直连报告 |
| STORE-01 | PLAT-01 | 生产 Router、PB IdentityAdapter、typed API client | 真实登录、JSON API、故障语义报告 |
| STORE-02 | PLAT-00A/01 | user 投影、偏好/通知/静默 MySQL schema 与迁移 | count/checksum/ETag/secret 报告 |
| STORE-03 | PLAT-02/03/04 | legacy Agent、system inventory 与 VM 写入迁移 | PB/VM 对账、重连/升级报告 |
| STORE-04 | STORE-02/03 | alert rule/state/event/delivery 与 SMART 闭环 | 触发/恢复/静默/投递/库存报告 |
| STORE-05 | STORE-00–04 + PLAT-03 | PB contract、VLogs 全裁撤、归档/恢复/销毁 | 30 天零读写、restore/rollback 报告 |
| PLAT-05 | PLAT-01/02/04 | QueryGateway、VM/CH provider contract | 权限/limits/completeness |
| PLAT-06 | PLAT-05 | visualization CRUD、query/binding/layout | 旧图表无损迁移 |
| PLAT-07 | PLAT-05 | 统计函数、provider-neutral export | 查询/导出同值同版本 |
| PLAT-08 | PLAT-01/05/07 | raw/supplier/customer policy/approval | raw 不变与版本复现 |
| FLOW-00 | PLAT-00A/02/03/04A | flow module、MySQL migration、资源/API 骨架 | 启停/升级/兼容/生命周期 |
| FLOW-01 | PLAT-03 + FLOW-00 | flow-collect listener、raw WAL、runbook/metrics | crash/disk/control-plane 故障恢复 |
| FLOW-02 | FLOW-01 | GoFlow2 adapter、normalized protobuf/Kafka | 四协议、采样、1.90M/2.86M 容量 |
| FLOW-03 | PLAT-04A + FLOW-02 | dimension worker、LPM/Geo/六维、base batch | snapshot/replay/守恒/非加和 |
| FLOW-04 | FLOW-03 + PLAT-05 | CH 六表/MV、base/derived repair、VM 管线指标、对账 | dedup/重建/TTL/故障恢复 |
| FLOW-05 | FLOW-04 + PLAT-06/07 | 总览/图表/API/CSV、health | UI/API/RBAC/查询导出一致 |
| FLOW-06 | FLOW-05 + PLAT-08 | endpoint/Geo 修正/对账/XLSX | Top/修正/三层/审计 |
| FLOW-07 | FLOW-06 | 境外/VPN 规则、行为评分、情报、异步 probe、finding、专题 | KPI/evidence/scope/误报/容量 |
| FLOW-08 | FLOW-04/07 | 跨机房、回算、rollup、analyzer 升级 | RPO/RTO/回算/长期恢复/兼容性 |
| RELEASE-01 | 全部目标工作包 | release/migration/dashboard/runbook/notes | 全量回归与观察窗口 |

### 2.2 全局一次性交付检查

- [ ] 数据字典冻结：NormalizedRecordBatch、MySQL 表、dimension bundle、ClickHouse base/derived、VM metrics、API DTO 和导出列均有 owner/version/兼容规则。
- [ ] 操作手册冻结：安装、enrollment、设备开通、地址库发布、扩缩容、凭据轮换、滚动升级、故障降级、备份恢复、回算、卸载均可由非开发人员照步骤执行。
- [ ] 性能基线冻结：容量输入表、公式、单实例证书、N+1 实例数、Kafka/WAL/CH 磁盘网络预算、查询并发和告警阈值全部有报告。
- [ ] 功能范围冻结：F0–F9、API 表、六个页面、RBAC、raw/supplier/customer、SNMP 可选对账和 nDPI 边界逐项映射到工作包。
- [ ] 测试资产冻结：真实脱敏 pcap、合成极值 fixture、golden protobuf、SQL expected、浏览器 E2E、故障注入和性能脚本有 checksum、版本和维护人。
- [ ] 发布门冻结：禁止跳过 migration、schema compatibility、race/security/performance/soak/chaos/restore/rollback 任一报告；不接受“上线后补”。

---

## 3. Platform P0：生产闭环

### PLAT-00 当前系统基线与迁移治理

本轮基线回归记录：`internal/watchdog`、命令包、生产 PB identity/route precedence 定向集成测试、前端 production build 及解除端口沙箱限制后的 `go test -count=1 -tags=testing ./...` 已通过。以下稳定性问题已在同轮关闭；父级“回归测试”仍须等生产副本、浏览器 E2E、race/soak 等证据齐全后再勾选：

- [x] GPU collector 测试不再用固定 50/150ms 猜测后台进程完成，改为带 2s 上限的条件等待并在锁内读取结果。
- [x] PB system updater 全部纳入 SystemManager 生命周期；Hub/test cleanup 在 DB teardown 前 cancel+join，新增 close/join 回归测试。
- [x] legacy WebSocket enrollment 测试等待 PB 业务状态收敛，不再把 TCP/WebSocket 建连瞬间误当作认证与首轮采集完成。

- [ ] **设计**：盘点 PocketBase/hub、watchdog runtime、MySQL migration/init、现有 API、前端路由及部署拓扑，形成带代码位置的现状基线。
- [ ] **编码**：建立 migration 版本规范、空库执行器和 init snapshot/parity 检查，不修改无关业务代码。
- [ ] **单元测试**：覆盖 migration 排序、重复执行、失败停止、版本校验和 snapshot diff。
- [ ] **集成测试**：在空库与当前生产版本副本上执行全量 migration，验证 schema、约束和种子数据。
- [ ] **变更设计**：记录无法由 migration 生成的 init 差异、历史表兼容期、数据迁移和回滚策略；无偏差也要签字确认。
- [ ] **变更测试**：验证旧版本→新版本、空库→新版本、失败 migration 恢复和允许范围内的 down/forward-fix。
- [ ] **回归测试**：运行现有 backend、SNMP、target、billing、export 和前端 smoke tests，确认基线治理未改变业务结果。

### PLAT-00A 全生命周期与 API/数据契约

- [ ] **设计**：冻结资源/job/data/feature 生命周期、公共字段、create/list/get/patch/action/delete-preview/delete/restore/purge/bulk 语义、ETag/If-Match、Idempotency-Key、错误码、query completeness envelope、retention/destruction receipt 和 desired-state 安全边界；只形成 watchdog core 公共契约，不新增独立微服务。
- [ ] **编码**：在现有 API/repository/worker 框架内实现公共 DTO、校验/middleware、乐观锁、幂等记录、审计钩子、异步 job lease/checkpoint、删除影响预览和 reconciler interface；模块按需注册资源规则，不建设通用工作流引擎。
- [ ] **单元测试**：覆盖重复 create/action、版本冲突、非法状态迁移、cursor/filter/sort、dry-run、依赖占用、tombstone/restore/purge、job cancel/retry/lease takeover、partial/empty/error 区分和未知配置拒绝。
- [ ] **集成测试**：以 target、collector、visualization、export 和 flow exporter 各跑一遍创建→验证→启用→停用→退役→删除→恢复→销毁；注入 API 重试、worker 崩溃和 actual-state 漂移，确认副作用不重复且不可逆动作不会自动执行。
- [ ] **变更设计**：记录与现有 handler/repository/job/status 字段的映射、兼容窗口和不适合统一的资源差异；若公共抽象开始承载业务流程，必须拆回模块 action 并更新 ADR。
- [ ] **变更测试**：验证旧客户端缺少 ETag/幂等键的兼容期、旧状态迁移、新旧错误响应、soft-delete 唯一键、retention 改动、schema forward-fix 和 reconciler 版本切换。
- [ ] **回归测试**：现有用户、target、agent、SNMP、图表、导出、billing 的列表/查看/增删改查、权限和后台任务均保持兼容。

### PLAT-01 生产身份、租户、用户、角色与权限

本轮已经完成的可独立核验切片；不因这些切片完成而提前勾选覆盖用户/角色 CRUD、变更与回滚的父项：

- [x] 生产 Hub 按 HTTP method 精确挂载 `/api/v1`，并验证 API 路由优先于 SPA fallback、未知 API 返回 JSON 404。
- [x] PB `users` token 经服务端验证后按 `external_subject_id` 投影到 MySQL；禁用/不存在/歧义/越权 tenant 以及 PB superuser 均 fail closed。
- [x] tenant discovery 与 tenant-scoped authorization 分离，解决多租户用户在尚未选择 tenant 时无法发现 membership 的启动闭环；前端提供 tenant selector。
- [x] 前端管理 API 注入 PB token、tenant 和 request ID，`/api/v1/me` 的 MySQL roles/grants 成为管理页面授权判断依据。
- [x] 登录改为被动触发：移除登录表单挂载时的 `authRefresh` 和单 OAuth provider 自动跳转；仅受控资源访问、用户主动提交或已主动发起的 OAuth callback 可以触发认证链路。
- [x] 身份投影、tenant discovery、请求 ID、生产路由与 readiness 的定向单元/集成测试通过。

- [ ] **设计**：确认 PocketBase/OIDC 身份权威、`external_subject_id` 投影、tenant 选择、AuthContext、用户/角色 CRUD 和资源继承规则。
- [ ] **编码**：在生产 hub 挂载 watchdog `/api/v1`，实现 IdentityAdapter、tenant/user/role/permission API、审计和 dev admin 隔离。
- [ ] **单元测试**：覆盖 token 映射、用户禁用、角色合并、resource inheritance、action 判定、跨 tenant ID 和 service account 隔离。
- [ ] **集成测试**：用真实登录 session 调用 watchdog API，验证用户/角色变更即时生效、前后端 401/403/404 行为一致。
- [ ] **变更设计**：记录身份字段弃用、session/tenant 切换、旧 MySQL password 字段和 permission 兼容策略。
- [ ] **变更测试**：验证旧用户/角色/grant 数据迁移、权限缓存失效、回滚后登录与授权不丢失。
- [ ] **回归测试**：覆盖现有 target、agent、network、metric、graph、billing、export 页面及 API 的原权限矩阵。

- [ ] **Platform P0 出口**：生产登录用户得到正确 tenant/grants；跨租户访问全部被拒绝；空库与升级 migration 通过；公共生命周期/幂等/乐观锁/删除恢复/任务恢复通过；现有主流程回归通过。

---

## 4. Platform P1：模块、资源、Collector 与 Target 基座

### PLAT-02 Module、Resource、TargetKind 与 Dataset Registry

- [ ] **设计**：冻结 descriptor、依赖、启停、migration、health、resource parent、target kind、dataset provider 和前端 manifest 契约。
- [ ] **编码**：实现 registry、重复 key/循环依赖校验、tenant module enablement，并先迁移 SNMP/system 模块验证契约。
- [ ] **单元测试**：覆盖注册成功、重复 key、依赖缺失、循环依赖、启停、资源继承、非法 dataset 字段和 descriptor 版本不一致。
- [ ] **集成测试**：启动完整 watchdog，验证模块路由、worker、health、菜单、tenant 启停和事实数据保留。
- [ ] **变更设计**：记录实际 Go interface、模块生命周期、兼容别名和 frontend manifest 生成方式与原设计差异。
- [ ] **变更测试**：验证模块新增/升级/禁用、migration 失败、兼容路由和旧 SNMP/system 注册前后行为。
- [ ] **回归测试**：现有 SNMP/system API、采集、页面、告警和权限结果保持一致。

### PLAT-03 通用 Collector/Agent 注册与接入

当前状态：**进行中**。migration `018` 已完成无损 expand 与旧 SNMP/system 同事务兼容投影；新 registry 写 API、plan/enrollment 和 contract 阶段尚未完成，因此不能宣称已切换权威。

- [x] **PLAT-03A registry expand 与兼容写**：落地 `collector_agents/collector_bindings/collector_plan_revisions`，复合 tenant/collector FK、desired/observed 分离、token/mTLS 互斥、ack/LKG/version 单调约束、单 active plan；回填 `target_agents`，现有 upsert/heartbeat/run/delete 在同一 transaction 双写，拒绝跨 tenant agent ID 接管。空库 001→018/rerun 和真实 MySQL 生命周期已验证。
- [ ] **PLAT-03B registry authority cutover**：实现新 repository/DTO/filter/API 与多 binding 引用校验，迁移 `agent_run_history` FK 和旧读路径；上线前逐行核对 legacy/new identity、binding、desired/observed，灰度期禁止第二套无投影写入口，最终停止 `target_agents` 写并 contract 删除。
- [ ] **PLAT-03C enrollment、credential 与 plan 生命周期（父项）**：C1 plan repository core 已完成；trust bundle、enrollment/credential、失败 ACK 与 rollout 仍未完成。
  - [x] **PLAT-03C1 plan repository core**：最大 4 MiB canonical JSON + SHA-256；Ed25519 envelope 绑定 plan/tenant/collector/version/schema/hash/key/effective interval/supersedes，只有验证后不可变值可入库。create 校验 schema/head/lineage；activate 以 collector+plan 双 row-version 在同事务 retire/activate/head/audit；服务端生成 activation/ACK 时间；ACK 精确匹配 active version/hash/expiry，只推进 ack/LKG/observed 且重复请求幂等。覆盖未验证签名、验证后篡改、非 canonical JSON、错误 ACK、退役 revision 复活、stale ETag、单 active、跨 tenant 读取、MySQL JSON 重格式化和 DB spec 篡改。
  - [ ] **PLAT-03C2 trust/enrollment/rollout 闭环**：signing key registry/trust bundle 与 rotation/revoke；一次性 secret、token 双窗口/mTLS、capability heartbeat；失败 ACK 保留 LKG、preview/canary/rollout、rollback 复制旧 spec 生成更高 version、expiry/kill switch。
- [ ] **PLAT-03D ownership transfer 机器证据**：plan retire/expiry、old-owner drain receipt、独立 Kafka principal revoke/provider receipt、new plan activation 和 authenticated restore ack 必须来自各自执行器，不接受管理员直接填写时间；完成后才能实现 `FlowStateCleanupEvidenceProvider`。

- [ ] **设计**：冻结 `collector_agents/collector_bindings/collector_plan_revisions` DDL、enrollment、token/mTLS、rotation、heartbeat、M:N binding、desired/observed health；本机硬上限∩签名 plan∩job 配置优先级、schema/API/capability 兼容、plan expiry/ack/LKG、preview/canary/rollback 和状态机。
- [ ] **编码**：实现表/migration、注册 API、一次性 secret、凭据轮换/吊销、capability heartbeat、不可变 plan CRUD/preview/validate/activate/rollback、原子 ack/LKG、rollout job、版本 diff/健康页和 `target_agents` 迁移；plan 只保存 secret reference。
- [ ] **单元测试**：覆盖 secret TTL/单次使用、hash、token 双窗口、mTLS fingerprint、规范化 plan hash、未知 key/签名/expiry、schema 区间、三层配置交集、单调 version、apply 失败保留 LKG、rollback 生成新版本、pending/active/suspended/revoked/deleted/restore/purge 状态和跨 tenant binding 拒绝。
- [ ] **集成测试**：真实新旧 agent 完成 enroll→capability→plan preview/publish/ack→canary→rollback→rotate→suspend/resume→revoke→delete/restore/purge；restore 不恢复凭据，控制面中断继续 LKG，过期主动授权 fail closed，一个 collector 绑定多个 target。
- [ ] **变更设计**：记录 legacy run history/config 映射、连续监听型 collector 语义、凭据存储、plan schema/version 兼容窗口、binary rollout 边界和 binding 引用校验变化。
- [ ] **变更测试**：迁移现有 SNMP/system agents，验证不停机兼容期、旧 token/plan schema、capability drift、scope 收缩、紧急 kill、canary 失败、rollback/forward-fix 和旧 revision 审计可读。
- [ ] **回归测试**：现有 agent 列表/详情/运行历史/心跳、SNMP 采集和 target 关联全部通过；未使用 revision/rollout 的 agent 仍能按兼容期接收旧 plan。

### PLAT-04 Target、网元、端口与资源管理

- [ ] **设计**：确认 target 资源根、TargetKindRegistry、network device/port 投影、列表/详情/CRUD、发现和删除影响预览。
- [ ] **编码**：实现可注册 target kind、统一 cursor/filter/sort、资源树、模块详情 tab、delete-preview 和 tombstone/retention 行为。
- [ ] **单元测试**：覆盖 target kind 校验、tenant 隔离、资源父子关系、删除约束、设备/端口 stable ID 和 ifIndex 映射。
- [ ] **集成测试**：完成 target→发现设备→端口→collector binding→模块 tab 全流程，并验证删除预览列出全部依赖。
- [ ] **变更设计**：记录当前 `system/network` 常量迁移、已有 API 兼容和事实数据不随管理对象删除的策略。
- [ ] **变更测试**：升级旧 targets/devices/ports，验证旧 URL/API、关联图表/导出/agent 和删除保护。
- [ ] **回归测试**：target、network discovery、device、port、SNMP 配置和列表/详情页面回归通过。

### PLAT-04A 地址统计维度快照与发布

- [ ] **设计**：冻结 `dimension_snapshots` DDL、prefix/set bundle、preview/publish、effective time、event-time 选版、worker ack、归档和回算边界。
- [ ] **编码**：实现 snapshot migration、bundle/checksum、preview/publish/retire/status API，并把 `address_prefixes/address_sets` 管理态发布为不可变版本。
- [ ] **单元测试**：覆盖版本递增、checksum、重叠 CIDR、最长前缀、selector、多 set 匹配、未命中、event-time 边界和非法扩张拒绝。
- [ ] **集成测试**：编辑 draft→preview→publish→workers ack→按 event time 生效；发布失败继续旧版本且不影响在线 flow。
- [ ] **变更设计**：记录现有 address CRUD 兼容、snapshot object 存储、版本保留、最大 set expansion 和历史回算范围。
- [ ] **变更测试**：验证旧 prefixes/sets 生成首个 snapshot、版本切换、retire、旧版本重放、bundle 丢失和 rollback/forward-fix。
- [ ] **回归测试**：现有 address prefixes/sets CRUD、labels、match_direction 和 SNMP/sFlow 原型读取保持可用。

### WATCHDOG-SUNSET：PB/MySQL/VictoriaLogs 收敛

本工作包执行[存储收敛详细设计](storage-consolidation.md)。它不是“删表清理”，而是按数据权威逐域完成 expand→backfill→shadow compare→cutover→observe→contract。任何 STORE 子包都必须同时交付以下七类证据。

#### STORE-00 现状基线、迁移治理与 VLogs 安全止血

本轮已完成的原子项（大项仍需 PB 调用指标、manifest 和生产副本证据后才能勾选）：

- [x] 删除 sFlow collector 的逐 flow VictoriaLogs goroutine/HTTP sink，保留现有 VM aggregate 行为。
- [x] 删除前端 FlowSearch 直连页面、菜单和路由；代码/配置不再包含可恢复 VLogs sink 的开关。
- [x] 删除 VLogs YAML 字段；旧 `WATCHDOG_VICTORIALOGS_URL/WATCHDOG_SFLOW_VLOGS_URL` 出现即明确启动失败。
- [x] `internal/watchdog` 单测、前端生产构建和全局 `git diff --check` 通过。
- [x] migration 原位嵌入二进制，按连续版本排序、SHA-256 校验、同连接 advisory lock 执行；重复运行零 DDL，未知/漂移版本 fail closed。
- [x] `install/init.sql` 包含 migration ledger 与 expand 后 users snapshot；隔离空库验证 001–011 首次执行和重复执行。
- [ ] 增加 PB collection read/write/subscribe 指标、migration manifest/checkpoint/quarantine 和生产副本零调用报告。

- [ ] **设计**：冻结 14 个 PB 业务集合的读/写/订阅/hook/cron/API/UI 矩阵、MySQL/VM 去向、数据不变量、迁移 manifest、quarantine、观察期和不可逆动作审批；纠正“system 域是僵尸集合”和调用次数百分比口径。
- [ ] **编码**：加入 PB collection read/write/subscribe 计数与迁移检查器；禁用前端 FlowSearch 直连，停止逐 flow VLogs goroutine HTTP 写；如需确认旧消费者，只保留请求计数/迁移日志，不保留可重新写入 VLogs 的运行开关。
- [ ] **单元测试**：覆盖 manifest checksum、分页断点/重复执行、PB ID→MySQL ID 映射、未知字段 quarantine、零读写判定和 VLogs sink 禁用。
- [ ] **集成测试**：对生产副本生成完整清单；验证浏览器/collector 到 9428 请求为 0、旧 sFlow 聚合在 VLogs 关闭后行为已知、PB 采集指标能定位全部调用方。
- [ ] **变更设计**：登记实际集合数量、隐式 hooks/raw SQL、旧 Agent 版本、VLogs 数据/端口/部署依赖和不能自动迁移的记录；无差异也要签字。
- [ ] **变更测试**：验证升级前后 manifest 可比、止血开关回退、VLogs 不可用/慢响应不会拖垮 collector，以及旧配置出现时给出明确弃用错误。
- [ ] **回归测试**：PB 登录、现有 system/SNMP/sFlow aggregate、target、图表和导出不因止血与观测埋点改变业务结果。

#### STORE-01 生产 Hub、身份投影与前端 API 客户端

本轮已完成的原子项（大项仍需 readiness、完整 CRUD/typed client、真实 OTP/OAuth/E2E 与回滚演练后才能勾选）：

- [x] 生产 Hub 延迟初始化 MySQL runtime，并在同一 PB Router 分方法挂载 `/api/v1`；API route precedence 测试排除 SPA fallback/启动冲突。
- [x] 实现 PB `users` token→external subject→MySQL tenant/user/role/grant 的 fail-closed IdentityAdapter；PB superuser/role 不成为业务授权。
- [x] 实现零/单/多 tenant、禁用主体、跨 tenant、MySQL failure 的 401/403/400/503 语义及 JSON API 404。
- [x] 实现 `/api/v1/me/tenants`，前端 API shim 注入 PB token 和按 subject 保存的 `X-Watchdog-Tenant-ID`。
- [x] 新增 MySQL migration 011、新装 schema parity 和显式 `watchdog-identity-link`；保留 nullable `password_hash` 作为 expand/contract 回滚窗口。
- [x] 空 MySQL 隔离库顺序执行 001–011，并验证重复执行、schema gate、subject link、投影读取和 tenant admin 解析；测试库已销毁。
- [x] 新增 `/api/v1/health/live` 与 `/api/v1/health/ready`；ready 校验 MySQL ping 和全部嵌入 migration 的版本/checksum。
- [x] 所有 `/api/v1` 请求生成或校验 `X-Request-ID`，写入请求上下文并在响应回显；前端兼容 client 主动生成 request ID。
- [x] tenant discovery 与 tenant-scoped authorization 分离，首次多租户登录无需预先发送 tenant header；前端 selector 按 PB subject 保存选择并在切换后整页重载，避免复用旧租户页面状态。
- [x] 前端管理入口与只读判定改读 `/api/v1/me` 的 MySQL `is_admin/grants`，不再使用 PB `users.role` 作为业务授权显示依据。
- [ ] 补 ETag/idempotency、用户/角色 CRUD。
- [ ] 完成 password/OTP/OAuth、Hub 重启、subpath、权限即时失效和旧客户端/rollback E2E。

- [ ] **设计**：冻结 Hub 启动/路由顺序、PB token server-side 校验、`auth_provider/external_subject_id`、tenant 选择、bootstrap saga、MySQL fail-closed、JSON 404/503、token refresh/idempotency 和 dev admin 隔离。
- [ ] **编码**：生产 Hub 挂载 runtime `/api/v1`；实现 IdentityAdapter、readiness 和 `/api/v1/me/tenants`；typed API client 注入 token/tenant/request ID/ETag，SPA fallback 排除 `/api/*`；MySQL user 先 expand/backfill，禁止同版直接 drop password。
- [ ] **单元测试**：覆盖 token 缺失/过期/伪造、零/单/多 tenant、禁用用户、重复 subject、PB role 与 MySQL grants 冲突、bootstrap 重试、MySQL down 和安全/非幂等请求重放。
- [ ] **集成测试**：真实 PB password/OTP/OAuth session 调用所有 `/api/v1`；验证 401/403/404/409/503、tenant 切换、权限即时失效、Hub 重启和 subpath 部署。
- [ ] **变更设计**：记录 PB `role` 兼容期、用户邀请/JIT 取舍、旧 `password_hash` 行、session 续期和 production/dev 配置差异。
- [ ] **变更测试**：空库、旧库升级、subject backfill、重复 identity 修复、nullable→drop password 两阶段、旧客户端兼容和失败 migration forward-fix。
- [ ] **回归测试**：登录/忘记密码/OTP、target、agent、network、metric、graph、billing、export、settings 全页面用生产身份运行。

#### STORE-02 用户偏好、通知渠道与静默域

- [ ] **设计**：冻结 `user_preferences/notification_channels/alert_suppressions` DDL、UI 设置 allowlist、webhook secret/SSRF、时区/DST/跨午夜、CRUD/ETag/soft-delete/retention 和 PB JSON 转换。
- [ ] **编码**：实现 migration/repository/API/前端；首次 GET 不隐式写库；email/webhook 从 settings JSON 拆分；旧 PB 记录按 subject→tenant projection 幂等 backfill。
- [ ] **单元测试**：覆盖默认偏好、未知 key、版本冲突、email 规范化、webhook 掩码/SSRF、one-time/daily、DST、跨午夜、重叠、过期清理和跨 tenant ID。
- [ ] **集成测试**：现有设置/通知/quiet-hours 页面完成 list/get/create/put/patch/delete/test；新旧值对比、并发编辑、secret store 故障和通知读取通过。
- [ ] **变更设计**：记录无法映射的 settings key、缺失用户时区、一个 PB subject 多 tenant 的复制策略和通知渠道兼容期。
- [ ] **变更测试**：执行 backfill→shadow read→短时写冻结→cutover→reverse export/forward-fix 演练；验证 count/checksum、quarantine 和 PB 只读窗口。
- [ ] **回归测试**：主题、语言、单位、布局、通知测试、告警发送与静默行为和迁移前一致。

#### STORE-03 Legacy Agent、system 运行域与历史归档

- [ ] **设计**：冻结 `systems→target/collector/binding/RBAC`、`system_details→system_target_profiles`、stats/container/systemd→VM、connection manager、legacy protocol 窗口、archive manifest/RPO/RTO 和 action 鉴权。
- [ ] **编码**：legacy `/api/watchdog/agent-connect` 适配 collector identity；样本标准化写 VM，profile 写 MySQL；容器日志/systemd info/SMART refresh 经 target binding 调在线连接；生成 system/container stats 离线归档工具。
- [ ] **单元测试**：覆盖 token/fingerprint、system→target 映射、幂等 inventory、状态 desired/observed 分离、断连/重连、重复样本、VM 写失败、action 越权、归档 count/checksum。
- [ ] **集成测试**：真实新旧 Agent 连续上报，比较 PB/VM 同窗口样本/速率/状态；执行 Hub 重启、Agent 升级、网络抖动、VM/MySQL 故障、日志/systemd action 和归档恢复。
- [ ] **变更设计**：记录不兼容 Agent 版本、旧 users membership→permission 映射、container/systemd current view 查询和无法归档的损坏 JSON。
- [ ] **变更测试**：按 target 灰度切写、shadow compare、回退/forward-fix；验证旧 PB history 只归档不在线双读、恢复样本可解码。
- [ ] **回归测试**：target/system overview、指标、containers、systemd、Agent status/heartbeat、告警输入和服务动作均通过。

#### STORE-04 告警与 SMART 业务闭环

- [ ] **设计**：冻结 `alert_rules/rule_states/events/deliveries/storage_devices` DDL、durable pending、dedup/for-duration/恢复/ack/suppression/delivery lease/retry/unknown outcome、UI 条件轮询、target 父资源、stable device key、latest-vs-history 和 forget 语义。
- [ ] **编码**：迁移 PB alert hooks/cache/timers/history 与 SMART hooks 到 MySQL repository/worker；实现 API、异步 refresh、VM SMART 历史、15s/60s ETag polling 和前端规则/历史/磁盘页面。
- [ ] **单元测试**：覆盖并发 dedup、pending 重启恢复、阈值边界、静默不删事件、delivery lease/幂等/retry/failed/unknown outcome、ack/resolve 状态机、SMART worsening/unknown/stale、设备 key/重现和跨 tenant。
- [ ] **集成测试**：使用相同 fixture 对比新旧 CPU/status/disk/SMART 触发和恢复；邮件/webhook sandbox 验证断网、重试、重复消费、Hub 重启、轮询断线/恢复和导出同值。
- [ ] **变更设计**：记录旧 `triggered`→active event、history 时间/owner/resource 映射、通知失败历史、SMART 超限 attributes 和 UI realtime→polling 差异。
- [ ] **变更测试**：alerts/quiet/history/smart backfill、shadow evaluator、单 tenant 灰度、切换/forward-fix、retention/purge 和旧 PB 只读查询演练。
- [ ] **回归测试**：规则增删改查、全局复制、告警历史筛选/删除/导出、通知/静默、SMART 列表/详情/刷新/forget 和权限矩阵通过。

#### STORE-05 PocketBase 收缩、VictoriaLogs 裁撤与发布验收

- [ ] **设计**：冻结 PB application collection 最终清单、hooks/cron/API/config/deploy 删除图、30 天观察门、备份/restore、reverse export/forward-fix、password/collection/VLogs 不可逆变更和 destruction receipt。
- [ ] **编码**：移除 13 个 PB 业务集合的运行读写/订阅/hooks/cron/schema；移除 PB role 业务读取和旧 universal token；删除 VLogs config/env/client/page/route/compose/Helm/health/docs，清理不再使用的依赖。
- [ ] **单元测试**：启动时断言只允许 auth 应用集合；未知旧 collection/config 拒绝或迁移；验证 PB auth migration、MySQL foreign keys、VLogs 符号/环境变量静态清零和 purge guard。
- [ ] **集成测试**：空库安装与生产副本升级后完成登录、全部管理 API/worker/Agent/告警/SMART；PB/MySQL 双备份 restore，离线 archive restore，VLogs 不部署仍全绿。
- [ ] **变更设计**：记录实际删改的 collection/field/API/config/port/image、兼容期偏差、保留的 PB internal tables 和销毁审批。
- [ ] **变更测试**：旧版→兼容版→收缩版、升级中断、PB/MySQL 单侧恢复、密码字段 drop、collection contract、旧配置启动、rollback/forward-fix 和销毁演练。
- [ ] **回归测试**：全量 Go/frontend/E2E/race/security/soak；连续至少 30 天 PB business read/write/subscribe=0、VLogs request=0 后才签署退出。

- [ ] **Platform P1 出口**：模块可独立启停；旧 agent 无中断迁移；同一 collector 可绑定多个资源；target CRUD、dimension publish 和权限继承通过；STORE-00–04 完成，PB 业务集合已进入只读观察期。

---

## 5. Platform P2：查询、图表、统计与导出

### PLAT-05 QueryGateway 与指标/数据查看

- [ ] **设计**：冻结 DatasetDescriptor、VM/CH provider、query JSON、资源/RBAC 过滤、统计函数、limits、completeness 和统一响应。
- [ ] **编码**：实现 DatasetRegistry、QueryGateway、VM provider、`POST /api/v1/query` 和旧 metrics API 兼容适配。
- [ ] **单元测试**：覆盖字段白名单、filter/group/aggregation、step/range/series 上限、tenant/resource 权限、value layer 和错误映射。
- [ ] **集成测试**：以相同 fixture 对比旧 metrics API 与新 gateway 结果，验证 VM 故障、超时、partial 和完整率。
- [ ] **变更设计**：记录 provider interface、query schema、旧响应兼容期及无法统一的 metric 语义。
- [ ] **变更测试**：验证新增/删除 dataset、descriptor 版本升级、旧客户端、缓存失效和 provider 切换。
- [ ] **回归测试**：现有 target/port/system/SNMP 指标、P95、峰值、平均和历史数据页面结果不变。

### PLAT-06 Visualization 图表 CRUD

- [ ] **设计**：冻结 visualization/query/binding DDL、图表 kind、query builder、资源绑定、layout、预览和 dashboard 组合规则。
- [ ] **编码**：实现三表 migration、CRUD/data/preview API、图表库/编辑器 UI，并迁移 aggregate graphs。
- [ ] **单元测试**：覆盖 query JSON 校验、owner/visibility、binding 权限、kind/display 配置、删除和非法 SQL/PromQL 拒绝。
- [ ] **集成测试**：创建、预览、修改、绑定、查看、复制和删除多种图表；验证 VM/CH dataset 均可工作。
- [ ] **变更设计**：记录 aggregate graph/data 的迁移、冻结 snapshot 处理、路由兼容和 layout schema 变化。
- [ ] **变更测试**：验证旧图表迁移前后序列/统计/权限一致、旧 API 兼容及 rollback/forward-fix。
- [ ] **回归测试**：现有 aggregate chart 列表、详情、编辑器、target/port 图表和空态/错误态通过。

### PLAT-07 统计、快照与 Provider-neutral Export

- [ ] **设计**：冻结统计函数、export task 的 dataset/query snapshot/value layer/version/format、worker、文件生命周期和下载权限。
- [ ] **编码**：provider 化 export worker，支持 CSV/XLSX 扩展点、进度/取消/重试/下载/过期，并固化查询和版本。
- [ ] **单元测试**：覆盖 P95/总量/share/difference、query hash、重试幂等、权限、文件 checksum、过期和行数上限。
- [ ] **集成测试**：从 VM 与 CH dataset 分别创建导出，验证在线查询与导出结果一致、失败重试仍用原 snapshot。
- [ ] **变更设计**：记录旧 target/port export 字段兼容、存储后端、XLSX 方案和审批/审计变化。
- [ ] **变更测试**：迁移旧 export tasks，验证旧文件下载、旧 CSV 行列、任务恢复、取消和过期清理。
- [ ] **回归测试**：现有 export、billing/P95、权限、下载和前端任务页面回归通过。

- [ ] **Platform P2 出口**：现有指标/图表/导出迁移前后一致；所有查询经 gateway；模块不能提交任意 SQL/PromQL；query/export 权限通过。

---

## 6. Platform P3：raw、supplier、customer 三层修正

### PLAT-08 Adjustment Policy、版本与审批

- [ ] **设计**：冻结三层语义、policy DDL、scale/add/clamp、dimension filter、匹配顺序、版本、有效期、审批和分层 actions。
- [ ] **编码**：实现 policy migration、CRUD/preview/submit/approve/retire、AdjustmentService、QueryGateway/export/billing 接入和 legacy correction 冻结。
- [ ] **单元测试**：覆盖 decimal 精度、raw 不可改、supplier/customer 平行计算、规则冲突、优先级、有效期、版本重现和权限隔离。
- [ ] **集成测试**：同一 raw 通过查询、图表、统计、导出得到一致的 supplier/customer 结果，响应固化 policy ID/version。
- [ ] **变更设计**：由业务确认旧 corrected 归属；记录无法归类规则、显式 customer-on-supplier 需求和审批流程变化。
- [ ] **变更测试**：验证 legacy policy 迁移/只读、active 新版本、历史版本查询、规则退役、升级/回滚不改 raw。
- [ ] **回归测试**：原始指标、计费、导出、traffic defaults、port policy 页面及没有新 policy 时的结果保持稳定。

- [ ] **Platform P3 出口**：raw 永不改写；相同 raw+policy version 结果可复现；冲突规则不能发布；三层 view/export/configure/approve 权限通过。

---

## 7. Flow P1：一体化 Flow-Collect、单一事实 Topic、异步维度与六维总览

### FLOW-00 Flow 模块骨架与管理表

- [ ] **设计**：冻结 flow module descriptor、资源/dataset/API/worker/health、canonical/兼容路由及五张 MySQL 表与平台表依赖。
- [ ] **编码**：建立 `internal/modules/flow`、模块注册、migration、exporter/settings/VPN repository 和 `/api/v1/modules/flow` 路由骨架。
- [ ] **单元测试**：覆盖 descriptor、依赖、repository CRUD/FK、tenant/collector/target binding、exporter pending/active/suspended/retired/deleted desired state、warming/healthy/degraded/stale observed health、非法迁移、乐观锁、幂等 action 和 canonical/compat route。
- [ ] **集成测试**：启停 flow module，验证 migration、菜单/API/worker/health 生命周期、suspend/retire/tombstone/restore/purge、删除影响预览和停用不删事实数据。
- [ ] **变更设计**：记录实际包路径、表字段/index/FK、兼容期限和旧 sFlow 原型迁移策略。
- [ ] **变更测试**：空库/升级 migration、模块启停、旧 `/api/v1/flow` 转发和 rollback/forward-fix。
- [ ] **回归测试**：未启用 flow 时现有 watchdog 功能、启动时间、路由和 DB migration 不受影响。

### FLOW-01 `watchdog-flow-collect` UDP、raw WAL 与接入控制

当前状态：**进行中**。首批数据面骨架已进入代码库，以下细项用于防止把“可构建”误当成 FLOW-01 整体验收完成。

- [x] **FLOW-01A 配置与身份边界**：`flow_collect` 严格 YAML/`WATCHDOG_FLOW_COLLECT_*` 配置、Ed25519 签名 plan 校验、4096 项 partition map 校验、source CIDR LPM + observation-domain 准入；tenant 不从 YAML 固定值或报文获取。
- [x] **FLOW-01B UDP 与 WAL v1**：sFlow/NetFlow 双 listener、可选 `SO_REUSEPORT`、固定 reader/worker、有界队列、单 WAL writer、segment header/record CRC、确定性 datagram ID、group fsync durable barrier、损坏尾截断恢复、软硬水位且 hard limit 不覆盖。
- [x] **FLOW-01C checkpoint 基线**：append-only ACK2 child journal 与 group commit、重启恢复部分 ack、增量 replay cursor/queue 背压续扫、只跳过全确认 datagram、只回收全确认的关闭 segment，并在回收时压缩 ack journal。
- [ ] **FLOW-01D 控制面闭环**：enrollment/heartbeat、plan 热刷新与 LKG/history、revocation/credential rotation、未知来源限速 quarantine topic、VM/Prometheus 暴露及 data-loss interval 审计。
- [ ] **FLOW-01E 故障与容量验收**：kill -9/掉电/磁盘满/滚动升级故障注入，以及目标硬件上 WAL MB/s、fsync P99、socket drops 和 1.90M/2.86M records/s 报告。

- [ ] **设计**：冻结 listener/source plan、exporter affinity、socket/queue 模型、raw WAL segment/header/checksum/group-commit/checkpoint、磁盘软硬水位、RPO、quarantine、凭据和按端口 PPS/采样率/方向的容量公式。
- [ ] **编码**：实现独立 flow-collect、enrollment/heartbeat/plan、`SO_REUSEPORT` listener、source admission、确定性 datagram ID、append-only WAL、启动恢复、segment 回收、磁盘保护和低基数 metrics。
- [ ] **单元测试**：覆盖 listener/source/tenant 准入、datagram ID、WAL append/fsync/checksum/截断、进程崩溃恢复、连续 checkpoint、segment 回收、磁盘 soft/hard limit、quarantine 限速和敏感 payload 策略。
- [ ] **集成测试**：真实 UDP/文件系统故障下注入 kill -9、部分写、磁盘满、控制面中断和滚动升级；确认已 fsync datagram 可恢复、未确认 segment 不覆盖、未知来源不进正式 topic，并记录 datagrams/s、kernel drops、WAL MB/s/fsync P99、CPU/RSS。
- [ ] **变更设计**：记录实际文件系统/磁盘型号、WAL 格式版本、fsync/RPO、容量小时数、VIP/affinity、部署凭据和旧 collector 端口切换方案。
- [ ] **变更测试**：验证 WAL v1→v2、带未确认 segment 的滚动升级、证书/plan 轮换、collector ID 变化、磁盘扩容、forward-fix 和旧版本拒绝新格式。
- [ ] **回归测试**：旧 collector 停用/端口切换过程不双监听、不重复采集；agent/target 管理和其他采集器不受影响。

### FLOW-02 GoFlow2 解码、采样归一与 normalized Kafka 契约

当前状态：**进行中**。GoFlow2、normalized、单节点 collect-state、DLQ/quarantine、质量状态机、本地 journal/snapshot、Kafka 启动回读、签名 plan history、跨进程 attempt generation 与指标出口已经贯通；跨 owner 质量状态和生产容量尚未闭环。

- [x] **FLOW-02A GoFlow2 adapter**：同进程 GoFlow2 v3 解码 sFlow v5、NetFlow v5/v9、IPFIX；sFlow 按 sample 转换并保留 sub-agent/source/sample-pool/drops，NetFlow v5 保留 ASN/采样率，v9/IPFIX 按 exporter/domain 隔离内存模板。
- [x] **FLOW-02B 计数与契约**：sampled/pre-scaled 明确分支、零采样率拒绝、精确 sampling rule、乘法溢出拒绝；protobuf v1 补齐 ASN 和 sFlow 状态，xxHash64/4096 virtual shard、稳定 child batch ID、records/bytes 边界均已有单测。
- [x] **FLOW-02C Kafka/恢复基线**：Sarama idempotent async producer、manual physical partition、`acks=all`、TLS/system roots、压缩、成功回执后 child ack；未确认 child 可从 raw WAL 重放。
- [ ] **FLOW-02D collect-state（父项）**：以下 D1/D2/D3A 已完成；D3B 未完成前不得宣称跨进程/跨节点恢复闭环。
  - [x] **FLOW-02D1 本地状态契约**：`CollectState` protobuf、exporter/domain 精确 template/sampling snapshot、本地 magic/length/CRC32 + payload SHA-256、file/directory fsync + atomic rename、损坏 fail-closed、超过可配置 TTL（默认 30 分钟）的 stale state 不恢复。
  - [x] **FLOW-02D2 发布安全点与 worker ownership**：WAL 是唯一 dispatch source；exporter/domain affinity queue；状态先本地 durable、再 Kafka `acks=all`、再 ACK2 child 0、最后 data children；已确认 state child 重试不重发；template-pending 保留 WAL；进程内 retry 推进 `replay_generation`。
  - [x] **FLOW-02D3A owner-independent 恢复契约核心**：CollectState v2 已拆分稳定 `state_identity_key` 与带 `ownership_epoch` 的 compacted `state_key`，plan schema v2 要求显式 epoch；decoder `state_generation` 可连续恢复；恢复选择器按当前签名 registry 校验 tenant/exporter/source/domain，按 `(epoch,generation)` 选新，epoch 复用或同序不同 payload fail closed；schema v1 仅允许同 collector 本地兼容。已覆盖跨 owner 恢复、旧 owner 迟到高 offset/高 generation 不能获胜、epoch 复用、冲突和 v1 边界。
  - [x] **FLOW-02D3B1 Kafka 启动恢复闸门**：启动先捕获各 partition `[log-start, high-watermark)`，并行读取固定边界；使用 leader Fetch response 的实际 record/batch offset 推进游标，空 response 只有在 broker watermark 覆盖冻结边界时才把尾部判定为 compacted hole，不依赖一定存在的 `high-watermark-1` 消息。严格校验 message order/value 上限、partial/overflow/事务型 data batch、Kafka key/payload、跨 partition 重复、tombstone、protobuf/digest/registry/epoch，受 2m timeout 与 262144 candidate 默认硬上限保护；本地/远端候选先一次性选择再恢复 decoder，完成前不打开 WAL/runner/listener。已覆盖内部/尾部 offset hole、边界冻结、coalesce、边界后消息排除、Kafka null/empty 区分、tombstone、key mismatch、timeout 与本地新状态不被远端旧状态反向覆盖。
  - [x] **FLOW-02D3B2 plan/attempt 持久化（父项）**：B2A/B2B 均已完成；旧 WAL 的签名解释和失败尝试在进程重启后连续。
    - [x] **FLOW-02D3B2A 签名 plan history 与旧 WAL 精确解析**：原子持久化签名 revision，payload/revision/collector identity 不可变、拒绝 downgrade；active 不可用时仅回退到仍在有效期的最高 LKG。旧 WAL 依其 `registry_version` 使用历史 binding/sampling/partition map，并校验接收时间处于历史 plan 有效期；collect-state 仅对 active 已释放的元组使用同 collector 历史授权，相同元组已改绑时 fail closed。registry 对签名 plan 做深拷贝，调用方不能篡改。
    - [x] **FLOW-02D3B2B attempt journal 与 history 回收**：失败后先把 next generation group-fsync durable 再重试/DLQ，成功主路径不写 journal；header 绑定 collector，record magic/version/CRC、逐一递增、torn-tail 截断/完整损坏 fail closed、硬上限与原子 checkpoint 已实现。启动只恢复仍在 WAL 的 ID；terminal generation 仅在 ACK durable 后回收。plan history 允许一次 `max+1` 原子安装窗口，随后按 WAL pending `registry_version` 保留 active、历史最高防回退水位及引用项并回收其余，缺 revision 或引用集合超限均拒绝启动。
  - [ ] **FLOW-02D3B3 Kafka/切换集成闭环（父项）**：D3B3A 已完成；D3B3B 的实际基础设施、reconciler 与真实故障矩阵未完成。
    - [x] **FLOW-02D3B3A topic/TLS 启动契约**：关闭 Sarama auto-create；启动在恢复 state/开放 UDP 前校验四 topic 的存在/角色唯一、连续 partition、leader/offline replicas、RF/ISR、`cleanup.policy`、retention、message 上限、unclean leader election 与 `DESCRIBE_CONFIGS` ACL。normalized 下限覆盖配置及全部保留历史 plan，collect-state partition 数精确且禁止原地扩容；producer 上限覆盖 64MiB state；支持私有 CA、hostname verification 和成对 mTLS client cert/key。旧 epoch tombstone 的 revoke→replacement durable→审计清理安全点已经冻结，但未在 runtime 暴露任意删除。
    - [x] **FLOW-02D3B3B1 topic provisioning CLI**：新增独立 `watchdog-flow-kafka-bootstrap`；显式 `--apply` 只创建缺失 topic，不 alter 既有 topic，使用配置冻结的 partition/RF/ISR/cleanup/retention/message/unclean-election 值并在创建后复用 runtime verifier。默认模式只读检查；部署契约要求短期 admin 配置/证书与 runtime principal 分离。
    - [ ] **FLOW-02D3B3B2 reconciler/真实故障验收（父项）**：B2A 状态机核心已完成；宿主持久化 wiring、实际基础设施和真实故障矩阵仍在 B2B。
      - [x] **FLOW-02D3B3B2A 安全回收状态机核心**：decoder v2 与 quality typed key 共用可持久化五阶段状态机；只有 old plan/独立 principal WRITE 撤销、expiry+clock-skew、ACL 传播和 drain 全部成熟，且 frozen boundary 证明新 owner 恢复 exact old epoch/generation 后在更高 epoch 产生新 generation，才生成 exact old key 的 Kafka-null tombstone。发布 receipt 先保存，再由覆盖 ACK offset 的更晚 frozen boundary 确认 key absent；snapshot 深拷贝、逐阶段校验并支持幂等恢复，已覆盖各阶段 JSON round-trip/kill 窗口、过早 fence、共享 principal、epoch 复用、错误 restore proof、未推进 generation、错误 verification boundary 和 typed key/null 编码。runtime 未暴露任意 key 删除。
      - [ ] **FLOW-02D3B3B2B 管理面 wiring 与真实故障验收（父项）**：B2B1 writer、B2B2 scanner、B2B3A MySQL repository、B2B3B1 reconciler core 已完成；真实 evidence provider、进程 wiring 与基础设施仍未完成。
        - [x] **FLOW-02D3B3B2B1 专用 tombstone writer**：与 runtime publisher 类型和生命周期隔离；关闭 auto-create，使用 idempotent producer、`acks=all`、单 in-flight、稳定 hash partition 和独立 mTLS 配置；只接收状态机产生的私有-key `StateTombstone`，发送 Kafka null 而非零长 value，成功返回 broker 实际 partition/offset 与 ACK 时间，失败不产生 receipt。reader 同步改为只把 nil value 识别为 tombstone。mock producer 覆盖消息、receipt、broker failure 和安全配置。
        - [x] **FLOW-02D3B3B2B2 targeted frozen-boundary scanner**：按 exact typed key 扫描先冻结全部 partition 边界，再并行越过 compacted offset hole；只保留目标 key 的最后 record/value，保留 tombstone 位置并区分 null 与 empty，拒绝跨 partition 重复 key/非法 partition 布局。replacement proof 与 tombstone absence proof 均由同一不可猜测的 boundary/position 生成，receipt offset 未被更晚 high watermark 覆盖时拒绝完成。
        - [x] **FLOW-02D3B3B2B3A MySQL job/lease/audit repository**：migration `017` 新增宿主 core 公共 `operation_jobs`，不是第六张 flow 私有表；cleanup 初始 snapshot 与 canonical request hash 绑定，`(tenant,job_type,idempotency_key)` 保证同请求重放/异请求冲突。领取使用 `FOR UPDATE SKIP LOCKED`，过期 running lease 可抢占，写检查点/重试/失败由 `lease_token + row_version + lease_expires_at` fencing；每次状态/阶段变化与 `audit_logs` 同事务提交，终态释放 lease。已覆盖 tamper/非法跨阶段、幂等冲突、过期 token、真实 MySQL 空库 migration 与完整 repository 生命周期。
        - [ ] **FLOW-02D3B3B2B3B reconciler 与生命周期 wiring（父项）**：B3B1 核心已完成；B3B2 宿主接入未完成。
          - [x] **FLOW-02D3B3B2B3B1 phase executor/lease/restart core**：reconciler 在单次 claim 内逐阶段执行，但 fence、replacement、Kafka-null ACK、later-boundary absence 每推进一步都先以 `lease_token + row_version` 落 MySQL，依赖等待则从最近 checkpoint requeue，证据不安全终态失败，heartbeat 丢租约立即停止且不写进度。replacement key 由状态机按新 epoch 派生，Kafka reader 对 payload 完整解码校验；restore proof 显式携带 new-epoch baseline generation，修复默认 0 会弱化 generation-advance 证明的问题。固定 error code、有界 detail、指数退避/稳定 jitter、attempt 上限和进程重启接续已有单测；真实 MySQL 下四阶段、六条审计和同毫秒连续 renew 已重复验证。
          - [ ] **FLOW-02D3B3B2B3B2 宿主 evidence/config/health wiring**：从 plan activation/revocation、collector drain/heartbeat、独立 principal ACL 撤销记录生成不可伪造 fence 与 restore proof；实例化专用 scanner/writer/reconciler，接 watchdog 启停 drain、严格配置、低基数 metrics/readiness 和受权 job create/get/retry API。覆盖服务关停、DB 短断、租约过期抢占与 API RBAC/幂等。
        - [ ] **FLOW-02D3B3B2B3C 真实基础设施与故障闭环**：以实际 IaC/短期 admin principal 建立四 topic 与最小 ACL，reconciler 专用 principal 只获得 collect-state targeted read/write，runtime principal 无 create/alter/delete。完成真实 template/options corpus、多 broker、ACL/TLS 拒绝、retention 容量、进程重启、kill -9、broker/ISR 故障和滚动 owner switch 集成测试。collect-state 如需扩容，必须新版本 topic + 有界迁移，禁止原地加 partition。
- [ ] **FLOW-02E 异常闭环（父项）**：以下 E1/E2A/E2B1/E3 已完成；跨 owner 质量状态尚未完成。
  - [x] **FLOW-02E1 DLQ/quarantine**：稳定 ID 的 `DecodeFailure` 与无 tenant/raw payload 的 `QuarantineEvent` protobuf；确定性 decode/normalize 错误有界指数退避，DLQ Kafka ack 后才确认 WAL；模板状态先于 DLQ；未知来源用固定异步队列和 per-source/global 双限流，慢 Kafka 不阻塞采集；DLQ raw payload 默认关闭且可设严格字节上限。
  - [x] **FLOW-02E2A 进程内质量状态**：按准确协议单位维护 sFlow datagram/source、NetFlow v5 engine、v9/IPFIX domain 的 sequence gap/out-of-order/restart，以及 sample-pool reset/rate-change epoch；排除 uint32 回绕；WAL/Kafka retry 按 datagram ID 复用首次判定；protobuf 追加 exporter/quality epoch 与 sample index；有界状态饱和只打标、不阻塞、不改流量计数。
  - [x] **FLOW-02E2B1 本地持久质量状态**：raw WAL durable 后、Kafka 前 append 一次/datagram 的绝对 post-state + retry decision journal，Kafka 等待与本地 group fsync 并行，terminal WAL ACK 前强制 durability barrier；同 ID 幂等、原子 snapshot、ACK 水位过滤、snapshot→journal 恢复、尾部 torn-write 截断和完整损坏 fail-closed 已实现；周期 checkpoint 用 RW barrier 取得一致切面，不产生 per-flow 或 per-datagram Kafka 写放大。
  - [ ] **FLOW-02E2B2 跨 owner 持久质量状态（父项）**：把 committed coalesced snapshot 接入固定 exporter ownership 与 compacted Kafka checkpoint；连同 D3 的 plan history/attempt generation 恢复，完成 kill -9、旧 WAL、迟到模板报文、跨节点 owner 切换和 journal 容量/恢复时间故障注入后，才宣称跨节点 epoch 连续。
    - [x] **FLOW-02E2B2A owner-fenced checkpoint 核心**：新增 `QualityCheckpoint` protobuf；稳定 identity 覆盖 tenant/exporter/protocol/transport source/domain/sequence scope/sFlow agent/sub-agent，state key 按 ownership epoch fencing；校验 payload SHA、source 归属/重复、容量、plan revision、active binding 和 epoch，选择严格按 `(epoch,generation)` 且同序异 payload fail closed。复用 collect-state compacted topic的方案冻结为 `0x51 || 32B state_key` 类型化 keyspace，不新增存储组件。
    - [x] **FLOW-02E2B2B committed cut 与本地恢复**：journal v2 已补齐 tenant/exporter/registry/ownership/domain 元数据；Runner 只在 terminal ACK 后标 complete，周期任务先截取 complete cutoff、再 `WAL.Sync`，并按 WAL 顺序合并。checkpoint 保存同 owner WAL 幂等水位，每 identity/轮只增一次 generation，完整合并并排序 sFlow source state；snapshot v2 原子持久 committed/dirty，pending 与 cutoff 后完成的 journal 原子改写保留，旧 publication ACK 不能清除新 generation。v1 只允许本地恢复、不得晋升远端；已覆盖 pending 不晋升、durable complete 晋升、dirty 重启恢复、snapshot 已落盘但旧 journal 仍在时不重复递增、epoch/plan 回退、容量失败原子回滚和 source 合并。
    - [ ] **FLOW-02E2B2C Kafka 读写与切换验收（父项）**：C1 已完成；真实基础设施切换和 tombstone 状态机仍在 C2，完成前不得宣称跨节点质量 epoch 闭环。
      - [x] **FLOW-02E2B2C1 类型化读写与启动恢复**：同一 frozen high-watermark 扫描按 32B decoder key 与 `0x51 + 32B` quality key 严格分派，分别处理 tombstone、key/payload、SHA、registry/epoch、跨 partition 重复和边界内 distinct typed-key 总上限；本地 committed 与远端候选先选择，新 owner 只向没有本地 snapshot/journal 状态的 tracker identity 注入旧 owner baseline。writer 由默认关闭的 `quality_checkpoint_write_enabled`/对应 `WATCHDOG_` 环境变量控制；Kafka ACK 后才清 exact dirty，失败保留 dirty 并进入 readiness 故障。已覆盖混合 keyspace、quality tombstone、历史 tombstone 容量、decoder-only 兼容读、未知 typed key 拒绝、跨 owner baseline、本地 pending 优先、默认 reader-only、发布失败重试、延迟 ACK fencing 与 typed producer key。
      - [ ] **FLOW-02E2B2C2 真实切换与回收矩阵（父项）**：C2A 已复用 D3B3B2A 完成 quality typed-key 状态机核心；真实管理面/Kafka 闭环仍在 C2B。
        - [x] **FLOW-02E2B2C2A quality 安全回收核心**：quality checkpoint 经 checksum/identity/epoch/revision 校验后进入共享五阶段状态机；只生成 `0x51 + old_state_key` 且 value=null 的 tombstone，要求新 owner exact restore proof、新 epoch generation advance、Kafka replacement frozen-boundary 可见、发布 ACK 持久化和更晚边界 absent verification。逐阶段序列化恢复覆盖 tombstone 前后 kill 窗口。
        - [ ] **FLOW-02E2B2C2B 真实切换与故障矩阵**：专用 Kafka-null writer/receipt、exact-key compacted scanner 和 MySQL job/lease/audit repository 已完成；仍需在真实多 broker 环境先全量部署 C1 reader，再分批开启 writer，并接入 reconciler 调度与专用 principal；完成旧 owner 迟到、相同 generation 冲突、跨节点 owner、broker/ISR/ACL 故障、journal/message/恢复容量与超时矩阵。
  - [x] **FLOW-02E3 指标出口**：独立 `observability.listen` 已提供 Prometheus/VM 可抓取的 `/metrics`、`/health/live`、`/health/ready`；协议/listener/topic/result/reason/scope 均为代码固定槽位，未把 source IP/exporter/tenant/target/error text 放入 label；receiver/共享 ingress/worker decode/quarantine queue、Linux `SO_RXQ_OVFL` 与非 Linux capability、WAL bytes/oldest retained age/soft-hard/writable/fsync、plan history entries/pruned、attempt journal bytes/tracked/lifecycle、decode/template/sampling/sequence/retry/DLQ/quarantine、normalized batch、四 topic produce/latency/逐 topic恢复状态和 quality journal 已覆盖。ready 对 runner、WAL hard admission、attempt/quality/collect-state 与每个 Kafka topic fail closed，soft/80% 返回 degraded；配置、阈值、语义边界和单测/race/Linux 交叉编译证据已落文档。
- [ ] **FLOW-02F 协议/性能验收**：NetFlow v9/IPFIX template/options/乱序 fixture、真实设备报文 corpus、Kafka 故障与扩分区测试、目标容量压测与 p95 batch/record bytes 报告。
  - [x] **FLOW-02F1 合成与上游 wire corpus**：NetFlow v9 与 IPFIX 均覆盖 options sampling 学习/刷新、options data 先于 template 的 WAL 可重放、source/domain 状态隔离、checkpoint 恢复和 IPFIX Options Data Record sequence 计数；补齐 GoFlow2 已更新 store 但 producer 字段映射失败时的部分结果，Runner 会先持久化 collect-state，原报文不提前 ACK。另固定 GoFlow2 `6dee964c38ee` 上游 NetFlow v9 原始 wire fixture 的 SHA-256，直接核验 template 及 bytes/packets/IP/端口/协议/TCP flags/in-out ifIndex 映射，避免本地编解码 round-trip 掩盖兼容问题。该项不替代真实厂商 pcap corpus。
  - [ ] **FLOW-02F2 真实协议与链路验收**：采集 NetStream/NetFlow v9/IPFIX 真实设备 pcap（含 template/options 刷新、IPv4/IPv6、enterprise fields、缺模板/乱序）并固化脱敏 corpus；完成真实 Kafka 故障/扩分区与目标容量端到端报告。
  - 2026-09-04 开发机非验收 microbenchmark（Apple M2、仅 normalize+batch、不含 decode/Kafka）：1024 records 单 shard `293099 ns/op`（约 3.49M records/s/core，387849 B/op）；随机 pair 分散到多 shard `697527 ns/op`（约 1.47M records/s/core，998337 B/op）。该结果只证明算法量级并暴露多 shard allocation 成本，不能替代 FLOW-02F 的真实链路容量验收。
  - 2026-09-04 开发机非验收 microbenchmark（Apple M2、含合成 fixture allocation）：并发 NetFlow quality state `283.0 ns/datagram`（约 3.53M datagrams/s），sFlow quality state `253.3 ns/datagram`（约 3.95M datagrams/s），均为 192 B/op；只验证本阶段状态机量级，不能替代真实 exporter 分布、decode/Kafka/WAL 端到端压测。

- [ ] **设计**：冻结 GoFlow2 v3 精确 commit、四协议字段映射、exporter/domain worker ownership、collect-state 模板/sampler checkpoint 与 WAL 安全重放点、sampling mode/rate、4096 virtual shards 与版本化 physical partition map、NormalizedRecordBatch protobuf、稳定 child-batch ID/ack bitmap、records/bytes/wait 上限、Kafka TLS/ACL/retention 和 ack→WAL checkpoint 条件。
- [ ] **编码**：在 flow-collect 内实现 GoFlow2 adapter、固定 decode workers、模板/sampler store、observation 准入、采样归一、Protobuf batcher、idempotent Kafka producer、decode-DLQ、checkpoint 推进和 metrics；禁止 JSON、逐条日志/HTTP 和每包 goroutine。
- [ ] **单元测试**：已覆盖 sFlow v5、NetFlow v5/v9、IPFIX、合成 NetFlow v9/IPFIX options 学习/刷新/乱序/隔离/恢复、模板学习后字段映射失败的 state-before-DLQ、模板/采样 checkpoint 恢复、损坏/stale state、state-before-data/DLQ、已确认 state child 重试、affinity、历史 plan/partition map 旧 WAL、跨进程 replay generation、attempt torn-tail/CRC/ACK 过滤/容量压缩、毒包有界重试、DLQ payload opt-in/截断、quarantine 双限流、sampled/pre-scaled、溢出、virtual-shard/batch schema/ID/边界、ack bitmap、协议 sequence unit、gap/rate/pool/restart epoch、uint32 回绕、retry 幂等、状态容量饱和、本地 quality journal 重启/幂等/ACK 过滤/snapshot 压缩/torn-tail/CRC 损坏、committed cutoff/dirty 恢复/WAL 水位幂等/source 合并，以及 Kafka 等待与 fsync 重叠但 terminal ACK 不越过 durability barrier；仍需 NetStream/真实厂商 pcap corpus、kill -9 与多 partition/跨 owner 完整恢复矩阵。
- [ ] **集成测试**：Kafka ack 超时/断连、进程重启、WAL replay、模板 cold/warm、同 datagram 跨 session 重发、schema 旧消费者和 DLQ 恢复时，已 ack 子批次不重发、未 ack 子批次可恢复、datagram 最终不漏 record；按 `2×100G+12×10G` 实际采样配置达到批准的 records/s 与 Kafka bytes/s。
- [ ] **变更设计**：记录实际 GoFlow2 字段差异、厂商 enterprise field mapping、模板 checkpoint、protobuf compatibility、topic 分区/retention、p95 batch/record bytes 和升级条件。
- [ ] **变更测试**：验证 GoFlow2 commit 升级、mapping/schema v1→v2、Kafka 扩分区 minute-boundary map 切换、旧 WAL 仍投原 partition、证书轮换、带旧 WAL 的滚动升级和旧 normalized consumer 兼容。
- [ ] **回归测试**：sFlow 现有 IPv4/IPv6/五元组字段与新解码结果对照，确认没有协议字段退化。

### FLOW-03 `watchdog-flow-dimension-worker` 与地址段异步归类

- [ ] **设计**：冻结 normalized consumer、event-time snapshot、local/remote、primary prefix 守恒、address-set 非加和、Geo/ASN/六维、有界 shard/spill、lag 软硬水位、逐级背压和 offset 条件。
- [ ] **编码**：实现 dimension worker、snapshot loader/LPM/selector、方向、地址段/set、Geo/ASN/ISP、业务/六维、分片汇聚、batch manifest 和 normalized commit。
- [ ] **单元测试**：覆盖 event-time 选版、重叠 CIDR、IPv4/IPv6、`_unassigned`、多 set、max expansion、四种方向、六维/unknown/HMT 和重复 record。
- [ ] **集成测试**：dimension worker/CH 中断先形成 normalized lag；Kafka 生产中断验证 flow-collect WAL 保护；恢复后按原 snapshot 重放，primary prefix 守恒且 address set 标记非加和。
- [ ] **变更设计**：记录地址库格式、snapshot bundle、admin code/ISP/ASN 缺失、set 扩张上限、spill 和回算窗口。
- [ ] **变更测试**：验证 snapshot/Geo/classification 版本切换、override merge/delete、旧 normalized replay、历史版本字典和 backfill。
- [ ] **回归测试**：现有 address sets/prefixes CRUD、local/business labels、最长前缀和 SNMP target/port 映射不受影响。

### FLOW-04 ClickHouse 聚合、VM 管线指标与对账

- [ ] **设计**：冻结六张 CH 表/MV（单一 enriched base + 五张携带 `ingest_batch_id` 的可重建派生）、生产 Replicated 变体、MV error isolation/互斥 rollup 模式、TTL、normalized offset dedup、分钟聚合、派生 checkpoint/divergence/repair、VM 仅保存 SNMP/系统/pipeline 指标及可选对账；Flow recording cache 默认关闭。
- [ ] **编码**：实现 CH migration/query provider、分片聚合、单一 base sink、MV error isolation 或互斥 rollup loop、`(ingest_batch_id,target)` checkpoint/repair、offset manifest、低基数 pipeline metrics 和 reconciliation service；基线不实现 Flow 业务双写 VM。
- [ ] **单元测试**：覆盖分片/迟到/spill/backpressure、category=pair、prefix=base 守恒、set 非加和、P95、counter reset/wrap 和 LAG 映射。
- [ ] **集成测试**：真实 CH 执行六表/MV/base single sink/TTL/重复 batch；删除派生分区后从 base 重建且结果一致；CH 故障形成 lag/恢复，VM 故障只影响 SNMP/系统/pipeline 监控；无 counter 时 flow 仍可查询。
- [ ] **变更设计**：记录 CH 版本/cluster/Keeper、dedup window、TTL/容量和允许的对账偏差；只有 CH 常用查询压测不达标时，另提 ADR 评估 VM recording cache 的收益、基数、一致性和回填成本。
- [ ] **变更测试**：验证 CH schema/MV/TTL 变更、重复 offset replay、VM pipeline metric 兼容、备份恢复和 rolling migration；若启用 recording cache，另测删除/全量重建和 CH 对值。
- [ ] **回归测试**：现有 VM 指标、SNMP 计费/P95、无 SNMP 场景和 billing 原始 counter 均不被 flow 覆盖。

### FLOW-05 Flow 查询、总览和分析图表

- [ ] **设计**：冻结 flow datasets、地址段/set additive 语义、API、六类总览、分析图表、开通/健康设置页、value layer、limits、颜色和空态。
- [ ] **编码**：实现 CH Flow provider、summary/series/address-dimensions/status/business matrix/health API、`/flow`、`/flow/charts` 和 `/settings/flow` 开通向导/健康/版本状态；SNMP/system 继续使用现有 VM provider。
- [ ] **单元测试**：覆盖 primary-prefix/address-set group、role/version/additive、filter/share/difference、stats/completeness、RBAC、value layer 和上限。
- [ ] **集成测试**：从开通向导、exporter/地址库/snapshot 发布到 raw datagram/WAL/normalized/page，验证总量、prefix/set、六类、业务矩阵、P95、导出、健康降级和浏览器不直连存储。
- [ ] **变更设计**：记录截图“差值”、时间档、高峰、图表交互、响应兼容和 UI 路由变化。
- [ ] **变更测试**：验证 dataset/API/schema/UI 配置升级、旧 `/api/v1/flow` 客户端、保存图表和导出 snapshot。
- [ ] **回归测试**：全站导航、tenant 切换、权限、旧 aggregate charts、格式化和响应式布局通过。

- [ ] **Flow P1 出口**：72h 连续采集稳定；flow-collect/WAL/Kafka/normalized/dimension/CH base/derived 故障可恢复；prefix 守恒、set 非加和、六维和采样 fixture 通过；容量达到批准场景的 2×持续/3×突发（1:1000、最小 64B Ethernet frame/84B on-wire、双向时约 1.90M/2.86M records/s）；N+1 切换、备份恢复和安全验收通过；无 SNMP 仍可完整分析。

---

## 8. Flow P2：多维明细、IP、归属修正与导出

### FLOW-06 Endpoint、Geo Override 与 Reconciliation

- [ ] **设计**：冻结源/目的 endpoint、Top/CIDR、详情、Geo override、自动识别、对账窗口/方向和 CSV 数据字典（XLSX 属 FLOW-07 交付，列字典需在本包一并冻结并前向兼容）。
- [ ] **编码**：实现 endpoint/Geo/对账 API、IP 列表/详情、归属修正 UI、按省/机构分组和平台导出 provider。
- [ ] **单元测试**：覆盖 Top/cursor/CIDR、src/dst side、override label merge、审计、ifIn/ifOut 映射、无 counter 和三层导出权限。
- [ ] **集成测试**：端到端验证 Top 行与 sparkline/详情汇总一致、修正 60 秒内影响新桶、导出与在线查询一致。
- [ ] **变更设计**：记录 IP 隐私、Geo 修正权限、counter 来源、LAG 口径、导出列和历史回算边界。
- [ ] **变更测试**：验证 override/version 变更、endpoint TTL、导出格式升级、旧地址标签和 reconciliation 配置迁移。
- [ ] **回归测试**：address prefixes/sets、target/port、SNMP counter、平台 export 和 raw/supplier/customer 结果回归通过。

- [ ] **Flow P2 出口**：Top/详情/汇总误差为 0；Geo 修正不覆盖 local/business；对账可选；三层查询/导出权限和版本一致。

---

## 9. Flow P3：境外与 VPN 风险

### FLOW-07 境外专题与 VPN 规则/评分

- [ ] **设计**：冻结境外 KPI；VPN `score/probe_trigger/allow/suppress` rule JSON Schema；src/dst 境内外、direction/port/transport、duration/fanout/periodic/multi-transport、balanced/unidirectional ratio；versioned prefix/address-set/ASN/org/route-class 情报；family score cap；heuristic/corroborated/confirmed_protocol/inconclusive 与人工 disposition；probe agent 三层配置、静态 analyzer/安全 profile contract、capability/plan/job/result schema、passive/active scope/审批/权限/quota/kill switch、attempt/spool/evidence/TTL 和 XLSX。
- [ ] **编码**：实现 overseas/vpn CH provider、canonical peer conversation query、规则 CRUD/preview/version、情报 selector、评分/finding worker、verdict/disposition 正交处置；实现薄 probe agent、静态 analyzer registry、builtin handshake、可选 nDPI adapter、plan 原子应用/LKG、prepared→network_started attempt checkpoint、签名有界 result spool/evidence merge；完成专题 UI、agent 能力/配置 diff/rollout、probe queue/timeline、API 和告警，不新增 VPN topic/事实库。
- [ ] **单元测试**：覆盖境内↔境外/境内↔境内、port side、TCP/UDP、UDP443/TCP443 candidate、长连接可信度、fanout、周期、多 transport、balanced/asymmetric/unidirectional、最小流量/完整率、情报过期、allow/suppress 优先级、family cap、verdict 与 disposition 正交；覆盖 analyzer descriptor/profile 未知 key、三层交集、plan 签名/expiry/LKG、SOCKS/CONNECT/WebSocket 握手、TLS-VPN 指纹、Trojan/SS 无凭据不确认、普通业务误报、spool 边界和 crash 后主动 attempt 不自动重放。
- [ ] **集成测试**：真实容量 fixture 下 KPI/趋势/分布/导出一致，24h 评分按时完成；`dispose_finding` 与 probe/read-sensitive 权限隔离，人工 disposition/annotation 不改变 verdict/evidence；passive 证明零主动连接；active 验证本机∩plan∩job scope、审批、DNS/IP pinning、rate/concurrency/timeout/cooldown/cancel/双 kill switch；执行 agent canary/rollback、API/结果库中断/spool 恢复和进程崩溃，job/attempt 幂等且不拖慢在线查询/收数。
- [ ] **变更设计**：记录港澳台口径、ratio/score 阈值、情报 owner/source/expiry、确认协议定义、合规文案、probe network vantage/本机边界、analyzer/profile/API/result 兼容矩阵、制品签名/SBOM、sidecar 例外、证据脱敏/IP 保留和 nDPI 契约。
- [ ] **变更测试**：验证 rule/intelligence/finding schema 迁移、agent/plan/job/result 新旧矩阵、analyzer/profile 升级/降级、capability drift、scope 收缩、kill switch、canary rollback、旧 spool/job/finding 和 XLSX 兼容；旧 heuristic 不被新证据静默重写。
- [ ] **回归测试**：六维总览、endpoint、query/export、权限和未部署 probe/nDPI 时的主管线与 flow-only heuristic 不变；active probe 本机和服务端均默认关闭，升级/plan rollback 不能自动开启或扩大 scope。

- [ ] **Flow P3 出口**：所有 finding 可解释且带版本；普通 443/单向合法业务不显示确认协议；只有协议特异握手可 confirmed_protocol；passive/active 边界、授权 scope、kill switch、敏感权限和审计通过；KPI 与底层事实一致；评分/probe 容量和租户公平性通过。

---

## 10. Flow P4：运营、容量、HA 与恢复

### FLOW-08 跨机房、历史回算、长期 Rollup 与 analyzer 升级

- [ ] **设计**：在 P1 单机房 N+1、WAL/Kafka/CH 备份恢复基线之上，冻结跨机房 RPO/RTO、normalized 复制、维度回算 run/version、长期 rollup 和 nDPI/其他 analyzer 的 descriptor/schema/制品/弃用升级；不改变 P1 normalized/base 或 P3 probe job/result 契约。
- [ ] **编码**：实现跨机房切换工具、受控回算/激活、partition/consumer 扩缩容、长期 rollup/归档，以及兼容 P3 契约、带签名/SBOM/canary/rollback/deprecation 的可选 analyzer/profile 升级。
- [ ] **单元测试**：覆盖回算 job 状态机、active range 不重叠、checkpoint/幂等/暂停/激活/退役、版本选择、rollup 边界、analyzer/profile/result schema 兼容和 nDPI 标签缺失。
- [ ] **集成测试**：执行 broker/flow-collect/dimension/CH/VM/控制面故障注入、跨机房恢复、历史回算激活和容量扩缩容；P1 单机房 HA 用例必须继续通过。
- [ ] **变更设计**：记录跨机房复制、回算 run 激活、rollup/归档和 nDPI 契约的实际偏差；P1 分区/WAL/retention/SLO 不在本阶段首次冻结。
- [ ] **变更测试**：验证在线扩分区/扩 consumer、Geo/classification 回算、rollup/TTL、灾备配置和版本升级。
- [ ] **回归测试**：HA/回算/nDPI 功能关闭时主管线不变；启用后不重复计量、不改变 raw 历史事实。

- [ ] **Flow P4 出口**：跨机房 RPO/RTO、历史回算和长期 rollup 验收通过；若本版本包含 analyzer 升级，其兼容性与误报集同时通过；2×峰值持续、3×突发及 P1 N+1/恢复基线仍通过；备份可重建配置/schema/base/派生/Geo 版本。

---

## 11. 发布前全量回归与验收

### RELEASE-01 版本发布

- [ ] **设计**：冻结 release scope、兼容矩阵、feature flag、数据迁移窗口、观测指标、回滚触发条件和运维手册。
- [ ] **编码**：完成版本号、配置示例、migration bundle、dashboard/alert、升级/回滚工具和 release notes。
- [ ] **单元测试**：全仓单测、race/static checks、前端组件测试和 schema contract tests 全部通过。
- [ ] **集成测试**：执行 API/UI、MySQL/Kafka/CH/VM、flow-collect/WAL/dimension、SNMP 可选对账的完整环境测试。
- [ ] **变更设计**：三份设计文档、ADR、DDL、API 表、配置、tasklist 与实际版本逐项对齐并完成最终评审。
- [ ] **变更测试**：从支持的每个旧版本升级并验证 forward-fix/回滚；检查旧 API、旧 agent、旧图表、旧导出和旧配置兼容。
- [ ] **回归测试**：执行用户/RBAC、agent、target、network/SNMP、metrics、graphs、billing、exports、三层修正及 Flow P1–P4 全量回归。
- [ ] 安全测试通过：跨租户、凭据轮换/吊销、Kafka/CH ACL、浏览器不直连存储、IP/导出审计和敏感日志检查。
- [ ] 性能测试通过：按端口 PPS/采样方向/采样率推导并用 pcap 验证 datagrams/s 与 records/s；kernel drops、WAL、normalized lag、flow-collect/dimension 吞吐、Kafka bytes/s、address-set expansion、CH base/派生、查询和导出达到 SLO。
- [ ] 数据正确性抽验通过：raw WAL/normalized/base 的 ID 可追溯，三层可复现、primary-prefix/六维守恒、address-set 非加和、WAL/Kafka/offset 重放不重计、派生可重建、SNMP 不参与 AS/Geo 分摊。
- [ ] 发布审批完成：产品、研发、测试、DBA/数据平台、网络、运维、安全和业务口径负责人签字。
- [ ] 上线后观察窗口完成，未触发回滚条件；遗留事项已进入下一迭代而非口头保留。

## 12. 阻断项与决策记录

- [ ] “差值”口径已确认并记录。
- [ ] 港澳台分类口径已确认并记录；`flow-geo-v1` 中港澳台统一为 `country=CN` + `71/81/82` 前缀 admin_code 的表示法已随导出器冻结。
- [ ] 设备型号/固件/协议/字段/采样语义已确认并保存 fixture。
- [ ] flow/counter 偏差、完整率和 unknown 阈值已冻结。
- [ ] 地址库生产打包、发布和负责人已确认。
- [ ] 地址段/address set 主维度、非加和口径、snapshot 保留、最大扩张和历史回算窗口已确认。
- [ ] Kafka 产品/版本、normalized/collect-state/DLQ/quarantine/checkpoint topics、分区与 retention、flow-collect WAL/RPO 和灾备目标已冻结。
- [ ] ClickHouse 六表、single-base/derived 契约、cluster、TTL、ingest batch dedup、重建、容量和备份责任已冻结。
- [ ] supplier/customer 合同口径、审批人和是否允许显式级联已确认。
- [ ] VPN ratio/score/fanout/periodicity/确认协议口径、allow/suppress、机器 verdict 与人工 disposition、情报 owner/source/expiry、普通业务误报集、合规文案、IP/握手证据保留和 DPI/TAP 授权已确认。
- [ ] passive observe 的镜像/TAP/BPF/snaplen 与零主动连接证据已确认；active handshake 的授权 CIDR/端口、专用 egress、审批人、`probe_active` 权限、DNS/IP pinning、quota/cooldown/kill switch 和禁止行为已确认，且默认关闭。
- [ ] probe agent 本机硬上限、plan schema/签名/expiry/LKG、analyzer/profile/result catalog、制品签名/SBOM、兼容窗口、canary/rollback、spool 容量和运维负责人已冻结。

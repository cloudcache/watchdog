# Watchdog 平台化与 Flow 模块实施 Tasklist

本清单把 [平台化与可插拔模块架构](watchdog-platform-module-architecture.md)、[流向需求与选型](flow-direction-requirements.md) 和 [Flow 详细设计](flow-module-design.md) 拆成可勾选工作包。所有任务初始为未完成；勾选时应在同一行或关联 issue/PR 中补充负责人和证据链接。

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

## 2. 阶段依赖与总览

- [ ] `Platform P0`：生产身份/API 与 migration 基线完成。
- [ ] `Platform P1`：module/resource/dataset 与 collector/target 基座完成。
- [ ] `Platform P2`：统一查询、图表和导出完成。
- [ ] `Platform P3`：raw/supplier/customer 三层修正完成。
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

- [ ] **Platform P1 出口**：模块可独立启停；旧 agent 无中断迁移；同一 collector 可绑定多个资源；target CRUD、dimension publish 和权限继承通过。

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

- [ ] **设计**：冻结 listener/source plan、exporter affinity、socket/queue 模型、raw WAL segment/header/checksum/group-commit/checkpoint、磁盘软硬水位、RPO、quarantine、凭据和按端口 PPS/采样率/方向的容量公式。
- [ ] **编码**：实现独立 flow-collect、enrollment/heartbeat/plan、`SO_REUSEPORT` listener、source admission、确定性 datagram ID、append-only WAL、启动恢复、segment 回收、磁盘保护和低基数 metrics。
- [ ] **单元测试**：覆盖 listener/source/tenant 准入、datagram ID、WAL append/fsync/checksum/截断、进程崩溃恢复、连续 checkpoint、segment 回收、磁盘 soft/hard limit、quarantine 限速和敏感 payload 策略。
- [ ] **集成测试**：真实 UDP/文件系统故障下注入 kill -9、部分写、磁盘满、控制面中断和滚动升级；确认已 fsync datagram 可恢复、未确认 segment 不覆盖、未知来源不进正式 topic，并记录 datagrams/s、kernel drops、WAL MB/s/fsync P99、CPU/RSS。
- [ ] **变更设计**：记录实际文件系统/磁盘型号、WAL 格式版本、fsync/RPO、容量小时数、VIP/affinity、部署凭据和旧 collector 端口切换方案。
- [ ] **变更测试**：验证 WAL v1→v2、带未确认 segment 的滚动升级、证书/plan 轮换、collector ID 变化、磁盘扩容、forward-fix 和旧版本拒绝新格式。
- [ ] **回归测试**：旧 collector 停用/端口切换过程不双监听、不重复采集；agent/target 管理和其他采集器不受影响。

### FLOW-02 GoFlow2 解码、采样归一与 normalized Kafka 契约

- [ ] **设计**：冻结 GoFlow2 v3 精确 commit、四协议字段映射、exporter/domain worker ownership、collect-state 模板/sampler checkpoint 与 WAL 安全重放点、sampling mode/rate、4096 virtual shards 与版本化 physical partition map、NormalizedRecordBatch protobuf、稳定 child-batch ID/ack bitmap、records/bytes/wait 上限、Kafka TLS/ACL/retention 和 ack→WAL checkpoint 条件。
- [ ] **编码**：在 flow-collect 内实现 GoFlow2 adapter、固定 decode workers、模板/sampler store、observation 准入、采样归一、Protobuf batcher、idempotent Kafka producer、decode-DLQ、checkpoint 推进和 metrics；禁止 JSON、逐条日志/HTTP 和每包 goroutine。
- [ ] **单元测试**：覆盖 sFlow v5、NetFlow v5/v9、IPFIX/NetStream fixture，模板缺失/乱序/刷新、sampling option、sample pool/sequence/rate change、sampled/pre-scaled、溢出、virtual-shard/batch schema/ID/边界、ack bitmap 和一 datagram 多 record/多 partition 可恢复性。
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

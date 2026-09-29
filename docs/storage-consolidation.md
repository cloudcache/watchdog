# 存储与管理面收敛详细设计

> **已归档（2026-09-28）**：ADR-SC-001 已归档，被 `docs/watchdog-kiss-architecture.md` 取代：PocketBase、VictoriaMetrics、tenant 均已删除（`internal/hub`、`internal/site`、`install/init.sql`、`deploy/migration/mysql` 不存在），MySQL 迁移在 `deploy/schema/mysql/0001`–`0050`；Flow 派生层全部在 ClickHouse（1m/5m/1h/1d），没有 VM recording cache；collector/worker 以全局共享 token 注册并上报运行摘要。

> **历史文档 / 已被取代：** 2026-09-08 已冻结 [Watchdog KISS 目标架构](watchdog-kiss-architecture.md)：删除 PocketBase、多租户和 VictoriaMetrics，只保留 MySQL + ClickHouse。本文关于“PB 认证内核”和“VM/CH 不合并”的结论不再实施，仅保留历史审计证据。

> 状态：ADR-SC-001，已评审修订；范围只覆盖 PocketBase、MySQL 与 VictoriaLogs。VictoriaMetrics 和 ClickHouse 的职责边界保持不变，见[平台架构](watchdog-platform-module-architecture.md)。平台实施见[平台重构 tasklist](platform-refactor-tasklist.md)，Flow 实施见[Flow tasklist](flow-module-tasklist.md)。

> 实施状态（2026-09-03）：STORE-00 的 VictoriaLogs 止血，以及 STORE-01 的 migration/readiness、生产路由和身份投影最小闭环已完成；PB 业务集合迁移、完整 typed client、用户/角色 CRUD 和 STORE-02–05 尚未完成。细粒度完成项及证据以 tasklist 为准。

## 1. 决策结论

采用“PocketBase 认证内核 + MySQL 唯一管理库 + VictoriaLogs 裁撤”：

1. PocketBase 只负责密码、OTP/MFA、重置邮件、OAuth、session/token 和 `users` auth collection；`role` 不再是授权权威；
2. MySQL 是 tenant、用户投影、角色权限、target、collector、配置、告警、库存、任务和审计的唯一管理面事实源；
3. VictoriaLogs 从运行时、前端、配置、部署、健康检查和测试矩阵中全部移除，不保留 `debug_victorialogs` 例外；限速诊断使用低基数指标和本地有界、脱敏 capture，不新增 Kafka DLQ 或常驻存储；
4. VictoriaMetrics 继续保存 SNMP、system、container、systemd、SMART 历史观测和管线指标；ClickHouse 继续保存 Flow 分析事实；二者不是本文要合并的管理库；
5. 生产只暴露一个 Hub HTTP 入口，但“一个进程”不等于“一个存储”。PocketBase 的 auth SQLite 仍需独立备份、恢复和升级测试。

所谓“1.5 层”只表示**一个管理数据权威 + 一个认证子系统**，不能据此降低 PocketBase auth 数据的可用性、备份和安全要求。

## 2. 代码审查结论与对原草案的校正

### 2.1 已确认的问题

| 发现 | 代码证据 | 结论 |
|---|---|---|
| 生产 `/api/v1` 原不可达 | 已由 [`platform_backend.go`](../internal/hub/platform_backend.go) 分方法挂载到生产 PB Router；[`server_production.go`](../internal/hub/server_production.go) 的 SPA fallback 保持最后注册 | 路由冲突与 API 被 SPA 吞掉的问题已由 route precedence 测试覆盖；liveness/readiness 已分别落到 `/api/v1/health/live` 与 `/api/v1/health/ready` |
| 生产身份适配原不存在 | [`identity_adapter.go`](../internal/watchdog/identity_adapter.go) 现将外部 subject 映射为 MySQL tenant/user/role/grant；[`platform_backend.go`](../internal/hub/platform_backend.go) 只接受 PB `users` token | dev 固定 admin 仍仅存在于 dev-server；生产不读取 PB `role` 授权 |
| 前端 API shim 原来丢失 PB token | [`api.ts`](../internal/site/src/lib/api.ts) 现注入 PB token，并按 PB subject 注入已选择的 tenant | 多租户 discovery/selector 已落地；服务端对缺失、歧义和越权 tenant fail closed，用户/角色 CRUD 仍待 PLAT-01 完成 |
| 双身份权威原先存在 | [`install/init.sql`](../install/init.sql) 已把 MySQL `password_hash` 改为 nullable，并增加外部 subject；migration 011 暂不 drop 旧 hash | 运行时只使用 PB/外部 IdP 凭据；旧 hash 仅处于 expand/rollback 窗口，最终 contract 仍待观察期结束 |
| VictoriaLogs 热路径原不安全 | [`sflow_collector.go`](../internal/watchdog/sflow_collector.go) 已删除逐 flow HTTP sink，旧 FlowSearch 路由/页面已删除，配置入口也已移除 | 旧 YAML 严格解码失败；旧 VLogs 环境变量显式报弃用错误；部署资产和 30 天零调用观察仍待 STORE-05 |

### 2.2 原草案中不能成立的结论

| 原结论 | 审查结果 | 修正 |
|---|---|---|
| “前端已迁移 86%（30/35）” | 当前静态检索有约 146 个 `pb.send` 调用行、约 120 行含 `/api/v1`、32 个 `pb.collection` 调用；调用次数会随格式变化，且 `pb.send` 只是传输封装，不代表数据权威 | 不再用百分比作为迁移验收；按 endpoint、collection、读/写/订阅和运行场景逐项清零 |
| “PB 14 个集合只有 6 个活着” | 前端仍直接引用 `users/user_settings/alerts/alerts_history/quiet_hours/fingerprints/smart_devices/containers/systemd_services`；后端还读写全部 system/alert 集合 | 14 个业务集合按下表逐一迁移，不能按页面是否可见判断存活 |
| “systems 域 8 个集合是僵尸，可直接删” | [`system.go`](../internal/hub/systems/system.go) 每轮事务写 `systems/system_stats/container_stats/containers/systemd_services/system_details`；[`system_manager.go`](../internal/hub/systems/system_manager.go)、[`internal/alerts`](../internal/alerts) 和 `/api/watchdog` 仍读取它们 | 它们是“遗留但在线”的运行域。必须先迁采集、连接、告警和详情路径，再停止 PB 写入 |
| “删除不是丢数据，SQLite 文件就是备份” | 活跃 SQLite 文件不是归档；没有时间范围、计数、checksum、恢复工具和保留期 | 归档必须生成 manifest、校验和并做恢复演练；生产文件副本只能作为迁移输入 |
| “SMART 字段照搬并挂 network_devices” | SMART 当前来源是 system agent，父对象是 `systems`；`network_devices` 只适用于 SNMP 网元 | 以通用 `targets` 为父资源，结构化身份/最新状态，历史观测进入 VM |

因此，推荐方向 C 保持不变，但工程量不是“只迁 6 个集合”。真正的退出条件是：旧 Agent WebSocket、system 状态、告警引擎、SMART、容器/systemd 详情和 enrollment 全部不再依赖 PB 业务集合。

## 3. 数据权威与不变量

### 3.1 权威矩阵

| 数据 | 唯一权威 | 允许的投影/缓存 | 禁止事项 |
|---|---|---|---|
| 密码、MFA/OTP、重置、OAuth、session/token | PocketBase auth 或未来外部 OIDC | MySQL `users.external_subject_id` | MySQL 保存密码 hash；PB `role` 决定业务授权 |
| tenant、用户状态、角色、权限、资源成员关系 | MySQL | 请求内 `AuthContext`、短 TTL grant cache | 从 PB `systems.users` 或 token 自报 tenant 授权 |
| target/collector desired state、配置、库存、告警规则、偏好 | MySQL | 进程缓存，必须有版本/失效 | PB 双写长期存在；JSON 无 schema |
| system/container/systemd/SMART 历史观测 | VictoriaMetrics | MySQL 可保存最新库存/健康摘要 | 把高频样本迁进 MySQL |
| Flow 分析事实 | ClickHouse | 可重建派生表、获批后少量 VM recording cache | VLogs/VM 作为 Flow 明细事实源 |
| 审计与迁移记录 | MySQL `audit_logs` + 迁移 manifest | 只读导出 | 以普通应用日志代替可核验审计 |

全局不变量：

- 同一 `(tenant_id, auth_provider, external_subject_id)` 最多一个有效 MySQL 用户投影；PB ID 与 MySQL ULID 不复用；
- 业务 API 的 tenant 只来自服务端 AuthContext，忽略或拒绝 body/query 自报 tenant；
- 所有管理写入只提交 MySQL 一次；迁移兼容写不是永久双写架构；
- desired state 与 observed state 分离：管理员配置不被 heartbeat 覆盖，heartbeat 不增加配置 ETag；
- 删除 target/collector 不级联删除 VM/CH 事实；清理必须经过 retention/purge job；
- 告警 UI 刷新方式不得影响告警检测、状态迁移和通知投递；
- PB 业务集合在销毁前连续一个观察窗口满足 read=0、write=0、subscribe=0。

### 3.2 14 个 PB 业务集合的确定去向

| PB 集合 | 当前语义 | 目标去向 | 退出前置条件 |
|---|---|---|---|
| `users` | auth + `role` | **保留 auth**；授权投影进 MySQL | 所有业务代码改读 `/api/v1/me`，PB `role` 只兼容一版后删除 |
| `user_settings` | UI 偏好 + email/webhook | `user_preferences` + `notification_channels` | GET/PUT、渠道验证/测试和告警发送均切换 |
| `alerts` | 规则与 `triggered` 状态混表 | `alert_rules` + `alert_events` | 新引擎完成规则、状态、恢复和去重 |
| `alerts_history` | 告警发生/恢复历史 | `alert_events` | 历史 backfill、分页/导出/retention 完成 |
| `quiet_hours` | 用户/系统静默 | `alert_suppressions` | 时区、跨午夜、一次性/周期规则验证完成 |
| `systems` | target、成员、连接和 observed status 混表 | `targets` + `collector_agents/bindings` + RBAC | 旧 Agent 协议适配和在线连接管理迁出 PB |
| `system_details` | 最新主机库存 | `system_target_profiles` | system agent 上报改写 MySQL |
| `system_stats` | 多分辨率主机历史 | VM；旧数据离线归档 | VM 写入/查询/完整率达标，归档恢复演练通过 |
| `containers` | 最新 container 投影 | VM 当前序列/查询；管理配置仍在 target/collector | 列表和详情不再查 PB；agent action 按 target 鉴权 |
| `container_stats` | 多分辨率 container 历史 | VM；旧数据离线归档 | 同 `system_stats` |
| `systemd_services` | 最新 service 状态 | VM 当前序列/查询 | 列表和详情不再查 PB；远程 action 按 target 鉴权 |
| `smart_devices` | 磁盘库存 + 最新健康 + attributes | `storage_devices` 最新态；VM 历史 | refresh/forget/list/detail/告警全链切换 |
| `fingerprints` | legacy agent token/fingerprint 与 system 绑定 | PLAT-03 `collector_agents/bindings` | enrollment、轮换、吊销、兼容 Agent 验证完成 |
| `universal_tokens` | 用户级永久/临时自动注册 token | 一次性短 TTL enrollment secret | 永久 universal token 停发，旧 token 轮换/撤销完成 |

PocketBase 终态“只剩 users”指只剩一个**应用业务集合**；PB 自身的 `_superusers`、migration、auth 运行表不在此计数中。

## 4. 目标运行架构

```text
浏览器
  ├─ PB auth SDK ───────────────▶ Hub /api/collections/users/*
  │                                  │ PB 校验 token/session
  └─ typed API client ──────────▶ Hub /api/v1/*
       Authorization: PB token        │ IdentityAdapter
       X-Watchdog-Tenant-ID?          ├─ MySQL user/role/grant
                                      ├─ MySQL management repositories
legacy/new agents ──────────────▶ Hub ├─ VM system/SNMP observations
flow collectors ──────────────────────└─ Kafka → CH Flow facts

VictoriaLogs：不存在于进程、配置或部署图中
```

### 4.1 Hub 启动和路由顺序

1. 启动 PB 并完成 PB migration；
2. 初始化 MySQL runtime；嵌入式 runner 在同库 advisory lock 下按版本和 checksum 执行 migration，随后 readiness 校验完整版本集；
3. 构造生产 `IdentityAdapter` 并把 watchdog Router 挂到 `/api/v1/*`；
4. 注册 `/api/watchdog/*` 兼容路由；能迁入 `/api/v1` 的管理接口不再新增在这里；
5. 最后注册静态资源和 SPA fallback；`/api/*` 未匹配必须返回 JSON 404，禁止回退为 HTML 200；
6. 启动时 MySQL 不可用或 migration 不完整时 Hub 直接 fail closed，不进入“仅 PB 可登录”的半服务状态；启动后 MySQL 失联时 `/api/v1/health/ready` 与管理 API 返回 503，且绝不回退到 PB role/collection 提供业务数据。

Hub 内嵌 runtime 不等于把 SNMP/export/flow worker 全塞进 Hub。HTTP API 可以同进程，独立 collector/worker 仍按故障域单独部署。

### 4.2 已实现的身份投影契约

- MySQL `users` 通过 migration `011_identity_projection.sql` expand 出 `auth_provider/external_subject_id`，`password_hash` 只改为 nullable，当前版本不 drop；新装 `install/init.sql` 与 expand 后结构一致；
- 首批只支持 `auth_provider=pocketbase`。PB auth record ID 是 external subject，MySQL user ID 继续使用管理面稳定 ID，两者不复用；
- 不做 email 自动绑定/JIT。管理员用 `watchdog-identity-link` 显式把 PB subject 绑定到一个 active tenant/user，避免“邮箱相同即接管授权”；
- 单 tenant subject 可省略 `X-Watchdog-Tenant-ID`；多 tenant subject 必须显式选择；header 只用于选择，不能创造 membership；
- `/api/v1/me/tenants` 使用独立的 tenant-discovery adapter：只校验 PB subject 并返回其 active memberships，不选择 tenant、不加载 role/grant，也不能访问任何 tenant-scoped 资源；这避免首次多租户登录出现“必须先选 tenant 才能查询 tenant”的循环依赖；
- `users.status` 或 `tenants.status` 非 active 返回 403；投影/RBAC 查询失败返回 503；无效 PB token 返回 401；未投影 subject 返回 403；
- `IsAdmin` 只由当前 tenant 的 MySQL `admin` role 产生，PB `role` 不参与业务授权；普通用户合并 user/role grants；
- 未知 `/api/v1/*` 由 watchdog Router 返回 JSON 404。生产 PB Router 必须逐 HTTP method 挂载，不能用无方法通配；否则会与 `GET /{path...}` SPA route 冲突并在启动时 panic；
- migration SQL 由 [`deploy/migration/mysql`](../deploy/migration/mysql) 原位嵌入二进制。`watchdog_schema_migrations` 记录版本/文件名/SHA-256；缺版、未知新版或 checksum 漂移均使 readiness 失败，不能跳过继续服务。

### 4.3 前端 API 客户端

当前全局 monkey-patch `pb.send` 只作为一版兼容层，必须先补齐：

- `/api/v1` 请求发送 `Authorization: ${pb.authStore.token}`、`Accept: application/json` 和 `X-Request-ID`；
- 多 tenant 用户显式发送已选择的 `X-Watchdog-Tenant-ID`；
- 应用启动、公开页面和登录表单挂载不做 auth refresh 或自动 OAuth；只有受控 API 已实际发出并返回 401 时才允许至多一次 session refresh，并只重放 GET/幂等请求。非幂等写必须使用 `Idempotency-Key`，不能自动盲重试；
- 统一处理 JSON error envelope、204、AbortSignal、ETag/If-Match 和 429/503；
- 最终改为独立 typed API client，PB SDK 只用于 auth，避免继续把两种协议藏在 `pb.send` 后面。

## 5. 身份投影详细契约

### 5.1 MySQL users 两阶段迁移

扩展阶段先允许兼容旧行：

```sql
ALTER TABLE users
  ADD COLUMN auth_provider VARCHAR(32) NOT NULL DEFAULT 'pocketbase' AFTER tenant_id,
  ADD COLUMN external_subject_id VARCHAR(190) NULL AFTER auth_provider,
  MODIFY COLUMN password_hash VARCHAR(255) NULL,
  ADD UNIQUE KEY uq_users_auth_subject
    (tenant_id, auth_provider, external_subject_id);
```

完成投影 backfill、登录验证和观察窗口后再收缩：

```sql
ALTER TABLE users
  MODIFY COLUMN external_subject_id VARCHAR(190) NOT NULL,
  DROP COLUMN password_hash;
```

不允许在同一次 migration 中直接 `DROP password_hash`。升级副本必须证明所有有效用户已有 subject 映射，且不存在重复 `(tenant, provider, subject)`。

### 5.2 IdentityAdapter 请求流程

1. 由 PB server middleware 校验 token 签名、过期和 auth record；不能只 decode token payload；
2. 取 `auth_provider=pocketbase`、`external_subject_id=PB record id`；
3. 查询所有 `status=active` 的 MySQL tenant 投影；零条返回 403 `identity_not_provisioned`，不是自动授予 admin；
4. 客户端先通过仅认证的 `/api/v1/me/tenants` 取得 active memberships；一条 membership 可默认，多条时必须使用服务端校验过的 tenant 选择，缺失返回 400 `invalid_request`；该发现接口本身不产生 tenant-scoped AuthContext；
5. 加载 role IDs 和 grants，形成 request-scoped `AuthContext`；
6. MySQL 用户被禁用后，即使 PB session 仍有效也拒绝业务 API；PB 删除/禁用用户则所有业务访问立即失效；
7. PB email/name 变化通过幂等 reconciler 更新非授权投影；角色和权限只允许 MySQL 管理 API 更新。

首次安装创建用户是跨库 saga：PB user 创建成功后，以固定 bootstrap operation ID 创建 tenant、MySQL user 投影和 admin role；失败时显示“认证已创建、平台初始化未完成”，可幂等重试，禁止创建第二个 tenant/admin。普通新用户默认不 JIT 获权，必须由邀请/成员 API 建立投影。

## 6. MySQL 目标表

以下是目标字段契约。真实 migration 必须遵循 PLAT-00 的版本和 init parity 规则，并补齐既有表命名/字符集的一致性；JSON 在 API 层严格校验、规范化后写入，不能当无约束扩展口。

### 6.1 偏好与通知渠道

```sql
CREATE TABLE user_preferences (
  user_id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  schema_version SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  settings_json JSON NOT NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  KEY idx_user_preferences_tenant (tenant_id, updated_at),
  CONSTRAINT fk_user_preferences_user FOREIGN KEY (user_id)
    REFERENCES users(id) ON DELETE CASCADE,
  CONSTRAINT fk_user_preferences_tenant FOREIGN KEY (tenant_id)
    REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE notification_channels (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  user_id CHAR(26) NOT NULL,
  channel_type VARCHAR(16) NOT NULL,
  display_name VARCHAR(190) NOT NULL DEFAULT '',
  address VARCHAR(512) NULL,
  secret_ref VARCHAR(255) NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  verified_at DATETIME(3) NULL,
  last_tested_at DATETIME(3) NULL,
  last_error_code VARCHAR(64) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  deleted_at DATETIME(3) NULL,
  KEY idx_notification_channels_user (tenant_id, user_id, enabled),
  CONSTRAINT fk_notification_channels_user FOREIGN KEY (user_id)
    REFERENCES users(id) ON DELETE RESTRICT,
  CONSTRAINT fk_notification_channels_tenant FOREIGN KEY (tenant_id)
    REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (channel_type IN ('email','webhook')),
  CHECK ((channel_type='email' AND address IS NOT NULL AND secret_ref IS NULL) OR
         (channel_type='webhook' AND address IS NULL AND secret_ref IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

`settings_json` 只允许 UI 偏好（时间范围、单位、主题、语言、布局、阈值颜色）；email/webhook 从 JSON 拆出。Webhook URL/token 进入 secret store，API 只返回掩码和 `secret_ref`，审计日志不记录密文或完整 URL。

### 6.2 System 与存储设备最新库存

```sql
CREATE TABLE system_target_profiles (
  target_id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  hostname VARCHAR(255) NOT NULL DEFAULT '',
  os_family VARCHAR(64) NOT NULL DEFAULT '',
  os_name VARCHAR(190) NOT NULL DEFAULT '',
  kernel_version VARCHAR(190) NOT NULL DEFAULT '',
  cpu_model VARCHAR(255) NOT NULL DEFAULT '',
  architecture VARCHAR(64) NOT NULL DEFAULT '',
  cpu_cores INT UNSIGNED NOT NULL DEFAULT 0,
  cpu_threads INT UNSIGNED NOT NULL DEFAULT 0,
  memory_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
  container_runtime VARCHAR(32) NOT NULL DEFAULT '',
  inventory_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  observed_at DATETIME(3) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  KEY idx_system_profiles_tenant (tenant_id, observed_at),
  CONSTRAINT fk_system_profiles_target FOREIGN KEY (target_id)
    REFERENCES targets(id) ON DELETE CASCADE,
  CONSTRAINT fk_system_profiles_tenant FOREIGN KEY (tenant_id)
    REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE storage_devices (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  target_id CHAR(26) NOT NULL,
  collector_id CHAR(26) NULL,
  device_key VARCHAR(512) NOT NULL,
  device_name VARCHAR(255) NOT NULL DEFAULT '',
  model VARCHAR(255) NOT NULL DEFAULT '',
  serial_number VARCHAR(255) NOT NULL DEFAULT '',
  firmware_version VARCHAR(128) NOT NULL DEFAULT '',
  media_type VARCHAR(32) NOT NULL DEFAULT 'unknown',
  capacity_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
  smart_state VARCHAR(16) NOT NULL DEFAULT 'UNKNOWN',
  temperature_c DECIMAL(6,2) NULL,
  power_on_hours BIGINT UNSIGNED NOT NULL DEFAULT 0,
  power_cycles BIGINT UNSIGNED NOT NULL DEFAULT 0,
  attributes_json JSON NULL,
  observed_at DATETIME(3) NOT NULL,
  last_seen_at DATETIME(3) NOT NULL,
  stale_after DATETIME(3) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  deleted_at DATETIME(3) NULL,
  live_identity TINYINT GENERATED ALWAYS AS
    (IF(deleted_at IS NULL, 1, NULL)) STORED,
  UNIQUE KEY uq_storage_device_key
    (tenant_id, target_id, device_key, live_identity),
  KEY idx_storage_devices_health
    (tenant_id, smart_state, last_seen_at),
  CONSTRAINT fk_storage_devices_target FOREIGN KEY (target_id)
    REFERENCES targets(id) ON DELETE CASCADE,
  CONSTRAINT fk_storage_devices_collector FOREIGN KEY (collector_id)
    REFERENCES collector_agents(id) ON DELETE SET NULL,
  CONSTRAINT fk_storage_devices_tenant FOREIGN KEY (tenant_id)
    REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (smart_state IN ('UNKNOWN','PASSED','WARNING','FAILED','STALE'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

`device_key` 的优先级固定为 WWN/EUI → serial+model → agent 稳定设备 key；不能只用 `/dev/sda`。`attributes_json` 只保存最新、限长、规范化属性，温度/寿命等历史点写 VM。`DELETE` 表示 forget 一个 stale inventory，不代表删除磁盘；同一设备再次被观测时以新事件恢复并审计。

### 6.3 告警闭环

```sql
CREATE TABLE alert_rules (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  owner_user_id CHAR(26) NOT NULL,
  resource_type VARCHAR(64) NOT NULL,
  resource_id CHAR(26) NOT NULL,
  rule_type VARCHAR(64) NOT NULL,
  metric_key VARCHAR(190) NULL,
  comparator VARCHAR(8) NULL,
  threshold_value DECIMAL(30,8) NULL,
  for_seconds INT UNSIGNED NOT NULL DEFAULT 0,
  severity VARCHAR(16) NOT NULL DEFAULT 'warning',
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  config_json JSON NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NOT NULL,
  updated_by CHAR(26) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  deleted_at DATETIME(3) NULL,
  KEY idx_alert_rules_resource
    (tenant_id, resource_type, resource_id, enabled),
  CONSTRAINT fk_alert_rules_owner FOREIGN KEY (owner_user_id)
    REFERENCES users(id) ON DELETE RESTRICT,
  CONSTRAINT fk_alert_rules_tenant FOREIGN KEY (tenant_id)
    REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (comparator IS NULL OR comparator IN ('>','>=','<','<=','=','!=')),
  CHECK (severity IN ('info','warning','critical'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE alert_suppressions (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  owner_user_id CHAR(26) NOT NULL,
  resource_type VARCHAR(64) NULL,
  resource_id CHAR(26) NULL,
  schedule_type VARCHAR(16) NOT NULL,
  timezone VARCHAR(64) NOT NULL DEFAULT 'UTC',
  starts_at DATETIME(3) NULL,
  ends_at DATETIME(3) NULL,
  local_start TIME NULL,
  local_end TIME NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  reason VARCHAR(512) NOT NULL DEFAULT '',
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  deleted_at DATETIME(3) NULL,
  KEY idx_alert_suppressions_scope
    (tenant_id, owner_user_id, enabled),
  CONSTRAINT fk_alert_suppressions_owner FOREIGN KEY (owner_user_id)
    REFERENCES users(id) ON DELETE RESTRICT,
  CONSTRAINT fk_alert_suppressions_tenant FOREIGN KEY (tenant_id)
    REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (schedule_type IN ('one_time','daily')),
  CHECK ((resource_type IS NULL AND resource_id IS NULL) OR
         (resource_type IS NOT NULL AND resource_id IS NOT NULL)),
  CHECK ((schedule_type='one_time' AND starts_at IS NOT NULL AND ends_at>starts_at) OR
         (schedule_type='daily' AND local_start IS NOT NULL AND local_end IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE alert_rule_states (
  rule_id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  evaluator_state VARCHAR(16) NOT NULL DEFAULT 'normal',
  pending_since DATETIME(3) NULL,
  active_event_id CHAR(26) NULL,
  last_value DECIMAL(30,8) NULL,
  last_observed_at DATETIME(3) NULL,
  state_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  KEY idx_alert_rule_states_pending
    (tenant_id, evaluator_state, pending_since),
  CONSTRAINT fk_alert_rule_states_rule FOREIGN KEY (rule_id)
    REFERENCES alert_rules(id) ON DELETE CASCADE,
  CONSTRAINT fk_alert_rule_states_tenant FOREIGN KEY (tenant_id)
    REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (evaluator_state IN ('normal','pending','firing'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE alert_events (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  rule_id CHAR(26) NULL,
  owner_user_id CHAR(26) NOT NULL,
  resource_type VARCHAR(64) NOT NULL,
  resource_id CHAR(26) NOT NULL,
  event_type VARCHAR(64) NOT NULL,
  severity VARCHAR(16) NOT NULL,
  state VARCHAR(16) NOT NULL DEFAULT 'open',
  dedup_key CHAR(64) NOT NULL,
  active_key CHAR(64) NULL,
  summary VARCHAR(512) NOT NULL,
  observed_value DECIMAL(30,8) NULL,
  unit VARCHAR(32) NOT NULL DEFAULT '',
  evidence_json JSON NULL,
  opened_at DATETIME(3) NOT NULL,
  last_observed_at DATETIME(3) NOT NULL,
  acknowledged_at DATETIME(3) NULL,
  acknowledged_by CHAR(26) NULL,
  resolved_at DATETIME(3) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  deleted_at DATETIME(3) NULL,
  UNIQUE KEY uq_alert_event_active (tenant_id, active_key),
  KEY idx_alert_events_list
    (tenant_id, owner_user_id, state, updated_at),
  KEY idx_alert_events_resource
    (tenant_id, resource_type, resource_id, opened_at),
  CONSTRAINT fk_alert_events_rule FOREIGN KEY (rule_id)
    REFERENCES alert_rules(id) ON DELETE SET NULL,
  CONSTRAINT fk_alert_events_owner FOREIGN KEY (owner_user_id)
    REFERENCES users(id) ON DELETE RESTRICT,
  CONSTRAINT fk_alert_events_ack_user FOREIGN KEY (acknowledged_by)
    REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT fk_alert_events_tenant FOREIGN KEY (tenant_id)
    REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (state IN ('open','acknowledged','resolved')),
  CHECK (severity IN ('info','warning','critical'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE alert_deliveries (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  event_id CHAR(26) NOT NULL,
  channel_id CHAR(26) NOT NULL,
  idempotency_key CHAR(64) NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'pending',
  attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
  next_attempt_at DATETIME(3) NULL,
  lease_owner VARCHAR(190) NULL,
  lease_expires_at DATETIME(3) NULL,
  last_attempt_at DATETIME(3) NULL,
  delivered_at DATETIME(3) NULL,
  last_error_code VARCHAR(64) NULL,
  last_error_detail VARCHAR(1024) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_alert_delivery_idempotency (idempotency_key),
  KEY idx_alert_delivery_due (status, next_attempt_at),
  CONSTRAINT fk_alert_delivery_event FOREIGN KEY (event_id)
    REFERENCES alert_events(id) ON DELETE CASCADE,
  CONSTRAINT fk_alert_delivery_channel FOREIGN KEY (channel_id)
    REFERENCES notification_channels(id) ON DELETE RESTRICT,
  CONSTRAINT fk_alert_delivery_tenant FOREIGN KEY (tenant_id)
    REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (status IN ('pending','sending','delivered','retry','failed','suppressed'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

`alert_rules` 是 desired configuration，`alert_rule_states` 是可恢复的 evaluator observed state，`alert_events` 是不可回写的机器事实，`alert_deliveries` 是副作用生命周期。规则状态更新、open event 和首批 delivery 必须在同一 MySQL transaction 中提交；worker 以 lease 抢占并发送稳定 `idempotency_key`。`active_key=dedup_key` 只在 open/acknowledged 时存在，resolved 后置 NULL，从而保证同一活动异常只有一个事件。外部 email/webhook 在响应超时后不可能承诺绝对 exactly-once，必须标记 `delivery_outcome_unknown`、携带稳定 event/idempotency header，并由策略决定重试。一次性 quiet period 过期由清理 job 软删除；daily 必须保存 IANA timezone 并覆盖 DST、跨午夜测试。

## 7. API 契约

所有 list 使用 cursor pagination、稳定二级排序 `updated_at,id`、字段/filter/sort allowlist；所有 create/action 接受 `Idempotency-Key`；PATCH/PUT/DELETE 使用 `If-Match`；响应带 `ETag`、`X-Request-ID`，时间统一 RFC3339 UTC。客户端提交未知字段返回 400，不静默忽略。

| 资源 | API | 权限与场景 |
|---|---|---|
| 当前身份 | `GET /api/v1/me` | 返回 MySQL user/tenant/roles/grants/is_admin；前端不得读 PB role |
| tenant 选择 | `GET /api/v1/me/tenants` | 只列当前 subject 已配置且 active 的 membership |
| 偏好 | `GET/PUT /api/v1/me/preferences` | 只能读写自己；typed schema；PUT 全量、If-Match 防覆盖 |
| 通知渠道 | `GET/POST /api/v1/me/notification-channels` | 只管理自己；webhook secret 永不回显 |
| 通知渠道 | `GET/PATCH/DELETE /api/v1/me/notification-channels/{id}` | soft delete；PATCH 不允许更换 owner/tenant |
| 渠道验证 | `POST /api/v1/me/notification-channels/{id}/actions/test` | 有界超时/频率、SSRF 防护、审计、幂等 |
| 告警规则 | `GET/POST /api/v1/alert-rules` | target view/configure + owner 约束；禁止 body 自报 tenant |
| 告警规则 | `GET/PATCH/DELETE /api/v1/alert-rules/{id}` | soft delete；删除不删历史事件 |
| 静默 | `GET/POST /api/v1/alert-suppressions` | 校验 scope、timezone、重叠和最大持续时间 |
| 静默 | `GET/PATCH/DELETE /api/v1/alert-suppressions/{id}` | own/admin；If-Match |
| 活动/历史 | `GET /api/v1/alert-events?state=&resource_type=&resource_id=&updated_after=&cursor=` | 查询同时覆盖活动和历史；返回 `next_cursor/as_of` |
| 事件详情 | `GET /api/v1/alert-events/{id}` | resource view + owner/admin；敏感 evidence 另需 `read_sensitive` |
| 确认 | `POST /api/v1/alert-events/{id}/actions/acknowledge` | 状态机动作，重复请求幂等 |
| 事件隐藏 | `DELETE /api/v1/alert-events/{id}` | 只软隐藏 owner 视图；机器事实按 retention 保留 |
| 事件导出 | `POST /api/v1/alert-events/actions/export` | 复用异步 export job；导出结果与查询同一过滤/RBAC |
| system 库存 | `GET /api/v1/targets/{id}/system-profile` | target view；observed inventory 只读 |
| 磁盘列表/详情 | `GET /api/v1/storage-devices`、`GET /api/v1/storage-devices/{id}` | target view；cursor/filter/sort |
| SMART 刷新 | `POST /api/v1/targets/{id}/actions/refresh-storage` | target operate；创建有状态 job，不阻塞 HTTP |
| 忘记设备 | `POST /api/v1/storage-devices/{id}/actions/forget` | 仅 stale/offline，不能伪造健康状态 |
| 迁移状态 | `GET /api/v1/admin/storage-consolidation` | admin；显示每域 source/target/count/checksum/cutover/read-write 计数 |

告警活动视图使用条件轮询：前台 15 秒、页面隐藏 60 秒并允许暂停，恢复可见立即刷新；携带 `If-None-Match` 或 `updated_after`，无变化返回 304。服务端检测、去重、恢复、抑制和 delivery worker 始终事件驱动，浏览器断网不影响告警。

## 8. 关键功能场景

### 8.1 登录与失效

- 未访问受控页面/数据且用户未主动登录时：不发送密码、OTP、OAuth 或 auth refresh 请求；单一 OAuth provider 也只显示可点击入口，不自动跳转；
- 已配置用户：PB 登录→token 转发→IdentityAdapter 加载 MySQL projection→`/api/v1/me`；
- PB 有用户但 MySQL 无 projection：403，不自动创建权限；邀请/bootstrap operation 可幂等恢复；
- 多 tenant：用户显式选择，切换后清空 tenant-scoped cache/query；
- MySQL down：业务 API 503，不使用 PB role 降级；恢复后无需重新登录；
- 用户禁用：MySQL 状态即时阻断业务 API；PB session 是否清理是额外动作，不影响 fail-closed；
- PB token 过期：仅由已经发生的受控资源请求触发至多一次 refresh，只重放安全/幂等请求；公开页面和登录页不得后台续期。

### 8.2 偏好和通知

- 首次 GET 没有行时返回默认 DTO + ETag，不在读请求中隐式写库；首次 PUT 才创建；
- 旧 JSON 只迁 allowlist 字段，未知键进入迁移报告，不原样扩散；
- email 地址规范化后保存；webhook 先做 scheme/域/IP SSRF 校验，再保存 secret reference；
- test 是审计副作用，有超时、速率限制和不可重放的 secret handling。

### 8.3 告警生命周期

```text
rule enabled → evaluator observes breach → pending(for_seconds)
  → open event → suppression decision → delivery jobs
  → acknowledged（仍可持续观测）→ resolved
```

- rule 修改产生新 `row_version`；已有 open event 保留触发时 evidence，不被新阈值篡改；
- Hub 重启后 pending timer 必须从 `alert_rule_states.pending_since` 和最近观测恢复，不能只存在内存；
- 同一 dedup key 并发触发只创建一个 active event；通知按 event/channel 幂等；
- suppression 只影响 delivery，不应删除 event；UI 明确显示 `suppressed`；
- SMART 只对已知状态从 PASSED/ WARNING 向更坏状态触发，UNKNOWN/STALE 单独按规则处理；
- 事件删除只是 owner 视图 soft-hide；审计和 retention 不被普通用户硬删。

### 8.4 Legacy Agent 与 system 数据

当前 `/api/watchdog/agent-connect` 仍以 PB fingerprint/system 记录完成注册、连接和采集。收敛不能简单关停：

1. 保留一个明确版本窗口的 legacy protocol adapter；
2. token/fingerprint 映射到 collector identity，system 映射到 `targets`，成员关系映射到 permission/binding；
3. system/container/systemd 样本直接标准化写 VM；details/storage latest inventory 写 MySQL；
4. 在线连接放内存 connection manager，以 collector/target ID 索引，不把连接状态当数据库锁；
5. 容器日志、systemd info、SMART refresh 通过 target→binding→live connection 调用，并复用 target RBAC；
6. 旧 Agent 覆盖率、版本和最后连接时间达到退出门后才移除 adapter。

## 9. 迁移、切换与回滚

采用 expand → backfill → shadow compare → cutover → observe → contract；不同域独立开关，禁止“一键删除 13 个集合”。

### 9.1 阶段

| 阶段 | 动作 | 出口门 |
|---|---|---|
| S0 安全止血 | 隐藏/禁用 FlowSearch；停止逐 flow VLogs HTTP 写或把 collector 停在受控环境；封禁 9428 对浏览器网络暴露 | VLogs 外部请求=0，现有 sFlow 聚合是否仍被使用已登记 |
| S1 生产闭环 | Hub 挂 `/api/v1`、JSON 404、PB IdentityAdapter、前端 token、MySQL readiness | 真实 PB login 调所有管理 API；401/403/404/503 正确 |
| S2 身份与低频域扩展 | users 两阶段字段、偏好/渠道/suppression 表和 API；生成 source manifest | count/checksum/unknown-field 报告为 0 或获批 |
| S3 system 运行域 | legacy adapter、target/collector 映射、system/container/systemd→VM、profile/storage→MySQL | 对比 PB/VM 同窗口完整率和状态；断连/重连/Hub 重启通过 |
| S4 告警域 | 规则/event/delivery engine、前端条件轮询、历史迁移 | 同 fixture 新旧触发/恢复/静默/通知结果一致；重复投递=0 |
| S5 enrollment | PLAT-03 collector enrollment/binding 替代 fingerprint/universal token | 旧凭据轮换/撤销，旧 Agent 兼容和恢复演练通过 |
| S6 收缩 | PB 业务集合只读观察；移除 hooks/cron/rules/schema；删除 VLogs config/deploy/code | 连续至少 30 天 PB 业务 read/write/subscribe=0；备份恢复通过 |

### 9.2 Backfill 规则

- 每次运行写不可变 manifest：source DB hash、collection、记录数、最早/最晚时间、逐页 checksum、目标表/版本、开始结束时间、代码版本和操作人；
- legacy PB ID 保存到迁移 manifest/audit detail，不复用为 MySQL 主键；重复执行按 `(source_collection, source_id, source_updated)` 幂等；
- preferences：PB identity 映射到每个 tenant user projection，JSON allowlist 转换；
- alerts：`alerts` 转 rule；`triggered=true` 生成 open event；history 转 resolved/open event，保留原时间；
- quiet hours：一次性转 UTC；daily 从用户时区解释。无法确定时区的记录阻断自动迁移；
- SMART：以 target 映射和 stable device key upsert；属性超限/非法状态进入 quarantine 报告；
- system stats/container stats：**不导入 MySQL/VM 在线库**，只进入离线归档；切换后新样本直接写 VM。

### 9.3 回滚边界

- schema expand 和 backfill 可重复、可前滚；不可在发布脚本中自动 drop；
- 每个域切写前可回滚到 PB；切写后旧二进制若不认识 MySQL，不能宣称“无损一键回滚”；必须冻结写后执行已测试的 reverse export，或 forward-fix；
- PB 集合至少保留只读一个兼容版本且不少于 30 天；观察期内禁止 PB Admin UI 修改；
- `password_hash` 删除、PB collection drop、VLogs 数据销毁是不可逆 contract 动作，必须独立变更单和最新备份恢复证据；
- 生产切换失败不得让同一域同时接受 PB/MySQL 双边写入。短时兼容双写若不可避免，必须以 MySQL transaction/outbox 驱动并有差异告警，不能 best-effort。

## 10. 归档与数据生命周期

旧 `system_stats/container_stats` 的决策冻结为**一次性离线归档，不在线双读**：

1. 导出压缩 NDJSON 或 Parquet，包含 schema version、collection、system→target mapping、UTC 时间范围、记录数、未压缩/压缩 SHA-256；
2. manifest 与归档分开保存，至少两份、不同故障域；
3. 在隔离环境恢复并抽样比对总数、首尾时间和 JSON 解码；
4. 默认保留 90 天；若合规/业务无长期要求，观察期后审批销毁并写 destruction receipt；
5. 产品 UI 不提供跨 PB archive 查询；确有历史查询需求时另立导入 job，不把归档变成第五个在线存储。

其他生命周期：

| 数据 | 在线保留 | 删除语义 |
|---|---|---|
| PB users/auth | 依身份政策 | 禁用→保留审计→删除；备份随 auth RPO/RTO |
| preferences | 用户存续期 | user 删除级联；迁移前可导出 |
| notification channels | soft delete 30 天 | secret 立即撤销，元数据到期 purge |
| alert rules/suppressions | soft delete 90 天 | 历史 event 不级联删除 |
| alert events/deliveries | 默认 180 天，可租户策略调整 | purge job + destruction receipt |
| latest inventory | target 存续期 + stale window | forget/soft delete；事实指标按 VM retention |

## 11. 权限与安全

- `me/preferences/channels` 只能操作自己；管理员代管必须是显式 admin endpoint 并审计；
- alert rule/suppression 要同时满足 owner 和 resource permission；event 查询按 resource + owner 交集过滤；
- system profile/storage device 是 observed resource，普通 configure 不得 PATCH health/temperature/state；
- agent action 使用 `operate(target)`，enrollment/rotation/revoke 使用 `configure(collector)`；
- webhook 禁止 loopback、link-local、metadata IP、私网重绑定和非 HTTPS（受控内网可用显式 allowlist）；
- 错误、审计、migration manifest 对 token、password、webhook、email 做分级脱敏；
- PB Admin UI 不作为业务管理界面，生产只允许受控管理网访问；
- VLogs 9428 从 compose/Helm/firewall/健康检查移除，不能只删前端链接。

## 12. 可观测性和非功能门

至少暴露：

- `watchdog_identity_projection_errors_total{reason}`、`watchdog_auth_context_seconds`；
- `watchdog_pb_business_reads_total{collection}`、`writes_total`、`subscriptions`；
- `watchdog_storage_migration_records_total{domain,status}`、`checksum_mismatch_total`；
- `watchdog_alert_events_total{type,state}`、`delivery_attempts_total{status}`、`delivery_queue_oldest_seconds`、`delivery_outcome_unknown_total`；
- `watchdog_alert_ui_snapshot_age_seconds`（客户端 telemetry 可选，不作检测 SLA）；
- `watchdog_storage_device_stale_total`、`smart_refresh_job_seconds`；
- `watchdog_legacy_agent_connections{version}`、`legacy_pb_writes_total{collection}`；
- `watchdog_victorialogs_requests_total`，收缩出口必须恒为 0。

发布门：

- 认证 P95≤100ms（不含外部 OAuth 跳转），管理 API P95≤300ms（常规 list/get），错误率<0.1%；
- PB/MySQL 备份恢复达到项目既定 RPO/RTO；MySQL 不可用时 fail-closed 且无跨 tenant fallback；
- migration count/checksum 100% 对齐，quarantine 全部处置；
- 告警重复 active event=0；可确认成功的副作用不重复，外部超时不明结果显式计数/处置；恢复/静默/重启测试通过；
- 旧 Agent 兼容、升级、断网、重连、token rotation/revoke 和 LKG 测试通过；
- PB 业务集合与 VLogs 连续观察期零读写后才能 contract。

## 13. 已冻结的决策门

| 决策 | 结论 | 理由 |
|---|---|---|
| 告警活动视图是否放弃 PB realtime | **是，仅 UI 改条件轮询** | 不保留 PB 业务订阅；检测/投递仍事件驱动，不牺牲正确性 |
| 旧 system/container stats 怎么处理 | **一次性可验证归档，不进入 MySQL，不在线双读** | 避免把历史 JSON 样本变成管理库负担 |
| SMART 挂哪里 | **挂通用 target；MySQL latest inventory + VM history** | 当前来源主要是 system agent，强绑 network device 语义错误 |
| Hub update-check/test-notification/heartbeat | **保留功能，逐步移入 authenticated `/api/v1`** | 与 PB collection 无直接关系，但必须统一 AuthContext/RBAC |
| 是否保留可选 VictoriaLogs debug | **否** | “裁撤”必须可验证；诊断采用有界 capture/DLQ，避免第五个在线存储回潮 |

## 14. 不做的事

- 不把 43+ MySQL 管理表迁回 PocketBase；
- 不在本方案重写 PB 已成熟的密码、OTP、重置和 OAuth；
- 不把 VM/CH 合并或让 MySQL 承接高频时序/Flow 明细；
- 不因前端页面暂时不可达就删除后端仍在运行的集合；
- 不新增身份服务、告警微服务、迁移服务或通用工作流引擎；上述能力均落在现有 Hub/runtime/repository/worker 框架；
- 不允许永久双写、静默回退、无 manifest 的数据搬迁或“上线后再补测试”。

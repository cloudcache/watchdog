# watchdog 平台化与可插拔模块架构

本文定义 flow 模块实施前必须完成的 watchdog 宿主平台整理。目标不是把所有功能重写一遍，而是把当前已经存在但边界分散的 tenant、用户权限、agent、target、指标、图表、导出和流量修正能力整理成稳定契约，使 flow、SNMP、system agent 等模块共享同一套管理面。

flow 的业务需求和数据面分别见 [flow-direction-requirements.md](flow-direction-requirements.md) 与 [flow-module-design.md](flow-module-design.md)，可勾选的设计/编码/测试实施项及本轮完成证据见 [flow-module-tasklist.md](flow-module-tasklist.md)；经代码审查冻结的 PocketBase/MySQL/VictoriaLogs 收敛 ADR（PB 只保留认证、MySQL 唯一管理库、VLogs 完全裁撤、legacy system 域先迁后删）见 [storage-consolidation.md](storage-consolidation.md)。

## 1. 结论与边界

目标形态由四部分组成：

1. **watchdog core**：身份、RBAC、module registry、collector registry、target/resource、统一查询、可视化、统计导出、修正规则、审计；
2. **业务模块**：flow、SNMP、system 等，以编译期注册方式提供资源类型、数据集、API、worker 和前端入口；
3. **独立采集器**：部署在数据源附近，通过一次性 enrollment 和持续 heartbeat 接入，不直接写业务数据库；
4. **数据基础设施**：Kafka-compatible MQ 提供 Flow RawFlow 唯一队列与短期重放边界，MySQL 保存管理数据，VictoriaMetrics 保存 SNMP/系统/运行指标，ClickHouse 保存 Flow 分析事实。

“可插拔”不采用 Go `.so` 动态插件。模块随 watchdog 二进制编译，通过 registry、feature flag 和 tenant module enablement 启停。这样保留类型安全、迁移可控和统一鉴权，同时避免模块把路由、菜单和表直接耦合进 core。

## 2. 当前代码库审阅

### 2.1 已有基础

| 能力 | 当前实现 | 可复用部分 |
|---|---|---|
| tenant/身份投影 | `tenants/users/roles/user_roles`；[`mysql_repository.go`](../internal/watchdog/mysql_repository.go) 可读取用户、角色和授权 | stable ID、tenant 约束、角色关联 |
| RBAC | [`permissions.go`](../internal/watchdog/permissions.go) 定义 `view/configure/operate/export/admin` 和 resource grant | action 检查、tenant/target/port 继承模型 |
| API 框架 | [`api_router.go`](../internal/watchdog/api_router.go) 用依赖非空决定注册路由 | middleware、错误格式、repository 注入 |
| target/网元 | `targets/network_devices/network_ports`；target、设备、端口均已有列表/详情/CRUD | target 作为资源根、网元和 ifIndex 稳定映射 |
| agent | `target_agents/agent_run_history`；[`api_agent_registry.go`](../internal/watchdog/api_agent_registry.go) 有 token、状态、心跳、运行历史 | enrollment 雏形、push/pull 模式、运行审计 |
| 指标 | [`metric_catalog.go`](../internal/watchdog/metric_catalog.go)、[`api_metrics.go`](../internal/watchdog/api_metrics.go)、VictoriaMetrics client | 时间范围、step、聚合、完整率和低基数指标查询 |
| 图表 | device/port overview + `aggregate_graphs/items/ports/data` CRUD | 图表定义、数据源 item、成员绑定和 rollup |
| 统计/导出 | `export_tasks`、worker、CSV、P95/平均/峰值、billing period | 异步任务、文件鉴权、审计快照思路 |
| 修正 | `port_policies`、`traffic_policy_defaults`、raw/corrected/both | 原始值不改写、查询时派生、确定性输出 |
| 地址标签 | `address_prefixes/address_sets` | tenant CIDR、labels、最长前缀匹配输入 |

### 2.2 必须先解决的结构问题

| 级别 | 现状证据 | 问题与目标 |
|---|---|---|
| P0（已完成最小闭环） | 前端使用 PocketBase 登录，而 watchdog MySQL 又有独立 `users/password_hash`；生产 hub 原先没有 `BackendRuntime.Router` 挂载 | 生产 Hub 现已挂载 `/api/v1`，PB 仅校验 `users` token，MySQL subject 投影生成 AuthContext；`password_hash` 已进入 nullable expand 窗口，最终 drop 仍待 STORE-05 |
| P0（已完成最小闭环） | 前端 [`api.ts`](../internal/site/src/lib/api.ts) 原先对 `/api/v1` 使用裸 `fetch`，没有转发 PB auth token；生产 SPA fallback 会捕获未知 API | 兼容 client 现已注入 token/tenant/request ID，未知 API 返回 JSON 404，生产 route precedence 已测试；完整 typed DTO、ETag 与幂等重放仍待完成 |
| P0 | PB system/alert 集合虽已从部分新页面退出，但 Hub 仍持续写 stats/inventory，告警恢复、SMART、容器/systemd action 和 legacy Agent 仍读取 | 这些是在线遗留域而非僵尸表；按存储 ADR 迁入 MySQL/VM/collector registry 后再停止 PB 业务写入 |
| P0（部分完成） | 原 `GET /api/v1/tenants` 返回空列表；没有完整 user/role CRUD | 已实现 subject membership discovery、单 tenant 默认选择和多 tenant UI 切换；用户、角色、成员关系 CRUD 仍待完成 |
| P0 | `APIV1RouterConfig` 手工列出每个 repository，路由依赖硬编码 | 改为 core services + module registry，模块自行声明依赖并注册 |
| P0 | permission 的 resource type 和 Go 常量只覆盖 tenant/target/port/export/billing | 建立可注册资源类型与父子关系，支持 module、collector、dataset、visualization、flow_exporter |
| P0 | `target_agents` 强制单一 `target_id`，agent type 只允许 snmp/system | collector 身份与 resource binding 分离，支持一个 flow collector 服务多个 exporter/target |
| P0（止血完成） | flow 原型原先同时写 VM aggregate 与逐 flow VLogs，没有 MQ、重放和 sink 隔离 | 逐 flow VLogs sink、配置和前端直连已删除；旧 `internal/flowcollect` 和状态清理链也已删除。正式链路固定为轻量 collector 写单一 RawFlow topic，Kafka 后 worker/GoFlow2 解码并写 CH |
| P1 | target DB 类型可扩展，但 Go `TargetKind` 只认 system/network | target kind 改为 registry，模块声明字段、校验器、详情 tab 和发现器 |
| P1 | metric catalog 静态，查询后端固定为 VM，任意 flow 高基数查询无法接入 | 引入 dataset/metric provider 和统一 QueryGateway，支持 VM 与 ClickHouse |
| P1 | aggregate graph 只绑定 port，item 只有 metric/direction | 图表定义改为 provider + dataset + query JSON + resource binding，支持 flow group-by/filter |
| P1 | export task 固定 target/port、VM、CSV | export provider 化，任务保存 dataset、query snapshot、value layer、policy version |
| P1 | 当前 `raw/corrected` 只有一层修正；supplier/customer 主要改变采样步长；修正算法是确定性随机加减固定值 | 改为 raw/supplier/customer 三个明确且可审计的平行数据层，禁止隐式随机修正 |
| P1（部分完成） | `install/init.sql` 与分散 migration 的表演进存在维护成本 | migration 已嵌入二进制并按连续版本/checksum/advisory lock 执行，空库重复执行已验证；init 自动生成或 CI parity gate 仍待补齐 |

### 2.3 本轮重构执行状态（2026-09-03）

| 工作包 | 已落地 | 尚未满足的退出条件 |
|---|---|---|
| STORE-00 | VictoriaLogs 热路径、配置、页面与路由删除；旧环境变量显式拒绝；migration runner 可重复且 fail closed | PB collection 调用指标、迁移 manifest/quarantine、生产副本零调用观察 |
| STORE-01 | 生产 Hub 路由、PB token server-side 校验、MySQL identity projection、tenant discovery/selector、MySQL RBAC 前端判定、request ID、liveness/readiness | 用户/角色 CRUD、ETag/Idempotency-Key、真实 password/OTP/OAuth 与回滚 E2E |
| 回归 | `go test -count=1 -tags=testing ./...`、前端 production build、`git diff --check` 均通过；GPU collector 改为有界条件等待，WebSocket 测试等待业务状态收敛，SystemManager 在 PB DB teardown 前 cancel+join 全部受管 updater | 浏览器真实 password/OTP/OAuth、升级/回滚、race 与长期 soak 仍是发布门，不因本轮全仓单测通过而豁免 |

完成项是可运行代码，不代表对应大项已签署完成。tasklist 只有在设计、编码、单元、集成、变更设计、变更测试和回归七类证据齐全后才允许勾选父项。

## 3. 目标技术架构

```text
                         ┌──────────────── watchdog core ────────────────┐
PocketBase auth/OIDC ───▶│ IdentityAdapter → AuthContext → RBAC           │
                         │ ModuleRegistry / ResourceRegistry              │
                         │ CollectorRegistry / TargetService              │
                         │ QueryGateway / Visualization / Export          │
                         │ AdjustmentPolicy / Audit / Health              │
                         └──────────┬───────────────────────┬──────────────┘
                                    │ module contract       │ management API
                    ┌───────────────▼──────────────┐        │
                    │ flow module                  │        │
                    │ API/query/export/worker      │        │
                    └───────────────▲──────────────┘        │
                                    │                       │
设备 sFlow/NetFlow ─▶ flow-collect                           │
                      UDP + source admission + RawFlow        │
                                            │                │
                                      Kafka raw topic        │
                                            ▼                │
                                  flow-worker + GoFlow2      │
                                            │                │
                                        ClickHouse           ▼
                                                          Web UI

SNMP/system collectors ──同一 CollectorRegistry/Target/RBAC/VM Provider────▶ core
flow pipeline `/metrics` ────────────────────────────────▶ existing VM
```

这是最小基线而不是组件堆叠：只有一个 watchdog 管理面、两个 Flow 数据面进程角色、一个 RawFlow topic、一个 Flow 事实库。生命周期、权限、幂等、删除和测试是同一管理/API 框架中的契约，不新增“生命周期服务”“权限服务”或另一套 Flow 时序库。ClickHouse 的派生表是同一库内可重建查询加速，不是新的事实源；VM 不默认保存 Flow 业务曲线。

控制面和数据面必须分开：API/MySQL 短暂不可用时，已注册 flow-collect 可在签名 plan 有效期内继续收数；接收、解码/归类和查询互相隔离。Kafka 不可用时 collector 的有界 producer 最终表现为 UDP drop；worker/CH 不可用由 RawFlow lag 承接，所有容量耗尽都产生显式 data-loss interval。

## 4. Module contract

### 4.1 后端契约

目标接口表达如下，具体 Go 类型可按现有 repository 风格拆分：

```go
type PlatformModule interface {
    Descriptor() ModuleDescriptor
    RegisterResources(*ResourceRegistry) error
    RegisterDatasets(*DatasetRegistry) error
    RegisterRoutes(*http.ServeMux, CoreServices) error
    RegisterWorkers(*WorkerRegistry, CoreServices) error
    Migrations() []Migration
    Health(context.Context) ModuleHealth
}

type ModuleDescriptor struct {
    Key, Version, DisplayName string
    Dependencies              []string
    DefaultEnabled            bool
}
```

约束：

- module key、resource type、dataset key 和 migration ID 全局唯一；
- 模块路由固定在 `/api/v1/modules/{module}/...`，兼容别名可保留一个版本；
- 模块不得自行解析用户 token、绕过 RBAC、直接向前端暴露 CH/VM；
- 模块不得修改其他模块事实表，只能通过 core service 或明确事件契约协作；
- tenant 禁用模块后停止新 worker 和入口，但保留数据，删除必须单独确认；
- module descriptor 与前端 manifest 在构建时生成，启动时校验版本一致。

### 4.2 前端契约

每个模块注册 `route/menu/permission/health badge` 描述，页面继续编译进同一前端 bundle并 lazy-load。core 负责主导航、403/404、tenant 切换和统一时间筛选；模块负责业务页面。禁止模块硬编码存储地址或在 navbar/main switch 中散落条件分支。

## 5. 身份、用户和权限

### 5.1 身份权威

当前阶段保留 PocketBase `users` auth collection（终局可换外部 OIDC）作为认证权威；PocketBase 不再保存业务集合或决定业务角色，MySQL 是唯一管理库并保存 watchdog 授权投影：

- `users.auth_provider + external_subject_id` 对应 IdP subject；同一 subject 可在不同 tenant 有独立投影，MySQL user ID 不复用 PB ID；
- 密码、MFA、重置和 session 由 IdP 管理；先回填 subject 并把 MySQL `password_hash` 置为 nullable，观察期后再独立 migration 删除；
- production `IdentityAdapter` 必须由 PB server 校验 token，映射 active tenant/user，加载 role IDs 和 grants；仅 `/api/v1/me/tenants` 使用不产生 tenant scope 的 discovery adapter；多 tenant 未选择时 tenant-scoped API 返回 400 `invalid_request`，零投影返回 403；
- 前端 `/api/v1` 客户端必须转发 PB token；`/api/v1/me` 返回 MySQL `is_admin/roles/grants`，前端和 `/api/watchdog` 不再读取 PB `role`；
- 认证按需发生：应用启动、公开页面和登录表单挂载不得调用 `authRefresh`、密码、OTP 或 OAuth 登录；只有访问受控页面/数据时校验现有 session，或在用户点击登录/提交凭据后发起认证。OAuth callback 只是用户主动登录的续程，不视为自动登录；
- MySQL 不可用时管理 API fail closed 为 503，绝不回退 PB role/collections；
- 每个请求只接受服务端解析的 tenant，不信任 query/body 中的 tenant_id；
- service account/collector 使用独立认证类型，不能伪装成人类用户。

完整字段、跨库 bootstrap saga、路由顺序、故障语义和回滚边界以[存储收敛详细设计](storage-consolidation.md)为准。

### 5.2 资源树

```text
tenant
├── module
├── target
│   ├── device
│   │   └── port
│   └── collector binding
├── collector
├── dataset
├── visualization
├── export_task
└── adjustment_policy
```

资源父子关系由 `ResourceRegistry` 提供，RBAC 不再在 `resourceMatches()` 中硬编码 port→target。授权默认向下继承，显式 deny 不在第一阶段引入；冲突以最具体 allow 为准。

### 5.3 actions

保留通用 `view/configure/operate/export/admin`，增加数据层 action：

| action | 含义 |
|---|---|
| `view_customer` / `export_customer` | 查看/导出客户层，普通业务用户默认层 |
| `view_supplier` / `export_supplier` | 查看/导出供应商结算层 |
| `view_raw` / `export_raw` | 查看/导出原始事实，仅审计/管理员 |
| `configure_adjustment` | 创建、审批、启停修正规则 |
| `dispose_finding` | acknowledge、调查、授权、误报或解决 finding；不能修改机器证据/verdict |
| `probe_passive` | 下发限时镜像/TAP 观察 plan，不允许主动连接 |
| `probe_active` | 创建受控主动握手 job；模块注册的高危 action，不能由通用 operate/admin 隐式推导 |
| `read_sensitive` | 查看完整 IP、握手指纹和受限错误上下文；读取也审计 |

API 和导出都按 action 检查，不能只靠前端隐藏 tab。raw/supplier/customer 同时返回时，需要三种权限的并集全部满足。

### 5.4 管理 API

```text
GET/POST/PATCH/DELETE /api/v1/users
GET/POST/PATCH/DELETE /api/v1/roles
PUT                    /api/v1/users/{id}/roles
GET/PUT/DELETE         /api/v1/permissions
GET                    /api/v1/permissions/effective
GET/PUT                /api/v1/tenants/{id}/modules
```

所有用户、角色、权限、module enablement 变更写 `audit_logs`，detail 保存 before/after、原因和 request ID。

## 6. Collector/agent 注册与接入

### 6.1 通用模型

collector 是独立身份，不再等同于某一个 target。建议把 `target_agents` 迁移为：

```sql
CREATE TABLE collector_agents (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  module_key VARCHAR(64) NOT NULL,
  name VARCHAR(190) NOT NULL,
  agent_type VARCHAR(64) NOT NULL,
  mode VARCHAR(16) NOT NULL,
  endpoint VARCHAR(512) NULL,
  status VARCHAR(24) NOT NULL DEFAULT 'pending',
  observed_health VARCHAR(24) NOT NULL DEFAULT 'unknown',
  auth_type VARCHAR(16) NOT NULL DEFAULT 'token',
  token_hash VARCHAR(255) NULL,
  certificate_fingerprint VARCHAR(190) NULL,
  capabilities_json JSON NULL,
  capabilities_hash CHAR(64) NOT NULL DEFAULT '',
  capability_schema_version SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  software_version VARCHAR(64) NOT NULL DEFAULT '',
  agent_api_version SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  plan_schema_min SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  plan_schema_max SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  boot_id VARCHAR(64) NOT NULL DEFAULT '',
  runtime_schema_version SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  heartbeat_sequence BIGINT UNSIGNED NOT NULL DEFAULT 0,
  heartbeat_sent_at DATETIME(3) NULL,
  clock_offset_ms BIGINT NULL,
  runtime_observation_json JSON NULL,
  runtime_observation_hash CHAR(64) NOT NULL DEFAULT '',
  heartbeat_payload_hash CHAR(64) NOT NULL DEFAULT '',
  config_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  acknowledged_config_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  last_good_config_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  plan_hash CHAR(64) NOT NULL DEFAULT '',
  plan_expires_at DATETIME(3) NULL,
  last_seen_at DATETIME(3) NULL,
  last_error_code VARCHAR(64) NULL,
  last_error_detail VARCHAR(1024) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NOT NULL,
  updated_by CHAR(26) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  deleted_at DATETIME(3) NULL,
  purge_after DATETIME(3) NULL,
  live_identity TINYINT GENERATED ALWAYS AS
    (IF(deleted_at IS NULL, 1, NULL)) STORED,
  UNIQUE KEY uq_collector_agent_tenant_id (tenant_id, id),
  UNIQUE KEY uq_collector_name (tenant_id, module_key, name, live_identity),
  KEY idx_collector_health
    (tenant_id, status, observed_health, last_seen_at),
  CONSTRAINT fk_collector_agent_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (mode IN ('push','pull','listen')),
  CHECK (status IN ('pending','active','suspended','revoked','deleted')),
  CHECK ((status = 'deleted') = (deleted_at IS NOT NULL)),
  CHECK (observed_health IN (
    'unknown','warming','healthy','degraded','stale','unavailable'
  )),
  CHECK (plan_schema_min <= plan_schema_max),
  CHECK (last_good_config_version <= acknowledged_config_version),
  CHECK (acknowledged_config_version <= config_version),
  CHECK (
    (runtime_schema_version = 0 AND heartbeat_sequence = 0 AND heartbeat_sent_at IS NULL AND clock_offset_ms IS NULL
      AND runtime_observation_json IS NULL AND runtime_observation_hash = '' AND heartbeat_payload_hash = '')
    OR
    (runtime_schema_version > 0 AND heartbeat_sequence > 0 AND heartbeat_sent_at IS NOT NULL AND clock_offset_ms IS NOT NULL
      AND capabilities_json IS NOT NULL AND CHAR_LENGTH(capabilities_hash) = 64
      AND runtime_observation_json IS NOT NULL AND CHAR_LENGTH(runtime_observation_hash) = 64
      AND CHAR_LENGTH(heartbeat_payload_hash) = 64)
  ),
  CHECK (auth_type IN ('token','mtls')),
  CHECK ((auth_type = 'token') = (token_hash IS NOT NULL)),
  CHECK ((auth_type = 'mtls') = (certificate_fingerprint IS NOT NULL))
);

CREATE TABLE collector_bindings (
  collector_id CHAR(26) NOT NULL,
  tenant_id CHAR(26) NOT NULL,
  resource_type VARCHAR(64) NOT NULL,
  resource_id CHAR(26) NOT NULL,
  binding_role VARCHAR(64) NOT NULL DEFAULT 'collect',
  config_json JSON NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NOT NULL,
  updated_by CHAR(26) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (collector_id, resource_type, resource_id),
  KEY idx_collector_binding_resource
    (tenant_id, resource_type, resource_id, binding_role),
  CONSTRAINT fk_collector_binding_collector
    FOREIGN KEY (tenant_id, collector_id)
    REFERENCES collector_agents(tenant_id, id) ON DELETE CASCADE,
  CONSTRAINT fk_collector_binding_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
);

CREATE TABLE collector_plan_revisions (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  collector_id CHAR(26) NOT NULL,
  config_version BIGINT UNSIGNED NOT NULL,
  plan_schema_version SMALLINT UNSIGNED NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'draft',
  spec_json JSON NOT NULL,
  spec_hash CHAR(64) NOT NULL,
  signing_key_id VARCHAR(64) NULL,
  signature VARBINARY(512) NULL,
  validation_json JSON NULL,
  not_before DATETIME(3) NULL,
  expires_at DATETIME(3) NULL,
  supersedes_config_version BIGINT UNSIGNED NULL,
  created_by CHAR(26) NOT NULL,
  updated_by CHAR(26) NOT NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  activated_at DATETIME(3) NULL,
  retired_at DATETIME(3) NULL,
  active_identity TINYINT GENERATED ALWAYS AS
    (IF(status = 'active', 1, NULL)) STORED,
  UNIQUE KEY uq_collector_plan_version (collector_id, config_version),
  KEY idx_collector_plan_hash (collector_id, spec_hash),
  KEY idx_collector_plan_status (collector_id, status, config_version),
  UNIQUE KEY uq_collector_active_plan (collector_id, active_identity),
  CONSTRAINT fk_collector_plan_collector
    FOREIGN KEY (tenant_id, collector_id)
    REFERENCES collector_agents(tenant_id, id) ON DELETE CASCADE,
  CONSTRAINT fk_collector_plan_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (status IN ('draft','validated','active','retired','rejected')),
  CHECK (config_version > 0),
  CHECK (plan_schema_version > 0),
  CHECK (status <> 'active' OR (
    signature IS NOT NULL AND activated_at IS NOT NULL AND expires_at IS NOT NULL
  )),
  CHECK (status <> 'retired' OR retired_at IS NOT NULL),
  CHECK (expires_at IS NULL OR not_before IS NULL OR expires_at > not_before),
  CHECK (supersedes_config_version IS NULL OR supersedes_config_version < config_version)
);
```

Migration `018_collector_registry_expand.sql` 已创建上述三表，并把现有 `target_agents` 原 ID、tenant、token、desired/observed 状态和 target binding 原样回填。兼容窗口采用受控 expand-contract，而不是两个可独立修改的 registry：新 collector 管理 API 尚未开放；现有 SNMP/system repository 是唯一写入口，并在一个 MySQL transaction 内同时写 `target_agents` 兼容投影和 `collector_agents/bindings`，跨 tenant 复用 agent ID 会在修改前拒绝。心跳/运行结果也同事务刷新兼容投影与新 observed health，删除同时清理两侧。待新 API、连续监听 collector 和 run history 全部切换后，先做逐行一致性校验，再移除 legacy 写读路径与 `agent_run_history → target_agents` FK；禁止在窗口中给新表增加第二套无投影写入口。兼容写的 `created_by/updated_by=system:compatibility` 只标识无用户上下文的旧 worker，原 HTTP 管理动作仍必须在 `audit_logs` 保存真实 actor。

`collector_bindings.resource_type/resource_id` 是多态资源引用，数据库无法对所有 module 表建立单一 FK；service 必须在同 tenant 下验证 resource 存在及 kind/capability 相容。`collector_plan_revisions` 保存规范化 JSON 的不可变版本，不保存明文 token、私钥或 Kafka 密码，只保存 secret reference；validated spec 由控制面 signing key 签名，agent 内置/轮换 trust bundle 验证，旧验证公钥的保留期不得短于仍可能启动的 LKG plan。rollback 复制旧 spec 生成更大的新 `config_version`，绝不倒退版本或原地重新激活旧行。Heartbeat 只更新 `software/boot/capabilities/ack/health/last_seen/error` 等 observed 字段，不递增管理 ETag 的 `row_version`、不覆盖管理员 `updated_by`；018 的 compatibility adapter 是过渡例外，因旧 DTO 尚无独立 config/observed 通道。管理员对 status/binding/spec 的有效修改才递增 row version，plan activate 再事务性更新 `config_version/plan_hash/plan_expires_at`。这样高频心跳不会让配置 PATCH 持续产生伪冲突。

plan repository core 已实现以下不能由 API 绕过的门禁：spec 必须是单一 JSON object、最大 4 MiB，并先规范化键顺序/空白再计算 SHA-256；签名载荷固定绑定 envelope version、plan/tenant/collector ID、config/schema version、spec hash、signing key ID、毫秒精度有效期和 supersedes version。只有 Ed25519 验证函数生成的内部 proof 才能 create，验证后再修改 spec、signature 或任一载荷字段会在入库前失效。create 只接受 `validated`，且 schema 必须位于 agent 声明区间、version 必须高于当前 head、supersedes 必须精确等于当前 head。activate 以 collector `row_version` 和 plan `row_version` 双乐观锁，在同一 transaction 中 retire 旧 active、激活新 revision、推进 collector head 并写审计；单 active unique key 是最后防线。activation/ACK 时间由服务端生成，API/agent 不得传入安全时间。ACK 必须与当前 active plan 的 version/hash/expiry 同时相符，只推进 `acknowledged_config_version/last_good_config_version` 和 observed 字段，不推进管理 `row_version`；重复 ACK 幂等且只在首次推进时审计。MySQL JSON 回读必须重新 canonicalize 后校验 hash，不能比较 MySQL 自行格式化的 JSON 原始字节。

这里的 Ed25519 public key 必须由 trust-bundle/key registry 按 `signing_key_id` 解析；repository 接收的是已由该 registry 验证出的值，不允许 HTTP DTO 直接构造内部 proof。agent-side v2 signature 验证、本地多 key bundle 的 overlap/retiring/revoked 执行、失败 ACK 保留 LKG 和 exact 成功 ACK 已完成；PLAT-03 管理面 key registry、bundle 签名分发/本地防回滚代际、fleet ACK 与 rollout/canary 尚未完成。

Migration `019_collector_ownership_evidence.sql` 历史上为旧 collector 数据面补过三类机器事实；收敛后只保留仍属于管理面的 principal 与 ownership transfer：

| 表 | 主键/唯一性 | 保存内容 | 禁止内容 |
|---|---|---|---|
| `collector_service_principals` | stable ID；`(service_type,principal_ref)`、grant/revoke operation key 全局唯一 | collector 所属、secret reference、provider、grant operation/request hash、revoke operation key、grant/revoke receipt ref+SHA-256、服务端 revoke 时间、ACL 传播窗、row version | Kafka 密码/私钥、客户端自报 revoke 时间、可被多个 collector 共用的 principal |
| `collector_ownership_transfers` | stable ID；`(tenant,exporter,old_epoch)` 唯一 | old/new collector、old/revoke/new plan revision、严格递增 epoch、old principal、clock skew、审批、old-owner drain receipt | 管理员填写的“已 drain/已 revoke”布尔值、未绑定 plan 的模糊切换 |
| `collector_state_restore_receipts` | — | migration 027 已删除；RawFlow collector 不复制模板/WAL/checkpoint 状态 | 禁止恢复该表或对应 API |

`collector_plan_revisions` 的 tenant/collector/version 约束、签名验证和 principal revoke 继续有效。ownership transfer 只负责停止旧 owner、撤销旧凭据、发布新 plan 和确认新 owner 收到数据；模板由 Kafka partition 内 worker 重新学习，不存在跨 collector state restore。旧 state-cleanup service、repository、API、heartbeat 字段和配置已经删除，升级路径由 migration 027 清表。

### 6.2 生命周期

```text
create enrollment → pending → active ↔ suspended → revoked → deleted → purged
                              ↘ stale（派生健康态）
```

1. 管理员创建 collector，生成只显示一次、短 TTL enrollment secret；数据库只存 hash；
2. agent 首次 exchange 获取长期 token 或 mTLS 证书、collector ID 和签名 plan；
3. heartbeat 上报 software/API version、boot ID、capabilities/hash、plan ack/LKG version、队列/丢弃和时钟；
4. token 可双 token 窗口轮换，旧 token 到期撤销；
5. suspended 停止下发新任务；revoked 永久撤销该身份的凭据并拒绝所有数据；delete 只给 revoked 对象建立 tombstone，restore 回到 revoked 而不是重新激活旧凭据；purge 才物理删除；
6. plan 使用单调 config version 和 hash，agent 必须回报已应用版本；
7. run history 与 heartbeat 分离，连续监听型 collector 用 uptime/health interval，不伪造“每分钟成功任务”。

flow collector 一个实例可绑定多个 exporter/target；exporter 的协议 source identity 仍属于 flow module，不塞进通用 collector 表。

VPN probe 也复用 collector 模型，而不是再建 agent 系统：`agent_type=flow_probe`，`capabilities_json` 声明 `passive_observe/active_handshake`、analyzer/version、profiles、network vantage 和最大并发；binding 指向 tenant 内获批的 `probe_scope` 资源。plan 只能取 capability、binding、scope policy 和全局安全上限的交集，agent 不得自行扩大 CIDR/端口或跟随重定向越界。`passive_observe` 与 `active_handshake` 是两个独立 capability/action；后者默认 kill switch 且要求审批。probe job/result 复用 §15 公共 job/idempotency/audit 契约，低容量结果经模块 API 提交，不增加第二个 Flow Kafka topic。

### 6.3 配置、维护与扩展契约

#### 6.3.0 现有组件配置基线

在引入签名 plan 前，现有 Watchdog 进程先统一到 [`config.go`](../internal/watchdog/config.go) 的单一 bootstrap 契约：`显式 CLI > 环境变量 > YAML > 默认值`。YAML 严格拒绝未知字段和多文档；环境变量的非法整数/周期不再静默回落；URL、ID、listen、路径和 MIB 列表规范化后再校验，DSN/token 不改写。MySQL `max_idle_conns=0` 保留其“禁用 idle pool”语义。SNMP poll/discovery、sFlow 聚合/地址同步、aggregate rollup、trap agent 和 system agent 的 YAML/环境变量/CLI 已使用相同字段，完整矩阵见 [`watchdog-install.md`](watchdog-install.md)。

当前 `target_agents` 写链也先执行统一领域规范化：type/mode/status 小写、endpoint/ID 去空白，API 拒绝未知 JSON 字段并按真实 PATCH 语义保留省略字段，repository 写入前再次验证；system agent 只允许 push，disabled agent 不得 plan/heartbeat/report/push，run status 只允许 success/failure。旧 hub `config.yml` 同步也严格拒绝未知字段/重复系统/缺失用户，并以 `(name,host,port)` 元组匹配；旧 agent 的显式 URL/token CLI 已真正覆盖 `WATCHDOG_AGENT_*` 环境变量。这个基线只解决现有静态 bootstrap 与 registry 数据一致性，不冒充下文的签名 immutable plan、LKG、canary 和 rollback；后续迁移必须保持已有 YAML 至少一个兼容窗口，并把 secret 从长期 YAML 迁出。

agent 必须是薄执行器，不保存业务评分规则、不自行判断 tenant，也不允许控制面远程下发脚本、共享库或任意握手 payload。配置分三层：

| 层 | 内容 | 变更与优先级 |
|---|---|---|
| 本机 bootstrap/硬上限 | control-plane URL、身份凭据引用、state dir、capture interface、允许 egress、CPU/RSS/并发硬上限、本机 emergency kill switch | 由 systemd/Kubernetes/配置管理发布；只能收紧控制面，不允许 plan 放宽 |
| 签名 immutable plan | tenant/binding、允许 capability/analyzer/profile、scope、vantage、quota、证据策略、`not_before/expires_at` | MySQL revision→validate→activate；agent 原子应用并回报 config version/hash |
| immutable job | job/finding ID、目标 IP/端口、mode、profile/version、scope policy、deadline、attempt 和结果上限 | 领取时取本机硬上限、active plan 和 job 的交集；任一不满足即拒绝并返回 stable reason |

有效配置固定为三层交集。agent 以 `If-None-Match: plan_hash` 拉取 active plan；控制面返回 canonical spec、config/schema version、key ID、signature 和 expiry。agent 依次执行签名/hash、schema、兼容范围、binding/scope、资源预算和 analyzer self-test 校验；通过后写临时文件、fsync、原子切换并 heartbeat `acknowledged_config_version`。失败继续 last-known-good，并上报失败阶段。Flow 收集型 agent 可在控制面短故障时继续使用 LKG；`flow_probe` plan 只要允许 `active_handshake`，validate 就强制非空 expiry，主动 plan/job 必须同时未过期，过期后不得领取新任务，inflight 只运行到固化 deadline。

当前轻量 `watchdog-flow-collect` 只实现本地 signed plan 验签、来源准入、RawFlow producer 和 UDP 生命周期。authenticated remote delivery、ETag/LKG 原子更新、credential/trust rotation、fleet rollout/canary 必须作为独立管理面切片重新接线和测试；旧 `internal/flowcollect` supervisor/history/WAL 实现已经删除，文档不得把它描述成现状。

服务端 runtime heartbeat 保留 strict envelope、boot/sequence fence、服务端接收时间和 observed/config 分离。Flow collector 动态 counter 已收敛为 received/rejected/invalid/kernel-drop、Kafka records/bytes/failures 和 Kafka queue；不再接受 WAL/decode/quarantine counter。collector 端 heartbeat client、enrollment、mTLS/token 双窗口和 trust bundle 仍未闭环，完成状态以平台 tasklist 为准。

投递格式已消除 repository/runtime 的签名分叉：旧 v1 文件继续验证 `Ed25519(spec_json)`，但拒绝附加未签名控制面字段；生产 v2 直接携带 `collector_plan_revisions` 已有 canonical spec、SHA-256、签名元数据和 signature。管理面与 flow-collect 共用唯一 signing-payload builder，内外 collector/version/schema/effective interval 必须一致，服务端只封装已有签名事实而不持有/调用 plan 私钥。authenticated GET/304、原子落盘、激活后 ACK、固定分类失败 heartbeat、周期 capability/runtime heartbeat、timeout/backoff 和低基数 health 已实现；enrollment、credential/trust rotation/revoke 的全生命周期、fleet drift 与 data-loss interval 审计尚未实现。

扩展以稳定 analyzer contract 完成：

```go
type ProbeAnalyzer interface {
    Descriptor() AnalyzerDescriptor
    ValidateProfile(profileVersion string, params json.RawMessage) error
    Execute(ctx context.Context, job ProbeJob, sink EvidenceSink) ProbeResult
}

type AnalyzerDescriptor struct {
    Key, Version       string
    APIMin, APIMax     uint16
    Modes              []string
    ProfileSchemas     map[string]uint16
    ResultSchema       uint16
    RequiredPrivileges []string
}
```

- v1 analyzer 在构建期静态注册；禁止运行时下载 Go plugin、`.so` 或 shell。nDPI 可用受版本固定的 cgo adapter；确需非 Go 依赖时，才允许同机、非特权、Unix socket 的受监督 sidecar，并固定镜像/包 digest、SBOM 和 adapter API；
- profile 是 analyzer 暴露 JSON Schema 的**安全参数**，可配置 timeout、snaplen、允许的 evidence 字段等；协议握手状态机、请求字节和解析器属于受测试代码，租户不能把 profile 变成任意扫描/发包 DSL；
- capability key、plan/job/result schema 均版本化。job 固化 analyzer/profile/result schema version；agent 只领取其 API/schema 区间支持的任务，服务端只接受注册 catalog 中的 analyzer 和签名结果；
- analyzer 只输出观察事实和 `verdict_candidate`，最终 finding verdict 仍由 watchdog 的版本化 evidence policy 合并；插件不能直接改 MySQL finding、扩大 scope 或访问其他 tenant；
- 新 analyzer 必须具有 manifest、golden pcap/handshake fixture、正常业务误报集、超时/取消/资源上限和结果兼容测试，按 experimental→canary→GA 发布。二进制由既有 systemd/Kubernetes/制品系统灰度，不把 watchdog 做成软件分发器；watchdog 负责兼容检查、drift、plan rollout 和回滚。

plan 发布流程固定为 `draft → preview/validate → validated → active → retired`；preview 不写状态，validate 固化校验结果和签名。批量 rollout 复用公共异步 job，先按 agent label/版本选择 canary，观察 plan apply、拒绝率、crash、CPU/RSS、probe 误报和 spool 后再扩大。rollback 生成新的递增 revision；scope 收缩、凭据撤销和 kill switch 属紧急配置，立即停止新任务并请求取消不再合法的 inflight job。

### 6.4 Collector API

```text
GET/POST                 /api/v1/collectors
GET/PATCH/DELETE         /api/v1/collectors/{id}
POST                     /api/v1/collectors/{id}/enrollment
POST                     /api/v1/collector-enrollments/exchange
POST                     /api/v1/collectors/{id}/credentials/rotate
POST                     /api/v1/collectors/{id}/suspend
POST                     /api/v1/collectors/{id}/resume
POST                     /api/v1/collectors/{id}/revoke
GET                      /api/v1/collectors/{id}/deletion-impact
POST                     /api/v1/collectors/{id}/restore
POST                     /api/v1/collectors/{id}/purge
GET/PUT                  /api/v1/collectors/{id}/bindings
GET                      /api/v1/collectors/{id}/health
GET                      /api/v1/collectors/{id}/capabilities
GET                      /api/v1/collectors/{id}/plans
GET                      /api/v1/collectors/{id}/plans/{version}
POST                     /api/v1/collectors/{id}/plans:preview
POST                     /api/v1/collectors/{id}/plans
POST                     /api/v1/collectors/{id}/plans/{version}:validate
POST                     /api/v1/collectors/{id}/plans/{version}:activate
POST                     /api/v1/collectors/{id}/plans/{version}:rollback
POST                     /api/v1/collectors/{id}/service-principals
POST                     /api/v1/collectors/{id}/service-principals/{principal_id}/actions/revoke-write
POST                     /api/v1/collectors/{id}/ownership-transfers/{transfer_id}/actions/drain
GET                      /api/v1/collectors/{id}/plan
POST                     /api/v1/collectors/{id}/plan-ack
POST                     /api/v1/collectors/{id}/heartbeat
```

创建 enrollment、轮换和 revoke 属 `admin/operate`；binding 和 plan draft/preview 属目标资源的 `configure`，activate/rollback 另需 `operate`，主动 probe scope 仍需 `probe_active`。DELETE 只允许 revoked collector 并要求 `If-Match`；restore 不恢复凭据，purge 要求恢复期届满、无 binding/job/spool 引用和独立权限。agent 自用 exchange/heartbeat/plan 走 service authentication，不复用人类 session。Plan list/get 默认脱敏 secret reference；每个 preview/publish/activate/rollback、capability drift 和拒绝原因都写审计。

service-principal grant 要求 `operate` 与 `Idempotency-Key`，body 只允许 provider 和 ACL propagation delay；revoke-write 要求 `operate` 与当前强 `If-Match`，body 为空。principal ref、credential secret ref、operation key、request hash、provider receipt 和事实时间全部由 provider/service 生成，管理 API 不接收；响应只返回安全的 principal identity/status/provider/row version，不返回 secret reference 或 evidence。provider 未配置时路由不得伪造成功或允许上传 receipt。启用 remote provider 时，唯一 allowlist name 和版本化 policy 一并参与 request hash；provider 的健康 200/404 必须声明 operation lookup retention 覆盖本地灾备窗，低于要求即失败，不能在超时后把未知副作用当成未执行。

ownership drain 是 collector 的机器事实入口，同样不复用人类 session。URL 中的 collector ID 只用于定位待认证主体；tenant、collector、当前 boot 由 `collector_agents` 在 token bcrypt 或已验证客户端证书 SHA-256 指纹认证后注入，请求 body 不允许覆盖。mTLS 指纹格式固定为 `sha256:<64 lowercase hex>`，未验证 peer certificate 与同时携带 token+证书均拒绝。repository 仍必须校验 transfer old/new owner、active/ACK/LKG plan 和当前 boot，避免“持有有效凭据”被误当作任意 transfer 的写权限。NetFlow/IPFIX 模板在 Kafka partition owner 内按有界回放重新学习，不提供 collector state-restore 写入口。

## 7. Target、网元和资源管理

`targets` 保持跨模块资源根，`network_devices/network_ports` 保持网元投影。需要新增 `TargetKindRegistry`，替代 Go 常量穷举：

```go
type TargetKindDescriptor struct {
    Key, ModuleKey      string
    DisplayName         string
    Readiness           string   // ga | beta | planned
    MenuGroup           string   // resources | analysis | settings
    AllowedCapabilities []string // 允许绑定的 collector capability
    Validate            func(Target) error
    DetailTabs          []string
    Discoverer          string
    MetricScopes        []MetricScope
}
```

`Readiness` 是“未就绪功能列入架构需求”的机制化表达：`planned` kind 照常注册（占住 key、文档和权限模型），但前端 manifest 不渲染其入口（或按产品决定渲染禁用态），API 对其 CRUD 返回明确的 `KIND_NOT_AVAILABLE`；转 `beta/ga` 只翻转 descriptor，不新增注册路径。

目标行为：

- core 提供 target 列表、搜索、标签、状态、CRUD 和 resource link；
- network 模块提供设备/端口/库存/BGP tab；flow 模块只追加 exporter/flow-health tab；
- 删除 target 先 preview 影响：collector binding、设备、端口、图表、导出和 flow exporter；
- 删除管理元数据不直接删事实数据，使用 retention/tombstone job；
- list API 统一 cursor、filter、sort、fields 和 summary，避免每个模块自造分页格式。

core API 保持资源导向：

```text
GET/POST                 /api/v1/targets
GET/PATCH/DELETE         /api/v1/targets/{id}
GET                      /api/v1/targets/{id}/summary
GET                      /api/v1/targets/{id}/resources
POST                     /api/v1/targets/{id}/delete-preview
GET/POST                 /api/v1/network-devices
GET/PATCH/DELETE         /api/v1/network-devices/{id}
GET/POST                 /api/v1/network-devices/{id}/ports
GET/PATCH/DELETE         /api/v1/network-ports/{id}
```

模块只注册 target kind、详情 tab 和关联资源，不另建“flow target CRUD”。

### 7.1 Target 五分类与采集角色

产品口径冻结为五个 target kind。kind 是 target 的**分类维度**；哪个 agent 能采由 **capability + binding** 决定，kind、module、agent_type 都不是一对一关系——一个 host target 可同时被 system capability（主机指标）与 storage capability（SMART/RAID）覆盖，network kind 的设备同时承载 core 域的 BGP tab。

| kind | 中文 | 覆盖范围 | 采集通路 | 数据落点 | 就绪度 |
|---|---|---|---|---|---|
| `host` | 主机 | 服务器/VM；**容器、systemd、GPU、SMART 视图都是 host 的子资源，不是独立 kind** | `agent_type=system`（push） | VM 观测 + MySQL 库存投影（ADR-SC-001） | ga（现 `system` 更名） |
| `network` | 网络 | SNMP 网元：交换机/路由器/防火墙，设备/端口/光模块/传感器/VLAN/LAG | `agent_type=snmp`（pull） | VM + MySQL `network_devices/ports` | ga |
| `storage` | 存储 | RAID 卡（storcli/perccli 类 CLI）、分布式与对象存储集群（Ceph/MinIO/RustFS/BeeGFS）、磁盘 SMART 聚合 | **不新建 agent 二进制**：system agent 扩 `storage.*` capabilities（smartctl 已有雏形；RAID CLI、集群本机探针）；集群型 target 由绑定 agent 经原生 API/exporter 代理采集 | MySQL `storage_devices` 最新态 + VM 历史（对齐 ADR-SC-001 SMART 结论） | SMART=beta（迁移中）；RAID/Ceph/MinIO/RustFS/BeeGFS=planned |
| `edge` | 边缘 | 轻量拨测：ping（ICMP）、dig/nslookup（DNS）、HTTP(S) 探活、mtr（路径质量） | `agent_type=edge_probe`：薄探针，复用 §6 的 enrollment/plan/job/result 基建；拨测任务 = plan 内的周期 job 定义 | VM 拨测时序（rtt/loss/status/http_code）+ MySQL 任务定义与最新结果摘要 | planned |
| `core` | 核心 | BGP 路由监控（会话/前缀/状态）+ IP 库查询（本地 `flow-geo-v1/v2` 只读 lookup 服务化） | 现：`agent_type=snmp`（按实际 MIB 表能力选择 BGP4-V2/厂商扩展，BGP4-MIB 仅作 IPv4 兼容回退；不按 OS 字符串分支）；未来：`agent_type=bmp`（RIB 级）。IP 库查询无采集，只读 flow 模块共享 Geo loader | VM BGP 指标 + MySQL `bgp_sessions`；IP 库不落库 | BGP-via-SNMP=ga；BMP=planned；IP 库查询页=planned（仅依赖共享 Geo loader，可先于 Flow P1 独立交付） |

约束与迁移：

- `edge_probe` 与 `flow_probe` 共享 collector plan/job/result 基础设施但是**两个 capability/权限域**：edge 拨测是常规低危主动监测（走普通 configure/operate），绝不复用 `probe_active` 的高危审批链，也不因此绕过它——反向亦然；
- `collector_agents.agent_type` 的 CHECK 从 `snmp/system` 枚举放开为 registry 校验，终态取值：`system | snmp | flow_collect | flow_probe | edge_probe | bmp`；`storage` 不是 agent_type，是 system agent 的 capability 组；
- Go `TargetKind` 常量 `system→host` 更名：migration 改存量值 + API 兼容读旧值一个版本；`network` 不变；
- 容器归 host：`containers` 保留为 host 域的跨 target 视图页，不再是与 kind 平级的资源入口。

### 7.2 导航信息架构

菜单按 kind 分组重排，未就绪项由 `Readiness=planned` 机制隐藏（架构上已占位）：

| 菜单组 | 条目 | 路由 | 就绪度 |
|---|---|---|---|
| Resources | All Targets（全部资源统一列表，跨 kind 过滤） | `/targets` | ga |
| Resources | Hosts（主机；子条目 All Containers 跨主机容器视图） | `/targets?kind=host`、`/containers` | ga |
| Resources | Network（网络） | `/network` | ga |
| Resources | Storage（存储：storage targets + Disk Health 聚合；现 `/smart` 并入并保留跳转） | `/storage` | beta |
| Resources | Edge（边缘拨测） | `/edge` | planned |
| Resources | Core（核心：BGP 会话/路由总览提升为独立页；IP 库查询） | `/core`、`/core/geo` | BGP=ga 提升；IP 库=planned |
| Analysis | 流量流向（六维总览/分析图表） | `/flow`、`/flow/charts` | planned（Flow P1） |
| Analysis | Flow 明细（sFlow/NetFlow 的 IP/端口/对端分析——即用户口径的“sflow/netflow 分析”，exporter 采集健康属 `/settings/flow`） | `/flow/ips` 等 | planned（Flow P2） |
| Analysis | 监控分析（Aggregate Charts、Saved Graphs、Historical） | 现有路由 | ga |
| Analysis | 日志分析 | 未定 | planned（**独立 ADR 选型存储；ADR-SC-001 已裁撤 VictoriaLogs，日志分析立项不得默认复活它**） |

前端 manifest（§4.2）相应增加 `menuGroup/readiness` 字段；导航栏从 route/menu descriptor 生成分组，不再手写平铺条目。

### 7.3 未就绪功能的架构需求登记

以下按 §7.1/7.2 的 planned 项登记为架构需求，立项时先补各自 ADR/设计，不进入当前任何 tasklist 排期：

| 需求 | 摘要 | 关键依赖/前置 |
|---|---|---|
| storage:RAID 监控 | storcli/perccli/MegaCLI 输出解析为 capability 化采集；控制器/虚拟盘/物理盘三级库存 + 健康事件 | system agent capability 框架；`storage_devices` 模型泛化 |
| storage:Ceph | 集群健康/容量/OSD/PG 指标，经 mgr API 或 exporter 代理 | 集群型 target 建模（多节点 binding）；凭据管理 |
| storage:MinIO/RustFS/BeeGFS | 各自原生 metrics 端点接入,统一为 storage 数据集 | 同上;DatasetRegistry storage.* datasets |
| edge:四类拨测 | ping/DNS/HTTP/mtr 的任务定义、调度、时序与告警;多 vantage 对比 | edge_probe agent;plan 内周期 job;VM 拨测指标命名 |
| core:BMP | BGP RIB 级路由监控(前缀/AS path/撤销事件) | `agent_type=bmp`;存储选型(路由表规模评估,可能需 CH) |
| core:IP 库查询页 | 输入 IP/CIDR 返回归属(洲/区域/国家/省市/机构/ASN)与版本,含租户 override 视图 | 共享 flow-geo-v1/v2 loader（禁止 hub 维护第二套索引）；`/flow/geo/lookup` 服务化 |
| analysis:日志 | 主机/网元/应用日志的采集、检索与告警 | 独立 ADR:存储选型、采集通道、保留与脱敏;禁止无 ADR 复活 VLogs |

## 8. Dataset、指标查询和图表 CRUD

### 8.1 Dataset/Metric provider

静态 `IsKnownMetric()` 改为 registry。一个 dataset descriptor 至少声明：

```text
module/dataset key、provider(vm|clickhouse)、事实时间字段、metric
允许的 resource scope、filters、group_by、aggregations、value layers
默认 step、最大范围/series/points、retention、敏感维度
```

统一查询入口：

```json
POST /api/v1/query
{
  "dataset": "network.snmp_interface",
  "from": "2026-08-24T11:00:00Z",
  "to": "2026-08-24T12:00:00Z",
  "step_seconds": 60,
  "limit": 1000,
  "value_layer": "customer",
  "require_complete": false,
  "parameters": {
    "metric": "watchdog_snmp_if_in_bps",
    "device_id": "device_..."
  }
}
```

平台 QueryGateway 固定拥有 tenant/user 注入、module enablement、dataset policy、value-layer RBAC、tenant+dataset 并发、provider 全局并发、时间/行数/timeout/cancel 与错误 envelope；provider 只接收已收敛的 `QueryProviderRequest`，不得从 `parameters` 接收 tenant、任意 SQL 或 MetricsQL。provider 返回 JSON data 和统一 `request_id/schema_version/query_hash/as_of/source/value_layer/unit/timezone/step_seconds/policy_version/versions/completeness/next_cursor` 元数据。`require_complete=true` 时，partial、unknown、late 或 `complete_ratio != 1` 均拒绝返回，禁止用未知完整性冒充完整结果。

`query_dataset_policies` 是 tenant+dataset 的唯一持久 admission policy：dataset/layer enablement 与 `view_raw/view_supplier/view_customer` 授权是两个独立门，任一不满足即 fail closed；export 对应使用独立 `export_raw/export_supplier/export_customer` action。默认策略只开放 customer 层。provider endpoint/凭据和 provider-global 并发留在部署配置，避免把基础设施 secret 写进管理库。当前 VM adapter 只接受注册 metric 和 target/device/port typed selector，复用现有资源解析及三层流量视图；VM 无法证明期望样本完整性，因此返回 unknown completeness。Flow ClickHouse adapter 由 Flow 工作包按同一 contract 注册并复用现有 CH pool，平台不得再建第二个连接池。

管理 API 为 `GET/PUT/DELETE /api/v1/query-policies[/{dataset_key}]`；PUT/DELETE 必须带强 `If-Match`，并写审计。统一查询为 `POST /api/v1/query`；raw/supplier 成功读取另写敏感访问审计。保留现有 metrics API 一个兼容版本，内部迁移到同一 gateway 后才能删除旧路径。

### 8.2 地址统计维度快照与异步汇聚

`address_prefixes/address_sets` 是管理态，不能由采集热路径逐条查询 MySQL，也不能在配置修改时原地改变排队中 flow 的语义。core 增加不可变 dimension snapshot：

地址库管理只保留一套平台 API/CRUD，内部明确分成四层，避免把“导入的百万行底图”和“人工编辑”混在一张可变表中：

| 层 | 权威数据 | 写入方式 | 生命周期 |
|---|---|---|---|
| source artifact | 管理员上传的 MMDB/IPDB 原文件、sha256、格式/build epoch | 上传端点只做限额、落隔离区和格式探测，随后投递 `address_import` operation job | quarantined → importing → ready/failed；按引用和保留期清理 |
| imported base | `address_base_prefixes` 中按 `import_id` 隔离的 canonical v4/v6 前缀及 Geo/ASN/运营商字段 | importer 流式解码、批量写新 import，成功后一次 CAS 切换 active import；绝不原地清空当前 active | immutable；未激活/失败 generation 可回收 |
| draft override/set | `address_prefixes` 人工修正层、typed `address_sets`、`geo_dict/isp_operators/geo_lines` | 平台 typed CRUD + ETag；所有大批量变更先 preview 再原子 apply | 可编辑 draft，完整审计；不直接影响 worker |
| publication | `dimension_snapshots` 引用的签名 bundle | validate/approve/publish；从明确 UTC 分钟生效 | immutable；按事实引用保留，可 retire/rollback |

MMDB 使用成熟 Go reader 顺序枚举 network；IPDB reader负责格式/字段/语言校验，平台 importer 按其 trie 网络边界枚举，不以逐 IP 查询生成库。Geo 与 ASN 可各自拥有独立 active generation，也可上传 combined 库；发布编译器按两边区间边界做线性 overlay 后输出统一记录，不把两个来源的版本切换强绑成一次大事务。IPDB 的 `country_code/continent_code/china_admin_code/region_name/city_name/isp_domain/asn` 只写存在的 typed 字段，不用显示名冒充稳定 ID。上传请求本身不解析百万行、不持有数据库事务；operation job 按批次 checkpoint，最终切换 active import 的事务只包含状态和指针更新。

所有地址输入共用一个服务端数学内核，接受 canonical/non-canonical CIDR、裸 IP 和 `start-end`，持久化前统一输出最小 canonical CIDR 集。任一坏值整体拒绝，禁止像旧 EdgeManager `filter_map` 一样静默丢行。运算语义固定为：

- `union(A,B)`、`intersection(A,B)`、`difference(A,B)`；IPv4/IPv6 分族计算，跨族不相交；
- `complement(A,U)=U-A` 必须显式给有限 `universe U`，不提供隐式 `0.0.0.0/0` 或 `::/0`；
- `normalize/merge` 只合并重叠或真正相邻的区间，再转最小 CIDR，覆盖集合严格等于输入并集，绝不跨空洞；
- “提升到 `/24`”是独立 `cover` 操作，不是 merge。它可能把 `/25` 或任意 range 扩大到包含它的 `/24`；preview 必须返回 `added_addresses_v4/v6`、结果条目数和 `requires_confirmation=true`，apply 必须携带 preview digest 与显式确认。IPv6 不套用 `/24`，目标前缀必须单独给出；
- overlap lint 返回总冲突对数和有界明细；发布预览同时报告输入/规范化/输出前缀数、v4/v6 地址数、DAG 深度、selector evaluation 预算以及单 endpoint 最坏 address-set membership。超过编译上限只拒绝发布，不截断后继续。

管理操作统一是 `draft read → preview → compare-and-apply → publish`。preview 纯计算且不落库；apply 使用 draft revision/ETag 和 preview digest，revision 已变化返回 412；导入、批量合并、覆盖提升和发布写 operation/audit，支持失败重试但不重复应用。列表必须后端分页/filter/search，前端统一使用带列筛选的 VTable；百万级 base 不允许全量返回浏览器。

`address_sets.match_direction` 唯一允许 `in/out/both`，与 Flow 已归一化的业务方向一致；migration 038 将旧 `source` 显式迁移为 `out`、将手工 schema 中可能存在的 `destination` 迁移为 `in`。它不表示报文原始 src/dst，查询端若需要原始端点必须使用独立的 endpoint filter，禁止混用两套方向语义。

地域/线路模型沿用 EdgeManager 已验证的稳定引用思路，但不连接或回写 EdgeManager：`geo_dict` 保存 `continent → region → country → province → city` 邻接树；稳定主键为 `id`，`(tenant, kind, code)` 是自然唯一键，允许不同层级复用诸如 `AS` 的代码。`isp_operators` 保存运营商/教育网/云/搜索等稳定 ID 与 ASN 列表，`geo_lines` 保存父子线路节点及可空的 `geo_selector/operator_id/address_set_id` 组合。线路选择器按根到叶累积约束，查询/发布按稳定 ID，名称只负责显示。地址库发布仍导出 watchdog 自己的 immutable bundle；Flow 热路径只加载 bundle 做内存 LPM/range lookup。

```sql
CREATE TABLE dimension_snapshots (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  module_key VARCHAR(64) NOT NULL,
  dimension_key VARCHAR(64) NOT NULL,
  version BIGINT UNSIGNED NOT NULL,
  effective_from DATETIME(3) NOT NULL,
  object_ref VARCHAR(512) NOT NULL,
  checksum VARCHAR(128) NOT NULL,
  draft_digest CHAR(71) NOT NULL,
  bundle_schema_version INT UNSIGNED NOT NULL,
  entry_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
  prefix_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
  address_set_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
  max_address_sets_per_record INT UNSIGNED NOT NULL DEFAULT 0,
  status VARCHAR(16) NOT NULL DEFAULT 'active',
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NULL,
  retired_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  retired_at DATETIME(3) NULL,
  UNIQUE KEY uq_dimension_snapshot_tenant_id (tenant_id, id),
  UNIQUE KEY uq_dimension_version
    (tenant_id, module_key, dimension_key, version),
  UNIQUE KEY uq_dimension_effective
    (tenant_id, module_key, dimension_key, effective_from),
  KEY idx_dimension_effective
    (tenant_id, module_key, dimension_key, status, effective_from),
  CONSTRAINT fk_dimension_snapshot_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (status IN ('active','retired'))
);

CREATE TABLE dimension_snapshot_acks (
  tenant_id CHAR(26) NOT NULL,
  snapshot_id CHAR(26) NOT NULL,
  worker_id VARCHAR(128) NOT NULL,
  boot_id VARCHAR(128) NOT NULL,
  software_version VARCHAR(64) NOT NULL,
  checksum VARCHAR(128) NOT NULL,
  installed_at DATETIME(3) NOT NULL,
  PRIMARY KEY (tenant_id, snapshot_id, worker_id),
  FOREIGN KEY (tenant_id, snapshot_id)
    REFERENCES dimension_snapshots(tenant_id, id) ON DELETE CASCADE
);
```

发布动作把当前 prefixes、labels、sets、selector 和 direction 固化为校验过的 bundle；worker 通过 `object_ref+checksum` 加载并构建 LPM/selector 索引。配置 CRUD 只改变 draft，`publish` 才生成递增 version，并从声明的分钟边界生效。排队记录按 `event_time` 选择当时有效版本，而不是按实际消费时间套用最新配置。migration 040、manual prefix/set compiler、语义 digest CAS、不可覆盖对象、异步 operation job 及版本管理页已在 `3a7db545` 落地；active base overlay、签名/approve、retire/rollback、worker download/ack 和引用保留仍是交付门禁，不能因表已存在就宣称 PLAT-04A 完成。

地址统计有两种不同口径：

- `primary_prefix`：每个 endpoint 只取最长前缀，一个 address role 内互斥且可加总；未命中进入 `_unassigned`，保证守恒；
- `address_set`：prefix labels 可命中多个 selector，属于非互斥 tag 统计；单个 set 内可汇总，但不同 set 之间禁止相加，响应必须返回 `additive=false`。

flow-collect 只把 UDP datagram 封装为 RawFlow 写入唯一 Kafka topic；独立 flow-worker 按 partition 用 GoFlow2 解码，再按 event-time snapshot 异步完成 local/remote、primary prefix、address set、Geo/ASN、业务、六维和 CH 批次。执行采用固定 partition worker 和有界 batch，不允许逐 flow 启动 goroutine。worker/CH 故障形成 RawFlow lag；Kafka 生产故障最终形成显式 UDP data-loss interval。重分类在 RawFlow retention 内可重放，超出后只能使用仍在 TTL 内、包含 IP 与版本字段的 CH base fact。

```text
GET                      /api/v1/dimensions/address/versions
GET                      /api/v1/address-prefixes?cursor=&q=&family=&source=&geo_leaf_id=&operator_id=&asn=
POST                     /api/v1/address-prefixes
PATCH/DELETE             /api/v1/address-prefixes/{id}              (If-Match)
GET/POST                 /api/v1/address-sets
GET/PATCH/DELETE         /api/v1/address-sets/{id}                   (If-Match)
POST                     /api/v1/address-sets/actions/preview        (集合运算/规范化/cover)
POST                     /api/v1/address-sets/actions/apply          (preview digest + confirm expansion)
POST                     /api/v1/address-imports                     (multipart MMDB/IPDB)
GET                      /api/v1/address-imports/{id}
GET                      /api/v1/address-imports/{id}/prefixes?cursor=&q=&family=&country_code=&asn=
GET                      /api/v1/address-imports/{id}/lookup?ip=
POST                     /api/v1/address-imports/{id}/actions/activate
GET/POST                 /api/v1/geo/dictionary
GET/PATCH/DELETE         /api/v1/geo/dictionary/{id}                 (If-Match)
GET/POST/PATCH/DELETE    /api/v1/network/operators[/{id}]
GET/POST/PATCH/DELETE    /api/v1/geo/lines[/{id}]
POST                     /api/v1/dimensions/address/preview
POST                     /api/v1/dimensions/address/publish
GET                      /api/v1/dimensions/address/versions/{snapshot_id}
POST                     /api/v1/dimensions/address/versions/{version}/retire
GET                      /api/v1/dimensions/address/workers/status
```

### 8.3 可视化模型

用通用模型取代只绑定 port 的 aggregate graph：

```sql
visualizations(
  id, tenant_id, module_key, name, kind, visibility,
  owner_id, layout_json, refresh_seconds, created_at, updated_at
)
visualization_queries(
  id, tenant_id, visualization_id, sequence,
  dataset_key, query_json, display_json
)
visualization_bindings(
  visualization_id, tenant_id, resource_type, resource_id
)
```

`kind` 第一阶段支持 `timeseries/stat/table/pie/bar`；复杂 dashboard 由多个 visualization + layout 组合。query JSON 必须通过 DatasetRegistry 验证，不能保存任意 SQL/PromQL。原 `aggregate_graphs/items/ports` 迁移为 visualization/query/binding；`aggregate_graph_data` 在确认业务是否需要冻结快照后保留为 statistics snapshot，而不是继续扩张为第二套时序库。

CRUD/API：

```text
GET/POST                /api/v1/visualizations
GET/PATCH/DELETE        /api/v1/visualizations/{id}
PUT                     /api/v1/visualizations/{id}/queries
PUT                     /api/v1/visualizations/{id}/bindings
POST                    /api/v1/visualizations/{id}/preview
GET                     /api/v1/visualizations/{id}/data
```

### 8.4 管理界面

| 页面 | 核心能力 |
|---|---|
| 用户与角色 | 用户/角色 CRUD、成员关系、有效权限预览、审计 |
| 模块中心 | tenant 模块启停、版本、依赖和健康 |
| Collector 中心 | 列表、注册向导、一次性 enrollment、binding、心跳、能力/软件版本、plan preview/version diff/ack/LKG、canary rollout/rollback、轮换/吊销、job/运行历史和脱敏诊断 |
| Target/网元中心 | target/设备/端口列表、搜索、CRUD、发现、资源树、模块 tab、删除影响预览 |
| 指标与数据浏览器 | dataset/metric catalog、资源和维度筛选、地址前缀/address set、时序/表格预览、完整率、raw/supplier/customer 层选择 |
| 图表库与编辑器 | visualization 列表/详情/CRUD、query builder、资源 binding、预览和 dashboard layout |
| 统计与导出 | P95/峰值/平均/总量预览、异步任务、进度、重试、下载、过期和审计 |
| 修正规则 | supplier/customer 规则列表、版本 diff、preview、提交/审批/退役、命中范围 |

列表统一支持 cursor、搜索、状态/模块/target 标签筛选和可见列；详情页从 resource link 跳转，避免每个模块复制一套列表壳。

## 9. 统计、快照和导出

统计函数注册为 `sum/avg/min/max/count/p95/nth_peak/total_bytes/share/difference`，由 dataset 决定可用集合。统计必须先完成 value layer 选择，再做 P95/汇总；不同层不能在同一序列里混算。

`export_tasks` 演进为 provider-neutral：

```text
contract_version, dataset_key, query_json, query_hash,
value_layer(raw|supplier|customer), versions_json,
authorization_json, operation_job_id,
format(csv|parquet), retention_seconds,
artifact_schema_version, content_type, file_ref, checksum, size_bytes, row_count,
status, row_version, created_by, expires_at
```

`export_tasks` 只保存领域请求/产物，`operation_jobs` 唯一负责 lease、heartbeat、attempt、retry/backoff、cancel 和 crash takeover；禁止两个表各自实现一套执行状态机。导出 handler 通过 QueryGateway/DatasetProvider 读取，不复制 API SQL。任务创建时固化 canonical query、descriptor/query-policy/Geo/classification/adjustment version 和授权决定证据；重试必须使用同一 snapshot，不能重新解释当前配置。旧任务由 migration 明确标记 `contract_version=0` 和 `snapshot_complete=false`，保留兼容读取但不冒充可复现的新任务。

raw/supplier/customer 创建、执行和下载分别用 `export_raw/export_supplier/export_customer` 复核当前权限；`authorization_json` 只保存当时的决定证据，不能替代当前授权。产物以 schema version、content type、row count、size 和 SHA-256 自描述；下载校验元数据并写审计，过期删除也写审计。CSV 与 Parquet 共享同一逻辑 row schema，格式 writer 不得各自重新计算查询或修正规则。

当前 SNMP interface dataset 尚未拥有可按版本重放的 immutable adjustment publication，因此 contract v1 对参与计算的端口策略保存 canonical fingerprint：执行或自动重试前重新计算 fingerprint，不一致就终态失败并要求创建新导出，禁止拿当前策略静默重解释旧任务。VM provider 的 completeness 元数据为 unknown，导出执行不能把 unknown 当 complete；任务创建时冻结实际 query step 与容许缺失率，worker 对按时间戳合并后的样本执行 expected-sample 校验。目标级查询返回多端口 series 时必须先在同一 timestamp 求和，再计算 P95/平均/总量，禁止把各端口样本铺平后做 percentile。

```text
POST                     /api/v1/query
POST                     /api/v1/exports
GET                      /api/v1/exports
GET                      /api/v1/exports/{id}
POST                     /api/v1/exports/{id}/cancel
POST                     /api/v1/exports/{id}/retry
GET                      /api/v1/exports/{id}/download
DELETE                   /api/v1/exports/{id}
```

## 10. 原始、供应商、客户三层修正

### 10.1 语义

三层从同一个 immutable raw fact 派生，默认不串联：

```text
raw      = 采集、协议归一和 sampling 估算后的事实值
supplier = Apply(raw, effective supplier policy)
customer = Apply(raw, effective customer policy)
```

供应商层不等于设备 `vendor` 字段，API 固定使用 `supplier`，避免混淆设备厂商。若某合同确需 customer 基于 supplier，必须另立显式公式版本，第一阶段不支持隐式级联。

### 10.2 规则

```sql
CREATE TABLE metric_adjustment_policies (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  module_key VARCHAR(64) NOT NULL,
  dataset_key VARCHAR(128) NOT NULL,
  value_layer VARCHAR(16) NOT NULL,
  resource_type VARCHAR(64) NOT NULL,
  resource_id CHAR(26) NOT NULL,
  dimension_filter_json JSON NULL,
  operation VARCHAR(16) NOT NULL,
  operand_decimal DECIMAL(30,10) NULL,
  min_value DECIMAL(30,10) NULL,
  max_value DECIMAL(30,10) NULL,
  priority INT NOT NULL DEFAULT 0,
  version BIGINT UNSIGNED NOT NULL,
  effective_from DATETIME(3) NOT NULL,
  effective_to DATETIME(3) NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'draft',
  reason VARCHAR(512) NOT NULL,
  created_by CHAR(26) NOT NULL,
  approved_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  KEY idx_adjustment_lookup
    (tenant_id, dataset_key, value_layer, resource_type, resource_id,
     status, effective_from),
  CHECK (value_layer IN ('supplier','customer')),
  CHECK (operation IN ('scale','add','clamp')),
  CHECK (status IN ('draft','active','retired')),
  CHECK (effective_to IS NULL OR effective_to > effective_from)
);
```

要求：

- raw 永远不可修正或覆盖；
- 操作为确定性 decimal `scale/add/clamp`，删除当前随机区间修正语义；
- dimension filter 只允许 dataset descriptor 声明的字段，例如 direction、category、business、ASN、地址集；
- 匹配顺序为 resource specificity → dimension specificity → priority → version；同优先级重叠 active 规则拒绝发布；
- draft→active 需要 `configure_adjustment`，可配置双人审批；active 不原地修改，创建新 version；
- 查询响应和导出必须返回 policy ID/version、是否命中和 raw 值可见性；
- 历史默认按查询时指定的 policy version 重现，不把修正值写回事实表。

现有 `port_policies/traffic_policy_defaults` 迁移为 supplier/customer policy；迁移前必须由业务确认原 corrected 值属于哪一层。无法确认的规则保持 legacy 只读，不自动归类。

```text
GET/POST                 /api/v1/adjustment-policies
GET/PATCH                /api/v1/adjustment-policies/{id}
POST                     /api/v1/adjustment-policies/{id}/submit
POST                     /api/v1/adjustment-policies/{id}/approve
POST                     /api/v1/adjustment-policies/{id}/retire
POST                     /api/v1/adjustment-policies/preview
GET                      /api/v1/adjustment-policies/effective
```

active policy 不允许 PATCH 原地改变公式；修改动作创建下一 version。preview 同时返回 raw、目标层结果、命中顺序和冲突，便于业务确认后再审批。

## 11. 平台管理表与所有权

| 表/域 | 所有者 | 处理 |
|---|---|---|
| tenants/users/roles/user_roles/permissions | core identity/RBAC | 增加 external subject、CRUD、可注册 resource/action；保留数据 |
| targets | core resource | 保留；`kind` 校验改 registry |
| network_devices/network_ports/inventory | network module | 保留；flow 只引用 stable ID |
| target_agents | legacy agent | 迁移为 collector_agents + collector_bindings 后删除写路径 |
| agent_run_history | core collector | agent FK 迁移；支持连续 collector health event |
| aggregate_graphs/items/ports | legacy visualization | 数据迁移到 visualizations 三表；兼容 API 一版 |
| aggregate_graph_data | statistics snapshot | 冻结用途，禁止作为通用时序库 |
| export_tasks | core export | 增加 dataset/query/layer/version；旧 target/port 列作为兼容投影 |
| port_policies/traffic_policy_defaults | legacy correction | 迁移到 adjustment policy，停止随机修正 |
| dimension_snapshots | core dimension | 新增；保存 prefix/set 等不可变发布版本、effective time、bundle checksum 和归档引用 |
| audit_logs | core audit | 保留并增加 request/correlation ID、before/after checksum |
| tenant_modules | core module | 新增，保存 enabled/config version/status |

每个 migration 必须同时有 up、兼容读取期和清理门；不采用一次上线同时重命名全部表。`install/init.sql` 应由 migration snapshot 生成，CI 在空库执行全量 migration并与 snapshot 比对。

## 12. Flow 数据面边界

平台文档只冻结宿主边界，不复制 Flow 的 topic、schema、DDL、采样和故障细节；唯一现行定义见 [flow-pipeline-adr.md](flow-pipeline-adr.md) 和 [flow-module-design.md](flow-module-design.md)。

- Kafka 是强制依赖，只有 `watchdog.flow.raw-v1` 一个 Flow 数据 topic；
- collector 只做 UDP、来源准入、RawFlow 和 Kafka，不连接 MySQL/CH/VM；
- worker 在 Kafka partition 内用 GoFlow2 有序解码、归类和批写 CH，成功后提交 offset；
- 平台只提供 identity/RBAC、collector/exporter、签名 plan、snapshot、query/export/job/audit；
- 禁止恢复 collector WAL、normalized/collect-state topic、state restore/cleanup API 或 Flow VM 双写。

Flow 执行状态只维护在 [flow-module-tasklist.md](flow-module-tasklist.md)，平台问题只维护在 [platform-refactor-tasklist.md](platform-refactor-tasklist.md)。

## 13. 分期顺序

### Platform P0：生产闭环

- production Auth adapter 和 `/api/v1` 挂载；
- user/role/tenant CRUD 与完整审计；
- migration snapshot/parity；
- dev admin 与生产配置强隔离。

验收：生产登录用户能得到正确 tenant/grants；跨租户 ID 全部 404/403；空库 migration 与 snapshot 一致。

### Platform P1：模块与 collector 基座

- Module/Resource/TargetKind/Dataset registry；
- collector_agents/bindings、enrollment、rotation、plan/heartbeat；
- dimension snapshot、address prefix/set preview/publish 和 event-time version；
- SNMP/system agent 迁移并保持行为；
- tenant module enablement 和健康页。

验收：同一 collector 可绑定多个 target；禁用 module 后停止 worker/入口但不删数据；旧 agent 无中断迁移。

### Platform P2：查询、图表和导出统一

- QueryGateway + VM provider；
- visualization 三表/API/UI；
- provider-neutral export task；
- 旧 metrics/aggregate graph/export API 兼容适配。

验收：现有 SNMP/系统图和导出结果迁移前后一致；模块不能提交任意 SQL/PromQL；权限过滤覆盖 query/export。

### Platform P3：三层修正

- adjustment policy/version/审批；
- raw/supplier/customer 权限与查询；
- 旧 correction 迁移/冻结；
- billing/export 固化 policy version。

验收：同一 raw+policy version 可重复得到完全一致结果；raw 永不改写；重叠规则不能 active；三层权限测试通过。

### Flow P1 及以后

Flow 与 Platform 使用独立任务集并可并行推进；Flow 遇到平台阻断点时只做最小平台修复并登记。Kafka 集群、collector enrollment、RawFlow collector、Kafka 后 GoFlow2 worker 和 dimension replay 属于 Flow P1；三层修正相关导出依赖 Platform P3。

## 14. 首批实施工作包

1. 在 hub production server 挂载 watchdog API，完成 PocketBase/OIDC→AuthContext adapter；
2. 增加 user/role CRUD 和 tenant 列表，收敛两套身份字段；
3. 引入 module/resource/dataset registry，先把现有 SNMP/system 注册进去验证契约；
4. 迁移 `target_agents` 为通用 collector + M:N binding；
5. 把 metric API 包到 QueryGateway/VM provider 后面，保持响应兼容；
6. 建 visualization 新模型并迁移 aggregate graph；
7. provider 化 export worker；
8. 实施三层 adjustment policy，移除新请求使用 legacy 随机 correction；
9. 建 dimension snapshot/publish 基座，先用现有 address prefixes/sets 验证版本和 event-time 语义；
10. 按 Flow tasklist 接入 `flow-collect→RawFlow Kafka→flow-worker/GoFlow2→CH`，不允许绕开平台契约或同步计算高成本统计维度。

每个工作包独立 migration、单测、API contract test 和回滚说明；不能以一次超大重构同时替换身份、agent、图表和 flow 数据面。

## 15. 全生命周期闭环契约

本节是所有模块的强制平台规范。Flow、SNMP、system、图表、导出和后续模块不得各自发明状态、分页、删除或错误语义。成熟系统的价值不只在高吞吐，而在于任何对象和数据都能回答：谁创建、当前为何状态、发生过什么、能否修改、谁可见、如何退役、何时删除、失败如何恢复、结果是否完整。

### 15.1 三类生命周期与独立健康态

| 生命周期 | 标准阶段 | 必须回答 |
|---|---|---|
| 数据 | accepted → durable → normalized → enriched/base → derived/served → archived → expired → purged | 每阶段 owner、ID、版本、完整性、retention、checkpoint、重放来源和销毁证据 |
| 功能 | proposed → experimental → canary → GA → deprecated → read-only → removed | 开关作用域、兼容矩阵、迁移、使用量、回滚条件、移除版本和数据处理 |
| 操作/资源 desired state | draft/pending → active ↔ suspended → retired → deleted → purged | 合法动作、前置校验、幂等性、并发冲突、依赖影响、恢复窗口和审计 actor |
| 运行 observed health | unknown → warming → healthy ↔ degraded ↔ stale/unavailable | 由探测/reconciler 计算，不接受用户 PATCH，也不因健康抖动改变配置状态 |

资源的 `desired_state` 与运行时 `observed_state` 分开。API 修改 desired state，reconciler 推进 observed state；只有 `observed_generation == generation` 才能宣称配置已生效。一次性 job 不复用资源状态机，统一使用：

```text
queued → running ↔ paused → validating → succeeded
             ├──────────────→ failed
             └──────────────→ cancel_requested → canceled
```

所有迁移幂等；终态 action 重复调用返回原结果而不是 500。`failed` 必须区分可重试/不可重试，记录 stable error code、最后 checkpoint 和下一步；取消是请求，不承诺正在执行的外部原子操作可瞬间停止。

### 15.2 管理资源公共字段

所有可 CRUD 管理对象必须有 stable ID、tenant、乐观锁、创建/更新时间和删除语义；其余字段只在对象确实具备发布、运行或恢复生命周期时使用，不能为了“统一”给静态字典强塞空状态。字段可直接位于资源表，也可由同一公共基表提供，但不能只靠日志反推当前状态：

| 字段 | 类型语义 | 约束 |
|---|---|---|
| `id` | ULID/CHAR(26) | 服务端生成、全局稳定，不复用已删除 ID |
| `tenant_id` | CHAR(26) | 所有唯一键和查询都含 tenant；服务端从 AuthContext 注入 |
| `name`/natural key | 稳定展示键 | tenant 内唯一；大小写/空白规范化规则冻结 |
| `status` | 有状态资源的 desired state | 只允许状态机转换，不接受任意字符串 PATCH；纯字典/只读事实不需要 |
| `generation` | 需发布配置的版本 | spec 有效变化才递增；无发布过程的普通 CRUD 不需要 |
| `observed_generation` | 外部应用的已生效版本 | 仅 reconciler/agent 更新；只有需下发/异步生效的资源需要 |
| `row_version` | 乐观锁版本 | 每次变更递增，映射强 ETag；PATCH/DELETE/动作要求 `If-Match` |
| `created_at/by`、`updated_at/by` | UTC 毫秒与 actor | actor 支持 user/service/system，禁用用户后历史仍可解析 |
| `retired_at`、`deleted_at/by` | 可退役/删除资源的生命周期时间 | delete 默认 tombstone；retire 与 delete 不能混为一谈 |
| `purge_after` | 有恢复期资源的最早物理删除时间 | 由 retention/依赖计算，普通调用者不可提前 |
| `labels`/`annotations` | 可选有界 metadata | 只在资源需要检索标签时使用；禁止把高基数事实塞入 |
| `last_error_code/detail` | 有运行态资源的摘要 | detail 脱敏且限长；完整诊断进受控日志/事件 |

同类对象必须使用同一语义；不同生命周期的对象不要求字段完全同构。旧表通过兼容 migration 补必要字段，API 在兼容期同时读旧值，但只写新模型。

异步任务公共字段至少包括：`id/tenant/type/status/idempotency_key/request_hash/progress_total/progress_done/checkpoint/result_ref/error_code/error_detail/cancel_requested_at/started_at/heartbeat_at/finished_at/expires_at/row_version/created_by/created_at/updated_at`。worker 领取任务使用 lease/heartbeat；lease 过期可恢复，但必须从 checkpoint 继续而不是从头重复产生副作用。

公共骨架已由 MySQL migration `017_operation_jobs.sql` 落地：`operation_jobs` 保存上述公共生命周期字段，并增加 `lease_owner/lease_token/lease_expires_at/next_attempt_at/attempt_count`。migration `028_operation_job_watermarks.sql` 另提供常数规模的 `(tenant_id,job_type,partition_key) -> UInt64 watermark`，供周期 producer 保存长期单调游标；终态 job 有保留期，严禁从 job 历史反推永久水位。producer 必须先 durable enqueue 再以 `GREATEST` 推进 watermark，使崩溃最多重复入队而不会跳过工作。后续 reclass、export、probe 和 purge worker 统一使用 canonical request hash、`FOR UPDATE SKIP LOCKED`、lease token/expiry/row-version fencing 与 checkpoint；checkpoint/requeue/terminal transition 必须和 audit 同事务提交。旧 Flow collect-state cleanup 是已删除的错误试用，不再作为 operation job 的参考实现；每个新 job type 必须在自己的切片中补状态机、重启接续和副作用幂等测试。

### 15.3 HTTP CRUD 与动作统一语义

| 场景 | 请求契约 | 成功 | 冲突/错误 |
|---|---|---|---|
| List | `limit`、opaque `cursor`、字段白名单 filter、稳定 sort + `id` tie-breaker、可选 `include_deleted` | `items,next_cursor,total_estimate,as_of` | 非法字段/排序 400；cursor 过期 409/明确重开 |
| Create | `Idempotency-Key`、可选 `dry_run=true`；服务端忽略 body tenant/id | 201 + `Location` + ETag；重复同 key/request 返回同资源 | 同 key 不同 body 409；natural key 重复 409 |
| Get | ID，必要时 `fields/expand` 白名单 | 200 + ETag；tombstone 默认 404 | 跨租户统一 404；无 sensitive field 权限则字段省略并回显 warning |
| Replace/Patch | `If-Match` 必填；PATCH 使用 JSON Merge Patch；immutable 字段拒绝 | 200 + 新 ETag；无有效变化保持 generation | ETag 过期 412；状态动作伪装为 PATCH 返回 409 |
| Action | `/validate\|activate\|suspend\|resume\|retire\|restore` + Idempotency-Key | 同步 200 或异步 202/job；终态重复调用幂等 | 非法状态转换 409，返回 current/allowed transitions |
| Delete preview | `GET /{id}/deletion-impact` | 依赖、事实保留、凭据、导出、purge_after、可恢复性 | 无权限 403；对象不存在/跨租户 404 |
| Delete | `If-Match` + confirm token；默认 tombstone/retire | 202 purge job 或 204 tombstone | 有阻断依赖 409；禁止隐式 cascade facts |
| Restore | tombstone ID + If-Match | 恢复到 retired/draft，不自动 active | 超恢复窗口 410；natural key 已被占用 409 |
| Purge | 独立 `purge` 权限、二次确认、异步 job | 验证所有存储为空并生成 destruction receipt | retention/legal hold/账单依赖未满足 409 |
| Bulk | ≤100 项，每项 idempotency key；默认非原子 | 207 每项结果；大批量转 job | `atomic=true` 只允许同一事务/同一存储能证明原子性时使用 |

GET/HEAD 不产生副作用。POST 动作不得靠字符串 message 表示状态。删除和 purge 永不使用 query 参数绕过确认。所有响应带 `request_id`，写操作审计包含 before/after 摘要、actor、reason、request/idempotency key；敏感字段只记录 hash/是否变化。

统一错误体：

```json
{
  "error": {
    "code": "RESOURCE_VERSION_CONFLICT",
    "message": "resource changed since it was read",
    "retryable": false,
    "details": {"current_etag": "...", "allowed_transitions": []}
  },
  "request_id": "01..."
}
```

error code 稳定且进入 API contract；不得让客户端解析中文 message。依赖超时为 503/504，限流为 429 + `Retry-After`，乐观锁失败为 412，业务状态冲突为 409，已物理删除为 410。

### 15.4 查询输出的准确性、完整性和一致性

所有 DatasetProvider 返回统一 envelope：

```text
data
meta {
  request_id, schema_version, query_hash, as_of,
  source, value_layer, unit, timezone, step,
  dimension/config/adjustment_versions,
  available_from, available_to,
  complete_ratio, late_ratio, unknown_ratio,
  partial, degraded_intervals[], warnings[], next_cursor
}
```

闭环要求：

- **准确性**：单位、采样/修正公式、聚合函数、时间边界和 rounding 有版本化定义与 golden fixture；raw 不被覆盖；
- **完整性**：expected/received/published/decoded/base/served 各阶段计数可对齐，缺失、迟到、DLQ、权限裁剪和 TTL 截断分别回显；
- **一致性**：在线查询、保存图表、统计和导出使用同一 canonical query、snapshot 和 value-layer version；同一 idempotency key/result hash 可复现；
- **高效率**：QueryGateway 先估算 cost，选择满足范围/维度的最粗可用 resolution；禁止全表扫描、无界 group-by 和先查全量再在 API 过滤；
- **时态**：列表使用 `as_of` 和 stable cursor；长查询/导出固定版本，不在翻页过程中混入新配置；
- **降级**：partial 只能显式返回；若调用方要求 `require_complete=true`，任何缺口返回 409/503 而不是残缺 200。

### 15.5 权限闭环

| Action | 典型能力 | 额外约束 |
|---|---|---|
| `list` | 看资源存在和非敏感摘要 | 总数、筛选候选也按资源权限裁剪 |
| `get` | 看详情 | IP、凭据状态、错误细节可另需 `read_sensitive` |
| `create/update` | 修改 draft/spec | 不能隐式 activate；字段级 allowlist |
| `operate` | validate/activate/suspend/resume/retry/cancel | 不能修改 RBAC/retention/policy |
| `delete/restore` | tombstone 与恢复 | 必须看 deletion impact；恢复不自动 active |
| `purge` | 物理销毁 | 独立高危权限、审批、双人原则和 receipt |
| `approve` | 规则/snapshot/修正发布 | 提交人与审批人按策略分离 |
| `export` | 创建/下载导出 | 创建、查看状态、下载文件分别校验；分享链接有 TTL/次数 |
| `read_sensitive` | 完整 IP、证据、错误上下文 | 默认列表脱敏；所有读取审计 |
| `admin` | 角色、配额和系统策略 | 不能替代 tenant 数据 purge 的审批链 |

Service account 只拥有其协议所需 action 和资源 binding；collector 不能调用人类 CRUD，QueryGateway/API reader 不能写 CH，flow-collect 不持业务数据库凭据。缓存 key 必须包含 tenant、subject/grant version、value layer 和 query/version hash，权限撤销时失效。

### 15.6 数据保留、删除与销毁

| 数据类 | 默认删除行为 | 恢复/销毁要求 |
|---|---|---|
| 管理配置 | tombstone，停止新引用 | 恢复窗口内可 restore；自然键占用需冲突处理 |
| 不可变 snapshot/字典 | retire，不原地修改 | 至少保留到所有引用事实 TTL 结束；checksum 可验证 |
| 流量/指标事实 | 不随 target/exporter tombstone 立即 cascade | 按 tenant retention/partition 删除；完成前查询返回 purge_pending |
| Kafka/DLQ | retention 自动回收 | 过期和人工清理形成区间/receipt；DLQ 遵循敏感策略 |
| 导出文件 | task metadata 与 object 分离过期 | 删除对象、撤销分享、物理 object delete 和 checksum 验证 |
| 凭据 | 先 revoke，再删除 secret material | 证明旧 token/cert 失败；审计不保存 secret |
| 审计/销毁证明 | append-only、单独 retention | purge 后只保留不含业务 payload 的最小合规记录 |

tenant purge 固定顺序：冻结写入 → 撤销 collector/service credentials → 等待/终止 jobs → 删除导出和缓存 → 删除 Kafka 可定位数据 → 删除 CH/VM facts → tombstone/purge MySQL 配置 → 校验所有 provider 查询为空 → 生成带计数/checksum 的 destruction receipt。任一步失败保持可恢复 checkpoint，禁止报告成功。

### 15.7 Desired-state reconciler 与漂移治理

参考成熟项目 orchestrator，平台增加只做**安全收敛**的 reconciler：

1. 根据 versioned descriptor/migration/plan 生成 desired inventory；
2. 读取 MySQL、Kafka、CH、VM、collector ack 和对象存储 actual state；
3. 分类为 `in_sync/drift_safe_to_fix/drift_needs_approval/incompatible/unknown`；
4. 自动动作仅限幂等创建、补字段/索引、增加 partition、刷新字典、重新下发 plan 等无损操作；
5. 缩 partition、降副本、缩 TTL、drop column/table/topic、批量删除和不可逆迁移只生成 change plan，审批后由 job 执行；
6. 每轮记录 generation、diff hash、耗时和结果，持续失败进入健康页和发布阻断。

Schema/catalog/config 只有一个 source of truth；init、migration、protobuf、DatasetDescriptor、API DTO 和导出列由同一版本清单校验，避免“数据库已改但 API/前端仍按旧字段”的半升级。

### 15.8 性能、配额与公平性

每个阶段同时配置 concurrency、queue、time/size limit、tenant quota 和 overload policy：

| 层 | 必须限制 | 超限行为 |
|---|---|---|
| 管理 API | body、filter、bulk 数、并发写、每用户速率 | 400/413/429，绝不进入无界 goroutine |
| Query | range、points、series、group cardinality、scan bytes、并发/cost | 拒绝或建议更粗 resolution；不静默截 Top |
| Export | 行数/bytes、并发、tenant 队列、文件 TTL | 排队/取消/分片；在线查询优先 |
| Collector | source PPS、socket/Kafka queue、每 exporter 公平性 | 拒绝未知来源/背压/明确 loss interval |
| Kafka/worker | batch bytes/records/time、lag、inflight、partition ownership | 扩容/暂停 partition/停止 backfill |
| CH/VM | 各自的 insert/query concurrency、memory、timeout、disk watermark | CH 保护 Flow base 写入并降级 derived/长查询；VM 保护 SNMP/系统与 pipeline 监控 |

配额必须显示 current/limit、拒绝原因和调整审计。压测包含 noisy-neighbor：一个 tenant 的高基数查询、坏 exporter 或大导出不能违反其他 tenant 的收数和查询 SLO。容量变更以新 plan/version 发布，并重复单组件、端到端、故障和 soak 测试。

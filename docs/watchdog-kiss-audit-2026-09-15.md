# KISS 全面审计 — install & target（device/agent/network/flow）+ PocketBase 残留

日期：2026-09-15。范围：多次并行变更（地址库、RBAC、flow 契约恢复、SNMP 预算、发布去签名等）后，对 **install/bootstrap** 与 **target（device、agent、network=snmp/core、flow；system 延后）** 做全面审计，并核销 [watchdog-kiss-refactor-tasklist.md](watchdog-kiss-refactor-tasklist.md) 中 PocketBase 残留清理项。方法：四路只读子代理并行审计（build/vet/route 静态追踪 + 真实 schema 应用验证）。

## 结论

活跃服务路径（`cmd/watchdog-server` → `internal/server` → `deploy/schema/mysql` + `frontend/`）在所有四个维度都**连贯（coherent）**：install 核心切片、device 根、agent 注册、SNMP/flow 查询均无 blocker、无 major 代码缺陷；`go build ./...` 与 `go vet` 通过。**PocketBase 已彻底退出活跃路径。**

风险集中在三个横切主题，均可在不做 KISS-08 全量删除的前提下**现在**修复接线：

1. **遗留生态危险接线** — `internal/watchdog` + `deploy/migration/mysql` + `install/init.sql` + `cmd/watchdog-install` + 遗留 worker，仍被 CI / Makefile / scripts / 文档**主动引用**，其中遗留安装器会把 KISS server 装成开不了机的库。
2. **ClickHouse 启动脆弱** — 配好但连不上的 CH 会让**整个** server 起不来（含 RBAC/device/address）；出厂 config 空密码与 dev CH 密码不符 → 首次 `POST /install` 返回难懂的 500。
3. **仓库卫生** — 根目录 `watchdog-server`（60MB 二进制）与 `dbbak-origin-*.sql`（DB dump）未跟踪且未 gitignore（dump 有轻度泄露风险）。

---

## 1. PocketBase 残留（KISS-01 核销）

**PB 已彻底退出活跃路径**：go.mod/active Go/前端 deps+src/clean schema/编译产物 `strings watchdog-server` 全部无 PB。残留全在未发布的遗留物：

| 级别 | 项 | 证据 | 处置 |
|---|---|---|---|
| 需处理 | 死的 PB-hub Dockerfile，仍在 CI（构建**已删除**的 `internal/cmd/hub`，带 PB `serve` + `/watchdog_data` volume） | `internal/dockerfile_hub:29,31,34`；`.github/workflows/docker-images.yml:18,71` | 删除或改指 `cmd/watchdog-server`（修复 CI + KISS-01E 门禁） |
| 需处理 | 遗留 init/migration PB 时代字段与文字（= 清单 line 77） | `install/init.sql:2020-2028`（`auth_provider`/`external_subject_id`，`tenant_id`×363）；`deploy/migration/mysql/030:1-2` "PocketBase" 注释 | 从 v2 baseline 重生成 `init.sql`；归档遗留 migration 树 |
| 需处理 | GitHub 模板指向 PB 后台 `/_/#/logs` | `.github/ISSUE_TEMPLATE/bug_report.yml:124`、`.github/DISCUSSION_TEMPLATE/support.yml:92` | 改用户可见文字 |
| KISS-08 延后 | 遗留 `internal/watchdog/*` 树仍被活跃代码 import | 见主题 1 | 先断 import 再删 |

KISS-01A 门禁是提交顺序纪律说明（非代码可补救）；KISS-01E 门禁的代码删除项由上表 1、2 阻塞。

## 2. Install / bootstrap

**核心切片 SOLID**（已逐条验证）：空 MySQL 起 0 表；装前登录 `428`；install 应用 schema 后在 `FOR UPDATE` 事务内恰好建 1 个管理员；重复 install `409`；migration 校验和防篡改；半失败重试幂等；前端 `/install` 门正确。migration 0025–0030 在全新 0001→0030 顺序应用下均正确。

| 级别 | 项 | 证据 |
|---|---|---|
| Major | **双 MySQL 安装系统**：`cmd/watchdog-install`+`deploy/migration/mysql`+`install/init.sql` 建出**不兼容**的 `watchdog_installation`（`id VARCHAR 'default'` vs KISS `id=1/schema_version`）→ KISS server 启动报 "Unknown column 'schema_version'"。仍接在 `Makefile:111-112`、`scripts/watchdog-dev-db.sh`、`docs/watchdog-kiss-architecture.md:115` | `internal/watchdog/install.go:86-108` vs `deploy/schema/mysql/0001_baseline.sql:29` |
| Major（文档已缓解） | **CH 首次运行陷阱**：install 本身要连 CH；出厂 `password_file:""` 与 dev CH `watchdog-local` 不符 → 首次 `POST /install` 返回 500 裹 `AUTHENTICATION_FAILED` | `server.go:128-133`；`config/watchdog.yaml`；`compose.flow-dev.yml:59` |
| Minor | 地址库 seed（新增）未被 install 加载，也未在安装文档提及 → 全新装完 geo 为空 | `docs/watchdog-install.md` |
| Minor | `ensureBuiltinMIBModules` 每次启动强制 `enabled=1`，覆盖管理员的禁用 | `snmp_mib_modules.go:131-133` |
| Minor | `0026_user_preferences.sql` 是永久 no-op（按不可变 migration 规则保留；`0028` 为准） | — |

## 3. Network — SNMP / core / flow

**连贯，纯 ClickHouse**：SNMP（`internal/snmpch` 包 `flowch.NativeInserter`）与 flow（`internal/flowquery` 五 runner 共享 CH 池）服务路径无 VM/QueryGateway/DatasetProvider 活跃分支；对退役泛化栈的引用**全是注释**。build/vet 干净。原任务两条前提已过时并纠正：查询收敛工作**已提交**；泛化栈是**真延后非坏删**（仍被 `NewBackendRuntime` 的非-flow worker 使用）。

| 级别 | 项 | 证据 |
|---|---|---|
| Major | **CH 启动 = 全 server fail-fast**：配好但连不上的 CH 让 `prepareRuntime` 失败 → 整个 server 起不来（含 RBAC/device/address），与运行时降级到 503 不对称；空 CH 配置则**静默**降级 | `server.go:128-136,257-287`；`install.go:154-186` |
| Major（延后） | 遗留 `watchdog-aggregate-rollup`/`watchdog-export-worker` 已被 in-server CH worker 取代，且查询 KISS schema 中**不存在**的表（`collector_agents INNER JOIN tenants`）→ 对 KISS 库运行即失败 | `runtime.go:542,582`；`flow_rollup_jobs.go:451-452` |
| Minor | 两套 config schema 并存；遗留 worker config 仍**必填** VM base_url，而 server 从不用 | `internal/watchdog/config.go:971` |
| Cosmetic | `snmp_vmquery.go` 名字像 VictoriaMetrics 实为 CH 兼容端点 | `snmp_vmquery.go:16-24` |

`snmp_aggregate.go` 的 chart 预算与 scoped-read 预算（commit `e2ad0a14`）分离正确并被 aggregate-graph 复用。

## 4. Target — device / agent

**连贯，无 blocker/major。** 单一 `devices` 表单写路径；`/targets` 与 `/network/*` alias 干净委派同 handler（无双写、无旁库），`system→host` 只在写入归一、`host→system` 只在 `/targets` 响应 DTO；device/port RBAC scope 完整且注入安全；agent 机器 API 单一鉴权路径 + Ed25519 不可变签名计划；`operation_jobs` 收敛单 store。你快照中未提交的 RBAC 文件**已提交**（`dbfba45f`）。

| 级别 | 项 | 证据 |
|---|---|---|
| Minor | `listTargets` 未调 `validDeviceListFilters`，`/targets?status=bogus` 返回 200 空而非 400（与 `/devices` 不一致） | `devices.go:495` vs `:128` |
| Minor | `replaceUserAccess` 对 device/port/billing/graph 对象 ID 未先做存在性校验 → 未知 ID 返回原始 SQL/FK 错（500）而非 400/422 | `handlers_grants.go` |
| Minor（路线图） | `probe` kind 可注册且有 capability/绑定校验，但**无消费进程**（Tier2 被动探测尚在设计） | `agents.go:977-981` |
| Negligible | 动态分组 label key 未拒 `"`/`\`（参数化、fail-closed，仅错误质量） | `device_organization.go:759-777` |

---

## 横切主题与建议动作

### 主题 1 — 遗留生态危险接线（跨 §1/§2/§3）
- **安全隔离遗留安装器**（Major）：确认 `deploy/schema/mysql`+HTTP `/install` 为唯一路径后，删除/隔离 `cmd/watchdog-install`、`install/init.sql`、`deploy/migration/mysql`、`Makefile:111-112` 的 `watchdog-install` target、`scripts/watchdog-dev-db.sh`；修正 `docs/watchdog-kiss-architecture.md:115`。若须暂留，加**响亮守卫**拒绝对 KISS 库操作。
- **护栏遗留 worker**（Major/延后）：KISS-08 前，停止发布/部署 `watchdog-aggregate-rollup`/`watchdog-export-worker`，或加 schema 守卫防其对 KISS 库运行。
- **PB Docker/CI**（需处理）：删 `internal/dockerfile_hub` + 两处 CI 引用。
- 均属 KISS-08 teardown，但**接线**可现在拆除。

### 主题 2 — ClickHouse 启动语义（跨 §2/§3）
- **决策并文档化 CH 启动语义**（Major）：要么让 CH「配了但连不上」时非致命降级（log + `snmpMetrics/clickHouse=nil` + health `clickhouse:false`），与空配置路径一致，使 CH 故障不拖垮 RBAC/device/address；要么明确 fail-fast 并在配置为空密码时**响亮报错**而非静默降级。区分 CH-auth 与其他错误，给 `install_failed` 可操作原因。
- 出厂 YAML 保持无密码；`make dev-*` 可自动导出密码文件。

### 主题 3 — 仓库卫生（跨 §3/§4）
- gitignore 或删除根目录 `watchdog-server`（60MB）与 `dbbak-origin-*.sql`（DB dump，勿提交）。

### 小项集合
- 安装文档引用地址库 seed（`deploy/seed/load-address-library.sh`）；或首次安装时若文件存在则自动加载。
- `listTargets` 补 `validDeviceListFilters`；`replaceUserAccess` 预校验对象存在性。
- `ensureBuiltinMIBModules` 更新时不要无条件 `enabled=1`。

> **精确到代码/表/字段/接口级的待清理清单 + 清理影响评估 + 执行状态**见 [watchdog-kiss-cleanup-ledger.md](watchdog-kiss-cleanup-ledger.md)（本审计的下沉版：L1-L19 代码/符号、S1-S12 schema、I1-I5 接口，逐项依赖+删除影响+严格删除顺序）。

## 按 KISS 包归属

- **KISS-01（PB）**：主题 1 的 Docker/CI + init/migration 文字 + GitHub 模板 → 可核销 KISS-01E 代码删除门的这部分；`internal/watchdog` 删除仍属 KISS-08。
- **KISS-08（遗留清理）**：遗留安装器、遗留 worker、`internal/watchdog` 树、VM config、`deploy/migration/mysql` 全量删除 —— 但危险**接线**（Makefile/scripts/CI/docs）建议提前拆。
- **install 硬化 / 运维**：CH 启动语义（主题 2）、seed 接线、仓库卫生 —— 独立小包，不阻塞。

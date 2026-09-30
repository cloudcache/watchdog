# Watchdog 上线与发布准备（2026-09-28）

本文为发布前整理：①当前代码的生产审计状态；②前后端正式部署文档与环境要求，重点是磁盘容量、数据库、ClickHouse 索引与聚合配置、ClickHouse/Kafka 优化。依据为本仓库当前状态 + 生产 .18 本轮实测 + 三次磁盘打满事故的教训。

---

## 1. 代码状态：生产审计基线

分支 `main` 长期领先 `origin/main`，远端不是当前发布事实源。发布必须以已审计提交或 tag 为唯一输入，禁止从脏工作区现场构建。

### 1.1 5m 生命周期改动（已进入当前分支，待生产部署验收）

| 文件 | 内容 | 测试状态 |
|---|---|---|
| `deploy/migration/clickhouse/021_flow_atomic_5m.sql` | `flow_interface_traffic_5m` + `flow_aggregate_5m` 两张 5m 表 | dev CH 26.3 建表/DDL 实测通过 |
| `deploy/migration/clickhouse/022_flow_records_event_time_index.sql` | `flow_event_time_minmax` 跳数索引 | dev 实测通过；生产实测子小时裁剪 ~6x |
| `internal/flowch/rollup.go` | `RollupFiveMinute` runner（从 1m 派生，排端点 kind） | 单测 + dev CH 端到端（折叠/求和/排除/marker）通过 |
| `internal/flowch/rollup_test.go` | 5m 构建/校验用例 | 通过 |
| `internal/flowch/migrations_test.go`、`schema_test.go` | 迁移集断言更新（20→22，表数 14→16） | 通过 |
| `internal/server/flow_hot_rollup.go` | `scanFiveMinute` 接入热调度（复用 hour 配置键） | 单测通过 |
| `internal/server/flow_hot_rollup_test.go` | 两个 5m 调度用例 | 通过 |
| `docs/watchdog-install.md` | 新增「storage tiers/capacity/retention」节 | — |

**测试口径**：`go build ./...` 干净；`internal/{flowch,server,flowquery}` 全套 `go test` 通过；schema/runner 已在本地 dev ClickHouse 26.3 端到端验证。**尚未做**：生产部署验证、集成测试（真实 Kafka→worker→CH→5m→查询/账单全链）、迭代 4–7（查询路由/账单读 5m/对账/retention）。这些未完成项不能由编译或单元测试替代。

**复核补记（2026-09-28）**：全项目复核发现 5m 路径两处缺陷——`flow_aggregate_5m` 未接入迟到 repair（1m 被迟到数据修正后 5m 不重建，目前尚无读者，属潜伏缺陷）；报表读路径的覆盖检查跑在只有 1 个连接的 batch 池上，会排在后台 rollup/归档作业之后。详见 `docs/watchdog-project-review-2026-09-28.md`。

### 1.3 Agent 共享 token 改动（已进入 `main`，待生产部署）

| 提交 | 内容 | 测试状态 |
|---|---|---|
| `fae7eee2c` | 全局共享 token 注册；flow worker/collector 每分钟上报每窗处理量（`agent_runs.summary_json`） | agentplan/server 单测通过 |
| `cdfd283d9` | SNMP collector 每轮轮询上报 run | 单测通过 |
| `95bca83d9` | 心跳不再写 health；health 由 run 上报 + `last_seen` 陈旧度派生 | 单测通过 |
| `e14b16200` | Agent 运行页「工作量」列 | 前端 typecheck/biome 通过 |
| `aa870f65d` | 删除一次性 enrollment token 与 per-agent 凭据；MySQL 迁移 `0050` DROP `agent_enrollment_tokens`/`agent_credentials` | 真实 MySQL 8 集成（设备/agent API、SNMP trap）通过 |

**升级顺序（必须，否则 agent 会停止采集）**——两个已核实的升级陷阱（2026-09-29 已在代码中缓解，见各条末尾）：

- **凭据陷阱**：生产 agent 的 token 文件里仍是旧的 per-agent `wda_…` 凭据；server 升级到 `aa870f65d` 后它不再被接受（Sync 401 → 注册重试 401 → 进程报错退出 → `Restart=on-failure` 反复拉起）。另外 server 的 `agents.shared_token` 为空时任何 agent 都无法认证。（2026-09-29 缓解：注册被拒现在同样返回 `ErrUnauthorized`，四个进程都记录"agent token was rejected"后以状态 0 退出、不再循环重启——但在换成共享 token 之前该 agent **停止采集**。）
- **参数陷阱**：旧版 `activate-agent.sh` 在 `/etc/watchdog/agents/<服务>.env` 的 `WATCHDOG_AGENT_ARGS` 中写入了 `-agent-enrollment-token-file`；新二进制已删除该参数，Go `flag.Parse()` 遇到未定义参数直接以状态 2 退出——新二进制根本起不来。（2026-09-29 缓解：四个二进制重新接受该参数但忽略，并打印"deprecated and ignored"警告；计划下个版本删除。）

按序执行：
1. 生成强随机 token，写入 server 配置 `agents.shared_token`（或环境变量 `WATCHDOG_AGENT_SHARED_TOKEN`），重启 server；
2. 对每个 agent 运行**新版** `deploy/systemd/activate-agent.sh`（提示输入共享 token）：它同时重写 token 文件（0600，属主 watchdog）和不含 enrollment 参数的 env；
3. 再滚动升级各 agent 二进制；
4. 验收：Agents 页四类进程 `run_count` 递增、health 由 run 上报驱动、无进程处于重启循环。

（2026-09-30 已完成：r20 cutover 由 `deploy.sh` 生成共享 token 并经 systemd drop-in 注入 server，三个 agent 用新版 `activate-agent.sh` 重新激活；详见 `docs/flow-snmp-clickhouse-storage-query-design.md` §18.8。）

### 1.2 发布前必须处理
- **发布门禁**：tag workflow 必须从 `go.mod` 读取 Go 版本，执行全库 Go 测试、前端 check/test/typecheck/build，再分别发布进程制品与独立 web 压缩包。
- **`.gitignore` 已覆盖缓存/二进制**（已核实）：`.gocache-*/`、`.codex-deploy-*/`、根目录 `watchdog-flow-collect`/`watchdog-flow-worker`/`watchdog-server` 都在已提交的 `.gitignore` 中，`git add .` 不会带入这约 10 GB 垃圾。
- **干净 checkout 完整性**：MPA 构建器、部署契约测试、安装状态缓存和页面路径测试均为正式源码，必须被 Git 跟踪；`.gitignore` 不得再次隐藏这些构建依赖。
- 并行开发者的 flow 查询/账单/对账代码（`flow_reports.go`、`rollup_joint.go`、`interface_reconciliation.go`、`config.go` 等）已提交（HEAD 附近）。
- **推送**：确认 origin 是否为发布源；长期未推送提交需要一次受控 push（或发布走签名 tag/制品，不依赖 origin）。

---

## 2. 环境要求

### 2.1 主机
| 资源 | 最低 | 推荐 | 说明 |
|---|---|---|---|
| CPU | 4 核 | 8 核+ | rollup 扫描与查询并发 |
| 内存 | 16 GB | 32 GB+ | ClickHouse 是内存大户；见 §5.1 内存治理 |
| 根盘 `/` | 40 GB | 60 GB | **只放系统/MySQL/docker，不放 ClickHouse raw** |
| CH 数据盘 | 见 §2.2 | 独立大盘 | **raw 必须落在独立大盘，不能与根盘争用** |

### 2.2 磁盘容量模型（关键，三次事故根因）

raw 压缩后约 **62 字节/行**（已加 codec；未加约 84）。单 exporter 约 2.5 亿行/天 ≈ **15.5 GiB/天**。

| 保留期 | raw 容量（单 exporter，×1.3 余量） |
|---|---|
| 3 天 | ≈ 60 GiB |
| 7 天（akvorado 默认参考） | ≈ 140 GiB |
| 15 天 | ≈ 300 GiB |

聚合层（1m/5m/1h/1d + interface-5m）稳态约 **1–2 GB**，非容量瓶颈。MySQL 约 8 GB。

**硬结论**：60 GB 的根盘放不下哪怕 3 天 raw，且与 MySQL/docker 争用 → 必然打满。两条出路（二选一或叠加）：
1. **独立 CH 数据盘 ≥ 200 GB**（推荐），或
2. **ClickHouse 存储策略**把冷分区放到大盘（当前 vda3 有 53 GB 空闲），配置见 `docs/watchdog-5m-atomic-tier-design-2026-09-23.md` §0，**并**发布保留策略让 raw 有界汰换（见 §6）。

**事故教训（必须避免）**：根盘一旦 100% 满，ClickHouse 以非 root（uid 101）运行、用不了 ext4 的 5% 保留块，会陷入死锁——连 `DROP PARTITION` 都因无法预留 1 MiB 而失败。生产必须在打满前告警并留 ≥15% 余量。

### 2.3 数据库
- **MySQL 8.0**：RBAC/设备/账单/地址等元数据。`binlog_expire_logs_seconds` 建议 1 天；`innodb_redo_log_capacity` 1 GiB；buffer pool 按内存 25–50%。地址库单表约 265 万 CIDR，`OPTIMIZE` 后约 1.3 GB。
- **ClickHouse 24.9+**：flow 事实 + SNMP/系统时序。密码经 `password_file`。schema 由服务端 `install.go` 用内嵌迁移自动应用（见 §4）。

---

## 3. 前后端部署与 /api/ 路由（本次报错根因）

### 3.1 拓扑
两种部署形态：
- **同源部署**：站点服务前端静态文件，并把 `/api/` 反代到后端 API 进程。前端 `WATCHDOG_CONFIG.API_URL=""`。
- **分离源部署**：仅在确实跨域时显式设置 `API_URL`，并在后端 `server.origins` 放行前端源。

### 3.2 报错「The API request returned a frontend document」
含义：前端对 `/api/...` 的请求拿回了 `index.html`。根因是 **`/api/` 没有被反代到 API 进程，而是落到了静态/MPA 文档路径**。`deploy/nginx/watchdog.conf` 现在通过 `watchdog_api` upstream 表达后端地址，生产环境必须把它指向真实 API 进程。

### 3.3 修复与验收
- 使 nginx `location ^~ /api/ { proxy_pass http://watchdog_api; }` 指向真实 API upstream。
- `location ^~ /api/` 用 `^~` 前缀优先级，确保 `/api/` **永不**落入 `try_files` 静态回退。
- 前端 `API_URL=""`（同源）为默认；仅在跨源部署时显式设置，并在后端 `server.origins` 放行前端源。
- **验收命令**（必须返回 JSON 而非 HTML）：
  ```bash
  curl -s http://<host>/api/v1/health | head -c 200
  ```
  返回 `{"status":...}` 为正确；返回 `<!DOCTYPE html>` 即路由错误。

---

## 4. ClickHouse 索引与聚合配置

### 4.1 迁移自动应用
`internal/server/install.go:173-182` 在安装/启动时用内嵌 FS（`//go:embed *.sql`）`LoadMigrations` + `Apply`，幂等。新增的 `021`/`022` **随部署自动生效**，无需手工建表。验收：`SELECT max(version) FROM watchdog_flow.flow_schema_migrations` = 22。

（2026-09-29 复核补记，详见 `docs/flow-snmp-clickhouse-storage-query-design.md` §13、§17：
- **不可回滚。** 部署后不能回滚到 021 之前的二进制：旧二进制启动时会报 “schema version 021 is newer than the available migration set”，ClickHouse 平面起不来（`internal/flowch/migrations.go:144-146`）。部署前先用新二进制跑只读 `watchdog-flow-migrate -command inspect`。
- **自动迁移不传 TLS**（`internal/server/install.go:164-168`），启用 ClickHouse TLS 的部署会迁移失败。
- **5m 影子写入会立即开始。** 部署后热聚合立刻开始影子写 `flow_aggregate_5m`（没有独立开关）；这是预期的影子阶段，此时没有任何 reader。
- **首次发布 retention policy 的两个约束。** `bootstrap_from` 不得早于 2026-09-27，否则归档会在已删 raw 的空日上发布空归档，遮住仍然存在的热 1h；首版就定好全部开关，因为换版会让旧版已归档的日永久卡在 `invalid_generation`。
- **索引收益只对非 FINAL 查询成立。** 022 的约 6 倍裁剪是非 FINAL 实测；生产 `use_skip_indexes_if_final=0`，raw 查询的 FINAL 路径用不到该索引。）

（2026-09-30 补记：retention policy 改由配置文件 `flow.lifecycle` 管理，策略写接口只读；首版 `bootstrap_from` 用 2026-09-29。部署顺序、配置块、Kafka 保留期与磁盘前置条件见 `docs/flow-snmp-clickhouse-storage-query-design.md` §18.7，事故复盘见 §18.6。）

### 4.2 分层表与索引
| 表 | 角色 | 分区 | TTL |
|---|---|---|---|
| `flow_records` | raw 事实（含端点/明细） | 按日 | 无（生命周期状态机管理） |
| `flow_aggregate_1m` | 近实时分钟缓存 | 按日 | 2 天 |
| `flow_aggregate_5m` | 中期非端点查询层 | 按日 | 90 天 |
| `flow_interface_traffic_5m` | 计费/接口证据 | 按月 | 无（归档月状态机删除） |
| `flow_aggregate_1h`/`_1d` | 长期趋势 + raw 删除守恒 | 按月 | 无（生命周期管理） |

索引（migration 020/022）：`flow_records` 上 `device_id/target_id/exporter_id` set、`src_ip/dst_ip` bloom、`business_direction/category` set，以及 `event_time` minmax（子小时裁剪）。

### 4.3 热 rollup 配置（`config/watchdog.yaml` flow.hot_rollup，必须启用）
```yaml
hot_rollup:
  enabled: true            # 代码默认 false;生产必须显式 true
  scan_interval: "1m"
  seal_delay: "5m"         # 该时长内的 raw 尾段仍权威,不入 rollup
  minute_lookback: "6h"
  hour_lookback: "72h"     # 注意:须 ≤ 1m 表 TTL(2 天),否则超窗小时的 5m/1h 只能靠冷路径
  max_threads: 4           # rollup 扫描 CPU 护栏
  priority: 10             # 低于交互查询
  max_memory_bytes: 6442450944  # 每条 rollup 语句;超过则外部 GROUP BY 落盘
  minimum_generation: 0    # 仅在线重建时抬高;0 关闭强制再生
```
验收：`flow_aggregate_1h` 每个封闭小时有连续 `_generation` marker；`flow_aggregate_5m` 每个封闭小时 12 个 5m marker。

---

## 5. ClickHouse / Kafka / MySQL 优化

### 5.1 ClickHouse 内存治理（OOM 事故教训）
- 交互/审计查询必须限 `max_memory_usage=1.5G, max_threads=2`；**禁止无限制的大精确 GROUP BY**（一次无界 4 列 GROUP BY 曾把宿主 OOM 杀掉 CH）。
- rollup 语句用 6 GiB 上限 + `max_bytes_before_external_group_by`（落盘），不得用 0（无界）。
- `operation_timeout=2m`（交互传输）、`batch_operation_timeout=15m`（rollup/对账传输）、`flow.query.execution_timeout=2m`（单语句）分别接线。
- 若设 `max_server_memory_usage_to_ram_ratio=0.9` 且默认 profile 无落盘，OOM 风险高；建议给默认 profile 设内存上限 + 外部聚合/排序阈值。

### 5.2 ClickHouse 存储与生命周期
- 采用存储策略（`storage.xml`）把冷分区放大盘，`ttl_only_drop_parts=1` 整 part 删除（查询缓存层 1m/5m）。
- 证据层（1h/1d/interface-5m）不加无条件 DDL TTL，走归档月审批状态机删除。
- **死代清理**：`minimum_generation` 再生与回填会堆积旧代（ReplacingMergeTree），须在发布新代后按分区清理旧代行。

### 5.3 Kafka 优化与风险
- topic `watchdog.flow.raw-v1`，12 分区，按 ExporterKey 排序 → 单 exporter 时全部落在一个分区（p11 倾斜，模板有序性的代价）。
- retention 当前 6h（`retention.ms`/`retention.bytes` 约 6 GiB），落在独立 vda3。**权衡**：6h 是「摄入停摆多久即永久丢数」的窗口——本轮 09-25→09-28 摄入停摆 3 天，那段 flow 已被 6h retention 汰除、不可恢复。若磁盘/摄入不稳定，应加大 retention 或对 lag 告警。
- Kafka 容器自身 log4j 日志无 delete policy 会持续增长，需配 retention。

### 5.4 Docker 日志（本次事故直接原因）
一次 ClickHouse 因盘满写不了自身日志 → stderr 错误循环 → docker json 日志涨到 **15 GiB** 把盘彻底占满。**必须封顶**：`/etc/docker/daemon.json`（注意是 JSON，键为 `log-driver`/`log-opts`，不是 compose 的 `logging:`）：
```json
{
  "log-driver": "json-file",
  "log-opts": { "max-size": "200m", "max-file": "3" }
}
```
改后需 `systemctl restart docker`，且**只对之后新建的容器生效**——已有容器要 `docker compose up -d` 重建才应用。

### 5.5 MySQL
`binlog` 1 天、`redo` 1 GiB、buffer pool 按内存配比；地址库导入后 `OPTIMIZE TABLE`。

---

## 6. 运维护栏（三次事故的固化教训）
1. **磁盘告警**：`/` 与 CH 数据盘在 85% 告警、90% 阻断新回填；绝不让根盘到 100%（非 root 的 CH 会死锁，连删数据都做不了）。
2. **Kafka lag 告警**：lag 接近 retention 窗口（6h）即告警——超过就是永久丢数。
3. **摄入健康**：监控 `flow_records` 最新 `event_time` 与 now 的差；停摆立即告警。
4. **保留策略是根治**：发布 lifecycle policy（先 `raw_delete_enabled=false` 跑守恒对账，再开删除），让 raw 有界；否则只能手工 `DROP PARTITION`，不可持续。
5. **已知缺陷**：摄入中断产生的空洞被 rollup 记为「空桶 marker」，与真实零流量不可区分（丢数被静默成 0）；后续须加「数据丢失 gap 标记」。

---

## 7. 上线检查清单

- [x] `.gitignore` 补 `.gocache-*/`、`.codex-deploy-*/`、根目录构建产物；确认无缓存/二进制入库。（2026-09-28 复核：已完成 — `456157562`, `8e7ec860e`; .gitignore:25-36,47-49；根目录 4 个二进制经 check-ignore 确认均被忽略，git ls-files 无 ELF/归档）
- [ ] 5m 三个迭代按域分提交；`go build ./...` + 全套 `go test` 绿。（2026-09-28 复核：部分完成 — 已按域分提交；2026-09-28 go build ./... 与 agentplan/flowmetrics/snmpdomain/flowch 单测通过，全套 go test（含集成）未跑；`423d820e2`, `c1385cd3b`, `d6c38b62e`）
- [ ] 集成测试（真实 Kafka→worker→CH→5m→查询/账单）至少跑一轮。
- [x] 迁移应用到目标库，`flow_schema_migrations` = 22；5m 两表、event_time 索引存在。（2026-09-30 已完成：r20 部署后 ledger=22，MySQL 至 0051；r22 带 0052）
- [x] `flow.hot_rollup.enabled=true`；1h/5m marker 连续推进。（2026-09-30 已完成：r20 起按 ingest 水位封桶，5m 开始写入，09-29 空洞已从 raw 修复）
- [ ] raw 落独立大盘或存储策略生效；发布保留策略（或明确「暂由人工管控」并配告警）。（2026-09-30：保留策略已由 `flow.lifecycle` 发布并自动删除，r22 起约 1 天 raw；独立大盘与告警仍待办）
- [ ] `/etc/docker/daemon.json` 日志封顶已生效（容器已重建）。（2026-09-28 复核：部分完成 — daemon.json 已封顶；既有容器是否已重建未确认（CH json 日志曾需手工截断））
- [ ] `curl /api/v1/health` 返回 JSON；nginx `/api/` 反代端口与实际 API 一致。
- [ ] CH 内存上限（交互 1.5G/rollup 6G+落盘）、operation/batch/execution 超时均已配。（2026-09-28 复核：部分完成 — 超时与 rollup 6G+落盘已配；交互上限为 4GiB/2GiB 而非 1.5G，未设 max_threads=2，生产配置未核对；`config/watchdog.yaml:20-21,39,64`; `internal/flowch/rollup.go:961-971`）
- [ ] 磁盘、Kafka lag、摄入新鲜度三项监控/告警上线。
- [ ] Kafka retention 与磁盘余量匹配；Kafka/CH 容器日志均有 retention/封顶。（2026-09-30：Kafka 已改为 24h/32GiB（vda3）；容器日志封顶仍待确认）

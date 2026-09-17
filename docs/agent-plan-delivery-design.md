# KISS-04B Agent plan、ACK 与 LKG 详细设计

状态：冻结（2026-09-09）
范围：单安装域 Agent 控制面；不包含 SNMP/Flow 数据面重写，也不恢复旧 tenant/fleet/canary 状态机。

## 1. 不变量

1. `agents` 是 system、SNMP、flow collect、flow worker、probe 的唯一身份根；凭证、绑定、计划、ACK、运行记录分别只落 `agent_credentials`、`agent_bindings`、`agent_plans`、`agent_plan_acks`、`agent_runs`。
2. 安装只有一个管理域。计划签名载荷、API、表和作业 payload 均不得出现 tenant、provider、fleet 或 ownership。
3. Agent 使用一次性 enrollment token 注册，之后使用 bearer token 或 mTLS 指纹。secret 只在创建/注册/轮换响应中出现一次，MySQL 只存 token SHA-256。
4. 计划是不可变对象。每个 Agent 的 `plan_version` 严格递增；`agents.desired_plan_version` 只前进，禁止覆盖、删除、降级或复用版本。
5. 计划使用 canonical JSON + Ed25519 签名。签名覆盖 plan/agent 身份、版本、kind/API、payload SHA-256、key id、生效/失效时间和 supersedes 版本；服务端从持久化私钥签名，Agent 使用固定公钥校验。
6. ACK 是 `(agent_id, plan_version, boot_id)` 幂等事件。完全相同的重放成功，不同内容冲突；只有 `applied` ACK 单调推进 `acked_plan_version`，`rejected` 只记录失败。
7. Agent 必须先校验签名、kind、API 和 required capabilities，再由本进程校验并应用 config；全部成功后才原子写 LKG 并 ACK。失败计划不得替换 LKG。
8. 网络不可达或控制面 5xx 时可恢复已验签 LKG；控制面明确返回 401/403（吊销/凭证错误）时必须停止，不得借 LKG 绕过吊销。离线恢复允许已过期但签名和身份仍正确的历史计划。
9. 心跳最大允许时钟偏差 300 秒；在线健康阈值为 `max(3 * heartbeat_interval, 3m)`。计划 ACK 同时刷新 `last_seen_at`，但不替代长期运行进程的周期心跳。
10. 批量 rollout 只是一组单 Agent immutable plan 的异步 fan-out，复用 `operation_jobs` 的 lease/heartbeat/cancel/retry；不建立 canary/fleet/rollout 第二状态机。
11. Gin 控制面不持有宿主机 root、Docker socket 或远程 SSH 权限，也不调用 `systemctl`。进程安装、`enable --now`、崩溃重启和资源限制由 systemd/容器编排负责；Agent Registry 只管理进程身份、凭证、绑定、心跳、计划、ACK 和运行记录。
12. 每个进程只有一份最小 bootstrap：控制面 URL、稳定 Agent ID、一次性 enrollment 文件、Agent 计划公钥和 LKG 路径。注册成功后长期凭证原子写入本机 token 文件；一次性 enrollment token 不再复用。
13. 首次注册时允许尚无 desired plan；进程以已经过本地校验的 bootstrap/default tunable 启动并继续心跳，不能因管理员还没来得及发布第一版计划而退出。`-agent-plan-check` 是发布门禁，仍要求计划存在。
14. heartbeat 响应携带 `desired_plan_version/acked_plan_version`。当 desired 大于本进程已应用版本时，进程先停止接收、排空并关闭数据面，再以非零状态退出；`Restart=on-failure` 的 systemd unit 重启后重新拉取、验签、应用、安装 LKG 并 ACK。运行中收到 401/403 则排空后以成功状态停止，避免被吊销的凭证形成重启风暴。

### 1.1 三类配置对象不得混称

| 对象 | 解决的问题 | 所有者/生命周期 | 是否启动进程 |
|---|---|---|---|
| systemd/容器 bootstrap | 二进制、控制面/数据源 endpoint、secret 文件路径、监听端口、权限和自动重启 | 安装器/运维，随主机配置变更 | 是，`enable --now` |
| Agent immutable plan | poll interval、并发/批次/缓冲预算等进程运行参数 | `/agents/:id/plans`，签名、ACK、LKG | 否，进程启动后拉取 |
| Flow 业务发布物 | exporter source/采样 plan、AddressSnap/classification/VPN/tombstone | Flow/地址库管理流程，独立版本与 ACK | 否，决定数据语义 |

Agent 计划不能冒充 Flow exporter/source plan，也不能携带 MySQL、Kafka、ClickHouse 口令。反过来，Flow 业务发布物不能承担进程注册、吊销和健康管理。签名可以复用同一个受信 Ed25519 root，但 envelope、版本和 ACK 域必须分开。

## 2. 兼容矩阵（v1）

| kind | API | 至少一个能力 | config 所有者 |
|---|---|---|---|
| `system` | `v1` | `system.samples/v*` | system agent |
| `snmp` | `v1` | `snmp.poll/v*` | SNMP collector |
| `flow_collect` | `v1` | `flow.receive.sflow/v*` 或 `flow.receive.netflow/v*` | flow collect |
| `flow_worker` | `v1` | `flow.write.clickhouse/v*` | flow worker |
| `probe` | `v1` | `probe.execute/v*` | probe（本包不做实进程验收） |

计划中的 `required_capabilities` 必须全部存在于 Agent 注册能力中。schema/API/kind 不匹配在服务端发布时和 Agent 应用时均 fail closed。

## 3. 状态与操作

`registered -> active -> draining -> revoked`。注册和凭证轮换不改变 desired plan；首次有效 heartbeat/ACK 将 registered 变为 active。revoke 在一个事务内吊销全部凭证并把 Agent 置为 revoked。计划与 ACK 保留作审计，不级联到其他 Agent。

计划发布事务锁定 Agent 行，校验当前 row/version/capability，分配 `desired + 1`，写 immutable plan，再 CAS 推进 desired。批量发布作业逐 Agent执行相同事务；checkpoint 保存已完成 Agent ID，takeover 从 checkpoint 继续。

## 4. API

- 管理面：`GET/POST /api/v1/agents/:id/plans`、`GET /api/v1/agents/:id/plans/:version`、`POST /api/v1/agents/plan-rollouts`、既有 Agent/binding/run/enroll/rotate/revoke API。
- 机器面：`GET /api/v1/agents/:id/plan`（ETag/304）、`POST /api/v1/agents/:id/plan-acks`、既有 heartbeat/status/errors。
- 所有列表使用服务端 `limit/offset/q/sort/order/column filter`；计划无 PATCH/DELETE。
- 管理 UI 必须有 Agent 导航入口，并覆盖 list/detail/enroll/rotate/revoke/bind/plan/run。Enrollment 页面只显示一次 token、公钥与该进程的 bootstrap 参数；它必须明确提示 systemd/容器才是进程生命周期所有者，不能显示一个实际上会远程启动服务的伪按钮。

## 5. 失败与恢复

- `compatibility`、`verify`、`persist`、`activate` 分阶段记录 rejected ACK；错误码为大写 ASCII、详情最多 1 KiB。
- 相同版本但不同 payload、ACK 重放内容变化、版本倒退、未知 schema、签名失败均为永久错误。
- LKG 文件以同目录临时文件、`fsync`、rename、目录 `fsync` 原子替换；损坏 LKG 明确失败，不静默回退默认配置。
- 远端对已有 LKG 返回 304 时仍必须重试 `applied` ACK；否则一次临时 ACK 网络错误会使 `desired/acked` 永久不收敛。
- 旧控制面 heartbeat 的空 202 响应在滚动升级期间兼容为“无版本提示”；新控制面返回版本后自动启用运行期收敛。
- 空库无需历史迁移。clean install 只创建新六表；旧 collector plan/rollout 表不进入新 baseline。

## 6. 验收门

单元覆盖 canonical/signature、LKG 单调性/损坏、兼容矩阵、ACK 幂等/冲突/降级、enrollment replay/过期、clock skew。真实 MySQL 集成覆盖发布/ETag/ACK/吊销和 operation job takeover；system、SNMP、flow collect、flow worker 四个真实二进制分别完成注册（或使用已注册凭证）、计划应用、ACK、离线 LKG 启动、吊销拒绝。最后执行全库 test/race/vet/build 与现有前端测试。

生产部署另设运行门禁：四个 systemd unit 必须使用统一安装路径、显式 bootstrap 文件、`Restart=on-failure` 并处于预期的 enabled/active 状态；MySQL 必须出现对应 `agents` 行，desired/acked version 收敛且 heartbeat 新鲜。`flow_worker` 还必须同时满足 Kafka/ClickHouse secret、source stream ID 和已批准 enrichment publication，不能因为 Agent 已注册就宣称数据面可用。

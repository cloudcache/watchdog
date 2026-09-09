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

## 5. 失败与恢复

- `compatibility`、`verify`、`persist`、`activate` 分阶段记录 rejected ACK；错误码为大写 ASCII、详情最多 1 KiB。
- 相同版本但不同 payload、ACK 重放内容变化、版本倒退、未知 schema、签名失败均为永久错误。
- LKG 文件以同目录临时文件、`fsync`、rename、目录 `fsync` 原子替换；损坏 LKG 明确失败，不静默回退默认配置。
- 空库无需历史迁移。clean install 只创建新六表；旧 collector plan/rollout 表不进入新 baseline。

## 6. 验收门

单元覆盖 canonical/signature、LKG 单调性/损坏、兼容矩阵、ACK 幂等/冲突/降级、enrollment replay/过期、clock skew。真实 MySQL 集成覆盖发布/ETag/ACK/吊销和 operation job takeover；system、SNMP、flow collect、flow worker 四个真实二进制分别完成注册（或使用已注册凭证）、计划应用、ACK、离线 LKG 启动、吊销拒绝。最后执行全库 test/race/vet/build 与现有前端测试。

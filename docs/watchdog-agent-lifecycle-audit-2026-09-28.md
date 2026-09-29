# Watchdog 采集 Agent / Worker 全生命周期审计（2026-09-28）

> **复核状态（2026-09-28）**：逐条对照当前代码复核——已修复 2 · 部分修复 4 · 未修复 19 · 作废 2（作废 = 被后续设计决策取代，如全局共享 token、不设审批门）。逐条状态与证据见文末「复核状态（2026-09-28）」。

范围:采集 agent(snmp/flow_collect)与 flow_worker 的**注册、下发、运行、状态感知、CRUD/操作**的缺陷审计。方法:三路并行代码审计 + 生产 .18 live 佐证。所有条目附 `file:line`。

生产 live 现状锚点(触发本次审计):agent 注册表只有 3 个 agent(flow_collect/flow_worker/snmp),全部 `active` / `health=ok` / `last_seen` 新鲜,但 `run_count=0`、`failure_count=0`、6h 内 `agent_runs` 为空、`desired/acked_plan_version=0/0`;flow worker 每 10s 日志 `flow deployment v2 endpoint is unavailable; retaining version 11`。下面的缺陷解释了这三个现象。

---

## 0. 三条根因主线

- **A. 存活 ≠ 正确。** 心跳只证明进程活着、能连控制面,不证明有数据在流。而心跳每拍强制写 `health='ok'`,注册表没有任何吞吐/滞后/新鲜度信号 → **摄入已死的 worker 看起来是健康的**。
- **B. 下发不可观测且会静默过期。** 富化/部署通道从不回写 agent 的 plan 版本列(恒 0/0);没有服务端 reconciler 推进期望版本;v2→v1 回退闩不可逆,持久 404 把 worker 冻在 LKG;分类档/绑定编辑只发 v1,已迁 v2 的 worker 收不到 → 用旧配置分类实时流量。
- **C. 操作是单向且粗粒度的。** 无审批门、无可逆停用(只有终态 revoke)、绑定 1:1 设备、`draining` 是死状态、状态机基本不校验、无批量操作。

---

## 1. P1 — 会造成生产危害

| # | 缺陷 | file:line | 影响 |
|---|---|---|---|
| P1-1 | **心跳只证存活,且每拍强制 `health='ok'`;无摄入存活信号** | `agents.go:601`;`agentplan/runtime.go:100-130` | Kafka 消费卡死 / CH 写失败 / 采集丢包的 worker 仍每 30s 心跳并被盖成 active+ok。这正是"摄入死了却显示健康"。 |
| P1-2 | **流式 agent 从不上报 run;run/活动记账只给 kind=system 接了线** | 唯一 `INSERT agent_runs`+计数 `UPDATE` 在 `recordAgentStatus`(`agents.go:685,689`),仅 system agent 调用(`cmd/watchdog-system-agent/...`);flow_collect/flow_worker/snmp 只调 `RunHeartbeats` | 这 3 个 agent 恒 `run_count=0`、`last_run/last_success=NULL`、`agent_runs` 空;流式采集无任何逐批/逐轮成功记录。 |
| P1-3 | **富化/部署通道从不回写 `agents.desired/acked_plan_version`** | 唯一写者是 `agent_plans.go:381`(desired)/`:517`(acked);`flow_worker_deployments.go`、`flow_enrichment_publications.go` 全走 `generation`/`classification_version`,不碰这两列 | 注册表的 plan 版本列对 flow worker **结构性无意义**——无论富化是否健康都是 0/0,运维无法区分"已下发/当前版本"与"未配置"。 |
| P1-4 | **无服务端 reconciler 推进期望 plan 版本;只有人工操作才会设** | `agent_plans.go:240-242`(desired=0 时 `fetchAgentPlan` 返回 204)、`:362,:381`(仅手工 `POST /agents/:id/plans` 或人工发起的 rollout job) | 无人工动作时,snmp/system/flow_collect/flow_worker 永远 desired=0、跑 bootstrap CLI 值。这是全员 0/0 的主因。 |
| P1-5 | **v2→v1 回退闩单调不可逆;服务端停发 v2 会永久把 worker 冻在 LKG** | `version_dual_sync.go:53-59`;`cmd/watchdog-flow-worker/main.go:614`(重启从 LKG 重新置 v2Active=true)、`:737`(每轮保留);`version_http.go:244`(404→`ErrDeploymentRemoteUnsupported`) | 一旦 `v2Active` 置真,`/deployments/desired` 的持久 404 变成硬错、每轮 retain、重启不清 → 观察到的"retaining version 11"。**瞬时 500/网络错**是 `...Unavailable` 会干净重试恢复;持久 404 = 真实 server↔worker 版本错位(server 不再服务 worker 已迁移到的 v2 路由)。 |
| P1-6 | **分类档 / 绑定编辑只发 v1;已迁 v2 的 worker 静默收不到** | `flow_enrichment_publications.go:386`(putFlowClassificationProfile)、`flow_worker_bindings.go:119` 只入队 v1;v2 部署只由地址快照激活(`handlers_address_dimension.go:246`)或手工 `POST /flow/deployments` 产生;`version_dual_sync.go:58` v2Active 忽略 v1 | **最危险的正确性缺口**:worker 迁到 v2 后,六类分类档/客户边界的编辑要等一次无关的地址激活或手工 v2 部署才生效——期间用旧配置分类实时流量。 |

---

## 2. P2 — 重要(运维 / 治理 / 一致性)

| # | 缺陷 | file:line | 影响 |
|---|---|---|---|
| P2-1 | **无审批门;`registered` 任意一次通信即自升 `active`,且不隔离** | `agents.go:898`(enroll 落 registered)、`:601/:689`、`agent_plans.go:517,519` 四处自升;无 approve handler(`router.go:186-202`);`fetchAgentPlan`(`agent_plans.go:332`)只挡 revoked | registered 看着像待批状态实则不设防——已能拉签名 plan 并运行。唯一真正的加入控制是管理员发的 enrollment token。 |
| P2-2 | **无可逆启用/停用;revoke 终态且不可逆;`disabled` 别名到终态 revoke** | `agents.go:329-335`(禁 revoked→非 revoked;强制走 revoke)、`:527`(revoke 杀全部凭据)、`:947-948`(disabled→revoked);UI 只有 Revoke/Delete(`agent-form.tsx:237-249`) | 无法临时挂起;要恢复只能删除+重新 enroll(新 token+新凭据)。日常维护代价高。 |
| P2-3 | **`agent_bindings` 每 agent 唯一(1:1 设备),与采集器扇出不符** | `0003_agents.sql:59`;kind 强约束 `agents.go:1057-1079`;RBAC 按该单设备(`agents.go:1021-1026`) | 无法表达"这个 SNMP 采集器管这 N 台设备";绑到一台会把可见性错误缩到该设备的查看者,不绑又对所有 `agent.view` 全局可见。 |
| P2-4 | **offline 只是读时 DTO 覆盖;存储 `health` 列永不变;其他视图恒见 ok** | `agents.go:79-93`(effectiveHealth 读时算 offline)但无 reaper 写库;`flow_worker_deployments.go:937` 直接读原始 `a.health` | 同一台死 worker 在 Agents 页显示 offline、在 worker-deployments 页显示 ok;所有 SQL 消费者/告警看到的都是恒 ok。 |
| P2-5 | **心跳把真实 `error` 在 ≤30s 内盖回 `ok`** | `agents.go:601`;system agent 心跳 goroutine 与 ReportError 并发(`cmd/watchdog-system-agent/main.go:84`) | 失败态几乎永远观察不到。 |
| P2-6 | **注册表零运营遥测;"健康"只等于"最近心跳过"** | `0003_agents.sql:4-29` 无吞吐/Kafka lag/摄入新鲜度/每端口每设备状态/运行中富化 payload 版本;`agent_runs.summary_json`(`:75`)从不写(`agents.go:685` 无该列) | 无法区分"每秒 20 万流"与"啥都没处理"。 |
| P2-7 | **无服务端"worker 卡在旧版本"检测;卡死 worker 看着健康** | 404 只进 worker 本地日志(`main.go:737`);`agentHeartbeat` 保持 ok;无 reconciler 比对最新 `flow_worker_deployments.generation` 与最后 ack | 冻结 worker 与健康 worker 无法区分;`listFlowWorkerDeployments` 显示其"正常"。 |
| P2-8 | **地址激活同时触发 v1+v2,重复工作且割裂 ack 状态** | `handlers_address_dimension.go:225`(v1)+`:246`(v2) | 已迁 v2 的 worker 只 ack v2,其并行 v1 发布永远"pending",v1 ack 看板误导;签名/建对象重复。 |
| P2-9 | **v1/v2 版本游标命名空间重叠,无跨锁** | v1 `classification_version`=全局 MAX+1(`flow_enrichment_publications.go:663`);v2 `generation` 首插种自 v1 MAX(`flow_worker_deployments.go:635`)再按 worker 自增(`:639`) | 同一数字"版本"可能指两套不同 payload,回退时脆弱。 |
| P2-10 | **加入-激活链审计有洞** | `createEnrollmentToken`(`agents.go:774-809`)**无任何 audit**;`enrollAgent` 仅 token 创建者非空才审计(`:915-917`);四处激活全不审计 | "谁授权它加入、何时上线"在审计日志里基本查不到——对会发凭据的 RBAC 产品是治理缺口。 |
| P2-11 | **时钟偏移处理不对称,可把健康 agent 判成 offline** | `agents.go:595`(>300s 硬拒心跳 400);`runtime.go:116-123` 客户端只认 401/403/plan-change,400 只本地记 | NTP 坏但在正常摄入的 agent 一直被拒 → `last_seen` 停更 → 被显示 offline;`recordAgentStatus` 又完全不做偏移校验。 |

---

## 3. P3 — 次要 / 正确性 / 卫生

| # | 缺陷 | file:line |
|---|---|---|
| P3-1 | `draining` 是死状态:可被设入,但无代码转入、也不挡 plan(`agent_plans.go:332,490` 只挡 revoked),既不排空也不停工 | `agents.go:83,935` |
| P3-2 | 管理态迁移除 revoked 边界外基本不校验;可把从未上线的 agent 强设 active | `agents.go:324-336` |
| P3-3 | enroll/create 的凭据默认永不过期(仅 rotate 能带 TTL) | `agents.go:901,267,270` vs `:480` |
| P3-4 | 硬删活跃 agent,无先 revoke/占用检查,级联毁掉凭据/绑定/run/plan 历史,孤立运行中的 systemd 进程 | `agents.go:393-413`;`0003:45,61,80,107,119` |
| P3-5 | 身份/凭据输入弱:client 提供的 `id` 只校验长度≤26 无字符集(可含空格/斜杠流入 `/agents/:id/`);createAgent 允许管理员在 body 里设任意低熵 `token` | `agents.go:1001,226,267` |
| P3-6 | LKG 过期无界、无告警(只有每轮本地日志) | `version_lkg.go`;`main.go:737` |
| P3-7 | 版本/ack 状态分散在三个不相关端点,无统一"该 worker 是否最新"视图 | `agents.go:187`;`router.go:252,254` |
| P3-8 | headless-optional 注册:Sync+心跳整条链由 `-control-plane-url` 门控,纯文件启动的采集器永不注册/心跳/现身,运维却以为"3 个 active"就是全部 | `agentplan/runtime.go:132`;各 cmd main |
| P3-9 | `effectiveHealth` 用虚构的 `heartbeat_interval_seconds`(建表默认 60、从不被任何 handler 写、实际 30s 心跳),offline 期限恒为 3 分钟地板,与真实节奏脱钩;`0003:8` 仍声明 DEFAULT 'pending'(已被 `0012:6` 覆盖为 'registered');`normalizeAgentStatus` 的 pending/up/down/error 分支 API 层不可达 | `agents.go:84`;`0012_agent_registry_lifecycle.sql:6-8`;`agents.go:943-948` |
| P3-10 | 无批量操作;乐观并发是"选择性":`checkVersion` 仅当客户端发 `If-Match` 才生效,直连 API 可无并发保护地 rotate/revoke | `agents.tsx`(无多选);`device_organization.go:923-934` |

---

## 4. 结论正确 / 非缺陷(避免误改)
- Enrollment token 一次性 + 过期 + kind/device 作用域正确(`agents.go:877,895`,`FOR UPDATE`+事务消费;TTL 900s 默认 / 86400s 上限)。
- client 自定义 id 冲突安全:重复 PK 使整个 enroll 事务回滚,token 不被消费。
- `authenticateAgent`(`agents.go:611-631`)按路由 agent_id 作用域校验凭据,要求 active 且未撤销未过期;未发现越权/绕过。mTLS 直接读 `PeerCertificates`,仅在 Go 进程直接终止 TLS 时有效(反代终止 TLS 则失效)。
- rotate/revoke 用 `FOR UPDATE`+row_version,先撤后发顺序正确。
- 签名/验签自洽:单 Ed25519 key + 单调 trust bundle;`deployment_manifest.go`/`version_lkg.go` 有 canonical-JSON 重编码校验。
- ack **有**记录且经管理 API 可见(P3-7 是分散+缺"落后"信号,不是缺失)。
- gin 路由 `/deployments/desired`(静态)与 `/:deployment_id`(参数)在 gin v1.12 下静态优先,无 panic/404——观察到的 404 是服务端版本错位,非路由 bug。

---

## 5. 建议整改顺序(高杠杆在前)

1. **让 health 表达正确性(A 主线)**:health 绑定摄入存活(records/bytes 在流、Kafka lag、watermark 新鲜度),停止心跳强制写 ok;加服务端 staleness/stuck 检测 + 告警。修 P1-1、P2-4、P2-5、P2-7。
2. **让下发可观测且正确(B 主线)**:统一"该 worker 是否最新"视图(把真实富化/部署 generation 与最后 ack 摆到 agent 行或一个聚合端点);加 reconciler 推进期望版本并解释 0/0;**让 v2 闩可恢复**;把分类档/绑定编辑路由到 worker 当前所在的通道(修 P1-6 静默旧配置)。修 P1-3、P1-4、P1-5、P1-6、P2-8、P2-9、P3-6、P3-7。
3. **让流式 agent 上报活动(A 主线)**:逐窗 run/summary(records/s、丢包、错误)写入 `summary_json`;暴露吞吐/lag/每端口每设备状态。修 P1-2、P2-6。
4. **补齐操作(C 主线)**:加 pending/approve 门 + 可逆启用/停用;绑定改一 agent 拥有 N 个 target;强制状态机迁移;删除前安全下线。修 P2-1、P2-2、P2-3、P3-2、P3-4。
5. **治理与卫生**:审计加入-激活链;凭据默认 TTL;id 字符集校验;移除死状态/死列;批量操作 + 强制并发保护。修 P2-10、P3-1、P3-3、P3-5、P3-9、P3-10。

每项落地都应能在注册表/管理 API 上被验证(而不仅在 worker 本地日志),否则运维仍然"看不见"。

---

## 复核状态（2026-09-28）

本节由 2026-09-28 全项目复核生成：每条发现都对照当前代码/迁移/提交核实，以代码为准。汇总：已修复 2 · 部分修复 4 · 未修复 19 · 作废 2（部分未修复项在复核时按组列出，故表格行数可能少于汇总数）。

| 位置 | 发现 | 状态 | 证据 / 说明 |
|---|---|---|---|
| L21 | P1-1 heartbeat forces health=ok; no ingest-liveness signal | 部分修复 | `95bca83d9` `agents.go:495-500` (heartbeat no longer writes health); `fae7eee2c` run reports；worker/collector RunReport never sets Status (worker main.go:317-336, collect main.go:253-270), so it always defaults to success; zero throughput never degrades health; no last_run staleness check; not deployed |
| L22 | P1-2 streaming agents never report runs | 已修复 | `fae7eee2c` (worker/collector RunReports every minute) + `cdfd283d9` (SNMP each poll pass) → `agents.go:626` INSERT agent_runs；not deployed; prod still shows run_count=0 |
| L23 | P1-3 enrichment/deployment never writes desired/acked_plan_version | 未修复 | only writers are still `agent_plans.go:381`/517; `flow_worker_deployments.go` does not touch these columns \| |
| L24 | P1-4 no reconciler advancing the desired plan | 未修复 | `agent_plans.go:240-243` desired=0→204; set only by manual/rollout \| |
| L25 | P1-5 irreversible v2→v1 latch; persistent 404 freezes LKG | 未修复 | `version_dual_sync.go:53-58` unchanged since `c00e6c646`; `version_http.go:244`；matches prod "retaining LKG 11" |
| L26 | P1-6 classification-profile/binding edits only publish v1 | 未修复 | `flow_enrichment_publications.go:386,` `flow_worker_bindings.go:119` only enqueue v1；boundary save now returns draft+publication_required (flow_customer_boundaries.go:231) |
| L34 | P2-1 no approval gate; registered auto-promotes | 作废（被后续决策取代） | user decision; `aa870f65d` shared-token register inserts 'active' directly (`agents.go:815`)；by decision |
| L35 | P2-2 no reversible enable/disable; revoke is terminal | 未修复 | `agents.go:318-325`; normalizeAgentStatus disabled→revoked (:860-861)；credential/new-token cost gone with aa870f65d; UI still Revoke/Delete only |
| L36 | P2-3 agent_bindings 1:1 | 未修复 | `0003_agents.sql:59` uq_agent_bindings_agent \| |
| L37 | P2-4 offline is only a read-time DTO override | 未修复 | `agents.go:80-94`; `flow_worker_deployments.go:937` still reads raw a.health；now that heartbeat no longer overwrites health, the stored value can stay stale longer |
| L38 | P2-5 heartbeat overwrites error with ok within 30s | 已修复 | `95bca83d9` `agents.go:500` UPDATE no longer touches health；not deployed |
| L39 | P2-6 no operational telemetry; summary_json never written | 部分修复 | `fae7eee2c` `agents.go:566-579,626` (records/bytes/drops/lag/partitions); `e14b16200` UI column；no ingest freshness watermark, per-port/device state or enrichment generation |
| L40 | P2-7 no server-side "worker stuck on old version" detection | 未修复 | worker `main.go:780-786` only logs; summary has plan_version only, no enrichment cursor \| |
| L41 | P2-8 address activation triggers both v1 and v2 | 未修复 | `handlers_address_dimension.go:225` + :246 \| |
| L42 | P2-9 v1/v2 version namespaces overlap | 未修复 | `flow_worker_deployments.go:626-641` seeds from v1 MAX; `flow_enrichment_publications.go:663` \| |
| L43 | P2-10 join/activation chain not audited | 未修复 | createEnrollmentToken removed (`aa870f65d`); enrollAgent `agents.go:720-837` and run/heartbeat auto-activation still have no s.audit；enrollment half gone with the decision; registration audit still missing |
| L44 | P2-11 asymmetric clock-skew handling marks agents offline | 部分修复 | heartbeat still returns 400 (`agents.go:489-491`); recordAgentStatus (:630) refreshes last_seen with no skew check, so `fae7eee2c` per-minute reports mitigate offline；asymmetry itself remains |
| L52 | P3-1 draining is a dead state | 未修复 | `agents.go:84,848`; `agent_plans.go:332,490` only block revoked \| |
| L53 | P3-2 admin state transitions barely validated | 未修复 | `agents.go:313-325` \| |
| L54 | P3-3 credentials never expire by default | 作废（被后续决策取代） | `aa870f65d` removed per-agent credentials; 0050 DROP agent_credentials；by decision |
| L55 | P3-4 hard delete of active agent, no checks, cascades | 未修复 | `agents.go:382-402`；process now stops itself on 401 after delete; restart re-registers with the shared token |
| L56 | P3-5 id charset unchecked / admin may set low-entropy token | 部分修复 | token field rejected `agents.go:227-230,289-292` (`aa870f65d`)；id still length-only (agents.go:235,752,906) |
| L57 | P3-6 LKG staleness unbounded, no alert | 未修复 | `version_lkg.go` has no max-age; worker `main.go:783` only logs \| |
| L58 | P3-7 version/ack state split across three endpoints | 未修复 | agent DTO carries plan versions only; `router.go:250,252` \| |
| L59 | P3-8 headless collectors never register | 未修复 | `runtime.go:225` Enabled()=BaseURL!=""; gated in every cmd main \| |
| L60 | P3-9 fictitious heartbeat_interval / DEFAULT 'pending' / unreachable branches | 未修复 | `agents.go:85,854-865`; 0012:6-8 \| |
| L61 | P3-10 no bulk operations; If-Match optional | 未修复 | `device_organization.go:923-934` \| |

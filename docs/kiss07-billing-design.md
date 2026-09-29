# KISS-07：Billing、三层修正与对账闭环设计

> **与代码的差异（2026-09-28 复核）**：本文有 1 处已落后于代码或与代码不一致——以代码为准，逐条见文末「与代码的差异（2026-09-28 复核）」。

> 状态：实现冻结（2026-09-17）。单域、无租户；MySQL 保存管理状态和不可变证据，ClickHouse 保存并计算 SNMP/Flow 事实。没有 DatasetProvider、VictoriaMetrics DTO、PB collection 或第二套异步状态机。

## 1. 不变量

1. `raw` 是 Flow 估算计数的原始口径；`supplier` 是采集时冻结的供应商事实；`customer` 是供应商事实叠加客户规则后的口径。三层值并排保存，任何一层都不得覆盖 Flow 原始事实。
2. SNMP 使用 KISS-03A2 已发布、已闭合的 5 分钟接口桶；counter interval 定义为 `(previous_at, observed_at]`，起点样本只作 baseline，终点样本参与最后一个 interval，跨边界 interval 排除并降低 coverage/gap。Flow 使用 `flow_records FINAL` 按设备、接口和方向聚合成同一 UTC 5 分钟窗。缺采样率不以 raw bytes 冒充估算值。
3. 所有周期是左闭右开 `[date_from,date_to)`，数据库保存 UTC；`timezone` 保存 IANA 展示/账期边界快照。边界必须落在 5 分钟上，最大跨度由 `billing.max_period_duration` 控制（默认 400 天）。同一账户的 period 禁止任何重叠，避免重复计费；`billing_day` 是生成默认周期的本地日 00:00（短月取最后一天），账户详情 API/UI 默认给出最近一个已经完整结束的周期，操作员仍可为补结/短周期显式输入非重叠窗口。
4. 算法为 `95th`、`daily_95th`、`average`、`total`。月 95 使用 nearest-rank：排序后索引 `ceil(0.95*N)-1`；日 95 按账户时区逐自然日计算 nearest-rank P95，再对账期内各日 P95 做算术平均；月平均是闭桶速率算术平均；total 使用 counter/estimated bytes 总量，绝不对 bps 裸求和。rate 的唯一分母是 SNMP + raw + supplier + customer **共同完整**的 UTC 5m bucket 交集；缺桶、SNMP reset/gap、任一 Flow 层 unknown/partial sampling 都从所有 rate 层同步排除并报告 issue，绝不以 0 污染平均/95th。bytes 总量仍保存已知事实并携带 coverage。
5. 周期状态机为 `open -> calculated -> approved -> closed`。`open/calculated` 可重算并生成新的 `calculation_version`；`approved/closed` 的基础值不可覆盖。审批必须携带 `If-Match` 和当前 `calculation_version`，防止 stale approval。
6. adjustment 是 append-only **用量** ledger，unit 只能是 bps/bytes；批准后生效。reversal 必须新建符号相反、指向原 adjustment 的记录，永不 UPDATE/DELETE 原用量。关闭时把有效 ledger 冻结到 period，关闭后的 reversal 只进入当前纠错 ledger，不改变关闭证据。有 period 的 account、被 account 引用的 party 均禁止删除；证据外键使用 RESTRICT，不允许级联抹掉账单。
7. reconciliation 只产生证据和 issue，不改任何层的值。绝对阈值和百分比阈值取较大者；采样缺失、SNMP reset/gap、窗口错位始终单独报 issue。reader 回传窗口错位会写一条幂等 `window_offset` critical run/issue/audit，但不写 value、不推进 calculation generation，也不改变 period row；operation job 将其判为不可重试的契约错误。
8. 所有 mutation 有 RBAC、CSRF、row-version CAS 和 audit。`bill.viewAll` 或 `user_billing_permissions` 决定账户可见性；能看端口不自动获得财务权限。
9. calculate/reconcile/export 复用平台 `operation_jobs` 的 lease、heartbeat、retry、cancel、idempotency；单条 external evidence 的 CAS 保存是短事务，不创建 billing job 状态机。
10. CSV/Parquet 导出包含 period/account/party/port 快照、三层、SNMP、external、adjustment、reconciliation、issue、source generation 和完整 provenance；Excel 公式前缀必须转义。关闭后的同一 calculation version 导出可复算且字段稳定。
11. `measurement_type` 只表示计量单位：`bandwidth`（bps）或 `traffic`（bytes）；`billing_method` 表示计费方式：`package_port`（包端口）、`monthly_95th`（月95）、`daily_95th`（日95）、`monthly_average`（月平均）。带宽账户必须由操作员直接填写 `contract_bandwidth_bps`，这是商业合同和保底计算的唯一权威；用量计费保底等于 `contract_bandwidth_bps × minimum_percent`，创建账期时冻结。所选端口的标称容量（优先 `if_high_speed*1,000,000`，否则 `if_speed`）只作为独立一致性证据，UI/API 提示 `insufficient/excess/unknown/match`，不得自动修改合同值、保底或阻断保存。包端口不使用保底百分比。流量计量当前仅允许包端口，并保留 `traffic_allowance_bytes` 作为超用证据。币种是 ISO 4217 三位大写代码，单价以 `DECIMAL(20,6)` 保存并作为字符串经 API 往返；不引入阶梯、税率、折扣和浮点金额。
12. API/MySQL 的权威单位保持 bps/bytes，避免迁移和精度歧义；操作界面统一使用十进制 Mbps（`1 Mbps = 1,000,000 bps`）和 GB（`1 GB = 1,000,000,000 bytes`）并在提交/回显边界转换。`reconcile_abs` 与算法同量纲：95th/average 为 bps（界面 Mbps），total 为 bytes（界面 GB）；`reconcile_percent` 为百分比。两种阈值只决定是否产生差异 issue，不改变 used、overuse、计费量或价格。

## 2. 管理模型

### party

`id, kind(customer|supplier), status(active|inactive), name, ref, notes, row_version, created/updated_by, created/updated_at`。party 只是交易对手，不是用户、租户或权限边界。

### account / port

账户冻结 `party_id, status, measurement_type, billing_method, algorithm, billing_day, timezone, direction, default_layer, price_currency, unit_price, contract_bandwidth_bps, minimum_percent, traffic_allowance_bytes, reconcile_abs, reconcile_percent, ref, notes`。`algorithm` 由计量类型和计费方式唯一派生，API 调用方不能组合出互相矛盾的值；`default_layer` 是 raw/supplier/customer 取值策略。account direction 是新增绑定的默认值；每个 port direction 是实际计费权威，显式值覆盖默认值。一个账户可跨设备绑定端口，上限由 `billing.max_account_ports` 控制；创建/编辑账户时账户字段与完整端口集合在同一 MySQL 事务提交，任一端口不存在或越权则整体回滚。创建 period 时把已解析方向、`port_id/device_id/if_index/if_name/capacity_bps`、容量校验结果和账户计价字段写入快照，后续修改账户、party、端口元数据或绑定不改变既有账期。

## 运行预算与异步边界

计费代码里的限制分成两类，不能混为“可配置越大越好”：

- 正确性硬边界保留在代码/数据库：合法枚举、百分比 `0..100`、UTC 5 分钟闭桶、`uint64` 溢出保护、幂等键长度、CAS 与关闭后不可变。这些值改变会改变协议或账本语义，不允许运行时漂移。
- 资源预算集中在 `billing` YAML：账户端口数、VTable 单页行数、账期最大跨度、单次导出证据行数、publication 引用数量/字节、worker poll/lease/retry。默认值只是安全初值，部署可按 CH/MySQL 和硬件容量调整；当前 SNMP/Flow reader 的硬上限为 1000 端口/400 天，配置可调低但不能假装调高，解除该上限必须先完成 KISS-07C 分片查询。任务创建后 `progress_total` 与输入快照固定，配置变化不改既有账期证据。HTTP JSON 体积、幂等键长度等是固定的协议/攻击面上限，不用它们承载批量业务数据；批量输出一律走 operation job 产物。

`calculate/reconcile/export` 必须走 `operation_jobs`，支持 lease、heartbeat、取消、重试、幂等和可见阶段进度。当前计算阶段为“冻结输入→并行读 SNMP/Flow→计算/对账→原子提交”，导出阶段为“装载证据→编码→原子发布”；事务前后的重试由 operation ref 收敛。超大账期按时间片 checkpoint 和 CSV/Parquet 流式分页仍作为独立优化包，未完成前由上述可配置预算 fail-closed，不能用无限数组或 HTTP 同步请求绕过。

当前源码上限审计如下；“配置化”不等于允许无界放大：

| 上限 | 当前性质 | 处置 |
|---|---|---|
| 账户端口数、账期跨度、VTable 页长、导出行数、publication 数量/字节、worker lease/retry | 部署资源预算 | 已集中到 `billing` YAML；账户端口和账期跨度暂受 reader ceiling 约束，只能调低 |
| SNMP/Flow 每次读取最多 1000 个端口、120000 个 5m 桶、400 天；SNMP/Flow CH 查询时间/线程/内存 | 当前单查询实现的执行预算 | 不作为商业规则；KISS-07C 用有界时间片和 checkpoint 取代“一次查询放大”，届时把每片查询预算纳入 reader 配置 |
| 百分比 `0..100`、5m 对齐、合法状态/枚举、整数溢出保护 | 账本正确性不变量 | 保留代码硬约束，不开放运行时修改 |
| JSON body 256 KiB、幂等键 190 字符 | HTTP/数据库协议与攻击面边界 | 保留硬约束；大输入和输出使用 operation job/artifact，不提高同步请求上限 |

因此，异步并不等同于“把同一个超大同步查询放到 goroutine”。现阶段 API 已经立即返回 202，由统一 operation worker 租约执行并报告阶段进度；KISS-07C 的完成条件是单个 CH 查询也被切成可续跑时间片、每片持久化 fenced checkpoint，导出按页流式写临时文件并原子发布。完成前不得把 15/120 秒查询超时简单调大来假装解决容量问题。

### period / value

period 保存完整 account+party JSON、端口关系快照、direction/阈值、UTC 窗口、状态、`calculation_version`、当前口径的 allowed/used/overuse、完整 publication/provenance、审批和关闭主体/时间。关闭时另存 adjustment ledger JSON。value 以 `(period_id,calculation_version,layer)` 唯一，保存 in/out/selected bytes、95th/average bps、算法值、coverage、missing/reset/gap/unknown sampling、source generation 和 JSON provenance。

external 输入先以 `calculation_version=0, layer=external` 保存；每次计算复制到新 generation，从而让审批和导出只读取一个完整 generation。导入必须同时满足：算法值等于对应 95th/average/selected-bytes 字段，`observed <= expected`、`missing = expected-observed`、`reset <= observed`，coverage 与桶计数一致且处于 `[0,1]`；不接受只有一个数但伪造为完整覆盖的证据。

### adjustment / reconciliation

adjustment 保存 layer、unit、signed amount、reason/evidence、状态、批准信息和 reversal 引用。原 adjustment 批准后状态保持 `approved`；reversal 是一条新的、符号相反并独立审批的记录，因此有效合计自然归零，禁止用 `reversed` 状态排除原记录。关闭后 reversal 不改写 period 的关闭快照，导出同时给出关闭快照和当前 ledger effective value。reconciliation run 固定 calculation version 和阈值快照；issue 保存比较双方、metric、expected/actual/delta/threshold、严重度、处理状态和处理人。

## 3. 计算流水线

1. 创建 period 的事务锁 account、拒绝重叠窗口并一次性冻结 account+party+port；计算只读取冻结的 period/port snapshot，验证状态、row version、UTC 5m 窗口和端口非空。
2. 在事务外用同一 scope 并行读取 `snmpch.ReadBilling` 与 `flowch.ReadBilling`；两边都只返回有预算的闭桶结果。
3. 强校验两个 reader 回传的 From/To 与 period 完全相等；Flow rate reader 必须提供带时间戳和 complete 标记的 `RateBuckets`，不接受无时间戳 `SelectedRates` 兼容值。取四层共同完整 5m bucket 交集后，纯函数生成 raw/supplier/customer/snmp value；external 从 generation 0 输入复制。任何 CH 失败、窗口错位或网格不完整都不写部分 generation；窗口错位只另存失败 run/issue 证据。
4. 纯函数按阈值生成 coverage/missing-sampling/reset/gap 和各层 delta issues；不自动调整。
5. 单一 MySQL 事务再次锁 period 并验证版本未变，写完整 value generation、run/issues，最后推进 `calculation_version` 和 `calculated` 状态。
6. 同一个 operation id 的 worker 重试收敛到同一 generation；用户显式发起新的计算才产生新 generation。在同一事实 fixture 上各 generation 数值必须完全一致，旧 generation 保留为证据。

## 4. API

所有 list 支持 `limit/offset/q/sort/order` 和列过滤，返回 `{items,total,limit,offset}`。

| 方法 | 路径 | 权限 | 说明 |
|---|---|---|---|
| GET/POST | `/api/v1/billing/parties` | `bill.viewAll` / `bill.create` | party VTable / 新建 |
| GET/PATCH/DELETE | `/api/v1/billing/parties/:id` | viewAll/update/delete | 详情、CAS 更新、未引用删除 |
| GET/POST | `/api/v1/billing/accounts` | view/create | 授权范围 VTable / 新建；POST 的 `items[]` 与账户原子创建 |
| GET/PATCH/DELETE | `/api/v1/billing/accounts/:id` | view/update/delete | 详情、CAS 更新；PATCH 可用 `items[]` 原子替换跨设备端口；存在 period 时禁止删除 |
| GET/PUT | `/api/v1/billing/accounts/:id/ports` | view/update | 绑定 VTable / 原子替换 |
| GET/POST | `/api/v1/billing/accounts/:id/periods` | view/create | 周期 VTable / 新建 |
| GET | `/api/v1/billing/periods/:id` | view | 周期、当前 values 和 reconciliation summary |
| POST | `/api/v1/billing/periods/:id/calculate` | calculate | 202 operation job |
| POST | `/api/v1/billing/periods/:id/external` | reconcile | 导入 external value/provenance |
| POST | `/api/v1/billing/periods/:id/reconcile` | reconcile | 202，基于当前 generation 重跑 issues |
| POST | `/api/v1/billing/periods/:id/approve` | approve | If-Match + calculation version |
| POST | `/api/v1/billing/periods/:id/close` | approve | 关闭且冻结基础 generation |
| GET/POST | `/api/v1/billing/periods/:id/adjustments` | view/update | ledger VTable / 新建 |
| POST | `/api/v1/billing/adjustments/:id/approve` | approve | CAS 批准 |
| POST | `/api/v1/billing/adjustments/:id/reverse` | approve | append-only reversal |
| GET | `/api/v1/billing/periods/:id/reconciliations` | view | run VTable |
| GET | `/api/v1/billing/periods/:id/issues` | view | 当前 generation 的 issue VTable |
| PATCH | `/api/v1/billing/issues/:id` | reconcile | CAS 处理 issue |
| POST | `/api/v1/billing/periods/:id/exports` | export | CSV/Parquet operation job |
| GET | `/api/v1/billing/exports` | export | export job VTable |
| GET/POST | `/api/v1/billing/exports/:job_id[/download or /cancel]` | export | 状态、取消与证据下载；下载校验 SHA-256 和 retention |

## 5. 验收向量

- 20 个 5m 桶 `[1..19,100]` 的 95th 为 19；total 取 bytes，不取 rate sum。
- DST 跨越仅改变展示标签，不改变 UTC 桶数量；非 5m 边界拒绝。
- SNMP reset/gap、Flow `estimated_valid=false`、某侧整桶缺失分别生成 issue；该桶从四层 rate 分母同步排除，数值不被补 0 或自动调平。无时间戳 rate fallback 必须拒绝；reader window offset 落 critical issue 且 operation 不重试。
- raw=1000、supplier=900、customer=800、external=750 在阈值 5%/20 时生成可人工复算的 signed delta。
- 相同固定 CH fixture 重算得到同样 values/issues；旧 generation 不覆盖。
- approval 的 calculation version 或 row version 过期返回 412；closed 后 calculate/import/普通 adjustment 均拒绝。
- adjustment `+100` 的 reversal 必须为新记录 `-100`，两行合计为零。
- CSV/Parquet 对同一 generation 的行数、值、provenance 一致。
- 两台设备各选一个端口并创建 account 后应得到两条绑定；任一 port 不存在或超出调用者 device/port scope 时 account 与绑定均不落库。
- `12.345600` 必须按原字符串精度往返；小写币种、负数、超过 6 位小数、计量类型与计费方式不兼容、保底超出 `0..100` 必须拒绝。
- 合同总带宽 8 Gbps、所选端口标称容量 10 Gbps、保底 30% 时，账期冻结保底必须是 2.4 Gbps，容量校验必须为 `excess`；改变端口速率不得反向修改合同值。日 95 的跨时区自然日分组和逐日 P95 平均必须有确定性测试向量。

## 6. 已执行验证（2026-09-17）

- 纯函数与仓储：`go test ./internal/billing`，覆盖 95th/average/total、UTC/DST 桶、三层 delta、sampling unknown、counter reset、external 一致性、adjustment/reversal。
- 真实依赖：独立 MySQL 管理库 + ClickHouse 临时 database 写入 fresh SNMP counter 和 fresh Flow records；同一端口、同一 10 分钟周期四层均复算为 `800 bps / 60000 bytes / 2 of 2 buckets`，重复计算一致。另以真实 MySQL 验证 window offset 只生成一条幂等 run/critical issue/audit、无 value generation，operation retry 仍返回终止错误。
- 2026-09-17 合同带宽变更门禁：真实隔离 MySQL 库验证 migration 0043 和 Gin account→period→calculate→export operation-job 全链；真实 ClickHouse 回归同时发现并修正 SNMP counter 桶边界，桶起点 sample 只作 baseline、桶终点 sample 参与 `(previous_at, observed_at]`，修正后四层恢复 `800 bps / 60000 bytes / 2 buckets`。
- HTTP 端到端：真实 Gin/MySQL 覆盖 party/account/port/period、异步 calculate、issue 处理、adjustment、approve/close、CSV/Parquet、RBAC/CSRF/CAS/audit；篡改 artifact 返回 `export_corrupt`，过期 artifact 返回 `export_expired`，存在 period 的 account 删除返回 409。
- 回归：billing/flowch/snmpch/server 包 test+vet、57 个前端单测、Billing 三页 Biome、Vite production build 通过。完整全仓门禁在提交前再执行一次。
- 计价与绑定扩展：真实空 MySQL migration 后，经 Gin 完成跨两台设备的两端口原子创建、CAS 原子更新、精确单价往返；无效端口验证整笔回滚。设备概览仅保留 Total/Up/Down/Disabled 统计，不再重复渲染全部端口名称。

---

## 与代码的差异（2026-09-28 复核）

2026-09-28 全项目复核将本文与当前代码/迁移/提交逐条对照，下列各处设计已被实现取代、改名或尚未实现。**以代码为准**；正文保留作设计历史，未逐句改写。

- **§4 API（:73-95）**：代码另有 `GET /billing/accounts/:id/snmp-usage`、`GET /billing/periods/:id/values`、`GET /billing/jobs/:id`、`POST /billing/jobs/:id/cancel`；`/billing/jobs` 只是 operation_jobs 的只读/取消外壳，不是第二套状态机。

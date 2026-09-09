# KISS-03A：SNMP 时序切换 ClickHouse 详细设计

> 状态：KISS-03A1/A2 实现冻结（2026-09-09）。本切片只迁移 SNMP；system/container agent 时序明确延后到 KISS-03B。Flow 继续使用既有 `flowch` 数据面，不因本切片重写。

## 1. 边界与删除项

- 复用现有 SNMP v1/v2c/v3 会话、MIB/OS/module discovery、OID recipe、设备并发轮询和 device/port/BGP/sensor/inventory 当前态导入；不重新拼接厂商字段，不改变采集语义。
- MySQL 是唯一管理库，只保存 `devices`、`ports`、`snmp_profiles`、`snmp_collection_recipes` 及 recipe 最近执行状态；SNMP 样本不写 MySQL。
- ClickHouse 是 SNMP 时序唯一权威。生产 `watchdog-snmp-collector` 的路径固定为 `v2 MySQL recipe -> existing poller -> snmpch writer -> ClickHouse`；Gin 图表查询固定为 `snmpch -> ClickHouse`。
- 该路径无 PocketBase、无 tenant 参数/列、无 VictoriaMetrics client/provider/fallback/双写。
- 删除未提交的通用 `telemetrych`。它把尚未迁移的 system/agent 抽象成 `source_kind`，又重复实现 `flowch` 已有的连接池与迁移器，不符合当前最小边界。`snmpch` 只负责 SNMP 表、写入、rate 和查询；连接池及唯一 CH migration ledger 继续复用 `flowch.NativeInserter`。

## 2. 输入与自然身份

每个已解码样本进入 writer 前必须具备：

`observed_at, device_id, agent_id, entity_kind, entity_id, recipe_id, metric, value_kind, gauge_value | counter_value, counter_width, interval_ms, quality_flags, poll_sequence, source_run_id, sample_index`

- Counter32/Counter64 从 SNMP varbind 直接解析成 `UInt64`，不得先经过 `float64`；这保证大于 `2^53` 的 ifHC counter 仍精确。
- 一次 poll 生成一个 `source_run_id`；该 poll 内 `sample_index` 从 0 连续递增。`poll_sequence + source_run_id + sample_index` 是写入重试的自然身份，不计算内容 hash。
- 一个写批只包含同一次 poll 的连续 sample index。writer 同步等待 CH 接收后才允许更新 MySQL recipe 的成功状态；失败最多重试 5 次并施加反压，不能先确认 recipe。
- `ReplacingMergeTree` 的排序键包含设备/指标/实体/时间和自然身份；API 查询按 `argMax`/`FINAL` 读取收敛结果。进程在“CH 成功、MySQL 状态未更新”之间崩溃时会重新轮询形成新的真实观测，不伪造旧观测，也不丢失已落盘样本。

## 3. ClickHouse 表

唯一 DDL 位于 `deploy/migration/clickhouse/012_snmp_telemetry.sql`，由 `watchdog-flow-migrate` 的既有 migration ledger 执行；server/collector 启动时只检查 schema，不创建或修改表。

- `snmp_samples`：原始 gauge/counter 审计事实；月分区；排序前缀适配 device + metric + entity + time 查询。
- `snmp_interface_traffic_5m`：已关闭 5 分钟端口流量桶，保存 in/out bytes、bps、reset/gap/coverage 和 generation。
- 不在 DDL 硬编码 30 天或其他 TTL。保留期属于全局运维生命周期决策，变更时通过受审查的 CH migration 执行；本项目没有历史数据，不设计 VM backfill/shadow read。

## 4. rate、乱序与 closed bucket

- 查询 rate 先按观测时间和实体去重排序，再用相邻 counter 求差，以真实 `elapsed_ms` 作分母，不以统一配置 interval 猜测。
- 正常递增直接求差；32-bit 仅在前值位于上界 90% 且新值位于下界 10% 时认定 wrap；其他下降认定 reset 并丢弃该段。64-bit 下降按 reset 处理。
- 相邻间隔超过 `max(3 * interval_ms, 15m)` 认定 gap，不跨缺口摊流量。
- 5m rollup 只处理已经关闭且对齐的 bucket。先写 generation 的 value rows，最后写 generation marker；读取端只接受 marker 对应 generation。这样 crash 不会发布半桶，空修复也可用新 generation 覆盖旧结果。

## 5. 查询、权限与兼容契约

- Gin 保留 `/api/v1/metrics/catalog|query|range|realtime`。SNMP 查询必须提供一个 device/target/port 根，先经过单域 RBAC，再进入 CH。
- API 只接受白名单参数；固定周期和自定义 RFC3339 时间范围都受最大 400 天、step 及 250000 行预算限制。`tenant_id` 等旧参数在访问 CH 前直接返回 400。
- 返回 JSON 继续是现有前端消费的 `status + data.resultType=matrix + metric + values`，其中 value 明确编码为 `[unix_seconds, decimal_string]`；active handler 使用中性 DTO，不依赖任何旧 VM 类型。
- `/api/v1/metrics/aggregate` 接受显式 `device_ids/target_ids/port_ids`、metric、`sum|avg|min|max|count`、固定/自定义时间和 step。Gin 先把端口解析到设备根并校验当前 device/port grant；`snmpch.Aggregate` 再把去重后的 scope 作为 CH external table 送入一条有界查询。device-wide scope 覆盖同设备的重复 port scope，避免重复计数；聚合函数来自固定白名单，不能进入 SQL 参数拼接。
- aggregate 与 CSV 当前输出层明确为 `raw`。`value_mode/traffic_view` 兼容查询参数不能使原始 SNMP 数据冒充 supplier/customer 修正值；三层修正属于 KISS-07/Flow 对账，不在 SNMP 数据面隐式实现。

## 6. 异步 CSV 契约

- `POST /api/v1/metrics/exports`（同时提供 `/api/v1/exports` 管理入口）冻结 payload v1：device/port IDs、metric、aggregate、`[from,to)`、step 和 row budget。请求最大 1000 个 scope、400 天、250000 行；幂等域是 `user + Idempotency-Key`，同域 key + request hash 返回原 job，同域 key 相同而 payload 不同返回 409，不允许不同用户碰撞并看到对方 job。
- CSV job type 固定为 `snmp.aggregate.csv`，只复用唯一 `operation_jobs` 的 enqueue、lease、heartbeat、retry、cancel 和 takeover；不启用基线中尚未承载完整生命周期的 `export_tasks`，也不新建另一套状态机。job 创建时和 worker 真正执行时分别校验 grant，排队期间撤权会使任务 terminal fail。
- worker 调用与同步 API 同一个 `snmpch.Aggregate`，以临时文件写入、`fsync`、同目录原子 rename 后才完成 job。CSV schema 固定为 `bucket_start,metric,aggregate,value`；结果引用只含 server 生成的 job ID 和 artifact SHA-256，下载时重新校验 checksum。这个 hash 是低频导出制品完整性校验，不进入 SNMP/Flow 热写路径。
- 导出默认保留 24 小时（`snmp.export_dir/export_retention`）；到期返回 410。list/get/download/cancel 仅 job owner 或管理员可见，列表按 owner/type/status 做服务端分页；暂时性 CH/文件错误由 operation job 自动重试，payload/schema/授权错误不重试。

## 7. Billing reader 契约

- `GET /api/v1/billing/accounts/:id/snmp-usage?start=&end=&limit=&offset=&max_buckets=` 只读 `billing_account_ports` 的 `in|out|agg` 选择和 `snmp_interface_traffic_5m` 已发布 generation；不得独立重算 raw counter，也不在本步骤写/关闭 `billing_periods`。KISS-07 只能消费此 reader 的证据。
- 时间窗必须是 UTC 对齐且已经关闭的 5 分钟 `[start,end)`，最长 400 天/120000 桶。`agg` 在每个桶内取 `in + out`；total 是所选方向逐桶字节和，average 与 nearest-rank 95th 都在同一等长 5 分钟序列上计算。
- 查询先建立“时间桶 × 账单端口”的有限网格，再 left join generation marker 和 value；整端口缺桶时 coverage=0、gap=true，不允许仅因其他端口有行就误报完整。返回 expected/observed bucket、expected/present port、reset/gap、coverage 和 generation 范围；不完整结果可展示和对账，但 KISS-07 不得自动批准。
- billing 可见性只由 `bill.viewAll` 或 `user_billing_permissions` 控制；能看设备/端口不等于获得财务权限。明细桶服务端分页，汇总始终对应完整请求窗口而不是当前页。

## 8. 运行、失败与可观测性

- server 与 collector 共用 `flowch.NativeInserter` 的有界 CH 连接实现，但使用各自进程内 pool；不引入第二套 CH 客户端配置或迁移表。
- CH schema 缺失时启动明确失败并提示先迁移。轮询 loop 的一次 MySQL/SNMP/CH 暂时错误会记录并在下一个有界 tick 重试，不会悄悄退出。
- `SNMP.PollLimit` 限制每轮设备数，`PollConcurrency` 限制同时访问设备数；recipe 自身 `sample_interval_seconds` 决定是否到期，scheduler wakeup 不改变采样周期。

## 9. 验收矩阵

1. 单元：UInt64 精度、自然批身份、retry、32-bit wrap、reset、gap、时间/行预算、closed bucket generation、aggregate scope 去重、billing 总量/95th/coverage、CSV 原子性/取消和旧参数拒绝。
2. API：管理员、device/port grant、billing grant、owner-only export、服务端分页、同一 JSON chart contract；查询 SQL 只访问 SNMP CH 表且不含 tenant。
3. 真实管理面：一次性 MySQL v2 schema 验证 aggregate 授权/拒绝、CSV 首次失败后重试收敛、运行中取消无制品、owner 分页和 billing 独立授权；测试库验证后删除。
4. 真实采集集成：一次性 MySQL v2 schema + 固定 SNMP varbind -> existing poller -> CH；核对 `>2^53` counter、recipe 状态和无 tenant 列。
5. 真实 CH：raw query/rate/跨 scope aggregate/closed 5m generation/billing total；额外绑定一个无数据端口时必须降低 coverage 并置 gap。VictoriaMetrics 停止或不存在不影响链路。
6. 回归：目标 Go packages、`go test ./...`、`go vet ./...`、`go build ./...`；沙箱若禁止测试监听 socket，必须在允许本地 loopback 的环境重跑，不能改测试掩盖。

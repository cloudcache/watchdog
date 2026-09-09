# KISS-03A：SNMP 时序切换 ClickHouse 详细设计

> 状态：实现冻结（2026-09-09）。本切片只迁移 SNMP；system/container agent 时序明确延后到 KISS-03B。Flow 继续使用既有 `flowch` 数据面，不因本切片重写。

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
- 返回 JSON 继续是现有前端消费的 `status + data.resultType=matrix + metric + values`，但 active handler 使用中性 DTO，不依赖任何旧 VM 类型。
- 本切片提供设备/端口 SNMP 原始值与接口 bps 图表查询。跨设备 aggregate、异步 CSV/账单消费在复用同一 `snmpch` query 契约后作为 KISS-03A2 收口，不得回接旧 provider。

## 6. 运行、失败与可观测性

- server 与 collector 共用 `flowch.NativeInserter` 的有界 CH 连接实现，但使用各自进程内 pool；不引入第二套 CH 客户端配置或迁移表。
- CH schema 缺失时启动明确失败并提示先迁移。轮询 loop 的一次 MySQL/SNMP/CH 暂时错误会记录并在下一个有界 tick 重试，不会悄悄退出。
- `SNMP.PollLimit` 限制每轮设备数，`PollConcurrency` 限制同时访问设备数；recipe 自身 `sample_interval_seconds` 决定是否到期，scheduler wakeup 不改变采样周期。

## 7. 验收矩阵

1. 单元：UInt64 精度、自然批身份、retry、32-bit wrap、reset、gap、时间/行预算、closed bucket generation、旧参数拒绝。
2. API：管理员/设备授权、同一 JSON chart contract、查询 SQL 只访问 `snmp_samples` 且不含 tenant。
3. 真实集成：一次性 MySQL v2 schema + 固定 SNMP varbind -> existing poller -> CH；核对 `>2^53` counter、recipe 状态和无 tenant 列。
4. 真实 CH：raw query/rate/closed 5m generation；VictoriaMetrics 停止或不存在不影响链路。
5. 回归：目标 Go packages、`go test ./...`、`go vet ./...`、`go build ./...`；沙箱若禁止测试监听 socket，必须在允许本地 loopback 的环境重跑，不能改测试掩盖。

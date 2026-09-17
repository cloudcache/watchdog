# 运行预算、硬上限与异步任务审计

> 2026-09-17 审计基线。目标是区分“协议/安全不变量”和“部署运行预算”，避免把计费口径写成程序上限，也避免通过无限放大查询参数掩盖同步接口设计错误。

## 1. 四类限制

| 类型 | 是否可配置 | 原因 | 代表实现 |
|---|---:|---|---|
| 协议、格式、状态枚举 | 否 | 修改会破坏 wire/schema/签名兼容性 | sFlow/NetFlow 字段宽度、Kafka 坐标、publication schema version、billing 状态机 |
| 绝对安全硬上限 | 否（只能改代码并重新评审） | 防止恶意输入或错误配置导致 OOM、无界展开、超大 SQL/对象 | API body、地址集合深度、filter node、snapshot/signature bytes、CH 单次结果硬上限 |
| 部署运行预算 | 是，配置值不得超过硬上限 | 随 ClickHouse/主机规模调整，但必须可预测失败 | SNMP CH 执行时间、扫描行/字节、内存、中间结果行；Billing page/export/worker lease/retry |
| 产品请求参数 | 是，每次请求但受运行预算约束 | 控制响应形态，不得被误当成数据面读取上限 | `max_data_points`、page size、top N、导出格式 |

结论：源码中存在 `const` 并不等于不合理 hardcode。状态枚举和防御性硬上限必须稳定；真正的问题是此前 SNMP 把 `max_data_points` 同时当作最终图点数和修正前读取行数，以及 ClickHouse 运行预算没有配置入口。

## 2. 本轮已修正的 SNMP 图表预算

原错误流程：

1. hover 图请求 24h、`max_data_points=180`；
2. customer/supplier 修正规则要求按 5m 原始步进处理，共 288 个点；
3. 查询层把 180 当 ClickHouse 中间结果硬上限；
4. 修正尚未执行即报 `SNMP query exceeds row budget`。

当前流程：

1. `max_data_points` 只表示每条最终返回曲线的点数；
2. 中间行预算按 `时间范围 / 修正步进 × 显式端口数` 计算，device-wide 查询使用配置上限；
3. 始终先逐端口修正，再聚合；
4. 最后才有界降采样：rate/gauge 按显示桶平均，counter/state 取最后值；
5. 中间结果仍受 `snmp.query_max_intermediate_rows` 和 ClickHouse 读取预算保护。

可配置项在 `config/watchdog.yaml`：

- `query_max_intermediate_rows`
- `query_max_execution_time`
- `query_max_rows_to_read`
- `query_max_bytes_to_read`
- `query_max_memory_bytes`

这些是运行保护值，不参与端口修正、95th、平均值、总量、合同带宽、保底比例或金额公式。

## 3. 主要 max/limit 审计结论

| 领域 | 当前限制 | 分类 | 处置 |
|---|---|---|---|
| SNMP 同步图表 | 结果行、执行时长、扫描行/字节、内存 | 部署预算 + 绝对硬上限 | 本轮已配置化；普通 hover 仍是同步有界查询 |
| SNMP scope | 最多 1000 个 scope | API/防 OOM 硬上限 | 保留；批量导出走 job，不靠放大同步接口 |
| Billing account | 端口数、账期长度、page/export/provenance | 产品预算；reader 仍有临时硬上限 | YAML 已可配置且不得越过 reader 安全上限；KISS-07C 完成后用切片消除单查询依赖 |
| Billing CH reader | 120000 个 5m bucket、400 天 | 当前单块 reader 内存边界 | 不是计费规则；KISS-07C 改为分片/checkpoint 后再解除单块限制 |
| Flow 查询 | top/filter/result rows | 同步查询防滥用硬上限 | 保留；大结果使用现有异步 CSV/Parquet export |
| Flow writer | block rows/bytes/partition days | 默认运行预算 + 硬上限 | 已由 writer config/agent plan 控制，不改 fast decode 和 Kafka/CH 写入 |
| Flow reconciliation | batch/fact/read bytes | 部署预算 | 已在 YAML 配置；冷路径 operation job，不进入 ingest 热路径 |
| Address import/publication | upload bytes、batch rows、集合深度/展开量 | 上传预算 + 安全硬上限 | 导入/发布均为可追踪异步 job；集合数学上限保留 |
| VPN | rule/value/candidate/window | publication 结构与运行预算 | threshold/candidate/window 已配置；规则结构上限保留 |
| Migration/signed artifact | SQL/manifest/plan/snapshot bytes | 完整性和启动安全硬上限 | 保留，禁止运行时任意放大 |

## 4. 计费是否能接受

计费结果不能因为查询预算不同而改变。预算只允许产生三种结果：成功得到完整证据、可重试失败、或明确拒绝超预算；禁止截断后继续出账。

当前小账期计算/对账/导出已通过 `operation_jobs` 异步执行，HTTP 返回 `202 + job`，因此不会占住浏览器请求。但内部 SNMP/Flow reader 仍对整个账期各发一个大查询，结果也会先完整放入内存；“外层异步”不等于“内部渐进式”。这对普通月账期可用，对长账期、很多端口或大导出不够可靠。

## 5. KISS-07C 的必要完成条件

- SNMP 与 Flow 按同一组 UTC 5m 时间片读取，单片有独立 execution/thread/memory/read budget。
- 每片写 fenced checkpoint（`next_from`、source generation、累积统计器、日 95 分组状态）；崩溃、lease takeover 后从检查点继续。
- 未完成 generation 不进入正式账期值；所有片完成后在一个 MySQL 事务原子发布。
- CSV/Parquet 按页读取并流式写临时文件，`fsync + rename` 后才发布 artifact；取消/失败不暴露半文件。
- `operation_jobs` 报告 bucket/row/phase 进度，支持 cancel/retry；不得在 Billing 内再造第二套 job 状态机。
- 95th/日 95/平均/总量在切片前后必须与黄金向量逐字段一致；预算只影响吞吐和失败方式，不影响金额。

## 6. 不应异步化的路径

普通页面折线和端口 hover 应保持短小的同步有界查询：它们需要即时交互，并且通过服务端步进、预算和最终降采样即可稳定完成。超长范围、跨大量端口、明细全量和证据导出才转 operation job。把每次 hover 都改成后台任务会增加状态、轮询和垃圾清理，却不能解决错误的点数/中间行语义。

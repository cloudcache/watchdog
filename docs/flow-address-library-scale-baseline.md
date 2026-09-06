# Flow 地址库规模基线（PLAT-04C3）

对 `internal/flowdimension` 的地址库编译路径（`CompileBundle`：prefix trie 构建 +
选择器集合成员解析 + 依赖 DAG）做的规模认证基线。不改数据面，只测既有路径。

复现：

```bash
go test ./internal/flowdimension -run '^$' -bench 'CompileBundle|AddressSet' -benchmem
go test ./internal/flowdimension -run TestAddressLibraryScaleRetainedMemory -v
WATCHDOG_ADDRESS_MANAGEMENT_SCALE=1 go test ./internal/watchdog -run '^TestAddressOperationScaleCertification$' -count=1 -v
```

基准与内存测试在 `internal/flowdimension/address_scale_bench_test.go`。
管理集合运算规模门禁在 `internal/watchdog/address_management_scale_test.go`。

## 基线数字（本机 darwin/arm64，参考量级，非绝对阈值）

| 场景 | 规模 | 时间 | 分配 |
|---|---|---|---|
| prefix 编译 | 1k | ~0.96 ms | ~1.06 MB / 8.1k allocs |
| prefix 编译 | 10k | ~10.2 ms | ~10.4 MB / 81k allocs |
| prefix 编译 | 100k | ~106 ms | ~102 MB / 809k allocs |
| set 编译（prefix 固定 20k） | 1k sets | ~124 ms | ~26 MB |
| set 编译（prefix 固定 20k） | 10k sets | ~532 ms | ~37 MB |
| set 编译（prefix 固定 20k，需抬 limit） | 50k sets | ~2.6 s | ~79 MB |
| 最坏 per-address overlap（20k prefix × 32 set，每址命中全部 32） | — | ~282 ms | ~266 MB churn |
| 保留内存（编译后常驻） | 100k prefix + 10k set | 7.1 MiB（**74 B/prefix**） | — |
| 集合规范化 | 50k 不相邻地址 | 28 ms | 55.6 MiB |
| overlap 最坏输入 | 50k 完全重叠地址 | 9 ms | 34.7 MiB |
| 结果上限拒绝 | 20k 隔离地址范围，展开 >200k prefix | 21 ms | 29.9 MiB |

## 结论

- **prefix 编译对网段数线性**（~1 µs/prefix，~1 KB/prefix 分配）；百万级网段按此外推约
  1 s 量级编译、~百 MB 分配，常驻按 74 B/prefix 约几十 MB——与「地址库常驻每副本一份」
  的容量假设一致。
- **set 编译对集合数线性**（扣除固定 prefix trie 成本后，每集合约几十 µs）；无
  二次爆炸。默认上限内（见下）编译在亚秒级。
- **最坏 overlap 由设计封顶**：单个地址的集合归属被 `MaxAddressSetsPerRecord`（默认
  **32**）限制，超限在**编译期**拒绝而非查询期爆炸；深 include 链若使某地址归属超 32
  会被拒。因此 query-time 的 per-address 集合展开天然有界。
- **管理集合运算受四道预算约束**：请求最多 50,000 个表达式、输出最多 200,000 个
  prefix、overlap 明细最多 1,000 条、HTTP request body 最多 4 MiB。50k 完全重叠仍计算
  完整的 1,249,975,000 对总数，同时返回 `overlap_details_cut_off=true`，不会靠截断总数
  掩盖冲突规模；结果超限整体返回错误，不返回部分集合。

## 设计上限（认证发现，作为回归阈值参照）

- `CompileLimits.MaxAddressSets` 默认 **10000**：50k 集合需显式抬高上限；默认档编译
  ~0.5 s。若产品要支持 >10k 租户集合，需评估把该上限连同 ~线性成本一起上调。
- `MaxAddressSetsPerRecord` 默认 **32**：单地址最多归属 32 个集合。
- 回归 tripwire：`TestAddressLibraryScaleRetainedMemory` 断言常驻堆
  **< 4096 B/prefix**（当前 74 B，留足余量，只在结构性泄漏/每址 map 膨胀时触发）。
- 管理运算 tripwire：每个规模场景必须在 **10s / 512 MiB 总分配**内完成；这是容纳
  CI/开发机差异的退化告警，不是对外 SLA。本机实测远低于阈值。
- preview handler 的普通单测构造超过 4 MiB 的真实 JSON body，锁定 HTTP 边界；输入、
  结果和 overlap 规模由 opt-in 测试锁定，避免让全库普通回归长期承担大 fixture 成本。

## 未覆盖（本切片不做，避免改数据面）

- MMDB/IPDB 真实解析→operation job batch/checkpoint→MySQL ready 的吞吐、峰值内存、
  crash/resume 和清理；必须使用各自合法的生产级数据库文件。
- 真实百万级导入 corpus 端到端。当前仓库只有 MaxMind 小型测试库；不能通过循环读取
  小 fixture 或复制回调计数伪造该结论，因此该项继续作为 C3b 独立门禁。

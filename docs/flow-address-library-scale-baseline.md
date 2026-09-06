# Flow 地址库编译规模基线（PLAT-04C3）

对 `internal/flowdimension` 的地址库编译路径（`CompileBundle`：prefix trie 构建 +
选择器集合成员解析 + 依赖 DAG）做的规模认证基线。不改数据面，只测既有路径。

复现：

```bash
go test ./internal/flowdimension -run '^$' -bench 'CompileBundle|AddressSet' -benchmem
go test ./internal/flowdimension -run TestAddressLibraryScaleRetainedMemory -v
```

基准与内存测试在 `internal/flowdimension/address_scale_bench_test.go`。

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

## 结论

- **prefix 编译对网段数线性**（~1 µs/prefix，~1 KB/prefix 分配）；百万级网段按此外推约
  1 s 量级编译、~百 MB 分配，常驻按 74 B/prefix 约几十 MB——与「地址库常驻每副本一份」
  的容量假设一致。
- **set 编译对集合数线性**（扣除固定 prefix trie 成本后，每集合约几十 µs）；无
  二次爆炸。默认上限内（见下）编译在亚秒级。
- **最坏 overlap 由设计封顶**：单个地址的集合归属被 `MaxAddressSetsPerRecord`（默认
  **32**）限制，超限在**编译期**拒绝而非查询期爆炸；深 include 链若使某地址归属超 32
  会被拒。因此 query-time 的 per-address 集合展开天然有界。

## 设计上限（认证发现，作为回归阈值参照）

- `CompileLimits.MaxAddressSets` 默认 **10000**：50k 集合需显式抬高上限；默认档编译
  ~0.5 s。若产品要支持 >10k 租户集合，需评估把该上限连同 ~线性成本一起上调。
- `MaxAddressSetsPerRecord` 默认 **32**：单地址最多归属 32 个集合。
- 回归 tripwire：`TestAddressLibraryScaleRetainedMemory` 断言常驻堆
  **< 4096 B/prefix**（当前 74 B，留足余量，只在结构性泄漏/每址 map 膨胀时触发）。

## 未覆盖（本切片不做，避免改数据面）

- MMDB/IPDB 真实导入吞吐/峰值内存（导入解析在别处，本基线只测编译）。
- API body/result limit 认证（属查询/API 面）。
- 真实百万级 corpus 端到端；此处用确定性合成 fixture 量级外推。

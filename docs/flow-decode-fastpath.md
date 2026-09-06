# Flow 解码快路径设计与性能基线

> 采集→Kafka→**解码**→分类→ClickHouse 链路中「解码」段的设计文档。
> 覆盖:zero-copy 信封解析、NetFlow v5 定长快解码器、sFlow v5 framing 快解码器、
> 零分配纪律、差分正确性门禁、**性能基线与瓶颈地图**(供进一步审计)。
> 代码:`internal/flowstream/`。相关提交见文末。

---

## 1. 背景:为什么要自研快路径

解码是全系统最高频操作(每个采样流记录一次)。原实现直接用 GoFlow2 的管道。profile 发现两类固有开销:

1. **反射式二进制读**:GoFlow2 用 `utils.BinaryDecoder`(`binary.Read` 语义 + `intDataSize`/`reflect`/`bytes.Buffer`)逐字段解析。NetFlow v5 里 3 处、v9/IPFIX 13 处、**sFlow 35 处**调用点。占 CPU ~40%。
2. **两段式 + 逐记录分配**:先解析成中间 struct,再 `Convert…` 成 protobuf,每记录分配地址 `[]byte`、每包 `netip.AddrPort.String()` 路由键。NetFlow v5 ~110 allocs/包,sFlow ~230/包。

自研快路径去掉这两类开销,输出与 GoFlow2 **逐字段一致**(差分门禁保证),对下游透明。

---

## 2. 架构总览

```
DecodeValue(value []byte)
  └─ parseRawFlowInto(value, &rawScratch, internID)   // §5 zero-copy 信封解析
  └─ Decode(&rawScratch)
       ├─ validateRawFlow / inspectFlowProtocol       // 定协议 + 边界
       ├─ metadata.recyclePending()                   // 回收上一批池化 buffer
       ├─ if NETFLOW_V5 && fastNetFlowV5  → decodeNetFlowV5Fast   // §3
       ├─ if SFLOW_5    && fastSFlow      → decodeSFlowV5Fast      // §4
       │      └─ 遇不可处理构造 → errSFlowFallback → 落到下面 GoFlow2
       └─ else / fallback                → GoFlow2 pipe(v9/IPFIX/sFlow 兜底) // §7
```

- **协议分发**:v5/sFlow 走快路径,**v9/IPFIX 仍走 GoFlow2**(模板/采样状态机,未自研)。
- **回落**:sFlow 遇 ExtendedGateway(BGP)或未知 sample format,`errSFlowFallback` 让整包重走 GoFlow2——**局部覆盖也永不产错**。
- 开关:`Decoder.fastNetFlowV5` / `Decoder.fastSFlow`(默认开;测试置 false 取 GoFlow2 做差分参照)。

### 2.1 Zero-copy 生命周期契约(关键,审计必读)

`Decode` 返回的 `DecodedBatch.Records`(`[]*goflowpb.FlowMessage`)以及其中的地址 `[]byte`,**引用**三类可复用/借来的内存,而非拷贝:

| 引用对象 | 后备 | 何时失效 |
|---|---|---|
| FlowMessage(NetFlow v5) | `netflowV5Backing` 复用数组 | 下次 `Decode` 覆写 |
| FlowMessage(sFlow) | `sflowBacking` 复用数组 | 下次 `Decode` 覆写 |
| record 指针切片 | `recordPtrs` 复用 | 下次 `Decode` 覆写 |
| 地址/Payload `[]byte` | 切 Kafka `value` buffer | value 被 franz-go 复用/回收 |

**契约:调用方必须在下一次 `Decode` 之前,把每个 batch map 进自有内存。** 每分区一个 `Decoder`(单 goroutine),worker 逐条 record 先 `adapter.Map`(标量按值拷、地址经 `canonicalAddress16` 深拷)再解下一条 → 天然满足。并发解码共享 GoFlow2 全局 `sync.Pool` 也安全:延迟回收(`recyclePending` 在下次 Decode 开头)使「在用」消息始终 checked-out。

> 门禁:`TestDecoderReuseKeepsBatchesCorrectAcrossDecodes`(顺序复用不串批)、
> `TestPartitionDecodersProcessPartitionsConcurrently`(8 分区读记录内容 + `-race`,
> 已用「改回立即回收即 FAIL」反证有效)。

---

## 3. NetFlow v5 定长快解码器(`netflow_v5_fast.go`)

NetFlow v5 是**定长**:24B 头 + N×48B 记录。按固定偏移直解,零反射、零中间 struct、零逐记录分配。

- 字段映射逐字段对齐 GoFlow2 `ConvertNetFlowLegacyRecord` + `ProcessMessageNetFlowLegacy`(采样率 = `SamplingInterval & 0x3FFF`;`baseTime = UnixSecs*1e9 + UnixNSecs`;时间戳 uint32 wrap 语义一致)。
- 地址(SrcAddr/DstAddr/NextHop)**切 payload**——GoFlow2 也是把 uint32 big-endian 回写,字节完全相同。
- pipe 层补的 `TimeReceivedNs`(= 收包时间)、`SamplerAddress`(= 源地址 `MarshalBinary`)也补上——差分测试抓出来的。
- 恶意/截断:header count 钳到 payload 实有记录数,GoFlow2 会吐幻影零记录,快路径不吐(**更安全的有意分歧**,`TestNetFlowV5FastClampsTruncatedCount` 锁定)。

---

## 4. sFlow v5 framing 快解码器(`sflow_v5_fast.go`)

sFlow 是 **TLV**(datagram 头 → 变长 samples → 变长 records),最常见的 `SampledHeader` 记录内含**被采样报文的以太网/IP/TCP 头**。

**策略:只自研 framing,抓包解析复用 GoFlow2 加固的 `ParseSampledHeader`**(不在攻击面最大的路径上重写以太网/IP 解析)。

- 后备 `[]protoproducer.ProtoProducerMessage`(内嵌 FlowMessage),以便调 `ParseSampledHeader(&msg, &sh)`;batch 取 `&backing[i].FlowMessage`。
- **处理**:FlowSample(1)/ExpandedFlowSample(3) → 一样本一 FlowMessage + 一条 metadata;record 类型 RAW→`ParseSampledHeader`、IPv4/IPv6/ExtendedRouter/ExtendedSwitch 直接映射。
- **跳过**:Counter(2/4)/Drop(5) 样本(GoFlow2 producer 也不映射);ETH/EgressQueue/ACL/Function/MPLS 等 record(按长度跳过)。
- **回落**:ExtendedGateway(1003,BGP AS-path/communities)或未知 sample format → `errSFlowFallback`。
- 数据报级字段 `SequenceNum`(= 包序列)、`SamplerAddress`(= **包内 agent IP**,非源地址——与 NetFlow 不同!)每消息补上。
- `TimeReceivedNs = TimeFlowStartNs = TimeFlowEndNs = tr`(sFlow 特有,GoFlow2 enrich 如此)。
- **安全**:每次读都 bounds-check(`sflowCursor`);整个快路径包在 `recoverDecoderPanic` 里(`ParseSampledHeader` 解不可信字节,可能 panic)。`samplesCount`/`recordsCount` 各钳 1000(对齐 GoFlow2 DDoS 护栏)。

`bytes` = FrameLength(RAW)/Length(IPv4/6);`packets` 恒为 1(GoFlow2 如此)。

---

## 5. Zero-copy Kafka 信封解析(`rawflow_fast.go`)

`proto.Unmarshal` 拷贝每个 bytes 字段——尤其**整个 payload**(30 记录约 1.5KB)。`parseRawFlowInto` 直接走 protobuf wire:

- `Payload`/`SourceAddress` **切 value 不拷**;`CollectorId`/`ListenerId` 经 `internID` 驻留(每分区一个 exporter,小固定集,warmup 后零分配;上限 `maxInternedIDs=1024` 防敌意膨胀)。
- 目标 `RawFlow`(`rawScratch`)跨调用复用。
- 未知字段(6/7/8 `TimestampSource`/`DecapsulationProtocol`/`RateLimit`,解码器不读,proto3 零值 0 字节)按 wire type 跳过 → 前向兼容。

---

## 6. 零分配纪律(池化清单)

| 池 | 字段 | 说明 |
|---|---|---|
| 信封目标 | `rawScratch` | 复用,不每包分配 |
| record 指针切片 | `recordPtrs` | 两路共用,复用 |
| 身份串 | `idIntern` | 驻留,warmup 后零分配 |
| sampler 地址(NetFlow) | `lastSampler{Addr,Bytes}` | 按源缓存 |
| FlowMessage(NetFlow) | `netflowV5Backing` | 复用 |
| FlowMessage(sFlow) | `sflowBacking` | 复用 |
| metadata(sFlow) | `sflowMetadata` | 复用 |
| 错误 | 包级 `errSFlow*` sentinel | **不在热路径 `errors.New`**(曾漏 1 alloc/样本) |

---

## 7. 正确性:差分门禁

**原则:任何快路径,输出必须与 GoFlow2 逐字段 `proto.Equal`。** 不自说自话。

| 测试 | 覆盖 |
|---|---|
| `TestNetFlowV5FastMatchesGoFlow2` | single/many/时间 wrap/饱和/空;逐记录 `proto.Equal` + batch 身份 |
| `TestSFlowFastMatchesGoFlow2` | IPv4/IPv6/抓包头/expanded/router/switch/计数跳过/多样本/IPv6 agent/gateway 回落;`proto.Equal` + metadata |
| `TestRawFlowZeroCopyParseMatchesProto` | 信封 vs `proto.Unmarshal`(含跳过字段、饱和标量) |
| `*ClampsTruncatedCount` / `*HandlesTruncatedSafely` / `*RejectsTruncated` | 恶意/截断:不 panic、不过读 |

差分法已两次证明其价值:抓出 NetFlow v5 漏设的 `TimeReceivedNs`/`SamplerAddress`,以及 sFlow 漏设的 `SequenceNum`/`SamplerAddress`。

---

## 8. 性能基线与「符合预期」判据

运行:`go test -run '^$' -bench 'BenchmarkDecode' -benchmem ./internal/flowstream/`
(A/B:同名 `…SlowPath` / `…Slow` 强制 `fast*=false` 取 GoFlow2。)

### 8.1 硬不变量(与机器无关,回归必须守住)

| 路径 | allocs/包 | 说明 |
|---|---|---|
| NetFlow v5 | **0** | 稳态零分配 |
| sFlow SampledIPv4/IPv6 | **0** | 稳态零分配 |
| sFlow SampledHeader | **≤ 30**(1/样本) | 全在复用的 `ParseSampledHeader` 内,非本包代码 |
| 信封 payload | **永不拷贝** | 切 value buffer |

> **任何令上述三条「0」变非 0 的改动,即视为回归**(通常是热路径里新引入的 `errors.New`/`make`/`append` 扩容/`MarshalBinary`)。

### 8.2 相对提升(同机 A/B vs GoFlow2,应满足)

| 路径 | 期望 ≥ |
|---|---|
| NetFlow v5 | 10× |
| sFlow SampledIPv4 | 6× |
| sFlow SampledHeader | 3× |

### 8.3 本会话参考绝对值(machine-dependent,Apple Silicon,`-8`)

| 路径 | GoFlow2 | 快路径 | 提升 | 每记录 | 吞吐 |
|---|---|---|---|---|---|
| NetFlow v5(30 rec) | 8,800 ns / 110 allocs / 5,309 B | **706 ns / 0 / 0** | 12.5× | ~24 ns | 42.5M/s |
| sFlow IPv4(30 样本) | 13,980 ns / 229 / 13,014 B | **1,791 ns / 0 / 0** | 7.8× | ~60 ns | 16.7M/s |
| sFlow Header(30 样本) | 15,580 ns / 259 / 13,974 B | **3,932 ns / 30 / 240 B** | 4.0× | ~131 ns | 7.6M/s |
| 信封解析(单独) | 344 ns / 5 / 1,744 B | **83 ns / 2 / 16 B** | 4× | — | — |

> **注意 per-record**:NetFlow v5(~24ns)比 sFlow(~60/131ns)更便宜——sFlow 每样本自带一圈框架(采样率/池/丢弃/进出口/source id/序列 + TLV 导航),SampledHeader 还多一层抓包解析。**sFlow 的「更省」体现在设备侧(无状态采样)与每 Gbps(1:N 采样 → 记录更少),不在单条 collector 解码成本上。**

---

## 9. 剩余瓶颈地图(供进一步审计)

CPU profile(`-cpuprofile`,`go tool pprof -top`)显示快路径内的热点:

### NetFlow v5(~706ns)
| 函数 | flat / cum | 性质 |
|---|---|---|
| `decodeNetFlowV5Fast` | 33% | 实际字段解析(binary reads) |
| **`FlowMessage.Reset` + `StoreMessageInfo` + `atomic.StorePointer`** | **~39% cum** | **protobuf 消息状态机(每消息 atomic)——#1 可优化点** |
| `parseRawFlowInto` | 11% | 信封 |
| binary reads(Uint16/32/Uvarint) | ~7% | 已内联 |

### sFlow IPv4(~1,791ns)
| 函数 | flat / cum | 性质 |
|---|---|---|
| **`sflowCursor.u32`** | **43%** | 逐字段 bounds-checked 读——TLV 固有(每样本 ~10-14 次 u32) |
| `decodeSFlowFlowSample` | 13% | 样本头/字段 |
| `mapSFlowRecord` | 19% cum | 记录映射 |
| `FlowMessage.Reset` | ~13% cum | 同上,protobuf 状态机 |

### 审计建议(候选,未做)
1. **`FlowMessage.Reset` 是两路的共同大头**(NetFlow ~39%、sFlow ~13%)。根因是用 protobuf `goflowpb.FlowMessage` 当载体,其状态机每次 Reset 走 atomic。**最大的下一步杠杆**:换成**精简 Go struct**(只含下游 `adapter.Map` 读的字段)当 batch 载体,消掉 protobuf 状态机开销。代价:改动 `DecodedBatch.Records` 类型 + `flowworker/decode_adapter.go` 的 `mapFlowMessage`(涉及 flowworker,需同样差分验证)。预估两路各再省 15–40%。
2. **sFlow `u32` 占 43%**:已内联、已 bounds-check,进一步只能靠「一次校验一段、批量读」或 unsafe,收益/风险比一般。可先不动。
3. **SampledHeader 的 30 allocs**:全在 GoFlow2 `ParseSampledHeader`。要清零须自研零拷贝抓包解析器——**不建议**(安全敏感,收益不抵风险)。
4. **v9/IPFIX 仍走 GoFlow2**(反射 + 模板)。若这两协议进入主力流量,是下一个自研目标(模板状态机较难)。

---

## 10. 非目标 / 已知取舍

- v9/IPFIX/NetStream 未自研,回落 GoFlow2。
- sFlow ExtendedGateway(BGP)回落 GoFlow2。
- 畸形**未映射** record(如截断的 ETH):快路径按长度跳过、比 GoFlow2 更宽松(GoFlow2 会整包报错)。已映射 record 截断则报错,与 GoFlow2 一致。
- per-record 上 NetFlow v5 比 sFlow 便宜(见 §8.3 注)。

---

## 11. 提交

- `1cc27c90` zero-copy 信封解析
- `649001c9` NetFlow v5 零分配(池化最后 3 处)
- `93fa3e92` NetFlow v5 定长快解码器
- `11e4dc08` sFlow v5 framing 快解码器
- 关联可靠性条目见 `flow-reliability-remediation.md` F15/F15b/F15c/F15d。

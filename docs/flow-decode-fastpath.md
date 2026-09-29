# Flow 解码快路径设计与性能基线

> **与代码的差异（2026-09-28 复核）**：本文有 2 处已落后于代码或与代码不一致——以代码为准，逐条见文末「与代码的差异（2026-09-28 复核）」。

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

`Decode` 返回的 `DecodedBatch.Records`(`[]DecodedRecord`,§2.2)以及其中的地址 `[]byte`,**引用**可复用/借来的内存,而非深拷:

| 引用对象 | 后备 | 何时失效 |
|---|---|---|
| DecodedRecord(所有路径) | `recordBacking` 复用值数组 | 下次 `Decode` 覆写 |
| 地址 `[]byte`(NetFlow/sFlow IPv4/6) | 切 Kafka `value` buffer(payload) | value 被 franz-go 复用/回收 |
| 地址 `[]byte`(sFlow SampledHeader) | GoFlow2 `ParseSampledHeader` 产出 | 下次 SampledHeader Reset scratch |
| 地址 `[]byte`(慢路径 v9/IPFIX) | GoFlow2 池化 buffer | 下次 `Decode` 回收 |

**契约:调用方必须在下一次 `Decode` 之前,把每个 batch map 进自有内存。** 每分区一个 `Decoder`(单 goroutine),worker 逐条 record 先 `adapter.Map`(标量按值拷、地址经 `canonicalAddress16` 深拷)再解下一条 → 天然满足。并发解码共享 GoFlow2 全局 `sync.Pool` 也安全:延迟回收(`recyclePending` 在下次 Decode 开头)使「在用」消息始终 checked-out。

> 门禁:`TestDecoderReuseKeepsBatchesCorrectAcrossDecodes`(顺序复用不串批)、
> `TestPartitionDecodersProcessPartitionsConcurrently`(8 分区读记录内容 + `-race`,
> 已用「改回立即回收即 FAIL」反证有效)。

### 2.2 精简载体 `DecodedRecord`(commit `16ca927a`)

batch 载体是**值结构 `DecodedRecord`**(**不是** GoFlow2 的 protobuf `FlowMessage`,后者 `Reset` 每消息走 atomic,曾是解码 #1 开销),但**含这些协议下 GoFlow2 产出的全部有意义字段**——不只当前 `mapFlowMessage` 读的:Type、Src/DstAddr、NextHop、SamplerAddress、Src/DstPort、Proto、TcpFlags、IpTos、Etype、In/OutIf、Src/DstAs、Src/DstNet、Src/DstVlan、SequenceNum、Bytes、Packets、SamplingRate、TimeFlow{Start,End}/ReceivedNs(共 26 字段)。**在解码边界不丢数据**——`SequenceNum`(丢包检测)、`Src/DstNet`(前缀聚合)、VLAN(L2)、`IpTos`(QoS)、`SamplerAddress`(exporter 归属)都是流平台可能要的。字段名与 `FlowMessage` 对齐,故 adapter 函数体不变。慢路径(v9/IPFIX、sFlow 回落)用 `recordFromFlowMessage` 把 GoFlow2 消息转成 `DecodedRecord`。
>
> **教训**:第一版精简到「只含当前读的 17 字段」是**过度精简**——把 GoFlow2 产的 9 个字段丢了。「今天没人读」≠「不需要」。已改回全字段(代价:NetFlow +~170ns、sFlow IPv4 +~226ns,仍远快于 protobuf 载体)。

```go
type DecodedRecord struct {
    SrcAddr, DstAddr, NextHop, SamplerAddress []byte // 地址:切 payload / GoFlow2 buffer,零拷贝
    Type                                      goflowpb.FlowMessage_FlowType
    SrcPort, DstPort, Proto, TcpFlags, IpTos, Etype  uint32
    InIf, OutIf, SrcAs, DstAs, SrcNet, DstNet, SrcVlan, DstVlan, SequenceNum uint32
    Bytes, Packets, SamplingRate                     uint64
    TimeFlowStartNs, TimeFlowEndNs, TimeReceivedNs   uint64
}
// 慢路径转换(v9/IPFIX、sFlow 回落):recordFromFlowMessage(dst, m) 逐字段拷这 26 个。
// 字段名与 goflowpb.FlowMessage 对齐 → flowworker/decode_adapter.go 的 mapFlowMessage 函数体不变,
// 只把参数类型 *goflowpb.FlowMessage 改成 *flowstream.DecodedRecord。
```

---

## 3. NetFlow v5 定长快解码器(`netflow_v5_fast.go`)

NetFlow v5 是**定长**:24B 头 + N×48B 记录。按固定偏移直解,零反射、零中间 struct、零逐记录分配。

- 字段映射逐字段对齐 GoFlow2 `ConvertNetFlowLegacyRecord` + `ProcessMessageNetFlowLegacy`(采样率 = `SamplingInterval & 0x3FFF`;`baseTime = UnixSecs*1e9 + UnixNSecs`;时间戳 uint32 wrap 语义一致),填 `DecodedRecord` 全部字段。
- 地址(SrcAddr/DstAddr)**切 payload**——GoFlow2 也是把 uint32 big-endian 回写,字节完全相同。
- pipe 层补的 `TimeReceivedNs`(= 收包时间)、`SamplerAddress`(= 源地址 `MarshalBinary`,按源缓存 0-alloc)也补上——差分测试抓出来的。
- 恶意/截断:header count 钳到 payload 实有记录数,GoFlow2 会吐幻影零记录,快路径不吐(**更安全的有意分歧**,`TestNetFlowV5FastClampsTruncatedCount` 锁定)。

### 3.1 wire 布局与映射(代码级)

**头(24B)**:`[0:2]` version=5、`[2:4]` count、`[4:8]` SysUptime(=uptime)、`[8:12]` UnixSecs、`[12:16]` UnixNSecs、`[16:20]` FlowSequence、`[22:24]` SamplingInterval。
`baseTime = UnixSecs*1e9 + UnixNSecs`;`samplingRate = SamplingInterval & 0x3FFF`(高 2 位是采样模式)。

**记录(48B,`decodeNetFlowV5Fast` 一次性 `*r = DecodedRecord{…}` 填满)**:

| off | 宽 | 源 | → DecodedRecord |
|---|---|---|---|
| 0 / 4 / 8 | 4 | SrcAddr / DstAddr / NextHop | `record[0:4]`/`[4:8]`/`[8:12]`(**切片,零拷贝**) |
| 12 / 14 | 2 | Input / Output | `InIf` / `OutIf` |
| 16 / 20 | 4 | dPkts / dOctets | `Packets` / `Bytes` |
| 24 / 28 | 4 | First / Last | → 时间戳(下) |
| 32 / 34 | 2 | SrcPort / DstPort | `SrcPort` / `DstPort` |
| 37 / 38 / 39 | 1 | TCPFlags / Proto / Tos | `TcpFlags` / `Proto` / `IpTos` |
| 40 / 42 | 2 | SrcAS / DstAS | `SrcAs` / `DstAs` |
| 44 / 45 | 1 | SrcMask / DstMask | `SrcNet` / `DstNet` |

常量:`Type=NETFLOW_V5`、`Etype=0x800`、`SequenceNum=header.FlowSequence`;`TimeReceivedNs`(收包时间)与 `SamplerAddress`(源地址 `MarshalBinary`,按源缓存)由 pipe 层语义补上。

**时间戳(与 GoFlow2 逐位一致的 uint32 回绕)**:
```go
// First/Last 是相对 uptime 的毫秒;uint32 减法可回绕,×1e6 转 ns。BE = binary.BigEndian
r.TimeFlowStartNs = baseTime - uint64(uptime-BE.Uint32(record[24:28]))*1_000_000
r.TimeFlowEndNs   = baseTime - uint64(uptime-BE.Uint32(record[28:32]))*1_000_000
```

---

## 4. sFlow v5 framing 快解码器(`sflow_v5_fast.go`)

sFlow 是 **TLV**(datagram 头 → 变长 samples → 变长 records),最常见的 `SampledHeader` 记录内含**被采样报文的以太网/IP/TCP 头**。

**策略:只自研 framing,抓包解析复用 GoFlow2 加固的 `ParseSampledHeader`**(不在攻击面最大的路径上重写以太网/IP 解析)。

- 输出到复用的 `[]DecodedRecord`(§2.2)。**RAW/SampledHeader**(抓包)先试 `parseSampledPacket`——对无标签 Ethernet/IPv4-6/TCP-UDP **按固定偏移直接切 5 元组**(零拷贝零分配,匹配 GoFlow2 `ParsePacket`:Etype=外层 ethertype、IPv4 定长 20B);VLAN/IP options/IPv6 扩展头/分片/异常 ethertype **回落**到复用 scratch + GoFlow2 加固 `ParseSampledHeader`(scratch **plain-zero** 免 atomic,`copyPacketFields` 拷出)。`ParsePacket` 对真帧 51% CPU + ~35 allocs/包,这是 sFlow 唯一还碰 protobuf 的路径。
- **处理**:FlowSample(1)/ExpandedFlowSample(3) → 一样本一 `DecodedRecord` + 一条 metadata;record 类型 RAW→上述抓包快路径、IPv4/IPv6 填 5 元组+IpTos+Etype、ExtendedRouter 填 NextHop/Src/DstNet、ExtendedSwitch 填 Src/DstVlan。
- **跳过**:Counter(2/4)/Drop(5) 样本(GoFlow2 producer 也不映射);ETH/EgressQueue/ACL/Function/MPLS 等 record(GoFlow2 也不映射任何 `DecodedRecord` 字段,按长度跳过)。
- **回落**:ExtendedGateway(1003,BGP,设 SrcAs/DstAs 是下游要读的)或未知 sample format → `errSFlowFallback`。
- `TimeReceivedNs = TimeFlowStartNs = TimeFlowEndNs = tr`(sFlow 特有,GoFlow2 enrich 如此)。`SequenceNum`(= 包序列)、`SamplerAddress`(= **包内 agent IP**,非源地址——与 NetFlow 不同!)每记录补上。
- **TLV 批量读**:定长字段组用「一次 `remaining()` 守卫 + `(*[N]byte)` 数组指针转换 + 常量偏移 `Uint32`」读,编译器省掉逐字段边界检查(`u32` 曾占 46% CPU)。地址从窗口切出、零拷贝。
- **安全**:每次读都 bounds-check(批量读也一样,一次守卫覆盖整组);整个快路径包在 `recoverDecoderPanic` 里(`ParseSampledHeader` 解不可信字节,可能 panic)。`samplesCount`/`recordsCount` 各钳 1000(对齐 GoFlow2 DDoS 护栏)。

`bytes` = FrameLength(RAW)/Length(IPv4/6);`packets` 恒为 1(GoFlow2 如此)。

### 4.1 批量读 idiom 与 wire 布局(代码级)

**批量读**(§8/§9 的核心加速):一组定长字段一次 `remaining()` 守卫,转定长数组指针,再按常量偏移读——编译器省掉逐字段边界检查:
```go
if c.remaining() < 32 { return DecodedRecordMetadata{}, errSFlowTruncatedSample }
w := (*[32]byte)(c.buf[c.off:]) // 定长数组指针:一次长度检查(被上面 remaining() 覆盖)
c.off += 32
samplingRate := binary.BigEndian.Uint32(w[8:12]) // 常量索引进 [32]byte → 无逐字段边界检查
```

**wire**:
- **datagram 头**:version(4)、agentIPType(4)+agentIP(0/4/16),然后 16B 窗口 = subAgentId(4)+sequence(4)+uptime(4)+samplesCount(4)。
- **每 sample**:`format(4)+length(4)` → body = length 字节。format 1/3=Flow/ExpandedFlow(处理)、2/4=Counter/ExpCounter、5=Drop(跳过)、其它→回落。
- **FlowSample(1) 定长前缀 32B**:seq、sourceId(**交错**:type=高8位、value=低24位)、samplingRate、samplePool、drops、input、output、recordsCount。**ExpandedFlowSample(3) 44B**:多 in/outIfFormat 各 4B(跳过)。
- **每 record**:`dataFormat(4)+length(4)` → body。`SampledIPv4(3)` 32B(srcIP=w[8:12]、dstIP=w[12:16] 切片,`Etype=0x800`);`SampledIPv6(4)` 56B(srcIP=w[8:24]、dstIP=w[24:40],`IpTos=priority`、`Etype=0x86dd`);`ExtendedRouter(1002)`=DecodeIP+srcMask+dstMask→NextHop/Src/DstNet;`ExtendedSwitch(1001)`=srcVlan/dstVlan;`ExtendedGateway(1003)`/未知→`errSFlowFallback`;其余(ETH/ACL/…)按长度跳过。

### 4.2 SampledHeader 抓包快提取(`parseSampledPacket`,代码级)

无标签 Ethernet/IPv4-6/TCP-UDP 定长切 5 元组(全 bounds-checked,地址切帧):
```
Ethernet:  frame[12:14]=etype;== 0x8100(VLAN)→ 回落
IPv4(0x0800): l3=frame[14:]; IHL=l3[0]&0xF(≠5=options→回落); 分片(l3[6]&0x3F || l3[7]→回落)
              IpTos=l3[1]; Proto=l3[9]; SrcAddr=l3[12:16]; DstAddr=l3[16:20]; L4=l3[20:]
IPv6(0x86dd): IpTos=(l3[0]&0xF)<<4 | l3[1]>>4; Proto=l3[6](扩展头→在 L4 处回落)
              SrcAddr=l3[8:24]; DstAddr=l3[24:40]; L4=l3[40:]
L4  TCP(6):   SrcPort=[0:2]; DstPort=[2:4]; TcpFlags=[13]
    UDP(17):  SrcPort=[0:2]; DstPort=[2:4]
    其它:     仅 IP 层字段(与 GoFlow2 一致)
```
必须匹配 GoFlow2 `ParsePacket` 两个 quirk:**Etype = 外层 ethertype**、**IPv4 固定按 20B 头**(所以 IHL≠5 回落)。任一读越界或遇不理解的构造 → 返回 false → 回落 `ParseSampledHeader`(那 30 allocs 在此,仅回落路径)。

---

## 5. Zero-copy Kafka 信封解析(`rawflow_fast.go`)

`proto.Unmarshal` 拷贝每个 bytes 字段——尤其**整个 payload**(30 记录约 1.5KB)。`parseRawFlowInto` 直接走 protobuf wire:

- `Payload`/`SourceAddress` **切 value 不拷**;`CollectorId`/`ListenerId` 经 `internID` 驻留(每分区一个 exporter,小固定集,warmup 后零分配;上限 `maxInternedIDs=1024` 防敌意膨胀)。
- 目标 `RawFlow`(`rawScratch`)跨调用复用。
- 未知字段(6/7/8 `TimestampSource`/`DecapsulationProtocol`/`RateLimit`,解码器不读,proto3 零值 0 字节)按 wire type 跳过 → 前向兼容。

### 5.1 wire 解析循环(代码级)

protobuf wire:每字段 `tag = fieldNum<<3 | wireType`(varint)。`parseRawFlowInto` 手写这个循环,只认解码器要的字段号:
```go
for len(value) > 0 {
    tag, n := binary.Uvarint(value); value = value[n:]
    field := tag >> 3
    switch tag & 0x7 {
    case 0: // varint: 1=TimeReceived 4=UseSourceAddress 5=Decoder 11=SourcePort 12=RegistryVersion
    case 2: // length-delimited:
        //   2=Payload / 3=SourceAddress → data := value[:length](切 value buffer,零拷贝)
        //   9=CollectorId / 10=ListenerId → internID(data)(驻留,warmup 后零分配)
    case 1: value = value[8:] // 64-bit:跳过
    case 5: value = value[4:] // 32-bit:跳过(未知字段 6/7/8 等)
    }
}
```
`Payload`/`SourceAddress` 直接切 `value`(Kafka record bytes),整个 flow payload(~1.5KB)不拷贝;身份串经 `internID` 用 `map[string]string` 驻留(`m[string(data)]` 查表不分配),上限 `maxInternedIDs=1024`。

---

## 6. 零分配纪律(池化清单)

| 池 | 字段 | 说明 |
|---|---|---|
| 信封目标 | `rawScratch` | 复用,不每包分配 |
| DecodedRecord 数组 | `recordBacking` | 三路共用(§2.2),复用 |
| 身份串 | `idIntern` | 驻留,warmup 后零分配 |
| metadata(sFlow) | `sflowMetadata` | 复用 |
| SampledHeader scratch | `sflowHeaderScratch` | 仅 RAW 记录用,复用 |
| 错误 | 包级 `errSFlow*` sentinel | **不在热路径 `errors.New`**(曾漏 1 alloc/样本) |

---

## 7. 正确性:差分门禁

**原则:任何快路径,输出必须与 GoFlow2 逐字段相等。** 不自说自话。载体换精简 struct 后,比较由 `reflect.DeepEqual(DecodedRecord)` 做——慢路径把 GoFlow2 消息 `recordFromFlowMessage` 转成 `DecodedRecord`,故仍是「快路径 vs GoFlow2」。只比下游真正消费的字段,正是**恰当**的严格度(不读的字段不影响管线输出)。

| 测试 | 覆盖 |
|---|---|
| `TestNetFlowV5FastMatchesGoFlow2` | single/many/时间 wrap/饱和/空;逐记录 `DeepEqual` + batch 身份 |
| `TestSFlowFastMatchesGoFlow2` | IPv4/IPv6/抓包头/expanded/router/switch/计数跳过/多样本/IPv6 agent/gateway 回落;`DeepEqual` + metadata |
| `TestRawFlowZeroCopyParseMatchesProto` | 信封 vs `proto.Unmarshal`(含跳过字段、饱和标量) |
| `*ClampsTruncatedCount` / `*HandlesTruncatedSafely` / `*RejectsTruncated` | 恶意/截断:不 panic、不过读 |

差分法已多次证明其价值:抓出 NetFlow v5 漏设的 `TimeReceivedNs`/`SamplerAddress`、sFlow 漏设的 `SequenceNum`/`SamplerAddress`。

> **陷阱(教训)**:差分「两路一致」只保证**相对**正确,不保证**绝对**正确——若 fixture 本身喂了垃圾,两路可能一致地产生垃圾。实例:`SampledHeader` fixture 曾漏设 `OriginalLength`(sFlow 编码器把它当 XDR opaque 长度写),导致编码出**空帧**,两路都解出空 → 差分通过、但抓包解析其实**从没被真正测过**。补上 `OriginalLength` 后才暴露真实的 `ParsePacket` 成本(48µs/1039 allocs)与快路径收益(35×)。**写差分 fixture 时务必确认 fixture 编码出的是真实数据。**

---

## 8. 性能基线与「符合预期」判据

运行:`go test -run '^$' -bench 'BenchmarkDecode' -benchmem ./internal/flowstream/`
(A/B:同名 `…SlowPath` / `…Slow` 强制 `fast*=false` 取 GoFlow2。)

### 8.1 硬不变量(与机器无关,回归必须守住)

| 路径 | allocs/包 | 说明 |
|---|---|---|
| NetFlow v5 | **0** | 稳态零分配 |
| sFlow SampledIPv4/IPv6 | **0** | 稳态零分配 |
| sFlow SampledHeader(常见:无标签 IPv4/6+TCP/UDP) | **0** | `parseSampledPacket` 直接切帧,零拷贝 |
| sFlow SampledHeader(回落:VLAN/IP options/IPv6 ext/分片) | GoFlow2 `ParsePacket`(~35/包) | 少见路径,用加固解析器 |
| 信封 payload | **永不拷贝** | 切 value buffer |

> **任何令上述三条「0」变非 0 的改动,即视为回归**(通常是热路径里新引入的 `errors.New`/`make`/`append` 扩容/`MarshalBinary`)。

### 8.2 相对提升(同机 A/B vs GoFlow2,应满足)

| 路径 | 期望 ≥ | 当前 |
|---|---|---|
| NetFlow v5 | 12× | 15.3× |
| sFlow SampledIPv4 | 7× | 9.2× |
| sFlow SampledHeader(无标签常见) | 15× | 50× |

### 8.3 本会话参考绝对值(machine-dependent,Apple Silicon,`-8`)

载体为 `DecodedRecord` 值结构(§2.2,含全部有意义字段),非 protobuf `FlowMessage`。

| 路径 | GoFlow2 | 快路径(精简 struct) | 提升 | 每记录 | 吞吐 |
|---|---|---|---|---|---|
| NetFlow v5(30 rec) | 8,800 ns / 110 allocs / 5,309 B | **575 ns / 0 / 0** | 15.3× | ~19 ns | 52M/s |
| sFlow IPv4(30 样本) | 15,370 ns / 229 / 13,014 B | **781 ns / 0 / 0** | 19.7× | ~26 ns | 38M/s |
| sFlow Header 无标签(30 样本) | 48,813 ns / 1,039 / 31,510 B | **971 ns / 0 / 0** | **50×** | ~32 ns | 31M/s |
| 信封解析(单独) | 344 ns / 5 / 1,744 B | **83 ns / 2 / 16 B** | 4× | — | — |

> **值结构 vs protobuf 载体的收益**:去掉 protobuf `FlowMessage.Reset` 的每消息 atomic + 更小清零(26 字段 vs 40+ 且无状态机)+ 缓存局部性。NetFlow v5 706→**575ns(−19%)**、sFlow IPv4 1,791→1,480→**781ns**(含全字段;末段 −47% 来自 TLV 批量读),零分配。SampledHeader 见下方 §8.3.1。
>
> **注意 per-record**:NetFlow v5(~19ns)与 sFlow IPv4(~26ns)已接近(TLV 批量读拉近);sFlow Header ~32ns——sFlow 每样本自带一圈框架(采样率/池/丢弃/进出口/source id/序列 + TLV 导航),SampledHeader 还多一层抓包解析。**sFlow 的「更省」体现在设备侧(无状态采样)与每 Gbps(1:N 采样 → 记录更少),不在单条 collector 解码成本上。**

---

## 9. 剩余瓶颈地图(供进一步审计)

CPU profile(`-cpuprofile`,`go tool pprof -top`)显示换精简 struct **之后**的热点。`FlowMessage.Reset` 已从两路 profile 消失(这是之前的 #1 杠杆,已做)。现在时间主要花在**真实解析工作**上,而非机制开销——健康的 profile。

### NetFlow v5(~575ns)
| 函数 | flat / cum | 性质 |
|---|---|---|
| `decodeNetFlowV5Fast` | **64%** | 实际字段解析 + 填 struct(binary reads)——已是真实工作 |
| `parseRawFlowInto` | 18% cum | 信封 wire 解析 |
| binary reads(Uint16/Uvarint) | ~10% | 已内联 |
| `internOrCopy` | 5% | 身份串 intern 查表 |

### sFlow IPv4(~781ns)
`FlowMessage.Reset` 和 `sflowCursor.u32` 都已从 profile 消失。剩余由 `decodeSFlowFlowSample`/`decodeSFlowV5Fast` 的真实解码逻辑 + 内联 `binary.BigEndian.Uint32` 读(~8%)主导;批量读的 `remaining()` 守卫仅 ~5%。

### 审计建议(剩余,收益递减)
1. ~~换值结构载体~~ **已做**(commit `16ca927a` + `c19106be`):NetFlow v5 −19%、sFlow IPv4 −17%(含全字段)。两路的 `FlowMessage.Reset` 大头已消除。第一版曾过度精简丢字段,已改回(§2.2 教训)。
2. ~~SampledHeader 的 allocs 在 GoFlow2 `ParsePacket`~~ **已做**(commit `1646540a`):`parseSampledPacket` 对无标签 Ethernet/IPv4-6/TCP-UDP 定长切 5 元组,零拷贝零分配,VLAN/options/ext/分片回落加固解析器。真帧 48,813ns/1,039 allocs → **971ns/0(50×)**。
3. ~~sFlow `u32` 占 46%(逐字段 bounds check)~~ **已做**(commit `adcd9fbc`):**TLV 批量读**——每组字段一次 `remaining()` 守卫 + `(*[N]byte)` 数组指针转换,让编译器省掉逐字段边界检查,再按常量偏移 `Uint32` 读。sFlow IPv4 −47%(1,480→781ns)、Header −29%(1,374→971ns),`u32` 从 profile 消失。差分门禁不变(仍逐字段对 GoFlow2)。
4. **NetFlow v5 现在 64% 在 `decodeNetFlowV5Fast`(真实 binary 解析)**:已内联、无分配,进一步只能靠 SIMD/unsafe,收益/风险比一般。
5. **信封 18%**:`parseRawFlowInto` 已是 wire-level partial parse,难再压。`internOrCopy` 5% 若 collector/listener 预解析成 id 可省,但 collision 与改动面不值。
6. **v9/IPFIX 仍走 GoFlow2**(反射 + 模板)。若这两协议进入主力流量,是下一个自研目标(模板状态机较难)。

---

## 10. 非目标 / 已知取舍

- v9/IPFIX/NetStream 未自研,回落 GoFlow2。
- sFlow ExtendedGateway(BGP)回落 GoFlow2。
- 畸形**未映射** record(如截断的 ETH):快路径按长度跳过、比 GoFlow2 更宽松(GoFlow2 会整包报错)。已映射 record 截断则报错,与 GoFlow2 一致。
- per-record 上 NetFlow v5 比 sFlow 便宜(见 §8.3 注)。

---

## 11. SIMD 可行性研究(结论:暂不采用,触发条件见 §11.4)

### 11.1 背景:decode 还剩什么可能被 SIMD 加速

优化到现在,decode 的 CPU 都花在**真实二进制解析**上(§9):NetFlow v5 ~64% 在 `decodeNetFlowV5Fast` 的逐字段读——每个 `binary.BigEndian.Uint16/Uint32(record[k:])` = 一次 load + 一次 **BSWAP**(大端→小端);sFlow 批量读后也是内联 `Uint32`。理论上 SIMD 能**一条指令批量翻转字节序**:如 AVX2 `VPSHUFB` 配一个 shuffle mask,一次翻转 16–32 字节里的 4–8 个 u32,替掉逐字段的 load+BSWAP。

### 11.2 三条路线(附链接调研)

| 路线 | 实现 | 覆盖 | 平台 | 成熟度 | 对本用途 |
|---|---|---|---|---|---|
| **Go 官方实验性 `simd` 包**<br>[Callista 博客 2025-10](https://callistaenterprise.se/blogg/teknik/2025/10/20/trying-out-go-simd-support/) | 原生 intrinsics,可内联;`dev.simd` 分支 / `gotip` / `GOEXPERIMENT=simd` / build tag `goexperiment.simd && amd64` | `Load/Add/Store` 等向量运算 | **仅 amd64** | 实验性,预计 ~Go 1.26 转正,无自动向量化 | 长期最有希望;best-case u8 向量加 inlined 0.48ns vs 朴素 17.48ns(~36×)。**但现在仅 amd64 + gotip** |
| [stuartcarnie/go-simd](https://github.com/stuartcarnie/go-simd) | 运行时按 CPU 选 SSE4/AVX2 汇编 + clang 自动向量化 C + Go 循环展开 | **聚合类**:`SumFloat64`、UTF-8/ASCII 校验 | x86-64 + 泛型 fallback | 25 commits,偏参考实现 | **不匹配**:无字节序翻转/定长整数解析 |
| [alivanz/go-simd](https://github.com/alivanz/go-simd) | `linkname` 绕过 cgo(~33.6 GB/s vs cgo 7.9)的 NEON intrinsics | 向量加/乘等 | **仅 ARM NEON**(x86 在做) | 71 commits,早期 | **不匹配**:无大端处理、无网络包解析,且非 amd64 |

### 11.3 适用性分析(为什么现在 ROI 为负)

1. **收益上限有限**:decode 已 ~19–32 ns/记录,大概率 **memory-bound**——load 才是主成本,BSWAP 现代 CPU ~1 cycle。SIMD 只省 BSWAP 那一小块。
2. **extract 省不掉**:即便一条指令翻转整条记录,**仍要逐字段从 SIMD 寄存器 extract 进 `DecodedRecord`**(标量活)。NetFlow v5 字段宽度**异构**(u16 口/if、u32 址/计数、u8 flags/proto),extract 无法向量化。
3. **地址本就零拷贝**:IP 地址是**切片**(保持网络字节序的 `[]byte`,§2.1),根本不翻转——SIMD 对最大的字段(地址)无用。
4. **工具链/可移植性**:官方 `simd` 实验性且**仅 amd64**,而基线机器是 **arm64(Apple Silicon)**;两个第三方库一个只做聚合、一个只 ARM。任何 SIMD 都要 per-arch 实现 + 标量 fallback,维护面 ×N,还破坏「纯 Go、易审计」。

### 11.4 结论与触发条件

**结论:暂不采用。** decode 现在 20–75M 记录/秒/核、零分配、纯 Go 可审计;SIMD 目标的 BSWAP 只是其中一小块,且要 experimental/不匹配的工具,ROI 为负。

**何时重启**:同时满足 (1) 官方 `simd` 包**转正(~Go 1.26)且支持 arm64**;(2) 生产压测证明 decode 是**实测吞吐瓶颈**(目前远不是)。届时最小切入:给 NetFlow v5 定长记录写一个**批量大端翻转 primitive**(官方 `simd`,或 `avo` 生成汇编——博客里 AVO 版向量加 2.7ns),置于 build tag 之后 + 差分门禁验证,**标量版永远作为 fallback**。不引第三方库。

---

## 12. 提交

- `93fa3e92` NetFlow v5 定长快解码器
- `1cc27c90` zero-copy 信封解析
- `649001c9` NetFlow v5 零分配(池化最后 3 处)
- `11e4dc08` sFlow v5 framing 快解码器
- `16ca927a` `DecodedRecord` 值载体(去掉热路径的 protobuf `FlowMessage`)
- `c19106be` `DecodedRecord` 改回含全部有意义字段(修过度精简)+ 修 sFlow header path;NetFlow v5 −19%、sFlow IPv4 −17%,均零分配
- `1646540a` sFlow SampledHeader 定长 5 元组抓包快解析器(无标签 Ethernet/IPv4-6/TCP-UDP 零拷贝零分配,VLAN/options 回落);修 fixture `OriginalLength` bug;真帧 48µs/1039allocs → 1.37µs/0(35×)
- `adcd9fbc` sFlow TLV 批量读(`(*[N]byte)` 数组指针省逐字段边界检查);sFlow IPv4 −47%(→781ns/19.7×)、Header −29%(→971ns/50×),均零分配
- 关联可靠性条目见 `flow-reliability-remediation.md` F15/F15b/F15c/F15d/F15e。

---

## 与代码的差异（2026-09-28 复核）

2026-09-28 全项目复核将本文与当前代码/迁移/提交逐条对照，下列各处设计已被实现取代、改名或尚未实现。**以代码为准**；正文保留作设计历史，未逐句改写。

- **counter（:41,124,144）**：快路径已把 generic interface counter 解码为 DecodedBatch.CounterRecords 并写入 `sflow_interface_counters`；走 GoFlow2 回落路径时该 datagram 的 counter 会丢失。
- **§7 门禁（:207-217）**：新增 FuzzParseRawFlowInto、FuzzDecodeNetFlowV5Fast、FuzzDecodeSFlowV5Fast 与 counter 解码测试（`4e9dd9bba`）。

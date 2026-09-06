# Flow 地址/分类改为查询时过滤 —— 方案

状态:提议。§4 是带理由的推荐决策,可逐条否;确认后修订 `flow-pipeline-adr.md`。

## 1. 现在怎么做(问题)

- flow-worker 收到每条 flow,用 flowdimension(Go 地址库查找)按源/目的 IP + 事件时间算出 category、业务方向、geo、ISP/ASN、业务、地址组归属,和原始字段一起写进 `flow_records`。
- rollup 按这些**已算好**的字段做分钟/小时聚合。
- 问题:分类在写入时就固定了。地址库/地址组定义改了,历史事实里还是旧分类;要让历史反映新定义,得单独写重新计算的任务(FLOW-06B)。

## 2. 提议改成怎么做(要点)

把分类的"来源"从"写入时算好、存进事实"改成"一份可查的地址库字典,谁要用谁现查":

1. **写入只存原始字段**,不算分类;
2. **地址段/地址组/geo 发布成一份 ClickHouse `IP_TRIE` 字典**:网段 → {国家, 省市, ASN, ISP, prefix 标签, 组ID列表};要按分类聚合或过滤的地方,都用 `dictGet(字典, 属性, IP)` 现查;
3. **常查的聚合预先算好存起来**:rollup 从原始行 `dictGet` 现导分类再聚合,存聚合表;定义改了重跑 rollup 就是新结果。

核心:**分类逻辑集中在"地址库字典"一处**,写入/rollup/查询都现查。定义改了 = 换字典版本 + 重跑受影响的聚合。没有固定在事实里的分类,也不需要单独的重新分类子系统。

## 2.1 本阶段范围(重要)

- **不开放用户自定义地址段**。地址组由**管理员基于已就绪的 geo 编辑规划**——即"geo 地址段分组":把已有的国家/省市/ISP/ASN 等 geo 实体命名成组(例如「华东」=某几个省、「海外运营商A」=某组 ASN)。
- 因此字典内容主要就是**已就绪的 geo 数据**(网段 → 国家/省市/ISP/ASN),地址组是**geo 实体的命名集合**;发布时把"这个网段的 geo 实体落在哪些组里"折进字典(网段 → 组ID列表),查询/rollup 直接 `dictGet` 组ID。
- **不需要**用户 CIDR 上传通道,也**不需要** F11 那套任意网段的 include/exclude 集合运算发布——那些留到以后真开放用户自定义地址段时再做。

## 3. 各部分具体做什么

### 3.1 原始表 `flow_records`
- 去掉写入时算的分类列(category / business / business_direction / geo.* / isp / asn / local_prefix / remote_prefix / address_set),只存原始字段(源/目的 IP、端口、协议、源/目的 ASN、字节/包数、采样率、事件时间、采集器/监听/exporter 身份)。
- worker enrich 只做采样归一 + 写原始,不算分类。(过渡期先不删列,只是不再用。)

### 3.2 地址库字典(分类的唯一来源)
- 字典内容 = **已就绪的 geo 数据**(网段 → 国家/省市/ISP/ASN,来自 PLAT-04D 的 geo hierarchy)+ **管理员的 geo 分组**(每个网段落在哪些管理员定义的组里,折成"网段 → 组ID列表")。编译成一份 CH `IP_TRIE` 字典。
- 本阶段不含用户自定义 CIDR 集合;管理员改组定义 = 重新编译一版字典。
- 字典**按版本发布**(每次改动一个新版本),旧版本保留供账单用历史定义解析(见 3.5)。与 PLAT-04D(geo)、PLAT-04A(发布)收敛。
- flowdimension 现在的"运行时逐 flow 分类"改为"编译产出字典源";写入路径不再调它算分类。

### 3.3 rollup / 预聚合(从原始按版本重算)
- rollup 从"`GROUP BY` 已算好的分类维度"改成"从原始行 `dictGet(字典版本, 维度, src_ip/dst_ip)` 现导维度再 `GROUP BY`",写聚合表 + generation。地址组维度的 `ARRAY JOIN` 从读固定的 `address_set_ids` 改成 `dictGet(字典,'set_ids',src_ip/dst_ip)`。
- 用当前字典版本跑 = 最新分类;定义改了重跑一个更高 generation,读侧取 `max(generation)` —— **重分类 = 重跑 rollup**。
- 复用现有 rollup 的 generation / ReplacingMergeTree / marker / FINAL / top-N(F6),和 `operation_jobs` 调度/租约/重试/checkpoint(PLAT-04F/04G),不新造状态机。

### 3.4 查询
- 按组/国家过滤:`dictGet` 现导 IP 归属过滤,或把组解析成网段过滤原始表。
- 按国家/ASN 排名(breakdown):走 3.3 的预聚合。
- 明细下钻:查原始 + `dictGet` 现导。

### 3.5 账单 as-of
- 查询**默认用当前字典版本**(报表、探索);**账单/审计查询指定历史字典版本**解析(按当月当时的组定义结算)。所以字典必须版本化,查询带一个版本参数。

### 3.6 原始表按网段过滤要能裁剪(性能前提)
- 现排序键 `(tenant, 小时, record_id)`,按 IP 过滤跳不过数据块;网段是范围,**bloom skip index 只帮等值下钻(`src_ip = 某个 IP`),帮不了网段范围过滤**。大范围 ad-hoc 组过滤 = 扫该租户该时间段全部行。
- 要更快得加一份按 `(tenant, src_ip, 时间)` 排序的 `PROJECTION`(额外一份排序副本,有存储/写入代价)——先不加,等实测。
- 快路径靠 3.3 预聚合(不扫原始);原始表扫只兜底 ad-hoc/未定义组 + 有界时间。

## 4. 决策(推荐值,可逐条否)

1. **live vs 账单历史版本**:查询默认 live(当前字典);账单/审计支持指定历史字典版本。→ 字典版本化 + 查询版本参数。
2. **分类走哪里**:统一走 CH `IP_TRIE` 字典 —— 写入不再 Go 分类,rollup 和查询都 `dictGet`。比在 Go、CH 两处各算一遍更一致,且 ad-hoc query-time 组过滤天然支持。字典约百万网段加载进 CH 内存、每副本一份、换版本重载——**已确认可接受**(其他项目验证过),仍列为容量项跟踪。
3. **预聚合范围 / projection**:预聚合覆盖"租户已定义的组 + 国家/ASN breakdown";ad-hoc 回落原始表扫;IP `PROJECTION` 先不加,按 3.6 实测成本再定。
4. **ADR**:这是对 `flow-pipeline-adr.md`「分类在 Kafka 后烘进 base」的修订,同步更新 ADR 和 tasklist,不悄悄改。

## 5. 现有代码要改什么

- worker enrich:去掉 flowdimension 逐 flow 分类,只写原始(3.1)。
- flowdimension:编译逻辑改为产出 CH `IP_TRIE` 字典源 + 版本发布(3.2)。
- `flow_records` schema:去分类列(breaking;过渡先不删)。
- rollup:`GROUP BY` 改成 `dictGet` 现导(3.3)。
- 查询层:支持 `dictGet` 现导的过滤/breakdown + 指定历史字典版本。

## 6. 分步落地(每步独立可验证、不推翻前一步)

1. **发字典**:flowdimension 编译成 CH `IP_TRIE` 字典 + 版本发布,不动写入——只多一份可查产物。gated CH 验 `dictGet` 结果与 flowdimension 一致。
2. **rollup 从原始按版本重算**:rollup 改成从原始 `dictGet` 现导再聚合(用字典版本),读侧不变(`max(generation)`)。gated CH 验:同一批原始 + 字典 → 聚合结果与旧 `GROUP BY` 一致;换字典版本 → 结果按新分类变。**此步之后重分类已经等于重跑 rollup。**
3. **查询按字典过滤/breakdown**:查询支持 `dictGet` 现导的组过滤 + 国家/ASN breakdown;实测原始表 ad-hoc 过滤成本,决定是否加 `PROJECTION`。
4. **账单历史版本**:账单查询指定历史字典版本。
5. **停写、删列**:确认 rollup/查询都不依赖 base 分类列后,worker 停写分类列,迁移删列。

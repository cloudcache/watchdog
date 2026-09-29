# 地址库工作台改造方案（对标 EdgeManager geo 实现）

> **与代码的差异（2026-09-28 复核）**：本文有 3 处已落后于代码或与代码不一致——以代码为准，逐条见文末「与代码的差异（2026-09-28 复核）」。

> 目标：把当前割裂、只读、慢、无批量的地址库，改造成 EdgeManager「地址库工作台」那样的
> 统一、可批量、可组合、可下钻、快分页的形态。本文只写方案，不含实现。

调研来源：`~/Documents/EdgeManager`
（前端 `frontend/src/components/dns/{GeoWorkbenchPage,AddressLibraryPage,LineTreeBrowser,FilterTable,GeoDataPage}.tsx`，
后端 `backend/src/geo/{index,setops,import}.rs`、`backend/src/dns_alias.rs`）。

---

## 0. 用户三条痛点

1. **无 ASN、无「运营商/geo 分组组合」及层级及绑定** —— 现在看不到别名/大区组，不能按组合跨库筛选，没有层级下钻。
2. **VTable 没有可选、全选、没有编辑、没有行内/批量操作** —— 现在的表是只读的。
3. **加载性能太慢、分页数量太少** —— 25/页；EdgeManager 200~5000/页，1.28M 行秒开。

---

## 1. EdgeManager 目标设计（调研结论）

### 1.1 信息架构：单一入口
`GeoWorkbenchPage.tsx`（63 行）是**一个入口 + 6 个 tab**，注释原文：「单一入口 + 一条发布路径，收敛原先割裂的 5 个地址库/线路/发布页」。
6 tab：`基础数据(字典) | 地址库工作台 | 线路/大区 | 版本/发布 | LDNS 库 | 地址段·修正`。

### 1.2 数据三层：base + 修正 → 生效视图
`AddressLibraryPage.tsx:50` 原文：「地址库(基础库+修正 三层合一，分页，v4/v6)。选中段 → 改归属(落为待审核手工覆盖) → 上线生效」。
- `srcLayer: "raw" | "edited"`（`:71`）：raw=基础库、edited=**生效视图(叠加修正)**。
- 编辑一段 = 在该 CIDR 落一条「覆盖」（其它列沿用当前生效值，`:180`），状态「待审核」，发布后生效。

### 1.3 别名 / 大区组（组合 + 层级 + 绑定）
- **别名 = 组合**：`selector(洲×N·国×N·省×N·市×N·运营商×N·ASN×N) + members(显式 CIDR×N) + exclude(排除别名/组×N)`，`enabled` → 可绑定。
  摘要函数 `aliasCoverage`（`LineTreeBrowser.tsx:13`）正是截图里的「ASN×3 · CIDR×1 · 排除组×1」。复杂分区如「海外=除中国大陆」在此表达。
- **层级 = 虚拟树**：`LineTreeBrowser.tsx:31` 原文「层级下钻浏览器…虚拟树，实时组合自后端字典」。
  树本身**不落库**，由 `geo_dict + operators` 实时组合（`lineChildren(parent, ctx)`），逐级 `drill()` 下钻，`查看下级(N)`。
  「启用」把某条线路落成**可绑定的别名**。
- 后端：`geo/index.rs` 里每个地址段带 `alias_mask`（bitmask，`alias_id → bit`），发布快照 `GeoSnap` 预标注，
  于是「按别名组合跨全库筛选」是位运算级别的快查；`geo/setops.rs`（610 行）做 union/intersect/exclude 的集合运算。

### 1.4 工作台的表：勾选/全选/行内编辑/批量 + Excel 筛选
`FilterTable.tsx`（159 行，**用 `@visactor/vtable` —— 和 watchdog 同一个库**）：
- 勾选列 `cellType:"checkbox"` + `onCheckbox`；**全选**（表头遍历所有行，`AddressLibraryPage.tsx:229`）。
- 行内编辑：`editor:"ft-input"`（双击进入、`onStart` 自动全选 → 键入替换）+ `onEdit`。
- 列头 **Excel 式筛选**：`FilterPlugin({ filterModes:["byValue","byCondition"] })`（漏斗，仅当前页）。
- 关键正确性：筛选/排序后**显示行 → 原始下标**映射 `origIdx()`，否则批量操作会命中错行。
- 批量：`applyReassign()`（选中段批量改归属→落覆盖，`:267`）、批量删除（`:293`）。

### 1.5 分页 + 跨库筛选（性能）
- 分页档位 `SIZES = [200, 500, 1000, 2000, 5000]`（`AddressLibraryPage.tsx:51`），服务端分页。
- 两种筛选：① 列头 Excel 筛选（仅当前页）；② 顶部**按别名/大区/运营商组合**跨全库筛选（服务端 `useGeoAliasSegments` → `/geo/aliases/segments`，`:111`）。
- 未选别名 → 全量 base 浏览；选了别名 → 走 alias-segments（selector 或成员 CIDR 命中基础库段）。

---

## 2. Watchdog 现状（映射）

| 维度 | 现状 |
|---|---|
| 字典 | `geo_dict`(kind/code/**parent_id**/name/sort_order，已支持层级)、`isp_operators`(code/name/category/**asns JSON**/flow_isp_id) |
| 线路 | `geo_lines`(parent_id/geo_selector JSON/operator_id/address_set_id) —— 有 selector+绑定，但**无 ASN/CIDR/排除组组合、无虚拟树、无「启用→别名」** |
| 地址段 | `address_base_prefixes`(导入/不可变/按 import) + `address_prefixes`(可编辑/修正) —— **两张表分离，未合成「生效视图」** |
| 集合 | `address_sets`(selector 集合) |
| 发布 | `dimension_snapshots`(版本/发布，已具备生命周期) |
| 前端 | `address-library.tsx` **9 个 tab**（imports/prefixes/sets/tools/batch/publications/geography/operators/lines），割裂 |
| 表组件 | `PagedVTable`（有 服务端 filter/sort/pagination；**无 勾选/全选/行内编辑/批量**） |

---

## 3. 差距分析（对应三痛点）

| 痛点 | EdgeManager | Watchdog | 差距 |
|---|---|---|---|
| ① 组合+层级+绑定 | 别名(selector+members+exclude)、虚拟树下钻、启用→绑定、位掩码快查 | geo_lines 仅 selector+operator 绑定 | 缺组合模型/排除组/虚拟树/跨库组合筛选 |
| ② VTable 勾选/编辑/批量 | FilterTable：checkbox+全选+行内 editor+列头筛选+批量 | PagedVTable 只读 | 缺 checkbox 列、全选、editor、批量 API |
| ③ 慢/分页少 | 200~5000/页、生效视图服务端分页、别名位掩码 | 25/页、导入浏览曾 filesort(已修) | 缺大分页、缺生效视图分页 API、缺组合筛选索引 |

---

## 4. 改造方案（分阶段，每阶段可独立上线）

### P0 · 已完成 / 已修
- 导入浏览覆盖索引 `idx_address_base_browse`（迁移 0027）：41.6s → 0.02s。**已上线。**
- Prefixes 页 `[Library | Imported]` 切换雏形。**已有。**

### P1 · 表组件升级（痛点②③，纯前端 + 少量 API 参数）
- 新增 `SelectableTable`（仿 EdgeManager `FilterTable`，复用同一 `@visactor/vtable`）：
  勾选列 + 全选表头 + 行内 `editor`（双击全选）+ 列头 `FilterPlugin` + `origIdx` 行映射 + 批量回调。
- 分页档位加 `200/500/1000/2000/5000`；`PagedVTable.serverPagination` 已支持，只改档位 + 默认 200。
- 验收：能勾选、全选、双击改一格、选 N 段批量操作；1000/页秒开。

### P2 · 生效视图（三层合一，工作台核心）
- 新 API `GET /api/v1/address-prefixes/effective`：
  以**当前活跃 import 的 base 段**为底，`LEFT JOIN` 可编辑覆盖(`address_prefixes`) → 生效值 + `source(base/修正)` 列；
  支持 v4/v6、`raw|edited` 切换、服务端分页/排序/筛选（复用 0027 同型索引，base+covering）。
- 批量改归属 → 在选中 CIDR 落 `address_prefixes` 覆盖（`source=修正`，待发布）；批量删除同理。
- 验收：一个表看到「基础库 + 我的修正」的合并结果，选中批量改运营商/geo/ASN，落为待发布覆盖。

### P3 · 别名/大区组 + 层级下钻（痛点①）
- 表模型：把 `geo_lines` 扩为 `address_aliases`（或新增列）：`selector_json(regions/operators/asns) + members(CIDR JSON) + exclude_alias_ids(JSON) + enabled`。
- 虚拟层级树 API：`GET /api/v1/geo/lines/tree?parent=<code>` —— 后端用 `geo_dict + isp_operators` **实时组合**出子级（洲→国→省→市 / 运营商），逐级下钻、`childCount`；不落库。
- 组合筛选 API：`GET /address-prefixes/effective?alias=<id>` 或 `?selector=…&members=…&exclude=…` —— 按别名的 selector/members/exclude 跨全库筛选（先按 geo/operator/asn 命中，再叠加 exclude）。
- 前端：「线路/大区」页 = 默认虚拟树（下钻 + 展开看下级/地址段）+「已定义别名」清单（`ASN×N·CIDR×N·排除组×N` 摘要 + 启用开关）+ 高级编辑器(组合 builder)。
- 验收：能像截图那样按「国内/海外/T1·AT&T/…」下钻、启用别名、并在工作台按别名跨库筛选地址段。

### P4 · 基础数据补全（字典单一来源）
- `geo_dict.kind` 扩：`continent/country/province/city` 明确分级 + 新增 `search_engine/cloud_provider/natural_region`（对应截图 tab）。
- 前端「基础数据」页：大洲/国家/省份/城市/运营商/搜索引擎/云厂商/自然区域，各自 CRUD + `sort_order`。

### P5 · IA 收敛（对标单一入口）
- 用一个「地址库工作台」入口收敛现有 9 tab → `基础数据 | 地址库工作台(生效视图) | 线路/大区 | 版本/发布`（+ 保留 imports/publications 作为子面板）。

---

## 5. 优先级建议

- **先做 P1 + P2**：直接解决「只读/无批量/慢/割裂」的主要体感（痛点②③ + 工作台雏形）。
- **再做 P3**：别名/大区组/层级（痛点①，工作量最大，含后端组合筛选 + 位掩码或等价索引方案）。
- **P4/P5** 收尾：字典补全 + IA 收敛。

## 6. 待确认

1. 「生效视图」的 base 取自**当前活跃 import slot**（geo/asn/combined 哪个，或合并）？EdgeManager 是单一基础库；watchdog 是多 slot。
2. 别名的「排除组」是否需要递归（别名引用别名）？EdgeManager 支持 `exclude_aliases`。
3. 组合筛选跨库性能：是走**发布快照位掩码**（EdgeManager 方案，需发布后生效）还是**实时 SQL 组合**（即时但需索引）？影响 P3 架构。

---

## 与代码的差异（2026-09-28 复核）

2026-09-28 全项目复核将本文与当前代码/迁移/提交逐条对照，下列各处设计已被实现取代、改名或尚未实现。**以代码为准**；正文保留作设计历史，未逐句改写。

- **状态（:4）**：P2–P5 均已实现——生效视图与批量改归属（`4376c326d`）、工作台 UI 与行内编辑（`597760f9b`、`2b924d135`）、虚拟地理树与运营商树（`d594e1b01`、`68cc38216`）、大区组组合/排除（`cdd2379ac`、`004ade543`、`261e26f35`）、基础数据字典（`45c9d12d7`）、IA 收敛（`e32ba37ac`）；仅 P1 的大分页档未做（`paged-vtable.tsx:243` 仍为 25/50/100）。
- **§2 现状 / §6 Q1（:57-65,122）**：§2 为改造前快照；PagedVTable 已有 checkbox 列；`GET /address-prefixes/effective` 默认以 `slot=combined` 为底（Q1 已决：默认 combined、可切换 slot）；IA 已收敛为 4 组。
- **P3 命名（:99-101）**：实际是 `geo_lines` 增加 `members` 与 `exclude_line_ids` 两列（`0030`）；路由为 `GET /api/v1/geo/tree?parent=`；生效视图过滤参数为 `?line=<id>`。

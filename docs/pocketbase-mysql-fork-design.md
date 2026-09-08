# PocketBase v0.36.8 → MySQL Fork — 设计 (Design)

> 状态：设计草案（research/design，无代码）。基于对 PB v0.36.8 源码（`core/`、`tools/search/`、`migrations/`、`pocketbase/dbx`）与 watchdog 实际用法的三路审计。配套：`docs/self-host-consolidation-audit.md`。

## 0. 目标与范围

**把 vendored 的 PocketBase v0.36.8 从 SQLite 改成 MySQL 后端**：PB 源码进仓库（不再是 go.mod 依赖），改其 DB 层，使 PB 的 collections/auth/realtime/hooks 全部跑在 MySQL 上。

**这条路是什么、不是什么**（先讲清楚，避免与"去 PB"混淆）：
- 它 **不是"去 PB"**——它把 PB *吸收进仓库并保留*（连同 auth/collections/realtime/hooks）。
- 它 **是"存储收敛的 PB 部分"**：把 PB 的 SQLite 数据并入 MySQL，四存储（MySQL+CH+VM+PB-SQLite）降为三（MySQL+CH+VM）；`auth` 继续由 PB 提供，**无需自建原生认证**。
- **VM→CH、去多租户是独立工作**（见 self-host 审计），本设计不涉及。
- 因为你现在**拥有代码**，未来若仍要"去 PB 框架"，可逐块剥离——本 fork 是那条路的垫脚石，而非终点。

## 1. 可行性结论：可行，且**有界**——不是重写 PB

四个结构性"礼物"把风险砍掉约一半：

1. **dbx 自带 MySQL builder**（`pocketbase/dbx` 的 `builder_mysql.go`，`BuilderFuncMap["mysql"]`）。`Insert/Update/Delete/CreateTable/AddColumn/DropColumn/Rename*/CreateIndex/Upsert` 等 **verb 一旦以 driver `"mysql"` 打开就自动适配**（反引号引用 + `?` 占位），PB 的 `{{table}}`/`[[col]]` token 不用动。
2. **读路径全是字符串**：每行按 `dbx.NullStringMap`（列全当 nullable string）读出，再由各字段 `PrepareValue` 在 Go 里还原类型（`core/record_model.go:482-516`）。**MySQL 严格类型在读侧几乎不是问题。**
3. **watchdog 只用等值过滤**（见 §3）：PB 的 `json_each`/高级 filter-AST 对 watchdog 的每条查询路径**都是死代码**，可 stub。
4. **PB 的主键在 Go 里生成**（`GenerateDefaultRandomId`，`core/db.go:56`）：**全程无 AUTOINCREMENT / `LastInsertId` / WITHOUT ROWID / 虚表 / FTS5** 依赖。

真正要写的是 PB **手写的原始 SQL** 与 **硬编码列类型串**——一份具体、有限的清单（§4）。

## 2. watchdog 实际用法（决定"能 stub 多少"）

- 前端 `pb` client 拦截 `pb.send`：`/api/v1/*` 全部改走 MySQL 自研 REST（`internal/site/src/lib/api.ts:15-23`）。**只有 `pb.collection(...)` 与 `/api/watchdog/*` 才碰 PB。**
- **每条应用查询（前后端）都是等值**：`system = {:x}`，最多带 `&&`/`AND`/`OR`/括号/`null`/`!= ''`。裸 SQL 是标准 SQL（GROUP BY/HAVING/子查询/NOT IN），无 PB filter-AST。
- **无** `ExpandRecord`（后端）、多值操作符、back-relation `_via_`、`@collection`、view collection。前端只有 1 处单关系 `expand: "system"`。
- **schema 全 scalar，唯一例外**：`systems.users`（→users，`maxSelect` 无上限，`snapshot:694-700`）——**全库唯一的多值字段**，也是**唯一能触发 `json_each` 的东西**，且仅经由行级 auth 规则 `system.users.id ?= @request.auth.id`（`collections.go:58-69`），并在 `SHARE_ALL_SYSTEMS=true` 时**整条被旁路**。
- **auth 是真正 load-bearing 的耦合**：`/api/v1` MySQL API 靠 PB 发/验 token——`FindAuthRecordByToken(token, TokenTypeAuth)` → `ExternalIdentity{pocketbase, record.Id}`（`internal/hub/platform_backend.go:54-71`）。fork 保留 PB auth = **这层桥不动**。
- 14 collections，除 `users`(auth) 全 `base`，**无 view collection**（无 view SQL 要移植）。

## 3. 端口清单（Port Surface）

| # | 区域 | 具体 | 处置 | 量级 |
|---|---|---|---|---|
| A | **连接/连接池** | 重写 connect 函数：SQLite 7 个 `_pragma` DSN（`core/db_connect.go:10-22`）→ MySQL DSN（`charset=utf8mb4&parseTime=false&loc&sql_mode`）。用 `no_default_driver` tag（`db_connect_nodefaultdriver.go`）注入，丢掉 `modernc.org/sqlite` import。 | **改** | 小 |
| A′ | **双池** | `concurrentDB`/`nonconcurrentDB`（`core/base.go:1174-1259`），写池 `SetMaxOpenConns(1)` 是 SQLite 单写者约束。InnoDB 行锁不需要——给写池正常池大小（否则嵌套 `RunInTransaction` 会自饿死锁），或令两池指同一 pool（`base.go:491/540` 已特判相等）。抽象保留、只改池大小。 | **改(小)** | 小 |
| B | **列类型 `ColumnType()`** | 14 个 `core/field_*.go`。核心：**id 主键** `TEXT PRIMARY KEY DEFAULT ('r'\|\|lower(hex(randomblob(7))))`（`field_text.go:152`）→ `VARCHAR(n) PRIMARY KEY`（去默认，id 本就 Go 生成）；`NUMERIC DEFAULT 0`（`field_number.go:120`）→ `DOUBLE`（否则 `DECIMAL(10,0)` 截断浮点）；TEXT/JSON **字面 DEFAULT** MySQL 不允许 → `VARCHAR` 或 `DEFAULT (JSON_ARRAY())` 表达式默认。 | **改** | 中 |
| C | **系统迁移原始 DDL** | 7 个 `migrations/*.go`。`_collections`/`_params`（`1640988000_init.go:37-115`）、`_logs` 的 `strftime` 函数索引（`aux_init.go:21`）是 SQLite 风味；`||` 串接、双引号字符串字面量也要处理。`_migrations` 表已是 `VARCHAR`（`migrations_runner.go:246`）。`_superusers/_otps/_mfas/_externalAuths/_authOrigins/users` 走 Collection 抽象 → 修好 B 即自动跟随。大 `v0.23_migrate.go` 是元数据变换，0 CREATE TABLE，低风险。 | **改** | 小-中 |
| D | **schema 自省** | `core/db_table.go` 的 `PRAGMA_TABLE_INFO`/`sqlite_master`/`sqlite_schema`（`:14,37,63,114`）+ `collection_validate.go:562` → `information_schema.COLUMNS/STATISTICS`。须保持 `TableInfoRow`（`cid/name/type/notnull/dflt_value/pk`）形状不变，下游依赖它。 | **改** | 中 |
| E | **单值↔多值转换** | `collection_record_table_sync.go:155-298` 的 `json_valid/json_type=='array'/json_extract($[#-1])`。**但 watchdog 唯一多值字段 `systems.users` 永不在单/多间转换 → 此路径实为死代码，可 stub**（未来若加多值字段再补）。 | **stub** | 小 |
| F | **filter 编译器** | `tools/search/filter.go:328-410` 的 **SQLite `IS`/`IS NOT` 空安全惯用法**——即使 `field != 'x'` 也编成 `[[field]] IS NOT {:p}`，MySQL 语法错误 → 映射为 `<=>`/`<>`+null。`apis/record_crud.go:84` 的 `CountCol("_rowid_")` → `id`（每次 list 都跑）。`@random`→`RAND()`、`@rowid` sort（watchdog 不用，可不管）。 | **改** | 小 |
| G | **`systems.users` 的 json_each** | 全库唯一 json_each 触发点。**单租户 self-host 推荐直接 `SHARE_ALL_SYSTEMS=true`**（`collections.go:64-67`），规则塌缩为 `@request.auth.id != ""`，**json_each 整条旁路、无需移植**。或按固定 JOIN（`systems_users` 表）复刻这一个成员检查。 | **旁路/stub** | 小 |
| H | **备份** | `core/base_backup.go` 是"DB=文件"模型（`wal_checkpoint(TRUNCATE)` + zip `pb_data` + 文件替换 + 重启）。MySQL 无此概念 → **self-host 直接禁用 PB 备份 UI/端点，用 MySQL 自身备份**（`mysqldump`）；或完整重写。 | **stub/改** | 中(禁用=小) |
| I | **VACUUM / wal_checkpoint / PRAGMA optimize** | `db_table.go:123`（`VACUUM`）、`base.go:1359-1374` cron、`table_sync.go:147`（`PRAGMA optimize`）→ no-op 或 `OPTIMIZE TABLE`。 | **stub** | 小 |
| J | **锁重试** | `core/db_retry.go` 按文本 `"database is locked"` 匹配 → MySQL 永不命中（无害死代码）；若要保留语义，加 `"Deadlock"`/`"Lock wait timeout"` 子串。无 `sqlite3` 错误码耦合。 | **stub/改(小)** | 小 |

**无需移植（死代码/可 stub）**：`json_each` 全部站点、multi-match 子查询、back-relation、`@collection`/`@request` join（除简单宏）、nested expand、view collection、serving 路径里的 `strftime`。`json_valid` "×111"、`strftime` "×12" 经核实多为测试/DDL/宏内部，非 serving 路径。

## 4. Vendoring 机制

- 复制 `pocketbase@v0.36.8` 的 `core/`、`apis/`、`tools/`、`migrations/`、`plugins/…`（按实际 import 面）进仓库，如 `internal/pb/`；**保留 `pocketbase/dbx` 作为 go.mod 依赖**（它多方言、正是我们要用的 MySQL builder），只 fork PB 自身。
- 从 go.mod 移除 `github.com/pocketbase/pocketbase`；把 `internal/hub/*`、`internal/cmd/hub` 的 import 重指向 vendored 路径（机械改动，量大但直接）。
- 以 `-tags no_default_driver` 构建，注入自研 MySQL `DBConnect`。
- **代价（你已接受）**：vendored fork **无法再拉 PB 上游**（安全/功能修复要手工回植）——永久维护税。realtime 是应用层（保存即广播，`OnRecord*` 驱动），**与 DB 无关，DB 换后照常工作**。

## 5. 建议顺序

1. **Vendored spike（只读验证）**：把 PB 拷进仓库、`no_default_driver` 构建通过、注入一个连 MySQL 的 `DBConnect`，跑起来看**第一个报错**——确认 A/B/D 就是真实拦路者。
2. **A 连接/池** + **B 列类型**（先让 CREATE TABLE 在 MySQL 成功，重点啃 id-PK 的 charset/collation/长度）。
3. **C 系统迁移** + **D 自省**（让 PB 冷启动能建出 `_collections`/`users`/`_superusers` 等系统表并通过自省）。
4. **F filter `IS/IS NOT` + `_rowid_`**（让 `pb.collection` 的等值查询能跑）。
5. **G 置 `SHARE_ALL_SYSTEMS=true`** 旁路 json_each；**E/H/I/J** 按表 stub。
6. **打通 auth**：`FindAuthRecordByToken` 走 MySQL 的 `users`/token 表；验证 `/api/v1` 桥不变。
7. **MySQL 目标测试**：PB 自带测试是 SQLite，针对移植路径补 MySQL 测试。

## 6. 工期/风险

- **最难三处**：(1) **id 主键**——`TEXT PK`→`VARCHAR(n) PK` 波及每表 + 关系列/索引的 utf8mb4 长度/collation 一致性；(2) **备份子系统**（禁用则小，重写则中）；(3) **自省层**保形改写。其余是机械活。
- **潜伏缺口**：E（单/多转换）现 stub——将来若新增多值字段会踩空，须登记。
- **realtime/hooks/auth**：DB 换不影响（应用层），是这条路相对"去 PB+自建认证"的最大省力点。
- 粗估：**有界，约 1–3 周聚焦工作**，主要在 DDL/类型映射 + 自省 + id-PK 打磨 + 测试；**远小于"重写 PB"，与"自建原生认证 + 逐集合搬迁"相当或更省**（且保住 auth）。

## 7. 与 self-host 审计的关系

- 本 fork = 该审计里"PB 数据 → MySQL"那一格的**具体实现路线**，但选择了**保留 PB（forked）**而非**去 PB + 自建认证**。二者互斥：**先定走哪条**。
- 走 fork：省掉自建认证 + 逐集合搬迁；换来"拥有并维护一个 PB/MySQL fork"。VM→CH、去多租户 PIN 仍照审计单独推进。
- 不冲突项：`SHARE_ALL_SYSTEMS=true`（单租户）顺带旁路了 PB 唯一的多值/json_each 复杂度，与"去多租户"同向。

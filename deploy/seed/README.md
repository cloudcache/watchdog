# Address library seed (EdgeManager base)

A ready-to-use base address library exported from the EdgeManager production geo
data, for bootstrapping a fresh watchdog install without re-importing.

`address-library-em.sql.gz` contains data-only `INSERT`s for:

| table | rows | notes |
|---|---|---|
| `isp_operators` | 17 | operator dictionary (name/category/ASNs); Chinese names in utf8mb4 |
| `geo_dict` | 606 | continent→country→province→city hierarchy (parent_id) |
| `address_imports` | 1 | synthetic import `edgemanager-geo-base` (`created_by` NULL) |
| `address_import_slots` | 1 | `combined` slot, pre-activated → this import |
| `address_base_prefixes` | 2,657,403 | base CIDRs (v4 right-aligned 16-byte, v6 native), ~1.98M with ASN |

The dump header sets `SET NAMES utf8mb4` and `FOREIGN_KEY_CHECKS=0`, so table
order and the self-referential `geo_dict.parent_id` load cleanly. `created_by` /
`activated_by` are NULL, so there is **no `users` FK dependency** — it loads into
a fresh install before any user exists.

## Not tracked in git

The `.sql.gz` is ~38 MB and is gitignored (`deploy/seed/*.sql.gz`) — the repo
otherwise has no blobs over ~400 KB and no git-lfs. Keep the file alongside this
README on the deploy host, or regenerate it (below). To version it, set up
git-lfs or commit deliberately.

## Load into a fresh install

Apply the schema first (server boot runs `deploy/schema/mysql/*.sql`), then:

```bash
deploy/seed/load-address-library.sh watchdog        # MYSQL_HOST/MYSQL_USER/MYSQL_PWD env optional
```

or manually:

```bash
gzip -dc deploy/seed/address-library-em.sql.gz \
  | mysql --default-character-set=utf8mb4 -h127.0.0.1 -uroot watchdog
```

Load only into **empty** address tables (plain `INSERT`s; loading twice fails on
duplicate primary keys).

## Regenerate from a populated DB

```bash
mysqldump -h127.0.0.1 -uroot --no-create-info --hex-blob --skip-triggers \
  --single-transaction --no-tablespaces --complete-insert \
  --set-gtid-purged=OFF --default-character-set=utf8mb4 \
  watchdog isp_operators geo_dict address_imports address_import_slots address_base_prefixes \
  | gzip -9 > deploy/seed/address-library-em.sql.gz
```

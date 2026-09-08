# PocketBase deletion-first execution record

> Status: destructive pass completed on 2026-09-08. This record describes the exact objects removed before the replacement MySQL authentication/server work starts. No intermediate build or regression test was run, by explicit instruction.

## Removed runtime and source

- Stopped the PocketBase-backed hub process that was listening on `127.0.0.1:8091`; the port had no listener after shutdown.
- Deleted the complete old PocketBase server tree under `internal/hub/`.
- Deleted the PocketBase-only packages `internal/alerts/`, `internal/records/`, `internal/users/`, `internal/migrations/` and `internal/tests/`.
- Deleted the PocketBase main program `internal/cmd/hub/` and the PB projection utility `cmd/watchdog-identity-link/`.
- Deleted the old external-identity/PocketBase projection slice from `internal/watchdog`: identity adapter, authority/admin repository/API and their coupled tests.
- Deleted PB-only MySQL migrations `011_identity_projection.sql` and `029_drop_user_password_hash.sql`.
- Removed `github.com/pocketbase/pocketbase` and `github.com/pocketbase/dbx` from the Go module. The operation schedule parser now uses `github.com/robfig/cron/v3`, so the remaining platform package has zero PocketBase Go imports.
- Removed the `pocketbase` npm package and lockfile entry. Existing UI components were not restyled or deleted; their API/auth imports are intentionally left as the next KISS-01B/C replacement boundary.

Total deleted in the old PocketBase Go trees before the additional identity slice: 75 files.

## Removed PocketBase data

The running hub was stopped before deleting these exact SQLite files:

- `watchdog_data/data.db`
- `watchdog_data/auxiliary.db`
- `internal/cmd/hub/watchdog_data/data.db`
- `internal/cmd/hub/watchdog_data/auxiliary.db`

Their empty data/main directories were then removed. No wildcard path was used.

## Dropped MySQL debug databases

Only `watchdog_dev` had active MySQL connections and is the database referenced by `config/watchdog.dev.yaml`; it was preserved. The following exact, unreferenced test/debug databases had no active connection and were dropped:

- `watchdog_f2_check`
- `watchdog_mig037`
- `watchdog_mig037b`
- `watchdog_mig039`
- `watchdog_plat04a_check`
- `watchdog_plat04c_check`
- `watchdog_plat04c_init_check`
- `watchdog_plat04c_repo_check`
- `watchdog_plat04c_taxonomy_check`
- `watchdog_platform_045_check`
- `watchdog_platform_046_check`
- `watchdog_platform_050_check`

Post-delete verification returned only `watchdog_dev` for `watchdog_%`. The unrelated `cloudcms`, `cloudplatform`, `gopress`, `quickstack`, `report` and MySQL system databases were not changed. No ClickHouse database was a PocketBase persistence target, so none was dropped in this pass.

## Intentional breakpoints for the next slice

This deletion pass intentionally did not preserve an intermediate runnable hub:

- KISS-01B must add the minimal MySQL users/sessions/RBAC schema and pure Go HTTP entry point.
- KISS-01C must replace `internal/site/src/lib/api.ts` and the remaining PocketBase type/auth/collection calls with the native API client while preserving the existing UI.
- KISS-01D must decide each former PB system/SMART/container/fingerprint/alert call as a typed MySQL/ClickHouse API or delete it if it is outside the target product.
- Legacy comments, DTO fields and tests that still describe `auth_provider=pocketbase` are not a runnable PB dependency; they are deletion/replacement inputs for KISS-01B/D and must not survive the final static gate.

No build, unit test, integration test or regression test was executed during this destructive pass.

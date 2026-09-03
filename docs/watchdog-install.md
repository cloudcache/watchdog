# Watchdog Install

Watchdog backend processes share one YAML config file. Copy `config/watchdog.example.yaml` to `config/watchdog.yaml`, then edit the MySQL DSN and VictoriaMetrics URL.

```bash
cp config/watchdog.example.yaml config/watchdog.yaml
go run ./cmd/watchdog-install --config config/watchdog.yaml --init-sql install/init.sql --lock .watchdog.lock
go run ./cmd/watchdog-dev-server --config config/watchdog.yaml
# Production Hub serves PocketBase auth, the SPA, and /api/v1 on one origin.
go run ./internal/cmd/hub serve --watchdog-config config/watchdog.yaml
# Independent SNMP discovery/polling data plane (run as a supervised service).
go run ./cmd/watchdog-snmp-collector --config config/watchdog.yaml --loop
```

The Hub intentionally serves the control plane only. Saving an SNMP device
queues discovery; at least one `watchdog-snmp-collector --loop` process must be
running for that tenant. The worker claims queued discovery jobs immediately,
then continues discovery and polling at the configured intervals.

The installer and embedded migration runner are intentionally idempotent:

- Every run reconciles `install/init.sql`, then applies every pending embedded migration under a MySQL advisory lock and records its version and SHA-256 in `watchdog_schema_migrations`.
- If `.watchdog.lock` does not exist, it writes the database installation marker and creates `.watchdog.lock` after schema success.
- If `.watchdog.lock` already exists, schema reconciliation still runs before refreshing the marker; the lock file never suppresses migrations.
- Applied migration checksum drift, an unknown newer migration, a failed statement, or an unavailable MySQL database stops startup/readiness. Migration files are immutable after release; corrections use a new forward migration.

The same config flag is accepted by:

- `cmd/watchdog-dev-server`
- `cmd/watchdog-export-worker`
- `cmd/watchdog-snmp-collector`
- `cmd/watchdog-sflow-collector`
- `cmd/watchdog-aggregate-rollup`
- `cmd/watchdog-system-agent`
- `cmd/watchdog-snmp-agent`
- `cmd/watchdog-install`
- `internal/cmd/hub serve` (uses `--watchdog-config` to avoid colliding with PocketBase flags)

## Configuration contract

The precedence is deterministic:

```text
explicit command flag > environment variable > YAML > built-in default
```

Only command-specific flags participate. For example, `watchdog-snmp-collector --limit` overrides `snmp_collector.poll_limit`, while processes without that flag use the environment/YAML/default value.

The YAML decoder rejects unknown fields, duplicate/malformed keys, and multiple YAML documents. Startup also rejects invalid URLs, listen addresses, durations, pool sizes, unsupported export metrics, and inconsistent MySQL pool settings. Environment values are not silently ignored: an invalid integer or duration stops startup and names the offending variable. `mysql.max_idle_conns: 0` is valid and disables idle connections; an explicitly empty `WATCHDOG_SNMP_MIB_DIRS` clears the YAML list.

HTTP base URLs are normalized by trimming surrounding whitespace and trailing slashes. IDs, listen addresses, export paths, metric names, and MIB paths are normalized; MIB paths are cleaned and de-duplicated. DSNs and tokens are not rewritten. Component identities and credentials are validated by the component after flags have been resolved, so an unused optional agent section does not make unrelated workers require that agent's token.

## Environment overrides

| Section | Variables |
|---|---|
| Config file | `WATCHDOG_CONFIG` |
| MySQL | `WATCHDOG_MYSQL_DSN`, `WATCHDOG_MYSQL_MAX_OPEN_CONNS`, `WATCHDOG_MYSQL_MAX_IDLE_CONNS`, `WATCHDOG_MYSQL_CONN_MAX_LIFETIME` |
| VictoriaMetrics | `WATCHDOG_VICTORIAMETRICS_URL` |
| Export worker | `WATCHDOG_EXPORT_DIR`, `WATCHDOG_EXPORT_WORKER_INTERVAL`, `WATCHDOG_EXPORT_WORKER_BATCH`, `WATCHDOG_EXPORT_METRIC` |
| SNMP collector / discovery | `WATCHDOG_SNMP_COLLECTOR_TENANT_ID`, `WATCHDOG_SNMP_COLLECTOR_INTERVAL`, `WATCHDOG_SNMP_COLLECTOR_POLL_LIMIT`, `WATCHDOG_SNMP_DISCOVERY_INTERVAL`, `WATCHDOG_SNMP_DISCOVERY_BATCH` |
| SNMP MIBs | `WATCHDOG_SNMP_MIB_DIRS`, `WATCHDOG_SNMP_MIBS` |
| sFlow collector | `WATCHDOG_SFLOW_LISTEN`, `WATCHDOG_SFLOW_TENANT_ID`, `WATCHDOG_SFLOW_AGG_INTERVAL`, `WATCHDOG_SFLOW_PREFIX_SYNC_INTERVAL` |
| Aggregate graph rollup | `WATCHDOG_AGGREGATE_GRAPH_ROLLUP_INTERVAL` |
| SNMP trap agent | `WATCHDOG_SNMP_TRAP_API_URL`, `WATCHDOG_SNMP_TRAP_TOKEN`, `WATCHDOG_SNMP_TRAP_LISTEN` |
| System agent | `WATCHDOG_AGENT_HUB_URL`, `WATCHDOG_AGENT_ID`, `WATCHDOG_AGENT_TOKEN`, `WATCHDOG_AGENT_INTERVAL` |

`WATCHDOG_VICTORIALOGS_URL` and `WATCHDOG_SFLOW_VLOGS_URL` were removed with the VictoriaLogs backend. Keeping either variable in a process environment is a startup error so obsolete deployment configuration cannot be silently accepted.

## Link PocketBase authentication to the management plane

Before a PocketBase user can call `/api/v1`, link its PocketBase record ID to an active MySQL user projection. The operation is explicit and tenant-scoped; unknown users, disabled users, and duplicate external identities fail closed.

```bash
go run ./cmd/watchdog-identity-link \
  --config config/watchdog.yaml \
  --tenant tenant_dev \
  --user user_dev \
  --subject POCKETBASE_USER_RECORD_ID
```

The production frontend sends the PocketBase auth token on same-origin `/api/v1` calls. A user with more than one active tenant projection must also select a tenant; the client persists that selection per PocketBase subject and sends it as `X-Watchdog-Tenant-ID`. The server validates membership on every request and never accepts tenant identity from the header alone.

Liveness is exposed at `/api/v1/health/live`. Readiness is exposed at `/api/v1/health/ready` and returns 503 until MySQL responds and every embedded migration version/checksum matches the ledger.

Keep DSNs and tokens in a process secret, root-readable environment file, or secret manager instead of committing production values to YAML. The example values are placeholders.

Windows agents do not download or embed `smartctl.exe` from a product-owned domain. Install the official smartmontools package or place `smartctl.exe` on `PATH` before enabling S.M.A.R.T. disk-health collection.

## Agent registry values

Agent registry writes normalize surrounding whitespace and lowercase `agent_type`, `mode`, and `status`. Valid current values are:

- type: `snmp`, `system`;
- mode: `push`, `pull`; a system agent must use `push`;
- status: `pending`, `up`, `down`, `error`, `disabled`.

The API rejects unknown JSON fields and path/body ID mismatches, and the repository repeats domain validation before writing MySQL. A disabled agent cannot fetch a plan, heartbeat, report a run, or push system samples. Run reports accept only `success` or `failure`.

`PATCH /api/v1/agent-registry/{id}` is a true partial update: omitted fields retain their stored values, an explicitly empty endpoint clears it, and an empty token preserves the current token. A supplied body ID must match the path.

## Existing runtime compatibility configuration

The pre-existing hub `config.yml` system sync remains separate from the Watchdog YAML. It now uses the same fail-fast principles: unknown fields and multiple documents are rejected; system name/host/user emails are normalized; default port `45876` is applied before duplicate detection; missing users, missing name/host, and duplicate `(name, host, port)` entries stop the sync before writes. Existing-system matching uses that tuple directly instead of ambiguous string concatenation.

For the pre-existing `watchdog-agent` command, explicit `--url` and `--token` flags override both `WATCHDOG_AGENT_*` and unprefixed environment variables. Other unprefixed variables retain their existing `WATCHDOG_AGENT_<KEY> > <KEY> > default` precedence until they are migrated into the signed collector-plan model. The canonical product namespace, API prefix, binary names, service names, and data directories use `watchdog`; pre-rename product aliases are intentionally unsupported.

# Watchdog Install

Watchdog backend processes share one YAML config file. Copy `config/watchdog.example.yaml` to `config/watchdog.yaml`, then edit the MySQL DSN and VictoriaMetrics URL.

```bash
cp config/watchdog.example.yaml config/watchdog.yaml
go run ./cmd/watchdog-install --config config/watchdog.yaml --init-sql install/init.sql --lock .watchdog.lock
go run ./cmd/watchdog-dev-server --config config/watchdog.yaml
```

The installer is intentionally idempotent:

- If `.watchdog.lock` does not exist, it executes `install/init.sql`, writes the database installation marker, then creates `.watchdog.lock`.
- If `.watchdog.lock` already exists, it only refreshes the `watchdog_installation` marker in MySQL and exits.

The same config flag is accepted by:

- `cmd/watchdog-dev-server`
- `cmd/watchdog-export-worker`
- `cmd/watchdog-snmp-collector`
- `cmd/watchdog-sflow-collector`
- `cmd/watchdog-aggregate-rollup`
- `cmd/watchdog-system-agent`
- `cmd/watchdog-snmp-agent`
- `cmd/watchdog-install`

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
| VictoriaMetrics / VictoriaLogs | `WATCHDOG_VICTORIAMETRICS_URL`, `WATCHDOG_VICTORIALOGS_URL` |
| Export worker | `WATCHDOG_EXPORT_DIR`, `WATCHDOG_EXPORT_WORKER_INTERVAL`, `WATCHDOG_EXPORT_WORKER_BATCH`, `WATCHDOG_EXPORT_METRIC` |
| SNMP collector / discovery | `WATCHDOG_SNMP_COLLECTOR_TENANT_ID`, `WATCHDOG_SNMP_COLLECTOR_INTERVAL`, `WATCHDOG_SNMP_COLLECTOR_POLL_LIMIT`, `WATCHDOG_SNMP_DISCOVERY_INTERVAL`, `WATCHDOG_SNMP_DISCOVERY_BATCH` |
| SNMP MIBs | `WATCHDOG_SNMP_MIB_DIRS`, `WATCHDOG_SNMP_MIBS` |
| sFlow collector | `WATCHDOG_SFLOW_LISTEN`, `WATCHDOG_SFLOW_TENANT_ID`, `WATCHDOG_SFLOW_VLOGS_URL`, `WATCHDOG_SFLOW_AGG_INTERVAL`, `WATCHDOG_SFLOW_PREFIX_SYNC_INTERVAL` |
| Aggregate graph rollup | `WATCHDOG_AGGREGATE_GRAPH_ROLLUP_INTERVAL` |
| SNMP trap agent | `WATCHDOG_SNMP_TRAP_API_URL`, `WATCHDOG_SNMP_TRAP_TOKEN`, `WATCHDOG_SNMP_TRAP_LISTEN` |
| System agent | `WATCHDOG_AGENT_HUB_URL`, `WATCHDOG_AGENT_ID`, `WATCHDOG_AGENT_TOKEN`, `WATCHDOG_AGENT_INTERVAL` |

Keep DSNs and tokens in a process secret, root-readable environment file, or secret manager instead of committing production values to YAML. The example values are placeholders.

## Agent registry values

Agent registry writes normalize surrounding whitespace and lowercase `agent_type`, `mode`, and `status`. Valid current values are:

- type: `snmp`, `system`;
- mode: `push`, `pull`; a system agent must use `push`;
- status: `pending`, `up`, `down`, `error`, `disabled`.

The API rejects unknown JSON fields and path/body ID mismatches, and the repository repeats domain validation before writing MySQL. A disabled agent cannot fetch a plan, heartbeat, report a run, or push system samples. Run reports accept only `success` or `failure`.

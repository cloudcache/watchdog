# Watchdog Local Dev Startup

This project currently has several Watchdog helper commands under `cmd/`.
For local UI/API testing, use these three entry points:

1. Initialize MySQL:

```bash
make watchdog-dev-db
```

This creates the database, runs `cmd/watchdog-install` with
`config/watchdog.dev.yaml`, writes `.watchdog-dev.lock`, and applies the local
seed data.

2. Start the Watchdog dev API:

```bash
make watchdog-dev-api
```

The API listens on:

```text
http://127.0.0.1:8091
```

3. In another terminal, start the web UI:

```bash
make watchdog-dev-web
```

The UI listens on:

```text
http://127.0.0.1:5173
```

The Vite dev server proxies `/api/v1` to `http://127.0.0.1:8091`.

4. When testing SNMP, run the independent collector/discovery worker in another terminal:

```bash
go run ./cmd/watchdog-snmp-collector \
  --config config/watchdog.dev.yaml \
  --tenant tenant_dev \
  --interval 1m \
  --loop
```

The loop claims queued discovery jobs immediately, loads each device's SNMP
profile and per-device override, imports collection recipes, and polls due
recipes into raw samples. The Hub is the control plane and does not perform
network polling in its HTTP process. For a one-off device diagnostic, omit
`--loop` and pass `--device "$WATCHDOG_NETWORK_DEVICE_ID"`.
When `--tenant`, `--interval`, or `--limit` is omitted, the command uses
`snmp_collector.tenant_id`, `interval`, and `poll_limit` from the shared config.
Set `--discover=false` to run due polling without requiring `--device`.

## MySQL Defaults

The scripts default to:

```text
host: 127.0.0.1
port: 3306
user: root
password: empty
database: watchdog_dev
```

Override them when needed:

```bash
WATCHDOG_MYSQL_USER=root \
WATCHDOG_MYSQL_PASSWORD='your-password' \
WATCHDOG_MYSQL_DB=watchdog_dev \
make watchdog-dev-db
```

Use the same variables for `make watchdog-dev-api`, or set the full DSN:

```bash
export WATCHDOG_MYSQL_DSN='root:your-password@tcp(127.0.0.1:3306)/watchdog_dev?parseTime=true&multiStatements=true'
make watchdog-dev-api
```

The API script passes `--config config/watchdog.dev.yaml`. Environment
variables override the YAML values, so changing `WATCHDOG_MYSQL_DSN` or
`WATCHDOG_VICTORIAMETRICS_URL` is enough for local testing.
Invalid or unknown configuration now fails startup instead of falling back
silently; the complete override list and precedence are in
[`watchdog-install.md`](watchdog-install.md).

## Commands

- `cmd/watchdog-dev-server`: local dev API with admin dev auth.
- `cmd/watchdog-install`: fresh MySQL installer using `install/init.sql` and a local lock file.
- `cmd/watchdog-export-worker`: async CSV export worker.
- `cmd/watchdog-snmp-collector`: SNMP discovery, recipe import, and raw sample polling.

## Flow Kafka and ClickHouse

The Flow development data plane uses the pinned single-node Kafka KRaft and
ClickHouse LTS images in `deploy/compose.flow-dev.yml`. It binds Kafka and both
ClickHouse endpoints to `127.0.0.1`; this is a functional integration setup,
not the multi-node capacity or HA environment.

Start the services and explicitly migrate ClickHouse:

```bash
export WATCHDOG_CLICKHOUSE_PASSWORD='watchdog-local'
make flow-dev-up

flow_secret_file=$(mktemp)
chmod 600 "$flow_secret_file"
printf '%s' "$WATCHDOG_CLICKHOUSE_PASSWORD" > "$flow_secret_file"
go run ./cmd/watchdog-flow-migrate \
  --command apply \
  --clickhouse-password-file "$flow_secret_file"
rm -f "$flow_secret_file"
```

The migration set is embedded in `watchdog-flow-migrate`; the worker and hub
never modify ClickHouse schema during startup. Use `--command inspect` for a
read-only state/lock report. A failed or interrupted migration is dirty and
requires `--command resume`. An orphaned lock can be removed only with
`--command unlock --lock-owner <exact-owner-token>` after the operator has
confirmed that the original process is no longer running.

Kafka auto-topic creation is disabled. The one-shot `kafka-init` service
idempotently creates `watchdog.flow.raw-v1` with 12 partitions. Host processes
connect to `127.0.0.1:9092`; Compose services use `kafka:19092`.

Inspect or stop the local data plane without deleting its named volumes:

```bash
make flow-dev-status
make flow-dev-down
```

`docker compose down -v` intentionally destroys the local Kafka log and
ClickHouse data and is therefore not wrapped in a Make target.

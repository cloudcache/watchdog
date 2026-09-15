# Watchdog engineering deployment

Watchdog has two persistent authorities: MySQL for management data and
ClickHouse for Flow/SNMP time-series. Kafka is the replayable Flow transport.
PocketBase, VictoriaMetrics and a same-origin frontend proxy are not part of
this deployment.

## 1. Start Kafka and ClickHouse

The development compose file starts Kafka with the 12-partition
`watchdog.flow.raw-v1` topic and ClickHouse on their host ports:

```bash
WATCHDOG_CLICKHOUSE_PASSWORD=watchdog-local \
  docker compose -f deploy/compose.flow-dev.yml up -d
docker compose -f deploy/compose.flow-dev.yml ps
```

Keep the ClickHouse password out of YAML. For local development only:

```bash
install -m 600 /dev/null /tmp/watchdog-clickhouse-password
printf '%s\n' 'watchdog-local' > /tmp/watchdog-clickhouse-password
```

Set `WATCHDOG_CLICKHOUSE_PASSWORD_FILE=/tmp/watchdog-clickhouse-password` when
starting every process that connects to ClickHouse. Production uses a mounted
secret file instead.

Kafka and ClickHouse are telemetry dependencies, not prerequisites for the
MySQL management-plane install. Starting them first makes Flow/SNMP query and
export available immediately. If ClickHouse is unavailable or its credentials
are wrong, installation and the login/RBAC/device/agent/address/billing
management APIs still start; `/api/v1/health` reports `503 degraded` with
`clickhouse_error`, and ClickHouse-backed query/export APIs return an explicit
`503`. Fix the dependency and restart `watchdog-server`; startup reruns the
idempotent ClickHouse migrations and wires the telemetry services. Kafka is
used only by the independent Flow collector/worker processes.

## 2. Start the independent backend and frontend

MySQL itself must be reachable, but the `watchdog` database may be absent or
empty. The backend creates the named database only; it does not create schema
or an administrator until installation is submitted.

```bash
WATCHDOG_CLICKHOUSE_PASSWORD_FILE=/tmp/watchdog-clickhouse-password \
  go run ./cmd/watchdog-server --config config/watchdog.yaml
```

In another terminal:

```bash
npm --prefix frontend run dev
```

The frontend listens on `127.0.0.1:8090`; the API listens on
`127.0.0.1:8091`. `frontend/public/watchdog-config.js` contains the direct
`API_URL`. CORS is controlled by `server.origins`; no proxy or static-file
server is involved.

## 3. First installation and login

On every page load the frontend reads `GET /api/v1/install-status`:

- an empty database redirects to `/install`;
- submitting `POST /api/v1/install` applies the embedded MySQL schema and
  atomically creates exactly one administrator; it also applies the ClickHouse
  schema when ClickHouse is reachable;
- a completed installation redirects to the normal login page;
- repeated installation is rejected with `409 already_installed`;
- all other API routes reject an uninstalled database with
  `428 install_required`.

No password is generated or logged. `WATCHDOG_ADMIN_PASSWORD` is an explicit
unattended-install override for automation; leave it unset for the UI flow.
Installation is intentionally public before the first administrator exists,
so keep the backend listener private until installation completes.

Health and install state can be checked independently:

```bash
curl -sS http://127.0.0.1:8091/api/v1/health
curl -sS http://127.0.0.1:8091/api/v1/install-status
```

`runtime_ready:true` means that the MySQL management runtime is available.
Telemetry readiness is reported independently by `clickhouse:true|false`; a
degraded health response therefore does not imply that login or management
CRUD is unavailable.

Installation seeds only RBAC and built-in MIB modules; the geo/address library
starts empty. To bootstrap the base library (EdgeManager-derived, ~2.66M CIDRs)
instead of importing an MMDB by hand, load the seed after installation:

```bash
deploy/seed/load-address-library.sh watchdog   # see deploy/seed/README.md
```

The seed is data-only and loads into empty address tables (the schema must
already be applied by a successful install). Skip this to start with an empty
library and import your own source via the Address Library UI.

## 4. Start the data-plane processes independently

SNMP discovery/polling reads device/profile state from MySQL and writes
samples directly to ClickHouse:

```bash
WATCHDOG_CLICKHOUSE_PASSWORD_FILE=/tmp/watchdog-clickhouse-password \
  go run ./cmd/watchdog-snmp-collector --config config/watchdog.yaml --loop
```

Flow collection and Flow processing are separate processes. The collector
decodes sFlow/NetFlow/IPFIX and publishes RawFlow to Kafka; the worker consumes
Kafka, performs in-memory classification and writes ClickHouse. Both require
the signed plans/publications produced through the administrator and agent
publication workflow:

```bash
go run ./cmd/watchdog-flow-collect \
  --plan /path/collector-plan.json \
  --plan-public-key /path/flow-plan.pub \
  --kafka-brokers 127.0.0.1:9092

WATCHDOG_CLICKHOUSE_PASSWORD_FILE=/tmp/watchdog-clickhouse-password \
  go run ./cmd/watchdog-flow-worker \
  --bootstrap-plan /path/collector-plan.json \
  --plan-public-key /path/flow-plan.pub \
  --bootstrap-version-publication /path/version-publication.json \
  --source-stream-id local-kafka-watchdog-flow-raw-v1 \
  --kafka-brokers 127.0.0.1:9092 \
  --clickhouse-password-file /tmp/watchdog-clickhouse-password
```

Using agent enrollment replaces the bootstrap files with signed immutable
plans plus local last-known-good files; it does not combine these processes
with the backend.

## Configuration precedence

The backend reads `--config`, then `WATCHDOG_CONFIG`, then
`config/watchdog.yaml`. Secret overrides are:

- `WATCHDOG_MYSQL_DSN`
- `WATCHDOG_CLICKHOUSE_PASSWORD_FILE`
- `WATCHDOG_ADMIN_PASSWORD` (unattended first install only)

The committed YAML must contain no passwords.

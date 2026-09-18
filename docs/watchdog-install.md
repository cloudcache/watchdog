# Watchdog engineering deployment

Watchdog has two persistent authorities: MySQL for management data and
ClickHouse for Flow/SNMP time-series. Kafka is the replayable Flow transport.
PocketBase, VictoriaMetrics and a same-origin frontend proxy are not part of
this deployment.

## 1. Start Kafka and ClickHouse

The development compose file starts Kafka with the 12-partition
`watchdog.flow.raw-v1` topic and ClickHouse on their host ports:

```bash
make flow-dev-up
docker compose -f deploy/compose.flow-dev.yml ps
```

The make target writes the local password with mode 0600 to the ignored stable
path `data/secrets/clickhouse-password`; `config/watchdog.yaml` references that
path. Override `WATCHDOG_CLICKHOUSE_PASSWORD` if needed. Production should set
`WATCHDOG_CLICKHOUSE_PASSWORD_FILE` to its mounted secret instead.

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
make dev-server
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
make dev-snmp-collector
```

Flow collection and Flow processing are separate processes. The collector
decodes sFlow/NetFlow/IPFIX and publishes RawFlow to Kafka; the worker consumes
Kafka, performs in-memory classification and writes ClickHouse. Both require
the signed plans/publications produced through the administrator and agent
publication workflow:

```bash
make build-flow-collect build-flow-worker

./build/watchdog-flow-collect \
  --plan /path/collector-plan.json \
  --plan-public-key /path/flow-plan.pub \
  --kafka-brokers 127.0.0.1:9092

./build/watchdog-flow-worker \
  --bootstrap-plan /path/collector-plan.json \
  --plan-public-key /path/flow-plan.pub \
  --bootstrap-version-publication /path/version-publication.json \
  --source-stream-id local-kafka-watchdog-flow-raw-v1 \
  --kafka-brokers 127.0.0.1:9092 \
  --clickhouse-password-file data/secrets/clickhouse-password
```

Agent enrollment does **not** replace process bootstrap or Flow business
artifacts. The host service manager still owns the binary, endpoints, secret
file paths, listener ports, restart policy and `enable --now`. Enrollment adds
the stable process identity and credential; the immutable Agent plan controls
only the process tunables implemented by that binary and is cached as an LKG.
The Flow collector source/sampling plan and the Flow worker enrichment
publication remain separate signed domain artifacts with their own version and
ACK. The administrator UI exposes Agent CRUD, enrollment, credential rotation,
binding, plans, ACK health and runs. For a process on the Watchdog host it also
displays one local activation command. Run it and paste the one-time token at
the hidden prompt:

```bash
sudo /opt/watchdog/current/deploy/systemd/activate-agent.sh \
  snmp snmp-main --plan-public-key PUBLIC_KEY_FROM_UI
```

The script uses the local API, installs the public trust key embedded in the
command, creates the protected bootstrap files and installs the packaged unit.
It then writes only file paths and the stable identity to its environment
file, and enables and restarts the service (the restart also upgrades a
previously active static unit into Registry mode). The explicit file-based form
remains available under the UI's advanced section for remote automation.
Registration atomically writes the long-lived credential with mode 0600 and
deletes the consumed enrollment file. Flow worker activation deliberately
refuses to run until `/etc/watchdog/flow/worker.env` exists; that file is the
deployment-owned Kafka/ClickHouse/source-stream/publication bootstrap, not an
Agent plan. Its Agent registry URL is separate from the Flow enrichment
publication URL, so enrollment cannot silently switch a working bootstrap
worker's data source. The Gin process is never granted root access to invoke
`systemctl`.

Use `Restart=on-failure` for each Agent-owned systemd service. A newly enrolled
process may run on its validated bootstrap/default tunables before the first
Agent plan exists. Heartbeats carry the desired plan version; publishing a new
immutable plan makes the process drain and exit non-zero, so systemd restarts
it and startup applies/ACKs that version. Revocation is different: a running
process receiving 401/403 drains and exits successfully, so it remains stopped
instead of entering a restart loop. The UI manages desired state and evidence;
the host service manager remains the only process-start authority.

## Configuration precedence

The backend reads `--config`, then `WATCHDOG_CONFIG`, then
`config/watchdog.yaml`. Secret overrides are:

- `WATCHDOG_MYSQL_DSN`
- `WATCHDOG_CLICKHOUSE_PASSWORD_FILE`
- `WATCHDOG_ADMIN_PASSWORD` (unattended first install only)

The committed YAML must contain no passwords.

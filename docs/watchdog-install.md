# Watchdog engineering deployment

Watchdog has two persistent authorities: MySQL for management data and
ClickHouse for Flow/SNMP time-series. Kafka is the replayable Flow transport.
PocketBase and VictoriaMetrics are not part of this deployment. The reference
frontend deployment is the static MPA served by nginx with a same-origin
`/api/` reverse proxy (`deploy/nginx/watchdog.conf`, `API_URL` left empty); set
`API_URL` only for a split-origin deployment.

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

### ClickHouse storage tiers, capacity and retention

The embedded migrations create one raw fact table and a pyramid of
generation-marked aggregates, all applied automatically on install/startup:

| Table | Role | Partition | DDL TTL |
|---|---|---|---|
| `flow_records` | raw facts (per-flow, endpoints) | by day | none — lifecycle-managed |
| `flow_aggregate_1m` | recent minute cache | by day | 2 days |
| `flow_aggregate_5m` | mid-range non-endpoint queries | by day | 90 days |
| `flow_interface_traffic_5m` | billing / interface evidence | by month | none — evidence, state-machine deleted |
| `flow_aggregate_1h` / `flow_aggregate_1d` | long-range trends + raw-delete conservation | by month | none — lifecycle-managed |
| `snmp_samples` / `snmp_interface_traffic_5m` / `snmp_events` | SNMP raw samples, billing 5m buckets, trap events | — | **none, and no retention is enforced** |

`flow_records` also carries a `flow_event_time_minmax` skip index so a sub-hour
query prunes within the containing hour instead of scanning it whole.

The SNMP tables have no DDL TTL, and `/api/v1/retention/policies` only stores
policies in MySQL — nothing executes them yet, so SNMP data grows without bound
until a retention executor exists. Budget disk for that, or prune manually.

**Capacity caveat — read before production.** Raw has **no automatic retention**.
It is removed only by the Flow lifecycle state machine, which is fail-closed and
does nothing until an operator publishes a retention policy. With no policy, raw
grows at roughly 15 GiB/day per exporter (≈62 compressed bytes/row) and must be
reclaimed by hand. The aggregate tiers are small (order 1 GB total) and are not
the capacity driver — raw is. Two prerequisites for a stable footprint:

1. **Keep raw off the OS root disk.** ClickHouse stores under
   `/var/lib/clickhouse` on the root filesystem by default; a full root wedges
   writes — and even `DROP PARTITION` fails, because ClickHouse runs as non-root
   and cannot use the ext4 reserved blocks. Put cold partitions on the largest
   disk with a storage policy (steps in
   `docs/watchdog-5m-atomic-tier-design-2026-09-23.md` §0).
2. **Publish a retention policy** so raw ages out by conservation instead of
   manual drops. Until then, monitor disk and Kafka lag: a root-disk fill that
   outlasts Kafka retention (default 6h) permanently loses the un-drained flow
   window.

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

The API service listens on `127.0.0.1:8091` by default. A deployment web server
may expose the frontend and API through one public address, or proxy them through
separate public addresses. Browsers reach same-origin `/api/` when
`watchdog-config.js` keeps `API_URL` empty; otherwise they use the configured
public API address. The frontend does not infer deployment topology.

For a deployment, build the frontend (`npm --prefix frontend run build`) and
serve `frontend/dist` with nginx using `deploy/nginx/watchdog.conf`. The build
creates a real HTML document for every fixed page and an explicit Nginx mapping
for each dynamic detail page. Navigation performs full browser document loads;
unknown paths and `/api/` return real errors instead of a frontend fallback.
HTML documents and `watchdog-config.js` are `no-store`; hashed assets remain
immutable. The backend itself never hosts static files. The production runtime
configuration is `/var/www/watchdog/watchdog-config.js`, at the site root next to
the generated documents. Edit that file in place to change `API_URL`; no frontend
rebuild, API restart, or Nginx reload is required. Keep the site root writable
only by the deployment owner; do not expose `/etc/watchdog`, which also contains
protected service configuration and secret paths.

Tagged releases publish the same directory as `watchdog-web_<tag>.tar.gz`.
Extract it into the Nginx site root; server and agent container images remain
process-only images and intentionally do not embed the web files.

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
curl -sS http://<watchdog-host>/api/v1/health
curl -sS http://<watchdog-host>/api/v1/install-status
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

SNMP discovery runs inside `watchdog-server`: once a device has an SNMP
profile, the server's reconcile loop discovers it and rediscovers it every
`snmp.rediscover_interval` (default 6h; `snmp.auto_discover` defaults to true),
and the manual discover action triggers it immediately. The SNMP collector only
polls: it reads device/profile/recipe state from MySQL, writes samples directly
to ClickHouse and rebuilds the most recent closed `snmp_interface_traffic_5m`
bucket after each pass:

```bash
make dev-snmp-collector
```

Flow collection and Flow processing are separate processes. The collector
receives sFlow/NetFlow/IPFIX datagrams and publishes them to Kafka as RawFlow
with a decoder hint; the worker consumes Kafka, decodes, performs in-memory
classification and writes ClickHouse. The collector's signed source plan is
still produced out-of-band and passed with `-plan` (the server only validates
exporter bindings); the worker's enrichment/deployment artifacts come from the
administrator publication workflow:

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

Agent registration does **not** replace process bootstrap or Flow business
artifacts. The host service manager still owns the binary, endpoints, secret
file paths, listener ports, restart policy and `enable --now`. Registration
adds the stable process identity; the immutable Agent plan controls only the
process tunables implemented by that binary and is cached as an LKG. The Flow
collector source/sampling plan and the Flow worker enrichment/deployment
artifacts remain separate signed domain artifacts with their own version and
ACK.

**Machine authentication is one installation-wide shared token** (since
`aa870f65d`). Set it on the server as `agents.shared_token` or the
`WATCHDOG_AGENT_SHARED_TOKEN` environment variable; while it is empty the
server rejects every register and machine call. Every agent (and the SNMP trap
forwarder's `-token`) is given the same token. There are no one-time enrollment
tokens, no per-agent credentials, no credential rotation and no mTLS agent
authentication. An agent registers itself idempotently on first contact when its
control-plane URL and shared-token file are configured (the Agent plan public
key and LKG path are needed for plan delivery); without them the process runs
headless and does not appear in the registry.

The administrator UI exposes Agent CRUD, binding, plans, ACK health and runs,
including each run's reported workload. For a process on the Watchdog host it
also displays one local activation command. Run it and paste the installation
shared token at the hidden prompt:

```bash
sudo /opt/watchdog/current/deploy/systemd/activate-agent.sh \
  snmp snmp-main --plan-public-key PUBLIC_KEY_FROM_UI
```

The script installs the public trust key embedded in the command, writes the
shared token to the agent's token file (mode 0600, owner `watchdog`), installs
the packaged unit, writes the control-plane URL, identity and file paths to the
unit's environment file, and enables and restarts the service (the restart also
upgrades a previously active static unit into Registry mode). The explicit
file-based form remains available for remote automation. Flow worker
activation deliberately refuses to run until `/etc/watchdog/flow/worker.env`
exists; that file is the deployment-owned Kafka/ClickHouse/source-stream
bootstrap, not an Agent plan. Its Agent registry URL is separate from the Flow
enrichment publication URL, so registration cannot silently switch a working
bootstrap worker's data source. The Gin process is never granted root access to
invoke `systemctl`.

**Upgrading from the enrollment era:** agents activated by the old script still
hold a per-agent `wda_…` credential in their token file and an
`-agent-enrollment-token-file` argument in their environment file. New binaries
accept and ignore that argument (with a deprecation warning), but they reject
the old credential: the process logs "agent token was rejected" and exits 0, so
it stays stopped rather than restart-looping. Set the server's shared token
first, re-run the new `activate-agent.sh` for every agent, then roll the
binaries — see `docs/watchdog-release-readiness-2026-09-28.md` §1.3.

Use `Restart=on-failure` for each Agent-owned systemd service. A newly
registered process may run on its validated bootstrap/default tunables before
the first Agent plan exists. Heartbeats carry the desired plan version;
publishing a new immutable plan makes the process drain and exit non-zero, so
systemd restarts it and startup applies/ACKs that version. Revocation is
different: the revoked agent id is rejected even with the shared token, and a
running process receiving 401/403 drains and exits successfully, so it remains
stopped instead of entering a restart loop. Health is derived from the
processing runs each agent reports (heartbeats only prove liveness) plus
staleness. The UI manages desired state and evidence; the host service manager
remains the only process-start authority.

## Configuration precedence

The backend reads `--config`, then `WATCHDOG_CONFIG`, then
`config/watchdog.yaml`. Secret overrides are:

- `WATCHDOG_MYSQL_DSN`
- `WATCHDOG_CLICKHOUSE_PASSWORD_FILE`
- `WATCHDOG_ADMIN_PASSWORD` (unattended first install only)
- `WATCHDOG_AGENT_SHARED_TOKEN` (overrides `agents.shared_token`; machine APIs
  stay closed while it is unset)

The committed YAML must contain no passwords.

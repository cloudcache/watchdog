# Watchdog local development

Local development uses one fixed control-plane process and independent data-plane
processes. The production deployment contract is still same-origin `/api/`; a
local frontend can point `API_URL` in `frontend/public/watchdog-config.js` at
`http://127.0.0.1:8091`. Vite has no API proxy. There is no PocketBase Hub or
static server in the backend.

| Process | Fixed executable | Responsibility |
| --- | --- | --- |
| Frontend | `npm --prefix frontend run dev` | Frontend development server |
| API | `build/watchdog-server` | Gin management/query API from `server.listen` |
| SNMP polling | `build/watchdog-snmp-collector` | Polling, MySQL recipes, ClickHouse samples, closed 5m interface buckets |
| SNMP traps | `build/watchdog-snmp-agent` | UDP Trap listener and forwarding to the API |
| Flow collection | `build/watchdog-flow-collect` | sFlow/NetFlow/IPFIX receive and Kafka production |
| Flow processing | `build/watchdog-flow-worker` | Kafka consumption, fast decode, in-memory classification, ClickHouse writes |

The API exposes SNMP and Flow management/query routes, but it does not open flow
sampling sockets, consume Kafka, or execute the SNMP polling loop. Query and export
workers that operate on already stored data remain control-plane jobs.

SNMP discovery is the exception: it runs inside the API process. Besides the
operator-triggered `POST /api/v1/devices/:id/snmp/discover`, the API runs a
continuous discovery reconcile loop that discovers profiled devices without
recipes and rediscovers every `snmp.rediscover_interval` (default 6h;
`snmp.auto_discover` defaults to true). `watchdog-snmp-collector` (and `make
dev-snmp-collector`, which runs with `--discover=false`) only polls and rebuilds
the most recent closed `snmp_interface_traffic_5m` bucket after each pass. So
the API is not a strictly network-I/O-free control plane.

## 1. Dependencies

MySQL must be reachable using `mysql.dsn` in `config/watchdog.yaml`. Kafka and
ClickHouse for local development can be started with:

`make flow-dev-up` creates the mode-0600 local secret at
`data/secrets/clickhouse-password` and starts both dependencies with the same
password. Override `WATCHDOG_CLICKHOUSE_PASSWORD` when needed; no process relies
on a task-specific or `/tmp` secret path.

```bash
make flow-dev-up
```

## 2. API and frontend

Build and run the API under its fixed name:

```bash
make dev-server
```

Run the frontend separately:

```bash
npm --prefix frontend run dev
```

Open `http://127.0.0.1:8090`. An empty management database redirects to
`/install`; installation creates the schema and first administrator only after
the form is submitted. The API remains on `http://127.0.0.1:8091`; `8090` is only
the local frontend development port and is not part of the production contract.

## 3. Independent SNMP processes

Build and start continuous polling independently from Gin:

```bash
make build-snmp-collector
./build/watchdog-snmp-collector \
  --config config/watchdog.yaml \
  --discover=false --poll=true --loop=true
```

One-off discovery uses the same collector executable:

```bash
./build/watchdog-snmp-collector \
  --config config/watchdog.yaml --device DEVICE_ID --discover=true --poll=false
```

The optional Trap listener is also an independent executable:

```bash
make build-snmp-agent
./build/watchdog-snmp-agent --config PATH_TO_TRAP_AGENT_CONFIG
```

## 4. Fixed local commands

- `make build-server`: compile only the Gin API to `build/watchdog-server`.
- `make dev-server`: build and run that fixed API executable.
- `make dev-frontend`: run the independent frontend development server.
- `make build-snmp-collector`: compile the SNMP polling process.
- `make dev-snmp-collector`: build and run continuous SNMP polling.
- `make build-snmp-agent`: compile the independent UDP Trap process.
- `make build-flow-collect`: compile the Flow receiver to `build/watchdog-flow-collect`.
- `make build-flow-worker`: compile the Flow worker to `build/watchdog-flow-worker`.
- `make build-runtime`: build the independent frontend and five product processes.
- `make build`: build the complete runtime plus the SNMP Trap agent and the
  legacy `watchdog-agent` (`internal/cmd/agent`, a WebSocket client whose PB-era
  server endpoint no longer exists). It does **not** build the system agent; use
  `go build -o build/watchdog-system-agent ./cmd/watchdog-system-agent`.
- `make build-web-ui`: build the frontend separately.

Do not name runtime binaries after a task (`watchdog-server-snmp`,
`watchdog-server-rbac`, and similar), and do not let multiple sessions replace
the listener configured in `config/watchdog.yaml`. Before starting a new API
process, resolve any existing listener on that address.

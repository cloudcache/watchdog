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

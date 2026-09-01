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

4. Optional: run the SNMP collector for one network device:

```bash
go run ./cmd/watchdog-snmp-collector \
  --config config/watchdog.dev.yaml \
  --tenant tenant_dev \
  --device "$WATCHDOG_NETWORK_DEVICE_ID" \
  --interval 1m \
  --loop
```

The collector loads the device SNMP profile, discovers supported modules,
imports collection recipes, and polls due recipes into raw samples.

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

## Commands

- `cmd/watchdog-dev-server`: local dev API with admin dev auth.
- `cmd/watchdog-install`: fresh MySQL installer using `install/init.sql` and a local lock file.
- `cmd/watchdog-export-worker`: async CSV export worker.
- `cmd/watchdog-snmp-collector`: SNMP discovery, recipe import, and raw sample polling.

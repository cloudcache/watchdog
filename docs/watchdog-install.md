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
- `cmd/watchdog-install`

Environment variables still override file values, so production deployments can keep secrets outside the YAML file. The main overrides are `WATCHDOG_MYSQL_DSN` and `WATCHDOG_VICTORIAMETRICS_URL`.

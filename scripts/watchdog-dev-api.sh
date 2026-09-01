#!/usr/bin/env bash
set -euo pipefail

MYSQL_HOST="${WATCHDOG_MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${WATCHDOG_MYSQL_PORT:-3306}"
MYSQL_USER="${WATCHDOG_MYSQL_USER:-root}"
MYSQL_PASSWORD="${WATCHDOG_MYSQL_PASSWORD:-}"
MYSQL_DB="${WATCHDOG_MYSQL_DB:-watchdog_dev}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WATCHDOG_CONFIG="${WATCHDOG_CONFIG:-$ROOT_DIR/config/watchdog.dev.yaml}"

if [[ -z "${WATCHDOG_MYSQL_DSN:-}" ]]; then
  export WATCHDOG_MYSQL_DSN="${MYSQL_USER}:${MYSQL_PASSWORD}@tcp(${MYSQL_HOST}:${MYSQL_PORT})/${MYSQL_DB}?parseTime=true&multiStatements=true"
fi

export WATCHDOG_DEV_ADDR="${WATCHDOG_DEV_ADDR:-127.0.0.1:8091}"
export WATCHDOG_DEV_TENANT_ID="${WATCHDOG_DEV_TENANT_ID:-tenant_dev}"
export WATCHDOG_DEV_USER_ID="${WATCHDOG_DEV_USER_ID:-user_dev}"

go run ./cmd/watchdog-dev-server --config "$WATCHDOG_CONFIG"

#!/usr/bin/env bash
set -euo pipefail

MYSQL_HOST="${WATCHDOG_MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${WATCHDOG_MYSQL_PORT:-3306}"
MYSQL_USER="${WATCHDOG_MYSQL_USER:-root}"
MYSQL_PASSWORD="${WATCHDOG_MYSQL_PASSWORD:-}"
MYSQL_DB="${WATCHDOG_MYSQL_DB:-watchdog_dev}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WATCHDOG_CONFIG="${WATCHDOG_CONFIG:-$ROOT_DIR/config/watchdog.dev.yaml}"
WATCHDOG_LOCK="${WATCHDOG_LOCK:-$ROOT_DIR/.watchdog-dev.lock}"

MYSQL_ARGS=(--protocol=TCP -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER")
if [[ -n "$MYSQL_PASSWORD" ]]; then
  MYSQL_ARGS+=("-p$MYSQL_PASSWORD")
fi

mysql "${MYSQL_ARGS[@]}" -e "CREATE DATABASE IF NOT EXISTS \`$MYSQL_DB\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

export WATCHDOG_MYSQL_DSN="${MYSQL_USER}:${MYSQL_PASSWORD}@tcp(${MYSQL_HOST}:${MYSQL_PORT})/${MYSQL_DB}?parseTime=true&multiStatements=true"

echo "Installing schema"
(cd "$ROOT_DIR" && go run ./cmd/watchdog-install --config "$WATCHDOG_CONFIG" --init-sql "$ROOT_DIR/install/init.sql" --lock "$WATCHDOG_LOCK")

echo "Applying dev seed"
mysql "${MYSQL_ARGS[@]}" "$MYSQL_DB" < "$ROOT_DIR/dev/seed.sql"

echo
echo "Database ready: $MYSQL_DB"
echo "export WATCHDOG_MYSQL_DSN='${MYSQL_USER}:${MYSQL_PASSWORD}@tcp(${MYSQL_HOST}:${MYSQL_PORT})/${MYSQL_DB}?parseTime=true&multiStatements=true'"

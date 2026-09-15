#!/usr/bin/env bash
# Load the EdgeManager base address library seed into a freshly-installed
# watchdog MySQL. Apply the schema first (server boot runs deploy/schema/mysql).
# Usage: deploy/seed/load-address-library.sh [db_name]
#   env: MYSQL_HOST (default 127.0.0.1), MYSQL_USER (default root), MYSQL_PWD (optional)
set -euo pipefail

db="${1:-watchdog}"
host="${MYSQL_HOST:-127.0.0.1}"
user="${MYSQL_USER:-root}"
seed="$(cd "$(dirname "$0")" && pwd)/address-library-em.sql.gz"

[ -f "$seed" ] || { echo "seed not found: $seed (see deploy/seed/README.md)" >&2; exit 1; }

existing=$(mysql -N -h"$host" -u"$user" "$db" -e \
  "SELECT COUNT(*) FROM address_base_prefixes" 2>/dev/null || echo "err")
if [ "$existing" = "err" ]; then
  echo "cannot query $db@$host (is the schema applied and are creds correct?)" >&2
  exit 1
fi
if [ "$existing" != "0" ]; then
  echo "refusing to load: address_base_prefixes already has $existing rows in $db" >&2
  exit 1
fi

echo "Loading $(du -h "$seed" | cut -f1) address library into $db@$host ..."
gzip -dc "$seed" | mysql --default-character-set=utf8mb4 -h"$host" -u"$user" "$db"
echo "done: $(mysql -N -h"$host" -u"$user" "$db" -e 'SELECT COUNT(*) FROM address_base_prefixes') base prefixes"

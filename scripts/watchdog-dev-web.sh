#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR/internal/site"

export VITE_WATCHDOG_DEV_AUTH=true
npm run dev -- --host 127.0.0.1 --port "${WATCHDOG_WEB_PORT:-5173}"

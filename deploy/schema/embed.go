// Package schema embeds the Watchdog v2 clean baselines (KISS). The MySQL baseline
// is the only mutable-management authority; the ClickHouse baseline holds time-series
// and the LibreNMS-structured log/alert tables. There is no tenant dimension and no
// PocketBase. New baselines are added as 0002_*.sql, 0003_*.sql, ... and applied in order.
package schema

import "embed"

//go:embed mysql/*.sql
var MySQL embed.FS

//go:embed clickhouse/*.sql
var ClickHouse embed.FS

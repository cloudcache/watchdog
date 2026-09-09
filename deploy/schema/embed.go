// Package schema embeds the Watchdog v2 MySQL management schema. ClickHouse has
// one immutable migration ledger under deploy/migration/clickhouse; application
// processes never create or alter ClickHouse tables at startup.
package schema

import "embed"

//go:embed mysql/*.sql
var MySQL embed.FS

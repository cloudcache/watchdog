// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestFlowSchemaMigrationKeepsOneCanonicalContract(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve schema test path")
	}
	directory := filepath.Join(filepath.Dir(file), "..", "..", "deploy", "migration", "clickhouse")
	paths, err := filepath.Glob(filepath.Join(directory, "*.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 || filepath.Base(paths[0]) != "001_flow_schema.sql" || filepath.Base(paths[1]) != "002_flow_geo_hierarchy.sql" {
		t.Fatalf("unexpected ClickHouse migrations: %v", paths)
	}
	var sql strings.Builder
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sql.Write(data)
		sql.WriteByte('\n')
	}
	allSQL := sql.String()
	if count := strings.Count(allSQL, "CREATE TABLE IF NOT EXISTS watchdog_flow."); count != 5 {
		t.Fatalf("ClickHouse flow table count=%d, want 5", count)
	}
	for _, required := range []string{
		"flow_records", "flow_aggregate_1m", "flow_aggregate_1h", "flow_ingest_batches", "flow_vpn_candidates",
		"record_id FixedString(32)", "quality_flags UInt64", "estimated_valid Bool",
		"'on_net_local_city'=1", "'off_net_in_province'=4", "remote_geo_continent_id", "remote_geo_region_id",
		"remote_geo_country_id", "remote_geo_province_id", "remote_geo_city_id", "'flow-geo-v2'=4",
	} {
		if !strings.Contains(allSQL, required) {
			t.Fatalf("ClickHouse migration is missing %q", required)
		}
	}
	for _, obsolete := range []string{"record_id FixedString(64)", "quality_flags Array", "'onnet_local_city'", "'offnet_same_province'"} {
		if strings.Contains(allSQL, obsolete) {
			t.Fatalf("ClickHouse migration retained obsolete contract %q", obsolete)
		}
	}
}

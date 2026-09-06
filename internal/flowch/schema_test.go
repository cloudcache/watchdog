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
	if len(paths) != 9 || filepath.Base(paths[0]) != "001_flow_schema.sql" || filepath.Base(paths[1]) != "002_flow_geo_hierarchy.sql" || filepath.Base(paths[2]) != "003_flow_vpn_candidate_generation.sql" || filepath.Base(paths[3]) != "004_flow_ingest_receipt_audit.sql" || filepath.Base(paths[4]) != "005_flow_fact_provenance.sql" || filepath.Base(paths[5]) != "006_flow_ingest_audit_projection.sql" || filepath.Base(paths[6]) != "007_flow_records_codecs.sql" || filepath.Base(paths[7]) != "008_flow_aggregate_codecs.sql" || filepath.Base(paths[8]) != "009_flow_address_dict_source.sql" {
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
	if count := strings.Count(allSQL, "CREATE TABLE IF NOT EXISTS watchdog_flow."); count != 6 {
		t.Fatalf("ClickHouse flow table count=%d, want 6", count)
	}
	for _, required := range []string{
		"flow_records", "flow_aggregate_1m", "flow_aggregate_1h", "flow_ingest_batches", "flow_vpn_candidates",
		"flow_address_dict_source", "dict_version UInt64", "group_ids Array(String)",
		"record_id FixedString(32)", "quality_flags UInt64", "estimated_valid Bool",
		"'on_net_local_city'=1", "'off_net_in_province'=4", "remote_geo_continent_id", "remote_geo_region_id",
		"remote_geo_country_id", "remote_geo_province_id", "remote_geo_city_id", "'flow-geo-v2'=4",
		"row_kind Enum8('candidate'=1,'_generation'=2)", "remote_prefix_id LowCardinality(String)",
		"geo_version LowCardinality(String)", "classification_version UInt32",
		"key_row_kind UInt8", "key_dimension_snapshot_id String", "key_geo_version String", "key_classification_version UInt32",
		"key_row_kind, key_dimension_snapshot_id, key_geo_version, key_classification_version",
		"receipt_schema UInt16 DEFAULT 1", "tenant_ids Array(String)",
		"raw_packets UInt64", "estimated_packets UInt64", "estimated_valid_records UInt64",
		"min_event_time DateTime64(3, 'UTC')", "max_event_time DateTime64(3, 'UTC')",
		"fact_schema UInt16 DEFAULT 1", "supplier_remote_country FixedString(2)",
		"supplier_remote_geo_continent_id", "supplier_remote_geo_region_id", "supplier_remote_geo_country_id",
		"supplier_remote_geo_province_id", "supplier_remote_geo_city_id", "supplier_remote_asn_source Enum8(",
		"supplier_geo_version LowCardinality(String)", "supplier_category Enum8(", "customer_geo_override_fields UInt8",
		"deduplicate_merge_projection_mode = 'rebuild'", "flow_ingest_audit_v1", "MATERIALIZE PROJECTION flow_ingest_audit_v1",
		"ORDER BY (kafka_topic, kafka_partition, kafka_offset, record_index, record_id)",
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

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
	expected := []string{
		"001_flow_schema.sql", "002_flow_geo_hierarchy.sql", "003_flow_vpn_candidate_generation.sql",
		"004_flow_ingest_receipt_audit.sql", "005_flow_fact_provenance.sql", "006_flow_ingest_audit_projection.sql",
		"007_flow_records_codecs.sql", "008_flow_aggregate_codecs.sql", "009_flow_address_dict_source.sql",
		"010_flow_aggregate_reorder.sql", "011_flow_storage_v2.sql", "012_snmp_telemetry.sql",
		"013_flow_vpn_candidate_features.sql", "014_snmp_events.sql", "015_flow_raw_delete_quarantine.sql",
		"016_flow_historical_reclassification.sql", "017_flow_reclassification_counter_totals.sql",
		"018_sflow_interface_counters.sql",
	}
	if len(paths) != len(expected) {
		t.Fatalf("unexpected ClickHouse migrations: %v", paths)
	}
	for index, want := range expected {
		if filepath.Base(paths[index]) != want {
			t.Fatalf("unexpected ClickHouse migration[%d]=%s, want %s", index, paths[index], want)
		}
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
	if count := strings.Count(allSQL, "CREATE TABLE IF NOT EXISTS watchdog_flow."); count != 13 {
		t.Fatalf("ClickHouse table count=%d, want 13", count)
	}
	for _, required := range []string{
		"flow_records", "flow_aggregate_1m", "flow_aggregate_1h", "flow_ingest_batches", "flow_vpn_candidates",
		"flow_address_dict_source", "dict_version UInt64", "group_ids Array(String)",
		"record_id FixedString(32)", "quality_flags UInt64", "estimated_valid Bool",
		"'on_net_local_city'=1", "'off_net_in_province'=4", "remote_geo_continent_id", "remote_geo_region_id",
		"remote_geo_country_id", "remote_geo_province_id", "remote_geo_city_id", "'flow-geo-v2'=4",
		"row_kind Enum8('candidate'=1,'_generation'=2)", "remote_prefix_id LowCardinality(String)",
		"local_prefix_id LowCardinality(String)", "packet_bytes_p50 UInt64",
		"geo_version LowCardinality(String)", "classification_version UInt32",
		"key_row_kind UInt8", "key_dimension_snapshot_id String", "key_geo_version String", "key_classification_version UInt32",
		"key_row_kind, key_dimension_snapshot_id, key_geo_version, key_classification_version",
		"receipt_schema UInt16 DEFAULT 1",
		"raw_packets UInt64", "estimated_packets UInt64", "estimated_valid_records UInt64",
		"min_event_time DateTime64(3, 'UTC')", "max_event_time DateTime64(3, 'UTC')",
		"fact_schema UInt16 DEFAULT 1", "supplier_remote_country FixedString(2)",
		"supplier_remote_geo_continent_id", "supplier_remote_geo_region_id", "supplier_remote_geo_country_id",
		"supplier_remote_geo_province_id", "supplier_remote_geo_city_id", "supplier_remote_asn_source Enum8(",
		"supplier_geo_version LowCardinality(String)", "supplier_category Enum8(", "customer_geo_override_fields UInt8",
		"deduplicate_merge_projection_mode = 'rebuild'", "flow_ingest_audit_v1", "MATERIALIZE PROJECTION flow_ingest_audit_v1",
		"ORDER BY (kafka_topic, kafka_partition, kafka_offset, record_index, record_id)",
		"snmp_samples", "snmp_interface_traffic_5m", "counter_value  UInt64", "counter_width  UInt8",
		"snmp_events", "ReplacingMergeTree(ingested_at)", "ORDER BY (device_id, occurred_at, id)",
		"flow_quarantined_datagrams", "late_quarantined", "raw_payload String CODEC(ZSTD(3))",
		"flow_reclassified_records", "flow_reclassification_generations", "reclassification_generation", "raw_bytes Decimal(39, 0)",
		"sflow_interface_counters", "counter_record_count UInt64", "if_in_octets UInt64", "if_out_octets UInt64",
	} {
		if !strings.Contains(allSQL, required) {
			t.Fatalf("ClickHouse migration is missing %q", required)
		}
	}
	for _, obsolete := range []string{"record_id FixedString(64)", "quality_flags Array", "'onnet_local_city'", "'offnet_same_province'", "tenant_id", "tenant_ids"} {
		if strings.Contains(allSQL, obsolete) {
			t.Fatalf("ClickHouse migration retained obsolete contract %q", obsolete)
		}
	}
	v2Data, err := os.ReadFile(paths[10])
	if err != nil {
		t.Fatal(err)
	}
	v2 := string(v2Data)
	for _, required := range []string{
		"source_stream_id LowCardinality(String)",
		"PARTITION BY toYYYYMMDD(event_time)",
		"ORDER BY (\n  toStartOfHour(event_time), source_stream_id,\n  kafka_partition, kafka_offset, record_index)",
		"flow_ingest_receipts_v2_staging", "receipt_schema UInt16 DEFAULT 4",
		"message_disposition Enum8(", "'template_missing'=2", "'mapping_rejected'=5",
		"ORDER BY (source_stream_id, kafka_partition, kafka_offset)",
		"flow_records_legacy_hash_v1", "flow_ingest_batches_legacy_hash_v1",
		"PARTITION BY toYYYYMM(bucket)",
		"flow_aggregate_1m_legacy_ttl_v1", "flow_aggregate_1h_legacy_ttl_v1",
	} {
		if !strings.Contains(v2, required) {
			t.Fatalf("ClickHouse V2 migration is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"TTL event_time", "TTL inserted_at", "record_id FixedString", "ingest_batch_id FixedString",
		"dimension_fingerprint UInt64", "checksum FixedString",
	} {
		if strings.Contains(v2, forbidden) {
			t.Fatalf("ClickHouse V2 migration retained forbidden contract %q", forbidden)
		}
	}
	snmpData, err := os.ReadFile(paths[11])
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"TTL observed_at", "TTL bucket_start", "tenant_id", "source_kind", "telemetry_samples"} {
		if strings.Contains(string(snmpData), forbidden) {
			t.Fatalf("ClickHouse SNMP migration retained forbidden contract %q", forbidden)
		}
	}
}

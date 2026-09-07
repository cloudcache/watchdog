// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/internal/flowquery"
)

// TestRealClickHouseStorageV2MigrationBackfill proves the maintenance-window
// upgrade against an actual V1 schema and retained data. It deliberately does
// not use openDataIntegrationClickHouse because that helper starts at V2.
func TestRealClickHouseStorageV2MigrationBackfill(t *testing.T) {
	if os.Getenv("WATCHDOG_FLOW_CLICKHOUSE_DATA_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_FLOW_CLICKHOUSE_DATA_INTEGRATION=1 to run")
	}
	const database = "watchdog_flow_it_storage_v2_upgrade"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	adminConfig := realMigrationConfig(t, "watchdog-flow-storage-v2-upgrade-admin", 30*time.Second)
	adminConfig.Database = "default"
	admin, err := NewNativeInserter(ctx, adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := admin.executor.Do(cleanupCtx, ch.Query{Body: "DROP DATABASE IF EXISTS " + database}); err != nil {
			t.Errorf("drop Storage V2 upgrade database: %v", err)
		}
	}()
	if err := admin.executor.Do(ctx, ch.Query{Body: "DROP DATABASE IF EXISTS " + database}); err != nil {
		t.Fatal(err)
	}

	migrations, err := LoadMigrations(os.DirFS("../../deploy/migration/clickhouse"), ".")
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) != 11 {
		t.Fatalf("migration count=%d, want 11", len(migrations))
	}
	applyStorageV2Migrations(t, ctx, admin, database, migrations[:10])

	dataConfig := realMigrationConfig(t, "watchdog-flow-storage-v2-upgrade", 30*time.Second)
	dataConfig.Database = database
	native, err := NewNativeInserter(ctx, dataConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	legacyInsert := `INSERT INTO flow_records (
  event_time, received_time, record_id, ingest_batch_id, ingest_generation,
  kafka_topic, kafka_partition, kafka_offset, record_index, tenant_id,
  raw_bytes, raw_packets, estimated_valid, estimated_bytes, estimated_packets,
  dimension_snapshot_id, geo_version, business_direction, category, disposition,
  classification_version, fact_schema
) VALUES (
  toDateTime64('2026-09-05 02:03:04.005', 3, 'UTC'),
  toDateTime64('2026-09-05 02:03:05.000', 3, 'UTC'),
  unhex(repeat('01', 32)), unhex(repeat('02', 32)), 7,
  'watchdog.flow.raw-v1', 3, 42, 0, 'tenant-a',
  1234, 12, 1, 123400, 1200,
  'snapshot-a', 'geo-a', 'out', 'overseas', 'count', 9, 2
)`
	if err := native.executor.Do(ctx, synchronousMigrationQuery(legacyInsert)); err != nil {
		t.Fatalf("insert legacy fact: %v", err)
	}

	applyStorageV2Migrations(t, ctx, admin, database, migrations[10:])
	if err := native.Ready(ctx); err != nil {
		t.Fatalf("V2 readiness after migration: %v", err)
	}

	var stream, topic proto.ColStr
	var partition, recordIndex proto.ColUInt32
	var offset, generation, count, rawBytes, rawPackets, estimatedBytes, estimatedPackets, estimatedValid proto.ColUInt64
	var disposition proto.ColStr
	var receiptCount, receiptRawBytes, receiptRawPackets, receiptEstimatedBytes, receiptEstimatedPackets, receiptEstimatedValid, receiptGeneration proto.ColUInt64
	query := ch.Query{
		Body: `SELECT
  any(toString(source_stream_id)) AS source_stream_id,
  any(toString(kafka_topic)) AS kafka_topic,
  any(kafka_partition) AS kafka_partition,
  any(kafka_offset) AS kafka_offset,
  any(record_index) AS record_index,
  max(ingest_generation) AS generation,
  count() AS record_count,
  sum(raw_bytes) AS raw_bytes,
  sum(raw_packets) AS raw_packets,
  sumIf(estimated_bytes, estimated_valid) AS estimated_bytes,
  sumIf(estimated_packets, estimated_valid) AS estimated_packets,
  countIf(estimated_valid) AS estimated_valid_records
FROM flow_records FINAL`,
		Result: proto.Results{
			{Name: "source_stream_id", Data: &stream}, {Name: "kafka_topic", Data: &topic},
			{Name: "kafka_partition", Data: &partition}, {Name: "kafka_offset", Data: &offset},
			{Name: "record_index", Data: &recordIndex}, {Name: "generation", Data: &generation},
			{Name: "record_count", Data: &count}, {Name: "raw_bytes", Data: &rawBytes},
			{Name: "raw_packets", Data: &rawPackets}, {Name: "estimated_bytes", Data: &estimatedBytes},
			{Name: "estimated_packets", Data: &estimatedPackets}, {Name: "estimated_valid_records", Data: &estimatedValid},
		},
	}
	factRows := 0
	query.OnResult = func(_ context.Context, block proto.Block) error {
		factRows += block.Rows
		return nil
	}
	if err := native.executor.Do(ctx, query); err != nil {
		t.Fatal(err)
	}
	if factRows != 1 || stream.Row(0) != "legacy:watchdog.flow.raw-v1" || topic.Row(0) != "watchdog.flow.raw-v1" ||
		partition[0] != 3 || offset[0] != 42 || recordIndex[0] != 0 || generation[0] != 7 ||
		count[0] != 1 || rawBytes[0] != 1234 || rawPackets[0] != 12 || estimatedBytes[0] != 123400 || estimatedPackets[0] != 1200 || estimatedValid[0] != 1 {
		t.Fatalf("migrated fact identity/counters mismatch: stream=%q topic=%q partition=%d offset=%d index=%d generation=%d count=%d raw=%d/%d estimated=%d/%d valid=%d",
			stream.Row(0), topic.Row(0), partition[0], offset[0], recordIndex[0], generation[0], count[0], rawBytes[0], rawPackets[0], estimatedBytes[0], estimatedPackets[0], estimatedValid[0])
	}
	rollup, err := NewRollupRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	for hour := 0; hour < 24; hour++ {
		bucket := day.Add(time.Duration(hour) * time.Hour)
		if err := rollup.Run(ctx, RollupRequest{TenantID: "tenant-a", Resolution: RollupOneHour,
			Bucket: bucket, Generation: 1, GeneratedAt: day.Add(48 * time.Hour)}); err != nil {
			t.Fatalf("rollup migrated V2 hour %d: %v", hour, err)
		}
	}
	sourceCounters, archiveCounters, err := rollup.DayStorageCounters(ctx, "tenant-a", day)
	if err != nil {
		t.Fatal(err)
	}
	if sourceCounters != archiveCounters || sourceCounters != (StorageCounters{
		RecordCount: 1, RawBytes: 1234, RawPackets: 12, EstimatedBytes: 123400, EstimatedPackets: 1200, EstimatedValidRecords: 1,
	}) {
		t.Fatalf("UTC-day conservation source=%+v archive=%+v", sourceCounters, archiveCounters)
	}
	queryRunner, err := flowquery.NewRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	compiledHybrid, err := flowquery.Compile(flowquery.Scope{TenantID: "tenant-a"}, flowquery.Request{
		From: day, To: day.Add(48 * time.Hour), Bucket: flowquery.BucketOneHour, Interval: 24 * time.Hour,
		Metric: flowquery.MetricEstimatedBytes, Dimension: flowquery.DimensionTotal,
		View: flowquery.ViewCustomer, TopN: 1, StorageV2: true, ArchiveThrough: day.Add(24 * time.Hour),
	}, day.Add(72*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	hybridResult, err := queryRunner.Run(ctx, compiledHybrid)
	if err != nil {
		t.Fatalf("run Storage V2 archive/raw hybrid query: %v", err)
	}
	if len(hybridResult.Points) != 1 || hybridResult.Points[0].Value != 123400 ||
		hybridResult.RollupCompleteness.ExpectedBuckets != 48 || hybridResult.RollupCompleteness.CoveredBuckets != 48 {
		t.Fatalf("hybrid result=%+v", hybridResult)
	}
	compiledRaw, err := flowquery.Compile(flowquery.Scope{TenantID: "tenant-a"}, flowquery.Request{
		From: day.Add(2 * time.Hour), To: day.Add(3 * time.Hour), Bucket: flowquery.BucketOneMinute,
		Metric: flowquery.MetricEstimatedBytes, Dimension: flowquery.DimensionTotal,
		View: flowquery.ViewCustomer, TopN: 1, StorageV2: true, ArchiveThrough: day.Add(2 * time.Hour),
	}, day.Add(72*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	rawResult, err := queryRunner.Run(ctx, compiledRaw)
	if err != nil {
		t.Fatalf("run Storage V2 raw-minute query: %v", err)
	}
	if len(rawResult.Points) != 1 || rawResult.Points[0].Value != 123400 ||
		rawResult.RollupCompleteness.ExpectedBuckets != 60 || rawResult.RollupCompleteness.CoveredBuckets != 60 {
		t.Fatalf("raw result=%+v", rawResult)
	}
	overseasRunner, err := flowquery.NewOverseasRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	compiledOverseas, err := flowquery.CompileOverseas(flowquery.Scope{TenantID: "tenant-a"}, flowquery.OverseasRequest{
		From: day, To: day.Add(48 * time.Hour), Bucket: flowquery.BucketOneHour,
		Metric: flowquery.MetricEstimatedBytes, GeoLevel: flowquery.OverseasGeoCountry,
		View: flowquery.ViewCustomer, TopN: 5, IncludeOther: true,
		StorageV2: true, ArchiveThrough: day.Add(24 * time.Hour),
	}, day.Add(72*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	overseasResult, err := overseasRunner.Run(ctx, compiledOverseas)
	if err != nil {
		t.Fatalf("run Storage V2 overseas query: %v", err)
	}
	if overseasResult.RollupCompleteness.ExpectedBuckets != 48 || overseasResult.RollupCompleteness.CoveredBuckets != 48 {
		t.Fatalf("overseas completeness=%+v", overseasResult.RollupCompleteness)
	}
	foundCombined := false
	for _, point := range overseasResult.Points {
		if point.Kind == flowquery.OverseasRowKPI && point.Direction == flowquery.OverseasDirectionCombined &&
			point.IPFamily == flowquery.OverseasIPFamilyAll && point.Value == 123400 {
			foundCombined = true
		}
	}
	if !foundCombined {
		t.Fatalf("overseas points=%+v", overseasResult.Points)
	}

	receiptQuery := ch.Query{
		Body: `SELECT
  any(toString(message_disposition)) AS message_disposition,
  any(record_count) AS record_count,
  any(raw_bytes) AS raw_bytes,
  any(raw_packets) AS raw_packets,
  any(estimated_bytes) AS estimated_bytes,
  any(estimated_packets) AS estimated_packets,
  any(estimated_valid_records) AS estimated_valid_records,
  any(generation) AS generation
FROM flow_ingest_receipts FINAL`,
		Result: proto.Results{
			{Name: "message_disposition", Data: &disposition}, {Name: "record_count", Data: &receiptCount},
			{Name: "raw_bytes", Data: &receiptRawBytes}, {Name: "raw_packets", Data: &receiptRawPackets},
			{Name: "estimated_bytes", Data: &receiptEstimatedBytes}, {Name: "estimated_packets", Data: &receiptEstimatedPackets},
			{Name: "estimated_valid_records", Data: &receiptEstimatedValid}, {Name: "generation", Data: &receiptGeneration},
		},
	}
	receiptRows := 0
	receiptQuery.OnResult = func(_ context.Context, block proto.Block) error {
		receiptRows += block.Rows
		return nil
	}
	if err := native.executor.Do(ctx, receiptQuery); err != nil {
		t.Fatal(err)
	}
	if receiptRows != 1 || disposition.Row(0) != "persisted" || receiptCount[0] != 1 || receiptRawBytes[0] != 1234 || receiptRawPackets[0] != 12 ||
		receiptEstimatedBytes[0] != 123400 || receiptEstimatedPackets[0] != 1200 || receiptEstimatedValid[0] != 1 || receiptGeneration[0] != 7 {
		t.Fatalf("reconstructed receipt mismatch: disposition=%q count=%d raw=%d/%d estimated=%d/%d valid=%d generation=%d",
			disposition.Row(0), receiptCount[0], receiptRawBytes[0], receiptRawPackets[0], receiptEstimatedBytes[0], receiptEstimatedPackets[0], receiptEstimatedValid[0], receiptGeneration[0])
	}

	var legacyTables proto.ColUInt64
	legacyQuery := ch.Query{
		Body: `SELECT count() FROM system.tables
WHERE database = currentDatabase()
  AND name IN ('flow_records_legacy_hash_v1', 'flow_ingest_batches_legacy_hash_v1')`,
		Result: proto.Results{{Name: "count()", Data: &legacyTables}},
	}
	legacyRows := 0
	legacyQuery.OnResult = func(_ context.Context, block proto.Block) error {
		legacyRows += block.Rows
		return nil
	}
	if err := native.executor.Do(ctx, legacyQuery); err != nil {
		t.Fatal(err)
	}
	if legacyRows != 1 || legacyTables[0] != 2 {
		t.Fatalf("legacy rollback tables=%d, want 2", legacyTables[0])
	}

	var tableNames, partitionKeys, createQueries proto.ColStr
	partitionQuery := ch.Query{
		Body: `SELECT name, partition_key, create_table_query
FROM system.tables
WHERE database = currentDatabase()
  AND name IN ('flow_records', 'flow_aggregate_1m', 'flow_aggregate_1h')
ORDER BY name`,
		Result: proto.Results{
			{Name: "name", Data: &tableNames}, {Name: "partition_key", Data: &partitionKeys},
			{Name: "create_table_query", Data: &createQueries},
		},
	}
	partitionRows := 0
	partitionQuery.OnResult = func(_ context.Context, block proto.Block) error {
		partitionRows += block.Rows
		return nil
	}
	if err := native.executor.Do(ctx, partitionQuery); err != nil {
		t.Fatal(err)
	}
	if partitionRows != 3 {
		t.Fatalf("live Storage V2 table rows=%d, want 3", partitionRows)
	}
	for index := 0; index < partitionRows; index++ {
		if !strings.Contains(partitionKeys.Row(index), "tenant_id") || strings.Contains(strings.ToUpper(createQueries.Row(index)), " TTL ") {
			t.Fatalf("table %s partition=%q retained fixed TTL: %s", tableNames.Row(index), partitionKeys.Row(index), createQueries.Row(index))
		}
	}
}

func applyStorageV2Migrations(t testing.TB, ctx context.Context, admin *NativeInserter, database string, migrations []Migration) {
	t.Helper()
	for _, migration := range migrations {
		for statementIndex, statement := range migration.Statements {
			isolated := strings.ReplaceAll(statement, migrationDatabase, database)
			if err := admin.executor.Do(ctx, synchronousMigrationQuery(isolated)); err != nil {
				t.Fatalf("apply isolated migration %03d statement %d: %v", migration.Version, statementIndex+1, err)
			}
		}
	}
}

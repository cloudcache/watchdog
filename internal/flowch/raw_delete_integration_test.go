// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowtombstone"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

// TestRealClickHouseRawDayDeletion proves that the destructive executor drops
// exactly one UTC raw partition, includes disposition=drop rows in its physical
// pre/post proof, preserves the hourly archive, and converges on replay with a
// stable ClickHouse query ID.
func TestRealClickHouseRawDayDeletion(t *testing.T) {
	ctx, native := openDataIntegrationClickHouse(t, "watchdog_flow_it_raw_delete")
	day := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	counted := integrationRecord(1, day.Add(10*time.Minute), "geo-city-a", 100)
	dropped := integrationRecord(2, day.Add(20*time.Minute), "geo-city-b", 900)
	dropped.Disposition = flowdimension.DispositionDrop
	otherDay := integrationRecord(3, day.Add(24*time.Hour+10*time.Minute), "geo-city-c", 50)
	insertIntegrationBatch(t, ctx, native, integrationBatch(10, day.Add(time.Hour), counted, dropped))
	insertIntegrationBatch(t, ctx, native, integrationBatch(11, day.Add(25*time.Hour), otherDay))

	runner, err := NewRollupRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	for hour := 0; hour < 24; hour++ {
		bucket := day.Add(time.Duration(hour) * time.Hour)
		if err := runner.Run(ctx, RollupRequest{
			Resolution: RollupOneHour, Bucket: bucket, Generation: 1, GeneratedAt: day.Add(48 * time.Hour),
		}); err != nil {
			t.Fatalf("roll up hour %d: %v", hour, err)
		}
	}
	physical, err := runner.RawDayPhysicalRecords(ctx, day)
	if err != nil {
		t.Fatal(err)
	}
	raw, archive, err := runner.DayStorageCounters(ctx, day)
	if err != nil {
		t.Fatal(err)
	}
	want := StorageCounters{RecordCount: 1, RawBytes: 100, RawPackets: 1, EstimatedBytes: 1000, EstimatedPackets: 10, EstimatedValidRecords: 1}
	if physical != 2 || raw != want || archive != want {
		t.Fatalf("pre-delete physical=%d raw=%+v archive=%+v", physical, raw, archive)
	}

	const queryID = "flow-raw-delete-real-it"
	if err := runner.DropRawDay(ctx, day, queryID); err != nil {
		t.Fatal(err)
	}
	if err := runner.DropRawDay(ctx, day, queryID); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	physical, err = runner.RawDayPhysicalRecords(ctx, day)
	if err != nil {
		t.Fatal(err)
	}
	raw, archive, err = runner.DayStorageCounters(ctx, day)
	if err != nil {
		t.Fatal(err)
	}
	otherPhysical, err := runner.RawDayPhysicalRecords(ctx, day.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if physical != 0 || raw != (StorageCounters{}) || archive != want || otherPhysical != 1 {
		t.Fatalf("post-delete physical=%d raw=%+v archive=%+v other-day=%d", physical, raw, archive, otherPhysical)
	}
}

func TestRealClickHouseLateDatagramQuarantineIsDurableAndIdempotent(t *testing.T) {
	ctx, native := openDataIntegrationClickHouse(t, "watchdog_flow_it_raw_quarantine")
	eventTime := time.Date(2026, 9, 5, 10, 15, 0, 0, time.UTC)
	old := integrationBatch(91, eventTime.Add(time.Second), integrationRecord(0, eventTime, "geo-city-a", 120))
	old.SourceStreamID = "cluster-a:raw-v1:incarnation-1"
	old.KafkaTopic = "watchdog.flow.raw-v1"
	old.KafkaPartition = 2
	insertIntegrationBatch(t, ctx, native, old)
	runner, err := NewRollupRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.DropRawDay(ctx, eventTime.Truncate(24*time.Hour), "flow-raw-quarantine-setup"); err != nil {
		t.Fatal(err)
	}
	batch := &flowworker.RecordBatch{
		BatchSchemaVersion: 1, MessageDisposition: flowworker.MessageDispositionPersisted,
		SourceStreamID: "cluster-a:raw-v1:incarnation-1", KafkaTopic: "watchdog.flow.raw-v1", KafkaPartition: 2, KafkaOffset: 91,
		CollectorID: "collector-a", ExporterID: "router-a", RegistryVersion: 3, ReceivedAtUnixMS: eventTime.Add(time.Second).UnixMilli(),
		Protocol: 5, SourceIP: netip.MustParseAddr("192.0.2.10").AsSlice(), ObservationDomainID: 7,
		SubAgentID: 4, DatagramSequence: 88, AgentIP: netip.MustParseAddr("192.0.2.20").AsSlice(), ExporterEpoch: 2,
		RawPayload: []byte{0xde, 0xad, 0xbe, 0xef},
		Records: []*flowworker.Record{{
			RecordIndex: 0, EventTimeUnixMS: eventTime.UnixMilli(), RawBytes: 120, RawPackets: 2,
			EstimatedValid: true, EstimatedBytes: 1_200, EstimatedPackets: 20,
		}},
	}
	decision := flowtombstone.Decision{Revision: 11, DeletedThrough: "2026-09-05", EventDay: "2026-09-05"}
	item, err := prepareQuarantine(batch, decision)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := native.InsertFlowQuarantine(ctx, item); err != nil {
			t.Fatalf("insert quarantine attempt %d: %v", attempt+1, err)
		}
	}
	quarantineRows, quarantineBytes := quarantineIntegrationCounters(t, ctx, native, batch)
	receiptRows, receiptBytes := quarantineReceiptIntegrationCounters(t, ctx, native, batch)
	factRows := quarantineTableCount(t, ctx, native, flowRecordsTable, batch)
	if quarantineRows != 1 || quarantineBytes != 120 || receiptRows != 1 || receiptBytes != 120 || factRows != 0 {
		t.Fatalf("quarantine rows/bytes=%d/%d receipt=%d/%d facts=%d", quarantineRows, quarantineBytes, receiptRows, receiptBytes, factRows)
	}
	scanner, err := NewReconciliationScanner(native)
	if err != nil {
		t.Fatal(err)
	}
	reconciled, err := scanner.Scan(ctx, ReconciliationScanRequest{
		Cursor: ReconciliationScanCursor{
			SourceStreamID: batch.SourceStreamID, KafkaTopic: batch.KafkaTopic,
			KafkaPartition: uint32(batch.KafkaPartition), NextOffset: uint64(batch.KafkaOffset),
		},
		CloseOffset: uint64(batch.KafkaOffset + 1), MaxBatches: 10, MaxFactRows: 10, MaxReadBytes: 1 << 20,
		CompareLimits: ReconciliationCompareLimits{MaxBatches: 10, MaxFacts: 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reconciled.Complete || reconciled.Comparison.Batches != 1 || reconciled.Comparison.Facts != 0 || len(reconciled.Comparison.Mismatches) != 0 {
		t.Fatalf("quarantined replay did not reconcile cleanly: %+v", reconciled)
	}
}

func quarantineIntegrationCounters(t testing.TB, ctx context.Context, native *NativeInserter, batch *flowworker.RecordBatch) (uint64, uint64) {
	t.Helper()
	var rows, rawBytes proto.ColUInt64
	query := ch.Query{
		Body: `SELECT count(), sum(raw_bytes) FROM flow_quarantined_datagrams FINAL
WHERE source_stream_id={stream:String} AND kafka_partition={partition:UInt32} AND kafka_offset={kafka_offset_value:UInt64}`,
		Parameters: ch.Parameters(map[string]any{"stream": batch.SourceStreamID, "partition": uint32(batch.KafkaPartition), "kafka_offset_value": uint64(batch.KafkaOffset)}),
		Result:     proto.Results{{Name: "count()", Data: &rows}, {Name: "sum(raw_bytes)", Data: &rawBytes}},
	}
	if err := native.executor.Do(ctx, query); err != nil {
		t.Fatalf("read quarantine counters: %v", err)
	}
	return rows[0], rawBytes[0]
}

func quarantineReceiptIntegrationCounters(t testing.TB, ctx context.Context, native *NativeInserter, batch *flowworker.RecordBatch) (uint64, uint64) {
	t.Helper()
	var rows, rawBytes proto.ColUInt64
	query := ch.Query{
		Body: `SELECT count(), sum(raw_bytes) FROM flow_ingest_receipts FINAL
WHERE source_stream_id={stream:String} AND kafka_partition={partition:UInt32} AND kafka_offset={kafka_offset_value:UInt64}
  AND message_disposition='late_quarantined'`,
		Parameters: ch.Parameters(map[string]any{"stream": batch.SourceStreamID, "partition": uint32(batch.KafkaPartition), "kafka_offset_value": uint64(batch.KafkaOffset)}),
		Result:     proto.Results{{Name: "count()", Data: &rows}, {Name: "sum(raw_bytes)", Data: &rawBytes}},
	}
	if err := native.executor.Do(ctx, query); err != nil {
		t.Fatalf("read quarantine receipt: %v", err)
	}
	return rows[0], rawBytes[0]
}

func quarantineTableCount(t testing.TB, ctx context.Context, native *NativeInserter, table string, batch *flowworker.RecordBatch) uint64 {
	t.Helper()
	var rows proto.ColUInt64
	query := ch.Query{
		Body:       "SELECT count() FROM " + table + " FINAL WHERE source_stream_id={stream:String} AND kafka_partition={partition:UInt32} AND kafka_offset={kafka_offset_value:UInt64}",
		Parameters: ch.Parameters(map[string]any{"stream": batch.SourceStreamID, "partition": uint32(batch.KafkaPartition), "kafka_offset_value": uint64(batch.KafkaOffset)}),
		Result:     proto.Results{{Name: "count()", Data: &rows}},
	}
	if err := native.executor.Do(ctx, query); err != nil {
		t.Fatalf("read %s rows: %v", table, err)
	}
	return rows[0]
}

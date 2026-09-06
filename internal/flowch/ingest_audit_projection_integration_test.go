// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

const ingestAuditCapacityRows = 2_000_000

// TestClickHouseIngestAuditProjectionCapacity applies migrations 001..005,
// inserts retained base facts, and then applies 006. This makes the before and
// after plans comparable and proves that migration completion includes old-part
// materialization rather than only accelerating new inserts.
func TestClickHouseIngestAuditProjectionCapacity(t *testing.T) {
	if os.Getenv("WATCHDOG_FLOW_CLICKHOUSE_CAPACITY_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_FLOW_CLICKHOUSE_CAPACITY_INTEGRATION=1 to run")
	}
	database := fmt.Sprintf("watchdog_flow_it_audit_path_%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	adminConfig := realMigrationConfig(t, "watchdog-flow-audit-capacity-admin", 30*time.Second)
	adminConfig.Database = "default"
	admin, err := NewNativeInserter(ctx, adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	var native *NativeInserter
	t.Cleanup(func() {
		if native != nil {
			native.Close()
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := admin.executor.Do(cleanupCtx, ch.Query{Body: "DROP DATABASE IF EXISTS " + database}); err != nil {
			t.Errorf("drop audit capacity database: %v", err)
		}
		admin.Close()
	})
	if err := admin.executor.Do(ctx, ch.Query{Body: "DROP DATABASE IF EXISTS " + database}); err != nil {
		t.Fatal(err)
	}

	migrations, err := LoadMigrations(os.DirFS("../../deploy/migration/clickhouse"), ".")
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) < 6 || migrations[5].Name != "006_flow_ingest_audit_projection.sql" {
		t.Fatalf("unexpected migration set: %+v", migrations)
	}
	applyAuditCapacityMigrations(t, ctx, admin, database, migrations[:5])

	dataConfig := realMigrationConfig(t, "watchdog-flow-audit-capacity", 30*time.Second)
	dataConfig.Database = database
	native, err = NewNativeInserter(ctx, dataConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := native.executor.Do(ctx, synchronousMigrationQuery(ingestAuditSyntheticInsert())); err != nil {
		t.Fatalf("insert synthetic audit facts: %v", err)
	}

	beforeBytes := ingestAuditTableBytes(t, ctx, native)
	beforeResult, beforeProgress := runIngestAuditCapacityQuery(t, ctx, native)
	applyAuditCapacityMigrations(t, ctx, admin, database, migrations[5:])
	afterResult, afterProgress := runIngestAuditCapacityQuery(t, ctx, native)
	plan := explainIngestAuditCapacityQuery(t, ctx, native)
	afterBytes := ingestAuditTableBytes(t, ctx, native)

	if beforeResult != afterResult || afterResult.records != 1_000 || afterResult.rawBytes != 100_000 {
		t.Fatalf("audit result before=%+v after=%+v", beforeResult, afterResult)
	}
	if !strings.Contains(plan, "flow_ingest_audit_v1") {
		t.Fatalf("audit projection was not selected:\n%s", plan)
	}
	if beforeProgress.rows < ingestAuditCapacityRows || afterProgress.rows*100 >= beforeProgress.rows {
		t.Fatalf("row pruning before=%+v after=%+v", beforeProgress, afterProgress)
	}
	if beforeProgress.bytes == 0 || afterProgress.bytes == 0 || afterProgress.bytes*50 >= beforeProgress.bytes {
		t.Fatalf("byte pruning before=%+v after=%+v", beforeProgress, afterProgress)
	}
	t.Logf("audit path rows %d -> %d, bytes %d -> %d, table bytes %d -> %d", beforeProgress.rows, afterProgress.rows, beforeProgress.bytes, afterProgress.bytes, beforeBytes, afterBytes)
}

func applyAuditCapacityMigrations(t testing.TB, ctx context.Context, admin *NativeInserter, database string, migrations []Migration) {
	t.Helper()
	for _, migration := range migrations {
		for statementIndex, statement := range migration.Statements {
			isolated := strings.ReplaceAll(statement, migrationDatabase, database)
			if err := admin.executor.Do(ctx, synchronousMigrationQuery(isolated)); err != nil {
				t.Fatalf("apply audit migration %03d statement %d: %v", migration.Version, statementIndex+1, err)
			}
		}
	}
}

func ingestAuditSyntheticInsert() string {
	return fmt.Sprintf(`INSERT INTO flow_records
  (event_time, received_time, record_id, ingest_batch_id, ingest_generation,
   kafka_topic, kafka_partition, kafka_offset, record_index,
   raw_bytes, raw_packets, estimated_valid, estimated_bytes, estimated_packets,
   quality_flags, dimension_fingerprint, classification_version)
SELECT
  toDateTime64('2026-09-01 00:00:00', 3, 'UTC') + toIntervalSecond(intDiv(number, 10000)),
  toDateTime64('2026-09-01 00:00:01', 3, 'UTC') + toIntervalSecond(intDiv(number, 10000)),
  SHA256(concat('record-', toString(number))),
  SHA256(concat('batch-', toString(number %% 4), '-', toString(intDiv(intDiv(number, 4), 100)))),
  1, 'watchdog.flow.raw-v1', number %% 4, intDiv(number, 4), number %% 8,
  100, 1, 1, 1000, 10, number, number, 1
FROM numbers(%d)`, ingestAuditCapacityRows)
}

type ingestAuditCapacityResult struct {
	records  uint64
	rawBytes uint64
}

type ingestAuditProgress struct {
	rows  uint64
	bytes uint64
}

func ingestAuditCapacityQuery(prefix string) string {
	return prefix + `SELECT count(), sum(tupleElement(latest, 6))
FROM (
  SELECT record_id,
    argMax(tuple(
      ingest_batch_id, kafka_topic, kafka_partition, kafka_offset, record_index,
      raw_bytes, raw_packets, estimated_valid, estimated_bytes, estimated_packets,
      quality_flags, dimension_fingerprint, classification_version), ingest_generation) AS latest
  FROM flow_records
  WHERE event_time >= toDateTime64('2026-09-01 00:00:00', 3, 'UTC')
    AND event_time < toDateTime64('2026-09-02 00:00:00', 3, 'UTC')
    AND kafka_topic = 'watchdog.flow.raw-v1'
    AND kafka_partition = 2
    AND kafka_offset BETWEEN 100000 AND 100999
  GROUP BY record_id
)`
}

func runIngestAuditCapacityQuery(t testing.TB, ctx context.Context, native *NativeInserter) (ingestAuditCapacityResult, ingestAuditProgress) {
	t.Helper()
	var records, rawBytes proto.ColUInt64
	progress := ingestAuditProgress{}
	err := native.executor.Do(ctx, ch.Query{
		Body:   ingestAuditCapacityQuery(""),
		Result: proto.Results{{Name: "count()", Data: &records}, {Name: "sum(tupleElement(latest, 6))", Data: &rawBytes}},
		OnProgress: func(_ context.Context, value proto.Progress) error {
			progress.rows += value.Rows
			progress.bytes += value.Bytes
			return nil
		},
	})
	if err != nil {
		t.Fatalf("run audit capacity query: %v", err)
	}
	if records.Rows() != 1 || rawBytes.Rows() != 1 {
		t.Fatalf("audit result rows=%d/%d", records.Rows(), rawBytes.Rows())
	}
	return ingestAuditCapacityResult{records: records[0], rawBytes: rawBytes[0]}, progress
}

func explainIngestAuditCapacityQuery(t testing.TB, ctx context.Context, native *NativeInserter) string {
	t.Helper()
	var lines proto.ColStr
	err := native.executor.Do(ctx, ch.Query{
		Body:   ingestAuditCapacityQuery("EXPLAIN projections=1, indexes=1 "),
		Result: proto.Results{{Name: "explain", Data: &lines}},
	})
	if err != nil {
		t.Fatalf("explain audit capacity query: %v", err)
	}
	var plan strings.Builder
	for row := 0; row < lines.Rows(); row++ {
		if row > 0 {
			plan.WriteByte('\n')
		}
		plan.WriteString(lines.Row(row))
	}
	return plan.String()
}

func ingestAuditTableBytes(t testing.TB, ctx context.Context, native *NativeInserter) uint64 {
	t.Helper()
	var bytes proto.ColUInt64
	if err := native.executor.Do(ctx, ch.Query{
		Body:   "SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table = 'flow_records'",
		Result: proto.Results{{Name: "sum(bytes_on_disk)", Data: &bytes}},
	}); err != nil {
		t.Fatalf("read audit table bytes: %v", err)
	}
	if bytes.Rows() != 1 {
		t.Fatalf("audit table byte rows=%d", bytes.Rows())
	}
	return bytes[0]
}

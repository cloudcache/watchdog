// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

// This test owns a dedicated database and removes it on exit. It exercises
// the native writer, both rollup resolutions, generation replay/repair, and
// the real aggregate result decoder without touching the development data.
func TestRealClickHouseRollupQueryRepair(t *testing.T) {
	if os.Getenv("WATCHDOG_FLOW_CLICKHOUSE_DATA_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_FLOW_CLICKHOUSE_DATA_INTEGRATION=1 to run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const database = "watchdog_flow_it_rollup_query"
	adminConfig := realMigrationConfig(t, "watchdog-flow-data-integration-admin", 30*time.Second)
	adminConfig.Database = "default"
	admin, err := NewNativeInserter(ctx, adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.executor.Do(ctx, ch.Query{Body: "DROP DATABASE IF EXISTS " + database}); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	defer func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := admin.executor.Do(cleanupContext, ch.Query{Body: "DROP DATABASE IF EXISTS " + database}); err != nil {
			t.Errorf("drop integration database: %v", err)
		}
		admin.Close()
	}()

	migrations, err := LoadMigrations(os.DirFS("../../deploy/migration/clickhouse"), ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		for statementIndex, statement := range migration.Statements {
			isolated := strings.ReplaceAll(statement, migrationDatabase, database)
			if err := admin.executor.Do(ctx, synchronousMigrationQuery(isolated)); err != nil {
				t.Fatalf("apply isolated migration %03d statement %d: %v", migration.Version, statementIndex+1, err)
			}
		}
	}

	dataConfig := realMigrationConfig(t, "watchdog-flow-data-integration", 30*time.Second)
	dataConfig.Database = database
	native, err := NewNativeInserter(ctx, dataConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()

	bucket := time.Date(2026, 9, 5, 10, 20, 0, 0, time.UTC)
	first := integrationRecord(1, bucket.Add(10*time.Second), "geo-city-a", 100)
	second := integrationRecord(2, bucket.Add(20*time.Second), "geo-city-a", 200)
	third := integrationRecord(3, bucket.Add(30*time.Second), "geo-city-b", 50)
	insertIntegrationBatch(t, ctx, native, integrationBatch(10, bucket.Add(2*time.Minute), first, second, third))

	initial := RollupRequest{
		TenantID: "flow-it-tenant", Resolution: RollupOneMinute, Bucket: bucket,
		Generation: 1, GeneratedAt: bucket.Add(3 * time.Minute),
	}
	firstRunner, err := NewRollupRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	if err := firstRunner.Run(ctx, initial); err != nil {
		t.Fatal(err)
	}
	// A new runner represents a process restart after the INSERT succeeded but
	// before the operation-job acknowledgement became durable.
	restartedRunner, err := NewRollupRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	if err := restartedRunner.Run(ctx, initial); err != nil {
		t.Fatal(err)
	}
	assertGeneration(t, ctx, restartedRunner, "flow-it-tenant", RollupOneMinute, bucket, 1)

	multiBlockExecutor := &integrationBlockExecutor{executor: native.executor}
	queryRunner, err := flowquery.NewRunner(multiBlockExecutor)
	if err != nil {
		t.Fatal(err)
	}
	beforeRepair := runIntegrationAggregate(t, ctx, queryRunner, bucket, bucket.Add(time.Minute), flowquery.BucketOneMinute, flowquery.DimensionGeoCity, 1, true)
	if multiBlockExecutor.lastBlocks < 2 {
		t.Fatalf("real ClickHouse query returned %d result block, want multiple blocks", multiBlockExecutor.lastBlocks)
	}
	assertAggregatePoints(t, beforeRepair, map[string]aggregateWant{
		"geo-city-a": {value: 300, records: 2},
		"_other":     {value: 50, records: 1},
	})

	late := integrationRecord(4, bucket.Add(40*time.Second), "geo-city-b", 500)
	insertIntegrationBatch(t, ctx, native, integrationBatch(11, bucket.Add(4*time.Minute), late))
	repair := initial
	repair.Generation = 2
	repair.GeneratedAt = bucket.Add(5 * time.Minute)
	if err := restartedRunner.Run(ctx, repair); err != nil {
		t.Fatal(err)
	}
	if err := restartedRunner.Run(ctx, repair); err != nil {
		t.Fatal(err)
	}
	assertGeneration(t, ctx, restartedRunner, "flow-it-tenant", RollupOneMinute, bucket, 2)
	afterRepair := runIntegrationAggregate(t, ctx, queryRunner, bucket, bucket.Add(time.Minute), flowquery.BucketOneMinute, flowquery.DimensionGeoCity, 1, true)
	assertAggregatePoints(t, afterRepair, map[string]aggregateWant{
		"geo-city-b": {value: 550, records: 2},
		"_other":     {value: 300, records: 2},
	})

	hourBucket := bucket.Truncate(time.Hour)
	hour := RollupRequest{
		TenantID: "flow-it-tenant", Resolution: RollupOneHour, Bucket: hourBucket,
		Generation: 1, GeneratedAt: hourBucket.Add(2 * time.Hour),
	}
	if err := restartedRunner.Run(ctx, hour); err != nil {
		t.Fatal(err)
	}
	hourResult := runIntegrationAggregate(t, ctx, queryRunner, hourBucket, hourBucket.Add(time.Hour), flowquery.BucketOneHour, flowquery.DimensionTotal, 1, false)
	assertAggregatePoints(t, hourResult, map[string]aggregateWant{"total": {value: 850, records: 4}})

	emptyBucket := bucket.Add(time.Minute)
	empty := RollupRequest{
		TenantID: "flow-it-tenant", Resolution: RollupOneMinute, Bucket: emptyBucket,
		Generation: 1, GeneratedAt: emptyBucket.Add(2 * time.Minute),
	}
	if err := restartedRunner.Run(ctx, empty); err != nil {
		t.Fatal(err)
	}
	emptyResult := runIntegrationAggregate(t, ctx, queryRunner, emptyBucket, emptyBucket.Add(time.Minute), flowquery.BucketOneMinute, flowquery.DimensionTotal, 1, false)
	if len(emptyResult.Points) != 0 || !emptyResult.RollupCompleteness.Complete || emptyResult.RollupCompleteness.CoveredBuckets != 1 {
		t.Fatalf("empty closed bucket result=%+v", emptyResult)
	}

	cancelExecutor := &integrationBlockExecutor{executor: native.executor, cancelAfterFirst: true}
	cancelRunner, err := flowquery.NewRunner(cancelExecutor)
	if err != nil {
		t.Fatal(err)
	}
	compiled := compileIntegrationAggregate(t, bucket, bucket.Add(time.Minute), flowquery.BucketOneMinute, flowquery.DimensionGeoCity, 1, true)
	canceledResult, err := cancelRunner.Run(ctx, compiled)
	if err == nil || !errors.Is(err, context.Canceled) || len(canceledResult.Points) != 0 || !cancelExecutor.canceled {
		t.Fatalf("canceled real query result=%+v error=%v canceled=%t", canceledResult, err, cancelExecutor.canceled)
	}
}

func integrationBatch(offset int64, receivedAt time.Time, records ...flowworker.EnrichedRecord) *flowworker.EnrichedBatch {
	batch := testEnrichedBatch(offset, records...)
	batch.TenantID = "flow-it-tenant"
	batch.ReceivedAt = receivedAt
	return batch
}

func integrationRecord(index byte, eventTime time.Time, cityID string, rawBytes uint64) flowworker.EnrichedRecord {
	record := testEnrichedRecord(index, rawBytes, rawBytes*10)
	record.EventTime = eventTime
	record.RemoteGeo.CityID = cityID
	return record
}

func insertIntegrationBatch(t *testing.T, ctx context.Context, native *NativeInserter, batch *flowworker.EnrichedBatch) {
	t.Helper()
	blocks, err := PrepareBlocks([]*flowworker.EnrichedBatch{batch}, BatchLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 {
		t.Fatalf("prepared blocks=%d, want 1", len(blocks))
	}
	if err := native.InsertFlowBlock(ctx, blocks[0]); err != nil {
		t.Fatal(err)
	}
}

func assertGeneration(t *testing.T, ctx context.Context, runner *RollupRunner, tenant string, resolution RollupResolution, bucket time.Time, want uint64) {
	t.Helper()
	generation, err := runner.LatestGeneration(ctx, tenant, resolution, bucket)
	if err != nil {
		t.Fatal(err)
	}
	if generation != want {
		t.Fatalf("generation=%d, want %d", generation, want)
	}
}

func runIntegrationAggregate(t *testing.T, ctx context.Context, runner *flowquery.Runner, from, to time.Time, bucket flowquery.Bucket, dimension flowquery.Dimension, topN uint16, includeOther bool) flowquery.Result {
	t.Helper()
	compiled := compileIntegrationAggregate(t, from, to, bucket, dimension, topN, includeOther)
	result, err := runner.Run(ctx, compiled)
	if err != nil {
		t.Fatal(err)
	}
	if !result.RollupCompleteness.Complete || result.RollupCompleteness.CoveredBuckets != 1 || result.RollupCompleteness.ExpectedBuckets != 1 {
		t.Fatalf("rollup completeness=%+v", result.RollupCompleteness)
	}
	return result
}

func compileIntegrationAggregate(t *testing.T, from, to time.Time, bucket flowquery.Bucket, dimension flowquery.Dimension, topN uint16, includeOther bool) flowquery.Compiled {
	t.Helper()
	compiled, err := flowquery.Compile(flowquery.Scope{TenantID: "flow-it-tenant"}, flowquery.Request{
		From: from, To: to, Bucket: bucket, Metric: flowquery.MetricRawBytes,
		Dimension: dimension, View: flowquery.ViewCustomer, TopN: topN,
		IncludeOther: includeOther, Timezone: "UTC",
	}, to.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

type aggregateWant struct {
	value   float64
	records uint64
}

func assertAggregatePoints(t *testing.T, result flowquery.Result, want map[string]aggregateWant) {
	t.Helper()
	if len(result.Points) != len(want) {
		t.Fatalf("aggregate points=%+v, want=%+v", result.Points, want)
	}
	for _, point := range result.Points {
		expected, exists := want[point.DimensionValue]
		if !exists || point.Value != expected.value || point.ReceivedRecords != expected.records {
			t.Fatalf("aggregate point=%+v, want=%+v", point, expected)
		}
		delete(want, point.DimensionValue)
	}
	if len(want) != 0 {
		t.Fatalf("missing aggregate points=%+v", want)
	}
}

type integrationBlockExecutor struct {
	executor         queryExecutor
	lastBlocks       int
	cancelAfterFirst bool
	canceled         bool
}

func (e *integrationBlockExecutor) Do(ctx context.Context, query ch.Query) error {
	e.lastBlocks = 0
	e.canceled = false
	query.Settings = append(query.Settings, ch.Setting{Key: "max_block_size", Value: "1", Important: true})
	original := query.OnResult
	queryContext, cancel := context.WithCancel(ctx)
	defer cancel()
	query.OnResult = func(resultContext context.Context, block proto.Block) error {
		if err := original(resultContext, block); err != nil {
			return err
		}
		if block.Rows > 0 {
			e.lastBlocks++
			if e.cancelAfterFirst && !e.canceled {
				e.canceled = true
				cancel()
			}
		}
		return nil
	}
	return e.executor.Do(queryContext, query)
}

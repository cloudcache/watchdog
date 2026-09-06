// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"strings"
	"sync"
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
	ctx, native := openDataIntegrationClickHouse(t, "watchdog_flow_it_rollup_query")

	bucket := time.Date(2026, 9, 5, 10, 20, 0, 0, time.UTC)
	first := integrationRecord(1, bucket.Add(10*time.Second), "geo-city-a", 100)
	second := integrationRecord(2, bucket.Add(20*time.Second), "geo-city-a", 200)
	third := integrationRecord(3, bucket.Add(30*time.Second), "geo-city-b", 50)
	first.RemoteASN, second.RemoteASN, third.RemoteASN = 4134, 4134, 4837
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
	late.RemoteASN = 4837
	insertIntegrationBatch(t, ctx, native, integrationBatch(11, bucket.Add(4*time.Minute), late))
	jointRunner, err := flowquery.NewJointRunner(native.executor)
	if err != nil {
		t.Fatal(err)
	}
	jointQuery, err := flowquery.CompileJoint(flowquery.Scope{TenantID: "flow-it-tenant"}, flowquery.JointRequest{
		From: bucket, To: bucket.Add(2 * time.Minute), Metric: flowquery.MetricRawBytes,
		Dimensions: []flowquery.Dimension{flowquery.DimensionGeoCity, flowquery.DimensionASN},
		View:       flowquery.ViewCustomer, TopN: 1, IncludeOther: true, TargetPoints: 300, Timezone: "UTC",
	}, bucket.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	jointResult, err := jointRunner.Run(ctx, jointQuery)
	if err != nil {
		t.Fatal(err)
	}
	if len(jointResult.Points) != 2 || jointResult.Points[0].DimensionValues[0] != "geo-city-b" ||
		jointResult.Points[0].DimensionValues[1] != "4837" || jointResult.Points[0].Value != 550 ||
		!jointResult.Points[1].Other || jointResult.Points[1].Value != 300 {
		t.Fatalf("true joint tuple result=%+v", jointResult)
	}
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

	// Presentation intervals are independent of the source table. This range
	// has two complete 1m source buckets but is shorter than the requested 15m
	// display interval; the final bps point must divide by the actual 120
	// seconds, while completeness still counts both source markers.
	resampled, err := flowquery.Compile(flowquery.Scope{TenantID: "flow-it-tenant"}, flowquery.Request{
		From: bucket, To: emptyBucket.Add(time.Minute), Bucket: flowquery.BucketOneMinute,
		Interval: 15 * time.Minute, Metric: flowquery.MetricRawBitsPerSecond,
		Dimension: flowquery.DimensionTotal, View: flowquery.ViewCustomer, TopN: 1, Timezone: "UTC",
	}, emptyBucket.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	resampledResult, err := queryRunner.Run(ctx, resampled)
	if err != nil {
		t.Fatal(err)
	}
	if len(resampledResult.Points) != 1 || resampledResult.Points[0].Value != float64(850*8)/120 ||
		resampledResult.RollupCompleteness.ExpectedBuckets != 2 || !resampledResult.RollupCompleteness.Complete {
		t.Fatalf("resampled result=%+v", resampledResult)
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

func TestRealClickHouseDetailPaginationAndLimits(t *testing.T) {
	ctx, native := openDataIntegrationClickHouse(t, "watchdog_flow_it_detail")

	eventTime := time.Date(2026, 9, 5, 11, 22, 33, 123_000_000, time.UTC)
	wanted := netip.MustParseAddr("192.0.2.10")
	first := integrationDetailRecord(1, eventTime, wanted, netip.MustParseAddr("2001:db8::1"), 100)
	second := integrationDetailRecord(2, eventTime, netip.MustParseAddr("198.51.100.2"), wanted, 200)
	third := integrationDetailRecord(3, eventTime, wanted, netip.MustParseAddr("2001:db8::3"), 300)
	insertIntegrationBatch(t, ctx, native, integrationBatch(20, eventTime.Add(time.Minute), first, second, third))

	// A later generation for the same record identity must replace the first
	// physical version when detail queries use FINAL.
	replacement := integrationDetailRecord(1, eventTime, wanted, netip.MustParseAddr("2001:db8::1"), 900)
	insertIntegrationBatch(t, ctx, native, integrationBatch(21, eventTime.Add(2*time.Minute), replacement))

	runner, err := flowquery.NewDetailRunner(&integrationBlockExecutor{executor: native.executor})
	if err != nil {
		t.Fatal(err)
	}
	request := integrationDetailRequest(eventTime, wanted, flowquery.DetailEndpointEither, 2)
	firstPage := runIntegrationDetail(t, ctx, runner, request)
	assertDetailPage(t, firstPage, []byte{3, 2}, []uint64{300, 200}, true)
	if firstPage.Rows[0].Values[flowquery.DetailFieldSourceIP] != wanted.String() ||
		firstPage.Rows[1].Values[flowquery.DetailFieldDestinationIP] != wanted.String() {
		t.Fatalf("IPv4-mapped detail values were not normalized: %+v", firstPage.Rows)
	}

	request.Cursor = firstPage.NextCursor
	secondPage := runIntegrationDetail(t, ctx, runner, request)
	assertDetailPage(t, secondPage, []byte{1}, []uint64{900}, false)

	sourceRequest := integrationDetailRequest(eventTime, wanted, flowquery.DetailEndpointSource, 10)
	sourcePage := runIntegrationDetail(t, ctx, runner, sourceRequest)
	assertDetailPage(t, sourcePage, []byte{3, 1}, []uint64{300, 900}, false)
	destinationRequest := integrationDetailRequest(eventTime, wanted, flowquery.DetailEndpointDestination, 10)
	destinationPage := runIntegrationDetail(t, ctx, runner, destinationRequest)
	assertDetailPage(t, destinationPage, []byte{2}, []uint64{200}, false)

	compiled := compileIntegrationDetail(t, integrationDetailRequest(eventTime, wanted, flowquery.DetailEndpointEither, 2))
	cancelExecutor := &integrationBlockExecutor{executor: native.executor, cancelAfterFirst: true}
	cancelRunner, err := flowquery.NewDetailRunner(cancelExecutor)
	if err != nil {
		t.Fatal(err)
	}
	canceledResult, err := cancelRunner.Run(ctx, compiled)
	if err == nil || !errors.Is(err, context.Canceled) || len(canceledResult.Rows) != 0 || !cancelExecutor.canceled {
		t.Fatalf("canceled detail result=%+v error=%v canceled=%t", canceledResult, err, cancelExecutor.canceled)
	}

	expiredContext, expiredCancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer expiredCancel()
	timedResult, err := runner.Run(expiredContext, compiled)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) || len(timedResult.Rows) != 0 {
		t.Fatalf("expired detail result=%+v error=%v", timedResult, err)
	}

	limitedRunner, err := flowquery.NewDetailRunner(&integrationSettingExecutor{
		executor: native.executor, overrides: map[string]string{"max_rows_to_read": "1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	limitedResult, err := limitedRunner.Run(ctx, compiled)
	if err == nil || len(limitedResult.Rows) != 0 {
		t.Fatalf("scan-limited detail result=%+v error=%v", limitedResult, err)
	}
}

func TestRealClickHouseConcurrentAggregateQueriesRemainIsolated(t *testing.T) {
	ctx, writerNative := openDataIntegrationClickHouse(t, "watchdog_flow_it_concurrent_query")
	bucket := time.Date(2026, 9, 6, 4, 0, 0, 0, time.UTC)
	insertIntegrationBatch(t, ctx, writerNative, integrationBatch(80, bucket.Add(time.Minute),
		integrationRecord(20, bucket.Add(10*time.Second), "geo-city-a", 100),
		integrationRecord(21, bucket.Add(20*time.Second), "geo-city-a", 200),
		integrationRecord(22, bucket.Add(30*time.Second), "geo-city-b", 50),
	))
	rollup, err := NewRollupRunner(writerNative)
	if err != nil {
		t.Fatal(err)
	}
	if err := rollup.Run(ctx, RollupRequest{
		TenantID: "flow-it-tenant", Resolution: RollupOneMinute, Bucket: bucket,
		Generation: 1, GeneratedAt: bucket.Add(2 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}

	poolConfig := realMigrationConfig(t, "watchdog-flow-concurrent-query", 30*time.Second)
	poolConfig.Database = "watchdog_flow_it_concurrent_query"
	poolConfig.MaxConns = 4
	poolConfig.MinConns = 1
	pooled, err := NewNativeInserter(ctx, poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer pooled.Close()
	runner, err := flowquery.NewRunner(pooled.executor)
	if err != nil {
		t.Fatal(err)
	}
	cityQuery := compileIntegrationAggregate(t, bucket, bucket.Add(time.Minute), flowquery.BucketOneMinute, flowquery.DimensionGeoCity, 1, true)
	totalQuery := compileIntegrationAggregate(t, bucket, bucket.Add(time.Minute), flowquery.BucketOneMinute, flowquery.DimensionTotal, 1, false)

	const concurrentQueries = 64
	errors := make(chan error, concurrentQueries)
	var wait sync.WaitGroup
	for index := 0; index < concurrentQueries; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			queryContext, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			compiled := cityQuery
			want := map[string]aggregateWant{"geo-city-a": {value: 300, records: 2}, "_other": {value: 50, records: 1}}
			if index%2 == 1 {
				compiled = totalQuery
				want = map[string]aggregateWant{"total": {value: 350, records: 3}}
			}
			result, err := runner.Run(queryContext, compiled)
			if err == nil {
				err = validateConcurrentAggregate(result, want)
			}
			if err != nil {
				errors <- fmt.Errorf("query %d: %w", index, err)
			}
		}()
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}

func validateConcurrentAggregate(result flowquery.Result, want map[string]aggregateWant) error {
	if !result.RollupCompleteness.Complete || result.RollupCompleteness.CoveredBuckets != 1 || result.RollupCompleteness.ExpectedBuckets != 1 {
		return fmt.Errorf("rollup completeness=%+v", result.RollupCompleteness)
	}
	if len(result.Points) != len(want) {
		return fmt.Errorf("points=%+v want=%+v", result.Points, want)
	}
	seen := make(map[string]struct{}, len(result.Points))
	for _, point := range result.Points {
		expected, exists := want[point.DimensionValue]
		if !exists || point.Value != expected.value || point.ReceivedRecords != expected.records {
			return fmt.Errorf("point=%+v want=%+v", point, expected)
		}
		if _, duplicate := seen[point.DimensionValue]; duplicate {
			return fmt.Errorf("duplicate dimension %q", point.DimensionValue)
		}
		seen[point.DimensionValue] = struct{}{}
	}
	return nil
}

func openDataIntegrationClickHouse(t *testing.T, database string) (context.Context, *NativeInserter) {
	t.Helper()
	if os.Getenv("WATCHDOG_FLOW_CLICKHOUSE_DATA_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_FLOW_CLICKHOUSE_DATA_INTEGRATION=1 to run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	adminConfig := realMigrationConfig(t, "watchdog-flow-data-integration-admin", 30*time.Second)
	adminConfig.Database = "default"
	admin, err := NewNativeInserter(ctx, adminConfig)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var native *NativeInserter
	t.Cleanup(func() {
		if native != nil {
			native.Close()
		}
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := admin.executor.Do(cleanupContext, ch.Query{Body: "DROP DATABASE IF EXISTS " + database}); err != nil {
			t.Errorf("drop integration database: %v", err)
		}
		admin.Close()
		cancel()
	})
	if err := admin.executor.Do(ctx, ch.Query{Body: "DROP DATABASE IF EXISTS " + database}); err != nil {
		t.Fatal(err)
	}

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
	native, err = NewNativeInserter(ctx, dataConfig)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, native
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

func integrationDetailRecord(index byte, eventTime time.Time, source, destination netip.Addr, rawBytes uint64) flowworker.EnrichedRecord {
	record := testEnrichedRecord(index, rawBytes, rawBytes*10)
	record.EventTime = eventTime
	record.SourceIP = source
	record.DestinationIP = destination
	record.SourcePort = uint16(10_000) + uint16(index)
	record.DestinationPort = 443
	record.IPProtocol = 6
	return record
}

func integrationDetailRequest(eventTime time.Time, ip netip.Addr, endpoint flowquery.DetailEndpoint, limit uint16) flowquery.DetailRequest {
	return flowquery.DetailRequest{
		IP: ip.String(), Endpoint: endpoint,
		From: eventTime.Add(-time.Minute), To: eventTime.Add(time.Minute),
		View: flowquery.ViewCustomer, Limit: limit,
		Fields: []flowquery.DetailField{
			flowquery.DetailFieldSourceIP,
			flowquery.DetailFieldDestinationIP,
			flowquery.DetailFieldRawBytes,
		},
	}
}

func compileIntegrationDetail(t *testing.T, request flowquery.DetailRequest) flowquery.CompiledDetail {
	t.Helper()
	compiled, err := flowquery.CompileDetail(flowquery.Scope{TenantID: "flow-it-tenant"}, request, request.To.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func runIntegrationDetail(t *testing.T, ctx context.Context, runner *flowquery.DetailRunner, request flowquery.DetailRequest) flowquery.DetailResult {
	t.Helper()
	result, err := runner.Run(ctx, compileIntegrationDetail(t, request))
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func assertDetailPage(t *testing.T, result flowquery.DetailResult, ids []byte, rawBytes []uint64, hasMore bool) {
	t.Helper()
	if len(result.Rows) != len(ids) || len(ids) != len(rawBytes) || result.HasMore != hasMore || (hasMore && result.NextCursor == "") || (!hasMore && result.NextCursor != "") {
		t.Fatalf("detail page=%+v, ids=%v raw=%v has_more=%t", result, ids, rawBytes, hasMore)
	}
	for index, row := range result.Rows {
		wantID := fmt.Sprintf("%064x", ids[index])
		if row.RecordID != wantID || row.Values[flowquery.DetailFieldRawBytes] != rawBytes[index] {
			t.Fatalf("detail row[%d]=%+v, want id=%s raw=%d", index, row, wantID, rawBytes[index])
		}
		for _, field := range []flowquery.DetailField{flowquery.DetailFieldSourceIP, flowquery.DetailFieldDestinationIP} {
			if _, err := netip.ParseAddr(row.Values[field].(string)); err != nil {
				t.Fatalf("detail row[%d] field %s=%v: %v", index, field, row.Values[field], err)
			}
		}
	}
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

type integrationSettingExecutor struct {
	executor  queryExecutor
	overrides map[string]string
}

func (e *integrationSettingExecutor) Do(ctx context.Context, query ch.Query) error {
	query.Settings = append([]ch.Setting(nil), query.Settings...)
	for index := range query.Settings {
		if value, exists := e.overrides[query.Settings[index].Key]; exists {
			query.Settings[index].Value = value
		}
	}
	return e.executor.Do(ctx, query)
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

// TestRealClickHouseRollupCapsPerIPDimension proves the rollup folds the per-IP
// long tail beyond top-N (by traffic) into a single _other bucket, so src_ip /
// dst_ip do not materialize at ~raw cardinality (F6).
func TestRealClickHouseRollupCapsPerIPDimension(t *testing.T) {
	ctx, native := openDataIntegrationClickHouse(t, "watchdog_flow_it_rollup_topn")

	// Cap at top-2 so a four-IP fixture exercises the fold.
	saved := rollupIPTopN
	rollupIPTopN = 2
	t.Cleanup(func() { rollupIPTopN = saved })

	bucket := time.Date(2026, 9, 5, 11, 0, 0, 0, time.UTC)
	records := make([]flowworker.EnrichedRecord, 0, 4)
	for i, rawBytes := range []uint64{40, 30, 20, 10} {
		record := integrationRecord(byte(i+1), bucket.Add(time.Duration(i+1)*time.Second), "", rawBytes)
		record.SourceIP = netip.MustParseAddr(fmt.Sprintf("10.9.0.%d", i+1))
		records = append(records, record)
	}
	insertIntegrationBatch(t, ctx, native, integrationBatch(20, bucket.Add(2*time.Minute), records...))

	runner, err := NewRollupRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Run(ctx, RollupRequest{
		TenantID: "flow-it-tenant", Resolution: RollupOneMinute, Bucket: bucket,
		Generation: 1, GeneratedAt: bucket.Add(3 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}

	queryRunner, err := flowquery.NewRunner(native.executor)
	if err != nil {
		t.Fatal(err)
	}
	// A high query top-N returns the aggregate rows unfolded, so we observe the
	// rollup's own capping: the two heaviest IPs stay, 10.9.0.3 (200) + 10.9.0.4
	// (100) fold into _other.
	result := runIntegrationAggregate(t, ctx, queryRunner, bucket, bucket.Add(time.Minute), flowquery.BucketOneMinute, flowquery.DimensionSourceIP, 100, false)
	assertAggregatePoints(t, result, map[string]aggregateWant{
		"::ffff:10.9.0.1": {value: 40, records: 1},
		"::ffff:10.9.0.2": {value: 30, records: 1},
		"_other":          {value: 30, records: 2},
	})
}

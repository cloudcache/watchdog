// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowstream"
)

type fakeFlowReconciliationOffsets struct {
	items []flowstream.CommittedPartitionOffset
	err   error
	calls int
}

func (f *fakeFlowReconciliationOffsets) CommittedOffsets(context.Context, string, string) ([]flowstream.CommittedPartitionOffset, error) {
	f.calls++
	return append([]flowstream.CommittedPartitionOffset(nil), f.items...), f.err
}

type fakeFlowReconciliationScanner struct {
	requests []flowch.ReconciliationScanRequest
	noMove   bool
}

func (f *fakeFlowReconciliationScanner) Scan(_ context.Context, request flowch.ReconciliationScanRequest) (flowch.ReconciliationScanResult, error) {
	f.requests = append(f.requests, request)
	next := request.CloseOffset
	complete := true
	if f.noMove {
		next, complete = request.Cursor.NextOffset, false
	} else if request.CloseOffset-request.Cursor.NextOffset > 3 {
		next, complete = request.Cursor.NextOffset+3, false
	}
	comparison := flowch.IngestReconciliationComparison{Batches: next - request.Cursor.NextOffset, Facts: 7}
	if len(f.requests) == 1 {
		comparison.Mismatches = []flowch.IngestReconciliationMismatch{{Reason: flowch.MismatchCount}}
	}
	return flowch.ReconciliationScanResult{Comparison: comparison,
		NextCursor: flowch.ReconciliationScanCursor{SourceStreamID: request.Cursor.SourceStreamID, KafkaTopic: request.Cursor.KafkaTopic,
			KafkaPartition: request.Cursor.KafkaPartition, NextOffset: next}, Complete: complete}, nil
}

type fakeFlowReconciliationWatermarks struct {
	values   map[string]uint64
	known    map[string]bool
	advances map[string]uint64
	err      error
}

func newFakeFlowReconciliationWatermarks() *fakeFlowReconciliationWatermarks {
	return &fakeFlowReconciliationWatermarks{values: map[string]uint64{}, known: map[string]bool{}, advances: map[string]uint64{}}
}

func (f *fakeFlowReconciliationWatermarks) LookupSystemOperationJobWatermark(_ context.Context, _, key string) (uint64, bool, error) {
	return f.values[key], f.known[key], f.err
}

func (f *fakeFlowReconciliationWatermarks) AdvanceSystemOperationJobWatermark(_ context.Context, _, key string, value uint64) error {
	if f.err != nil {
		return f.err
	}
	if value > f.advances[key] {
		f.advances[key] = value
	}
	return nil
}

type fakeFlowReconciliationMetrics struct {
	begins     int
	publishes  int
	mismatches map[flowch.ReconciliationMismatchReason]uint64
}

func (f *fakeFlowReconciliationMetrics) Begin() { f.begins++ }
func (f *fakeFlowReconciliationMetrics) PublishComplete(_ time.Time, mismatches map[flowch.ReconciliationMismatchReason]uint64) {
	f.publishes++
	f.mismatches = mismatches
}

func flowReconciliationTestConfig() flowReconciliationJobConfig {
	return flowReconciliationJobConfig{SourceStreamID: "cluster-a:raw-v1:incarnation-1", KafkaTopic: "watchdog.flow.raw-v1",
		ConsumerGroup: "watchdog-flow-worker-v1", BootstrapOffset: map[int32]uint64{0: 0, 1: 2},
		MaxBatches: 3, MaxFactRows: 100, MaxReadBytes: 1 << 20}
}

func flowReconciliationTestJob(t *testing.T, payload flowReconciliationJobPayload) OperationJob {
	t.Helper()
	checkpoint, err := EncodeJobPayload(FlowReconciliationPayloadVersion, payload)
	if err != nil {
		t.Fatal(err)
	}
	return OperationJob{ID: "job-a", ScopeType: OperationJobScopeSystem, JobType: FlowReconciliationJobType, CheckpointJSON: checkpoint}
}

func TestFlowReconciliationJobFreezesBrokerOffsetsCheckpointsAndPublishesCompleteSnapshot(t *testing.T) {
	config := flowReconciliationTestConfig()
	offsets := &fakeFlowReconciliationOffsets{items: []flowstream.CommittedPartitionOffset{{Partition: 0, NextOffset: 5}, {Partition: 1, NextOffset: 2}}}
	scanner := &fakeFlowReconciliationScanner{}
	watermarks := newFakeFlowReconciliationWatermarks()
	metrics := &fakeFlowReconciliationMetrics{}
	repo := &capturingHeartbeatRepo{}
	reporter := &OperationJobReporter{repo: repo, jobID: "job-a", leaseToken: "lease", leaseFor: time.Minute}
	ctx := context.WithValue(context.Background(), operationJobReporterKey{}, reporter)

	result, err := NewFlowReconciliationJobHandler(offsets, scanner, watermarks, metrics)(ctx,
		flowReconciliationTestJob(t, flowReconciliationJobPayload{Config: config}))
	if err != nil {
		t.Fatal(err)
	}
	if result == "" || offsets.calls != 1 || len(scanner.requests) != 2 || metrics.begins != 1 || metrics.publishes != 1 || metrics.mismatches[flowch.MismatchCount] != 1 {
		t.Fatalf("result=%q offset_calls=%d scans=%d metrics=%+v", result, offsets.calls, len(scanner.requests), metrics)
	}
	for _, partition := range []int32{0, 1} {
		key := flowReconciliationWatermarkKey(config, partition)
		if watermarks.advances[key] != map[int32]uint64{0: 5, 1: 2}[partition] {
			t.Fatalf("partition %d watermark = %d", partition, watermarks.advances[key])
		}
	}
	repo.mu.Lock()
	checkpoint, progress := append(json.RawMessage(nil), repo.checkpoint...), repo.progress
	repo.mu.Unlock()
	var persisted flowReconciliationJobPayload
	if err := DecodeJobPayload(checkpoint, FlowReconciliationPayloadVersion, &persisted); err != nil {
		t.Fatal(err)
	}
	if progress != 5 || !persisted.Frozen || len(persisted.Partitions) != 2 || !persisted.Partitions[0].Complete || !persisted.Partitions[1].Complete {
		t.Fatalf("progress=%d checkpoint=%+v", progress, persisted)
	}
}

func TestFlowReconciliationJobRequiresExplicitBootstrapAndRejectsRegressedCommit(t *testing.T) {
	config := flowReconciliationTestConfig()
	delete(config.BootstrapOffset, 0)
	for name, setup := range map[string]func(*fakeFlowReconciliationWatermarks){
		"missing bootstrap": func(*fakeFlowReconciliationWatermarks) {},
		"regressed commit": func(w *fakeFlowReconciliationWatermarks) {
			key := flowReconciliationWatermarkKey(config, 0)
			w.known[key], w.values[key] = true, 11
		},
	} {
		t.Run(name, func(t *testing.T) {
			watermarks := newFakeFlowReconciliationWatermarks()
			setup(watermarks)
			_, err := NewFlowReconciliationJobHandler(
				&fakeFlowReconciliationOffsets{items: []flowstream.CommittedPartitionOffset{{Partition: 0, NextOffset: 10}}},
				&fakeFlowReconciliationScanner{}, watermarks, &fakeFlowReconciliationMetrics{},
			)(context.Background(), flowReconciliationTestJob(t, flowReconciliationJobPayload{Config: config}))
			if err == nil || !IsTerminalJobError(err) {
				t.Fatalf("error = %v, want terminal", err)
			}
		})
	}
}

func TestFlowReconciliationJobResumeAdvancesCompletedCheckpointWithoutRefetch(t *testing.T) {
	config := flowReconciliationTestConfig()
	payload := flowReconciliationJobPayload{Config: config, Frozen: true, Partitions: []flowReconciliationPartitionCheckpoint{
		{Partition: 0, Start: 2, Close: 9, Next: 9, Batches: 7, Complete: true},
	}}
	offsets := &fakeFlowReconciliationOffsets{err: errors.New("must not fetch")}
	scanner := &fakeFlowReconciliationScanner{}
	watermarks := newFakeFlowReconciliationWatermarks()
	metrics := &fakeFlowReconciliationMetrics{}
	if _, err := NewFlowReconciliationJobHandler(offsets, scanner, watermarks, metrics)(context.Background(), flowReconciliationTestJob(t, payload)); err != nil {
		t.Fatal(err)
	}
	if offsets.calls != 0 || len(scanner.requests) != 0 || watermarks.advances[flowReconciliationWatermarkKey(config, 0)] != 9 || metrics.publishes != 1 {
		t.Fatalf("offset_calls=%d scans=%d advances=%v metrics=%+v", offsets.calls, len(scanner.requests), watermarks.advances, metrics)
	}
}

func TestFlowReconciliationJobRejectsNonAdvancingScanner(t *testing.T) {
	config := flowReconciliationTestConfig()
	_, err := NewFlowReconciliationJobHandler(
		&fakeFlowReconciliationOffsets{items: []flowstream.CommittedPartitionOffset{{Partition: 0, NextOffset: 1}}},
		&fakeFlowReconciliationScanner{noMove: true}, newFakeFlowReconciliationWatermarks(), &fakeFlowReconciliationMetrics{},
	)(context.Background(), flowReconciliationTestJob(t, flowReconciliationJobPayload{Config: config}))
	if err == nil || !IsTerminalJobError(err) {
		t.Fatalf("error = %v, want terminal", err)
	}
}

func TestMySQLFlowReconciliationJobLeaseCheckpointAndWatermark(t *testing.T) {
	db, _ := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	config := flowReconciliationTestConfig()
	config.SourceStreamID = "cluster-a:reconcile:mysql-it"
	checkpoint, err := encodeFlowReconciliationJobPayload(config)
	if err != nil {
		t.Fatal(err)
	}
	job, err := store.EnqueueOperationJob(ctx, OperationJob{ScopeType: OperationJobScopeSystem,
		JobType: FlowReconciliationJobType, IdempotencyKey: "mysql-it-" + strings.ToLower(t.Name()),
		RequestHash: strings.Repeat("a", 64), CheckpointJSON: checkpoint})
	if err != nil {
		t.Fatal(err)
	}
	key := flowReconciliationWatermarkKey(config, 0)
	_, _ = db.ExecContext(ctx, `DELETE FROM operation_job_system_watermarks WHERE job_type = ? AND partition_key = ?`, FlowReconciliationJobType, key)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM operation_jobs WHERE id = ?`, job.ID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM operation_job_system_watermarks WHERE job_type = ? AND partition_key = ?`, FlowReconciliationJobType, key)
	})
	leased, err := store.LeaseNextOperationJob(ctx, FlowReconciliationJobType, "mysql-reconcile-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	worker := OperationJobWorker{Repo: store, JobType: FlowReconciliationJobType, Owner: "mysql-reconcile-test",
		Handler: NewFlowReconciliationJobHandler(
			&fakeFlowReconciliationOffsets{items: []flowstream.CommittedPartitionOffset{{Partition: 0, NextOffset: 2}}},
			&fakeFlowReconciliationScanner{}, store, &fakeFlowReconciliationMetrics{}),
		LeaseFor: time.Minute, MaxAttempts: 1,
	}
	worker.runAttempt(ctx, leased)
	finished, err := store.GetSystemOperationJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finished.Status != OperationJobStatusSucceeded || finished.ProgressDone != 2 {
		t.Fatalf("finished job = %#v", finished)
	}
	value, known, err := store.LookupSystemOperationJobWatermark(ctx, FlowReconciliationJobType, key)
	if err != nil || !known || value != 2 {
		t.Fatalf("watermark value=%d known=%t err=%v", value, known, err)
	}
}

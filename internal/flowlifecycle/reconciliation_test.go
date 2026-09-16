package flowlifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/cloudcache/watchdog/internal/opjob"
)

type reconciliationOffsetStub struct {
	offsets []flowstream.CommittedPartitionOffset
	err     error
}

func (stub reconciliationOffsetStub) CommittedOffsets(context.Context, string, string) ([]flowstream.CommittedPartitionOffset, error) {
	return stub.offsets, stub.err
}

type reconciliationScannerStub struct {
	results []flowch.ReconciliationScanResult
	err     error
	calls   int
}

func (stub *reconciliationScannerStub) Scan(_ context.Context, request flowch.ReconciliationScanRequest) (flowch.ReconciliationScanResult, error) {
	if stub.err != nil {
		return flowch.ReconciliationScanResult{}, stub.err
	}
	if stub.calls >= len(stub.results) {
		return flowch.ReconciliationScanResult{}, errors.New("unexpected reconciliation scan")
	}
	result := stub.results[stub.calls]
	stub.calls++
	result.NextCursor.SourceStreamID = request.Cursor.SourceStreamID
	result.NextCursor.KafkaTopic = request.Cursor.KafkaTopic
	result.NextCursor.KafkaPartition = request.Cursor.KafkaPartition
	return result, nil
}

type reconciliationWatermarkStub struct {
	watermark Watermark
	freezeErr error
	complete  []reconciliationCompletion
}

type reconciliationCompletion struct {
	partition       uint32
	frozenCommitted uint64
	verifiedNext    uint64
	mismatchCount   uint64
}

func (stub *reconciliationWatermarkStub) FreezeReconciliation(_ context.Context, _ ReconciliationConfig,
	partition uint32, bootstrap *uint64, committed uint64, _ time.Time) (Watermark, error) {
	if stub.freezeErr != nil {
		return Watermark{}, stub.freezeErr
	}
	watermark := stub.watermark
	watermark.KafkaPartition = partition
	watermark.CommittedNextOffset = committed
	if watermark.SourceStreamID == "" && bootstrap != nil {
		watermark.ReconciledNextOffset = *bootstrap
	}
	return watermark, nil
}

func (stub *reconciliationWatermarkStub) CompleteReconciliation(_ context.Context, _ ReconciliationConfig,
	partition uint32, frozenCommitted, verifiedNext, mismatchCount uint64, _ time.Time) error {
	stub.complete = append(stub.complete, reconciliationCompletion{partition, frozenCommitted, verifiedNext, mismatchCount})
	return nil
}

func testReconciliationConfig() ReconciliationConfig {
	return ReconciliationConfig{
		SourceStreamID: "site-a:raw-v2:boot-1", KafkaTopic: "watchdog.flow.raw", ConsumerGroup: "watchdog-flow-worker",
		BootstrapOffset: map[uint32]uint64{0: 10}, MaxBatches: 100, MaxFactRows: 10_000, MaxReadBytes: 1 << 20,
	}
}

func reconciliationJob(t *testing.T, config ReconciliationConfig) opjob.Job {
	t.Helper()
	payload, err := EncodeReconciliationPayload(config)
	if err != nil {
		t.Fatal(err)
	}
	return opjob.Job{ID: "job-a", JobType: ReconciliationJobType, CheckpointJSON: payload}
}

func TestReconciliationHandlerAdvancesOnlyContiguousCleanOffsets(t *testing.T) {
	config := testReconciliationConfig()
	offsets := reconciliationOffsetStub{offsets: []flowstream.CommittedPartitionOffset{{Partition: 0, NextOffset: 20}}}
	scanner := &reconciliationScannerStub{results: []flowch.ReconciliationScanResult{
		{NextCursor: flowch.ReconciliationScanCursor{NextOffset: 15}, Comparison: flowch.IngestReconciliationComparison{Batches: 5, Facts: 8}},
		{NextCursor: flowch.ReconciliationScanCursor{NextOffset: 20}, Complete: true, Comparison: flowch.IngestReconciliationComparison{Batches: 5, Facts: 9}},
	}}
	watermarks := &reconciliationWatermarkStub{watermark: Watermark{SourceStreamID: config.SourceStreamID, ReconciledNextOffset: 10}}
	result, err := NewReconciliationHandler(offsets, scanner, watermarks)(context.Background(), reconciliationJob(t, config))
	if err != nil {
		t.Fatal(err)
	}
	if result == "" || scanner.calls != 2 || len(watermarks.complete) != 1 {
		t.Fatalf("result=%q calls=%d completions=%#v", result, scanner.calls, watermarks.complete)
	}
	completion := watermarks.complete[0]
	if completion.verifiedNext != 20 || completion.frozenCommitted != 20 || completion.mismatchCount != 0 {
		t.Fatalf("completion=%#v", completion)
	}
}

func TestReconciliationHandlerStopsWatermarkAtFirstMismatch(t *testing.T) {
	config := testReconciliationConfig()
	offsets := reconciliationOffsetStub{offsets: []flowstream.CommittedPartitionOffset{{Partition: 0, NextOffset: 20}}}
	firstMismatch := flowch.IngestReconciliationMismatch{
		Reason:           flowch.MismatchCounter,
		SourceMessageKey: flowch.SourceMessageKey{SourceStreamID: config.SourceStreamID, KafkaPartition: 0, KafkaOffset: 13},
	}
	laterMismatch := firstMismatch
	laterMismatch.Reason = flowch.MismatchMissingReceipt
	laterMismatch.KafkaOffset = 18
	scanner := &reconciliationScannerStub{results: []flowch.ReconciliationScanResult{{
		NextCursor: flowch.ReconciliationScanCursor{NextOffset: 20}, Complete: true,
		Comparison: flowch.IngestReconciliationComparison{Batches: 10, Facts: 12, Mismatches: []flowch.IngestReconciliationMismatch{laterMismatch, firstMismatch}},
	}}}
	watermarks := &reconciliationWatermarkStub{watermark: Watermark{SourceStreamID: config.SourceStreamID, ReconciledNextOffset: 10}}
	if _, err := NewReconciliationHandler(offsets, scanner, watermarks)(context.Background(), reconciliationJob(t, config)); err != nil {
		t.Fatal(err)
	}
	if got := watermarks.complete[0]; got.verifiedNext != 13 || got.mismatchCount != 2 {
		t.Fatalf("completion=%#v", got)
	}
}

func TestReconciliationHandlerRejectsMissingBootstrapAndNonAdvancingScan(t *testing.T) {
	config := testReconciliationConfig()
	job := reconciliationJob(t, config)
	offsets := reconciliationOffsetStub{offsets: []flowstream.CommittedPartitionOffset{{Partition: 0, NextOffset: 20}}}
	watermarks := &reconciliationWatermarkStub{freezeErr: ErrBootstrapRequired}
	if _, err := NewReconciliationHandler(offsets, &reconciliationScannerStub{}, watermarks)(context.Background(), job); !opjob.IsTerminalError(err) || !errors.Is(err, ErrBootstrapRequired) {
		t.Fatalf("missing bootstrap error=%v", err)
	}

	watermarks.freezeErr = nil
	watermarks.watermark = Watermark{SourceStreamID: config.SourceStreamID, ReconciledNextOffset: 10}
	scanner := &reconciliationScannerStub{results: []flowch.ReconciliationScanResult{{NextCursor: flowch.ReconciliationScanCursor{NextOffset: 10}}}}
	if _, err := NewReconciliationHandler(offsets, scanner, watermarks)(context.Background(), job); !opjob.IsTerminalError(err) {
		t.Fatalf("non-advancing scan error=%v", err)
	}
}

func TestValidateFrozenReconciliationRejectsBadCheckpoint(t *testing.T) {
	payload := reconciliationPayload{Frozen: true, Config: testReconciliationConfig(), Partitions: []reconciliationPartition{
		{Partition: 1, Start: 10, Next: 11, Close: 12},
		{Partition: 1, Start: 10, Next: 12, Close: 12, Complete: true},
	}}
	if err := validateFrozenReconciliation(payload); err == nil {
		t.Fatal("duplicate partition checkpoint accepted")
	}
}

package flowcollect

import (
	"context"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAttemptStoreRestoresGenerationAndFiltersDurableWALAcknowledgement(t *testing.T) {
	wal, err := OpenWAL(t.TempDir(), "collector-a", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	record, err := wal.Append(testAttemptWALInput(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	store, err := OpenAttemptStore(dir, "collector-a", testAttemptConfig(), wal, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Advance(context.Background(), record.DatagramID, 1); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	restored, err := OpenAttemptStore(dir, "collector-a", testAttemptConfig(), wal, nil)
	if err != nil {
		t.Fatal(err)
	}
	if generation, err := restored.Generation(record.DatagramID); err != nil || generation != 1 {
		t.Fatalf("restored generation=%d err=%v, want 1", generation, err)
	}
	if err := restored.Advance(context.Background(), record.DatagramID, 2); err != nil {
		t.Fatal(err)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Acknowledge(record.DatagramID); err != nil {
		t.Fatal(err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}

	filtered, err := OpenAttemptStore(dir, "collector-a", testAttemptConfig(), wal, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer filtered.Close()
	if filtered.RestoredCount() != 0 {
		t.Fatalf("durably acknowledged attempt state was restored: %+v", filtered.State())
	}
	info, err := os.Stat(filepath.Join(dir, "attempt.journal"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != attemptHeaderSize {
		t.Fatalf("filtered journal size=%d, want header only", info.Size())
	}
}

func TestAttemptStoreTruncatesTornTailButRejectsCompleteCorruption(t *testing.T) {
	wal, err := OpenWAL(t.TempDir(), "collector-a", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	record, err := wal.Append(testAttemptWALInput(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	store, err := OpenAttemptStore(dir, "collector-a", testAttemptConfig(), wal, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Advance(context.Background(), record.DatagramID, 1); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "attempt.journal")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	recovered, err := OpenAttemptStore(dir, "collector-a", testAttemptConfig(), wal, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != attemptHeaderSize+attemptRecordSize {
		t.Fatalf("torn tail was not truncated: size=%d", info.Size())
	}
	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, attemptHeaderSize+4); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if _, err := OpenAttemptStore(dir, "collector-a", testAttemptConfig(), wal, nil); err == nil {
		t.Fatal("complete corrupt attempt record was accepted")
	}
}

func TestAttemptStoreCompactsDurablyAcknowledgedRecordsAtHardLimit(t *testing.T) {
	wal, err := OpenWAL(t.TempDir(), "collector-a", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	first, err := wal.Append(testAttemptWALInput(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	config := testAttemptConfig()
	config.AttemptJournalMaxBytes = attemptHeaderSize + attemptRecordSize
	store, err := OpenAttemptStore(t.TempDir(), "collector-a", config, wal, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Advance(context.Background(), first.DatagramID, 1); err != nil {
		t.Fatal(err)
	}
	if err := wal.Acknowledge(first.DatagramID); err != nil {
		t.Fatal(err)
	}
	second, err := wal.Append(testAttemptWALInput(time.Now().Add(time.Millisecond)))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Advance(context.Background(), second.DatagramID, 1); err != nil {
		t.Fatal(err)
	}
	if generation, err := store.Generation(first.DatagramID); err != nil || generation != 0 {
		t.Fatalf("acknowledged generation survived compaction: generation=%d err=%v", generation, err)
	}
	if generation, err := store.Generation(second.DatagramID); err != nil || generation != 1 {
		t.Fatalf("new generation was not appended: generation=%d err=%v", generation, err)
	}
}

func TestDecodeLoopUsesRestoredAttemptGenerationForDLQThreshold(t *testing.T) {
	now := time.Now()
	plan := validPlan(now)
	plan.Sources = []SourceBinding{{Protocol: ProtocolNetFlow5, SourcePrefix: "192.0.2.1/32", TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", SamplingMode: SamplingModeSampled, Enabled: true}}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := OpenWAL(t.TempDir(), plan.CollectorID, testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	record, err := wal.Append(WALInput{Protocol: ProtocolNetFlow5, ReceivedAt: now, Source: netip.MustParseAddrPort("192.0.2.1:2055"), RegistryVersion: plan.Revision, TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", Payload: []byte{0, 5}})
	if err != nil {
		t.Fatal(err)
	}
	attemptDir := t.TempDir()
	attempts, err := OpenAttemptStore(attemptDir, plan.CollectorID, testAttemptConfig(), wal, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := attempts.Advance(context.Background(), record.DatagramID, 1); err != nil {
		t.Fatal(err)
	}
	if err := attempts.Close(); err != nil {
		t.Fatal(err)
	}
	attempts, err = OpenAttemptStore(attemptDir, plan.CollectorID, testAttemptConfig(), wal, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer attempts.Close()
	decoder, _ := NewDecoder()
	defer decoder.Close()
	stateStore, err := OpenCollectStateStore(t.TempDir(), plan.CollectorID, registry, decoder)
	if err != nil {
		t.Fatal(err)
	}
	publisher := &recordingPublisher{dlqPublished: make(chan struct{}, 1)}
	runner := &Runner{
		Config:   Config{NormalizedBatch: NormalizedBatchCfg{MaxRecords: 100, MaxBytes: 1 << 20, MaxWait: time.Millisecond}, Diagnostics: DiagnosticsConfig{DecodeMaxAttempts: 2, RetryInitial: time.Millisecond, RetryMax: time.Millisecond}},
		Registry: registry, WAL: wal, Decoder: decoder, State: stateStore, Attempts: attempts, Publisher: publisher,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	queue := make(chan WALRecord, 1)
	go func() {
		defer close(done)
		runner.decodeLoop(ctx, queue)
	}()
	queue <- record
	select {
	case <-publisher.dlqPublished:
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("restored attempt did not reach the DLQ threshold")
	}
	cancel()
	<-done
	if len(publisher.decodeFailures) != 1 || publisher.decodeFailures[0].Attempts != 2 {
		t.Fatalf("unexpected restored-attempt DLQ: %+v", publisher.decodeFailures)
	}
	if generation, err := attempts.Generation(record.DatagramID); err != nil || generation != 2 {
		t.Fatalf("terminal generation=%d err=%v, want 2 until durable ACK compaction", generation, err)
	}
}

func testAttemptConfig() DiagnosticsConfig {
	config := DefaultConfig().Diagnostics
	config.AttemptJournalFsync = time.Millisecond
	config.AttemptCheckpointEvery = time.Hour
	config.AttemptJournalMaxBytes = 1 << 20
	return config
}

func testAttemptWALInput(receivedAt time.Time) WALInput {
	return WALInput{
		Protocol: ProtocolNetFlow5, ReceivedAt: receivedAt, Source: netip.MustParseAddrPort("192.0.2.1:2055"),
		RegistryVersion: 1, TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", Payload: []byte{0, 5},
	}
}

func TestAttemptStoreRejectsGenerationSkips(t *testing.T) {
	wal, err := OpenWAL(t.TempDir(), "collector-a", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	record, err := wal.Append(testAttemptWALInput(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenAttemptStore(t.TempDir(), "collector-a", testAttemptConfig(), wal, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Advance(context.Background(), record.DatagramID, 2); err == nil {
		t.Fatal("attempt generation skip was accepted")
	}
	if err := store.Advance(context.Background(), record.DatagramID, 1); err != nil {
		t.Fatal(err)
	}
	if err := store.Advance(context.Background(), record.DatagramID, 0); err == nil {
		t.Fatal("zero attempt generation was accepted")
	}
}

package flowcollect

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
)

func TestQualityStateStoreRestoresDecisionAndSequenceState(t *testing.T) {
	walDir, qualityDir := t.TempDir(), t.TempDir()
	config := testQualityStateConfig()
	w, err := OpenWAL(walDir, "collector-a", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	record := appendQualityWALRecord(t, w, 1, time.Unix(1000, 0))
	metrics := &Metrics{}
	tracker := NewQualityTracker(config, metrics)
	store, err := OpenQualityStateStore(qualityDir, "collector-a", config, tracker, w, metrics)
	if err != nil {
		t.Fatal(err)
	}
	decoded, fresh := tracker.Observe(record, sflowQualityDatagram(10, 1000, 20, 1000, 100, 0))
	if !fresh {
		t.Fatal("first quality observation was unexpectedly cached")
	}
	journal, err := tracker.BuildJournalRecord(record, decoded, testQualityBinding(record), "collector-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendAndWait(context.Background(), journal); err != nil {
		t.Fatal(err)
	}
	tracker.Remember(record.DatagramID, decoded)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w, err = OpenWAL(walDir, "collector-a", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	restored := NewQualityTracker(config, metrics)
	store, err = OpenQualityStateStore(qualityDir, "collector-a", config, restored, w, metrics)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	retry, retryFresh := restored.Observe(record, sflowQualityDatagram(10, 1000, 20, 1000, 100, 0))
	if retryFresh || retry.ExporterEpoch != decoded.ExporterEpoch || retry.Records[0].QualityEpoch != decoded.Records[0].QualityEpoch {
		t.Fatalf("pending decision was not restored: fresh=%v retry=%+v", retryFresh, retry)
	}
	nextRecord := appendQualityWALRecord(t, w, 2, time.Unix(1001, 0))
	next, nextFresh := restored.Observe(nextRecord, sflowQualityDatagram(11, 2000, 21, 1100, 100, 0))
	if !nextFresh || next.Records[0].QualityFlags != 0 || next.Records[0].QualityEpoch != decoded.Records[0].QualityEpoch {
		t.Fatalf("restored sequence state did not continue: fresh=%v record=%+v", nextFresh, next.Records[0])
	}
}

func TestQualityStateStoreAppendIsIdempotentPerDatagram(t *testing.T) {
	w, err := OpenWAL(t.TempDir(), "collector-a", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	config := testQualityStateConfig()
	metrics := &Metrics{}
	tracker := NewQualityTracker(config, metrics)
	store, err := OpenQualityStateStore(t.TempDir(), "collector-a", config, tracker, w, metrics)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	record := appendQualityWALRecord(t, w, 1, time.Unix(2000, 0))
	decoded, _ := tracker.Observe(record, sflowQualityDatagram(10, 1000, 20, 1000, 100, 0))
	journal, err := tracker.BuildJournalRecord(record, decoded, testQualityBinding(record), "collector-a")
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		if err := store.AppendAndWait(context.Background(), journal); err != nil {
			t.Fatal(err)
		}
	}
	if got := metrics.Snapshot().QualityJournalAppends; got != 1 {
		t.Fatalf("quality journal appends=%d, want 1", got)
	}
}

func TestQualityStateStoreCompactsAndRestoresSnapshot(t *testing.T) {
	w, err := OpenWAL(t.TempDir(), "collector-a", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	qualityDir := t.TempDir()
	config := testQualityStateConfig()
	tracker := NewQualityTracker(config, &Metrics{})
	store, err := OpenQualityStateStore(qualityDir, "collector-a", config, tracker, w, nil)
	if err != nil {
		t.Fatal(err)
	}
	record := appendQualityWALRecord(t, w, 1, time.Unix(3000, 0))
	decoded, _ := tracker.Observe(record, sflowQualityDatagram(10, 1000, 20, 1000, 100, 0))
	journal, err := tracker.BuildJournalRecord(record, decoded, testQualityBinding(record), "collector-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendAndWait(context.Background(), journal); err != nil {
		t.Fatal(err)
	}
	tracker.Remember(record.DatagramID, decoded)
	if err := store.Compact(time.Unix(3001, 0)); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(filepath.Join(qualityDir, "quality.journal")); err != nil || info.Size() == 0 {
		t.Fatalf("pending quality journal was not retained: info=%v err=%v", info, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	restored := NewQualityTracker(config, &Metrics{})
	store, err = OpenQualityStateStore(qualityDir, "collector-a", config, restored, w, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, fresh := restored.Observe(record, sflowQualityDatagram(10, 1000, 20, 1000, 100, 0))
	if fresh {
		t.Fatal("snapshot did not restore the pending quality decision")
	}
}

func TestQualityStateStorePromotesOnlyCompletedDurableStateAndRestoresDirty(t *testing.T) {
	walDir, qualityDir := t.TempDir(), t.TempDir()
	config := testQualityStateConfig()
	w, err := OpenWAL(walDir, "collector-a", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	tracker := NewQualityTracker(config, nil)
	store, err := OpenQualityStateStore(qualityDir, "collector-a", config, tracker, w, nil)
	if err != nil {
		t.Fatal(err)
	}
	record := appendQualityWALRecord(t, w, 1, time.Unix(3200, 0))
	decoded, _ := tracker.Observe(record, sflowQualityDatagram(10, 1000, 20, 1000, 100, 0))
	journal, err := tracker.BuildJournalRecord(record, decoded, testQualityBinding(record), "collector-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendAndWait(context.Background(), journal); err != nil {
		t.Fatal(err)
	}
	if err := store.Compact(time.Unix(3201, 0)); err != nil {
		t.Fatal(err)
	}
	if checkpoints := store.PendingQualityCheckpoints(); len(checkpoints) != 0 {
		t.Fatalf("unacknowledged quality state was promoted: %+v", checkpoints)
	}
	if err := w.Acknowledge(record.DatagramID); err != nil {
		t.Fatal(err)
	}
	store.Complete(record.DatagramID)
	if err := store.Compact(time.Unix(3202, 0)); err != nil {
		t.Fatal(err)
	}
	checkpoints := store.PendingQualityCheckpoints()
	if len(checkpoints) != 1 || checkpoints[0].StateGeneration != 1 || checkpoints[0].LastWalSegment != record.Segment || checkpoints[0].LastWalOffset != record.Offset {
		t.Fatalf("durably completed quality state was not promoted: %+v", checkpoints)
	}
	if info, err := os.Stat(filepath.Join(qualityDir, "quality.journal")); err != nil || info.Size() != 0 {
		t.Fatalf("folded quality journal was not removed: info=%v err=%v", info, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	restored := NewQualityTracker(config, nil)
	store, err = OpenQualityStateStore(qualityDir, "collector-a", config, restored, w, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	checkpoints = store.PendingQualityCheckpoints()
	if len(checkpoints) != 1 || checkpoints[0].StateGeneration != 1 {
		t.Fatalf("dirty quality checkpoint was not restored: %+v", checkpoints)
	}
	if err := store.MarkQualityCheckpointPublished(checkpoints[0]); err != nil {
		t.Fatal(err)
	}
	if checkpoints := store.PendingQualityCheckpoints(); len(checkpoints) != 0 {
		t.Fatalf("published quality checkpoint remained dirty: %+v", checkpoints)
	}
}

func TestQualityStateStoreOldJournalAfterSnapshotIsIdempotent(t *testing.T) {
	walDir, qualityDir := t.TempDir(), t.TempDir()
	config := testQualityStateConfig()
	w, err := OpenWAL(walDir, "collector-a", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	tracker := NewQualityTracker(config, nil)
	store, err := OpenQualityStateStore(qualityDir, "collector-a", config, tracker, w, nil)
	if err != nil {
		t.Fatal(err)
	}
	record := appendQualityWALRecord(t, w, 1, time.Unix(3300, 0))
	decoded, _ := tracker.Observe(record, sflowQualityDatagram(10, 1000, 20, 1000, 100, 0))
	journal, err := tracker.BuildJournalRecord(record, decoded, testQualityBinding(record), "collector-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendAndWait(context.Background(), journal); err != nil {
		t.Fatal(err)
	}
	journalPath := filepath.Join(qualityDir, "quality.journal")
	oldJournal, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Acknowledge(record.DatagramID); err != nil {
		t.Fatal(err)
	}
	store.Complete(record.DatagramID)
	if err := store.Compact(time.Unix(3301, 0)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(journalPath, oldJournal, 0o640); err != nil {
		t.Fatal(err)
	}

	store, err = OpenQualityStateStore(qualityDir, "collector-a", config, NewQualityTracker(config, nil), w, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Compact(time.Unix(3302, 0)); err != nil {
		t.Fatal(err)
	}
	checkpoints := store.PendingQualityCheckpoints()
	if len(checkpoints) != 1 || checkpoints[0].StateGeneration != 1 {
		t.Fatalf("old journal advanced the committed generation twice: %+v", checkpoints)
	}
}

func TestQualityStateStoreRestoresPreviousOwnerCheckpointAsBaseline(t *testing.T) {
	now := time.Now()
	config := testQualityStateConfig()
	checkpoint := qualityCheckpointFixture(t, "collector-a", 3, 9, 10, now.Add(-time.Second))
	registry := qualityCheckpointRegistry(t, "collector-b", 4, 11, now)
	w, err := OpenWAL(t.TempDir(), "collector-b", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	tracker := NewQualityTracker(config, nil)
	store, err := OpenQualityStateStoreWithRemote(t.TempDir(), "collector-b", config, tracker, w, nil, registry, []QualityCheckpointRecord{{Checkpoint: checkpoint, Partition: 2, Offset: 41}}, now)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	state := store.State()
	if store.RestoredRemoteCount() != 1 || state.Committed != 1 || state.Dirty != 0 {
		t.Fatalf("previous owner quality baseline was not restored: restored=%d state=%+v", store.RestoredRemoteCount(), state)
	}
	record := qualityWALRecord(2, now)
	decoded, fresh := tracker.Observe(record, sflowQualityDatagram(11, 1100, 21, 1100, 100, 0))
	if !fresh || decoded.Records[0].QualityFlags != 0 {
		t.Fatalf("restored owner baseline did not continue sequence state: fresh=%v decoded=%+v", fresh, decoded)
	}
}

func TestQualityStateStoreLocalPendingStateWinsRemoteCheckpoint(t *testing.T) {
	now := time.Now()
	config := testQualityStateConfig()
	walDir, qualityDir := t.TempDir(), t.TempDir()
	w, err := OpenWAL(walDir, "collector-b", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	localTracker := NewQualityTracker(config, nil)
	localStore, err := OpenQualityStateStore(qualityDir, "collector-b", config, localTracker, w, nil)
	if err != nil {
		t.Fatal(err)
	}
	localRecord := appendQualityWALRecord(t, w, 2, now.Add(-time.Second))
	localDecision, _ := localTracker.Observe(localRecord, sflowQualityDatagram(30, 3000, 40, 3000, 100, 0))
	journal, err := localTracker.BuildJournalRecord(localRecord, localDecision, testQualityBinding(localRecord), "collector-b")
	if err != nil {
		t.Fatal(err)
	}
	if err := localStore.AppendAndWait(context.Background(), journal); err != nil {
		t.Fatal(err)
	}
	localTracker.Remember(localRecord.DatagramID, localDecision)
	if err := localStore.Close(); err != nil {
		t.Fatal(err)
	}

	remote := qualityCheckpointFixture(t, "collector-a", 3, 9, 10, now.Add(-2*time.Second))
	registry := qualityCheckpointRegistry(t, "collector-b", 4, 11, now)
	restoredTracker := NewQualityTracker(config, nil)
	store, err := OpenQualityStateStoreWithRemote(qualityDir, "collector-b", config, restoredTracker, w, nil, registry, []QualityCheckpointRecord{{Checkpoint: remote, Partition: 1, Offset: 8}}, now)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if store.RestoredRemoteCount() != 0 {
		t.Fatalf("remote state replaced a local pending exporter: restored=%d", store.RestoredRemoteCount())
	}
	next := qualityWALRecord(3, now)
	decoded, fresh := restoredTracker.Observe(next, sflowQualityDatagram(31, 3100, 41, 3100, 100, 0))
	if !fresh || decoded.Records[0].QualityFlags != 0 {
		t.Fatalf("local pending sequence state did not win remote checkpoint: fresh=%v decoded=%+v", fresh, decoded)
	}
}

func TestQualityStateStoreDelayedPublishAckCannotClearNewGeneration(t *testing.T) {
	now := time.Now()
	config := testQualityStateConfig()
	old := qualityCheckpointFixture(t, "collector-a", 3, 1, 10, now.Add(-time.Second))
	current := qualityCheckpointFixture(t, "collector-a", 3, 2, 10, now)
	committed, err := newQualityCommitAccumulator([]*flowpb.QualityCheckpoint{current}, config.MaxExporters, config.MaxDataSources)
	if err != nil {
		t.Fatal(err)
	}
	var identity [32]byte
	copy(identity[:], current.StateIdentityKey)
	store := &QualityStateStore{
		collectorID: "collector-a",
		config:      config,
		committed:   committed,
		dirty:       map[[32]byte]struct{}{identity: {}},
	}
	if err := store.MarkQualityCheckpointPublished(old); err != nil {
		t.Fatal(err)
	}
	if len(store.PendingQualityCheckpoints()) != 1 {
		t.Fatal("delayed acknowledgement cleared the newer quality checkpoint")
	}
	if err := store.MarkQualityCheckpointPublished(current); err != nil {
		t.Fatal(err)
	}
	if len(store.PendingQualityCheckpoints()) != 0 {
		t.Fatal("exact quality checkpoint acknowledgement did not clear dirty state")
	}
}

func TestQualityStateStoreDropsDurablyAcknowledgedRetryDecision(t *testing.T) {
	w, err := OpenWAL(t.TempDir(), "collector-a", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	qualityDir := t.TempDir()
	config := testQualityStateConfig()
	tracker := NewQualityTracker(config, nil)
	store, err := OpenQualityStateStore(qualityDir, "collector-a", config, tracker, w, nil)
	if err != nil {
		t.Fatal(err)
	}
	record := appendQualityWALRecord(t, w, 1, time.Unix(3500, 0))
	decoded, _ := tracker.Observe(record, sflowQualityDatagram(10, 1000, 20, 1000, 100, 0))
	journal, err := tracker.BuildJournalRecord(record, decoded, testQualityBinding(record), "collector-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendAndWait(context.Background(), journal); err != nil {
		t.Fatal(err)
	}
	if err := w.Acknowledge(record.DatagramID); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	restored := NewQualityTracker(config, nil)
	store, err = OpenQualityStateStore(qualityDir, "collector-a", config, restored, w, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if len(restored.observed) != 0 || len(store.pending) != 0 {
		t.Fatalf("durably acknowledged decision was retained: observed=%d pending=%d", len(restored.observed), len(store.pending))
	}
	nextRecord := appendQualityWALRecord(t, w, 2, time.Unix(3501, 0))
	next, fresh := restored.Observe(nextRecord, sflowQualityDatagram(11, 2000, 21, 1100, 100, 0))
	if !fresh || next.Records[0].QualityFlags != 0 {
		t.Fatalf("ack filtering discarded sequence state: fresh=%v record=%+v", fresh, next.Records[0])
	}
}

func TestQualityStateStoreTruncatesOnlyIncompleteTail(t *testing.T) {
	w, err := OpenWAL(t.TempDir(), "collector-a", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	qualityDir := t.TempDir()
	config := testQualityStateConfig()
	tracker := NewQualityTracker(config, nil)
	store, err := OpenQualityStateStore(qualityDir, "collector-a", config, tracker, w, nil)
	if err != nil {
		t.Fatal(err)
	}
	record := appendQualityWALRecord(t, w, 1, time.Unix(4000, 0))
	decoded, _ := tracker.Observe(record, sflowQualityDatagram(10, 1000, 20, 1000, 100, 0))
	journal, err := tracker.BuildJournalRecord(record, decoded, testQualityBinding(record), "collector-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendAndWait(context.Background(), journal); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(qualityDir, "quality.journal")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenQualityStateStore(qualityDir, "collector-a", config, NewQualityTracker(config, nil), w, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("recovered journal size=%d, want %d", after.Size(), before.Size())
	}
}

func TestQualityStateStoreRejectsCorruptCompleteFrame(t *testing.T) {
	w, err := OpenWAL(t.TempDir(), "collector-a", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	qualityDir := t.TempDir()
	config := testQualityStateConfig()
	tracker := NewQualityTracker(config, nil)
	store, err := OpenQualityStateStore(qualityDir, "collector-a", config, tracker, w, nil)
	if err != nil {
		t.Fatal(err)
	}
	record := appendQualityWALRecord(t, w, 1, time.Unix(5000, 0))
	decoded, _ := tracker.Observe(record, sflowQualityDatagram(10, 1000, 20, 1000, 100, 0))
	journal, err := tracker.BuildJournalRecord(record, decoded, testQualityBinding(record), "collector-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendAndWait(context.Background(), journal); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(qualityDir, "quality.journal")
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	last := []byte{0}
	if _, err := file.ReadAt(last, info.Size()-1); err != nil {
		t.Fatal(err)
	}
	last[0] ^= 0xff
	if _, err := file.WriteAt(last, info.Size()-1); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if recovered, err := OpenQualityStateStore(qualityDir, "collector-a", config, NewQualityTracker(config, nil), w, nil); err == nil {
		recovered.Close()
		t.Fatal("corrupt quality journal was accepted")
	}
}

func appendQualityWALRecord(t *testing.T, w *WAL, marker byte, receivedAt time.Time) WALRecord {
	t.Helper()
	input := testWALInput([]byte{marker})
	input.ReceivedAt = receivedAt
	record, err := w.Append(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WaitDurable(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	return record
}

func testQualityBinding(record WALRecord) SourceBinding {
	return SourceBinding{Protocol: record.Protocol, SourcePrefix: record.Source.Addr().String() + "/32", TenantID: record.TenantID, ExporterID: record.ExporterID, TargetID: record.TargetID, DeviceID: record.DeviceID, OwnershipEpoch: 1, SamplingMode: SamplingModeSampled, Enabled: true}
}

func testQualityStateConfig() QualityConfig {
	return QualityConfig{StateTTL: time.Hour, AnomalyWindow: time.Minute, JournalFsync: time.Millisecond, CheckpointEvery: time.Hour, JournalMaxBytes: 1 << 20, MaxExporters: 100, MaxDataSources: 100}
}

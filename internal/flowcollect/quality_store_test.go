package flowcollect

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
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
	journal, err := tracker.BuildJournalRecord(record, decoded, "collector-a")
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
	journal, err := tracker.BuildJournalRecord(record, decoded, "collector-a")
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
	journal, err := tracker.BuildJournalRecord(record, decoded, "collector-a")
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
	if info, err := os.Stat(filepath.Join(qualityDir, "quality.journal")); err != nil || info.Size() != 0 {
		t.Fatalf("journal was not compacted: info=%v err=%v", info, err)
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
	journal, err := tracker.BuildJournalRecord(record, decoded, "collector-a")
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
	journal, err := tracker.BuildJournalRecord(record, decoded, "collector-a")
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
	journal, err := tracker.BuildJournalRecord(record, decoded, "collector-a")
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

func testQualityStateConfig() QualityConfig {
	return QualityConfig{StateTTL: time.Hour, AnomalyWindow: time.Minute, JournalFsync: time.Millisecond, CheckpointEvery: time.Hour, JournalMaxBytes: 1 << 20, MaxExporters: 100, MaxDataSources: 100}
}

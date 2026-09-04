package flowcollect

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"testing"
	"time"
)

func TestWALRecoveryReplayAndAcknowledgement(t *testing.T) {
	dir := t.TempDir()
	config := testWALConfig()
	w, err := OpenWAL(dir, "collector-a", config)
	if err != nil {
		t.Fatal(err)
	}
	record, err := w.Append(testWALInput([]byte("durable datagram")))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.WaitDurable(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	segments, err := filepathSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(segments[len(segments)-1], os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()

	w, err = OpenWAL(dir, "collector-a", config)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var replayed []WALRecord
	if err := w.Replay(func(item WALRecord) error { replayed = append(replayed, item); return w.Acknowledge(item.DatagramID) }); err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 1 || replayed[0].DatagramID != record.DatagramID || string(replayed[0].Payload) != "durable datagram" {
		t.Fatalf("unexpected replay: %+v", replayed)
	}
	replayed = nil
	if err := w.Replay(func(item WALRecord) error { replayed = append(replayed, item); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 0 {
		t.Fatalf("acknowledged record replayed: %+v", replayed)
	}
}

func TestWALReclaimsOnlyClosedAcknowledgedSegments(t *testing.T) {
	config := testWALConfig()
	config.SegmentBytes = 300
	w, err := OpenWAL(t.TempDir(), "collector-a", config)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	first, err := w.Append(testWALInput(make([]byte, 100)))
	if err != nil {
		t.Fatal(err)
	}
	second, err := w.Append(testWALInput(make([]byte, 100)))
	if err != nil {
		t.Fatal(err)
	}
	if first.Segment == second.Segment {
		t.Fatal("test did not rotate segment")
	}
	if err := w.Acknowledge(first.DatagramID); err != nil {
		t.Fatal(err)
	}
	removed, err := w.Reclaim()
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed %d segments, want 1", removed)
	}
}

func TestWALPersistsPartialChildAcknowledgements(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, "collector-a", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	record, err := w.Append(testWALInput([]byte("split datagram")))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AcknowledgeChild(record.DatagramID, 0, 2); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = OpenWAL(dir, "collector-a", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if !w.ChildAcknowledged(record.DatagramID, 0, 2) || w.ChildAcknowledged(record.DatagramID, 1, 2) {
		t.Fatal("partial acknowledgement was not restored")
	}
	count := 0
	if err := w.Replay(func(WALRecord) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("partially acknowledged datagram replay count=%d", count)
	}
	if err := w.AcknowledgeChild(record.DatagramID, 1, 2); err != nil {
		t.Fatal(err)
	}
	count = 0
	if err := w.Replay(func(WALRecord) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("fully acknowledged datagram was replayed")
	}
}

func TestWALReplayCursorStopsAtBackpressureWithoutRescanning(t *testing.T) {
	w, err := OpenWAL(t.TempDir(), "collector-a", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, payload := range []string{"one", "two", "three"} {
		if _, err := w.Append(testWALInput([]byte(payload))); err != nil {
			t.Fatal(err)
		}
	}
	seen := []string{}
	cursor, reachedEnd, err := w.ReplayFrom(ReplayCursor{}, func(record WALRecord) bool {
		if string(record.Payload) == "two" {
			return false
		}
		seen = append(seen, string(record.Payload))
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if reachedEnd || len(seen) != 1 || seen[0] != "one" {
		t.Fatalf("unexpected first pass: cursor=%+v end=%v seen=%v", cursor, reachedEnd, seen)
	}
	seen = nil
	_, reachedEnd, err = w.ReplayFrom(cursor, func(record WALRecord) bool { seen = append(seen, string(record.Payload)); return true })
	if err != nil {
		t.Fatal(err)
	}
	if !reachedEnd || len(seen) != 2 || seen[0] != "two" || seen[1] != "three" {
		t.Fatalf("unexpected resumed pass: end=%v seen=%v", reachedEnd, seen)
	}
}

func TestWALHardWatermarkNeverOverwrites(t *testing.T) {
	config := testWALConfig()
	config.MaxBytes = 600
	config.SegmentBytes = 550
	config.HardWatermark = .75
	config.SoftWatermark = .5
	w, err := OpenWAL(t.TempDir(), "collector-a", config)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Append(testWALInput(make([]byte, 100))); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(testWALInput(make([]byte, 100))); !errors.Is(err, ErrWALHardLimit) {
		t.Fatalf("got %v, want hard limit", err)
	}
}

func TestWALRejectsConcurrentWriterForSameDirectory(t *testing.T) {
	dir := t.TempDir()
	first, err := OpenWAL(dir, "collector-a", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if second, err := OpenWAL(dir, "collector-a", testWALConfig()); err == nil {
		second.Close()
		t.Fatal("second WAL writer acquired the same directory")
	}
}

func TestOpenWALRejectsInvalidRetentionAndWatermarks(t *testing.T) {
	for name, mutate := range map[string]func(*WALConfig){
		"max age":       func(config *WALConfig) { config.MaxAge = 0 },
		"soft zero":     func(config *WALConfig) { config.SoftWatermark = 0 },
		"watermark gap": func(config *WALConfig) { config.SoftWatermark = config.HardWatermark },
		"hard maximum":  func(config *WALConfig) { config.HardWatermark = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			config := testWALConfig()
			mutate(&config)
			if wal, err := OpenWAL(t.TempDir(), "collector-a", config); err == nil {
				_ = wal.Close()
				t.Fatal("invalid WAL configuration was accepted")
			}
		})
	}
}

func testWALConfig() WALConfig {
	return WALConfig{MaxBytes: 10000, SegmentBytes: 4096, FsyncInterval: time.Millisecond, SoftWatermark: .7, HardWatermark: .9, MaxAge: time.Hour}
}

func testWALInput(payload []byte) WALInput {
	return WALInput{Protocol: ProtocolSFlow5, ReceivedAt: time.Unix(1700000000, 0), Source: netip.MustParseAddrPort("192.0.2.1:6343"), RegistryVersion: 1, TenantID: "tenant", ExporterID: "exporter", TargetID: "target", DeviceID: "device", Payload: payload}
}

func filepathSegments(dir string) ([]string, error) {
	w := &WAL{dir: dir}
	return w.segmentFiles()
}

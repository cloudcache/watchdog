package flowcollect

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

func TestKafkaStateKeyScannerFreezesAllPartitionsAndKeepsExactKey(t *testing.T) {
	key := make([]byte, sha256.Size)
	key[0] = 7
	emptyValue := []byte{}
	source := newFakeCollectStateSource("state", map[int32][]*sarama.ConsumerMessage{
		1: nil,
		0: {
			{Topic: "state", Partition: 0, Offset: 0, Key: bytes.Repeat([]byte{1}, sha256.Size), Value: []byte("other")},
			{Topic: "state", Partition: 0, Offset: 2, Key: key, Value: emptyValue},
			{Topic: "state", Partition: 0, Offset: 4, Key: key, Value: []byte("post-boundary")},
		},
	}, map[int32][2]int64{0: {0, 4}, 1: {0, 0}})
	scanner, err := newKafkaStateKeyScanner("state", time.Second, source)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(900_000, 0).UTC()
	scanner.now = func() time.Time { return base }
	snapshot, err := scanner.Scan(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Present || snapshot.Value == nil || len(snapshot.Value) != 0 || snapshot.LastRecord == nil || *snapshot.LastRecord != (KafkaRecordPosition{Partition: 0, Offset: 2}) || !snapshot.CapturedAt.Equal(base) {
		t.Fatalf("unexpected exact-key snapshot: %+v", snapshot)
	}
	if watermark, ok := snapshot.HighWatermark(0); !ok || watermark != 4 {
		t.Fatalf("partition 0 high watermark=%d ok=%t", watermark, ok)
	}
	replacement, err := snapshot.ReplacementObservation(5, 9)
	if err != nil || replacement.Position != *snapshot.LastRecord || replacement.HighWatermark != 4 || replacement.RestoredOldOwnershipEpoch != 5 || replacement.RestoredOldGeneration != 9 {
		t.Fatalf("replacement observation=%+v err=%v", replacement, err)
	}
	if got := source.eventsSnapshot(); len(got) < 6 || got[0] != "partitions" || got[1] != "oldest/0" || got[2] != "newest/0" || got[3] != "oldest/1" || got[4] != "newest/1" {
		t.Fatalf("partition scans started before the boundary vector was frozen: %v", got)
	}
	key[0] = 9
	if snapshot.KafkaKey[0] != 7 {
		t.Fatal("scanner snapshot retained the caller key")
	}
}

func TestKafkaStateKeyScannerRetainsLastTombstonePosition(t *testing.T) {
	key := make([]byte, 1+sha256.Size)
	key[0] = qualityCheckpointKeyPrefix
	source := newFakeCollectStateSource("state", map[int32][]*sarama.ConsumerMessage{0: {
		{Topic: "state", Partition: 0, Offset: 3, Key: key, Value: []byte("checkpoint")},
		{Topic: "state", Partition: 0, Offset: 8, Key: key, Value: nil},
	}}, map[int32][2]int64{0: {3, 9}})
	scanner, err := newKafkaStateKeyScanner("state", time.Second, source)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(910_000, 0).UTC()
	scanner.now = func() time.Time { return base }
	snapshot, err := scanner.Scan(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Present || snapshot.Value != nil || snapshot.LastRecord == nil || snapshot.LastRecord.Offset != 8 {
		t.Fatalf("latest tombstone was not preserved: %+v", snapshot)
	}
	receipt := StateTombstoneReceipt{Position: KafkaRecordPosition{Partition: 0, Offset: 8}, AcknowledgedAt: base.Add(-time.Second)}
	verification, err := snapshot.TombstoneVerification(receipt)
	if err != nil || verification.Partition != 0 || verification.HighWatermark != 9 || !verification.KeyAbsent || !verification.CapturedAt.Equal(base) {
		t.Fatalf("tombstone verification=%+v err=%v", verification, err)
	}
	tooNew := receipt
	tooNew.Position.Offset = 9
	if _, err := snapshot.TombstoneVerification(tooNew); err == nil {
		t.Fatal("uncovered tombstone receipt was accepted")
	}
}

func TestKafkaStateKeyScannerRejectsCrossPartitionKey(t *testing.T) {
	key := bytes.Repeat([]byte{3}, sha256.Size)
	source := newFakeCollectStateSource("state", map[int32][]*sarama.ConsumerMessage{
		0: {{Topic: "state", Partition: 0, Offset: 0, Key: key, Value: []byte("first")}},
		1: {{Topic: "state", Partition: 1, Offset: 0, Key: key, Value: nil}},
	}, map[int32][2]int64{0: {0, 1}, 1: {0, 1}})
	scanner, err := newKafkaStateKeyScanner("state", time.Second, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scanner.Scan(context.Background(), key); err == nil {
		t.Fatal("state key present in two partitions was accepted")
	}
}

func TestKafkaStateKeyScannerTimesOut(t *testing.T) {
	key := bytes.Repeat([]byte{4}, sha256.Size)
	source := newFakeCollectStateSource("state", map[int32][]*sarama.ConsumerMessage{0: nil}, map[int32][2]int64{0: {0, 1}})
	source.stalled[0] = true
	scanner, err := newKafkaStateKeyScanner("state", 10*time.Millisecond, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scanner.Scan(context.Background(), key); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("scan error=%v, want deadline exceeded", err)
	}
}

func TestKafkaStateKeyScannerRejectsInvalidTypedKeyAndPartitionLayout(t *testing.T) {
	source := newFakeCollectStateSource("state", map[int32][]*sarama.ConsumerMessage{1: nil}, map[int32][2]int64{1: {0, 0}})
	scanner, err := newKafkaStateKeyScanner("state", time.Second, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scanner.Scan(context.Background(), []byte("invalid")); err == nil {
		t.Fatal("invalid typed key was accepted")
	}
	if _, err := scanner.Scan(context.Background(), bytes.Repeat([]byte{1}, sha256.Size)); err == nil {
		t.Fatal("non-contiguous partition metadata was accepted")
	}
}

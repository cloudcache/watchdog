package flowcollect

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
)

func TestKafkaStateTombstoneWriterReturnsBrokerReceipt(t *testing.T) {
	base := time.Unix(600_000, 0).UTC()
	tombstone := readyQualityStateTombstone(t, base)
	config, err := buildStateTombstoneKafkaConfig(KafkaConfig{}, "reconciler-a")
	if err != nil {
		t.Fatal(err)
	}
	producer := mocks.NewAsyncProducer(t, config)
	wantKey := tombstone.Key()
	producer.ExpectInputWithMessageCheckerFunctionAndSucceed(func(message *sarama.ProducerMessage) error {
		key, err := message.Key.Encode()
		if err != nil {
			return err
		}
		if message.Topic != "state" || message.Value != nil || !bytes.Equal(key, wantKey) {
			return fmt.Errorf("unexpected tombstone message: topic=%q key=%x value=%v", message.Topic, key, message.Value)
		}
		return nil
	})
	writer, err := newKafkaStateTombstoneWriter("state", producer)
	if err != nil {
		t.Fatal(err)
	}
	writer.now = func() time.Time { return base.Add(9 * time.Second) }
	receipt, err := writer.Publish(context.Background(), tombstone, base.Add(8*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Position.Partition < 0 || receipt.Position.Offset != 1 || !receipt.AcknowledgedAt.Equal(base.Add(9*time.Second)) {
		t.Fatalf("unexpected tombstone receipt: %+v", receipt)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestKafkaStateTombstoneWriterReturnsBrokerFailure(t *testing.T) {
	base := time.Unix(700_000, 0).UTC()
	tombstone := readyQualityStateTombstone(t, base)
	config, err := buildStateTombstoneKafkaConfig(KafkaConfig{}, "reconciler-a")
	if err != nil {
		t.Fatal(err)
	}
	producer := mocks.NewAsyncProducer(t, config)
	want := errors.New("injected tombstone failure")
	producer.ExpectInputAndFail(want)
	writer, err := newKafkaStateTombstoneWriter("state", producer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Publish(context.Background(), tombstone, base.Add(8*time.Second)); !errors.Is(err, want) {
		t.Fatalf("publish error=%v, want %v", err, want)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStateTombstoneKafkaConfigUsesDedicatedSafeProducer(t *testing.T) {
	config, err := buildStateTombstoneKafkaConfig(KafkaConfig{}, "reconciler-a")
	if err != nil {
		t.Fatal(err)
	}
	if config.ClientID != "reconciler-a-state-cleanup" || config.Metadata.AllowAutoTopicCreation || !config.Producer.Idempotent || config.Producer.RequiredAcks != sarama.WaitForAll || config.Net.MaxOpenRequests != 1 || !config.Producer.Return.Successes || !config.Producer.Return.Errors || config.Producer.Compression != sarama.CompressionNone {
		t.Fatalf("unsafe state tombstone Kafka config: client=%q producer=%+v", config.ClientID, config.Producer)
	}
}

func readyQualityStateTombstone(t *testing.T, base time.Time) StateTombstone {
	t.Helper()
	old := qualityCheckpointFixture(t, "collector-a", 5, 9, 20, base)
	job, err := NewQualityStateCleanupJob("cleanup-kafka", "approval-kafka", "user-kafka", old, base.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	fence := stateCleanupFenceForQuality(old, "collector-b", 6, 21, base)
	if err := job.ConfirmFence(fence, base.Add(6*time.Second)); err != nil {
		t.Fatal(err)
	}
	replacement := qualityCheckpointFixture(t, "collector-b", 6, 1, 21, base.Add(6*time.Second))
	observation := FrozenReplacementObservation{
		CapturedAt: base.Add(7 * time.Second), Position: KafkaRecordPosition{Partition: 2, Offset: 40}, HighWatermark: 41,
		RestoredOldOwnershipEpoch: 5, RestoredOldGeneration: 9,
	}
	if err := job.ObserveQualityStateReplacement(replacement, observation); err != nil {
		t.Fatal(err)
	}
	tombstone, err := job.Tombstone()
	if err != nil {
		t.Fatal(err)
	}
	return tombstone
}

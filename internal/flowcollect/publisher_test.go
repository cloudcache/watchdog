package flowcollect

import (
	"bytes"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"google.golang.org/protobuf/proto"
)

func TestKafkaPublisherConfigIsIdempotentAndManuallyPartitioned(t *testing.T) {
	config, err := buildSaramaConfig(KafkaConfig{Compression: "zstd", TLS: true}, NormalizedBatchCfg{MaxWait: 5 * time.Millisecond}, "collector-a")
	if err != nil {
		t.Fatal(err)
	}
	if !config.Producer.Idempotent || config.Producer.RequiredAcks != sarama.WaitForAll || config.Net.MaxOpenRequests != 1 {
		t.Fatalf("unsafe producer config: %+v", config.Producer)
	}
	if config.Producer.MaxMessageBytes < collectStateMaxBytes {
		t.Fatalf("producer cannot carry the bounded collect-state message: %d", config.Producer.MaxMessageBytes)
	}
	if config.Metadata.AllowAutoTopicCreation {
		t.Fatal("flow collector Kafka client may not auto-create topics")
	}
	message := &sarama.ProducerMessage{Partition: 7}
	partitioner := config.Producer.Partitioner("topic")
	partition, err := partitioner.Partition(message, 10)
	if err != nil {
		t.Fatal(err)
	}
	if partition != 7 {
		t.Fatalf("manual partition ignored: %d", partition)
	}
	stateMessage := &sarama.ProducerMessage{Partition: -1, Key: sarama.StringEncoder("collector/source/domain")}
	first, err := partitioner.Partition(stateMessage, 10)
	if err != nil {
		t.Fatal(err)
	}
	second, err := partitioner.Partition(stateMessage, 10)
	if err != nil {
		t.Fatal(err)
	}
	if first < 0 || first >= 10 || second != first {
		t.Fatalf("collect-state key was not consistently partitioned: %d %d", first, second)
	}
	if _, err := partitioner.Partition(&sarama.ProducerMessage{Partition: 10}, 10); err == nil {
		t.Fatal("out-of-range explicit partition was accepted")
	}
}

func TestQualityCheckpointProducerMessageUsesTypedCompactedKey(t *testing.T) {
	checkpoint := qualityCheckpointFixture(t, "collector-a", 3, 7, 10, time.Unix(120_000, 0))
	message, err := qualityCheckpointProducerMessage("state", checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	key, err := message.Key.Encode()
	if err != nil {
		t.Fatal(err)
	}
	value, err := message.Value.Encode()
	if err != nil {
		t.Fatal(err)
	}
	restored := &flowpb.QualityCheckpoint{}
	if err := proto.Unmarshal(value, restored); err != nil {
		t.Fatal(err)
	}
	if message.Topic != "state" || message.Partition != -1 || !isQualityCheckpointKafkaKey(key) || !bytes.Equal(key[1:], checkpoint.StateKey) || !proto.Equal(restored, checkpoint) || !message.Timestamp.Equal(time.UnixMilli(checkpoint.CommittedAtUnixMs)) {
		t.Fatalf("quality checkpoint Kafka message is invalid: topic=%q partition=%d key=%x timestamp=%s", message.Topic, message.Partition, key, message.Timestamp)
	}
}

func TestKafkaPublisherRejectsUnknownCompression(t *testing.T) {
	if _, err := buildSaramaConfig(KafkaConfig{Compression: "mystery"}, NormalizedBatchCfg{MaxWait: time.Millisecond}, "collector-a"); err == nil {
		t.Fatal("unknown compression was accepted")
	}
}

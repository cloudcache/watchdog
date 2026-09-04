package flowcollect

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

func TestKafkaPublisherConfigIsIdempotentAndManuallyPartitioned(t *testing.T) {
	config, err := buildSaramaConfig(KafkaConfig{Compression: "zstd", TLS: true}, NormalizedBatchCfg{MaxWait: 5 * time.Millisecond}, "collector-a")
	if err != nil {
		t.Fatal(err)
	}
	if !config.Producer.Idempotent || config.Producer.RequiredAcks != sarama.WaitForAll || config.Net.MaxOpenRequests != 1 {
		t.Fatalf("unsafe producer config: %+v", config.Producer)
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

func TestKafkaPublisherRejectsUnknownCompression(t *testing.T) {
	if _, err := buildSaramaConfig(KafkaConfig{Compression: "mystery"}, NormalizedBatchCfg{MaxWait: time.Millisecond}, "collector-a"); err == nil {
		t.Fatal("unknown compression was accepted")
	}
}

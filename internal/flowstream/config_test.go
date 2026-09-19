// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"testing"
	"time"
)

func TestDefaultKafkaConfigsAreValid(t *testing.T) {
	producer := DefaultProducerConfig()
	if err := producer.Validate(); err != nil {
		t.Fatal(err)
	}
	consumer := DefaultConsumerConfig()
	if err := consumer.Validate(); err != nil {
		t.Fatal(err)
	}
	if topicForVersion(producer.Kafka.Topic) != "watchdog.flow.raw-v1" {
		t.Fatalf("unexpected versioned topic %q", topicForVersion(producer.Kafka.Topic))
	}
}

func TestConsumerPartitionBatchRecordsValidation(t *testing.T) {
	config := DefaultConsumerConfig()
	if config.PartitionBatchRecords != 4_096 {
		t.Fatalf("default partition batch records=%d", config.PartitionBatchRecords)
	}
	config.PartitionBatchRecords = 0
	if err := config.Validate(); err == nil {
		t.Fatal("zero partition batch records was accepted")
	}
	config.PartitionBatchRecords = 1_000_001
	if err := config.Validate(); err == nil {
		t.Fatal("oversized partition batch records was accepted")
	}
}

func TestConsumerFetchByteLimitsValidation(t *testing.T) {
	config := DefaultConsumerConfig()
	if config.FetchMaxPartitionBytes != 16<<20 || config.FetchMaxBytes != 64<<20 {
		t.Fatalf("default fetch limits partition=%d broker=%d", config.FetchMaxPartitionBytes, config.FetchMaxBytes)
	}
	config.FetchMaxPartitionBytes = config.FetchMinBytes - 1
	if err := config.Validate(); err == nil {
		t.Fatal("partition maximum below fetch minimum was accepted")
	}
	config = DefaultConsumerConfig()
	config.FetchMaxBytes = config.FetchMaxPartitionBytes - 1
	if err := config.Validate(); err == nil {
		t.Fatal("broker maximum below partition maximum was accepted")
	}
}

func TestKafkaConfigValidation(t *testing.T) {
	producer := DefaultProducerConfig()
	producer.Kafka.SASL = SASLConfig{Mechanism: SASLSCRAMSHA256}
	if err := producer.Validate(); err == nil {
		t.Fatal("SASL without credentials was accepted")
	}
	producer = DefaultProducerConfig()
	producer.Kafka.TLS = TLSConfig{Enabled: true, CertFile: "client.pem"}
	if err := producer.Validate(); err == nil {
		t.Fatal("TLS certificate without key was accepted")
	}
	producer = DefaultProducerConfig()
	producer.Kafka.TLS = TLSConfig{CAFile: "ca.pem"}
	if err := producer.Validate(); err == nil {
		t.Fatal("TLS parameters without explicit TLS enablement were accepted")
	}
	producer = DefaultProducerConfig()
	producer.Compression = "invalid"
	if err := producer.Validate(); err == nil {
		t.Fatal("invalid compression was accepted")
	}
	consumer := DefaultConsumerConfig()
	consumer.FetchMaxWait = time.Millisecond
	if err := consumer.Validate(); err == nil {
		t.Fatal("undersized fetch wait was accepted")
	}
	consumer = DefaultConsumerConfig()
	consumer.TemplateReplayRecords = -1
	if err := consumer.Validate(); err == nil {
		t.Fatal("negative template replay window was accepted")
	}
	consumer = DefaultConsumerConfig()
	if consumer.TemplateReplayRecords != 0 {
		t.Fatalf("default template replay=%d, want disabled for stateless sFlow", consumer.TemplateReplayRecords)
	}
}

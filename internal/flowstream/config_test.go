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
	consumer.TemplateReplayRecords = 0
	if err := consumer.Validate(); err == nil {
		t.Fatal("disabled template replay window was accepted")
	}
}

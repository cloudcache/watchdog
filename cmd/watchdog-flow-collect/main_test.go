package main

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/cloudcache/watchdog/internal/flowstream"
)

func TestMetricsListenerCanBeDisabledWithoutBinding(t *testing.T) {
	server, err := startMetricsServer("", http.NotFoundHandler())
	if err != nil || server != nil {
		t.Fatalf("server=%v error=%v", server, err)
	}
}

func TestBuildProducerConfigMapsTLSAndSASLWithoutInlineSecret(t *testing.T) {
	secretPath := filepath.Join(t.TempDir(), "kafka-password")
	if err := os.WriteFile(secretPath, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := buildProducerConfig(options{
		brokers: "kafka-1:9093, kafka-2:9093", topic: "watchdog.flow.raw", clientID: "collector-a",
		queueSize: 1024, compression: "lz4", kafkaTLS: true, kafkaCAFile: "ca.pem", kafkaServerName: "kafka.internal",
		saslMechanism: string(flowstream.SASLSCRAMSHA512), saslUsername: "collector", saslPasswordFile: secretPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(config.Kafka.Brokers) != 2 || config.Kafka.TLS.CAFile != "ca.pem" || config.Kafka.SASL.Password != "secret" || config.Kafka.SASL.Mechanism != flowstream.SASLSCRAMSHA512 {
		t.Fatalf("unexpected producer config: %+v", config)
	}
}

func TestBuildProducerConfigRejectsMisleadingSecurityOptions(t *testing.T) {
	base := options{brokers: "kafka:9092", topic: "watchdog.flow.raw", clientID: "collector", queueSize: 1, compression: "lz4", saslMechanism: "none"}
	withDisabledTLS := base
	withDisabledTLS.kafkaCAFile = "ca.pem"
	if _, err := buildProducerConfig(withDisabledTLS); err == nil {
		t.Fatal("TLS parameters were accepted while TLS was disabled")
	}
	withMissingSecret := base
	withMissingSecret.saslMechanism = string(flowstream.SASLSCRAMSHA256)
	withMissingSecret.saslUsername = "collector"
	if _, err := buildProducerConfig(withMissingSecret); err == nil {
		t.Fatal("SASL without a password file was accepted")
	}
}

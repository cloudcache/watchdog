// SPDX-FileCopyrightText: 2022 Free Mobile
// SPDX-License-Identifier: AGPL-3.0-only
//
// Adapted from Akvorado common/kafka and inlet/outlet Kafka configuration.

package flowstream

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type SASLMechanism string

const (
	SASLNone        SASLMechanism = "none"
	SASLPlain       SASLMechanism = "plain"
	SASLSCRAMSHA256 SASLMechanism = "scram-sha-256"
	SASLSCRAMSHA512 SASLMechanism = "scram-sha-512"
)

type TLSConfig struct {
	Enabled    bool
	CAFile     string
	CertFile   string
	KeyFile    string
	ServerName string
}

type SASLConfig struct {
	Mechanism SASLMechanism
	Username  string
	Password  string
}

type KafkaConfig struct {
	Brokers  []string
	Topic    string
	ClientID string
	TLS      TLSConfig
	SASL     SASLConfig
}

// Validate checks broker transport and identity without constructing a
// producer or consumer. Management-plane users such as reconciliation share
// exactly the same TLS/SASL contract as the Flow data-plane clients.
func (c KafkaConfig) Validate() error { return c.validate() }

type ProducerConfig struct {
	Kafka       KafkaConfig
	QueueSize   int
	Compression string
}

type ConsumerConfig struct {
	Kafka                  KafkaConfig
	ConsumerGroup          string
	FetchMinBytes          int32
	FetchMaxBytes          int32
	FetchMaxPartitionBytes int32
	FetchMaxWait           time.Duration
	PartitionBatchRecords  int
	TemplateReplayRecords  int64
	StartAtEnd             bool
}

func DefaultProducerConfig() ProducerConfig {
	return ProducerConfig{
		Kafka: KafkaConfig{
			Brokers:  []string{"127.0.0.1:9092"},
			Topic:    "watchdog.flow.raw",
			ClientID: "watchdog-flow-collect",
		},
		QueueSize:   65536,
		Compression: "lz4",
	}
}

func DefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		Kafka: KafkaConfig{
			Brokers:  []string{"127.0.0.1:9092"},
			Topic:    "watchdog.flow.raw",
			ClientID: "watchdog-flow-decode",
		},
		ConsumerGroup: "watchdog-flow-decode",
		FetchMinBytes: 1_000_000,
		FetchMaxBytes: 64 << 20,
		// A single exporter is intentionally pinned to one Kafka partition.
		// The franz-go default is only 1 MiB, which prevents a recovered worker
		// from draining that partition faster than a busy live producer.
		FetchMaxPartitionBytes: 16 << 20,
		FetchMaxWait:           time.Second,
		// Bound one durable handler call independently of Kafka's fetch size.
		// A ClickHouse failure after earlier chunks have succeeded can then
		// commit those chunks instead of replaying the entire fetch.
		PartitionBatchRecords: 4_096,
		// Packet formats such as sFlow do not need decoder template replay.
		// NetFlow/IPFIX deployments may opt into a bounded replay window sized
		// above their maximum records-per-template refresh interval.
		TemplateReplayRecords: 0,
	}
}

func (c KafkaConfig) validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("at least one Kafka broker is required")
	}
	for _, broker := range c.Brokers {
		if strings.TrimSpace(broker) == "" {
			return errors.New("Kafka broker cannot be empty")
		}
	}
	if !validTopicBase(c.Topic) {
		return errors.New("Kafka topic must contain 1..240 letters, digits, '.', '_' or '-'")
	}
	if strings.TrimSpace(c.ClientID) == "" {
		return errors.New("Kafka client ID is required")
	}
	if (c.TLS.CertFile == "") != (c.TLS.KeyFile == "") {
		return errors.New("Kafka TLS certificate and key must be configured together")
	}
	if !c.TLS.Enabled && (c.TLS.CAFile != "" || c.TLS.CertFile != "" || c.TLS.KeyFile != "" || c.TLS.ServerName != "") {
		return errors.New("Kafka TLS parameters require TLS to be enabled")
	}
	mechanism := c.SASL.Mechanism
	if mechanism == "" {
		mechanism = SASLNone
	}
	switch mechanism {
	case SASLNone:
		if c.SASL.Username != "" || c.SASL.Password != "" {
			return errors.New("Kafka SASL credentials require a mechanism")
		}
	case SASLPlain, SASLSCRAMSHA256, SASLSCRAMSHA512:
		if c.SASL.Username == "" || c.SASL.Password == "" {
			return errors.New("Kafka SASL username and password are required")
		}
	default:
		return fmt.Errorf("unsupported Kafka SASL mechanism %q", mechanism)
	}
	return nil
}

func (c ProducerConfig) Validate() error {
	if err := c.Kafka.validate(); err != nil {
		return err
	}
	if c.QueueSize < 1 {
		return errors.New("Kafka producer queue size must be positive")
	}
	_, err := compressionCodec(c.Compression)
	return err
}

func (c ConsumerConfig) Validate() error {
	if err := c.Kafka.validate(); err != nil {
		return err
	}
	if strings.TrimSpace(c.ConsumerGroup) == "" {
		return errors.New("Kafka consumer group is required")
	}
	if c.FetchMinBytes < 1 {
		return errors.New("Kafka fetch minimum bytes must be positive")
	}
	if c.FetchMaxPartitionBytes < c.FetchMinBytes || c.FetchMaxPartitionBytes > 1<<30 {
		return errors.New("Kafka fetch maximum partition bytes must be at least the minimum and no more than 1 GiB")
	}
	if c.FetchMaxBytes < c.FetchMaxPartitionBytes || c.FetchMaxBytes > 1<<30 {
		return errors.New("Kafka fetch maximum bytes must be at least the partition maximum and no more than 1 GiB")
	}
	if c.FetchMaxWait < 100*time.Millisecond {
		return errors.New("Kafka fetch maximum wait must be at least 100ms")
	}
	if c.PartitionBatchRecords < 1 || c.PartitionBatchRecords > 1_000_000 {
		return errors.New("Kafka partition batch records must be 1..1000000")
	}
	if c.TemplateReplayRecords < 0 || c.TemplateReplayRecords > 100_000_000 {
		return errors.New("Kafka template replay records must be 0..100000000")
	}
	return nil
}

func (c KafkaConfig) options() ([]kgo.Opt, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	// Flow services validate connectivity during startup. Once that succeeds,
	// an EOF on the first request of a replacement connection is a transient
	// broker restart/load event, not evidence of a TLS or SASL mismatch.
	opts := []kgo.Opt{kgo.SeedBrokers(c.Brokers...), kgo.ClientID(c.ClientID), kgo.AlwaysRetryEOF()}
	if c.TLS.Enabled {
		tlsConfig, err := c.TLS.ClientConfig()
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	mechanism, err := c.saslMechanism()
	if err != nil {
		return nil, err
	}
	if mechanism != nil {
		opts = append(opts, kgo.SASL(mechanism))
	}
	return opts, nil
}

// ClientConfig builds an isolated client TLS configuration shared by Flow
// Kafka and ClickHouse transports. File contents are loaded only at startup.
func (c TLSConfig) ClientConfig() (*tls.Config, error) {
	if !c.Enabled {
		if c.CAFile != "" || c.CertFile != "" || c.KeyFile != "" || c.ServerName != "" {
			return nil, errors.New("TLS parameters require TLS to be enabled")
		}
		return nil, nil
	}
	if (c.CertFile == "") != (c.KeyFile == "") {
		return nil, errors.New("TLS certificate and key must be configured together")
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("load system certificate pool: %w", err)
	}
	if pool == nil {
		pool = x509.NewCertPool()
	}
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read Kafka TLS CA: %w", err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("Kafka TLS CA contains no certificate")
		}
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: c.ServerName}
	if c.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load Kafka TLS client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	return tlsConfig, nil
}

func (c KafkaConfig) saslMechanism() (sasl.Mechanism, error) {
	switch c.SASL.Mechanism {
	case "", SASLNone:
		return nil, nil
	case SASLPlain:
		return plain.Auth{User: c.SASL.Username, Pass: c.SASL.Password}.AsMechanism(), nil
	case SASLSCRAMSHA256:
		return scram.Auth{User: c.SASL.Username, Pass: c.SASL.Password}.AsSha256Mechanism(), nil
	case SASLSCRAMSHA512:
		return scram.Auth{User: c.SASL.Username, Pass: c.SASL.Password}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported Kafka SASL mechanism %q", c.SASL.Mechanism)
	}
}

func compressionCodec(name string) (kgo.CompressionCodec, error) {
	switch name {
	case "", "lz4":
		return kgo.Lz4Compression(), nil
	case "none":
		return kgo.NoCompression(), nil
	case "gzip":
		return kgo.GzipCompression(), nil
	case "snappy":
		return kgo.SnappyCompression(), nil
	case "zstd":
		return kgo.ZstdCompression(), nil
	default:
		return kgo.CompressionCodec{}, fmt.Errorf("unsupported Kafka compression %q", name)
	}
}

func topicForVersion(base string) string {
	return fmt.Sprintf("%s-v%d", base, SchemaVersion)
}

func validTopicBase(topic string) bool {
	if len(topic) == 0 || len(topic) > 240 {
		return false
	}
	for _, char := range topic {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '.' || char == '_' || char == '-' {
			continue
		}
		return false
	}
	return true
}

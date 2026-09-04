package flowcollect

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"google.golang.org/protobuf/proto"
)

type Publisher interface {
	Publish(context.Context, *flowpb.NormalizedRecordBatch) error
	Close() error
}

type KafkaPublisher struct {
	topic     string
	producer  sarama.AsyncProducer
	closed    chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

type publishResult struct{ done chan error }

func NewKafkaPublisher(config KafkaConfig, batchConfig NormalizedBatchCfg, collectorID string) (*KafkaPublisher, error) {
	if len(config.Brokers) == 0 || config.NormalizedTopic == "" {
		return nil, errors.New("Kafka brokers and normalized topic are required")
	}
	saramaConfig, err := buildSaramaConfig(config, batchConfig, collectorID)
	if err != nil {
		return nil, err
	}
	producer, err := sarama.NewAsyncProducer(config.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	publisher := &KafkaPublisher{topic: config.NormalizedTopic, producer: producer, closed: make(chan struct{}), done: make(chan struct{})}
	go publisher.collectResults()
	return publisher, nil
}

func buildSaramaConfig(config KafkaConfig, batchConfig NormalizedBatchCfg, collectorID string) (*sarama.Config, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = collectorID
	saramaConfig.Version = sarama.V2_8_0_0
	saramaConfig.Net.MaxOpenRequests = 1
	saramaConfig.Net.TLS.Enable = config.TLS
	if config.TLS {
		saramaConfig.Net.TLS.Config = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Idempotent = true
	saramaConfig.Producer.Retry.Max = 10
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.Partitioner = sarama.NewManualPartitioner
	saramaConfig.Producer.Flush.Frequency = batchConfig.MaxWait
	saramaConfig.Producer.Compression = sarama.CompressionZSTD
	switch config.Compression {
	case "", "zstd":
	case "gzip":
		saramaConfig.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		saramaConfig.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		saramaConfig.Producer.Compression = sarama.CompressionLZ4
	case "none":
		saramaConfig.Producer.Compression = sarama.CompressionNone
	default:
		return nil, fmt.Errorf("unsupported Kafka compression %q", config.Compression)
	}
	if err := saramaConfig.Validate(); err != nil {
		return nil, fmt.Errorf("validate Kafka producer config: %w", err)
	}
	return saramaConfig, nil
}

func (p *KafkaPublisher) Publish(ctx context.Context, batch *flowpb.NormalizedRecordBatch) error {
	data, err := proto.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshal normalized batch: %w", err)
	}
	result := &publishResult{done: make(chan error, 1)}
	message := &sarama.ProducerMessage{Topic: p.topic, Partition: int32(batch.PhysicalPartition), Key: sarama.ByteEncoder(batch.NormalizedBatchId), Value: sarama.ByteEncoder(data), Timestamp: time.UnixMilli(batch.ReceivedAtUnixMs), Metadata: result}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closed:
		return errors.New("Kafka publisher is closed")
	case p.producer.Input() <- message:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-result.done:
		return err
	}
}

func (p *KafkaPublisher) Close() error {
	p.closeOnce.Do(func() {
		close(p.closed)
		p.producer.AsyncClose()
		<-p.done
	})
	return nil
}

func (p *KafkaPublisher) collectResults() {
	defer close(p.done)
	successes, failures := p.producer.Successes(), p.producer.Errors()
	for successes != nil || failures != nil {
		select {
		case message, ok := <-successes:
			if !ok {
				successes = nil
				continue
			}
			completePublish(message, nil)
		case failure, ok := <-failures:
			if !ok {
				failures = nil
				continue
			}
			completePublish(failure.Msg, failure.Err)
		}
	}
}

func completePublish(message *sarama.ProducerMessage, err error) {
	if message == nil {
		return
	}
	if result, ok := message.Metadata.(*publishResult); ok {
		result.done <- err
	}
}

package flowcollect

import (
	"context"
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
	PublishCollectState(context.Context, *flowpb.CollectState) error
	PublishQualityCheckpoint(context.Context, *flowpb.QualityCheckpoint) error
	PublishDecodeFailure(context.Context, *flowpb.DecodeFailure) error
	PublishQuarantine(context.Context, *flowpb.QuarantineEvent) error
	Close() error
}

type KafkaPublisher struct {
	topic             string
	collectStateTopic string
	decodeDLQTopic    string
	quarantineTopic   string
	producer          sarama.AsyncProducer
	closed            chan struct{}
	done              chan struct{}
	closeOnce         sync.Once
}

type publishResult struct{ done chan error }

func NewKafkaPublisher(config KafkaConfig, batchConfig NormalizedBatchCfg, collectorID string) (*KafkaPublisher, error) {
	if len(config.Brokers) == 0 || config.NormalizedTopic == "" || config.CollectStateTopic == "" || config.DecodeDLQTopic == "" || config.QuarantineTopic == "" {
		return nil, errors.New("all Kafka flow topics are required")
	}
	saramaConfig, err := buildSaramaConfig(config, batchConfig, collectorID)
	if err != nil {
		return nil, err
	}
	producer, err := sarama.NewAsyncProducer(config.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	publisher := &KafkaPublisher{topic: config.NormalizedTopic, collectStateTopic: config.CollectStateTopic, decodeDLQTopic: config.DecodeDLQTopic, quarantineTopic: config.QuarantineTopic, producer: producer, closed: make(chan struct{}), done: make(chan struct{})}
	go publisher.collectResults()
	return publisher, nil
}

func buildSaramaConfig(config KafkaConfig, batchConfig NormalizedBatchCfg, collectorID string) (*sarama.Config, error) {
	saramaConfig, err := buildKafkaClientConfig(config, collectorID)
	if err != nil {
		return nil, err
	}
	saramaConfig.Net.MaxOpenRequests = 1
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Idempotent = true
	saramaConfig.Producer.MaxMessageBytes = collectStateMaxBytes + kafkaRecordOverheadBytes
	saramaConfig.Producer.Retry.Max = 10
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.Partitioner = func(topic string) sarama.Partitioner {
		return &explicitOrHashPartitioner{fallback: sarama.NewHashPartitioner(topic)}
	}
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
	message := &sarama.ProducerMessage{Topic: p.topic, Partition: int32(batch.PhysicalPartition), Key: sarama.ByteEncoder(batch.NormalizedBatchId), Value: sarama.ByteEncoder(data), Timestamp: time.UnixMilli(batch.ReceivedAtUnixMs)}
	return p.publishMessage(ctx, message)
}

func (p *KafkaPublisher) PublishCollectState(ctx context.Context, state *flowpb.CollectState) error {
	if p.collectStateTopic == "" {
		return errors.New("Kafka collect-state topic is required")
	}
	if state == nil {
		return errors.New("collect state is required")
	}
	data, err := proto.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal collect state: %w", err)
	}
	message := &sarama.ProducerMessage{Topic: p.collectStateTopic, Partition: -1, Key: sarama.ByteEncoder(state.StateKey), Value: sarama.ByteEncoder(data), Timestamp: time.UnixMilli(state.ReceivedAtUnixMs)}
	return p.publishMessage(ctx, message)
}

func (p *KafkaPublisher) PublishQualityCheckpoint(ctx context.Context, checkpoint *flowpb.QualityCheckpoint) error {
	if p.collectStateTopic == "" {
		return errors.New("Kafka collect-state topic is required")
	}
	message, err := qualityCheckpointProducerMessage(p.collectStateTopic, checkpoint)
	if err != nil {
		return err
	}
	return p.publishMessage(ctx, message)
}

func qualityCheckpointProducerMessage(topic string, checkpoint *flowpb.QualityCheckpoint) (*sarama.ProducerMessage, error) {
	if topic == "" {
		return nil, errors.New("Kafka collect-state topic is required")
	}
	if err := validateQualityCheckpoint(checkpoint, 0); err != nil {
		return nil, err
	}
	key, err := qualityCheckpointKafkaKey(checkpoint)
	if err != nil {
		return nil, err
	}
	data, err := proto.Marshal(checkpoint)
	if err != nil {
		return nil, fmt.Errorf("marshal quality checkpoint: %w", err)
	}
	if len(data) > collectStateMaxBytes-collectStateHeaderSize {
		return nil, errors.New("quality checkpoint exceeds Kafka value limit")
	}
	return &sarama.ProducerMessage{Topic: topic, Partition: -1, Key: sarama.ByteEncoder(key), Value: sarama.ByteEncoder(data), Timestamp: time.UnixMilli(checkpoint.CommittedAtUnixMs)}, nil
}

func (p *KafkaPublisher) PublishDecodeFailure(ctx context.Context, failure *flowpb.DecodeFailure) error {
	if p.decodeDLQTopic == "" {
		return errors.New("Kafka decode-DLQ topic is required")
	}
	if failure == nil {
		return errors.New("decode failure is required")
	}
	data, err := proto.Marshal(failure)
	if err != nil {
		return fmt.Errorf("marshal decode failure: %w", err)
	}
	message := &sarama.ProducerMessage{Topic: p.decodeDLQTopic, Partition: -1, Key: sarama.ByteEncoder(failure.DatagramId), Value: sarama.ByteEncoder(data), Timestamp: time.UnixMilli(failure.ReceivedAtUnixMs)}
	return p.publishMessage(ctx, message)
}

func (p *KafkaPublisher) PublishQuarantine(ctx context.Context, event *flowpb.QuarantineEvent) error {
	if p.quarantineTopic == "" {
		return errors.New("Kafka quarantine topic is required")
	}
	if event == nil {
		return errors.New("quarantine event is required")
	}
	data, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal quarantine event: %w", err)
	}
	message := &sarama.ProducerMessage{Topic: p.quarantineTopic, Partition: -1, Key: sarama.ByteEncoder(event.EventId), Value: sarama.ByteEncoder(data), Timestamp: time.UnixMilli(event.ReceivedAtUnixMs)}
	return p.publishMessage(ctx, message)
}

func (p *KafkaPublisher) publishMessage(ctx context.Context, message *sarama.ProducerMessage) error {
	result := &publishResult{done: make(chan error, 1)}
	message.Metadata = result
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

type explicitOrHashPartitioner struct {
	fallback sarama.Partitioner
}

func (p *explicitOrHashPartitioner) Partition(message *sarama.ProducerMessage, partitions int32) (int32, error) {
	if message.Partition >= 0 {
		if message.Partition >= partitions {
			return -1, fmt.Errorf("explicit Kafka partition %d is outside topic partition count %d", message.Partition, partitions)
		}
		return message.Partition, nil
	}
	return p.fallback.Partition(message, partitions)
}

func (p *explicitOrHashPartitioner) RequiresConsistency() bool {
	return true
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

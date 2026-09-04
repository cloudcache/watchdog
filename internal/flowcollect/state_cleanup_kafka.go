package flowcollect

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

// KafkaStateTombstoneWriter is a control-plane component. It is intentionally
// separate from KafkaPublisher so flow-collect runtime credentials and APIs do
// not gain compacted-state deletion capability.
type KafkaStateTombstoneWriter struct {
	topic     string
	producer  sarama.AsyncProducer
	closed    chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	now       func() time.Time
}

type stateTombstonePublishCompletion struct {
	position KafkaRecordPosition
	err      error
}

type stateTombstonePublishResult struct {
	done chan stateTombstonePublishCompletion
}

func NewKafkaStateTombstoneWriter(config KafkaConfig, reconcilerID string) (*KafkaStateTombstoneWriter, error) {
	if len(config.Brokers) == 0 || config.CollectStateTopic == "" || reconcilerID == "" {
		return nil, errors.New("Kafka brokers, collect-state topic, and reconciler ID are required")
	}
	saramaConfig, err := buildStateTombstoneKafkaConfig(config, reconcilerID)
	if err != nil {
		return nil, err
	}
	producer, err := sarama.NewAsyncProducer(config.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("create Kafka state-tombstone producer: %w", err)
	}
	return newKafkaStateTombstoneWriter(config.CollectStateTopic, producer)
}

func buildStateTombstoneKafkaConfig(config KafkaConfig, reconcilerID string) (*sarama.Config, error) {
	saramaConfig, err := buildKafkaClientConfig(config, reconcilerID+"-state-cleanup")
	if err != nil {
		return nil, err
	}
	saramaConfig.Net.MaxOpenRequests = 1
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Idempotent = true
	saramaConfig.Producer.Retry.Max = 10
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.Partitioner = sarama.NewHashPartitioner
	saramaConfig.Producer.Compression = sarama.CompressionNone
	if err := saramaConfig.Validate(); err != nil {
		return nil, fmt.Errorf("validate Kafka state-tombstone producer config: %w", err)
	}
	return saramaConfig, nil
}

func newKafkaStateTombstoneWriter(topic string, producer sarama.AsyncProducer) (*KafkaStateTombstoneWriter, error) {
	if topic == "" || producer == nil {
		return nil, errors.New("Kafka state-tombstone topic and producer are required")
	}
	writer := &KafkaStateTombstoneWriter{topic: topic, producer: producer, closed: make(chan struct{}), done: make(chan struct{}), now: time.Now}
	go writer.collectResults()
	return writer, nil
}

func (w *KafkaStateTombstoneWriter) Publish(ctx context.Context, tombstone StateTombstone, createdAt time.Time) (StateTombstoneReceipt, error) {
	if w == nil || ctx == nil {
		return StateTombstoneReceipt{}, errors.New("Kafka state-tombstone writer and context are required")
	}
	message, err := stateTombstoneProducerMessage(w.topic, tombstone, createdAt)
	if err != nil {
		return StateTombstoneReceipt{}, err
	}
	result := &stateTombstonePublishResult{done: make(chan stateTombstonePublishCompletion, 1)}
	message.Metadata = result
	select {
	case <-ctx.Done():
		return StateTombstoneReceipt{}, ctx.Err()
	case <-w.closed:
		return StateTombstoneReceipt{}, errors.New("Kafka state-tombstone writer is closed")
	case w.producer.Input() <- message:
	}
	select {
	case <-ctx.Done():
		return StateTombstoneReceipt{}, ctx.Err()
	case completion := <-result.done:
		if completion.err != nil {
			return StateTombstoneReceipt{}, completion.err
		}
		if completion.position.Partition < 0 || completion.position.Offset < 0 {
			return StateTombstoneReceipt{}, errors.New("Kafka state-tombstone acknowledgement has an invalid position")
		}
		return StateTombstoneReceipt{Position: completion.position, AcknowledgedAt: w.now().UTC()}, nil
	}
}

func (w *KafkaStateTombstoneWriter) Close() error {
	if w == nil {
		return nil
	}
	w.closeOnce.Do(func() {
		close(w.closed)
		w.producer.AsyncClose()
		<-w.done
	})
	return nil
}

func (w *KafkaStateTombstoneWriter) collectResults() {
	defer close(w.done)
	successes, failures := w.producer.Successes(), w.producer.Errors()
	for successes != nil || failures != nil {
		select {
		case message, ok := <-successes:
			if !ok {
				successes = nil
				continue
			}
			completeStateTombstonePublish(message, nil)
		case failure, ok := <-failures:
			if !ok {
				failures = nil
				continue
			}
			completeStateTombstonePublish(failure.Msg, failure.Err)
		}
	}
}

func completeStateTombstonePublish(message *sarama.ProducerMessage, err error) {
	if message == nil {
		return
	}
	if result, ok := message.Metadata.(*stateTombstonePublishResult); ok {
		result.done <- stateTombstonePublishCompletion{position: KafkaRecordPosition{Partition: message.Partition, Offset: message.Offset}, err: err}
	}
}

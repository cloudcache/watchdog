// SPDX-FileCopyrightText: 2022 Free Mobile
// SPDX-License-Identifier: AGPL-3.0-only
//
// Adapted from Akvorado inlet/kafka/root.go.

package flowstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type producerClient interface {
	Produce(context.Context, *kgo.Record, func(*kgo.Record, error))
	Flush(context.Context) error
	Ping(context.Context) error
	Close()
}

type Producer struct {
	client producerClient
	topic  string
	stats  producerStats
	closed atomic.Bool
	once   sync.Once
}

type producerStats struct {
	records              atomic.Uint64
	bytes                atomic.Uint64
	errors               atomic.Uint64
	produceDurationNanos atomic.Uint64
}

type ProducerStats struct {
	Records              uint64
	Bytes                uint64
	Errors               uint64
	BufferedRecords      int64
	ProduceDurationNanos uint64
}

func NewProducer(config ProducerConfig) (*Producer, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	opts, err := config.Kafka.options()
	if err != nil {
		return nil, err
	}
	compression, err := compressionCodec(config.Compression)
	if err != nil {
		return nil, err
	}
	opts = append(opts,
		kgo.MaxBufferedRecords(config.QueueSize),
		kgo.ProducerBatchCompression(compression),
		kgo.RecordPartitioner(kgo.UniformBytesPartitioner(64<<20, true, true, nil)),
	)
	if config.MaxBufferedBytes > 0 {
		// Bound total buffered bytes so a broker outage applies backpressure
		// (Produce blocks, kernel-drop accounting stays honest) instead of growing
		// the producer buffer until the collector OOMs.
		opts = append(opts, kgo.MaxBufferedBytes(config.MaxBufferedBytes))
	}
	if err := kgo.ValidateOpts(opts...); err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return newProducerWithClient(client, topicForVersion(config.Kafka.Topic)), nil
}

func newProducerWithClient(client producerClient, topic string) *Producer {
	return &Producer{client: client, topic: topic}
}

func (p *Producer) Ping(ctx context.Context) error {
	if p == nil || p.client == nil {
		return errors.New("Kafka producer is not initialized")
	}
	return p.client.Ping(ctx)
}

// metadataRequester is satisfied by *kgo.Client. A client that does not
// implement it (a test fake) skips the topic check.
type metadataRequester interface {
	Request(ctx context.Context, req kmsg.Request) (kmsg.Response, error)
}

// VerifyTopic fails fast when the destination topic is absent or has no
// partitions. Ping only proves broker reachability; auto-topic-creation is off
// cluster-side, so producing to a missing or mistyped topic silently drops every
// datagram after franz-go's unknown-topic retries while the process looks
// healthy. A startup metadata check turns that into a visible boot failure.
func (p *Producer) VerifyTopic(ctx context.Context) error {
	if p == nil || p.client == nil {
		return errors.New("Kafka producer is not initialized")
	}
	requester, ok := p.client.(metadataRequester)
	if !ok {
		return nil
	}
	request := kmsg.NewPtrMetadataRequest()
	request.AllowAutoTopicCreation = false
	topic := kmsg.NewMetadataRequestTopic()
	name := p.topic
	topic.Topic = &name
	request.Topics = append(request.Topics, topic)
	raw, err := requester.Request(ctx, request)
	if err != nil {
		return fmt.Errorf("request Kafka topic metadata: %w", err)
	}
	response, ok := raw.(*kmsg.MetadataResponse)
	if !ok {
		return errors.New("unexpected Kafka metadata response")
	}
	for _, reported := range response.Topics {
		if reported.Topic == nil || *reported.Topic != p.topic {
			continue
		}
		if reported.ErrorCode != 0 {
			return fmt.Errorf("Kafka topic %q metadata error code %d", p.topic, reported.ErrorCode)
		}
		if len(reported.Partitions) == 0 {
			return fmt.Errorf("Kafka topic %q has no partitions", p.topic)
		}
		return nil
	}
	return fmt.Errorf("Kafka topic %q does not exist", p.topic)
}

// Send checks ctx before transferring ownership of payload to Kafka. Once
// accepted, delivery is independent of the receiver lifecycle so shutdown can
// stop UDP reads before Close flushes records already buffered by franz-go.
// release is called exactly once after delivery succeeds or permanently fails.
func (p *Producer) Send(ctx context.Context, key, payload []byte, release func(), completion func(error)) error {
	finish := func(err error) {
		if completion != nil {
			completion(err)
		}
		if release != nil {
			release()
		}
	}
	if p == nil || p.client == nil {
		// p.stats is unreachable here; this is a startup wiring error, not a
		// per-datagram runtime path. Still finish so the buffer is released.
		err := errors.New("Kafka producer is not initialized")
		finish(err)
		return err
	}
	// Every synchronous rejection below must count as an error so the
	// received = records + errors + buffered identity holds; otherwise a
	// datagram that never reaches franz-go vanishes from the metrics.
	if p.closed.Load() {
		err := errors.New("Kafka producer is closed")
		p.stats.errors.Add(1)
		finish(err)
		return err
	}
	if len(key) == 0 || len(payload) == 0 {
		err := errors.New("Kafka record key and payload are required")
		p.stats.errors.Add(1)
		finish(err)
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		p.stats.errors.Add(1)
		finish(err)
		return err
	}

	record := &kgo.Record{Topic: p.topic, Key: key, Value: payload}
	started := time.Now()
	// Akvorado uses a producer-owned context here. The caller context only
	// governs admission; retaining it on a buffered record would cancel valid
	// datagrams when the UDP receiver is stopped immediately before flush.
	p.client.Produce(context.Background(), record, func(_ *kgo.Record, err error) {
		p.stats.produceDurationNanos.Add(uint64(time.Since(started)))
		if err != nil {
			p.stats.errors.Add(1)
		} else {
			p.stats.records.Add(1)
			p.stats.bytes.Add(uint64(len(payload)))
		}
		finish(err)
	})
	return nil
}

func (p *Producer) Stats() ProducerStats {
	if p == nil {
		return ProducerStats{}
	}
	stats := ProducerStats{
		Records:              p.stats.records.Load(),
		Bytes:                p.stats.bytes.Load(),
		Errors:               p.stats.errors.Load(),
		ProduceDurationNanos: p.stats.produceDurationNanos.Load(),
	}
	if buffered, ok := p.client.(interface{ BufferedProduceRecords() int64 }); ok {
		stats.BufferedRecords = buffered.BufferedProduceRecords()
	}
	return stats
}

func (p *Producer) Close(ctx context.Context) error {
	if p == nil || p.client == nil {
		return nil
	}
	var flushErr error
	p.once.Do(func() {
		p.closed.Store(true)
		flushErr = p.client.Flush(ctx)
		p.client.Close()
	})
	return flushErr
}

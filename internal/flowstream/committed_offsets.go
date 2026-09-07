// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// CommittedPartitionOffset is Kafka's committed next offset for one consumer
// group partition. Records below NextOffset have been acknowledged by the
// consumer group and are therefore eligible for ingest reconciliation.
type CommittedPartitionOffset struct {
	Partition  int32
	NextOffset uint64
}

type offsetFetchRequestor interface {
	Request(context.Context, kmsg.Request) (kmsg.Response, error)
}

// CommittedOffsetReader reads group offsets without joining the group. It does
// not consume records or alter assignment/commits.
type CommittedOffsetReader struct {
	client offsetFetchRequestor
	topic  string
	ping   func(context.Context) error
	close  func()
}

func NewCommittedOffsetReader(config KafkaConfig) (*CommittedOffsetReader, error) {
	opts, err := config.options()
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka committed-offset client: %w", err)
	}
	return &CommittedOffsetReader{client: client, topic: config.Topic, ping: client.Ping, close: client.Close}, nil
}

// Ready verifies both broker reachability and the exact topic without allowing
// auto-creation. This prevents a typo from being reported as a clean group
// with no committed partitions.
func (r *CommittedOffsetReader) Ready(ctx context.Context) error {
	if err := r.Ping(ctx); err != nil {
		return err
	}
	topic := r.topic
	request := kmsg.NewPtrMetadataRequest()
	request.Version = 9
	request.AllowAutoTopicCreation = false
	request.Topics = []kmsg.MetadataRequestTopic{{Topic: &topic}}
	response, err := request.RequestWith(ctx, r.client)
	if err != nil {
		return fmt.Errorf("fetch Kafka topic metadata: %w", err)
	}
	if response == nil || len(response.Topics) != 1 || response.Topics[0].Topic == nil || *response.Topics[0].Topic != topic {
		return fmt.Errorf("Kafka metadata did not return exact topic %q", topic)
	}
	if err := kerr.ErrorForCode(response.Topics[0].ErrorCode); err != nil {
		return fmt.Errorf("Kafka topic %q metadata: %w", topic, err)
	}
	if len(response.Topics[0].Partitions) == 0 {
		return fmt.Errorf("Kafka topic %q has no partitions", topic)
	}
	for _, partition := range response.Topics[0].Partitions {
		if partition.Partition < 0 || kerr.ErrorForCode(partition.ErrorCode) != nil {
			return fmt.Errorf("Kafka topic %q returned invalid partition metadata", topic)
		}
	}
	return nil
}

func (r *CommittedOffsetReader) Ping(ctx context.Context) error {
	if r == nil || r.client == nil || r.ping == nil {
		return errors.New("Kafka committed-offset reader is not initialized")
	}
	return r.ping(ctx)
}

func (r *CommittedOffsetReader) Close() {
	if r != nil && r.close != nil {
		r.close()
	}
}

// CommittedOffsets returns a stable broker response (RequireStable=true).
// Uncommitted partitions (offset -1) are omitted because no acknowledged
// source window exists for them. A negative value other than -1 is rejected.
func (r *CommittedOffsetReader) CommittedOffsets(ctx context.Context, group, topic string) ([]CommittedPartitionOffset, error) {
	if r == nil || r.client == nil {
		return nil, errors.New("Kafka committed-offset reader is not initialized")
	}
	group, topic = strings.TrimSpace(group), strings.TrimSpace(topic)
	if group == "" || !validTopicBase(topic) {
		return nil, errors.New("Kafka consumer group and exact topic are required")
	}
	request := kmsg.NewPtrOffsetFetchRequest()
	request.Version = 7
	request.Group = group
	request.RequireStable = true
	// Version 7 with nil Topics asks the coordinator for all commits. Filtering
	// the response avoids assuming the topic's current partition count and does
	// not invent offset zero for partitions the group never acknowledged.
	response, err := request.RequestWith(ctx, r.client)
	if err != nil {
		return nil, fmt.Errorf("fetch Kafka committed offsets: %w", err)
	}
	return committedOffsetsFromResponse(response, topic)
}

func committedOffsetsFromResponse(response *kmsg.OffsetFetchResponse, topic string) ([]CommittedPartitionOffset, error) {
	if response == nil {
		return nil, errors.New("Kafka offset fetch returned no response")
	}
	if err := kerr.ErrorForCode(response.ErrorCode); err != nil {
		return nil, fmt.Errorf("Kafka offset fetch group error: %w", err)
	}
	offsets := make([]CommittedPartitionOffset, 0)
	seen := make(map[int32]struct{})
	for _, responseTopic := range response.Topics {
		if responseTopic.Topic != topic {
			continue
		}
		for _, partition := range responseTopic.Partitions {
			if partition.Partition < 0 {
				return nil, fmt.Errorf("Kafka offset fetch returned invalid partition %d", partition.Partition)
			}
			if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
				return nil, fmt.Errorf("Kafka offset fetch partition %d: %w", partition.Partition, err)
			}
			if _, exists := seen[partition.Partition]; exists {
				return nil, fmt.Errorf("Kafka offset fetch returned duplicate partition %d", partition.Partition)
			}
			seen[partition.Partition] = struct{}{}
			switch {
			case partition.Offset == -1:
				continue
			case partition.Offset < -1:
				return nil, fmt.Errorf("Kafka offset fetch partition %d returned invalid offset %d", partition.Partition, partition.Offset)
			default:
				offsets = append(offsets, CommittedPartitionOffset{Partition: partition.Partition, NextOffset: uint64(partition.Offset)})
			}
		}
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i].Partition < offsets[j].Partition })
	return offsets, nil
}

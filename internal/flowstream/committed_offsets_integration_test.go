// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestCommittedOffsetReaderUsesRealKafkaGroupCoordinator(t *testing.T) {
	if os.Getenv("WATCHDOG_FLOW_KAFKA_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_FLOW_KAFKA_INTEGRATION=1 to run")
	}
	brokers := []string{"127.0.0.1:9092"}
	if configured := strings.TrimSpace(os.Getenv("WATCHDOG_FLOW_KAFKA_BROKERS")); configured != "" {
		brokers = strings.Split(configured, ",")
	}
	topic := "watchdog.flow.raw-v1"
	group := fmt.Sprintf("watchdog-flow-offset-reader-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ClientID("watchdog-flow-offset-reader-test"), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		request := kmsg.NewPtrDeleteGroupsRequest()
		request.Groups = []string{group}
		_, _ = request.RequestWith(cleanupCtx, admin)
	})
	produced, err := admin.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: []byte("offset-reader-integration")}).First()
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ClientID("watchdog-flow-offset-reader-consumer"),
		kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().At(produced.Offset)))
	if err != nil {
		t.Fatal(err)
	}
	fetches := consumer.PollRecords(ctx, 1)
	if err := fetches.Err(); err != nil {
		consumer.Close()
		t.Fatal(err)
	}
	var fetched *kgo.Record
	fetches.EachRecord(func(record *kgo.Record) { fetched = record })
	if fetched == nil {
		consumer.Close()
		t.Fatal("consumer returned no record")
	}
	if err := consumer.CommitRecords(ctx, fetched); err != nil {
		consumer.Close()
		t.Fatal(err)
	}
	consumer.Close()

	reader, err := NewCommittedOffsetReader(KafkaConfig{Brokers: brokers, Topic: topic, ClientID: "watchdog-flow-offset-reader-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := reader.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	offsets, err := reader.CommittedOffsets(ctx, group, topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 1 || offsets[0] != (CommittedPartitionOffset{Partition: 0, NextOffset: uint64(fetched.Offset + 1)}) {
		t.Fatalf("offsets = %#v", offsets)
	}
}

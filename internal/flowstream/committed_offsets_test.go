// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestCommittedOffsetsFromResponseFiltersSortsAndOmitsUncommitted(t *testing.T) {
	response := &kmsg.OffsetFetchResponse{Topics: []kmsg.OffsetFetchResponseTopic{
		{Topic: "other", Partitions: []kmsg.OffsetFetchResponseTopicPartition{{Partition: 0, Offset: 99}}},
		{Topic: "watchdog.flow.raw-v1", Partitions: []kmsg.OffsetFetchResponseTopicPartition{
			{Partition: 3, Offset: 42}, {Partition: 1, Offset: -1}, {Partition: 0, Offset: 7},
		}},
	}}
	offsets, err := committedOffsetsFromResponse(response, "watchdog.flow.raw-v1")
	if err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 2 || offsets[0] != (CommittedPartitionOffset{Partition: 0, NextOffset: 7}) ||
		offsets[1] != (CommittedPartitionOffset{Partition: 3, NextOffset: 42}) {
		t.Fatalf("unexpected offsets: %#v", offsets)
	}
}

func TestCommittedOffsetsFromResponseRejectsBrokerErrorsAndInvalidIdentity(t *testing.T) {
	for name, response := range map[string]*kmsg.OffsetFetchResponse{
		"group":              {ErrorCode: kerr.GroupAuthorizationFailed.Code},
		"partition":          {Topics: []kmsg.OffsetFetchResponseTopic{{Topic: "t", Partitions: []kmsg.OffsetFetchResponseTopicPartition{{Partition: 0, Offset: 1, ErrorCode: kerr.UnknownTopicOrPartition.Code}}}}},
		"negative partition": {Topics: []kmsg.OffsetFetchResponseTopic{{Topic: "t", Partitions: []kmsg.OffsetFetchResponseTopicPartition{{Partition: -1, Offset: 1}}}}},
		"negative offset":    {Topics: []kmsg.OffsetFetchResponseTopic{{Topic: "t", Partitions: []kmsg.OffsetFetchResponseTopicPartition{{Partition: 0, Offset: -2}}}}},
		"duplicate":          {Topics: []kmsg.OffsetFetchResponseTopic{{Topic: "t", Partitions: []kmsg.OffsetFetchResponseTopicPartition{{Partition: 0, Offset: 1}, {Partition: 0, Offset: 2}}}}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := committedOffsetsFromResponse(response, "t"); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

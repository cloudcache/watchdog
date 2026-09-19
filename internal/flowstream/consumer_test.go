// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeConsumerClient struct {
	mu        sync.Mutex
	polled    bool
	fetches   kgo.Fetches
	committed []*kgo.Record
	commitErr error
	commits   int
	allowed   int
	closed    bool
}

func (f *fakeConsumerClient) PollFetches(context.Context) kgo.Fetches {
	if f.polled {
		return nil
	}
	f.polled = true
	return f.fetches
}

func (f *fakeConsumerClient) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits++
	f.committed = append(f.committed, records...)
	return f.commitErr
}

func (f *fakeConsumerClient) AllowRebalance()         { f.allowed++ }
func (f *fakeConsumerClient) CloseAllowingRebalance() { f.closed = true }
func (f *fakeConsumerClient) Ping(context.Context) error {
	return nil
}

func testFetches(records ...*kgo.Record) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "watchdog.flow.raw-v1", Partitions: []kgo.FetchPartition{{Partition: 3, Records: records}}}}}}
}

func testPartitionFetches(partitions ...kgo.FetchPartition) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "watchdog.flow.raw-v1", Partitions: partitions}}}}
}

func TestConsumerMarksOnlySuccessfullyProcessedRecords(t *testing.T) {
	first := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 10, Value: []byte("first")}
	second := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 11, Value: []byte("second")}
	client := &fakeConsumerClient{fetches: testFetches(first, second)}
	consumer := newConsumerWithClient(client, nil)
	wantErr := errors.New("decode failed")
	err := consumer.Run(context.Background(), func(_ context.Context, record *kgo.Record) error {
		if record == second {
			return wantErr
		}
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(client.committed) != 1 || client.committed[0] != first {
		t.Fatalf("failed record was committed: %+v", client.committed)
	}
	if !client.closed || client.commits != 1 || client.allowed != 1 {
		t.Fatalf("unexpected lifecycle: %+v", client)
	}
	if got := consumer.Stats(); got.Records != 1 || got.Bytes != uint64(len(first.Value)) || got.Errors != 1 {
		t.Fatalf("unexpected stats: %+v", got)
	}
}

func TestConsumerProcessesAndMarksFetchedRecords(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	first := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 10, Value: []byte("first")}
	second := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 11, Value: []byte("second")}
	client := &fakeConsumerClient{fetches: testFetches(first, second)}
	consumer := newConsumerWithClient(client, nil)
	processed := 0
	err := consumer.Run(ctx, func(_ context.Context, record *kgo.Record) error {
		processed++
		if record == second {
			cancel()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if processed != 2 || len(client.committed) != 1 || client.committed[0] != second {
		t.Fatalf("processed=%d committed=%+v", processed, client.committed)
	}
	if got := consumer.Stats(); got.Records != 2 || got.Bytes != uint64(len(first.Value)+len(second.Value)) || got.Errors != 0 {
		t.Fatalf("unexpected stats: %+v", got)
	}
	if client.commits != 1 {
		t.Fatalf("commits=%d, want one exact durable poll commit", client.commits)
	}
}

func TestConsumerReturnsDurableOffsetCommitFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	want := errors.New("coordinator unavailable")
	record := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 10, Value: []byte("first")}
	client := &fakeConsumerClient{fetches: testFetches(record), commitErr: want}
	consumer := newConsumerWithClient(client, nil)
	err := consumer.Run(ctx, func(context.Context, *kgo.Record) error { return nil })
	if !errors.Is(err, want) {
		t.Fatalf("error=%v, want commit failure %v", err, want)
	}
	if len(client.committed) != 1 || client.allowed != 1 || client.commits != 1 {
		t.Fatalf("unexpected lifecycle: committed=%d allowed=%d commits=%d", len(client.committed), client.allowed, client.commits)
	}
}

func TestConsumerSerializesEachPartitionAndRunsPartitionsConcurrently(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p0first := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 0, Offset: 10}
	p0second := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 0, Offset: 11}
	p1first := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 1, Offset: 20}
	client := &fakeConsumerClient{fetches: testPartitionFetches(
		kgo.FetchPartition{Partition: 0, Records: []*kgo.Record{p0first, p0second}},
		kgo.FetchPartition{Partition: 1, Records: []*kgo.Record{p1first}},
	)}
	consumer := newConsumerWithClient(client, nil)
	p0Started := make(chan struct{})
	p1Started := make(chan struct{})
	var processed atomic.Uint32
	var mu sync.Mutex
	order := map[int32][]int64{}
	err := consumer.Run(ctx, func(_ context.Context, record *kgo.Record) error {
		switch record {
		case p0first:
			close(p0Started)
			<-p1Started
		case p1first:
			close(p1Started)
			<-p0Started
		}
		mu.Lock()
		order[record.Partition] = append(order[record.Partition], record.Offset)
		mu.Unlock()
		if processed.Add(1) == 3 {
			cancel()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(order[0]) != 2 || order[0][0] != 10 || order[0][1] != 11 || len(order[1]) != 1 || order[1][0] != 20 {
		t.Fatalf("partition order=%v", order)
	}
	if len(client.committed) != 2 || client.committed[0].Partition == client.committed[1].Partition {
		t.Fatalf("committed partition watermarks=%+v", client.committed)
	}
}

func TestConsumerPartitionFailureDoesNotAbortHealthyPartitions(t *testing.T) {
	// One partition's ClickHouse failure must not cancel a healthy sibling's
	// in-flight work: the healthy partition runs to completion and marks its
	// records (committed on shutdown); only the failed partition replays.
	p0 := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 0, Offset: 10}
	p1 := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 1, Offset: 20, Value: []byte("healthy")}
	client := &fakeConsumerClient{fetches: testPartitionFetches(
		kgo.FetchPartition{Partition: 0, Records: []*kgo.Record{p0}},
		kgo.FetchPartition{Partition: 1, Records: []*kgo.Record{p1}},
	)}
	consumer := newConsumerWithClient(client, nil)
	p0errored := make(chan struct{})
	wantErr := errors.New("ClickHouse rejected partition 0")
	err := consumer.RunPartitionBatches(context.Background(), func(ctx context.Context, records []*kgo.Record) error {
		if records[0].Partition == 0 {
			close(p0errored)
			return wantErr
		}
		<-p0errored      // partition 0 has already failed
		return ctx.Err() // must be nil: a sibling failure no longer cancels this partition
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("want partition 0 failure surfaced, got %v", err)
	}
	if len(client.committed) != 1 || client.committed[0] != p1 {
		t.Fatalf("healthy partition 1 record was not committed: %+v", client.committed)
	}
}

func TestConsumerPartitionBatchMarksOnlyAfterDurableSuccess(t *testing.T) {
	first := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 10, Value: []byte("first")}
	second := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 11, Value: []byte("second")}
	t.Run("success", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		client := &fakeConsumerClient{fetches: testFetches(first, second)}
		consumer := newConsumerWithClient(client, nil)
		calls := 0
		err := consumer.RunPartitionBatches(ctx, func(_ context.Context, records []*kgo.Record) error {
			calls++
			if len(records) != 2 || records[0] != first || records[1] != second {
				t.Fatalf("unexpected partition batch: %+v", records)
			}
			cancel()
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if calls != 1 || len(client.committed) != 1 || client.committed[0] != second {
			t.Fatalf("calls=%d committed=%+v", calls, client.committed)
		}
	})

	t.Run("durable failure", func(t *testing.T) {
		client := &fakeConsumerClient{fetches: testFetches(first, second)}
		consumer := newConsumerWithClient(client, nil)
		want := errors.New("ClickHouse unavailable")
		err := consumer.RunPartitionBatches(context.Background(), func(context.Context, []*kgo.Record) error { return want })
		if !errors.Is(err, want) {
			t.Fatalf("error=%v", err)
		}
		if len(client.committed) != 0 {
			t.Fatalf("failed durable batch committed %d records", len(client.committed))
		}
	})
}

func TestConsumerPartitionBatchCommitsDurableChunksBeforeLaterFailure(t *testing.T) {
	records := []*kgo.Record{
		{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 10},
		{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 11},
		{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 12},
		{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 13},
		{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 14},
	}
	client := &fakeConsumerClient{fetches: testFetches(records...)}
	consumer := newConsumerWithClient(client, nil)
	consumer.partitionBatchRecords = 2
	want := errors.New("ClickHouse disk full")
	calls := 0
	err := consumer.RunPartitionBatches(context.Background(), func(_ context.Context, chunk []*kgo.Record) error {
		calls++
		if calls == 3 {
			return want
		}
		if len(chunk) != 2 {
			t.Fatalf("durable chunk size=%d, want 2", len(chunk))
		}
		return nil
	})
	if !errors.Is(err, want) {
		t.Fatalf("error=%v, want %v", err, want)
	}
	if calls != 3 || len(client.committed) != 1 || client.committed[0] != records[3] {
		t.Fatalf("calls=%d committed=%+v, want final durable record offset 13", calls, client.committed)
	}
	if client.commits != 1 {
		t.Fatalf("durable commits=%d, want 1", client.commits)
	}
}

func TestRewindAssignedOffsetsRebuildsTemplateStateWithoutChangingResetSentinels(t *testing.T) {
	offsets := map[string]map[int32]kgo.Offset{
		"watchdog.flow.raw-v1": {
			0: kgo.NewOffset().At(2_000).WithEpoch(7),
			1: kgo.NewOffset().AtStart(),
			2: kgo.NewOffset().AtEnd(),
		},
	}
	rewound := rewindAssignedOffsets(offsets, 1_000)
	if got := rewound["watchdog.flow.raw-v1"][0].String(); got != "{2000-1000 e-1 ce0}" {
		t.Fatalf("absolute offset=%s", got)
	}
	if got := rewound["watchdog.flow.raw-v1"][1].String(); got != offsets["watchdog.flow.raw-v1"][1].String() {
		t.Fatalf("start sentinel changed to %s", got)
	}
	if got := rewound["watchdog.flow.raw-v1"][2].String(); got != offsets["watchdog.flow.raw-v1"][2].String() {
		t.Fatalf("end sentinel changed to %s", got)
	}
	if offsets["watchdog.flow.raw-v1"][0].String() == rewound["watchdog.flow.raw-v1"][0].String() {
		t.Fatal("input offset map was mutated")
	}
}

func TestReplayWatermarkNeverRegressesCommittedOffset(t *testing.T) {
	watermarks := &replayWatermarks{floors: make(map[replayPartition]int64)}
	original := map[string]map[int32]kgo.Offset{"watchdog.flow.raw-v1": {3: kgo.NewOffset().At(2_000).WithEpoch(7)}}
	rewound := watermarks.rewind(original, 1_000)
	if got := rewound["watchdog.flow.raw-v1"][3].String(); got != "{2000-1000 e-1 ce0}" {
		t.Fatalf("rewound=%s", got)
	}
	for _, offset := range []int64{1_000, 1_500, 1_998} {
		if watermarks.committable(&kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: offset}) {
			t.Fatalf("replay offset %d could regress committed offset", offset)
		}
	}
	if !watermarks.committable(&kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 1_999}) {
		t.Fatal("record reaching the original commit watermark was not committable")
	}
	if !watermarks.committable(&kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 2_000}) {
		t.Fatal("records after the original commit watermark remained blocked")
	}
}

func TestConsumerProcessesTemplateReplayButMarksOnlyAtOriginalWatermark(t *testing.T) {
	first := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 1_998}
	second := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 1_999}
	client := &fakeConsumerClient{fetches: testFetches(first, second)}
	watermarks := &replayWatermarks{floors: map[replayPartition]int64{{topic: first.Topic, partition: first.Partition}: 2_000}}
	consumer := newConsumerWithClientAndWatermarks(client, watermarks, nil)
	ctx, cancel := context.WithCancel(context.Background())
	err := consumer.RunPartitionBatches(ctx, func(context.Context, []*kgo.Record) error { cancel(); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(client.committed) != 1 || client.committed[0] != second {
		t.Fatalf("committed replay records=%+v", client.committed)
	}
}

func TestConsumerStatsTrackAssignmentLagAndLossWithoutPartitionLabels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	first := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 10}
	second := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 11}
	client := &fakeConsumerClient{fetches: testPartitionFetches(kgo.FetchPartition{
		Partition: 3, HighWatermark: 20, Records: []*kgo.Record{first, second},
	})}
	consumer := newConsumerWithClient(client, nil)
	consumer.stats.assign(map[string][]int32{"watchdog.flow.raw-v1": {3, 4}})
	if err := consumer.RunPartitionBatches(ctx, func(context.Context, []*kgo.Record) error {
		cancel()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	stats := consumer.Stats()
	if stats.Rebalances != 1 || stats.AssignedPartitions != 2 || stats.LagKnownPartitions != 1 || stats.LagRecords != 8 {
		t.Fatalf("unexpected assigned/lag stats: %+v", stats)
	}
	consumer.stats.revoke(map[string][]int32{"watchdog.flow.raw-v1": {3}}, true)
	stats = consumer.Stats()
	if stats.LostPartitions != 1 || stats.AssignedPartitions != 1 || stats.LagKnownPartitions != 0 || stats.LagRecords != 0 {
		t.Fatalf("unexpected revoked/lost stats: %+v", stats)
	}
}

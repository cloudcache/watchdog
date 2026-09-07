// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowworker"
)

type funcBlockInserter struct {
	fn func() error
}

func (f *funcBlockInserter) InsertFlowBlock(context.Context, PreparedBlock) error { return f.fn() }

func TestWriterGivesUpAfterRetryBudgetWithoutDroppingBlock(t *testing.T) {
	// A persistently failing partition must not retry forever (which would hang
	// the consumer's per-fetch barrier and block rebalance). The block is
	// surfaced as an error for the normal unmarked->replay path, not counted as
	// durable and not dropped.
	sentinel := errors.New("ClickHouse persistently unavailable")
	inserter := &fakeBlockInserter{failures: 1 << 30, err: sentinel}
	writer, err := NewWriter(inserter, WriterConfig{RetryInitial: time.Millisecond, RetryMax: time.Millisecond, RetryMaxElapsed: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Write(context.Background(), []*flowworker.EnrichedBatch{testEnrichedBatch(10, testEnrichedRecord(1, 100, 1_000))})
	if !errors.Is(err, sentinel) {
		t.Fatalf("want the underlying failure wrapped, got %v", err)
	}
	stats := writer.Stats()
	if stats.BudgetExceeded != 1 {
		t.Fatalf("BudgetExceeded=%d, want 1", stats.BudgetExceeded)
	}
	if stats.RetryingNow != 0 {
		t.Fatalf("RetryingNow gauge leaked after give-up: %d", stats.RetryingNow)
	}
	if stats.Blocks != 0 {
		t.Fatalf("a block that exhausted its budget must not count as durable: Blocks=%d", stats.Blocks)
	}
}

func TestWriterRetryingGaugeReflectsInFlightRetries(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var attempts atomic.Int32
	inserter := &funcBlockInserter{fn: func() error {
		if attempts.Add(1) == 1 {
			return errors.New("transient ClickHouse failure")
		}
		close(entered) // second attempt: the gauge was already incremented
		<-release
		return nil
	}}
	writer, err := NewWriter(inserter, WriterConfig{RetryInitial: time.Millisecond, RetryMax: time.Millisecond, RetryMaxElapsed: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- writer.Write(context.Background(), []*flowworker.EnrichedBatch{testEnrichedBatch(10, testEnrichedRecord(1, 100, 1_000))})
	}()
	<-entered
	if g := writer.Stats().RetryingNow; g != 1 {
		t.Fatalf("RetryingNow=%d while a block is retrying, want 1", g)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if g := writer.Stats().RetryingNow; g != 0 {
		t.Fatalf("RetryingNow=%d after the retry succeeded, want 0", g)
	}
}

type fakeBlockInserter struct {
	mu       sync.Mutex
	failures int
	err      error
	ids      []string
}

func (f *fakeBlockInserter) InsertFlowBlock(_ context.Context, block PreparedBlock) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, blockDeduplicationToken(block))
	if f.failures > 0 {
		f.failures--
		return f.err
	}
	return nil
}

func TestWriterRetriesTheSameStableBlock(t *testing.T) {
	inserter := &fakeBlockInserter{failures: 2, err: errors.New("temporary ClickHouse failure")}
	writer, err := NewWriter(inserter, WriterConfig{RetryInitial: time.Nanosecond, RetryMax: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(context.Background(), []*flowworker.EnrichedBatch{testEnrichedBatch(10, testEnrichedRecord(1, 100, 1_000))}); err != nil {
		t.Fatal(err)
	}
	if len(inserter.ids) != 3 || inserter.ids[0] != inserter.ids[1] || inserter.ids[1] != inserter.ids[2] {
		t.Fatalf("retry changed block identity: %v", inserter.ids)
	}
	if stats := writer.Stats(); stats.InsertAttempts != 3 || stats.InsertErrors != 2 || stats.RetryableErrors != 2 ||
		stats.Retries != 2 || stats.Blocks != 1 || stats.Rows != 1 || stats.PermanentErrors != 0 {
		t.Fatalf("unexpected writer stats: %+v", stats)
	}
}

func TestWriterStopsOnPermanentErrorAndContext(t *testing.T) {
	want := errors.New("schema mismatch")
	permanentInserter := &fakeBlockInserter{failures: 1, err: Permanent(want)}
	writer, err := NewWriter(permanentInserter, WriterConfig{RetryInitial: time.Nanosecond, RetryMax: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Write(context.Background(), []*flowworker.EnrichedBatch{testEnrichedBatch(10, testEnrichedRecord(1, 100, 1_000))})
	if !errors.Is(err, want) || len(permanentInserter.ids) != 1 {
		t.Fatalf("permanent error=%v attempts=%d", err, len(permanentInserter.ids))
	}
	if stats := writer.Stats(); stats.InsertAttempts != 1 || stats.InsertErrors != 1 || stats.PermanentErrors != 1 || stats.Retries != 0 {
		t.Fatalf("unexpected permanent writer stats: %+v", stats)
	}

	temporary := errors.New("unavailable")
	contextInserter := &fakeBlockInserter{failures: 100, err: temporary}
	writer, err = NewWriter(contextInserter, WriterConfig{RetryInitial: time.Second, RetryMax: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = writer.Write(ctx, []*flowworker.EnrichedBatch{testEnrichedBatch(10, testEnrichedRecord(1, 100, 1_000))})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, temporary) || len(contextInserter.ids) != 1 {
		t.Fatalf("context error=%v attempts=%d", err, len(contextInserter.ids))
	}
	if stats := writer.Stats(); stats.InsertAttempts != 1 || stats.InsertErrors != 1 || stats.RetryableErrors != 1 || stats.Retries != 0 {
		t.Fatalf("unexpected cancelled writer stats: %+v", stats)
	}
}

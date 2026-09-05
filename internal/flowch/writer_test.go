// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowworker"
)

type fakeBlockInserter struct {
	mu       sync.Mutex
	failures int
	err      error
	ids      [][32]byte
}

func (f *fakeBlockInserter) InsertFlowBlock(_ context.Context, block PreparedBlock) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, block.ID)
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
		t.Fatalf("retry changed block identity: %x", inserter.ids)
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

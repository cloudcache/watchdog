// SPDX-FileCopyrightText: 2025 Free Mobile
// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/cloudcache/watchdog/internal/flowworker"
)

type BlockInserter interface {
	InsertFlowBlock(context.Context, PreparedBlock) error
}

type WriterConfig struct {
	Limits       BatchLimits
	RetryInitial time.Duration
	RetryMax     time.Duration
	// RetryMaxElapsed bounds the total wall-clock a single block may retry
	// before it gives up for this cycle. It exists so one partition whose
	// ClickHouse insert is persistently failing cannot retry forever and hang
	// the consumer's per-fetch barrier — which would stall every healthy
	// partition and block group rebalance indefinitely. Exceeding the budget is
	// not a drop: the block's records are left unmarked and replay through the
	// consumer's normal restart path (ADR lag-absorb), so nothing is lost. The
	// value trades head-of-line blocking of healthy partitions during a stuck
	// event (up to one budget) against restart churn on a ClickHouse outage
	// longer than the budget; operators can tune it.
	RetryMaxElapsed time.Duration
}

type Writer struct {
	inserter BlockInserter
	config   WriterConfig
	stats    writerStats
}

type writerStats struct {
	insertAttempts      atomic.Uint64
	insertErrors        atomic.Uint64
	retryableErrors     atomic.Uint64
	permanentErrors     atomic.Uint64
	retries             atomic.Uint64
	blocks              atomic.Uint64
	rows                atomic.Uint64
	insertDurationNanos atomic.Uint64
	retryingNow         atomic.Int64
	budgetExceeded      atomic.Uint64
}

type WriterStats struct {
	InsertAttempts      uint64
	InsertErrors        uint64
	RetryableErrors     uint64
	PermanentErrors     uint64
	Retries             uint64
	Blocks              uint64
	Rows                uint64
	InsertDurationNanos uint64
	// RetryingNow is a gauge of blocks currently in retry (a partition insert
	// that has failed at least once and is grinding). A sustained non-zero value
	// is the "stuck partition" signal. BudgetExceeded counts blocks that hit
	// RetryMaxElapsed and were surfaced for replay.
	RetryingNow    int64
	BudgetExceeded uint64
}

type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

func NewWriter(inserter BlockInserter, config WriterConfig) (*Writer, error) {
	if inserter == nil {
		return nil, errors.New("ClickHouse flow block inserter is required")
	}
	limits, err := normalizeLimits(config.Limits)
	if err != nil {
		return nil, err
	}
	config.Limits = limits
	if config.RetryInitial == 0 {
		config.RetryInitial = 20 * time.Millisecond
	}
	if config.RetryMax == 0 {
		config.RetryMax = 30 * time.Second
	}
	if config.RetryMaxElapsed == 0 {
		config.RetryMaxElapsed = 2 * time.Minute
	}
	if config.RetryInitial < 0 || config.RetryMax < config.RetryInitial || config.RetryMax > time.Minute {
		return nil, errors.New("ClickHouse retry durations are invalid")
	}
	if config.RetryMaxElapsed < config.RetryMax || config.RetryMaxElapsed > time.Hour {
		return nil, errors.New("ClickHouse retry budget is invalid")
	}
	return &Writer{inserter: inserter, config: config}, nil
}

// Write is synchronous: nil means all record blocks and their receipts are
// durable. The Kafka consumer may mark the partition offsets only afterwards.
func (w *Writer) Write(ctx context.Context, batches []*flowworker.EnrichedBatch) error {
	if w == nil || w.inserter == nil {
		return errors.New("ClickHouse flow writer is not initialized")
	}
	blocks, err := PrepareBlocks(batches, w.config.Limits)
	if err != nil {
		return err
	}
	for index := range blocks {
		if err := w.insertWithRetry(ctx, blocks[index]); err != nil {
			return fmt.Errorf("insert ClickHouse flow block %x: %w", blocks[index].ID[:8], err)
		}
	}
	return nil
}

func (w *Writer) insertWithRetry(ctx context.Context, block PreparedBlock) error {
	delay := w.config.RetryInitial
	deadline := time.Now().Add(w.config.RetryMaxElapsed)
	retrying := false
	defer func() {
		if retrying {
			w.stats.retryingNow.Add(-1)
		}
	}()
	for {
		started := time.Now()
		w.stats.insertAttempts.Add(1)
		err := w.inserter.InsertFlowBlock(ctx, block)
		w.stats.insertDurationNanos.Add(uint64(time.Since(started)))
		if err == nil {
			w.stats.blocks.Add(1)
			w.stats.rows.Add(uint64(len(block.Records)))
			return nil
		}
		w.stats.insertErrors.Add(1)
		var permanent *PermanentError
		if errors.As(err, &permanent) {
			w.stats.permanentErrors.Add(1)
			return err
		}
		w.stats.retryableErrors.Add(1)
		if ctx.Err() != nil {
			return errors.Join(err, ctx.Err())
		}
		// Bound total retry time so a persistently failing partition cannot hang
		// the consumer's per-fetch barrier forever and block rebalance. The block
		// is surfaced for the normal unmarked->replay path; nothing is dropped.
		if time.Now().After(deadline) {
			w.stats.budgetExceeded.Add(1)
			return fmt.Errorf("ClickHouse flow block insert exceeded %s retry budget: %w", w.config.RetryMaxElapsed, err)
		}
		if !retrying {
			retrying = true
			w.stats.retryingNow.Add(1)
		}
		wait := jitter(delay)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return errors.Join(err, ctx.Err())
		case <-timer.C:
		}
		w.stats.retries.Add(1)
		if delay < w.config.RetryMax {
			if delay > w.config.RetryMax/2 {
				delay = w.config.RetryMax
			} else {
				delay *= 2
			}
		}
	}
}

func (w *Writer) Stats() WriterStats {
	if w == nil {
		return WriterStats{}
	}
	return WriterStats{
		InsertAttempts: w.stats.insertAttempts.Load(), InsertErrors: w.stats.insertErrors.Load(),
		RetryableErrors: w.stats.retryableErrors.Load(), PermanentErrors: w.stats.permanentErrors.Load(),
		Retries: w.stats.retries.Load(), Blocks: w.stats.blocks.Load(), Rows: w.stats.rows.Load(),
		InsertDurationNanos: w.stats.insertDurationNanos.Load(),
		RetryingNow:         w.stats.retryingNow.Load(), BudgetExceeded: w.stats.budgetExceeded.Load(),
	}
}

func jitter(delay time.Duration) time.Duration {
	if delay <= 1 {
		return delay
	}
	// Full synchronization avoidance without extending beyond the configured
	// cap: wait uniformly in [delay/2, delay].
	half := delay / 2
	return half + time.Duration(rand.Int64N(int64(delay-half)+1))
}

// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowtombstone"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

type recordingEnricher struct {
	results map[*flowworker.RecordBatch]*flowworker.EnrichedBatch
	failOn  *flowworker.RecordBatch
	failErr error
}

func (e *recordingEnricher) EnrichBatch(batch *flowworker.RecordBatch) (*flowworker.EnrichedBatch, error) {
	if batch == e.failOn {
		if e.failErr != nil {
			return nil, e.failErr
		}
		return nil, errors.New("snapshot unavailable")
	}
	return e.results[batch], nil
}

type recordingBatchWriter struct {
	batches []*flowworker.EnrichedBatch
	err     error
}

type recordingQuarantineWriter struct {
	batch    *flowworker.RecordBatch
	decision flowtombstone.Decision
	err      error
}

func (w *recordingQuarantineWriter) WriteQuarantined(_ context.Context, batch *flowworker.RecordBatch, decision flowtombstone.Decision) error {
	w.batch, w.decision = batch, decision
	return w.err
}

func (w *recordingBatchWriter) Write(_ context.Context, batches []*flowworker.EnrichedBatch) error {
	w.batches = append([]*flowworker.EnrichedBatch(nil), batches...)
	return w.err
}

func TestPipelineEnrichesWholePartitionBeforeDurableWrite(t *testing.T) {
	first := &flowworker.RecordBatch{KafkaOffset: 10}
	second := &flowworker.RecordBatch{KafkaOffset: 11}
	firstEnriched := &flowworker.EnrichedBatch{KafkaOffset: 10, Records: []flowworker.EnrichedRecord{
		{EstimatedValid: false},
		{EstimatedValid: true, QualityFlags: flowworker.QualitySamplingConflict},
	}}
	secondEnriched := &flowworker.EnrichedBatch{KafkaOffset: 11, Records: []flowworker.EnrichedRecord{{EstimatedValid: true}}}
	enricher := &recordingEnricher{results: map[*flowworker.RecordBatch]*flowworker.EnrichedBatch{first: firstEnriched, second: secondEnriched}}
	writer := &recordingBatchWriter{}
	pipeline := &Pipeline{enricher: enricher, writer: writer}
	if err := pipeline.Handle(context.Background(), []*flowworker.RecordBatch{first, second}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(writer.batches, []*flowworker.EnrichedBatch{firstEnriched, secondEnriched}) {
		t.Fatalf("write order changed: %+v", writer.batches)
	}
	if stats := pipeline.Stats(); stats.Records != 3 || stats.SamplingUnknown != 1 || stats.SamplingConflict != 1 || stats.SnapshotMiss != 0 {
		t.Fatalf("unexpected pipeline stats: %+v", stats)
	}
}

func TestPipelineDoesNotWritePartiallyEnrichedGroup(t *testing.T) {
	first := &flowworker.RecordBatch{KafkaOffset: 10}
	second := &flowworker.RecordBatch{KafkaOffset: 11}
	enricher := &recordingEnricher{
		results: map[*flowworker.RecordBatch]*flowworker.EnrichedBatch{first: {KafkaOffset: 10}},
		failOn:  second,
		failErr: &flowworker.VersionBlockedError{
			Dependency: "geo", Cause: flowworker.ErrVersionUnavailable,
		},
	}
	writer := &recordingBatchWriter{}
	pipeline := &Pipeline{enricher: enricher, writer: writer}
	err := pipeline.Handle(context.Background(), []*flowworker.RecordBatch{first, second})
	if err == nil || len(writer.batches) != 0 {
		t.Fatalf("error=%v writes=%d, want fail before first write", err, len(writer.batches))
	}
	if stats := pipeline.Stats(); stats.SnapshotMiss != 1 || stats.Records != 0 {
		t.Fatalf("unexpected failed enrichment stats: %+v", stats)
	}
}

func TestPipelinePropagatesDurableFailure(t *testing.T) {
	batch := &flowworker.RecordBatch{KafkaOffset: 10}
	want := errors.New("ClickHouse unavailable")
	pipeline := &Pipeline{
		enricher: &recordingEnricher{results: map[*flowworker.RecordBatch]*flowworker.EnrichedBatch{batch: {KafkaOffset: 10}}},
		writer:   &recordingBatchWriter{err: want},
	}
	if err := pipeline.Handle(context.Background(), []*flowworker.RecordBatch{batch}); !errors.Is(err, want) {
		t.Fatalf("error=%v", err)
	}
	if stats := pipeline.Stats(); stats.Records != 0 || stats.SamplingUnknown != 0 || stats.SamplingConflict != 0 {
		t.Fatalf("failed durable write changed committed stats: %+v", stats)
	}
}

func TestPipelineQuarantinesTombstonedDatagramBeforeEnrichment(t *testing.T) {
	publishedAt := time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC)
	barrier, err := flowtombstone.Advance(nil, time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), "barrier_a", 1, publishedAt)
	if err != nil {
		t.Fatal(err)
	}
	guard, _ := flowtombstone.NewGuard(&barrier)
	batch := &flowworker.RecordBatch{MessageDisposition: flowworker.MessageDispositionPersisted, Records: []*flowworker.Record{{EventTimeUnixMS: time.Date(2026, 8, 1, 23, 0, 0, 0, time.UTC).UnixMilli()}}}
	normal := &recordingBatchWriter{}
	quarantine := &recordingQuarantineWriter{}
	pipeline := &Pipeline{
		enricher: &recordingEnricher{failOn: batch, failErr: errors.New("must not enrich tombstoned data")},
		writer:   normal, quarantine: quarantine, barrier: guard,
	}
	if err := pipeline.Handle(context.Background(), []*flowworker.RecordBatch{batch}); err != nil {
		t.Fatal(err)
	}
	if quarantine.batch != batch || quarantine.decision.Revision != 1 || len(normal.batches) != 0 {
		t.Fatalf("quarantine=%+v normal=%d", quarantine.decision, len(normal.batches))
	}
	if stats := pipeline.Stats(); stats.QuarantinedDatagrams != 1 || stats.QuarantinedRecords != 1 || stats.Records != 0 {
		t.Fatalf("unexpected quarantine stats: %+v", stats)
	}
}

func TestPipelineQuarantineFailureLeavesBatchRetryable(t *testing.T) {
	barrier, _ := flowtombstone.Advance(nil, time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), "barrier_a", 1, time.Now().UTC())
	guard, _ := flowtombstone.NewGuard(&barrier)
	batch := &flowworker.RecordBatch{MessageDisposition: flowworker.MessageDispositionPersisted, Records: []*flowworker.Record{{EventTimeUnixMS: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC).UnixMilli()}}}
	want := errors.New("quarantine unavailable")
	pipeline := &Pipeline{enricher: &recordingEnricher{}, writer: &recordingBatchWriter{}, quarantine: &recordingQuarantineWriter{err: want}, barrier: guard}
	if err := pipeline.Handle(context.Background(), []*flowworker.RecordBatch{batch}); !errors.Is(err, want) {
		t.Fatalf("error=%v", err)
	}
	if stats := pipeline.Stats(); stats.QuarantinedDatagrams != 0 || stats.Records != 0 {
		t.Fatalf("failed quarantine changed durable stats: %+v", stats)
	}
}

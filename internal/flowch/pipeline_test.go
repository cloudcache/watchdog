// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"reflect"
	"testing"

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
			Dependency: "geo", TenantID: "tenant-a", Cause: flowworker.ErrVersionUnavailable,
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

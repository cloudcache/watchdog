// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cloudcache/watchdog/internal/flowtombstone"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

type batchEnricher interface {
	EnrichBatch(*flowworker.RecordBatch) (*flowworker.EnrichedBatch, error)
}

type enrichedBatchWriter interface {
	Write(context.Context, []*flowworker.EnrichedBatch) error
}

type quarantineBatchWriter interface {
	WriteQuarantined(context.Context, *flowworker.RecordBatch, flowtombstone.Decision) error
}

// Pipeline is the production durable handler passed to flowworker. It enriches
// the complete partition group before the first ClickHouse write, then writes
// all deterministic blocks synchronously. A nil return is the only signal the
// Kafka consumer may use to mark offsets.
type Pipeline struct {
	enricher   batchEnricher
	writer     enrichedBatchWriter
	quarantine quarantineBatchWriter
	barrier    *flowtombstone.Guard
	stats      pipelineStats
}

type pipelineStats struct {
	records              atomic.Uint64
	samplingUnknown      atomic.Uint64
	samplingConflict     atomic.Uint64
	snapshotMiss         atomic.Uint64
	rejected             atomic.Uint64
	quarantinedDatagrams atomic.Uint64
	quarantinedRecords   atomic.Uint64
}

type PipelineStats struct {
	Records              uint64
	SamplingUnknown      uint64
	SamplingConflict     uint64
	SnapshotMiss         uint64
	Rejected             uint64
	QuarantinedDatagrams uint64
	QuarantinedRecords   uint64
}

func NewPipeline(enricher *flowworker.Enricher, writer *Writer) (*Pipeline, error) {
	if enricher == nil || writer == nil {
		return nil, errors.New("flow enricher and ClickHouse writer are required")
	}
	return &Pipeline{enricher: enricher, writer: writer}, nil
}

func NewGuardedPipeline(enricher *flowworker.Enricher, writer *Writer, barrier *flowtombstone.Guard) (*Pipeline, error) {
	pipeline, err := NewPipeline(enricher, writer)
	if err != nil {
		return nil, err
	}
	if barrier == nil {
		return nil, errors.New("raw Flow deletion barrier guard is required")
	}
	pipeline.barrier = barrier
	pipeline.quarantine = writer
	return pipeline, nil
}

func (p *Pipeline) Handle(ctx context.Context, batches []*flowworker.RecordBatch) error {
	if p == nil || p.enricher == nil || p.writer == nil {
		return errors.New("flow ClickHouse pipeline is not initialized")
	}
	if len(batches) == 0 {
		return errors.New("decoded partition batch group is required")
	}
	enriched := make([]*flowworker.EnrichedBatch, 0, len(batches))
	var records, samplingUnknown, samplingConflict uint64
	for index, batch := range batches {
		if batch == nil {
			return fmt.Errorf("enrich flow batch %d: batch is nil", index)
		}
		if decision, covered := p.quarantineDecision(batch); covered {
			if p.quarantine == nil {
				return errors.New("raw Flow quarantine writer is not initialized")
			}
			if err := p.quarantine.WriteQuarantined(ctx, batch, decision); err != nil {
				return fmt.Errorf("quarantine tombstoned flow batch %d: %w", index, err)
			}
			p.stats.quarantinedDatagrams.Add(1)
			p.stats.quarantinedRecords.Add(uint64(len(batch.Records)))
			continue
		}
		result, err := p.enricher.EnrichBatch(batch)
		if err != nil {
			// A permanent data-shape failure (future skew beyond the bound, record
			// count over the limit, a malformed record/counter) can never succeed on
			// replay. Returning it here fails the whole partition group and, since
			// the offset is never marked, crash-loops the worker on one bad datagram
			// — halting every partition. Record a mapping_rejected receipt at this
			// offset and advance instead; the rejection stays visible via the
			// receipt, the reconciliation, and the reject-observer log. Version and
			// initialization errors are transient/loud and still fail the group.
			if errors.Is(err, flowworker.ErrInvalidRecordBatch) {
				rejected, rejectErr := p.enricher.EnrichBatch(rejectedReceiptBatch(batch))
				if rejectErr != nil {
					return fmt.Errorf("build rejected receipt for flow batch %d: %w", index, rejectErr)
				}
				p.stats.rejected.Add(1)
				enriched = append(enriched, rejected)
				continue
			}
			if errors.Is(err, flowworker.ErrVersionUnavailable) {
				p.stats.snapshotMiss.Add(1)
			}
			return fmt.Errorf("enrich flow batch %d: %w", index, err)
		}
		records += uint64(len(result.Records))
		for recordIndex := range result.Records {
			record := &result.Records[recordIndex]
			if !record.EstimatedValid {
				samplingUnknown++
			}
			if record.QualityFlags&flowworker.QualitySamplingConflict != 0 {
				samplingConflict++
			}
		}
		enriched = append(enriched, result)
	}
	if len(enriched) > 0 {
		if err := p.writer.Write(ctx, enriched); err != nil {
			return fmt.Errorf("persist enriched partition batch: %w", err)
		}
	}
	p.stats.records.Add(records)
	p.stats.samplingUnknown.Add(samplingUnknown)
	p.stats.samplingConflict.Add(samplingConflict)
	return nil
}

func (p *Pipeline) quarantineDecision(batch *flowworker.RecordBatch) (flowtombstone.Decision, bool) {
	if p == nil || p.barrier == nil || batch == nil || batch.MessageDisposition != flowworker.MessageDispositionPersisted {
		return flowtombstone.Decision{}, false
	}
	for _, record := range batch.Records {
		if record == nil || record.EventTimeUnixMS <= 0 {
			continue
		}
		if decision, covered := p.barrier.Covers(time.UnixMilli(record.EventTimeUnixMS)); covered {
			return decision, true
		}
	}
	return flowtombstone.Decision{}, false
}

func (p *Pipeline) Stats() PipelineStats {
	if p == nil {
		return PipelineStats{}
	}
	return PipelineStats{
		Records: p.stats.records.Load(), SamplingUnknown: p.stats.samplingUnknown.Load(),
		SamplingConflict: p.stats.samplingConflict.Load(), SnapshotMiss: p.stats.snapshotMiss.Load(),
		Rejected:             p.stats.rejected.Load(),
		QuarantinedDatagrams: p.stats.quarantinedDatagrams.Load(), QuarantinedRecords: p.stats.quarantinedRecords.Load(),
	}
}

// rejectedReceiptBatch mirrors the failed datagram's identity as a receipt-only
// batch. validateBatch requires only schema version, Kafka identity, receive
// time, and a non-persisted disposition with no records, so this enriches into a
// mapping_rejected receipt that records the drop at the datagram's offset.
func rejectedReceiptBatch(batch *flowworker.RecordBatch) *flowworker.RecordBatch {
	return &flowworker.RecordBatch{
		BatchSchemaVersion: flowworker.RecordBatchSchemaVersion,
		MessageDisposition: flowworker.MessageDispositionMappingRejected,
		SourceStreamID:     batch.SourceStreamID,
		KafkaTopic:         batch.KafkaTopic,
		KafkaPartition:     batch.KafkaPartition,
		KafkaOffset:        batch.KafkaOffset,
		ReceivedAtUnixMS:   batch.ReceivedAtUnixMS,
	}
}

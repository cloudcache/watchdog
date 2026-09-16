package flowworker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/netsampler/goflow2/v3/decoders/netflow"
	"github.com/twmb/franz-go/pkg/kgo"
)

type RejectReason string

const (
	RejectDecode  RejectReason = "decode_invalid"
	RejectMapping RejectReason = "mapping_invalid"
)

// BatchHandler must durably handle the complete decoded batch before it
// returns nil. The Kafka consumer marks the RawFlow offset only afterwards.
type BatchHandler func(context.Context, *RecordBatch) error
type BatchGroupHandler func(context.Context, []*RecordBatch) error

// RejectObserver is optional and must stay non-blocking. Implementations may
// emit only bounded diagnostics; Kafka remains the sole replay queue.
type RejectObserver func(*kgo.Record, RejectReason, error)

type Processor struct {
	decoders    *flowstream.PartitionDecoders
	adapter     DecodeAdapter
	handleGroup BatchGroupHandler
	onRejected  RejectObserver
	now         func() time.Time
	stats       processorStats
	once        sync.Once
}

type processorStats struct {
	datagrams       atomic.Uint64
	records         atomic.Uint64
	templateMissing atomic.Uint64
	rejected        atomic.Uint64
	retryableErrors atomic.Uint64
}

type ProcessorStats struct {
	Datagrams       uint64
	Records         uint64
	TemplateMissing uint64
	Rejected        uint64
	RetryableErrors uint64
}

func NewProcessor(stateTTL time.Duration, resolver BindingResolver, handleBatch BatchHandler, onRejected RejectObserver) (*Processor, error) {
	if resolver == nil || handleBatch == nil {
		return nil, errors.New("binding resolver and batch handler are required")
	}
	return NewBatchProcessor(stateTTL, resolver, func(ctx context.Context, batches []*RecordBatch) error {
		for _, batch := range batches {
			if err := handleBatch(ctx, batch); err != nil {
				return err
			}
		}
		return nil
	}, onRejected)
}

func NewBatchProcessor(stateTTL time.Duration, resolver BindingResolver, handleGroup BatchGroupHandler, onRejected RejectObserver) (*Processor, error) {
	return NewBatchProcessorForStream(stateTTL, "", resolver, handleGroup, onRejected)
}

// NewBatchProcessorForStream is the production V2 constructor. sourceStreamID
// must be a stable, never-reused Kafka cluster/topic-incarnation identifier.
func NewBatchProcessorForStream(stateTTL time.Duration, sourceStreamID string, resolver BindingResolver, handleGroup BatchGroupHandler, onRejected RejectObserver) (*Processor, error) {
	if resolver == nil || handleGroup == nil {
		return nil, errors.New("binding resolver and batch group handler are required")
	}
	if sourceStreamID != "" && !ValidSourceStreamID(sourceStreamID) {
		return nil, errors.New("source stream ID is invalid")
	}
	return &Processor{
		decoders:    flowstream.NewPartitionDecoders(stateTTL),
		adapter:     DecodeAdapter{ResolveBinding: resolver, SourceStreamID: sourceStreamID},
		handleGroup: handleGroup,
		onRejected:  onRejected,
		now:         time.Now,
	}, nil
}

// HandleRecord is a flowstream.RecordHandler. Missing templates are completed
// so a later template on the same partition can be reached. Malformed input is
// counted and skipped, matching GoFlow2/Akvorado behavior. Binding and durable
// sink failures remain retryable and therefore leave the offset unmarked.
func (p *Processor) HandleRecord(ctx context.Context, record *kgo.Record) error {
	return p.HandleRecords(ctx, []*kgo.Record{record})
}

// HandleRecords decodes one ordered Kafka partition fetch and invokes the
// durable handler once. Retryable failures occur before any offset in the
// group is marked by flowstream.Consumer.RunPartitionBatches.
func (p *Processor) HandleRecords(ctx context.Context, records []*kgo.Record) error {
	if p == nil || p.decoders == nil || p.handleGroup == nil {
		return errors.New("flow worker processor is not initialized")
	}
	if err := validatePartitionRecords(records); err != nil {
		return err
	}
	batches := make([]*RecordBatch, 0, len(records))
	for _, record := range records {
		batch, err := p.decodeRecord(record)
		if err != nil {
			return err
		}
		batches = append(batches, batch)
	}
	if err := p.handleGroup(ctx, batches); err != nil {
		p.stats.retryableErrors.Add(1)
		return fmt.Errorf("handle decoded flow batch group: %w", err)
	}
	for _, batch := range batches {
		if batch.MessageDisposition == MessageDispositionPersisted {
			p.stats.records.Add(uint64(len(batch.Records)))
		}
	}
	return nil
}

func (p *Processor) decodeRecord(record *kgo.Record) (*RecordBatch, error) {
	decoded, err := p.decoders.DecodeRecord(record)
	if err != nil {
		if errors.Is(err, netflow.ErrorTemplateNotFound) {
			p.stats.templateMissing.Add(1)
			return p.receiptBatch(record, MessageDispositionTemplateMissing, time.Time{}), nil
		}
		p.handleRejected(record, RejectDecode, err)
		return p.receiptBatch(record, MessageDispositionDecodeRejected, time.Time{}), nil
	}
	p.stats.datagrams.Add(1)
	if len(decoded.Records) == 0 {
		return p.receiptBatch(record, MessageDispositionEmpty, decoded.ReceivedAt), nil
	}
	batch, err := p.adapter.Map(record, decoded)
	if err != nil {
		if errors.Is(err, ErrDecodedFlowInvalid) {
			p.handleRejected(record, RejectMapping, err)
			return p.receiptBatch(record, MessageDispositionMappingRejected, decoded.ReceivedAt), nil
		}
		p.stats.retryableErrors.Add(1)
		return nil, err
	}
	batch.RawPayload = record.Value
	return batch, nil
}

func (p *Processor) receiptBatch(record *kgo.Record, disposition MessageDisposition, decodedAt time.Time) *RecordBatch {
	receivedAt := decodedAt.UTC()
	if receivedAt.IsZero() && !record.Timestamp.IsZero() {
		receivedAt = record.Timestamp.UTC()
	}
	if receivedAt.UnixMilli() <= 0 {
		receivedAt = p.now().UTC()
	}
	sourceStreamID := p.adapter.SourceStreamID
	if sourceStreamID == "" {
		sourceStreamID = "legacy:" + record.Topic
	}
	return &RecordBatch{
		BatchSchemaVersion: RecordBatchSchemaVersion, MessageDisposition: disposition,
		SourceStreamID: sourceStreamID, KafkaTopic: record.Topic, KafkaPartition: record.Partition,
		KafkaOffset: record.Offset, ReceivedAtUnixMS: receivedAt.UnixMilli(),
	}
}

func validatePartitionRecords(records []*kgo.Record) error {
	if len(records) == 0 || records[0] == nil || records[0].Topic == "" || records[0].Partition < 0 {
		return errors.New("ordered Kafka partition records are required")
	}
	for index, record := range records {
		if record == nil || record.Topic != records[0].Topic || record.Partition != records[0].Partition || record.Offset < 0 {
			return errors.New("Kafka batch must contain one partition")
		}
		if index > 0 && records[index-1].Offset >= record.Offset {
			return errors.New("Kafka partition offsets must be strictly increasing")
		}
	}
	return nil
}

func (p *Processor) handleRejected(record *kgo.Record, reason RejectReason, cause error) {
	p.stats.rejected.Add(1)
	if p.onRejected != nil {
		p.onRejected(record, reason, cause)
	}
}

func (p *Processor) Stats() ProcessorStats {
	if p == nil {
		return ProcessorStats{}
	}
	return ProcessorStats{
		Datagrams:       p.stats.datagrams.Load(),
		Records:         p.stats.records.Load(),
		TemplateMissing: p.stats.templateMissing.Load(),
		Rejected:        p.stats.rejected.Load(),
		RetryableErrors: p.stats.retryableErrors.Load(),
	}
}

func (p *Processor) Close() {
	if p == nil {
		return
	}
	p.once.Do(func() { p.decoders.Close() })
}

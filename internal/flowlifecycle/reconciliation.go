package flowlifecycle

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/cloudcache/watchdog/internal/flowworker"
	"github.com/cloudcache/watchdog/internal/opjob"
)

const (
	ReconciliationJobType       = "flow.ingest_reconciliation"
	reconciliationPayloadSchema = 1
)

var (
	ErrBootstrapRequired = errors.New("explicit Kafka bootstrap offset is required")
	ErrOffsetRegression  = errors.New("Kafka committed offset regressed")
)

// ReconciliationConfig is frozen into each operation job. BootstrapOffsets are
// used only when a partition has no durable watermark; later runs always resume
// from MySQL and never silently assume offset zero.
type ReconciliationConfig struct {
	SourceStreamID  string            `json:"source_stream_id"`
	KafkaTopic      string            `json:"kafka_topic"`
	ConsumerGroup   string            `json:"consumer_group"`
	BootstrapOffset map[uint32]uint64 `json:"bootstrap_offsets,omitempty"`
	MaxBatches      int               `json:"max_batches"`
	MaxFactRows     int               `json:"max_fact_rows"`
	MaxReadBytes    uint64            `json:"max_read_bytes"`
}

func (config ReconciliationConfig) Validate() error {
	if !flowworker.ValidSourceStreamID(strings.TrimSpace(config.SourceStreamID)) ||
		strings.TrimSpace(config.KafkaTopic) == "" || len(config.KafkaTopic) > 249 ||
		strings.TrimSpace(config.ConsumerGroup) == "" || len(config.ConsumerGroup) > 249 ||
		config.MaxBatches < 1 || config.MaxBatches > 10_000 ||
		config.MaxFactRows < 1 || config.MaxFactRows > 1_000_000 || config.MaxReadBytes == 0 {
		return ErrInvalidWatermark
	}
	return nil
}

type reconciliationPartition struct {
	Partition  uint32            `json:"partition"`
	Start      uint64            `json:"start_offset"`
	Close      uint64            `json:"close_offset"`
	Next       uint64            `json:"next_offset"`
	Batches    uint64            `json:"batches"`
	Facts      uint64            `json:"facts"`
	Mismatches map[string]uint64 `json:"mismatches,omitempty"`
	FirstBad   *uint64           `json:"first_bad_offset,omitempty"`
	Complete   bool              `json:"complete"`
}

type reconciliationPayload struct {
	Config     ReconciliationConfig      `json:"config"`
	Frozen     bool                      `json:"frozen"`
	Partitions []reconciliationPartition `json:"partitions,omitempty"`
}

func EncodeReconciliationPayload(config ReconciliationConfig) (json.RawMessage, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return opjob.EncodePayload(reconciliationPayloadSchema, reconciliationPayload{Config: config})
}

type committedOffsetReader interface {
	CommittedOffsets(context.Context, string, string) ([]flowstream.CommittedPartitionOffset, error)
}

type reconciliationScanner interface {
	Scan(context.Context, flowch.ReconciliationScanRequest) (flowch.ReconciliationScanResult, error)
}

type reconciliationWatermarks interface {
	FreezeReconciliation(context.Context, ReconciliationConfig, uint32, *uint64, uint64, time.Time) (Watermark, error)
	CompleteReconciliation(context.Context, ReconciliationConfig, uint32, uint64, uint64, uint64, time.Time) error
}

// NewReconciliationHandler returns the KISS opjob handler. Kafka coordinates
// remain plain coordinates: no per-record or per-window content hash is used.
func NewReconciliationHandler(offsets committedOffsetReader, scanner reconciliationScanner, watermarks reconciliationWatermarks) opjob.Handler {
	return func(ctx context.Context, job opjob.Job) (string, error) {
		if offsets == nil || scanner == nil || watermarks == nil {
			return "", opjob.TerminalError(errors.New("Flow reconciliation dependencies are not initialized"))
		}
		if job.JobType != ReconciliationJobType || strings.TrimSpace(job.ID) == "" {
			return "", opjob.TerminalError(errors.New("Flow reconciliation job identity is invalid"))
		}
		var payload reconciliationPayload
		if err := opjob.DecodePayload(job.CheckpointJSON, reconciliationPayloadSchema, &payload); err != nil {
			return "", err
		}
		if err := payload.Config.Validate(); err != nil {
			return "", opjob.TerminalError(err)
		}
		if !payload.Frozen {
			committed, err := offsets.CommittedOffsets(ctx, payload.Config.ConsumerGroup, payload.Config.KafkaTopic)
			if err != nil {
				return "", err
			}
			payload.Partitions = make([]reconciliationPartition, 0, len(committed))
			for _, boundary := range committed {
				if boundary.Partition < 0 {
					return "", opjob.TerminalError(ErrInvalidWatermark)
				}
				partition := uint32(boundary.Partition)
				bootstrap, hasBootstrap := payload.Config.BootstrapOffset[partition]
				var bootstrapPointer *uint64
				if hasBootstrap {
					bootstrapPointer = &bootstrap
				}
				watermark, err := watermarks.FreezeReconciliation(ctx, payload.Config, partition, bootstrapPointer, boundary.NextOffset, time.Now())
				if err != nil {
					if errors.Is(err, ErrBootstrapRequired) || errors.Is(err, ErrOffsetRegression) || errors.Is(err, ErrInvalidWatermark) {
						return "", opjob.TerminalError(err)
					}
					return "", err
				}
				payload.Partitions = append(payload.Partitions, reconciliationPartition{
					Partition: partition, Start: watermark.ReconciledNextOffset, Close: boundary.NextOffset,
					Next: watermark.ReconciledNextOffset, Mismatches: make(map[string]uint64),
					Complete: watermark.ReconciledNextOffset == boundary.NextOffset,
				})
			}
			payload.Frozen = true
			if err := reportReconciliationCheckpoint(ctx, payload); err != nil {
				return "", err
			}
		}
		if err := validateFrozenReconciliation(payload); err != nil {
			return "", opjob.TerminalError(err)
		}

		for index := range payload.Partitions {
			partition := &payload.Partitions[index]
			for partition.Next < partition.Close {
				result, err := scanner.Scan(ctx, flowch.ReconciliationScanRequest{
					Cursor: flowch.ReconciliationScanCursor{SourceStreamID: payload.Config.SourceStreamID,
						KafkaTopic: payload.Config.KafkaTopic, KafkaPartition: partition.Partition, NextOffset: partition.Next},
					CloseOffset: partition.Close, MaxBatches: payload.Config.MaxBatches,
					MaxFactRows: payload.Config.MaxFactRows, MaxReadBytes: payload.Config.MaxReadBytes,
					CompareLimits: flowch.ReconciliationCompareLimits{MaxBatches: payload.Config.MaxBatches, MaxFacts: payload.Config.MaxFactRows},
				})
				if err != nil {
					return "", err
				}
				if result.NextCursor.NextOffset <= partition.Next || result.NextCursor.NextOffset > partition.Close {
					return "", opjob.TerminalError(errors.New("Flow reconciliation scan did not advance within its frozen boundary"))
				}
				partition.Next = result.NextCursor.NextOffset
				partition.Batches = saturatingReconciliationAdd(partition.Batches, result.Comparison.Batches)
				partition.Facts = saturatingReconciliationAdd(partition.Facts, result.Comparison.Facts)
				for _, mismatch := range result.Comparison.Mismatches {
					if !validMismatchReason(mismatch.Reason) {
						return "", opjob.TerminalError(fmt.Errorf("unsupported reconciliation mismatch reason %q", mismatch.Reason))
					}
					partition.Mismatches[string(mismatch.Reason)]++
					bad := mismatch.KafkaOffset
					if partition.FirstBad == nil || bad < *partition.FirstBad {
						partition.FirstBad = &bad
					}
				}
				partition.Complete = result.Complete && partition.Next == partition.Close
				if err := reportReconciliationCheckpoint(ctx, payload); err != nil {
					return "", err
				}
			}
			partition.Complete = true
			verifiedNext := partition.Close
			if partition.FirstBad != nil {
				verifiedNext = *partition.FirstBad
			}
			mismatchCount := uint64(0)
			for _, count := range partition.Mismatches {
				mismatchCount = saturatingReconciliationAdd(mismatchCount, count)
			}
			if err := watermarks.CompleteReconciliation(ctx, payload.Config, partition.Partition,
				partition.Close, verifiedNext, mismatchCount, time.Now()); err != nil {
				return "", err
			}
		}
		return fmt.Sprintf("flow-reconciliation:%s:%s:%s", payload.Config.SourceStreamID,
			payload.Config.KafkaTopic, payload.Config.ConsumerGroup), nil
	}
}

func validateFrozenReconciliation(payload reconciliationPayload) error {
	if !payload.Frozen {
		return ErrInvalidWatermark
	}
	lastPartition := int64(-1)
	for _, partition := range payload.Partitions {
		if int64(partition.Partition) <= lastPartition || partition.Start > partition.Next || partition.Next > partition.Close ||
			partition.Complete != (partition.Next == partition.Close) ||
			(partition.FirstBad != nil && (*partition.FirstBad < partition.Start || *partition.FirstBad >= partition.Close)) {
			return ErrInvalidWatermark
		}
		for reason := range partition.Mismatches {
			if !validMismatchReason(flowch.ReconciliationMismatchReason(reason)) {
				return ErrInvalidWatermark
			}
		}
		lastPartition = int64(partition.Partition)
	}
	return nil
}

func reportReconciliationCheckpoint(ctx context.Context, payload reconciliationPayload) error {
	checkpoint, err := opjob.EncodePayload(reconciliationPayloadSchema, payload)
	if err != nil {
		return err
	}
	done := uint64(0)
	for _, partition := range payload.Partitions {
		done = saturatingReconciliationAdd(done, partition.Next-partition.Start)
	}
	return opjob.ReporterFromContext(ctx).Report(ctx, done, checkpoint)
}

func validMismatchReason(reason flowch.ReconciliationMismatchReason) bool {
	switch reason {
	case flowch.MismatchMissingReceipt, flowch.MismatchMissingRecords, flowch.MismatchIdentity,
		flowch.MismatchCount, flowch.MismatchCounter:
		return true
	default:
		return false
	}
}

func saturatingReconciliationAdd(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}

func (store *Store) FreezeReconciliation(ctx context.Context, config ReconciliationConfig, partition uint32,
	bootstrap *uint64, committed uint64, at time.Time) (Watermark, error) {
	if store == nil || store.db == nil || config.Validate() != nil || at.IsZero() {
		return Watermark{}, ErrInvalidWatermark
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return Watermark{}, err
	}
	defer tx.Rollback()
	watermark, err := scanWatermark(tx.QueryRowContext(ctx, `SELECT source_stream_id,kafka_topic,consumer_group,kafka_partition,
		bootstrap_offset,committed_next_offset,reconciled_next_offset FROM flow_reconciliation_watermarks
		WHERE source_stream_id=? AND kafka_partition=? FOR UPDATE`, config.SourceStreamID, partition))
	if errors.Is(err, sql.ErrNoRows) {
		if bootstrap == nil {
			return Watermark{}, ErrBootstrapRequired
		}
		if *bootstrap > committed {
			return Watermark{}, ErrInvalidWatermark
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO flow_reconciliation_watermarks
			(source_stream_id,kafka_topic,consumer_group,kafka_partition,bootstrap_offset,committed_next_offset,
			reconciled_next_offset,status,committed_snapshot_at)
			VALUES (?,?,?,?,?,?,?,'initializing',?)`, config.SourceStreamID, config.KafkaTopic, config.ConsumerGroup,
			partition, *bootstrap, committed, *bootstrap, at.UTC().Truncate(time.Millisecond))
		if err != nil {
			return Watermark{}, err
		}
		watermark = Watermark{SourceStreamID: config.SourceStreamID, KafkaTopic: config.KafkaTopic,
			ConsumerGroup: config.ConsumerGroup, KafkaPartition: partition, BootstrapOffset: *bootstrap,
			CommittedNextOffset: committed, ReconciledNextOffset: *bootstrap}
	} else if err != nil {
		return Watermark{}, err
	} else {
		if watermark.KafkaTopic != config.KafkaTopic || watermark.ConsumerGroup != config.ConsumerGroup {
			return Watermark{}, ErrInvalidWatermark
		}
		if committed < watermark.CommittedNextOffset || committed < watermark.ReconciledNextOffset {
			return Watermark{}, ErrOffsetRegression
		}
		_, err = tx.ExecContext(ctx, `UPDATE flow_reconciliation_watermarks SET committed_next_offset=?,
			committed_snapshot_at=?,row_version=row_version+1 WHERE source_stream_id=? AND kafka_partition=?`,
			committed, at.UTC().Truncate(time.Millisecond), config.SourceStreamID, partition)
		if err != nil {
			return Watermark{}, err
		}
		watermark.CommittedNextOffset = committed
	}
	if err := tx.Commit(); err != nil {
		return Watermark{}, err
	}
	return watermark, nil
}

func (store *Store) CompleteReconciliation(ctx context.Context, config ReconciliationConfig, partition uint32,
	frozenCommitted, verifiedNext, mismatchCount uint64, at time.Time) error {
	if store == nil || store.db == nil || config.Validate() != nil || at.IsZero() || verifiedNext > frozenCommitted {
		return ErrInvalidWatermark
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, err := scanWatermark(tx.QueryRowContext(ctx, `SELECT source_stream_id,kafka_topic,consumer_group,kafka_partition,
		bootstrap_offset,committed_next_offset,reconciled_next_offset FROM flow_reconciliation_watermarks
		WHERE source_stream_id=? AND kafka_partition=? FOR UPDATE`, config.SourceStreamID, partition))
	if err != nil {
		return err
	}
	if current.KafkaTopic != config.KafkaTopic || current.ConsumerGroup != config.ConsumerGroup ||
		frozenCommitted > current.CommittedNextOffset {
		return ErrInvalidWatermark
	}
	if current.ReconciledNextOffset >= frozenCommitted {
		return tx.Commit()
	}
	if verifiedNext < current.ReconciledNextOffset {
		return ErrInvalidWatermark
	}
	status, errorCode, errorDetail := "healthy", "", ""
	if verifiedNext < frozenCommitted || mismatchCount > 0 {
		status, errorCode = "mismatch", "RECONCILIATION_MISMATCH"
		errorDetail = fmt.Sprintf("%d mismatches; contiguous coverage stops at offset %d of %d", mismatchCount, verifiedNext, frozenCommitted)
	}
	_, err = tx.ExecContext(ctx, `UPDATE flow_reconciliation_watermarks SET reconciled_next_offset=?,status=?,
		mismatch_count=?,last_error_code=NULLIF(?,''),last_error_detail=NULLIF(?,''),last_verified_at=?,
		row_version=row_version+1 WHERE source_stream_id=? AND kafka_partition=?`, verifiedNext, status,
		mismatchCount, errorCode, errorDetail, at.UTC().Truncate(time.Millisecond), config.SourceStreamID, partition)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func scanWatermark(row rowScanner) (Watermark, error) {
	var watermark Watermark
	err := row.Scan(&watermark.SourceStreamID, &watermark.KafkaTopic, &watermark.ConsumerGroup,
		&watermark.KafkaPartition, &watermark.BootstrapOffset, &watermark.CommittedNextOffset,
		&watermark.ReconciledNextOffset)
	return watermark, err
}

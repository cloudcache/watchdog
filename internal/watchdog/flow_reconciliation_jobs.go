// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package watchdog

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

const (
	FlowReconciliationJobType        = "flow_ingest_reconciliation"
	FlowReconciliationPayloadVersion = 1
)

type flowReconciliationJobConfig struct {
	SourceStreamID  string           `json:"source_stream_id"`
	KafkaTopic      string           `json:"kafka_topic"`
	ConsumerGroup   string           `json:"consumer_group"`
	BootstrapOffset map[int32]uint64 `json:"bootstrap_offsets,omitempty"`
	MaxBatches      int              `json:"max_batches"`
	MaxFactRows     int              `json:"max_fact_rows"`
	MaxReadBytes    uint64           `json:"max_read_bytes"`
}

type flowReconciliationPartitionCheckpoint struct {
	Partition  int32             `json:"partition"`
	Start      uint64            `json:"start_offset"`
	Close      uint64            `json:"close_offset"`
	Next       uint64            `json:"next_offset"`
	Batches    uint64            `json:"batches"`
	Facts      uint64            `json:"facts"`
	Mismatches map[string]uint64 `json:"mismatches,omitempty"`
	Complete   bool              `json:"complete"`
}

type flowReconciliationJobPayload struct {
	Config     flowReconciliationJobConfig             `json:"config"`
	Frozen     bool                                    `json:"frozen"`
	Partitions []flowReconciliationPartitionCheckpoint `json:"partitions,omitempty"`
}

type flowReconciliationOffsetReader interface {
	CommittedOffsets(context.Context, string, string) ([]flowstream.CommittedPartitionOffset, error)
}

type flowReconciliationScanner interface {
	Scan(context.Context, flowch.ReconciliationScanRequest) (flowch.ReconciliationScanResult, error)
}

type flowReconciliationWatermarks interface {
	LookupSystemOperationJobWatermark(context.Context, string, string) (uint64, bool, error)
	AdvanceSystemOperationJobWatermark(context.Context, string, string, uint64) error
}

type flowReconciliationMetrics interface {
	Begin()
	PublishComplete(time.Time, map[flowch.ReconciliationMismatchReason]uint64)
}

func encodeFlowReconciliationJobPayload(config flowReconciliationJobConfig) (json.RawMessage, error) {
	if err := validateFlowReconciliationJobConfig(config); err != nil {
		return nil, err
	}
	return EncodeJobPayload(FlowReconciliationPayloadVersion, flowReconciliationJobPayload{Config: config})
}

func validateFlowReconciliationJobConfig(config flowReconciliationJobConfig) error {
	config.SourceStreamID = strings.TrimSpace(config.SourceStreamID)
	config.KafkaTopic = strings.TrimSpace(config.KafkaTopic)
	config.ConsumerGroup = strings.TrimSpace(config.ConsumerGroup)
	if !flowworker.ValidSourceStreamID(config.SourceStreamID) || config.KafkaTopic == "" || len(config.KafkaTopic) > 249 || config.ConsumerGroup == "" || len(config.ConsumerGroup) > 255 {
		return errors.New("Flow reconciliation source stream, exact Kafka topic, and consumer group are invalid")
	}
	if config.MaxBatches < 1 || config.MaxBatches > 10_000 || config.MaxFactRows < 1 || config.MaxFactRows > 1_000_000 || config.MaxReadBytes < 1 {
		return errors.New("Flow reconciliation scan budgets are invalid")
	}
	for partition := range config.BootstrapOffset {
		if partition < 0 {
			return errors.New("Flow reconciliation bootstrap partition must not be negative")
		}
	}
	return nil
}

func NewFlowReconciliationJobHandler(offsets flowReconciliationOffsetReader, scanner flowReconciliationScanner, watermarks flowReconciliationWatermarks, metrics flowReconciliationMetrics) OperationJobHandler {
	return func(ctx context.Context, job OperationJob) (string, error) {
		if offsets == nil || scanner == nil || watermarks == nil || metrics == nil {
			return "", TerminalJobError(errors.New("Flow reconciliation dependencies are not initialized"))
		}
		if job.JobType != FlowReconciliationJobType || job.ScopeType != OperationJobScopeSystem || job.TenantID != "" || job.ID == "" {
			return "", TerminalJobError(errors.New("Flow reconciliation job identity is invalid"))
		}
		var payload flowReconciliationJobPayload
		if err := DecodeJobPayload(job.CheckpointJSON, FlowReconciliationPayloadVersion, &payload); err != nil {
			return "", err
		}
		if err := validateFlowReconciliationJobConfig(payload.Config); err != nil {
			return "", TerminalJobError(err)
		}
		metrics.Begin()
		if !payload.Frozen {
			committed, err := offsets.CommittedOffsets(ctx, payload.Config.ConsumerGroup, payload.Config.KafkaTopic)
			if err != nil {
				return "", err
			}
			payload.Partitions = make([]flowReconciliationPartitionCheckpoint, 0, len(committed))
			for _, boundary := range committed {
				start, known, err := watermarks.LookupSystemOperationJobWatermark(ctx, FlowReconciliationJobType,
					flowReconciliationWatermarkKey(payload.Config, boundary.Partition))
				if err != nil {
					return "", err
				}
				if !known {
					start, known = payload.Config.BootstrapOffset[boundary.Partition]
				}
				if !known {
					return "", TerminalJobError(fmt.Errorf("Flow reconciliation partition %d has no persisted watermark or explicit bootstrap offset", boundary.Partition))
				}
				if start > boundary.NextOffset {
					return "", TerminalJobError(fmt.Errorf("Flow reconciliation partition %d committed offset regressed from %d to %d", boundary.Partition, start, boundary.NextOffset))
				}
				payload.Partitions = append(payload.Partitions, flowReconciliationPartitionCheckpoint{
					Partition: boundary.Partition, Start: start, Close: boundary.NextOffset, Next: start,
					Mismatches: make(map[string]uint64), Complete: start == boundary.NextOffset,
				})
			}
			payload.Frozen = true
			if err := reportFlowReconciliationCheckpoint(ctx, payload); err != nil {
				return "", err
			}
		}
		if err := validateFrozenFlowReconciliationPayload(payload); err != nil {
			return "", TerminalJobError(err)
		}

		for index := range payload.Partitions {
			partition := &payload.Partitions[index]
			watermarkKey := flowReconciliationWatermarkKey(payload.Config, partition.Partition)
			if partition.Complete {
				if err := watermarks.AdvanceSystemOperationJobWatermark(ctx, FlowReconciliationJobType, watermarkKey, partition.Close); err != nil {
					return "", err
				}
				continue
			}
			for partition.Next < partition.Close {
				result, err := scanner.Scan(ctx, flowch.ReconciliationScanRequest{
					Cursor: flowch.ReconciliationScanCursor{SourceStreamID: payload.Config.SourceStreamID, KafkaTopic: payload.Config.KafkaTopic,
						KafkaPartition: uint32(partition.Partition), NextOffset: partition.Next},
					CloseOffset: partition.Close, MaxBatches: payload.Config.MaxBatches,
					MaxFactRows: payload.Config.MaxFactRows, MaxReadBytes: payload.Config.MaxReadBytes,
					CompareLimits: flowch.ReconciliationCompareLimits{MaxBatches: payload.Config.MaxBatches, MaxFacts: payload.Config.MaxFactRows},
				})
				if err != nil {
					return "", err
				}
				if result.NextCursor.NextOffset <= partition.Next {
					return "", TerminalJobError(errors.New("Flow reconciliation scan budget cannot advance the cursor"))
				}
				partition.Next = result.NextCursor.NextOffset
				partition.Batches = saturatingAdd(partition.Batches, result.Comparison.Batches)
				partition.Facts = saturatingAdd(partition.Facts, result.Comparison.Facts)
				for _, mismatch := range result.Comparison.Mismatches {
					partition.Mismatches[string(mismatch.Reason)]++
				}
				partition.Complete = result.Complete && partition.Next == partition.Close
				if err := reportFlowReconciliationCheckpoint(ctx, payload); err != nil {
					return "", err
				}
			}
			partition.Complete = true
			if err := reportFlowReconciliationCheckpoint(ctx, payload); err != nil {
				return "", err
			}
			if err := watermarks.AdvanceSystemOperationJobWatermark(ctx, FlowReconciliationJobType, watermarkKey, partition.Close); err != nil {
				return "", err
			}
		}

		mismatches := make(map[flowch.ReconciliationMismatchReason]uint64)
		for _, partition := range payload.Partitions {
			for reason, count := range partition.Mismatches {
				typed := flowch.ReconciliationMismatchReason(reason)
				if !validFlowReconciliationMismatchReason(typed) {
					return "", TerminalJobError(fmt.Errorf("Flow reconciliation checkpoint contains unsupported mismatch reason %q", reason))
				}
				mismatches[typed] = saturatingAdd(mismatches[typed], count)
			}
		}
		metrics.PublishComplete(time.Now(), mismatches)
		return fmt.Sprintf("flow-reconciliation:%s:%s:%s", payload.Config.SourceStreamID, payload.Config.KafkaTopic, payload.Config.ConsumerGroup), nil
	}
}

func validateFrozenFlowReconciliationPayload(payload flowReconciliationJobPayload) error {
	if !payload.Frozen {
		return errors.New("Flow reconciliation checkpoint is not frozen")
	}
	lastPartition := int32(-1)
	for _, partition := range payload.Partitions {
		if partition.Partition <= lastPartition || partition.Start > partition.Next || partition.Next > partition.Close || partition.Complete != (partition.Next == partition.Close) {
			return errors.New("Flow reconciliation partition checkpoint is invalid")
		}
		for reason := range partition.Mismatches {
			if !validFlowReconciliationMismatchReason(flowch.ReconciliationMismatchReason(reason)) {
				return fmt.Errorf("Flow reconciliation checkpoint contains unsupported mismatch reason %q", reason)
			}
		}
		lastPartition = partition.Partition
	}
	return nil
}

func validFlowReconciliationMismatchReason(reason flowch.ReconciliationMismatchReason) bool {
	switch reason {
	case flowch.MismatchMissingReceipt, flowch.MismatchMissingRecords, flowch.MismatchIdentity, flowch.MismatchCount, flowch.MismatchCounter:
		return true
	default:
		return false
	}
}

func reportFlowReconciliationCheckpoint(ctx context.Context, payload flowReconciliationJobPayload) error {
	checkpoint, err := EncodeJobPayload(FlowReconciliationPayloadVersion, payload)
	if err != nil {
		return err
	}
	done := uint64(0)
	for _, partition := range payload.Partitions {
		done = saturatingAdd(done, partition.Next-partition.Start)
	}
	return OperationJobReporterFromContext(ctx).Report(ctx, done, checkpoint)
}

func flowReconciliationWatermarkKey(config flowReconciliationJobConfig, partition int32) string {
	// This is a cold management-plane key, not a per-record hot-path hash. The
	// full source identity remains in the job payload while the digest keeps the
	// platform's generic 128-byte partition-key contract bounded.
	digest := sha256.Sum256([]byte(config.SourceStreamID + "\x00" + config.KafkaTopic + "\x00" + config.ConsumerGroup))
	return fmt.Sprintf("v1:%x:p%d", digest[:12], partition)
}

func saturatingAdd(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}

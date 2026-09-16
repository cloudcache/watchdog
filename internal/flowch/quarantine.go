// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/internal/flowtombstone"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

type PreparedQuarantine struct {
	Batch         *flowworker.RecordBatch
	Decision      flowtombstone.Decision
	Receipt       PreparedReceipt
	QuarantinedAt time.Time
}

func prepareQuarantine(batch *flowworker.RecordBatch, decision flowtombstone.Decision) (PreparedQuarantine, error) {
	if batch == nil || batch.MessageDisposition != flowworker.MessageDispositionPersisted || !flowworker.ValidSourceStreamID(batch.SourceStreamID) ||
		batch.KafkaTopic == "" || batch.KafkaPartition < 0 || batch.KafkaOffset < 0 || batch.ReceivedAtUnixMS <= 0 || len(batch.Records) == 0 ||
		len(batch.RawPayload) == 0 || decision.Revision == 0 || decision.DeletedThrough == "" || decision.EventDay == "" {
		return PreparedQuarantine{}, fmt.Errorf("%w: quarantined datagram identity is invalid", ErrInvalidBatchGroup)
	}
	receipt := PreparedReceipt{
		Disposition: flowworker.MessageDispositionLateQuarantined, SourceStreamID: batch.SourceStreamID, KafkaTopic: batch.KafkaTopic,
		KafkaPartition: batch.KafkaPartition, KafkaOffset: batch.KafkaOffset, RecordCount: uint64(len(batch.Records)),
		ReceivedAt: time.UnixMilli(batch.ReceivedAtUnixMS).UTC(), Generation: 1<<63 | decision.Revision,
	}
	for index, record := range batch.Records {
		if record == nil || record.EventTimeUnixMS <= 0 {
			return PreparedQuarantine{}, fmt.Errorf("%w: quarantined record %d is invalid", ErrInvalidBatchGroup, index)
		}
		if receipt.RawBytes > math.MaxUint64-record.RawBytes || receipt.RawPackets > math.MaxUint64-record.RawPackets {
			return PreparedQuarantine{}, fmt.Errorf("%w: quarantined raw counters overflow", ErrInvalidBatchGroup)
		}
		receipt.RawBytes += record.RawBytes
		receipt.RawPackets += record.RawPackets
		if record.EstimatedValid {
			if receipt.EstimatedBytes > math.MaxUint64-record.EstimatedBytes || receipt.EstimatedPackets > math.MaxUint64-record.EstimatedPackets {
				return PreparedQuarantine{}, fmt.Errorf("%w: quarantined estimated counters overflow", ErrInvalidBatchGroup)
			}
			receipt.EstimatedBytes += record.EstimatedBytes
			receipt.EstimatedPackets += record.EstimatedPackets
			receipt.EstimatedValidRecords++
		}
		eventTime := time.UnixMilli(record.EventTimeUnixMS).UTC()
		if receipt.MinEventTime.IsZero() || eventTime.Before(receipt.MinEventTime) {
			receipt.MinEventTime = eventTime
		}
		if eventTime.After(receipt.MaxEventTime) {
			receipt.MaxEventTime = eventTime
		}
	}
	return PreparedQuarantine{Batch: batch, Decision: decision, Receipt: receipt, QuarantinedAt: time.Now().UTC()}, nil
}

func (n *NativeInserter) InsertFlowQuarantine(ctx context.Context, item PreparedQuarantine) error {
	if n == nil || n.executor == nil {
		return Permanent(errors.New("ClickHouse quarantine inserter is not initialized"))
	}
	input, err := buildQuarantineInput(item)
	if err != nil {
		return Permanent(err)
	}
	token := fmt.Sprintf("flow-quarantine-v1:%s:%d:%d", item.Batch.SourceStreamID, item.Batch.KafkaPartition, item.Batch.KafkaOffset)
	if err := n.executor.Do(ctx, insertQuery(flowQuarantineTable, token, input)); err != nil {
		return classifyClickHouseError(fmt.Errorf("insert quarantined Flow datagram: %w", err))
	}
	receiptBlock := PreparedBlock{
		SourceStreamID: item.Batch.SourceStreamID, KafkaTopic: item.Batch.KafkaTopic, KafkaPartition: item.Batch.KafkaPartition,
		FirstOffset: item.Batch.KafkaOffset, LastOffset: item.Batch.KafkaOffset, Receipts: []PreparedReceipt{item.Receipt},
	}
	if err := validatePreparedReceipts(receiptBlock); err != nil {
		return Permanent(err)
	}
	if err := n.executor.Do(ctx, insertQuery(flowReceiptsTable, token+":receipt", buildReceiptInput(receiptBlock))); err != nil {
		return classifyClickHouseError(fmt.Errorf("insert quarantined Flow receipt: %w", err))
	}
	return nil
}

func buildQuarantineInput(item PreparedQuarantine) (proto.Input, error) {
	if item.QuarantinedAt.IsZero() || item.Receipt.ReceivedAt.IsZero() {
		prepared, err := prepareQuarantine(item.Batch, item.Decision)
		if err != nil {
			return nil, err
		}
		item = prepared
	}
	deletedThrough, err := time.Parse(time.DateOnly, item.Decision.DeletedThrough)
	if err != nil {
		return nil, flowtombstone.ErrInvalidBarrier
	}
	matchedDay, err := time.Parse(time.DateOnly, item.Decision.EventDay)
	if err != nil {
		return nil, flowtombstone.ErrInvalidBarrier
	}
	batch := item.Batch
	receipt := item.Receipt
	quarantinedAt := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
	receivedAt := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
	minEventTime := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
	maxEventTime := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
	var barrierRevision, kafkaOffset, registryVersion, exporterEpoch, recordCount, rawBytes, rawPackets proto.ColUInt64
	var estimatedBytes, estimatedPackets, estimatedValid proto.ColUInt64
	var kafkaPartition, subAgentID, datagramSequence proto.ColUInt32
	var flowProtocol proto.ColUInt8
	var deletedDate, matchedDate proto.ColDate
	var exporterIP, agentIP proto.ColIPv6
	var sourceStreamID, rawPayload proto.ColStr
	kafkaTopic := new(proto.ColStr).LowCardinality()
	collectorID := new(proto.ColStr).LowCardinality()
	exporterID := new(proto.ColStr).LowCardinality()
	quarantinedAt.Append(item.QuarantinedAt.UTC())
	barrierRevision.Append(item.Decision.Revision)
	deletedDate.Append(deletedThrough)
	matchedDate.Append(matchedDay)
	sourceStreamID.Append(batch.SourceStreamID)
	kafkaTopic.Append(batch.KafkaTopic)
	kafkaPartition.Append(uint32(batch.KafkaPartition))
	kafkaOffset.Append(uint64(batch.KafkaOffset))
	collectorID.Append(batch.CollectorID)
	exporterID.Append(batch.ExporterID)
	registryVersion.Append(batch.RegistryVersion)
	receivedAt.Append(receipt.ReceivedAt)
	flowProtocol.Append(uint8(batch.Protocol))
	exporterIP.Append(clickHouseIP(addressFromBytes(batch.SourceIP)))
	var observationDomain proto.ColUInt64
	observationDomain.Append(batch.ObservationDomainID)
	subAgentID.Append(batch.SubAgentID)
	datagramSequence.Append(batch.DatagramSequence)
	agentIP.Append(clickHouseIP(addressFromBytes(batch.AgentIP)))
	exporterEpoch.Append(batch.ExporterEpoch)
	recordCount.Append(receipt.RecordCount)
	rawBytes.Append(receipt.RawBytes)
	rawPackets.Append(receipt.RawPackets)
	estimatedBytes.Append(receipt.EstimatedBytes)
	estimatedPackets.Append(receipt.EstimatedPackets)
	estimatedValid.Append(receipt.EstimatedValidRecords)
	minEventTime.Append(receipt.MinEventTime)
	maxEventTime.Append(receipt.MaxEventTime)
	rawPayload.Append(string(batch.RawPayload))
	return proto.Input{
		{Name: "quarantined_at", Data: quarantinedAt}, {Name: "barrier_revision", Data: &barrierRevision},
		{Name: "barrier_deleted_through", Data: &deletedDate}, {Name: "matched_event_day", Data: &matchedDate},
		{Name: "source_stream_id", Data: &sourceStreamID}, {Name: "kafka_topic", Data: kafkaTopic},
		{Name: "kafka_partition", Data: &kafkaPartition}, {Name: "kafka_offset", Data: &kafkaOffset},
		{Name: "collector_id", Data: collectorID}, {Name: "exporter_id", Data: exporterID}, {Name: "registry_version", Data: &registryVersion},
		{Name: "received_at", Data: receivedAt}, {Name: "flow_protocol", Data: &flowProtocol}, {Name: "exporter_source_ip", Data: &exporterIP},
		{Name: "observation_domain_id", Data: &observationDomain}, {Name: "sub_agent_id", Data: &subAgentID},
		{Name: "datagram_sequence", Data: &datagramSequence}, {Name: "agent_ip", Data: &agentIP}, {Name: "exporter_epoch", Data: &exporterEpoch},
		{Name: "decoded_record_count", Data: &recordCount}, {Name: "raw_bytes", Data: &rawBytes}, {Name: "raw_packets", Data: &rawPackets},
		{Name: "estimated_bytes", Data: &estimatedBytes}, {Name: "estimated_packets", Data: &estimatedPackets},
		{Name: "estimated_valid_records", Data: &estimatedValid}, {Name: "min_event_time", Data: minEventTime}, {Name: "max_event_time", Data: maxEventTime},
		{Name: "raw_payload", Data: &rawPayload},
	}, nil
}

func addressFromBytes(value []byte) netip.Addr {
	address, _ := netip.AddrFromSlice(value)
	return address.Unmap()
}

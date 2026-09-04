package flowcollect

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"sort"

	"github.com/cespare/xxhash/v2"
	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func BuildNormalizedBatches(walRecord WALRecord, decoded DecodedDatagram, binding SourceBinding, collectorID string, plan Plan, config NormalizedBatchCfg, replayGeneration uint32) ([]*flowpb.NormalizedRecordBatch, error) {
	if len(plan.PartitionMap) != VirtualShardCount {
		return nil, errors.New("partition map must contain 4096 virtual shards")
	}
	if config.MaxRecords <= 0 || config.MaxBytes <= 0 {
		return nil, errors.New("normalized batch limits must be positive")
	}
	recordsByShard := make(map[uint32][]*flowpb.NormalizedRecord)
	for index, record := range decoded.Records {
		if record.FlowDuration < 0 {
			return nil, fmt.Errorf("normalize record %d: negative flow duration", index)
		}
		values, err := NormalizeCounters(binding, decoded, record)
		if err != nil {
			return nil, fmt.Errorf("normalize record %d: %w", index, err)
		}
		observationIf, direction := observationFor(binding, record)
		normalized := &flowpb.NormalizedRecord{
			RecordIndex: uint32(index), EventTimeUnixMs: record.EventTime.UnixMilli(), TargetId: binding.TargetID, DeviceId: binding.DeviceID,
			ObservationIfIndex: observationIf, ObservationDirection: direction, InIf: record.InIf, OutIf: record.OutIf,
			SrcIp: address16(record.SrcIP), DstIp: address16(record.DstIP), SrcPort: record.SrcPort, DstPort: record.DstPort,
			IpProto: record.IPProtocol, TcpFlags: record.TCPFlags, RawBytes: record.RawBytes, RawPackets: record.RawPackets,
			SamplingMode: uint32(values.SamplingMode), SamplingRate: values.SamplingRate, EstimatedBytes: values.EstimatedBytes,
			EstimatedPackets: values.EstimatedPackets, FlowDurationMs: uint64(record.FlowDuration.Milliseconds()), QualityFlags: values.QualityFlags,
			SrcAs: record.SrcAS, DstAs: record.DstAS, SourceIdType: record.SourceIDType, SourceIdValue: record.SourceIDValue,
			SampleSequence: record.SampleSequence, SamplePool: record.SamplePool, ExporterDrops: record.ExporterDrops,
		}
		shard := VirtualShard(binding.TenantID, record.SrcIP, record.DstIP)
		recordsByShard[shard] = append(recordsByShard[shard], normalized)
	}
	shards := make([]int, 0, len(recordsByShard))
	for shard := range recordsByShard {
		shards = append(shards, int(shard))
	}
	sort.Ints(shards)
	result := make([]*flowpb.NormalizedRecordBatch, 0, len(shards))
	for _, shardValue := range shards {
		shard := uint32(shardValue)
		records := recordsByShard[shard]
		chunk := uint32(0)
		for len(records) > 0 {
			batch := newBatch(walRecord, decoded, binding, collectorID, plan, shard, chunk, replayGeneration)
			batchBytes := proto.Size(batch)
			for len(records) > 0 && len(batch.Records) < config.MaxRecords {
				recordBytes := proto.Size(records[0])
				entryBytes := protowire.SizeTag(16) + protowire.SizeVarint(uint64(recordBytes)) + recordBytes
				if batchBytes+entryBytes > config.MaxBytes {
					if len(batch.Records) == 0 {
						return nil, fmt.Errorf("normalized record %d exceeds max_bytes", records[0].RecordIndex)
					}
					break
				}
				batch.Records = append(batch.Records, records[0])
				batchBytes += entryBytes
				records = records[1:]
			}
			result = append(result, batch)
			chunk++
		}
	}
	return result, nil
}

func VirtualShard(tenantID string, source, destination netip.Addr) uint32 {
	hash := xxhash.New()
	_, _ = hash.WriteString(tenantID)
	_, _ = hash.Write([]byte{0})
	if source.IsValid() {
		value := source.As16()
		_, _ = hash.Write(value[:])
	}
	if destination.IsValid() {
		value := destination.As16()
		_, _ = hash.Write(value[:])
	}
	return uint32(hash.Sum64() & (VirtualShardCount - 1))
}

func newBatch(record WALRecord, decoded DecodedDatagram, binding SourceBinding, collectorID string, plan Plan, shard, chunk, replay uint32) *flowpb.NormalizedRecordBatch {
	batchID := normalizedBatchID(record.DatagramID, shard, chunk)
	return &flowpb.NormalizedRecordBatch{
		BatchSchemaVersion: 1, NormalizedBatchId: batchID[:], DatagramId: record.DatagramID[:], VirtualShard: shard,
		PartitionMapVersion: plan.PartitionMapVersion, PhysicalPartition: plan.PartitionMap[shard], ReplayGeneration: replay,
		TenantId: binding.TenantID, CollectorId: collectorID, ExporterId: binding.ExporterID, RegistryVersion: plan.Revision,
		ReceivedAtUnixMs: record.ReceivedAt.UnixMilli(), Protocol: uint32(decoded.Protocol), SourceIp: address16(record.Source.Addr()),
		ObservationDomainId: decoded.ObservationDomainID, SubAgentId: decoded.SubAgentID, DatagramSequence: decoded.DatagramSequence, AgentIp: address16(decoded.AgentIP),
	}
}

func normalizedBatchID(datagramID DatagramID, shard, chunk uint32) [32]byte {
	input := make([]byte, 40)
	copy(input[:32], datagramID[:])
	binary.BigEndian.PutUint32(input[32:36], shard)
	binary.BigEndian.PutUint32(input[36:40], chunk)
	return sha256.Sum256(input)
}

func observationFor(binding SourceBinding, record DecodedRecord) (uint32, uint32) {
	if observation, ok := binding.Observations[record.InIf]; ok {
		return record.InIf, observation.Direction
	}
	if observation, ok := binding.Observations[record.OutIf]; ok {
		return record.OutIf, observation.Direction
	}
	return 0, 0
}

func address16(addr netip.Addr) []byte {
	if !addr.IsValid() {
		return nil
	}
	value := addr.As16()
	out := make([]byte, len(value))
	copy(out, value[:])
	return out
}

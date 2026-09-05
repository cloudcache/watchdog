package flowworker

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"time"

	"github.com/cloudcache/watchdog/internal/flowplan"
	"github.com/cloudcache/watchdog/internal/flowstream"
	goflowpb "github.com/netsampler/goflow2/v3/pb"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	SamplingUnknown uint32 = iota
	SamplingSampled
	SamplingPreScaled
)

const (
	SamplingSourceUnknown uint32 = iota
	SamplingSourceProtocol
	SamplingSourcePlanRule
	SamplingSourceExporterDefault
	SamplingSourceCounterMode
)

const (
	QualitySamplingPlanFallback uint64 = 1 << iota
	QualitySamplingConflict
	QualityCounterOverflow
	QualityEventTimeFallback
	QualityObservationAmbiguous
	QualitySamplingSelectorUnavailable
)

var (
	ErrBindingUnavailable = errors.New("flow exporter binding is unavailable")
	ErrDecodedFlowInvalid = errors.New("decoded flow record is invalid")
)

// BindingResolver returns the immutable binding named by the RawFlow registry
// version. It must not fall back to a newer plan when processing Kafka lag.
type BindingResolver func(collectorID string, registryVersion uint64, protocol flowplan.Protocol, source netip.Addr, observationDomainID uint64) (flowplan.SourceBinding, error)

type DecodeAdapter struct {
	ResolveBinding BindingResolver
}

// Map converts one decoded RawFlow Kafka record into the worker's in-memory
// representation. It performs no I/O other than the injected immutable lookup.
func (a DecodeAdapter) Map(kafkaRecord *kgo.Record, decoded flowstream.DecodedBatch) (*RecordBatch, error) {
	if kafkaRecord == nil || kafkaRecord.Topic == "" || kafkaRecord.Partition < 0 || kafkaRecord.Offset < 0 {
		return nil, fmt.Errorf("%w: Kafka source identity", ErrDecodedFlowInvalid)
	}
	if a.ResolveBinding == nil || decoded.CollectorID == "" || decoded.RegistryVersion == 0 || !decoded.Source.Addr().IsValid() {
		return nil, fmt.Errorf("%w: decoder identity", ErrDecodedFlowInvalid)
	}
	protocol, err := workerProtocol(decoded.FlowType)
	if err != nil {
		return nil, err
	}
	binding, err := a.ResolveBinding(decoded.CollectorID, decoded.RegistryVersion, protocol, decoded.Source.Addr(), decoded.ObservationDomainID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBindingUnavailable, err)
	}
	if !binding.Enabled {
		return nil, fmt.Errorf("%w: binding is disabled", ErrBindingUnavailable)
	}

	sourceID := kafkaSourceID(kafkaRecord.Topic, kafkaRecord.Partition, kafkaRecord.Offset)
	sourceAddress := canonicalAddressBytes(decoded.Source.Addr())
	result := &RecordBatch{
		BatchSchemaVersion:  RecordBatchSchemaVersion,
		SourceID:            sourceID[:],
		KafkaTopic:          kafkaRecord.Topic,
		KafkaPartition:      kafkaRecord.Partition,
		KafkaOffset:         kafkaRecord.Offset,
		TenantID:            binding.TenantID,
		CollectorID:         decoded.CollectorID,
		ExporterID:          binding.ExporterID,
		RegistryVersion:     decoded.RegistryVersion,
		ReceivedAtUnixMS:    decoded.ReceivedAt.UnixMilli(),
		Protocol:            uint32(protocol),
		SourceIP:            sourceAddress,
		ObservationDomainID: decoded.ObservationDomainID,
		SubAgentID:          decoded.SubAgentID,
		DatagramSequence:    decoded.DatagramSequence,
		ExporterEpoch:       binding.EffectiveOwnershipEpoch(),
		Records:             make([]*Record, 0, len(decoded.Records)),
	}
	if decoded.AgentIP.IsValid() {
		result.AgentIP = canonicalAddressBytes(decoded.AgentIP)
	}
	for index, message := range decoded.Records {
		var metadata flowstream.DecodedRecordMetadata
		if index < len(decoded.RecordMetadata) {
			metadata = decoded.RecordMetadata[index]
		}
		record, err := mapFlowMessage(binding, decoded, metadata, uint32(index), message)
		if err != nil {
			return nil, fmt.Errorf("%w: record[%d]: %v", ErrDecodedFlowInvalid, index, err)
		}
		result.Records = append(result.Records, record)
	}
	return result, nil
}

func workerProtocol(flowType goflowpb.FlowMessage_FlowType) (flowplan.Protocol, error) {
	switch flowType {
	case goflowpb.FlowMessage_SFLOW_5:
		return flowplan.ProtocolSFlow5, nil
	case goflowpb.FlowMessage_NETFLOW_V5:
		return flowplan.ProtocolNetFlow5, nil
	case goflowpb.FlowMessage_NETFLOW_V9:
		return flowplan.ProtocolNetFlow9, nil
	case goflowpb.FlowMessage_IPFIX:
		return flowplan.ProtocolIPFIX, nil
	default:
		return 0, fmt.Errorf("%w: unsupported protocol", ErrDecodedFlowInvalid)
	}
}

func mapFlowMessage(binding flowplan.SourceBinding, decoded flowstream.DecodedBatch, metadata flowstream.DecodedRecordMetadata, index uint32, message *goflowpb.FlowMessage) (*Record, error) {
	if message == nil || message.Type != decoded.FlowType {
		return nil, errors.New("message protocol differs from datagram")
	}
	source, ok := canonicalAddress16(message.SrcAddr)
	if !ok {
		return nil, errors.New("source address is invalid")
	}
	destination, ok := canonicalAddress16(message.DstAddr)
	if !ok {
		return nil, errors.New("destination address is invalid")
	}
	if message.SrcPort > math.MaxUint16 || message.DstPort > math.MaxUint16 || message.Proto > math.MaxUint8 || message.TcpFlags > math.MaxUint8 {
		return nil, errors.New("port, protocol, or TCP flags exceed storage range")
	}

	eventTime, duration, quality := flowTimes(message, decoded.ReceivedAt)
	observationIfIndex, observationDirection, observationQuality := observation(binding, message.InIf, message.OutIf)
	quality |= observationQuality
	mode, rate, sourceKind, estimatedBytes, estimatedPackets, estimatedValid, samplingQuality := normalizeDecodedCounters(binding, decoded.ObservationDomainID, metadata, message)
	quality |= samplingQuality
	return &Record{
		RecordIndex:     index,
		EventTimeUnixMS: eventTime.UnixMilli(),
		TargetID:        binding.TargetID, DeviceID: binding.DeviceID,
		ObservationIfIndex: observationIfIndex, ObservationDirection: observationDirection,
		InIf: message.InIf, OutIf: message.OutIf,
		SourceIP: source, DestinationIP: destination,
		SourcePort: message.SrcPort, DestinationPort: message.DstPort,
		IPProtocol: message.Proto, TCPFlags: message.TcpFlags,
		RawBytes: message.Bytes, RawPackets: message.Packets,
		SamplingMode: mode, SamplingRate: rate, SamplingSource: sourceKind,
		EstimatedValid: estimatedValid, EstimatedBytes: estimatedBytes, EstimatedPackets: estimatedPackets,
		FlowDurationMS: duration, QualityFlags: quality,
		SourceASN: message.SrcAs, DestinationASN: message.DstAs,
		SourceIDType: metadata.SourceIDType, SourceIDValue: metadata.SourceIDValue,
		SampleSequence: metadata.SampleSequence, SamplePool: metadata.SamplePool,
		ExporterDrops: metadata.ExporterDrops, SampleIndex: metadata.SampleIndex,
	}, nil
}

func flowTimes(message *goflowpb.FlowMessage, receivedAt time.Time) (time.Time, uint64, uint64) {
	eventTime := receivedAt.UTC()
	quality := uint64(QualityEventTimeFallback)
	if message.TimeFlowEndNs > 0 && message.TimeFlowEndNs <= math.MaxInt64 {
		eventTime = time.Unix(0, int64(message.TimeFlowEndNs)).UTC()
		quality = 0
	} else if message.TimeReceivedNs > 0 && message.TimeReceivedNs <= math.MaxInt64 {
		eventTime = time.Unix(0, int64(message.TimeReceivedNs)).UTC()
	}
	var duration uint64
	if message.TimeFlowEndNs >= message.TimeFlowStartNs {
		duration = (message.TimeFlowEndNs - message.TimeFlowStartNs) / uint64(time.Millisecond)
	}
	return eventTime, duration, quality
}

func observation(binding flowplan.SourceBinding, inIf, outIf uint32) (uint32, uint32, uint64) {
	type match struct{ ifIndex, direction uint32 }
	matches := make([]match, 0, 2)
	if value, ok := binding.Observations[inIf]; ok && inIf != 0 {
		matches = append(matches, match{inIf, value.Direction})
	}
	if value, ok := binding.Observations[outIf]; ok && outIf != 0 && outIf != inIf {
		matches = append(matches, match{outIf, value.Direction})
	}
	if len(matches) == 1 {
		return matches[0].ifIndex, matches[0].direction, 0
	}
	if len(matches) > 1 {
		return 0, uint32(ObservationUnknown), QualityObservationAmbiguous
	}
	if inIf != 0 {
		return inIf, uint32(ObservationUnknown), 0
	}
	return outIf, uint32(ObservationUnknown), 0
}

func normalizeDecodedCounters(binding flowplan.SourceBinding, observationDomainID uint64, metadata flowstream.DecodedRecordMetadata, message *goflowpb.FlowMessage) (mode uint32, rate uint64, source uint32, estimatedBytes uint64, estimatedPackets uint64, valid bool, quality uint64) {
	if binding.SamplingMode == flowplan.SamplingModePreScaled {
		if message.SamplingRate > 1 {
			quality |= QualitySamplingConflict
		}
		return SamplingPreScaled, message.SamplingRate, SamplingSourceCounterMode, message.Bytes, message.Packets, true, quality
	}
	if message.SamplingRate > 0 {
		mode, rate, source = SamplingSampled, message.SamplingRate, SamplingSourceProtocol
	} else if rule, ok, unavailable := selectSamplingRule(binding.SamplingRules, observationDomainID, metadata, message.InIf, message.OutIf); ok {
		quality |= QualitySamplingPlanFallback
		if rule.Mode == flowplan.SamplingModePreScaled {
			return SamplingPreScaled, rule.Rate, SamplingSourcePlanRule, message.Bytes, message.Packets, true, quality
		}
		mode, rate, source = SamplingSampled, rule.Rate, SamplingSourcePlanRule
	} else if binding.DefaultSamplingRate > 0 {
		mode, rate, source = SamplingSampled, binding.DefaultSamplingRate, SamplingSourceExporterDefault
		quality |= QualitySamplingPlanFallback
	} else {
		if unavailable {
			quality |= QualitySamplingSelectorUnavailable
		}
		return SamplingUnknown, 0, SamplingSourceUnknown, 0, 0, false, quality
	}
	if rate == 0 {
		return SamplingUnknown, 0, SamplingSourceUnknown, 0, 0, false, quality
	}
	if (message.Bytes != 0 && rate > math.MaxUint64/message.Bytes) || (message.Packets != 0 && rate > math.MaxUint64/message.Packets) {
		return mode, rate, source, 0, 0, false, quality | QualityCounterOverflow
	}
	return mode, rate, source, message.Bytes * rate, message.Packets * rate, true, quality
}

func selectSamplingRule(rules []flowplan.SamplingRule, observationDomainID uint64, metadata flowstream.DecodedRecordMetadata, inIf, outIf uint32) (flowplan.SamplingRule, bool, bool) {
	bestSpecificity := -1
	unavailable := false
	var best flowplan.SamplingRule
	for _, rule := range rules {
		if rule.ObservationDomainID != nil && *rule.ObservationDomainID != observationDomainID {
			continue
		}
		if rule.IfIndex != nil && *rule.IfIndex != inIf && *rule.IfIndex != outIf {
			continue
		}
		if rule.SubAgentID != nil && (!metadata.Present || *rule.SubAgentID != metadata.SubAgentID) {
			if !metadata.Present {
				unavailable = true
			}
			continue
		}
		if rule.SourceIDType != nil && (!metadata.Present || *rule.SourceIDType != metadata.SourceIDType) {
			if !metadata.Present {
				unavailable = true
			}
			continue
		}
		if rule.SourceIDValue != nil && (!metadata.Present || *rule.SourceIDValue != metadata.SourceIDValue) {
			if !metadata.Present {
				unavailable = true
			}
			continue
		}
		specificity := 0
		if rule.ObservationDomainID != nil {
			specificity++
		}
		if rule.IfIndex != nil {
			specificity++
		}
		if rule.SubAgentID != nil {
			specificity++
		}
		if rule.SourceIDType != nil {
			specificity++
		}
		if rule.SourceIDValue != nil {
			specificity++
		}
		if specificity > bestSpecificity {
			bestSpecificity, best = specificity, rule
		}
	}
	return best, bestSpecificity >= 0, unavailable
}

func kafkaSourceID(topic string, partition int32, offset int64) [32]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte(topic))
	_, _ = hash.Write([]byte{0})
	var encoded [12]byte
	binary.BigEndian.PutUint32(encoded[:4], uint32(partition))
	binary.BigEndian.PutUint64(encoded[4:], uint64(offset))
	_, _ = hash.Write(encoded[:])
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func canonicalAddress16(value []byte) ([]byte, bool) {
	address, ok := netip.AddrFromSlice(value)
	if !ok {
		return nil, false
	}
	encoded := address.Unmap().As16()
	return append([]byte(nil), encoded[:]...), true
}

func canonicalAddressBytes(address netip.Addr) []byte {
	encoded := address.Unmap().As16()
	return append([]byte(nil), encoded[:]...)
}

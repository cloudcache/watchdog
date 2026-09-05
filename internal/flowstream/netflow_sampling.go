// SPDX-FileCopyrightText: 2021 NetSampler
// SPDX-FileCopyrightText: 2023-2026 Free Mobile
// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only AND BSD-3-Clause

package flowstream

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/netsampler/goflow2/v3/decoders/netflow"
	"github.com/netsampler/goflow2/v3/producer"
	protoproducer "github.com/netsampler/goflow2/v3/producer/proto"
)

// GoFlow2's built-in sampling store is keyed by exporter/version/domain. Some
// exporters publish multiple sampler IDs in one domain, so the last option
// record otherwise overwrites every record's rate. This small post-processor
// uses GoFlow2's already-decoded fields and bounded FlowStore; it does not
// decode the datagram a second time. The selector semantics follow Akvorado's
// mature NetFlow decoder.

const maxNetFlowSamplingEntries = 65536

type netflowSamplingKey struct {
	router              string
	version             uint16
	observationDomainID uint32
	samplerID           uint64
}

func (p *metadataProducer) applyNetFlowSampling(message any, args *producer.ProduceArgs, messages []producer.ProducerMessage) error {
	if p.netflowSampling == nil {
		return errors.New("NetFlow sampling state is not initialized")
	}
	var version uint16
	var observationDomainID uint32
	var flowSets []any
	switch packet := message.(type) {
	case *netflow.NFv9Packet:
		version = 9
		observationDomainID = packet.SourceId
		flowSets = packet.FlowSets
	case *netflow.IPFIXPacket:
		version = 10
		observationDomainID = packet.ObservationDomainId
		flowSets = packet.FlowSets
	default:
		return nil
	}

	router := args.Src.String()
	if args.FlowContext != nil {
		router = args.FlowContext.RouterKey
	}
	rates := make([]uint64, 0, len(messages))
	for _, flowSet := range flowSets {
		switch value := flowSet.(type) {
		case netflow.OptionsDataFlowSet:
			p.updateNetFlowSamplingOptions(router, version, observationDomainID, value)
		case netflow.DataFlowSet:
			for _, record := range value.Records {
				rates = append(rates, p.netFlowRecordSamplingRate(router, version, observationDomainID, record.Values))
			}
		}
	}
	if len(rates) != len(messages) {
		return fmt.Errorf("GoFlow2 NetFlow record/rate count differs: %d/%d", len(messages), len(rates))
	}
	for index, message := range messages {
		flowMessage, ok := message.(*protoproducer.ProtoProducerMessage)
		if !ok {
			return fmt.Errorf("unexpected GoFlow2 NetFlow message type %T", message)
		}
		flowMessage.SamplingRate = rates[index]
	}
	return nil
}

func (p *metadataProducer) updateNetFlowSamplingOptions(router string, version uint16, observationDomainID uint32, flowSet netflow.OptionsDataFlowSet) {
	for _, record := range flowSet.Records {
		var samplerID, rate, packetInterval, packetSpace uint64
		for _, field := range record.OptionsValues {
			if field.PenProvided {
				continue
			}
			value, ok := netflowUnsigned(field.Value)
			if !ok {
				continue
			}
			switch field.Type {
			case netflow.IPFIX_FIELD_samplingInterval, netflow.IPFIX_FIELD_samplerRandomInterval:
				rate = value
			case netflow.IPFIX_FIELD_samplerId, netflow.IPFIX_FIELD_selectorId:
				samplerID = value
			case netflow.IPFIX_FIELD_samplingPacketInterval:
				packetInterval = value
			case netflow.IPFIX_FIELD_samplingPacketSpace:
				packetSpace = value
			}
		}
		if packetInterval > 0 && packetSpace <= math.MaxUint64-packetInterval {
			rate = (packetInterval + packetSpace) / packetInterval
		}
		if rate > 0 {
			_, _ = p.netflowSampling.Set(netflowSamplingKey{router, version, observationDomainID, samplerID}, rate)
		}
	}
}

func (p *metadataProducer) netFlowRecordSamplingRate(router string, version uint16, observationDomainID uint32, fields []netflow.DataField) uint64 {
	var samplerID, directRate uint64
	selectorPresent := false
	for _, field := range fields {
		if field.PenProvided {
			continue
		}
		value, ok := netflowUnsigned(field.Value)
		if !ok {
			continue
		}
		switch field.Type {
		case netflow.IPFIX_FIELD_samplingInterval, netflow.IPFIX_FIELD_samplerRandomInterval:
			directRate = value
		case netflow.IPFIX_FIELD_samplerId, netflow.IPFIX_FIELD_selectorId:
			samplerID = value
			selectorPresent = true
		}
	}
	if directRate > 0 {
		return directRate
	}
	if !selectorPresent {
		samplerID = 0
	}
	var rate uint64
	p.netflowSampling.Get(netflowSamplingKey{router, version, observationDomainID, samplerID}, &rate)
	return rate
}

func netflowUnsigned(value any) (uint64, bool) {
	bytes, ok := value.([]byte)
	if !ok || len(bytes) == 0 || len(bytes) > 8 {
		return 0, false
	}
	switch len(bytes) {
	case 1:
		return uint64(bytes[0]), true
	case 2:
		return uint64(binary.BigEndian.Uint16(bytes)), true
	case 4:
		return uint64(binary.BigEndian.Uint32(bytes)), true
	case 8:
		return binary.BigEndian.Uint64(bytes), true
	default:
		var result uint64
		for _, value := range bytes {
			result = result<<8 | uint64(value)
		}
		return result, true
	}
}

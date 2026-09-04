package flowcollect

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"time"

	"github.com/netsampler/goflow2/v3/decoders/netflow"
	"github.com/netsampler/goflow2/v3/decoders/netflowlegacy"
	"github.com/netsampler/goflow2/v3/decoders/sflow"
	"github.com/netsampler/goflow2/v3/producer"
	protoproducer "github.com/netsampler/goflow2/v3/producer/proto"
	"github.com/netsampler/goflow2/v3/utils/store/templates"
)

var ErrTemplatePending = errors.New("NetFlow/IPFIX template is not available")

type DecodedDatagram struct {
	Protocol            Protocol
	ObservationDomainID uint64
	AgentIP             netip.Addr
	SubAgentID          uint32
	DatagramSequence    uint32
	TemplateChanged     bool
	Records             []DecodedRecord
}

type DecodedRecord struct {
	EventTime      time.Time
	InIf           uint32
	OutIf          uint32
	SrcIP          netip.Addr
	DstIP          netip.Addr
	SrcPort        uint32
	DstPort        uint32
	IPProtocol     uint32
	TCPFlags       uint32
	RawBytes       uint64
	RawPackets     uint64
	SamplingRate   uint64
	FlowDuration   time.Duration
	SrcAS          uint32
	DstAS          uint32
	SourceIDType   uint32
	SourceIDValue  uint32
	SampleSequence uint32
	SamplePool     uint64
	ExporterDrops  uint64
}

type Decoder struct {
	templates *templates.TemplateFlowStore
	producer  producer.ProducerInterface
}

func NewDecoder() (*Decoder, error) {
	producerConfig, err := (&protoproducer.ProducerConfig{}).Compile()
	if err != nil {
		return nil, fmt.Errorf("compile GoFlow2 producer config: %w", err)
	}
	protoProducer, err := protoproducer.CreateProtoProducer(producerConfig, nil)
	if err != nil {
		return nil, fmt.Errorf("create GoFlow2 producer: %w", err)
	}
	templateStore := templates.NewTemplateFlowStore(templates.WithTTL(30*time.Minute), templates.WithExtendOnAccess(true))
	templateStore.Start()
	return &Decoder{templates: templateStore, producer: protoProducer}, nil
}

func (d *Decoder) Close() {
	if d.producer != nil {
		d.producer.Close()
	}
	if d.templates != nil {
		d.templates.Close()
	}
}

func (d *Decoder) Decode(record WALRecord) (DecodedDatagram, error) {
	protocol, domain, err := InspectDatagram(record.Payload)
	if err != nil {
		return DecodedDatagram{}, err
	}
	if record.Protocol != 0 && record.Protocol != protocol {
		return DecodedDatagram{}, errors.New("WAL protocol does not match datagram version")
	}
	switch protocol {
	case ProtocolSFlow5:
		return d.decodeSFlow(record)
	case ProtocolNetFlow5:
		return d.decodeNetFlow5(record)
	case ProtocolNetFlow9, ProtocolIPFIX:
		return d.decodeTemplateFlow(record, protocol, domain)
	default:
		return DecodedDatagram{}, errors.New("unsupported flow protocol")
	}
}

func (d *Decoder) decodeSFlow(record WALRecord) (DecodedDatagram, error) {
	var packet sflow.Packet
	if err := sflow.DecodeMessageVersion(bytes.NewBuffer(record.Payload), &packet); err != nil {
		return DecodedDatagram{}, fmt.Errorf("decode sFlow v5: %w", err)
	}
	result := DecodedDatagram{Protocol: ProtocolSFlow5, AgentIP: bytesToAddr(packet.AgentIP), SubAgentID: packet.SubAgentId, DatagramSequence: packet.SequenceNumber}
	for _, sample := range packet.Samples {
		meta, ok := sflowSampleMetadata(sample)
		if !ok {
			continue
		}
		messages, err := protoproducer.SearchSFlowSamplesConfig([]interface{}{sample}, nil)
		if err != nil {
			return DecodedDatagram{}, fmt.Errorf("map sFlow sample: %w", err)
		}
		for _, message := range messages {
			mapped, ok := message.(*protoproducer.ProtoProducerMessage)
			if !ok {
				continue
			}
			decoded := decodedFromMessage(&mapped.FlowMessage, record.ReceivedAt)
			decoded.SourceIDType = meta.sourceIDType
			decoded.SourceIDValue = meta.sourceIDValue
			decoded.SampleSequence = meta.sequence
			decoded.SamplePool = meta.pool
			decoded.ExporterDrops = meta.drops
			result.Records = append(result.Records, decoded)
		}
		d.producer.Commit(messages)
	}
	return result, nil
}

type sampleMetadata struct {
	sourceIDType, sourceIDValue, sequence uint32
	pool, drops                           uint64
}

func sflowSampleMetadata(sample interface{}) (sampleMetadata, bool) {
	switch value := sample.(type) {
	case sflow.FlowSample:
		return sampleMetadata{value.Header.SourceIdType, value.Header.SourceIdValue, value.Header.SampleSequenceNumber, uint64(value.SamplePool), uint64(value.Drops)}, true
	case sflow.ExpandedFlowSample:
		return sampleMetadata{value.Header.SourceIdType, value.Header.SourceIdValue, value.Header.SampleSequenceNumber, uint64(value.SamplePool), uint64(value.Drops)}, true
	default:
		return sampleMetadata{}, false
	}
}

func (d *Decoder) decodeNetFlow5(record WALRecord) (DecodedDatagram, error) {
	var packet netflowlegacy.PacketNetFlowV5
	if err := netflowlegacy.DecodeMessageVersion(bytes.NewBuffer(record.Payload), &packet); err != nil {
		return DecodedDatagram{}, fmt.Errorf("decode NetFlow v5: %w", err)
	}
	messages, err := d.producer.Produce(&packet, produceArgs(record))
	if err != nil {
		return DecodedDatagram{}, err
	}
	defer d.producer.Commit(messages)
	return DecodedDatagram{Protocol: ProtocolNetFlow5, DatagramSequence: packet.FlowSequence, Records: copyMessages(messages, record.ReceivedAt)}, nil
}

func (d *Decoder) decodeTemplateFlow(record WALRecord, protocol Protocol, domain uint64) (DecodedDatagram, error) {
	ctx := netflow.FlowContext{RouterKey: record.Source.Addr().String()}
	var nf9 netflow.NFv9Packet
	var ipfix netflow.IPFIXPacket
	if err := netflow.DecodeMessageVersion(bytes.NewBuffer(record.Payload), d.templates, ctx, &nf9, &ipfix); err != nil {
		return DecodedDatagram{}, fmt.Errorf("decode NetFlow/IPFIX: %w", err)
	}
	var packet interface{}
	var sequence uint32
	var sets []interface{}
	if protocol == ProtocolNetFlow9 {
		packet, sequence, sets = &nf9, nf9.SequenceNumber, nf9.FlowSets
	} else {
		packet, sequence, sets = &ipfix, ipfix.SequenceNumber, ipfix.FlowSets
	}
	messages, err := d.producer.Produce(packet, produceArgs(record))
	if err != nil {
		return DecodedDatagram{}, err
	}
	defer d.producer.Commit(messages)
	result := DecodedDatagram{Protocol: protocol, ObservationDomainID: domain, DatagramSequence: sequence, TemplateChanged: containsTemplate(sets), Records: copyMessages(messages, record.ReceivedAt)}
	if containsRawFlowSet(sets) {
		return result, ErrTemplatePending
	}
	return result, nil
}

func produceArgs(record WALRecord) *producer.ProduceArgs {
	return &producer.ProduceArgs{Src: record.Source, SamplerAddress: record.Source.Addr(), TimeReceived: record.ReceivedAt, FlowContext: &netflow.FlowContext{RouterKey: record.Source.Addr().String()}}
}

func copyMessages(messages []producer.ProducerMessage, receivedAt time.Time) []DecodedRecord {
	records := make([]DecodedRecord, 0, len(messages))
	for _, message := range messages {
		mapped, ok := message.(*protoproducer.ProtoProducerMessage)
		if !ok {
			continue
		}
		records = append(records, decodedFromMessage(&mapped.FlowMessage, receivedAt))
	}
	return records
}

func decodedFromMessage(message interface {
	GetTimeFlowStartNs() uint64
	GetTimeFlowEndNs() uint64
	GetInIf() uint32
	GetOutIf() uint32
	GetSrcAddr() []byte
	GetDstAddr() []byte
	GetSrcPort() uint32
	GetDstPort() uint32
	GetProto() uint32
	GetTcpFlags() uint32
	GetBytes() uint64
	GetPackets() uint64
	GetSamplingRate() uint64
	GetSrcAs() uint32
	GetDstAs() uint32
}, receivedAt time.Time) DecodedRecord {
	eventTime := receivedAt
	if ns := message.GetTimeFlowEndNs(); ns > 0 && ns <= math.MaxInt64 {
		eventTime = time.Unix(0, int64(ns))
	}
	duration := time.Duration(0)
	start, end := message.GetTimeFlowStartNs(), message.GetTimeFlowEndNs()
	if end >= start && end-start <= math.MaxInt64 {
		duration = time.Duration(end - start)
	}
	return DecodedRecord{EventTime: eventTime, InIf: message.GetInIf(), OutIf: message.GetOutIf(), SrcIP: bytesToAddr(message.GetSrcAddr()), DstIP: bytesToAddr(message.GetDstAddr()), SrcPort: message.GetSrcPort(), DstPort: message.GetDstPort(), IPProtocol: message.GetProto(), TCPFlags: message.GetTcpFlags(), RawBytes: message.GetBytes(), RawPackets: message.GetPackets(), SamplingRate: message.GetSamplingRate(), FlowDuration: duration, SrcAS: message.GetSrcAs(), DstAS: message.GetDstAs()}
}

func bytesToAddr(value []byte) netip.Addr {
	addr, ok := netip.AddrFromSlice(value)
	if !ok {
		return netip.Addr{}
	}
	return addr.Unmap()
}

func containsRawFlowSet(sets []interface{}) bool {
	for _, set := range sets {
		if _, ok := set.(netflow.RawFlowSet); ok {
			return true
		}
	}
	return false
}

func containsTemplate(sets []interface{}) bool {
	for _, set := range sets {
		switch set.(type) {
		case netflow.TemplateFlowSet, netflow.NFv9OptionsTemplateFlowSet, netflow.IPFIXOptionsTemplateFlowSet:
			return true
		}
	}
	return false
}

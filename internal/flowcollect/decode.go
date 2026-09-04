package flowcollect

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"sync"
	"time"

	"github.com/netsampler/goflow2/v3/decoders/netflow"
	"github.com/netsampler/goflow2/v3/decoders/netflowlegacy"
	"github.com/netsampler/goflow2/v3/decoders/sflow"
	"github.com/netsampler/goflow2/v3/producer"
	protoproducer "github.com/netsampler/goflow2/v3/producer/proto"
	"github.com/netsampler/goflow2/v3/utils/store/samplingrate"
	"github.com/netsampler/goflow2/v3/utils/store/templates"
)

var ErrTemplatePending = errors.New("NetFlow/IPFIX template is not available")

const defaultDecoderStateTTL = 30 * time.Minute

type DecodedDatagram struct {
	Protocol            Protocol
	ObservationDomainID uint64
	AgentIP             netip.Addr
	SubAgentID          uint32
	DatagramSequence    uint32
	SequenceIncrement   uint32
	SequenceScope       uint64
	ExporterUptime      uint32
	ExporterUptimeValid bool
	ExporterEpoch       uint64
	TemplateChanged     bool
	CollectStateChanged bool
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
	SampleIndex    uint32
	QualityFlags   uint64
	QualityEpoch   uint64
}

type Decoder struct {
	templates    *templates.TemplateFlowStore
	sampling     samplingrate.Store
	producer     producer.ProducerInterface
	stateMu      sync.Mutex
	stateChanges map[decoderStateKey]uint64
	stateTTL     time.Duration
}

type decoderStateKey struct {
	router  string
	version uint16
	domain  uint32
}

func NewDecoder() (*Decoder, error) {
	return NewDecoderWithStateTTL(defaultDecoderStateTTL)
}

func NewDecoderWithStateTTL(stateTTL time.Duration) (*Decoder, error) {
	if stateTTL <= 0 {
		return nil, errors.New("decoder state TTL must be positive")
	}
	producerConfig, err := (&protoproducer.ProducerConfig{}).Compile()
	if err != nil {
		return nil, fmt.Errorf("compile GoFlow2 producer config: %w", err)
	}
	decoder := &Decoder{stateChanges: make(map[decoderStateKey]uint64), stateTTL: stateTTL}
	templateStore := templates.NewTemplateFlowStore(
		templates.WithTTL(stateTTL),
		templates.WithExtendOnAccess(true),
		templates.WithHooks(templates.TemplateHooks{
			OnAdd: func(router string, version uint16, domain uint32, _ uint16, _ interface{}, _ bool) {
				decoder.markStateChanged(decoderStateKey{router: router, version: version, domain: domain})
			},
			OnRemove: func(router string, version uint16, domain uint32, _ uint16, _ interface{}) {
				decoder.markStateChanged(decoderStateKey{router: router, version: version, domain: domain})
			},
		}),
	)
	samplingStore := samplingrate.NewSamplingRateFlowStore(
		samplingrate.WithTTL(stateTTL),
		samplingrate.WithExtendOnAccess(true),
		samplingrate.WithHooks(samplingrate.Hooks{
			OnSet: func(router string, version uint16, domain uint32, _ uint32, _ bool) {
				decoder.markStateChanged(decoderStateKey{router: router, version: version, domain: domain})
			},
			OnRemove: func(router string, version uint16, domain uint32, _ uint32) {
				decoder.markStateChanged(decoderStateKey{router: router, version: version, domain: domain})
			},
		}),
	)
	protoProducer, err := protoproducer.CreateProtoProducer(producerConfig, samplingStore)
	if err != nil {
		return nil, fmt.Errorf("create GoFlow2 producer: %w", err)
	}
	templateStore.Start()
	decoder.templates = templateStore
	decoder.sampling = samplingStore
	decoder.producer = protoProducer
	return decoder, nil
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
	result := DecodedDatagram{Protocol: ProtocolSFlow5, AgentIP: bytesToAddr(packet.AgentIP), SubAgentID: packet.SubAgentId, DatagramSequence: packet.SequenceNumber, SequenceIncrement: 1, ExporterUptime: packet.Uptime, ExporterUptimeValid: true}
	for sampleIndex, sample := range packet.Samples {
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
			decoded.SampleIndex = uint32(sampleIndex) + 1
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
	return DecodedDatagram{Protocol: ProtocolNetFlow5, DatagramSequence: packet.FlowSequence, SequenceIncrement: uint32(len(packet.Records)), SequenceScope: uint64(packet.EngineType)<<8 | uint64(packet.EngineId), ExporterUptime: packet.SysUptime, ExporterUptimeValid: true, Records: copyMessages(messages, record.ReceivedAt)}, nil
}

func (d *Decoder) decodeTemplateFlow(record WALRecord, protocol Protocol, domain uint64) (DecodedDatagram, error) {
	version := protocolVersion(protocol)
	if domain > math.MaxUint32 || version == 0 {
		return DecodedDatagram{}, errors.New("invalid NetFlow/IPFIX decoder state key")
	}
	stateKey := decoderStateKey{router: record.Source.Addr().String(), version: version, domain: uint32(domain)}
	stateRevision := d.stateRevision(stateKey)
	ctx := netflow.FlowContext{RouterKey: record.Source.Addr().String()}
	var nf9 netflow.NFv9Packet
	var ipfix netflow.IPFIXPacket
	if err := netflow.DecodeMessageVersion(bytes.NewBuffer(record.Payload), d.templates, ctx, &nf9, &ipfix); err != nil {
		partial := DecodedDatagram{Protocol: protocol, ObservationDomainID: domain, CollectStateChanged: d.stateRevision(stateKey) != stateRevision}
		if errors.Is(err, netflow.ErrorTemplateNotFound) {
			return partial, errors.Join(ErrTemplatePending, fmt.Errorf("decode NetFlow/IPFIX: %w", err))
		}
		return partial, fmt.Errorf("decode NetFlow/IPFIX: %w", err)
	}
	var packet interface{}
	var sequence uint32
	var sets []interface{}
	if protocol == ProtocolNetFlow9 {
		packet, sequence, sets = &nf9, nf9.SequenceNumber, nf9.FlowSets
	} else {
		packet, sequence, sets = &ipfix, ipfix.SequenceNumber, ipfix.FlowSets
	}
	sequenceIncrement := uint32(1)
	exporterUptime, exporterUptimeValid := nf9.SystemUptime, protocol == ProtocolNetFlow9
	if protocol == ProtocolIPFIX {
		sequenceIncrement = countIPFIXDataRecords(sets)
	}
	result := DecodedDatagram{Protocol: protocol, ObservationDomainID: domain, DatagramSequence: sequence, SequenceIncrement: sequenceIncrement, SequenceScope: domain, ExporterUptime: exporterUptime, ExporterUptimeValid: exporterUptimeValid, TemplateChanged: containsTemplate(sets), CollectStateChanged: d.stateRevision(stateKey) != stateRevision}
	messages, err := d.producer.Produce(packet, produceArgs(record))
	if err != nil {
		result.CollectStateChanged = d.stateRevision(stateKey) != stateRevision
		return result, err
	}
	defer d.producer.Commit(messages)
	result.CollectStateChanged = d.stateRevision(stateKey) != stateRevision
	result.Records = copyMessages(messages, record.ReceivedAt)
	if containsRawFlowSet(sets) {
		return result, ErrTemplatePending
	}
	return result, nil
}

func countIPFIXDataRecords(sets []interface{}) uint32 {
	var count uint64
	for _, set := range sets {
		switch value := set.(type) {
		case netflow.DataFlowSet:
			count += uint64(len(value.Records))
		case netflow.OptionsDataFlowSet:
			count += uint64(len(value.Records))
		case *netflow.DataFlowSet:
			count += uint64(len(value.Records))
		case *netflow.OptionsDataFlowSet:
			count += uint64(len(value.Records))
		}
	}
	if count > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(count)
}

func (d *Decoder) SnapshotState(protocol Protocol, source netip.Addr, domain uint64) ([]byte, []byte, uint64, error) {
	version := protocolVersion(protocol)
	if d == nil || d.templates == nil || d.sampling == nil || !source.IsValid() || version == 0 || domain > math.MaxUint32 {
		return nil, nil, 0, errors.New("invalid decoder state snapshot key")
	}
	router := source.Unmap().String()
	templateDocument := make(map[string]map[string]interface{})
	if entries := d.templates.GetAll()[router]; len(entries) > 0 {
		selected := make(map[string]interface{})
		for key, value := range entries {
			entryVersion := uint16(key >> 48)
			entryDomain := uint32((key >> 16) & math.MaxUint32)
			if entryVersion == version && entryDomain == uint32(domain) {
				selected[fmt.Sprintf("%d/%d/%d", entryVersion, entryDomain, uint16(key))] = value
			}
		}
		if len(selected) > 0 {
			templateDocument[router] = selected
		}
	}
	templateJSON, err := json.Marshal(templateDocument)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("marshal template state: %w", err)
	}
	samplingDocument := make(map[string]map[string]uint32)
	if entries := d.sampling.GetAll()[router]; len(entries) > 0 {
		selected := make(map[string]uint32)
		for key, rate := range entries {
			entryVersion := uint16(key >> 32)
			entryDomain := uint32(key)
			if entryVersion == version && entryDomain == uint32(domain) {
				selected[fmt.Sprintf("%d/%d", entryVersion, entryDomain)] = rate
			}
		}
		if len(selected) > 0 {
			samplingDocument[router] = selected
		}
	}
	samplingJSON, err := json.Marshal(samplingDocument)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("marshal sampling-rate state: %w", err)
	}
	generation := d.stateRevision(decoderStateKey{router: router, version: version, domain: uint32(domain)})
	if generation == 0 {
		return nil, nil, 0, errors.New("decoder state generation is zero")
	}
	return templateJSON, samplingJSON, generation, nil
}

func (d *Decoder) RestoreState(templateJSON, samplingJSON []byte) error {
	if d == nil || d.templates == nil || d.sampling == nil {
		return errors.New("decoder state stores are not initialized")
	}
	if err := templates.LoadJSON(d.templates, templateJSON); err != nil {
		return fmt.Errorf("restore template state: %w", err)
	}
	if err := samplingrate.LoadJSON(d.sampling, samplingJSON); err != nil {
		return fmt.Errorf("restore sampling-rate state: %w", err)
	}
	return nil
}

func (d *Decoder) RestoreStateRevision(protocol Protocol, source netip.Addr, domain, generation uint64, templateJSON, samplingJSON []byte) error {
	version := protocolVersion(protocol)
	if !source.IsValid() || version == 0 || domain > math.MaxUint32 || generation == 0 {
		return errors.New("invalid decoder state restore key")
	}
	if err := d.RestoreState(templateJSON, samplingJSON); err != nil {
		return err
	}
	key := decoderStateKey{router: source.Unmap().String(), version: version, domain: uint32(domain)}
	d.stateMu.Lock()
	if d.stateChanges[key] < generation {
		d.stateChanges[key] = generation
	}
	d.stateMu.Unlock()
	return nil
}

func (d *Decoder) markStateChanged(key decoderStateKey) {
	d.stateMu.Lock()
	d.stateChanges[key]++
	d.stateMu.Unlock()
}

func (d *Decoder) stateRevision(key decoderStateKey) uint64 {
	d.stateMu.Lock()
	defer d.stateMu.Unlock()
	return d.stateChanges[key]
}

func protocolVersion(protocol Protocol) uint16 {
	switch protocol {
	case ProtocolNetFlow9:
		return 9
	case ProtocolIPFIX:
		return 10
	default:
		return 0
	}
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

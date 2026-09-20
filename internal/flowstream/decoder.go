// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"sync"
	"time"

	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
	"github.com/netsampler/goflow2/v3/decoders/netflow"
	goflowpb "github.com/netsampler/goflow2/v3/pb"
	"github.com/netsampler/goflow2/v3/producer"
	protoproducer "github.com/netsampler/goflow2/v3/producer/proto"
	"github.com/netsampler/goflow2/v3/utils"
	"github.com/netsampler/goflow2/v3/utils/store/samplingrate"
	"github.com/netsampler/goflow2/v3/utils/store/templates"
	"github.com/twmb/franz-go/pkg/kgo"
)

const defaultDecoderStateTTL = 30 * time.Minute

type DecodedBatch struct {
	CollectorID         string
	ListenerID          string
	RegistryVersion     uint64
	ReceivedAt          time.Time
	Source              netip.AddrPort
	FlowType            goflowpb.FlowMessage_FlowType
	ObservationDomainID uint64
	SubAgentID          uint32
	DatagramSequence    uint32
	AgentIP             netip.Addr
	Records             []DecodedRecord
	RecordMetadata      []DecodedRecordMetadata
	CounterRecords      []DecodedCounterRecord
}

// DecodedCounterRecord is one generic-interface counter record carried by an
// sFlow counter sample. Values are cumulative device counters; consumers must
// difference adjacent records and handle reset/wrap before deriving rates.
type DecodedCounterRecord struct {
	SubAgentID     uint32
	SourceIDType   uint32
	SourceIDValue  uint32
	SampleSequence uint32
	SampleIndex    uint32
	RecordIndex    uint32

	IfIndex            uint32
	IfType             uint32
	IfSpeed            uint64
	IfDirection        uint32
	IfStatus           uint32
	IfInOctets         uint64
	IfInUcastPkts      uint32
	IfInMulticastPkts  uint32
	IfInBroadcastPkts  uint32
	IfInDiscards       uint32
	IfInErrors         uint32
	IfInUnknownProtos  uint32
	IfOutOctets        uint64
	IfOutUcastPkts     uint32
	IfOutMulticastPkts uint32
	IfOutBroadcastPkts uint32
	IfOutDiscards      uint32
	IfOutErrors        uint32
	IfPromiscuousMode  uint32
}

// DecodedRecord is the carrier for one decoded flow record: a plain Go value
// struct rather than GoFlow2's protobuf FlowMessage, so the hot path never pays
// that message's per-record state machinery (Reset + atomic StoreMessageInfo,
// once the top decode cost). It carries every field GoFlow2 produces for these
// protocols that is meaningful network data — not just what mapFlowMessage reads
// today — so no information is lost at the decode boundary (loss detection needs
// SequenceNum, prefix work needs Src/DstNet, L2 needs VLANs, QoS needs IpTos,
// etc.). Field names mirror goflowpb.FlowMessage so mapping stays a plain copy.
// The slow path converts GoFlow2 messages into this via recordFromFlowMessage.
type DecodedRecord struct {
	SrcAddr         []byte
	DstAddr         []byte
	NextHop         []byte
	SamplerAddress  []byte
	Type            goflowpb.FlowMessage_FlowType
	SrcPort         uint32
	DstPort         uint32
	Proto           uint32
	TcpFlags        uint32
	IpTos           uint32
	Etype           uint32
	InIf            uint32
	OutIf           uint32
	SrcAs           uint32
	DstAs           uint32
	SrcNet          uint32
	DstNet          uint32
	SrcVlan         uint32
	DstVlan         uint32
	SequenceNum     uint32
	Bytes           uint64
	Packets         uint64
	SamplingRate    uint64
	TimeFlowStartNs uint64
	TimeFlowEndNs   uint64
	TimeReceivedNs  uint64
}

// recordFromFlowMessage copies the meaningful fields out of a GoFlow2 message
// (slow path: v9/IPFIX and sFlow fallback). Address slices reference the pooled
// message and are valid only until the next Decode, per the contract.
func recordFromFlowMessage(dst *DecodedRecord, m *goflowpb.FlowMessage) {
	dst.Type = m.Type
	dst.SrcAddr, dst.DstAddr, dst.NextHop = m.SrcAddr, m.DstAddr, m.NextHop
	dst.SamplerAddress = m.SamplerAddress
	dst.SrcPort, dst.DstPort = m.SrcPort, m.DstPort
	dst.Proto, dst.TcpFlags, dst.IpTos, dst.Etype = m.Proto, m.TcpFlags, m.IpTos, m.Etype
	dst.InIf, dst.OutIf = m.InIf, m.OutIf
	dst.SrcAs, dst.DstAs = m.SrcAs, m.DstAs
	dst.SrcNet, dst.DstNet = m.SrcNet, m.DstNet
	dst.SrcVlan, dst.DstVlan = m.SrcVlan, m.DstVlan
	dst.SequenceNum = m.SequenceNum
	dst.Bytes, dst.Packets = m.Bytes, m.Packets
	dst.SamplingRate = m.SamplingRate
	dst.TimeFlowStartNs, dst.TimeFlowEndNs, dst.TimeReceivedNs = m.TimeFlowStartNs, m.TimeFlowEndNs, m.TimeReceivedNs
}

func growRecords(dst []DecodedRecord, n int) []DecodedRecord {
	if cap(dst) >= n {
		return dst[:n]
	}
	return make([]DecodedRecord, n)
}

// Decoder owns one GoFlow2 template and sampling state set. It is not safe for
// concurrent use and must be assigned to exactly one Kafka partition worker.
//
// Zero-copy contract: Decode returns DecodedRecords in the decoder's reused
// recordBacking array, whose address []byte fields slice either the source
// payload (fast paths) or GoFlow2's pooled buffers (slow path) — none marshalled
// or deep-copied. All stay valid only until the NEXT Decode call on this decoder,
// which overwrites recordBacking (and recycles the pooled buffers) before
// decoding again. The caller MUST fully consume (map into its own structs) each
// returned batch before it calls Decode again on the same decoder. The
// per-partition worker satisfies this by mapping every record of a batch before
// decoding the next record.
type Decoder struct {
	pipe      *utils.AutoFlowPipe
	producer  producer.ProducerInterface
	metadata  *metadataProducer
	templates *templates.TemplateFlowStore
	// recordBacking is the reused DecodedRecord array returned as
	// DecodedBatch.Records by every path (fast NetFlow v5, fast sFlow, and the
	// GoFlow2 slow path via recordFromFlowMessage). Valid only until the next
	// Decode, per the zero-copy contract.
	recordBacking []DecodedRecord
	// fastNetFlowV5 selects the fixed-offset NetFlow v5 decoder over GoFlow2's pipe.
	fastNetFlowV5 bool
	// rawScratch is the reused envelope target for DecodeValue's zero-copy parse.
	rawScratch flowpb.RawFlow
	// idIntern caches collector/listener identity strings so the envelope parse
	// stops allocating them after warmup. lastSampler* caches the per-datagram
	// NetFlow sampler address (a partition serves one exporter) to keep it 0-alloc.
	idIntern         map[string]string
	lastSamplerAddr  netip.Addr
	lastSamplerBytes []byte
	// fastSFlow selects the hand-written sFlow v5 framing decoder. sflowHeader
	// Scratch is the one GoFlow2 message reused only for SampledHeader records
	// (fed to the hardened ParseSampledHeader). sflowMetadata / sflow{AgentIP,
	// SubAgent,Sequence} are its reused outputs and datagram identity.
	fastSFlow           bool
	sflowHeaderScratch  protoproducer.ProtoProducerMessage
	sflowMetadata       []DecodedRecordMetadata
	sflowCounterBacking []DecodedCounterRecord
	sflowAgentIP        netip.Addr
	sflowSubAgent       uint32
	sflowSequence       uint32
}

// maxInternedIDs bounds the collector/listener identity cache so hostile input
// carrying many distinct identities cannot grow it without limit.
const maxInternedIDs = 1024

// guardedTemplateStore rejects NetFlow v9 / IPFIX templates that would make a
// data set decode consume zero bytes per record. goflow2's DecodeDataSet loops
// `for payload.Len() >= listFieldsSize`; when a template has no fields (or only
// zero-length fixed fields) listFieldsSize is 0, so every iteration reads nothing
// and appends an empty record forever — an unbounded loop and heap growth that
// recoverDecoderPanic cannot interrupt (it catches panics, not live loops) and
// that OOMs the worker. A single crafted or truncated datagram triggers it, e.g.
// an RFC 7011 template withdrawal (field count 0) followed by a data set for that
// id, or reordered UDP delivering the two out of order. Rejecting at store time
// leaves the later data set to resolve as a finite template-not-found error.
type guardedTemplateStore struct {
	*templates.TemplateFlowStore
}

func (g guardedTemplateStore) AddTemplate(ctx netflow.FlowContext, version uint16, obsDomainID uint32, templateID uint16, template interface{}) (netflow.TemplateStatus, error) {
	if zeroProgressTemplate(version, template) {
		return netflow.TemplateUnchanged, fmt.Errorf("flowstream: rejecting zero-progress NetFlow/IPFIX template obs=%d id=%d", obsDomainID, templateID)
	}
	return g.TemplateFlowStore.AddTemplate(ctx, version, obsDomainID, templateID, template)
}

// zeroProgressTemplate reports whether decoding a data set with this template
// would make no forward progress. Options templates loop on scopes+options, so
// they stall only when both sides are zero-progress.
func zeroProgressTemplate(version uint16, template interface{}) bool {
	switch t := template.(type) {
	case netflow.TemplateRecord:
		return fieldsScoreZero(version, t.Fields)
	case netflow.IPFIXOptionsTemplateRecord:
		return fieldsScoreZero(version, t.Scopes) && fieldsScoreZero(version, t.Options)
	case netflow.NFv9OptionsTemplateRecord:
		return fieldsScoreZero(version, t.Scopes) && fieldsScoreZero(version, t.Options)
	default:
		return false
	}
}

// fieldsScoreZero reports whether these fields score zero fixed bytes and carry
// no variable-length field. A variable-length (0xffff) field reads at least one
// byte per record, so the decode terminates on buffer exhaustion; without one, a
// zero scorable size is the unbounded-loop condition.
func fieldsScoreZero(version uint16, fields []netflow.Field) bool {
	if len(fields) == 0 {
		return true
	}
	for _, field := range fields {
		if field.Length == 0xffff {
			return false
		}
	}
	return netflow.GetTemplateSize(version, fields) == 0
}

func NewDecoder(stateTTL time.Duration) (*Decoder, error) {
	if stateTTL <= 0 {
		stateTTL = defaultDecoderStateTTL
	}
	producerConfig, err := (&protoproducer.ProducerConfig{}).Compile()
	if err != nil {
		return nil, fmt.Errorf("compile GoFlow2 producer configuration: %w", err)
	}
	templateStore := templates.NewTemplateFlowStore(
		templates.WithTTL(stateTTL),
		templates.WithExtendOnAccess(true),
	)
	samplingStore := samplingrate.NewSamplingRateFlowStore(
		samplingrate.WithTTL(stateTTL),
		samplingrate.WithExtendOnAccess(true),
	)
	protoProducer, err := protoproducer.CreateProtoProducer(producerConfig, samplingStore)
	if err != nil {
		return nil, fmt.Errorf("create GoFlow2 producer: %w", err)
	}
	metadata := newMetadataProducer(protoProducer, stateTTL)
	decoder := &Decoder{producer: metadata, metadata: metadata, templates: templateStore, fastNetFlowV5: true, fastSFlow: true}
	// No Format/Transport: with a nil format the pipe's formatSend is a no-op, so
	// the pooled decode buffers are never marshalled. Decode reads them directly
	// from the producer (zero-copy) instead of unmarshalling a transport payload.
	decoder.pipe = utils.NewFlowPipe(&utils.PipeConfig{
		Producer:      metadata,
		TemplateStore: guardedTemplateStore{templateStore},
	})
	templateStore.Start()
	decoder.pipe.Start()
	return decoder, nil
}

func (d *Decoder) DecodeValue(value []byte) (DecodedBatch, error) {
	if d == nil || d.pipe == nil {
		return DecodedBatch{}, errors.New("GoFlow2 decoder is not initialized")
	}
	// Zero-copy envelope parse: Payload and SourceAddress slice value directly
	// rather than being copied by proto.Unmarshal, and d.rawScratch is reused
	// across calls. The returned batch never retains d.rawScratch, only slices of
	// value — valid until the next Decode under the Decoder zero-copy contract.
	if err := parseRawFlowInto(value, &d.rawScratch, d.internID); err != nil {
		return DecodedBatch{}, fmt.Errorf("parse raw flow: %w", err)
	}
	return d.Decode(&d.rawScratch)
}

// internID returns a stable, shared string for a collector/listener identity so
// the envelope parse stops allocating once the (small, fixed) set of identities
// on this partition is seen. The map lookup with a []byte key does not allocate.
func (d *Decoder) internID(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	if s, ok := d.idIntern[string(data)]; ok {
		return s
	}
	s := string(data)
	if d.idIntern == nil {
		d.idIntern = make(map[string]string, 8)
	}
	if len(d.idIntern) < maxInternedIDs {
		d.idIntern[s] = s
	}
	return s
}

// samplerAddress returns the NetFlow exporter address bytes, matching GoFlow2's
// SamplerAddress (source.Addr().Unmap().MarshalBinary()). It caches the last
// result — a partition serves one exporter — so this allocates once per source,
// not per datagram. A prior batch may still hold the previous slice; a source
// change installs a fresh slice and never mutates it.
func (d *Decoder) samplerAddress(source netip.AddrPort) []byte {
	addr := source.Addr().Unmap()
	if d.lastSamplerBytes == nil || addr != d.lastSamplerAddr {
		encoded, _ := addr.MarshalBinary()
		d.lastSamplerAddr = addr
		d.lastSamplerBytes = encoded
	}
	return d.lastSamplerBytes
}

// recoverDecoderPanic runs the GoFlow2 pipe and converts a decoder panic on
// crafted/malformed bytes into an error. The collector does not decode, so a
// panic-triggering datagram reaches the worker undecoded; without this, one such
// packet crashes the process, replays from its uncommitted offset, and crashes
// again — a permanent poison-pill loop. Turning the panic into a skippable
// decode error lets the offset advance and the partition keep making progress.
func recoverDecoderPanic(run func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("goflow2 decoder panic: %v", r)
		}
	}()
	return run()
}

func (d *Decoder) Decode(raw *flowpb.RawFlow) (DecodedBatch, error) {
	if err := validateRawFlow(raw); err != nil {
		return DecodedBatch{}, err
	}
	flowType, observationDomainID, err := inspectFlowProtocol(raw.Decoder, raw.Payload)
	if err != nil {
		return DecodedBatch{}, err
	}
	address, _ := netip.AddrFromSlice(raw.SourceAddress)
	source := netip.AddrPortFrom(address.Unmap(), uint16(raw.SourcePort))
	receivedAt := time.Unix(int64(raw.TimeReceived), 0).UTC()

	// Recycle the previous batch's pooled buffers before decoding into them
	// again. Safe under the zero-copy contract: the caller has finished mapping
	// the prior batch by the time it asks for the next one.
	d.metadata.recyclePending()

	// Fast path: NetFlow v5 has a fixed wire layout, decoded at fixed offsets
	// without GoFlow2's reflection reader, intermediate struct, or per-record
	// allocation. It produces FlowMessages identical to the slow path
	// (TestNetFlowV5FastMatchesGoFlow2). NetFlow v5 carries no sFlow sample
	// metadata or agent identity, so those batch fields are zero — matching what
	// the slow path's metadataProducer sets for a non-sFlow packet.
	if d.fastNetFlowV5 && flowType == goflowpb.FlowMessage_NETFLOW_V5 {
		records, ferr := decodeNetFlowV5Fast(raw.Payload, uint64(receivedAt.UnixNano()), d.samplerAddress(source), d.recordBacking)
		if ferr != nil {
			return DecodedBatch{}, ferr
		}
		d.recordBacking = records
		return d.makeBatch(raw, source, receivedAt, flowType, observationDomainID, 0, 0, netip.Addr{}, records, nil, nil), nil
	}

	// Fast path: hand-written sFlow v5 framing decoder, reusing GoFlow2's hardened
	// ParseSampledHeader for the untrusted packet parse. Produces the same
	// DecodedRecords + sample metadata as the slow path (TestSFlowFastMatchesGoFlow2)
	// for the handled record types, and falls back to GoFlow2 for the rest.
	if d.fastSFlow && flowType == goflowpb.FlowMessage_SFLOW_5 {
		err = recoverDecoderPanic(func() error { return d.decodeSFlowV5Fast(raw.Payload, uint64(receivedAt.UnixNano())) })
		if err == nil {
			if len(d.recordBacking) != len(d.sflowMetadata) {
				return DecodedBatch{}, errors.New("sflow fast path records and sample metadata are inconsistent")
			}
			return d.makeBatch(raw, source, receivedAt, flowType, observationDomainID, d.sflowSubAgent, d.sflowSequence, d.sflowAgentIP, d.recordBacking, d.sflowMetadata, d.sflowCounterBacking), nil
		}
		if !errors.Is(err, errSFlowFallback) {
			return DecodedBatch{}, err
		}
		// Unhandled construct: fall through and let GoFlow2 decode this datagram.
	}

	message := &utils.Message{Src: source, Payload: raw.Payload, Received: receivedAt}
	err = recoverDecoderPanic(func() error {
		switch raw.Decoder {
		case flowpb.RawFlow_DECODER_NETFLOW:
			return d.pipe.NetFlowPipe.DecodeFlow(message)
		case flowpb.RawFlow_DECODER_SFLOW:
			return d.pipe.SFlowPipe.DecodeFlow(message)
		default:
			return errors.New("unsupported raw flow decoder")
		}
	})
	if err != nil {
		// The deferred Commit inside DecodeFlow already handed any produced
		// buffers to recyclePending; the next Decode returns them to the pool.
		return DecodedBatch{}, err
	}
	// Slow path (v9/IPFIX, sFlow fallback): convert each pooled GoFlow2 message
	// into a DecodedRecord. The address slices still reference the pooled buffers,
	// valid until the next Decode recycles them (see the Decoder contract).
	records := growRecords(d.recordBacking, len(d.metadata.pending))
	for index, msg := range d.metadata.pending {
		ppm, ok := msg.(*protoproducer.ProtoProducerMessage)
		if !ok {
			return DecodedBatch{}, fmt.Errorf("unexpected GoFlow2 message type %T", msg)
		}
		recordFromFlowMessage(&records[index], &ppm.FlowMessage)
	}
	d.recordBacking = records
	metadata := append([]DecodedRecordMetadata(nil), d.metadata.records...)
	if flowType == goflowpb.FlowMessage_SFLOW_5 && len(records) != len(metadata) {
		return DecodedBatch{}, errors.New("GoFlow2 sFlow records and sample metadata are inconsistent")
	}
	return d.makeBatch(raw, source, receivedAt, flowType, observationDomainID, d.metadata.subAgentID, d.metadata.datagramSequence, d.metadata.agentIP, records, metadata, nil), nil
}

func (d *Decoder) makeBatch(raw *flowpb.RawFlow, source netip.AddrPort, receivedAt time.Time, flowType goflowpb.FlowMessage_FlowType, observationDomainID uint64, subAgentID, datagramSequence uint32, agentIP netip.Addr, records []DecodedRecord, metadata []DecodedRecordMetadata, counters []DecodedCounterRecord) DecodedBatch {
	return DecodedBatch{
		CollectorID:         raw.CollectorId,
		ListenerID:          raw.ListenerId,
		RegistryVersion:     raw.RegistryVersion,
		ReceivedAt:          receivedAt,
		Source:              source,
		FlowType:            flowType,
		ObservationDomainID: observationDomainID,
		SubAgentID:          subAgentID,
		DatagramSequence:    datagramSequence,
		AgentIP:             agentIP,
		Records:             records,
		RecordMetadata:      metadata,
		CounterRecords:      counters,
	}
}

func inspectFlowProtocol(decoder flowpb.RawFlow_Decoder, payload []byte) (goflowpb.FlowMessage_FlowType, uint64, error) {
	switch decoder {
	case flowpb.RawFlow_DECODER_SFLOW:
		if len(payload) < 4 || binary.BigEndian.Uint32(payload[:4]) != 5 {
			return goflowpb.FlowMessage_FLOWUNKNOWN, 0, errors.New("unsupported sFlow version")
		}
		return goflowpb.FlowMessage_SFLOW_5, 0, nil
	case flowpb.RawFlow_DECODER_NETFLOW:
		if len(payload) < 2 {
			return goflowpb.FlowMessage_FLOWUNKNOWN, 0, errors.New("NetFlow/IPFIX header is truncated")
		}
		switch binary.BigEndian.Uint16(payload[:2]) {
		case 5:
			return goflowpb.FlowMessage_NETFLOW_V5, 0, nil
		case 9:
			if len(payload) < 20 {
				return goflowpb.FlowMessage_FLOWUNKNOWN, 0, errors.New("NetFlow v9 header is truncated")
			}
			return goflowpb.FlowMessage_NETFLOW_V9, uint64(binary.BigEndian.Uint32(payload[16:20])), nil
		case 10:
			if len(payload) < 16 {
				return goflowpb.FlowMessage_FLOWUNKNOWN, 0, errors.New("IPFIX header is truncated")
			}
			return goflowpb.FlowMessage_IPFIX, uint64(binary.BigEndian.Uint32(payload[12:16])), nil
		default:
			return goflowpb.FlowMessage_FLOWUNKNOWN, 0, errors.New("unsupported NetFlow/IPFIX version")
		}
	default:
		return goflowpb.FlowMessage_FLOWUNKNOWN, 0, errors.New("unsupported raw flow decoder")
	}
}

func validateRawFlow(raw *flowpb.RawFlow) error {
	if raw == nil {
		return errors.New("raw flow is required")
	}
	if raw.CollectorId == "" || len(raw.CollectorId) > maxIdentitySize || raw.ListenerId == "" || len(raw.ListenerId) > maxIdentitySize {
		return errors.New("raw flow collector and listener identities are invalid")
	}
	if raw.RegistryVersion == 0 {
		return errors.New("raw flow registry version must be positive")
	}
	if raw.TimeReceived > math.MaxInt64 {
		return errors.New("raw flow receive time is invalid")
	}
	if len(raw.Payload) == 0 || len(raw.Payload) > maxPayloadSize {
		return errors.New("raw flow payload size is invalid")
	}
	if raw.SourcePort > math.MaxUint16 {
		return errors.New("raw flow source port is invalid")
	}
	if _, ok := netip.AddrFromSlice(raw.SourceAddress); !ok {
		return errors.New("raw flow source address is invalid")
	}
	if raw.Decoder != flowpb.RawFlow_DECODER_NETFLOW && raw.Decoder != flowpb.RawFlow_DECODER_SFLOW {
		return errors.New("raw flow decoder is invalid")
	}
	return nil
}

func (d *Decoder) Close() {
	if d == nil {
		return
	}
	if d.pipe != nil {
		d.pipe.Close()
	}
	if d.producer != nil {
		d.producer.Close()
	}
	if d.templates != nil {
		d.templates.Close()
	}
}

type topicPartition struct {
	topic     string
	partition int32
}

// PartitionDecoders ensures NetFlow/IPFIX templates never cross Kafka
// partitions. Kafka rebalances may create a cold decoder on the new owner;
// template-missing data is observable but does not block later templates.
type PartitionDecoders struct {
	stateTTL time.Duration
	mu       sync.Mutex
	decoders map[topicPartition]*partitionDecoder
	closed   bool
}

type partitionDecoder struct {
	mu      sync.Mutex
	decoder *Decoder
}

func NewPartitionDecoders(stateTTL time.Duration) *PartitionDecoders {
	if stateTTL <= 0 {
		stateTTL = defaultDecoderStateTTL
	}
	return &PartitionDecoders{stateTTL: stateTTL, decoders: make(map[topicPartition]*partitionDecoder)}
}

func (d *PartitionDecoders) DecodeRecord(record *kgo.Record) (DecodedBatch, error) {
	if d == nil || record == nil {
		return DecodedBatch{}, errors.New("Kafka flow record is required")
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return DecodedBatch{}, errors.New("Kafka partition decoders are closed")
	}
	key := topicPartition{topic: record.Topic, partition: record.Partition}
	entry := d.decoders[key]
	if entry == nil {
		var err error
		decoder, err := NewDecoder(d.stateTTL)
		if err != nil {
			d.mu.Unlock()
			return DecodedBatch{}, err
		}
		entry = &partitionDecoder{decoder: decoder}
		d.decoders[key] = entry
	}
	d.mu.Unlock()

	entry.mu.Lock()
	defer entry.mu.Unlock()
	batch, err := entry.decoder.DecodeValue(record.Value)
	if err != nil {
		return DecodedBatch{}, err
	}
	return batch, nil
}

func (d *PartitionDecoders) Close() {
	if d == nil {
		return
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	entries := make([]*partitionDecoder, 0, len(d.decoders))
	for key, entry := range d.decoders {
		entries = append(entries, entry)
		delete(d.decoders, key)
	}
	d.mu.Unlock()
	for _, entry := range entries {
		entry.mu.Lock()
		entry.decoder.Close()
		entry.mu.Unlock()
	}
}

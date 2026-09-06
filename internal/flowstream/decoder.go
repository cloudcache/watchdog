// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"sync"
	"time"

	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
	formatbinary "github.com/netsampler/goflow2/v3/format/binary"
	goflowpb "github.com/netsampler/goflow2/v3/pb"
	"github.com/netsampler/goflow2/v3/producer"
	protoproducer "github.com/netsampler/goflow2/v3/producer/proto"
	"github.com/netsampler/goflow2/v3/utils"
	"github.com/netsampler/goflow2/v3/utils/store/samplingrate"
	"github.com/netsampler/goflow2/v3/utils/store/templates"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"
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
	Records             []*goflowpb.FlowMessage
	RecordMetadata      []DecodedRecordMetadata
}

// Decoder owns one GoFlow2 template and sampling state set. It is not safe for
// concurrent use and must be assigned to exactly one Kafka partition worker.
type Decoder struct {
	pipe      *utils.AutoFlowPipe
	producer  producer.ProducerInterface
	metadata  *metadataProducer
	templates *templates.TemplateFlowStore
	sink      captureTransport
}

type captureTransport struct {
	records []*goflowpb.FlowMessage
}

func (t *captureTransport) Send(_ []byte, data []byte) error {
	message := &goflowpb.FlowMessage{}
	if err := protodelim.UnmarshalFrom(bytes.NewReader(data), message); err != nil {
		return fmt.Errorf("unmarshal GoFlow2 message: %w", err)
	}
	t.records = append(t.records, message)
	return nil
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
	decoder := &Decoder{producer: metadata, metadata: metadata, templates: templateStore}
	decoder.pipe = utils.NewFlowPipe(&utils.PipeConfig{
		Format:        &formatbinary.BinaryDriver{},
		Transport:     &decoder.sink,
		Producer:      metadata,
		TemplateStore: templateStore,
	})
	templateStore.Start()
	decoder.pipe.Start()
	return decoder, nil
}

func (d *Decoder) DecodeValue(value []byte) (DecodedBatch, error) {
	if d == nil || d.pipe == nil {
		return DecodedBatch{}, errors.New("GoFlow2 decoder is not initialized")
	}
	var raw flowpb.RawFlow
	if err := proto.Unmarshal(value, &raw); err != nil {
		return DecodedBatch{}, fmt.Errorf("unmarshal raw flow: %w", err)
	}
	return d.Decode(&raw)
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
	message := &utils.Message{Src: source, Payload: raw.Payload, Received: receivedAt}

	d.sink.records = d.sink.records[:0]
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
		d.sink.records = d.sink.records[:0]
		d.metadata.records = d.metadata.records[:0]
		return DecodedBatch{}, err
	}
	records := append([]*goflowpb.FlowMessage(nil), d.sink.records...)
	metadata := append([]DecodedRecordMetadata(nil), d.metadata.records...)
	if flowType == goflowpb.FlowMessage_SFLOW_5 && len(records) != len(metadata) {
		return DecodedBatch{}, errors.New("GoFlow2 sFlow records and sample metadata are inconsistent")
	}
	return DecodedBatch{
		CollectorID:         raw.CollectorId,
		ListenerID:          raw.ListenerId,
		RegistryVersion:     raw.RegistryVersion,
		ReceivedAt:          receivedAt,
		Source:              source,
		FlowType:            flowType,
		ObservationDomainID: observationDomainID,
		SubAgentID:          d.metadata.subAgentID,
		DatagramSequence:    d.metadata.datagramSequence,
		AgentIP:             d.metadata.agentIP,
		Records:             records,
		RecordMetadata:      metadata,
	}, nil
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

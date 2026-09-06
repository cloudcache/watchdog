package flowstream

import (
	"net/netip"
	"time"

	"github.com/netsampler/goflow2/v3/decoders/sflow"
	"github.com/netsampler/goflow2/v3/pkg/flowstore"
	"github.com/netsampler/goflow2/v3/producer"
)

// DecodedRecordMetadata contains sFlow sample fields intentionally absent
// from GoFlow2's protocol-neutral FlowMessage.
type DecodedRecordMetadata struct {
	Present        bool
	SubAgentID     uint32
	SourceIDType   uint32
	SourceIDValue  uint32
	SampleSequence uint32
	SamplePool     uint64
	ExporterDrops  uint64
	SampleIndex    uint32
}

type metadataProducer struct {
	delegate         producer.ProducerInterface
	netflowSampling  *flowstore.Store[netflowSamplingKey, uint64]
	agentIP          netip.Addr
	subAgentID       uint32
	datagramSequence uint32
	records          []DecodedRecordMetadata
	// pending holds the pooled messages of the batch the decoder last returned.
	// They are recycled to GoFlow2's pool by recyclePending at the start of the
	// next Decode — deferred so the decoder can reference them zero-copy until
	// then (see the Decoder contract).
	pending []producer.ProducerMessage
}

func newMetadataProducer(delegate producer.ProducerInterface, stateTTL time.Duration) *metadataProducer {
	store := flowstore.NewStore[netflowSamplingKey, uint64](
		flowstore.WithDefaultTTL[netflowSamplingKey, uint64](stateTTL),
		flowstore.WithRefreshTTLOnWrite[netflowSamplingKey, uint64](),
		flowstore.WithRefreshTTLOnRead[netflowSamplingKey, uint64](),
		flowstore.WithMaxSize[netflowSamplingKey, uint64](maxNetFlowSamplingEntries),
	)
	store.Start(time.Minute)
	return &metadataProducer{delegate: delegate, netflowSampling: store}
}

func (p *metadataProducer) Produce(message any, args *producer.ProduceArgs) ([]producer.ProducerMessage, error) {
	p.agentIP = netip.Addr{}
	p.subAgentID = 0
	p.datagramSequence = 0
	p.records = p.records[:0]
	if packet, ok := message.(*sflow.Packet); ok {
		if address, valid := netip.AddrFromSlice(packet.AgentIP); valid {
			p.agentIP = address.Unmap()
		}
		p.subAgentID = packet.SubAgentId
		p.datagramSequence = packet.SequenceNumber
		for _, sample := range packet.Samples {
			var metadata DecodedRecordMetadata
			switch value := sample.(type) {
			case sflow.FlowSample:
				metadata = sampleMetadata(packet.SubAgentId, value.Header, value.SamplePool, value.Drops, uint32(len(p.records)))
			case sflow.ExpandedFlowSample:
				metadata = sampleMetadata(packet.SubAgentId, value.Header, value.SamplePool, value.Drops, uint32(len(p.records)))
			default:
				continue
			}
			p.records = append(p.records, metadata)
		}
	}
	messages, err := p.delegate.Produce(message, args)
	if err != nil {
		return messages, err
	}
	if err := p.applyNetFlowSampling(message, args, messages); err != nil {
		return messages, err
	}
	return messages, nil
}

func sampleMetadata(subAgentID uint32, header sflow.SampleHeader, samplePool, drops, sampleIndex uint32) DecodedRecordMetadata {
	return DecodedRecordMetadata{
		Present: true, SubAgentID: subAgentID,
		SourceIDType: header.SourceIdType, SourceIDValue: header.SourceIdValue,
		SampleSequence: header.SampleSequenceNumber, SamplePool: uint64(samplePool),
		ExporterDrops: uint64(drops), SampleIndex: sampleIndex,
	}
}

// Commit defers recycling instead of returning the pooled messages immediately.
// GoFlow2 calls this from DecodeFlow's defer, but the decoder still references
// these buffers zero-copy after DecodeFlow returns. recyclePending returns them
// to the pool at the start of the next Decode, once the caller has consumed the
// batch. The slice is copied because GoFlow2 may reuse its own backing array.
func (p *metadataProducer) Commit(messages []producer.ProducerMessage) {
	p.pending = append(p.pending[:0], messages...)
}

func (p *metadataProducer) recyclePending() {
	if len(p.pending) == 0 {
		return
	}
	p.delegate.Commit(p.pending)
	p.pending = p.pending[:0]
}

func (p *metadataProducer) Close() {
	p.recyclePending()
	p.delegate.Close()
	if p.netflowSampling != nil {
		p.netflowSampling.Stop()
	}
}

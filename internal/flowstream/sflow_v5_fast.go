// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"errors"
	"fmt"
	"net/netip"

	"github.com/netsampler/goflow2/v3/decoders/sflow"
	goflowpb "github.com/netsampler/goflow2/v3/pb"
	protoproducer "github.com/netsampler/goflow2/v3/producer/proto"
)

// sFlow is TLV-structured, not fixed-offset like NetFlow v5, and its common
// record (a sampled raw packet) needs a full Ethernet/IP/TCP parse. This fast
// path hand-writes only the framing — the reflection-heavy part (35 GoFlow2
// call sites) — and hands the untrusted packet-header parse to GoFlow2's
// hardened ParseSampledHeader. It produces FlowMessages and per-sample metadata
// identical to GoFlow2's ProcessMessageSFlowConfig for the sample and record
// types it handles, and returns errSFlowFallback (so the caller reruns GoFlow2)
// for ExtendedGateway BGP records or an unknown sample format rather than
// guessing. Every read is bounds-checked.
//
// These are package-level sentinels, not per-call errors.New, so the happy path
// allocates nothing.
var (
	errSFlowFallback        = errors.New("sflow: fall back to the reference decoder")
	errSFlowTruncatedSample = errors.New("sflow: truncated flow sample")
	errSFlowTruncatedRecord = errors.New("sflow: truncated flow record")
)

// sflowCursor is a bounds-checked big-endian reader over one XDR-encoded slice.
type sflowCursor struct {
	buf []byte
	off int
}

func (c *sflowCursor) remaining() int { return len(c.buf) - c.off }

func (c *sflowCursor) u32() (uint32, bool) {
	if c.off+4 > len(c.buf) {
		return 0, false
	}
	v := uint32(c.buf[c.off])<<24 | uint32(c.buf[c.off+1])<<16 | uint32(c.buf[c.off+2])<<8 | uint32(c.buf[c.off+3])
	c.off += 4
	return v, true
}

func (c *sflowCursor) take(n int) ([]byte, bool) {
	if n < 0 || c.off+n > len(c.buf) {
		return nil, false
	}
	s := c.buf[c.off : c.off+n]
	c.off += n
	return s, true
}

// decodeSFlowV5Fast decodes an sFlow v5 datagram into the decoder's reused sFlow
// backing and metadata. On success the batch is read from d.sflowBacking /
// d.sflowMetadata plus d.sflowAgentIP/SubAgent/Sequence. It returns
// errSFlowFallback when it meets a construct it declines to fast-path.
func (d *Decoder) decodeSFlowV5Fast(payload []byte, timeReceivedNs uint64) error {
	c := sflowCursor{buf: payload}
	version, ok := c.u32()
	if !ok {
		return errors.New("sflow: truncated version")
	}
	if version != 5 {
		return fmt.Errorf("sflow: unsupported version %d", version)
	}
	ipVersion, ok := c.u32()
	if !ok {
		return errors.New("sflow: truncated agent IP version")
	}
	var agentIP netip.Addr
	switch ipVersion {
	case 0:
		agentIP = netip.Addr{}
	case 1:
		b, ok := c.take(4)
		if !ok {
			return errors.New("sflow: truncated agent IPv4")
		}
		agentIP, _ = netip.AddrFromSlice(b)
	case 2:
		b, ok := c.take(16)
		if !ok {
			return errors.New("sflow: truncated agent IPv6")
		}
		agentIP, _ = netip.AddrFromSlice(b)
	default:
		return fmt.Errorf("sflow: unknown agent IP version %d", ipVersion)
	}
	subAgentID, ok1 := c.u32()
	sequence, ok2 := c.u32()
	_, ok3 := c.u32() // uptime: advance past, not mapped
	samplesCount, ok4 := c.u32()
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return errors.New("sflow: truncated datagram header")
	}
	if samplesCount > 1000 { // match GoFlow2's DDoS guard
		return fmt.Errorf("sflow: too many samples: %d", samplesCount)
	}

	d.recordBacking = growRecords(d.recordBacking, int(samplesCount))
	d.sflowMetadata = d.sflowMetadata[:0]
	d.sflowAgentIP = agentIP.Unmap()
	d.sflowSubAgent = subAgentID
	d.sflowSequence = sequence

	out := 0
	for i := 0; i < int(samplesCount) && c.remaining() >= 8; i++ {
		format, _ := c.u32()
		length, _ := c.u32()
		if int(length) > c.remaining() {
			break
		}
		body, _ := c.take(int(length))
		switch format {
		case sflow.SAMPLE_FORMAT_FLOW, sflow.SAMPLE_FORMAT_EXPANDED_FLOW:
			record := &d.recordBacking[out]
			*record = DecodedRecord{}
			metadata, err := d.decodeSFlowFlowSample(sflowCursor{buf: body}, record, format, subAgentID, timeReceivedNs, out)
			if err != nil {
				return err
			}
			d.sflowMetadata = append(d.sflowMetadata, metadata)
			out++
		case sflow.SAMPLE_FORMAT_COUNTER, sflow.SAMPLE_FORMAT_EXPANDED_COUNTER, sflow.SAMPLE_FORMAT_DROP:
			// Valid samples, but GoFlow2's producer maps neither counters nor drops.
		default:
			return errSFlowFallback // GoFlow2's DecodeSample errors on unknown formats.
		}
	}
	d.recordBacking = d.recordBacking[:out]
	return nil
}

func (d *Decoder) decodeSFlowFlowSample(c sflowCursor, record *DecodedRecord, format, subAgentID uint32, timeReceivedNs uint64, sampleIndex int) (DecodedRecordMetadata, error) {
	sequence, ok := c.u32()
	if !ok {
		return DecodedRecordMetadata{}, errSFlowTruncatedSample
	}
	var sourceIDType, sourceIDValue uint32
	if format == sflow.SAMPLE_FORMAT_FLOW {
		sid, ok := c.u32() // interlaced: type in the top byte, value in the low 24 bits
		if !ok {
			return DecodedRecordMetadata{}, errSFlowTruncatedSample
		}
		sourceIDType, sourceIDValue = sid>>24, sid&0x00ffffff
	} else {
		var okA, okB bool
		sourceIDType, okA = c.u32()
		sourceIDValue, okB = c.u32()
		if !okA || !okB {
			return DecodedRecordMetadata{}, errSFlowTruncatedSample
		}
	}

	var samplingRate, samplePool, drops, inIf, outIf, recordsCount uint32
	oks := make([]bool, 0, 8)
	if format == sflow.SAMPLE_FORMAT_FLOW {
		var a, b, cc, dd, e, f bool
		samplingRate, a = c.u32()
		samplePool, b = c.u32()
		drops, cc = c.u32()
		inIf, dd = c.u32()
		outIf, e = c.u32()
		recordsCount, f = c.u32()
		oks = append(oks, a, b, cc, dd, e, f)
	} else {
		// Expanded: SamplingRate, SamplePool, Drops, InputIfFormat, InputIfValue,
		// OutputIfFormat, OutputIfValue, FlowRecordsCount. Only the values map.
		var a, b, cc, dd, e, f, g, h bool
		samplingRate, a = c.u32()
		samplePool, b = c.u32()
		drops, cc = c.u32()
		_, dd = c.u32()
		inIf, e = c.u32()
		_, f = c.u32()
		outIf, g = c.u32()
		recordsCount, h = c.u32()
		oks = append(oks, a, b, cc, dd, e, f, g, h)
	}
	for _, o := range oks {
		if !o {
			return DecodedRecordMetadata{}, errSFlowTruncatedSample
		}
	}
	if recordsCount > 1000 {
		return DecodedRecordMetadata{}, fmt.Errorf("sflow: too many flow records: %d", recordsCount)
	}

	record.Type = goflowpb.FlowMessage_SFLOW_5
	record.Packets = 1
	record.SamplingRate = uint64(samplingRate)
	record.InIf = inIf
	record.OutIf = outIf
	record.TimeReceivedNs = timeReceivedNs
	record.TimeFlowStartNs = timeReceivedNs
	record.TimeFlowEndNs = timeReceivedNs

	for r := 0; r < int(recordsCount) && c.remaining() >= 8; r++ {
		dataFormat, _ := c.u32()
		recLen, _ := c.u32()
		if int(recLen) > c.remaining() {
			break
		}
		body, _ := c.take(int(recLen))
		if err := d.mapSFlowRecord(record, dataFormat, body); err != nil {
			return DecodedRecordMetadata{}, err
		}
	}

	return DecodedRecordMetadata{
		Present: true, SubAgentID: subAgentID,
		SourceIDType: sourceIDType, SourceIDValue: sourceIDValue,
		SampleSequence: sequence, SamplePool: uint64(samplePool),
		ExporterDrops: uint64(drops), SampleIndex: uint32(sampleIndex),
	}, nil
}

func (d *Decoder) mapSFlowRecord(record *DecodedRecord, dataFormat uint32, data []byte) error {
	c := sflowCursor{buf: data}
	switch dataFormat {
	case sflow.FLOW_TYPE_RAW:
		protocol, a := c.u32()
		frameLength, b := c.u32()
		_, cc := c.u32() // stripped: not mapped
		headerLength, dd := c.u32()
		if !a || !b || !cc || !dd {
			return errSFlowTruncatedRecord
		}
		headerData, ok := c.take(int(headerLength))
		if !ok {
			return errSFlowTruncatedRecord
		}
		record.Bytes = uint64(frameLength)
		// The raw sampled packet's Ethernet/IP/TCP headers are parsed by GoFlow2's
		// hardened ParseSampledHeader into a scratch message; copy the fields the
		// worker reads. This is the only record type that still touches a protobuf
		// message — and it already allocates inside ParseSampledHeader.
		scratch := &d.sflowHeaderScratch
		scratch.Reset()
		sampledHeader := sflow.SampledHeader{Protocol: protocol, FrameLength: frameLength, OriginalLength: headerLength, HeaderData: headerData}
		if err := protoproducer.ParseSampledHeader(scratch, &sampledHeader); err != nil {
			return fmt.Errorf("sflow sampled header: %w", err)
		}
		record.SrcAddr, record.DstAddr = scratch.SrcAddr, scratch.DstAddr
		record.Proto = scratch.Proto
		record.SrcPort, record.DstPort = scratch.SrcPort, scratch.DstPort
		record.TcpFlags = scratch.TcpFlags
	case sflow.FLOW_TYPE_IPV4:
		length, a := c.u32()
		protocol, b := c.u32()
		src, okS := c.take(4)
		dst, okD := c.take(4)
		srcPort, cc := c.u32()
		dstPort, dd := c.u32()
		_, e := c.u32() // TcpFlags: GoFlow2 decodes but does not map for SampledIPv4
		_, f := c.u32() // Tos: not read by the worker
		if !a || !b || !okS || !okD || !cc || !dd || !e || !f {
			return errSFlowTruncatedRecord
		}
		record.SrcAddr, record.DstAddr = src, dst
		record.Bytes = uint64(length)
		record.Proto = protocol
		record.SrcPort, record.DstPort = srcPort, dstPort
	case sflow.FLOW_TYPE_IPV6:
		length, a := c.u32()
		protocol, b := c.u32()
		src, okS := c.take(16)
		dst, okD := c.take(16)
		srcPort, cc := c.u32()
		dstPort, dd := c.u32()
		_, e := c.u32() // TcpFlags: not mapped
		_, f := c.u32() // Priority: not read by the worker
		if !a || !b || !okS || !okD || !cc || !dd || !e || !f {
			return errSFlowTruncatedRecord
		}
		record.SrcAddr, record.DstAddr = src, dst
		record.Bytes = uint64(length)
		record.Proto = protocol
		record.SrcPort, record.DstPort = srcPort, dstPort
	case sflow.FLOW_TYPE_EXT_GATEWAY:
		return errSFlowFallback // BGP AS-path/communities set SrcAs/DstAs — run GoFlow2.
	default:
		// ExtendedRouter/ExtendedSwitch and everything else (ETH, EgressQueue, ACL,
		// Function, MPLS, …) map only to fields the worker never reads (NextHop/
		// masks/VLANs/…), so the lean record ignores them. The record body was
		// already consumed by the caller.
	}
	return nil
}

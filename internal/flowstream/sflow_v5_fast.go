// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"encoding/binary"
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
// hardened ParseSampledHeader. It produces DecodedRecords and per-sample metadata
// identical to GoFlow2's ProcessMessageSFlowConfig for the sample and record
// types it handles, and returns errSFlowFallback (so the caller reruns GoFlow2)
// for ExtendedGateway BGP records or an unknown sample format rather than
// guessing. Every read is bounds-checked.
//
// These are package-level sentinels, not per-call errors.New, so the happy path
// allocates nothing.
var (
	errSFlowFallback         = errors.New("sflow: fall back to the reference decoder")
	errSFlowTruncatedSample  = errors.New("sflow: truncated flow sample")
	errSFlowTruncatedRecord  = errors.New("sflow: truncated flow record")
	errSFlowTruncatedCounter = errors.New("sflow: truncated counter record")
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

func (c *sflowCursor) u64() (uint64, bool) {
	if c.off+8 > len(c.buf) {
		return 0, false
	}
	v := binary.BigEndian.Uint64(c.buf[c.off : c.off+8])
	c.off += 8
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

// decodeSFlowV5Fast decodes an sFlow v5 datagram into the decoder's reused
// recordBacking and sflowMetadata. On success the batch is read from those plus
// d.sflowAgentIP/SubAgent/Sequence. It returns errSFlowFallback when it meets a
// construct it declines to fast-path.
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
	var agentBytes []byte // GoFlow2 sets SamplerAddress to these raw packet bytes.
	switch ipVersion {
	case 0:
		agentIP = netip.Addr{}
	case 1:
		b, ok := c.take(4)
		if !ok {
			return errors.New("sflow: truncated agent IPv4")
		}
		agentBytes = b
		agentIP, _ = netip.AddrFromSlice(b)
	case 2:
		b, ok := c.take(16)
		if !ok {
			return errors.New("sflow: truncated agent IPv6")
		}
		agentBytes = b
		agentIP, _ = netip.AddrFromSlice(b)
	default:
		return fmt.Errorf("sflow: unknown agent IP version %d", ipVersion)
	}
	// subAgentId, sequence, uptime, samplesCount — one bounds-checked window.
	if c.remaining() < 16 {
		return errors.New("sflow: truncated datagram header")
	}
	h := (*[16]byte)(c.buf[c.off:])
	c.off += 16
	subAgentID := binary.BigEndian.Uint32(h[0:4])
	sequence := binary.BigEndian.Uint32(h[4:8])
	// h[8:12] uptime: not mapped.
	samplesCount := binary.BigEndian.Uint32(h[12:16])
	if samplesCount > 1000 { // match GoFlow2's DDoS guard
		return fmt.Errorf("sflow: too many samples: %d", samplesCount)
	}

	d.recordBacking = growRecords(d.recordBacking, int(samplesCount))
	d.sflowMetadata = d.sflowMetadata[:0]
	d.sflowCounterBacking = d.sflowCounterBacking[:0]
	d.sflowAgentIP = agentIP.Unmap()
	d.sflowSubAgent = subAgentID
	d.sflowSequence = sequence

	out := 0
	for i := 0; i < int(samplesCount) && c.remaining() >= 8; i++ {
		h := (*[8]byte)(c.buf[c.off:]) // loop guard ensures >= 8 bytes
		c.off += 8
		format := binary.BigEndian.Uint32(h[0:4])
		length := binary.BigEndian.Uint32(h[4:8])
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
			// Datagram-level fields GoFlow2 stamps on every sFlow record.
			record.SequenceNum = sequence
			record.SamplerAddress = agentBytes
			d.sflowMetadata = append(d.sflowMetadata, metadata)
			out++
		case sflow.SAMPLE_FORMAT_COUNTER, sflow.SAMPLE_FORMAT_EXPANDED_COUNTER:
			if err := d.decodeSFlowCounterSample(sflowCursor{buf: body}, format, subAgentID, uint32(i)); err != nil {
				return err
			}
		case sflow.SAMPLE_FORMAT_DROP:
			// Drop samples are valid but are not interface counters.
		default:
			return errSFlowFallback // GoFlow2's DecodeSample errors on unknown formats.
		}
	}
	d.recordBacking = d.recordBacking[:out]
	return nil
}

func (d *Decoder) decodeSFlowCounterSample(c sflowCursor, format, subAgentID, sampleIndex uint32) error {
	var sequence, sourceIDType, sourceIDValue, recordsCount uint32
	if format == sflow.SAMPLE_FORMAT_COUNTER {
		if c.remaining() < 12 {
			return errSFlowTruncatedCounter
		}
		sequence = binary.BigEndian.Uint32(c.buf[c.off : c.off+4])
		sourceID := binary.BigEndian.Uint32(c.buf[c.off+4 : c.off+8])
		sourceIDType, sourceIDValue = sourceID>>24, sourceID&0x00ffffff
		recordsCount = binary.BigEndian.Uint32(c.buf[c.off+8 : c.off+12])
		c.off += 12
	} else {
		if c.remaining() < 16 {
			return errSFlowTruncatedCounter
		}
		sequence = binary.BigEndian.Uint32(c.buf[c.off : c.off+4])
		sourceIDType = binary.BigEndian.Uint32(c.buf[c.off+4 : c.off+8])
		sourceIDValue = binary.BigEndian.Uint32(c.buf[c.off+8 : c.off+12])
		recordsCount = binary.BigEndian.Uint32(c.buf[c.off+12 : c.off+16])
		c.off += 16
	}
	if recordsCount > 1000 {
		return fmt.Errorf("sflow: too many counter records: %d", recordsCount)
	}

	for recordIndex := uint32(0); recordIndex < recordsCount; recordIndex++ {
		if c.remaining() < 8 {
			return errSFlowTruncatedCounter
		}
		dataFormat := binary.BigEndian.Uint32(c.buf[c.off : c.off+4])
		recordLength := binary.BigEndian.Uint32(c.buf[c.off+4 : c.off+8])
		c.off += 8
		body, ok := c.take(int(recordLength))
		if !ok {
			return errSFlowTruncatedCounter
		}
		// dataFormat encodes enterprise in the high 20 bits and format in the
		// low 12. Only enterprise 0 / generic interface counters are needed for
		// portable interface truth; all other legal records remain skippable.
		if dataFormat>>12 != 0 || dataFormat&0x0fff != sflow.COUNTER_TYPE_IF {
			continue
		}
		counter, err := decodeSFlowGenericInterfaceCounter(body)
		if err != nil {
			return err
		}
		counter.SubAgentID = subAgentID
		counter.SourceIDType = sourceIDType
		counter.SourceIDValue = sourceIDValue
		counter.SampleSequence = sequence
		counter.SampleIndex = sampleIndex
		counter.RecordIndex = recordIndex
		d.sflowCounterBacking = append(d.sflowCounterBacking, counter)
	}
	return nil
}

func decodeSFlowGenericInterfaceCounter(data []byte) (DecodedCounterRecord, error) {
	// The generic interface counter record is fixed-width XDR (88 bytes).
	if len(data) < 88 {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	c := sflowCursor{buf: data}
	var result DecodedCounterRecord
	var ok bool
	if result.IfIndex, ok = c.u32(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	if result.IfType, ok = c.u32(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	if result.IfSpeed, ok = c.u64(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	if result.IfDirection, ok = c.u32(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	if result.IfStatus, ok = c.u32(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	if result.IfInOctets, ok = c.u64(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	if result.IfInUcastPkts, ok = c.u32(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	if result.IfInMulticastPkts, ok = c.u32(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	if result.IfInBroadcastPkts, ok = c.u32(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	if result.IfInDiscards, ok = c.u32(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	if result.IfInErrors, ok = c.u32(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	if result.IfInUnknownProtos, ok = c.u32(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	if result.IfOutOctets, ok = c.u64(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	if result.IfOutUcastPkts, ok = c.u32(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	if result.IfOutMulticastPkts, ok = c.u32(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	if result.IfOutBroadcastPkts, ok = c.u32(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	if result.IfOutDiscards, ok = c.u32(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	if result.IfOutErrors, ok = c.u32(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	if result.IfPromiscuousMode, ok = c.u32(); !ok {
		return DecodedCounterRecord{}, errSFlowTruncatedCounter
	}
	return result, nil
}

func (d *Decoder) decodeSFlowFlowSample(c sflowCursor, record *DecodedRecord, format, subAgentID uint32, timeReceivedNs uint64, sampleIndex int) (DecodedRecordMetadata, error) {
	// Batch-read the sample's fixed prefix in one bounds-checked window instead of
	// a per-field u32() each (~46% of this path's CPU). Slicing a fixed-size array
	// pointer lets the compiler drop the per-field bounds check.
	var sequence, sourceIDType, sourceIDValue, samplingRate, samplePool, drops, inIf, outIf, recordsCount uint32
	if format == sflow.SAMPLE_FORMAT_FLOW {
		// seq, interlaced source-id, rate, pool, drops, input, output, records.
		if c.remaining() < 32 {
			return DecodedRecordMetadata{}, errSFlowTruncatedSample
		}
		w := (*[32]byte)(c.buf[c.off:])
		c.off += 32
		sequence = binary.BigEndian.Uint32(w[0:4])
		sid := binary.BigEndian.Uint32(w[4:8]) // type in the top byte, value in the low 24 bits
		sourceIDType, sourceIDValue = sid>>24, sid&0x00ffffff
		samplingRate = binary.BigEndian.Uint32(w[8:12])
		samplePool = binary.BigEndian.Uint32(w[12:16])
		drops = binary.BigEndian.Uint32(w[16:20])
		inIf = binary.BigEndian.Uint32(w[20:24])
		outIf = binary.BigEndian.Uint32(w[24:28])
		recordsCount = binary.BigEndian.Uint32(w[28:32])
	} else {
		// Expanded: seq, sourceIdType, sourceIdValue, rate, pool, drops,
		// inputFmt, inputVal, outputFmt, outputVal, records. Only values map.
		if c.remaining() < 44 {
			return DecodedRecordMetadata{}, errSFlowTruncatedSample
		}
		w := (*[44]byte)(c.buf[c.off:])
		c.off += 44
		sequence = binary.BigEndian.Uint32(w[0:4])
		sourceIDType = binary.BigEndian.Uint32(w[4:8])
		sourceIDValue = binary.BigEndian.Uint32(w[8:12])
		samplingRate = binary.BigEndian.Uint32(w[12:16])
		samplePool = binary.BigEndian.Uint32(w[16:20])
		drops = binary.BigEndian.Uint32(w[20:24])
		inIf = binary.BigEndian.Uint32(w[28:32])  // skip inputFmt at [24:28]
		outIf = binary.BigEndian.Uint32(w[36:40]) // skip outputFmt at [32:36]
		recordsCount = binary.BigEndian.Uint32(w[40:44])
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
		h := (*[8]byte)(c.buf[c.off:]) // loop guard ensures >= 8 bytes
		c.off += 8
		dataFormat := binary.BigEndian.Uint32(h[0:4])
		recLen := binary.BigEndian.Uint32(h[4:8])
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
		if protocol != 1 {
			break // GoFlow2's ParseSampledHeader only parses Ethernet (protocol 1).
		}
		// Fast path: pull the 5-tuple straight out of a plain Ethernet/IP/L4 frame
		// at fixed offsets (zero-copy, zero-alloc). Falls back to GoFlow2's hardened
		// ParseSampledHeader for anything it does not fully understand (VLAN tags, IP
		// options, IPv6 extension headers, fragments, unusual ethertypes) — that
		// parser is 51% of this path's CPU and 100% of its allocs.
		if parseSampledPacket(headerData, record) {
			break
		}
		scratch := &d.sflowHeaderScratch
		*scratch = protoproducer.ProtoProducerMessage{} // plain-zero, not Reset: skips the protobuf state atomic.
		sampledHeader := sflow.SampledHeader{Protocol: protocol, FrameLength: frameLength, OriginalLength: headerLength, HeaderData: headerData}
		if err := protoproducer.ParseSampledHeader(scratch, &sampledHeader); err != nil {
			return fmt.Errorf("sflow sampled header: %w", err)
		}
		copyPacketFields(record, scratch)
	case sflow.FLOW_TYPE_IPV4:
		// length, protocol, srcIP(4), dstIP(4), srcPort, dstPort, tcpFlags, tos.
		if c.remaining() < 32 {
			return errSFlowTruncatedRecord
		}
		w := (*[32]byte)(c.buf[c.off:])
		c.off += 32
		record.Bytes = uint64(binary.BigEndian.Uint32(w[0:4]))
		record.Proto = binary.BigEndian.Uint32(w[4:8])
		record.SrcAddr = w[8:12]
		record.DstAddr = w[12:16]
		record.SrcPort = binary.BigEndian.Uint32(w[16:20])
		record.DstPort = binary.BigEndian.Uint32(w[20:24])
		// w[24:28] TcpFlags: GoFlow2 decodes but does not map for SampledIPv4.
		record.IpTos = binary.BigEndian.Uint32(w[28:32])
		record.Etype = 0x800
	case sflow.FLOW_TYPE_IPV6:
		// length, protocol, srcIP(16), dstIP(16), srcPort, dstPort, tcpFlags, priority.
		if c.remaining() < 56 {
			return errSFlowTruncatedRecord
		}
		w := (*[56]byte)(c.buf[c.off:])
		c.off += 56
		record.Bytes = uint64(binary.BigEndian.Uint32(w[0:4]))
		record.Proto = binary.BigEndian.Uint32(w[4:8])
		record.SrcAddr = w[8:24]
		record.DstAddr = w[24:40]
		record.SrcPort = binary.BigEndian.Uint32(w[40:44])
		record.DstPort = binary.BigEndian.Uint32(w[44:48])
		// w[48:52] TcpFlags: not mapped.
		record.IpTos = binary.BigEndian.Uint32(w[52:56]) // priority
		record.Etype = 0x86dd
	case sflow.FLOW_TYPE_EXT_ROUTER:
		nextHop, err := decodeSFlowIP(&c)
		if err != nil {
			return err
		}
		srcMask, a := c.u32()
		dstMask, b := c.u32()
		if !a || !b {
			return errSFlowTruncatedRecord
		}
		record.NextHop = nextHop
		record.SrcNet, record.DstNet = srcMask, dstMask
	case sflow.FLOW_TYPE_EXT_SWITCH:
		srcVlan, a := c.u32()
		_, b := c.u32() // SrcPriority: not mapped
		dstVlan, cc := c.u32()
		_, dd := c.u32() // DstPriority: not mapped
		if !a || !b || !cc || !dd {
			return errSFlowTruncatedRecord
		}
		record.SrcVlan, record.DstVlan = srcVlan, dstVlan
	case sflow.FLOW_TYPE_EXT_GATEWAY:
		return errSFlowFallback // BGP AS-path/communities set SrcAs/DstAs — run GoFlow2.
	default:
		// ETH, EgressQueue, ACL, Function, MPLS, etc.: GoFlow2 decodes them but its
		// producer maps no DecodedRecord field, so skipping (record already consumed)
		// matches its output for well-formed datagrams.
	}
	return nil
}

// copyPacketFields copies the fields ParseSampledHeader derives from the sampled
// packet into the record; sample/datagram-level fields are already set.
func copyPacketFields(dst *DecodedRecord, m *protoproducer.ProtoProducerMessage) {
	dst.SrcAddr, dst.DstAddr = m.SrcAddr, m.DstAddr
	dst.Proto, dst.TcpFlags, dst.IpTos, dst.Etype = m.Proto, m.TcpFlags, m.IpTos, m.Etype
	dst.SrcPort, dst.DstPort = m.SrcPort, m.DstPort
	dst.SrcVlan, dst.DstVlan = m.SrcVlan, m.DstVlan
}

// parseSampledPacket extracts the 5-tuple (+ Etype/IpTos/TcpFlags) from a plain
// sampled Ethernet frame by slicing at fixed offsets — no allocation, addresses
// slice the frame. It reproduces GoFlow2 ParsePacket's mapping for the common
// case and returns false (fall back to that hardened parser) for anything it does
// not fully understand. Matching GoFlow2's quirks: Etype is the outer ethertype,
// and IPv4 uses a fixed 20-byte header (IHL/options ignored — so options fall
// back). Every read is bounds-checked.
func parseSampledPacket(frame []byte, r *DecodedRecord) bool {
	if len(frame) < 14 {
		return false
	}
	etype := binary.BigEndian.Uint16(frame[12:14])
	r.Etype = uint32(etype)
	l3 := frame[14:]
	switch etype {
	case 0x0800: // IPv4
		if len(l3) < 20 {
			return false
		}
		if l3[0]&0x0F != 5 { // IHL != 5: IP options — GoFlow2 assumes 20B, fall back
			return false
		}
		if l3[6]&0x3F != 0 || l3[7] != 0 { // more-fragments or fragment offset set
			return false
		}
		r.IpTos = uint32(l3[1])
		r.Proto = uint32(l3[9])
		r.SrcAddr = l3[12:16]
		r.DstAddr = l3[16:20]
		return parseSampledL4(l3[20:], r)
	case 0x86dd: // IPv6
		if len(l3) < 40 {
			return false
		}
		r.IpTos = uint32(l3[0]&0x0F)<<4 | uint32(l3[1]>>4) // traffic class
		r.Proto = uint32(l3[6])                            // next header (extension headers fall back in L4)
		r.SrcAddr = l3[8:24]
		r.DstAddr = l3[24:40]
		return parseSampledL4(l3[40:], r)
	default:
		return false // VLAN (0x8100), ARP, MPLS, … → GoFlow2
	}
}

func parseSampledL4(l4 []byte, r *DecodedRecord) bool {
	switch r.Proto {
	case 6: // TCP
		if len(l4) < 20 {
			return false
		}
		r.SrcPort = uint32(binary.BigEndian.Uint16(l4[0:2]))
		r.DstPort = uint32(binary.BigEndian.Uint16(l4[2:4]))
		r.TcpFlags = uint32(l4[13])
		return true
	case 17: // UDP
		if len(l4) < 8 {
			return false
		}
		r.SrcPort = uint32(binary.BigEndian.Uint16(l4[0:2]))
		r.DstPort = uint32(binary.BigEndian.Uint16(l4[2:4]))
		return true
	default:
		// ICMP and others carry no ports; GoFlow2 stops after the IP layer too, so
		// the IP fields already set are the whole mapping.
		return true
	}
}

// decodeSFlowIP mirrors GoFlow2's DecodeIP: an IP-version word then 0/4/16 bytes.
func decodeSFlowIP(c *sflowCursor) ([]byte, error) {
	ipVersion, ok := c.u32()
	if !ok {
		return nil, errSFlowTruncatedRecord
	}
	switch ipVersion {
	case 0:
		return nil, nil
	case 1:
		b, ok := c.take(4)
		if !ok {
			return nil, errSFlowTruncatedRecord
		}
		return b, nil
	case 2:
		b, ok := c.take(16)
		if !ok {
			return nil, errSFlowTruncatedRecord
		}
		return b, nil
	default:
		return nil, fmt.Errorf("sflow: unknown router IP version %d", ipVersion)
	}
}

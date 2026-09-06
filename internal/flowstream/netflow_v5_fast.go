// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"encoding/binary"
	"errors"

	goflowpb "github.com/netsampler/goflow2/v3/pb"
)

// NetFlow v5 is a fixed wire layout: a 24-byte header followed by count records
// of 48 bytes each. That makes it decodable at fixed offsets, avoiding GoFlow2's
// reflection-based binary reader, its intermediate record struct, its per-field
// bytes.Buffer reads, and its per-record protobuf allocation. This fast path
// produces FlowMessages field-for-field identical to GoFlow2's
// ProcessMessageNetFlowLegacy — TestNetFlowV5FastMatchesGoFlow2 asserts that
// against a differential corpus — so nothing downstream changes.
const (
	netflowV5HeaderSize = 24
	netflowV5RecordSize = 48
)

// decodeNetFlowV5Fast decodes a NetFlow v5 datagram into dst, reusing dst's
// backing array across calls (grown only when a datagram carries more records
// than before). Addresses slice the payload directly rather than allocating —
// valid for the batch's lifetime under the Decoder zero-copy contract, since the
// worker copies them out before the next Decode. Returns the populated prefix.
//
// timeReceivedNs and samplerAddress are datagram-level and identical for every
// record; GoFlow2 stamps them from the pipe's ProduceArgs (packet receive time
// and exporter source address), so the fast path must too. All records share the
// one samplerAddress slice, exactly as GoFlow2 does.
//
// It never reads past the payload: the header's record count is clamped to what
// the buffer actually holds, so a malformed or hostile count cannot panic or
// over-read.
func decodeNetFlowV5Fast(payload []byte, timeReceivedNs uint64, samplerAddress []byte, dst []goflowpb.FlowMessage) ([]goflowpb.FlowMessage, error) {
	if len(payload) < netflowV5HeaderSize {
		return nil, errors.New("NetFlow v5 header is truncated")
	}
	count := int(binary.BigEndian.Uint16(payload[2:4]))
	uptime := binary.BigEndian.Uint32(payload[4:8])
	baseTime := uint64(binary.BigEndian.Uint32(payload[8:12]))*1_000_000_000 + uint64(binary.BigEndian.Uint32(payload[12:16]))
	sequence := binary.BigEndian.Uint32(payload[16:20])
	// The low 14 bits are the rate; the high 2 bits are the sampling mode.
	samplingRate := uint64(binary.BigEndian.Uint16(payload[22:24]) & 0x3FFF)

	if available := (len(payload) - netflowV5HeaderSize) / netflowV5RecordSize; count > available {
		count = available
	}
	dst = growFlowMessages(dst, count)
	for index := 0; index < count; index++ {
		offset := netflowV5HeaderSize + index*netflowV5RecordSize
		record := payload[offset : offset+netflowV5RecordSize]
		message := &dst[index]
		message.Reset()
		message.Type = goflowpb.FlowMessage_NETFLOW_V5
		message.SequenceNum = sequence
		message.SamplingRate = samplingRate
		message.TimeReceivedNs = timeReceivedNs
		message.SamplerAddress = samplerAddress
		// uint32 subtraction then uint64 nanosecond math, matching GoFlow2's
		// wrapping behaviour exactly (First/Last are uptime-relative in ms).
		message.TimeFlowStartNs = baseTime - uint64(uptime-binary.BigEndian.Uint32(record[24:28]))*1_000_000
		message.TimeFlowEndNs = baseTime - uint64(uptime-binary.BigEndian.Uint32(record[28:32]))*1_000_000
		message.SrcAddr = record[0:4]
		message.DstAddr = record[4:8]
		message.NextHop = record[8:12]
		message.Etype = 0x800
		message.InIf = uint32(binary.BigEndian.Uint16(record[12:14]))
		message.OutIf = uint32(binary.BigEndian.Uint16(record[14:16]))
		message.Packets = uint64(binary.BigEndian.Uint32(record[16:20]))
		message.Bytes = uint64(binary.BigEndian.Uint32(record[20:24]))
		message.SrcPort = uint32(binary.BigEndian.Uint16(record[32:34]))
		message.DstPort = uint32(binary.BigEndian.Uint16(record[34:36]))
		message.TcpFlags = uint32(record[37])
		message.Proto = uint32(record[38])
		message.IpTos = uint32(record[39])
		message.SrcAs = uint32(binary.BigEndian.Uint16(record[40:42]))
		message.DstAs = uint32(binary.BigEndian.Uint16(record[42:44]))
		message.SrcNet = uint32(record[44])
		message.DstNet = uint32(record[45])
	}
	return dst[:count], nil
}

func growFlowMessages(dst []goflowpb.FlowMessage, n int) []goflowpb.FlowMessage {
	if cap(dst) >= n {
		return dst[:n]
	}
	return make([]goflowpb.FlowMessage, n)
}

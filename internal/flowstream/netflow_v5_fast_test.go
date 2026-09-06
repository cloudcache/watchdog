// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
	"github.com/netsampler/goflow2/v3/decoders/netflowlegacy"
	"google.golang.org/protobuf/proto"
)

// TestNetFlowV5FastMatchesGoFlow2 is the correctness gate for the fast path. It
// decodes a corpus of well-formed NetFlow v5 datagrams with BOTH the fast path
// and GoFlow2's reflection pipe and asserts the resulting FlowMessages are
// proto-equal field for field, plus the batch-level identity fields. If the
// fast path ever diverges from GoFlow2's output, this fails.
func TestNetFlowV5FastMatchesGoFlow2(t *testing.T) {
	fast, err := NewDecoder(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Close()
	slow, err := NewDecoder(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	slow.fastNetFlowV5 = false // force GoFlow2 as the reference
	defer slow.Close()

	for name, payload := range netflowV5Corpus(t) {
		t.Run(name, func(t *testing.T) {
			raw := rawFlowValue(t, flowpb.RawFlow_DECODER_NETFLOW, payload)
			fastBatch, err := fast.DecodeValue(raw)
			if err != nil {
				t.Fatalf("fast decode: %v", err)
			}
			slowBatch, err := slow.DecodeValue(raw)
			if err != nil {
				t.Fatalf("slow decode: %v", err)
			}
			if fastBatch.FlowType != slowBatch.FlowType || fastBatch.SubAgentID != slowBatch.SubAgentID ||
				fastBatch.DatagramSequence != slowBatch.DatagramSequence || fastBatch.AgentIP != slowBatch.AgentIP {
				t.Fatalf("batch identity differs:\nfast=%+v\nslow=%+v", fastBatch, slowBatch)
			}
			if len(fastBatch.Records) != len(slowBatch.Records) {
				t.Fatalf("record count: fast=%d slow=%d", len(fastBatch.Records), len(slowBatch.Records))
			}
			for i := range fastBatch.Records {
				if !proto.Equal(fastBatch.Records[i], slowBatch.Records[i]) {
					t.Fatalf("record %d differs:\nfast=%v\nslow=%v", i, fastBatch.Records[i], slowBatch.Records[i])
				}
			}
		})
	}
}

// TestNetFlowV5FastClampsTruncatedCount documents a deliberate, safer divergence
// from GoFlow2: when a datagram's header count exceeds the records actually
// present (truncated or hostile), the fast path decodes only the records the
// buffer holds. GoFlow2 instead emits phantom zero records for the missing tail.
func TestNetFlowV5FastClampsTruncatedCount(t *testing.T) {
	payload := buildNetFlowV5(t, netflowlegacy.PacketNetFlowV5{
		SysUptime: 10000, UnixSecs: 1_800_000_000, FlowSequence: 1, SamplingInterval: 1,
		Records: []netflowlegacy.RecordsNetFlowV5{sampleV5Record(nil), sampleV5Record(nil), sampleV5Record(nil)},
	})
	// Lie: claim 200 records in the header while only 3 are present.
	binary.BigEndian.PutUint16(payload[2:4], 200)

	decoder, err := NewDecoder(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	batch, err := decoder.DecodeValue(rawFlowValue(t, flowpb.RawFlow_DECODER_NETFLOW, payload))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(batch.Records) != 3 {
		t.Fatalf("truncated count not clamped: got %d records, want 3", len(batch.Records))
	}
}

// BenchmarkDecodeNetFlowV5SlowPath measures GoFlow2's reflection pipe on the same
// datagram BenchmarkDecodeNetFlowV5 (fast path) uses, for an honest A/B.
func BenchmarkDecodeNetFlowV5SlowPath(b *testing.B) {
	many := make([]netflowlegacy.RecordsNetFlowV5, 30)
	for i := range many {
		many[i] = netflowlegacy.RecordsNetFlowV5{SrcAddr: netflowlegacy.IPAddress(0x0a000001 + uint32(i)), DstAddr: 0xcb007102, Input: 3, Output: 4, DPkts: 2, DOctets: 1500, First: 9000, Last: 9500, SrcPort: 12345, DstPort: 443, TCPFlags: 0x12, Proto: 6, SrcAS: 64512, DstAS: 64513}
	}
	payload := buildNetFlowV5(b, netflowlegacy.PacketNetFlowV5{SysUptime: 10000, UnixSecs: 1_800_000_000, FlowSequence: 88, SamplingInterval: 500, Records: many})
	value := rawFlowValue(b, flowpb.RawFlow_DECODER_NETFLOW, payload)
	decoder, err := NewDecoder(time.Minute)
	if err != nil {
		b.Fatal(err)
	}
	decoder.fastNetFlowV5 = false
	defer decoder.Close()
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		batch, err := decoder.DecodeValue(value)
		if err != nil {
			b.Fatal(err)
		}
		if len(batch.Records) != 30 {
			b.Fatalf("records=%d", len(batch.Records))
		}
	}
	b.ReportMetric(float64(b.N*30)/b.Elapsed().Seconds(), "records/s")
}

func sampleV5Record(mut func(*netflowlegacy.RecordsNetFlowV5)) netflowlegacy.RecordsNetFlowV5 {
	r := netflowlegacy.RecordsNetFlowV5{
		SrcAddr: 0x0a000001, DstAddr: 0xcb007102, NextHop: 0x08080808,
		Input: 3, Output: 4, DPkts: 2, DOctets: 1500, First: 9000, Last: 9500,
		SrcPort: 12345, DstPort: 443, TCPFlags: 0x12, Proto: 6, Tos: 0x10,
		SrcAS: 64512, DstAS: 64513, SrcMask: 24, DstMask: 16,
	}
	if mut != nil {
		mut(&r)
	}
	return r
}

func buildNetFlowV5(tb testing.TB, pkt netflowlegacy.PacketNetFlowV5) []byte {
	tb.Helper()
	pkt.Version = 5
	pkt.Count = uint16(len(pkt.Records))
	payload, err := pkt.MarshalBinary()
	if err != nil {
		tb.Fatal(err)
	}
	return payload
}

func netflowV5Corpus(t *testing.T) map[string][]byte {
	t.Helper()
	corpus := map[string][]byte{}

	corpus["single"] = buildNetFlowV5(t, netflowlegacy.PacketNetFlowV5{
		SysUptime: 10000, UnixSecs: 1_800_000_000, UnixNSecs: 123, FlowSequence: 88, SamplingInterval: 500,
		Records: []netflowlegacy.RecordsNetFlowV5{sampleV5Record(nil)},
	})

	many := make([]netflowlegacy.RecordsNetFlowV5, 30)
	for i := range many {
		many[i] = sampleV5Record(func(r *netflowlegacy.RecordsNetFlowV5) {
			r.SrcAddr = netflowlegacy.IPAddress(0x0a000000 + uint32(i))
			r.DstAddr = netflowlegacy.IPAddress(0xcb000000 + uint32(i))
			r.DPkts = uint32(i + 1)
			r.DOctets = uint32(100*i + 64)
			r.SrcPort = uint16(1000 + i)
			r.DstPort = uint16(2000 + i)
			r.First = uint32(8000 + i*10)
			r.Last = uint32(9000 + i*10)
		})
	}
	// SamplingInterval with the high mode bits set: rate must mask to 0x3FFF.
	corpus["many"] = buildNetFlowV5(t, netflowlegacy.PacketNetFlowV5{
		SysUptime: 50000, UnixSecs: 1_800_000_500, UnixNSecs: 999, FlowSequence: 7, SamplingInterval: 0x4000 | 1000,
		Records: many,
	})

	// First/Last greater than uptime: exercise the uint32 wrap in the timestamps.
	corpus["timeWrap"] = buildNetFlowV5(t, netflowlegacy.PacketNetFlowV5{
		SysUptime: 100, UnixSecs: 1_800_000_000, FlowSequence: 1, SamplingInterval: 1,
		Records: []netflowlegacy.RecordsNetFlowV5{sampleV5Record(func(r *netflowlegacy.RecordsNetFlowV5) { r.First = 5000; r.Last = 6000 })},
	})

	// Saturated header and record fields.
	corpus["maxValues"] = buildNetFlowV5(t, netflowlegacy.PacketNetFlowV5{
		SysUptime: 0xFFFFFFFF, UnixSecs: 0xFFFFFFFF, UnixNSecs: 0xFFFFFFFF, FlowSequence: 0xFFFFFFFF, SamplingInterval: 0xFFFF,
		Records: []netflowlegacy.RecordsNetFlowV5{sampleV5Record(func(r *netflowlegacy.RecordsNetFlowV5) {
			r.DPkts = 0xFFFFFFFF
			r.DOctets = 0xFFFFFFFF
			r.SrcPort = 0xFFFF
			r.DstPort = 0xFFFF
			r.SrcAS = 0xFFFF
			r.DstAS = 0xFFFF
			r.TCPFlags = 0xFF
			r.Proto = 0xFF
			r.Tos = 0xFF
			r.SrcMask = 0xFF
			r.DstMask = 0xFF
		})},
	})

	corpus["empty"] = buildNetFlowV5(t, netflowlegacy.PacketNetFlowV5{SysUptime: 1000, UnixSecs: 1_800_000_000})

	return corpus
}

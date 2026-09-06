// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"reflect"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
	"github.com/netsampler/goflow2/v3/decoders/sflow"
	decoderutils "github.com/netsampler/goflow2/v3/decoders/utils"
	"google.golang.org/protobuf/proto"
)

// TestSFlowFastMatchesGoFlow2 is the correctness gate for the sFlow fast path. It
// decodes a corpus covering every handled record type — plus counter-sample
// skipping, expanded samples, an IPv6 agent, and an ExtendedGateway that must
// fall back to GoFlow2 — with BOTH the fast path and GoFlow2, and asserts the
// resulting FlowMessages are proto-equal and the per-sample metadata matches.
func TestSFlowFastMatchesGoFlow2(t *testing.T) {
	fast, err := NewDecoder(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Close()
	slow, err := NewDecoder(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	slow.fastSFlow = false // force GoFlow2 as the reference
	defer slow.Close()

	for name, payload := range sflowCorpus(t) {
		t.Run(name, func(t *testing.T) {
			raw := rawFlowValue(t, flowpb.RawFlow_DECODER_SFLOW, payload)
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
			if !reflect.DeepEqual(fastBatch.RecordMetadata, slowBatch.RecordMetadata) {
				t.Fatalf("metadata differs:\nfast=%+v\nslow=%+v", fastBatch.RecordMetadata, slowBatch.RecordMetadata)
			}
		})
	}
}

func sflowCorpus(t *testing.T) map[string][]byte {
	t.Helper()
	build := func(pkt sflow.Packet) []byte {
		pkt.Version = 5
		payload, err := pkt.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	agent := decoderutils.IPAddress{192, 0, 2, 9}
	ipv4Rec := sflow.FlowRecord{Data: sflow.SampledIPv4{SampledIPBase: sflow.SampledIPBase{
		Length: 128, Protocol: 6, SrcIP: decoderutils.IPAddress{10, 0, 0, 1}, DstIP: decoderutils.IPAddress{203, 0, 113, 2}, SrcPort: 12345, DstPort: 443, TcpFlags: 0x12,
	}, Tos: 0x10}}
	ipv6Rec := sflow.FlowRecord{Data: sflow.SampledIPv6{SampledIPBase: sflow.SampledIPBase{
		Length: 256, Protocol: 17, SrcIP: decoderutils.IPAddress{0x20, 1, 0xd, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, DstIP: decoderutils.IPAddress{0x20, 1, 0xd, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2}, SrcPort: 53, DstPort: 33000, TcpFlags: 0,
	}, Priority: 5}}
	routerRec := sflow.FlowRecord{Data: sflow.ExtendedRouter{NextHopIPVersion: 1, NextHop: decoderutils.IPAddress{192, 0, 2, 254}, SrcMaskLen: 24, DstMaskLen: 16}}
	switchRec := sflow.FlowRecord{Data: sflow.ExtendedSwitch{SrcVlan: 100, SrcPriority: 1, DstVlan: 200, DstPriority: 2}}
	headerRec := sflow.FlowRecord{Data: sflow.SampledHeader{Protocol: 1, FrameLength: 1514, Stripped: 4, HeaderData: ethernetIPv4TCPFrame()}}

	flowSample := func(seq, srcVal uint32, records ...sflow.FlowRecord) sflow.FlowSample {
		return sflow.FlowSample{
			Header:       sflow.SampleHeader{SampleSequenceNumber: seq, SourceIdValue: srcVal},
			SamplingRate: 1000, SamplePool: 9000, Drops: 3, Input: 10, Output: 20, Records: records,
		}
	}
	expandedSample := sflow.ExpandedFlowSample{
		Header:       sflow.SampleHeader{SampleSequenceNumber: 77, SourceIdType: 0, SourceIdValue: 55},
		SamplingRate: 2048, SamplePool: 4096, Drops: 1, InputIfValue: 11, OutputIfValue: 22, Records: []sflow.FlowRecord{ipv4Rec},
	}
	counter := sflow.CounterSample{Header: sflow.SampleHeader{SampleSequenceNumber: 5}, Records: []sflow.CounterRecord{}}
	gateway := sflow.FlowRecord{Data: sflow.ExtendedGateway{NextHopIPVersion: 1, NextHop: decoderutils.IPAddress{192, 0, 2, 1}, AS: 64512, SrcAS: 64513}}

	return map[string][]byte{
		"ipv4":          build(sflow.Packet{IPVersion: 1, AgentIP: agent, SubAgentId: 7, SequenceNumber: 99, Uptime: 1000, Samples: []interface{}{flowSample(12, 44, ipv4Rec)}}),
		"ipv6":          build(sflow.Packet{IPVersion: 1, AgentIP: agent, SubAgentId: 7, SequenceNumber: 100, Uptime: 1000, Samples: []interface{}{flowSample(13, 44, ipv6Rec)}}),
		"sampledHeader": build(sflow.Packet{IPVersion: 1, AgentIP: agent, SubAgentId: 7, SequenceNumber: 101, Uptime: 1000, Samples: []interface{}{flowSample(14, 44, headerRec)}}),
		"expanded":      build(sflow.Packet{IPVersion: 1, AgentIP: agent, SubAgentId: 7, SequenceNumber: 102, Uptime: 1000, Samples: []interface{}{expandedSample}}),
		"router":        build(sflow.Packet{IPVersion: 1, AgentIP: agent, SubAgentId: 7, SequenceNumber: 103, Uptime: 1000, Samples: []interface{}{flowSample(15, 44, ipv4Rec, routerRec)}}),
		"switch":        build(sflow.Packet{IPVersion: 1, AgentIP: agent, SubAgentId: 7, SequenceNumber: 104, Uptime: 1000, Samples: []interface{}{flowSample(16, 44, ipv4Rec, switchRec)}}),
		"counterSkip":   build(sflow.Packet{IPVersion: 1, AgentIP: agent, SubAgentId: 7, SequenceNumber: 105, Uptime: 1000, Samples: []interface{}{flowSample(17, 44, ipv4Rec), counter, flowSample(18, 44, ipv6Rec)}}),
		"multiSample":   build(sflow.Packet{IPVersion: 1, AgentIP: agent, SubAgentId: 7, SequenceNumber: 106, Uptime: 1000, Samples: []interface{}{flowSample(19, 1, ipv4Rec), flowSample(20, 2, ipv6Rec), flowSample(21, 3, ipv4Rec, routerRec)}}),
		"agentIPv6":     build(sflow.Packet{IPVersion: 2, AgentIP: decoderutils.IPAddress{0x20, 1, 0xd, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9}, SubAgentId: 3, SequenceNumber: 107, Uptime: 1000, Samples: []interface{}{flowSample(22, 44, ipv4Rec)}}),
		"gatewayFallback": build(sflow.Packet{IPVersion: 1, AgentIP: agent, SubAgentId: 7, SequenceNumber: 108, Uptime: 1000, Samples: []interface{}{flowSample(23, 44, ipv4Rec, gateway)}}),
	}
}

// TestSFlowFastHandlesTruncatedSafely feeds every truncation of a valid sFlow
// datagram through the fast path and asserts it never panics and never reports
// more records than the whole datagram carries — the untrusted-input guarantee.
func TestSFlowFastHandlesTruncatedSafely(t *testing.T) {
	full := sflowCorpus(t)["multiSample"]
	decoder, err := NewDecoder(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	for cut := 0; cut < len(full); cut++ {
		raw := rawFlowValue(t, flowpb.RawFlow_DECODER_SFLOW, full[:cut+1])
		batch, err := decoder.DecodeValue(raw) // must not panic
		if err == nil && len(batch.Records) > 3 {
			t.Fatalf("cut %d produced %d records from a 3-sample datagram", cut, len(batch.Records))
		}
	}
}

func sflowBenchPayload(tb testing.TB, samples int, record sflow.FlowRecord) []byte {
	tb.Helper()
	list := make([]interface{}, samples)
	for i := range list {
		list[i] = sflow.FlowSample{
			Header:       sflow.SampleHeader{SampleSequenceNumber: uint32(i), SourceIdValue: 44},
			SamplingRate: 1000, SamplePool: 9000, Drops: 3, Input: 10, Output: 20, Records: []sflow.FlowRecord{record},
		}
	}
	pkt := sflow.Packet{Version: 5, IPVersion: 1, AgentIP: decoderutils.IPAddress{192, 0, 2, 9}, SubAgentId: 7, SequenceNumber: 99, Uptime: 1000, Samples: list}
	payload, err := pkt.MarshalBinary()
	if err != nil {
		tb.Fatal(err)
	}
	return payload
}

func benchmarkSFlow(b *testing.B, record sflow.FlowRecord, fast bool) {
	const samples = 30
	value := rawFlowValue(b, flowpb.RawFlow_DECODER_SFLOW, sflowBenchPayload(b, samples, record))
	decoder, err := NewDecoder(time.Minute)
	if err != nil {
		b.Fatal(err)
	}
	decoder.fastSFlow = fast
	defer decoder.Close()
	b.SetBytes(int64(len(value)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		batch, err := decoder.DecodeValue(value)
		if err != nil {
			b.Fatal(err)
		}
		if len(batch.Records) != samples {
			b.Fatalf("records=%d", len(batch.Records))
		}
	}
	b.ReportMetric(float64(b.N*samples)/b.Elapsed().Seconds(), "records/s")
}

func benchIPv4Record() sflow.FlowRecord {
	return sflow.FlowRecord{Data: sflow.SampledIPv4{SampledIPBase: sflow.SampledIPBase{Length: 128, Protocol: 6, SrcIP: decoderutils.IPAddress{10, 0, 0, 1}, DstIP: decoderutils.IPAddress{203, 0, 113, 2}, SrcPort: 12345, DstPort: 443, TcpFlags: 0x12}, Tos: 0x10}}
}

func benchHeaderRecord() sflow.FlowRecord {
	return sflow.FlowRecord{Data: sflow.SampledHeader{Protocol: 1, FrameLength: 1514, Stripped: 4, HeaderData: ethernetIPv4TCPFrame()}}
}

// SampledIPv4: 5-tuple already in the record (no packet parse) — the light case.
func BenchmarkDecodeSFlowIPv4Fast(b *testing.B) { benchmarkSFlow(b, benchIPv4Record(), true) }
func BenchmarkDecodeSFlowIPv4Slow(b *testing.B) { benchmarkSFlow(b, benchIPv4Record(), false) }

// SampledHeader: a raw sampled packet the collector must parse — the heavy case.
func BenchmarkDecodeSFlowHeaderFast(b *testing.B) { benchmarkSFlow(b, benchHeaderRecord(), true) }
func BenchmarkDecodeSFlowHeaderSlow(b *testing.B) { benchmarkSFlow(b, benchHeaderRecord(), false) }

// ethernetIPv4TCPFrame is a minimal valid Ethernet/IPv4/TCP frame for a sampled
// header record. Both decode paths run GoFlow2's ParseSampledHeader over it, so
// the test verifies the fast path's framing feeds it identically.
func ethernetIPv4TCPFrame() []byte {
	frame := make([]byte, 0, 54)
	// Ethernet: dst MAC, src MAC, ethertype 0x0800.
	frame = append(frame, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0x08, 0x00)
	// IPv4 header (20 bytes): v4/IHL5, TOS, total len 40, id, flags, TTL, proto TCP(6), checksum, src, dst.
	frame = append(frame, 0x45, 0x00, 0x00, 0x28, 0x00, 0x01, 0x00, 0x00, 0x40, 0x06, 0x00, 0x00, 10, 0, 0, 1, 203, 0, 113, 2)
	// TCP header (20 bytes): src port 12345, dst port 443, seq, ack, offset/flags, window, checksum, urgent.
	frame = append(frame, 0x30, 0x39, 0x01, 0xBB, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x50, 0x18, 0x72, 0x10, 0x00, 0x00, 0x00, 0x00)
	return frame
}

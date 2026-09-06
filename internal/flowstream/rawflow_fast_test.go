// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"bytes"
	"net/netip"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
	"google.golang.org/protobuf/proto"
)

// TestRawFlowZeroCopyParseMatchesProto is the correctness gate for the zero-copy
// envelope parser: for a corpus of marshalled RawFlow envelopes it asserts
// parseRawFlowInto yields the same decoder-relevant fields as proto.Unmarshal,
// including envelopes carrying fields the parser intentionally skips.
func TestRawFlowZeroCopyParseMatchesProto(t *testing.T) {
	must := func(raw *flowpb.RawFlow, err error) *flowpb.RawFlow {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	corpus := []*flowpb.RawFlow{
		must(NewRawFlow("collector-a", "listener-x", 7, time.Unix(1_800_000_000, 0), netip.MustParseAddrPort("192.0.2.1:9999"), flowpb.RawFlow_DECODER_NETFLOW, []byte{0, 5, 0, 1, 2, 3})),
		must(NewRawFlow("c2", "l2", 99, time.Unix(1, 0), netip.MustParseAddrPort("[2001:db8::1]:6343"), flowpb.RawFlow_DECODER_SFLOW, bytes.Repeat([]byte{0xAB}, 1400))),
		// Envelope carrying UseSourceAddress (field 4) and the skipped fields
		// 6/7/8 — the parser must read 4 and step over 6/7/8 without corruption.
		{
			TimeReceived: 42, Payload: []byte{9, 9, 9}, SourceAddress: []byte{10, 0, 0, 1}, SourcePort: 1234,
			RegistryVersion: 3, Decoder: flowpb.RawFlow_DECODER_SFLOW, CollectorId: "cc", ListenerId: "ll",
			UseSourceAddress: true, TimestampSource: 1, DecapsulationProtocol: 1, RateLimit: 5,
		},
		// Saturated scalars.
		{
			TimeReceived: 1<<64 - 1, Payload: []byte{1}, SourceAddress: []byte{255, 255, 255, 255},
			SourcePort: 0xFFFF, RegistryVersion: 1<<64 - 1, Decoder: flowpb.RawFlow_DECODER_NETFLOW,
			CollectorId: "x", ListenerId: "y",
		},
	}
	for i, want := range corpus {
		encoded, err := proto.Marshal(want)
		if err != nil {
			t.Fatalf("case %d marshal: %v", i, err)
		}
		var viaProto, viaFast flowpb.RawFlow
		if err := proto.Unmarshal(encoded, &viaProto); err != nil {
			t.Fatalf("case %d proto.Unmarshal: %v", i, err)
		}
		if err := parseRawFlowInto(encoded, &viaFast); err != nil {
			t.Fatalf("case %d parseRawFlowInto: %v", i, err)
		}
		if !decoderRawFieldsEqual(&viaProto, &viaFast) {
			t.Fatalf("case %d fields differ:\nproto=%+v\nfast =%+v", i, &viaProto, &viaFast)
		}
	}
}

func TestRawFlowZeroCopyParseRejectsTruncated(t *testing.T) {
	encoded, err := proto.Marshal(&flowpb.RawFlow{Payload: bytes.Repeat([]byte{1}, 40), CollectorId: "c", ListenerId: "l"})
	if err != nil {
		t.Fatal(err)
	}
	var dst flowpb.RawFlow
	// Cut mid-field: a length-delimited field now claims more bytes than remain.
	if err := parseRawFlowInto(encoded[:len(encoded)-5], &dst); err == nil {
		t.Fatal("truncated envelope was accepted")
	}
}

func decoderRawFieldsEqual(a, b *flowpb.RawFlow) bool {
	return a.TimeReceived == b.TimeReceived &&
		bytes.Equal(a.Payload, b.Payload) &&
		bytes.Equal(a.SourceAddress, b.SourceAddress) &&
		a.UseSourceAddress == b.UseSourceAddress &&
		a.Decoder == b.Decoder &&
		a.CollectorId == b.CollectorId &&
		a.ListenerId == b.ListenerId &&
		a.SourcePort == b.SourcePort &&
		a.RegistryVersion == b.RegistryVersion
}

// BenchmarkRawFlowEnvelope compares the envelope decode A/B on a realistic
// 30-record NetFlow v5 datagram (~1.5KB payload).
func BenchmarkRawFlowEnvelope(b *testing.B) {
	raw, err := NewRawFlow("collector-a", "flow", 7, time.Unix(1_800_000_000, 0), netip.MustParseAddrPort("192.0.2.1:9999"), flowpb.RawFlow_DECODER_NETFLOW, bytes.Repeat([]byte{0xAB}, 24+30*48))
	if err != nil {
		b.Fatal(err)
	}
	encoded, err := proto.Marshal(raw)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("proto.Unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(encoded)))
		for range b.N {
			var dst flowpb.RawFlow
			if err := proto.Unmarshal(encoded, &dst); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("parseRawFlowInto", func(b *testing.B) {
		var dst flowpb.RawFlow
		b.ReportAllocs()
		b.SetBytes(int64(len(encoded)))
		for range b.N {
			if err := parseRawFlowInto(encoded, &dst); err != nil {
				b.Fatal(err)
			}
		}
	})
}

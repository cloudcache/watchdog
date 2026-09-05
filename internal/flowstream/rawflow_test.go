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

func TestRawFlowRoundTripAndExporterKey(t *testing.T) {
	receivedAt := time.Unix(1_800_000_000, 0)
	source := netip.MustParseAddrPort("192.0.2.10:6343")
	flow, err := NewRawFlow("collector-a", "sflow", 7, receivedAt, source, flowpb.RawFlow_DECODER_SFLOW, []byte{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	key, err := ExporterKey(flow)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(key, append([]byte("collector-a\x00"), source.Addr().AsSlice()...)) {
		t.Fatalf("unexpected exporter key %x", key)
	}

	encoder := NewEncoder()
	encoded, release, err := encoder.Marshal(flow)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var decoded flowpb.RawFlow
	if err := proto.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.CollectorId != "collector-a" || decoded.ListenerId != "sflow" || decoded.RegistryVersion != 7 || decoded.SourcePort != 6343 || decoded.TimeReceived != uint64(receivedAt.Unix()) || decoded.Decoder != flowpb.RawFlow_DECODER_SFLOW || !bytes.Equal(decoded.Payload, []byte{1, 2, 3}) {
		t.Fatalf("unexpected round trip: %+v", &decoded)
	}
}

func TestExporterKeySeparatesCollectorsAndIgnoresSourcePort(t *testing.T) {
	build := func(collector, source string) []byte {
		flow, err := NewRawFlow(collector, "netflow", 1, time.Now(), netip.MustParseAddrPort(source), flowpb.RawFlow_DECODER_NETFLOW, []byte{1})
		if err != nil {
			t.Fatal(err)
		}
		key, err := ExporterKey(flow)
		if err != nil {
			t.Fatal(err)
		}
		return key
	}
	if !bytes.Equal(build("collector-a", "192.0.2.1:1000"), build("collector-a", "192.0.2.1:2000")) {
		t.Fatal("source port changed exporter affinity")
	}
	if bytes.Equal(build("collector-a", "192.0.2.1:1000"), build("collector-b", "192.0.2.1:1000")) {
		t.Fatal("collector identity did not isolate duplicate exporter addresses")
	}
}

func TestNewRawFlowRejectsInvalidInput(t *testing.T) {
	validSource := netip.MustParseAddrPort("192.0.2.1:6343")
	validTime := time.Unix(1_800_000_000, 0)
	tests := []struct {
		name      string
		collector string
		listener  string
		version   uint64
		received  time.Time
		source    netip.AddrPort
		decoder   flowpb.RawFlow_Decoder
		payload   []byte
	}{
		{name: "collector", listener: "sflow", version: 1, received: validTime, source: validSource, decoder: flowpb.RawFlow_DECODER_SFLOW, payload: []byte{1}},
		{name: "listener", collector: "collector-a", version: 1, received: validTime, source: validSource, decoder: flowpb.RawFlow_DECODER_SFLOW, payload: []byte{1}},
		{name: "version", collector: "collector-a", listener: "sflow", received: validTime, source: validSource, decoder: flowpb.RawFlow_DECODER_SFLOW, payload: []byte{1}},
		{name: "time", collector: "collector-a", listener: "sflow", version: 1, source: validSource, decoder: flowpb.RawFlow_DECODER_SFLOW, payload: []byte{1}},
		{name: "source", collector: "collector-a", listener: "sflow", version: 1, received: validTime, decoder: flowpb.RawFlow_DECODER_SFLOW, payload: []byte{1}},
		{name: "decoder", collector: "collector-a", listener: "sflow", version: 1, received: validTime, source: validSource, payload: []byte{1}},
		{name: "payload", collector: "collector-a", listener: "sflow", version: 1, received: validTime, source: validSource, decoder: flowpb.RawFlow_DECODER_SFLOW},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewRawFlow(test.collector, test.listener, test.version, test.received, test.source, test.decoder, test.payload); err == nil {
				t.Fatal("invalid raw flow was accepted")
			}
		})
	}
}

func BenchmarkRawFlowMarshal(b *testing.B) {
	flow, err := NewRawFlow("collector-a", "sflow", 1, time.Unix(1_800_000_000, 0), netip.MustParseAddrPort("192.0.2.1:6343"), flowpb.RawFlow_DECODER_SFLOW, make([]byte, 1400))
	if err != nil {
		b.Fatal(err)
	}
	encoder := NewEncoder()
	b.ReportAllocs()
	b.SetBytes(int64(len(flow.Payload)))
	for range b.N {
		_, release, err := encoder.Marshal(flow)
		if err != nil {
			b.Fatal(err)
		}
		release()
	}
}

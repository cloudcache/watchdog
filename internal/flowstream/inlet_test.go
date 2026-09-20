// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
	"google.golang.org/protobuf/proto"
)

func TestInletEncodesAndProducesRawDatagram(t *testing.T) {
	client := &fakeProducerClient{}
	producer := newProducerWithClient(client, "watchdog.flow.raw-v1")
	inlet, err := NewInlet(producer)
	if err != nil {
		t.Fatal(err)
	}
	completed := 0
	err = inlet.Send(context.Background(), Datagram{
		CollectorID: "collector-a", ListenerID: "sflow", RegistryVersion: 7,
		ReceivedAt: time.Unix(1_800_000_000, 0), Source: netip.MustParseAddrPort("192.0.2.1:6343"),
		Decoder: flowpb.RawFlow_DECODER_SFLOW, Payload: []byte{1, 2, 3},
	}, func(err error) {
		if err != nil {
			t.Errorf("unexpected completion error: %v", err)
		}
		completed++
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed != 1 {
		t.Fatalf("completion calls=%d", completed)
	}
	if string(client.record.Key[:len("collector-a")]) != "collector-a" {
		t.Fatalf("unexpected Kafka key %x", client.record.Key)
	}
	var raw flowpb.RawFlow
	if err := proto.Unmarshal(client.record.Value, &raw); err != nil {
		t.Fatal(err)
	}
	if raw.CollectorId != "collector-a" || raw.ListenerId != "sflow" || raw.RegistryVersion != 7 || raw.SourcePort != 6343 || raw.Decoder != flowpb.RawFlow_DECODER_SFLOW {
		t.Fatalf("unexpected raw flow: %+v", &raw)
	}
}

func TestInletReusesEnvelopeWithoutLeak(t *testing.T) {
	client := &fakeProducerClient{}
	inlet, err := NewInlet(newProducerWithClient(client, "watchdog.flow.raw-v1"))
	if err != nil {
		t.Fatal(err)
	}
	send := func(collector, listener string, version uint64, addr string) flowpb.RawFlow {
		if err := inlet.Send(context.Background(), Datagram{
			CollectorID: collector, ListenerID: listener, RegistryVersion: version,
			ReceivedAt: time.Unix(1_800_000_000, 0), Source: netip.MustParseAddrPort(addr),
			Decoder: flowpb.RawFlow_DECODER_SFLOW, Payload: []byte{1, 2, 3},
		}, nil); err != nil {
			t.Fatal(err)
		}
		var raw flowpb.RawFlow
		if err := proto.Unmarshal(client.record.Value, &raw); err != nil {
			t.Fatal(err)
		}
		return raw
	}
	first := send("collector-a", "sflow", 7, "192.0.2.1:6343")
	second := send("collector-b", "netflow", 9, "198.51.100.2:2055")
	if first.CollectorId != "collector-a" || first.RegistryVersion != 7 {
		t.Fatalf("first datagram corrupted: %+v", &first)
	}
	// The pooled envelope must carry none of the first datagram's fields.
	if second.CollectorId != "collector-b" || second.ListenerId != "netflow" || second.RegistryVersion != 9 || second.SourcePort != 2055 {
		t.Fatalf("pool reuse leaked prior datagram fields: %+v", &second)
	}
}

func BenchmarkInletSend(b *testing.B) {
	inlet, err := NewInlet(newProducerWithClient(&fakeProducerClient{}, "watchdog.flow.raw-v1"))
	if err != nil {
		b.Fatal(err)
	}
	datagram := Datagram{
		CollectorID: "collector-a", ListenerID: "sflow", RegistryVersion: 7,
		ReceivedAt: time.Unix(1_800_000_000, 0), Source: netip.MustParseAddrPort("203.0.113.9:6343"),
		Decoder: flowpb.RawFlow_DECODER_SFLOW, Payload: make([]byte, 256),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := inlet.Send(context.Background(), datagram, nil); err != nil {
			b.Fatal(err)
		}
	}
}

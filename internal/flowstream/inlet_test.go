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

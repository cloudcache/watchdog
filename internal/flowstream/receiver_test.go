// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
)

type receiverSender struct {
	datagrams []Datagram
	err       error
}

func (s *receiverSender) Send(_ context.Context, datagram Datagram, completion func(error)) error {
	copyOfDatagram := datagram
	copyOfDatagram.Payload = append([]byte(nil), datagram.Payload...)
	s.datagrams = append(s.datagrams, copyOfDatagram)
	completion(s.err)
	return s.err
}

func TestInspectDecoder(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		want    flowpb.RawFlow_Decoder
	}{
		{name: "sflow", payload: version32(5, 8), want: flowpb.RawFlow_DECODER_SFLOW},
		{name: "netflow5", payload: version16(5, 24), want: flowpb.RawFlow_DECODER_NETFLOW},
		{name: "netflow9", payload: version16(9, 20), want: flowpb.RawFlow_DECODER_NETFLOW},
		{name: "ipfix", payload: version16(10, 16), want: flowpb.RawFlow_DECODER_NETFLOW},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := InspectDecoder(test.payload)
			if err != nil || got != test.want {
				t.Fatalf("InspectDecoder() = %v, %v; want %v", got, err, test.want)
			}
		})
	}
	for _, payload := range [][]byte{{}, {0}, version16(9, 19), version16(11, 24)} {
		if _, err := InspectDecoder(payload); err == nil {
			t.Fatalf("InspectDecoder(%x) should fail", payload)
		}
	}
}

func TestReceiverRequiresIdentityAndSender(t *testing.T) {
	receiver := Receiver{}
	if err := receiver.Run(context.Background()); err == nil {
		t.Fatal("Run() should reject a missing sender")
	}
	receiver.Sender = &receiverSender{}
	if err := receiver.Run(context.Background()); err == nil {
		t.Fatal("Run() should reject missing identities")
	}
}

func version16(version uint16, size int) []byte {
	payload := make([]byte, size)
	if size >= 2 {
		binary.BigEndian.PutUint16(payload, version)
	}
	return payload
}

func version32(version uint32, size int) []byte {
	payload := make([]byte, size)
	if size >= 4 {
		binary.BigEndian.PutUint32(payload, version)
	}
	return payload
}

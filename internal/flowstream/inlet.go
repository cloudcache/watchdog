// SPDX-FileCopyrightText: 2022 Free Mobile
// SPDX-License-Identifier: AGPL-3.0-only
//
// Adapted from Akvorado inlet/flow/root.go.

package flowstream

import (
	"context"
	"errors"
	"net/netip"
	"time"

	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
)

type Datagram struct {
	CollectorID     string
	ListenerID      string
	RegistryVersion uint64
	ReceivedAt      time.Time
	Source          netip.AddrPort
	Decoder         flowpb.RawFlow_Decoder
	Payload         []byte
}

// DatagramSender synchronously copies the datagram into its downstream
// envelope before Send returns. The receiver may therefore reuse Payload as
// soon as the call completes; Kafka owns only the encoded envelope buffer.
type DatagramSender interface {
	Send(context.Context, Datagram, func(error)) error
}

// Inlet performs only receive-path envelope work. Kafka owns buffering,
// batching, compression and retries; decoding remains downstream.
type Inlet struct {
	encoder  *Encoder
	producer *Producer
}

func NewInlet(producer *Producer) (*Inlet, error) {
	if producer == nil || producer.client == nil {
		return nil, errors.New("raw-flow Kafka producer is required")
	}
	return &Inlet{encoder: NewEncoder(), producer: producer}, nil
}

func (i *Inlet) Send(ctx context.Context, datagram Datagram, completion func(error)) error {
	if i == nil || i.encoder == nil || i.producer == nil {
		return errors.New("raw-flow inlet is not initialized")
	}
	raw, err := NewRawFlow(datagram.CollectorID, datagram.ListenerID, datagram.RegistryVersion, datagram.ReceivedAt, datagram.Source, datagram.Decoder, datagram.Payload)
	if err != nil {
		return err
	}
	key, err := ExporterKey(raw)
	if err != nil {
		return err
	}
	value, release, err := i.encoder.Marshal(raw)
	if err != nil {
		return err
	}
	return i.producer.Send(ctx, key, value, release, completion)
}

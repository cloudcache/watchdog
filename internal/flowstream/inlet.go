// SPDX-FileCopyrightText: 2022 Free Mobile
// SPDX-License-Identifier: AGPL-3.0-only
//
// Adapted from Akvorado inlet/flow/root.go.

package flowstream

import (
	"context"
	"errors"
	"net/netip"
	"sync"
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
	// rawPool reuses the RawFlow envelope struct across datagrams. Marshal copies
	// every field into the encoded buffer, so the envelope is returned to the pool
	// immediately after Marshal and holds no reference the record retains.
	rawPool sync.Pool
}

func NewInlet(producer *Producer) (*Inlet, error) {
	if producer == nil || producer.client == nil {
		return nil, errors.New("raw-flow Kafka producer is required")
	}
	inlet := &Inlet{encoder: NewEncoder(), producer: producer}
	inlet.rawPool.New = func() any { return &flowpb.RawFlow{} }
	return inlet, nil
}

func (i *Inlet) Send(ctx context.Context, datagram Datagram, completion func(error)) error {
	if i == nil || i.encoder == nil || i.producer == nil {
		return errors.New("raw-flow inlet is not initialized")
	}
	raw := i.rawPool.Get().(*flowpb.RawFlow)
	if err := fillRawFlow(raw, datagram.CollectorID, datagram.ListenerID, datagram.RegistryVersion, datagram.ReceivedAt, datagram.Source, datagram.Decoder, datagram.Payload); err != nil {
		i.rawPool.Put(raw)
		return err
	}
	key, err := ExporterKey(raw)
	if err != nil {
		i.rawPool.Put(raw)
		return err
	}
	value, release, err := i.encoder.Marshal(raw)
	// Marshal has serialized every field into the encoded buffer; the record keeps
	// only key and value, so the envelope can be reused immediately.
	i.rawPool.Put(raw)
	if err != nil {
		return err
	}
	return i.producer.Send(ctx, key, value, release, completion)
}

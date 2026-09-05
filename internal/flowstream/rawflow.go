// SPDX-FileCopyrightText: 2024 Free Mobile
// SPDX-License-Identifier: AGPL-3.0-only
//
// Adapted from Akvorado's raw-flow inlet buffer lifecycle.

package flowstream

import (
	"errors"
	"net/netip"
	"sync"
	"time"

	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
	"google.golang.org/protobuf/proto"
)

const (
	SchemaVersion   = 1
	maxPayloadSize  = 65535
	maxIdentitySize = 255
)

// Encoder reuses protobuf output buffers until Kafka finishes with a record.
type Encoder struct {
	pool sync.Pool
}

func NewEncoder() *Encoder {
	encoder := &Encoder{}
	encoder.pool.New = func() any {
		buffer := make([]byte, 0, 2048)
		return &buffer
	}
	return encoder
}

// NewRawFlow builds the immutable envelope written to the raw-flow topic.
func NewRawFlow(collectorID, listenerID string, registryVersion uint64, receivedAt time.Time, source netip.AddrPort, decoder flowpb.RawFlow_Decoder, payload []byte) (*flowpb.RawFlow, error) {
	if collectorID == "" || len(collectorID) > maxIdentitySize {
		return nil, errors.New("collector ID must contain 1..255 bytes")
	}
	if listenerID == "" || len(listenerID) > maxIdentitySize {
		return nil, errors.New("listener ID must contain 1..255 bytes")
	}
	if registryVersion == 0 {
		return nil, errors.New("registry version must be positive")
	}
	if receivedAt.IsZero() || receivedAt.Unix() < 0 {
		return nil, errors.New("receive time must be a non-negative Unix time")
	}
	if !source.IsValid() {
		return nil, errors.New("source address is required")
	}
	if decoder != flowpb.RawFlow_DECODER_NETFLOW && decoder != flowpb.RawFlow_DECODER_SFLOW {
		return nil, errors.New("decoder must be NetFlow or sFlow")
	}
	if len(payload) == 0 || len(payload) > maxPayloadSize {
		return nil, errors.New("payload must contain 1..65535 bytes")
	}

	address := source.Addr().Unmap().AsSlice()
	return &flowpb.RawFlow{
		TimeReceived:    uint64(receivedAt.Unix()),
		Payload:         payload,
		SourceAddress:   address,
		Decoder:         decoder,
		CollectorId:     collectorID,
		ListenerId:      listenerID,
		SourcePort:      uint32(source.Port()),
		RegistryVersion: registryVersion,
	}, nil
}

// ExporterKey keeps all datagrams from one exporter and collector on the same
// Kafka partition so NetFlow/IPFIX template state stays ordered.
func ExporterKey(flow *flowpb.RawFlow) ([]byte, error) {
	if flow == nil || flow.CollectorId == "" {
		return nil, errors.New("raw flow collector ID is required")
	}
	address, ok := netip.AddrFromSlice(flow.SourceAddress)
	if !ok {
		return nil, errors.New("raw flow source address is invalid")
	}
	address = address.Unmap()
	key := make([]byte, 0, len(flow.CollectorId)+1+16)
	key = append(key, flow.CollectorId...)
	key = append(key, 0)
	key = append(key, address.AsSlice()...)
	return key, nil
}

// Marshal returns an encoded value and a release function. The caller must
// invoke release exactly once after Kafka's completion callback fires.
func (e *Encoder) Marshal(flow *flowpb.RawFlow) ([]byte, func(), error) {
	if e == nil {
		return nil, nil, errors.New("raw flow encoder is required")
	}
	if _, err := ExporterKey(flow); err != nil {
		return nil, nil, err
	}
	buffer := e.pool.Get().(*[]byte)
	encoded, err := proto.MarshalOptions{}.MarshalAppend((*buffer)[:0], flow)
	if err != nil {
		e.pool.Put(buffer)
		return nil, nil, err
	}
	*buffer = encoded[:0]
	return encoded, func() { e.pool.Put(buffer) }, nil
}

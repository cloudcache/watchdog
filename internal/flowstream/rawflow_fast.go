// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
)

// parseRawFlowInto decodes the RawFlow envelope's protobuf wire bytes into dst
// without copying the flow payload. proto.Unmarshal copies every bytes field;
// here Payload and SourceAddress are sliced straight out of value (valid for the
// batch's lifetime under the Decoder zero-copy contract — the worker maps each
// batch before the Kafka record buffer is reused). Only the two small identity
// strings are copied, since downstream keeps them.
//
// It handles exactly the RawFlow fields the decoder reads and skips any other
// field by wire type, so it stays forward-compatible. dst is reused across
// calls; it is Reset first. TestRawFlowZeroCopyParseMatchesProto asserts this
// produces the same result as proto.Unmarshal for well-formed envelopes.
func parseRawFlowInto(value []byte, dst *flowpb.RawFlow) error {
	dst.Reset()
	for len(value) > 0 {
		tag, width := binary.Uvarint(value)
		if width <= 0 {
			return errors.New("raw flow: invalid field tag")
		}
		value = value[width:]
		field := tag >> 3
		switch tag & 0x7 {
		case 0: // varint
			v, n := binary.Uvarint(value)
			if n <= 0 {
				return errors.New("raw flow: invalid varint")
			}
			value = value[n:]
			switch field {
			case 1:
				dst.TimeReceived = v
			case 4:
				dst.UseSourceAddress = v != 0
			case 5:
				dst.Decoder = flowpb.RawFlow_Decoder(v)
			case 11:
				dst.SourcePort = uint32(v)
			case 12:
				dst.RegistryVersion = v
			}
		case 2: // length-delimited
			length, n := binary.Uvarint(value)
			if n <= 0 {
				return errors.New("raw flow: invalid length prefix")
			}
			value = value[n:]
			if uint64(len(value)) < length {
				return errors.New("raw flow: truncated length-delimited field")
			}
			data := value[:length]
			value = value[length:]
			switch field {
			case 2:
				dst.Payload = data // zero-copy: slice of value
			case 3:
				dst.SourceAddress = data // zero-copy: slice of value
			case 9:
				dst.CollectorId = string(data)
			case 10:
				dst.ListenerId = string(data)
			}
		case 1: // 64-bit
			if len(value) < 8 {
				return errors.New("raw flow: truncated 64-bit field")
			}
			value = value[8:]
		case 5: // 32-bit
			if len(value) < 4 {
				return errors.New("raw flow: truncated 32-bit field")
			}
			value = value[4:]
		default:
			return fmt.Errorf("raw flow: unsupported wire type %d", tag&0x7)
		}
	}
	return nil
}

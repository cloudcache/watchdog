// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
)

// The hand-written fast paths read the untrusted datagram at fixed offsets; their
// bounds safety rests entirely on the length guards preceding each read. These
// fuzz targets exercise those guards directly against adversarial input so a
// future edit that weakens one is caught, rather than relying on the
// prefix-truncation corpus alone. A returned error is fine; a panic (an
// out-of-bounds read or slice conversion) is the failure.

func FuzzParseRawFlowInto(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x08, 0x01})
	f.Add([]byte{0x0a, 0x03, 1, 2, 3, 0x12, 0x04, 10, 0, 0, 1})
	intern := func(b []byte) string { return string(b) }
	f.Fuzz(func(_ *testing.T, value []byte) {
		var dst flowpb.RawFlow
		_ = parseRawFlowInto(value, &dst, intern)
	})
}

func FuzzDecodeNetFlowV5Fast(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 5})
	f.Add([]byte{0, 5, 0, 1})
	sampler := make([]byte, 16)
	f.Fuzz(func(_ *testing.T, payload []byte) {
		_, _ = decodeNetFlowV5Fast(payload, uint64(time.Now().UnixNano()), sampler, nil)
	})
}

func FuzzDecodeSFlowV5Fast(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 5})
	f.Add([]byte{0, 0, 0, 5, 0, 0, 0, 1})
	decoder, err := NewDecoder(time.Minute)
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(decoder.Close)
	f.Fuzz(func(_ *testing.T, payload []byte) {
		// decodeSFlowV5Fast returns errSFlowFallback for unhandled constructs and
		// a decode error for malformed input; both are acceptable. It must never
		// read out of bounds.
		_ = decoder.decodeSFlowV5Fast(payload, uint64(time.Now().UnixNano()))
	})
}

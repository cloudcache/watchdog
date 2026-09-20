// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"sync"
	"syscall"
	"time"

	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
	"golang.org/x/sys/unix"
)

// Receiver is the complete collector hot path: receive, source admission,
// RawFlow envelope, Kafka. Protocol decode and business lookups are downstream.
type Receiver struct {
	ListenAddr         string
	CollectorID        string
	ListenerID         string
	RegistryVersion    uint64
	Decoder            flowpb.RawFlow_Decoder
	ReceiveBufferBytes int
	MaxDatagramBytes   int
	ReusePort          bool
	Sender             DatagramSender
	Admit              func(netip.AddrPort) bool

	OnReceived     func(flowpb.RawFlow_Decoder)
	OnRejected     func()
	OnInvalid      func()
	OnOversize     func()
	OnPublishError func()
	OnKernelDrops  func(uint64)
	// OnReceiveBuffer reports the requested SO_RCVBUF and the value the kernel
	// actually granted (as returned by getsockopt; Linux reports roughly twice
	// the usable bytes). It fires once after the buffer is set so a silent clamp
	// to net.core.rmem_max is observable instead of assumed.
	OnReceiveBuffer func(requested, effective int)

	pool sync.Pool
}

func (r *Receiver) Run(ctx context.Context) error {
	if r.Sender == nil {
		return errors.New("raw-flow datagram sender is required")
	}
	if r.CollectorID == "" || r.ListenerID == "" || r.RegistryVersion == 0 {
		return errors.New("collector, listener and registry version are required")
	}
	if r.Decoder != flowpb.RawFlow_DECODER_UNSPECIFIED && r.Decoder != flowpb.RawFlow_DECODER_NETFLOW && r.Decoder != flowpb.RawFlow_DECODER_SFLOW {
		return errors.New("listener decoder must be automatic, NetFlow or sFlow")
	}

	conn, err := r.listen(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if r.ReceiveBufferBytes > 0 {
		if err := conn.SetReadBuffer(r.ReceiveBufferBytes); err != nil {
			return err
		}
		if r.OnReceiveBuffer != nil {
			if effective, ok := readSocketReceiveBuffer(conn); ok {
				r.OnReceiveBuffer(r.ReceiveBufferBytes, effective)
			}
		}
	}
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	oob := make([]byte, 64)
	var lastOverflow uint32
	for {
		payload := r.getBuffer()
		n, oobn, flags, source, err := conn.ReadMsgUDPAddrPort(payload, oob)
		if err != nil {
			r.putBuffer(payload)
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if overflow, ok := socketOverflowCount(oob[:oobn]); ok {
			dropped := uint64(uint32(overflow - lastOverflow))
			lastOverflow = overflow
			if dropped > 0 && r.OnKernelDrops != nil {
				r.OnKernelDrops(dropped)
			}
		}
		if flags&syscall.MSG_TRUNC != 0 {
			r.putBuffer(payload)
			if r.OnOversize != nil {
				r.OnOversize()
			}
			continue
		}
		if n == 0 {
			r.putBuffer(payload)
			r.invalid()
			continue
		}
		if r.Admit != nil && !r.Admit(source) {
			r.putBuffer(payload)
			if r.OnRejected != nil {
				r.OnRejected()
			}
			continue
		}
		decoder := r.Decoder
		if decoder == flowpb.RawFlow_DECODER_UNSPECIFIED {
			decoder, err = InspectDecoder(payload[:n])
			if err != nil {
				r.putBuffer(payload)
				r.invalid()
				continue
			}
		}
		if r.OnReceived != nil {
			r.OnReceived(decoder)
		}

		var completed sync.Once
		complete := func(err error) {
			completed.Do(func() {
				if err != nil && r.OnPublishError != nil {
					r.OnPublishError()
				}
			})
		}
		err = r.Sender.Send(ctx, Datagram{
			CollectorID:     r.CollectorID,
			ListenerID:      r.ListenerID,
			RegistryVersion: r.RegistryVersion,
			ReceivedAt:      time.Now().UTC(),
			Source:          source,
			Decoder:         decoder,
			Payload:         payload[:n],
		}, complete)
		if err != nil {
			complete(err)
		}
		r.putBuffer(payload)
	}
}

// InspectDecoder distinguishes the two GoFlow2 decoder families without
// parsing templates or records on the collector hot path.
func InspectDecoder(payload []byte) (flowpb.RawFlow_Decoder, error) {
	if len(payload) >= 4 && binary.BigEndian.Uint32(payload[:4]) == 5 {
		return flowpb.RawFlow_DECODER_SFLOW, nil
	}
	if len(payload) < 2 {
		return flowpb.RawFlow_DECODER_UNSPECIFIED, errors.New("flow datagram is shorter than a version field")
	}
	switch binary.BigEndian.Uint16(payload[:2]) {
	case 5:
		if len(payload) < 24 {
			break
		}
		return flowpb.RawFlow_DECODER_NETFLOW, nil
	case 9:
		if len(payload) < 20 {
			break
		}
		return flowpb.RawFlow_DECODER_NETFLOW, nil
	case 10:
		if len(payload) < 16 {
			break
		}
		return flowpb.RawFlow_DECODER_NETFLOW, nil
	}
	return flowpb.RawFlow_DECODER_UNSPECIFIED, errors.New("unsupported or truncated flow datagram")
}

func (r *Receiver) listen(ctx context.Context) (*net.UDPConn, error) {
	listenConfig := net.ListenConfig{Control: func(_, _ string, raw syscall.RawConn) error {
		var controlErr error
		if err := raw.Control(func(fd uintptr) {
			if r.ReusePort {
				if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
					controlErr = err
					return
				}
				if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
					controlErr = err
					return
				}
			}
			controlErr = configureRXQOverflow(int(fd))
		}); err != nil {
			return err
		}
		return controlErr
	}}
	packetConn, err := listenConfig.ListenPacket(ctx, "udp", r.ListenAddr)
	if err != nil {
		return nil, err
	}
	udpConn, ok := packetConn.(*net.UDPConn)
	if !ok {
		_ = packetConn.Close()
		return nil, errors.New("UDP listener did not return *net.UDPConn")
	}
	return udpConn, nil
}

// readSocketReceiveBuffer returns the kernel's current SO_RCVBUF for the
// datagram socket. SetReadBuffer requests a size but the kernel silently clamps
// it to net.core.rmem_max, so reading it back is the only way to know the buffer
// the collector actually has. Linux reports roughly twice the usable bytes.
func readSocketReceiveBuffer(conn *net.UDPConn) (int, bool) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, false
	}
	var size int
	var opErr error
	if controlErr := raw.Control(func(fd uintptr) {
		size, opErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_RCVBUF)
	}); controlErr != nil || opErr != nil {
		return 0, false
	}
	return size, true
}

func (r *Receiver) invalid() {
	if r.OnInvalid != nil {
		r.OnInvalid()
	}
}

func (r *Receiver) bufferSize() int {
	if r.MaxDatagramBytes > 0 && r.MaxDatagramBytes <= maxPayloadSize {
		return r.MaxDatagramBytes
	}
	return maxPayloadSize
}

func (r *Receiver) getBuffer() []byte {
	size := r.bufferSize()
	if value := r.pool.Get(); value != nil {
		buffer := value.([]byte)
		if cap(buffer) >= size {
			return buffer[:size]
		}
	}
	return make([]byte, size)
}

func (r *Receiver) putBuffer(buffer []byte) {
	size := r.bufferSize()
	if cap(buffer) >= size {
		r.pool.Put(buffer[:size])
	}
}

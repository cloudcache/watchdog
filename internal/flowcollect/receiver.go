package flowcollect

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type Datagram struct {
	Protocol   Protocol
	ReceivedAt time.Time
	Source     netip.AddrPort
	Payload    []byte
	release    func([]byte)
}

func (d *Datagram) Release() {
	if d.release != nil && d.Payload != nil {
		d.release(d.Payload)
		d.Payload = nil
	}
}

type Receiver struct {
	Protocol           Protocol
	ListenAddr         string
	ReceiveBufferBytes int
	MaxDatagramBytes   int
	Queue              chan<- Datagram
	OnQueueFull        func(Protocol)
	OnInvalid          func()
	ReusePort          bool
	pool               sync.Pool
}

func (r *Receiver) Run(ctx context.Context) error {
	if r.Queue == nil {
		return errors.New("flow receiver queue is required")
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
	}
	go func() { <-ctx.Done(); _ = conn.Close() }()
	for {
		buf := r.getBuffer()
		n, source, err := conn.ReadFromUDPAddrPort(buf)
		if err != nil {
			r.putBuffer(buf)
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		protocol := r.Protocol
		if protocol == 0 {
			protocol, _, err = InspectDatagram(buf[:n])
			if err != nil {
				r.putBuffer(buf)
				if r.OnInvalid != nil {
					r.OnInvalid()
				}
				continue
			}
		}
		d := Datagram{Protocol: protocol, ReceivedAt: time.Now(), Source: source, Payload: buf[:n], release: r.putBuffer}
		select {
		case r.Queue <- d:
		case <-ctx.Done():
			d.Release()
			return nil
		default:
			d.Release()
			if r.OnQueueFull != nil {
				r.OnQueueFull(r.Protocol)
			}
		}
	}
}

func (r *Receiver) listen(ctx context.Context) (*net.UDPConn, error) {
	if !r.ReusePort {
		addr, err := net.ResolveUDPAddr("udp", r.ListenAddr)
		if err != nil {
			return nil, err
		}
		return net.ListenUDP("udp", addr)
	}
	listenConfig := net.ListenConfig{Control: func(_, _ string, raw syscall.RawConn) error {
		var controlErr error
		if err := raw.Control(func(fd uintptr) {
			if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
				controlErr = err
				return
			}
			controlErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
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

func InspectDatagram(payload []byte) (Protocol, uint64, error) {
	if len(payload) >= 4 && binary.BigEndian.Uint32(payload[:4]) == 5 {
		return ProtocolSFlow5, 0, nil
	}
	if len(payload) < 2 {
		return 0, 0, errors.New("flow datagram is shorter than a version field")
	}
	switch binary.BigEndian.Uint16(payload[:2]) {
	case 5:
		if len(payload) < 24 {
			return 0, 0, errors.New("NetFlow v5 datagram is shorter than its header")
		}
		return ProtocolNetFlow5, 0, nil
	case 9:
		if len(payload) < 20 {
			return 0, 0, errors.New("NetFlow v9 datagram is shorter than its header")
		}
		return ProtocolNetFlow9, uint64(binary.BigEndian.Uint32(payload[16:20])), nil
	case 10:
		if len(payload) < 16 {
			return 0, 0, errors.New("IPFIX datagram is shorter than its header")
		}
		return ProtocolIPFIX, uint64(binary.BigEndian.Uint32(payload[12:16])), nil
	default:
		return 0, 0, errors.New("unsupported flow datagram version")
	}
}

func (r *Receiver) getBuffer() []byte {
	if r.MaxDatagramBytes <= 0 {
		r.MaxDatagramBytes = 65535
	}
	if value := r.pool.Get(); value != nil {
		return value.([]byte)
	}
	return make([]byte, r.MaxDatagramBytes)
}

func (r *Receiver) putBuffer(buf []byte) {
	if cap(buf) >= r.MaxDatagramBytes {
		r.pool.Put(buf[:r.MaxDatagramBytes])
	}
}

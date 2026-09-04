//go:build linux

package flowcollect

import (
	"encoding/binary"

	"golang.org/x/sys/unix"
)

func configureRXQOverflow(fd int) error {
	return unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RXQ_OVFL, 1)
}

func rxqTelemetrySupported() bool {
	return true
}

func socketOverflowCount(oob []byte) (uint32, bool) {
	messages, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return 0, false
	}
	for _, message := range messages {
		if message.Header.Level == unix.SOL_SOCKET && message.Header.Type == unix.SO_RXQ_OVFL && len(message.Data) >= 4 {
			return binary.NativeEndian.Uint32(message.Data[:4]), true
		}
	}
	return 0, false
}

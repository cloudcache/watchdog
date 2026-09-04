//go:build !linux

package flowcollect

func configureRXQOverflow(int) error {
	return nil
}

func rxqTelemetrySupported() bool {
	return false
}

func socketOverflowCount([]byte) (uint32, bool) {
	return 0, false
}

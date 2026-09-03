//go:build windows

package agent

import (
	"errors"
	"sync"
)

var (
	smartctlOnce sync.Once
	smartctlPath string
	smartctlErr  error
)

func ensureEmbeddedSmartctl() (string, error) {
	smartctlOnce.Do(func() {
		smartctlErr = errors.New("smartctl is not bundled; install smartmontools or add smartctl.exe to PATH")
	})

	return smartctlPath, smartctlErr
}

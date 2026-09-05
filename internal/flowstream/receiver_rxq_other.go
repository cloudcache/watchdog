//go:build !linux

// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

func configureRXQOverflow(int) error {
	return nil
}

func socketOverflowCount([]byte) (uint32, bool) {
	return 0, false
}

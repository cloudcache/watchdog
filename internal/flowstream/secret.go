// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// ReadSecretFile reads a small single-value secret without accepting NUL or
// an empty value. Callers never expose the returned value in effective-config
// logs or errors.
func ReadSecretFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("read secret file: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	if err != nil {
		return "", fmt.Errorf("read secret file: %w", err)
	}
	if len(data) == 0 || len(data) > 64<<10 {
		return "", errors.New("secret file size is invalid")
	}
	secret := strings.TrimRight(string(data), "\r\n")
	if secret == "" || strings.ContainsRune(secret, '\x00') {
		return "", errors.New("secret file value is invalid")
	}
	return secret, nil
}

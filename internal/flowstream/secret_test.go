// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadSecretFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(path, []byte("secret\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if value, err := ReadSecretFile(path); err != nil || value != "secret" {
		t.Fatalf("value=%q error=%v", value, err)
	}
	if err := os.WriteFile(path, []byte{'a', 0, 'b'}, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSecretFile(path); err == nil {
		t.Fatal("NUL secret was accepted")
	}
}

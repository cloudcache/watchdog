package watchdog

import (
	"crypto/rand"
	"encoding/hex"
)

// newManagementID returns a random 26-character identifier that fits the
// CHAR(26) primary keys used by the legacy management repositories while they
// are moved behind the KISS server.
func newManagementID() (ID, error) {
	buf := make([]byte, 13)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return ID(hex.EncodeToString(buf)), nil
}

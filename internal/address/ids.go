package address

import (
	"crypto/rand"
	"encoding/base32"

	"github.com/google/uuid"
)

// ID is a plain string identifier. It is kept as a named alias so the ported
// address logic reads verbatim; the single-domain build has no tenant/owner
// scoping, so IDs are just opaque strings.
type ID = string

var crockford = base32.NewEncoding("0123456789ABCDEFGHJKMNPQRSTVWXYZ").WithPadding(base32.NoPadding)

// newManagementID returns a 26-char CHAR(26) ULID-style id (geo nodes,
// operators, geo lines, imports).
func newManagementID() (ID, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return crockford.EncodeToString(b[:]), nil
}

// NewSetID returns a 36-char UUIDv4 (address sets and prefixes use CHAR(36)).
func NewSetID() string {
	return uuid.NewString()
}

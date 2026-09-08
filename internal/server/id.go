package server

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
)

// crockford is the ULID/Crockford base32 alphabet (no I, L, O, U).
var crockford = base32.NewEncoding("0123456789ABCDEFGHJKMNPQRSTVWXYZ").WithPadding(base32.NoPadding)

// newID returns an opaque 26-char CHAR(26) identifier (128 bits of randomness).
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return crockford.EncodeToString(b[:])
}

// randomToken returns a URL-safe opaque secret (used for sessions and resets).
func randomToken() string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return crockford.EncodeToString(b[:])
}

// sha256hex returns the lowercase hex SHA-256 of s (for at-rest token hashing).
func sha256hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

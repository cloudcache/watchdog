package address

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"
)

// Faithful de-tenant port of internal/watchdog/address_dimension_trust.go.
// The only change is removing the tenant scope: keys are resolved by key id
// alone (single install), so the resolver interface, lookup, config entry and
// trusted-key struct drop their TenantID. The ed25519 PEM/PKIX handling is
// unchanged.

var ErrAddressDimensionTrustedKeyNotFound = errors.New("address dimension trusted signing key not found")

// AddressDimensionPublicKeyResolver resolves a trusted ed25519 public key by its key id.
type AddressDimensionPublicKeyResolver interface {
	ResolveAddressDimensionPublicKey(context.Context, string) (ed25519.PublicKey, error)
}

// AddressDimensionTrustedKeyConfig configures one trusted publication signing key.
type AddressDimensionTrustedKeyConfig struct {
	KeyID         string
	PublicKeyFile string
}

type AddressDimensionTrustedPublicKey struct {
	KeyID string
	Key   ed25519.PublicKey
}

type StaticAddressDimensionPublicKeyResolver struct {
	keys map[string]ed25519.PublicKey
}

func NewStaticAddressDimensionPublicKeyResolver(entries []AddressDimensionTrustedPublicKey) (*StaticAddressDimensionPublicKeyResolver, error) {
	resolver := &StaticAddressDimensionPublicKeyResolver{keys: make(map[string]ed25519.PublicKey, len(entries))}
	for _, entry := range entries {
		entry.KeyID = strings.TrimSpace(entry.KeyID)
		if entry.KeyID == "" || len(entry.KeyID) > 128 || len(entry.Key) != ed25519.PublicKeySize {
			return nil, ErrAddressDimensionInvalid
		}
		if _, exists := resolver.keys[entry.KeyID]; exists {
			return nil, fmt.Errorf("duplicate address dimension trusted key %q", entry.KeyID)
		}
		resolver.keys[entry.KeyID] = append(ed25519.PublicKey(nil), entry.Key...)
	}
	return resolver, nil
}

func LoadAddressDimensionPublicKeyResolver(entries []AddressDimensionTrustedKeyConfig) (*StaticAddressDimensionPublicKeyResolver, error) {
	trusted := make([]AddressDimensionTrustedPublicKey, 0, len(entries))
	for _, entry := range entries {
		data, err := os.ReadFile(entry.PublicKeyFile)
		if err != nil {
			return nil, fmt.Errorf("read address dimension trusted key %q: %w", entry.KeyID, err)
		}
		publicKey, err := parseAddressDimensionPublicKeyPEM(data)
		if err != nil {
			return nil, fmt.Errorf("parse address dimension trusted key %q: %w", entry.KeyID, err)
		}
		trusted = append(trusted, AddressDimensionTrustedPublicKey{KeyID: entry.KeyID, Key: publicKey})
	}
	return NewStaticAddressDimensionPublicKeyResolver(trusted)
}

func (r *StaticAddressDimensionPublicKeyResolver) ResolveAddressDimensionPublicKey(_ context.Context, keyID string) (ed25519.PublicKey, error) {
	if r == nil || strings.TrimSpace(keyID) == "" {
		return nil, ErrAddressDimensionTrustedKeyNotFound
	}
	key, ok := r.keys[strings.TrimSpace(keyID)]
	if !ok {
		return nil, ErrAddressDimensionTrustedKeyNotFound
	}
	return append(ed25519.PublicKey(nil), key...), nil
}

func parseAddressDimensionPublicKeyPEM(data []byte) (ed25519.PublicKey, error) {
	block, rest := pem.Decode(data)
	if block == nil || len(strings.TrimSpace(string(rest))) != 0 {
		return nil, errors.New("public key file must contain one PEM public key")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	key, ok := parsed.(ed25519.PublicKey)
	if !ok || len(key) != ed25519.PublicKeySize {
		return nil, errors.New("public key is not Ed25519")
	}
	return append(ed25519.PublicKey(nil), key...), nil
}

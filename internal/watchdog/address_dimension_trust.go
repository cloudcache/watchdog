package watchdog

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

var ErrAddressDimensionTrustedKeyNotFound = errors.New("address dimension trusted signing key not found")

type AddressDimensionPublicKeyResolver interface {
	ResolveAddressDimensionPublicKey(context.Context, ID, string) (ed25519.PublicKey, error)
}

type AddressDimensionTrustedPublicKey struct {
	TenantID ID
	KeyID    string
	Key      ed25519.PublicKey
}

type StaticAddressDimensionPublicKeyResolver struct {
	keys map[string]ed25519.PublicKey
}

func NewStaticAddressDimensionPublicKeyResolver(entries []AddressDimensionTrustedPublicKey) (*StaticAddressDimensionPublicKeyResolver, error) {
	resolver := &StaticAddressDimensionPublicKeyResolver{keys: make(map[string]ed25519.PublicKey, len(entries))}
	for _, entry := range entries {
		entry.TenantID = ID(strings.TrimSpace(string(entry.TenantID)))
		entry.KeyID = strings.TrimSpace(entry.KeyID)
		if entry.TenantID == "" || entry.KeyID == "" || len(entry.KeyID) > 128 || len(entry.Key) != ed25519.PublicKeySize {
			return nil, ErrAddressDimensionInvalid
		}
		lookup := addressDimensionTrustedKeyLookup(entry.TenantID, entry.KeyID)
		if _, exists := resolver.keys[lookup]; exists {
			return nil, fmt.Errorf("duplicate address dimension trusted key %q for tenant %q", entry.KeyID, entry.TenantID)
		}
		resolver.keys[lookup] = append(ed25519.PublicKey(nil), entry.Key...)
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
		trusted = append(trusted, AddressDimensionTrustedPublicKey{TenantID: entry.TenantID, KeyID: entry.KeyID, Key: publicKey})
	}
	return NewStaticAddressDimensionPublicKeyResolver(trusted)
}

func (r *StaticAddressDimensionPublicKeyResolver) ResolveAddressDimensionPublicKey(_ context.Context, tenantID ID, keyID string) (ed25519.PublicKey, error) {
	if r == nil || tenantID == "" || strings.TrimSpace(keyID) == "" {
		return nil, ErrAddressDimensionTrustedKeyNotFound
	}
	key, ok := r.keys[addressDimensionTrustedKeyLookup(tenantID, keyID)]
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

func addressDimensionTrustedKeyLookup(tenantID ID, keyID string) string {
	return string(tenantID) + "\x00" + strings.TrimSpace(keyID)
}

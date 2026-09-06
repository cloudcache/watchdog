package watchdog

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAddressDimensionTrustedKeyResolverLoadsPEMAndScopesTenant(t *testing.T) {
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "publisher.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	resolver, err := LoadAddressDimensionPublicKeyResolver([]AddressDimensionTrustedKeyConfig{{
		TenantID: "tenant-a", KeyID: "publisher-2026", PublicKeyFile: path,
	}})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolver.ResolveAddressDimensionPublicKey(context.Background(), "tenant-a", "publisher-2026")
	if err != nil || !publicKey.Equal(resolved) {
		t.Fatalf("resolved key = %x, %v", resolved, err)
	}
	resolved[0] ^= 0xff
	resolvedAgain, err := resolver.ResolveAddressDimensionPublicKey(context.Background(), "tenant-a", "publisher-2026")
	if err != nil || publicKey.Equal(resolved) || !publicKey.Equal(resolvedAgain) {
		t.Fatal("resolver did not return an isolated key copy")
	}
	if _, err := resolver.ResolveAddressDimensionPublicKey(context.Background(), "tenant-b", "publisher-2026"); !errors.Is(err, ErrAddressDimensionTrustedKeyNotFound) {
		t.Fatalf("cross-tenant key lookup error = %v", err)
	}
}

func TestAddressDimensionTrustedKeyResolverRejectsInvalidAndDuplicateKeys(t *testing.T) {
	invalid := filepath.Join(t.TempDir(), "invalid.pem")
	if err := os.WriteFile(invalid, []byte("not a public key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAddressDimensionPublicKeyResolver([]AddressDimensionTrustedKeyConfig{{TenantID: "tenant-a", KeyID: "key-a", PublicKeyFile: invalid}}); err == nil {
		t.Fatal("invalid public key was accepted")
	}
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewStaticAddressDimensionPublicKeyResolver([]AddressDimensionTrustedPublicKey{
		{TenantID: "tenant-a", KeyID: "key-a", Key: publicKey},
		{TenantID: "tenant-a", KeyID: "key-a", Key: publicKey},
	}); err == nil {
		t.Fatal("duplicate trusted key was accepted")
	}
}

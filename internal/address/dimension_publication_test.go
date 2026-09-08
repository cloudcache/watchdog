package address

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"
)

// Faithful ports of internal/watchdog/dimension_publication_test.go (pure): the
// scope validation contract, and that the generic signing payload is byte-identical
// to the address alias and binds the publication scope (module/dimension).
func TestDimensionPublicationScopeValidation(t *testing.T) {
	for _, test := range []struct {
		name  string
		scope DimensionPublicationScope
		valid bool
	}{
		{name: "address", scope: DimensionPublicationScope{ModuleKey: "flow", DimensionKey: "address"}, valid: true},
		{name: "vpn", scope: DimensionPublicationScope{ModuleKey: "flow", DimensionKey: "vpn_rule_set"}, valid: true},
		{name: "missing module", scope: DimensionPublicationScope{DimensionKey: "address"}},
		{name: "trimmed", scope: DimensionPublicationScope{ModuleKey: " flow", DimensionKey: "address"}},
		{name: "uppercase", scope: DimensionPublicationScope{ModuleKey: "Flow", DimensionKey: "address"}},
		{name: "separator", scope: DimensionPublicationScope{ModuleKey: "flow", DimensionKey: "vpn/rules"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.scope.validate(); (err == nil) != test.valid {
				t.Fatalf("validate() error = %v, valid = %v", err, test.valid)
			}
		})
	}
}

func TestDimensionPublicationSigningBindsScopeAndPreservesAddressWire(t *testing.T) {
	snapshot := addressDimensionSignatureFixture()
	signedAt := time.Date(2026, 9, 7, 8, 9, 10, 111000000, time.UTC)
	legacy, err := AddressDimensionSigningPayload(snapshot, "publisher-1", signedAt)
	if err != nil {
		t.Fatal(err)
	}
	generic, err := DimensionPublicationSigningPayload(snapshot, "publisher-1", signedAt)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(legacy, generic) {
		t.Fatal("generic signing changed the existing address wire bytes")
	}

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	approval, err := VerifyDimensionPublicationApproval(snapshot, "publisher-1", signedAt, ed25519.Sign(privateKey, generic), publicKey)
	if err != nil {
		t.Fatal(err)
	}
	tampered := snapshot
	tampered.DimensionKey = "vpn_rule_set"
	if err := validateVerifiedDimensionPublicationApproval(tampered, approval); err == nil {
		t.Fatal("approval proof accepted a different publication scope")
	}
}

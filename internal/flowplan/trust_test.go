package flowplan

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestTrustStoreRejectsRollbackConflictAndRevokedKeys(t *testing.T) {
	public1, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	public2, private2, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	public3, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	issued := time.Date(2026, 9, 7, 1, 0, 0, 0, time.UTC)
	first := marshalTrustBundleForTest(t, TrustBundle{
		SchemaVersion: TrustBundleSchemaVersion, Generation: 1, IssuedAtUnixMilli: issued.UnixMilli(),
		Keys:          []TrustBundleKey{{KeyID: "plan-key-1", Algorithm: "ed25519", PublicKey: base64.StdEncoding.EncodeToString(public1), Status: "active"}},
		RevokedKeyIDs: []string{},
	})
	store := &TrustStore{}
	if err := store.Install(first); err != nil {
		t.Fatal(err)
	}
	if err := store.Install(first); err != nil {
		t.Fatalf("idempotent install: %v", err)
	}
	secondBundle := TrustBundle{
		SchemaVersion: TrustBundleSchemaVersion, Generation: 2, IssuedAtUnixMilli: issued.Add(time.Minute).UnixMilli(),
		Keys: []TrustBundleKey{
			{KeyID: "plan-key-1", Algorithm: "ed25519", PublicKey: base64.StdEncoding.EncodeToString(public1), Status: "retiring", TrustUntilUnixMilli: issued.Add(time.Hour).UnixMilli()},
			{KeyID: "plan-key-2", Algorithm: "ed25519", PublicKey: base64.StdEncoding.EncodeToString(public2), Status: "active"},
		},
		RevokedKeyIDs: []string{},
	}
	second := marshalTrustBundleForTest(t, secondBundle)
	if err := store.Install(second); err != nil || store.Generation() != 2 {
		t.Fatalf("second install generation=%d err=%v", store.Generation(), err)
	}
	if err := store.Install(first); !errors.Is(err, ErrTrustBundleRollback) {
		t.Fatalf("rollback error=%v", err)
	}
	secondBundle.Keys[1].PublicKey = base64.StdEncoding.EncodeToString(public3)
	if err := store.Install(marshalTrustBundleForTest(t, secondBundle)); !errors.Is(err, ErrTrustBundleConflict) {
		t.Fatalf("same-generation conflict error=%v", err)
	}
	third := marshalTrustBundleForTest(t, TrustBundle{
		SchemaVersion: TrustBundleSchemaVersion, Generation: 3, IssuedAtUnixMilli: issued.Add(2 * time.Hour).UnixMilli(),
		Keys:          []TrustBundleKey{{KeyID: "plan-key-2", Algorithm: "ed25519", PublicKey: base64.StdEncoding.EncodeToString(public2), Status: "active"}},
		RevokedKeyIDs: []string{"plan-key-1"},
	})
	if err := store.Install(third); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Resolve("plan-key-1"); !errors.Is(err, ErrTrustKeyUnavailable) {
		t.Fatalf("revoked key error=%v", err)
	}

	plan := Plan{SchemaVersion: 2, Revision: 7, CollectorID: "collector-a", NotBefore: issued, ExpiresAt: issued.Add(time.Hour)}
	planJSON, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	metadata := PlanSignatureMetadata{
		PlanID: "plan-a", CollectorID: plan.CollectorID,
		ConfigVersion: plan.Revision, PlanSchemaVersion: uint16(plan.SchemaVersion),
		SpecHash: trustTestPayloadHash(planJSON), SigningKeyID: "plan-key-2",
		NotBeforeUnixMilli: plan.NotBefore.UnixMilli(), ExpiresAtUnixMilli: plan.ExpiresAt.UnixMilli(),
	}
	payload, err := BuildPlanSignaturePayload(metadata)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := MarshalControlPlaneSignedPlan(metadata, planJSON, ed25519.Sign(private2, payload))
	if err != nil {
		t.Fatal(err)
	}
	registry, verified, err := store.VerifyControlPlanePlan(envelope, issued.Add(time.Minute))
	if err != nil || registry.Plan().Revision != 7 || verified.SigningKeyID != "plan-key-2" {
		t.Fatalf("verified=%+v registry=%v err=%v", verified, registry, err)
	}
}

func TestTrustStoreRejectsExpiredRetiringKeyFromStaleBundle(t *testing.T) {
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	issued := time.Date(2026, 9, 7, 1, 0, 0, 0, time.UTC)
	bundle := marshalTrustBundleForTest(t, TrustBundle{
		SchemaVersion: TrustBundleSchemaVersion, Generation: 1, IssuedAtUnixMilli: issued.UnixMilli(),
		Keys: []TrustBundleKey{{
			KeyID: "retiring-key", Algorithm: "ed25519", PublicKey: base64.StdEncoding.EncodeToString(publicKey),
			Status: "retiring", TrustUntilUnixMilli: issued.Add(time.Hour).UnixMilli(),
		}},
		RevokedKeyIDs: []string{},
	})
	store := &TrustStore{}
	if err := store.Install(bundle); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ResolveAt("retiring-key", issued.Add(time.Hour-time.Millisecond)); err != nil {
		t.Fatalf("key rejected before trust_until: %v", err)
	}
	if _, err := store.ResolveAt("retiring-key", issued.Add(time.Hour)); !errors.Is(err, ErrTrustKeyUnavailable) {
		t.Fatalf("key accepted at trust_until: %v", err)
	}
}

func trustTestPayloadHash(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func TestTrustBundleRequiresCanonicalSortedKeys(t *testing.T) {
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	bundle := TrustBundle{
		SchemaVersion: TrustBundleSchemaVersion, Generation: 1, IssuedAtUnixMilli: time.Now().UnixMilli(),
		Keys: []TrustBundleKey{
			{KeyID: "z-key", Algorithm: "ed25519", PublicKey: base64.StdEncoding.EncodeToString(publicKey), Status: "active"},
			{KeyID: "a-key", Algorithm: "ed25519", PublicKey: base64.StdEncoding.EncodeToString(publicKey), Status: "retiring", TrustUntilUnixMilli: time.Now().Add(time.Hour).UnixMilli()},
		},
	}
	if _, _, err := MarshalTrustBundle(bundle); err == nil {
		t.Fatal("unsorted bundle was accepted")
	}
}

func marshalTrustBundleForTest(t testing.TB, bundle TrustBundle) []byte {
	t.Helper()
	data, _, err := MarshalTrustBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

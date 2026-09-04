package flowcollect

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPlanTrustBundleSelectsKeyAndEnforcesRotationPolicy(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	plan := validPlan(now)
	envelope := signedTrustPlan(t, plan, "key-a", privateKey)
	active := marshalPlanTrustBundle(t, planTrustKey{
		ID: "key-a", PublicKey: base64.StdEncoding.EncodeToString(publicKey), Status: "active",
		NotBeforeUnixMilli: now.Add(-time.Hour).UnixMilli(), NotAfterUnixMilli: now.Add(2 * time.Hour).UnixMilli(),
	})
	registry, metadata, err := VerifyControlPlaneSignedPlanWithTrustBundle(envelope, active, now)
	if err != nil || registry.Plan().Revision != plan.Revision || metadata.SigningKeyID != "key-a" {
		t.Fatalf("registry=%+v metadata=%+v err=%v", registry, metadata, err)
	}

	unknown := marshalPlanTrustBundle(t, planTrustKey{
		ID: "key-b", PublicKey: base64.StdEncoding.EncodeToString(publicKey), Status: "active",
		NotBeforeUnixMilli: now.Add(-time.Hour).UnixMilli(), NotAfterUnixMilli: now.Add(2 * time.Hour).UnixMilli(),
	})
	if _, _, err := VerifyControlPlaneSignedPlanWithTrustBundle(envelope, unknown, now); !errors.Is(err, ErrPlanSigningKeyUnknown) {
		t.Fatalf("unknown key error=%v", err)
	}
	shortWindow := marshalPlanTrustBundle(t, planTrustKey{
		ID: "key-a", PublicKey: base64.StdEncoding.EncodeToString(publicKey), Status: "active",
		NotBeforeUnixMilli: now.Add(-time.Hour).UnixMilli(), NotAfterUnixMilli: now.Add(30 * time.Minute).UnixMilli(),
	})
	if _, _, err := VerifyControlPlaneSignedPlanWithTrustBundle(envelope, shortWindow, now); !errors.Is(err, ErrPlanSigningKeyNotAcceptable) {
		t.Fatalf("key validity window error=%v", err)
	}

	revoked := marshalPlanTrustBundle(t, planTrustKey{
		ID: "key-a", PublicKey: base64.StdEncoding.EncodeToString(publicKey), Status: "revoked",
		NotBeforeUnixMilli: now.Add(-time.Hour).UnixMilli(), NotAfterUnixMilli: now.Add(2 * time.Hour).UnixMilli(),
	})
	if _, _, err := VerifyControlPlaneSignedPlanWithTrustBundle(envelope, revoked, now); !errors.Is(err, ErrPlanSigningKeyRevoked) {
		t.Fatalf("revoked key error=%v", err)
	}
	if failure := classifyDeliveredPlanVerificationFailure(ErrPlanSigningKeyRevoked); failure.Code != "PLAN_SIGNING_KEY_REVOKED" || failure.Stage != "verify" {
		t.Fatalf("revoked delivery failure=%+v", failure)
	}

	retiring := marshalPlanTrustBundle(t, planTrustKey{
		ID: "key-a", PublicKey: base64.StdEncoding.EncodeToString(publicKey), Status: "retiring",
		NotBeforeUnixMilli: now.Add(-time.Hour).UnixMilli(), NotAfterUnixMilli: now.Add(2 * time.Hour).UnixMilli(), AcceptUntilUnixMilli: now.Add(-time.Second).UnixMilli(),
	})
	if _, _, err := VerifyControlPlaneSignedPlanWithTrustBundle(envelope, retiring, now); !errors.Is(err, ErrPlanSigningKeyNotAcceptable) {
		t.Fatalf("late retiring-key delivery error=%v", err)
	}
	if _, err := verifySignedPlanPayloadWithTrust(envelope, nil, retiring, planTrustExisting, now); err != nil {
		t.Fatalf("existing plan signed before retirement cutoff was rejected: %v", err)
	}
}

func TestPlanTrustBundleRejectsAmbiguousConfiguration(t *testing.T) {
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key := planTrustKey{ID: "duplicate", PublicKey: base64.StdEncoding.EncodeToString(publicKey), Status: "active", NotAfterUnixMilli: time.Now().Add(time.Hour).UnixMilli()}
	if _, err := parsePlanTrustBundle(marshalPlanTrustBundle(t, key, key)); err == nil {
		t.Fatal("duplicate trust key was accepted")
	}
	if _, err := parsePlanTrustBundle([]byte(`{"schema_version":1,"keys":[],"unexpected":true}`)); err == nil {
		t.Fatal("unknown trust bundle field was accepted")
	}
	key.Status = "retiring"
	if _, err := parsePlanTrustBundle(marshalPlanTrustBundle(t, key)); err == nil {
		t.Fatal("retiring key without acceptance cutoff was accepted")
	}
}

func TestPlanHistoryReloadsTrustBundleAndFailsClosedOnRevocation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	publicKey1, privateKey1, _ := ed25519.GenerateKey(rand.Reader)
	publicKey2, privateKey2, _ := ed25519.GenerateKey(rand.Reader)
	first := validPlan(now)
	second := validPlan(now)
	second.Revision = 2
	second.PartitionMapVersion = 2
	directory := t.TempDir()
	planPath := filepath.Join(directory, "plan.json")
	bundlePath := filepath.Join(directory, "trust.json")
	historyPath := filepath.Join(directory, "history")
	writeTestFile(t, planPath, signedTrustPlan(t, first, "key-one", privateKey1))
	writeTestFile(t, bundlePath, marshalPlanTrustBundle(t, activeTrustKey("key-one", publicKey1, now)))
	history, err := OpenPlanHistoryWithTrust(planPath, "", bundlePath, historyPath, 4, now)
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, planPath, signedTrustPlan(t, second, "key-two", privateKey2))
	_, unknownErr := history.Refresh(now.Add(time.Second), func() (map[uint64]struct{}, error) { return map[uint64]struct{}{}, nil }, func([]*Registry) error { return nil })
	var unknownFailure planRefreshFailure
	if !errors.Is(unknownErr, ErrPlanSigningKeyUnknown) || !errors.As(unknownErr, &unknownFailure) || unknownFailure.code != "PLAN_SIGNING_KEY_UNKNOWN" {
		t.Fatalf("untrusted rotation error=%v code=%q", unknownErr, unknownFailure.code)
	}
	writeTestFile(t, bundlePath, marshalPlanTrustBundle(t,
		planTrustKey{ID: "key-one", PublicKey: base64.StdEncoding.EncodeToString(publicKey1), Status: "retiring", NotBeforeUnixMilli: now.Add(-2 * time.Hour).UnixMilli(), NotAfterUnixMilli: now.Add(2 * time.Hour).UnixMilli(), AcceptUntilUnixMilli: now.Add(time.Minute).UnixMilli()},
		activeTrustKey("key-two", publicKey2, now),
	))
	result, err := history.Refresh(now.Add(2*time.Second), func() (map[uint64]struct{}, error) { return map[uint64]struct{}{}, nil }, func([]*Registry) error { return nil })
	if err != nil || !result.Changed || result.Revision != 2 {
		t.Fatalf("rotated refresh result=%+v err=%v", result, err)
	}
	writeTestFile(t, bundlePath, marshalPlanTrustBundle(t,
		planTrustKey{ID: "key-one", PublicKey: base64.StdEncoding.EncodeToString(publicKey1), Status: "retiring", NotBeforeUnixMilli: now.Add(-2 * time.Hour).UnixMilli(), NotAfterUnixMilli: now.Add(2 * time.Hour).UnixMilli(), AcceptUntilUnixMilli: now.Add(time.Minute).UnixMilli()},
		planTrustKey{ID: "key-two", PublicKey: base64.StdEncoding.EncodeToString(publicKey2), Status: "revoked", NotBeforeUnixMilli: now.Add(-2 * time.Hour).UnixMilli(), NotAfterUnixMilli: now.Add(2 * time.Hour).UnixMilli()},
	))
	_, refreshErr := history.Refresh(now.Add(3*time.Second), func() (map[uint64]struct{}, error) { return map[uint64]struct{}{}, nil }, func([]*Registry) error { return nil })
	var failure planRefreshFailure
	if !errors.Is(refreshErr, ErrPlanSigningKeyRevoked) || !errors.As(refreshErr, &failure) || failure.code != "PLAN_SIGNING_KEY_REVOKED" {
		t.Fatalf("revocation refresh error=%T %v code=%q", refreshErr, refreshErr, failure.code)
	}

	wal, err := OpenWAL(filepath.Join(directory, "wal"), first.CollectorID, testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	runner := &Runner{Registry: history.Active(), Plans: history}
	if err := runner.ActivateRegistry(history.Active()); err != nil {
		t.Fatal(err)
	}
	supervisor, err := NewPlanSupervisor(time.Hour, history, wal, runner, &Metrics{}, NewRuntimeState(), func([]*Registry) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Run(context.Background()); !errors.Is(err, ErrPlanSigningKeyRevoked) {
		t.Fatalf("supervisor revocation error=%v", err)
	}
}

func activeTrustKey(id string, publicKey ed25519.PublicKey, now time.Time) planTrustKey {
	return planTrustKey{
		ID: id, PublicKey: base64.StdEncoding.EncodeToString(publicKey), Status: "active",
		NotBeforeUnixMilli: now.Add(-2 * time.Hour).UnixMilli(), NotAfterUnixMilli: now.Add(2 * time.Hour).UnixMilli(),
	}
}

func marshalPlanTrustBundle(t *testing.T, keys ...planTrustKey) []byte {
	t.Helper()
	data, err := json.Marshal(planTrustBundle{SchemaVersion: 1, Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func signedTrustPlan(t *testing.T, plan Plan, signingKeyID string, privateKey ed25519.PrivateKey) []byte {
	t.Helper()
	plan.NotBefore = plan.NotBefore.UTC().Truncate(time.Millisecond)
	plan.ExpiresAt = plan.ExpiresAt.UTC().Truncate(time.Millisecond)
	payload, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	metadata := PlanSignatureMetadata{
		PlanID: "plan-trust-test", TenantID: "tenant-trust-test", CollectorID: plan.CollectorID,
		ConfigVersion: plan.Revision, PlanSchemaVersion: uint16(plan.SchemaVersion), SpecHash: hex.EncodeToString(digest[:]), SigningKeyID: signingKeyID,
		NotBeforeUnixMilli: plan.NotBefore.UnixMilli(), ExpiresAtUnixMilli: plan.ExpiresAt.UnixMilli(),
	}
	signingPayload, err := BuildPlanSignaturePayload(metadata)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := MarshalControlPlaneSignedPlan(metadata, payload, ed25519.Sign(privateKey, signingPayload))
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func writeTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

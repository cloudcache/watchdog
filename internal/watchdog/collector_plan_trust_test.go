package watchdog

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowplan"
)

type collectorPlanTrustRepositoryStub struct {
	key         CollectorPlanSigningKey
	publication CollectorPlanTrustBundlePublication
	err         error
}

func (r *collectorPlanTrustRepositoryStub) ActivateCollectorPlanSigningKey(context.Context, string, ed25519.PublicKey, time.Time) (CollectorPlanTrustBundlePublication, error) {
	return r.publication, r.err
}

func (r *collectorPlanTrustRepositoryStub) GetCollectorPlanSigningKey(context.Context, string) (CollectorPlanSigningKey, error) {
	return r.key, r.err
}

func (r *collectorPlanTrustRepositoryStub) GetCollectorPlanTrustBundle(context.Context) (CollectorPlanTrustBundlePublication, error) {
	return r.publication, r.err
}

func (r *collectorPlanTrustRepositoryStub) RevokeCollectorPlanSigningKey(context.Context, string, string, time.Time) (CollectorPlanTrustBundlePublication, error) {
	return r.publication, r.err
}

func (r *collectorPlanTrustRepositoryStub) ExpireRetiringCollectorPlanSigningKeys(context.Context, time.Time) (CollectorPlanTrustBundlePublication, bool, error) {
	return r.publication, false, r.err
}

func TestCollectorPlanSignerLoadsOwnerOnlyFileAndChecksActiveKey(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "collector-plan.key")
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(privateKey)), 0o644); err != nil {
		t.Fatal(err)
	}
	repository := &collectorPlanTrustRepositoryStub{key: CollectorPlanSigningKey{
		KeyID: "plan-key-1", PublicKey: publicKey, Status: CollectorPlanSigningKeyActive,
	}}
	config := CollectorPlanSigningConfig{KeyID: "plan-key-1", PrivateKeyFile: path}
	if _, err := LoadCollectorPlanSigner(config, repository); err == nil {
		t.Fatal("world-readable private key was accepted")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	signer, err := LoadCollectorPlanSigner(config, repository)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	canonical, hash, err := CanonicalCollectorPlanJSON([]byte(`{"schema_version":1}`))
	if err != nil {
		t.Fatal(err)
	}
	plan := CollectorPlanRevision{
		ID: "plan_signer_test_000001", TenantID: "tenant_signer_test", CollectorID: "collector_signer_test",
		ConfigVersion: 1, PlanSchemaVersion: 1, Status: CollectorPlanValidated,
		SpecJSON: canonical, SpecHash: hash, CreatedBy: "user_signer_test", UpdatedBy: "user_signer_test",
		NotBefore: now, ExpiresAt: now.Add(time.Hour),
	}
	signed, err := signer.Sign(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if signed.SigningKeyID != "plan-key-1" || len(signed.Signature) != ed25519.SignatureSize || ValidateNewCollectorPlanRevision(signed) != nil {
		t.Fatalf("signed plan = %+v", signed)
	}
	payloadSigner, ok := signer.(ControlPlanePayloadSigner)
	if !ok {
		t.Fatal("collector plan signer does not expose the shared control-plane payload capability")
	}
	payload := []byte(`{"schema_version":1}`)
	payloadSignature, err := payloadSigner.SignControlPlanePayload(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.Verify(publicKey, payload, payloadSignature) {
		t.Fatal("control-plane payload signature did not verify")
	}
	repository.key.Status = CollectorPlanSigningKeyRetiring
	if _, err := signer.Sign(context.Background(), plan); !errors.Is(err, ErrCollectorPlanSigningKeyUnavailable) {
		t.Fatalf("retiring signer error = %v", err)
	}
	if _, err := payloadSigner.SignControlPlanePayload(context.Background(), payload); !errors.Is(err, ErrCollectorPlanSigningKeyUnavailable) {
		t.Fatalf("retiring payload signer error = %v", err)
	}
	repository.key.Status = CollectorPlanSigningKeyActive
	repository.key.PublicKey = append(ed25519.PublicKey(nil), publicKey...)
	repository.key.PublicKey[0] ^= 0xff
	if _, err := signer.Sign(context.Background(), plan); !errors.Is(err, ErrCollectorPlanSigningKeyUnavailable) {
		t.Fatalf("mismatched signer error = %v", err)
	}
}

func TestCollectorPlanTrustBundleServiceRequiresMatchingMachineIdentity(t *testing.T) {
	repository := &collectorPlanTrustRepositoryStub{publication: CollectorPlanTrustBundlePublication{
		Generation: 4, BundleJSON: []byte(`{"schema_version":1}`), Checksum: "abcd",
	}}
	authenticator := &collectorEvidenceServiceAuthenticator{identity: CollectorMachineIdentity{
		TenantID: "tenant-trust", CollectorID: "collector-trust",
	}}
	service, err := NewCollectorPlanTrustBundleService(authenticator, repository)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := service.FetchTrustBundle(context.Background(), "collector-trust", CollectorMachineCredential{Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Generation != 4 || delivery.ETag != `"g4-abcd"` || string(delivery.Payload) != string(repository.publication.BundleJSON) {
		t.Fatalf("delivery = %+v", delivery)
	}
	authenticator.identity.CollectorID = "other-collector"
	if _, err := service.FetchTrustBundle(context.Background(), "collector-trust", CollectorMachineCredential{Token: "token"}); !errors.Is(err, ErrCollectorMachineUnauthorized) {
		t.Fatalf("mismatched identity error = %v", err)
	}
}

func TestCollectorPlanTrustMaintenanceRegistersExpiry(t *testing.T) {
	repository := &collectorPlanTrustRepositoryStub{}
	maintenance := NewCollectorPlanTrustMaintenance(repository, nil)
	if len(maintenance.tasks) != 1 || maintenance.tasks[0].Name != "collector_plan_trust_keys" || maintenance.tasks[0].Interval != time.Hour {
		t.Fatalf("trust maintenance tasks=%+v", maintenance.tasks)
	}
	if _, err := maintenance.tasks[0].Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	repository.err = ErrCollectorPlanTrustBundleUnavailable
	if _, err := maintenance.tasks[0].Run(context.Background()); err != nil {
		t.Fatalf("empty trust registry must be a no-op: %v", err)
	}
}

func TestMySQLCollectorPlanTrustRotationLifecycle(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_COLLECTOR_TRUST_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_COLLECTOR_TRUST_MYSQL_TEST_DSN to an isolated database")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := ApplyMySQLMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"collector_plan_signing_keys", "collector_plan_trust_state"} {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s is not empty; trust lifecycle test requires an isolated database", table)
		}
	}

	const (
		tenantID    = ID("tenant_trust_test")
		userID      = ID("user_trust_test")
		targetID    = ID("target_trust_test")
		collectorID = ID("collector_trust_test")
	)
	defer func() { _, _ = db.ExecContext(context.Background(), "DELETE FROM tenants WHERE id = ?", tenantID) }()
	if _, err := db.ExecContext(ctx, "INSERT INTO tenants (id, name, status) VALUES (?, 'Trust Test', 'active')", tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO users (id, tenant_id, email, name, status) VALUES (?, ?, 'trust-test@watchdog.local', 'Trust Test', 'active')", userID, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO targets (id, tenant_id, name, kind, host, status) VALUES (?, ?, 'Trust Target', 'network', '192.0.2.51', 'pending')", targetID, tenantID); err != nil {
		t.Fatal(err)
	}
	store := NewMySQLStore(db)
	if _, err := store.UpsertAgent(ctx, SNMPAgentConfig{
		ID: collectorID, TenantID: tenantID, TargetID: targetID, AgentType: AgentTypeSNMP,
		Mode: AgentModePull, TokenHash: "trust-test-token", Status: AgentStatusPending,
	}); err != nil {
		t.Fatal(err)
	}

	base := time.Now().UTC().Truncate(time.Millisecond)
	signer1 := newCollectorPlanSignerForTest(t, store, "trust-key-1")
	publication1, err := store.ActivateCollectorPlanSigningKey(ctx, signer1.KeyID(), signer1.PublicKey(), base)
	if err != nil || publication1.Generation != 1 {
		t.Fatalf("activate key 1 publication=%+v err=%v", publication1, err)
	}
	idempotent, err := store.ActivateCollectorPlanSigningKey(ctx, signer1.KeyID(), signer1.PublicKey(), base.Add(time.Second))
	if err != nil || idempotent.Generation != publication1.Generation || idempotent.Checksum != publication1.Checksum {
		t.Fatalf("idempotent activation publication=%+v err=%v", idempotent, err)
	}

	canonical, hash, err := CanonicalCollectorPlanJSON([]byte(`{"schema_version":1,"collector_id":"collector_trust_test"}`))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := signer1.Sign(ctx, CollectorPlanRevision{
		ID: "plan_trust_test_000001", TenantID: tenantID, CollectorID: collectorID,
		ConfigVersion: 1, PlanSchemaVersion: 1, Status: CollectorPlanValidated,
		SpecJSON: canonical, SpecHash: hash, CreatedBy: userID, UpdatedBy: userID,
		NotBefore: base, ExpiresAt: base.Add(2 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateCollectorPlanRevision(ctx, plan); err != nil {
		t.Fatal(err)
	}

	signer2 := newCollectorPlanSignerForTest(t, store, "trust-key-2")
	publication2, err := store.ActivateCollectorPlanSigningKey(ctx, signer2.KeyID(), signer2.PublicKey(), base.Add(time.Minute))
	if err != nil || publication2.Generation != 2 {
		t.Fatalf("rotate key 2 publication=%+v err=%v", publication2, err)
	}
	key1, err := store.GetCollectorPlanSigningKey(ctx, signer1.KeyID())
	if err != nil || key1.Status != CollectorPlanSigningKeyRetiring || !key1.TrustUntil.Equal(plan.ExpiresAt) {
		t.Fatalf("retiring key 1=%+v err=%v", key1, err)
	}
	if _, err := signer1.Sign(ctx, plan); !errors.Is(err, ErrCollectorPlanSigningKeyUnavailable) {
		t.Fatalf("retiring key signed a new plan: %v", err)
	}
	storeCache := &flowplan.TrustStore{}
	if err := storeCache.Install(publication2.BundleJSON); err != nil {
		t.Fatal(err)
	}
	if _, err := storeCache.Resolve("trust-key-1"); err != nil {
		t.Fatalf("retiring key missing from overlap bundle: %v", err)
	}

	publication3, err := store.RevokeCollectorPlanSigningKey(ctx, signer2.KeyID(), "operator emergency revoke", base.Add(2*time.Minute))
	if err != nil || publication3.Generation != 3 {
		t.Fatalf("revoke key 2 publication=%+v err=%v", publication3, err)
	}
	if err := storeCache.Install(publication3.BundleJSON); err != nil {
		t.Fatal(err)
	}
	if _, err := storeCache.Resolve("trust-key-2"); !errors.Is(err, flowplan.ErrTrustKeyUnavailable) {
		t.Fatalf("revoked key remained trusted: %v", err)
	}
	if _, err := signer2.Sign(ctx, plan); !errors.Is(err, ErrCollectorPlanSigningKeyUnavailable) {
		t.Fatalf("revoked key signed a new plan: %v", err)
	}

	publication4, changed, err := store.ExpireRetiringCollectorPlanSigningKeys(ctx, plan.ExpiresAt.Add(time.Millisecond))
	if err != nil || !changed || publication4.Generation != 4 {
		t.Fatalf("expire overlap publication=%+v changed=%v err=%v", publication4, changed, err)
	}
	if err := storeCache.Install(publication4.BundleJSON); err != nil {
		t.Fatal(err)
	}
	if _, err := storeCache.Resolve("trust-key-1"); !errors.Is(err, flowplan.ErrTrustKeyUnavailable) {
		t.Fatalf("expired retiring key remained trusted: %v", err)
	}
	if err := storeCache.Install(publication2.BundleJSON); !errors.Is(err, flowplan.ErrTrustBundleRollback) {
		t.Fatalf("trust generation rollback error=%v", err)
	}
	bundle, _, err := flowplan.ParseTrustBundle(publication4.BundleJSON)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Keys) != 0 || strings.Join(bundle.RevokedKeyIDs, ",") != "trust-key-1,trust-key-2" {
		t.Fatalf("terminal trust bundle=%+v", bundle)
	}
}

func newCollectorPlanSignerForTest(t testing.TB, repository CollectorPlanTrustRepository, keyID string) CollectorPlanSigner {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), keyID+".key")
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(privateKey)), 0o600); err != nil {
		t.Fatal(err)
	}
	signer, err := LoadCollectorPlanSigner(CollectorPlanSigningConfig{KeyID: keyID, PrivateKeyFile: path}, repository)
	if err != nil {
		t.Fatal(err)
	}
	return signer
}

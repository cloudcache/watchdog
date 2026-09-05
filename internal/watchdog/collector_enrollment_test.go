package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

type fakeEnrollmentRepository struct {
	secrets map[ID]struct {
		hash    string
		tenant  ID
		expired bool
		used    bool
	}
	consumed int
}

func (f *fakeEnrollmentRepository) CreateEnrollmentSecret(_ context.Context, secret CollectorEnrollmentSecret, secretHash string) (CollectorEnrollmentSecret, error) {
	secret.ID = "secret-fake-1"
	if f.secrets == nil {
		f.secrets = map[ID]struct {
			hash    string
			tenant  ID
			expired bool
			used    bool
		}{}
	}
	f.secrets[secret.ID] = struct {
		hash    string
		tenant  ID
		expired bool
		used    bool
	}{hash: secretHash, tenant: secret.TenantID}
	return secret, nil
}

func (f *fakeEnrollmentRepository) ListEnrollmentSecrets(context.Context, ID) ([]CollectorEnrollmentSecret, error) {
	return nil, nil
}

func (f *fakeEnrollmentRepository) DeleteEnrollmentSecret(context.Context, ID, ID) error {
	return nil
}

func (f *fakeEnrollmentRepository) ConsumeEnrollmentSecret(_ context.Context, secretID ID, verify func(string) bool, _ string) (CollectorEnrollmentResult, error) {
	record, ok := f.secrets[secretID]
	if !ok || record.expired || record.used || !verify(record.hash) {
		return CollectorEnrollmentResult{}, ErrEnrollmentSecretInvalid
	}
	f.consumed++
	return CollectorEnrollmentResult{CollectorID: "collector-fake-1", TenantID: record.tenant, ModuleKey: "flow", AgentType: "flow_collect"}, nil
}

func TestCollectorEnrollmentSecretMintAndExchange(t *testing.T) {
	repo := &fakeEnrollmentRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPrincipalAPIAuth(true), CollectorEnrollment: repo})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/collector-enrollment-secrets",
		strings.NewReader(`{"collector_name":"edge-1","module_key":"flow","agent_type":"flow_collect","mode":"listen"}`)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("mint status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var minted CollectorEnrollmentSecret
	if err := json.Unmarshal(rec.Body.Bytes(), &minted); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(minted.Secret, "wde_") || minted.ID == "" {
		t.Fatalf("minted secret = %+v", minted)
	}
	if stored := repo.secrets[minted.ID]; stored.hash == minted.Secret || stored.hash == "" {
		t.Fatal("stored value must be a hash of the secret")
	}

	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/collectors/enroll",
		strings.NewReader(`{"secret_id":"`+string(minted.ID)+`","secret":"`+minted.Secret+`"}`)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("enroll status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var result CollectorEnrollmentResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.CollectorID == "" || !strings.HasPrefix(result.Token, "wdc_") {
		t.Fatalf("enroll result = %+v", result)
	}

	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/collectors/enroll",
		strings.NewReader(`{"secret_id":"`+string(minted.ID)+`","secret":"wde_wrong"}`)))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong secret status = %d", rec.Code)
	}
}

// TestMySQLCollectorEnrollmentLifecycle proves single-use consumption on real
// MySQL: the exchange creates an active collector whose token authenticates,
// a replay of the same secret is rejected, a wrong secret leaves the row
// unused, and expiry or revocation close the window.
func TestMySQLCollectorEnrollmentLifecycle(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_COLLECTOR_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_COLLECTOR_MYSQL_TEST_DSN to run collector enrollment integration test")
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

	tenant := ID("tenant_enroll_test_001")
	if _, err := db.ExecContext(ctx, "DELETE FROM tenants WHERE id = ?", tenant); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), "DELETE FROM tenants WHERE id = ?", tenant)
	}()
	if _, err := db.ExecContext(ctx, "INSERT INTO tenants (id, name, status) VALUES (?, 'Enroll', 'active')", tenant); err != nil {
		t.Fatal(err)
	}

	store := NewMySQLStore(db)
	secretValue := "wde_enroll_test_secret"
	secret, err := store.CreateEnrollmentSecret(ctx, CollectorEnrollmentSecret{
		TenantID: tenant, ModuleKey: "flow", AgentType: "flow_collect", Mode: "listen",
		CollectorName: "edge-enroll-1", ExpiresAt: time.Now().UTC().Add(time.Hour), CreatedBy: "user_registry_admin",
	}, NewAgentTokenHash(secretValue))
	if err != nil {
		t.Fatal(err)
	}

	// Wrong secret value: rejected, row stays unused.
	verifyWrong := func(hash string) bool { return AgentTokenMatches("wde_wrong", hash) }
	if _, err := store.ConsumeEnrollmentSecret(ctx, secret.ID, verifyWrong, NewAgentTokenHash("wdc_x")); err == nil {
		t.Fatal("wrong secret must be rejected")
	}
	secrets, err := store.ListEnrollmentSecrets(ctx, tenant)
	if err != nil || len(secrets) != 1 || !secrets[0].UsedAt.IsZero() {
		t.Fatalf("secret after wrong attempt = %+v, err = %v", secrets, err)
	}

	// Correct exchange: collector row exists, is active, token authenticates.
	initialToken := "wdc_enroll_initial_token"
	verifyRight := func(hash string) bool { return AgentTokenMatches(secretValue, hash) }
	result, err := store.ConsumeEnrollmentSecret(ctx, secret.ID, verifyRight, NewAgentTokenHash(initialToken))
	if err != nil {
		t.Fatal(err)
	}
	if result.TenantID != tenant || result.ModuleKey != "flow" {
		t.Fatalf("enrollment result = %+v", result)
	}
	authenticator, err := NewMySQLCollectorMachineAuthenticator(db)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := authenticator.AuthenticateCollector(ctx, result.CollectorID, CollectorMachineCredential{Token: initialToken})
	if err != nil || identity.TenantID != tenant {
		t.Fatalf("enrolled collector auth identity=%+v err=%v", identity, err)
	}

	// Replay of the consumed secret mints nothing.
	if _, err := store.ConsumeEnrollmentSecret(ctx, secret.ID, verifyRight, NewAgentTokenHash("wdc_second")); err == nil {
		t.Fatal("replayed secret must be rejected")
	}
	var collectorCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM collector_agents WHERE tenant_id = ?", tenant).Scan(&collectorCount); err != nil || collectorCount != 1 {
		t.Fatalf("collector count after replay = %d, err = %v", collectorCount, err)
	}

	// Expired secret: rejected.
	expired, err := store.CreateEnrollmentSecret(ctx, CollectorEnrollmentSecret{
		TenantID: tenant, ModuleKey: "flow", AgentType: "flow_collect", Mode: "listen",
		CollectorName: "edge-enroll-expired", ExpiresAt: time.Now().UTC().Add(-time.Minute), CreatedBy: "user_registry_admin",
	}, NewAgentTokenHash(secretValue))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ConsumeEnrollmentSecret(ctx, expired.ID, verifyRight, NewAgentTokenHash("wdc_y")); err == nil {
		t.Fatal("expired secret must be rejected")
	}

	// Revoked (deleted) unused secret: gone; used secrets cannot be deleted.
	revocable, err := store.CreateEnrollmentSecret(ctx, CollectorEnrollmentSecret{
		TenantID: tenant, ModuleKey: "flow", AgentType: "flow_collect", Mode: "listen",
		CollectorName: "edge-enroll-revoked", ExpiresAt: time.Now().UTC().Add(time.Hour), CreatedBy: "user_registry_admin",
	}, NewAgentTokenHash(secretValue))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteEnrollmentSecret(ctx, tenant, revocable.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ConsumeEnrollmentSecret(ctx, revocable.ID, verifyRight, NewAgentTokenHash("wdc_z")); err == nil {
		t.Fatal("revoked secret must be rejected")
	}
	if err := store.DeleteEnrollmentSecret(ctx, tenant, secret.ID); err == nil {
		t.Fatal("used secret must not be deletable")
	}
}

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

type fakeCollectorCredentialRepository struct {
	state      CollectorCredentialState
	stageErr   error
	commitErr  error
	stagedHash string
	actor      ID
}

func (f *fakeCollectorCredentialRepository) GetCollectorCredentialState(context.Context, ID, ID) (CollectorCredentialState, error) {
	if f.state.CollectorID == "" {
		return CollectorCredentialState{}, sql.ErrNoRows
	}
	return f.state, nil
}

func (f *fakeCollectorCredentialRepository) StageCollectorCredential(_ context.Context, _, _ ID, _ uint64, pendingTokenHash, _ string, _ time.Time, actor ID) error {
	f.stagedHash = pendingTokenHash
	f.actor = actor
	return f.stageErr
}

func (f *fakeCollectorCredentialRepository) CommitCollectorCredential(context.Context, ID, ID, uint64, ID) error {
	return f.commitErr
}

func (f *fakeCollectorCredentialRepository) AbortCollectorCredential(context.Context, ID, ID, uint64, ID) error {
	return nil
}

func (f *fakeCollectorCredentialRepository) RevokeCollectorCredential(context.Context, ID, ID, uint64, ID) error {
	return nil
}

func TestCollectorCredentialRotateReturnsTokenOnce(t *testing.T) {
	repo := &fakeCollectorCredentialRepository{state: CollectorCredentialState{
		CollectorID: "collector-a", TenantID: "tenant-a", AuthType: "token", Status: "active", RowVersion: 3,
	}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPrincipalAPIAuth(true), CollectorCredentials: repo})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/collectors/collector-a/credentials/actions/rotate", nil)
	req.Header.Set("If-Match", `"3"`)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var rotation CollectorCredentialRotation
	if err := json.Unmarshal(rec.Body.Bytes(), &rotation); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(rotation.Token, "wdc_") || rotation.RowVersion != 4 {
		t.Fatalf("rotation = %+v", rotation)
	}
	if repo.stagedHash == "" || repo.stagedHash == rotation.Token {
		t.Fatalf("staged value must be a hash, got %q", repo.stagedHash)
	}
	if !AgentTokenMatches(rotation.Token, repo.stagedHash) {
		t.Fatal("staged hash does not match returned token")
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/collectors/collector-a/credentials/actions/rotate", nil)
	router.ServeHTTP(rec, req) // no If-Match
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing If-Match status = %d", rec.Code)
	}
}

func TestCollectorCredentialCommitConflict(t *testing.T) {
	repo := &fakeCollectorCredentialRepository{
		state: CollectorCredentialState{
			CollectorID: "collector-a", TenantID: "tenant-a", AuthType: "token", Status: "active", RowVersion: 3,
		},
		commitErr: ErrCollectorCredentialConflict,
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPrincipalAPIAuth(true), CollectorCredentials: repo})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/collectors/collector-a/credentials/actions/commit", nil)
	req.Header.Set("If-Match", `"2"`)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "version_conflict") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

// TestMySQLCollectorCredentialDualWindow proves the full rotation lifecycle on
// real MySQL: both credentials authenticate during the window, commit retires
// the old one, abort discards the staged one, expiry closes the window, and
// revoke fails closed.
func TestMySQLCollectorCredentialDualWindow(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_COLLECTOR_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_COLLECTOR_MYSQL_TEST_DSN to run collector credential integration test")
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

	tenant := ID("tenant_cred_rotate_test")
	collectorID := ID("agent_cred_rotate_01")
	if _, err := db.ExecContext(ctx, "DELETE FROM tenants WHERE id = ?", tenant); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), "DELETE FROM tenants WHERE id = ?", tenant)
	}()
	if _, err := db.ExecContext(ctx, "INSERT INTO tenants (id, name, status) VALUES (?, 'Cred Rotate', 'active')", tenant); err != nil {
		t.Fatal(err)
	}
	oldToken := "wdc_old_credential_token"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO collector_agents (
			id, tenant_id, module_key, name, agent_type, mode,
			status, observed_health, auth_type, token_hash, created_by, updated_by
		) VALUES (?, ?, 'flow', 'cred-rotate', 'flow_collect', 'listen',
			'active', 'unknown', 'token', ?, 'user_registry_admin', 'user_registry_admin')
	`, collectorID, tenant, NewAgentTokenHash(oldToken)); err != nil {
		t.Fatal(err)
	}

	store := NewMySQLStore(db)
	authenticator, err := NewMySQLCollectorMachineAuthenticator(db)
	if err != nil {
		t.Fatal(err)
	}
	authOK := func(token string) bool {
		_, err := authenticator.AuthenticateCollector(ctx, collectorID, CollectorMachineCredential{Token: token})
		return err == nil
	}
	if !authOK(oldToken) {
		t.Fatal("baseline token must authenticate")
	}

	state, err := store.GetCollectorCredentialState(ctx, tenant, collectorID)
	if err != nil {
		t.Fatal(err)
	}
	newToken := "wdc_new_credential_token"
	window := time.Now().UTC().Add(time.Hour)
	if err := store.StageCollectorCredential(ctx, tenant, collectorID, state.RowVersion, NewAgentTokenHash(newToken), "", window, "user_registry_admin"); err != nil {
		t.Fatal(err)
	}
	if !authOK(oldToken) || !authOK(newToken) {
		t.Fatal("both credentials must authenticate inside the rotation window")
	}
	if authOK("wdc_wrong_token") {
		t.Fatal("unknown token must not authenticate")
	}

	// Stale row_version conflicts instead of interleaving.
	if err := store.CommitCollectorCredential(ctx, tenant, collectorID, state.RowVersion, "user_registry_admin"); !errorsIsCollectorConflict(err) {
		t.Fatalf("stale commit error = %v", err)
	}
	state, err = store.GetCollectorCredentialState(ctx, tenant, collectorID)
	if err != nil || !state.HasPending {
		t.Fatalf("state after stage = %+v, err = %v", state, err)
	}
	if err := store.CommitCollectorCredential(ctx, tenant, collectorID, state.RowVersion, "user_registry_admin"); err != nil {
		t.Fatal(err)
	}
	if authOK(oldToken) {
		t.Fatal("old token must stop authenticating after commit")
	}
	if !authOK(newToken) {
		t.Fatal("new token must stay valid after commit")
	}

	// Abort path: staged credential is discarded, active stays.
	state, _ = store.GetCollectorCredentialState(ctx, tenant, collectorID)
	abortToken := "wdc_abort_candidate"
	if err := store.StageCollectorCredential(ctx, tenant, collectorID, state.RowVersion, NewAgentTokenHash(abortToken), "", time.Now().UTC().Add(time.Hour), "user_registry_admin"); err != nil {
		t.Fatal(err)
	}
	state, _ = store.GetCollectorCredentialState(ctx, tenant, collectorID)
	if err := store.AbortCollectorCredential(ctx, tenant, collectorID, state.RowVersion, "user_registry_admin"); err != nil {
		t.Fatal(err)
	}
	if authOK(abortToken) {
		t.Fatal("aborted credential must not authenticate")
	}
	if !authOK(newToken) {
		t.Fatal("active credential must survive an abort")
	}

	// Expired window: the staged credential no longer authenticates and the
	// rotation cannot be committed.
	state, _ = store.GetCollectorCredentialState(ctx, tenant, collectorID)
	expiredToken := "wdc_expired_candidate"
	if err := store.StageCollectorCredential(ctx, tenant, collectorID, state.RowVersion, NewAgentTokenHash(expiredToken), "", time.Now().UTC().Add(-time.Minute), "user_registry_admin"); err != nil {
		t.Fatal(err)
	}
	if authOK(expiredToken) {
		t.Fatal("expired staged credential must not authenticate")
	}
	state, _ = store.GetCollectorCredentialState(ctx, tenant, collectorID)
	if err := store.CommitCollectorCredential(ctx, tenant, collectorID, state.RowVersion, "user_registry_admin"); !errorsIsNoPending(err) {
		t.Fatalf("expired commit error = %v", err)
	}
	state, _ = store.GetCollectorCredentialState(ctx, tenant, collectorID)
	if err := store.AbortCollectorCredential(ctx, tenant, collectorID, state.RowVersion, "user_registry_admin"); err != nil {
		t.Fatal(err)
	}

	// Revoke fails closed for every credential.
	state, _ = store.GetCollectorCredentialState(ctx, tenant, collectorID)
	if err := store.RevokeCollectorCredential(ctx, tenant, collectorID, state.RowVersion, "user_registry_admin"); err != nil {
		t.Fatal(err)
	}
	if authOK(newToken) {
		t.Fatal("revoked collector must not authenticate")
	}
	state, _ = store.GetCollectorCredentialState(ctx, tenant, collectorID)
	if state.Status != "revoked" || state.HasPending {
		t.Fatalf("state after revoke = %+v", state)
	}
}

func errorsIsCollectorConflict(err error) bool {
	return err != nil && strings.Contains(err.Error(), "credential state changed")
}

func errorsIsNoPending(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no unexpired pending credential")
}

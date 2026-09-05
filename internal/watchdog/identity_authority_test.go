package watchdog

import (
	"context"
	"database/sql"
	"os"
	"testing"
)

// TestMySQLIdentityStoresNoCredentialMaterial locks in the single-authority
// guarantee: the tenant identity API is an authorization projection only, so
// creating and updating a user never writes credential material to MySQL.
// PocketBase remains the sole authentication authority.
func TestMySQLIdentityStoresNoCredentialMaterial(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_COLLECTOR_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_COLLECTOR_MYSQL_TEST_DSN to run identity authority test")
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
	tenant := ID("tenant_identity_authority")
	_, _ = db.Exec("DELETE FROM tenants WHERE id = ?", tenant)
	defer db.Exec("DELETE FROM tenants WHERE id = ?", tenant)
	if _, err := db.Exec("INSERT INTO tenants (id, name, status) VALUES (?, 'Identity Authority', 'active')", tenant); err != nil {
		t.Fatal(err)
	}

	store := NewMySQLStore(db)
	created, err := store.CreateUser(ctx, User{
		TenantID: tenant, Email: "projected@example.com", Name: "Projected", Status: "active",
		AuthProvider: "pocketbase", ExternalSubjectID: "pb_projected_1",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Migration 029 dropped password_hash entirely: MySQL has no column for
	// credential material at all, so create/update cannot introduce any.
	var passwordHashColumns int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = 'users' AND column_name = 'password_hash'
	`).Scan(&passwordHashColumns); err != nil {
		t.Fatal(err)
	}
	if passwordHashColumns != 0 {
		t.Fatal("users.password_hash column still exists; PB must be the sole authentication authority")
	}

	if _, err := store.UpdateUser(ctx, User{
		ID: created.ID, TenantID: tenant, Email: "projected2@example.com", Name: "Projected2",
		Status: "active", AuthProvider: "pocketbase", ExternalSubjectID: "pb_projected_1",
	}); err != nil {
		t.Fatal(err)
	}

	// The projection carries the external identity (PB record id), which is the
	// only link back to the authentication authority.
	var provider, subject string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(auth_provider, ''), COALESCE(external_subject_id, '') FROM users WHERE id = ?
	`, created.ID).Scan(&provider, &subject); err != nil {
		t.Fatal(err)
	}
	if provider != "pocketbase" || subject != "pb_projected_1" {
		t.Fatalf("external identity link = %q/%q, want pocketbase/pb_projected_1", provider, subject)
	}
}

// TestMySQLDisabledUserFailsAuthorizationProjection proves the disable model:
// setting the MySQL projection status to disabled immediately fails closed at
// the authorization boundary even though the PocketBase token is still valid.
func TestMySQLDisabledUserFailsAuthorizationProjection(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_COLLECTOR_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_COLLECTOR_MYSQL_TEST_DSN to run identity disable test")
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
	tenant := ID("tenant_identity_disable")
	_, _ = db.Exec("DELETE FROM tenants WHERE id = ?", tenant)
	defer db.Exec("DELETE FROM tenants WHERE id = ?", tenant)
	if _, err := db.Exec("INSERT INTO tenants (id, name, status) VALUES (?, 'Identity Disable', 'active')", tenant); err != nil {
		t.Fatal(err)
	}
	store := NewMySQLStore(db)
	created, err := store.CreateUser(ctx, User{
		TenantID: tenant, Email: "disable@example.com", Status: "active",
		AuthProvider: "pocketbase", ExternalSubjectID: "pb_disable_1",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Active projection resolves.
	projections, err := store.ListIdentityProjections(ctx, "pocketbase", "pb_disable_1")
	if err != nil || len(projections) != 1 {
		t.Fatalf("active projection = %d err=%v", len(projections), err)
	}

	// Disable revokes authorization while the external subject id (the PB link)
	// is untouched — the token still verifies at PB, but the projection is
	// disabled, so every tenant-scoped request fails closed.
	if err := store.DisableUser(ctx, tenant, created.ID); err != nil {
		t.Fatal(err)
	}
	projections, err = store.ListIdentityProjections(ctx, "pocketbase", "pb_disable_1")
	if err != nil {
		t.Fatal(err)
	}
	if len(projections) != 1 || projections[0].User.Status != "disabled" {
		t.Fatalf("disabled projection = %+v", projections)
	}
}

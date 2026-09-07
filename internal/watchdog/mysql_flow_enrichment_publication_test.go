package watchdog

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"
)

func TestFlowEnrichmentPublicationMigrationLifecycle(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	server, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if err := server.PingContext(ctx); err != nil {
		t.Skipf("mysql not reachable: %v", err)
	}

	schema := "watchdog_enrichment_pub_" + randomSchemaSuffix(t)
	createScratchSchema(ctx, t, server, schema)
	db := openScratchSchema(t, dsn, schema)
	defer db.Close()
	if _, err := ApplyMySQLMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}
	migrations, err := EmbeddedMySQLMigrations()
	if err != nil {
		t.Fatal(err)
	}
	var migration MySQLMigration
	for _, candidate := range migrations {
		if candidate.Version == "059" {
			migration = candidate
			break
		}
	}
	if migration.Version == "" {
		t.Fatal("migration 059 not found")
	}
	for replay := 0; replay < 2; replay++ {
		for index, statement := range SplitSQLStatements(migration.SQL) {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				t.Fatalf("replay 059 pass %d statement %d: %v", replay+1, index+1, err)
			}
		}
	}

	const (
		tenantID      = "tenant_enrichment_pub"
		workerID      = "worker_enrichment_pub"
		collectorID   = "collect_enrichment_pub"
		snapshotID    = "snapshot_enrichment_pub"
		publicationID = "publish_enrichment_pub"
		actorID       = "actor_enrichment_pub"
	)
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants (id, name, status) VALUES (?, 'Flow Enrichment', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO collector_agents (
			id, tenant_id, module_key, name, agent_type, mode, status,
			observed_health, auth_type, token_hash, created_by, updated_by
		) VALUES
			(?, ?, 'flow', 'worker', 'flow_worker', 'pull', 'active', 'healthy', 'token', 'worker-token', ?, ?),
			(?, ?, 'flow', 'collector', 'flow_collect', 'listen', 'active', 'healthy', 'token', 'collector-token', ?, ?)
	`, workerID, tenantID, actorID, actorID, collectorID, tenantID, actorID, actorID); err != nil {
		t.Fatal(err)
	}
	checksum := "sha256:" + strings.Repeat("a", 64)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dimension_snapshots (
			id, tenant_id, module_key, dimension_key, version, effective_from,
			object_ref, object_format, object_format_version, builder_version, build_job_id,
			checksum, draft_digest, source_manifest_version, source_manifest,
			bundle_schema_version, entry_count, approval_state
		) VALUES (?, ?, 'flow', 'address', 7, '2026-09-07 08:00:00.000',
			'dimension-snapshots/tenant_enrichment_pub/snapshot_enrichment_pub/bundle.wads',
			'wads', 1, 'watchdog-test', 'build_enrichment_pub', ?, ?, 0, JSON_ARRAY(), 1, 1, 'approved')
	`, snapshotID, tenantID, checksum, checksum); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_classification_profiles (
			tenant_id, home_province, home_city, home_isp_ids, home_asns,
			overseas_includes_hmt, internal_policy, transit_policy,
			definition_digest, created_by, updated_by
		) VALUES (?, 'CN-11', '1101', JSON_ARRAY(1, 2), JSON_ARRAY(4134, 4837),
			FALSE, 'count', 'drop', ?, ?, ?)
	`, tenantID, strings.Repeat("b", 64), actorID, actorID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_enrichment_publications (
			id, tenant_id, classification_version, effective_from, profile_row_version,
			dimension_snapshot_id, dimension_version, dimension_effective_from,
			dimension_object_ref, dimension_object_format, dimension_object_format_version,
			dimension_checksum, classification_schema_version, classification_object_ref,
			classification_checksum, signature_algorithm, signing_key_id, signature,
			signed_at, created_by
		) VALUES (?, ?, 1, '2026-09-07 09:00:00.000', 1,
			?, 7, '2026-09-07 08:00:00.000',
			'dimension-snapshots/tenant_enrichment_pub/snapshot_enrichment_pub/bundle.wads',
			'wads', 1, ?, 1,
			'dimension-snapshots/tenant_enrichment_pub/publish_enrichment_pub/classification.json',
			?, 'ed25519', 'test-key', REPEAT('s', 64), '2026-09-07 08:59:00.000', ?)
	`, publicationID, tenantID, snapshotID, checksum, checksum, actorID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_enrichment_publication_acks (
			tenant_id, publication_id, worker_id, boot_id, software_version,
			state, attempted_at, downloaded_at, installed_at
		) VALUES (?, ?, ?, 'boot-1', 'test', 'installed',
			'2026-09-07 09:01:00.000', '2026-09-07 09:01:00.000', '2026-09-07 09:01:01.000')
	`, tenantID, publicationID, workerID); err != nil {
		t.Fatal(err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_enrichment_publication_acks (
			tenant_id, publication_id, worker_id, boot_id, software_version,
			state, attempted_at, downloaded_at, installed_at
		) VALUES (?, ?, ?, 'boot-2', 'test', 'installed', NOW(3), NOW(3), NOW(3))
	`, tenantID, publicationID, collectorID); err != nil {
		t.Fatal(err)
	}
	// The FK deliberately proves registry ownership only. The repository and
	// machine authenticator enforce agent_type=flow_worker at the API boundary.

	if _, err := db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID); err != nil {
		t.Fatalf("tenant cascade: %v", err)
	}
	for _, table := range []string{
		"flow_classification_profiles",
		"flow_enrichment_publications",
		"flow_enrichment_publication_acks",
	} {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE tenant_id = ?", tenantID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s retained %d rows after tenant delete", table, count)
		}
	}
}

package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowvpn"
)

func TestMySQLFlowVPNRuleCRUDCanonicalCASAndTenantScope(t *testing.T) {
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

	schema := "watchdog_vpn_rule_repo_" + randomSchemaSuffix(t)
	createScratchSchema(ctx, t, server, schema)
	db := openScratchSchema(t, dsn, schema)
	defer db.Close()
	if _, err := ApplyMySQLMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}
	const tenantID = ID("tenant_vpn_rule_repo")
	if _, err := db.ExecContext(ctx, "INSERT INTO tenants (id, name, status) VALUES (?, 'VPN Rule Repository', 'active')", tenantID); err != nil {
		t.Fatal(err)
	}
	store := NewMySQLStore(db)
	ruleID, err := newIdentityID()
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.CreateVPNRule(ctx, VPNRule{
		ID: ruleID, TenantID: tenantID, Name: "  Risk Rule  ", Kind: VPNRuleKindPassive,
		Match:  flowvpn.Match{RemotePorts: []uint16{8443, 443, 443}, RemoteCountries: []string{"US"}},
		Effect: flowvpn.EffectScore, Weight: 25, Priority: 9, Status: VPNRuleStatusDraft,
		CreatedBy: "actor_vpn_rule_repo", UpdatedBy: "actor_vpn_rule_repo",
	})
	if err != nil || created.Name != "Risk Rule" || created.RowVersion != 1 || len(created.Match.RemotePorts) != 2 || created.Match.RemotePorts[0] != 443 {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	otherTenant, err := store.GetVPNRule(ctx, "tenant_other", created.ID)
	if !errors.Is(err, sql.ErrNoRows) || otherTenant.ID != "" {
		t.Fatalf("cross-tenant item=%+v err=%v", otherTenant, err)
	}
	items, total, err := store.ListVPNRules(ctx, tenantID, VPNRuleListFilter{
		Search: "risk", Kind: VPNRuleKindPassive, Effect: string(flowvpn.EffectScore),
		Status: VPNRuleStatusDraft, SortBy: "priority", Limit: 10,
	})
	if err != nil || total != 1 || len(items) != 1 || items[0].ID != created.ID {
		t.Fatalf("items=%+v total=%d err=%v", items, total, err)
	}
	created.Name = "Allow Rule"
	created.Kind = VPNRuleKindIntelligence
	created.Match = flowvpn.Match{RemoteASNs: []uint32{64512}}
	created.Effect = flowvpn.EffectAllow
	created.Weight = 0
	created.Status = VPNRuleStatusActive
	created.UpdatedBy = "actor_vpn_rule_update"
	updated, err := store.UpdateVPNRule(ctx, created, 1)
	if err != nil || updated.RowVersion != 2 || updated.Effect != flowvpn.EffectAllow || updated.CreatedBy != "actor_vpn_rule_repo" {
		t.Fatalf("updated=%+v err=%v", updated, err)
	}
	if _, err := store.UpdateVPNRule(ctx, created, 1); !errors.Is(err, ErrVPNRuleVersionConflict) {
		t.Fatalf("stale update err=%v", err)
	}
	duplicateID, _ := newIdentityID()
	created.ID, created.Name, created.CreatedBy = duplicateID, updated.Name, "actor_vpn_rule_repo"
	if _, err := store.CreateVPNRule(ctx, created); !errors.Is(err, ErrVPNRuleNameConflict) {
		t.Fatalf("duplicate name err=%v", err)
	}
	if err := store.DeleteVPNRule(ctx, tenantID, updated.ID, "actor_vpn_rule_delete", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetVPNRule(ctx, tenantID, updated.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("get deleted err=%v", err)
	}
	if err := store.DeleteVPNRule(ctx, tenantID, updated.ID, "actor_vpn_rule_delete", 2); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("delete deleted err=%v", err)
	}
}

func TestMySQLFlowVPNFindingPagingFacetsAndDispositionCAS(t *testing.T) {
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

	schema := "watchdog_vpn_repo_" + randomSchemaSuffix(t)
	createScratchSchema(ctx, t, server, schema)
	db := openScratchSchema(t, dsn, schema)
	defer db.Close()
	if _, err := ApplyMySQLMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}
	const tenantID = ID("tenant_vpn_repository")
	if _, err := db.ExecContext(ctx, "INSERT INTO tenants (id, name, status) VALUES (?, 'VPN Repository', 'active')", tenantID); err != nil {
		t.Fatal(err)
	}
	insertVPNFindingForTest(t, db, tenantID, "finding_vpn_repo_000001", "11", "2026-09-07 00:05:00.000", "192.0.2.1", 443, 70, "high", "review", "US")
	insertVPNFindingForTest(t, db, tenantID, "finding_vpn_repo_000002", "22", "2026-09-07 00:10:00.000", "2001:db8::2", 8443, 90, "critical", "probe_candidate", "SG")

	store := NewMySQLStore(db)
	items, total, err := store.ListVPNFindingsPage(ctx, tenantID, VPNFindingListFilter{
		From: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC), To: time.Date(2026, 9, 7, 1, 0, 0, 0, time.UTC),
		ColumnFilters: map[string][]string{"risk_level": {"high", "critical"}}, SortBy: "score", SortDirection: "DESC", Limit: 1,
	})
	if err != nil || total != 2 || len(items) != 1 || items[0].ID != "finding_vpn_repo_000002" || items[0].RemoteIP != "2001:db8::2" {
		t.Fatalf("sorted page=%+v total=%d err=%v", items, total, err)
	}
	items, total, err = store.ListVPNFindingsPage(ctx, tenantID, VPNFindingListFilter{Search: "192.0.2", Limit: 10})
	if err != nil || total != 1 || len(items) != 1 || items[0].RemoteIP != "192.0.2.1" || items[0].LocalIP != "10.0.0.1" {
		t.Fatalf("searched page=%+v total=%d err=%v", items, total, err)
	}
	exportItems, err := store.ListVPNFindingsForExport(ctx, tenantID, VPNFindingListFilter{
		From: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC), To: time.Date(2026, 9, 7, 1, 0, 0, 0, time.UTC),
		ColumnFilters: map[string][]string{"risk_level": {"high", "critical"}}, SortBy: "score", SortDirection: "DESC",
	}, 1)
	if err != nil || len(exportItems) != 2 || exportItems[0].ID != "finding_vpn_repo_000002" || exportItems[1].ID != "finding_vpn_repo_000001" {
		t.Fatalf("export items=%+v err=%v", exportItems, err)
	}

	facets, err := store.ListVPNFindingFacets(ctx, tenantID, VPNFindingFacetFilter{
		Field: "risk_level", ColumnFilters: map[string][]string{"risk_level": {"high"}, "disposition": {"unreviewed"}}, Limit: 10,
	})
	if err != nil || len(facets) != 2 || facets[0].Count != 1 || facets[1].Count != 1 {
		t.Fatalf("facets=%+v err=%v", facets, err)
	}
	ipFacets, err := store.ListVPNFindingFacets(ctx, tenantID, VPNFindingFacetFilter{Field: "local_ip", Limit: 10})
	if err != nil || len(ipFacets) != 1 || ipFacets[0].Value != "10.0.0.1" || ipFacets[0].Count != 2 {
		t.Fatalf("IP facets=%+v err=%v", ipFacets, err)
	}
	windowFacets, err := store.ListVPNFindingFacets(ctx, tenantID, VPNFindingFacetFilter{Field: "window_end", Limit: 10})
	if err != nil || len(windowFacets) != 2 {
		t.Fatalf("window facets=%+v err=%v", windowFacets, err)
	}
	for _, facet := range windowFacets {
		if _, err := time.Parse(time.RFC3339Nano, facet.Value); err != nil {
			t.Fatalf("window facet %q is not RFC3339: %v", facet.Value, err)
		}
	}

	updated, err := store.UpdateVPNFindingDisposition(ctx, tenantID, "finding_vpn_repo_000001", "confirmed", "validated", "actor_vpn_repo", 1)
	if err != nil || updated.RowVersion != 2 || updated.Disposition != "confirmed" || updated.DispositionBy != "actor_vpn_repo" || updated.DispositionAt == nil {
		t.Fatalf("updated=%+v err=%v", updated, err)
	}
	if _, err := store.UpdateVPNFindingDisposition(ctx, tenantID, updated.ID, "false_positive", "stale", "actor_vpn_repo", 1); !errors.Is(err, ErrVPNFindingConflict) {
		t.Fatalf("stale update error=%v", err)
	}
	cleared, err := store.UpdateVPNFindingDisposition(ctx, tenantID, updated.ID, "unreviewed", "", "actor_vpn_repo", 2)
	if err != nil || cleared.DispositionBy != "" || cleared.DispositionAt != nil || cleared.RowVersion != 3 {
		t.Fatalf("cleared=%+v err=%v", cleared, err)
	}
}

func insertVPNFindingForTest(t *testing.T, db *sql.DB, tenantID ID, id, hashByte, windowEnd, remoteIP string, remotePort, score int, risk, verdict, country string) {
	t.Helper()
	parsedRemote := netip.MustParseAddr(remoteIP).As16()
	if _, err := db.Exec(`
		INSERT INTO flow_vpn_findings (
			id, tenant_id, window_start, window_end, conversation_key, local_ip, remote_ip,
			primary_protocol, primary_local_port, primary_remote_port,
			local_to_remote_bytes, remote_to_local_bytes, flow_record_count, active_bucket_count, max_duration_ms,
			remote_asn, remote_country, remote_prefix_id, complete_ratio, score, risk_level, verdict,
			evidence_json, rule_set_version, dimension_snapshot_id, geo_version, classification_version,
			source_generation, generated_at, probe_result_json, expires_at
		) VALUES (
			?, ?, '2026-09-07 00:00:00.000', ?, UNHEX(REPEAT(?, 32)),
			UNHEX('00000000000000000000ffff0a000001'), ?,
			6, 54321, ?, 1000, 900, 2, 1, 60000,
			64512, ?, 'risk-prefix', 1, ?, ?, ?,
			'{"schema_version":1}', 'rules-v1', 'dimensions-v1', 'geo-v1', 1,
			1, '2026-09-07 00:11:00.000', '{}', '2026-10-07 00:00:00.000'
		)
	`, id, tenantID, windowEnd, hashByte, parsedRemote[:], remotePort, country, score, risk, verdict); err != nil {
		t.Fatalf("insert VPN finding %s: %v", id, err)
	}
}

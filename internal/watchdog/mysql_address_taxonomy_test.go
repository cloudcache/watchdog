package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
)

func TestNormalizeAddressTaxonomyInputs(t *testing.T) {
	operator, err := normalizeISPOperator(ISPOperator{Code: " telecom ", Name: " Telecom ", ASNs: []uint32{4134, 4809, 4134}})
	if err != nil {
		t.Fatal(err)
	}
	if operator.Code != "telecom" || operator.Name != "Telecom" || operator.Category != "other" || len(operator.ASNs) != 2 || operator.ASNs[0] != 4134 || operator.ASNs[1] != 4809 {
		t.Fatalf("operator = %#v", operator)
	}
	line, err := normalizeGeoLine(GeoLine{Code: " cn-telecom ", Name: " China Telecom ", GeoSelector: GeoLineSelector{GeoNodeIDs: []ID{"geo-b", "geo-a", "geo-b"}, Families: []int{6, 4, 6}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(line.GeoSelector.GeoNodeIDs) != 2 || line.GeoSelector.GeoNodeIDs[0] != "geo-a" || len(line.GeoSelector.Families) != 2 || line.GeoSelector.Families[0] != 4 {
		t.Fatalf("line = %#v", line)
	}
	if _, err := normalizeGeoLine(GeoLine{Code: "bad", Name: "Bad", GeoSelector: GeoLineSelector{Families: []int{5}}}); !errors.Is(err, ErrAddressTaxonomyInvalid) {
		t.Fatalf("invalid family error = %v", err)
	}
	set, err := normalizeAddressSet(AddressSet{
		Name: "mixed selector", MatchDirection: "in", Enabled: true,
		Selector: map[string]any{
			"labels":       map[string]any{"region": []any{"east", "east", "west"}},
			"geo_node_ids": []any{"geo-b", "geo-a", "geo-b"},
			"operator_ids": []any{"operator-a"},
			"asns":         []any{float64(4809), float64(4134), float64(4134)},
			"families":     []any{float64(6), float64(4), float64(6)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := set.Selector["geo_node_ids"].([]ID); len(got) != 2 || got[0] != "geo-a" || got[1] != "geo-b" {
		t.Fatalf("normalized geo ids = %#v", got)
	}
	if got := set.Selector["asns"].([]uint32); len(got) != 2 || got[0] != 4134 || got[1] != 4809 {
		t.Fatalf("normalized ASNs = %#v", got)
	}
	legacySelector, err := normalizeAddressSetSelector(map[string]any{"labels": map[string]any{"type": "customer"}})
	if err != nil {
		t.Fatal(err)
	}
	legacyLabels := legacySelector["labels"].(map[string]any)
	if got := legacyLabels["type"].([]string); len(got) != 1 || got[0] != "customer" {
		t.Fatalf("normalized legacy label = %#v", got)
	}
	if _, err := normalizeAddressSet(AddressSet{Name: "bad selector", Selector: map[string]any{"labels": map[string]any{"region": 7}}}); !errors.Is(err, ErrAddressTaxonomyInvalid) {
		t.Fatalf("invalid selector value error = %v", err)
	}
	if _, err := normalizeAddressSet(AddressSet{Name: "empty selector", Selector: map[string]any{"families": []any{}}}); !errors.Is(err, ErrAddressTaxonomyInvalid) {
		t.Fatalf("empty selector error = %v", err)
	}
}

func TestMySQLAddressTaxonomyCRUDHierarchyAndCAS(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_MYSQL_TEST_DSN to run address taxonomy integration test")
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
	const tenantID = ID("tenant_addr_taxonomy_test")
	_, _ = db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID)
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants (id, name, status) VALUES (?, 'Address taxonomy test', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID); err != nil {
			t.Errorf("tenant cascade cleanup: %v", err)
		}
	}()
	store := NewMySQLStore(db)

	continent, err := store.CreateGeoDictionary(ctx, GeoDictionaryNode{TenantID: tenantID, Kind: GeoKindContinent, Code: "AS", Name: "Asia", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	region, err := store.CreateGeoDictionary(ctx, GeoDictionaryNode{TenantID: tenantID, Kind: GeoKindRegion, Code: "east-asia", ParentID: continent.ID, Name: "East Asia", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	country, err := store.CreateGeoDictionary(ctx, GeoDictionaryNode{TenantID: tenantID, Kind: GeoKindCountry, Code: "CN", ParentID: region.ID, Name: "China", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateGeoDictionary(ctx, GeoDictionaryNode{TenantID: tenantID, Kind: GeoKindContinent, Code: "CN", ParentID: region.ID, Name: "Invalid"}); !errors.Is(err, ErrAddressTaxonomyInvalid) {
		t.Fatalf("invalid parent kind error = %v", err)
	}
	items, cursor, err := store.ListGeoDictionary(ctx, tenantID, AddressTaxonomyListFilter{Search: "a", Limit: 1})
	if err != nil || len(items) != 1 || cursor == "" {
		t.Fatalf("geo first page=%#v cursor=%q err=%v", items, cursor, err)
	}
	items, _, err = store.ListGeoDictionary(ctx, tenantID, AddressTaxonomyListFilter{Search: "a", Limit: 1, Cursor: cursor})
	if err != nil || len(items) != 1 {
		t.Fatalf("geo second page=%#v err=%v", items, err)
	}
	country.Name = "China mainland"
	country, err = store.UpdateGeoDictionary(ctx, country, country.RowVersion)
	if err != nil || country.RowVersion != 2 {
		t.Fatalf("geo update=%#v err=%v", country, err)
	}
	if _, err := store.UpdateGeoDictionary(ctx, country, 1); !errors.Is(err, ErrAddressTaxonomyConflict) {
		t.Fatalf("stale geo update error = %v", err)
	}

	operator, err := store.CreateISPOperator(ctx, ISPOperator{TenantID: tenantID, Code: "china-telecom", Name: "China Telecom", Category: "carrier", ASNs: []uint32{4809, 4134, 4134}, Enabled: true})
	if err != nil || len(operator.ASNs) != 2 {
		t.Fatalf("operator=%#v err=%v", operator, err)
	}
	operators, _, err := store.ListISPOperators(ctx, tenantID, AddressTaxonomyListFilter{Search: "telecom", Limit: 10})
	if err != nil || len(operators) != 1 {
		t.Fatalf("operators=%#v err=%v", operators, err)
	}

	set, err := store.UpsertAddressSet(ctx, AddressSet{ID: "addr-set-taxonomy-test", TenantID: tenantID, Name: "Customer prefixes", Selector: map[string]any{"labels": map[string]any{"type": "customer"}}, MatchDirection: "both", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	dependentSet, err := store.UpsertAddressSet(ctx, AddressSet{
		ID: "addr-set-dependent-test", TenantID: tenantID, Name: "Dependent prefixes",
		IncludeSetIDs: []string{set.ID}, MatchDirection: "in", Enabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	sets, _, err := store.ListAddressSetsPage(ctx, tenantID, AddressSetListFilter{Search: "Dependent", MatchDirection: "in", Enabled: func() *bool { value := false; return &value }(), Limit: 10})
	if err != nil || len(sets) != 1 || sets[0].ID != dependentSet.ID || sets[0].Enabled {
		t.Fatalf("address sets=%#v err=%v", sets, err)
	}
	set.IncludeSetIDs = []string{dependentSet.ID}
	if _, err := store.UpdateAddressSet(ctx, set, set.RowVersion); !errors.Is(err, ErrAddressTaxonomyCycle) {
		t.Fatalf("address set cycle error = %v", err)
	}
	dependentSet.Description = "updated"
	dependentSet, err = store.UpdateAddressSet(ctx, dependentSet, dependentSet.RowVersion)
	if err != nil || dependentSet.RowVersion != 2 || dependentSet.Description != "updated" {
		t.Fatalf("address set update=%#v err=%v", dependentSet, err)
	}
	if _, err := store.UpdateAddressSet(ctx, dependentSet, 1); !errors.Is(err, ErrAddressTaxonomyConflict) {
		t.Fatalf("stale address set update error = %v", err)
	}
	root, err := store.CreateGeoLine(ctx, GeoLine{TenantID: tenantID, Code: "cn", Name: "China", GeoSelector: GeoLineSelector{GeoNodeIDs: []ID{country.ID}}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	child, err := store.CreateGeoLine(ctx, GeoLine{TenantID: tenantID, ParentID: root.ID, Code: "cn-telecom", Name: "China Telecom", OperatorID: operator.ID, AddressSetID: set.ID, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	root.ParentID = child.ID
	if _, err := store.UpdateGeoLine(ctx, root, root.RowVersion); !errors.Is(err, ErrAddressTaxonomyCycle) {
		t.Fatalf("line cycle error = %v", err)
	}
	lines, _, err := store.ListGeoLines(ctx, tenantID, AddressTaxonomyListFilter{ParentID: root.ID, Limit: 10})
	if err != nil || len(lines) != 1 || lines[0].ID != child.ID {
		t.Fatalf("child lines=%#v err=%v", lines, err)
	}
	asn := uint32(4134)
	prefix, err := store.UpsertAddressPrefix(ctx, AddressPrefix{
		ID: "address-prefix-taxonomy-test", TenantID: tenantID, CIDR: "192.0.2.7/24",
		Labels: map[string]string{"type": "customer"}, GeoLeafID: country.ID, OperatorID: operator.ID,
		ASN: &asn, Source: "customer",
	})
	if err != nil || prefix.CIDR != "192.0.2.0/24" || prefix.Family != 4 || prefix.PrefixLength != 24 || prefix.RowVersion != 1 {
		t.Fatalf("prefix=%#v err=%v", prefix, err)
	}
	prefixes, _, err := store.ListAddressPrefixesPage(ctx, tenantID, AddressPrefixListFilter{
		Search: "customer", Family: 4, Source: "customer", GeoLeafID: country.ID, OperatorID: operator.ID, ASN: &asn, Limit: 10,
	})
	if err != nil || len(prefixes) != 1 || prefixes[0].ID != prefix.ID {
		t.Fatalf("prefixes=%#v err=%v", prefixes, err)
	}
	prefix.Labels["tier"] = "gold"
	prefix, err = store.UpdateAddressPrefix(ctx, prefix, prefix.RowVersion)
	if err != nil || prefix.RowVersion != 2 || prefix.Labels["tier"] != "gold" {
		t.Fatalf("prefix update=%#v err=%v", prefix, err)
	}
	if _, err := store.UpdateAddressPrefix(ctx, prefix, 1); !errors.Is(err, ErrAddressTaxonomyConflict) {
		t.Fatalf("stale prefix update error = %v", err)
	}
	if err := store.DeleteGeoDictionary(ctx, tenantID, country.ID, country.RowVersion); !errors.Is(err, ErrAddressTaxonomyInUse) {
		t.Fatalf("referenced geo deletion error = %v", err)
	}
	if err := store.DeleteISPOperator(ctx, tenantID, operator.ID, operator.RowVersion); !errors.Is(err, ErrAddressTaxonomyInUse) {
		t.Fatalf("referenced operator deletion error = %v", err)
	}
	if err := store.DeleteAddressSet(ctx, tenantID, set.ID); !errors.Is(err, ErrAddressTaxonomyInUse) {
		t.Fatalf("referenced address set deletion error = %v", err)
	}
}

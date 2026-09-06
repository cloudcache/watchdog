package watchdog

import (
	"context"
	"testing"
)

// TestMySQLAddressServerTables proves that Address Prefixes/Sets table filters,
// totals, numeric IP ordering, stable sorts, and offset pages are executed by
// MySQL rather than over the browser's current page.
func TestMySQLAddressServerTables(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()

	for _, prefix := range []AddressPrefix{
		{ID: "address-table-prefix-1", TenantID: tenant, CIDR: "192.0.2.0/24", Labels: map[string]string{"type": "customer"}, Source: "manual"},
		{ID: "address-table-prefix-2", TenantID: tenant, CIDR: "10.0.0.0/8", Labels: map[string]string{"type": "supplier"}, Source: "vendor"},
		{ID: "address-table-prefix-3", TenantID: tenant, CIDR: "2001:db8::/32", Labels: map[string]string{"type": "customer"}, Source: "manual"},
	} {
		if _, err := store.UpsertAddressPrefix(ctx, prefix); err != nil {
			t.Fatalf("seed prefix %s: %v", prefix.ID, err)
		}
	}

	prefixes, cursor, total, err := store.ListAddressPrefixesPage(ctx, tenant, AddressPrefixListFilter{
		Family: 4, Sort: "prefix_length", Desc: true, Limit: 1, Offset: 1, TableMode: true,
	})
	if err != nil || cursor != "" || total != 2 || len(prefixes) != 1 || prefixes[0].CIDR != "10.0.0.0/8" {
		t.Fatalf("prefix page=%v cursor=%q total=%d err=%v", addressPrefixCIDRs(prefixes), cursor, total, err)
	}
	prefixes, _, total, err = store.ListAddressPrefixesPage(ctx, tenant, AddressPrefixListFilter{
		Search: "customer", Source: "manual", Sort: "cidr", Limit: 10, TableMode: true,
	})
	if err != nil || total != 2 || len(prefixes) != 2 || prefixes[0].CIDR != "192.0.2.0/24" || prefixes[1].CIDR != "2001:db8::/32" {
		t.Fatalf("searched prefixes=%v total=%d err=%v", addressPrefixCIDRs(prefixes), total, err)
	}

	for _, set := range []AddressSet{
		{ID: "address-table-set-1", TenantID: tenant, Name: "Customer", ExplicitMembers: []string{"192.0.2.0/24"}, MatchDirection: "in", Enabled: true},
		{ID: "address-table-set-2", TenantID: tenant, Name: "Supplier", ExplicitMembers: []string{"10.0.0.0/8"}, MatchDirection: "out", Enabled: false},
	} {
		if _, err := store.UpsertAddressSet(ctx, set); err != nil {
			t.Fatalf("seed set %s: %v", set.ID, err)
		}
	}
	enabled := false
	sets, cursor, total, err := store.ListAddressSetsPage(ctx, tenant, AddressSetListFilter{
		Search: "supplier", MatchDirection: "out", Enabled: &enabled, Sort: "name", Desc: true, Limit: 25, TableMode: true,
	})
	if err != nil || cursor != "" || total != 1 || len(sets) != 1 || sets[0].Name != "Supplier" {
		t.Fatalf("set page=%v cursor=%q total=%d err=%v", addressSetNames(sets), cursor, total, err)
	}
}

func TestMySQLAddressTaxonomyServerTables(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	// Operator identity rows intentionally outlive ordinary operator deletion.
	// Remove the business rows before operationJobTestDB deletes the tenant so
	// this test does not depend on the separately tracked PLAT-DB-01 cascade fix.
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM isp_operators WHERE tenant_id = ?`, tenant)
	})

	continent, err := store.CreateGeoDictionary(ctx, GeoDictionaryNode{
		TenantID: tenant, Kind: GeoKindContinent, Code: "AS", Name: "Asia", SortOrder: 1, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	country, err := store.CreateGeoDictionary(ctx, GeoDictionaryNode{
		TenantID: tenant, Kind: GeoKindCountry, Code: "CN", Name: "China", ParentID: continent.ID, SortOrder: 2, Enabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	geos, cursor, total, err := store.ListGeoDictionary(ctx, tenant, AddressTaxonomyListFilter{
		Search: "china", Kind: GeoKindCountry, Enabled: boolPointer(false), Sort: "name", Desc: true, Limit: 25, TableMode: true,
	})
	if err != nil || cursor != "" || total != 1 || len(geos) != 1 || geos[0].ID != country.ID {
		t.Fatalf("geo page=%v cursor=%q total=%d err=%v", taxonomyGeoNames(geos), cursor, total, err)
	}

	for _, operator := range []ISPOperator{
		{TenantID: tenant, Code: "telecom", Name: "Telecom", Category: "carrier", ASNs: []uint32{4134}, SortOrder: 2, Enabled: true},
		{TenantID: tenant, Code: "mobile", Name: "Mobile", Category: "carrier", ASNs: []uint32{9808}, SortOrder: 1, Enabled: false},
	} {
		if _, err := store.CreateISPOperator(ctx, operator); err != nil {
			t.Fatalf("seed operator %s: %v", operator.Code, err)
		}
	}
	operators, cursor, total, err := store.ListISPOperators(ctx, tenant, AddressTaxonomyListFilter{
		Enabled: boolPointer(false), Sort: "flow_isp_id", Desc: true, Limit: 25, TableMode: true,
	})
	if err != nil || cursor != "" || total != 1 || len(operators) != 1 || operators[0].Code != "mobile" {
		t.Fatalf("operator page=%v cursor=%q total=%d err=%v", taxonomyOperatorNames(operators), cursor, total, err)
	}

	line, err := store.CreateGeoLine(ctx, GeoLine{
		TenantID: tenant, Code: "china", Name: "China line", GeoSelector: GeoLineSelector{GeoNodeIDs: []ID{country.ID}}, SortOrder: 3, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	lines, cursor, total, err := store.ListGeoLines(ctx, tenant, AddressTaxonomyListFilter{
		Search: "china", Enabled: boolPointer(true), Sort: "code", Limit: 25, TableMode: true,
	})
	if err != nil || cursor != "" || total != 1 || len(lines) != 1 || lines[0].ID != line.ID {
		t.Fatalf("line page=%v cursor=%q total=%d err=%v", taxonomyLineNames(lines), cursor, total, err)
	}
}

func boolPointer(value bool) *bool { return &value }

func taxonomyGeoNames(items []GeoDictionaryNode) []string {
	values := make([]string, 0, len(items))
	for _, item := range items {
		values = append(values, item.Name)
	}
	return values
}

func taxonomyOperatorNames(items []ISPOperator) []string {
	values := make([]string, 0, len(items))
	for _, item := range items {
		values = append(values, item.Name)
	}
	return values
}

func taxonomyLineNames(items []GeoLine) []string {
	values := make([]string, 0, len(items))
	for _, item := range items {
		values = append(values, item.Name)
	}
	return values
}

func addressPrefixCIDRs(prefixes []AddressPrefix) []string {
	values := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		values = append(values, prefix.CIDR)
	}
	return values
}

func addressSetNames(sets []AddressSet) []string {
	values := make([]string, 0, len(sets))
	for _, set := range sets {
		values = append(values, set.Name)
	}
	return values
}

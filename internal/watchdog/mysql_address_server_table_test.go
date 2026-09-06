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

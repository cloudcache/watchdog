package address

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// Pure unit port: taxonomy input normalization (operator/line/set selector).
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

// Integration port (de-tenanted): geo/operator/set/line/prefix CRUD, parent-kind
// validation, keyset + table pagination, row-version CAS, cycle rejection, and
// in-use delete guards.
func TestStoreAddressTaxonomyCRUDHierarchyAndCAS(t *testing.T) {
	db := addressTestDB(t)
	store := NewStore(db)
	ctx := context.Background()

	continent, err := store.CreateGeoDictionary(ctx, GeoDictionaryNode{Kind: GeoKindContinent, Code: "AS", Name: "Asia", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	region, err := store.CreateGeoDictionary(ctx, GeoDictionaryNode{Kind: GeoKindRegion, Code: "east-asia", ParentID: continent.ID, Name: "East Asia", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	country, err := store.CreateGeoDictionary(ctx, GeoDictionaryNode{Kind: GeoKindCountry, Code: "CN", ParentID: region.ID, Name: "China", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateGeoDictionary(ctx, GeoDictionaryNode{Kind: GeoKindContinent, Code: "CN", ParentID: region.ID, Name: "Invalid"}); !errors.Is(err, ErrAddressTaxonomyInvalid) {
		t.Fatalf("invalid parent kind error = %v", err)
	}
	items, cursor, _, err := store.ListGeoDictionary(ctx, AddressTaxonomyListFilter{Search: "a", Limit: 1})
	if err != nil || len(items) != 1 || cursor == "" {
		t.Fatalf("geo first page=%#v cursor=%q err=%v", items, cursor, err)
	}
	items, _, _, err = store.ListGeoDictionary(ctx, AddressTaxonomyListFilter{Search: "a", Limit: 1, Cursor: cursor})
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

	operator, err := store.CreateISPOperator(ctx, ISPOperator{Code: "china-telecom", Name: "China Telecom", Category: "carrier", ASNs: []uint32{4809, 4134, 4134}, Enabled: true})
	if err != nil || operator.FlowISPID == 0 || len(operator.ASNs) != 2 {
		t.Fatalf("operator=%#v err=%v", operator, err)
	}
	operators, _, _, err := store.ListISPOperators(ctx, AddressTaxonomyListFilter{Search: "telecom", Limit: 10})
	if err != nil || len(operators) != 1 {
		t.Fatalf("operators=%#v err=%v", operators, err)
	}

	set, err := store.UpsertAddressSet(ctx, AddressSet{ID: "addr-set-taxonomy-test", Name: "Customer prefixes", Selector: map[string]any{"labels": map[string]any{"type": "customer"}}, MatchDirection: "both", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	dependentSet, err := store.UpsertAddressSet(ctx, AddressSet{
		ID: "addr-set-dependent-test", Name: "Dependent prefixes",
		IncludeSetIDs: []string{set.ID}, MatchDirection: "in", Enabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	disabled := false
	sets, _, setTotal, err := store.ListAddressSetsPage(ctx, AddressSetListFilter{Search: "Dependent", MatchDirection: "in", Enabled: &disabled, Limit: 10})
	if err != nil || len(sets) != 1 || setTotal != 1 || sets[0].ID != dependentSet.ID || sets[0].Enabled {
		t.Fatalf("address sets=%#v total=%d err=%v", sets, setTotal, err)
	}
	sets, cursor, setTotal, err = store.ListAddressSetsPage(ctx, AddressSetListFilter{Sort: "enabled", Desc: true, Limit: 1, Offset: 1, TableMode: true})
	if err != nil || cursor != "" || len(sets) != 1 || setTotal != 2 || sets[0].Enabled {
		t.Fatalf("address set table page=%#v cursor=%q total=%d err=%v", sets, cursor, setTotal, err)
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
	root, err := store.CreateGeoLine(ctx, GeoLine{Code: "cn", Name: "China", GeoSelector: GeoLineSelector{GeoNodeIDs: []ID{country.ID}}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	child, err := store.CreateGeoLine(ctx, GeoLine{ParentID: root.ID, Code: "cn-telecom", Name: "China Telecom", OperatorID: operator.ID, AddressSetID: set.ID, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	root.ParentID = child.ID
	if _, err := store.UpdateGeoLine(ctx, root, root.RowVersion); !errors.Is(err, ErrAddressTaxonomyCycle) {
		t.Fatalf("line cycle error = %v", err)
	}
	lines, _, _, err := store.ListGeoLines(ctx, AddressTaxonomyListFilter{ParentID: root.ID, Limit: 10})
	if err != nil || len(lines) != 1 || lines[0].ID != child.ID {
		t.Fatalf("child lines=%#v err=%v", lines, err)
	}
	asn := uint32(4134)
	prefix, err := store.UpsertAddressPrefix(ctx, AddressPrefix{
		ID: "address-prefix-taxonomy-test", CIDR: "192.0.2.7/24",
		Labels: map[string]string{"type": "customer"}, GeoLeafID: country.ID, OperatorID: operator.ID,
		ASN: &asn, Source: "customer",
	})
	if err != nil || prefix.CIDR != "192.0.2.0/24" || prefix.Family != 4 || prefix.PrefixLength != 24 || prefix.RowVersion != 1 {
		t.Fatalf("prefix=%#v err=%v", prefix, err)
	}
	prefixes, _, prefixTotal, err := store.ListAddressPrefixesPage(ctx, AddressPrefixListFilter{
		Search: "customer", Family: 4, Source: "customer", GeoLeafID: country.ID, OperatorID: operator.ID, ASN: &asn, Limit: 10,
	})
	if err != nil || len(prefixes) != 1 || prefixTotal != 1 || prefixes[0].ID != prefix.ID {
		t.Fatalf("prefixes=%#v total=%d err=%v", prefixes, prefixTotal, err)
	}
	prefixes, cursor, prefixTotal, err = store.ListAddressPrefixesPage(ctx, AddressPrefixListFilter{
		Family: 4, Sort: "prefix_length", Desc: true, Limit: 1, TableMode: true,
	})
	if err != nil || cursor != "" || len(prefixes) != 1 || prefixTotal != 1 || prefixes[0].ID != prefix.ID {
		t.Fatalf("prefix table page=%#v cursor=%q total=%d err=%v", prefixes, cursor, prefixTotal, err)
	}
	prefix.Labels["tier"] = "gold"
	prefix, err = store.UpdateAddressPrefix(ctx, prefix, prefix.RowVersion)
	if err != nil || prefix.RowVersion != 2 || prefix.Labels["tier"] != "gold" {
		t.Fatalf("prefix update=%#v err=%v", prefix, err)
	}
	if _, err := store.UpdateAddressPrefix(ctx, prefix, 1); !errors.Is(err, ErrAddressTaxonomyConflict) {
		t.Fatalf("stale prefix update error = %v", err)
	}
	if err := store.DeleteGeoDictionary(ctx, country.ID, country.RowVersion); !errors.Is(err, ErrAddressTaxonomyInUse) {
		t.Fatalf("referenced geo deletion error = %v", err)
	}
	if err := store.DeleteISPOperator(ctx, operator.ID, operator.RowVersion); !errors.Is(err, ErrAddressTaxonomyInUse) {
		t.Fatalf("referenced operator deletion error = %v", err)
	}
	if err := store.DeleteAddressSet(ctx, set.ID); !errors.Is(err, ErrAddressTaxonomyInUse) {
		t.Fatalf("referenced address set deletion error = %v", err)
	}
}

// Integration port (de-tenanted): concurrent operator creation allocates unique
// Flow-ISP ids from the singleton sequence, and exhaustion is reported.
func TestStoreISPOperatorFlowIdentityConcurrentAllocation(t *testing.T) {
	db := addressTestDB(t)
	store := NewStore(db)
	ctx := context.Background()

	const workers = 24
	results := make(chan ISPOperator, workers)
	errResults := make(chan error, workers)
	var group sync.WaitGroup
	for index := 0; index < workers; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			operator, err := store.CreateISPOperator(ctx, ISPOperator{
				Code: fmt.Sprintf("operator-%02d", index), Name: fmt.Sprintf("Operator %02d", index), Enabled: true,
			})
			if err != nil {
				errResults <- err
				return
			}
			results <- operator
		}(index)
	}
	group.Wait()
	close(results)
	close(errResults)
	for err := range errResults {
		t.Errorf("create operator: %v", err)
	}
	seen := make(map[uint16]bool, workers)
	for operator := range results {
		if operator.FlowISPID == 0 || seen[operator.FlowISPID] {
			t.Errorf("invalid or duplicate Flow ISP id: %d", operator.FlowISPID)
		}
		seen[operator.FlowISPID] = true
	}
	if len(seen) != workers {
		t.Fatalf("allocated %d unique ids, want %d", len(seen), workers)
	}
	if _, err := db.ExecContext(ctx, `UPDATE isp_operator_flow_id_sequences SET next_flow_isp_id = 65536 WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateISPOperator(ctx, ISPOperator{Code: "operator-exhausted", Name: "Operator exhausted", Enabled: true}); !errors.Is(err, ErrAddressTaxonomyInvalid) {
		t.Fatalf("exhausted allocation error = %v, want ErrAddressTaxonomyInvalid", err)
	}
}

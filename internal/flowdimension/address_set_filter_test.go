package flowdimension

import "testing"

func TestAddressSetFilterBooleanSemantics(t *testing.T) {
	filter, err := CompileAddressSetFilter(AddressSetFilter{
		IncludeAny: []string{"set-b", "set-a", "set-a"},
		IncludeAll: []string{"set-c"},
		ExcludeAny: []string{"set-d"},
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		ids  []string
		want bool
	}{
		{name: "union-and-intersection", ids: []string{"set-a", "set-c"}, want: true},
		{name: "other-union-member", ids: []string{"set-b", "set-c"}, want: true},
		{name: "missing-all", ids: []string{"set-a"}, want: false},
		{name: "missing-any", ids: []string{"set-c"}, want: false},
		{name: "excluded", ids: []string{"set-a", "set-c", "set-d"}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if actual := filter.MatchesIDs(test.ids); actual != test.want {
				t.Fatalf("MatchesIDs(%v) = %v, want %v", test.ids, actual, test.want)
			}
		})
	}
}

func TestAddressSetUnionCountsEachFactOnce(t *testing.T) {
	filter, err := CompileAddressSetFilter(AddressSetFilter{IncludeAny: []string{"set-a", "set-b"}})
	if err != nil {
		t.Fatal(err)
	}
	facts := []struct {
		ids   []string
		bytes uint64
	}{
		{ids: []string{"set-a"}, bytes: 10},
		{ids: []string{"set-a", "set-b"}, bytes: 20},
		{ids: []string{"set-b"}, bytes: 30},
		{ids: []string{"set-c"}, bytes: 40},
	}
	var total uint64
	for _, fact := range facts {
		if filter.MatchesIDs(fact.ids) {
			total += fact.bytes
		}
	}
	if total != 60 {
		t.Fatalf("union total = %d, want 60; overlapping fact must be counted once", total)
	}
}

func TestAddressSetFilterValidation(t *testing.T) {
	if _, err := CompileAddressSetFilter(AddressSetFilter{IncludeAny: []string{"bad id"}}); err == nil {
		t.Fatal("expected invalid ID to be rejected")
	}
	tooMany := make([]string, maxAddressSetFilterIDs+1)
	for index := range tooMany {
		tooMany[index] = "set-a"
	}
	if _, err := CompileAddressSetFilter(AddressSetFilter{IncludeAny: tooMany}); err == nil {
		t.Fatal("expected filter ID limit to be enforced before deduplication")
	}
	combined := AddressSetFilter{IncludeAny: make([]string, 100), IncludeAll: make([]string, 100), ExcludeAny: make([]string, 57)}
	for index := range combined.IncludeAny {
		combined.IncludeAny[index] = "set-a"
	}
	for index := range combined.IncludeAll {
		combined.IncludeAll[index] = "set-b"
	}
	for index := range combined.ExcludeAny {
		combined.ExcludeAny[index] = "set-c"
	}
	if _, err := CompileAddressSetFilter(combined); err == nil {
		t.Fatal("expected combined raw ID limit to be enforced before deduplication")
	}
}

func TestCompiledAddressSetFilterCanonicalIsImmutable(t *testing.T) {
	compiled, err := CompileAddressSetFilter(AddressSetFilter{
		IncludeAny: []string{"set-b", "set-a", "set-a"}, ExcludeAny: []string{"set-c"},
	})
	if err != nil {
		t.Fatal(err)
	}
	canonical := compiled.Canonical()
	if len(canonical.IncludeAny) != 2 || canonical.IncludeAny[0] != "set-a" || canonical.IncludeAny[1] != "set-b" {
		t.Fatalf("canonical=%+v", canonical)
	}
	canonical.IncludeAny[0] = "mutated"
	if compiled.Canonical().IncludeAny[0] != "set-a" {
		t.Fatal("canonical result mutated the compiled filter")
	}
}

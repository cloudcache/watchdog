package flowdimension

import (
	"net/netip"
	"strings"
	"testing"
)

func TestAddressSetCompilationAppliesSetAlgebraAtPrefixBoundaries(t *testing.T) {
	bundle := testBundle("snapshot-set-algebra", 1, testMinute(12, 0))
	bundle.Prefixes = []PrefixDefinition{
		{ID: "local", CIDR: "10.0.0.0/8", Labels: map[string]string{"flow": "local", "business": "core"}},
		{ID: "cn", CIDR: "203.0.0.0/16", Labels: map[string]string{"geo.country": "CN"}},
	}
	bundle.AddressSets = []AddressSetDefinition{
		{ID: "set-a", Members: []string{"203.0.113.0/24", "2001:db8::/32"}, ExcludeMembers: []string{"203.0.113.128/25", "2001:db8:1::/48"}, MatchDirection: "both", Enabled: true},
		{ID: "set-b", IncludeSetIDs: []string{"set-a"}, MatchDirection: "both", Enabled: true},
		{ID: "set-c", Selector: LabelSelector{Labels: map[string][]string{"geo.country": {"CN"}}}, ExcludeSetIDs: []string{"set-a"}, MatchDirection: "both", Enabled: true},
		{ID: "set-cn", Selector: LabelSelector{Labels: map[string][]string{"geo.country": {"CN"}}}, MatchDirection: "both", Enabled: true},
		{ID: "set-external", Members: []string{"198.51.100.0/24"}, MatchDirection: "both", Enabled: true},
	}

	snapshot, err := CompileBundle(bundle, CompileLimits{})
	if err != nil {
		t.Fatalf("CompileBundle: %v", err)
	}

	assertRemoteSets(t, snapshot, "203.0.113.10", []string{"set-a", "set-b", "set-cn"})
	assertRemoteSets(t, snapshot, "203.0.113.200", []string{"set-c", "set-cn"})
	assertRemoteSets(t, snapshot, "203.0.114.10", []string{"set-c", "set-cn"})
	assertRemoteSets(t, snapshot, "198.51.100.10", []string{"set-external"})
	assertRemoteSets(t, snapshot, "2001:db8::1", []string{"set-a", "set-b"})
	assertRemoteSets(t, snapshot, "2001:db8:1::1", nil)

	classified := snapshot.ClassifyEndpoints(netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr("198.51.100.10"))
	if classified.Remote.PrefixID != UnassignedDimensionID {
		t.Fatalf("explicit address-set member unexpectedly requires a primary prefix: %+v", classified.Remote)
	}
	if snapshot.Metadata().MaxAddressSetsPerRecord != 3 {
		t.Fatalf("MaxAddressSetsPerRecord = %d, want 3", snapshot.Metadata().MaxAddressSetsPerRecord)
	}
}

func TestAddressSetCompilationRejectsInvalidAlgebra(t *testing.T) {
	deepChain := make([]AddressSetDefinition, maxAddressSetDependencyDepth+1)
	for index := range deepChain {
		deepChain[index] = AddressSetDefinition{ID: "depth-" + strings.Repeat("x", index+1), Enabled: true}
	}
	for index := range deepChain {
		if index == len(deepChain)-1 {
			deepChain[index].Members = []string{"192.0.2.0/24"}
		} else {
			deepChain[index].IncludeSetIDs = []string{deepChain[index+1].ID}
		}
	}
	tests := []struct {
		name string
		sets []AddressSetDefinition
		want string
	}{
		{
			name: "cycle",
			sets: []AddressSetDefinition{
				{ID: "a", IncludeSetIDs: []string{"b"}, Enabled: true},
				{ID: "b", IncludeSetIDs: []string{"a"}, Enabled: true},
			},
			want: "dependency cycle",
		},
		{
			name: "missing-reference",
			sets: []AddressSetDefinition{{ID: "a", IncludeSetIDs: []string{"missing"}, Enabled: true}},
			want: "missing or disabled",
		},
		{
			name: "disabled-reference",
			sets: []AddressSetDefinition{
				{ID: "a", IncludeSetIDs: []string{"b"}, Enabled: true},
				{ID: "b", Members: []string{"192.0.2.0/24"}, Enabled: false},
			},
			want: "missing or disabled",
		},
		{
			name: "non-canonical-member",
			sets: []AddressSetDefinition{{ID: "a", Members: []string{"192.0.2.1/24"}, Enabled: true}},
			want: "canonical IPv4 or IPv6 CIDRs",
		},
		{
			name: "exclude-only",
			sets: []AddressSetDefinition{{ID: "a", ExcludeMembers: []string{"192.0.2.0/24"}, Enabled: true}},
			want: "unless a member or included set is present",
		},
		{
			name: "dependency-depth",
			sets: deepChain,
			want: "dependency depth exceeds limit",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bundle := testBundle("snapshot-invalid-set", 1, testMinute(12, 0))
			bundle.AddressSets = test.sets
			_, err := CompileBundle(bundle, CompileLimits{})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("CompileBundle error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestAddressSetCompilationEnforcesExpandedRecordLimit(t *testing.T) {
	bundle := testBundle("snapshot-set-limit", 1, testMinute(12, 0))
	bundle.AddressSets = []AddressSetDefinition{
		{ID: "local-set", Members: []string{"10.0.0.0/8"}, Enabled: true},
		{ID: "remote-set", Members: []string{"203.0.113.0/24"}, Enabled: true},
	}
	_, err := CompileBundle(bundle, CompileLimits{MaxAddressSetsPerRecord: 1})
	if err == nil || !strings.Contains(err.Error(), "address-set expansion 2") {
		t.Fatalf("CompileBundle error = %v, want combined expansion limit error", err)
	}
}

func TestAddressSetCompilationSupportsFiniteIPv4AndIPv6Universes(t *testing.T) {
	bundle := testBundle("snapshot-universe", 1, testMinute(12, 0))
	bundle.Prefixes = []PrefixDefinition{
		{ID: "local", CIDR: "10.0.0.0/8", Labels: map[string]string{"flow": "local"}},
	}
	bundle.AddressSets = []AddressSetDefinition{
		{ID: "universe-v4", Members: []string{"0.0.0.0/0"}, ExcludeMembers: []string{"192.0.2.0/24"}, Enabled: true},
		{ID: "universe-v6", Members: []string{"::/0"}, ExcludeMembers: []string{"2001:db8:dead::/48"}, Enabled: true},
	}
	snapshot, err := CompileBundle(bundle, CompileLimits{})
	if err != nil {
		t.Fatalf("CompileBundle: %v", err)
	}
	assertRemoteSets(t, snapshot, "203.0.113.1", []string{"universe-v4"})
	assertRemoteSets(t, snapshot, "192.0.2.1", nil)
	assertRemoteSets(t, snapshot, "2001:db8::1", []string{"universe-v6"})
	assertRemoteSets(t, snapshot, "2001:db8:dead::1", nil)
}

func assertRemoteSets(t *testing.T, snapshot *CompiledSnapshot, remote string, want []string) {
	t.Helper()
	classified := snapshot.ClassifyEndpoints(netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr(remote))
	if classified.Direction != DirectionOut {
		t.Fatalf("direction for %s = %s, want out", remote, classified.Direction)
	}
	got := classified.Remote.AddressSets.IDs()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("address sets for %s = %v, want %v", remote, got, want)
	}
}

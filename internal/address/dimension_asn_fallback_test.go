package address

import "testing"

// TestBuildASNSupplierIndex covers the ASN->operator fallback index used when a
// prefix carries an ASN but no operator_name: single-operator ASNs map, an ASN
// claimed by more than one operator is left unmapped, and ASN 0 / nil evidence
// are ignored.
func TestBuildASNSupplierIndex(t *testing.T) {
	evidence := map[string]*addressSnapshotSupplierEvidence{
		"cmcc":  {name: "中国移动", asns: map[uint32]struct{}{9808: {}, 56041: {}, 4134: {}}},
		"ct":    {name: "中国电信", asns: map[uint32]struct{}{4134: {}, 4809: {}, 0: {}}},
		"empty": nil,
	}
	index := buildASNSupplierIndex(evidence)
	if index[9808] != "cmcc" {
		t.Fatalf("9808 -> %q, want cmcc", index[9808])
	}
	if index[56041] != "cmcc" {
		t.Fatalf("56041 -> %q, want cmcc", index[56041])
	}
	if index[4809] != "ct" {
		t.Fatalf("4809 -> %q, want ct", index[4809])
	}
	if key, ok := index[4134]; ok {
		t.Fatalf("4134 is claimed by two operators and must be unmapped, got %q", key)
	}
	if _, ok := index[0]; ok {
		t.Fatal("ASN 0 must never be mapped")
	}
}

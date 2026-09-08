package address

import (
	"errors"
	"testing"
)

func TestPrepareAddressPrefixRevisionNormalizesAndIsOrderStable(t *testing.T) {
	current := []AddressPrefix{
		{ID: "old-b", CIDR: "198.51.100.0/24", Labels: map[string]string{"kind": "old"}, Source: "manual", RowVersion: 3},
		{ID: "keep-a", CIDR: "192.0.2.0/24", Labels: map[string]string{}, Source: "manual", RowVersion: 1},
	}
	left, err := prepareAddressPrefixRevision(current, []AddressPrefixBatchOperation{
		{Action: "create", CIDR: "203.0.113.7/24", Labels: map[string]string{"kind": "new"}},
		{Action: "delete", PrefixID: "old-b", ExpectedVersion: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	right, err := prepareAddressPrefixRevision(current, []AddressPrefixBatchOperation{
		{Action: "delete", PrefixID: "old-b", ExpectedVersion: 3},
		{Action: "create", CIDR: "203.0.113.0/24", Labels: map[string]string{"kind": "new"}, Source: "manual"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if left.BaseDigest != right.BaseDigest || left.RequestDigest != right.RequestDigest || left.ResultDigest != right.ResultDigest {
		t.Fatalf("digests are not stable: left=%#v right=%#v", left, right)
	}
	if len(left.Operations) != 2 || left.Operations[0].Action != "create" || left.Operations[0].CIDR != "203.0.113.0/24" || left.Operations[0].PrefixID == "" {
		t.Fatalf("operations = %#v", left.Operations)
	}
	if left.Operations[0].PrefixID != right.Operations[0].PrefixID {
		t.Fatalf("deterministic create ids differ: %q vs %q", left.Operations[0].PrefixID, right.Operations[0].PrefixID)
	}
	if left.Preview.CreateCount != 1 || left.Preview.DeleteCount != 1 || left.Preview.BeforePrefixCount != 2 || left.Preview.AfterPrefixCount != 2 || left.Preview.ExpectedResultDigest != left.ResultDigest {
		t.Fatalf("preview = %#v", left.Preview)
	}
}

func TestPrepareAddressPrefixRevisionRejectsStaleAndAmbiguousChanges(t *testing.T) {
	current := []AddressPrefix{{ID: "prefix-a", CIDR: "192.0.2.0/24", Labels: map[string]string{}, Source: "manual", RowVersion: 2}}
	for name, operations := range map[string][]AddressPrefixBatchOperation{
		"stale delete": {{Action: "delete", PrefixID: "prefix-a", ExpectedVersion: 1}},
		"duplicate delete": {
			{Action: "delete", PrefixID: "prefix-a", ExpectedVersion: 2},
			{Action: "delete", PrefixID: "prefix-a", ExpectedVersion: 2},
		},
		"duplicate cidr":  {{Action: "create", CIDR: "192.0.2.7/24"}},
		"create identity": {{Action: "create", PrefixID: "caller-id", CIDR: "203.0.113.0/24"}},
		"unknown action":  {{Action: "update", PrefixID: "prefix-a", ExpectedVersion: 2, CIDR: "203.0.113.0/24"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := prepareAddressPrefixRevision(current, operations)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
	_, err := prepareAddressPrefixRevision(current, []AddressPrefixBatchOperation{{Action: "delete", PrefixID: "prefix-a", ExpectedVersion: 1}})
	if !errors.Is(err, ErrAddressDraftRevisionChanged) {
		t.Fatalf("stale error = %v", err)
	}
}

func TestPrepareAddressPrefixRevisionAllowsDeleteThenRecreateCIDR(t *testing.T) {
	current := []AddressPrefix{{ID: "prefix-a", CIDR: "192.0.2.0/24", Labels: map[string]string{"old": "true"}, Source: "manual", RowVersion: 2}}
	prepared, err := prepareAddressPrefixRevision(current, []AddressPrefixBatchOperation{
		{Action: "delete", PrefixID: "prefix-a", ExpectedVersion: 2},
		{Action: "create", CIDR: "192.0.2.0/24", Labels: map[string]string{"new": "true"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.Result) != 1 || prepared.Result[0].ID == "prefix-a" || prepared.Result[0].Labels["new"] != "true" {
		t.Fatalf("result = %#v", prepared.Result)
	}
}

func TestPrepareAddressPrefixRevisionTreatsASNZeroAsUnknown(t *testing.T) {
	zero := uint32(0)
	prepared, err := prepareAddressPrefixRevision(nil, []AddressPrefixBatchOperation{{
		Action: "create", CIDR: "203.0.113.0/24", ASN: &zero,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Operations[0].ASN != nil || prepared.Result[0].ASN != nil {
		t.Fatalf("ASN zero must normalize to unknown: %#v", prepared.Operations[0])
	}
}

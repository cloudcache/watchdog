// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"testing"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

// TestFlowFilterReferencesISP: the "operator owns the isp predicate" guard must find
// an isp predicate anywhere in a typed filter tree, and ignore other fields.
func TestFlowFilterReferencesISP(t *testing.T) {
	if flowFilterReferencesISP(nil) {
		t.Fatal("nil filter must not reference isp")
	}
	ispPred := &flowquery.FilterExpression{Op: flowquery.FilterPredicate, Field: "isp", Operator: flowquery.FilterEqual, Values: []string{"7"}}
	if !flowFilterReferencesISP(ispPred) {
		t.Fatal("bare isp predicate must be detected")
	}
	other := &flowquery.FilterExpression{Op: flowquery.FilterPredicate, Field: "business", Operator: flowquery.FilterEqual, Values: []string{"b1"}}
	if flowFilterReferencesISP(other) {
		t.Fatal("non-isp predicate must not be detected")
	}
	nested := &flowquery.FilterExpression{Op: flowquery.FilterAnd, Args: []flowquery.FilterExpression{*other, *ispPred}}
	if !flowFilterReferencesISP(nested) {
		t.Fatal("isp nested under AND must be detected")
	}
	clean := &flowquery.FilterExpression{Op: flowquery.FilterAnd, Args: []flowquery.FilterExpression{*other, *other}}
	if flowFilterReferencesISP(clean) {
		t.Fatal("tree without isp must not be detected")
	}
}

// TestInjectFlowOperatorConstraints: the pin sets the snapshot filter and either
// installs the isp predicate directly (no prior filter) or ANDs it onto an existing
// one (verified by finding isp in the merged tree).
func TestInjectFlowOperatorConstraints(t *testing.T) {
	// No existing filter → the isp predicate becomes the whole filter.
	var filters flowquery.Filters
	var filter *flowquery.FilterExpression
	if err := injectFlowOperatorConstraints(&filters, &filter, []string{"snap-a", "snap-b"}, 42); err != nil {
		t.Fatal(err)
	}
	if len(filters.DimensionSnapshotIDs) != 2 || filters.DimensionSnapshotIDs[0] != "snap-a" || filters.DimensionSnapshotIDs[1] != "snap-b" {
		t.Fatalf("snapshot pin = %+v", filters.DimensionSnapshotIDs)
	}
	if filter == nil || filter.Op != flowquery.FilterPredicate || filter.Field != "isp" ||
		filter.Operator != flowquery.FilterEqual || len(filter.Values) != 1 || filter.Values[0] != "42" {
		t.Fatalf("isp predicate = %+v", filter)
	}

	// Existing filter → isp is ANDed on; the merged tree still references isp and the
	// original field.
	existing := &flowquery.FilterExpression{Op: flowquery.FilterPredicate, Field: "business", Operator: flowquery.FilterEqual, Values: []string{"b1"}}
	var filters2 flowquery.Filters
	filter2 := existing
	if err := injectFlowOperatorConstraints(&filters2, &filter2, []string{"snap-c"}, 9); err != nil {
		t.Fatal(err)
	}
	if !flowFilterReferencesISP(filter2) {
		t.Fatalf("merged filter must reference isp: %+v", filter2)
	}
	if len(filters2.DimensionSnapshotIDs) != 1 || filters2.DimensionSnapshotIDs[0] != "snap-c" {
		t.Fatalf("snapshot pin (existing filter) = %+v", filters2.DimensionSnapshotIDs)
	}
}

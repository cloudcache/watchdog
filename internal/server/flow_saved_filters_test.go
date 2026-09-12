// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"testing"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

func validSavedFilterExpr() flowquery.FilterExpression {
	return flowquery.FilterExpression{Op: flowquery.FilterPredicate, Field: "src_ip", Operator: flowquery.FilterIn, Values: []string{"203.0.113.0/24"}}
}

// TestNormalizeFlowSavedFilter covers the validation gates: name/description
// bounds, share-scope enum (private/shared, defaulting to private), and the
// resource-field rejection that stops target/device/exporter from being baked
// into a shareable AST.
func TestNormalizeFlowSavedFilter(t *testing.T) {
	longName := make([]byte, flowSavedFilterNameMax+1)
	for i := range longName {
		longName[i] = 'a'
	}
	resourceExpr := flowquery.FilterExpression{Op: flowquery.FilterPredicate, Field: "device", Operator: flowquery.FilterIn, Values: []string{"01HXDEVICE0000000000000000"}}

	t.Run("valid defaults to private", func(t *testing.T) {
		item, err := normalizeFlowSavedFilter(flowSavedFilter{Name: "  Overseas egress ", Filter: validSavedFilterExpr()})
		if err != nil {
			t.Fatalf("normalize: %v", err)
		}
		if item.Name != "Overseas egress" || item.ShareScope != flowSavedFilterPrivate || item.FilterSchemaVersion != flowSavedFilterSchemaVersion {
			t.Fatalf("normalized = %+v", item)
		}
	})
	t.Run("explicit tenant", func(t *testing.T) {
		item, err := normalizeFlowSavedFilter(flowSavedFilter{Name: "Team view", ShareScope: "TENANT", Filter: validSavedFilterExpr()})
		if err != nil || item.ShareScope != flowSavedFilterTenant {
			t.Fatalf("tenant scope = %+v err=%v", item, err)
		}
	})
	for _, tc := range []struct {
		name string
		item flowSavedFilter
	}{
		{"empty name", flowSavedFilter{Name: "  ", Filter: validSavedFilterExpr()}},
		{"name too long", flowSavedFilter{Name: string(longName), Filter: validSavedFilterExpr()}},
		{"description too long", flowSavedFilter{Name: "ok", Description: string(longName) + string(longName) + string(longName) + string(longName) + string(longName) + string(longName), Filter: validSavedFilterExpr()}},
		{"bad scope", flowSavedFilter{Name: "ok", ShareScope: "shared", Filter: validSavedFilterExpr()}},
		{"resource field rejected", flowSavedFilter{Name: "ok", Filter: resourceExpr}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := normalizeFlowSavedFilter(tc.item); err == nil {
				t.Fatalf("%s: expected error", tc.name)
			}
		})
	}
}

// TestCanShareFlowFilter: install-wide sharing needs admin or flow management,
// never a plain flow viewer.
func TestCanShareFlowFilter(t *testing.T) {
	if !canShareFlowFilter(&principal{IsAdmin: true}) {
		t.Fatal("admin must be able to share")
	}
	if !canShareFlowFilter(&principal{Abilities: map[string]bool{"flow.device.manage": true}}) {
		t.Fatal("flow.device.manage must be able to share")
	}
	if canShareFlowFilter(&principal{Abilities: map[string]bool{"flow.view.customer": true}}) {
		t.Fatal("a plain flow viewer must not be able to share")
	}
	if canShareFlowFilter(nil) {
		t.Fatal("nil principal must not be able to share")
	}
}

// TestCanEditFlowSavedFilter: shared filters follow the share permission; private
// filters are owner-only (an admin cannot edit another user's private filter).
func TestCanEditFlowSavedFilter(t *testing.T) {
	admin := &principal{UserID: "admin", IsAdmin: true}
	owner := &principal{UserID: "u1", Abilities: map[string]bool{"flow.view.customer": true}}
	other := &principal{UserID: "u2", Abilities: map[string]bool{"flow.view.customer": true}}

	shared := flowSavedFilter{OwnerUserID: "u1", ShareScope: flowSavedFilterTenant}
	if !canEditFlowSavedFilter(admin, shared) {
		t.Fatal("admin may edit a shared filter")
	}
	if canEditFlowSavedFilter(other, shared) {
		t.Fatal("a plain viewer may not edit a shared filter")
	}

	private := flowSavedFilter{OwnerUserID: "u1", ShareScope: flowSavedFilterPrivate}
	if !canEditFlowSavedFilter(owner, private) {
		t.Fatal("owner may edit their private filter")
	}
	if canEditFlowSavedFilter(other, private) {
		t.Fatal("a non-owner may not edit a private filter")
	}
	if canEditFlowSavedFilter(admin, private) {
		t.Fatal("an admin may not edit another user's private filter")
	}
}

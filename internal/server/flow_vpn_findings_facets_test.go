// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func facetTestContext(target string) (*gin.Context, *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodGet, target, nil)
	return c, rec
}

// TestVPNFindingFilterExpr honors the window/search/column filters and skips the
// excluded field; it rejects a filter column outside the allowlist.
func TestVPNFindingFilterExpr(t *testing.T) {
	s := &Server{}
	c, _ := facetTestContext("/f?field=disposition&search=10.0&filter.risk_level=high&filter.disposition=confirmed")
	where, args, ok := s.vpnFindingFilterExpr(c, "disposition", "search")
	if !ok {
		t.Fatalf("expected ok")
	}
	joined := strings.Join(where, " AND ")
	if !strings.Contains(joined, "risk_level IN (?)") {
		t.Fatalf("risk_level filter missing: %s", joined)
	}
	if strings.Contains(joined, "disposition IN") {
		t.Fatalf("excluded field must be skipped: %s", joined)
	}
	if !strings.Contains(joined, "id LIKE ?") {
		t.Fatalf("search clause missing: %s", joined)
	}
	if len(args) == 0 {
		t.Fatalf("expected bound args")
	}

	c, rec := facetTestContext("/f?field=disposition&filter.evil=x")
	if _, _, ok := s.vpnFindingFilterExpr(c, "disposition", "search"); ok || rec.Code != http.StatusBadRequest {
		t.Fatalf("disallowed filter column: ok=%v code=%d", ok, rec.Code)
	}
}

// TestListVPNFindingFacetsRejectsBadField returns 400 before any query when the
// faceted field is not allowlisted.
func TestListVPNFindingFacetsRejectsBadField(t *testing.T) {
	s := &Server{}
	c, rec := facetTestContext("/f?field=drop_table")
	s.listVPNFindingFacets(c)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

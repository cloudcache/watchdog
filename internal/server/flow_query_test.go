// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/gin-gonic/gin"
)

func init() { gin.SetMode(gin.TestMode) }

func flowTestContext(p *principal) (*gin.Context, *httptest.ResponseRecorder) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	if p != nil {
		c.Set(principalKey, p)
	}
	return c, rec
}

// TestFlowDetailResourceFilters verifies target/device/exporter ids are gathered
// from both structured filters and column filters (the inputs the resource-scope
// check authorizes).
func TestFlowDetailResourceFilters(t *testing.T) {
	targets, devices, exporters := flowDetailResourceFilters(
		flowquery.DetailFilters{TargetIDs: []string{"t1"}, DeviceIDs: []string{"d1"}, ExporterIDs: []string{"e1"}},
		[]flowquery.DetailColumnFilter{
			{Field: string(flowquery.DetailFieldTargetID), Values: []string{"t2"}},
			{Field: string(flowquery.DetailFieldDeviceID), Values: []string{"d2"}},
			{Field: string(flowquery.DetailFieldExporterID), Values: []string{"e2"}},
			{Field: "src_ip", Values: []string{"10.0.0.1"}}, // not a resource field
		},
	)
	if !reflect.DeepEqual(targets, []string{"t1", "t2"}) || !reflect.DeepEqual(devices, []string{"d1", "d2"}) || !reflect.DeepEqual(exporters, []string{"e1", "e2"}) {
		t.Fatalf("resource filters targets=%v devices=%v exporters=%v", targets, devices, exporters)
	}
}

// TestWriteFlowQueryError maps flowquery errors to the right status: typed
// validation -> 400, permission -> 403, timeout -> 504, opaque backend -> 503.
func TestWriteFlowQueryError(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want int
	}{
		{"validation", &flowquery.RequestError{Field: "view", Code: flowquery.ErrorUnsupported, Message: "bad"}, http.StatusBadRequest},
		{"permission", &flowquery.RequestError{Field: "scope", Code: flowquery.ErrorPermissionDenied, Message: "no"}, http.StatusForbidden},
		{"timeout", context.DeadlineExceeded, http.StatusGatewayTimeout},
		{"backend", errors.New("clickhouse unavailable"), http.StatusServiceUnavailable},
	} {
		c, rec := flowTestContext(nil)
		writeFlowQueryError(c, test.err)
		if rec.Code != test.want {
			t.Fatalf("%s: status=%d want %d", test.name, rec.Code, test.want)
		}
	}
}

// TestAuthorizeFlowView defaults to the least-privileged customer view, rejects
// an unsupported view (400), and gates supplier/raw on the matching ability (403),
// with admin bypassing.
func TestAuthorizeFlowView(t *testing.T) {
	customer := &principal{Abilities: map[string]bool{"flow.view.customer": true}}
	admin := &principal{IsAdmin: true}

	// Empty view -> customer default, authorized for a customer-view principal.
	c, rec := flowTestContext(customer)
	if view, ok := (&Server{}).authorizeFlowView(c, ""); !ok || view != flowquery.ViewCustomer || rec.Code != http.StatusOK {
		t.Fatalf("empty view -> view=%q ok=%t status=%d", view, ok, rec.Code)
	}

	// Unsupported view -> 400.
	c, rec = flowTestContext(customer)
	if _, ok := (&Server{}).authorizeFlowView(c, flowquery.View("bogus")); ok || rec.Code != http.StatusBadRequest {
		t.Fatalf("bogus view -> ok=%t status=%d", ok, rec.Code)
	}

	// Supplier without the ability -> 403.
	c, rec = flowTestContext(customer)
	if _, ok := (&Server{}).authorizeFlowView(c, flowquery.ViewSupplier); ok || rec.Code != http.StatusForbidden {
		t.Fatalf("supplier without grant -> ok=%t status=%d", ok, rec.Code)
	}

	// Admin may query the raw layer.
	c, rec = flowTestContext(admin)
	if view, ok := (&Server{}).authorizeFlowView(c, flowquery.ViewRaw); !ok || view != flowquery.ViewRaw || rec.Code != http.StatusOK {
		t.Fatalf("admin raw -> view=%q ok=%t status=%d", view, ok, rec.Code)
	}
}

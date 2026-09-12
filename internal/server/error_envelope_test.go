// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/gin-gonic/gin"
)

// errorEnvelope mirrors the restored hub error shape {error:{code,message,retryable,details?}}.
type errorEnvelope struct {
	Error struct {
		Code      string         `json:"code"`
		Message   string         `json:"message"`
		Retryable bool           `json:"retryable"`
		Details   map[string]any `json:"details"`
	} `json:"error"`
}

// TestFailEnvelopeRetryable: fail() derives retryable exactly as the hub did — a
// transient condition (429 or any 5xx) is retryable; validation/permission/conflict
// are not — and omits details when there are none.
func TestFailEnvelopeRetryable(t *testing.T) {
	cases := []struct {
		status int
		want   bool
	}{{400, false}, {403, false}, {404, false}, {409, false}, {429, true}, {500, true}, {503, true}, {504, true}}
	for _, tc := range cases {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		fail(c, tc.status, "some_code", "boom")
		var env errorEnvelope
		if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
			t.Fatalf("status %d: %v (%s)", tc.status, err, rec.Body.String())
		}
		if env.Error.Code != "some_code" || env.Error.Message != "boom" || env.Error.Retryable != tc.want {
			t.Fatalf("status %d: envelope = %+v", tc.status, env.Error)
		}
		if env.Error.Details != nil {
			t.Fatalf("status %d: details must be omitted when empty, got %v", tc.status, env.Error.Details)
		}
	}
}

// TestFailDetailsEnvelope: failDetails carries a structured details map.
func TestFailDetailsEnvelope(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	failDetails(c, 400, "QUERY_INVALID", "bad request", map[string]any{"field": "metric"})
	body := rec.Body.String()
	if !strings.Contains(body, `"details":{"field":"metric"}`) || !strings.Contains(body, `"retryable":false`) || !strings.Contains(body, `"code":"QUERY_INVALID"`) {
		t.Fatalf("body = %s", body)
	}
}

// TestWriteFlowQueryErrorVocabulary: flow execution errors map to the hub's QUERY_*
// codes with the correct status, and carry the field + engine code in details.
func TestWriteFlowQueryErrorVocabulary(t *testing.T) {
	cases := []struct {
		code       flowquery.ErrorCode
		wantStatus int
		wantCode   string
	}{
		{flowquery.ErrorInvalid, 400, "QUERY_INVALID"},
		{flowquery.ErrorLimitExceeded, 400, "QUERY_RANGE_LIMIT"},
		{flowquery.ErrorIncompleteRange, 409, "QUERY_INCOMPLETE"},
		{flowquery.ErrorPermissionDenied, 403, "QUERY_PERMISSION_DENIED"},
	}
	for _, tc := range cases {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		writeFlowQueryError(c, &flowquery.RequestError{Code: tc.code, Field: "view", Message: "nope"})
		if rec.Code != tc.wantStatus {
			t.Fatalf("code %q: status = %d, want %d", tc.code, rec.Code, tc.wantStatus)
		}
		var env errorEnvelope
		if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
			t.Fatal(err)
		}
		if env.Error.Code != tc.wantCode || env.Error.Details["flow_code"] != string(tc.code) || env.Error.Details["field"] != "view" {
			t.Fatalf("code %q: envelope = %+v", tc.code, env.Error)
		}
	}
}

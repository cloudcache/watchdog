package server

import (
	"errors"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/gin-gonic/gin"
)

func TestValidateAggregateGraphMutation(t *testing.T) {
	tests := []struct {
		name string
		in   aggregateGraphMutation
		want string
	}{
		{name: "valid", in: aggregateGraphMutation{Name: "WAN total", Aggregation: "sum", ValueMode: "corrected"}},
		{name: "name", in: aggregateGraphMutation{Aggregation: "sum", ValueMode: "corrected"}, want: "name is required"},
		{name: "aggregation", in: aggregateGraphMutation{Name: "x", Aggregation: "median", ValueMode: "corrected"}, want: "aggregation must"},
		{name: "value mode", in: aggregateGraphMutation{Name: "x", Aggregation: "sum", ValueMode: "tenant"}, want: "value mode must"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message := validateAggregateGraphMutation(test.in)
			if test.want == "" && message != "" {
				t.Fatalf("unexpected validation error: %s", message)
			}
			if test.want != "" && !strings.Contains(message, test.want) {
				t.Fatalf("message=%q; want substring %q", message, test.want)
			}
		})
	}
}

func TestAggregateGraphValueModesPreserveHistoricalQueryContract(t *testing.T) {
	for _, test := range []struct {
		name, query string
		admin       bool
		want        []string
		wantErr     error
	}{
		{name: "graph default", admin: false, want: []string{"corrected"}},
		{name: "raw admin", query: "?value_mode=raw", admin: true, want: []string{"raw"}},
		{name: "both admin", query: "?value_mode=both", admin: true, want: []string{"corrected", "raw"}},
		{name: "raw user", query: "?value_mode=raw", wantErr: errAggregateGraphRawForbidden},
		{name: "invalid", query: "?value_mode=vendor", admin: true, wantErr: errAggregateGraphValueMode},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest("GET", "/api/v1/aggregate-graphs/graph/series"+test.query, nil)
			c.Set(principalKey, &principal{IsAdmin: test.admin})
			got, err := aggregateGraphValueModes(c, aggregateGraphRecord{ValueMode: "corrected"})
			if !errors.Is(err, test.wantErr) || !slices.Equal(got, test.want) {
				t.Fatalf("modes=%v err=%v; want modes=%v err=%v", got, err, test.want, test.wantErr)
			}
		})
	}
}

func TestSummarizeAggregateGraph(t *testing.T) {
	base := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	points := []snmpch.Point{
		{Time: base, Value: 8_000},
		{Time: base.Add(time.Minute), Value: 16_000},
		{Time: base.Add(2 * time.Minute), Value: 24_000},
	}
	result := summarizeAggregateGraph(points, time.Minute, "bps")
	if result["samples"] != 3 || result["peak"] != float64(24_000) || result["average"] != float64(16_000) {
		t.Fatalf("unexpected summary: %#v", result)
	}
	if result["total_bytes"] != uint64(360_000) {
		t.Fatalf("total_bytes=%v; want 360000", result["total_bytes"])
	}
}

func TestAggregateGraphWindowHonorsFinalPointBudget(t *testing.T) {
	for _, test := range []struct {
		name, query string
		maxPoints   int
		wantStep    time.Duration
	}{
		{name: "automatic", query: "?time_mode=fixed&window=24h", maxPoints: 600, wantStep: 5 * time.Minute},
		{name: "explicit step", query: "?time_mode=fixed&window=24h&step=60", maxPoints: 600, wantStep: time.Minute},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest("GET", "/api/v1/aggregate-graphs/graph/series"+test.query, nil)
			_, _, step, ok := aggregateGraphWindow(c, test.maxPoints)
			if !ok || step != test.wantStep {
				t.Fatalf("step=%s ok=%v; want %s", step, ok, test.wantStep)
			}
		})
	}
}

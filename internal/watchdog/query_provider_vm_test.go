package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestVictoriaMetricsQueryProviderUsesTypedTenantScopedQuery(t *testing.T) {
	start := time.Date(2026, 8, 24, 11, 0, 0, 0, time.UTC)
	response := VictoriaMetricsResponse{Status: "success"}
	response.Data.ResultType = "matrix"
	response.Data.Result = []VMRangeQueryItem{{
		Metric: map[string]string{"port_id": "port-a"},
		Values: []VMValue{{Time: start, Value: 100}, {Time: start.Add(time.Minute), Value: 200}},
	}}
	client := &fakeMetricsQueryClient{response: response}
	provider := VictoriaMetricsQueryProvider{Client: client}
	result, err := provider.Query(context.Background(), QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{
			Key: "network.snmp_interface", Metrics: []string{MetricSNMPIfInBps},
		},
		From: start, To: start.Add(time.Hour), StepSeconds: 60, Limit: 10,
		ValueLayer: QueryValueRaw,
		Parameters: json.RawMessage(`{"metric":"watchdog_snmp_if_in_bps","target_id":"target-a"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(client.query.Query, `tenant_id="tenant-a"`) || !strings.Contains(client.query.Query, `target_id="target-a"`) {
		t.Fatalf("selector = %s", client.query.Query)
	}
	if result.Unit != "bps" || result.Completeness.UnknownRatio != 1 || !json.Valid(result.Data) {
		t.Fatalf("result = %#v", result)
	}
}

func TestVictoriaMetricsQueryProviderRejectsUnknownParametersAndExcessRows(t *testing.T) {
	provider := VictoriaMetricsQueryProvider{Client: &fakeMetricsQueryClient{}}
	base := QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: "network.snmp_interface", Metrics: []string{MetricSNMPIfInOctetsTotal}},
		From: time.Now().Add(-time.Hour), To: time.Now(), StepSeconds: 60, Limit: 10,
		ValueLayer: QueryValueRaw,
		Parameters: json.RawMessage(`{"metric":"watchdog_snmp_if_in_octets_total","target_id":"target-a","tenant_id":"tenant-b"}`),
	}
	_, err := provider.Query(context.Background(), base)
	var queryErr *QueryGatewayError
	if !errors.As(err, &queryErr) || queryErr.Code != QueryErrorInvalidRequest {
		t.Fatalf("unknown parameter error = %#v", err)
	}

	response := VictoriaMetricsResponse{Status: "success"}
	response.Data.Result = []VMRangeQueryItem{{Values: []VMValue{{}, {}, {}}}}
	provider.Client = &fakeMetricsQueryClient{response: response}
	base.Parameters = json.RawMessage(`{"metric":"watchdog_snmp_if_in_octets_total","target_id":"target-a"}`)
	base.Limit = 2
	_, err = provider.Query(context.Background(), base)
	if !errors.As(err, &queryErr) || queryErr.Code != QueryErrorRowLimit {
		t.Fatalf("row limit error = %#v", err)
	}
}

func TestVictoriaMetricsClientReadiness(t *testing.T) {
	client := VictoriaMetricsClient{BaseURL: "http://victoria.test:8428", HTTPClient: &http.Client{Transport: queryRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/health" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader("OK")), Header: make(http.Header)}, nil
	})}}
	if err := client.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}

	unhealthy := VictoriaMetricsClient{BaseURL: "http://victoria.test:8428", HTTPClient: &http.Client{Transport: queryRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Status: "503 Service Unavailable", Body: io.NopCloser(strings.NewReader("not ready")), Header: make(http.Header)}, nil
	})}}
	if err := unhealthy.Ready(context.Background()); !errors.Is(err, ErrVictoriaMetricsUnavailable) {
		t.Fatalf("readiness error = %v", err)
	}
}

type queryRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn queryRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

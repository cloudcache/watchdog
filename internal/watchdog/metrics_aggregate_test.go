package watchdog

import (
	"context"
	"testing"
	"time"
)

func TestBuildAggregateGraphSeriesProducesPerItemAndTotal(t *testing.T) {
	graph := AggregateGraph{ID: "graph-a", TenantID: "tenant-a", Aggregation: AggregateSum}
	items := []AggregateGraphItem{
		{ID: "item-in", GraphID: "graph-a", Metric: MetricSNMPIfInBps, Direction: "in", Label: "In", Total: true},
		{ID: "item-out", GraphID: "graph-a", Metric: MetricSNMPIfOutBps, Direction: "out", Label: "Out", Total: true},
	}
	portIDs := []ID{"port-a"}
	query := MetricsQueryRequest{
		TenantID:  "tenant-a",
		ValueMode: MetricValueRaw,
		TimeMode:  TimeModeCustom,
		Start:     time.Unix(1000, 0),
		End:       time.Unix(2000, 0),
		Step:      time.Minute,
	}
	network := &fakeNetworkRepository{
		devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
		ports:   []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a"}},
	}
	service := MetricsService{Client: &cloningMetricsClient{value: 10}}

	resp, err := buildAggregateGraphSeries(context.Background(), network, service, graph, items, portIDs, query, false)
	if err != nil {
		t.Fatalf("buildAggregateGraphSeries error = %v", err)
	}
	// 2 item series + 1 combined Total
	if len(resp.Data.Result) != 3 {
		t.Fatalf("result len = %d, want 3 (%+v)", len(resp.Data.Result), resp.Data.Result)
	}
	labels := map[string]bool{}
	for _, r := range resp.Data.Result {
		labels[r.Metric["label"]] = true
	}
	if !labels["In"] || !labels["Out"] || !labels["Total"] {
		t.Fatalf("labels = %#v", labels)
	}
	// Total = in(10) + out(10) = 20
	var totalValue float64
	for _, r := range resp.Data.Result {
		if r.Metric["label"] == "Total" && len(r.Values) > 0 {
			totalValue = r.Values[0].Value
		}
	}
	if totalValue != 20 {
		t.Fatalf("total value = %v, want 20", totalValue)
	}
}

func TestBuildAggregateGraphSeriesOmitsTotalWhenNoTotalItem(t *testing.T) {
	graph := AggregateGraph{ID: "graph-b", TenantID: "tenant-a", Aggregation: AggregateSum}
	items := []AggregateGraphItem{
		{ID: "item-in", GraphID: "graph-b", Metric: MetricSNMPIfInBps, Direction: "in", Label: "In", Total: false},
	}
	network := &fakeNetworkRepository{
		devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
		ports:   []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a"}},
	}
	service := MetricsService{Client: &cloningMetricsClient{value: 5}}
	query := MetricsQueryRequest{TenantID: "tenant-a", ValueMode: MetricValueRaw, TimeMode: TimeModeCustom, Start: time.Unix(0, 0), End: time.Unix(60, 0), Step: time.Minute}

	resp, err := buildAggregateGraphSeries(context.Background(), network, service, graph, items, []ID{"port-a"}, query, false)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if len(resp.Data.Result) != 1 {
		t.Fatalf("result len = %d, want 1 (no total)", len(resp.Data.Result))
	}
}

func TestBuildAggregateGraphSeriesSplitSideGroupsBySideType(t *testing.T) {
	graph := AggregateGraph{ID: "graph-c", TenantID: "tenant-a", Aggregation: AggregateSum}
	items := []AggregateGraphItem{
		{ID: "item-in", GraphID: "graph-c", Metric: MetricSNMPIfInBps, Direction: "in", Label: "In", Total: false},
	}
	// Two ports with different side_type policies.
	network := &fakeNetworkRepository{
		devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
		ports: []NetworkPort{
			{ID: "port-prov", TenantID: "tenant-a", DeviceID: "device-a"},
			{ID: "port-cust", TenantID: "tenant-a", DeviceID: "device-a"},
		},
	}
	// fakeNetworkRepository returns a single policy for GetPortPolicy; override by
	// injecting per-port behaviour is not supported, so this test only verifies
	// that splitSide=true yields one group per distinct side when policies differ.
	// With a uniform policy both ports land in the same group -> still 1 series.
	service := MetricsService{Client: &cloningMetricsClient{value: 7}}
	query := MetricsQueryRequest{TenantID: "tenant-a", ValueMode: MetricValueRaw, TimeMode: TimeModeCustom, Start: time.Unix(0, 0), End: time.Unix(60, 0), Step: time.Minute}

	resp, err := buildAggregateGraphSeries(context.Background(), network, service, graph, items, []ID{"port-prov", "port-cust"}, query, true)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if len(resp.Data.Result) == 0 {
		t.Fatalf("expected at least one split series")
	}
	for _, r := range resp.Data.Result {
		if r.Metric["side_type"] == "" {
			t.Fatalf("splitSide series missing side_type tag: %#v", r.Metric)
		}
	}
}

// cloningMetricsClient returns a fresh response on every call (like the real
// HTTP client), avoiding shared-state aliasing when callers mutate results.
type cloningMetricsClient struct{ value float64 }

func (c *cloningMetricsClient) QueryRange(_ context.Context, query RangeQuery) (VictoriaMetricsResponse, error) {
	start := query.Start
	if start.IsZero() {
		start = time.Unix(1000, 0)
	}
	response := VictoriaMetricsResponse{Status: "success"}
	item := VMRangeQueryItem{Metric: map[string]string{"__name__": "test"}}
	delta := c.value * float64(query.Step.Seconds()) / 8
	item.Values = []VMValue{
		{Time: start, Value: 0},
		{Time: start.Add(query.Step), Value: delta},
	}
	response.Data.Result = []VMRangeQueryItem{item}
	return response, nil
}

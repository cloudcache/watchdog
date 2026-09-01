package watchdog

import (
	"context"
	"testing"
	"time"
)

// fakeAggregateGraphRepo is an in-memory AggregateGraphRepository for rollup tests.
type fakeAggregateGraphRepo struct {
	graphs []AggregateGraph
	ports  map[ID][]AggregateGraphPort
	items  map[ID][]AggregateGraphItem
	data   map[ID][]AggregateGraphDataPoint
}

func newFakeAggregateGraphRepo() *fakeAggregateGraphRepo {
	return &fakeAggregateGraphRepo{ports: map[ID][]AggregateGraphPort{}, items: map[ID][]AggregateGraphItem{}, data: map[ID][]AggregateGraphDataPoint{}}
}

func (r *fakeAggregateGraphRepo) ListAggregateGraphs(_ context.Context, tenantID ID) ([]AggregateGraph, error) {
	var out []AggregateGraph
	for _, g := range r.graphs {
		if g.TenantID == tenantID {
			out = append(out, g)
		}
	}
	return out, nil
}
func (r *fakeAggregateGraphRepo) GetAggregateGraph(_ context.Context, _ ID, graphID ID) (AggregateGraph, error) {
	for _, g := range r.graphs {
		if g.ID == graphID {
			return g, nil
		}
	}
	return AggregateGraph{}, errNotFoundForTest{}
}
func (r *fakeAggregateGraphRepo) CreateAggregateGraph(_ context.Context, g AggregateGraph) (AggregateGraph, error) {
	r.graphs = append(r.graphs, g)
	return g, nil
}
func (r *fakeAggregateGraphRepo) UpdateAggregateGraph(_ context.Context, g AggregateGraph) (AggregateGraph, error) {
	return g, nil
}
func (r *fakeAggregateGraphRepo) DeleteAggregateGraph(_ context.Context, _ ID, _ ID) error {
	return nil
}
func (r *fakeAggregateGraphRepo) ListAggregateGraphPorts(_ context.Context, _ ID, graphID ID) ([]AggregateGraphPort, error) {
	return r.ports[graphID], nil
}
func (r *fakeAggregateGraphRepo) ListAggregateGraphItems(_ context.Context, _ ID, graphID ID) ([]AggregateGraphItem, error) {
	return r.items[graphID], nil
}
func (r *fakeAggregateGraphRepo) ReplaceAggregateGraphItems(_ context.Context, _ ID, _ ID, items []AggregateGraphItem) error {
	return nil
}
func (r *fakeAggregateGraphRepo) ReplaceAggregateGraphPorts(_ context.Context, _ ID, graphID ID, ports []AggregateGraphPort) error {
	r.ports[graphID] = ports
	return nil
}
func (r *fakeAggregateGraphRepo) AppendAggregateGraphData(_ context.Context, point AggregateGraphDataPoint) error {
	for _, existing := range r.data[point.GraphID] {
		if existing.Timestamp.Equal(point.Timestamp) {
			return nil
		}
	}
	r.data[point.GraphID] = append(r.data[point.GraphID], point)
	return nil
}
func (r *fakeAggregateGraphRepo) ListAggregateGraphData(_ context.Context, _ ID, graphID ID, _, _ time.Time) ([]AggregateGraphDataPoint, error) {
	return r.data[graphID], nil
}
func (r *fakeAggregateGraphRepo) ListAllAggregateGraphs(_ context.Context) ([]AggregateGraph, error) {
	return r.graphs, nil
}

func TestAggregateGraphRollupStoresSnapshot(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	graphs := newFakeAggregateGraphRepo()
	graphs.graphs = []AggregateGraph{{ID: "graph-a", TenantID: "tenant-a", Aggregation: AggregateSum}}
	graphs.ports["graph-a"] = []AggregateGraphPort{{AggregateGraphID: "graph-a", TenantID: "tenant-a", PortID: "port-a"}}
	graphs.items["graph-a"] = []AggregateGraphItem{
		{ID: "item-in", GraphID: "graph-a", TenantID: "tenant-a", Metric: MetricSNMPIfInBps, Direction: "in", Label: "In"},
		{ID: "item-out", GraphID: "graph-a", TenantID: "tenant-a", Metric: MetricSNMPIfOutBps, Direction: "out", Label: "Out"},
	}

	network := &fakeNetworkRepository{
		devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
		ports:   []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a"}},
	}
	metrics := MetricsService{Client: &fakeMetricsQueryClient{response: rollupVMResponse(time.Unix(1000, 0), 1234)}}

	rollup := AggregateGraphRollup{Graphs: graphs, Network: network, Metrics: metrics, Now: func() time.Time { return now }}
	res, err := rollup.RunOnce(context.Background(), 5*time.Minute)
	if err != nil {
		t.Fatalf("RunOnce error = %v", err)
	}
	if res.Stored != 1 {
		t.Fatalf("stored = %d, want 1", res.Stored)
	}
	points := graphs.data["graph-a"]
	if len(points) != 1 {
		t.Fatalf("data = %+v", points)
	}

	// Re-running must not duplicate or overwrite (immutable snapshot via
	// deterministic id / timestamp).
	if _, err := rollup.RunOnce(context.Background(), 5*time.Minute); err != nil {
		t.Fatalf("second RunOnce error = %v", err)
	}
	if len(graphs.data["graph-a"]) != 1 {
		t.Fatalf("expected frozen snapshot, got data = %+v", graphs.data["graph-a"])
	}
}

func TestLatestValuePicksMostRecent(t *testing.T) {
	resp := vmResponseForMetricsTest(10)
	resp.Data.Result[0].Values = append(resp.Data.Result[0].Values, VMValue{Time: time.Unix(50, 0), Value: 5})
	latest := latestValue(resp)
	if latest == nil || latest.value != 10 {
		t.Fatalf("latest = %+v", latest)
	}
}

func rollupVMResponse(start time.Time, bps float64) VictoriaMetricsResponse {
	step := 60 * time.Second
	delta := bps * step.Seconds() / 8
	response := VictoriaMetricsResponse{Status: "success"}
	response.Data.Result = []VMRangeQueryItem{{
		Metric: map[string]string{"__name__": "test"},
		Values: []VMValue{
			{Time: start, Value: 0},
			{Time: start.Add(step), Value: delta},
		},
	}}
	return response
}

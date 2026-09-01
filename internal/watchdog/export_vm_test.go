package watchdog

import (
	"context"
	"testing"
	"time"
)

func TestVictoriaMetricsExportDataProviderQueriesRawCounterAndAppliesDelta(t *testing.T) {
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	client := &fakeMetricsQueryClient{response: exportVMResponse(start, []float64{0, 37500, 75000, 112500, 150000})}
	provider := VictoriaMetricsExportDataProvider{Client: client}
	task := ExportTask{
		TenantID:    "tenant-a",
		TargetID:    "target-a",
		PortID:      "port-a",
		RangeStart:  start,
		RangeEnd:    start.Add(25 * time.Minute),
		Step:        5 * time.Minute,
		Aggregation: AggregationP95FiveMinute,
	}
	samples, err := provider.LoadSamples(context.Background(), task)
	if err != nil {
		t.Fatalf("LoadSamples() error = %v", err)
	}
	if client.query.Query != `watchdog_snmp_if_in_octets_total{tenant_id="tenant-a",target_id="target-a",port_id="port-a"}` {
		t.Fatalf("query = %q", client.query.Query)
	}
	// LoadSamples returns raw delta samples (pre-aggregation): 4 delta points from 5 counter values
	if len(samples) != 4 {
		t.Fatalf("samples = %d, want 4", len(samples))
	}
	want := 37500.0 / 300.0 * 8
	if samples[0].Value != want {
		t.Fatalf("value = %v, want %v", samples[0].Value, want)
	}
}

func TestVictoriaMetricsExportDataProviderAggregatesDailyFromCounter(t *testing.T) {
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	response := VictoriaMetricsResponse{Status: "success"}
	item := VMRangeQueryItem{Metric: map[string]string{"__name__": MetricSNMPIfInOctetsTotal}}
	for hour := 0; hour < 48; hour++ {
		item.Values = append(item.Values, VMValue{
			Time:  start.Add(time.Duration(hour) * time.Hour),
			Value: float64(hour) * 3600000,
		})
	}
	response.Data.Result = []VMRangeQueryItem{item}
	provider := VictoriaMetricsExportDataProvider{Client: &fakeMetricsQueryClient{response: response}}
	samples, err := provider.LoadSamples(context.Background(), ExportTask{
		TenantID:    "tenant-a",
		TargetID:    "target-a",
		RangeStart:  start,
		RangeEnd:    start.Add(48 * time.Hour),
		Step:        5 * time.Minute,
		Aggregation: AggregationDailyAverage,
	})
	if err != nil {
		t.Fatalf("LoadSamples() error = %v", err)
	}
	// LoadSamples now returns raw samples (pre-aggregation)
	if len(samples) != 47 {
		t.Fatalf("samples = %d, want 47 (raw delta points)", len(samples))
	}
	want := 3600000.0 / 3600.0 * 8
	if samples[0].Value != want {
		t.Fatalf("first sample value = %v, want %v", samples[0].Value, want)
	}
}

func exportVMResponse(start time.Time, values []float64) VictoriaMetricsResponse {
	response := VictoriaMetricsResponse{Status: "success"}
	item := VMRangeQueryItem{Metric: map[string]string{"__name__": MetricSNMPIfInOctetsTotal}}
	for i, value := range values {
		item.Values = append(item.Values, VMValue{Time: start.Add(time.Duration(i) * 5 * time.Minute), Value: value})
	}
	response.Data.Result = []VMRangeQueryItem{item}
	return response
}

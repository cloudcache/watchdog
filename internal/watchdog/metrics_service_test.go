package watchdog

import (
	"context"
	"testing"
	"time"
)

type fakeMetricsQueryClient struct {
	query    RangeQuery
	queries  []RangeQuery
	response VictoriaMetricsResponse
	err      error
}

func (c *fakeMetricsQueryClient) QueryRange(_ context.Context, query RangeQuery) (VictoriaMetricsResponse, error) {
	c.query = query
	c.queries = append(c.queries, query)
	if c.err != nil {
		return VictoriaMetricsResponse{}, c.err
	}
	if c.response.Status != "" {
		return c.response, nil
	}
	return VictoriaMetricsResponse{Status: "success"}, nil
}

func TestMetricsServiceQueryRealtimeUsesRecentWindow(t *testing.T) {
	client := &fakeMetricsQueryClient{}
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	service := MetricsService{
		Client: client,
		Now: func() time.Time {
			return now
		},
	}
	_, err := service.QueryRealtime(context.Background(), MetricsQueryRequest{Step: 30 * time.Second}, "metric{}")
	if err != nil {
		t.Fatalf("QueryRealtime() error = %v", err)
	}
	if !client.query.Start.Equal(now.Add(-5*time.Minute)) || !client.query.End.Equal(now) {
		t.Fatalf("range = %s - %s", client.query.Start, client.query.End)
	}
	if client.query.Step != 30*time.Second {
		t.Fatalf("step = %s", client.query.Step)
	}
}

func TestMetricsServiceQueryRealtimeRejectsLargeStep(t *testing.T) {
	service := MetricsService{Client: &fakeMetricsQueryClient{}}
	_, err := service.QueryRealtime(context.Background(), MetricsQueryRequest{Step: 5 * time.Minute}, "metric{}")
	if err == nil {
		t.Fatal("expected realtime step error")
	}
}

func TestMetricsServiceQueryFixedWindowUsesDefaultStep(t *testing.T) {
	client := &fakeMetricsQueryClient{}
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	service := MetricsService{
		Client: client,
		Now: func() time.Time {
			return now
		},
	}
	_, err := service.QueryFixedWindow(context.Background(), MetricsQueryRequest{}, "metric{}", FixedWindowDay, true)
	if err != nil {
		t.Fatalf("QueryFixedWindow() error = %v", err)
	}
	if !client.query.Start.Equal(now.Add(-24*time.Hour)) || !client.query.End.Equal(now) {
		t.Fatalf("range = %s - %s", client.query.Start, client.query.End)
	}
	if client.query.Step != 5*time.Minute {
		t.Fatalf("step = %s, want 5m", client.query.Step)
	}
}

func TestMetricsServiceQueryFixedWindowRejectsUnsupportedWindow(t *testing.T) {
	service := MetricsService{Client: &fakeMetricsQueryClient{}}
	_, err := service.QueryFixedWindow(context.Background(), MetricsQueryRequest{}, "metric{}", "13d", true)
	if err == nil {
		t.Fatal("expected unsupported window error")
	}
}

package watchdog

import (
	"context"
	"errors"
	"log"
	"time"
)

const defaultAggregateGraphRollupInterval = 5 * time.Minute

type AggregateGraphRollupLoopConfig struct {
	Interval       time.Duration
	RunImmediately bool
}

type AggregateGraphRollupResult struct {
	Processed int
	Stored    int
	Skipped   int
}

// AggregateGraphRollup periodically computes one aggregated sample per graph
// (summed across the graph's ports, honoring its value mode and per-port
// correction) and appends it to aggregate_graph_data as an immutable snapshot.
type AggregateGraphRollup struct {
	Graphs  AggregateGraphRepository
	Network NetworkRepository
	Metrics MetricsService
	Now     func() time.Time
}

func (r AggregateGraphRollup) RunLoop(ctx context.Context, cfg AggregateGraphRollupLoopConfig) error {
	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultAggregateGraphRollupInterval
	}
	if cfg.RunImmediately {
		if _, err := r.RunOnce(ctx, interval); err != nil {
			return err
		}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := r.RunOnce(ctx, interval); err != nil {
				return err
			}
		}
	}
}

func (r AggregateGraphRollup) RunOnce(ctx context.Context, interval time.Duration) (AggregateGraphRollupResult, error) {
	result := AggregateGraphRollupResult{}
	if r.Graphs == nil {
		return result, errors.New("aggregate graph repository is required")
	}
	graphs, err := r.Graphs.ListAllAggregateGraphs(ctx)
	if err != nil {
		return result, err
	}
	now := r.now()
	start := now.Add(-2 * interval)
	for _, graph := range graphs {
		result.Processed++
		stored, err := r.rollupGraph(ctx, graph, start, now, interval)
		if err != nil {
			result.Skipped++
			log.Printf("aggregate graph rollup skipped %s: %v", graph.ID, err)
			continue
		}
		if stored {
			result.Stored++
		} else {
			result.Skipped++
		}
	}
	return result, nil
}

func (r AggregateGraphRollup) rollupGraph(ctx context.Context, graph AggregateGraph, start, end time.Time, interval time.Duration) (bool, error) {
	portLinks, err := r.Graphs.ListAggregateGraphPorts(ctx, graph.TenantID, graph.ID)
	if err != nil {
		return false, err
	}
	if len(portLinks) == 0 {
		return false, nil
	}
	items, err := r.Graphs.ListAggregateGraphItems(ctx, graph.TenantID, graph.ID)
	if err != nil {
		return false, err
	}
	if len(items) == 0 {
		return false, nil
	}
	portIDs := make([]ID, 0, len(portLinks))
	for _, link := range portLinks {
		if link.PortID != "" {
			portIDs = append(portIDs, link.PortID)
		}
	}
	stored := false
	for _, item := range items {
		req := MetricsAggregateRequest{
			Query: MetricsQueryRequest{
				TenantID:  graph.TenantID,
				Metric:    item.Metric,
				ValueMode: graph.ValueMode,
				TimeMode:  TimeModeCustom,
				Start:     start,
				End:       end,
				Step:      interval,
			},
			PortIDs: portIDs,
			Method:  string(graph.Aggregation),
		}
		response, err := aggregatePortSeries(ctx, r.Network, r.Metrics, req)
		if err != nil {
			return stored, err
		}
		latest := latestValue(response)
		if latest == nil {
			continue
		}
		point := AggregateGraphDataPoint{
			ID:        stableID("agdata", string(item.ID), latest.timestamp.UTC().Format(time.RFC3339Nano)),
			TenantID:  graph.TenantID,
			GraphID:   graph.ID,
			ItemID:    item.ID,
			Timestamp: latest.timestamp,
			Value:     latest.value,
		}
		if err := r.Graphs.AppendAggregateGraphData(ctx, point); err != nil {
			return stored, err
		}
		stored = true
	}
	return stored, nil
}

type aggregatedSample struct {
	timestamp time.Time
	value     float64
}

func latestValue(response VictoriaMetricsResponse) *aggregatedSample {
	var latest *aggregatedSample
	var latestCorrected *aggregatedSample
	for _, item := range response.Data.Result {
		corrected := item.Metric["value_mode"] == "corrected"
		for _, v := range item.Values {
			captured := v
			if latest == nil || captured.Time.After(latest.timestamp) {
				latest = &aggregatedSample{timestamp: captured.Time, value: captured.Value}
			}
			if corrected && (latestCorrected == nil || captured.Time.After(latestCorrected.timestamp)) {
				latestCorrected = &aggregatedSample{timestamp: captured.Time, value: captured.Value}
			}
		}
	}
	// A "both" graph returns raw and corrected series with the same
	// timestamps; the snapshot should record the corrected value.
	if latestCorrected != nil {
		return latestCorrected
	}
	return latest
}

func (r AggregateGraphRollup) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now().UTC()
}

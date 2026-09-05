// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowmetrics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

// TestVictoriaMetricsStoresAndQueriesFlowRuntimeMetrics verifies the exact
// Prometheus text emitted by the production collector and worker handlers
// against a real VictoriaMetrics instance. The running VM still needs an
// external promscrape/vmagent configuration for production pull discovery.
func TestVictoriaMetricsStoresAndQueriesFlowRuntimeMetrics(t *testing.T) {
	if os.Getenv("WATCHDOG_FLOW_VICTORIAMETRICS_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_FLOW_VICTORIAMETRICS_INTEGRATION=1 to run")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("WATCHDOG_VICTORIAMETRICS_URL")), "/")
	if baseURL == "" {
		baseURL = "http://127.0.0.1:8428"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	runID := fmt.Sprintf("flow-metrics-it-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := deleteVictoriaMetricsSeries(cleanupCtx, baseURL, runID); err != nil {
			t.Errorf("clean VictoriaMetrics integration series: %v", err)
		}
	})

	collector, err := NewCollector(7, time.Now().Add(-time.Minute), func() flowstream.ProducerStats {
		return flowstream.ProducerStats{Records: 17, Bytes: 2048, BufferedRecords: 3}
	})
	if err != nil {
		t.Fatal(err)
	}
	collector.ObserveReceived(flowpb.RawFlow_DECODER_SFLOW)
	collector.ObserveReceived(flowpb.RawFlow_DECODER_NETFLOW)
	collectorServer, err := startIntegrationMetricsServer(collector.Handler())
	if err != nil {
		t.Fatal(err)
	}
	defer shutdownIntegrationServer(t, collectorServer)

	worker, err := NewWorker(
		func() flowstream.ConsumerStats {
			return flowstream.ConsumerStats{Records: 11, Bytes: 1024, Rebalances: 2, AssignedPartitions: 4, LagKnownPartitions: 3, LagRecords: 33}
		},
		func() flowworker.ProcessorStats { return flowworker.ProcessorStats{Datagrams: 10, Records: 12} },
		func() flowch.PipelineStats { return flowch.PipelineStats{Records: 12, SamplingUnknown: 1} },
		func() flowch.WriterStats { return flowch.WriterStats{InsertAttempts: 2, Blocks: 2, Rows: 12} },
	)
	if err != nil {
		t.Fatal(err)
	}
	workerServer, err := startIntegrationMetricsServer(worker.Handler())
	if err != nil {
		t.Fatal(err)
	}
	defer shutdownIntegrationServer(t, workerServer)

	importFlowMetrics(t, ctx, baseURL, collectorServer.Address(), runID, "collector")
	importFlowMetrics(t, ctx, baseURL, workerServer.Address(), runID, "worker")
	waitForVictoriaMetricsValue(t, ctx, baseURL, runID, "collector", "watchdog_flow_collector_kafka_records_total", 17)
	waitForVictoriaMetricsValue(t, ctx, baseURL, runID, "worker", "watchdog_flow_worker_kafka_lag_records", 33)
	assertVictoriaMetricsSeriesLabels(t, ctx, baseURL, runID)
}

func importFlowMetrics(t testing.TB, ctx context.Context, baseURL, address, runID, processName string) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("scrape Flow %s metrics: %v", processName, err)
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("scrape Flow %s metrics: status=%s read=%v close=%v", processName, response.Status, readErr, closeErr)
	}
	values := url.Values{}
	values.Add("extra_label", "integration_run="+runID)
	values.Add("extra_label", "process="+processName)
	endpoint := baseURL + "/api/v1/import/prometheus?" + values.Encode()
	request, err = http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", contentType)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("import Flow %s metrics into VictoriaMetrics: %v", processName, err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	closeErr = response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 || closeErr != nil {
		t.Fatalf("import Flow %s metrics into VictoriaMetrics: status=%s close=%v", processName, response.Status, closeErr)
	}
}

func waitForVictoriaMetricsValue(t testing.TB, ctx context.Context, baseURL, runID, processName, metric string, want float64) {
	t.Helper()
	query := fmt.Sprintf(`%s{integration_run=%q,process=%q}`, metric, runID, processName)
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		got, found, err := queryVictoriaMetricsValue(ctx, baseURL, query)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			if got != want {
				t.Fatalf("VictoriaMetrics query %q=%v, want %v", query, got, want)
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("VictoriaMetrics query %q: %v", query, ctx.Err())
		case <-deadline.C:
			t.Fatalf("VictoriaMetrics query %q did not become visible", query)
		case <-ticker.C:
		}
	}
}

func queryVictoriaMetricsValue(ctx context.Context, baseURL, query string) (float64, bool, error) {
	endpoint := baseURL + "/api/v1/query?" + url.Values{"query": []string{query}, "nocache": []string{"1"}}.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, false, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, false, err
	}
	defer response.Body.Close()
	var result struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value []json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if response.StatusCode != http.StatusOK {
		return 0, false, fmt.Errorf("VictoriaMetrics query status %s", response.Status)
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return 0, false, err
	}
	if result.Status != "success" || len(result.Data.Result) == 0 {
		return 0, false, nil
	}
	if len(result.Data.Result) != 1 || len(result.Data.Result[0].Value) != 2 {
		return 0, false, fmt.Errorf("VictoriaMetrics query returned an invalid vector")
	}
	var value string
	if err := json.Unmarshal(result.Data.Result[0].Value[1], &value); err != nil {
		return 0, false, err
	}
	parsed, err := strconv.ParseFloat(value, 64)
	return parsed, true, err
}

func assertVictoriaMetricsSeriesLabels(t testing.TB, ctx context.Context, baseURL, runID string) {
	t.Helper()
	matcher := fmt.Sprintf(`{integration_run=%q}`, runID)
	endpoint := baseURL + "/api/v1/series?" + url.Values{"match[]": []string{matcher}}.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result struct {
		Status string              `json:"status"`
		Data   []map[string]string `json:"data"`
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("VictoriaMetrics series status=%s", response.Status)
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "success" || len(result.Data) < 2 {
		t.Fatalf("VictoriaMetrics integration series=%+v", result)
	}
	for _, labels := range result.Data {
		for _, forbidden := range []string{"tenant", "tenant_id", "exporter", "exporter_id", "asn", "prefix", "source_ip", "topic", "partition"} {
			if _, exists := labels[forbidden]; exists {
				t.Fatalf("VictoriaMetrics Flow series contains forbidden label %q: %+v", forbidden, labels)
			}
		}
	}
}

func deleteVictoriaMetricsSeries(ctx context.Context, baseURL, runID string) error {
	matcher := fmt.Sprintf(`{integration_run=%q}`, runID)
	endpoint := baseURL + "/api/v1/admin/tsdb/delete_series?" + url.Values{"match[]": []string{matcher}}.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("VictoriaMetrics delete series status %s", response.Status)
	}
	return nil
}

func shutdownIntegrationServer(t testing.TB, server *Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Errorf("shut down Flow metrics server: %v", err)
	}
}

func startIntegrationMetricsServer(handler http.Handler) (*Server, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	return startServerOnListener(listener, handler), nil
}

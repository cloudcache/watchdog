// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowmetrics

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

func TestCollectorMetricsHaveFixedLowCardinalityContract(t *testing.T) {
	metrics, err := NewCollector(7, time.Now().Add(-time.Minute), func() flowstream.ProducerStats {
		return flowstream.ProducerStats{Records: 3, Bytes: 400, Errors: 1, BufferedRecords: 2, ProduceDurationNanos: uint64(2 * time.Second)}
	})
	if err != nil {
		t.Fatal(err)
	}
	metrics.ObserveReceived(flowpb.RawFlow_DECODER_SFLOW)
	metrics.ObserveReceived(flowpb.RawFlow_DECODER_NETFLOW)
	metrics.ObserveRejected()
	metrics.ObserveInvalid()
	metrics.ObserveOversize()
	metrics.ObserveKernelDrops(4)
	body := scrape(t, metrics.Handler())
	for _, expected := range []string{
		`watchdog_flow_collector_datagrams_received_total{decoder="sflow"} 1`,
		`watchdog_flow_collector_datagrams_received_total{decoder="netflow"} 1`,
		"watchdog_flow_collector_datagrams_rejected_total 1",
		"watchdog_flow_collector_datagrams_oversize_total 1",
		"watchdog_flow_collector_kernel_drops_total 4",
		"watchdog_flow_collector_kafka_records_total 3",
		"watchdog_flow_collector_kafka_buffered_records 2",
		"watchdog_flow_collector_kafka_produce_duration_seconds_total 2",
		"watchdog_flow_collector_plan_revision 7",
		"watchdog_flow_process_cpu_seconds_total",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("metrics missing %q:\n%s", expected, body)
		}
	}
	for _, forbidden := range []string{"tenant=", "exporter=", "asn=", "prefix=", "source_ip="} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("metrics contain forbidden high-cardinality label %q", forbidden)
		}
	}
}

func TestCollectorMetricsCountConcurrentObservations(t *testing.T) {
	metrics, err := NewCollector(1, time.Now(), func() flowstream.ProducerStats { return flowstream.ProducerStats{} })
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for index := 0; index < 100; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			metrics.ObserveReceived(flowpb.RawFlow_DECODER_SFLOW)
			metrics.ObserveKernelDrops(2)
		}()
	}
	wait.Wait()
	body := scrape(t, metrics.Handler())
	for _, expected := range []string{
		`watchdog_flow_collector_datagrams_received_total{decoder="sflow"} 100`,
		"watchdog_flow_collector_kernel_drops_total 200",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("metrics missing %q:\n%s", expected, body)
		}
	}
}

func TestWorkerMetricsExposeDurableAndFailureCounters(t *testing.T) {
	metrics, err := NewWorker(
		func() flowstream.ConsumerStats {
			return flowstream.ConsumerStats{
				Records: 5, Bytes: 600, Errors: 1, Rebalances: 2, LostPartitions: 1,
				AssignedPartitions: 4, LagKnownPartitions: 3, LagRecords: 25,
				Polls: 7, PollDurationNanos: uint64(1500 * time.Millisecond),
			}
		},
		func() flowworker.ProcessorStats {
			return flowworker.ProcessorStats{Datagrams: 4, Records: 5, TemplateMissing: 2, Rejected: 1, RetryableErrors: 1}
		},
		func() flowch.PipelineStats {
			return flowch.PipelineStats{Records: 5, SamplingUnknown: 2, SamplingConflict: 1, SnapshotMiss: 3}
		},
		func() flowch.WriterStats {
			return flowch.WriterStats{InsertAttempts: 4, RetryableErrors: 1, PermanentErrors: 1, Retries: 1, Blocks: 2, Rows: 5, InsertDurationNanos: uint64(2 * time.Second), RetryingNow: 1, BudgetExceeded: 2}
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	body := scrape(t, metrics.Handler())
	for _, expected := range []string{
		"watchdog_flow_worker_kafka_records_total 5",
		"watchdog_flow_worker_kafka_rebalances_total 2",
		"watchdog_flow_worker_kafka_lost_partitions_total 1",
		"watchdog_flow_worker_kafka_assigned_partitions 4",
		"watchdog_flow_worker_kafka_lag_known_partitions 3",
		"watchdog_flow_worker_kafka_lag_records 25",
		"watchdog_flow_worker_kafka_polls_total 7",
		"watchdog_flow_worker_kafka_poll_duration_seconds_total 1.5",
		"watchdog_flow_worker_records_persisted_total 5",
		"watchdog_flow_worker_sampling_unknown_records_total 2",
		"watchdog_flow_worker_snapshot_miss_total 3",
		`watchdog_flow_clickhouse_insert_errors_total{class="retryable"} 1`,
		`watchdog_flow_clickhouse_insert_errors_total{class="permanent"} 1`,
		"watchdog_flow_clickhouse_insert_duration_seconds_total 2",
		"watchdog_flow_clickhouse_blocks_retrying 1",
		"watchdog_flow_clickhouse_insert_budget_exceeded_total 2",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("metrics missing %q:\n%s", expected, body)
		}
	}
}

func TestMetricsHandlerAndServerLifecycle(t *testing.T) {
	handler := metricsHandler(func(output io.Writer) { writeUint(output, "watchdog_flow_test_total", "Test counter.", "counter", 1) })
	post := httptest.NewRecorder()
	handler.ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if post.Code != http.StatusMethodNotAllowed || post.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("POST status=%d allow=%q", post.Code, post.Header().Get("Allow"))
	}

	listener := &blockingListener{closed: make(chan struct{})}
	server := startServerOnListener(listener, handler)
	if server.Address() != "test-listener" {
		t.Fatalf("server address=%q", server.Address())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := server.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateListenAddress(t *testing.T) {
	for _, valid := range []string{"", "127.0.0.1:9090", "[::1]:9090", ":9090"} {
		if err := ValidateListenAddress(valid); err != nil {
			t.Fatalf("ValidateListenAddress(%q) = %v", valid, err)
		}
	}
	for _, invalid := range []string{"127.0.0.1", "http://127.0.0.1:9090", "127.0.0.1:0", "127.0.0.1:70000"} {
		if err := ValidateListenAddress(invalid); err == nil {
			t.Fatalf("ValidateListenAddress(%q) succeeded", invalid)
		}
	}
}

type blockingListener struct {
	closed chan struct{}
	once   sync.Once
}

func (l *blockingListener) Accept() (net.Conn, error) {
	<-l.closed
	return nil, net.ErrClosed
}

func (l *blockingListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *blockingListener) Addr() net.Addr { return testAddress("test-listener") }

type testAddress string

func (a testAddress) Network() string { return "test" }
func (a testAddress) String() string  { return string(a) }

func scrape(t *testing.T, handler http.Handler) string {
	t.Helper()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != contentType {
		t.Fatalf("status=%d content-type=%q", response.Code, response.Header().Get("Content-Type"))
	}
	return response.Body.String()
}

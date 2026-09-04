package flowcollect

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestObservabilityMetricsAreBoundedAndExposePipelineState(t *testing.T) {
	server, metrics, runtime, closeStores := testObservabilityServer(t)
	defer closeStores()
	runtime.start(time.Unix(1700000000, 0))
	metrics.recordReceived(ProtocolSFlow5)
	metrics.recordDecodedRecords(ProtocolSFlow5, 3)
	metrics.recordSequenceGap(ProtocolSFlow5, true, 2)
	metrics.recordSamplingRateChange(ProtocolSFlow5)
	metrics.recordNormalized(ProtocolSFlow5, 3, nil)
	metrics.observeKafka(kafkaTopicNormalized, nil, 25*time.Millisecond)
	runtime.observeKafka(kafkaTopicNormalized, nil, time.Unix(1700000001, 0))

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != prometheusContentType {
		t.Fatalf("metrics response status=%d content-type=%q", response.Code, response.Header().Get("Content-Type"))
	}
	body := response.Body.String()
	for _, expected := range []string{
		`watchdog_flow_datagrams_received_total{protocol="sflow5"} 1`,
		`watchdog_flow_records_decoded_total{protocol="sflow5"} 3`,
		`watchdog_flow_sequence_gaps_total{protocol="sflow5",scope="sample"} 1`,
		`watchdog_flow_kafka_produce_total{topic="normalized",result="success"} 1`,
		`watchdog_flow_kafka_produce_latency_seconds_bucket{topic="normalized",le="0.025"} 1`,
		`watchdog_flow_kafka_produce_latency_seconds_bucket{topic="normalized",le="+Inf"} 1`,
		`watchdog_flow_kafka_produce_latency_seconds_sum{topic="normalized"} 0.025`,
		`watchdog_flow_runtime_running 1`,
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("metrics output does not contain %q", expected)
		}
	}
	for _, forbidden := range []string{"exporter_id=", "source_ip=", "target_id=", "message_id="} {
		if strings.Contains(body, forbidden) {
			t.Errorf("metrics output contains forbidden high-cardinality label %q", forbidden)
		}
	}
}

func TestObservabilityReadinessFailsClosedAndRecovers(t *testing.T) {
	server, _, runtime, closeStores := testObservabilityServer(t)
	defer closeStores()
	assertReadyStatus(t, server, http.StatusServiceUnavailable, `"runner":"failed"`)

	runtime.start(time.Now())
	assertReadyStatus(t, server, http.StatusOK, `"status":"ready"`)
	runtime.observeKafka(kafkaTopicNormalized, errors.New("broker unavailable"), time.Now())
	assertReadyStatus(t, server, http.StatusServiceUnavailable, `"kafka_normalized":"failed"`)
	runtime.observeKafka(kafkaTopicNormalized, nil, time.Now())
	assertReadyStatus(t, server, http.StatusOK, `"kafka_normalized":"ok"`)
	runtime.observeQualityCheckpoint(errors.New("checkpoint failed"), time.Now())
	assertReadyStatus(t, server, http.StatusServiceUnavailable, `"quality_state":"failed"`)
	runtime.observeQualityJournal(nil, time.Now())
	assertReadyStatus(t, server, http.StatusServiceUnavailable, `"quality_state":"failed"`)
	runtime.observeQualityCheckpoint(nil, time.Now())
	assertReadyStatus(t, server, http.StatusOK, `"quality_state":"ok"`)
}

func TestObservabilityRejectsWritesAndHeadHasNoBody(t *testing.T) {
	server, _, _, closeStores := testObservabilityServer(t)
	defer closeStores()

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("POST status=%d allow=%q", response.Code, response.Header().Get("Allow"))
	}
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodHead, "/metrics", nil))
	if response.Code != http.StatusOK || response.Body.Len() != 0 {
		t.Fatalf("HEAD status=%d body=%q", response.Code, response.Body.String())
	}
}

func TestWALHardAdmissionMakesReadinessUnavailable(t *testing.T) {
	server, _, runtime, closeStores := testObservabilityServer(t)
	defer closeStores()
	runtime.start(time.Now())
	payload := make([]byte, 1400)
	for {
		_, err := server.wal.Append(testWALInput(payload))
		if errors.Is(err, ErrWALHardLimit) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	assertReadyStatus(t, server, http.StatusServiceUnavailable, `"wal":"failed"`)
}

func testObservabilityServer(t *testing.T) (*ObservabilityServer, *Metrics, *RuntimeState, func()) {
	t.Helper()
	metrics := &Metrics{}
	wal, err := OpenWAL(t.TempDir(), "collector-a", testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	qualityConfig := testQualityStateConfig()
	quality, err := OpenQualityStateStore(t.TempDir(), "collector-a", qualityConfig, NewQualityTracker(qualityConfig, metrics), wal, metrics)
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	runtime := NewRuntimeState()
	config := DefaultConfig().Observability
	server, err := NewObservabilityServer(config, metrics, wal, quality, runtime)
	if err != nil {
		_ = quality.Close()
		_ = wal.Close()
		t.Fatal(err)
	}
	return server, metrics, runtime, func() {
		_ = quality.Close()
		_ = wal.Close()
	}
}

func assertReadyStatus(t *testing.T, server *ObservabilityServer, status int, contains string) {
	t.Helper()
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if response.Code != status || !strings.Contains(response.Body.String(), contains) {
		t.Fatalf("ready status=%d body=%s", response.Code, response.Body.String())
	}
}

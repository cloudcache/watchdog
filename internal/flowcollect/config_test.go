package flowcollect

import (
	"testing"
	"time"
)

func TestDefaultConfigValid(t *testing.T) {
	config := DefaultConfig()
	config.Normalize()
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestConfigRejectsUnsafeWatermarks(t *testing.T) {
	config := DefaultConfig()
	config.WAL.SoftWatermark = .95
	config.WAL.HardWatermark = .90
	if err := config.Validate(); err == nil {
		t.Fatal("expected invalid watermarks to fail")
	}
}

func TestConfigRejectsInvalidQualityBounds(t *testing.T) {
	config := DefaultConfig()
	config.Quality.AnomalyWindow = config.Quality.StateTTL + time.Second
	if err := config.Validate(); err == nil {
		t.Fatal("expected anomaly window beyond state TTL to fail")
	}
	config = DefaultConfig()
	config.Quality.MaxDataSources = 0
	if err := config.Validate(); err == nil {
		t.Fatal("expected zero quality data-source capacity to fail")
	}
}

func TestConfigEnvironmentUsesFlowCollectNamespace(t *testing.T) {
	t.Setenv("WATCHDOG_FLOW_COLLECT_SOCKET_COUNT", "4")
	t.Setenv("WATCHDOG_FLOW_COLLECT_KAFKA_BROKERS", "kafka-a:9093, kafka-b:9093")
	t.Setenv("WATCHDOG_FLOW_COLLECT_WAL_HARD_WATERMARK", "0.85")
	t.Setenv("WATCHDOG_FLOW_COLLECT_BATCH_MAX_WAIT", "7ms")
	t.Setenv("WATCHDOG_FLOW_COLLECT_DECODER_STATE_TTL", "45m")
	t.Setenv("WATCHDOG_FLOW_COLLECT_DIAGNOSTICS_DECODE_MAX_ATTEMPTS", "5")
	t.Setenv("WATCHDOG_FLOW_COLLECT_DIAGNOSTICS_DLQ_PAYLOAD_MAX_BYTES", "1024")
	t.Setenv("WATCHDOG_FLOW_COLLECT_QUALITY_STATE_TTL", "2h")
	t.Setenv("WATCHDOG_FLOW_COLLECT_QUALITY_ANOMALY_WINDOW", "2m")
	t.Setenv("WATCHDOG_FLOW_COLLECT_QUALITY_MAX_EXPORTERS", "1000")
	t.Setenv("WATCHDOG_FLOW_COLLECT_QUALITY_MAX_DATA_SOURCES", "5000")
	t.Setenv("WATCHDOG_FLOW_COLLECT_KAFKA_TLS", "false")
	config := DefaultConfig()
	if err := config.ApplyEnv(); err != nil {
		t.Fatal(err)
	}
	if config.SocketCount != 4 || len(config.Kafka.Brokers) != 2 || config.WAL.HardWatermark != .85 || config.NormalizedBatch.MaxWait.String() != "7ms" || config.DecoderStateTTL != 45*time.Minute || config.Diagnostics.DecodeMaxAttempts != 5 || config.Diagnostics.DLQPayloadMaxBytes != 1024 || config.Quality.StateTTL != 2*time.Hour || config.Quality.AnomalyWindow != 2*time.Minute || config.Quality.MaxExporters != 1000 || config.Quality.MaxDataSources != 5000 || config.Kafka.TLS {
		t.Fatalf("environment not applied: %+v", config)
	}
}

package flowcollect

import (
	"testing"
	"time"
)

func TestDefaultConfigValid(t *testing.T) {
	config := DefaultConfig()
	if config.Kafka.QualityCheckpointWriteEnabled {
		t.Fatal("quality checkpoint writer must remain disabled until compatible readers are deployed")
	}
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

func TestConfigRejectsInvalidObservability(t *testing.T) {
	config := DefaultConfig()
	config.Observability.Listen = "127.0.0.1"
	if err := config.Validate(); err == nil {
		t.Fatal("expected invalid observability listen address to fail")
	}
	config = DefaultConfig()
	config.Observability.WriteTimeout = 0
	if err := config.Validate(); err == nil {
		t.Fatal("expected zero observability timeout to fail")
	}
}

func TestConfigRejectsInvalidCollectStateRestoreBounds(t *testing.T) {
	config := DefaultConfig()
	config.Kafka.CollectStateRestoreTimeout = 0
	if err := config.Validate(); err == nil {
		t.Fatal("expected zero collect-state restore timeout to fail")
	}
	config = DefaultConfig()
	config.Kafka.CollectStateRestoreMaxCandidates = 0
	if err := config.Validate(); err == nil {
		t.Fatal("expected zero collect-state candidate bound to fail")
	}
}

func TestConfigRejectsInvalidKafkaTopicAndTLSContracts(t *testing.T) {
	config := DefaultConfig()
	config.Kafka.TopicContract.MinInSyncReplicas = config.Kafka.TopicContract.MinReplicationFactor + 1
	if err := config.Validate(); err == nil {
		t.Fatal("expected min ISR greater than replication factor to fail")
	}
	config = DefaultConfig()
	config.Kafka.TLSCertFile = "/tmp/client.crt"
	if err := config.Validate(); err == nil {
		t.Fatal("expected partial Kafka mTLS credentials to fail")
	}
	config = DefaultConfig()
	config.Kafka.TLS = false
	config.Kafka.TLSCAFile = "/tmp/ca.crt"
	if err := config.Validate(); err == nil {
		t.Fatal("expected TLS material with TLS disabled to fail")
	}
	config = DefaultConfig()
	config.Kafka.DecodeDLQTopic = config.Kafka.NormalizedTopic
	if err := config.Validate(); err == nil {
		t.Fatal("expected a topic shared by two roles to fail")
	}
	config = DefaultConfig()
	config.NormalizedBatch.MaxBytes = collectStateMaxBytes + 1
	if err := config.Validate(); err == nil {
		t.Fatal("expected an unsupported Kafka batch size to fail")
	}
}

func TestRuntimeConfigRequiresKafkaMTLSIdentity(t *testing.T) {
	config := DefaultConfig()
	config.PlanFile = "/tmp/plan.json"
	config.PlanPublicKeyFile = "/tmp/plan.pub"
	config.Kafka.Brokers = []string{"kafka.internal:9093"}
	if err := config.ValidateRuntime(); err == nil {
		t.Fatal("Kafka TLS without a client identity was accepted at runtime")
	}
	config.Kafka.TLSCertFile = "/tmp/collector.crt"
	config.Kafka.TLSKeyFile = "/tmp/collector.key"
	if err := config.ValidateRuntime(); err != nil {
		t.Fatalf("Kafka mTLS runtime config was rejected: %v", err)
	}
}

func TestConfigRequiresPlanHistoryActiveAndAntiRollbackSlots(t *testing.T) {
	config := DefaultConfig()
	config.PlanHistoryMaxEntries = 2
	if err := config.Validate(); err == nil {
		t.Fatal("expected one-entry plan history to fail")
	}
}

func TestConfigRejectsInvalidAttemptJournalBounds(t *testing.T) {
	config := DefaultConfig()
	config.Diagnostics.AttemptJournalMaxBytes = attemptHeaderSize
	if err := config.Validate(); err == nil {
		t.Fatal("expected undersized attempt journal to fail")
	}
	config = DefaultConfig()
	config.Diagnostics.AttemptJournalFsync = 0
	if err := config.Validate(); err == nil {
		t.Fatal("expected zero attempt journal fsync interval to fail")
	}
}

func TestConfigEnvironmentUsesFlowCollectNamespace(t *testing.T) {
	t.Setenv("WATCHDOG_FLOW_COLLECT_SOCKET_COUNT", "4")
	t.Setenv("WATCHDOG_FLOW_COLLECT_KAFKA_BROKERS", "kafka-a:9093, kafka-b:9093")
	t.Setenv("WATCHDOG_FLOW_COLLECT_KAFKA_COLLECT_STATE_RESTORE_TIMEOUT", "3m")
	t.Setenv("WATCHDOG_FLOW_COLLECT_KAFKA_COLLECT_STATE_RESTORE_MAX_CANDIDATES", "1234")
	t.Setenv("WATCHDOG_FLOW_COLLECT_WAL_HARD_WATERMARK", "0.85")
	t.Setenv("WATCHDOG_FLOW_COLLECT_BATCH_MAX_WAIT", "7ms")
	t.Setenv("WATCHDOG_FLOW_COLLECT_PLAN_HISTORY_MAX_ENTRIES", "64")
	t.Setenv("WATCHDOG_FLOW_COLLECT_DECODER_STATE_TTL", "45m")
	t.Setenv("WATCHDOG_FLOW_COLLECT_DIAGNOSTICS_DECODE_MAX_ATTEMPTS", "5")
	t.Setenv("WATCHDOG_FLOW_COLLECT_DIAGNOSTICS_DLQ_PAYLOAD_MAX_BYTES", "1024")
	t.Setenv("WATCHDOG_FLOW_COLLECT_DIAGNOSTICS_ATTEMPT_JOURNAL_FSYNC_INTERVAL", "4ms")
	t.Setenv("WATCHDOG_FLOW_COLLECT_DIAGNOSTICS_ATTEMPT_CHECKPOINT_INTERVAL", "11m")
	t.Setenv("WATCHDOG_FLOW_COLLECT_DIAGNOSTICS_ATTEMPT_JOURNAL_MAX_BYTES", "3145728")
	t.Setenv("WATCHDOG_FLOW_COLLECT_QUALITY_STATE_TTL", "2h")
	t.Setenv("WATCHDOG_FLOW_COLLECT_QUALITY_ANOMALY_WINDOW", "2m")
	t.Setenv("WATCHDOG_FLOW_COLLECT_QUALITY_JOURNAL_FSYNC_INTERVAL", "3ms")
	t.Setenv("WATCHDOG_FLOW_COLLECT_QUALITY_CHECKPOINT_INTERVAL", "10m")
	t.Setenv("WATCHDOG_FLOW_COLLECT_QUALITY_JOURNAL_MAX_BYTES", "2097152")
	t.Setenv("WATCHDOG_FLOW_COLLECT_QUALITY_MAX_EXPORTERS", "1000")
	t.Setenv("WATCHDOG_FLOW_COLLECT_QUALITY_MAX_DATA_SOURCES", "5000")
	t.Setenv("WATCHDOG_FLOW_COLLECT_OBSERVABILITY_LISTEN", "127.0.0.1:19464")
	t.Setenv("WATCHDOG_FLOW_COLLECT_OBSERVABILITY_WRITE_TIMEOUT", "9s")
	t.Setenv("WATCHDOG_FLOW_COLLECT_KAFKA_TLS", "false")
	t.Setenv("WATCHDOG_FLOW_COLLECT_KAFKA_NORMALIZED_PARTITIONS", "128")
	t.Setenv("WATCHDOG_FLOW_COLLECT_KAFKA_COLLECT_STATE_PARTITIONS", "16")
	t.Setenv("WATCHDOG_FLOW_COLLECT_KAFKA_QUALITY_CHECKPOINT_WRITE_ENABLED", "true")
	config := DefaultConfig()
	if err := config.ApplyEnv(); err != nil {
		t.Fatal(err)
	}
	if config.SocketCount != 4 || len(config.Kafka.Brokers) != 2 || config.Kafka.CollectStateRestoreTimeout != 3*time.Minute || config.Kafka.CollectStateRestoreMaxCandidates != 1234 || !config.Kafka.QualityCheckpointWriteEnabled || config.Kafka.TopicContract.NormalizedPartitions != 128 || config.Kafka.TopicContract.CollectStatePartitions != 16 || config.WAL.HardWatermark != .85 || config.NormalizedBatch.MaxWait.String() != "7ms" || config.PlanHistoryMaxEntries != 64 || config.DecoderStateTTL != 45*time.Minute || config.Diagnostics.DecodeMaxAttempts != 5 || config.Diagnostics.DLQPayloadMaxBytes != 1024 || config.Diagnostics.AttemptJournalFsync != 4*time.Millisecond || config.Diagnostics.AttemptCheckpointEvery != 11*time.Minute || config.Diagnostics.AttemptJournalMaxBytes != 3<<20 || config.Quality.StateTTL != 2*time.Hour || config.Quality.AnomalyWindow != 2*time.Minute || config.Quality.JournalFsync != 3*time.Millisecond || config.Quality.CheckpointEvery != 10*time.Minute || config.Quality.JournalMaxBytes != 2<<20 || config.Quality.MaxExporters != 1000 || config.Quality.MaxDataSources != 5000 || config.Observability.Listen != "127.0.0.1:19464" || config.Observability.WriteTimeout != 9*time.Second || config.Kafka.TLS {
		t.Fatalf("environment not applied: %+v", config)
	}
}

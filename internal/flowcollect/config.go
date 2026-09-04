package flowcollect

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const VirtualShardCount = 4096

type Config struct {
	ControlPlaneURL       string              `yaml:"control_plane_url"`
	StateDir              string              `yaml:"state_dir"`
	PlanFile              string              `yaml:"plan_file"`
	PlanPublicKeyFile     string              `yaml:"plan_public_key_file"`
	SFlowListen           string              `yaml:"sflow_listen"`
	NetFlowListen         string              `yaml:"netflow_listen"`
	SocketCount           int                 `yaml:"socket_count"`
	DecodeWorkers         int                 `yaml:"decode_workers"`
	DecodeQueueDatagrams  int                 `yaml:"decode_queue_datagrams"`
	ReceiveBufferBytes    int                 `yaml:"receive_buffer_bytes"`
	MaxDatagramBytes      int                 `yaml:"max_datagram_bytes"`
	PlanRefreshInterval   time.Duration       `yaml:"plan_refresh_interval"`
	PlanHistoryMaxEntries int                 `yaml:"plan_history_max_entries"`
	ExporterRefreshPeriod time.Duration       `yaml:"exporter_refresh_interval"`
	DecoderStateTTL       time.Duration       `yaml:"decoder_state_ttl"`
	WAL                   WALConfig           `yaml:"wal"`
	Kafka                 KafkaConfig         `yaml:"kafka"`
	NormalizedBatch       NormalizedBatchCfg  `yaml:"normalized_batch"`
	Diagnostics           DiagnosticsConfig   `yaml:"diagnostics"`
	Quality               QualityConfig       `yaml:"quality"`
	Observability         ObservabilityConfig `yaml:"observability"`
}

type WALConfig struct {
	MaxBytes      int64         `yaml:"max_bytes"`
	MaxAge        time.Duration `yaml:"max_age"`
	SegmentBytes  int64         `yaml:"segment_bytes"`
	FsyncInterval time.Duration `yaml:"fsync_interval"`
	SoftWatermark float64       `yaml:"soft_watermark"`
	HardWatermark float64       `yaml:"hard_watermark"`
}

type KafkaConfig struct {
	Brokers                          []string      `yaml:"brokers"`
	NormalizedTopic                  string        `yaml:"normalized_topic"`
	CollectStateTopic                string        `yaml:"collect_state_topic"`
	CollectStateRestoreTimeout       time.Duration `yaml:"collect_state_restore_timeout"`
	CollectStateRestoreMaxCandidates int           `yaml:"collect_state_restore_max_candidates"`
	DecodeDLQTopic                   string        `yaml:"decode_dlq_topic"`
	QuarantineTopic                  string        `yaml:"quarantine_topic"`
	Acks                             string        `yaml:"acks"`
	Compression                      string        `yaml:"compression"`
	TLS                              bool          `yaml:"tls"`
}

type NormalizedBatchCfg struct {
	MaxRecords int           `yaml:"max_records"`
	MaxBytes   int           `yaml:"max_bytes"`
	MaxWait    time.Duration `yaml:"max_wait"`
}

type DiagnosticsConfig struct {
	DecodeMaxAttempts                  int           `yaml:"decode_max_attempts"`
	RetryInitial                       time.Duration `yaml:"retry_initial"`
	RetryMax                           time.Duration `yaml:"retry_max"`
	QuarantineQueueEvents              int           `yaml:"quarantine_queue_events"`
	QuarantineMaxEventsPerSecond       int           `yaml:"quarantine_max_events_per_second"`
	QuarantineMaxEventsPerSourceSecond int           `yaml:"quarantine_max_events_per_source_second"`
	DLQPayloadMaxBytes                 int           `yaml:"dlq_payload_max_bytes"`
}

type QualityConfig struct {
	StateTTL        time.Duration `yaml:"state_ttl"`
	AnomalyWindow   time.Duration `yaml:"anomaly_window"`
	JournalFsync    time.Duration `yaml:"journal_fsync_interval"`
	CheckpointEvery time.Duration `yaml:"checkpoint_interval"`
	JournalMaxBytes int64         `yaml:"journal_max_bytes"`
	MaxExporters    int           `yaml:"max_exporters"`
	MaxDataSources  int           `yaml:"max_data_sources"`
}

type ObservabilityConfig struct {
	Listen            string        `yaml:"listen"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	ShutdownTimeout   time.Duration `yaml:"shutdown_timeout"`
}

func DefaultConfig() Config {
	return Config{
		StateDir:              "/var/lib/watchdog-flow-collect",
		SFlowListen:           ":6343",
		NetFlowListen:         ":2055",
		SocketCount:           1,
		DecodeWorkers:         1,
		DecodeQueueDatagrams:  65536,
		ReceiveBufferBytes:    32 << 20,
		MaxDatagramBytes:      65535,
		PlanRefreshInterval:   30 * time.Second,
		PlanHistoryMaxEntries: 128,
		ExporterRefreshPeriod: 30 * time.Second,
		DecoderStateTTL:       30 * time.Minute,
		WAL: WALConfig{
			MaxBytes:      100 << 30,
			MaxAge:        24 * time.Hour,
			SegmentBytes:  128 << 20,
			FsyncInterval: 10 * time.Millisecond,
			SoftWatermark: .70,
			HardWatermark: .90,
		},
		Kafka: KafkaConfig{
			NormalizedTopic:                  "watchdog.flow.normalized.v1",
			CollectStateTopic:                "watchdog.flow.collect-state.v1",
			CollectStateRestoreTimeout:       2 * time.Minute,
			CollectStateRestoreMaxCandidates: 262144,
			DecodeDLQTopic:                   "watchdog.flow.decode-dlq.v1",
			QuarantineTopic:                  "watchdog.flow.quarantine.v1",
			Acks:                             "all",
			Compression:                      "zstd",
			TLS:                              true,
		},
		NormalizedBatch: NormalizedBatchCfg{
			MaxRecords: 1024,
			MaxBytes:   1 << 20,
			MaxWait:    5 * time.Millisecond,
		},
		Diagnostics: DiagnosticsConfig{
			DecodeMaxAttempts:                  3,
			RetryInitial:                       250 * time.Millisecond,
			RetryMax:                           5 * time.Second,
			QuarantineQueueEvents:              4096,
			QuarantineMaxEventsPerSecond:       100,
			QuarantineMaxEventsPerSourceSecond: 2,
			DLQPayloadMaxBytes:                 0,
		},
		Quality: QualityConfig{
			StateTTL:        time.Hour,
			AnomalyWindow:   time.Minute,
			JournalFsync:    10 * time.Millisecond,
			CheckpointEvery: 5 * time.Minute,
			JournalMaxBytes: 512 << 20,
			MaxExporters:    65536,
			MaxDataSources:  262144,
		},
		Observability: ObservabilityConfig{
			Listen:            "127.0.0.1:9464",
			ReadHeaderTimeout: 2 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       30 * time.Second,
			ShutdownTimeout:   5 * time.Second,
		},
	}
}

func (c *Config) Normalize() {
	c.ControlPlaneURL = strings.TrimRight(strings.TrimSpace(c.ControlPlaneURL), "/")
	c.StateDir = filepath.Clean(strings.TrimSpace(c.StateDir))
	c.PlanFile = strings.TrimSpace(c.PlanFile)
	if c.PlanFile == "" && c.StateDir != "." {
		c.PlanFile = filepath.Join(c.StateDir, "plan.json")
	} else if c.PlanFile != "" {
		c.PlanFile = filepath.Clean(c.PlanFile)
	}
	c.PlanPublicKeyFile = strings.TrimSpace(c.PlanPublicKeyFile)
	if c.PlanPublicKeyFile != "" {
		c.PlanPublicKeyFile = filepath.Clean(c.PlanPublicKeyFile)
	}
	c.SFlowListen = strings.TrimSpace(c.SFlowListen)
	c.NetFlowListen = strings.TrimSpace(c.NetFlowListen)
	c.Observability.Listen = strings.TrimSpace(c.Observability.Listen)
	seenBrokers := make(map[string]struct{}, len(c.Kafka.Brokers))
	brokers := make([]string, 0, len(c.Kafka.Brokers))
	for _, broker := range c.Kafka.Brokers {
		broker = strings.TrimSpace(broker)
		if broker == "" {
			continue
		}
		if _, exists := seenBrokers[broker]; exists {
			continue
		}
		seenBrokers[broker] = struct{}{}
		brokers = append(brokers, broker)
	}
	c.Kafka.Brokers = brokers
	c.Kafka.NormalizedTopic = strings.TrimSpace(c.Kafka.NormalizedTopic)
	c.Kafka.CollectStateTopic = strings.TrimSpace(c.Kafka.CollectStateTopic)
	c.Kafka.DecodeDLQTopic = strings.TrimSpace(c.Kafka.DecodeDLQTopic)
	c.Kafka.QuarantineTopic = strings.TrimSpace(c.Kafka.QuarantineTopic)
	c.Kafka.Acks = strings.TrimSpace(c.Kafka.Acks)
	c.Kafka.Compression = strings.TrimSpace(c.Kafka.Compression)
}

func (c Config) Validate() error {
	if c.StateDir == "" || c.StateDir == "." {
		return errors.New("flow_collect.state_dir is required")
	}
	if c.ControlPlaneURL != "" {
		u, err := url.Parse(c.ControlPlaneURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return errors.New("flow_collect.control_plane_url must be an absolute http(s) URL")
		}
	}
	for name, addr := range map[string]string{
		"flow_collect.sflow_listen":         c.SFlowListen,
		"flow_collect.netflow_listen":       c.NetFlowListen,
		"flow_collect.observability.listen": c.Observability.Listen,
	} {
		if err := validateListen(name, addr); err != nil {
			return err
		}
	}
	if c.SocketCount <= 0 || c.DecodeWorkers <= 0 || c.DecodeQueueDatagrams <= 0 || c.ReceiveBufferBytes <= 0 {
		return errors.New("flow_collect socket_count, decode_workers, decode_queue_datagrams, and receive_buffer_bytes must be positive")
	}
	if c.MaxDatagramBytes < 1500 || c.MaxDatagramBytes > 65535 {
		return errors.New("flow_collect.max_datagram_bytes must be between 1500 and 65535")
	}
	if c.PlanRefreshInterval <= 0 || c.PlanHistoryMaxEntries <= 0 || c.ExporterRefreshPeriod <= 0 || c.DecoderStateTTL <= 0 {
		return errors.New("flow_collect refresh intervals and decoder_state_ttl must be positive")
	}
	if c.WAL.MaxBytes <= 0 || c.WAL.SegmentBytes <= 0 || c.WAL.SegmentBytes > c.WAL.MaxBytes || c.WAL.MaxAge <= 0 || c.WAL.FsyncInterval <= 0 {
		return errors.New("flow_collect.wal sizes and intervals must be positive and segment_bytes <= max_bytes")
	}
	if c.WAL.SoftWatermark <= 0 || c.WAL.HardWatermark >= 1 || c.WAL.SoftWatermark >= c.WAL.HardWatermark {
		return errors.New("flow_collect.wal watermarks must satisfy 0 < soft < hard < 1")
	}
	if c.NormalizedBatch.MaxRecords <= 0 || c.NormalizedBatch.MaxBytes <= 0 || c.NormalizedBatch.MaxWait <= 0 {
		return errors.New("flow_collect.normalized_batch limits must be positive")
	}
	if c.Diagnostics.DecodeMaxAttempts <= 0 || c.Diagnostics.RetryInitial <= 0 || c.Diagnostics.RetryMax < c.Diagnostics.RetryInitial || c.Diagnostics.QuarantineQueueEvents <= 0 || c.Diagnostics.QuarantineMaxEventsPerSecond <= 0 || c.Diagnostics.QuarantineMaxEventsPerSourceSecond <= 0 {
		return errors.New("flow_collect.diagnostics retry and quarantine limits are invalid")
	}
	if c.Diagnostics.QuarantineMaxEventsPerSourceSecond > c.Diagnostics.QuarantineMaxEventsPerSecond {
		return errors.New("flow_collect.diagnostics per-source quarantine rate cannot exceed the global rate")
	}
	if c.Diagnostics.DLQPayloadMaxBytes < 0 || c.Diagnostics.DLQPayloadMaxBytes > c.MaxDatagramBytes {
		return errors.New("flow_collect.diagnostics.dlq_payload_max_bytes must be between 0 and max_datagram_bytes")
	}
	if c.Quality.StateTTL <= 0 || c.Quality.AnomalyWindow <= 0 || c.Quality.AnomalyWindow > c.Quality.StateTTL || c.Quality.JournalFsync <= 0 || c.Quality.CheckpointEvery <= 0 || c.Quality.JournalMaxBytes <= qualityFrameHeaderSize || c.Quality.MaxExporters <= 0 || c.Quality.MaxDataSources <= 0 {
		return errors.New("flow_collect.quality requires positive limits and anomaly_window <= state_ttl")
	}
	if c.Observability.ReadHeaderTimeout <= 0 || c.Observability.WriteTimeout <= 0 || c.Observability.IdleTimeout <= 0 || c.Observability.ShutdownTimeout <= 0 {
		return errors.New("flow_collect.observability timeouts must be positive")
	}
	if c.Kafka.Acks != "all" {
		return errors.New("flow_collect.kafka.acks must be all")
	}
	if c.Kafka.CollectStateRestoreTimeout <= 0 || c.Kafka.CollectStateRestoreMaxCandidates <= 0 {
		return errors.New("flow_collect.kafka collect-state restore limits must be positive")
	}
	switch c.Kafka.Compression {
	case "none", "gzip", "snappy", "lz4", "zstd":
	default:
		return errors.New("flow_collect.kafka.compression must be none, gzip, snappy, lz4, or zstd")
	}
	for _, broker := range c.Kafka.Brokers {
		if err := validateListen("flow_collect.kafka.brokers", broker); err != nil {
			return err
		}
	}
	return nil
}

func (c Config) ValidateRuntime() error {
	if err := c.Validate(); err != nil {
		return err
	}
	if c.PlanFile == "" || c.PlanPublicKeyFile == "" {
		return errors.New("flow_collect.plan_file and plan_public_key_file are required at runtime")
	}
	if len(c.Kafka.Brokers) == 0 {
		return errors.New("flow_collect.kafka.brokers is required at runtime")
	}
	if !c.Kafka.TLS {
		for _, broker := range c.Kafka.Brokers {
			host, _, _ := net.SplitHostPort(broker)
			ip := net.ParseIP(host)
			if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
				return errors.New("flow_collect.kafka.tls may be disabled only for loopback development brokers")
			}
		}
	}
	if c.Kafka.NormalizedTopic == "" || c.Kafka.CollectStateTopic == "" || c.Kafka.DecodeDLQTopic == "" || c.Kafka.QuarantineTopic == "" {
		return errors.New("all flow_collect.kafka topics are required at runtime")
	}
	return nil
}

func (c *Config) ApplyEnv() error {
	var err error
	c.ControlPlaneURL = env("WATCHDOG_FLOW_COLLECT_CONTROL_PLANE_URL", c.ControlPlaneURL)
	c.StateDir = env("WATCHDOG_FLOW_COLLECT_STATE_DIR", c.StateDir)
	c.PlanFile = env("WATCHDOG_FLOW_COLLECT_PLAN_FILE", c.PlanFile)
	c.PlanPublicKeyFile = env("WATCHDOG_FLOW_COLLECT_PLAN_PUBLIC_KEY_FILE", c.PlanPublicKeyFile)
	c.SFlowListen = env("WATCHDOG_FLOW_COLLECT_SFLOW_LISTEN", c.SFlowListen)
	c.NetFlowListen = env("WATCHDOG_FLOW_COLLECT_NETFLOW_LISTEN", c.NetFlowListen)
	if c.SocketCount, err = envInt("WATCHDOG_FLOW_COLLECT_SOCKET_COUNT", c.SocketCount); err != nil {
		return err
	}
	if c.DecodeWorkers, err = envInt("WATCHDOG_FLOW_COLLECT_DECODE_WORKERS", c.DecodeWorkers); err != nil {
		return err
	}
	if c.DecodeQueueDatagrams, err = envInt("WATCHDOG_FLOW_COLLECT_DECODE_QUEUE_DATAGRAMS", c.DecodeQueueDatagrams); err != nil {
		return err
	}
	if c.ReceiveBufferBytes, err = envInt("WATCHDOG_FLOW_COLLECT_RECEIVE_BUFFER_BYTES", c.ReceiveBufferBytes); err != nil {
		return err
	}
	if c.MaxDatagramBytes, err = envInt("WATCHDOG_FLOW_COLLECT_MAX_DATAGRAM_BYTES", c.MaxDatagramBytes); err != nil {
		return err
	}
	if c.PlanRefreshInterval, err = envDuration("WATCHDOG_FLOW_COLLECT_PLAN_REFRESH_INTERVAL", c.PlanRefreshInterval); err != nil {
		return err
	}
	if c.PlanHistoryMaxEntries, err = envInt("WATCHDOG_FLOW_COLLECT_PLAN_HISTORY_MAX_ENTRIES", c.PlanHistoryMaxEntries); err != nil {
		return err
	}
	if c.ExporterRefreshPeriod, err = envDuration("WATCHDOG_FLOW_COLLECT_EXPORTER_REFRESH_INTERVAL", c.ExporterRefreshPeriod); err != nil {
		return err
	}
	if c.DecoderStateTTL, err = envDuration("WATCHDOG_FLOW_COLLECT_DECODER_STATE_TTL", c.DecoderStateTTL); err != nil {
		return err
	}
	if c.WAL.MaxBytes, err = envInt64("WATCHDOG_FLOW_COLLECT_WAL_MAX_BYTES", c.WAL.MaxBytes); err != nil {
		return err
	}
	if c.WAL.SegmentBytes, err = envInt64("WATCHDOG_FLOW_COLLECT_WAL_SEGMENT_BYTES", c.WAL.SegmentBytes); err != nil {
		return err
	}
	if c.WAL.FsyncInterval, err = envDuration("WATCHDOG_FLOW_COLLECT_WAL_FSYNC_INTERVAL", c.WAL.FsyncInterval); err != nil {
		return err
	}
	if c.WAL.MaxAge, err = envDuration("WATCHDOG_FLOW_COLLECT_WAL_MAX_AGE", c.WAL.MaxAge); err != nil {
		return err
	}
	if c.WAL.SoftWatermark, err = envFloat("WATCHDOG_FLOW_COLLECT_WAL_SOFT_WATERMARK", c.WAL.SoftWatermark); err != nil {
		return err
	}
	if c.WAL.HardWatermark, err = envFloat("WATCHDOG_FLOW_COLLECT_WAL_HARD_WATERMARK", c.WAL.HardWatermark); err != nil {
		return err
	}
	if v, ok := os.LookupEnv("WATCHDOG_FLOW_COLLECT_KAFKA_BROKERS"); ok {
		c.Kafka.Brokers = splitList(v)
	}
	c.Kafka.NormalizedTopic = env("WATCHDOG_FLOW_COLLECT_KAFKA_NORMALIZED_TOPIC", c.Kafka.NormalizedTopic)
	c.Kafka.CollectStateTopic = env("WATCHDOG_FLOW_COLLECT_KAFKA_COLLECT_STATE_TOPIC", c.Kafka.CollectStateTopic)
	if c.Kafka.CollectStateRestoreTimeout, err = envDuration("WATCHDOG_FLOW_COLLECT_KAFKA_COLLECT_STATE_RESTORE_TIMEOUT", c.Kafka.CollectStateRestoreTimeout); err != nil {
		return err
	}
	if c.Kafka.CollectStateRestoreMaxCandidates, err = envInt("WATCHDOG_FLOW_COLLECT_KAFKA_COLLECT_STATE_RESTORE_MAX_CANDIDATES", c.Kafka.CollectStateRestoreMaxCandidates); err != nil {
		return err
	}
	c.Kafka.DecodeDLQTopic = env("WATCHDOG_FLOW_COLLECT_KAFKA_DECODE_DLQ_TOPIC", c.Kafka.DecodeDLQTopic)
	c.Kafka.QuarantineTopic = env("WATCHDOG_FLOW_COLLECT_KAFKA_QUARANTINE_TOPIC", c.Kafka.QuarantineTopic)
	c.Kafka.Acks = env("WATCHDOG_FLOW_COLLECT_KAFKA_ACKS", c.Kafka.Acks)
	c.Kafka.Compression = env("WATCHDOG_FLOW_COLLECT_KAFKA_COMPRESSION", c.Kafka.Compression)
	if c.Kafka.TLS, err = envBool("WATCHDOG_FLOW_COLLECT_KAFKA_TLS", c.Kafka.TLS); err != nil {
		return err
	}
	if c.NormalizedBatch.MaxRecords, err = envInt("WATCHDOG_FLOW_COLLECT_BATCH_MAX_RECORDS", c.NormalizedBatch.MaxRecords); err != nil {
		return err
	}
	if c.NormalizedBatch.MaxBytes, err = envInt("WATCHDOG_FLOW_COLLECT_BATCH_MAX_BYTES", c.NormalizedBatch.MaxBytes); err != nil {
		return err
	}
	if c.NormalizedBatch.MaxWait, err = envDuration("WATCHDOG_FLOW_COLLECT_BATCH_MAX_WAIT", c.NormalizedBatch.MaxWait); err != nil {
		return err
	}
	if c.Diagnostics.DecodeMaxAttempts, err = envInt("WATCHDOG_FLOW_COLLECT_DIAGNOSTICS_DECODE_MAX_ATTEMPTS", c.Diagnostics.DecodeMaxAttempts); err != nil {
		return err
	}
	if c.Diagnostics.RetryInitial, err = envDuration("WATCHDOG_FLOW_COLLECT_DIAGNOSTICS_RETRY_INITIAL", c.Diagnostics.RetryInitial); err != nil {
		return err
	}
	if c.Diagnostics.RetryMax, err = envDuration("WATCHDOG_FLOW_COLLECT_DIAGNOSTICS_RETRY_MAX", c.Diagnostics.RetryMax); err != nil {
		return err
	}
	if c.Diagnostics.QuarantineQueueEvents, err = envInt("WATCHDOG_FLOW_COLLECT_DIAGNOSTICS_QUARANTINE_QUEUE_EVENTS", c.Diagnostics.QuarantineQueueEvents); err != nil {
		return err
	}
	if c.Diagnostics.QuarantineMaxEventsPerSecond, err = envInt("WATCHDOG_FLOW_COLLECT_DIAGNOSTICS_QUARANTINE_MAX_EVENTS_PER_SECOND", c.Diagnostics.QuarantineMaxEventsPerSecond); err != nil {
		return err
	}
	if c.Diagnostics.QuarantineMaxEventsPerSourceSecond, err = envInt("WATCHDOG_FLOW_COLLECT_DIAGNOSTICS_QUARANTINE_MAX_EVENTS_PER_SOURCE_SECOND", c.Diagnostics.QuarantineMaxEventsPerSourceSecond); err != nil {
		return err
	}
	if c.Diagnostics.DLQPayloadMaxBytes, err = envNonNegativeInt("WATCHDOG_FLOW_COLLECT_DIAGNOSTICS_DLQ_PAYLOAD_MAX_BYTES", c.Diagnostics.DLQPayloadMaxBytes); err != nil {
		return err
	}
	if c.Quality.StateTTL, err = envDuration("WATCHDOG_FLOW_COLLECT_QUALITY_STATE_TTL", c.Quality.StateTTL); err != nil {
		return err
	}
	if c.Quality.AnomalyWindow, err = envDuration("WATCHDOG_FLOW_COLLECT_QUALITY_ANOMALY_WINDOW", c.Quality.AnomalyWindow); err != nil {
		return err
	}
	if c.Quality.JournalFsync, err = envDuration("WATCHDOG_FLOW_COLLECT_QUALITY_JOURNAL_FSYNC_INTERVAL", c.Quality.JournalFsync); err != nil {
		return err
	}
	if c.Quality.CheckpointEvery, err = envDuration("WATCHDOG_FLOW_COLLECT_QUALITY_CHECKPOINT_INTERVAL", c.Quality.CheckpointEvery); err != nil {
		return err
	}
	if c.Quality.JournalMaxBytes, err = envInt64("WATCHDOG_FLOW_COLLECT_QUALITY_JOURNAL_MAX_BYTES", c.Quality.JournalMaxBytes); err != nil {
		return err
	}
	if c.Quality.MaxExporters, err = envInt("WATCHDOG_FLOW_COLLECT_QUALITY_MAX_EXPORTERS", c.Quality.MaxExporters); err != nil {
		return err
	}
	if c.Quality.MaxDataSources, err = envInt("WATCHDOG_FLOW_COLLECT_QUALITY_MAX_DATA_SOURCES", c.Quality.MaxDataSources); err != nil {
		return err
	}
	c.Observability.Listen = env("WATCHDOG_FLOW_COLLECT_OBSERVABILITY_LISTEN", c.Observability.Listen)
	if c.Observability.ReadHeaderTimeout, err = envDuration("WATCHDOG_FLOW_COLLECT_OBSERVABILITY_READ_HEADER_TIMEOUT", c.Observability.ReadHeaderTimeout); err != nil {
		return err
	}
	if c.Observability.WriteTimeout, err = envDuration("WATCHDOG_FLOW_COLLECT_OBSERVABILITY_WRITE_TIMEOUT", c.Observability.WriteTimeout); err != nil {
		return err
	}
	if c.Observability.IdleTimeout, err = envDuration("WATCHDOG_FLOW_COLLECT_OBSERVABILITY_IDLE_TIMEOUT", c.Observability.IdleTimeout); err != nil {
		return err
	}
	if c.Observability.ShutdownTimeout, err = envDuration("WATCHDOG_FLOW_COLLECT_OBSERVABILITY_SHUTDOWN_TIMEOUT", c.Observability.ShutdownTimeout); err != nil {
		return err
	}
	return nil
}

func validateListen(name, value string) error {
	_, portText, err := net.SplitHostPort(value)
	if err != nil {
		return fmt.Errorf("%s must be a host:port listen address: %w", name, err)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return fmt.Errorf("%s must contain a port between 1 and 65535", name)
	}
	return nil
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n <= 0 {
		return fallback, fmt.Errorf("%s must be a positive integer", key)
	}
	return n, nil
}

func envNonNegativeInt(key string, fallback int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n < 0 {
		return fallback, fmt.Errorf("%s must be a non-negative integer", key)
	}
	return n, nil
}

func envInt64(key string, fallback int64) (int64, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil || n <= 0 {
		return fallback, fmt.Errorf("%s must be a positive integer", key)
	}
	return n, nil
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil || d <= 0 {
		return fallback, fmt.Errorf("%s must be a positive duration", key)
	}
	return d, nil
}

func envFloat(key string, fallback float64) (float64, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	n, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		return fallback, fmt.Errorf("%s must be a number", key)
	}
	return n, nil
}

func envBool(key string, fallback bool) (bool, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		return fallback, fmt.Errorf("%s must be a boolean", key)
	}
	return parsed, nil
}

func splitList(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

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
	ControlPlaneURL       string             `yaml:"control_plane_url"`
	StateDir              string             `yaml:"state_dir"`
	PlanFile              string             `yaml:"plan_file"`
	PlanPublicKeyFile     string             `yaml:"plan_public_key_file"`
	SFlowListen           string             `yaml:"sflow_listen"`
	NetFlowListen         string             `yaml:"netflow_listen"`
	SocketCount           int                `yaml:"socket_count"`
	DecodeWorkers         int                `yaml:"decode_workers"`
	DecodeQueueDatagrams  int                `yaml:"decode_queue_datagrams"`
	ReceiveBufferBytes    int                `yaml:"receive_buffer_bytes"`
	MaxDatagramBytes      int                `yaml:"max_datagram_bytes"`
	PlanRefreshInterval   time.Duration      `yaml:"plan_refresh_interval"`
	ExporterRefreshPeriod time.Duration      `yaml:"exporter_refresh_interval"`
	WAL                   WALConfig          `yaml:"wal"`
	Kafka                 KafkaConfig        `yaml:"kafka"`
	NormalizedBatch       NormalizedBatchCfg `yaml:"normalized_batch"`
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
	Brokers           []string `yaml:"brokers"`
	NormalizedTopic   string   `yaml:"normalized_topic"`
	CollectStateTopic string   `yaml:"collect_state_topic"`
	DecodeDLQTopic    string   `yaml:"decode_dlq_topic"`
	QuarantineTopic   string   `yaml:"quarantine_topic"`
	Acks              string   `yaml:"acks"`
	Compression       string   `yaml:"compression"`
	TLS               bool     `yaml:"tls"`
}

type NormalizedBatchCfg struct {
	MaxRecords int           `yaml:"max_records"`
	MaxBytes   int           `yaml:"max_bytes"`
	MaxWait    time.Duration `yaml:"max_wait"`
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
		ExporterRefreshPeriod: 30 * time.Second,
		WAL: WALConfig{
			MaxBytes:      100 << 30,
			MaxAge:        24 * time.Hour,
			SegmentBytes:  128 << 20,
			FsyncInterval: 10 * time.Millisecond,
			SoftWatermark: .70,
			HardWatermark: .90,
		},
		Kafka: KafkaConfig{
			NormalizedTopic:   "watchdog.flow.normalized.v1",
			CollectStateTopic: "watchdog.flow.collect-state.v1",
			DecodeDLQTopic:    "watchdog.flow.decode-dlq.v1",
			QuarantineTopic:   "watchdog.flow.quarantine.v1",
			Acks:              "all",
			Compression:       "zstd",
			TLS:               true,
		},
		NormalizedBatch: NormalizedBatchCfg{
			MaxRecords: 1024,
			MaxBytes:   1 << 20,
			MaxWait:    5 * time.Millisecond,
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
		"flow_collect.sflow_listen":   c.SFlowListen,
		"flow_collect.netflow_listen": c.NetFlowListen,
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
	if c.PlanRefreshInterval <= 0 || c.ExporterRefreshPeriod <= 0 {
		return errors.New("flow_collect refresh intervals must be positive")
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
	if c.Kafka.Acks != "all" {
		return errors.New("flow_collect.kafka.acks must be all")
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
	if c.ExporterRefreshPeriod, err = envDuration("WATCHDOG_FLOW_COLLECT_EXPORTER_REFRESH_INTERVAL", c.ExporterRefreshPeriod); err != nil {
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

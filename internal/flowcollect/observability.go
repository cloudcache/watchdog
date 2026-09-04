package flowcollect

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

const prometheusContentType = "text/plain; version=0.0.4; charset=utf-8"

type ObservabilityServer struct {
	config  ObservabilityConfig
	metrics *Metrics
	wal     *WAL
	attempt *AttemptStore
	quality *QualityStateStore
	runtime *RuntimeState
	handler http.Handler
}

type readinessChecks struct {
	Runner            string `json:"runner"`
	Plan              string `json:"plan"`
	ControlPlane      string `json:"control_plane"`
	WAL               string `json:"wal"`
	AttemptState      string `json:"attempt_state"`
	QualityState      string `json:"quality_state"`
	CollectState      string `json:"collect_state"`
	KafkaNormalized   string `json:"kafka_normalized"`
	KafkaCollectState string `json:"kafka_collect_state"`
	KafkaDecodeDLQ    string `json:"kafka_decode_dlq"`
	KafkaQuarantine   string `json:"kafka_quarantine"`
}

type readinessResponse struct {
	Status string          `json:"status"`
	Checks readinessChecks `json:"checks"`
}

func NewObservabilityServer(config ObservabilityConfig, metrics *Metrics, wal *WAL, attempt *AttemptStore, quality *QualityStateStore, runtime *RuntimeState) (*ObservabilityServer, error) {
	if metrics == nil || wal == nil || attempt == nil || quality == nil || runtime == nil {
		return nil, errors.New("observability metrics, WAL, attempt state, quality state, and runtime state are required")
	}
	if err := validateListen("flow_collect.observability.listen", config.Listen); err != nil {
		return nil, err
	}
	if config.ReadHeaderTimeout <= 0 || config.WriteTimeout <= 0 || config.IdleTimeout <= 0 || config.ShutdownTimeout <= 0 {
		return nil, errors.New("flow_collect.observability timeouts must be positive")
	}
	server := &ObservabilityServer{config: config, metrics: metrics, wal: wal, attempt: attempt, quality: quality, runtime: runtime}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", server.serveMetrics)
	mux.HandleFunc("/health/live", server.serveLive)
	mux.HandleFunc("/health/ready", server.serveReady)
	server.handler = mux
	return server, nil
}

func (s *ObservabilityServer) Handler() http.Handler {
	return s.handler
}

func (s *ObservabilityServer) Run(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}
	listener, err := net.Listen("tcp", s.config.Listen)
	if err != nil {
		return fmt.Errorf("listen for flow observability: %w", err)
	}
	server := &http.Server{
		Handler:           s.handler,
		ReadHeaderTimeout: s.config.ReadHeaderTimeout,
		WriteTimeout:      s.config.WriteTimeout,
		IdleTimeout:       s.config.IdleTimeout,
		MaxHeaderBytes:    16 << 10,
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	select {
	case err := <-serveResult:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
		defer cancel()
		shutdownErr := server.Shutdown(shutdownCtx)
		if shutdownErr != nil {
			_ = server.Close()
		}
		serveErr := <-serveResult
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		return errors.Join(shutdownErr, serveErr)
	}
}

func (s *ObservabilityServer) serveLive(w http.ResponseWriter, r *http.Request) {
	if !allowReadMethod(w, r) {
		return
	}
	writeJSON(w, r, http.StatusOK, map[string]string{"status": "live"})
}

func (s *ObservabilityServer) serveReady(w http.ResponseWriter, r *http.Request) {
	if !allowReadMethod(w, r) {
		return
	}
	runtime := s.runtime.Snapshot()
	wal := s.wal.State()
	attempt := s.attempt.State()
	quality := s.quality.State()
	checks := readinessChecks{
		Runner:            healthValue(runtime.Running),
		Plan:              healthValue(runtime.Plan.Accepting),
		ControlPlane:      "disabled",
		WAL:               healthValue(wal.Writable && !wal.HardWatermark),
		AttemptState:      healthValue(attempt.Writable && runtime.AttemptJournal.Healthy && runtime.AttemptCheckpoint.Healthy),
		QualityState:      healthValue(quality.Writable && runtime.QualityJournal.Healthy && runtime.QualityCheckpoint.Healthy),
		CollectState:      healthValue(runtime.Collect.Healthy),
		KafkaNormalized:   healthValue(runtime.Kafka[kafkaTopicNormalized].Healthy),
		KafkaCollectState: healthValue(runtime.Kafka[kafkaTopicCollectState].Healthy),
		KafkaDecodeDLQ:    healthValue(runtime.Kafka[kafkaTopicDecodeDLQ].Healthy),
		KafkaQuarantine:   healthValue(runtime.Kafka[kafkaTopicQuarantine].Healthy),
	}
	ready := runtime.Running && runtime.Plan.Accepting && wal.Writable && !wal.HardWatermark && attempt.Writable && runtime.AttemptJournal.Healthy && runtime.AttemptCheckpoint.Healthy && quality.Writable && runtime.QualityJournal.Healthy && runtime.QualityCheckpoint.Healthy && runtime.Collect.Healthy
	for _, topic := range runtime.Kafka {
		ready = ready && topic.Healthy
	}
	if runtime.ControlPlane.Enabled {
		checks.ControlPlane = healthValue(runtime.ControlPlane.Healthy)
	}
	status, code := "ready", http.StatusOK
	if !ready {
		status, code = "unavailable", http.StatusServiceUnavailable
	} else if !runtime.Plan.Healthy || runtime.Plan.UsedLKG || (runtime.ControlPlane.Enabled && !runtime.ControlPlane.Healthy) || wal.SoftWatermark || attempt.UsageRatio >= .8 || quality.UsageRatio >= .8 {
		status = "degraded"
		if !runtime.Plan.Healthy || runtime.Plan.UsedLKG {
			checks.Plan = "degraded"
		}
		if wal.SoftWatermark {
			checks.WAL = "degraded"
		}
		if runtime.ControlPlane.Enabled && !runtime.ControlPlane.Healthy {
			checks.ControlPlane = "degraded"
		}
		if quality.UsageRatio >= .8 {
			checks.QualityState = "degraded"
		}
		if attempt.UsageRatio >= .8 {
			checks.AttemptState = "degraded"
		}
	}
	writeJSON(w, r, code, readinessResponse{Status: status, Checks: checks})
}

func (s *ObservabilityServer) serveMetrics(w http.ResponseWriter, r *http.Request) {
	if !allowReadMethod(w, r) {
		return
	}
	w.Header().Set("Content-Type", prometheusContentType)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, _ = w.Write(s.prometheusText())
}

func (s *ObservabilityServer) prometheusText() []byte {
	var out bytes.Buffer
	metrics := s.metrics
	runtime := s.runtime.Snapshot()
	wal := s.wal.State()
	attempt := s.attempt.State()
	quality := s.quality.State()

	metricHeader(&out, "watchdog_flow_datagrams_received_total", "Accepted flow datagrams by decoded wire protocol.", "counter")
	for index := range metrics.protocol {
		metricUint(&out, "watchdog_flow_datagrams_received_total", `protocol="`+protocolMetricName(index)+`"`, metrics.protocol[index].received.Load())
	}
	metricHeader(&out, "watchdog_flow_udp_kernel_drops_total", "Datagrams dropped by the UDP socket receive queue.", "counter")
	metricUint(&out, "watchdog_flow_udp_kernel_drops_total", `listener="sflow"`, metrics.UDPKernelDropsSFlow.Load())
	metricUint(&out, "watchdog_flow_udp_kernel_drops_total", `listener="netflow"`, metrics.UDPKernelDropsNetFlow.Load())
	metricHeader(&out, "watchdog_flow_udp_kernel_drop_telemetry_supported", "Whether the operating system exposes per-socket UDP overflow counters.", "gauge")
	metricBool(&out, "watchdog_flow_udp_kernel_drop_telemetry_supported", `listener="sflow"`, rxqTelemetrySupported())
	metricBool(&out, "watchdog_flow_udp_kernel_drop_telemetry_supported", `listener="netflow"`, rxqTelemetrySupported())
	metricHeader(&out, "watchdog_flow_udp_queue_drops_total", "Datagrams dropped because the bounded application receive queue was full.", "counter")
	metricUint(&out, "watchdog_flow_udp_queue_drops_total", `listener="sflow"`, metrics.UDPQueueDropsSFlow.Load())
	metricUint(&out, "watchdog_flow_udp_queue_drops_total", `listener="netflow"`, metrics.UDPQueueDropsNetFlow.Load())
	metricHeader(&out, "watchdog_flow_receive_queue_depth", "Current datagrams in the shared bounded receiver-to-WAL queue.", "gauge")
	metricInt(&out, "watchdog_flow_receive_queue_depth", `listener="shared"`, metrics.ReceiveQueueDepth.Load())
	metricHeader(&out, "watchdog_flow_receive_queue_capacity", "Configured datagram capacity of the shared receiver-to-WAL queue.", "gauge")
	metricInt(&out, "watchdog_flow_receive_queue_capacity", `listener="shared"`, metrics.ReceiveQueueCapacity.Load())
	metricHeader(&out, "watchdog_flow_decode_queue_depth", "Current durable WAL records queued across fixed decode workers.", "gauge")
	metricInt(&out, "watchdog_flow_decode_queue_depth", `listener="workers"`, metrics.DecodeQueueDepth.Load())
	metricHeader(&out, "watchdog_flow_decode_queue_capacity", "Configured record capacity across fixed decode workers.", "gauge")
	metricInt(&out, "watchdog_flow_decode_queue_capacity", `listener="workers"`, metrics.DecodeQueueCapacity.Load())

	metricHeader(&out, "watchdog_flow_records_decoded_total", "Flow records emitted by successful protocol decode attempts.", "counter")
	metricHeader(&out, "watchdog_flow_samples_missing_rate_total", "Sampled records rejected without an authoritative sampling rate.", "counter")
	metricHeader(&out, "watchdog_flow_sampling_rate_changes_total", "Observed sampling-rate epoch changes.", "counter")
	metricHeader(&out, "watchdog_flow_sample_pool_resets_total", "Observed sFlow sample-pool resets.", "counter")
	metricHeader(&out, "watchdog_flow_sflow_exporter_drops_total", "Exporter-reported sFlow dropped samples.", "counter")
	metricHeader(&out, "watchdog_flow_sequence_gaps_total", "Observed flow sequence-gap events by protocol and scope.", "counter")
	metricHeader(&out, "watchdog_flow_decode_errors_total", "Rejected flow decode or normalization attempts by bounded reason.", "counter")
	metricHeader(&out, "watchdog_flow_normalized_records_total", "Normalized records by protocol and result.", "counter")
	for index := range metrics.protocol {
		label := `protocol="` + protocolMetricName(index) + `"`
		metric := &metrics.protocol[index]
		metricUint(&out, "watchdog_flow_records_decoded_total", label, metric.decodedRecords.Load())
		metricUint(&out, "watchdog_flow_samples_missing_rate_total", label, metric.missingSamplingRate.Load())
		metricUint(&out, "watchdog_flow_sampling_rate_changes_total", label, metric.samplingRateChanges.Load())
		metricUint(&out, "watchdog_flow_sample_pool_resets_total", label, metric.samplePoolResets.Load())
		metricUint(&out, "watchdog_flow_sflow_exporter_drops_total", label, metric.exporterDrops.Load())
		metricUint(&out, "watchdog_flow_sequence_gaps_total", label+`,scope="datagram"`, metric.sequenceGapDatagram.Load())
		metricUint(&out, "watchdog_flow_sequence_gaps_total", label+`,scope="sample"`, metric.sequenceGapSample.Load())
		metricUint(&out, "watchdog_flow_decode_errors_total", label+`,reason="invalid_datagram"`, metric.decodeInvalid.Load())
		metricUint(&out, "watchdog_flow_decode_errors_total", label+`,reason="template_pending"`, metric.decodeTemplate.Load())
		metricUint(&out, "watchdog_flow_decode_errors_total", label+`,reason="decode_rejected"`, metric.decodeRejected.Load())
		metricUint(&out, "watchdog_flow_decode_errors_total", label+`,reason="normalize_rejected"`, metric.normalizeRejected.Load())
		metricUint(&out, "watchdog_flow_normalized_records_total", label+`,result="success"`, metric.normalizedSucceeded.Load())
		metricUint(&out, "watchdog_flow_normalized_records_total", label+`,result="rejected"`, metric.normalizedRejected.Load())
	}
	metricHeader(&out, "watchdog_flow_normalized_batch_records", "Records in the most recently published normalized batch.", "gauge")
	metricInt(&out, "watchdog_flow_normalized_batch_records", "", metrics.NormalizedBatchRecords.Load())
	metricHeader(&out, "watchdog_flow_normalized_batch_bytes", "Encoded bytes in the most recently published normalized batch.", "gauge")
	metricInt(&out, "watchdog_flow_normalized_batch_bytes", "", metrics.NormalizedBatchBytes.Load())

	metricHeader(&out, "watchdog_flow_kafka_produce_total", "Kafka produce results by fixed flow topic role.", "counter")
	metricHeader(&out, "watchdog_flow_kafka_produce_latency_seconds", "Kafka produce acknowledgement latency by fixed flow topic role.", "histogram")
	metricHeader(&out, "watchdog_flow_kafka_ready", "Whether the most recent produce result for a fixed flow topic role succeeded.", "gauge")
	metricHeader(&out, "watchdog_flow_kafka_last_success_timestamp_seconds", "Unix time of the last successful produce by fixed flow topic role.", "gauge")
	metricHeader(&out, "watchdog_flow_kafka_last_failure_timestamp_seconds", "Unix time of the last failed produce by fixed flow topic role.", "gauge")
	for index := range metrics.kafka {
		topic := kafkaTopic(index)
		label := `topic="` + kafkaTopicName(topic) + `"`
		metric := &metrics.kafka[index]
		metricUint(&out, "watchdog_flow_kafka_produce_total", label+`,result="success"`, metric.succeeded.Load())
		metricUint(&out, "watchdog_flow_kafka_produce_total", label+`,result="failure"`, metric.failed.Load())
		metricHistogram(&out, "watchdog_flow_kafka_produce_latency_seconds", label, metric.latencyBuckets[:], metric.latencyNanoseconds.Load(), metric.latencyCount.Load())
		metricBool(&out, "watchdog_flow_kafka_ready", label, runtime.Kafka[index].Healthy)
		metricInt(&out, "watchdog_flow_kafka_last_success_timestamp_seconds", label, runtime.Kafka[index].LastSuccessAt)
		metricInt(&out, "watchdog_flow_kafka_last_failure_timestamp_seconds", label, runtime.Kafka[index].LastFailureAt)
	}

	metricHeader(&out, "watchdog_flow_wal_bytes", "Bytes retained by the local raw WAL.", "gauge")
	metricInt(&out, "watchdog_flow_wal_bytes", "", wal.Bytes)
	metricHeader(&out, "watchdog_flow_wal_max_bytes", "Configured maximum bytes for the local raw WAL.", "gauge")
	metricInt(&out, "watchdog_flow_wal_max_bytes", "", wal.MaxBytes)
	metricHeader(&out, "watchdog_flow_wal_usage_ratio", "Fraction of configured raw WAL capacity in use.", "gauge")
	metricFloat(&out, "watchdog_flow_wal_usage_ratio", "", wal.UsageRatio)
	metricHeader(&out, "watchdog_flow_wal_oldest_age_seconds", "Age of the oldest record in retained raw WAL segments.", "gauge")
	metricFloat(&out, "watchdog_flow_wal_oldest_age_seconds", "", wal.OldestAge.Seconds())
	metricHeader(&out, "watchdog_flow_wal_soft_watermark", "Whether raw WAL usage crossed its soft watermark.", "gauge")
	metricBool(&out, "watchdog_flow_wal_soft_watermark", "", wal.SoftWatermark)
	metricHeader(&out, "watchdog_flow_wal_hard_watermark", "Whether raw WAL admission stopped at its hard watermark.", "gauge")
	metricBool(&out, "watchdog_flow_wal_hard_watermark", "", wal.HardWatermark)
	metricHeader(&out, "watchdog_flow_wal_fsync_latency_seconds", "Local raw WAL group-fsync latency.", "histogram")
	metricHistogram(&out, "watchdog_flow_wal_fsync_latency_seconds", "", metrics.walFsync.buckets[:], metrics.walFsync.nanoseconds.Load(), metrics.walFsync.count.Load())
	metricHeader(&out, "watchdog_flow_wal_dropped_total", "Datagrams rejected by the raw WAL admission boundary.", "counter")
	metricUint(&out, "watchdog_flow_wal_dropped_total", `reason="hard_watermark"`, metrics.WALHardStops.Load())
	metricHeader(&out, "watchdog_flow_plan_history_entries", "Signed collector plan revisions retained after WAL-reference reconciliation.", "gauge")
	metricInt(&out, "watchdog_flow_plan_history_entries", "", metrics.PlanHistoryEntries.Load())
	metricHeader(&out, "watchdog_flow_plan_history_pruned_total", "Unreferenced signed collector plan revisions pruned.", "counter")
	metricUint(&out, "watchdog_flow_plan_history_pruned_total", "", metrics.PlanHistoryPruned.Load())
	metricHeader(&out, "watchdog_flow_plan_refresh_total", "Signed collector plan refresh attempts by bounded result.", "counter")
	metricUint(&out, "watchdog_flow_plan_refresh_total", `result="success"`, metrics.PlanRefreshSuccesses.Load())
	metricUint(&out, "watchdog_flow_plan_refresh_total", `result="failure"`, metrics.PlanRefreshFailures.Load())
	metricHeader(&out, "watchdog_flow_plan_changes_total", "Successfully activated signed collector plan revisions.", "counter")
	metricUint(&out, "watchdog_flow_plan_changes_total", "", metrics.PlanRefreshChanges.Load())
	metricHeader(&out, "watchdog_flow_plan_refresh_ready", "Whether the most recent signed plan refresh succeeded.", "gauge")
	metricBool(&out, "watchdog_flow_plan_refresh_ready", "", runtime.Plan.Healthy)
	metricHeader(&out, "watchdog_flow_plan_accepting", "Whether the active signed plan authorizes new datagrams at the last refresh check.", "gauge")
	metricBool(&out, "watchdog_flow_plan_accepting", "", runtime.Plan.Accepting)
	metricHeader(&out, "watchdog_flow_plan_active_revision", "Active signed collector plan revision.", "gauge")
	metricUint(&out, "watchdog_flow_plan_active_revision", "", runtime.Plan.ActiveRevision)
	metricHeader(&out, "watchdog_flow_plan_expires_timestamp_seconds", "Unix expiry time of the active signed collector plan.", "gauge")
	metricInt(&out, "watchdog_flow_plan_expires_timestamp_seconds", "", runtime.Plan.ExpiresAt)
	metricHeader(&out, "watchdog_flow_plan_using_lkg", "Whether startup or refresh is using a last-known-good plan.", "gauge")
	metricBool(&out, "watchdog_flow_plan_using_lkg", "", runtime.Plan.UsedLKG)
	metricHeader(&out, "watchdog_flow_plan_last_success_timestamp_seconds", "Unix time of the last successful signed plan refresh.", "gauge")
	metricInt(&out, "watchdog_flow_plan_last_success_timestamp_seconds", "", runtime.Plan.LastSuccessAt)
	metricHeader(&out, "watchdog_flow_plan_last_failure_timestamp_seconds", "Unix time of the last failed signed plan refresh.", "gauge")
	metricInt(&out, "watchdog_flow_plan_last_failure_timestamp_seconds", "", runtime.Plan.LastFailureAt)
	metricHeader(&out, "watchdog_flow_control_plane_enabled", "Whether remote collector plan delivery is configured.", "gauge")
	metricBool(&out, "watchdog_flow_control_plane_enabled", "", runtime.ControlPlane.Enabled)
	metricHeader(&out, "watchdog_flow_control_plane_ready", "Whether the most recent remote plan delivery operation succeeded.", "gauge")
	metricBool(&out, "watchdog_flow_control_plane_ready", "", !runtime.ControlPlane.Enabled || runtime.ControlPlane.Healthy)
	metricHeader(&out, "watchdog_flow_control_plane_last_success_timestamp_seconds", "Unix time of the last successful remote plan delivery operation.", "gauge")
	metricInt(&out, "watchdog_flow_control_plane_last_success_timestamp_seconds", "", runtime.ControlPlane.LastSuccessAt)
	metricHeader(&out, "watchdog_flow_control_plane_last_failure_timestamp_seconds", "Unix time of the last failed remote plan delivery operation.", "gauge")
	metricInt(&out, "watchdog_flow_control_plane_last_failure_timestamp_seconds", "", runtime.ControlPlane.LastFailureAt)
	metricHeader(&out, "watchdog_flow_plan_delivery_fetch_total", "Remote signed-plan fetch outcomes by bounded result.", "counter")
	metricUint(&out, "watchdog_flow_plan_delivery_fetch_total", `result="downloaded"`, metrics.PlanDeliveryFetchSuccesses.Load())
	metricUint(&out, "watchdog_flow_plan_delivery_fetch_total", `result="not_modified"`, metrics.PlanDeliveryNotModified.Load())
	metricUint(&out, "watchdog_flow_plan_delivery_fetch_total", `result="failure"`, metrics.PlanDeliveryFetchFailures.Load())
	metricHeader(&out, "watchdog_flow_plan_delivery_persist_failures_total", "Remote signed plans rejected because atomic local persistence failed.", "counter")
	metricUint(&out, "watchdog_flow_plan_delivery_persist_failures_total", "", metrics.PlanDeliveryPersistFailures.Load())
	metricHeader(&out, "watchdog_flow_plan_delivery_ack_total", "Exact plan activation acknowledgement outcomes by bounded result.", "counter")
	metricUint(&out, "watchdog_flow_plan_delivery_ack_total", `result="success"`, metrics.PlanDeliveryAckSuccesses.Load())
	metricUint(&out, "watchdog_flow_plan_delivery_ack_total", `result="failure"`, metrics.PlanDeliveryAckFailures.Load())

	metricHeader(&out, "watchdog_flow_replay_attempts_total", "WAL record processing attempts after the first generation.", "counter")
	metricUint(&out, "watchdog_flow_replay_attempts_total", "", metrics.ReplayAttempts.Load())
	metricHeader(&out, "watchdog_flow_attempt_journal_bytes", "Bytes retained by the local replay-attempt journal.", "gauge")
	metricInt(&out, "watchdog_flow_attempt_journal_bytes", "", attempt.JournalBytes)
	metricHeader(&out, "watchdog_flow_attempt_journal_max_bytes", "Configured maximum bytes for the local replay-attempt journal.", "gauge")
	metricInt(&out, "watchdog_flow_attempt_journal_max_bytes", "", attempt.MaxJournalBytes)
	metricHeader(&out, "watchdog_flow_attempt_tracked", "Datagram attempt generations retained until durable terminal WAL acknowledgement.", "gauge")
	metricInt(&out, "watchdog_flow_attempt_tracked", "", int64(attempt.Tracked))
	metricHeader(&out, "watchdog_flow_attempt_journal_appends_total", "Generation records appended to the replay-attempt journal.", "counter")
	metricUint(&out, "watchdog_flow_attempt_journal_appends_total", "", metrics.AttemptJournalAppends.Load())
	metricHeader(&out, "watchdog_flow_attempt_journal_failures_total", "Replay-attempt journal write or sync failures.", "counter")
	metricUint(&out, "watchdog_flow_attempt_journal_failures_total", "", metrics.AttemptJournalFailures.Load())
	metricHeader(&out, "watchdog_flow_attempt_state_restores_total", "Pending datagram attempt generations restored at startup.", "counter")
	metricUint(&out, "watchdog_flow_attempt_state_restores_total", "", metrics.AttemptStateRestores.Load())
	metricHeader(&out, "watchdog_flow_attempt_state_checkpoints_total", "Replay-attempt journal compactions completed.", "counter")
	metricUint(&out, "watchdog_flow_attempt_state_checkpoints_total", "", metrics.AttemptStateCheckpoints.Load())
	metricHeader(&out, "watchdog_flow_attempt_checkpoint_failures_total", "Replay-attempt journal compaction failures.", "counter")
	metricUint(&out, "watchdog_flow_attempt_checkpoint_failures_total", "", metrics.AttemptCheckpointFailures.Load())
	metricHeader(&out, "watchdog_flow_dlq_total", "Datagrams durably published to the decode DLQ.", "counter")
	metricUint(&out, "watchdog_flow_dlq_total", `reason="permanent_decode"`, metrics.DLQDatagrams.Load())
	metricHeader(&out, "watchdog_flow_quarantine_total", "Unknown-source quarantine outcomes.", "counter")
	metricUint(&out, "watchdog_flow_quarantine_total", `result="admitted"`, metrics.QuarantinedDatagrams.Load())
	metricUint(&out, "watchdog_flow_quarantine_total", `result="rate_limited"`, metrics.QuarantineRateLimited.Load())
	metricUint(&out, "watchdog_flow_quarantine_total", `result="queue_full"`, metrics.QuarantineQueueDrops.Load())
	metricUint(&out, "watchdog_flow_quarantine_total", `result="published"`, metrics.PublishedQuarantine.Load())
	metricUint(&out, "watchdog_flow_quarantine_total", `result="publish_failure"`, metrics.QuarantinePublishFailures.Load())
	metricHeader(&out, "watchdog_flow_quarantine_queue_depth", "Current events waiting in the bounded quarantine publish queue.", "gauge")
	metricInt(&out, "watchdog_flow_quarantine_queue_depth", "", metrics.QuarantineQueueDepth.Load())
	metricHeader(&out, "watchdog_flow_quarantine_queue_capacity", "Configured capacity of the bounded quarantine publish queue.", "gauge")
	metricInt(&out, "watchdog_flow_quarantine_queue_capacity", "", metrics.QuarantineQueueCapacity.Load())

	metricHeader(&out, "watchdog_flow_quality_journal_bytes", "Bytes retained by the local quality-state journal.", "gauge")
	metricInt(&out, "watchdog_flow_quality_journal_bytes", "", quality.JournalBytes)
	metricHeader(&out, "watchdog_flow_quality_journal_max_bytes", "Configured maximum bytes for the local quality-state journal.", "gauge")
	metricInt(&out, "watchdog_flow_quality_journal_max_bytes", "", quality.MaxJournalBytes)
	metricHeader(&out, "watchdog_flow_quality_pending_decisions", "Quality decisions waiting for terminal WAL acknowledgement.", "gauge")
	metricInt(&out, "watchdog_flow_quality_pending_decisions", "", int64(quality.Pending))
	metricHeader(&out, "watchdog_flow_quality_committed_checkpoints", "Bounded committed quality checkpoint identities held locally.", "gauge")
	metricInt(&out, "watchdog_flow_quality_committed_checkpoints", "", int64(quality.Committed))
	metricHeader(&out, "watchdog_flow_quality_dirty_checkpoints", "Committed quality checkpoints still requiring Kafka acknowledgement.", "gauge")
	metricInt(&out, "watchdog_flow_quality_dirty_checkpoints", "", int64(quality.Dirty))
	metricHeader(&out, "watchdog_flow_quality_checkpoints_published_total", "Quality checkpoints acknowledged by Kafka.", "counter")
	metricUint(&out, "watchdog_flow_quality_checkpoints_published_total", "", metrics.PublishedQualityCheckpoints.Load())
	metricHeader(&out, "watchdog_flow_quality_restore_candidates", "Eligible quality checkpoint candidates captured at startup.", "gauge")
	metricInt(&out, "watchdog_flow_quality_restore_candidates", "", metrics.QualityRestoreCandidates.Load())
	metricHeader(&out, "watchdog_flow_quality_restored", "Quality exporter identities restored from the committed checkpoint merge.", "gauge")
	metricInt(&out, "watchdog_flow_quality_restored", "", metrics.QualityRestored.Load())
	metricHeader(&out, "watchdog_flow_runtime_running", "Whether the flow data-plane runner is active.", "gauge")
	metricBool(&out, "watchdog_flow_runtime_running", "", runtime.Running)
	metricHeader(&out, "watchdog_flow_runtime_started_at_seconds", "Unix time when the current runner lifecycle started.", "gauge")
	metricInt(&out, "watchdog_flow_runtime_started_at_seconds", "", runtime.StartedAt)
	metricHeader(&out, "watchdog_flow_collect_state_ready", "Whether the most recent local collect-state persistence succeeded.", "gauge")
	metricBool(&out, "watchdog_flow_collect_state_ready", "", runtime.Collect.Healthy)
	metricHeader(&out, "watchdog_flow_collect_state_last_failure_timestamp_seconds", "Unix time of the last local collect-state persistence failure.", "gauge")
	metricInt(&out, "watchdog_flow_collect_state_last_failure_timestamp_seconds", "", runtime.Collect.LastFailureAt)
	metricHeader(&out, "watchdog_flow_collect_state_restore_candidates", "Eligible compacted Kafka checkpoint candidates captured at startup.", "gauge")
	metricInt(&out, "watchdog_flow_collect_state_restore_candidates", "", metrics.CollectStateRestoreCandidates.Load())
	metricHeader(&out, "watchdog_flow_collect_state_restored", "Exporter/domain decoder identities restored after local and remote merge.", "gauge")
	metricInt(&out, "watchdog_flow_collect_state_restored", "", metrics.CollectStateRestored.Load())
	metricHeader(&out, "watchdog_flow_collect_state_restore_duration_seconds", "Duration of the startup collect-state read and restore gate.", "gauge")
	metricFloat(&out, "watchdog_flow_collect_state_restore_duration_seconds", "", float64(metrics.CollectStateRestoreNanos.Load())/float64(time.Second))
	metricHeader(&out, "watchdog_flow_attempt_state_ready", "Whether replay-attempt persistence and checkpointing are writable.", "gauge")
	metricBool(&out, "watchdog_flow_attempt_state_ready", "", attempt.Writable && runtime.AttemptJournal.Healthy && runtime.AttemptCheckpoint.Healthy)
	metricHeader(&out, "watchdog_flow_attempt_state_last_failure_timestamp_seconds", "Unix time of the last replay-attempt journal or checkpoint failure.", "gauge")
	metricInt(&out, "watchdog_flow_attempt_state_last_failure_timestamp_seconds", `operation="journal"`, runtime.AttemptJournal.LastFailureAt)
	metricInt(&out, "watchdog_flow_attempt_state_last_failure_timestamp_seconds", `operation="checkpoint"`, runtime.AttemptCheckpoint.LastFailureAt)
	metricHeader(&out, "watchdog_flow_quality_state_ready", "Whether the local quality-state journal and checkpoint are writable.", "gauge")
	metricBool(&out, "watchdog_flow_quality_state_ready", "", runtime.QualityJournal.Healthy && runtime.QualityCheckpoint.Healthy && quality.Writable)
	metricHeader(&out, "watchdog_flow_quality_state_last_failure_timestamp_seconds", "Unix time of the last quality-state journal or checkpoint failure.", "gauge")
	metricInt(&out, "watchdog_flow_quality_state_last_failure_timestamp_seconds", `operation="journal"`, runtime.QualityJournal.LastFailureAt)
	metricInt(&out, "watchdog_flow_quality_state_last_failure_timestamp_seconds", `operation="checkpoint"`, runtime.QualityCheckpoint.LastFailureAt)

	return out.Bytes()
}

func allowReadMethod(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return true
	}
	w.Header().Set("Allow", "GET, HEAD")
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	return false
}

func writeJSON(w http.ResponseWriter, r *http.Request, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_ = json.NewEncoder(w).Encode(value)
	}
}

func healthValue(ok bool) string {
	if ok {
		return "ok"
	}
	return "failed"
}

func metricHeader(out *bytes.Buffer, name, help, metricType string) {
	fmt.Fprintf(out, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, metricType)
}

func metricUint(out *bytes.Buffer, name, labels string, value uint64) {
	metricLabel(out, name, labels)
	out.WriteString(strconv.FormatUint(value, 10))
	out.WriteByte('\n')
}

func metricInt(out *bytes.Buffer, name, labels string, value int64) {
	metricLabel(out, name, labels)
	out.WriteString(strconv.FormatInt(value, 10))
	out.WriteByte('\n')
}

func metricFloat(out *bytes.Buffer, name, labels string, value float64) {
	metricLabel(out, name, labels)
	out.WriteString(strconv.FormatFloat(value, 'g', -1, 64))
	out.WriteByte('\n')
}

func metricBool(out *bytes.Buffer, name, labels string, value bool) {
	if value {
		metricInt(out, name, labels, 1)
	} else {
		metricInt(out, name, labels, 0)
	}
}

func metricHistogram(out *bytes.Buffer, name, labels string, buckets []atomic.Uint64, nanoseconds, count uint64) {
	cumulative := uint64(0)
	for index := range buckets {
		cumulative += buckets[index].Load()
		upper := "+Inf"
		if index < len(latencyUpperBounds) {
			upper = strconv.FormatFloat(latencyUpperBounds[index].Seconds(), 'g', -1, 64)
		}
		bucketLabels := `le="` + upper + `"`
		if labels != "" {
			bucketLabels = labels + "," + bucketLabels
		}
		metricUint(out, name+"_bucket", bucketLabels, cumulative)
	}
	metricFloat(out, name+"_sum", labels, float64(nanoseconds)/float64(time.Second))
	metricUint(out, name+"_count", labels, count)
}

func metricLabel(out *bytes.Buffer, name, labels string) {
	out.WriteString(name)
	if labels != "" {
		out.WriteByte('{')
		out.WriteString(labels)
		out.WriteByte('}')
	}
	out.WriteByte(' ')
}

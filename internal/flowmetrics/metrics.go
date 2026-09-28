// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

// Package flowmetrics exposes the fixed, low-cardinality runtime contract for
// the Flow collector and worker. Flow facts and tenant identifiers never enter
// this package; VictoriaMetrics scrapes the Prometheus text endpoint.
package flowmetrics

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
	"github.com/cloudcache/watchdog/internal/flowworker"
	"github.com/shirou/gopsutil/v4/process"
)

const contentType = "text/plain; version=0.0.4; charset=utf-8"

type Collector struct {
	producerStats   func() flowstream.ProducerStats
	planRevision    uint64
	planLoadedAt    time.Time
	sflowReceived   atomic.Uint64
	netflowReceived atomic.Uint64
	rejected        atomic.Uint64
	invalid         atomic.Uint64
	oversize        atomic.Uint64
	kernelDrops     atomic.Uint64
	process         processMetrics
}

func NewCollector(planRevision uint64, planLoadedAt time.Time, producerStats func() flowstream.ProducerStats) (*Collector, error) {
	if planRevision == 0 || planLoadedAt.IsZero() || producerStats == nil {
		return nil, errors.New("collector plan revision, load time, and producer stats are required")
	}
	return &Collector{
		producerStats: producerStats, planRevision: planRevision, planLoadedAt: planLoadedAt.UTC(),
		process: newProcessMetrics(),
	}, nil
}

func (m *Collector) ObserveReceived(decoder flowpb.RawFlow_Decoder) {
	if m == nil {
		return
	}
	switch decoder {
	case flowpb.RawFlow_DECODER_SFLOW:
		m.sflowReceived.Add(1)
	case flowpb.RawFlow_DECODER_NETFLOW:
		m.netflowReceived.Add(1)
	}
}

func (m *Collector) ObserveRejected() {
	if m != nil {
		m.rejected.Add(1)
	}
}

func (m *Collector) ObserveInvalid() {
	if m != nil {
		m.invalid.Add(1)
	}
}

func (m *Collector) ObserveOversize() {
	if m != nil {
		m.oversize.Add(1)
	}
}

func (m *Collector) ObserveKernelDrops(count uint64) {
	if m != nil {
		m.kernelDrops.Add(count)
	}
}

func (m *Collector) Handler() http.Handler {
	return metricsHandler(func(output io.Writer) {
		stats := m.producerStats()
		writeFamily(output, "watchdog_flow_collector_datagrams_received_total", "Admitted Flow datagrams received by decoder family.", "counter",
			sample{labels: `{decoder="sflow"}`, value: uintValue(m.sflowReceived.Load())},
			sample{labels: `{decoder="netflow"}`, value: uintValue(m.netflowReceived.Load())})
		writeUint(output, "watchdog_flow_collector_datagrams_rejected_total", "Datagrams rejected by the source admission plan.", "counter", m.rejected.Load())
		writeUint(output, "watchdog_flow_collector_datagrams_invalid_total", "Empty or unsupported Flow datagrams.", "counter", m.invalid.Load())
		writeUint(output, "watchdog_flow_collector_datagrams_oversize_total", "UDP datagrams truncated by the configured receive limit.", "counter", m.oversize.Load())
		writeUint(output, "watchdog_flow_collector_kernel_drops_total", "Datagrams dropped by the kernel UDP receive queue.", "counter", m.kernelDrops.Load())
		writeUint(output, "watchdog_flow_collector_kafka_records_total", "RawFlow records durably accepted by Kafka.", "counter", stats.Records)
		writeUint(output, "watchdog_flow_collector_kafka_bytes_total", "Encoded RawFlow bytes durably accepted by Kafka.", "counter", stats.Bytes)
		writeUint(output, "watchdog_flow_collector_kafka_publish_errors_total", "RawFlow records that Kafka permanently failed to publish.", "counter", stats.Errors)
		writeInt(output, "watchdog_flow_collector_kafka_buffered_records", "RawFlow records currently buffered by the Kafka producer.", "gauge", stats.BufferedRecords)
		writeFloat(output, "watchdog_flow_collector_kafka_produce_duration_seconds_total", "Cumulative RawFlow Kafka produce acknowledgement latency.", "counter", float64(stats.ProduceDurationNanos)/float64(time.Second))
		writeUint(output, "watchdog_flow_collector_plan_revision", "Active signed collector plan revision.", "gauge", m.planRevision)
		age := time.Since(m.planLoadedAt).Seconds()
		if age < 0 {
			age = 0
		}
		writeFloat(output, "watchdog_flow_collector_plan_age_seconds", "Age of the active signed collector plan.", "gauge", age)
		m.process.write(output)
	})
}

// CollectorSnapshot is a point-in-time copy of the collector's workload
// counters, used for periodic agent run reports so the registry reflects real
// collection throughput rather than liveness alone.
type CollectorSnapshot struct {
	SFlowReceived   uint64
	NetFlowReceived uint64
	Rejected        uint64
	Invalid         uint64
	Oversize        uint64
	KernelDrops     uint64
	KafkaRecords    uint64
	KafkaBytes      uint64
	KafkaErrors     uint64
	BufferedRecords int64
}

// Snapshot returns the current collector workload counters.
func (m *Collector) Snapshot() CollectorSnapshot {
	if m == nil {
		return CollectorSnapshot{}
	}
	stats := m.producerStats()
	return CollectorSnapshot{
		SFlowReceived:   m.sflowReceived.Load(),
		NetFlowReceived: m.netflowReceived.Load(),
		Rejected:        m.rejected.Load(),
		Invalid:         m.invalid.Load(),
		Oversize:        m.oversize.Load(),
		KernelDrops:     m.kernelDrops.Load(),
		KafkaRecords:    stats.Records,
		KafkaBytes:      stats.Bytes,
		KafkaErrors:     stats.Errors,
		BufferedRecords: stats.BufferedRecords,
	}
}

type Worker struct {
	consumerStats  func() flowstream.ConsumerStats
	processorStats func() flowworker.ProcessorStats
	pipelineStats  func() flowch.PipelineStats
	writerStats    func() flowch.WriterStats
	process        processMetrics
}

func NewWorker(
	consumerStats func() flowstream.ConsumerStats,
	processorStats func() flowworker.ProcessorStats,
	pipelineStats func() flowch.PipelineStats,
	writerStats func() flowch.WriterStats,
) (*Worker, error) {
	if consumerStats == nil || processorStats == nil || pipelineStats == nil || writerStats == nil {
		return nil, errors.New("worker consumer, processor, pipeline, and writer stats are required")
	}
	return &Worker{
		consumerStats: consumerStats, processorStats: processorStats,
		pipelineStats: pipelineStats, writerStats: writerStats, process: newProcessMetrics(),
	}, nil
}

func (m *Worker) Handler() http.Handler {
	return metricsHandler(func(output io.Writer) {
		consumer := m.consumerStats()
		processor := m.processorStats()
		pipeline := m.pipelineStats()
		writer := m.writerStats()
		writeUint(output, "watchdog_flow_worker_kafka_records_total", "RawFlow Kafka records committed after durable handling.", "counter", consumer.Records)
		writeUint(output, "watchdog_flow_worker_kafka_bytes_total", "RawFlow Kafka bytes committed after durable handling.", "counter", consumer.Bytes)
		writeUint(output, "watchdog_flow_worker_kafka_errors_total", "Kafka fetch or durable handler failures.", "counter", consumer.Errors)
		writeUint(output, "watchdog_flow_worker_kafka_polls_total", "Kafka fetch polls completed, including the final canceled poll.", "counter", consumer.Polls)
		writeFloat(output, "watchdog_flow_worker_kafka_poll_duration_seconds_total", "Cumulative time spent in Kafka fetch polls.", "counter", float64(consumer.PollDurationNanos)/float64(time.Second))
		writeUint(output, "watchdog_flow_worker_kafka_rebalances_total", "Completed Kafka partition assignment events.", "counter", consumer.Rebalances)
		writeUint(output, "watchdog_flow_worker_kafka_lost_partitions_total", "Kafka partitions lost without a safe revoke.", "counter", consumer.LostPartitions)
		writeUint(output, "watchdog_flow_worker_kafka_assigned_partitions", "Kafka partitions currently assigned to this worker.", "gauge", consumer.AssignedPartitions)
		writeUint(output, "watchdog_flow_worker_kafka_lag_known_partitions", "Assigned Kafka partitions with a broker high-watermark observation.", "gauge", consumer.LagKnownPartitions)
		writeUint(output, "watchdog_flow_worker_kafka_lag_records", "RawFlow records behind the latest observed broker high watermarks across assigned partitions.", "gauge", consumer.LagRecords)
		writeUint(output, "watchdog_flow_worker_datagrams_decoded_total", "RawFlow datagrams successfully decoded.", "counter", processor.Datagrams)
		writeUint(output, "watchdog_flow_worker_records_persisted_total", "Flow records durably persisted before Kafka commit.", "counter", pipeline.Records)
		writeUint(output, "watchdog_flow_worker_template_missing_total", "NetFlow or IPFIX datagrams skipped because a template was unavailable.", "counter", processor.TemplateMissing)
		writeUint(output, "watchdog_flow_worker_rejected_total", "Malformed decoded datagrams or mapped records skipped permanently.", "counter", processor.Rejected)
		writeUint(output, "watchdog_flow_worker_retryable_errors_total", "Binding or durable sink failures left for replay.", "counter", processor.RetryableErrors)
		writeUint(output, "watchdog_flow_worker_sampling_unknown_records_total", "Persisted records without a valid sampling estimate.", "counter", pipeline.SamplingUnknown)
		writeUint(output, "watchdog_flow_worker_sampling_conflict_records_total", "Persisted records with conflicting sampling semantics.", "counter", pipeline.SamplingConflict)
		writeUint(output, "watchdog_flow_worker_snapshot_miss_total", "Enrichment attempts blocked by an unavailable event-time snapshot.", "counter", pipeline.SnapshotMiss)
		writeUint(output, "watchdog_flow_worker_quarantined_datagrams_total", "RawFlow datagrams diverted by an installed raw-delete tombstone.", "counter", pipeline.QuarantinedDatagrams)
		writeUint(output, "watchdog_flow_worker_quarantined_records_total", "Decoded Flow records contained in tombstoned datagrams.", "counter", pipeline.QuarantinedRecords)
		writeUint(output, "watchdog_flow_clickhouse_insert_attempts_total", "ClickHouse flow block insert attempts.", "counter", writer.InsertAttempts)
		writeFamily(output, "watchdog_flow_clickhouse_insert_errors_total", "ClickHouse flow block insert attempt failures.", "counter",
			sample{labels: `{class="retryable"}`, value: uintValue(writer.RetryableErrors)},
			sample{labels: `{class="permanent"}`, value: uintValue(writer.PermanentErrors)})
		writeUint(output, "watchdog_flow_clickhouse_insert_retries_total", "ClickHouse flow block retries after backoff.", "counter", writer.Retries)
		writeUint(output, "watchdog_flow_clickhouse_blocks_total", "Flow blocks durably written with their receipts.", "counter", writer.Blocks)
		writeUint(output, "watchdog_flow_clickhouse_rows_total", "Flow rows durably written with their receipts.", "counter", writer.Rows)
		writeFloat(output, "watchdog_flow_clickhouse_insert_duration_seconds_total", "Cumulative time spent in ClickHouse flow block insert attempts.", "counter", float64(writer.InsertDurationNanos)/float64(time.Second))
		writeInt(output, "watchdog_flow_clickhouse_blocks_retrying", "Flow blocks currently retrying a ClickHouse insert; a sustained non-zero value marks a stuck partition.", "gauge", writer.RetryingNow)
		writeUint(output, "watchdog_flow_clickhouse_insert_budget_exceeded_total", "Flow blocks that exhausted their retry budget and were surfaced for replay.", "counter", writer.BudgetExceeded)
		writeUint(output, "watchdog_flow_clickhouse_quarantined_datagrams_total", "Tombstoned RawFlow datagrams durably written to quarantine.", "counter", writer.QuarantinedDatagrams)
		writeUint(output, "watchdog_flow_clickhouse_quarantined_records_total", "Decoded records represented by durable quarantine rows.", "counter", writer.QuarantinedRecords)
		m.process.write(output)
	})
}

type sample struct {
	labels string
	value  string
}

func metricsHandler(render func(io.Writer)) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			response.Header().Set("Allow", http.MethodGet)
			http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		response.Header().Set("Content-Type", contentType)
		response.Header().Set("Cache-Control", "no-store")
		render(response)
	})
}

func writeUint(output io.Writer, name, help, metricType string, value uint64) {
	writeFamily(output, name, help, metricType, sample{value: uintValue(value)})
}

func writeInt(output io.Writer, name, help, metricType string, value int64) {
	writeFamily(output, name, help, metricType, sample{value: strconv.FormatInt(value, 10)})
}

func writeFloat(output io.Writer, name, help, metricType string, value float64) {
	writeFamily(output, name, help, metricType, sample{value: strconv.FormatFloat(value, 'g', -1, 64)})
}

func writeFamily(output io.Writer, name, help, metricType string, samples ...sample) {
	_, _ = fmt.Fprintf(output, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, metricType)
	for _, current := range samples {
		_, _ = fmt.Fprintf(output, "%s%s %s\n", name, current.labels, current.value)
	}
}

func uintValue(value uint64) string { return strconv.FormatUint(value, 10) }

type processMetrics struct {
	process   *process.Process
	startedAt float64
}

func newProcessMetrics() processMetrics {
	current, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return processMetrics{startedAt: float64(time.Now().Unix())}
	}
	startedAt := float64(time.Now().Unix())
	if milliseconds, err := current.CreateTime(); err == nil && milliseconds > 0 {
		startedAt = float64(milliseconds) / 1000
	}
	return processMetrics{process: current, startedAt: startedAt}
}

func (m processMetrics) write(output io.Writer) {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	writeUint(output, "watchdog_flow_process_goroutines", "Current number of goroutines.", "gauge", uint64(runtime.NumGoroutine()))
	writeUint(output, "watchdog_flow_process_heap_alloc_bytes", "Bytes of allocated Go heap objects.", "gauge", memory.HeapAlloc)
	writeUint(output, "watchdog_flow_process_gc_cycles_total", "Completed Go garbage collection cycles.", "counter", uint64(memory.NumGC))
	writeFloat(output, "watchdog_flow_process_start_time_seconds", "Process start time since the Unix epoch.", "gauge", m.startedAt)

	var cpuSeconds float64
	var residentBytes uint64
	var openFDs int64
	up := uint64(1)
	if m.process == nil {
		up = 0
	} else {
		cpu, cpuErr := m.process.Times()
		memoryInfo, memoryErr := m.process.MemoryInfo()
		fds, fdErr := m.process.NumFDs()
		if cpuErr != nil || memoryErr != nil || fdErr != nil {
			up = 0
		} else {
			cpuSeconds = cpu.User + cpu.System
			residentBytes = memoryInfo.RSS
			openFDs = int64(fds)
		}
	}
	writeUint(output, "watchdog_flow_process_stats_up", "Whether OS process CPU, RSS, and file descriptor stats were read successfully.", "gauge", up)
	writeFloat(output, "watchdog_flow_process_cpu_seconds_total", "Total user and system CPU time in seconds.", "counter", cpuSeconds)
	writeUint(output, "watchdog_flow_process_resident_memory_bytes", "Resident memory size in bytes.", "gauge", residentBytes)
	writeInt(output, "watchdog_flow_process_open_fds", "Current number of open file descriptors.", "gauge", openFDs)
}

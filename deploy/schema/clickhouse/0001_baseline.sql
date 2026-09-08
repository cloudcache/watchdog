-- Watchdog v2 ClickHouse baseline: time-series + log/alert (per docs/watchdog-kiss-architecture.md §6.2).
-- Flow facts stay under internal/flowch Storage V2; these are the SNMP/system telemetry
-- and the LibreNMS-structured eventlog/alert tables (log/alert subsystem is implemented later,
-- but the storage locus is ClickHouse). No tenant dimension.

-- Unified raw time-series for SNMP + system/agent metrics. Natural idempotency by
-- (source_run_id, sample_index); counter rate is computed downstream, not stored.
CREATE TABLE IF NOT EXISTS telemetry_samples (
  observed_at   DateTime64(3),
  ingested_at   DateTime64(3),
  source_kind   LowCardinality(String),   -- snmp | system | agent
  device_id     String,
  agent_id      String,
  entity_kind   LowCardinality(String),   -- port | sensor | cpu | mem | ...
  entity_id     String,
  metric        LowCardinality(String),
  value_kind    LowCardinality(String),   -- gauge | counter
  gauge_value   Float64,
  counter_value UInt64,
  interval_ms   UInt32,
  quality_flags UInt32,
  source_run_id String,
  sample_index  UInt32
) ENGINE = MergeTree
PARTITION BY toYYYYMMDD(observed_at)
ORDER BY (device_id, metric, entity_id, observed_at);

-- Closed 5-minute interface traffic derived from continuous counters (charts, 95th, billing).
CREATE TABLE IF NOT EXISTS interface_traffic_5m (
  bucket_start DateTime,
  device_id    String,
  port_id      String,
  in_bytes     UInt64,
  out_bytes    UInt64,
  in_bps       Float64,
  out_bps      Float64,
  reset_flag   UInt8,
  gap_flag     UInt8,
  coverage     Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (device_id, port_id, bucket_start);

-- Device event timeline (LibreNMS `eventlog`): state changes, discovery, poll events.
CREATE TABLE IF NOT EXISTS eventlog (
  occurred_at DateTime64(3),
  device_id   String,
  type        LowCardinality(String),
  severity    LowCardinality(String),   -- ok | info | notice | warning | error | critical
  message     String,
  reference   String,
  username    String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (device_id, occurred_at);

-- Alert firing history (LibreNMS `alert_log`): append-only transitions.
CREATE TABLE IF NOT EXISTS alert_log (
  occurred_at DateTime64(3),
  rule_id     String,
  device_id   String,
  state       LowCardinality(String),   -- ok | alert | ack | worse | better
  severity    LowCardinality(String),
  message     String,
  details     String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (rule_id, device_id, occurred_at);

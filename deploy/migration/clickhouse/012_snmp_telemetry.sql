CREATE TABLE IF NOT EXISTS watchdog_flow.snmp_samples (
  observed_at    DateTime64(3, 'UTC'),
  ingested_at    DateTime64(3, 'UTC'),
  device_id      String,
  agent_id       String,
  entity_kind    LowCardinality(String),
  entity_id      String,
  recipe_id      String,
  metric         LowCardinality(String),
  value_kind     LowCardinality(String),
  gauge_value    Float64,
  counter_value  UInt64,
  counter_width  UInt8,
  interval_ms    UInt32,
  quality_flags  UInt32,
  poll_sequence  UInt64,
  source_run_id  String,
  sample_index   UInt32
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (device_id, metric, entity_kind, entity_id, observed_at, poll_sequence, source_run_id, sample_index)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS watchdog_flow.snmp_interface_traffic_5m (
  bucket_start DateTime('UTC'),
  row_kind     Enum8('value'=1, 'generation'=2),
  device_id    String,
  port_id      String,
  in_bytes     UInt64,
  out_bytes    UInt64,
  in_bps       Float64,
  out_bps      Float64,
  reset_flag   UInt8,
  gap_flag     UInt8,
  coverage     Float64,
  generation   UInt64,
  generated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(generation)
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (bucket_start, row_kind, device_id, port_id)
SETTINGS index_granularity = 8192;

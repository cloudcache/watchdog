CREATE TABLE IF NOT EXISTS watchdog_flow.snmp_events (
  id          String,
  occurred_at DateTime64(3, 'UTC'),
  ingested_at DateTime64(3, 'UTC'),
  device_id   String,
  entity_type LowCardinality(String),
  entity_id   String,
  source      LowCardinality(String),
  severity    LowCardinality(String),
  event_type  LowCardinality(String),
  message     String,
  raw_json    String
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (device_id, occurred_at, id)
SETTINGS index_granularity = 8192;

ALTER TABLE watchdog_flow.snmp_events
  ADD INDEX IF NOT EXISTS idx_snmp_events_severity severity TYPE set(64) GRANULARITY 4,
  ADD INDEX IF NOT EXISTS idx_snmp_events_type event_type TYPE bloom_filter(0.01) GRANULARITY 4,
  ADD INDEX IF NOT EXISTS idx_snmp_events_source source TYPE set(128) GRANULARITY 4;

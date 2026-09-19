-- Store the exact correction used for each estimated byte value. Existing
-- facts are identity-scaled; only newly ingested facts use a non-default value.

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS estimated_bytes_scale_ppm UInt32 DEFAULT 1000000 CODEC(T64, ZSTD(1))
  AFTER sampling_source;

ALTER TABLE watchdog_flow.flow_reclassified_records
  ADD COLUMN IF NOT EXISTS estimated_bytes_scale_ppm UInt32 DEFAULT 1000000 CODEC(T64, ZSTD(1))
  AFTER sampling_source;

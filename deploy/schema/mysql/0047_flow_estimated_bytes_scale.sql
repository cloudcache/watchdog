-- Keep exporter truth (raw counters and sampling rate) separate from the
-- evidence-backed correction applied to estimated byte volume.

ALTER TABLE flow_exporter_bindings
  ADD COLUMN estimated_bytes_scale_ppm INT UNSIGNED NOT NULL DEFAULT 1000000
  AFTER default_sampling_rate;

ALTER TABLE flow_exporter_bindings
  ADD CONSTRAINT chk_flow_estimated_bytes_scale_ppm
  CHECK (estimated_bytes_scale_ppm BETWEEN 500000 AND 2000000);

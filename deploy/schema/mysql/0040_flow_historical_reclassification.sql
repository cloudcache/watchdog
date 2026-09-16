-- FLOW-06B management metadata only. operation_jobs remains the sole job state
-- machine; this table freezes request/evidence and records atomic visibility.
CREATE TABLE IF NOT EXISTS flow_reclassification_sequence (
  id TINYINT UNSIGNED NOT NULL,
  next_generation BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT chk_flow_reclassification_sequence_singleton CHECK (id = 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO flow_reclassification_sequence (id, next_generation)
VALUES (1, 0)
ON DUPLICATE KEY UPDATE id = id;

CREATE TABLE IF NOT EXISTS flow_reclassifications (
  id CHAR(26) NOT NULL,
  operation_job_id CHAR(26) NOT NULL,
  request_hash CHAR(64) NOT NULL,
  source_publication_id CHAR(26) NOT NULL,
  target_publication_id CHAR(26) NOT NULL,
  value_view VARCHAR(16) NOT NULL,
  window_start DATETIME(3) NOT NULL,
  window_end DATETIME(3) NOT NULL,
  generation BIGINT UNSIGNED NOT NULL,
  source_dimension_snapshot_id CHAR(26) NOT NULL,
  source_classification_version INT UNSIGNED NOT NULL,
  target_dimension_snapshot_id CHAR(26) NOT NULL,
  target_dimension_version BIGINT UNSIGNED NOT NULL,
  target_dimension_object_ref VARCHAR(512) NOT NULL,
  target_dimension_checksum CHAR(71) NOT NULL,
  target_classification_version INT UNSIGNED NOT NULL,
  target_classification_object_ref VARCHAR(512) NOT NULL,
  target_classification_checksum CHAR(71) NOT NULL,
  authorization_json JSON NOT NULL,
  expected_record_count BIGINT UNSIGNED NOT NULL,
  expected_raw_bytes DECIMAL(39,0) NOT NULL,
  expected_raw_packets DECIMAL(39,0) NOT NULL,
  expected_estimated_bytes DECIMAL(39,0) NOT NULL,
  expected_estimated_packets DECIMAL(39,0) NOT NULL,
  expected_estimated_valid_records BIGINT UNSIGNED NOT NULL,
  output_record_count BIGINT UNSIGNED NULL,
  output_raw_bytes DECIMAL(39,0) NULL,
  output_raw_packets DECIMAL(39,0) NULL,
  output_estimated_bytes DECIMAL(39,0) NULL,
  output_estimated_packets DECIMAL(39,0) NULL,
  output_estimated_valid_records BIGINT UNSIGNED NULL,
  activated_at DATETIME(3) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_flow_reclassification_job (operation_job_id),
  UNIQUE KEY uq_flow_reclassification_request (request_hash),
  UNIQUE KEY uq_flow_reclassification_generation (generation),
  KEY idx_flow_reclassification_window (window_start, window_end),
  KEY idx_flow_reclassification_target (target_publication_id, activated_at),
  CONSTRAINT fk_flow_reclassification_job FOREIGN KEY (operation_job_id) REFERENCES operation_jobs(id) ON DELETE RESTRICT,
  CONSTRAINT fk_flow_reclassification_source FOREIGN KEY (source_publication_id) REFERENCES flow_enrichment_publications(id) ON DELETE RESTRICT,
  CONSTRAINT fk_flow_reclassification_target FOREIGN KEY (target_publication_id) REFERENCES flow_enrichment_publications(id) ON DELETE RESTRICT,
  CONSTRAINT fk_flow_reclassification_created_by FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT chk_flow_reclassification_view CHECK (value_view IN ('customer','supplier')),
  CONSTRAINT chk_flow_reclassification_window CHECK (window_start < window_end),
  CONSTRAINT chk_flow_reclassification_expected CHECK (expected_record_count > 0),
  CONSTRAINT chk_flow_reclassification_activation CHECK (
    activated_at IS NULL OR
    (output_record_count IS NOT NULL AND output_raw_bytes IS NOT NULL AND output_raw_packets IS NOT NULL AND
     output_estimated_bytes IS NOT NULL AND output_estimated_packets IS NOT NULL AND
     output_estimated_valid_records IS NOT NULL)
  )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

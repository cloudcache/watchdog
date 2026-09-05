-- Durable cursors for periodic operation-job producers. Job rows are finite
-- retention execution history and must not be reused as long-lived schedule
-- state. Producers advance only after the corresponding job is durably
-- enqueued; GREATEST makes concurrent scanners monotonic.
CREATE TABLE IF NOT EXISTS operation_job_watermarks (
  tenant_id CHAR(26) NOT NULL,
  job_type VARCHAR(64) NOT NULL,
  partition_key VARCHAR(128) NOT NULL,
  watermark_value BIGINT UNSIGNED NOT NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (tenant_id, job_type, partition_key),
  CONSTRAINT fk_operation_job_watermark_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Forward-fill any FLOW-04B v1 jobs written during a rolling deployment
-- before this migration reached the hub. Malformed/foreign payloads are
-- ignored and will still be rejected by their handler.
INSERT INTO operation_job_watermarks (
  tenant_id, job_type, partition_key, watermark_value
)
SELECT
  tenant_id,
  'flow_rollup',
  CONCAT('v1:', resolution),
  MAX(CAST(bucket_unix AS UNSIGNED))
FROM (
  SELECT
    tenant_id,
    JSON_UNQUOTE(JSON_EXTRACT(checkpoint_json, '$.payload.resolution')) AS resolution,
    JSON_UNQUOTE(JSON_EXTRACT(checkpoint_json, '$.payload.bucket_unix')) AS bucket_unix
  FROM operation_jobs
  WHERE job_type = 'flow_rollup'
    AND JSON_UNQUOTE(JSON_EXTRACT(checkpoint_json, '$.schema_version')) = '1'
) AS existing_flow_rollups
WHERE resolution IN ('1m', '1h') AND bucket_unix REGEXP '^[0-9]{1,19}$'
GROUP BY tenant_id, resolution
ON DUPLICATE KEY UPDATE
  watermark_value = GREATEST(watermark_value, VALUES(watermark_value)),
  row_version = row_version + 1;

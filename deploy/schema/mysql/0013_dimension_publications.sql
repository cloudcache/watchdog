-- Watchdog v2 address library — immutable dimension publish/lifecycle tables.
-- De-tenanted fold of legacy MySQL migrations 040/041/042/047/048/049/051/058.
-- Every CREATE TABLE and every later ALTER TABLE for each table below has been
-- merged into one final CREATE TABLE IF NOT EXISTS reflecting the legacy end
-- state, with the multi-tenant dimension removed throughout:
--   * the tenant_id column is dropped from every table;
--   * every FOREIGN KEY to tenants(id) is dropped;
--   * tenant_id is removed from every PRIMARY/UNIQUE/KEY (other columns kept in
--     order), and (tenant_id,id) UNIQUE keys — now redundant with PRIMARY KEY
--     (id) — are dropped;
--   * composite child FKs (tenant_id,x) -> dimension_snapshots(tenant_id,id) are
--     rewritten to (x) -> dimension_snapshots(id) with the same ON DELETE mode.
-- module_key and dimension_key are NOT tenant columns and are retained verbatim
-- (columns and key positions unchanged).
--
-- Note on legacy 051: its isp_operator_flow_id{,_sequences} tables were ported
-- separately in 0011_isp_operator_flow_identity.sql and are intentionally NOT
-- re-declared here. The operator tables folded in this file are
-- address_supplier_operator{s,_sequences} from legacy 058 (grep "supplier_operator").

-- ---------------------------------------------------------------------------
-- dimension_snapshots  (legacy 040 base; ALTERs 042/047/049/058)
-- Immutable, event-time address dimension publications. Editable address tables
-- remain draft state; workers consume only checksummed objects.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dimension_snapshots (
  id CHAR(26) NOT NULL,
  module_key VARCHAR(64) NOT NULL,
  dimension_key VARCHAR(64) NOT NULL,
  version BIGINT UNSIGNED NOT NULL,
  effective_from DATETIME(3) NOT NULL,
  object_ref VARCHAR(512) NOT NULL,
  object_format VARCHAR(16) NOT NULL DEFAULT 'json',
  object_format_version SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  builder_version VARCHAR(64) NOT NULL DEFAULT '',
  build_job_id CHAR(26) NULL,
  checksum VARCHAR(128) NOT NULL,
  draft_digest CHAR(71) NOT NULL,
  source_manifest_version SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  source_manifest JSON NOT NULL,
  source_prefix_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
  bundle_schema_version INT UNSIGNED NOT NULL,
  entry_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
  prefix_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
  address_set_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
  max_address_sets_per_record INT UNSIGNED NOT NULL DEFAULT 0,
  status VARCHAR(16) NOT NULL DEFAULT 'active',
  approval_state VARCHAR(16) NOT NULL DEFAULT 'pending',
  decided_by CHAR(26) NULL,
  decided_at DATETIME(3) NULL,
  decision_reason VARCHAR(512) NULL,
  signature_algorithm VARCHAR(32) NULL,
  signing_key_id VARCHAR(128) NULL,
  signature VARBINARY(512) NULL,
  signed_at DATETIME(3) NULL,
  retention_until DATETIME(3) NULL,
  object_deleted_at DATETIME(3) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NULL,
  retired_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  retired_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_dimension_version (module_key, dimension_key, version),
  UNIQUE KEY uq_dimension_effective (module_key, dimension_key, effective_from),
  UNIQUE KEY uq_dimension_snapshot_build_job (module_key, dimension_key, build_job_id),
  KEY idx_dimension_effective (module_key, dimension_key, status, effective_from),
  KEY idx_dimension_snapshot_approval (module_key, dimension_key, approval_state, version),
  KEY idx_dimension_snapshot_retention (status, retention_until, object_deleted_at),
  KEY idx_dimension_snapshot_gc (module_key, dimension_key, status, object_deleted_at, retention_until, id),
  CONSTRAINT fk_dimension_snapshot_creator FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT fk_dimension_snapshot_retired_by FOREIGN KEY (retired_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT fk_dimension_snapshot_decider FOREIGN KEY (decided_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT dimension_snapshots_chk_approval CHECK (approval_state IN ('pending','approved','rejected')),
  CONSTRAINT dimension_snapshots_chk_signature CHECK (
    (signature IS NULL AND signature_algorithm IS NULL AND signing_key_id IS NULL AND signed_at IS NULL)
    OR (signature IS NOT NULL AND signature_algorithm IS NOT NULL AND signing_key_id IS NOT NULL AND signed_at IS NOT NULL)
  ),
  CONSTRAINT chk_dimension_snapshot_source_manifest CHECK (
    source_manifest_version IN (0,1)
    AND JSON_TYPE(source_manifest) = 'ARRAY'
    AND (source_manifest_version <> 0 OR JSON_LENGTH(source_manifest) = 0)
  ),
  CONSTRAINT chk_dimension_snapshot_object_gc CHECK (
    object_deleted_at IS NULL OR (
      status = 'retired' AND retention_until IS NOT NULL
      AND object_deleted_at >= retention_until
    )
  ),
  CONSTRAINT chk_dimension_snapshot_object_format CHECK (
    object_format = 'json'
    OR (object_format = 'wads' AND object_format_version = 1 AND builder_version <> '' AND build_job_id IS NOT NULL)
  ),
  CHECK (status IN ('active','retired')),
  CHECK (version > 0),
  CHECK (bundle_schema_version > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- dimension_snapshot_activations  (legacy 042)
-- Append-only, event-time activation timeline. Rollback never rewrites an old
-- snapshot or fact; it appends a new activation instead.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dimension_snapshot_activations (
  id CHAR(26) NOT NULL,
  module_key VARCHAR(64) NOT NULL,
  dimension_key VARCHAR(64) NOT NULL,
  snapshot_id CHAR(26) NOT NULL,
  effective_from DATETIME(3) NOT NULL,
  reason VARCHAR(16) NOT NULL,
  rollback_of_snapshot_id CHAR(26) NULL,
  created_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_dimension_activation_effective (module_key, dimension_key, effective_from),
  KEY idx_dimension_activation_snapshot (snapshot_id, effective_from),
  KEY idx_dimension_activation_rollback (rollback_of_snapshot_id),
  CONSTRAINT fk_dimension_activation_snapshot FOREIGN KEY (snapshot_id)
    REFERENCES dimension_snapshots(id) ON DELETE RESTRICT,
  CONSTRAINT fk_dimension_activation_rollback FOREIGN KEY (rollback_of_snapshot_id)
    REFERENCES dimension_snapshots(id) ON DELETE RESTRICT,
  CONSTRAINT fk_dimension_activation_creator FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
  CHECK (reason IN ('publish','rollback')),
  CHECK ((reason = 'publish' AND rollback_of_snapshot_id IS NULL) OR (reason = 'rollback' AND rollback_of_snapshot_id IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- dimension_snapshot_acks  (legacy 040 base; ALTERs 042/048)
-- Per-worker download/install acknowledgements for an immutable snapshot.
-- installed_at is nullable so a worker can report a failed attempt.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dimension_snapshot_acks (
  snapshot_id CHAR(26) NOT NULL,
  worker_id VARCHAR(128) NOT NULL,
  boot_id VARCHAR(128) NOT NULL,
  software_version VARCHAR(64) NOT NULL,
  checksum VARCHAR(128) NOT NULL,
  state VARCHAR(16) NOT NULL DEFAULT 'installed',
  attempted_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  installed_at DATETIME(3) NULL,
  error_code VARCHAR(64) NULL,
  error_message VARCHAR(512) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (snapshot_id, worker_id),
  KEY idx_dimension_snapshot_acks_worker (worker_id, installed_at),
  KEY idx_dimension_snapshot_acks_state (snapshot_id, state, attempted_at),
  KEY idx_dimension_snapshot_acks_observed (worker_id, attempted_at, snapshot_id),
  CONSTRAINT fk_dimension_snapshot_acks_snapshot FOREIGN KEY (snapshot_id)
    REFERENCES dimension_snapshots(id) ON DELETE CASCADE,
  CONSTRAINT dimension_snapshot_acks_chk_state CHECK (state IN ('downloaded','installed','failed')),
  CONSTRAINT dimension_snapshot_acks_chk_installed CHECK (state <> 'installed' OR installed_at IS NOT NULL)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- dimension_snapshot_references  (legacy 042)
-- Aggregate event-time envelope per immutable snapshot per consumer. GC may
-- remove an object only after every reference's retain_until and the snapshot
-- retention_until have passed; base facts themselves stay in ClickHouse.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dimension_snapshot_references (
  snapshot_id CHAR(26) NOT NULL,
  consumer_kind VARCHAR(32) NOT NULL,
  consumer_id VARCHAR(190) NOT NULL,
  min_event_time DATETIME(3) NOT NULL,
  max_event_time DATETIME(3) NOT NULL,
  retain_until DATETIME(3) NOT NULL,
  last_observed_at DATETIME(3) NOT NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  PRIMARY KEY (snapshot_id, consumer_kind, consumer_id),
  KEY idx_dimension_reference_retention (retain_until),
  CONSTRAINT fk_dimension_reference_snapshot FOREIGN KEY (snapshot_id)
    REFERENCES dimension_snapshots(id) ON DELETE RESTRICT,
  CHECK (min_event_time <= max_event_time),
  CHECK (max_event_time <= retain_until)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- address_draft_revisions  (legacy 041)
-- Immutable preview records for atomic address-library batch changes. A
-- prepared revision is never rewritten into a different request.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS address_draft_revisions (
  id CHAR(26) NOT NULL,
  scope VARCHAR(16) NOT NULL,
  base_digest CHAR(71) NOT NULL,
  request_digest CHAR(71) NOT NULL,
  result_digest CHAR(71) NULL,
  operations_json JSON NOT NULL,
  preview_json JSON NOT NULL,
  operation_count INT UNSIGNED NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'prepared',
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NULL,
  applied_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  expires_at DATETIME(3) NOT NULL,
  applied_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_address_draft_revisions_request (request_digest),
  KEY idx_address_draft_revisions_status (status, created_at, id),
  KEY idx_address_draft_revisions_expiry (status, expires_at),
  KEY fk_address_draft_revisions_created_by (created_by),
  KEY fk_address_draft_revisions_applied_by (applied_by),
  CONSTRAINT fk_address_draft_revisions_created_by FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT fk_address_draft_revisions_applied_by FOREIGN KEY (applied_by) REFERENCES users(id) ON DELETE SET NULL,
  CHECK (scope IN ('prefix','set','mixed')),
  CHECK (status IN ('prepared','applied','superseded','cancelled')),
  CHECK (operation_count > 0),
  CHECK (base_digest LIKE 'sha256:%' AND CHAR_LENGTH(base_digest) = 71),
  CHECK (request_digest LIKE 'sha256:%' AND CHAR_LENGTH(request_digest) = 71),
  CHECK (result_digest IS NULL OR (result_digest LIKE 'sha256:%' AND CHAR_LENGTH(result_digest) = 71)),
  CHECK (expires_at > created_at),
  CHECK ((status = 'applied' AND applied_at IS NOT NULL AND result_digest IS NOT NULL)
      OR (status <> 'applied' AND applied_at IS NULL AND result_digest IS NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- address_supplier_operator_sequences  (legacy 058)
-- Monotonic UInt16 Flow-id allocator for supplier ISP identities. Legacy PK was
-- tenant_id alone; de-tenanted to a single global row via the singleton pattern
-- used elsewhere in this schema (see 0011_isp_operator_flow_identity.sql).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS address_supplier_operator_sequences (
  id TINYINT UNSIGNED NOT NULL DEFAULT 1, -- singleton
  next_flow_isp_id INT UNSIGNED NOT NULL DEFAULT 1,
  PRIMARY KEY (id),
  CHECK (id = 1),
  CHECK (next_flow_isp_id BETWEEN 1 AND 65536)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- address_supplier_operators  (legacy 058)
-- Supplier ISP identities: exact normalized names from the pinned source. IDs
-- are monotonic and never reused, so facts stay interpretable across generations.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS address_supplier_operators (
  supplier_key VARCHAR(190) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  flow_isp_id SMALLINT UNSIGNED NOT NULL,
  name VARCHAR(190) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (supplier_key),
  UNIQUE KEY uq_address_supplier_operator_flow_id (flow_isp_id),
  CHECK (flow_isp_id > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

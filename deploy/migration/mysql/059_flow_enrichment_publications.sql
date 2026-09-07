-- PLAT-04C4c: persist one tenant classification draft and immutable,
-- event-time enrichment pairs. Large AddressSnap objects remain in the
-- bounded object store; MySQL owns only metadata, lineage, signatures and
-- worker delivery evidence.

CREATE TABLE IF NOT EXISTS flow_classification_profiles (
  tenant_id CHAR(26) NOT NULL,
  home_province VARCHAR(6) NOT NULL DEFAULT '',
  home_city VARCHAR(6) NOT NULL DEFAULT '',
  home_isp_ids JSON NOT NULL,
  home_asns JSON NOT NULL,
  overseas_includes_hmt BOOLEAN NOT NULL DEFAULT FALSE,
  internal_policy VARCHAR(8) NOT NULL DEFAULT 'count',
  transit_policy VARCHAR(8) NOT NULL DEFAULT 'count',
  definition_digest CHAR(64) NOT NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NOT NULL,
  updated_by CHAR(26) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (tenant_id),
  CONSTRAINT fk_flow_classification_profile_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT chk_flow_classification_profile_isp_ids
    CHECK (JSON_TYPE(home_isp_ids) = 'ARRAY'),
  CONSTRAINT chk_flow_classification_profile_asns
    CHECK (JSON_TYPE(home_asns) = 'ARRAY'),
  CONSTRAINT chk_flow_classification_profile_internal
    CHECK (internal_policy IN ('count','drop')),
  CONSTRAINT chk_flow_classification_profile_transit
    CHECK (transit_policy IN ('count','drop'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS flow_enrichment_publications (
  id CHAR(26) NOT NULL,
  tenant_id CHAR(26) NOT NULL,
  pair_schema_version SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  classification_version INT UNSIGNED NOT NULL,
  effective_from DATETIME(3) NOT NULL,
  profile_row_version BIGINT UNSIGNED NOT NULL,
  dimension_snapshot_id CHAR(26) NOT NULL,
  dimension_version BIGINT UNSIGNED NOT NULL,
  dimension_effective_from DATETIME(3) NOT NULL,
  dimension_object_ref VARCHAR(512) NOT NULL,
  dimension_object_format VARCHAR(16) NOT NULL,
  dimension_object_format_version SMALLINT UNSIGNED NOT NULL,
  dimension_checksum VARCHAR(71) NOT NULL,
  classification_schema_version SMALLINT UNSIGNED NOT NULL,
  classification_object_ref VARCHAR(512) NOT NULL,
  classification_checksum VARCHAR(71) NOT NULL,
  signature_algorithm VARCHAR(16) NOT NULL,
  signing_key_id VARCHAR(64) NOT NULL,
  signature VARBINARY(64) NOT NULL,
  signed_at DATETIME(3) NOT NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_flow_enrichment_publication_tenant_id (tenant_id, id),
  UNIQUE KEY uq_flow_enrichment_publication_version (tenant_id, classification_version),
  UNIQUE KEY uq_flow_enrichment_publication_effective (tenant_id, effective_from),
  KEY idx_flow_enrichment_publication_dimension (tenant_id, dimension_snapshot_id, effective_from),
  KEY idx_flow_enrichment_publication_signing_key (signing_key_id, effective_from),
  CONSTRAINT fk_flow_enrichment_publication_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_flow_enrichment_publication_dimension
    FOREIGN KEY (tenant_id, dimension_snapshot_id)
    REFERENCES dimension_snapshots(tenant_id, id) ON DELETE CASCADE,
  CONSTRAINT chk_flow_enrichment_publication_pair_schema
    CHECK (pair_schema_version = 1),
  CONSTRAINT chk_flow_enrichment_publication_classification_version
    CHECK (classification_version > 0),
  CONSTRAINT chk_flow_enrichment_publication_profile_version
    CHECK (profile_row_version > 0),
  CONSTRAINT chk_flow_enrichment_publication_dimension
    CHECK (dimension_version > 0 AND dimension_effective_from <= effective_from),
  CONSTRAINT chk_flow_enrichment_publication_dimension_format
    CHECK (dimension_object_format = 'wads' AND dimension_object_format_version = 1),
  CONSTRAINT chk_flow_enrichment_publication_classification_schema
    CHECK (classification_schema_version = 1),
  CONSTRAINT chk_flow_enrichment_publication_signature
    CHECK (signature_algorithm = 'ed25519' AND OCTET_LENGTH(signature) = 64)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS flow_enrichment_publication_acks (
  tenant_id CHAR(26) NOT NULL,
  publication_id CHAR(26) NOT NULL,
  worker_id CHAR(26) NOT NULL,
  boot_id VARCHAR(128) NOT NULL,
  software_version VARCHAR(64) NOT NULL,
  state VARCHAR(16) NOT NULL,
  attempted_at DATETIME(3) NOT NULL,
  downloaded_at DATETIME(3) NULL,
  installed_at DATETIME(3) NULL,
  error_code VARCHAR(64) NULL,
  error_message VARCHAR(512) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (tenant_id, publication_id, worker_id),
  KEY idx_flow_enrichment_ack_worker (tenant_id, worker_id, attempted_at),
  KEY idx_flow_enrichment_ack_state (tenant_id, publication_id, state, attempted_at),
  CONSTRAINT fk_flow_enrichment_ack_publication
    FOREIGN KEY (tenant_id, publication_id)
    REFERENCES flow_enrichment_publications(tenant_id, id) ON DELETE CASCADE,
  CONSTRAINT fk_flow_enrichment_ack_worker
    FOREIGN KEY (tenant_id, worker_id)
    REFERENCES collector_agents(tenant_id, id) ON DELETE CASCADE,
  CONSTRAINT chk_flow_enrichment_ack_state
    CHECK (state IN ('downloaded','installed','failed')),
  CONSTRAINT chk_flow_enrichment_ack_downloaded
    CHECK (state <> 'downloaded' OR downloaded_at IS NOT NULL),
  CONSTRAINT chk_flow_enrichment_ack_installed
    CHECK (state <> 'installed' OR (downloaded_at IS NOT NULL AND installed_at IS NOT NULL)),
  CONSTRAINT chk_flow_enrichment_ack_failed
    CHECK ((state = 'failed') = (error_code IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

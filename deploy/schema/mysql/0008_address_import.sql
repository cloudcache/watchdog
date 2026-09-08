-- Watchdog v2 address library — Phase 2: source imports (KISS, de-tenanted).
-- Ported from legacy migration 035 by stripping tenant_id and its FKs/indexes.
-- Uploaded MMDB/IPDB artifacts decode into an immutable staging generation; only
-- address_import_slots flips when a generation is fully ready, so readers never
-- observe a partial import. Column types, the (import_id, cidr) upsert key, the
-- BINARY(16) range-lookup index, and the CHECK constraints are preserved verbatim
-- so the reused batch-insert and lookup SQL drops in unchanged.

CREATE TABLE IF NOT EXISTS address_imports (
  id CHAR(26) PRIMARY KEY,
  source_slot VARCHAR(16) NOT NULL,
  format VARCHAR(8) NOT NULL,
  original_name VARCHAR(255) NOT NULL,
  artifact_ref VARCHAR(512) NOT NULL,
  checksum_sha256 CHAR(64) NOT NULL,
  size_bytes BIGINT UNSIGNED NOT NULL,
  database_type VARCHAR(128) NULL,
  build_epoch BIGINT NULL,
  ip_version TINYINT UNSIGNED NULL,
  language VARCHAR(16) NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'quarantined',
  row_count_v4 BIGINT UNSIGNED NOT NULL DEFAULT 0,
  row_count_v6 BIGINT UNSIGNED NOT NULL DEFAULT 0,
  created_by CHAR(26) NULL,
  error_code VARCHAR(64) NULL,
  error_detail TEXT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  ready_at DATETIME(3) NULL,
  activated_at DATETIME(3) NULL,
  KEY idx_address_imports_status (status, created_at),
  KEY idx_address_imports_checksum (checksum_sha256),
  CONSTRAINT fk_address_imports_creator FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
  CHECK (source_slot IN ('geo','asn','combined')),
  CHECK (format IN ('mmdb','ipdb')),
  CHECK (status IN ('quarantined','queued','importing','ready','failed','retired')),
  CHECK (ip_version IS NULL OR ip_version IN (4,6))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS address_base_prefixes (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  import_id CHAR(26) NOT NULL,
  family TINYINT UNSIGNED NOT NULL,
  prefix_length TINYINT UNSIGNED NOT NULL,
  cidr VARCHAR(43) NOT NULL,
  ip_start BINARY(16) NOT NULL,
  ip_end BINARY(16) NOT NULL,
  continent_code VARCHAR(16) NULL,
  country_code CHAR(2) NULL,
  country_name VARCHAR(128) NULL,
  subdivision_code VARCHAR(16) NULL,
  subdivision_name VARCHAR(128) NULL,
  city_code VARCHAR(32) NULL,
  city_name VARCHAR(128) NULL,
  asn BIGINT UNSIGNED NULL,
  operator_name VARCHAR(190) NULL,
  latitude DECIMAL(10,7) NULL,
  longitude DECIMAL(10,7) NULL,
  labels JSON NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_address_base_import_cidr (import_id, cidr),
  KEY idx_address_base_lookup (import_id, family, ip_start),
  KEY idx_address_base_country (import_id, country_code, family, ip_start),
  KEY idx_address_base_asn (import_id, asn, family, ip_start),
  CONSTRAINT fk_address_base_import FOREIGN KEY (import_id) REFERENCES address_imports(id) ON DELETE CASCADE,
  CHECK (family IN (4,6)),
  CHECK ((family = 4 AND prefix_length <= 32) OR (family = 6 AND prefix_length <= 128)),
  CHECK (ip_start <= ip_end)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS address_import_slots (
  source_slot VARCHAR(16) NOT NULL,
  import_id CHAR(26) NOT NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  activated_by CHAR(26) NULL,
  activated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (source_slot),
  KEY fk_address_import_slots_import (import_id),
  CONSTRAINT fk_address_import_slots_import FOREIGN KEY (import_id) REFERENCES address_imports(id),
  CONSTRAINT fk_address_import_slots_actor FOREIGN KEY (activated_by) REFERENCES users(id) ON DELETE SET NULL,
  CHECK (source_slot IN ('geo','asn','combined'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Watchdog v2 address library — Phase 1: editable data (KISS-05, de-tenanted).
-- Ported from legacy migrations 010/038/051/058 by stripping tenant_id and the
-- tenant-scoped uniques/FKs. Business columns, JSON shapes, BINARY(16) range indexes
-- and CHAR(36) ids for prefixes/sets are preserved so the reused Go logic drops in.
-- Import staging and publication lifecycle land in later phases (0006/0007).

-- Geographic hierarchy (continent/country/subdivision/city/...).
CREATE TABLE IF NOT EXISTS geo_dict (
  id         CHAR(26)     NOT NULL,
  kind       VARCHAR(16)  NOT NULL,
  code       VARCHAR(64)  NOT NULL,
  parent_id  CHAR(26)     NULL,
  name       VARCHAR(190) NOT NULL,
  short_name VARCHAR(190) NULL,
  sort_order INT          NOT NULL DEFAULT 0,
  enabled    TINYINT(1)   NOT NULL DEFAULT 1,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_geo_dict_kind_code (kind, code),
  KEY idx_geo_dict_parent (parent_id),
  CONSTRAINT fk_geo_dict_parent FOREIGN KEY (parent_id) REFERENCES geo_dict(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ISP / operator dictionary. flow_isp_id is the stable UInt16 Flow identity.
CREATE TABLE IF NOT EXISTS isp_operators (
  id          CHAR(26)     NOT NULL,
  code        VARCHAR(64)  NOT NULL,
  name        VARCHAR(190) NOT NULL,
  short_name  VARCHAR(190) NULL,
  category    VARCHAR(32)  NOT NULL DEFAULT 'other',
  flow_isp_id SMALLINT UNSIGNED NOT NULL,
  asns        JSON         NOT NULL,
  sort_order  INT          NOT NULL DEFAULT 0,
  enabled     TINYINT(1)   NOT NULL DEFAULT 1,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_isp_operators_code (code),
  UNIQUE KEY uq_isp_operators_flow_id (flow_isp_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Label-selector groupings with set algebra (union/intersection/difference).
CREATE TABLE IF NOT EXISTS address_sets (
  id                       CHAR(36)     NOT NULL,
  name                     VARCHAR(190) NOT NULL,
  description              TEXT         NULL,
  selector                 JSON         NOT NULL,
  explicit_members         JSON         NOT NULL,
  explicit_exclude_members JSON         NOT NULL,
  include_set_ids          JSON         NOT NULL,
  exclude_set_ids          JSON         NOT NULL,
  match_direction          VARCHAR(8)   NOT NULL DEFAULT 'both',
  enabled                  TINYINT(1)   NOT NULL DEFAULT 1,
  row_version              BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at               DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at               DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_address_sets_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Geo "lines" (线路): geo-selector + operator + address-set routing definitions.
CREATE TABLE IF NOT EXISTS geo_lines (
  id             CHAR(26)     NOT NULL,
  parent_id      CHAR(26)     NULL,
  code           VARCHAR(64)  NOT NULL,
  name           VARCHAR(190) NOT NULL,
  description    TEXT         NULL,
  geo_selector   JSON         NOT NULL,
  operator_id    CHAR(26)     NULL,
  address_set_id CHAR(36)     NULL,
  sort_order     INT          NOT NULL DEFAULT 0,
  enabled        TINYINT(1)   NOT NULL DEFAULT 1,
  row_version    BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_geo_lines_code (code),
  KEY idx_geo_lines_parent (parent_id),
  KEY idx_geo_lines_operator (operator_id),
  KEY idx_geo_lines_set (address_set_id),
  CONSTRAINT fk_geo_lines_parent FOREIGN KEY (parent_id) REFERENCES geo_lines(id) ON DELETE SET NULL,
  CONSTRAINT fk_geo_lines_operator FOREIGN KEY (operator_id) REFERENCES isp_operators(id) ON DELETE SET NULL,
  CONSTRAINT fk_geo_lines_set FOREIGN KEY (address_set_id) REFERENCES address_sets(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Manually-maintained prefixes with geo/operator/asn attribution.
CREATE TABLE IF NOT EXISTS address_prefixes (
  id            CHAR(36)     NOT NULL,
  cidr          VARCHAR(64)  NOT NULL,
  family        TINYINT UNSIGNED NULL,
  prefix_length TINYINT UNSIGNED NULL,
  ip_start      BINARY(16)   NULL,
  ip_end        BINARY(16)   NULL,
  labels        JSON         NOT NULL,
  geo_leaf_id   CHAR(26)     NULL,
  operator_id   CHAR(26)     NULL,
  asn           BIGINT UNSIGNED NULL,
  source        VARCHAR(32)  NOT NULL DEFAULT 'manual',
  row_version   BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_address_prefixes_cidr (cidr),
  KEY idx_address_prefixes_range (ip_start, ip_end),
  KEY idx_address_prefixes_geo (geo_leaf_id),
  KEY idx_address_prefixes_operator (operator_id),
  CONSTRAINT fk_address_prefixes_geo FOREIGN KEY (geo_leaf_id) REFERENCES geo_dict(id) ON DELETE SET NULL,
  CONSTRAINT fk_address_prefixes_operator FOREIGN KEY (operator_id) REFERENCES isp_operators(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

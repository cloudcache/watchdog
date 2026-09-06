-- PLAT-04C4a: assign each management-plane operator a tenant-scoped Flow
-- UInt16 identity. Allocation history is retained after operator deletion so a
-- published isp_id can never be rebound to a different operator.
CREATE TABLE IF NOT EXISTS isp_operator_flow_id_sequences (
  tenant_id CHAR(26) NOT NULL,
  next_flow_isp_id INT UNSIGNED NOT NULL DEFAULT 1,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (tenant_id),
  CONSTRAINT fk_isp_operator_flow_sequences_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (next_flow_isp_id BETWEEN 1 AND 65536)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS isp_operator_flow_ids (
  tenant_id CHAR(26) NOT NULL,
  flow_isp_id SMALLINT UNSIGNED NOT NULL,
  operator_id CHAR(26) NOT NULL,
  allocated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (tenant_id, flow_isp_id),
  UNIQUE KEY uq_isp_operator_flow_ids_operator (tenant_id, operator_id),
  UNIQUE KEY uq_isp_operator_flow_ids_reference (tenant_id, operator_id, flow_isp_id),
  CONSTRAINT fk_isp_operator_flow_ids_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (flow_isp_id > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SET @schema_name = DATABASE();

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns
   WHERE table_schema = @schema_name AND table_name = 'isp_operators' AND column_name = 'flow_isp_id') = 0,
  'ALTER TABLE isp_operators ADD COLUMN flow_isp_id SMALLINT UNSIGNED NULL AFTER category',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Existing operators receive a deterministic identity ordered by their
-- original creation and stable management id. Strict mode rejects tenants
-- already exceeding the UInt16 address space instead of truncating ids.
INSERT INTO isp_operator_flow_ids (tenant_id, flow_isp_id, operator_id)
SELECT ranked.tenant_id, ranked.flow_isp_id, ranked.id
FROM (
  SELECT tenant_id, id,
         ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY created_at, id) AS flow_isp_id
  FROM isp_operators
) AS ranked
LEFT JOIN isp_operator_flow_ids AS existing
  ON existing.tenant_id = ranked.tenant_id AND existing.operator_id = ranked.id
WHERE existing.operator_id IS NULL;

INSERT INTO isp_operator_flow_id_sequences (tenant_id, next_flow_isp_id)
SELECT tenant_id, MAX(flow_isp_id) + 1
FROM isp_operator_flow_ids
GROUP BY tenant_id
ON DUPLICATE KEY UPDATE
  next_flow_isp_id = GREATEST(next_flow_isp_id, VALUES(next_flow_isp_id));

UPDATE isp_operators AS operators
JOIN isp_operator_flow_ids AS identities
  ON identities.tenant_id = operators.tenant_id AND identities.operator_id = operators.id
SET operators.flow_isp_id = identities.flow_isp_id
WHERE operators.flow_isp_id IS NULL;

ALTER TABLE isp_operators
  MODIFY COLUMN flow_isp_id SMALLINT UNSIGNED NOT NULL AFTER category;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.statistics
   WHERE table_schema = @schema_name AND table_name = 'isp_operators' AND index_name = 'uq_isp_operators_flow_isp_id') = 0,
  'ALTER TABLE isp_operators ADD UNIQUE KEY uq_isp_operators_flow_isp_id (tenant_id, flow_isp_id)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.table_constraints
   WHERE table_schema = @schema_name AND table_name = 'isp_operators' AND constraint_name = 'fk_isp_operators_flow_identity') = 0,
  'ALTER TABLE isp_operators ADD CONSTRAINT fk_isp_operators_flow_identity
     FOREIGN KEY (tenant_id, id, flow_isp_id)
     REFERENCES isp_operator_flow_ids (tenant_id, operator_id, flow_isp_id)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

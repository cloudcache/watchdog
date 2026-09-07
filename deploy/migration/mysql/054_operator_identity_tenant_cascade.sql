-- PLAT-DB-01: deleting a tenant cascades both isp_operators and the retained
-- Flow identity ledger. The operator -> ledger reference must therefore
-- cascade when the ledger parent is removed; deleting an operator itself still
-- leaves its ledger allocation in place and cannot make the UInt16 id reusable.
SET @schema_name = DATABASE();

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.referential_constraints
   WHERE BINARY constraint_schema = BINARY @schema_name
     AND table_name = 'isp_operators'
     AND constraint_name = 'fk_isp_operators_flow_identity'
     AND delete_rule <> 'CASCADE') > 0,
  'ALTER TABLE isp_operators DROP FOREIGN KEY fk_isp_operators_flow_identity',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.table_constraints
   WHERE table_schema = @schema_name
     AND table_name = 'isp_operators'
     AND constraint_name = 'fk_isp_operators_flow_identity') = 0,
  'ALTER TABLE isp_operators ADD CONSTRAINT fk_isp_operators_flow_identity
     FOREIGN KEY (tenant_id, id, flow_isp_id)
     REFERENCES isp_operator_flow_ids (tenant_id, operator_id, flow_isp_id)
     ON DELETE CASCADE',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

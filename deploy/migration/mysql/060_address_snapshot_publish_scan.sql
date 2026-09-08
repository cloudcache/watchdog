-- PLAT-04C4b2: bound the AddressSnap builder's immutable-generation keyset
-- scan. The previous index stopped at ip_start, while the publication cursor
-- and deterministic order also use prefix_length and id for nested CIDRs.
SET @schema_name = DATABASE();

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.statistics
   WHERE table_schema = @schema_name
     AND table_name = 'address_base_prefixes'
     AND index_name = 'idx_address_base_publish_scan') = 0,
  'ALTER TABLE address_base_prefixes
     ADD KEY idx_address_base_publish_scan
       (tenant_id, import_id, family, ip_start, prefix_length, id)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

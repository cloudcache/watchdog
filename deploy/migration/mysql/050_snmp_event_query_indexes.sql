-- PLAT-02: keep per-device event/alert keyset scans bounded when operators
-- select one or more severities or an exact event type. Search remains an
-- accurate substring predicate inside the already tenant+device scoped scan.
SET @schema_name = DATABASE();

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = 'snmp_events' AND index_name = 'idx_snmp_events_device_severity_time') = 0,
  'ALTER TABLE snmp_events
     ADD KEY idx_snmp_events_device_severity_time
       (tenant_id, device_id, severity, occurred_at, id)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = 'snmp_events' AND index_name = 'idx_snmp_events_device_type_time') = 0,
  'ALTER TABLE snmp_events
     ADD KEY idx_snmp_events_device_type_time
       (tenant_id, device_id, event_type, occurred_at, id)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Generic SNMP inventory fields consumed by the device-detail APIs. These are
-- protocol/MIB-neutral discovered attributes; time-series samples remain in
-- ClickHouse and are not duplicated here.

ALTER TABLE ports
  ADD COLUMN metadata_json JSON NULL AFTER ignore_alerts;

ALTER TABLE interface_addresses
  ADD COLUMN if_index BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER port_id,
  ADD COLUMN origin VARCHAR(32) NOT NULL DEFAULT '' AFTER context;

ALTER TABLE bgp_sessions
  ADD COLUMN local_as INT UNSIGNED NOT NULL DEFAULT 0 AFTER peer_as,
  ADD COLUMN denied_prefixes INT UNSIGNED NOT NULL DEFAULT 0 AFTER prefixes,
  ADD COLUMN advertised_prefixes INT UNSIGNED NOT NULL DEFAULT 0 AFTER denied_prefixes,
  ADD COLUMN uptime_seconds BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER advertised_prefixes,
  ADD COLUMN metadata_json JSON NULL AFTER uptime_seconds;

ALTER TABLE sensors
  ADD COLUMN port_id CHAR(26) NULL AFTER device_id,
  ADD COLUMN sensor_index BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER port_id,
  ADD COLUMN oid VARCHAR(255) NOT NULL DEFAULT '' AFTER oid_index,
  ADD COLUMN unit VARCHAR(32) NOT NULL DEFAULT '' AFTER oid,
  ADD COLUMN value_num DOUBLE NULL AFTER unit,
  ADD COLUMN warn_limit DOUBLE NULL AFTER value_num,
  ADD COLUMN crit_limit DOUBLE NULL AFTER warn_limit,
  ADD COLUMN status VARCHAR(32) NOT NULL DEFAULT '' AFTER crit_limit,
  ADD COLUMN metadata_json JSON NULL AFTER status,
  ADD KEY idx_sensors_port (port_id),
  ADD CONSTRAINT fk_sensors_port FOREIGN KEY (port_id) REFERENCES ports(id) ON DELETE SET NULL;

ALTER TABLE physical_entities
  ADD COLUMN description TEXT NULL AFTER name,
  ADD COLUMN vendor_type VARCHAR(255) NOT NULL DEFAULT '' AFTER class,
  ADD COLUMN contained_in BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER vendor_type,
  ADD COLUMN parent_rel_pos INT NOT NULL DEFAULT 0 AFTER contained_in,
  ADD COLUMN hardware_revision VARCHAR(128) NOT NULL DEFAULT '' AFTER parent_rel_pos,
  ADD COLUMN firmware_revision VARCHAR(128) NOT NULL DEFAULT '' AFTER hardware_revision,
  ADD COLUMN software_revision VARCHAR(128) NOT NULL DEFAULT '' AFTER firmware_revision,
  ADD COLUMN manufacturer_name VARCHAR(190) NOT NULL DEFAULT '' AFTER software_revision,
  ADD COLUMN model_name VARCHAR(190) NOT NULL DEFAULT '' AFTER manufacturer_name,
  ADD COLUMN alias VARCHAR(190) NOT NULL DEFAULT '' AFTER model_name,
  ADD COLUMN asset_id VARCHAR(190) NOT NULL DEFAULT '' AFTER alias,
  ADD COLUMN is_fru TINYINT(1) NOT NULL DEFAULT 0 AFTER asset_id;

ALTER TABLE vlans
  ADD COLUMN status VARCHAR(32) NOT NULL DEFAULT '' AFTER name;

ALTER TABLE lag_groups
  ADD COLUMN mac_address VARCHAR(32) NOT NULL DEFAULT '' AFTER name,
  ADD COLUMN mode VARCHAR(32) NOT NULL DEFAULT '' AFTER mac_address;

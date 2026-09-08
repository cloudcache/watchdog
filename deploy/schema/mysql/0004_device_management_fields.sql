-- Device fields still consumed by the existing inventory UI and SNMP editor.
-- SNMP profile supplies defaults; per-device JSON is an optional override.

ALTER TABLE devices
  ADD COLUMN labels_json JSON NULL AFTER display_name,
  ADD COLUMN vendor VARCHAR(128) NOT NULL DEFAULT '' AFTER kind,
  ADD COLUMN model VARCHAR(190) NOT NULL DEFAULT '' AFTER vendor,
  ADD COLUMN platform VARCHAR(190) NOT NULL DEFAULT '' AFTER model,
  ADD COLUMN snmp_port SMALLINT UNSIGNED NULL AFTER snmp_profile_id,
  ADD COLUMN snmp_security_json JSON NULL AFTER snmp_port;

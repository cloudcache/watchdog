-- Bind external provider side effects to stable operation keys. Existing
-- principals receive deterministic imported keys so this expand migration is
-- safe on non-empty installations.

ALTER TABLE collector_service_principals
  ADD COLUMN grant_operation_key CHAR(64)
    CHARACTER SET ascii COLLATE ascii_bin NULL AFTER provider,
  ADD COLUMN grant_request_hash CHAR(64)
    CHARACTER SET ascii COLLATE ascii_bin NULL AFTER grant_operation_key,
  ADD COLUMN revoke_operation_key CHAR(64)
    CHARACTER SET ascii COLLATE ascii_bin NULL AFTER write_revoked_at;

UPDATE collector_service_principals
SET grant_operation_key = LOWER(SHA2(CONCAT(
  'watchdog:imported:grant:', tenant_id, ':', id, ':', grant_receipt_sha256
), 256))
WHERE grant_operation_key IS NULL;

UPDATE collector_service_principals
SET grant_request_hash = LOWER(SHA2(CONCAT(
  'watchdog:imported:request:', tenant_id, ':', collector_id, ':',
  service_type, ':', provider, ':', acl_propagation_delay_ms
), 256))
WHERE grant_request_hash IS NULL;

UPDATE collector_service_principals
SET revoke_operation_key = LOWER(SHA2(CONCAT(
  'watchdog:imported:revoke:', tenant_id, ':', id, ':', revoke_receipt_sha256
), 256))
WHERE status = 'revoked' AND revoke_operation_key IS NULL;

ALTER TABLE collector_service_principals
  MODIFY COLUMN grant_operation_key CHAR(64)
    CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  MODIFY COLUMN grant_request_hash CHAR(64)
    CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  ADD UNIQUE KEY uq_collector_principal_grant_operation (grant_operation_key),
  ADD UNIQUE KEY uq_collector_principal_revoke_operation (revoke_operation_key),
  ADD CHECK ((status = 'revoked') = (revoke_operation_key IS NOT NULL));

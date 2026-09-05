-- PLAT-03C2: server-side dual-window credential rotation. A rotation stages a
-- pending credential next to the active one; both authenticate until the
-- window expires or the rotation is committed (pending becomes active) or
-- aborted (pending cleared). The pending kind always matches auth_type, and a
-- pending credential always carries its expiry.

ALTER TABLE collector_agents
  ADD COLUMN pending_token_hash VARCHAR(255) NULL AFTER token_hash,
  ADD COLUMN pending_certificate_fingerprint VARCHAR(190) NULL AFTER certificate_fingerprint,
  ADD COLUMN pending_credential_expires_at DATETIME(3) NULL AFTER pending_certificate_fingerprint,
  ADD CONSTRAINT chk_collector_pending_credential_expiry CHECK (
    ((pending_token_hash IS NOT NULL) OR (pending_certificate_fingerprint IS NOT NULL))
    = (pending_credential_expires_at IS NOT NULL)
  ),
  ADD CONSTRAINT chk_collector_pending_credential_kind CHECK (
    (pending_token_hash IS NULL OR auth_type = 'token')
    AND (pending_certificate_fingerprint IS NULL OR auth_type = 'mtls')
  );

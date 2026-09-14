-- KISS: the ed25519 publication-approval signature was dropped (approve is a
-- plain authenticated UI confirmation now; accountability = audit log + version
-- rollback). Remove the always-NULL signature-envelope columns and the CHECK
-- constraint that coupled them from dimension_snapshots.
ALTER TABLE dimension_snapshots
  DROP CHECK dimension_snapshots_chk_signature,
  DROP COLUMN signature,
  DROP COLUMN signature_algorithm,
  DROP COLUMN signing_key_id,
  DROP COLUMN signed_at;

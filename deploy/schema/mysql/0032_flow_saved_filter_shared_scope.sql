-- Single-domain saved filters use private/shared. Normalize the obsolete
-- multi-tenant wire value before enforcing the current contract.
UPDATE flow_saved_filters
SET share_scope = 'shared'
WHERE share_scope = 'tenant';

ALTER TABLE flow_saved_filters
  ADD CONSTRAINT chk_flow_saved_filters_share_scope
  CHECK (share_scope IN ('private', 'shared'));

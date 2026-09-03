-- A target's display name is not identity. Within one tenant, one collector
-- kind owns one normalized host so duplicate form submissions cannot create
-- two polling pipelines for the same endpoint.
ALTER TABLE targets
  ADD UNIQUE KEY uq_targets_tenant_kind_host (tenant_id, kind, host);

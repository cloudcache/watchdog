-- Correct migration 042 without changing its checksum. Lifecycle rows belong
-- exclusively to an immutable snapshot. CASCADE allows tenant destruction to
-- remove the graph in one transaction; application-level retention remains the
-- gate before an individual snapshot may be physically deleted.
ALTER TABLE dimension_snapshot_activations
  DROP FOREIGN KEY fk_dimension_activation_snapshot,
  DROP FOREIGN KEY fk_dimension_activation_rollback;

ALTER TABLE dimension_snapshot_activations
  ADD CONSTRAINT fk_dimension_activation_snapshot FOREIGN KEY (tenant_id, snapshot_id)
    REFERENCES dimension_snapshots(tenant_id, id) ON DELETE CASCADE,
  ADD CONSTRAINT fk_dimension_activation_rollback FOREIGN KEY (tenant_id, rollback_of_snapshot_id)
    REFERENCES dimension_snapshots(tenant_id, id) ON DELETE CASCADE;

ALTER TABLE dimension_snapshot_references
  DROP FOREIGN KEY fk_dimension_reference_snapshot;

ALTER TABLE dimension_snapshot_references
  ADD CONSTRAINT fk_dimension_reference_snapshot FOREIGN KEY (tenant_id, snapshot_id)
    REFERENCES dimension_snapshots(tenant_id, id) ON DELETE CASCADE;

-- Canonical target naming. This single ALTER preserves rows, indexes and all
-- inbound foreign keys while avoiding a partially-renamed schema on failure.
ALTER TABLE monitor_targets
  CHANGE COLUMN target_type kind VARCHAR(32) NOT NULL,
  RENAME TO targets;

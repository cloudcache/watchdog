-- raw_retention_seconds may now be as short as one hour: a raw day is archived
-- max(raw_retention, late_arrival) after it ends, so an installation can keep
-- about one day of raw. 0033 declared the old one-day bound as its third
-- unnamed CHECK, which MySQL names flow_retention_policy_revisions_chk_3.
ALTER TABLE flow_retention_policy_revisions
  DROP CHECK flow_retention_policy_revisions_chk_3,
  ADD CONSTRAINT chk_flow_retention_raw_retention CHECK (raw_retention_seconds BETWEEN 3600 AND 315576000);

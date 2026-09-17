-- Contract bandwidth is an operator-entered commercial value.  Discovered
-- interface speeds remain independent capacity evidence and must never become
-- the billing authority.

ALTER TABLE billing_accounts
  ADD COLUMN contract_bandwidth_bps BIGINT UNSIGNED NULL AFTER minimum_percent;

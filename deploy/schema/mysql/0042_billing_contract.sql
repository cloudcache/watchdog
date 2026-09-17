-- KISS-07 billing contract: separate what is measured from how it is billed.
-- Existing development rows are converted in place; period evidence remains
-- immutable and snapshots the derived minimum plus every port's capacity.

ALTER TABLE billing_accounts
  DROP CHECK ck_billing_account_type,
  DROP CHECK ck_billing_account_algorithm,
  DROP CHECK ck_billing_account_pricing_model;

UPDATE billing_accounts SET bill_type=IF(bill_type='quota','traffic','bandwidth');
UPDATE billing_accounts SET pricing_model=IF(pricing_model='flat_port','package_port','monthly_95th');

ALTER TABLE billing_accounts
  CHANGE COLUMN bill_type measurement_type VARCHAR(16) NOT NULL DEFAULT 'bandwidth',
  CHANGE COLUMN pricing_model billing_method VARCHAR(16) NOT NULL DEFAULT 'monthly_95th',
  CHANGE COLUMN quota_bytes traffic_allowance_bytes BIGINT UNSIGNED NULL,
  DROP COLUMN cdr_bps,
  ADD COLUMN minimum_percent DECIMAL(5,2) NOT NULL DEFAULT 0 AFTER unit_price,
  ADD CONSTRAINT ck_billing_account_measurement CHECK (measurement_type IN ('bandwidth','traffic')),
  ADD CONSTRAINT ck_billing_account_method CHECK (billing_method IN ('package_port','monthly_95th','daily_95th','monthly_average')),
  ADD CONSTRAINT ck_billing_account_algorithm CHECK (
    (measurement_type='traffic' AND algorithm='total') OR
    (measurement_type='bandwidth' AND billing_method='monthly_95th' AND algorithm='95th') OR
    (measurement_type='bandwidth' AND billing_method='daily_95th' AND algorithm='daily_95th') OR
    (measurement_type='bandwidth' AND billing_method IN ('package_port','monthly_average') AND algorithm='average')
  ),
  ADD CONSTRAINT ck_billing_account_minimum CHECK (minimum_percent BETWEEN 0 AND 100),
  ADD CONSTRAINT ck_billing_account_measurement_method CHECK (measurement_type='bandwidth' OR billing_method='package_port'),
  ADD CONSTRAINT ck_billing_account_package_minimum CHECK (billing_method<>'package_port' OR minimum_percent=0),
  ADD CONSTRAINT ck_billing_account_traffic_allowance CHECK (measurement_type<>'traffic' OR traffic_allowance_bytes IS NOT NULL);

ALTER TABLE billing_periods
  DROP CHECK ck_billing_period_algorithm,
  ADD CONSTRAINT ck_billing_period_algorithm CHECK (algorithm IN ('95th','daily_95th','average','total'));

ALTER TABLE billing_period_ports
  ADD COLUMN capacity_bps BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER if_name;

ALTER TABLE billing_period_values
  DROP CHECK ck_billing_value_algorithm,
  ADD COLUMN rate_daily_95th_bps BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER rate_95th_bps,
  ADD CONSTRAINT ck_billing_value_algorithm CHECK (algorithm IN ('95th','daily_95th','average','total'));

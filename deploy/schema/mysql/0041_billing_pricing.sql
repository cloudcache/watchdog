-- Billing price contract. A price is stored as an exact decimal string by the
-- application and frozen into billing_periods.account_snapshot_json when a
-- period is created.

ALTER TABLE billing_accounts
  ADD COLUMN pricing_model VARCHAR(16) NOT NULL DEFAULT 'flat_port' AFTER default_layer,
  ADD COLUMN price_currency CHAR(3) NOT NULL DEFAULT 'CNY' AFTER pricing_model,
  ADD COLUMN unit_price DECIMAL(20,6) NOT NULL DEFAULT 0 AFTER price_currency,
  ADD CONSTRAINT ck_billing_account_pricing_model CHECK (pricing_model IN ('flat_port','usage_95th')),
  ADD CONSTRAINT ck_billing_account_currency CHECK (price_currency REGEXP '^[A-Z]{3}$'),
  ADD CONSTRAINT ck_billing_account_unit_price CHECK (unit_price >= 0);

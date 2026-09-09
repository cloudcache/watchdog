-- KISS-07 no-data cutover: the early baseline billing draft was never active.
-- Rebuild it under the final single-domain, immutable-evidence contract.
DROP TABLE IF EXISTS user_billing_permissions;
DROP TABLE IF EXISTS reconciliation_issues;
DROP TABLE IF EXISTS reconciliation_runs;
DROP TABLE IF EXISTS billing_adjustments;
DROP TABLE IF EXISTS billing_period_values;
DROP TABLE IF EXISTS billing_period_ports;
DROP TABLE IF EXISTS billing_periods;
DROP TABLE IF EXISTS billing_account_ports;
DROP TABLE IF EXISTS billing_accounts;
DROP TABLE IF EXISTS parties;

CREATE TABLE parties (
  id CHAR(26) NOT NULL,
  kind VARCHAR(16) NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'active',
  name VARCHAR(190) NOT NULL,
  ref VARCHAR(64) NOT NULL DEFAULT '',
  notes VARCHAR(255) NOT NULL DEFAULT '',
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NULL,
  updated_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_parties_kind_name (kind,name),
  KEY idx_parties_list (status,kind,name,id),
  CONSTRAINT ck_parties_kind CHECK (kind IN ('customer','supplier')),
  CONSTRAINT ck_parties_status CHECK (status IN ('active','inactive')),
  CONSTRAINT fk_parties_created_by FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT fk_parties_updated_by FOREIGN KEY (updated_by) REFERENCES users(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE billing_accounts (
  id CHAR(26) NOT NULL,
  party_id CHAR(26) NULL,
  name VARCHAR(190) NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'active',
  bill_type VARCHAR(16) NOT NULL DEFAULT 'cdr',
  algorithm VARCHAR(16) NOT NULL DEFAULT '95th',
  billing_day TINYINT UNSIGNED NOT NULL DEFAULT 1,
  timezone VARCHAR(64) NOT NULL DEFAULT 'UTC',
  direction VARCHAR(4) NOT NULL DEFAULT 'agg',
  default_layer VARCHAR(16) NOT NULL DEFAULT 'customer',
  cdr_bps BIGINT UNSIGNED NULL,
  quota_bytes BIGINT UNSIGNED NULL,
  reconcile_abs BIGINT UNSIGNED NOT NULL DEFAULT 0,
  reconcile_percent DECIMAL(9,4) NOT NULL DEFAULT 5.0000,
  ref VARCHAR(64) NOT NULL DEFAULT '',
  notes VARCHAR(255) NOT NULL DEFAULT '',
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NULL,
  updated_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_billing_accounts_name (name),
  KEY idx_billing_accounts_list (status,bill_type,name,id),
  CONSTRAINT ck_billing_account_status CHECK (status IN ('active','paused')),
  CONSTRAINT ck_billing_account_type CHECK (bill_type IN ('cdr','quota')),
  CONSTRAINT ck_billing_account_algorithm CHECK (algorithm IN ('95th','average','total')),
  CONSTRAINT ck_billing_account_direction CHECK (direction IN ('in','out','agg')),
  CONSTRAINT ck_billing_account_layer CHECK (default_layer IN ('raw','supplier','customer')),
  CONSTRAINT ck_billing_account_day CHECK (billing_day BETWEEN 1 AND 31),
  CONSTRAINT ck_billing_account_percent CHECK (reconcile_percent BETWEEN 0 AND 100),
  CONSTRAINT fk_billing_accounts_party FOREIGN KEY (party_id) REFERENCES parties(id) ON DELETE RESTRICT,
  CONSTRAINT fk_billing_accounts_created_by FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT fk_billing_accounts_updated_by FOREIGN KEY (updated_by) REFERENCES users(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE billing_account_ports (
  account_id CHAR(26) NOT NULL,
  port_id CHAR(26) NOT NULL,
  direction VARCHAR(4) NOT NULL DEFAULT 'agg',
  created_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (account_id,port_id),
  KEY idx_billing_account_ports_port (port_id,account_id),
  CONSTRAINT ck_billing_port_direction CHECK (direction IN ('in','out','agg')),
  CONSTRAINT fk_billing_port_account FOREIGN KEY (account_id) REFERENCES billing_accounts(id) ON DELETE CASCADE,
  CONSTRAINT fk_billing_port_port FOREIGN KEY (port_id) REFERENCES ports(id) ON DELETE RESTRICT,
  CONSTRAINT fk_billing_port_created_by FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE billing_periods (
  id CHAR(26) NOT NULL,
  account_id CHAR(26) NOT NULL,
  date_from DATETIME(3) NOT NULL,
  date_to DATETIME(3) NOT NULL,
  timezone VARCHAR(64) NOT NULL,
  direction VARCHAR(4) NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'open',
  algorithm VARCHAR(16) NOT NULL,
  default_layer VARCHAR(16) NOT NULL,
  reconcile_abs BIGINT UNSIGNED NOT NULL,
  reconcile_percent DECIMAL(9,4) NOT NULL,
  allowed BIGINT UNSIGNED NULL,
  used BIGINT UNSIGNED NULL,
  overuse BIGINT UNSIGNED NULL,
  calculation_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  publication_ref MEDIUMTEXT NOT NULL,
  account_snapshot_json JSON NOT NULL,
  adjustment_snapshot_json JSON NOT NULL,
  provenance_json JSON NOT NULL,
  approved_calculation_version BIGINT UNSIGNED NULL,
  approved_by CHAR(26) NULL,
  approved_at DATETIME(3) NULL,
  closed_by CHAR(26) NULL,
  closed_at DATETIME(3) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_billing_period_window (account_id,date_from,date_to),
  KEY idx_billing_period_list (account_id,status,date_from,id),
  CONSTRAINT ck_billing_period_window CHECK (date_to > date_from),
  CONSTRAINT ck_billing_period_status CHECK (status IN ('open','calculated','approved','closed')),
  CONSTRAINT ck_billing_period_direction CHECK (direction IN ('in','out','agg')),
  CONSTRAINT ck_billing_period_algorithm CHECK (algorithm IN ('95th','average','total')),
  CONSTRAINT ck_billing_period_layer CHECK (default_layer IN ('raw','supplier','customer')),
  CONSTRAINT fk_billing_period_account FOREIGN KEY (account_id) REFERENCES billing_accounts(id) ON DELETE RESTRICT,
  CONSTRAINT fk_billing_period_approved_by FOREIGN KEY (approved_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT fk_billing_period_closed_by FOREIGN KEY (closed_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT fk_billing_period_created_by FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE billing_period_ports (
  period_id CHAR(26) NOT NULL,
  port_id CHAR(26) NOT NULL,
  device_id CHAR(26) NOT NULL,
  if_index INT UNSIGNED NOT NULL,
  if_name VARCHAR(255) NOT NULL DEFAULT '',
  direction VARCHAR(4) NOT NULL,
  PRIMARY KEY (period_id,port_id),
  KEY idx_billing_period_ports_scope (period_id,device_id,if_index),
  CONSTRAINT ck_billing_period_port_direction CHECK (direction IN ('in','out','agg')),
  CONSTRAINT fk_billing_period_port_period FOREIGN KEY (period_id) REFERENCES billing_periods(id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE billing_period_values (
  id CHAR(26) NOT NULL,
  period_id CHAR(26) NOT NULL,
  calculation_version BIGINT UNSIGNED NOT NULL,
  layer VARCHAR(16) NOT NULL,
  algorithm VARCHAR(16) NOT NULL,
  unit VARCHAR(8) NOT NULL,
  in_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
  out_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
  selected_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
  rate_95th_bps BIGINT UNSIGNED NOT NULL DEFAULT 0,
  rate_average_bps BIGINT UNSIGNED NOT NULL DEFAULT 0,
  algorithm_value BIGINT UNSIGNED NOT NULL DEFAULT 0,
  coverage DECIMAL(9,6) NOT NULL DEFAULT 0,
  expected_buckets INT UNSIGNED NOT NULL DEFAULT 0,
  observed_buckets INT UNSIGNED NOT NULL DEFAULT 0,
  missing_buckets INT UNSIGNED NOT NULL DEFAULT 0,
  reset_buckets INT UNSIGNED NOT NULL DEFAULT 0,
  gap_buckets INT UNSIGNED NOT NULL DEFAULT 0,
  unknown_sampling_records BIGINT UNSIGNED NOT NULL DEFAULT 0,
  source_generation_min BIGINT UNSIGNED NOT NULL DEFAULT 0,
  source_generation_max BIGINT UNSIGNED NOT NULL DEFAULT 0,
  provenance_json JSON NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_billing_value_generation (period_id,calculation_version,layer),
  KEY idx_billing_value_layer (period_id,layer,calculation_version),
  CONSTRAINT ck_billing_value_layer CHECK (layer IN ('raw','supplier','customer','snmp','external')),
  CONSTRAINT ck_billing_value_algorithm CHECK (algorithm IN ('95th','average','total')),
  CONSTRAINT ck_billing_value_unit CHECK (unit IN ('bps','bytes')),
  CONSTRAINT fk_billing_value_period FOREIGN KEY (period_id) REFERENCES billing_periods(id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE billing_adjustments (
  id CHAR(26) NOT NULL,
  period_id CHAR(26) NOT NULL,
  layer VARCHAR(16) NOT NULL DEFAULT 'customer',
  unit VARCHAR(8) NOT NULL,
  amount BIGINT NOT NULL,
  reason VARCHAR(255) NOT NULL,
  evidence_ref VARCHAR(255) NOT NULL DEFAULT '',
  status VARCHAR(16) NOT NULL DEFAULT 'pending',
  reverses_adjustment_id CHAR(26) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NULL,
  approved_by CHAR(26) NULL,
  approved_at DATETIME(3) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_billing_adjustment_reversal (reverses_adjustment_id),
  KEY idx_billing_adjustment_period (period_id,status,created_at,id),
  CONSTRAINT ck_billing_adjustment_layer CHECK (layer IN ('raw','supplier','customer','snmp','external')),
  CONSTRAINT ck_billing_adjustment_unit CHECK (unit IN ('bps','bytes')),
  CONSTRAINT ck_billing_adjustment_status CHECK (status IN ('pending','approved')),
  CONSTRAINT fk_billing_adjustment_period FOREIGN KEY (period_id) REFERENCES billing_periods(id) ON DELETE RESTRICT,
  CONSTRAINT fk_billing_adjustment_reverses FOREIGN KEY (reverses_adjustment_id) REFERENCES billing_adjustments(id) ON DELETE RESTRICT,
  CONSTRAINT fk_billing_adjustment_created_by FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT fk_billing_adjustment_approved_by FOREIGN KEY (approved_by) REFERENCES users(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE reconciliation_runs (
  id CHAR(26) NOT NULL,
  operation_ref CHAR(26) NOT NULL,
  period_id CHAR(26) NOT NULL,
  calculation_version BIGINT UNSIGNED NOT NULL,
  status VARCHAR(16) NOT NULL,
  threshold_abs BIGINT UNSIGNED NOT NULL,
  threshold_percent DECIMAL(9,4) NOT NULL,
  issue_count INT UNSIGNED NOT NULL DEFAULT 0,
  summary_json JSON NOT NULL,
  created_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_reconciliation_operation (operation_ref),
  KEY idx_reconciliation_runs_period (period_id,calculation_version,created_at,id),
  CONSTRAINT ck_reconciliation_status CHECK (status IN ('ok','issues')),
  CONSTRAINT fk_reconciliation_period FOREIGN KEY (period_id) REFERENCES billing_periods(id) ON DELETE RESTRICT,
  CONSTRAINT fk_reconciliation_created_by FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE reconciliation_issues (
  id CHAR(26) NOT NULL,
  run_id CHAR(26) NOT NULL,
  kind VARCHAR(32) NOT NULL,
  severity VARCHAR(16) NOT NULL DEFAULT 'warning',
  left_layer VARCHAR(16) NOT NULL DEFAULT '',
  right_layer VARCHAR(16) NOT NULL DEFAULT '',
  metric VARCHAR(32) NOT NULL DEFAULT '',
  expected_value BIGINT NOT NULL DEFAULT 0,
  actual_value BIGINT NOT NULL DEFAULT 0,
  delta_value BIGINT NOT NULL DEFAULT 0,
  threshold_value BIGINT UNSIGNED NOT NULL DEFAULT 0,
  status VARCHAR(16) NOT NULL DEFAULT 'open',
  detail_json JSON NOT NULL,
  resolution_note VARCHAR(255) NOT NULL DEFAULT '',
  resolved_by CHAR(26) NULL,
  resolved_at DATETIME(3) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_reconciliation_issue_run (run_id,status,severity,created_at,id),
  CONSTRAINT ck_reconciliation_issue_severity CHECK (severity IN ('info','warning','critical')),
  CONSTRAINT ck_reconciliation_issue_status CHECK (status IN ('open','acknowledged','resolved')),
  CONSTRAINT fk_reconciliation_issue_run FOREIGN KEY (run_id) REFERENCES reconciliation_runs(id) ON DELETE RESTRICT,
  CONSTRAINT fk_reconciliation_issue_resolver FOREIGN KEY (resolved_by) REFERENCES users(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE user_billing_permissions (
  user_id CHAR(26) NOT NULL,
  account_id CHAR(26) NOT NULL,
  PRIMARY KEY (user_id,account_id),
  CONSTRAINT fk_user_billing_permission_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  CONSTRAINT fk_user_billing_permission_account FOREIGN KEY (account_id) REFERENCES billing_accounts(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

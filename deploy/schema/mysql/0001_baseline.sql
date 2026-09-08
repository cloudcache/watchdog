-- Watchdog v2 MySQL baseline (KISS single-domain management store).
-- Target architecture: docs/watchdog-kiss-architecture.md (ADR-KISS-001).
-- Grounded in LibreNMS's user/role/permission/device/port/billing model, with the
-- KISS three-layer billing evidence added. Time-series (SNMP/system) and log/alert
-- data live in ClickHouse (deploy/schema/clickhouse), NOT here.
--
-- Conventions: InnoDB/utf8mb4; CHAR(26) ULID primary keys generated in Go; every
-- human-writable object carries row_version + created_by/updated_by + created_at/
-- updated_at; UTC timestamps via DATETIME(3). No tenant_id anywhere.

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 1;

-- ---------------------------------------------------------------------------
-- Install / configuration / traceability
-- ---------------------------------------------------------------------------

-- Applied schema versions (both MySQL and ClickHouse baselines record here).
CREATE TABLE IF NOT EXISTS schema_migrations (
  version      VARCHAR(64)  NOT NULL,
  store        VARCHAR(16)  NOT NULL DEFAULT 'mysql',   -- mysql | clickhouse
  checksum     CHAR(64)     NOT NULL,
  applied_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (store, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Single-row install state: schema version, when initialised, whether the first
-- administrator has been bootstrapped. Lets the server report a traceable status.
CREATE TABLE IF NOT EXISTS watchdog_installation (
  id                 TINYINT UNSIGNED NOT NULL DEFAULT 1,
  schema_version     VARCHAR(64)  NOT NULL,
  product_version    VARCHAR(64)  NOT NULL DEFAULT '',
  admin_bootstrapped TINYINT(1)   NOT NULL DEFAULT 0,
  installed_at       DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at         DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  CONSTRAINT chk_installation_singleton CHECK (id = 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Manageable global configuration (retention windows, thresholds, defaults...).
-- value_json keeps the value typed; every change is auditable via updated_by.
CREATE TABLE IF NOT EXISTS settings (
  `key`       VARCHAR(190) NOT NULL,
  value_json  JSON         NOT NULL,
  description VARCHAR(255) NOT NULL DEFAULT '',
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  updated_by  CHAR(26)     NULL,
  created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- Auth / RBAC (LibreNMS role+ability model; MySQL is the only auth authority)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS users (
  id             CHAR(26)     NOT NULL,
  username       VARCHAR(190) NOT NULL,
  email          VARCHAR(190) NOT NULL DEFAULT '',
  display_name   VARCHAR(190) NOT NULL DEFAULT '',
  password_hash  VARCHAR(255) NULL,          -- bcrypt; NULL only for externally-authed
  status         VARCHAR(16)  NOT NULL DEFAULT 'active',   -- active | disabled
  can_modify_password TINYINT(1) NOT NULL DEFAULT 1,
  row_version    BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by     CHAR(26)     NULL,
  updated_by     CHAR(26)     NULL,
  created_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_users_username (username),
  KEY idx_users_email (email)   -- contact field; username is the unique login identity
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Server-side sessions: cookie holds an opaque token; we store only its SHA-256.
CREATE TABLE IF NOT EXISTS sessions (
  id             CHAR(26)     NOT NULL,
  user_id        CHAR(26)     NOT NULL,
  token_sha256   CHAR(64)     NOT NULL,
  client_digest  VARCHAR(255) NOT NULL DEFAULT '',   -- UA/IP summary, not PII store
  created_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  last_active_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  expires_at     DATETIME(3)  NOT NULL,
  revoked_at     DATETIME(3)  NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_sessions_token (token_sha256),
  KEY idx_sessions_user (user_id, revoked_at, expires_at),
  CONSTRAINT fk_sessions_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS user_preferences (
  user_id    CHAR(26)    NOT NULL,
  prefs_json JSON        NOT NULL,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (user_id),
  CONSTRAINT fk_user_preferences_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS roles (
  id          CHAR(26)     NOT NULL,
  name        VARCHAR(64)  NOT NULL,          -- administrator|operator|analyst|billing|viewer|...
  title       VARCHAR(190) NOT NULL DEFAULT '',
  protected   TINYINT(1)   NOT NULL DEFAULT 0, -- built-in roles cannot be deleted/renamed
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_roles_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Fixed ability catalogue (e.g. device.view, device.viewAll, bill.calculate, ...).
CREATE TABLE IF NOT EXISTS permissions (
  id       CHAR(26)     NOT NULL,
  ability  VARCHAR(64)  NOT NULL,             -- "{subject}.{action}"
  subject  VARCHAR(32)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_permissions_ability (ability),
  KEY idx_permissions_subject (subject)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS role_permissions (
  role_id       CHAR(26) NOT NULL,
  permission_id CHAR(26) NOT NULL,
  PRIMARY KEY (role_id, permission_id),
  CONSTRAINT fk_role_permissions_role FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE CASCADE,
  CONSTRAINT fk_role_permissions_perm FOREIGN KEY (permission_id) REFERENCES permissions(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS user_roles (
  user_id CHAR(26) NOT NULL,
  role_id CHAR(26) NOT NULL,
  PRIMARY KEY (user_id, role_id),
  CONSTRAINT fk_user_roles_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  CONSTRAINT fk_user_roles_role FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- Inventory (one device root; LibreNMS-style organisation)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS locations (
  id         CHAR(26)     NOT NULL,
  name       VARCHAR(190) NOT NULL,
  latitude   DECIMAL(10,7) NULL,
  longitude  DECIMAL(10,7) NULL,
  address    VARCHAR(255) NOT NULL DEFAULT '',
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26)     NULL,
  updated_by CHAR(26)     NULL,
  created_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_locations_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS snmp_profiles (
  id            CHAR(26)     NOT NULL,
  name          VARCHAR(190) NOT NULL,
  version       VARCHAR(8)   NOT NULL,           -- v1 | v2c | v3
  port          SMALLINT UNSIGNED NOT NULL DEFAULT 161,
  security_json JSON         NOT NULL,           -- community / v3 creds, secrets encrypted
  timeout_ms    INT UNSIGNED NOT NULL DEFAULT 3000,
  retries       TINYINT UNSIGNED NOT NULL DEFAULT 2,
  row_version   BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by    CHAR(26)     NULL,
  updated_by    CHAR(26)     NULL,
  created_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_snmp_profiles_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS devices (
  id            CHAR(26)     NOT NULL,
  host          VARCHAR(255) NOT NULL,           -- IP or FQDN; unique add key
  display_name  VARCHAR(190) NOT NULL DEFAULT '',
  kind          VARCHAR(16)  NOT NULL DEFAULT 'network', -- network | host
  sys_name      VARCHAR(255) NOT NULL DEFAULT '',
  sys_descr     TEXT         NULL,
  sys_object_id VARCHAR(255) NOT NULL DEFAULT '',
  sys_contact   VARCHAR(255) NOT NULL DEFAULT '',
  os            VARCHAR(64)  NOT NULL DEFAULT '',
  os_version    VARCHAR(128) NOT NULL DEFAULT '',
  hardware      VARCHAR(190) NOT NULL DEFAULT '',
  serial        VARCHAR(190) NOT NULL DEFAULT '',
  location_id   CHAR(26)     NULL,
  snmp_profile_id CHAR(26)   NULL,
  status        VARCHAR(16)  NOT NULL DEFAULT 'pending', -- pending | up | down | paused
  status_reason VARCHAR(64)  NOT NULL DEFAULT '',
  disabled      TINYINT(1)   NOT NULL DEFAULT 0,
  ignore_alerts TINYINT(1)   NOT NULL DEFAULT 0,
  uptime_seconds BIGINT UNSIGNED NOT NULL DEFAULT 0,
  last_polled_at DATETIME(3) NULL,
  row_version   BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by    CHAR(26)     NULL,
  updated_by    CHAR(26)     NULL,
  created_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_devices_host (host),
  KEY idx_devices_status (status, disabled),
  KEY idx_devices_location (location_id),
  CONSTRAINT fk_devices_location FOREIGN KEY (location_id) REFERENCES locations(id) ON DELETE SET NULL,
  CONSTRAINT fk_devices_snmp_profile FOREIGN KEY (snmp_profile_id) REFERENCES snmp_profiles(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS device_groups (
  id          CHAR(26)     NOT NULL,
  name        VARCHAR(190) NOT NULL,
  kind        VARCHAR(16)  NOT NULL DEFAULT 'static',  -- static | dynamic
  rule_json   JSON         NULL,                       -- dynamic match rule
  description VARCHAR(255) NOT NULL DEFAULT '',
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by  CHAR(26)     NULL,
  updated_by  CHAR(26)     NULL,
  created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_device_groups_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS device_group_members (
  device_group_id CHAR(26) NOT NULL,
  device_id       CHAR(26) NOT NULL,
  PRIMARY KEY (device_group_id, device_id),
  KEY idx_device_group_members_device (device_id),
  CONSTRAINT fk_dgm_group FOREIGN KEY (device_group_id) REFERENCES device_groups(id) ON DELETE CASCADE,
  CONSTRAINT fk_dgm_device FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ports (
  id             CHAR(26)     NOT NULL,
  device_id      CHAR(26)     NOT NULL,
  if_index       BIGINT       NOT NULL,
  if_name        VARCHAR(190) NOT NULL DEFAULT '',
  if_descr       VARCHAR(255) NOT NULL DEFAULT '',
  if_alias       VARCHAR(255) NOT NULL DEFAULT '',
  if_speed       BIGINT UNSIGNED NULL,           -- bits/sec (billing sanity ceiling)
  if_high_speed  INT UNSIGNED NULL,              -- Mbit/s
  if_type        VARCHAR(48)  NOT NULL DEFAULT '',
  if_mtu         INT          NULL,
  if_phys_address VARCHAR(32) NOT NULL DEFAULT '',
  if_oper_status  VARCHAR(16) NOT NULL DEFAULT '',
  if_admin_status VARCHAR(16) NOT NULL DEFAULT '',
  disabled       TINYINT(1)   NOT NULL DEFAULT 0,
  ignore_alerts  TINYINT(1)   NOT NULL DEFAULT 0,
  discovered_at  DATETIME(3)  NULL,
  row_version    BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_ports_device_ifindex (device_id, if_index),
  KEY idx_ports_device (device_id),
  CONSTRAINT fk_ports_device FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS interface_addresses (
  id         CHAR(26)      NOT NULL,
  device_id  CHAR(26)      NOT NULL,
  port_id    CHAR(26)      NULL,
  family     TINYINT UNSIGNED NOT NULL,          -- 4 | 6
  address    VARBINARY(16) NOT NULL,
  prefix_len TINYINT UNSIGNED NOT NULL,
  context    VARCHAR(96)   NOT NULL DEFAULT '',   -- VRF/context
  created_at DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_iface_addr_device (device_id),
  KEY idx_iface_addr_port (port_id),
  CONSTRAINT fk_iface_addr_device FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE,
  CONSTRAINT fk_iface_addr_port FOREIGN KEY (port_id) REFERENCES ports(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS bgp_sessions (
  id           CHAR(26)     NOT NULL,
  device_id    CHAR(26)     NOT NULL,
  peer_address VARCHAR(64)  NOT NULL,
  peer_as      INT UNSIGNED NOT NULL,
  afi          VARCHAR(16)  NOT NULL DEFAULT '',
  safi         VARCHAR(16)  NOT NULL DEFAULT '',
  state        VARCHAR(24)  NOT NULL DEFAULT '',
  prefixes     INT UNSIGNED NOT NULL DEFAULT 0,
  updated_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_bgp_device_peer (device_id, peer_address, afi, safi),
  CONSTRAINT fk_bgp_device FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS sensors (
  id          CHAR(26)     NOT NULL,
  device_id   CHAR(26)     NOT NULL,
  class       VARCHAR(32)  NOT NULL,             -- temperature|voltage|dbm|...
  label       VARCHAR(190) NOT NULL DEFAULT '',
  oid_index   VARCHAR(96)  NOT NULL DEFAULT '',
  updated_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_sensors_device (device_id),
  CONSTRAINT fk_sensors_device FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS physical_entities (
  id           CHAR(26)     NOT NULL,
  device_id    CHAR(26)     NOT NULL,
  entity_index VARCHAR(96)  NOT NULL,
  name         VARCHAR(190) NOT NULL DEFAULT '',
  class        VARCHAR(48)  NOT NULL DEFAULT '',
  serial       VARCHAR(190) NOT NULL DEFAULT '',
  updated_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_phys_device_index (device_id, entity_index),
  CONSTRAINT fk_phys_device FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS vlans (
  id         CHAR(26)     NOT NULL,
  device_id  CHAR(26)     NOT NULL,
  vlan_id    INT UNSIGNED NOT NULL,
  name       VARCHAR(190) NOT NULL DEFAULT '',
  updated_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_vlans_device_vlan (device_id, vlan_id),
  CONSTRAINT fk_vlans_device FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS lag_groups (
  id           CHAR(26)     NOT NULL,
  device_id    CHAR(26)     NOT NULL,
  lag_if_index BIGINT       NOT NULL,
  name         VARCHAR(190) NOT NULL DEFAULT '',
  updated_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_lag_device_index (device_id, lag_if_index),
  CONSTRAINT fk_lag_device FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- Per-object RBAC grants (LibreNMS *_perms; "ownership" = a grant row exists)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS user_device_permissions (
  user_id   CHAR(26) NOT NULL,
  device_id CHAR(26) NOT NULL,
  PRIMARY KEY (user_id, device_id),
  CONSTRAINT fk_udp_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  CONSTRAINT fk_udp_device FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS user_device_group_permissions (
  user_id         CHAR(26) NOT NULL,
  device_group_id CHAR(26) NOT NULL,
  PRIMARY KEY (user_id, device_group_id),
  CONSTRAINT fk_udgp_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  CONSTRAINT fk_udgp_group FOREIGN KEY (device_group_id) REFERENCES device_groups(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS user_port_permissions (
  user_id CHAR(26) NOT NULL,
  port_id CHAR(26) NOT NULL,
  PRIMARY KEY (user_id, port_id),
  CONSTRAINT fk_upp_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  CONSTRAINT fk_upp_port FOREIGN KEY (port_id) REFERENCES ports(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- Operations (one async job state machine; audit)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS operation_jobs (
  id              CHAR(26)     NOT NULL,
  job_type        VARCHAR(64)  NOT NULL,
  status          VARCHAR(16)  NOT NULL DEFAULT 'queued', -- queued|running|succeeded|failed|canceled
  idempotency_key VARCHAR(190) NOT NULL DEFAULT '',
  request_hash    CHAR(64)     NOT NULL DEFAULT '',
  payload_json    JSON         NULL,
  progress_done   BIGINT UNSIGNED NOT NULL DEFAULT 0,
  progress_total  BIGINT UNSIGNED NOT NULL DEFAULT 0,
  checkpoint_json JSON         NULL,
  lease_token     CHAR(26)     NULL,
  lease_expires_at DATETIME(3) NULL,
  result_ref      VARCHAR(255) NOT NULL DEFAULT '',
  last_error      TEXT         NULL,
  cancel_requested_at DATETIME(3) NULL,
  finished_at     DATETIME(3)  NULL,
  row_version     BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by      CHAR(26)     NULL,
  created_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_operation_jobs_idem (job_type, idempotency_key),
  KEY idx_operation_jobs_due (status, job_type, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS idempotency_records (
  scope       VARCHAR(64)  NOT NULL,
  idem_key    VARCHAR(190) NOT NULL,
  result_ref  VARCHAR(255) NOT NULL DEFAULT '',
  created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (scope, idem_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS export_tasks (
  id          CHAR(26)     NOT NULL,
  job_id      CHAR(26)     NULL,
  kind        VARCHAR(64)  NOT NULL,
  status      VARCHAR(16)  NOT NULL DEFAULT 'queued',
  format      VARCHAR(16)  NOT NULL DEFAULT 'csv',
  result_ref  VARCHAR(255) NOT NULL DEFAULT '',
  created_by  CHAR(26)     NULL,
  created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_export_tasks_status (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS audit_logs (
  id          CHAR(26)     NOT NULL,
  actor_id    CHAR(26)     NULL,
  action      VARCHAR(64)  NOT NULL,
  resource    VARCHAR(64)  NOT NULL,
  resource_id VARCHAR(64)  NOT NULL DEFAULT '',
  detail_json JSON         NULL,
  occurred_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_audit_logs_time (occurred_at, id),
  KEY idx_audit_logs_resource (resource, resource_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- Billing (LibreNMS bill+ports+period, plus KISS three-layer evidence)
-- ---------------------------------------------------------------------------

-- Counterparties: customer / supplier. NOT auth/isolation boundaries.
CREATE TABLE IF NOT EXISTS parties (
  id          CHAR(26)     NOT NULL,
  kind        VARCHAR(16)  NOT NULL,             -- customer | supplier
  name        VARCHAR(190) NOT NULL,
  ref         VARCHAR(64)  NOT NULL DEFAULT '',
  notes       VARCHAR(255) NOT NULL DEFAULT '',
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by  CHAR(26)     NULL,
  updated_by  CHAR(26)     NULL,
  created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_parties_kind_name (kind, name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS billing_accounts (
  id            CHAR(26)     NOT NULL,
  name          VARCHAR(190) NOT NULL,
  party_id      CHAR(26)     NULL,               -- counterparty
  bill_type     VARCHAR(16)  NOT NULL DEFAULT 'cdr',   -- cdr(95th) | quota(transfer)
  cdr_bps       BIGINT UNSIGNED NULL,            -- allowance when cdr (bits/s)
  quota_bytes   BIGINT UNSIGNED NULL,            -- allowance when quota (bytes)
  bill_day      TINYINT UNSIGNED NOT NULL DEFAULT 1,  -- period rollover day-of-month
  dir_95th      VARCHAR(4)   NOT NULL DEFAULT 'in',    -- in | out | agg
  default_view  VARCHAR(16)  NOT NULL DEFAULT 'raw',
  ref           VARCHAR(64)  NOT NULL DEFAULT '',
  notes         VARCHAR(255) NOT NULL DEFAULT '',
  row_version   BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by    CHAR(26)     NULL,
  updated_by    CHAR(26)     NULL,
  created_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_billing_accounts_name (name),
  CONSTRAINT fk_billing_accounts_party FOREIGN KEY (party_id) REFERENCES parties(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS billing_account_ports (
  account_id CHAR(26)    NOT NULL,
  port_id    CHAR(26)    NOT NULL,
  direction  VARCHAR(4)  NOT NULL DEFAULT 'agg',   -- in | out | agg
  PRIMARY KEY (account_id, port_id),
  KEY idx_bap_port (port_id),
  CONSTRAINT fk_bap_account FOREIGN KEY (account_id) REFERENCES billing_accounts(id) ON DELETE CASCADE,
  CONSTRAINT fk_bap_port FOREIGN KEY (port_id) REFERENCES ports(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- One row per account per billing period; frozen once approved/closed.
CREATE TABLE IF NOT EXISTS billing_periods (
  id             CHAR(26)     NOT NULL,
  account_id     CHAR(26)     NOT NULL,
  date_from      DATETIME(3)  NOT NULL,
  date_to        DATETIME(3)  NOT NULL,
  status         VARCHAR(16)  NOT NULL DEFAULT 'open',  -- open | approved | closed
  algorithm      VARCHAR(16)  NOT NULL DEFAULT '95th',  -- 95th | average | total
  allowed        BIGINT UNSIGNED NULL,
  used           BIGINT UNSIGNED NULL,
  overuse        BIGINT UNSIGNED NULL,
  sampling_complete DECIMAL(6,4) NULL,               -- 0..1 coverage
  publication_ref VARCHAR(128) NOT NULL DEFAULT '',   -- geo/address publication version, frozen
  approved_by    CHAR(26)     NULL,
  approved_at    DATETIME(3)  NULL,
  row_version    BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_billing_periods_window (account_id, date_from, date_to),
  CONSTRAINT fk_billing_periods_account FOREIGN KEY (account_id) REFERENCES billing_accounts(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Per-layer / per-source computed evidence for a period (raw/supplier/customer
-- from flow; interface total from SNMP; the counterparty's imported figure).
CREATE TABLE IF NOT EXISTS billing_period_values (
  id         CHAR(26)     NOT NULL,
  period_id  CHAR(26)     NOT NULL,
  layer      VARCHAR(16)  NOT NULL,               -- raw | supplier | customer | snmp | external
  in_bytes   BIGINT UNSIGNED NULL,
  out_bytes  BIGINT UNSIGNED NULL,
  total_bytes BIGINT UNSIGNED NULL,
  rate_95th  BIGINT UNSIGNED NULL,
  rate_avg   BIGINT UNSIGNED NULL,
  detail_json JSON        NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_bpv_period_layer (period_id, layer),
  CONSTRAINT fk_bpv_period FOREIGN KEY (period_id) REFERENCES billing_periods(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS billing_adjustments (
  id          CHAR(26)     NOT NULL,
  period_id   CHAR(26)     NOT NULL,
  amount      BIGINT       NOT NULL,              -- signed adjustment (bytes or bps per algo)
  reason      VARCHAR(255) NOT NULL DEFAULT '',
  evidence_ref VARCHAR(255) NOT NULL DEFAULT '',
  created_by  CHAR(26)     NULL,
  approved_by CHAR(26)     NULL,
  created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_billing_adjustments_period (period_id),
  CONSTRAINT fk_billing_adjustments_period FOREIGN KEY (period_id) REFERENCES billing_periods(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS reconciliation_runs (
  id          CHAR(26)     NOT NULL,
  period_id   CHAR(26)     NOT NULL,
  status      VARCHAR(16)  NOT NULL DEFAULT 'ok',   -- ok | issues
  summary_json JSON        NULL,
  created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_recon_runs_period (period_id),
  CONSTRAINT fk_recon_runs_period FOREIGN KEY (period_id) REFERENCES billing_periods(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS reconciliation_issues (
  id          CHAR(26)     NOT NULL,
  run_id      CHAR(26)     NOT NULL,
  kind        VARCHAR(32)  NOT NULL,               -- coverage|missing_sampling|counter_reset|missing_bucket|threshold
  severity    VARCHAR(16)  NOT NULL DEFAULT 'warning',
  detail_json JSON         NULL,
  created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_recon_issues_run (run_id),
  CONSTRAINT fk_recon_issues_run FOREIGN KEY (run_id) REFERENCES reconciliation_runs(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS user_billing_permissions (
  user_id    CHAR(26) NOT NULL,
  account_id CHAR(26) NOT NULL,
  PRIMARY KEY (user_id, account_id),
  CONSTRAINT fk_ubp_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  CONSTRAINT fk_ubp_account FOREIGN KEY (account_id) REFERENCES billing_accounts(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Customer source boundaries are operational Flow configuration, not rows in
-- the shared Geo/operator address library. One observed device can serve many
-- customers and each customer can own many editable IPv4/IPv6 source CIDRs.

CREATE TABLE IF NOT EXISTS flow_device_customers (
  id           CHAR(26)        NOT NULL,
  device_id    CHAR(26)        NOT NULL,
  customer_id  CHAR(26)        NOT NULL,
  row_version  BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by   CHAR(26)        NULL,
  updated_by   CHAR(26)        NULL,
  created_at   DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at   DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_flow_device_customer (device_id, customer_id),
  KEY idx_flow_device_customers_customer (customer_id, device_id),
  CONSTRAINT fk_flow_device_customers_device FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE,
  CONSTRAINT fk_flow_device_customers_customer FOREIGN KEY (customer_id) REFERENCES parties(id) ON DELETE RESTRICT,
  CONSTRAINT fk_flow_device_customers_created_by FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT fk_flow_device_customers_updated_by FOREIGN KEY (updated_by) REFERENCES users(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS flow_customer_source_prefixes (
  id                  CHAR(26)        NOT NULL,
  device_customer_id  CHAR(26)        NOT NULL,
  cidr                VARCHAR(45)     NOT NULL,
  family              TINYINT UNSIGNED NOT NULL,
  prefix_length       TINYINT UNSIGNED NOT NULL,
  row_version         BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_flow_customer_source_prefix (device_customer_id, cidr),
  KEY idx_flow_customer_source_prefix_cidr (family, prefix_length, cidr),
  CONSTRAINT fk_flow_customer_source_prefix_binding FOREIGN KEY (device_customer_id) REFERENCES flow_device_customers(id) ON DELETE CASCADE,
  CONSTRAINT chk_flow_customer_source_prefix_family CHECK (family IN (4,6)),
  CONSTRAINT chk_flow_customer_source_prefix_length CHECK (
    (family=4 AND prefix_length BETWEEN 0 AND 32) OR
    (family=6 AND prefix_length BETWEEN 0 AND 128)
  )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

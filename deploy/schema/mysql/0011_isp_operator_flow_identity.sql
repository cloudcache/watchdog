-- Watchdog v2 address library — ISP operator Flow-id allocation (KISS-05).
-- De-tenanted port of legacy migration 051. Each operator gets a stable UInt16
-- flow_isp_id from a monotonic sequence; the id namespace is 1..65535 (0 is
-- reserved for unknown). With no tenant, the sequence is a single global row.

CREATE TABLE IF NOT EXISTS isp_operator_flow_id_sequences (
  id               TINYINT UNSIGNED NOT NULL DEFAULT 1, -- singleton
  next_flow_isp_id INT UNSIGNED NOT NULL DEFAULT 1,
  updated_at       DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  CHECK (id = 1),
  CHECK (next_flow_isp_id BETWEEN 1 AND 65536)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS isp_operator_flow_ids (
  flow_isp_id  SMALLINT UNSIGNED NOT NULL,
  operator_id  CHAR(26)     NOT NULL,
  allocated_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (flow_isp_id),
  UNIQUE KEY uq_isp_operator_flow_ids_operator (operator_id),
  CHECK (flow_isp_id > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- The role gate answers what a user may do. These tables are the resource
-- gate for saved SNMP graphs and metric names, matching the existing
-- device/port/billing grants. A user with no metric rows keeps the historical
-- "all metrics inside an allowed device/port" behaviour; once rows exist they
-- form an explicit metric allow-list.
CREATE TABLE IF NOT EXISTS user_aggregate_graph_permissions (
  user_id            CHAR(26)    NOT NULL,
  aggregate_graph_id VARCHAR(64) NOT NULL,
  PRIMARY KEY (user_id, aggregate_graph_id),
  CONSTRAINT fk_uagp_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  CONSTRAINT fk_uagp_graph FOREIGN KEY (aggregate_graph_id) REFERENCES aggregate_graphs(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS user_metric_permissions (
  user_id CHAR(26)     NOT NULL,
  metric  VARCHAR(190) NOT NULL,
  PRIMARY KEY (user_id, metric),
  CONSTRAINT fk_ump_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

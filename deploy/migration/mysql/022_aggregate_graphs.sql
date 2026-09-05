-- Aggregate graph tables previously existed only in install/init.sql, so a
-- runner-upgraded database (the hub applies embedded migrations on boot) was
-- missing them entirely. IF NOT EXISTS keeps init.sql-provisioned databases
-- idempotent.

CREATE TABLE IF NOT EXISTS aggregate_graphs (
  id VARCHAR(64) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  name VARCHAR(190) NOT NULL,
  aggregation VARCHAR(32) NOT NULL DEFAULT 'sum',
  value_mode VARCHAR(16) NOT NULL DEFAULT 'corrected',
  unit VARCHAR(64) NOT NULL DEFAULT '',
  description TEXT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_aggregate_graphs_tenant_name (tenant_id, name),
  KEY idx_aggregate_graphs_tenant (tenant_id),
  CONSTRAINT fk_aggregate_graphs_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (aggregation IN ('sum', 'avg', 'max', 'min', 'count')),
  CHECK (value_mode IN ('corrected', 'raw', 'both'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS aggregate_graph_items (
  id VARCHAR(64) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  aggregate_graph_id VARCHAR(64) NOT NULL,
  sequence INT UNSIGNED NOT NULL DEFAULT 0,
  metric VARCHAR(190) NOT NULL,
  direction VARCHAR(16) NOT NULL DEFAULT 'other',
  label VARCHAR(190) NOT NULL DEFAULT '',
  graph_type VARCHAR(16) NOT NULL DEFAULT 'line',
  total BOOLEAN NOT NULL DEFAULT FALSE,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  KEY idx_aggregate_graph_items_graph (tenant_id, aggregate_graph_id, sequence),
  CONSTRAINT fk_aggregate_graph_items_graph FOREIGN KEY (aggregate_graph_id) REFERENCES aggregate_graphs(id) ON DELETE CASCADE,
  CONSTRAINT fk_aggregate_graph_items_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (direction IN ('in', 'out', 'other')),
  CHECK (graph_type IN ('line', 'area', 'stack'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS aggregate_graph_ports (
  aggregate_graph_id VARCHAR(64) NOT NULL,
  tenant_id CHAR(26) NOT NULL,
  port_id CHAR(26) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (aggregate_graph_id, port_id),
  KEY idx_aggregate_graph_ports_port (tenant_id, port_id),
  CONSTRAINT fk_aggregate_graph_ports_graph FOREIGN KEY (aggregate_graph_id) REFERENCES aggregate_graphs(id) ON DELETE CASCADE,
  CONSTRAINT fk_aggregate_graph_ports_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_aggregate_graph_ports_port FOREIGN KEY (port_id) REFERENCES network_ports(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS aggregate_graph_data (
  id VARCHAR(64) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  aggregate_graph_id VARCHAR(64) NOT NULL,
  item_id CHAR(26) NOT NULL,
  timestamp DATETIME(3) NOT NULL,
  value DOUBLE NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_aggregate_graph_data_ts (tenant_id, aggregate_graph_id, item_id, timestamp),
  KEY idx_aggregate_graph_data_graph (tenant_id, aggregate_graph_id, timestamp),
  CONSTRAINT fk_aggregate_graph_data_graph FOREIGN KEY (aggregate_graph_id) REFERENCES aggregate_graphs(id) ON DELETE CASCADE,
  CONSTRAINT fk_aggregate_graph_data_item FOREIGN KEY (item_id) REFERENCES aggregate_graph_items(id) ON DELETE CASCADE,
  CONSTRAINT fk_aggregate_graph_data_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

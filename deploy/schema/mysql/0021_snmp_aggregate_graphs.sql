-- Saved SNMP aggregate graphs are management definitions. Samples and every
-- range/summary query remain in ClickHouse; MySQL stores only the graph,
-- metric membership, and selected device ports.
CREATE TABLE IF NOT EXISTS aggregate_graphs (
  id          VARCHAR(64)  NOT NULL,
  name        VARCHAR(190) NOT NULL,
  aggregation VARCHAR(16)  NOT NULL DEFAULT 'sum',
  value_mode  VARCHAR(16)  NOT NULL DEFAULT 'corrected',
  unit        VARCHAR(64)  NOT NULL DEFAULT '',
  description TEXT         NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by  CHAR(26)     NULL,
  updated_by  CHAR(26)     NULL,
  created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_aggregate_graphs_name (name),
  CONSTRAINT fk_aggregate_graphs_created_by FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT fk_aggregate_graphs_updated_by FOREIGN KEY (updated_by) REFERENCES users(id) ON DELETE SET NULL,
  CHECK (aggregation IN ('sum','avg','max','min','count')),
  CHECK (value_mode IN ('corrected','raw','both'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS aggregate_graph_items (
  id                 VARCHAR(64)  NOT NULL,
  aggregate_graph_id VARCHAR(64)  NOT NULL,
  sequence           INT UNSIGNED NOT NULL DEFAULT 0,
  metric             VARCHAR(190) NOT NULL,
  direction          VARCHAR(16)  NOT NULL DEFAULT 'other',
  label              VARCHAR(190) NOT NULL DEFAULT '',
  graph_type         VARCHAR(16)  NOT NULL DEFAULT 'line',
  total              TINYINT(1)   NOT NULL DEFAULT 0,
  created_at         DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_aggregate_graph_items_sequence (aggregate_graph_id,sequence),
  KEY idx_aggregate_graph_items_metric (metric),
  CONSTRAINT fk_aggregate_graph_items_graph FOREIGN KEY (aggregate_graph_id) REFERENCES aggregate_graphs(id) ON DELETE CASCADE,
  CHECK (direction IN ('in','out','other')),
  CHECK (graph_type IN ('line','area','stack'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS aggregate_graph_ports (
  aggregate_graph_id VARCHAR(64) NOT NULL,
  port_id            CHAR(26)    NOT NULL,
  created_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (aggregate_graph_id,port_id),
  KEY idx_aggregate_graph_ports_port (port_id),
  CONSTRAINT fk_aggregate_graph_ports_graph FOREIGN KEY (aggregate_graph_id) REFERENCES aggregate_graphs(id) ON DELETE CASCADE,
  CONSTRAINT fk_aggregate_graph_ports_port FOREIGN KEY (port_id) REFERENCES ports(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

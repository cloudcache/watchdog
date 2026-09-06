-- P2 Visualization: dashboards are named, owned, versioned layouts that arrange
-- references to existing aggregate graphs (and, later, other visualizations) in
-- a grid. The layout itself is opaque JSON — panels with a graph reference and a
-- grid position — validated at the API boundary, not by the schema. `version`
-- is the user-facing revision counter bumped on every content save; `updated_at`
-- drives the ETag/If-Match optimistic-concurrency contract.
CREATE TABLE IF NOT EXISTS `dashboards` (
  `id` char(26) COLLATE utf8mb4_unicode_ci NOT NULL,
  `tenant_id` char(26) COLLATE utf8mb4_unicode_ci NOT NULL,
  `owner_id` char(26) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `name` varchar(190) COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` text COLLATE utf8mb4_unicode_ci,
  `layout_json` json NOT NULL,
  `version` int unsigned NOT NULL DEFAULT '1',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_dashboards_tenant_name` (`tenant_id`,`name`),
  KEY `idx_dashboards_tenant_owner` (`tenant_id`,`owner_id`),
  KEY `fk_dashboards_owner` (`owner_id`),
  CONSTRAINT `fk_dashboards_owner` FOREIGN KEY (`owner_id`) REFERENCES `users` (`id`) ON DELETE SET NULL,
  CONSTRAINT `fk_dashboards_tenant` FOREIGN KEY (`tenant_id`) REFERENCES `tenants` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

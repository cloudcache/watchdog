INSERT INTO tenants (id, name, status)
VALUES ('tenant_dev', 'Watchdog Dev Tenant', 'active')
ON DUPLICATE KEY UPDATE name = VALUES(name), status = VALUES(status);

INSERT INTO users (id, tenant_id, email, name, status, password_hash)
VALUES ('user_dev', 'tenant_dev', 'dev@watchdog.local', 'Watchdog Dev Admin', 'active', 'dev-auth-disabled')
ON DUPLICATE KEY UPDATE tenant_id = VALUES(tenant_id), name = VALUES(name), status = VALUES(status);

INSERT INTO roles (id, tenant_id, name, scope)
VALUES ('role_dev_admin', 'tenant_dev', 'admin', 'tenant')
ON DUPLICATE KEY UPDATE name = VALUES(name), scope = VALUES(scope);

INSERT IGNORE INTO user_roles (user_id, role_id)
VALUES ('user_dev', 'role_dev_admin');

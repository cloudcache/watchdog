package watchdog

import (
	"context"
	"database/sql"
	"time"
)

func (s *MySQLStore) ListSNMPProfiles(ctx context.Context, tenantID ID) ([]SNMPProfile, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, name, version, security_json, timeout_ms, retries, created_at, updated_at
		FROM snmp_profiles
		WHERE tenant_id = ?
		ORDER BY name
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var profiles []SNMPProfile
	for rows.Next() {
		profile, err := scanSNMPProfile(rows)
		if err != nil {
			return nil, err
		}
		profiles = append(profiles, profile)
	}
	return profiles, rows.Err()
}

func (s *MySQLStore) GetSNMPProfile(ctx context.Context, tenantID, profileID ID) (SNMPProfile, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, name, version, security_json, timeout_ms, retries, created_at, updated_at
		FROM snmp_profiles
		WHERE tenant_id = ? AND id = ?
	`, tenantID, profileID)
	return scanSNMPProfile(row)
}

func (s *MySQLStore) UpsertSNMPProfile(ctx context.Context, profile SNMPProfile) (SNMPProfile, error) {
	securityJSON, err := encodeStringMapJSON(profile.Security)
	if err != nil {
		return profile, err
	}
	timeoutMS := profile.Timeout.Milliseconds()
	if timeoutMS <= 0 {
		timeoutMS = 5_000
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO snmp_profiles (
			id, tenant_id, name, version, security_json, timeout_ms, retries
		) VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			name = VALUES(name),
			version = VALUES(version),
			security_json = VALUES(security_json),
			timeout_ms = VALUES(timeout_ms),
			retries = VALUES(retries),
			updated_at = CURRENT_TIMESTAMP(3)
	`, profile.ID, profile.TenantID, profile.Name, profile.Version, securityJSON, timeoutMS, profile.Retries)
	if err != nil {
		return profile, err
	}
	return s.GetSNMPProfile(ctx, profile.TenantID, profile.ID)
}

func (s *MySQLStore) DeleteSNMPProfile(ctx context.Context, tenantID, profileID ID) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM snmp_profiles
		WHERE tenant_id = ? AND id = ?
	`, tenantID, profileID)
	return err
}

func (s *MySQLStore) ListMIBModules(ctx context.Context) ([]MIBModule, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, source, version, checksum, enabled, created_at, updated_at
		FROM mib_modules
		ORDER BY source, name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var modules []MIBModule
	for rows.Next() {
		module, err := scanMIBModule(rows)
		if err != nil {
			return nil, err
		}
		modules = append(modules, module)
	}
	return modules, rows.Err()
}

func (s *MySQLStore) UpsertMIBModule(ctx context.Context, module MIBModule) (MIBModule, error) {
	if module.ID == "" {
		module.ID = stableID("mib", module.Source, module.Name)
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO mib_modules (
			id, name, source, version, checksum, enabled
		) VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			version = VALUES(version),
			checksum = VALUES(checksum),
			enabled = VALUES(enabled),
			updated_at = CURRENT_TIMESTAMP(3)
	`, module.ID, module.Name, defaultString(module.Source, "librenms"), module.Version, module.Checksum, module.Enabled)
	if err != nil {
		return MIBModule{}, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, source, version, checksum, enabled, created_at, updated_at
		FROM mib_modules
		WHERE id = ?
	`, module.ID)
	if err != nil {
		return MIBModule{}, err
	}
	defer rows.Close()
	if rows.Next() {
		return scanMIBModule(rows)
	}
	return module, rows.Err()
}

func (s *MySQLStore) DeleteMIBModule(ctx context.Context, moduleID ID) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM mib_modules
		WHERE id = ?
	`, moduleID)
	return err
}

func scanSNMPProfile(row rowScanner) (SNMPProfile, error) {
	var profile SNMPProfile
	var securityJSON []byte
	var timeoutMS uint32
	if err := row.Scan(&profile.ID, &profile.TenantID, &profile.Name, &profile.Version, &securityJSON, &timeoutMS, &profile.Retries, &profile.CreatedAt, &profile.UpdatedAt); err != nil {
		return profile, err
	}
	security, err := decodeStringMapJSON(securityJSON)
	if err != nil {
		return profile, err
	}
	profile.Security = security
	profile.Timeout = time.Duration(timeoutMS) * time.Millisecond
	return profile, nil
}

func scanMIBModule(row rowScanner) (MIBModule, error) {
	var module MIBModule
	if err := row.Scan(&module.ID, &module.Name, &module.Source, &module.Version, &module.Checksum, &module.Enabled, &module.CreatedAt, &module.UpdatedAt); err != nil {
		return module, err
	}
	return module, nil
}

var _ SNMPRepository = (*MySQLStore)(nil)

// Keep database/sql imported where row scanners are compile-checked by concrete methods.
var _ = sql.ErrNoRows

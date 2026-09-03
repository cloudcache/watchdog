package watchdog

import (
	"context"
	"encoding/json"
	"errors"

	mysqldriver "github.com/go-sql-driver/mysql"
)

var ErrTargetHostExists = errors.New("target host already exists")

func (s *MySQLStore) ListTargets(ctx context.Context, tenantID ID) ([]Target, error) {
	rows, err := s.db.QueryContext(ctx, targetSelect()+`
		WHERE tenant_id = ?
		ORDER BY name
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var targets []Target
	for rows.Next() {
		target, err := scanTarget(rows)
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	return targets, rows.Err()
}

func (s *MySQLStore) GetTarget(ctx context.Context, tenantID, targetID ID) (Target, error) {
	row := s.db.QueryRowContext(ctx, targetSelect()+`
		WHERE tenant_id = ? AND id = ?
	`, tenantID, targetID)
	return scanTarget(row)
}

func (s *MySQLStore) CreateTarget(ctx context.Context, target Target) (Target, error) {
	if target.ID == "" {
		return Target{}, errors.New("target id is required")
	}
	labelsJSON, err := encodeStringMapJSON(target.Labels)
	if err != nil {
		return Target{}, err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO targets (
			id, tenant_id, name, kind, host, mgmt_ip, status, labels_json
		) VALUES (?, ?, ?, ?, ?, INET6_ATON(?), ?, ?)
	`, target.ID, target.TenantID, target.Name, target.Kind, target.Host, target.Host, defaultString(target.Status, "pending"), labelsJSON)
	if err != nil {
		return Target{}, normalizeTargetWriteError(err)
	}
	return s.GetTarget(ctx, target.TenantID, target.ID)
}

func (s *MySQLStore) CreateNetworkTarget(ctx context.Context, target Target, device NetworkDevice) (Target, NetworkDevice, error) {
	if target.ID == "" || device.ID == "" {
		return Target{}, NetworkDevice{}, errors.New("target and device ids are required")
	}
	labelsJSON, err := encodeStringMapJSON(target.Labels)
	if err != nil {
		return Target{}, NetworkDevice{}, err
	}
	securityJSON, err := encodeStringMapJSON(device.SNMPSecurity)
	if err != nil {
		return Target{}, NetworkDevice{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Target{}, NetworkDevice{}, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO targets (
			id, tenant_id, name, kind, host, mgmt_ip, status, labels_json
		) VALUES (?, ?, ?, ?, ?, INET6_ATON(?), ?, ?)
	`, target.ID, target.TenantID, target.Name, target.Kind, target.Host, target.Host, defaultString(target.Status, "pending"), labelsJSON); err != nil {
		return Target{}, NetworkDevice{}, normalizeTargetWriteError(err)
	}
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO network_devices (
			id, tenant_id, target_id, vendor, model, platform, os_name, os_version,
			sys_object_id, sys_name, sys_descr, sys_location, uptime_seconds,
			snmp_profile_id, snmp_port, snmp_security_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?)
	`, device.ID, device.TenantID, device.TargetID, device.Vendor, device.Model,
		device.Platform, device.OSName, device.OSVersion, device.SysObjectID,
		device.SysName, device.SysDescr, device.SysLocation, uint64(device.Uptime.Seconds()),
		device.SNMPProfileID, normalizeSNMPPort(device.SNMPPort), securityJSON); err != nil {
		return Target{}, NetworkDevice{}, err
	}
	if err = tx.Commit(); err != nil {
		return Target{}, NetworkDevice{}, err
	}
	createdTarget, err := s.GetTarget(ctx, target.TenantID, target.ID)
	if err != nil {
		return Target{}, NetworkDevice{}, err
	}
	createdDevice, err := s.GetDevice(ctx, device.TenantID, device.ID)
	if err != nil {
		return Target{}, NetworkDevice{}, err
	}
	return createdTarget, createdDevice, nil
}

func (s *MySQLStore) UpdateTarget(ctx context.Context, target Target) (Target, error) {
	labelsJSON, err := encodeStringMapJSON(target.Labels)
	if err != nil {
		return Target{}, err
	}
	_, err = s.db.ExecContext(ctx, `
		UPDATE targets
		SET name = ?, kind = ?, host = ?, mgmt_ip = INET6_ATON(?), status = ?, labels_json = ?, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ?
	`, target.Name, target.Kind, target.Host, target.Host, defaultString(target.Status, "pending"), labelsJSON, target.TenantID, target.ID)
	if err != nil {
		return Target{}, normalizeTargetWriteError(err)
	}
	return s.GetTarget(ctx, target.TenantID, target.ID)
}

func (s *MySQLStore) DeleteTarget(ctx context.Context, tenantID, targetID ID) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM targets
		WHERE tenant_id = ? AND id = ?
	`, tenantID, targetID)
	return err
}

func targetSelect() string {
	return `
		SELECT id, tenant_id, name, kind, host, status, labels_json, created_at, updated_at
		FROM targets
	`
}

func scanTarget(row rowScanner) (Target, error) {
	var target Target
	var labelsJSON []byte
	if err := row.Scan(&target.ID, &target.TenantID, &target.Name, &target.Kind, &target.Host, &target.Status, &labelsJSON, &target.CreatedAt, &target.UpdatedAt); err != nil {
		return target, err
	}
	labels, err := decodeStringMapJSON(labelsJSON)
	if err != nil {
		return target, err
	}
	target.Labels = labels
	return target, nil
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func normalizeTargetWriteError(err error) error {
	var mysqlErr *mysqldriver.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return ErrTargetHostExists
	}
	return err
}

func encodeLabelsForTest(labels map[string]string) ([]byte, error) {
	return json.Marshal(labels)
}

var _ TargetRepository = (*MySQLStore)(nil)
var _ NetworkTargetProvisioner = (*MySQLStore)(nil)

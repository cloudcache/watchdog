package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"
)

func (s *MySQLStore) ListDevices(ctx context.Context, tenantID ID) ([]NetworkDevice, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, target_id, vendor, model,
		       COALESCE(platform, ''), COALESCE(os_name, ''), COALESCE(os_version, ''),
		       sys_object_id, sys_name, sys_descr,
		       COALESCE(sys_location, ''), COALESCE(uptime_seconds, 0),
		       COALESCE(snmp_profile_id, ''), snmp_port, COALESCE(snmp_security_json, JSON_OBJECT())
		FROM network_devices
		WHERE tenant_id = ?
		ORDER BY sys_name, id
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var devices []NetworkDevice
	for rows.Next() {
		device, err := scanNetworkDevice(rows)
		if err != nil {
			return nil, err
		}
		devices = append(devices, device)
	}
	return devices, rows.Err()
}

func (s *MySQLStore) GetDevice(ctx context.Context, tenantID, deviceID ID) (NetworkDevice, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, target_id, vendor, model,
		       COALESCE(platform, ''), COALESCE(os_name, ''), COALESCE(os_version, ''),
		       sys_object_id, sys_name, sys_descr,
		       COALESCE(sys_location, ''), COALESCE(uptime_seconds, 0),
		       COALESCE(snmp_profile_id, ''), snmp_port, COALESCE(snmp_security_json, JSON_OBJECT())
		FROM network_devices
		WHERE tenant_id = ? AND id = ?
	`, tenantID, deviceID)
	return scanNetworkDevice(row)
}

func (s *MySQLStore) UpsertDevice(ctx context.Context, device NetworkDevice) (NetworkDevice, error) {
	securityJSON, err := encodeStringMapJSON(device.SNMPSecurity)
	if err != nil {
		return NetworkDevice{}, err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO network_devices (
			id, tenant_id, target_id, vendor, model, platform, os_name, os_version, sys_object_id, sys_name, sys_descr,
			sys_location, uptime_seconds, snmp_profile_id, snmp_port, snmp_security_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?)
		ON DUPLICATE KEY UPDATE
			vendor = COALESCE(NULLIF(VALUES(vendor), ''), vendor),
			model = COALESCE(NULLIF(VALUES(model), ''), model),
			platform = COALESCE(NULLIF(VALUES(platform), ''), platform),
			os_name = COALESCE(NULLIF(VALUES(os_name), ''), os_name),
			os_version = COALESCE(NULLIF(VALUES(os_version), ''), os_version),
			sys_object_id = COALESCE(NULLIF(VALUES(sys_object_id), ''), sys_object_id),
			sys_name = COALESCE(NULLIF(VALUES(sys_name), ''), sys_name),
			sys_descr = COALESCE(NULLIF(VALUES(sys_descr), ''), sys_descr),
			sys_location = COALESCE(NULLIF(VALUES(sys_location), ''), sys_location),
			uptime_seconds = IF(VALUES(uptime_seconds) = 0, uptime_seconds, VALUES(uptime_seconds)),
			snmp_profile_id = COALESCE(VALUES(snmp_profile_id), snmp_profile_id),
			snmp_port = IF(VALUES(snmp_port) = 0, snmp_port, VALUES(snmp_port)),
			snmp_security_json = IF(JSON_LENGTH(VALUES(snmp_security_json)) = 0, snmp_security_json, VALUES(snmp_security_json)),
			updated_at = CURRENT_TIMESTAMP(3)
	`, device.ID, device.TenantID, device.TargetID, device.Vendor, device.Model, device.Platform, device.OSName, device.OSVersion, device.SysObjectID, device.SysName, device.SysDescr, device.SysLocation, uint64(device.Uptime.Seconds()), device.SNMPProfileID, normalizeSNMPPort(device.SNMPPort), securityJSON)
	if err != nil {
		return NetworkDevice{}, err
	}
	created, err := s.GetDevice(ctx, device.TenantID, device.ID)
	if err == nil {
		return created, nil
	}
	if err != sql.ErrNoRows {
		return NetworkDevice{}, err
	}
	return s.GetDeviceByTarget(ctx, device.TenantID, device.TargetID)
}

func (s *MySQLStore) UpdateDeviceInventory(ctx context.Context, device NetworkDevice) (NetworkDevice, error) {
	securityJSON, err := encodeStringMapJSON(device.SNMPSecurity)
	if err != nil {
		return NetworkDevice{}, err
	}
	_, err = s.db.ExecContext(ctx, `
		UPDATE network_devices
		SET target_id = ?,
			vendor = ?,
			model = ?,
			platform = ?,
			os_name = ?,
			os_version = ?,
			sys_object_id = ?,
			sys_name = ?,
			sys_descr = ?,
			sys_location = ?,
			uptime_seconds = ?,
			snmp_profile_id = NULLIF(?, ''),
			snmp_port = ?,
			snmp_security_json = ?,
			updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ?
	`, device.TargetID, device.Vendor, device.Model, device.Platform, device.OSName, device.OSVersion, device.SysObjectID, device.SysName, device.SysDescr, device.SysLocation, uint64(device.Uptime.Seconds()), device.SNMPProfileID, normalizeSNMPPort(device.SNMPPort), securityJSON, device.TenantID, device.ID)
	if err != nil {
		return NetworkDevice{}, err
	}
	return s.GetDevice(ctx, device.TenantID, device.ID)
}

func (s *MySQLStore) GetDeviceByTarget(ctx context.Context, tenantID, targetID ID) (NetworkDevice, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, target_id, vendor, model,
		       COALESCE(platform, ''), COALESCE(os_name, ''), COALESCE(os_version, ''),
		       sys_object_id, sys_name, sys_descr,
		       COALESCE(sys_location, ''), COALESCE(uptime_seconds, 0),
		       COALESCE(snmp_profile_id, ''), snmp_port, COALESCE(snmp_security_json, JSON_OBJECT())
		FROM network_devices
		WHERE tenant_id = ? AND target_id = ?
	`, tenantID, targetID)
	return scanNetworkDevice(row)
}

func (s *MySQLStore) DeleteDevice(ctx context.Context, tenantID, deviceID ID) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM network_devices
		WHERE tenant_id = ? AND id = ?
	`, tenantID, deviceID)
	return err
}

func (s *MySQLStore) ListPorts(ctx context.Context, tenantID, deviceID ID) ([]NetworkPort, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, device_id, if_index, if_name, if_alias, if_descr, admin_status, oper_status, speed_bps, metadata_json
		FROM network_ports
		WHERE tenant_id = ? AND device_id = ?
		ORDER BY if_index
	`, tenantID, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ports []NetworkPort
	for rows.Next() {
		port, err := scanNetworkPort(rows)
		if err != nil {
			return nil, err
		}
		ports = append(ports, port)
	}
	return ports, rows.Err()
}

func (s *MySQLStore) GetPort(ctx context.Context, tenantID, portID ID) (NetworkPort, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, device_id, if_index, if_name, if_alias, if_descr, admin_status, oper_status, speed_bps, metadata_json
		FROM network_ports
		WHERE tenant_id = ? AND id = ?
	`, tenantID, portID)
	return scanNetworkPort(row)
}

func (s *MySQLStore) UpsertPorts(ctx context.Context, ports []NetworkPort) error {
	if len(ports) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO network_ports (
			id, tenant_id, device_id, if_index, if_name, if_alias, if_descr, admin_status, oper_status, speed_bps, metadata_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			if_name = VALUES(if_name),
			if_alias = VALUES(if_alias),
			if_descr = VALUES(if_descr),
			admin_status = VALUES(admin_status),
			oper_status = VALUES(oper_status),
			speed_bps = VALUES(speed_bps),
			metadata_json = VALUES(metadata_json),
			updated_at = CURRENT_TIMESTAMP(3)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, port := range ports {
		metadataJSON, err := encodeStringMapJSON(port.Metadata)
		if err != nil {
			return err
		}
		if _, err := stmt.ExecContext(ctx, port.ID, port.TenantID, port.DeviceID, port.IfIndex, port.IfName, port.IfAlias, port.IfDescr, NormalizeIfStatus(port.AdminStatus), NormalizeIfStatus(port.OperStatus), port.SpeedBps, metadataJSON); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *MySQLStore) DeletePort(ctx context.Context, tenantID, portID ID) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM network_ports
		WHERE tenant_id = ? AND id = ?
	`, tenantID, portID)
	return err
}

func (s *MySQLStore) UpsertPortTransceiver(ctx context.Context, transceiver NetworkPortTransceiver) (NetworkPortTransceiver, error) {
	rawJSON, err := encodeAnyMapJSON(transceiver.Raw)
	if err != nil {
		return NetworkPortTransceiver{}, err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO network_port_transceivers (
			id, tenant_id, port_id, module_type, vendor, model, serial, wavelength_nm, distance_m, connector, raw_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, NULLIF(?, 0), NULLIF(?, 0), ?, ?)
		ON DUPLICATE KEY UPDATE
			module_type = VALUES(module_type),
			vendor = VALUES(vendor),
			model = VALUES(model),
			serial = VALUES(serial),
			wavelength_nm = VALUES(wavelength_nm),
			distance_m = VALUES(distance_m),
			connector = VALUES(connector),
			raw_json = VALUES(raw_json),
			updated_at = CURRENT_TIMESTAMP(3)
	`, transceiver.ID, transceiver.TenantID, transceiver.PortID, transceiver.ModuleType, transceiver.Vendor, transceiver.Model, transceiver.Serial, transceiver.WavelengthNM, transceiver.DistanceM, transceiver.Connector, rawJSON)
	if err != nil {
		return NetworkPortTransceiver{}, err
	}
	row := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, port_id, module_type, vendor, model, serial, COALESCE(wavelength_nm, 0), COALESCE(distance_m, 0), connector, raw_json, updated_at
		FROM network_port_transceivers
		WHERE tenant_id = ? AND port_id = ?
	`, transceiver.TenantID, transceiver.PortID)
	return scanNetworkPortTransceiver(row)
}

func (s *MySQLStore) GetPortTransceiver(ctx context.Context, tenantID, portID ID) (NetworkPortTransceiver, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, port_id, module_type, vendor, model, serial, COALESCE(wavelength_nm, 0), COALESCE(distance_m, 0), connector, raw_json, updated_at
		FROM network_port_transceivers
		WHERE tenant_id = ? AND port_id = ?
	`, tenantID, portID)
	return scanNetworkPortTransceiver(row)
}

func (s *MySQLStore) ListDeviceSensors(ctx context.Context, tenantID, deviceID ID) ([]NetworkDeviceSensor, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, device_id, COALESCE(port_id, ''), sensor_index, sensor_class, name, oid, unit,
		       COALESCE(current_value, 0), COALESCE(warn_limit, 0), COALESCE(crit_limit, 0), status, metadata_json, updated_at
		FROM network_device_sensors
		WHERE tenant_id = ? AND device_id = ?
		ORDER BY sensor_class, sensor_index, name
	`, tenantID, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sensors []NetworkDeviceSensor
	for rows.Next() {
		sensor, err := scanNetworkDeviceSensor(rows)
		if err != nil {
			return nil, err
		}
		sensors = append(sensors, sensor)
	}
	return sensors, rows.Err()
}

func (s *MySQLStore) UpsertDeviceSensors(ctx context.Context, sensors []NetworkDeviceSensor) error {
	if len(sensors) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO network_device_sensors (
			id, tenant_id, device_id, port_id, sensor_index, sensor_class, name, oid, unit,
			current_value, warn_limit, crit_limit, status, metadata_json
		) VALUES (?, ?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, NULLIF(?, 0), NULLIF(?, 0), ?, ?)
		ON DUPLICATE KEY UPDATE
			port_id = VALUES(port_id),
			name = VALUES(name),
			oid = VALUES(oid),
			unit = VALUES(unit),
			current_value = VALUES(current_value),
			warn_limit = VALUES(warn_limit),
			crit_limit = VALUES(crit_limit),
			status = VALUES(status),
			metadata_json = VALUES(metadata_json),
			updated_at = CURRENT_TIMESTAMP(3)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, sensor := range sensors {
		metadataJSON, err := encodeStringMapJSON(sensor.Metadata)
		if err != nil {
			return err
		}
		if _, err := stmt.ExecContext(ctx, sensor.ID, sensor.TenantID, sensor.DeviceID, sensor.PortID, sensor.SensorIndex, sensor.Class, sensor.Name, sensor.OID, sensor.Unit, sensor.Value, sensor.WarnLimit, sensor.CritLimit, sensor.Status, metadataJSON); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *MySQLStore) ListDevicePhysicalEntities(ctx context.Context, tenantID, deviceID ID) ([]PhysicalEntity, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT entity_index, name, description, class, vendor_type, contained_in,
		       hardware_revision, serial_number, manufacturer_name, model_name, is_fru
		FROM device_physical_entities
		WHERE tenant_id = ? AND device_id = ?
		ORDER BY entity_index
	`, tenantID, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entities []PhysicalEntity
	for rows.Next() {
		var e PhysicalEntity
		if err := rows.Scan(&e.Index, &e.Name, &e.Description, &e.Class, &e.VendorType, &e.ContainedIn, &e.HardwareRevision, &e.SerialNumber, &e.ManufacturerName, &e.ModelName, &e.IsFRU); err != nil {
			return nil, err
		}
		entities = append(entities, e)
	}
	return entities, rows.Err()
}

func (s *MySQLStore) UpsertDevicePhysicalEntities(ctx context.Context, tenantID, deviceID ID, entities []PhysicalEntity) error {
	if len(entities) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM device_physical_entities WHERE tenant_id = ? AND device_id = ?`, tenantID, deviceID); err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO device_physical_entities (
			id, tenant_id, device_id, entity_index, name, description, class,
			vendor_type, contained_in, hardware_revision, serial_number,
			manufacturer_name, model_name, is_fru
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, e := range entities {
		id := stableID("ent", string(deviceID), fmt.Sprint(e.Index))
		if _, err := stmt.ExecContext(ctx, id, tenantID, deviceID, e.Index, e.Name, e.Description, e.Class, e.VendorType, e.ContainedIn, e.HardwareRevision, e.SerialNumber, e.ManufacturerName, e.ModelName, e.IsFRU); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *MySQLStore) ListDeviceVLANs(ctx context.Context, tenantID, deviceID ID) ([]DeviceVLAN, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT vlan_id, name, status FROM device_vlans WHERE tenant_id = ? AND device_id = ? ORDER BY vlan_id`, tenantID, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var vlans []DeviceVLAN
	for rows.Next() {
		var v DeviceVLAN
		if err := rows.Scan(&v.VLANID, &v.Name, &v.Status); err != nil {
			return nil, err
		}
		vlans = append(vlans, v)
	}
	return vlans, rows.Err()
}

func (s *MySQLStore) UpsertDeviceVLANs(ctx context.Context, tenantID, deviceID ID, vlans []DeviceVLAN) error {
	return upsertSimple(ctx, s.db, "device_vlans", tenantID, deviceID, len(vlans), func(stmt *sql.Stmt, i int) error {
		v := vlans[i]
		id := stableID("vlan", string(deviceID), fmt.Sprint(v.VLANID))
		_, err := stmt.ExecContext(ctx, id, tenantID, deviceID, v.VLANID, v.Name, v.Status)
		return err
	}, "id, tenant_id, device_id, vlan_id, name, status")
}

func (s *MySQLStore) ListDeviceLAGGroups(ctx context.Context, tenantID, deviceID ID) ([]DeviceLAGGroup, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT aggregate_index, mac_address, mode FROM device_lag_groups WHERE tenant_id = ? AND device_id = ? ORDER BY aggregate_index`, tenantID, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []DeviceLAGGroup
	for rows.Next() {
		var g DeviceLAGGroup
		if err := rows.Scan(&g.AggregateIndex, &g.MACAddress, &g.Mode); err != nil {
			return nil, err
		}
		groups = append(groups, g)
	}
	return groups, rows.Err()
}

func (s *MySQLStore) UpsertDeviceLAGGroups(ctx context.Context, tenantID, deviceID ID, groups []DeviceLAGGroup) error {
	return upsertSimple(ctx, s.db, "device_lag_groups", tenantID, deviceID, len(groups), func(stmt *sql.Stmt, i int) error {
		g := groups[i]
		id := stableID("lag", string(deviceID), fmt.Sprint(g.AggregateIndex))
		_, err := stmt.ExecContext(ctx, id, tenantID, deviceID, g.AggregateIndex, g.MACAddress, g.Mode)
		return err
	}, "id, tenant_id, device_id, aggregate_index, mac_address, mode")
}

// upsertSimple is a helper for the delete-and-reinsert pattern used by VLAN/LAG
// discovery storage.
func upsertSimple(ctx context.Context, db *sql.DB, table string, tenantID, deviceID ID, count int, exec func(*sql.Stmt, int) error, columns string) error {
	if count == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE tenant_id = ? AND device_id = ?", table), tenantID, deviceID); err != nil {
		return err
	}
	placeholders := strings.Repeat("?, ", len(strings.Split(columns, ", ")))
	placeholders = strings.TrimSuffix(placeholders, ", ")
	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, columns, placeholders))
	if err != nil {
		return err
	}
	defer stmt.Close()
	for i := 0; i < count; i++ {
		if err := exec(stmt, i); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *MySQLStore) ListBGPSessions(ctx context.Context, tenantID, deviceID ID) ([]BGPSession, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, device_id, peer_addr, peer_as, local_as, afi, safi, state,
		       accepted_prefixes, denied_prefixes, advertised_prefixes, uptime_seconds, metadata_json, updated_at
		FROM bgp_sessions
		WHERE tenant_id = ? AND device_id = ?
		ORDER BY peer_as, peer_addr, afi, safi
	`, tenantID, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sessions []BGPSession
	for rows.Next() {
		session, err := scanBGPSession(rows)
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, session)
	}
	return sessions, rows.Err()
}

func (s *MySQLStore) GetBGPSession(ctx context.Context, tenantID, sessionID ID) (BGPSession, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, device_id, peer_addr, peer_as, local_as, afi, safi, state,
		       accepted_prefixes, denied_prefixes, advertised_prefixes, uptime_seconds, metadata_json, updated_at
		FROM bgp_sessions
		WHERE tenant_id = ? AND id = ?
	`, tenantID, sessionID)
	return scanBGPSession(row)
}

func (s *MySQLStore) UpsertBGPSessions(ctx context.Context, sessions []BGPSession) error {
	if len(sessions) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO bgp_sessions (
			id, tenant_id, device_id, peer_addr, peer_as, local_as, afi, safi, state,
			accepted_prefixes, denied_prefixes, advertised_prefixes, uptime_seconds, metadata_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			state = VALUES(state),
			accepted_prefixes = VALUES(accepted_prefixes),
			denied_prefixes = VALUES(denied_prefixes),
			advertised_prefixes = VALUES(advertised_prefixes),
			uptime_seconds = VALUES(uptime_seconds),
			metadata_json = VALUES(metadata_json),
			updated_at = CURRENT_TIMESTAMP(3)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, session := range sessions {
		metadataJSON, err := encodeStringMapJSON(session.Metadata)
		if err != nil {
			return err
		}
		peerAddr := net.ParseIP(session.PeerAddr)
		if peerAddr == nil {
			return &net.ParseError{Type: "IP address", Text: session.PeerAddr}
		}
		if _, err := stmt.ExecContext(ctx, session.ID, session.TenantID, session.DeviceID, []byte(peerAddr), session.PeerAS, session.LocalAS, session.AFI, session.SAFI, session.State, session.AcceptedPrefixes, session.DeniedPrefixes, session.AdvertisedPrefixes, uint64(session.Uptime.Seconds()), metadataJSON); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *MySQLStore) GetPortPolicy(ctx context.Context, tenantID, portID ID) (PortPolicy, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, port_id, side_type, billing_base_bps, sample_step_seconds,
		       correction_direction, correction_min, correction_max, enabled
		FROM port_policies
		WHERE tenant_id = ? AND port_id = ?
	`, tenantID, portID)
	return scanPortPolicy(row)
}

func (s *MySQLStore) UpsertPortPolicy(ctx context.Context, policy PortPolicy) (PortPolicy, error) {
	policy = policy.Normalize()
	if policy.ID == "" || len(policy.ID) > 26 {
		policy.ID = stableID("policy", string(policy.TenantID), string(policy.PortID))
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO port_policies (
			id, tenant_id, port_id, side_type, billing_base_bps, sample_step_seconds,
			correction_direction, correction_min, correction_max, enabled
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			side_type = VALUES(side_type),
			billing_base_bps = VALUES(billing_base_bps),
			sample_step_seconds = VALUES(sample_step_seconds),
			correction_direction = VALUES(correction_direction),
			correction_min = VALUES(correction_min),
			correction_max = VALUES(correction_max),
			enabled = VALUES(enabled),
			updated_at = CURRENT_TIMESTAMP(3)
	`, policy.ID, policy.TenantID, policy.PortID, policy.SideType, policy.BillingBaseBps, uint16(policy.SampleStep.Seconds()), policy.CorrectionDirection, policy.CorrectionMin, policy.CorrectionMax, policy.Enabled)
	if err != nil {
		return PortPolicy{}, err
	}
	return s.GetPortPolicy(ctx, policy.TenantID, policy.PortID)
}

func (s *MySQLStore) GetTrafficPolicyDefaults(ctx context.Context, tenantID ID) (TrafficPolicyDefaults, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, COALESCE(tenant_id, ''), side_type, billing_base_bps, sample_step_seconds,
		       correction_direction, correction_min, correction_max
		FROM traffic_policy_defaults
		WHERE tenant_id = ? OR tenant_id IS NULL
		ORDER BY tenant_id IS NULL
	`, tenantID)
	if err != nil {
		return TrafficPolicyDefaults{}, err
	}
	defer rows.Close()
	defaults := BuiltinTrafficPolicyDefaults
	for rows.Next() {
		policyDefault, err := scanTrafficPolicyDefault(rows)
		if err != nil {
			return TrafficPolicyDefaults{}, err
		}
		switch policyDefault.SideType {
		case PortSideProvider:
			defaults.Provider = policyDefault.Normalize(PortSideProvider)
		case PortSideCustomer:
			defaults.Customer = policyDefault.Normalize(PortSideCustomer)
		}
	}
	return defaults, rows.Err()
}

func (s *MySQLStore) UpsertTrafficPolicyDefault(ctx context.Context, policyDefault TrafficPolicyDefault) (TrafficPolicyDefault, error) {
	policyDefault = policyDefault.Normalize(policyDefault.SideType)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO traffic_policy_defaults (
			id, tenant_id, side_type, billing_base_bps, sample_step_seconds,
			correction_direction, correction_min, correction_max
		) VALUES (?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			billing_base_bps = VALUES(billing_base_bps),
			sample_step_seconds = VALUES(sample_step_seconds),
			correction_direction = VALUES(correction_direction),
			correction_min = VALUES(correction_min),
			correction_max = VALUES(correction_max),
			updated_at = CURRENT_TIMESTAMP(3)
	`, policyDefault.ID, policyDefault.TenantID, policyDefault.SideType, policyDefault.BillingBaseBps, uint16(policyDefault.SampleStep.Seconds()), policyDefault.CorrectionDirection, policyDefault.CorrectionMin, policyDefault.CorrectionMax)
	return policyDefault, err
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanNetworkDevice(row rowScanner) (NetworkDevice, error) {
	var device NetworkDevice
	var securityJSON []byte
	var uptimeSeconds uint64
	err := row.Scan(&device.ID, &device.TenantID, &device.TargetID, &device.Vendor, &device.Model, &device.Platform, &device.OSName, &device.OSVersion, &device.SysObjectID, &device.SysName, &device.SysDescr, &device.SysLocation, &uptimeSeconds, &device.SNMPProfileID, &device.SNMPPort, &securityJSON)
	if err != nil {
		return device, err
	}
	security, err := decodeStringMapJSON(securityJSON)
	if err != nil {
		return device, err
	}
	device.Uptime = time.Duration(uptimeSeconds) * time.Second
	device.SNMPPort = normalizeSNMPPort(device.SNMPPort)
	device.SNMPSecurity = security
	return device, nil
}

func scanNetworkPort(row rowScanner) (NetworkPort, error) {
	var port NetworkPort
	var metadataJSON []byte
	if err := row.Scan(&port.ID, &port.TenantID, &port.DeviceID, &port.IfIndex, &port.IfName, &port.IfAlias, &port.IfDescr, &port.AdminStatus, &port.OperStatus, &port.SpeedBps, &metadataJSON); err != nil {
		return port, err
	}
	metadata, err := decodeStringMapJSON(metadataJSON)
	if err != nil {
		return port, err
	}
	port.Metadata = metadata
	// rows written before status normalization still hold raw integers
	port.AdminStatus = NormalizeIfStatus(port.AdminStatus)
	port.OperStatus = NormalizeIfStatus(port.OperStatus)
	return port, nil
}

func scanNetworkPortTransceiver(row rowScanner) (NetworkPortTransceiver, error) {
	var transceiver NetworkPortTransceiver
	var rawJSON []byte
	if err := row.Scan(&transceiver.ID, &transceiver.TenantID, &transceiver.PortID, &transceiver.ModuleType, &transceiver.Vendor, &transceiver.Model, &transceiver.Serial, &transceiver.WavelengthNM, &transceiver.DistanceM, &transceiver.Connector, &rawJSON, &transceiver.UpdatedAt); err != nil {
		return transceiver, err
	}
	raw, err := decodeAnyMapJSON(rawJSON)
	if err != nil {
		return transceiver, err
	}
	transceiver.Raw = raw
	return transceiver, nil
}

func scanNetworkDeviceSensor(row rowScanner) (NetworkDeviceSensor, error) {
	var sensor NetworkDeviceSensor
	var metadataJSON []byte
	if err := row.Scan(&sensor.ID, &sensor.TenantID, &sensor.DeviceID, &sensor.PortID, &sensor.SensorIndex, &sensor.Class, &sensor.Name, &sensor.OID, &sensor.Unit, &sensor.Value, &sensor.WarnLimit, &sensor.CritLimit, &sensor.Status, &metadataJSON, &sensor.UpdatedAt); err != nil {
		return sensor, err
	}
	metadata, err := decodeStringMapJSON(metadataJSON)
	if err != nil {
		return sensor, err
	}
	sensor.Metadata = metadata
	return sensor, nil
}

func scanBGPSession(row rowScanner) (BGPSession, error) {
	var session BGPSession
	var peerAddrBytes []byte
	var uptimeSeconds uint64
	var metadataJSON []byte
	if err := row.Scan(&session.ID, &session.TenantID, &session.DeviceID, &peerAddrBytes, &session.PeerAS, &session.LocalAS, &session.AFI, &session.SAFI, &session.State, &session.AcceptedPrefixes, &session.DeniedPrefixes, &session.AdvertisedPrefixes, &uptimeSeconds, &metadataJSON, &session.UpdatedAt); err != nil {
		return session, err
	}
	session.PeerAddr = net.IP(peerAddrBytes).String()
	session.Uptime = time.Duration(uptimeSeconds) * time.Second
	metadata, err := decodeStringMapJSON(metadataJSON)
	if err != nil {
		return session, err
	}
	session.Metadata = metadata
	return session, nil
}

func scanPortPolicy(row rowScanner) (PortPolicy, error) {
	var policy PortPolicy
	var sampleStepSeconds uint16
	if err := row.Scan(&policy.ID, &policy.TenantID, &policy.PortID, &policy.SideType, &policy.BillingBaseBps, &sampleStepSeconds, &policy.CorrectionDirection, &policy.CorrectionMin, &policy.CorrectionMax, &policy.Enabled); err != nil {
		return policy, err
	}
	policy.SampleStep = time.Duration(sampleStepSeconds) * time.Second
	return policy.Normalize(), nil
}

func scanTrafficPolicyDefault(row rowScanner) (TrafficPolicyDefault, error) {
	var policyDefault TrafficPolicyDefault
	var sampleStepSeconds uint16
	if err := row.Scan(&policyDefault.ID, &policyDefault.TenantID, &policyDefault.SideType, &policyDefault.BillingBaseBps, &sampleStepSeconds, &policyDefault.CorrectionDirection, &policyDefault.CorrectionMin, &policyDefault.CorrectionMax); err != nil {
		return policyDefault, err
	}
	policyDefault.SampleStep = time.Duration(sampleStepSeconds) * time.Second
	return policyDefault.Normalize(policyDefault.SideType), nil
}

func encodeStringMapJSON(values map[string]string) ([]byte, error) {
	if len(values) == 0 {
		return []byte("{}"), nil
	}
	return json.Marshal(values)
}

func decodeStringMapJSON(data []byte) (map[string]string, error) {
	if len(data) == 0 {
		return map[string]string{}, nil
	}
	var values map[string]string
	if err := json.Unmarshal(data, &values); err != nil {
		return nil, err
	}
	if values == nil {
		return map[string]string{}, nil
	}
	return values, nil
}

func encodeAnyMapJSON(values map[string]any) ([]byte, error) {
	if len(values) == 0 {
		return []byte("{}"), nil
	}
	return json.Marshal(values)
}

func decodeAnyMapJSON(data []byte) (map[string]any, error) {
	if len(data) == 0 {
		return map[string]any{}, nil
	}
	var values map[string]any
	if err := json.Unmarshal(data, &values); err != nil {
		return nil, err
	}
	if values == nil {
		return map[string]any{}, nil
	}
	return values, nil
}

var _ NetworkRepository = (*MySQLStore)(nil)

// Keep database/sql imported where row scanners are compile-checked by concrete methods.
var _ = sql.ErrNoRows

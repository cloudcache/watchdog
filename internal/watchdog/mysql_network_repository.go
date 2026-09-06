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

// networkDeviceSelect is aliased `d` so it composes with a JOIN to targets in
// the device-summary query; single-table callers reference the columns
// unqualified, which stays unambiguous.
const networkDeviceSelect = `
	SELECT d.id, d.tenant_id, d.target_id, d.vendor, d.model,
	       COALESCE(d.platform, ''), COALESCE(d.os_name, ''), COALESCE(d.os_version, ''),
	       d.sys_object_id, d.sys_name, d.sys_descr,
	       COALESCE(d.sys_location, ''), COALESCE(d.uptime_seconds, 0),
	       COALESCE(d.snmp_profile_id, ''), d.snmp_port, COALESCE(d.snmp_security_json, JSON_OBJECT()), d.updated_at
	FROM network_devices d`

func (s *MySQLStore) ListDevices(ctx context.Context, tenantID ID) ([]NetworkDevice, error) {
	rows, err := s.db.QueryContext(ctx, networkDeviceSelect+`
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

func (s *MySQLStore) ListDevicesPage(ctx context.Context, tenantID ID, all bool, allowedTargetIDs []ID, filter NetworkDevicePageFilter) ([]NetworkDevice, string, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	// A non-admin with no target grants sees nothing; skip the query.
	if !all && len(allowedTargetIDs) == 0 {
		return nil, "", nil
	}
	query := networkDeviceSelect + ` WHERE tenant_id = ?`
	args := []any{tenantID}
	if !all {
		query += ` AND target_id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(allowedTargetIDs)), ",") + `)`
		for _, id := range allowedTargetIDs {
			args = append(args, id)
		}
	}
	if filter.Cursor != "" {
		sysName, id, err := decodeStringCursor(filter.Cursor)
		if err != nil {
			return nil, "", err
		}
		query += ` AND (sys_name > ? OR (sys_name = ? AND id > ?))`
		args = append(args, sysName, sysName, id)
	}
	query += ` ORDER BY sys_name, id LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var devices []NetworkDevice
	for rows.Next() {
		device, err := scanNetworkDevice(rows)
		if err != nil {
			return nil, "", err
		}
		devices = append(devices, device)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	nextCursor := ""
	if len(devices) > limit {
		devices = devices[:limit]
		last := devices[limit-1]
		nextCursor = encodeStringCursor(last.SysName, last.ID)
	}
	return devices, nextCursor, nil
}

// DeviceSummaryQuery drives the server-driven Network Devices table: search
// over device+target text, a status filter, a sort column and offset paging.
type DeviceSummaryQuery struct {
	Search string
	Status string // "" (all) | up | down (not up) | pending
	Sort   string // "" (name) | name | host | status | vendor | os
	Desc   bool
	Limit  int
	Offset int
}

// DeviceStatusCounts are the full grant-scoped totals behind the table's
// badges. Down mirrors the UI's "not up" semantics (total - up).
type DeviceStatusCounts struct {
	Total   int `json:"total"`
	Up      int `json:"up"`
	Down    int `json:"down"`
	Pending int `json:"pending"`
}

// deviceSummarySortColumns whitelists sort keys to real columns so the sort
// input can never reach the query as raw SQL.
var deviceSummarySortColumns = map[string]string{
	"":       "t.name",
	"name":   "t.name",
	"host":   "t.host",
	"status": "t.status",
	"vendor": "d.vendor",
	"os":     "d.os_name",
}

func deviceSummaryScope(query string, args []any, all bool, allowedTargetIDs []ID) (string, []any) {
	if !all {
		query += ` AND d.target_id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(allowedTargetIDs)), ",") + `)`
		for _, id := range allowedTargetIDs {
			args = append(args, id)
		}
	}
	return query, args
}

// ListDeviceSummaryDevicesPage returns one offset page of devices for the
// summary table. Search/status/sort operate on device+target columns via the
// join; the caller enriches only this page. A non-admin with no grants and no
// tenant-wide grant gets nothing.
func (s *MySQLStore) ListDeviceSummaryDevicesPage(ctx context.Context, tenantID ID, all bool, allowedTargetIDs []ID, q DeviceSummaryQuery) ([]NetworkDevice, error) {
	limit := q.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	offset := q.Offset
	if offset < 0 {
		offset = 0
	}
	if !all && len(allowedTargetIDs) == 0 {
		return nil, nil
	}
	query := networkDeviceSelect + ` JOIN targets t ON t.id = d.target_id AND t.tenant_id = d.tenant_id WHERE d.tenant_id = ?`
	args := []any{tenantID}
	query, args = deviceSummaryScope(query, args, all, allowedTargetIDs)
	query, args = applyDeviceSummaryFilters(query, args, q)

	sortCol := deviceSummarySortColumns[q.Sort]
	if sortCol == "" {
		sortCol = "t.name"
	}
	dir := "ASC"
	if q.Desc {
		dir = "DESC"
	}
	query += fmt.Sprintf(" ORDER BY %s %s, d.id %s LIMIT ? OFFSET ?", sortCol, dir, dir)
	args = append(args, limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
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

// CountDeviceStatuses returns the grant-scoped status totals for the badges.
// It applies the search but not the status filter, so the badges show the whole
// searched set (matching the table's client-side behaviour it replaces).
func (s *MySQLStore) CountDeviceStatuses(ctx context.Context, tenantID ID, all bool, allowedTargetIDs []ID, search string) (DeviceStatusCounts, error) {
	var counts DeviceStatusCounts
	if !all && len(allowedTargetIDs) == 0 {
		return counts, nil
	}
	query := `SELECT t.status, COUNT(*) FROM network_devices d
		JOIN targets t ON t.id = d.target_id AND t.tenant_id = d.tenant_id
		WHERE d.tenant_id = ?`
	args := []any{tenantID}
	query, args = deviceSummaryScope(query, args, all, allowedTargetIDs)
	query, args = applyDeviceSummaryFilters(query, args, DeviceSummaryQuery{Search: search})
	query += ` GROUP BY t.status`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return counts, err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return counts, err
		}
		counts.Total += n
		switch status {
		case "up":
			counts.Up += n
		case "pending":
			counts.Pending += n
		}
	}
	counts.Down = counts.Total - counts.Up
	return counts, rows.Err()
}

// applyDeviceSummaryFilters appends the status and search predicates shared by
// the page and count queries.
func applyDeviceSummaryFilters(query string, args []any, q DeviceSummaryQuery) (string, []any) {
	switch q.Status {
	case "up":
		query += ` AND t.status = 'up'`
	case "pending":
		query += ` AND t.status = 'pending'`
	case "down":
		query += ` AND t.status <> 'up'`
	}
	if search := strings.TrimSpace(q.Search); search != "" {
		like := "%" + escapeSQLLike(search) + "%"
		query += ` AND (t.name LIKE ? OR t.host LIKE ? OR d.sys_name LIKE ? OR d.vendor LIKE ? OR COALESCE(d.sys_location, '') LIKE ? OR COALESCE(d.os_name, '') LIKE ?)`
		for i := 0; i < 6; i++ {
			args = append(args, like)
		}
	}
	return query, args
}

func (s *MySQLStore) GetDevice(ctx context.Context, tenantID, deviceID ID) (NetworkDevice, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, target_id, vendor, model,
		       COALESCE(platform, ''), COALESCE(os_name, ''), COALESCE(os_version, ''),
		       sys_object_id, sys_name, sys_descr,
		       COALESCE(sys_location, ''), COALESCE(uptime_seconds, 0),
		       COALESCE(snmp_profile_id, ''), snmp_port, COALESCE(snmp_security_json, JSON_OBJECT()), updated_at
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
		       COALESCE(snmp_profile_id, ''), snmp_port, COALESCE(snmp_security_json, JSON_OBJECT()), updated_at
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
		SELECT id, tenant_id, device_id, if_index, if_name, if_alias, if_descr, admin_status, oper_status, speed_bps, metadata_json, updated_at
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

func (s *MySQLStore) ListInterfaceAddresses(ctx context.Context, tenantID, deviceID ID) ([]NetworkInterfaceAddress, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, device_id, COALESCE(port_id, ''), if_index,
		       address, family, prefix_length, origin, context_name, updated_at
		FROM network_interface_addresses
		WHERE tenant_id = ? AND device_id = ?
		ORDER BY if_index, family, address
	`, tenantID, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var addresses []NetworkInterfaceAddress
	for rows.Next() {
		address, err := scanNetworkInterfaceAddress(rows)
		if err != nil {
			return nil, err
		}
		addresses = append(addresses, address)
	}
	return addresses, rows.Err()
}

func (s *MySQLStore) ReplaceInterfaceAddresses(ctx context.Context, tenantID, deviceID ID, addresses []NetworkInterfaceAddress) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM network_interface_addresses WHERE tenant_id = ? AND device_id = ?`, tenantID, deviceID); err != nil {
		return err
	}
	if len(addresses) == 0 {
		return tx.Commit()
	}
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO network_interface_addresses (
			id, tenant_id, device_id, port_id, if_index, address, family, prefix_length, origin, context_name
		) VALUES (?, ?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, address := range addresses {
		ip := net.ParseIP(address.Address)
		if ip == nil {
			return &net.ParseError{Type: "IP address", Text: address.Address}
		}
		if address.Family == "ipv4" {
			ip = ip.To4()
		} else {
			ip = ip.To16()
		}
		if _, err := stmt.ExecContext(ctx, address.ID, tenantID, deviceID, address.PortID, address.IfIndex, []byte(ip), address.Family, address.PrefixLength, address.Origin, address.ContextName); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *MySQLStore) GetPort(ctx context.Context, tenantID, portID ID) (NetworkPort, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, device_id, if_index, if_name, if_alias, if_descr, admin_status, oper_status, speed_bps, metadata_json, updated_at
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
		e, err := scanPhysicalEntity(rows)
		if err != nil {
			return nil, err
		}
		entities = append(entities, e)
	}
	return entities, rows.Err()
}

var physicalEntitySortColumns = map[string]string{
	"":             "entity_index",
	"entity_index": "entity_index",
	"name":         "name",
	"class":        "class",
	"manufacturer": "manufacturer_name",
	"model":        "model_name",
	"serial":       "serial_number",
	"contained_in": "contained_in",
}

func (s *MySQLStore) ListDevicePhysicalEntitiesPage(ctx context.Context, tenantID, deviceID ID, query PhysicalEntityQuery) ([]PhysicalEntity, int, error) {
	limit := query.Limit
	if limit <= 0 || limit > 500 {
		limit = 25
	}
	offset := query.Offset
	if offset < 0 {
		offset = 0
	}
	where := ` WHERE tenant_id = ? AND device_id = ?`
	args := []any{tenantID, deviceID}
	if class := strings.TrimSpace(query.Class); class != "" {
		where += ` AND class = ?`
		args = append(args, class)
	}
	if query.FRU != nil {
		where += ` AND is_fru = ?`
		args = append(args, *query.FRU)
	}
	if search := strings.TrimSpace(query.Search); search != "" {
		like := "%" + escapeSQLLike(search) + "%"
		where += ` AND (name LIKE ? OR description LIKE ? OR class LIKE ? OR vendor_type LIKE ? OR hardware_revision LIKE ? OR serial_number LIKE ? OR manufacturer_name LIKE ? OR model_name LIKE ?)`
		for range 8 {
			args = append(args, like)
		}
	}
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM device_physical_entities`+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	sortColumn := physicalEntitySortColumns[query.Sort]
	if sortColumn == "" {
		sortColumn = physicalEntitySortColumns[""]
	}
	direction := "ASC"
	if query.Desc {
		direction = "DESC"
	}
	statement := `
		SELECT entity_index, name, description, class, vendor_type, contained_in,
		       hardware_revision, serial_number, manufacturer_name, model_name, is_fru
		FROM device_physical_entities` + where + fmt.Sprintf(` ORDER BY %s %s, entity_index %s LIMIT ? OFFSET ?`, sortColumn, direction, direction)
	pageArgs := append(append([]any(nil), args...), limit, offset)
	rows, err := s.db.QueryContext(ctx, statement, pageArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	entities := make([]PhysicalEntity, 0, min(limit, total))
	for rows.Next() {
		entity, err := scanPhysicalEntity(rows)
		if err != nil {
			return nil, 0, err
		}
		entities = append(entities, entity)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return entities, total, nil
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

var deviceVLANSortColumns = map[string]string{
	"":        "vlan_id",
	"vlan_id": "vlan_id",
	"name":    "name",
	"status":  "status",
}

func (s *MySQLStore) ListDeviceVLANsPage(ctx context.Context, tenantID, deviceID ID, q DeviceVLANQuery) ([]DeviceVLAN, int, error) {
	limit, offset := boundedPage(q.Limit, q.Offset)
	where := ` WHERE tenant_id = ? AND device_id = ?`
	args := []any{tenantID, deviceID}
	if search := strings.TrimSpace(q.Search); search != "" {
		like := "%" + escapeSQLLike(search) + "%"
		where += ` AND (CAST(vlan_id AS CHAR) LIKE ? OR name LIKE ? OR status LIKE ?)`
		args = append(args, like, like, like)
	}
	if q.Status != "" {
		where += ` AND status = ?`
		args = append(args, q.Status)
	}
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM device_vlans`+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	dir := "ASC"
	if q.Desc {
		dir = "DESC"
	}
	sortColumn := deviceVLANSortColumns[q.Sort]
	if sortColumn == "" {
		sortColumn = "vlan_id"
	}
	query := `SELECT vlan_id, name, status FROM device_vlans` + where + fmt.Sprintf(" ORDER BY %s %s, id %s LIMIT ? OFFSET ?", sortColumn, dir, dir)
	args = append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	vlans := make([]DeviceVLAN, 0, min(limit, total))
	for rows.Next() {
		var vlan DeviceVLAN
		if err := rows.Scan(&vlan.VLANID, &vlan.Name, &vlan.Status); err != nil {
			return nil, 0, err
		}
		vlans = append(vlans, vlan)
	}
	return vlans, total, rows.Err()
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

var deviceLAGSortColumns = map[string]string{
	"":                "aggregate_index",
	"aggregate_index": "aggregate_index",
	"mac_address":     "mac_address",
	"mode":            "mode",
}

func (s *MySQLStore) ListDeviceLAGGroupsPage(ctx context.Context, tenantID, deviceID ID, q DeviceLAGQuery) ([]DeviceLAGGroup, int, error) {
	limit, offset := boundedPage(q.Limit, q.Offset)
	where := ` WHERE tenant_id = ? AND device_id = ?`
	args := []any{tenantID, deviceID}
	if search := strings.TrimSpace(q.Search); search != "" {
		like := "%" + escapeSQLLike(search) + "%"
		where += ` AND (CAST(aggregate_index AS CHAR) LIKE ? OR mac_address LIKE ? OR mode LIKE ?)`
		args = append(args, like, like, like)
	}
	if q.Mode != "" {
		where += ` AND mode = ?`
		args = append(args, q.Mode)
	}
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM device_lag_groups`+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	dir := "ASC"
	if q.Desc {
		dir = "DESC"
	}
	sortColumn := deviceLAGSortColumns[q.Sort]
	if sortColumn == "" {
		sortColumn = "aggregate_index"
	}
	query := `SELECT aggregate_index, mac_address, mode FROM device_lag_groups` + where + fmt.Sprintf(" ORDER BY %s %s, id %s LIMIT ? OFFSET ?", sortColumn, dir, dir)
	args = append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	groups := make([]DeviceLAGGroup, 0, min(limit, total))
	for rows.Next() {
		var group DeviceLAGGroup
		if err := rows.Scan(&group.AggregateIndex, &group.MACAddress, &group.Mode); err != nil {
			return nil, 0, err
		}
		groups = append(groups, group)
	}
	return groups, total, rows.Err()
}

func boundedPage(limit, offset int) (int, int) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
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

// bgpSessionSelect is aliased `b` so it composes with a JOIN to network_devices
// for search/sort/grant; single-table callers still read the columns fine.
const bgpSessionSelect = `
	SELECT b.id, b.tenant_id, b.device_id, b.peer_addr, b.peer_as, b.local_as, b.afi, b.safi, b.state,
	       b.accepted_prefixes, b.denied_prefixes, b.advertised_prefixes, b.uptime_seconds, b.metadata_json, b.updated_at
	FROM bgp_sessions b`

// BGPSessionQuery drives the server-driven Core (BGP) table: search over device
// name / peer address / peer AS, a state filter, a sort column and offset paging.
type BGPSessionQuery struct {
	Search string
	State  string // "" (all) or an exact state, e.g. "established"
	Sort   string // "" (device) | device | peer | peer_as | state
	Desc   bool
	Limit  int
	Offset int
}

// BGPSessionCounts are the grant-scoped totals behind the table's badges.
type BGPSessionCounts struct {
	Total       int `json:"total"`
	Established int `json:"established"`
}

var bgpSortColumns = map[string]string{
	"":        "d.sys_name",
	"device":  "d.sys_name",
	"peer":    "b.peer_addr",
	"peer_as": "b.peer_as",
	"state":   "b.state",
}

func bgpDeviceScope(query string, args []any, all bool, allowedTargetIDs []ID) (string, []any) {
	if !all {
		query += ` AND d.target_id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(allowedTargetIDs)), ",") + `)`
		for _, id := range allowedTargetIDs {
			args = append(args, id)
		}
	}
	return query, args
}

// applyBGPFilters appends the state and search predicates shared by the page and
// count queries. peer_addr is stored binary, so it is searched via INET6_NTOA.
func applyBGPFilters(query string, args []any, q BGPSessionQuery, withState bool) (string, []any) {
	if withState && q.State != "" {
		query += ` AND b.state = ?`
		args = append(args, q.State)
	}
	if search := strings.TrimSpace(q.Search); search != "" {
		like := "%" + escapeSQLLike(search) + "%"
		query += ` AND (d.sys_name LIKE ? OR INET6_NTOA(b.peer_addr) LIKE ? OR CAST(b.peer_as AS CHAR) LIKE ?)`
		args = append(args, like, like, like)
	}
	return query, args
}

// ListAllBGPSessionsPage returns one offset page of BGP sessions for the
// server-driven Core table, with search/state/sort applied via a join to the
// owning devices (grant pushed into SQL).
func (s *MySQLStore) ListAllBGPSessionsPage(ctx context.Context, tenantID ID, all bool, allowedTargetIDs []ID, q BGPSessionQuery) ([]BGPSession, error) {
	limit := q.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	offset := q.Offset
	if offset < 0 {
		offset = 0
	}
	if !all && len(allowedTargetIDs) == 0 {
		return nil, nil
	}
	query := bgpSessionSelect + ` JOIN network_devices d ON d.id = b.device_id AND d.tenant_id = b.tenant_id WHERE b.tenant_id = ?`
	args := []any{tenantID}
	query, args = bgpDeviceScope(query, args, all, allowedTargetIDs)
	query, args = applyBGPFilters(query, args, q, true)

	sortCol := bgpSortColumns[q.Sort]
	if sortCol == "" {
		sortCol = "d.sys_name"
	}
	dir := "ASC"
	if q.Desc {
		dir = "DESC"
	}
	query += fmt.Sprintf(" ORDER BY %s %s, b.id %s LIMIT ? OFFSET ?", sortCol, dir, dir)
	args = append(args, limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
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

// ListDeviceBGPSessionsPage returns one filtered offset page and its filtered
// total. The owning device permission is checked before this repository call.
func (s *MySQLStore) ListDeviceBGPSessionsPage(ctx context.Context, tenantID, deviceID ID, q BGPSessionQuery) ([]BGPSession, int, error) {
	limit := q.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	offset := q.Offset
	if offset < 0 {
		offset = 0
	}
	where := ` JOIN network_devices d ON d.id = b.device_id AND d.tenant_id = b.tenant_id
		WHERE b.tenant_id = ? AND b.device_id = ?`
	args := []any{tenantID, deviceID}
	filteredWhere, filteredArgs := applyBGPFilters(where, append([]any(nil), args...), q, true)

	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bgp_sessions b`+filteredWhere, filteredArgs...).Scan(&total); err != nil {
		return nil, 0, err
	}

	sortCol := bgpSortColumns[q.Sort]
	if sortCol == "" {
		sortCol = "b.peer_addr"
	}
	dir := "ASC"
	if q.Desc {
		dir = "DESC"
	}
	query := bgpSessionSelect + filteredWhere + fmt.Sprintf(" ORDER BY %s %s, b.id %s LIMIT ? OFFSET ?", sortCol, dir, dir)
	filteredArgs = append(filteredArgs, limit, offset)
	rows, err := s.db.QueryContext(ctx, query, filteredArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	sessions := make([]BGPSession, 0, min(limit, total))
	for rows.Next() {
		session, err := scanBGPSession(rows)
		if err != nil {
			return nil, 0, err
		}
		sessions = append(sessions, session)
	}
	return sessions, total, rows.Err()
}

func (s *MySQLStore) CountDeviceBGPSessions(ctx context.Context, tenantID, deviceID ID) (BGPSessionCounts, error) {
	var counts BGPSessionCounts
	rows, err := s.db.QueryContext(ctx, `
		SELECT state, COUNT(*)
		FROM bgp_sessions
		WHERE tenant_id = ? AND device_id = ?
		GROUP BY state
	`, tenantID, deviceID)
	if err != nil {
		return counts, err
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return counts, err
		}
		counts.Total += count
		if state == "established" {
			counts.Established += count
		}
	}
	return counts, rows.Err()
}

// CountBGPSessions returns the grant-scoped total and established counts for the
// badges. It applies the search but not the state filter.
func (s *MySQLStore) CountBGPSessions(ctx context.Context, tenantID ID, all bool, allowedTargetIDs []ID, search string) (BGPSessionCounts, error) {
	var counts BGPSessionCounts
	if !all && len(allowedTargetIDs) == 0 {
		return counts, nil
	}
	query := `SELECT b.state, COUNT(*) FROM bgp_sessions b
		JOIN network_devices d ON d.id = b.device_id AND d.tenant_id = b.tenant_id
		WHERE b.tenant_id = ?`
	args := []any{tenantID}
	query, args = bgpDeviceScope(query, args, all, allowedTargetIDs)
	query, args = applyBGPFilters(query, args, BGPSessionQuery{Search: search}, false)
	query += ` GROUP BY b.state`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return counts, err
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return counts, err
		}
		counts.Total += n
		if state == "established" {
			counts.Established += n
		}
	}
	return counts, rows.Err()
}

func (s *MySQLStore) ListAllBGPSessions(ctx context.Context, tenantID ID) ([]BGPSession, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, device_id, peer_addr, peer_as, local_as, afi, safi, state,
		       accepted_prefixes, denied_prefixes, advertised_prefixes, uptime_seconds, metadata_json, updated_at
		FROM bgp_sessions
		WHERE tenant_id = ?
		ORDER BY device_id, peer_as, peer_addr, afi, safi
	`, tenantID)
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

func (s *MySQLStore) ReplaceBGPSessions(ctx context.Context, tenantID, deviceID ID, sessions []BGPSession) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM bgp_sessions WHERE tenant_id = ? AND device_id = ?`, tenantID, deviceID); err != nil {
		return err
	}
	if len(sessions) == 0 {
		return tx.Commit()
	}
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO bgp_sessions (
			id, tenant_id, device_id, peer_addr, peer_as, local_as, afi, safi, state,
			accepted_prefixes, denied_prefixes, advertised_prefixes, uptime_seconds, metadata_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
		if peerAddr.To4() != nil {
			peerAddr = peerAddr.To4()
		} else {
			peerAddr = peerAddr.To16()
		}
		if _, err := stmt.ExecContext(ctx, session.ID, tenantID, deviceID, []byte(peerAddr), session.PeerAS, session.LocalAS, session.AFI, session.SAFI, session.State, session.AcceptedPrefixes, session.DeniedPrefixes, session.AdvertisedPrefixes, uint64(session.Uptime.Seconds()), metadataJSON); err != nil {
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
	err := row.Scan(&device.ID, &device.TenantID, &device.TargetID, &device.Vendor, &device.Model, &device.Platform, &device.OSName, &device.OSVersion, &device.SysObjectID, &device.SysName, &device.SysDescr, &device.SysLocation, &uptimeSeconds, &device.SNMPProfileID, &device.SNMPPort, &securityJSON, &device.UpdatedAt)
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
	if err := row.Scan(&port.ID, &port.TenantID, &port.DeviceID, &port.IfIndex, &port.IfName, &port.IfAlias, &port.IfDescr, &port.AdminStatus, &port.OperStatus, &port.SpeedBps, &metadataJSON, &port.UpdatedAt); err != nil {
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

func scanNetworkInterfaceAddress(row rowScanner) (NetworkInterfaceAddress, error) {
	var address NetworkInterfaceAddress
	var rawAddress []byte
	if err := row.Scan(&address.ID, &address.TenantID, &address.DeviceID, &address.PortID, &address.IfIndex, &rawAddress, &address.Family, &address.PrefixLength, &address.Origin, &address.ContextName, &address.UpdatedAt); err != nil {
		return address, err
	}
	address.Address = net.IP(rawAddress).String()
	return address, nil
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

func scanPhysicalEntity(row rowScanner) (PhysicalEntity, error) {
	var entity PhysicalEntity
	err := row.Scan(
		&entity.Index, &entity.Name, &entity.Description, &entity.Class, &entity.VendorType,
		&entity.ContainedIn, &entity.HardwareRevision, &entity.SerialNumber,
		&entity.ManufacturerName, &entity.ModelName, &entity.IsFRU,
	)
	return entity, err
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

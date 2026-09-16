package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/snmpdomain"
	"github.com/gin-gonic/gin"
)

// snmpDiscoveryRunner keeps the HTTP/database boundary independently testable
// while production reuses the existing generic MIB-driven discovery engine.
type snmpDiscoveryRunner = snmpdomain.DiscoveryRunner

type snmpDiscoveryImportResult struct {
	Ports, DeletedPorts, InterfaceAddresses, BGPSessions int
	Sensors, PhysicalEntities, VLANs, LAGs               int
}

func (s *Server) discoverDeviceSNMP(c *gin.Context) {
	device, ok := s.loadScopedNetworkDevice(c, c.Param("id"))
	if !ok {
		return
	}
	if !device.SNMPProfileID.Valid || device.SNMPProfileID.String == "" {
		fail(c, http.StatusConflict, "invalid_state", "device has no SNMP profile")
		return
	}
	actorID := currentPrincipal(c).UserID
	profile, err := s.readSNMPProfile(c, device.SNMPProfileID.String)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	security := map[string]string{}
	if err := json.Unmarshal(profile.Security, &security); err != nil {
		fail(c, http.StatusInternalServerError, "invalid_snmp_profile", "SNMP profile security is invalid")
		return
	}
	var overrides json.RawMessage
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COALESCE(snmp_security_json,JSON_OBJECT()) FROM devices WHERE id=?", device.ID).Scan(&overrides); err != nil {
		writeSQLError(c, err)
		return
	}
	var overrideValues map[string]string
	if err := json.Unmarshal(overrides, &overrideValues); err != nil {
		fail(c, http.StatusInternalServerError, "invalid_device_snmp", "device SNMP security override is invalid")
		return
	}
	for key, value := range overrideValues {
		if strings.TrimSpace(value) != "" {
			security[key] = value
		}
	}
	port := profile.Port
	if device.SNMPPort.Valid && device.SNMPPort.Int64 > 0 {
		port = uint16(device.SNMPPort.Int64)
	}
	request := snmpdomain.DiscoveryRequest{
		TargetID: device.ID,
		Target:   snmpdomain.QueryTarget{Host: device.Host, Port: port},
		Device: snmpdomain.Device{
			ID: device.ID, TargetID: device.ID, Vendor: device.Vendor,
			Model: device.Model, Platform: device.Platform, OSName: device.OS, OSVersion: device.OSVersion,
			SysObjectID: device.SysObjectID, SysName: device.SysName, SysDescr: device.SysDescr,
			SysLocation: device.SysLocation, Uptime: time.Duration(device.UptimeSeconds) * time.Second,
			SNMPProfileID: profile.ID, SNMPPort: port, SNMPSecurity: security,
		},
		Profile: snmpdomain.Profile{
			ID: profile.ID, Name: profile.Name, Version: snmpdomain.Version(profile.Version),
			Security: security, Timeout: time.Duration(profile.TimeoutMS) * time.Millisecond, Retries: profile.Retries,
		},
	}
	result, err := s.snmpDiscovery.Discover(c.Request.Context(), request)
	if err != nil {
		reason := truncateUTF8(err.Error(), 64)
		_, _ = s.db.ExecContext(c.Request.Context(), "UPDATE devices SET status='down',status_reason=?,last_polled_at=UTC_TIMESTAMP(3),updated_by=?,row_version=row_version+1 WHERE id=?", reason, actorID, device.ID)
		s.audit(c.Request.Context(), actorID, "snmp.discovery.failed", "device", device.ID)
		fail(c, http.StatusBadGateway, "snmp_discovery_failed", reason)
		return
	}
	imported, err := s.importSNMPDiscovery(c.Request.Context(), device.ID, actorID, result)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if err := s.syncDynamicGroupsForDevice(c.Request.Context(), device.ID); err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), actorID, "snmp.discovery.succeeded", "device", device.ID)
	c.JSON(http.StatusOK, gin.H{
		"device_id": device.ID, "ports": imported.Ports, "deleted": imported.DeletedPorts,
		"interface_addresses": imported.InterfaceAddresses, "bgp_sessions": imported.BGPSessions,
		"sensors": imported.Sensors, "entities": imported.PhysicalEntities, "vlans": imported.VLANs,
		"lags": imported.LAGs, "recipes": len(result.Recipes), "modules": result.CompletedModules,
	})
}

func (s *Server) importSNMPDiscovery(ctx context.Context, deviceID, actorID string, result snmpdomain.DiscoveryResult) (snmpDiscoveryImportResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return snmpDiscoveryImportResult{}, err
	}
	defer tx.Rollback()
	updates := result.DeviceUpdates
	_, err = tx.ExecContext(ctx, `UPDATE devices SET
		vendor=COALESCE(NULLIF(?,''),vendor),model=COALESCE(NULLIF(?,''),model),platform=COALESCE(NULLIF(?,''),platform),
		os=COALESCE(NULLIF(?,''),os),os_version=COALESCE(NULLIF(?,''),os_version),
		sys_name=COALESCE(NULLIF(?,''),sys_name),sys_descr=COALESCE(NULLIF(?,''),sys_descr),
		sys_location=COALESCE(NULLIF(?,''),sys_location),sys_object_id=COALESCE(NULLIF(?,''),sys_object_id),
		uptime_seconds=?,status='up',status_reason='',last_polled_at=UTC_TIMESTAMP(3),updated_by=NULLIF(?,''),row_version=row_version+1
		WHERE id=?`, updates.Vendor, updates.Model, updates.Platform, updates.OSName, updates.OSVersion,
		updates.SysName, updates.SysDescr, updates.SysLocation, updates.SysObjectID, uint64(updates.Uptime/time.Second), actorID, deviceID)
	if err != nil {
		return snmpDiscoveryImportResult{}, err
	}
	imported := snmpDiscoveryImportResult{}
	portIDsByDiscoveryID, portIDsByIfIndex, err := importDiscoveredPorts(ctx, tx, deviceID, result.Ports)
	if err != nil {
		return imported, err
	}
	imported.Ports = len(result.Ports)
	if completedModule(result.CompletedModules, "ports") {
		imported.DeletedPorts, err = pruneDiscoveredPorts(ctx, tx, deviceID, result.Ports)
		if err != nil {
			return imported, err
		}
		if err = replaceInterfaceAddresses(ctx, tx, deviceID, result.InterfaceAddresses, portIDsByDiscoveryID, portIDsByIfIndex); err != nil {
			return imported, err
		}
		imported.InterfaceAddresses = len(result.InterfaceAddresses)
	}
	if completedModule(result.CompletedModules, "bgp") {
		if err = replaceBGPSessions(ctx, tx, deviceID, result.BGPSessions); err != nil {
			return imported, err
		}
		imported.BGPSessions = len(result.BGPSessions)
	}
	if completedModule(result.CompletedModules, "sensors") {
		if err = replaceSensors(ctx, tx, deviceID, result.Sensors, portIDsByDiscoveryID); err != nil {
			return imported, err
		}
		imported.Sensors = len(result.Sensors)
	}
	if completedModule(result.CompletedModules, "entity-physical") {
		if err = replacePhysicalEntities(ctx, tx, deviceID, result.PhysicalEntities); err != nil {
			return imported, err
		}
		imported.PhysicalEntities = len(result.PhysicalEntities)
	}
	if completedModule(result.CompletedModules, "vlans") {
		if err = replaceVLANs(ctx, tx, deviceID, result.VLANs); err != nil {
			return imported, err
		}
		imported.VLANs = len(result.VLANs)
	}
	if completedModule(result.CompletedModules, "lags") {
		if err = replaceLAGs(ctx, tx, deviceID, result.LAGs); err != nil {
			return imported, err
		}
		imported.LAGs = len(result.LAGs)
	}
	if err = replaceSNMPCollectionRecipes(ctx, tx, deviceID, result.Recipes, result.CompletedModules); err != nil {
		return imported, err
	}
	if err := tx.Commit(); err != nil {
		return imported, err
	}
	return imported, nil
}

func replaceSNMPCollectionRecipes(ctx context.Context, tx *sql.Tx, deviceID string, recipes []snmpdomain.Recipe, completed []string) error {
	now := time.Now().UTC()
	keepByModule := map[string][]string{}
	for _, recipe := range recipes {
		id := string(recipe.ID)
		if id == "" || len(id) > 26 {
			id = newID()
		}
		interval := recipe.SampleIntervalSeconds
		if interval == 0 {
			interval = 60
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO snmp_collection_recipes
			(id,device_id,entity_kind,entity_id,module_name,metric,value_kind,oid,numeric_oid,oid_index,mib,context_name,
			 divisor,multiplier,sample_interval_seconds,labels_json,options_json,enabled,discovered_at,last_error)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, '')
			ON DUPLICATE KEY UPDATE value_kind=VALUES(value_kind),oid=VALUES(oid),numeric_oid=VALUES(numeric_oid),
			mib=VALUES(mib),divisor=VALUES(divisor),multiplier=VALUES(multiplier),sample_interval_seconds=VALUES(sample_interval_seconds),
			labels_json=VALUES(labels_json),options_json=VALUES(options_json),enabled=VALUES(enabled),discovered_at=VALUES(discovered_at),last_error=''`,
			id, deviceID, string(recipe.EntityType), string(recipe.EntityID), recipe.ModuleName, recipe.MetricName, string(recipe.ValueType),
			recipe.OID, recipe.NumericOID, recipe.OIDIndex, recipe.MIB, recipe.ContextName, nullableSNMPFloat(recipe.Divisor, recipe.HasDivisor),
			nullableSNMPFloat(recipe.Multiplier, recipe.HasMultiplier), interval, mustJSON(recipe.Labels), mustJSON(recipe.Options), recipe.Enabled, now)
		if err != nil {
			return err
		}
		keepByModule[recipe.ModuleName] = append(keepByModule[recipe.ModuleName], id)
	}
	for _, module := range completed {
		ids := keepByModule[module]
		query := "DELETE FROM snmp_collection_recipes WHERE device_id=? AND module_name=?"
		args := []any{deviceID, module}
		if len(ids) > 0 {
			query += " AND id NOT IN (" + placeholders(len(ids)) + ")"
			for _, id := range ids {
				args = append(args, id)
			}
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return err
		}
	}
	return nil
}

func nullableSNMPFloat(value float64, present bool) any {
	if !present {
		return nil
	}
	return value
}

func importDiscoveredPorts(ctx context.Context, tx *sql.Tx, deviceID string, ports []snmpdomain.Port) (map[string]string, map[uint64]string, error) {
	byDiscoveryID := make(map[string]string, len(ports))
	byIfIndex := make(map[uint64]string, len(ports))
	for _, port := range ports {
		id := string(port.ID)
		if id == "" || len(id) > 26 {
			id = newID()
		}
		metadata := port.Metadata
		if metadata == nil {
			metadata = map[string]string{}
		}
		ifType := metadata["if_type"]
		_, err := tx.ExecContext(ctx, `INSERT INTO ports
			(id,device_id,if_index,if_name,if_descr,if_alias,if_speed,if_type,if_oper_status,if_admin_status,metadata_json,discovered_at)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,UTC_TIMESTAMP(3))
			ON DUPLICATE KEY UPDATE if_name=VALUES(if_name),if_descr=VALUES(if_descr),if_alias=VALUES(if_alias),
			if_speed=VALUES(if_speed),if_type=VALUES(if_type),if_oper_status=VALUES(if_oper_status),
			if_admin_status=VALUES(if_admin_status),metadata_json=VALUES(metadata_json),discovered_at=VALUES(discovered_at),row_version=row_version+1`,
			id, deviceID, port.IfIndex, port.IfName, port.IfDescr, port.IfAlias, port.SpeedBps, ifType,
			snmpdomain.NormalizeIfStatus(port.OperStatus), snmpdomain.NormalizeIfStatus(port.AdminStatus), mustJSON(metadata))
		if err != nil {
			return nil, nil, err
		}
		var actualID string
		if err := tx.QueryRowContext(ctx, "SELECT id FROM ports WHERE device_id=? AND if_index=?", deviceID, port.IfIndex).Scan(&actualID); err != nil {
			return nil, nil, err
		}
		byDiscoveryID[string(port.ID)] = actualID
		byIfIndex[port.IfIndex] = actualID
	}
	return byDiscoveryID, byIfIndex, nil
}

func pruneDiscoveredPorts(ctx context.Context, tx *sql.Tx, deviceID string, current []snmpdomain.Port) (int, error) {
	indexes := make([]uint64, 0, len(current))
	for _, port := range current {
		indexes = append(indexes, port.IfIndex)
	}
	where := "device_id=?"
	args := []any{deviceID}
	if len(indexes) > 0 {
		where += " AND if_index NOT IN (" + placeholders(len(indexes)) + ")"
		for _, index := range indexes {
			args = append(args, index)
		}
	}
	// Billing references are evidence and must never disappear because an
	// interface was absent in one discovery. Keep them as notPresent.
	if _, err := tx.ExecContext(ctx, "UPDATE ports SET if_oper_status='notPresent',row_version=row_version+1 WHERE "+where+" AND EXISTS (SELECT 1 FROM billing_account_ports bap WHERE bap.port_id=ports.id)", args...); err != nil {
		return 0, err
	}
	deleted, err := tx.ExecContext(ctx, "DELETE FROM ports WHERE "+where+" AND NOT EXISTS (SELECT 1 FROM billing_account_ports bap WHERE bap.port_id=ports.id)", args...)
	if err != nil {
		return 0, err
	}
	count, _ := deleted.RowsAffected()
	return int(count), nil
}

func replaceInterfaceAddresses(ctx context.Context, tx *sql.Tx, deviceID string, addresses []snmpdomain.InterfaceAddress, byDiscoveryID map[string]string, byIfIndex map[uint64]string) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM interface_addresses WHERE device_id=?", deviceID); err != nil {
		return err
	}
	for _, address := range addresses {
		ip := net.ParseIP(strings.TrimSpace(address.Address))
		if ip == nil {
			return fmt.Errorf("invalid discovered interface address %q", address.Address)
		}
		family := 6
		if ip.To4() != nil {
			family = 4
		}
		portID := byDiscoveryID[string(address.PortID)]
		if portID == "" {
			portID = byIfIndex[address.IfIndex]
		}
		id := string(address.ID)
		if id == "" || len(id) > 26 {
			id = newID()
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO interface_addresses
			(id,device_id,port_id,if_index,family,address,prefix_len,context,origin) VALUES (?,?,?,?,?,INET6_ATON(?),?,?,?)`,
			id, deviceID, nullableText(portID), address.IfIndex, family, ip.String(), address.PrefixLength, address.ContextName, address.Origin); err != nil {
			return err
		}
	}
	return nil
}

func replaceBGPSessions(ctx context.Context, tx *sql.Tx, deviceID string, sessions []snmpdomain.BGPSession) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM bgp_sessions WHERE device_id=?", deviceID); err != nil {
		return err
	}
	for _, session := range sessions {
		if net.ParseIP(strings.TrimSpace(session.PeerAddr)) == nil {
			return fmt.Errorf("invalid discovered BGP peer address %q", session.PeerAddr)
		}
		id := string(session.ID)
		if id == "" || len(id) > 26 {
			id = newID()
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO bgp_sessions
			(id,device_id,peer_address,peer_as,local_as,afi,safi,state,prefixes,denied_prefixes,advertised_prefixes,uptime_seconds,metadata_json)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`, id, deviceID, session.PeerAddr, session.PeerAS, session.LocalAS,
			session.AFI, session.SAFI, session.State, session.AcceptedPrefixes, session.DeniedPrefixes,
			session.AdvertisedPrefixes, uint64(session.Uptime/time.Second), mustJSON(session.Metadata)); err != nil {
			return err
		}
	}
	return nil
}

func replaceSensors(ctx context.Context, tx *sql.Tx, deviceID string, sensors []snmpdomain.Sensor, portIDs map[string]string) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM sensors WHERE device_id=?", deviceID); err != nil {
		return err
	}
	for _, sensor := range sensors {
		id := string(sensor.ID)
		if id == "" || len(id) > 26 {
			id = newID()
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO sensors
			(id,device_id,port_id,sensor_index,class,label,oid_index,oid,unit,value_num,warn_limit,crit_limit,status,metadata_json)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, id, deviceID, nullableText(portIDs[string(sensor.PortID)]), sensor.SensorIndex,
			sensor.Class, sensor.Name, fmt.Sprint(sensor.SensorIndex), sensor.OID, sensor.Unit, sensor.Value,
			sensor.WarnLimit, sensor.CritLimit, sensor.Status, mustJSON(sensor.Metadata)); err != nil {
			return err
		}
	}
	return nil
}

func replacePhysicalEntities(ctx context.Context, tx *sql.Tx, deviceID string, entities []snmpdomain.PhysicalEntity) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM physical_entities WHERE device_id=?", deviceID); err != nil {
		return err
	}
	for _, entity := range entities {
		if _, err := tx.ExecContext(ctx, `INSERT INTO physical_entities
			(id,device_id,entity_index,name,description,class,serial,vendor_type,contained_in,parent_rel_pos,hardware_revision,
			 firmware_revision,software_revision,manufacturer_name,model_name,alias,asset_id,is_fru)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, newID(), deviceID, entity.Index, entity.Name, entity.Description,
			entity.Class, entity.SerialNumber, entity.VendorType, entity.ContainedIn, entity.ParentRelPos, entity.HardwareRevision,
			entity.FirmwareRevision, entity.SoftwareRevision, entity.ManufacturerName, entity.ModelName, entity.Alias, entity.AssetID, entity.IsFRU); err != nil {
			return err
		}
	}
	return nil
}

func replaceVLANs(ctx context.Context, tx *sql.Tx, deviceID string, vlans []snmpdomain.VLAN) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM vlans WHERE device_id=?", deviceID); err != nil {
		return err
	}
	for _, vlan := range vlans {
		if _, err := tx.ExecContext(ctx, "INSERT INTO vlans (id,device_id,vlan_id,name,status) VALUES (?,?,?,?,?)", newID(), deviceID, vlan.VLANID, vlan.Name, vlan.Status); err != nil {
			return err
		}
	}
	return nil
}

func replaceLAGs(ctx context.Context, tx *sql.Tx, deviceID string, lags []snmpdomain.LAGGroup) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM lag_groups WHERE device_id=?", deviceID); err != nil {
		return err
	}
	for _, lag := range lags {
		if _, err := tx.ExecContext(ctx, "INSERT INTO lag_groups (id,device_id,lag_if_index,mac_address,mode) VALUES (?,?,?,?,?)", newID(), deviceID, lag.AggregateIndex, lag.MACAddress, lag.Mode); err != nil {
			return err
		}
	}
	return nil
}

func completedModule(modules []string, name string) bool { return slices.Contains(modules, name) }

func truncateUTF8(value string, max int) string {
	runes := []rune(value)
	if len(runes) <= max {
		return value
	}
	return string(runes[:max])
}

var _ snmpDiscoveryRunner = legacySNMPDiscoveryRunner{}

package server

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/cloudcache/watchdog/internal/watchdog"
	"github.com/gin-gonic/gin"
)

type snmpTrapRequest struct {
	SourceIP string                     `json:"source_ip"`
	Hostname string                     `json:"hostname"`
	TrapOID  string                     `json:"trap_oid"`
	Uptime   uint64                     `json:"uptime"`
	VarBinds []watchdog.SNMPTrapVarBind `json:"varbinds"`
	RawText  string                     `json:"raw_text"`
}

const snmpTrapAgentContextKey = "wd_snmp_trap_agent"

// authenticateSNMPTrapCaller preserves the historical trap URL for both the
// UDP forwarding process and administrator diagnostics. A machine token is
// resolved globally because the historical URL does not carry an agent ID;
// token and mTLS fingerprints are unique in the registry.
func (s *Server) authenticateSNMPTrapCaller(c *gin.Context) {
	token := strings.TrimSpace(c.GetHeader("X-Watchdog-Agent-Token"))
	if token == "" {
		if value := c.GetHeader("Authorization"); strings.HasPrefix(value, "Bearer ") {
			token = strings.TrimSpace(strings.TrimPrefix(value, "Bearer "))
		}
	}
	fingerprint := requestCertificateFingerprint(c)
	if token != "" || fingerprint != "" {
		var credentialID, agentID, kind, status string
		credentialQuery := `SELECT ac.id,a.id,a.kind,a.status
			FROM agent_credentials ac JOIN agents a ON a.id=ac.agent_id
			WHERE ac.token_sha256=? AND ac.status='active' AND ac.revoked_at IS NULL
			AND (ac.expires_at IS NULL OR ac.expires_at>NOW(3))`
		credentialValue := sha256hex(token)
		if token == "" {
			credentialQuery = `SELECT ac.id,a.id,a.kind,a.status
				FROM agent_credentials ac JOIN agents a ON a.id=ac.agent_id
				WHERE ac.mtls_fingerprint=? AND ac.status='active' AND ac.revoked_at IS NULL
				AND (ac.expires_at IS NULL OR ac.expires_at>NOW(3))`
			credentialValue = fingerprint
		}
		err := s.db.QueryRowContext(c.Request.Context(), credentialQuery, credentialValue).Scan(&credentialID, &agentID, &kind, &status)
		if err != nil || kind != "snmp" || status == "revoked" {
			fail(c, http.StatusUnauthorized, "unauthorized", "valid SNMP agent credential required")
			c.Abort()
			return
		}
		_, _ = s.db.ExecContext(c.Request.Context(), `UPDATE agent_credentials SET last_used_at=NOW(3) WHERE id=?`, credentialID)
		c.Set(snmpTrapAgentContextKey, agentID)
		c.Next()
		return
	}

	principal, err := s.authenticate(c)
	if err != nil {
		fail(c, http.StatusUnauthorized, "unauthorized", "authentication required")
		c.Abort()
		return
	}
	if !principal.can("device.update") {
		fail(c, http.StatusForbidden, "forbidden", "missing ability: device.update")
		c.Abort()
		return
	}
	cookie, _ := c.Cookie(csrfCookie)
	if header := c.GetHeader("X-CSRF-Token"); cookie == "" || header == "" || cookie != header {
		fail(c, http.StatusForbidden, "csrf", "invalid CSRF token")
		c.Abort()
		return
	}
	c.Set(principalKey, principal)
	c.Next()
}

func (s *Server) receiveSNMPTrap(c *gin.Context) {
	if s.snmpMetrics == nil {
		fail(c, http.StatusServiceUnavailable, "clickhouse_unavailable", "SNMP ClickHouse store is not configured")
		return
	}
	var request snmpTrapRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	request.SourceIP = normalizeTrapSource(request.SourceIP)
	request.Hostname = strings.TrimSpace(request.Hostname)
	request.TrapOID = strings.TrimPrefix(strings.TrimSpace(request.TrapOID), ".")
	if request.TrapOID == "" {
		fail(c, http.StatusBadRequest, "invalid_request", "trap_oid is required")
		return
	}
	device, err := s.findTrapDevice(c, request.SourceIP, request.Hostname)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !s.requireSNMPTrapDeviceAccess(c, device.ID) {
		return
	}
	ports, portRecipes, err := s.loadTrapPorts(c, device.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	sessions, bgpRecipes, err := s.loadTrapBGPSessions(c, device.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	trap := watchdog.SNMPTrap{
		SourceIP: request.SourceIP, Hostname: request.Hostname, TrapOID: request.TrapOID,
		Uptime: request.Uptime, VarBinds: request.VarBinds, RawText: request.RawText,
		ReceivedAt: time.Now().UTC(),
	}
	dispatcher := watchdog.NewSNMPTrapDispatcher(watchdog.DefaultSNMPTrapHandlers(ports, portRecipes, sessions, bgpRecipes))
	result, err := dispatcher.Dispatch(c.Request.Context(), trapNetworkDevice(device), trap)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := s.persistTrapResult(c, device.ID, result); err != nil {
		fail(c, http.StatusServiceUnavailable, "trap_persist_failed", err.Error())
		return
	}
	if principal := currentPrincipal(c); principal != nil {
		s.audit(c.Request.Context(), principal.UserID, "snmp.trap.received", "device", device.ID)
	}
	c.JSON(http.StatusOK, gin.H{
		"events": len(result.Events), "port_updates": len(result.PortUpdates),
		"bgp_updates": len(result.BGPUpdates), "immediate_poll": len(result.ImmediatePollRecipe),
		"rediscover": result.RediscoverDevice,
	})
}

func (s *Server) requireSNMPTrapDeviceAccess(c *gin.Context, deviceID string) bool {
	value, machine := c.Get(snmpTrapAgentContextKey)
	if !machine {
		return s.requireDeviceAccess(c, deviceID)
	}
	agentID, ok := value.(string)
	if !ok || agentID == "" {
		fail(c, http.StatusUnauthorized, "unauthorized", "invalid SNMP agent identity")
		return false
	}
	var boundDevice sql.NullString
	err := s.db.QueryRowContext(c.Request.Context(), `SELECT device_id FROM agent_bindings WHERE agent_id=?`, agentID).Scan(&boundDevice)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		writeSQLError(c, err)
		return false
	}
	if boundDevice.Valid && boundDevice.String != "" && boundDevice.String != deviceID {
		fail(c, http.StatusForbidden, "forbidden", "SNMP agent is not bound to this device")
		return false
	}
	return true
}

func normalizeTrapSource(value string) string {
	value = strings.TrimSpace(value)
	if host, _, err := net.SplitHostPort(value); err == nil {
		return strings.Trim(host, "[]")
	}
	return strings.Trim(value, "[]")
}

func (s *Server) findTrapDevice(c *gin.Context, sourceIP, hostname string) (deviceRecord, error) {
	if sourceIP == "" && hostname == "" {
		return deviceRecord{}, sql.ErrNoRows
	}
	var value deviceRecord
	err := s.db.QueryRowContext(c.Request.Context(), `SELECT d.id,d.host,d.display_name,COALESCE(d.labels_json,JSON_OBJECT()),
		d.kind,d.vendor,d.model,d.platform,d.sys_name,COALESCE(d.sys_descr,''),d.sys_location,d.sys_object_id,
		d.os,d.os_version,d.hardware,d.serial,d.location_id,l.name,d.snmp_profile_id,d.snmp_port,d.status,d.status_reason,
		d.disabled,d.ignore_alerts,d.uptime_seconds,d.last_polled_at,d.row_version,d.created_at,d.updated_at
		FROM devices d LEFT JOIN locations l ON l.id=d.location_id WHERE d.kind='network' AND
		((?<>'' AND (d.host=? OR d.sys_name=?)) OR (?<>'' AND (d.host=? OR d.sys_name=?)))
		ORDER BY CASE WHEN d.host=? THEN 0 WHEN d.sys_name=? THEN 1 ELSE 2 END LIMIT 1`,
		sourceIP, sourceIP, sourceIP, hostname, hostname, hostname, sourceIP, sourceIP).Scan(
		&value.ID, &value.Host, &value.DisplayName, &value.Labels, &value.Kind, &value.Vendor, &value.Model,
		&value.Platform, &value.SysName, &value.SysDescr, &value.SysLocation, &value.SysObjectID, &value.OS,
		&value.OSVersion, &value.Hardware, &value.Serial, &value.LocationID, &value.LocationName,
		&value.SNMPProfileID, &value.SNMPPort, &value.Status, &value.StatusReason, &value.Disabled,
		&value.IgnoreAlerts, &value.UptimeSeconds, &value.LastPolledAt, &value.RowVersion, &value.CreatedAt, &value.UpdatedAt)
	return value, err
}

func trapNetworkDevice(value deviceRecord) watchdog.NetworkDevice {
	return watchdog.NetworkDevice{
		ID: watchdog.ID(value.ID), TargetID: watchdog.ID(value.ID), Vendor: value.Vendor, Model: value.Model,
		Platform: value.Platform, OSName: value.OS, OSVersion: value.OSVersion, SysObjectID: value.SysObjectID,
		SysName: value.SysName, SysDescr: value.SysDescr, SysLocation: value.SysLocation,
		Uptime: time.Duration(value.UptimeSeconds) * time.Second,
	}
}

func (s *Server) loadTrapPorts(c *gin.Context, deviceID string) (map[uint64]watchdog.NetworkPort, map[watchdog.ID][]watchdog.ID, error) {
	rows, err := s.db.QueryContext(c.Request.Context(), portSelect+" WHERE p.device_id=?", deviceID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	ports := make(map[uint64]watchdog.NetworkPort)
	for rows.Next() {
		row, err := scanPort(rows)
		if err != nil {
			return nil, nil, err
		}
		metadataAny := map[string]any{}
		_ = json.Unmarshal(row.Metadata, &metadataAny)
		metadata := make(map[string]string, len(metadataAny))
		for key, value := range metadataAny {
			if text, ok := value.(string); ok {
				metadata[key] = text
			}
		}
		port := watchdog.NetworkPort{
			ID: watchdog.ID(row.ID), DeviceID: watchdog.ID(row.DeviceID), IfIndex: row.IfIndex,
			IfName: row.IfName, IfAlias: row.IfAlias, IfDescr: row.IfDescr,
			AdminStatus: row.AdminStatus, OperStatus: row.OperStatus, Metadata: metadata, UpdatedAt: row.UpdatedAt,
		}
		if row.IfSpeed.Valid && row.IfSpeed.Int64 > 0 {
			port.SpeedBps = uint64(row.IfSpeed.Int64)
		}
		ports[row.IfIndex] = port
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	recipes, err := s.loadTrapRecipeIDs(c, deviceID, "port")
	return ports, recipes, err
}

func (s *Server) loadTrapBGPSessions(c *gin.Context, deviceID string) (map[string]watchdog.BGPSession, map[watchdog.ID][]watchdog.ID, error) {
	rows, err := s.db.QueryContext(c.Request.Context(), bgpSelect+" WHERE b.device_id=?", deviceID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	result := make(map[string]watchdog.BGPSession)
	for rows.Next() {
		row, err := scanBGPSession(rows)
		if err != nil {
			return nil, nil, err
		}
		metadata := make(map[string]string, len(row.Metadata))
		for key, value := range row.Metadata {
			if text, ok := value.(string); ok {
				metadata[key] = text
			}
		}
		result[row.PeerAddr] = watchdog.BGPSession{
			ID: watchdog.ID(row.ID), DeviceID: watchdog.ID(row.DeviceID), PeerAddr: row.PeerAddr,
			PeerAS: row.PeerAS, LocalAS: row.LocalAS, AFI: row.AFI, SAFI: row.SAFI, State: row.State,
			AcceptedPrefixes: row.AcceptedPrefixes, DeniedPrefixes: row.DeniedPrefixes,
			AdvertisedPrefixes: row.AdvertisedPrefixes, Uptime: time.Duration(row.Uptime), Metadata: metadata, UpdatedAt: row.Updated,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	recipes, err := s.loadTrapRecipeIDs(c, deviceID, "bgp_peer")
	return result, recipes, err
}

func (s *Server) loadTrapRecipeIDs(c *gin.Context, deviceID, entityKind string) (map[watchdog.ID][]watchdog.ID, error) {
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT id,entity_id FROM snmp_collection_recipes
		WHERE device_id=? AND entity_kind=? AND enabled=1`, deviceID, entityKind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[watchdog.ID][]watchdog.ID)
	for rows.Next() {
		var recipeID, entityID watchdog.ID
		if err := rows.Scan(&recipeID, &entityID); err != nil {
			return nil, err
		}
		result[entityID] = append(result[entityID], recipeID)
	}
	return result, rows.Err()
}

func (s *Server) persistTrapResult(c *gin.Context, deviceID string, result watchdog.SNMPTrapHandleResult) error {
	events := make([]snmpch.Event, 0, len(result.Events))
	for _, event := range result.Events {
		events = append(events, snmpch.Event{
			ID: string(event.ID), DeviceID: string(event.DeviceID), EntityType: string(event.EntityType), EntityID: string(event.EntityID),
			Source: event.Source, Severity: event.Severity, EventType: event.EventType, Message: event.Message,
			Raw: event.Raw, OccurredAt: event.OccurredAt,
		})
	}
	if err := s.snmpMetrics.WriteEvents(c.Request.Context(), events); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, port := range result.PortUpdates {
		if _, err := tx.ExecContext(c.Request.Context(), `UPDATE ports SET if_oper_status=?,updated_at=UTC_TIMESTAMP(3),row_version=row_version+1 WHERE id=? AND device_id=?`,
			watchdog.NormalizeIfStatus(port.OperStatus), port.ID, deviceID); err != nil {
			return err
		}
	}
	for _, session := range result.BGPUpdates {
		if _, err := tx.ExecContext(c.Request.Context(), `UPDATE bgp_sessions SET state=?,updated_at=UTC_TIMESTAMP(3) WHERE id=? AND device_id=?`, session.State, session.ID, deviceID); err != nil {
			return err
		}
	}
	if len(result.ImmediatePollRecipe) > 0 {
		args := make([]any, 0, len(result.ImmediatePollRecipe)+1)
		args = append(args, deviceID)
		for _, id := range result.ImmediatePollRecipe {
			args = append(args, id)
		}
		if _, err := tx.ExecContext(c.Request.Context(), `UPDATE snmp_collection_recipes SET last_polled_at=NULL,last_error=''
			WHERE device_id=? AND id IN (`+placeholders(len(result.ImmediatePollRecipe))+`)`, args...); err != nil {
			return err
		}
	}
	if result.RediscoverDevice {
		if _, err := tx.ExecContext(c.Request.Context(), `UPDATE devices SET status='pending',status_reason='trap_rediscover',row_version=row_version+1 WHERE id=?`, deviceID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

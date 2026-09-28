package server

import (
	"database/sql"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/cloudcache/watchdog/internal/snmpdomain"
	"github.com/gin-gonic/gin"
)

type snmpTrapRequest struct {
	SourceIP string                   `json:"source_ip"`
	Hostname string                   `json:"hostname"`
	TrapOID  string                   `json:"trap_oid"`
	Uptime   uint64                   `json:"uptime"`
	VarBinds []snmpdomain.TrapVarBind `json:"varbinds"`
	RawText  string                   `json:"raw_text"`
}

const snmpTrapAgentContextKey = "wd_snmp_trap_agent"

// authenticateSNMPTrapCaller preserves the historical trap URL for both the UDP
// forwarding process and administrator diagnostics. A collector authenticates
// with the installation-wide shared token; the URL does not carry an agent ID,
// and under one shared token the token cannot identify a specific agent, so the
// target device is resolved from the trap source IP instead.
func (s *Server) authenticateSNMPTrapCaller(c *gin.Context) {
	token := strings.TrimSpace(c.GetHeader("X-Watchdog-Agent-Token"))
	if token == "" {
		if value := c.GetHeader("Authorization"); strings.HasPrefix(value, "Bearer ") {
			token = strings.TrimSpace(strings.TrimPrefix(value, "Bearer "))
		}
	}
	if token != "" {
		if !agentSharedTokenMatch(s.cfg.Agents.SharedToken, token) {
			fail(c, http.StatusUnauthorized, "unauthorized", "valid SNMP agent token required")
			c.Abort()
			return
		}
		c.Set(snmpTrapAgentContextKey, true)
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
	trap := snmpdomain.Trap{
		SourceIP: request.SourceIP, Hostname: request.Hostname, TrapOID: request.TrapOID,
		Uptime: request.Uptime, VarBinds: request.VarBinds, RawText: request.RawText,
		ReceivedAt: time.Now().UTC(),
	}
	dispatcher := snmpdomain.NewTrapDispatcher(snmpdomain.DefaultTrapHandlers(ports, portRecipes, sessions, bgpRecipes))
	result, err := dispatcher.Dispatch(c.Request.Context(), snmpdomain.TrapDevice{ID: device.ID}, trap)
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
	if _, machine := c.Get(snmpTrapAgentContextKey); machine {
		// A shared-token collector submits traps for whichever device the source
		// IP resolves to; there is no per-agent device binding left to enforce.
		return true
	}
	return s.requireDeviceAccess(c, deviceID)
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

func (s *Server) loadTrapPorts(c *gin.Context, deviceID string) (map[uint64]snmpdomain.TrapPort, map[string][]string, error) {
	rows, err := s.db.QueryContext(c.Request.Context(), portSelect+" WHERE p.device_id=?", deviceID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	ports := make(map[uint64]snmpdomain.TrapPort)
	for rows.Next() {
		row, err := scanPort(rows)
		if err != nil {
			return nil, nil, err
		}
		port := snmpdomain.TrapPort{ID: row.ID, IfIndex: row.IfIndex, IfName: row.IfName, OperStatus: row.OperStatus}
		ports[row.IfIndex] = port
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	recipes, err := s.loadTrapRecipeIDs(c, deviceID, "port")
	return ports, recipes, err
}

func (s *Server) loadTrapBGPSessions(c *gin.Context, deviceID string) (map[string]snmpdomain.TrapBGPSession, map[string][]string, error) {
	rows, err := s.db.QueryContext(c.Request.Context(), bgpSelect+" WHERE b.device_id=?", deviceID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	result := make(map[string]snmpdomain.TrapBGPSession)
	for rows.Next() {
		row, err := scanBGPSession(rows)
		if err != nil {
			return nil, nil, err
		}
		result[row.PeerAddr] = snmpdomain.TrapBGPSession{ID: row.ID, PeerAddr: row.PeerAddr, State: row.State}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	recipes, err := s.loadTrapRecipeIDs(c, deviceID, "bgp_peer")
	return result, recipes, err
}

func (s *Server) loadTrapRecipeIDs(c *gin.Context, deviceID, entityKind string) (map[string][]string, error) {
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT id,entity_id FROM snmp_collection_recipes
		WHERE device_id=? AND entity_kind=? AND enabled=1`, deviceID, entityKind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string][]string)
	for rows.Next() {
		var recipeID, entityID string
		if err := rows.Scan(&recipeID, &entityID); err != nil {
			return nil, err
		}
		result[entityID] = append(result[entityID], recipeID)
	}
	return result, rows.Err()
}

func (s *Server) persistTrapResult(c *gin.Context, deviceID string, result snmpdomain.TrapHandleResult) error {
	events := make([]snmpch.Event, 0, len(result.Events))
	for _, event := range result.Events {
		events = append(events, snmpch.Event{
			ID: event.ID, DeviceID: event.DeviceID, EntityType: string(event.EntityType), EntityID: event.EntityID,
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
			snmpdomain.NormalizeIfStatus(port.OperStatus), port.ID, deviceID); err != nil {
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

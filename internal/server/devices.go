package server

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type deviceRecord struct {
	ID, Host, DisplayName, Kind, Vendor, Model, Platform                         string
	SysName, SysDescr, SysLocation, SysObjectID, OS, OSVersion, Hardware, Serial string
	Labels                                                                       json.RawMessage
	LocationID, LocationName, SNMPProfileID                                      sql.NullString
	SNMPPort                                                                     sql.NullInt64
	Status, StatusReason                                                         string
	Disabled, IgnoreAlerts                                                       bool
	UptimeSeconds, RowVersion                                                    uint64
	LastPolledAt                                                                 sql.NullTime
	CreatedAt, UpdatedAt                                                         time.Time
	PortCount, UpPorts, DownPorts, BGPSessions, EstablishedBGP                   uint64
}

type deviceDTO struct {
	ID            string            `json:"id"`
	TargetID      string            `json:"target_id,omitempty"`
	Host          string            `json:"host"`
	DisplayName   string            `json:"display_name"`
	Name          string            `json:"name"`
	Kind          string            `json:"kind"`
	Labels        map[string]string `json:"labels"`
	Vendor        string            `json:"vendor"`
	Model         string            `json:"model"`
	Platform      string            `json:"platform"`
	SysName       string            `json:"sys_name"`
	SysDescr      string            `json:"sys_descr"`
	SysObjectID   string            `json:"sys_object_id"`
	OS            string            `json:"os"`
	OSName        string            `json:"os_name"`
	OSVersion     string            `json:"os_version"`
	Hardware      string            `json:"hardware"`
	Serial        string            `json:"serial"`
	LocationID    string            `json:"location_id,omitempty"`
	LocationName  string            `json:"location_name,omitempty"`
	SysLocation   string            `json:"sys_location,omitempty"`
	SNMPProfileID string            `json:"snmp_profile_id,omitempty"`
	SNMPPort      uint16            `json:"snmp_port,omitempty"`
	Status        string            `json:"status"`
	StatusReason  string            `json:"status_reason,omitempty"`
	Disabled      bool              `json:"disabled"`
	IgnoreAlerts  bool              `json:"ignore_alerts"`
	UptimeSeconds uint64            `json:"uptime_seconds"`
	Uptime        uint64            `json:"uptime"`
	LastPolledAt  any               `json:"last_polled_at,omitempty"`
	RowVersion    uint64            `json:"row_version"`
	CreatedAt     string            `json:"created_at"`
	UpdatedAt     string            `json:"updated_at"`
}

func (r deviceRecord) dto() deviceDTO {
	name := r.DisplayName
	if name == "" {
		name = r.SysName
	}
	if name == "" {
		name = r.Host
	}
	port := uint16(0)
	if r.SNMPPort.Valid && r.SNMPPort.Int64 > 0 {
		port = uint16(r.SNMPPort.Int64)
	}
	labels := map[string]string{}
	_ = json.Unmarshal(r.Labels, &labels)
	return deviceDTO{
		ID: r.ID, TargetID: r.ID, Host: r.Host, DisplayName: r.DisplayName, Name: name, Kind: r.Kind,
		Labels: labels,
		Vendor: r.Vendor, Model: r.Model, Platform: r.Platform, SysName: r.SysName, SysDescr: r.SysDescr,
		SysObjectID: r.SysObjectID, OS: r.OS, OSName: r.OS, OSVersion: r.OSVersion, Hardware: r.Hardware,
		Serial: r.Serial, LocationID: r.LocationID.String, LocationName: r.LocationName.String, SysLocation: r.SysLocation,
		SNMPProfileID: r.SNMPProfileID.String, SNMPPort: port, Status: r.Status, StatusReason: r.StatusReason,
		Disabled: r.Disabled, IgnoreAlerts: r.IgnoreAlerts, UptimeSeconds: r.UptimeSeconds,
		Uptime: r.UptimeSeconds * uint64(time.Second), LastPolledAt: nullableTime(r.LastPolledAt),
		RowVersion: r.RowVersion, CreatedAt: r.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt: r.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

type deviceMutation struct {
	ID                  string            `json:"id"`
	Host                *string           `json:"host"`
	DisplayName         *string           `json:"display_name"`
	Name                *string           `json:"name"`
	Kind                *string           `json:"kind"`
	Labels              map[string]string `json:"labels"`
	Vendor              *string           `json:"vendor"`
	Model               *string           `json:"model"`
	Platform            *string           `json:"platform"`
	OS                  *string           `json:"os"`
	LegacyOSName        *string           `json:"OSName"`
	OSVersion           *string           `json:"os_version"`
	LegacyOSVersion     *string           `json:"OSVersion"`
	SysName             *string           `json:"sys_name"`
	LegacySysName       *string           `json:"SysName"`
	SysDescr            *string           `json:"sys_descr"`
	LegacySysDescr      *string           `json:"SysDescr"`
	SysObjectID         *string           `json:"sys_object_id"`
	LegacySysObjectID   *string           `json:"SysObjectID"`
	Hardware            *string           `json:"hardware"`
	Serial              *string           `json:"serial"`
	LocationID          *string           `json:"location_id"`
	SNMPProfileID       *string           `json:"snmp_profile_id"`
	LegacySNMPProfileID *string           `json:"SNMPProfileID"`
	SNMPPort            *uint16           `json:"snmp_port"`
	SNMPSecurity        map[string]any    `json:"snmp_security"`
	LegacySNMPSecurity  map[string]any    `json:"SNMPSecurity"`
	Status              *string           `json:"status"`
	Disabled            *bool             `json:"disabled"`
	IgnoreAlerts        *bool             `json:"ignore_alerts"`
}

func (s *Server) listDevices(c *gin.Context) {
	if !validDeviceListFilters(c) {
		return
	}
	rows, total, counts, err := s.queryDevices(c, c.Query("kind"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	items := make([]deviceDTO, 0, len(rows))
	for _, row := range rows {
		items = append(items, row.dto())
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "counts": counts})
}

func (s *Server) listNetworkDevices(c *gin.Context) {
	if !validDeviceListFilters(c) {
		return
	}
	rows, total, counts, err := s.queryDevices(c, "network")
	if err != nil {
		writeSQLError(c, err)
		return
	}
	items := make([]deviceDTO, 0, len(rows))
	for _, row := range rows {
		items = append(items, row.dto())
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "counts": counts})
}

func (s *Server) listDeviceSummaries(c *gin.Context) {
	if !validDeviceListFilters(c) {
		return
	}
	rows, total, counts, err := s.queryDevices(c, "network")
	if err != nil {
		writeSQLError(c, err)
		return
	}
	items := make([]gin.H, 0, len(rows))
	for _, row := range rows {
		d := row.dto()
		target := gin.H{"id": d.ID, "name": d.Name, "host": d.Host, "kind": d.Kind, "status": d.Status, "labels": gin.H{}}
		items = append(items, gin.H{
			"device": d, "target": target, "port_count": row.PortCount, "up_ports": row.UpPorts,
			"down_ports": row.DownPorts, "bgp_sessions": row.BGPSessions, "established_bgp": row.EstablishedBGP,
			"last_seen": d.LastPolledAt,
		})
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "counts": counts})
}

func (s *Server) queryDevices(c *gin.Context, kind string) ([]deviceRecord, int, gin.H, error) {
	limit, offset := pageParams(c)
	where := []string{"1=1"}
	args := []any{}
	if p := currentPrincipal(c); p != nil && !p.can("device.viewAll") {
		where = append(where, `(EXISTS (SELECT 1 FROM user_device_permissions udp WHERE udp.user_id=? AND udp.device_id=d.id)
			OR EXISTS (SELECT 1 FROM user_device_group_permissions udgp JOIN device_group_members dgm ON dgm.device_group_id=udgp.device_group_id WHERE udgp.user_id=? AND dgm.device_id=d.id))`)
		args = append(args, p.UserID, p.UserID)
	}
	if kind != "" {
		where = append(where, "d.kind = ?")
		args = append(args, canonicalDeviceKind(kind))
	}
	if excluded := strings.TrimSpace(c.Query("exclude_kind")); excluded != "" {
		where = append(where, "d.kind <> ?")
		args = append(args, canonicalDeviceKind(excluded))
	}
	// Status badges are totals inside the caller's resource scope and device
	// kind, independent of the currently selected status/search filter.
	countClause := " WHERE " + strings.Join(where, " AND ")
	countArgs := append([]any{}, args...)
	if status := strings.TrimSpace(c.Query("status")); status != "" {
		where = append(where, "d.status = ?")
		args = append(args, status)
	}
	for _, filter := range []struct {
		param, column string
	}{
		{"vendor", "d.vendor"}, {"model", "d.model"}, {"platform", "d.platform"},
		{"os", "d.os"}, {"location_id", "d.location_id"},
	} {
		if value := strings.TrimSpace(c.Query(filter.param)); value != "" {
			where = append(where, filter.column+" = ?")
			args = append(args, value)
		}
	}
	if raw := strings.TrimSpace(c.Query("disabled")); raw != "" {
		disabled, _ := strconv.ParseBool(raw)
		where = append(where, "d.disabled = ?")
		args = append(args, disabled)
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		where = append(where, "(d.host LIKE ? OR d.display_name LIKE ? OR d.sys_name LIKE ? OR d.vendor LIKE ? OR d.model LIKE ? OR d.os LIKE ?)")
		like := "%" + escapeLike(q) + "%"
		for range 6 {
			args = append(args, like)
		}
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM devices d"+clause, args...).Scan(&total); err != nil {
		return nil, 0, nil, err
	}
	var all, up, down, pending int
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT COUNT(*), COALESCE(SUM(d.status='up'),0), COALESCE(SUM(d.status='down'),0), COALESCE(SUM(d.status='pending'),0) FROM devices d`+countClause, countArgs...).Scan(&all, &up, &down, &pending); err != nil {
		return nil, 0, nil, err
	}
	counts := gin.H{"total": all, "up": up, "down": down, "pending": pending}
	sorts := map[string]string{"": "d.updated_at", "id": "d.id", "name": "COALESCE(NULLIF(d.display_name,''),NULLIF(d.sys_name,''),d.host)", "host": "d.host", "vendor": "d.vendor", "model": "d.model", "platform": "d.platform", "os": "d.os", "status": "d.status", "disabled": "d.disabled", "location": "l.name", "uptime": "d.uptime_seconds", "updated_at": "d.updated_at", "last_polled_at": "d.last_polled_at"}
	sortColumn := sorts[c.Query("sort")]
	if sortColumn == "" {
		sortColumn = sorts[""]
	}
	query := `SELECT d.id,d.host,d.display_name,COALESCE(d.labels_json,JSON_OBJECT()),d.kind,d.vendor,d.model,d.platform,d.sys_name,COALESCE(d.sys_descr,''),d.sys_location,d.sys_object_id,d.os,d.os_version,d.hardware,d.serial,d.location_id,l.name,d.snmp_profile_id,d.snmp_port,d.status,d.status_reason,d.disabled,d.ignore_alerts,d.uptime_seconds,d.last_polled_at,d.row_version,d.created_at,d.updated_at,
		(SELECT COUNT(*) FROM ports p WHERE p.device_id=d.id),
		(SELECT COUNT(*) FROM ports p WHERE p.device_id=d.id AND p.if_oper_status='up'),
		(SELECT COUNT(*) FROM ports p WHERE p.device_id=d.id AND p.if_oper_status='down'),
		(SELECT COUNT(*) FROM bgp_sessions b WHERE b.device_id=d.id),
		(SELECT COUNT(*) FROM bgp_sessions b WHERE b.device_id=d.id AND LOWER(b.state)='established')
		FROM devices d LEFT JOIN locations l ON l.id=d.location_id` + clause + fmt.Sprintf(" ORDER BY %s %s, d.id %s LIMIT ? OFFSET ?", sortColumn, sortDirection(c), sortDirection(c))
	queryArgs := append(append([]any{}, args...), limit, offset)
	rs, err := s.db.QueryContext(c.Request.Context(), query, queryArgs...)
	if err != nil {
		return nil, 0, nil, err
	}
	defer rs.Close()
	items := make([]deviceRecord, 0, limit)
	for rs.Next() {
		var r deviceRecord
		if err := rs.Scan(&r.ID, &r.Host, &r.DisplayName, &r.Labels, &r.Kind, &r.Vendor, &r.Model, &r.Platform, &r.SysName, &r.SysDescr, &r.SysLocation, &r.SysObjectID, &r.OS, &r.OSVersion, &r.Hardware, &r.Serial, &r.LocationID, &r.LocationName, &r.SNMPProfileID, &r.SNMPPort, &r.Status, &r.StatusReason, &r.Disabled, &r.IgnoreAlerts, &r.UptimeSeconds, &r.LastPolledAt, &r.RowVersion, &r.CreatedAt, &r.UpdatedAt, &r.PortCount, &r.UpPorts, &r.DownPorts, &r.BGPSessions, &r.EstablishedBGP); err != nil {
			return nil, 0, nil, err
		}
		items = append(items, r)
	}
	return items, total, counts, rs.Err()
}

func (s *Server) getDevice(c *gin.Context) {
	if !s.requireDeviceAccess(c, c.Param("id")) {
		return
	}
	r, err := s.readDevice(c, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", etag(r.RowVersion))
	c.JSON(http.StatusOK, r.dto())
}

func (s *Server) readDevice(c *gin.Context, id string) (deviceRecord, error) {
	var r deviceRecord
	err := s.db.QueryRowContext(c.Request.Context(), `SELECT d.id,d.host,d.display_name,COALESCE(d.labels_json,JSON_OBJECT()),d.kind,d.vendor,d.model,d.platform,d.sys_name,COALESCE(d.sys_descr,''),d.sys_location,d.sys_object_id,d.os,d.os_version,d.hardware,d.serial,d.location_id,l.name,d.snmp_profile_id,d.snmp_port,d.status,d.status_reason,d.disabled,d.ignore_alerts,d.uptime_seconds,d.last_polled_at,d.row_version,d.created_at,d.updated_at
		FROM devices d LEFT JOIN locations l ON l.id=d.location_id WHERE d.id=?`, id).Scan(
		&r.ID, &r.Host, &r.DisplayName, &r.Labels, &r.Kind, &r.Vendor, &r.Model, &r.Platform, &r.SysName, &r.SysDescr, &r.SysLocation, &r.SysObjectID, &r.OS, &r.OSVersion, &r.Hardware, &r.Serial, &r.LocationID, &r.LocationName, &r.SNMPProfileID, &r.SNMPPort, &r.Status, &r.StatusReason, &r.Disabled, &r.IgnoreAlerts, &r.UptimeSeconds, &r.LastPolledAt, &r.RowVersion, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}

func (s *Server) createDevice(c *gin.Context) {
	var req deviceMutation
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	host := ""
	if req.Host != nil {
		host = normalizeDeviceHost(*req.Host)
	}
	if host == "" {
		fail(c, http.StatusBadRequest, "invalid_request", "host is required")
		return
	}
	id := strings.TrimSpace(req.ID)
	if id == "" {
		id = newID()
	}
	if len(id) > 26 {
		fail(c, http.StatusBadRequest, "invalid_request", "id must not exceed 26 characters")
		return
	}
	kind := canonicalDeviceKind(valueOr(req.Kind, "network"))
	status := valueOr(req.Status, "pending")
	if !validDeviceKind(kind) || !validDeviceStatus(status) {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid kind or status")
		return
	}
	display := valueOr(req.DisplayName, valueOr(req.Name, ""))
	_, err := s.db.ExecContext(c.Request.Context(), `INSERT INTO devices
		(id,host,display_name,labels_json,kind,vendor,model,platform,os,os_version,sys_name,sys_descr,sys_object_id,hardware,serial,location_id,snmp_profile_id,snmp_port,snmp_security_json,status,disabled,ignore_alerts,created_by,updated_by)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,NULLIF(?,''),?,?,?,?,?,?,?,?,?,?,?,?)`,
		id, host, display, mustJSON(req.Labels), kind, valueOr(req.Vendor, ""), valueOr(req.Model, ""), valueOr(req.Platform, ""), deviceOS(req), deviceOSVersion(req), deviceSysName(req), deviceSysDescr(req), deviceSysObjectID(req), valueOr(req.Hardware, ""), valueOr(req.Serial, ""), nullableString(req.LocationID), nullableString(firstPointer(req.SNMPProfileID, req.LegacySNMPProfileID)), nullableUint16(req.SNMPPort), nullableJSON(firstSecurity(req.SNMPSecurity, req.LegacySNMPSecurity)), status, valueOrBool(req.Disabled, false), valueOrBool(req.IgnoreAlerts, false), principalUserID(c), principalUserID(c))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if p := currentPrincipal(c); p != nil && !p.can("device.viewAll") {
		if _, err := s.db.ExecContext(c.Request.Context(), `INSERT INTO user_device_permissions (user_id,device_id) VALUES (?,?) ON DUPLICATE KEY UPDATE device_id=device_id`, p.UserID, id); err != nil {
			writeSQLError(c, err)
			return
		}
	}
	r, err := s.readDevice(c, id)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if err := s.syncDynamicGroupsForDevice(c.Request.Context(), id); err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", etag(r.RowVersion))
	c.Header("Location", "/api/v1/devices/"+id)
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "device.create", "device", id)
	if targetResponse(c) {
		c.Header("Location", "/api/v1/targets/"+id)
		c.JSON(http.StatusCreated, targetDTO(r.dto()))
		return
	}
	c.JSON(http.StatusCreated, r.dto())
}

func (s *Server) updateDevice(c *gin.Context) {
	if !s.requireDeviceAccess(c, c.Param("id")) {
		return
	}
	var req deviceMutation
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	current, err := s.readDevice(c, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	expected, supplied, err := ifMatch(c)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_if_match", "invalid If-Match")
		return
	}
	if supplied && expected != current.RowVersion {
		fail(c, http.StatusPreconditionFailed, "version_conflict", "device changed since it was loaded")
		return
	}
	host := current.Host
	if req.Host != nil {
		host = normalizeDeviceHost(*req.Host)
		if host == "" {
			fail(c, http.StatusBadRequest, "invalid_request", "host is required")
			return
		}
	}
	kind, status := current.Kind, current.Status
	if req.Kind != nil {
		kind = canonicalDeviceKind(*req.Kind)
	}
	if req.Status != nil {
		status = strings.TrimSpace(*req.Status)
	}
	if !validDeviceKind(kind) || !validDeviceStatus(status) {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid kind or status")
		return
	}
	display := current.DisplayName
	if req.DisplayName != nil {
		display = strings.TrimSpace(*req.DisplayName)
	} else if req.Name != nil {
		display = strings.TrimSpace(*req.Name)
	}
	result, err := s.db.ExecContext(c.Request.Context(), `UPDATE devices SET host=?,display_name=?,labels_json=?,kind=?,vendor=?,model=?,platform=?,os=?,os_version=?,sys_name=?,sys_descr=NULLIF(?,''),sys_object_id=?,hardware=?,serial=?,location_id=?,snmp_profile_id=?,snmp_port=?,status=?,disabled=?,ignore_alerts=?,updated_by=?,row_version=row_version+1 WHERE id=? AND row_version=?`,
		host, display, patchLabels(req.Labels, current.Labels), kind, patchString(req.Vendor, current.Vendor), patchString(req.Model, current.Model), patchString(req.Platform, current.Platform), patchOS(req, current.OS), patchOSVersion(req, current.OSVersion), patchSysName(req, current.SysName), patchSysDescr(req, current.SysDescr), patchSysObjectID(req, current.SysObjectID), patchString(req.Hardware, current.Hardware), patchString(req.Serial, current.Serial), patchNullString(req.LocationID, current.LocationID), patchNullString(firstPointer(req.SNMPProfileID, req.LegacySNMPProfileID), current.SNMPProfileID), patchNullInt(req.SNMPPort, current.SNMPPort), status, patchBool(req.Disabled, current.Disabled), patchBool(req.IgnoreAlerts, current.IgnoreAlerts), principalUserID(c), current.ID, current.RowVersion)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		writeSQLError(c, errVersionConflict)
		return
	}
	r, err := s.readDevice(c, current.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if err := s.syncDynamicGroupsForDevice(c.Request.Context(), current.ID); err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", etag(r.RowVersion))
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "device.update", "device", current.ID)
	if targetResponse(c) {
		c.JSON(http.StatusOK, targetDTO(r.dto()))
		return
	}
	c.JSON(http.StatusOK, r.dto())
}

func (s *Server) deleteDevice(c *gin.Context) {
	if !s.requireDeviceAccess(c, c.Param("id")) {
		return
	}
	current, err := s.readDevice(c, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if expected, supplied, err := ifMatch(c); err != nil || (supplied && expected != current.RowVersion) {
		fail(c, http.StatusPreconditionFailed, "version_conflict", "device changed since it was loaded")
		return
	}
	result, err := s.db.ExecContext(c.Request.Context(), `DELETE FROM devices WHERE id=?`, current.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if n, _ := result.RowsAffected(); n == 0 {
		writeSQLError(c, sql.ErrNoRows)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "device.delete", "device", current.ID)
	c.Status(http.StatusNoContent)
}

func (s *Server) deviceDeletePreview(c *gin.Context) {
	if !s.requireDeviceAccess(c, c.Param("id")) {
		return
	}
	device, err := s.readDevice(c, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	type impact struct {
		ResourceType string `json:"resource_type"`
		Behavior     string `json:"behavior"`
		Count        int    `json:"count"`
		Detail       string `json:"detail,omitempty"`
	}
	impacts := []impact{}
	for _, item := range []struct {
		resourceType, behavior, detail, query string
	}{
		{"flow_exporter_binding", "blocked", "remove unpublished bindings or disable and publish active bindings before deleting the device", `SELECT COUNT(*) FROM flow_exporter_bindings WHERE device_id=?`},
		{"port", "deleted", "", `SELECT COUNT(*) FROM ports WHERE device_id=?`},
		{"interface_address", "deleted", "", `SELECT COUNT(*) FROM interface_addresses WHERE device_id=?`},
		{"bgp_session", "deleted", "", `SELECT COUNT(*) FROM bgp_sessions WHERE device_id=?`},
		{"sensor", "deleted", "", `SELECT COUNT(*) FROM sensors WHERE device_id=?`},
		{"physical_entity", "deleted", "", `SELECT COUNT(*) FROM physical_entities WHERE device_id=?`},
		{"vlan", "deleted", "", `SELECT COUNT(*) FROM vlans WHERE device_id=?`},
		{"lag_group", "deleted", "", `SELECT COUNT(*) FROM lag_groups WHERE device_id=?`},
		{"agent_binding", "detached", "agents remain registered and lose this device binding", `SELECT COUNT(*) FROM agent_bindings WHERE device_id=?`},
	} {
		var count int
		if err := s.db.QueryRowContext(c.Request.Context(), item.query, device.ID).Scan(&count); err != nil {
			writeSQLError(c, err)
			return
		}
		if count > 0 {
			impacts = append(impacts, impact{item.resourceType, item.behavior, count, item.detail})
		}
	}
	c.JSON(http.StatusOK, gin.H{"device": device.dto(), "impacts": impacts})
}

func (s *Server) listTargets(c *gin.Context) {
	if !validDeviceListFilters(c) {
		return
	}
	rows, total, counts, err := s.queryDevices(c, c.Query("kind"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	items := make([]gin.H, 0, len(rows))
	for _, row := range rows {
		d := row.dto()
		items = append(items, targetDTO(d))
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "counts": counts})
}

func (s *Server) getTarget(c *gin.Context) {
	if !s.requireDeviceAccess(c, c.Param("id")) {
		return
	}
	r, err := s.readDevice(c, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	d := r.dto()
	c.Header("ETag", etag(r.RowVersion))
	c.JSON(http.StatusOK, targetDTO(d))
}

func (s *Server) createTarget(c *gin.Context) {
	c.Set("target_response", true)
	s.createDevice(c)
}

func (s *Server) updateTarget(c *gin.Context) {
	c.Set("target_response", true)
	s.updateDevice(c)
}

func targetResponse(c *gin.Context) bool {
	value, _ := c.Get("target_response")
	result, _ := value.(bool)
	return result
}

func (s *Server) patchDeviceSNMP(c *gin.Context) {
	if _, ok := s.loadScopedNetworkDevice(c, c.Param("id")); !ok {
		return
	}
	var req struct {
		ProfileID       *string        `json:"snmp_profile_id"`
		LegacyProfileID *string        `json:"SNMPProfileID"`
		Port            *uint16        `json:"snmp_port"`
		LegacyPort      *uint16        `json:"SNMPPort"`
		Security        map[string]any `json:"snmp_security"`
		LegacySecurity  map[string]any `json:"SNMPSecurity"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	profile := firstPointer(req.ProfileID, req.LegacyProfileID)
	port := req.Port
	if port == nil {
		port = req.LegacyPort
	}
	security := req.Security
	if security == nil {
		security = req.LegacySecurity
	}
	_, err := s.db.ExecContext(c.Request.Context(), `UPDATE devices SET snmp_profile_id=COALESCE(?,snmp_profile_id),snmp_port=COALESCE(?,snmp_port),snmp_security_json=COALESCE(?,snmp_security_json),row_version=row_version+1,updated_by=? WHERE id=?`, nullableString(profile), nullableUint16(port), nullableJSON(security), principalUserID(c), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.getDevice(c)
}

func normalizeDeviceHost(host string) string {
	host = strings.TrimSpace(host)
	if ip := net.ParseIP(strings.Trim(host, "[]")); ip != nil {
		return ip.String()
	}
	return strings.ToLower(strings.TrimSuffix(host, "."))
}

func validDeviceKind(v string) bool { return v == "network" || v == "host" }

func canonicalDeviceKind(v string) string {
	v = strings.TrimSpace(v)
	if v == "system" {
		return "host"
	}
	return v
}

func targetDTO(d deviceDTO) gin.H {
	kind := d.Kind
	if kind == "host" {
		kind = "system"
	}
	return gin.H{"id": d.ID, "name": d.Name, "host": d.Host, "kind": kind, "status": d.Status, "labels": d.Labels, "row_version": d.RowVersion, "updated_at": d.UpdatedAt}
}
func validDeviceStatus(v string) bool {
	return v == "pending" || v == "up" || v == "down" || v == "paused"
}

func validDeviceListFilters(c *gin.Context) bool {
	if status := strings.TrimSpace(c.Query("status")); status != "" && !validDeviceStatus(status) {
		fail(c, http.StatusBadRequest, "invalid_filter", "invalid status")
		return false
	}
	for _, field := range []string{"kind", "exclude_kind"} {
		if kind := strings.TrimSpace(c.Query(field)); kind != "" && !validDeviceKind(canonicalDeviceKind(kind)) {
			fail(c, http.StatusBadRequest, "invalid_filter", "invalid "+field)
			return false
		}
	}
	if raw := strings.TrimSpace(c.Query("disabled")); raw != "" {
		if _, err := strconv.ParseBool(raw); err != nil {
			fail(c, http.StatusBadRequest, "invalid_filter", "disabled must be true or false")
			return false
		}
	}
	return true
}

func (s *Server) requireDeviceAccess(c *gin.Context, deviceID string) bool {
	p := currentPrincipal(c)
	if p == nil {
		fail(c, http.StatusUnauthorized, "unauthorized", "authentication required")
		return false
	}
	allowed, err := s.principalCanAccessDevice(c.Request.Context(), p, deviceID)
	if err != nil {
		writeSQLError(c, err)
		return false
	}
	if !allowed {
		fail(c, http.StatusForbidden, "forbidden", "device is outside the caller's resource scope")
		return false
	}
	return true
}
func valueOr(v *string, fallback string) string {
	if v == nil {
		return fallback
	}
	return strings.TrimSpace(*v)
}
func valueOrBool(v *bool, fallback bool) bool {
	if v == nil {
		return fallback
	}
	return *v
}
func firstPointer(a, b *string) *string {
	if a != nil {
		return a
	}
	return b
}
func firstString(a, b *string) string {
	if a != nil {
		return *a
	}
	if b != nil {
		return *b
	}
	return ""
}
func deviceOS(r deviceMutation) string        { return firstString(r.OS, r.LegacyOSName) }
func deviceOSVersion(r deviceMutation) string { return firstString(r.OSVersion, r.LegacyOSVersion) }
func deviceSysName(r deviceMutation) string   { return firstString(r.SysName, r.LegacySysName) }
func deviceSysDescr(r deviceMutation) string  { return firstString(r.SysDescr, r.LegacySysDescr) }
func deviceSysObjectID(r deviceMutation) string {
	return firstString(r.SysObjectID, r.LegacySysObjectID)
}
func patchString(v *string, old string) string {
	if v == nil {
		return old
	}
	return strings.TrimSpace(*v)
}
func patchOS(r deviceMutation, old string) string {
	if r.OS != nil {
		return strings.TrimSpace(*r.OS)
	}
	return patchString(r.LegacyOSName, old)
}
func patchOSVersion(r deviceMutation, old string) string {
	if r.OSVersion != nil {
		return strings.TrimSpace(*r.OSVersion)
	}
	return patchString(r.LegacyOSVersion, old)
}
func patchSysName(r deviceMutation, old string) string {
	if r.SysName != nil {
		return strings.TrimSpace(*r.SysName)
	}
	return patchString(r.LegacySysName, old)
}
func patchSysDescr(r deviceMutation, old string) string {
	if r.SysDescr != nil {
		return strings.TrimSpace(*r.SysDescr)
	}
	return patchString(r.LegacySysDescr, old)
}
func patchSysObjectID(r deviceMutation, old string) string {
	if r.SysObjectID != nil {
		return strings.TrimSpace(*r.SysObjectID)
	}
	return patchString(r.LegacySysObjectID, old)
}
func patchBool(v *bool, old bool) bool {
	if v == nil {
		return old
	}
	return *v
}
func nullableString(v *string) any {
	if v == nil || strings.TrimSpace(*v) == "" {
		return nil
	}
	return strings.TrimSpace(*v)
}
func nullableUint16(v *uint16) any {
	if v == nil || *v == 0 {
		return nil
	}
	return *v
}
func patchNullString(v *string, old sql.NullString) any {
	if v == nil {
		if old.Valid {
			return old.String
		}
		return nil
	}
	return nullableString(v)
}
func patchNullInt(v *uint16, old sql.NullInt64) any {
	if v == nil {
		if old.Valid {
			return old.Int64
		}
		return nil
	}
	return nullableUint16(v)
}
func nullableJSON(v map[string]any) any {
	if v == nil {
		return nil
	}
	return mustJSON(v)
}
func firstSecurity(a, b map[string]any) map[string]any {
	if a != nil {
		return a
	}
	return b
}
func patchLabels(v map[string]string, old json.RawMessage) string {
	if v == nil {
		if len(old) == 0 {
			return `{}`
		}
		return string(old)
	}
	return mustJSON(v)
}
func escapeLike(v string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(v)
}

func parseUint16(value string) *uint16 {
	v, err := strconv.ParseUint(value, 10, 16)
	if err != nil || v == 0 {
		return nil
	}
	out := uint16(v)
	return &out
}

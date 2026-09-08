package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// Locations and device groups are inventory organization, not a generic
// resource registry. Dynamic groups use a deliberately small typed rule and
// materialize their members in device_group_members so every device/port/flow
// scope check has one fast, identical authorization path.

type locationRecord struct {
	ID, Name, Address    string
	Latitude, Longitude  sql.NullFloat64
	RowVersion           uint64
	CreatedAt, UpdatedAt time.Time
	DeviceCount          uint64
}

type locationDTO struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Latitude    *float64 `json:"latitude"`
	Longitude   *float64 `json:"longitude"`
	Address     string   `json:"address"`
	DeviceCount uint64   `json:"device_count"`
	RowVersion  uint64   `json:"row_version"`
	CreatedAt   string   `json:"created_at"`
	UpdatedAt   string   `json:"updated_at"`
}

func (r locationRecord) dto() locationDTO {
	return locationDTO{ID: r.ID, Name: r.Name, Latitude: nullFloat(r.Latitude), Longitude: nullFloat(r.Longitude),
		Address: r.Address, DeviceCount: r.DeviceCount, RowVersion: r.RowVersion,
		CreatedAt: r.CreatedAt.UTC().Format(time.RFC3339Nano), UpdatedAt: r.UpdatedAt.UTC().Format(time.RFC3339Nano)}
}

type locationMutation struct {
	Name      *string  `json:"name"`
	Latitude  *float64 `json:"latitude"`
	Longitude *float64 `json:"longitude"`
	Address   *string  `json:"address"`
}

const locationSelect = `SELECT l.id,l.name,l.latitude,l.longitude,l.address,l.row_version,l.created_at,l.updated_at,
	(SELECT COUNT(*) FROM devices d WHERE d.location_id=l.id) AS device_count FROM locations l`

func scanLocation(row rowScanner) (locationRecord, error) {
	var value locationRecord
	err := row.Scan(&value.ID, &value.Name, &value.Latitude, &value.Longitude, &value.Address,
		&value.RowVersion, &value.CreatedAt, &value.UpdatedAt, &value.DeviceCount)
	return value, err
}

func (s *Server) readLocation(ctx context.Context, id string) (locationRecord, error) {
	return scanLocation(s.db.QueryRowContext(ctx, locationSelect+" WHERE l.id=?", id))
}

func (s *Server) listLocations(c *gin.Context) {
	page, ok := parseInventoryPage(c, nil, map[string]string{"name": "l.name", "device_count": "device_count", "updated_at": "l.updated_at"}, "name")
	if !ok {
		return
	}
	where, args := []string{"1=1"}, []any{}
	if p := currentPrincipal(c); p != nil && !p.can("device.viewAll") {
		where = append(where, `EXISTS (SELECT 1 FROM devices d WHERE d.location_id=l.id AND (
			EXISTS (SELECT 1 FROM user_device_permissions udp WHERE udp.user_id=? AND udp.device_id=d.id)
			OR EXISTS (SELECT 1 FROM user_device_group_permissions ug JOIN device_group_members gm ON gm.device_group_id=ug.device_group_id WHERE ug.user_id=? AND gm.device_id=d.id)))`)
		args = append(args, p.UserID, p.UserID)
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + escapeLike(q) + "%"
		where = append(where, "(l.name LIKE ? OR l.address LIKE ?)")
		args = append(args, like, like)
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM locations l"+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	queryArgs := append(append([]any{}, args...), page.Limit, page.Offset)
	rows, err := s.db.QueryContext(c.Request.Context(), locationSelect+clause+fmt.Sprintf(" ORDER BY %s %s,l.id %s LIMIT ? OFFSET ?", page.Sort, page.Order, page.Order), queryArgs...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := []locationDTO{}
	for rows.Next() {
		value, err := scanLocation(rows)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, value.dto())
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total})
}

func (s *Server) getLocation(c *gin.Context) {
	value, err := s.readLocation(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !s.locationInScope(c, value.ID) {
		return
	}
	c.Header("ETag", etag(value.RowVersion))
	c.JSON(http.StatusOK, value.dto())
}

func (s *Server) createLocation(c *gin.Context) {
	var req locationMutation
	if !decodeStrictBody(c, &req) {
		return
	}
	name := valueOr(req.Name, "")
	if err := validateLocation(name, req.Latitude, req.Longitude); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	id := newID()
	_, err := s.db.ExecContext(c.Request.Context(), `INSERT INTO locations (id,name,latitude,longitude,address,created_by,updated_by) VALUES (?,?,?,?,?,?,?)`,
		id, name, nullableFloat(req.Latitude), nullableFloat(req.Longitude), valueOr(req.Address, ""), principalUserID(c), principalUserID(c))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	value, err := s.readLocation(c.Request.Context(), id)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "location.create", "location", id)
	c.Header("ETag", etag(value.RowVersion))
	c.Header("Location", "/api/v1/locations/"+id)
	c.JSON(http.StatusCreated, value.dto())
}

func (s *Server) updateLocation(c *gin.Context) {
	var req locationMutation
	if !decodeStrictBody(c, &req) {
		return
	}
	current, err := s.readLocation(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !checkVersion(c, current.RowVersion, "location") {
		return
	}
	name := patchString(req.Name, current.Name)
	lat, lon := patchNullFloat(req.Latitude, current.Latitude), patchNullFloat(req.Longitude, current.Longitude)
	if err := validateLocation(name, floatPointer(lat), floatPointer(lon)); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	result, err := s.db.ExecContext(c.Request.Context(), `UPDATE locations SET name=?,latitude=?,longitude=?,address=?,updated_by=?,row_version=row_version+1 WHERE id=? AND row_version=?`,
		name, lat, lon, patchString(req.Address, current.Address), principalUserID(c), current.ID, current.RowVersion)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		writeSQLError(c, errVersionConflict)
		return
	}
	value, err := s.readLocation(c.Request.Context(), current.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "location.update", "location", current.ID)
	c.Header("ETag", etag(value.RowVersion))
	c.JSON(http.StatusOK, value.dto())
}

func (s *Server) locationDeletePreview(c *gin.Context) {
	value, err := s.readLocation(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"location": value.dto(), "impacts": []gin.H{{"resource_type": "device", "behavior": "detached", "count": value.DeviceCount}}})
}

func (s *Server) deleteLocation(c *gin.Context) {
	current, err := s.readLocation(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !checkVersion(c, current.RowVersion, "location") {
		return
	}
	result, err := s.db.ExecContext(c.Request.Context(), "DELETE FROM locations WHERE id=?", current.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		writeSQLError(c, sql.ErrNoRows)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "location.delete", "location", current.ID)
	c.Status(http.StatusNoContent)
}

func (s *Server) locationInScope(c *gin.Context, id string) bool {
	p := currentPrincipal(c)
	if p == nil {
		fail(c, http.StatusUnauthorized, "unauthorized", "authentication required")
		return false
	}
	if p.can("device.viewAll") {
		return true
	}
	var allowed bool
	err := s.db.QueryRowContext(c.Request.Context(), `SELECT EXISTS(SELECT 1 FROM devices d WHERE d.location_id=? AND (
		EXISTS (SELECT 1 FROM user_device_permissions udp WHERE udp.user_id=? AND udp.device_id=d.id)
		OR EXISTS (SELECT 1 FROM user_device_group_permissions ug JOIN device_group_members gm ON gm.device_group_id=ug.device_group_id WHERE ug.user_id=? AND gm.device_id=d.id)))`, id, p.UserID, p.UserID).Scan(&allowed)
	if err != nil {
		writeSQLError(c, err)
		return false
	}
	if !allowed {
		fail(c, http.StatusForbidden, "forbidden", "location is outside the caller's resource scope")
		return false
	}
	return true
}

type deviceGroupRule struct {
	Kind       []string            `json:"kind,omitempty"`
	Status     []string            `json:"status,omitempty"`
	Vendor     []string            `json:"vendor,omitempty"`
	Model      []string            `json:"model,omitempty"`
	Platform   []string            `json:"platform,omitempty"`
	OS         []string            `json:"os,omitempty"`
	LocationID []string            `json:"location_id,omitempty"`
	Labels     map[string][]string `json:"labels,omitempty"`
}

type deviceGroupRecord struct {
	ID, Name, Kind, Description string
	Rule                        json.RawMessage
	RowVersion, MemberCount     uint64
	CreatedAt, UpdatedAt        time.Time
}

type deviceGroupDTO struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Kind        string          `json:"kind"`
	Rule        deviceGroupRule `json:"rule"`
	Description string          `json:"description"`
	MemberCount uint64          `json:"member_count"`
	RowVersion  uint64          `json:"row_version"`
	CreatedAt   string          `json:"created_at"`
	UpdatedAt   string          `json:"updated_at"`
}

func (r deviceGroupRecord) dto() deviceGroupDTO {
	rule := deviceGroupRule{}
	if len(r.Rule) > 0 {
		_ = json.Unmarshal(r.Rule, &rule)
	}
	return deviceGroupDTO{ID: r.ID, Name: r.Name, Kind: r.Kind, Rule: rule, Description: r.Description,
		MemberCount: r.MemberCount, RowVersion: r.RowVersion,
		CreatedAt: r.CreatedAt.UTC().Format(time.RFC3339Nano), UpdatedAt: r.UpdatedAt.UTC().Format(time.RFC3339Nano)}
}

type deviceGroupMutation struct {
	Name        *string          `json:"name"`
	Kind        *string          `json:"kind"`
	Rule        *deviceGroupRule `json:"rule"`
	Description *string          `json:"description"`
}

const deviceGroupSelect = `SELECT g.id,g.name,g.kind,COALESCE(g.rule_json,JSON_OBJECT()),g.description,g.row_version,g.created_at,g.updated_at,
	(SELECT COUNT(*) FROM device_group_members gm WHERE gm.device_group_id=g.id) AS member_count FROM device_groups g`

func scanDeviceGroup(row rowScanner) (deviceGroupRecord, error) {
	var value deviceGroupRecord
	err := row.Scan(&value.ID, &value.Name, &value.Kind, &value.Rule, &value.Description,
		&value.RowVersion, &value.CreatedAt, &value.UpdatedAt, &value.MemberCount)
	return value, err
}

func (s *Server) readDeviceGroup(ctx context.Context, id string) (deviceGroupRecord, error) {
	return scanDeviceGroup(s.db.QueryRowContext(ctx, deviceGroupSelect+" WHERE g.id=?", id))
}

func (s *Server) listDeviceGroups(c *gin.Context) {
	page, ok := parseInventoryPage(c, []string{"kind"}, map[string]string{"name": "g.name", "kind": "g.kind", "member_count": "member_count", "updated_at": "g.updated_at"}, "name")
	if !ok {
		return
	}
	where, args := []string{"1=1"}, []any{}
	if p := currentPrincipal(c); p != nil && !p.can("device.viewAll") {
		where = append(where, "EXISTS (SELECT 1 FROM user_device_group_permissions ug WHERE ug.user_id=? AND ug.device_group_id=g.id)")
		args = append(args, p.UserID)
	}
	if kind := strings.TrimSpace(c.Query("kind")); kind != "" {
		if !validDeviceGroupKind(kind) {
			fail(c, http.StatusBadRequest, "invalid_filter", "kind must be static or dynamic")
			return
		}
		where = append(where, "g.kind=?")
		args = append(args, kind)
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + escapeLike(q) + "%"
		where = append(where, "(g.name LIKE ? OR g.description LIKE ?)")
		args = append(args, like, like)
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM device_groups g"+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	queryArgs := append(append([]any{}, args...), page.Limit, page.Offset)
	rows, err := s.db.QueryContext(c.Request.Context(), deviceGroupSelect+clause+fmt.Sprintf(" ORDER BY %s %s,g.id %s LIMIT ? OFFSET ?", page.Sort, page.Order, page.Order), queryArgs...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := []deviceGroupDTO{}
	for rows.Next() {
		value, err := scanDeviceGroup(rows)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, value.dto())
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total})
}

func (s *Server) getDeviceGroup(c *gin.Context) {
	value, err := s.readDeviceGroup(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !s.deviceGroupInScope(c, value.ID) {
		return
	}
	c.Header("ETag", etag(value.RowVersion))
	c.JSON(http.StatusOK, value.dto())
}

func (s *Server) createDeviceGroup(c *gin.Context) {
	var req deviceGroupMutation
	if !decodeStrictBody(c, &req) {
		return
	}
	name, kind := valueOr(req.Name, ""), valueOr(req.Kind, "static")
	rule, err := validateDeviceGroup(name, kind, req.Rule)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	id := newID()
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO device_groups (id,name,kind,rule_json,description,created_by,updated_by) VALUES (?,?,?,?,?,?,?)`,
		id, name, kind, nullableRule(kind, rule), valueOr(req.Description, ""), principalUserID(c), principalUserID(c))
	if err == nil && kind == "dynamic" {
		err = refreshDynamicGroupMembership(c.Request.Context(), tx, id, rule)
	}
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		writeSQLError(c, err)
		return
	}
	value, err := s.readDeviceGroup(c.Request.Context(), id)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "device_group.create", "device_group", id)
	c.Header("ETag", etag(value.RowVersion))
	c.Header("Location", "/api/v1/device-groups/"+id)
	c.JSON(http.StatusCreated, value.dto())
}

func (s *Server) updateDeviceGroup(c *gin.Context) {
	var req deviceGroupMutation
	if !decodeStrictBody(c, &req) {
		return
	}
	current, err := s.readDeviceGroup(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !checkVersion(c, current.RowVersion, "device group") {
		return
	}
	oldRule := current.dto().Rule
	name, kind := patchString(req.Name, current.Name), patchString(req.Kind, current.Kind)
	var ruleInput *deviceGroupRule
	if kind == "dynamic" {
		ruleInput = &oldRule
	}
	if req.Rule != nil {
		ruleInput = req.Rule
	}
	rule, err := validateDeviceGroup(name, kind, ruleInput)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(c.Request.Context(), `UPDATE device_groups SET name=?,kind=?,rule_json=?,description=?,updated_by=?,row_version=row_version+1 WHERE id=? AND row_version=?`,
		name, kind, nullableRule(kind, rule), patchString(req.Description, current.Description), principalUserID(c), current.ID, current.RowVersion)
	if err == nil {
		if changed, _ := result.RowsAffected(); changed != 1 {
			err = errVersionConflict
		}
	}
	if err == nil && kind == "dynamic" {
		err = refreshDynamicGroupMembership(c.Request.Context(), tx, current.ID, rule)
	} else if err == nil && current.Kind == "dynamic" {
		_, err = tx.ExecContext(c.Request.Context(), "DELETE FROM device_group_members WHERE device_group_id=?", current.ID)
	}
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		writeSQLError(c, err)
		return
	}
	value, err := s.readDeviceGroup(c.Request.Context(), current.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "device_group.update", "device_group", current.ID)
	c.Header("ETag", etag(value.RowVersion))
	c.JSON(http.StatusOK, value.dto())
}

func (s *Server) deviceGroupDeletePreview(c *gin.Context) {
	value, err := s.readDeviceGroup(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	var grants int
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM user_device_group_permissions WHERE device_group_id=?", value.ID).Scan(&grants); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"device_group": value.dto(), "impacts": []gin.H{
		{"resource_type": "membership", "behavior": "deleted", "count": value.MemberCount},
		{"resource_type": "user_grant", "behavior": "deleted", "count": grants},
	}})
}

func (s *Server) deleteDeviceGroup(c *gin.Context) {
	current, err := s.readDeviceGroup(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !checkVersion(c, current.RowVersion, "device group") {
		return
	}
	result, err := s.db.ExecContext(c.Request.Context(), "DELETE FROM device_groups WHERE id=?", current.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		writeSQLError(c, sql.ErrNoRows)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "device_group.delete", "device_group", current.ID)
	c.Status(http.StatusNoContent)
}

func (s *Server) listDeviceGroupMembers(c *gin.Context) {
	group, err := s.readDeviceGroup(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !s.deviceGroupInScope(c, group.ID) {
		return
	}
	page, ok := parseInventoryPage(c, []string{"status", "kind", "location_id"}, map[string]string{
		"name": "COALESCE(NULLIF(d.display_name,''),NULLIF(d.sys_name,''),d.host)", "host": "d.host", "status": "d.status", "kind": "d.kind", "os": "d.os", "updated_at": "d.updated_at",
	}, "name")
	if !ok {
		return
	}
	where, args := []string{"gm.device_group_id=?"}, []any{group.ID}
	for _, filter := range []struct{ param, column string }{{"status", "d.status"}, {"kind", "d.kind"}, {"location_id", "d.location_id"}} {
		if value := strings.TrimSpace(c.Query(filter.param)); value != "" {
			where = append(where, filter.column+"=?")
			args = append(args, value)
		}
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + escapeLike(q) + "%"
		where = append(where, "(d.host LIKE ? OR d.display_name LIKE ? OR d.sys_name LIKE ? OR d.vendor LIKE ? OR d.model LIKE ? OR d.os LIKE ?)")
		for range 6 {
			args = append(args, like)
		}
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM device_group_members gm JOIN devices d ON d.id=gm.device_id"+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	queryArgs := append(append([]any{}, args...), page.Limit, page.Offset)
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT d.id,d.host,d.display_name,COALESCE(d.labels_json,JSON_OBJECT()),d.kind,d.vendor,d.model,d.platform,d.sys_name,COALESCE(d.sys_descr,''),d.sys_location,d.sys_object_id,d.os,d.os_version,d.hardware,d.serial,d.location_id,l.name,d.snmp_profile_id,d.snmp_port,d.status,d.status_reason,d.disabled,d.ignore_alerts,d.uptime_seconds,d.last_polled_at,d.row_version,d.created_at,d.updated_at
		FROM device_group_members gm JOIN devices d ON d.id=gm.device_id LEFT JOIN locations l ON l.id=d.location_id`+clause+
		fmt.Sprintf(" ORDER BY %s %s,d.id %s LIMIT ? OFFSET ?", page.Sort, page.Order, page.Order), queryArgs...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := []deviceDTO{}
	for rows.Next() {
		var value deviceRecord
		if err := rows.Scan(&value.ID, &value.Host, &value.DisplayName, &value.Labels, &value.Kind, &value.Vendor, &value.Model, &value.Platform, &value.SysName, &value.SysDescr, &value.SysLocation, &value.SysObjectID, &value.OS, &value.OSVersion, &value.Hardware, &value.Serial, &value.LocationID, &value.LocationName, &value.SNMPProfileID, &value.SNMPPort, &value.Status, &value.StatusReason, &value.Disabled, &value.IgnoreAlerts, &value.UptimeSeconds, &value.LastPolledAt, &value.RowVersion, &value.CreatedAt, &value.UpdatedAt); err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, value.dto())
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "group": group.dto()})
}

func (s *Server) replaceDeviceGroupMembers(c *gin.Context) {
	group, ok := s.staticDeviceGroupForMutation(c)
	if !ok {
		return
	}
	var req struct {
		DeviceIDs []string `json:"device_ids"`
	}
	if !decodeStrictBody(c, &req) {
		return
	}
	ids, err := normalizeObjectIDs("device_ids", req.DeviceIDs, 10000)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(c.Request.Context(), "DELETE FROM device_group_members WHERE device_group_id=?", group.ID); err == nil {
		for _, id := range ids {
			if _, err = tx.ExecContext(c.Request.Context(), "INSERT INTO device_group_members (device_group_id,device_id) VALUES (?,?)", group.ID, id); err != nil {
				break
			}
		}
	}
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "device_group.members_replace", "device_group", group.ID)
	c.JSON(http.StatusOK, gin.H{"device_group_id": group.ID, "device_ids": ids, "total": len(ids)})
}

func (s *Server) addDeviceGroupMember(c *gin.Context) {
	group, ok := s.staticDeviceGroupForMutation(c)
	if !ok {
		return
	}
	_, err := s.db.ExecContext(c.Request.Context(), `INSERT INTO device_group_members (device_group_id,device_id) VALUES (?,?) ON DUPLICATE KEY UPDATE device_id=device_id`, group.ID, c.Param("device_id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "device_group.member_add", "device_group", group.ID)
	c.Status(http.StatusNoContent)
}

func (s *Server) deleteDeviceGroupMember(c *gin.Context) {
	group, ok := s.staticDeviceGroupForMutation(c)
	if !ok {
		return
	}
	result, err := s.db.ExecContext(c.Request.Context(), "DELETE FROM device_group_members WHERE device_group_id=? AND device_id=?", group.ID, c.Param("device_id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		writeSQLError(c, sql.ErrNoRows)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "device_group.member_delete", "device_group", group.ID)
	c.Status(http.StatusNoContent)
}

func (s *Server) refreshDeviceGroup(c *gin.Context) {
	group, err := s.readDeviceGroup(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if group.Kind != "dynamic" {
		fail(c, http.StatusConflict, "invalid_state", "only dynamic groups can be refreshed")
		return
	}
	rule := group.dto().Rule
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err == nil {
		defer tx.Rollback()
		err = refreshDynamicGroupMembership(c.Request.Context(), tx, group.ID, rule)
	}
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		writeSQLError(c, err)
		return
	}
	value, err := s.readDeviceGroup(c.Request.Context(), group.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "device_group.refresh", "device_group", group.ID)
	c.JSON(http.StatusOK, value.dto())
}

func (s *Server) staticDeviceGroupForMutation(c *gin.Context) (deviceGroupRecord, bool) {
	group, err := s.readDeviceGroup(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return deviceGroupRecord{}, false
	}
	if group.Kind != "static" {
		fail(c, http.StatusConflict, "invalid_state", "dynamic group membership is rule-managed")
		return deviceGroupRecord{}, false
	}
	return group, true
}

func (s *Server) deviceGroupInScope(c *gin.Context, id string) bool {
	p := currentPrincipal(c)
	if p == nil {
		fail(c, http.StatusUnauthorized, "unauthorized", "authentication required")
		return false
	}
	if p.can("device.viewAll") {
		return true
	}
	var allowed bool
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT EXISTS(SELECT 1 FROM user_device_group_permissions WHERE user_id=? AND device_group_id=?)", p.UserID, id).Scan(&allowed); err != nil {
		writeSQLError(c, err)
		return false
	}
	if !allowed {
		fail(c, http.StatusForbidden, "forbidden", "device group is outside the caller's resource scope")
		return false
	}
	return true
}

func validDeviceGroupKind(kind string) bool { return kind == "static" || kind == "dynamic" }

func validateDeviceGroup(name, kind string, rule *deviceGroupRule) (deviceGroupRule, error) {
	if name == "" || len(name) > 190 {
		return deviceGroupRule{}, errors.New("name is required and must not exceed 190 characters")
	}
	if !validDeviceGroupKind(kind) {
		return deviceGroupRule{}, errors.New("kind must be static or dynamic")
	}
	if kind == "static" {
		if rule != nil && !emptyDeviceGroupRule(*rule) {
			return deviceGroupRule{}, errors.New("static groups cannot have a rule")
		}
		return deviceGroupRule{}, nil
	}
	if rule == nil || emptyDeviceGroupRule(*rule) {
		return deviceGroupRule{}, errors.New("dynamic groups require at least one rule selector")
	}
	value := *rule
	for _, entry := range []struct {
		name   string
		values *[]string
	}{{"kind", &value.Kind}, {"status", &value.Status}, {"vendor", &value.Vendor}, {"model", &value.Model}, {"platform", &value.Platform}, {"os", &value.OS}, {"location_id", &value.LocationID}} {
		normalized, err := normalizeRuleValues(*entry.values)
		if err != nil {
			return deviceGroupRule{}, fmt.Errorf("%s: %w", entry.name, err)
		}
		*entry.values = normalized
	}
	for _, kind := range value.Kind {
		if !validDeviceKind(kind) {
			return deviceGroupRule{}, fmt.Errorf("invalid device kind %q", kind)
		}
	}
	for _, status := range value.Status {
		if !validDeviceStatus(status) {
			return deviceGroupRule{}, fmt.Errorf("invalid device status %q", status)
		}
	}
	if len(value.Labels) > 16 {
		return deviceGroupRule{}, errors.New("labels accepts at most 16 keys")
	}
	normalizedLabels := make(map[string][]string, len(value.Labels))
	for rawKey, values := range value.Labels {
		key := strings.TrimSpace(rawKey)
		if key == "" || len(key) > 64 || strings.ContainsAny(key, `.$[]*`) {
			return deviceGroupRule{}, errors.New("label keys must be 1 to 64 plain characters")
		}
		normalized, err := normalizeRuleValues(values)
		if err != nil {
			return deviceGroupRule{}, fmt.Errorf("label %s: %w", key, err)
		}
		if len(normalized) == 0 {
			return deviceGroupRule{}, fmt.Errorf("label %s requires at least one value", key)
		}
		if _, duplicate := normalizedLabels[key]; duplicate {
			return deviceGroupRule{}, fmt.Errorf("duplicate normalized label key %s", key)
		}
		normalizedLabels[key] = normalized
	}
	value.Labels = normalizedLabels
	return value, nil
}

func normalizeRuleValues(values []string) ([]string, error) {
	if len(values) > 32 {
		return nil, errors.New("accepts at most 32 values")
	}
	seen := map[string]bool{}
	result := make([]string, 0, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" || len(value) > 190 {
			return nil, errors.New("values must be 1 to 190 characters")
		}
		if !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result, nil
}

func emptyDeviceGroupRule(rule deviceGroupRule) bool {
	return len(rule.Kind)+len(rule.Status)+len(rule.Vendor)+len(rule.Model)+len(rule.Platform)+len(rule.OS)+len(rule.LocationID)+len(rule.Labels) == 0
}

func refreshDynamicGroupMembership(ctx context.Context, tx *sql.Tx, groupID string, rule deviceGroupRule) error {
	where, args := compileDeviceGroupRule(rule, "d")
	if where == "" {
		return errors.New("dynamic group rule is empty")
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM device_group_members WHERE device_group_id=?", groupID); err != nil {
		return err
	}
	queryArgs := append([]any{groupID}, args...)
	_, err := tx.ExecContext(ctx, "INSERT INTO device_group_members (device_group_id,device_id) SELECT ?,d.id FROM devices d WHERE "+where, queryArgs...)
	return err
}

func compileDeviceGroupRule(rule deviceGroupRule, alias string) (string, []any) {
	conditions, args := []string{}, []any{}
	for _, entry := range []struct {
		column string
		values []string
	}{{"kind", rule.Kind}, {"status", rule.Status}, {"vendor", rule.Vendor}, {"model", rule.Model}, {"platform", rule.Platform}, {"os", rule.OS}, {"location_id", rule.LocationID}} {
		if len(entry.values) == 0 {
			continue
		}
		conditions = append(conditions, alias+"."+entry.column+" IN ("+placeholders(len(entry.values))+")")
		for _, value := range entry.values {
			args = append(args, value)
		}
	}
	keys := make([]string, 0, len(rule.Labels))
	for key := range rule.Labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		values := rule.Labels[key]
		conditions = append(conditions, "JSON_UNQUOTE(JSON_EXTRACT("+alias+".labels_json, ?)) IN ("+placeholders(len(values))+")")
		args = append(args, `$."`+key+`"`)
		for _, value := range values {
			args = append(args, value)
		}
	}
	return strings.Join(conditions, " AND "), args
}

func (s *Server) syncDynamicGroupsForDevice(ctx context.Context, deviceID string) error {
	rows, err := s.db.QueryContext(ctx, "SELECT id,rule_json FROM device_groups WHERE kind='dynamic'")
	if err != nil {
		return err
	}
	type group struct {
		id   string
		rule deviceGroupRule
	}
	groups := []group{}
	for rows.Next() {
		var id string
		var raw json.RawMessage
		if err := rows.Scan(&id, &raw); err != nil {
			rows.Close()
			return err
		}
		var rule deviceGroupRule
		if err := json.Unmarshal(raw, &rule); err != nil {
			rows.Close()
			return err
		}
		groups = append(groups, group{id: id, rule: rule})
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, group := range groups {
		where, args := compileDeviceGroupRule(group.rule, "d")
		if where == "" {
			return errors.New("stored dynamic group rule is empty")
		}
		var matches bool
		if err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM devices d WHERE d.id=? AND "+where+")", append([]any{deviceID}, args...)...).Scan(&matches); err != nil {
			return err
		}
		if matches {
			_, err = s.db.ExecContext(ctx, `INSERT INTO device_group_members (device_group_id,device_id) VALUES (?,?) ON DUPLICATE KEY UPDATE device_id=device_id`, group.id, deviceID)
		} else {
			_, err = s.db.ExecContext(ctx, "DELETE FROM device_group_members WHERE device_group_id=? AND device_id=?", group.id, deviceID)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func validateLocation(name string, lat, lon *float64) error {
	if name == "" || len(name) > 190 {
		return errors.New("name is required and must not exceed 190 characters")
	}
	if (lat == nil) != (lon == nil) {
		return errors.New("latitude and longitude must be provided together")
	}
	if lat != nil && (*lat < -90 || *lat > 90 || *lon < -180 || *lon > 180) {
		return errors.New("latitude or longitude is outside its valid range")
	}
	return nil
}

func decodeStrictBody(c *gin.Context, target any) bool {
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body: "+err.Error())
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		fail(c, http.StatusBadRequest, "invalid_request", "body must contain one JSON object")
		return false
	}
	return true
}

func checkVersion(c *gin.Context, actual uint64, resource string) bool {
	expected, supplied, err := ifMatch(c)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_if_match", "invalid If-Match")
		return false
	}
	if supplied && expected != actual {
		fail(c, http.StatusPreconditionFailed, "version_conflict", resource+" changed since it was loaded")
		return false
	}
	return true
}

func nullableFloat(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

func patchNullFloat(value *float64, current sql.NullFloat64) any {
	if value != nil {
		return *value
	}
	if current.Valid {
		return current.Float64
	}
	return nil
}

func floatPointer(value any) *float64 {
	if value == nil {
		return nil
	}
	result := value.(float64)
	return &result
}

func nullableRule(kind string, rule deviceGroupRule) any {
	if kind != "dynamic" {
		return nil
	}
	return mustJSON(rule)
}

func normalizeObjectIDs(field string, values []string, max int) ([]string, error) {
	if len(values) > max {
		return nil, fmt.Errorf("%s accepts at most %d values", field, max)
	}
	seen := map[string]bool{}
	result := make([]string, 0, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" || len(value) > 26 {
			return nil, fmt.Errorf("%s contains an invalid id", field)
		}
		if !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result, nil
}

func placeholders(count int) string {
	if count < 1 {
		return "NULL"
	}
	return strings.TrimSuffix(strings.Repeat("?,", count), ",")
}

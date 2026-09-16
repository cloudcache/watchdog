package server

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type inventoryPage struct {
	Limit, Offset int
	Sort, Order   string
}

func parseInventoryPage(c *gin.Context, filters []string, sorts map[string]string, defaultSort string) (inventoryPage, bool) {
	allowed := map[string]bool{"q": true, "limit": true, "offset": true, "sort": true, "order": true}
	for _, filter := range filters {
		allowed[filter] = true
	}
	for key := range c.Request.URL.Query() {
		if !allowed[key] {
			fail(c, http.StatusBadRequest, "invalid_filter", "unsupported query parameter: "+key)
			return inventoryPage{}, false
		}
	}
	if len(strings.TrimSpace(c.Query("q"))) > 256 {
		fail(c, http.StatusBadRequest, "invalid_filter", "q must not exceed 256 characters")
		return inventoryPage{}, false
	}
	page := inventoryPage{Limit: 100, Sort: defaultSort, Order: "ASC"}
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 500 {
			fail(c, http.StatusBadRequest, "invalid_filter", "limit must be between 1 and 500")
			return inventoryPage{}, false
		}
		page.Limit = value
	}
	if raw := strings.TrimSpace(c.Query("offset")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 {
			fail(c, http.StatusBadRequest, "invalid_filter", "offset must be a non-negative integer")
			return inventoryPage{}, false
		}
		page.Offset = value
	}
	if requested := strings.TrimSpace(c.Query("sort")); requested != "" {
		page.Sort = requested
	}
	column, ok := sorts[page.Sort]
	if !ok {
		fail(c, http.StatusBadRequest, "invalid_filter", "invalid sort")
		return inventoryPage{}, false
	}
	page.Sort = column
	if order := strings.ToLower(strings.TrimSpace(c.Query("order"))); order != "" {
		if order != "asc" && order != "desc" {
			fail(c, http.StatusBadRequest, "invalid_filter", "order must be asc or desc")
			return inventoryPage{}, false
		}
		page.Order = strings.ToUpper(order)
	}
	return page, true
}

func (s *Server) loadScopedDevice(c *gin.Context, id string) (deviceRecord, bool) {
	device, err := s.readDevice(c, id)
	if err != nil {
		writeSQLError(c, err)
		return deviceRecord{}, false
	}
	if !s.requireDeviceAccess(c, id) {
		return deviceRecord{}, false
	}
	return device, true
}

func (s *Server) loadScopedNetworkDevice(c *gin.Context, id string) (deviceRecord, bool) {
	device, ok := s.loadScopedDevice(c, id)
	if !ok {
		return deviceRecord{}, false
	}
	if device.Kind != "network" {
		fail(c, http.StatusConflict, "invalid_device_kind", "SNMP operations require a network device")
		return deviceRecord{}, false
	}
	return device, true
}

type interfaceAddressDTO struct {
	ID, DeviceID, PortID string
	IfIndex              uint64
	Address, Family      string
	PrefixLength         uint8
	Origin, ContextName  string
	UpdatedAt            string
}

type portRecord struct {
	ID, DeviceID, IfName, IfDescr, IfAlias, IfType, PhysAddress, OperStatus, AdminStatus string
	IfIndex                                                                              uint64
	IfSpeed                                                                              sql.NullInt64
	IfHighSpeed                                                                          sql.NullInt64
	IfMTU                                                                                sql.NullInt64
	Disabled, IgnoreAlerts                                                               bool
	Metadata                                                                             json.RawMessage
	RowVersion                                                                           uint64
	UpdatedAt                                                                            time.Time
}

type portDTO struct {
	ID, DeviceID, IfName, IfDescr, IfAlias, AdminStatus, OperStatus string
	IfIndex, SpeedBps                                               uint64
	Disabled, IgnoreAlerts                                          bool
	Metadata                                                        map[string]any
	Addresses                                                       []interfaceAddressDTO
	RowVersion                                                      uint64
	UpdatedAt                                                       string
}

func (r portRecord) dto() portDTO {
	metadata := map[string]any{}
	_ = json.Unmarshal(r.Metadata, &metadata)
	if r.IfType != "" {
		metadata["if_type"] = r.IfType
	}
	if r.IfHighSpeed.Valid {
		metadata["if_high_speed_mbps"] = r.IfHighSpeed.Int64
	}
	if r.IfMTU.Valid {
		metadata["if_mtu"] = r.IfMTU.Int64
	}
	if r.PhysAddress != "" {
		metadata["if_phys_address"] = r.PhysAddress
	}
	speed := uint64(0)
	if r.IfSpeed.Valid && r.IfSpeed.Int64 > 0 {
		speed = uint64(r.IfSpeed.Int64)
	}
	return portDTO{
		ID: r.ID, DeviceID: r.DeviceID, IfIndex: r.IfIndex, IfName: r.IfName, IfDescr: r.IfDescr,
		IfAlias: r.IfAlias, AdminStatus: r.AdminStatus, OperStatus: r.OperStatus, SpeedBps: speed,
		Disabled: r.Disabled, IgnoreAlerts: r.IgnoreAlerts, Metadata: metadata,
		Addresses: []interfaceAddressDTO{}, RowVersion: r.RowVersion, UpdatedAt: r.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

const portSelect = `SELECT p.id,p.device_id,p.if_index,p.if_name,p.if_descr,p.if_alias,p.if_speed,p.if_high_speed,
	p.if_type,p.if_mtu,p.if_phys_address,p.if_oper_status,p.if_admin_status,p.disabled,p.ignore_alerts,
	COALESCE(p.metadata_json,JSON_OBJECT()),p.row_version,p.updated_at FROM ports p`

func scanPort(row rowScanner) (portRecord, error) {
	var r portRecord
	err := row.Scan(&r.ID, &r.DeviceID, &r.IfIndex, &r.IfName, &r.IfDescr, &r.IfAlias, &r.IfSpeed,
		&r.IfHighSpeed, &r.IfType, &r.IfMTU, &r.PhysAddress, &r.OperStatus, &r.AdminStatus,
		&r.Disabled, &r.IgnoreAlerts, &r.Metadata, &r.RowVersion, &r.UpdatedAt)
	return r, err
}

func (s *Server) readPort(c *gin.Context, id string) (portRecord, error) {
	return scanPort(s.db.QueryRowContext(c.Request.Context(), portSelect+" WHERE p.id=?", id))
}

func (s *Server) listDevicePorts(c *gin.Context) {
	deviceID := c.Param("id")
	if _, err := s.readDevice(c, deviceID); err != nil {
		writeSQLError(c, err)
		return
	}
	allPorts, ok := s.portScope(c, deviceID, "")
	if !ok {
		return
	}
	page, ok := parseInventoryPage(c, []string{"admin_status", "oper_status", "address_family", "disabled"}, map[string]string{
		"if_index": "p.if_index", "name": "COALESCE(NULLIF(p.if_name,''),p.if_descr)", "alias": "p.if_alias",
		"admin_status": "p.if_admin_status", "oper_status": "p.if_oper_status", "speed": "p.if_speed",
	}, "if_index")
	if !ok {
		return
	}
	where := []string{"p.device_id=?"}
	args := []any{deviceID}
	if !allPorts {
		where = append(where, "EXISTS (SELECT 1 FROM user_port_permissions upp WHERE upp.user_id=? AND upp.port_id=p.id)")
		args = append(args, currentPrincipal(c).UserID)
	}
	for _, filter := range []struct{ param, column string }{{"admin_status", "p.if_admin_status"}, {"oper_status", "p.if_oper_status"}} {
		if value := strings.TrimSpace(c.Query(filter.param)); value != "" && value != "all" {
			if len(value) > 32 {
				fail(c, http.StatusBadRequest, "invalid_filter", filter.param+" is too long")
				return
			}
			where = append(where, filter.column+"=?")
			args = append(args, value)
		}
	}
	if raw := strings.TrimSpace(c.Query("disabled")); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			fail(c, http.StatusBadRequest, "invalid_filter", "disabled must be true or false")
			return
		}
		where = append(where, "p.disabled=?")
		args = append(args, value)
	}
	if family := strings.ToLower(strings.TrimSpace(c.Query("address_family"))); family != "" && family != "all" {
		familyNumber := 0
		switch family {
		case "ipv4":
			familyNumber = 4
		case "ipv6":
			familyNumber = 6
		default:
			fail(c, http.StatusBadRequest, "invalid_filter", "address_family must be ipv4 or ipv6")
			return
		}
		where = append(where, "EXISTS (SELECT 1 FROM interface_addresses ia WHERE ia.port_id=p.id AND ia.family=?)")
		args = append(args, familyNumber)
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + escapeLike(q) + "%"
		where = append(where, `(p.if_name LIKE ? OR p.if_descr LIKE ? OR p.if_alias LIKE ? OR p.if_phys_address LIKE ? OR
			EXISTS (SELECT 1 FROM interface_addresses ia WHERE ia.port_id=p.id AND INET6_NTOA(ia.address) LIKE ?))`)
		args = append(args, like, like, like, like, like)
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM ports p"+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	rows, err := s.db.QueryContext(c.Request.Context(), portSelect+clause+fmt.Sprintf(" ORDER BY %s %s,p.id %s LIMIT ? OFFSET ?", page.Sort, page.Order, page.Order), append(args, page.Limit, page.Offset)...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]portDTO, 0, page.Limit)
	for rows.Next() {
		record, err := scanPort(rows)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, record.dto())
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	if err := s.attachPortAddresses(c, items); err != nil {
		writeSQLError(c, err)
		return
	}
	var counts struct{ Total, Up, Down int }
	countSQL := `SELECT COUNT(*),COALESCE(SUM(p.if_oper_status='up'),0),COALESCE(SUM(p.if_oper_status='down'),0) FROM ports p WHERE p.device_id=?`
	countArgs := []any{deviceID}
	if !allPorts {
		countSQL += " AND EXISTS (SELECT 1 FROM user_port_permissions upp WHERE upp.user_id=? AND upp.port_id=p.id)"
		countArgs = append(countArgs, currentPrincipal(c).UserID)
	}
	if err := s.db.QueryRowContext(c.Request.Context(), countSQL, countArgs...).Scan(&counts.Total, &counts.Up, &counts.Down); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset, "counts": gin.H{"total": counts.Total, "up": counts.Up, "down": counts.Down}})
}

func (s *Server) attachPortAddresses(c *gin.Context, ports []portDTO) error {
	if len(ports) == 0 {
		return nil
	}
	placeholders := make([]string, len(ports))
	args := make([]any, len(ports))
	index := make(map[string]int, len(ports))
	for i := range ports {
		placeholders[i], args[i], index[ports[i].ID] = "?", ports[i].ID, i
	}
	rows, err := s.db.QueryContext(c.Request.Context(), interfaceAddressSelect+" WHERE ia.port_id IN ("+strings.Join(placeholders, ",")+") ORDER BY ia.port_id,ia.family,ia.address", args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		address, err := scanInterfaceAddress(rows)
		if err != nil {
			return err
		}
		if i, ok := index[address.PortID]; ok {
			ports[i].Addresses = append(ports[i].Addresses, address)
		}
	}
	return rows.Err()
}

const interfaceAddressSelect = `SELECT ia.id,ia.device_id,COALESCE(ia.port_id,''),COALESCE(NULLIF(ia.if_index,0),p.if_index,0),
	COALESCE(INET6_NTOA(ia.address),''),CASE ia.family WHEN 4 THEN 'ipv4' WHEN 6 THEN 'ipv6' ELSE 'unknown' END,
	ia.prefix_len,ia.origin,ia.context,ia.updated_at FROM interface_addresses ia LEFT JOIN ports p ON p.id=ia.port_id`

func scanInterfaceAddress(row rowScanner) (interfaceAddressDTO, error) {
	var value interfaceAddressDTO
	var prefix uint64
	var updated time.Time
	err := row.Scan(&value.ID, &value.DeviceID, &value.PortID, &value.IfIndex, &value.Address, &value.Family,
		&prefix, &value.Origin, &value.ContextName, &updated)
	value.PrefixLength = uint8(prefix)
	value.UpdatedAt = updated.UTC().Format(time.RFC3339Nano)
	return value, err
}

func (s *Server) listDeviceAddresses(c *gin.Context) {
	deviceID := c.Param("id")
	if _, err := s.readDevice(c, deviceID); err != nil {
		writeSQLError(c, err)
		return
	}
	allPorts, ok := s.portScope(c, deviceID, "")
	if !ok {
		return
	}
	page, ok := parseInventoryPage(c, []string{"family", "port_id", "context"}, map[string]string{
		"address": "ia.address", "family": "ia.family", "prefix_length": "ia.prefix_len", "port": "p.if_index", "context": "ia.context",
	}, "address")
	if !ok {
		return
	}
	where, args := []string{"ia.device_id=?"}, []any{deviceID}
	if !allPorts {
		where = append(where, "EXISTS (SELECT 1 FROM user_port_permissions upp WHERE upp.user_id=? AND upp.port_id=ia.port_id)")
		args = append(args, currentPrincipal(c).UserID)
	}
	if family := strings.ToLower(strings.TrimSpace(c.Query("family"))); family != "" && family != "all" {
		if family != "ipv4" && family != "ipv6" {
			fail(c, http.StatusBadRequest, "invalid_filter", "family must be ipv4 or ipv6")
			return
		}
		where, args = append(where, "ia.family=?"), append(args, map[string]int{"ipv4": 4, "ipv6": 6}[family])
	}
	for _, filter := range []struct{ param, column string }{{"port_id", "ia.port_id"}, {"context", "ia.context"}} {
		if value := strings.TrimSpace(c.Query(filter.param)); value != "" {
			where, args = append(where, filter.column+"=?"), append(args, value)
		}
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + escapeLike(q) + "%"
		where, args = append(where, "(INET6_NTOA(ia.address) LIKE ? OR ia.context LIKE ? OR p.if_name LIKE ? OR p.if_descr LIKE ?)"), append(args, like, like, like, like)
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM interface_addresses ia LEFT JOIN ports p ON p.id=ia.port_id"+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	rows, err := s.db.QueryContext(c.Request.Context(), interfaceAddressSelect+clause+fmt.Sprintf(" ORDER BY %s %s,ia.id %s LIMIT ? OFFSET ?", page.Sort, page.Order, page.Order), append(args, page.Limit, page.Offset)...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]interfaceAddressDTO, 0, page.Limit)
	for rows.Next() {
		item, err := scanInterfaceAddress(rows)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

func (s *Server) getPort(c *gin.Context) {
	record, err := s.readPort(c, c.Param("port_id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	device, err := s.readDevice(c, record.DeviceID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	_, ok := s.portScope(c, record.DeviceID, record.ID)
	if !ok {
		return
	}
	ports := []portDTO{record.dto()}
	if err := s.attachPortAddresses(c, ports); err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", etag(record.RowVersion))
	c.JSON(http.StatusOK, gin.H{"port": ports[0], "device": device.dto(), "transceiver": nil})
}

type portMutation struct {
	ID, LegacyID                   string
	IfIndex, LegacyIfIndex         *uint64
	IfName, LegacyIfName           *string
	IfDescr, LegacyIfDescr         *string
	IfAlias, LegacyIfAlias         *string
	AdminStatus, LegacyAdminStatus *string
	OperStatus, LegacyOperStatus   *string
	SpeedBps, LegacySpeedBps       *uint64
	Disabled, IgnoreAlerts         *bool
	Metadata, LegacyMetadata       map[string]any
}

func (m *portMutation) UnmarshalJSON(data []byte) error {
	type wire struct {
		ID             string         `json:"id"`
		LegacyID       string         `json:"ID"`
		IfIndex        *uint64        `json:"if_index"`
		LegacyIfIndex  *uint64        `json:"IfIndex"`
		IfName         *string        `json:"if_name"`
		LegacyIfName   *string        `json:"IfName"`
		IfDescr        *string        `json:"if_descr"`
		LegacyIfDescr  *string        `json:"IfDescr"`
		IfAlias        *string        `json:"if_alias"`
		LegacyIfAlias  *string        `json:"IfAlias"`
		AdminStatus    *string        `json:"admin_status"`
		LegacyAdmin    *string        `json:"AdminStatus"`
		OperStatus     *string        `json:"oper_status"`
		LegacyOper     *string        `json:"OperStatus"`
		SpeedBps       *uint64        `json:"speed_bps"`
		LegacySpeedBps *uint64        `json:"SpeedBps"`
		Disabled       *bool          `json:"disabled"`
		IgnoreAlerts   *bool          `json:"ignore_alerts"`
		Metadata       map[string]any `json:"metadata"`
		LegacyMetadata map[string]any `json:"Metadata"`
	}
	var value wire
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	*m = portMutation{ID: value.ID, LegacyID: value.LegacyID, IfIndex: value.IfIndex, LegacyIfIndex: value.LegacyIfIndex,
		IfName: value.IfName, LegacyIfName: value.LegacyIfName, IfDescr: value.IfDescr, LegacyIfDescr: value.LegacyIfDescr,
		IfAlias: value.IfAlias, LegacyIfAlias: value.LegacyIfAlias, AdminStatus: value.AdminStatus, LegacyAdminStatus: value.LegacyAdmin,
		OperStatus: value.OperStatus, LegacyOperStatus: value.LegacyOper, SpeedBps: value.SpeedBps, LegacySpeedBps: value.LegacySpeedBps,
		Disabled: value.Disabled, IgnoreAlerts: value.IgnoreAlerts, Metadata: value.Metadata, LegacyMetadata: value.LegacyMetadata}
	return nil
}

func (s *Server) updatePort(c *gin.Context) {
	current, err := s.readPort(c, c.Param("port_id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if _, ok := s.portScope(c, current.DeviceID, current.ID); !ok {
		return
	}
	var req portMutation
	decoder := json.NewDecoder(c.Request.Body)
	if err := decoder.Decode(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid JSON body: "+err.Error())
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		fail(c, http.StatusBadRequest, "invalid_request", "body must contain one JSON object")
		return
	}
	if expected, supplied, err := ifMatch(c); err != nil {
		fail(c, http.StatusBadRequest, "invalid_if_match", "invalid If-Match")
		return
	} else if supplied && expected != current.RowVersion {
		writeSQLError(c, errVersionConflict)
		return
	}
	ifIndex := valueOrUint64(firstUint64(req.IfIndex, req.LegacyIfIndex), current.IfIndex)
	if ifIndex == 0 {
		fail(c, http.StatusBadRequest, "invalid_request", "if_index must be greater than zero")
		return
	}
	ifName := patchString(firstPointer(req.IfName, req.LegacyIfName), current.IfName)
	ifDescr := patchString(firstPointer(req.IfDescr, req.LegacyIfDescr), current.IfDescr)
	ifAlias := patchString(firstPointer(req.IfAlias, req.LegacyIfAlias), current.IfAlias)
	admin := normalizeIfStatus(patchString(firstPointer(req.AdminStatus, req.LegacyAdminStatus), current.AdminStatus))
	oper := normalizeIfStatus(patchString(firstPointer(req.OperStatus, req.LegacyOperStatus), current.OperStatus))
	if !validIfStatus(admin) || !validIfStatus(oper) {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid interface status")
		return
	}
	speed := valueOrUint64(firstUint64(req.SpeedBps, req.LegacySpeedBps), uint64(maxInt64(current.IfSpeed)))
	metadata := req.Metadata
	if metadata == nil {
		metadata = req.LegacyMetadata
	}
	metadataJSON := string(current.Metadata)
	if metadata != nil {
		encoded, err := json.Marshal(metadata)
		if err != nil || len(encoded) > 256*1024 {
			fail(c, http.StatusBadRequest, "invalid_request", "metadata is invalid or too large")
			return
		}
		metadataJSON = string(encoded)
	}
	result, err := s.db.ExecContext(c.Request.Context(), `UPDATE ports SET if_index=?,if_name=?,if_descr=?,if_alias=?,if_speed=?,
		if_admin_status=?,if_oper_status=?,disabled=?,ignore_alerts=?,metadata_json=?,row_version=row_version+1
		WHERE id=? AND row_version=?`, ifIndex, ifName, ifDescr, ifAlias, speed, admin, oper,
		valueOrBool(req.Disabled, current.Disabled), valueOrBool(req.IgnoreAlerts, current.IgnoreAlerts), metadataJSON, current.ID, current.RowVersion)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		writeSQLError(c, errVersionConflict)
		return
	}
	updated, err := s.readPort(c, current.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "port.update", "port", current.ID)
	c.Header("ETag", etag(updated.RowVersion))
	c.JSON(http.StatusOK, updated.dto())
}

func (s *Server) portDeletePreview(c *gin.Context) {
	port, err := s.readPort(c, c.Param("port_id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if _, ok := s.portScope(c, port.DeviceID, port.ID); !ok {
		return
	}
	var addresses, bills int
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT
		(SELECT COUNT(*) FROM interface_addresses WHERE port_id=?),
		(SELECT COUNT(*) FROM billing_account_ports WHERE port_id=?)`, port.ID, port.ID).Scan(&addresses, &bills); err != nil {
		writeSQLError(c, err)
		return
	}
	impacts := []gin.H{}
	if addresses > 0 {
		impacts = append(impacts, gin.H{"resource_type": "network_interface_address", "behavior": "deleted", "count": addresses})
	}
	if bills > 0 {
		impacts = append(impacts, gin.H{"resource_type": "billing_account", "behavior": "blocked", "count": bills, "detail": "detach billing accounts before deleting the port"})
	}
	c.JSON(http.StatusOK, gin.H{"port": port.dto(), "impacts": impacts})
}

func (s *Server) deletePort(c *gin.Context) {
	port, err := s.readPort(c, c.Param("port_id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if _, ok := s.portScope(c, port.DeviceID, port.ID); !ok {
		return
	}
	if expected, supplied, err := ifMatch(c); err != nil {
		fail(c, http.StatusBadRequest, "invalid_if_match", "invalid If-Match")
		return
	} else if supplied && expected != port.RowVersion {
		writeSQLError(c, errVersionConflict)
		return
	}
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	var lockedVersion uint64
	if err := tx.QueryRowContext(c.Request.Context(), `SELECT row_version FROM ports WHERE id=? FOR UPDATE`, port.ID).Scan(&lockedVersion); err != nil {
		writeSQLError(c, err)
		return
	}
	if lockedVersion != port.RowVersion {
		writeSQLError(c, errVersionConflict)
		return
	}
	rows, err := tx.QueryContext(c.Request.Context(), `SELECT account_id FROM billing_account_ports WHERE port_id=? FOR UPDATE`, port.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	bills := 0
	for rows.Next() {
		bills++
	}
	rowsErr := rows.Err()
	_ = rows.Close()
	if rowsErr != nil {
		writeSQLError(c, rowsErr)
		return
	}
	if bills > 0 {
		fail(c, http.StatusConflict, "resource_in_use", "detach billing accounts before deleting the port")
		return
	}
	if _, err := tx.ExecContext(c.Request.Context(), `DELETE FROM interface_addresses WHERE port_id=?`, port.ID); err != nil {
		writeSQLError(c, err)
		return
	}
	result, err := tx.ExecContext(c.Request.Context(), `DELETE FROM ports WHERE id=? AND row_version=?`, port.ID, port.RowVersion)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		writeSQLError(c, errVersionConflict)
		return
	}
	if err := tx.Commit(); err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "port.delete", "port", port.ID)
	c.Status(http.StatusNoContent)
}

func firstUint64(a, b *uint64) *uint64 {
	if a != nil {
		return a
	}
	return b
}

// portScope implements the two-level LibreNMS-style resource scope: a device
// grant includes every child port, while an explicit port grant includes only
// that port. The caller has already passed the action ability middleware.
func (s *Server) portScope(c *gin.Context, deviceID, portID string) (allPorts bool, allowed bool) {
	p := currentPrincipal(c)
	if p == nil {
		fail(c, http.StatusUnauthorized, "unauthorized", "authentication required")
		return false, false
	}
	if p.can("port.viewAll") || p.can("device.viewAll") {
		return true, true
	}
	var deviceGranted bool
	err := s.db.QueryRowContext(c.Request.Context(), `SELECT EXISTS(
		SELECT 1 FROM user_device_permissions WHERE user_id=? AND device_id=?
		UNION ALL
		SELECT 1 FROM user_device_group_permissions udgp JOIN device_group_members dgm ON dgm.device_group_id=udgp.device_group_id WHERE udgp.user_id=? AND dgm.device_id=?
	)`, p.UserID, deviceID, p.UserID, deviceID).Scan(&deviceGranted)
	if err != nil {
		writeSQLError(c, err)
		return false, false
	}
	if deviceGranted {
		return true, true
	}
	var explicitlyGranted bool
	if portID == "" {
		err = s.db.QueryRowContext(c.Request.Context(), `SELECT EXISTS(
			SELECT 1 FROM user_port_permissions upp JOIN ports p ON p.id=upp.port_id WHERE upp.user_id=? AND p.device_id=?
		)`, p.UserID, deviceID).Scan(&explicitlyGranted)
	} else {
		err = s.db.QueryRowContext(c.Request.Context(), `SELECT EXISTS(
			SELECT 1 FROM user_port_permissions WHERE user_id=? AND port_id=?
		)`, p.UserID, portID).Scan(&explicitlyGranted)
	}
	if err != nil {
		writeSQLError(c, err)
		return false, false
	}
	if !explicitlyGranted {
		fail(c, http.StatusForbidden, "forbidden", "port is outside the caller's resource scope")
		return false, false
	}
	return false, true
}

func maxInt64(value sql.NullInt64) int64 {
	if value.Valid && value.Int64 > 0 {
		return value.Int64
	}
	return 0
}

func normalizeIfStatus(value string) string {
	switch value {
	case "1":
		return "up"
	case "2":
		return "down"
	case "3":
		return "testing"
	case "4":
		return "unknown"
	case "5":
		return "dormant"
	case "6":
		return "notPresent"
	case "7":
		return "lowerLayerDown"
	default:
		return value
	}
}

func validIfStatus(value string) bool {
	switch value {
	case "", "up", "down", "testing", "unknown", "dormant", "notPresent", "lowerLayerDown":
		return true
	default:
		return false
	}
}

type bgpSessionDTO struct {
	ID, DeviceID, DeviceSysName, TargetID string
	PeerAddr                              string
	PeerAS, LocalAS                       uint64
	AFI, SAFI, State                      string
	AcceptedPrefixes                      uint64
	DeniedPrefixes                        uint64
	AdvertisedPrefixes                    uint64
	Uptime                                uint64
	Metadata                              map[string]any
	UpdatedAt                             string
}

type bgpSessionRecord struct {
	bgpSessionDTO
	MetadataJSON json.RawMessage
	Updated      time.Time
}

const bgpSelect = `SELECT b.id,b.device_id,COALESCE(NULLIF(d.sys_name,''),NULLIF(d.display_name,''),d.host),d.id,
	b.peer_address,b.peer_as,b.local_as,b.afi,b.safi,b.state,b.prefixes,b.denied_prefixes,b.advertised_prefixes,
	b.uptime_seconds,COALESCE(b.metadata_json,JSON_OBJECT()),b.updated_at
	FROM bgp_sessions b JOIN devices d ON d.id=b.device_id`

func scanBGPSession(row rowScanner) (bgpSessionRecord, error) {
	var value bgpSessionRecord
	err := row.Scan(&value.ID, &value.DeviceID, &value.DeviceSysName, &value.TargetID, &value.PeerAddr,
		&value.PeerAS, &value.LocalAS, &value.AFI, &value.SAFI, &value.State, &value.AcceptedPrefixes,
		&value.DeniedPrefixes, &value.AdvertisedPrefixes, &value.Uptime, &value.MetadataJSON, &value.Updated)
	if err == nil {
		value.Metadata = map[string]any{}
		_ = json.Unmarshal(value.MetadataJSON, &value.Metadata)
		value.Uptime *= uint64(time.Second)
		value.UpdatedAt = value.Updated.UTC().Format(time.RFC3339Nano)
	}
	return value, err
}

func (s *Server) listDeviceBGP(c *gin.Context) { s.listBGP(c, true) }
func (s *Server) listAllBGP(c *gin.Context)    { s.listBGP(c, false) }

func (s *Server) listBGP(c *gin.Context, deviceOnly bool) {
	page, ok := parseInventoryPage(c, []string{"state", "afi", "safi"}, map[string]string{
		"peer": "b.peer_address", "peer_as": "b.peer_as", "local_as": "b.local_as", "state": "b.state",
		"afi": "b.afi", "safi": "b.safi", "device": "COALESCE(NULLIF(d.sys_name,''),NULLIF(d.display_name,''),d.host)",
		"updated_at": "b.updated_at",
	}, map[bool]string{true: "peer", false: "device"}[deviceOnly])
	if !ok {
		return
	}
	where := []string{"1=1"}
	args := []any{}
	if deviceOnly {
		deviceID := c.Param("id")
		if _, ok := s.loadScopedDevice(c, deviceID); !ok {
			return
		}
		where, args = append(where, "b.device_id=?"), append(args, deviceID)
	} else if p := currentPrincipal(c); p != nil && !p.can("device.viewAll") {
		where = append(where, `(EXISTS (SELECT 1 FROM user_device_permissions udp WHERE udp.user_id=? AND udp.device_id=b.device_id)
			OR EXISTS (SELECT 1 FROM user_device_group_permissions udgp JOIN device_group_members dgm ON dgm.device_group_id=udgp.device_group_id WHERE udgp.user_id=? AND dgm.device_id=b.device_id))`)
		args = append(args, p.UserID, p.UserID)
	}
	if state := strings.ToLower(strings.TrimSpace(c.Query("state"))); state != "" && state != "all" {
		switch state {
		case "idle", "connect", "active", "opensent", "openconfirm", "established":
		default:
			fail(c, http.StatusBadRequest, "invalid_filter", "invalid BGP state")
			return
		}
		where, args = append(where, "LOWER(b.state)=?"), append(args, state)
	}
	for _, filter := range []struct{ param, column string }{{"afi", "b.afi"}, {"safi", "b.safi"}} {
		if value := strings.TrimSpace(c.Query(filter.param)); value != "" && value != "all" {
			if len(value) > 16 {
				fail(c, http.StatusBadRequest, "invalid_filter", filter.param+" is too long")
				return
			}
			where, args = append(where, filter.column+"=?"), append(args, value)
		}
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + escapeLike(q) + "%"
		where = append(where, `(b.peer_address LIKE ? OR CAST(b.peer_as AS CHAR) LIKE ? OR CAST(b.local_as AS CHAR) LIKE ?
			OR b.afi LIKE ? OR b.safi LIKE ? OR b.state LIKE ? OR d.host LIKE ? OR d.sys_name LIKE ? OR d.display_name LIKE ?)`)
		for range 9 {
			args = append(args, like)
		}
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM bgp_sessions b JOIN devices d ON d.id=b.device_id"+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	rows, err := s.db.QueryContext(c.Request.Context(), bgpSelect+clause+fmt.Sprintf(" ORDER BY %s %s,b.id %s LIMIT ? OFFSET ?", page.Sort, page.Order, page.Order), append(args, page.Limit, page.Offset)...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]bgpSessionDTO, 0, page.Limit)
	for rows.Next() {
		value, err := scanBGPSession(rows)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, value.bgpSessionDTO)
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	countWhere := []string{"1=1"}
	countArgs := []any{}
	if deviceOnly {
		countWhere, countArgs = append(countWhere, "b.device_id=?"), append(countArgs, c.Param("id"))
	} else if p := currentPrincipal(c); p != nil && !p.can("device.viewAll") {
		countWhere = append(countWhere, `(EXISTS (SELECT 1 FROM user_device_permissions udp WHERE udp.user_id=? AND udp.device_id=b.device_id)
			OR EXISTS (SELECT 1 FROM user_device_group_permissions udgp JOIN device_group_members dgm ON dgm.device_group_id=udgp.device_group_id WHERE udgp.user_id=? AND dgm.device_id=b.device_id))`)
		countArgs = append(countArgs, p.UserID, p.UserID)
	}
	var all, established int
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT COUNT(*),COALESCE(SUM(LOWER(b.state)='established'),0) FROM bgp_sessions b WHERE `+strings.Join(countWhere, " AND "), countArgs...).Scan(&all, &established); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset, "counts": gin.H{"total": all, "established": established}})
}

func (s *Server) getBGP(c *gin.Context) {
	record, err := scanBGPSession(s.db.QueryRowContext(c.Request.Context(), bgpSelect+" WHERE b.id=?", c.Param("session_id")))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if _, ok := s.loadScopedDevice(c, record.DeviceID); !ok {
		return
	}
	c.JSON(http.StatusOK, record.bgpSessionDTO)
}

type sensorDTO struct {
	ID, DeviceID, PortID string
	SensorIndex          uint64
	Class, Name, OID     string
	Unit                 string
	Value                *float64
	WarnLimit, CritLimit *float64
	Status               string
	Metadata             map[string]any
	UpdatedAt            string
}

func (s *Server) listDeviceSensors(c *gin.Context) {
	deviceID := c.Param("id")
	if _, ok := s.loadScopedDevice(c, deviceID); !ok {
		return
	}
	page, ok := parseInventoryPage(c, []string{"class", "status", "health"}, map[string]string{
		"class": "s.class", "name": "s.label", "status": "s.status", "value": "s.value_num", "updated_at": "s.updated_at",
	}, "class")
	if !ok {
		return
	}
	where, args := []string{"s.device_id=?"}, []any{deviceID}
	for _, filter := range []struct{ param, column string }{{"class", "s.class"}, {"status", "s.status"}} {
		if value := strings.TrimSpace(c.Query(filter.param)); value != "" && value != "all" {
			if len(value) > 48 {
				fail(c, http.StatusBadRequest, "invalid_filter", filter.param+" is too long")
				return
			}
			where, args = append(where, filter.column+"=?"), append(args, value)
		}
	}
	if health := strings.ToLower(strings.TrimSpace(c.Query("health"))); health != "" && health != "all" {
		healthy := "LOWER(s.status) IN ('','ok','up','normal','1')"
		switch health {
		case "healthy":
			where = append(where, healthy)
		case "problem":
			where = append(where, "NOT "+healthy)
		default:
			fail(c, http.StatusBadRequest, "invalid_filter", "health must be healthy or problem")
			return
		}
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + escapeLike(q) + "%"
		where, args = append(where, "(s.class LIKE ? OR s.label LIKE ? OR s.oid LIKE ? OR s.oid_index LIKE ? OR s.status LIKE ?)"), append(args, like, like, like, like, like)
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM sensors s"+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT s.id,s.device_id,COALESCE(s.port_id,''),s.sensor_index,s.class,s.label,s.oid,s.unit,
		s.value_num,s.warn_limit,s.crit_limit,s.status,COALESCE(s.metadata_json,JSON_OBJECT()),s.updated_at FROM sensors s`+clause+
		fmt.Sprintf(" ORDER BY %s %s,s.id %s LIMIT ? OFFSET ?", page.Sort, page.Order, page.Order), append(args, page.Limit, page.Offset)...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]sensorDTO, 0, page.Limit)
	for rows.Next() {
		var item sensorDTO
		var value, warn, crit sql.NullFloat64
		var metadata json.RawMessage
		var updated time.Time
		if err := rows.Scan(&item.ID, &item.DeviceID, &item.PortID, &item.SensorIndex, &item.Class, &item.Name, &item.OID,
			&item.Unit, &value, &warn, &crit, &item.Status, &metadata, &updated); err != nil {
			writeSQLError(c, err)
			return
		}
		item.Value, item.WarnLimit, item.CritLimit = nullFloat(value), nullFloat(warn), nullFloat(crit)
		item.Metadata = map[string]any{}
		_ = json.Unmarshal(metadata, &item.Metadata)
		item.UpdatedAt = updated.UTC().Format(time.RFC3339Nano)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	var all, problems int
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT COUNT(*),COALESCE(SUM(LOWER(status) NOT IN ('','ok','up','normal','1')),0) FROM sensors WHERE device_id=?`, deviceID).Scan(&all, &problems); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset, "counts": gin.H{"total": all, "problems": problems}})
}

func nullFloat(value sql.NullFloat64) *float64 {
	if !value.Valid {
		return nil
	}
	return &value.Float64
}

type physicalEntityDTO struct {
	Index                                uint64
	Name, Description, Class, VendorType string
	ContainedIn                          uint64
	ParentRelPos                         int
	HardwareRevision, FirmwareRevision   string
	SoftwareRevision, SerialNumber       string
	ManufacturerName, ModelName, Alias   string
	AssetID                              string
	IsFRU                                bool
}

func (s *Server) listDeviceInventory(c *gin.Context) {
	deviceID := c.Param("id")
	if _, ok := s.loadScopedDevice(c, deviceID); !ok {
		return
	}
	page, ok := parseInventoryPage(c, []string{"class", "fru"}, map[string]string{
		"entity_index": "pe.entity_index+0", "name": "pe.name", "class": "pe.class", "model": "pe.model_name",
		"serial": "pe.serial", "manufacturer": "pe.manufacturer_name", "updated_at": "pe.updated_at",
	}, "entity_index")
	if !ok {
		return
	}
	where, args := []string{"pe.device_id=?"}, []any{deviceID}
	if class := strings.TrimSpace(c.Query("class")); class != "" {
		if len(class) > 48 {
			fail(c, http.StatusBadRequest, "invalid_filter", "class is too long")
			return
		}
		where, args = append(where, "pe.class=?"), append(args, class)
	}
	if raw := strings.TrimSpace(c.Query("fru")); raw != "" && raw != "all" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			fail(c, http.StatusBadRequest, "invalid_filter", "fru must be true or false")
			return
		}
		where, args = append(where, "pe.is_fru=?"), append(args, value)
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + escapeLike(q) + "%"
		where = append(where, `(pe.entity_index LIKE ? OR pe.name LIKE ? OR pe.description LIKE ? OR pe.class LIKE ? OR pe.serial LIKE ?
			OR pe.model_name LIKE ? OR pe.manufacturer_name LIKE ? OR pe.asset_id LIKE ?)`)
		for range 8 {
			args = append(args, like)
		}
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM physical_entities pe"+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT pe.entity_index,pe.name,COALESCE(pe.description,''),pe.class,pe.vendor_type,
		pe.contained_in,pe.parent_rel_pos,pe.hardware_revision,pe.firmware_revision,pe.software_revision,pe.serial,
		pe.manufacturer_name,pe.model_name,pe.alias,pe.asset_id,pe.is_fru FROM physical_entities pe`+clause+
		fmt.Sprintf(" ORDER BY %s %s,pe.id %s LIMIT ? OFFSET ?", page.Sort, page.Order, page.Order), append(args, page.Limit, page.Offset)...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]physicalEntityDTO, 0, page.Limit)
	for rows.Next() {
		var item physicalEntityDTO
		var index string
		if err := rows.Scan(&index, &item.Name, &item.Description, &item.Class, &item.VendorType, &item.ContainedIn,
			&item.ParentRelPos, &item.HardwareRevision, &item.FirmwareRevision, &item.SoftwareRevision,
			&item.SerialNumber, &item.ManufacturerName, &item.ModelName, &item.Alias, &item.AssetID, &item.IsFRU); err != nil {
			writeSQLError(c, err)
			return
		}
		item.Index, _ = strconv.ParseUint(index, 10, 64)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

type deviceVLANDTO struct {
	VLANID uint32
	Name   string
	Status string
}

func (s *Server) listDeviceVLANs(c *gin.Context) {
	deviceID := c.Param("id")
	if _, ok := s.loadScopedDevice(c, deviceID); !ok {
		return
	}
	page, ok := parseInventoryPage(c, []string{"status"}, map[string]string{
		"vlan_id": "v.vlan_id", "name": "v.name", "status": "v.status", "updated_at": "v.updated_at",
	}, "vlan_id")
	if !ok {
		return
	}
	where, args := []string{"v.device_id=?"}, []any{deviceID}
	if status := strings.TrimSpace(c.Query("status")); status != "" && status != "all" {
		if len(status) > 32 {
			fail(c, http.StatusBadRequest, "invalid_filter", "status is too long")
			return
		}
		where, args = append(where, "v.status=?"), append(args, status)
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + escapeLike(q) + "%"
		where, args = append(where, "(CAST(v.vlan_id AS CHAR) LIKE ? OR v.name LIKE ? OR v.status LIKE ?)"), append(args, like, like, like)
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM vlans v"+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT v.vlan_id,v.name,v.status FROM vlans v`+clause+fmt.Sprintf(" ORDER BY %s %s,v.id %s LIMIT ? OFFSET ?", page.Sort, page.Order, page.Order), append(args, page.Limit, page.Offset)...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]deviceVLANDTO, 0, page.Limit)
	for rows.Next() {
		var item deviceVLANDTO
		if err := rows.Scan(&item.VLANID, &item.Name, &item.Status); err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

type deviceLAGDTO struct {
	AggregateIndex uint64
	MACAddress     string
	Mode           string
}

func (s *Server) listDeviceLAGs(c *gin.Context) {
	deviceID := c.Param("id")
	if _, ok := s.loadScopedDevice(c, deviceID); !ok {
		return
	}
	page, ok := parseInventoryPage(c, []string{"mode"}, map[string]string{
		"aggregate_index": "l.lag_if_index", "mac_address": "l.mac_address", "mode": "l.mode", "updated_at": "l.updated_at",
	}, "aggregate_index")
	if !ok {
		return
	}
	where, args := []string{"l.device_id=?"}, []any{deviceID}
	if mode := strings.TrimSpace(c.Query("mode")); mode != "" && mode != "all" {
		if len(mode) > 32 {
			fail(c, http.StatusBadRequest, "invalid_filter", "mode is too long")
			return
		}
		where, args = append(where, "l.mode=?"), append(args, mode)
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + escapeLike(q) + "%"
		where, args = append(where, "(CAST(l.lag_if_index AS CHAR) LIKE ? OR l.name LIKE ? OR l.mac_address LIKE ? OR l.mode LIKE ?)"), append(args, like, like, like, like)
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM lag_groups l"+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT l.lag_if_index,l.mac_address,l.mode FROM lag_groups l`+clause+fmt.Sprintf(" ORDER BY %s %s,l.id %s LIMIT ? OFFSET ?", page.Sort, page.Order, page.Order), append(args, page.Limit, page.Offset)...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]deviceLAGDTO, 0, page.Limit)
	for rows.Next() {
		var item deviceLAGDTO
		if err := rows.Scan(&item.AggregateIndex, &item.MACAddress, &item.Mode); err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

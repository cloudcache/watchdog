package server

import (
	"encoding/json"
	"time"

	watchdogdomain "github.com/cloudcache/watchdog/internal/watchdog"
	"github.com/gin-gonic/gin"
)

// deviceGraphOverview preserves the historical dashboard-schema endpoint while
// reading inventory from the single MySQL device root. The returned queries are
// resolved by the canonical ClickHouse-backed /metrics API.
func (s *Server) deviceGraphOverview(c *gin.Context) {
	deviceID := c.Param("id")
	if !s.requireDeviceAccess(c, deviceID) {
		return
	}
	device, err := s.readDevice(c, deviceID)
	if err != nil {
		writeSQLError(c, err)
		return
	}

	allPorts, ok := s.portScope(c, deviceID, "")
	if !ok {
		return
	}
	query := portSelect + " WHERE p.device_id=?"
	args := []any{deviceID}
	if !allPorts {
		query += " AND EXISTS (SELECT 1 FROM user_port_permissions upp WHERE upp.user_id=? AND upp.port_id=p.id)"
		args = append(args, currentPrincipal(c).UserID)
	}
	query += " ORDER BY p.if_index,p.id"
	rows, err := s.db.QueryContext(c.Request.Context(), query, args...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	ports := make([]watchdogdomain.NetworkPort, 0)
	for rows.Next() {
		port, err := scanPort(rows)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		ports = append(ports, graphNetworkPort(port))
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}

	bgpSessions := make([]watchdogdomain.BGPSession, 0)
	var hasBGP bool
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT EXISTS(SELECT 1 FROM bgp_sessions WHERE device_id=?)", deviceID).Scan(&hasBGP); err != nil {
		writeSQLError(c, err)
		return
	}
	if hasBGP {
		bgpSessions = append(bgpSessions, watchdogdomain.BGPSession{})
	}

	sensors := make([]watchdogdomain.NetworkDeviceSensor, 0)
	sensorRows, err := s.db.QueryContext(c.Request.Context(), "SELECT class,label,unit FROM sensors WHERE device_id=?", deviceID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer sensorRows.Close()
	for sensorRows.Next() {
		var sensor watchdogdomain.NetworkDeviceSensor
		if err := sensorRows.Scan(&sensor.Class, &sensor.Name, &sensor.Unit); err != nil {
			writeSQLError(c, err)
			return
		}
		sensors = append(sensors, sensor)
	}
	if err := sensorRows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}

	c.JSON(200, watchdogdomain.NewNetworkDeviceOverviewDashboard(graphNetworkDevice(device), ports, bgpSessions, sensors))
}

func (s *Server) portGraphOverview(c *gin.Context) {
	port, err := s.readPort(c, c.Param("port_id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if _, ok := s.portScope(c, port.DeviceID, port.ID); !ok {
		return
	}
	c.JSON(200, watchdogdomain.NewNetworkPortOverviewDashboard(graphNetworkPort(port)))
}

func graphNetworkDevice(value deviceRecord) watchdogdomain.NetworkDevice {
	return watchdogdomain.NetworkDevice{
		ID:            watchdogdomain.ID(value.ID),
		Vendor:        value.Vendor,
		Model:         value.Model,
		Platform:      value.Platform,
		OSName:        value.OS,
		OSVersion:     value.OSVersion,
		SysObjectID:   value.SysObjectID,
		SysName:       value.SysName,
		SysDescr:      value.SysDescr,
		SysLocation:   value.SysLocation,
		Uptime:        time.Duration(value.UptimeSeconds) * time.Second,
		SNMPProfileID: watchdogdomain.ID(value.SNMPProfileID.String),
		SNMPPort:      uint16(value.SNMPPort.Int64),
		UpdatedAt:     value.UpdatedAt,
	}
}

func graphNetworkPort(value portRecord) watchdogdomain.NetworkPort {
	metadata := map[string]string{}
	var raw map[string]any
	if json.Unmarshal(value.Metadata, &raw) == nil {
		for key, item := range raw {
			if text, ok := item.(string); ok {
				metadata[key] = text
			}
		}
	}
	if value.IfType != "" {
		metadata["if_type"] = value.IfType
	}
	speed := uint64(0)
	if value.IfSpeed.Valid && value.IfSpeed.Int64 > 0 {
		speed = uint64(value.IfSpeed.Int64)
	}
	return watchdogdomain.NetworkPort{
		ID:          watchdogdomain.ID(value.ID),
		DeviceID:    watchdogdomain.ID(value.DeviceID),
		IfIndex:     value.IfIndex,
		IfName:      value.IfName,
		IfAlias:     value.IfAlias,
		IfDescr:     value.IfDescr,
		AdminStatus: value.AdminStatus,
		OperStatus:  value.OperStatus,
		SpeedBps:    speed,
		Metadata:    metadata,
		UpdatedAt:   value.UpdatedAt,
	}
}

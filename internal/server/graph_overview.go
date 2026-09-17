package server

import (
	"encoding/json"

	"github.com/cloudcache/watchdog/internal/snmpdomain"
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
	ports := make([]snmpdomain.GraphPort, 0)
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

	var hasBGP bool
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT EXISTS(SELECT 1 FROM bgp_sessions WHERE device_id=?)", deviceID).Scan(&hasBGP); err != nil {
		writeSQLError(c, err)
		return
	}
	sensors := make([]snmpdomain.GraphSensor, 0)
	sensorRows, err := s.db.QueryContext(c.Request.Context(), "SELECT class,label,unit FROM sensors WHERE device_id=?", deviceID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer sensorRows.Close()
	for sensorRows.Next() {
		var sensor snmpdomain.GraphSensor
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

	c.JSON(200, snmpdomain.NewDeviceOverviewDashboard(graphNetworkDevice(device), ports, hasBGP, sensors))
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
	c.JSON(200, snmpdomain.NewPortOverviewDashboard(graphNetworkPort(port)))
}

func graphNetworkDevice(value deviceRecord) snmpdomain.GraphDevice {
	return snmpdomain.GraphDevice{
		ID:      value.ID,
		Model:   value.Model,
		SysName: value.SysName,
	}
}

func graphNetworkPort(value portRecord) snmpdomain.GraphPort {
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
	return snmpdomain.GraphPort{
		ID:          value.ID,
		IfName:      value.IfName,
		IfDescr:     value.IfDescr,
		AdminStatus: value.AdminStatus,
		OperStatus:  value.OperStatus,
		Disabled:    value.Disabled,
		Metadata:    metadata,
	}
}

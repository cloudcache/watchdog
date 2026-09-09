package server

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const maxAPIRequestBytes int64 = 8 << 20

// newRouter wires the KISS API surface by domain (docs/watchdog-kiss-architecture.md §7).
// Auth/RBAC (session, profile, users, roles, permissions) is implemented; the remaining
// domains are 501 scaffolds filled by their work packages (KISS-02 device, KISS-07 billing,
// KISS-04 agent, KISS-L log/alert). No legacy management store and no tenant.
func (s *Server) newRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery(), requestID(), requestBodyLimit(maxAPIRequestBytes), s.cors())

	api := r.Group("/api/v1")

	// --- public ---
	api.GET("/health", s.health)
	api.GET("/install-status", s.installStatus)
	api.POST("/session/login", s.login)
	api.POST("/session/forgot", s.forgotPassword)
	api.POST("/session/reset", s.resetPassword)

	// --- authenticated (session cookie + CSRF on mutations) ---
	auth := api.Group("")
	auth.Use(s.requireAuth, s.requireCSRF)

	auth.POST("/session/logout", s.logout)
	auth.GET("/session/current", s.current)

	me := auth.Group("/me")
	me.GET("", s.current)
	me.PATCH("", s.updateProfile)
	me.POST("/password", s.changeOwnPassword)

	// RBAC administration (action-gated by fixed abilities)
	users := auth.Group("/users")
	users.GET("", s.requirePermission("user.view"), s.listUsers)
	users.POST("", s.requirePermission("user.create"), s.createUser)
	users.GET("/:id", s.requirePermission("user.view"), s.getUser)
	users.PATCH("/:id", s.requirePermission("user.update"), s.updateUser)
	users.DELETE("/:id", s.requirePermission("user.delete"), s.deleteUser)
	users.POST("/:id/password", s.requirePermission("user.manage"), s.adminResetPassword)

	roles := auth.Group("/roles")
	roles.GET("", s.requirePermission("role.view"), s.listRoles)
	roles.POST("", s.requirePermission("role.create"), s.createRole)
	roles.GET("/:id", s.requirePermission("role.view"), s.getRole)
	roles.PATCH("/:id", s.requirePermission("role.update"), s.updateRole)
	roles.DELETE("/:id", s.requirePermission("role.delete"), s.deleteRole)

	auth.GET("/permissions", s.requirePermission("role.view"), s.listPermissions)

	// Inventory: a single device row is the root. The network/target aliases
	// below serve the current UI while preserving that one identity in MySQL.
	devices := auth.Group("/devices")
	devices.GET("", s.requirePermission("device.view"), s.listDevices)
	devices.POST("", s.requirePermission("device.create"), s.createDevice)
	devices.GET("/:id", s.requirePermission("device.view"), s.getDevice)
	devices.PATCH("/:id", s.requirePermission("device.update"), s.updateDevice)
	devices.DELETE("/:id", s.requirePermission("device.delete"), s.deleteDevice)
	devices.GET("/:id/delete-preview", s.requirePermission("device.delete"), s.deviceDeletePreview)
	groups := auth.Group("/device-groups")
	groups.GET("", s.requirePermission("device.view"), s.listDeviceGroups)
	groups.POST("", s.requirePermission("device.update"), s.createDeviceGroup)
	groups.GET("/:id", s.requirePermission("device.view"), s.getDeviceGroup)
	groups.PATCH("/:id", s.requirePermission("device.update"), s.updateDeviceGroup)
	groups.DELETE("/:id", s.requirePermission("device.update"), s.deleteDeviceGroup)
	groups.GET("/:id/delete-preview", s.requirePermission("device.update"), s.deviceGroupDeletePreview)
	groups.GET("/:id/members", s.requirePermission("device.view"), s.listDeviceGroupMembers)
	groups.PUT("/:id/members", s.requirePermission("device.update"), s.replaceDeviceGroupMembers)
	groups.PUT("/:id/members/:device_id", s.requirePermission("device.update"), s.addDeviceGroupMember)
	groups.DELETE("/:id/members/:device_id", s.requirePermission("device.update"), s.deleteDeviceGroupMember)
	groups.POST("/:id/refresh", s.requirePermission("device.update"), s.refreshDeviceGroup)
	locations := auth.Group("/locations")
	locations.GET("", s.requirePermission("device.view"), s.listLocations)
	locations.POST("", s.requirePermission("device.update"), s.createLocation)
	locations.GET("/:id", s.requirePermission("device.view"), s.getLocation)
	locations.PATCH("/:id", s.requirePermission("device.update"), s.updateLocation)
	locations.DELETE("/:id", s.requirePermission("device.update"), s.deleteLocation)
	locations.GET("/:id/delete-preview", s.requirePermission("device.update"), s.locationDeletePreview)
	profiles := auth.Group("/snmp-profiles")
	profiles.GET("", s.requirePermission("device.view"), s.listSNMPProfiles)
	profiles.POST("", s.requirePermission("device.update"), s.createSNMPProfile)
	profiles.GET("/:id", s.requirePermission("device.update"), s.getSNMPProfile)
	profiles.PATCH("/:id", s.requirePermission("device.update"), s.updateSNMPProfile)
	profiles.DELETE("/:id", s.requirePermission("device.update"), s.deleteSNMPProfile)
	dev := auth.Group("/devices/:id")
	dev.GET("/ports", s.requirePermission("port.view"), s.listDevicePorts)
	dev.GET("/addresses", s.requirePermission("port.view"), s.listDeviceAddresses)
	dev.GET("/bgp", s.requirePermission("device.view"), s.listDeviceBGP)
	dev.GET("/sensors", s.requirePermission("device.view"), s.listDeviceSensors)
	dev.GET("/inventory", s.requirePermission("device.view"), s.listDeviceInventory)
	dev.GET("/vlans", s.requirePermission("device.view"), s.listDeviceVLANs)
	dev.GET("/lags", s.requirePermission("device.view"), s.listDeviceLAGs)
	dev.POST("/snmp/discover", s.requirePermission("device.discover"), s.discoverDeviceSNMP)
	metrics := auth.Group("/metrics")
	metrics.GET("/catalog", s.requirePermission("device.view"), s.metricCatalog)
	metrics.GET("/query", s.requirePermission("device.view"), s.queryMetrics)
	metrics.GET("/range", s.requirePermission("device.view"), s.queryMetrics)
	metrics.GET("/realtime", s.requirePermission("device.view"), s.queryMetrics)
	metrics.GET("/aggregate", s.requirePermission("device.view"), s.aggregateMetrics)
	metrics.POST("/exports", s.requirePermission("device.view"), s.createSNMPExport)
	dev.GET("/events", s.todo)
	ports := auth.Group("/ports")
	ports.GET("/:port_id", s.requirePermission("port.view"), s.getPort)
	ports.PATCH("/:port_id", s.requirePermission("port.update"), s.updatePort)
	ports.DELETE("/:port_id", s.requirePermission("port.update"), s.deletePort)
	ports.GET("/:port_id/delete-preview", s.requirePermission("port.update"), s.portDeletePreview)
	bgp := auth.Group("/bgp")
	bgp.GET("", s.requirePermission("device.view"), s.listAllBGP)
	bgp.GET("/:session_id", s.requirePermission("device.view"), s.getBGP)
	s.registerBillingRoutes(auth)

	// Agent registration and heartbeats use agent credentials, not a user
	// session. Administrative registry operations remain RBAC protected.
	api.POST("/agents/register", s.enrollAgent)
	api.POST("/agents/:id/heartbeat", s.agentHeartbeat)
	api.POST("/agents/:id/status", s.recordAgentStatus)
	api.POST("/agents/:id/errors", s.recordAgentStatus)
	api.GET("/agents/:id/plan", s.fetchAgentPlan)
	api.POST("/agents/:id/plan-acks", s.acknowledgeAgentPlan)
	agents := auth.Group("/agents")
	agents.GET("", s.requirePermission("agent.view"), s.listAgents)
	agents.POST("", s.requirePermission("agent.manage"), s.createAgent)
	agents.GET("/plan-public-key", s.requirePermission("agent.view"), s.getAgentPlanPublicKey)
	agents.POST("/plan-rollouts", s.requirePermission("agent.manage"), s.enqueueAgentPlanRollout)
	agents.GET("/plan-rollouts/:job_id", s.requirePermission("agent.view"), s.getAgentPlanRollout)
	agents.POST("/plan-rollouts/:job_id/cancel", s.requirePermission("agent.manage"), s.cancelAgentPlanRollout)
	agents.POST("/enrollment-tokens", s.requirePermission("agent.manage"), s.createEnrollmentToken)
	agents.GET("/:id", s.requirePermission("agent.view"), s.getAgent)
	agents.PATCH("/:id", s.requirePermission("agent.manage"), s.updateAgent)
	agents.DELETE("/:id", s.requirePermission("agent.manage"), s.deleteAgent)
	agents.GET("/:id/runs", s.requirePermission("agent.view"), s.listAgentRuns)
	agents.GET("/:id/plans", s.requirePermission("agent.view"), s.listAgentPlans)
	agents.POST("/:id/plans", s.requirePermission("agent.manage"), s.createAgentPlan)
	agents.GET("/:id/plans/:version", s.requirePermission("agent.view"), s.getAgentPlan)
	agents.POST("/:id/credentials/rotate", s.requirePermission("agent.manage"), s.rotateAgentCredential)
	agents.POST("/:id/revoke", s.requirePermission("agent.manage"), s.revokeAgent)

	// Temporary URL/DTO aliases for the existing UI. They call the canonical
	// repositories above and never touch the removed targets/target_agents tables.
	legacyNetwork := auth.Group("/network/devices")
	legacyNetwork.GET("", s.requirePermission("device.view"), s.listNetworkDevices)
	legacyNetwork.POST("", s.requirePermission("device.create"), s.createDevice)
	legacyNetwork.GET("/summary", s.requirePermission("device.view"), s.listDeviceSummaries)
	legacyNetwork.GET("/:id", s.requirePermission("device.view"), s.getDevice)
	legacyNetwork.PATCH("/:id", s.requirePermission("device.update"), s.updateDevice)
	legacyNetwork.DELETE("/:id", s.requirePermission("device.delete"), s.deleteDevice)
	legacyNetwork.GET("/:id/delete-preview", s.requirePermission("device.delete"), s.deviceDeletePreview)
	legacyNetwork.PATCH("/:id/snmp", s.requirePermission("device.update"), s.patchDeviceSNMP)
	legacyNetwork.GET("/:id/ports", s.requirePermission("port.view"), s.listDevicePorts)
	legacyNetwork.GET("/:id/addresses", s.requirePermission("port.view"), s.listDeviceAddresses)
	legacyNetwork.GET("/:id/bgp", s.requirePermission("device.view"), s.listDeviceBGP)
	legacyNetwork.GET("/:id/sensors", s.requirePermission("device.view"), s.listDeviceSensors)
	legacyNetwork.GET("/:id/inventory", s.requirePermission("device.view"), s.listDeviceInventory)
	legacyNetwork.GET("/:id/vlans", s.requirePermission("device.view"), s.listDeviceVLANs)
	legacyNetwork.GET("/:id/lags", s.requirePermission("device.view"), s.listDeviceLAGs)
	legacyNetwork.POST("/:id/snmp/discover", s.requirePermission("device.discover"), s.discoverDeviceSNMP)
	legacyPorts := auth.Group("/network/ports")
	legacyPorts.GET("/:port_id", s.requirePermission("port.view"), s.getPort)
	legacyPorts.PATCH("/:port_id", s.requirePermission("port.update"), s.updatePort)
	legacyPorts.DELETE("/:port_id", s.requirePermission("port.update"), s.deletePort)
	legacyPorts.GET("/:port_id/delete-preview", s.requirePermission("port.update"), s.portDeletePreview)
	legacyBGP := auth.Group("/network/bgp")
	legacyBGP.GET("", s.requirePermission("device.view"), s.listAllBGP)
	legacyBGP.GET("/:session_id", s.requirePermission("device.view"), s.getBGP)
	targets := auth.Group("/targets")
	targets.GET("", s.requirePermission("device.view"), s.listTargets)
	targets.POST("", s.requirePermission("device.create"), s.createTarget)
	targets.GET("/:id", s.requirePermission("device.view"), s.getTarget)
	targets.PATCH("/:id", s.requirePermission("device.update"), s.updateTarget)
	targets.DELETE("/:id", s.requirePermission("device.delete"), s.deleteDevice)
	targets.GET("/:id/delete-preview", s.requirePermission("device.delete"), s.deviceDeletePreview)

	// A Flow device is an inventory device with one or more exporter bindings.
	// Both URLs expose the same binding resource; /flow/devices is the UI-facing
	// compatibility name while /flow/exporter-bindings states the data model.
	for _, path := range []string{"/flow/devices", "/flow/exporter-bindings"} {
		flowDevices := auth.Group(path)
		flowDevices.GET("", s.requirePermission("flow.device.view"), s.listFlowExporters)
		flowDevices.POST("", s.requirePermission("flow.device.manage"), s.createFlowExporter)
		flowDevices.GET("/:id", s.requirePermission("flow.device.view"), s.getFlowExporter)
		flowDevices.PATCH("/:id", s.requirePermission("flow.device.manage"), s.updateFlowExporter)
		flowDevices.DELETE("/:id", s.requirePermission("flow.device.manage"), s.deleteFlowExporter)
	}
	snmpProfiles := auth.Group("/snmp/profiles")
	snmpProfiles.GET("", s.requirePermission("device.view"), s.listSNMPProfiles)
	snmpProfiles.POST("", s.requirePermission("device.update"), s.createSNMPProfile)
	snmpProfiles.GET("/:id", s.requirePermission("device.update"), s.getSNMPProfile)
	snmpProfiles.PATCH("/:id", s.requirePermission("device.update"), s.updateSNMPProfile)
	snmpProfiles.DELETE("/:id", s.requirePermission("device.update"), s.deleteSNMPProfile)
	// KISS-05 geo/address library (owned slice): editable CRUD/list + source imports,
	// plus the de-tenanted publication lifecycle (preview/publish/versions/lifecycle).
	s.registerAddressRoutes(auth)
	s.registerAddressImportRoutes(auth)
	s.registerAddressDimensionRoutes(auth)

	// Per-user resource-grant management (device/port/billing access rights).
	s.registerAccessRoutes(auth)

	auth.GET("/jobs", s.todo)
	auth.GET("/audit", s.todo)
	exports := auth.Group("/exports")
	exports.GET("", s.requirePermission("device.view"), s.listSNMPExports)
	exports.POST("", s.requirePermission("device.view"), s.createSNMPExport)
	exports.GET("/:id", s.requirePermission("device.view"), s.getSNMPExport)
	exports.GET("/:id/download", s.requirePermission("device.view"), s.downloadSNMPExport)
	exports.POST("/:id/cancel", s.requirePermission("device.view"), s.cancelSNMPExport)

	return r
}

func requestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := strings.TrimSpace(c.GetHeader("X-Request-ID"))
		if id == "" || len(id) > 128 {
			id = newID()
		}
		c.Header("X-Request-ID", id)
		c.Set("request_id", id)
		c.Next()
	}
}

func requestBodyLimit(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		// The address-database upload streams tens of MB to disk; it enforces its
		// own cfg.Address.MaxUploadBytes ceiling, so the small JSON-body cap is skipped.
		if c.Request.Method == http.MethodPost && c.FullPath() == "/api/v1/address-imports" {
			c.Next()
			return
		}
		if c.Request.ContentLength > maxBytes {
			fail(c, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds the configured limit")
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
		c.Next()
	}
}

func todoCRUD(g *gin.RouterGroup, s *Server) {
	g.GET("", s.todo)
	g.POST("", s.todo)
	g.GET("/:id", s.todo)
	g.PATCH("/:id", s.todo)
	g.DELETE("/:id", s.todo)
}

func (s *Server) health(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	dbOK := s.db.PingContext(ctx) == nil
	chOK := s.snmpMetrics != nil && s.snmpMetrics.Ready(ctx) == nil
	status := http.StatusOK
	if !dbOK || !chOK {
		status = http.StatusServiceUnavailable
	}
	c.JSON(status, gin.H{"status": ternary(dbOK && chOK, "ok", "degraded"), "mysql": dbOK, "clickhouse": chOK})
}

func (s *Server) installStatus(c *gin.Context) {
	st, err := GetInstallStatus(c.Request.Context(), s.db)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": gin.H{"code": "internal", "message": err.Error()}})
		return
	}
	c.JSON(http.StatusOK, st)
}

func (s *Server) todo(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{
		"error": gin.H{"code": "not_implemented", "message": "endpoint scaffolded; implemented in its KISS work package", "path": c.FullPath()},
	})
}

func (s *Server) cors() gin.HandlerFunc {
	allowed := make(map[string]bool, len(s.cfg.Server.Origins))
	for _, o := range s.cfg.Server.Origins {
		allowed[o] = true
	}
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if origin != "" && allowed[origin] {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Vary", "Origin")
			c.Header("Access-Control-Allow-Credentials", "true")
			c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			c.Header("Access-Control-Allow-Headers", "Authorization, Content-Type, If-Match, X-CSRF-Token, X-Watchdog-Agent-Token, X-Request-ID")
			c.Header("Access-Control-Expose-Headers", "ETag, X-Request-ID")
		}
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

func ternary(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

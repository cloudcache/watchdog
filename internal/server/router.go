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
	todoCRUD(auth.Group("/device-groups"), s)
	todoCRUD(auth.Group("/locations"), s)
	profiles := auth.Group("/snmp-profiles")
	profiles.GET("", s.requirePermission("device.view"), s.listSNMPProfiles)
	profiles.POST("", s.requirePermission("device.update"), s.createSNMPProfile)
	profiles.GET("/:id", s.requirePermission("device.update"), s.getSNMPProfile)
	profiles.PATCH("/:id", s.requirePermission("device.update"), s.updateSNMPProfile)
	profiles.DELETE("/:id", s.requirePermission("device.update"), s.deleteSNMPProfile)
	dev := auth.Group("/devices/:id")
	dev.GET("/ports", s.todo)
	dev.GET("/addresses", s.todo)
	dev.GET("/bgp", s.todo)
	dev.GET("/inventory", s.todo)
	dev.GET("/events", s.todo)
	todoCRUD(auth.Group("/billing/accounts"), s)
	todoCRUD(auth.Group("/billing/parties"), s)

	// Agent registration and heartbeats use agent credentials, not a user
	// session. Administrative registry operations remain RBAC protected.
	api.POST("/agents/register", s.enrollAgent)
	api.POST("/agents/:id/heartbeat", s.agentHeartbeat)
	api.POST("/agents/:id/status", s.recordAgentStatus)
	api.POST("/agents/:id/errors", s.recordAgentStatus)
	agents := auth.Group("/agents")
	agents.GET("", s.requirePermission("agent.view"), s.listAgents)
	agents.POST("", s.requirePermission("agent.manage"), s.createAgent)
	agents.POST("/enrollment-tokens", s.requirePermission("agent.manage"), s.createEnrollmentToken)
	agents.GET("/:id", s.requirePermission("agent.view"), s.getAgent)
	agents.PATCH("/:id", s.requirePermission("agent.manage"), s.updateAgent)
	agents.DELETE("/:id", s.requirePermission("agent.manage"), s.deleteAgent)
	agents.GET("/:id/runs", s.requirePermission("agent.view"), s.listAgentRuns)

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
	legacyAgents := auth.Group("/agent-registry")
	legacyAgents.GET("", s.requirePermission("agent.view"), s.listAgents)
	legacyAgents.POST("", s.requirePermission("agent.manage"), s.createAgent)
	legacyAgents.GET("/:id", s.requirePermission("agent.view"), s.getAgent)
	legacyAgents.PATCH("/:id", s.requirePermission("agent.manage"), s.updateAgent)
	legacyAgents.DELETE("/:id", s.requirePermission("agent.manage"), s.deleteAgent)
	legacyAgents.GET("/:id/runs", s.requirePermission("agent.view"), s.listAgentRuns)
	todoCRUD(auth.Group("/alerts/channels"), s)
	todoCRUD(auth.Group("/alerts/quiet-hours"), s)

	// KISS-05 geo/address library (owned slice): editable CRUD/list now; import+publish next.
	s.registerAddressRoutes(auth)

	auth.GET("/jobs", s.todo)
	auth.GET("/audit", s.todo)
	auth.GET("/exports", s.todo)

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
	status := http.StatusOK
	if !dbOK {
		status = http.StatusServiceUnavailable
	}
	c.JSON(status, gin.H{"status": ternary(dbOK, "ok", "degraded"), "mysql": dbOK})
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

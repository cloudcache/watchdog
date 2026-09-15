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
	api.POST("/install", s.installWatchdog)
	installedAPI := api.Group("")
	installedAPI.Use(s.requireInstalled)
	installedAPI.POST("/session/login", s.login)
	installedAPI.POST("/session/forgot", s.forgotPassword)
	installedAPI.POST("/session/reset", s.resetPassword)
	flowWorkers := installedAPI.Group("/flow-workers/:id")
	flowWorkers.GET("/trust-bundle", s.fetchFlowWorkerTrustBundle)
	flowWorkers.GET("/enrichment-publications", s.fetchFlowWorkerPublications)
	flowWorkers.GET("/enrichment-publications/:publication_id/objects/:kind", s.fetchFlowWorkerObject)
	flowWorkers.POST("/enrichment-publications/:publication_id/ack", s.acknowledgeFlowWorkerPublication)

	// --- authenticated (session cookie + CSRF on mutations) ---
	auth := installedAPI.Group("")
	auth.Use(s.requireAuth, s.requireCSRF)

	auth.POST("/session/logout", s.logout)
	auth.GET("/session/current", s.current)

	me := auth.Group("/me")
	me.GET("", s.current)
	me.PATCH("", s.updateProfile)
	me.POST("/password", s.changeOwnPassword)
	me.GET("/preferences", s.getUserPreferences)
	me.PUT("/preferences", s.putUserPreferences)

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
	metrics.GET("/vmquery", s.requirePermission("device.view"), s.vmQueryMetrics)
	metrics.POST("/exports", s.requirePermission("device.view"), s.createSNMPExport)
	graphs := auth.Group("/graph")
	graphs.GET("/devices/:id/overview", s.requirePermission("device.view"), s.deviceGraphOverview)
	graphs.GET("/ports/:port_id/overview", s.requirePermission("port.view"), s.portGraphOverview)
	dev.GET("/events", s.requirePermission("device.view"), s.listDeviceEvents)
	dev.GET("/events/facets", s.requirePermission("device.view"), s.listDeviceEventFacets)
	ports := auth.Group("/ports")
	ports.GET("/:port_id", s.requirePermission("port.view"), s.getPort)
	ports.PATCH("/:port_id", s.requirePermission("port.update"), s.updatePort)
	ports.DELETE("/:port_id", s.requirePermission("port.update"), s.deletePort)
	ports.GET("/:port_id/delete-preview", s.requirePermission("port.update"), s.portDeletePreview)
	bgp := auth.Group("/bgp")
	bgp.GET("", s.requirePermission("device.view"), s.listAllBGP)
	bgp.GET("/:session_id", s.requirePermission("device.view"), s.getBGP)
	aggregateGraphs := auth.Group("/aggregate-graphs")
	aggregateGraphs.GET("", s.requirePermission("device.view"), s.listAggregateGraphs)
	aggregateGraphs.POST("", s.requirePermission("device.update"), s.createAggregateGraph)
	aggregateGraphs.GET("/:id", s.requirePermission("device.view"), s.getAggregateGraph)
	aggregateGraphs.PATCH("/:id", s.requirePermission("device.update"), s.updateAggregateGraph)
	aggregateGraphs.DELETE("/:id", s.requirePermission("device.update"), s.deleteAggregateGraph)
	aggregateGraphs.GET("/:id/items", s.requirePermission("device.view"), s.listAggregateGraphItems)
	aggregateGraphs.PUT("/:id/items", s.requirePermission("device.update"), s.replaceAggregateGraphItems)
	aggregateGraphs.GET("/:id/ports", s.requirePermission("device.view"), s.listAggregateGraphPorts)
	aggregateGraphs.PUT("/:id/ports", s.requirePermission("device.update"), s.replaceAggregateGraphPorts)
	aggregateGraphs.GET("/:id/series", s.requirePermission("device.view"), s.aggregateGraphSeries)
	aggregateGraphs.GET("/:id/data", s.requirePermission("device.view"), s.aggregateGraphData)
	aggregateGraphs.GET("/:id/summary", s.requirePermission("device.view"), s.aggregateGraphSummary)
	dashboards := auth.Group("/dashboards")
	dashboards.GET("", s.requirePermission("device.view"), s.listDashboards)
	dashboards.GET("/graph-options", s.requirePermission("device.view"), s.listDashboardGraphOptions)
	dashboards.POST("", s.requirePermission("device.update"), s.createDashboard)
	dashboards.POST("/actions/preview", s.requirePermission("device.view"), s.previewDashboardDraft)
	dashboards.GET("/:id", s.requirePermission("device.view"), s.getDashboard)
	dashboards.GET("/:id/preview", s.requirePermission("device.view"), s.previewDashboard)
	dashboards.PATCH("/:id", s.requirePermission("device.update"), s.updateDashboard)
	dashboards.DELETE("/:id", s.requirePermission("device.update"), s.deleteDashboard)
	s.registerBillingRoutes(auth)

	// Agent registration and heartbeats use agent credentials, not a user
	// session. Administrative registry operations remain RBAC protected.
	installedAPI.POST("/agents/register", s.enrollAgent)
	installedAPI.POST("/agents/:id/heartbeat", s.agentHeartbeat)
	installedAPI.POST("/agents/:id/status", s.recordAgentStatus)
	installedAPI.POST("/agents/:id/errors", s.recordAgentStatus)
	installedAPI.GET("/agents/:id/plan", s.fetchAgentPlan)
	installedAPI.POST("/agents/:id/plan-acks", s.acknowledgeAgentPlan)
	// The UDP trap listener forwards with its SNMP agent credential. The same
	// historical endpoint also accepts an authorized user session for diagnostics.
	installedAPI.POST("/snmp/traps", s.authenticateSNMPTrapCaller, s.receiveSNMPTrap)
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

	// Historical public API contract used by the existing UI. The Gin migration
	// preserves these URLs and DTOs while replacing only the PB/tenant storage.
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
	legacyNetwork.GET("/:id/events", s.requirePermission("device.view"), s.listDeviceEvents)
	legacyNetwork.GET("/:id/events/facets", s.requirePermission("device.view"), s.listDeviceEventFacets)
	auth.GET("/network/traffic-policy-defaults", s.requirePermission("device.view"), s.getTrafficPolicyDefaults)
	auth.PUT("/network/traffic-policy-defaults", s.requirePermission("port.update"), s.putTrafficPolicyDefaults)
	legacyPorts := auth.Group("/network/ports")
	legacyPorts.GET("/:port_id", s.requirePermission("port.view"), s.getPort)
	legacyPorts.PATCH("/:port_id", s.requirePermission("port.update"), s.updatePort)
	legacyPorts.DELETE("/:port_id", s.requirePermission("port.update"), s.deletePort)
	legacyPorts.GET("/:port_id/delete-preview", s.requirePermission("port.update"), s.portDeletePreview)
	legacyPorts.GET("/:port_id/policy", s.requirePermission("port.view"), s.getPortPolicy)
	legacyPorts.PATCH("/:port_id/policy", s.requirePermission("port.update"), s.patchPortPolicy)
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
	snmpMIBModules := auth.Group("/snmp/mib-modules")
	snmpMIBModules.GET("", s.requirePermission("device.view"), s.listMIBModules)
	snmpMIBModules.PUT("", s.requirePermission("device.update"), s.putMIBModule)
	snmpMIBModules.DELETE("/:module_id", s.requirePermission("device.update"), s.deleteMIBModule)
	// KISS-05 geo/address library (owned slice): editable CRUD/list + source imports,
	// plus the de-tenanted publication lifecycle (preview/publish/versions/lifecycle).
	s.registerAddressRoutes(auth)
	s.registerAddressImportRoutes(auth)
	s.registerAddressDimensionRoutes(auth)

	// KISS-06 phase-1b: ClickHouse-backed flow query API (records/facets), the v2
	// FlowQueryService replacing the retired hub QueryGateway stack.
	s.registerFlowRoutes(auth)
	flowEnrichment := auth.Group("/flow")
	flowEnrichment.GET("/classification-profile", s.requirePermission("address.view"), s.getFlowClassificationProfile)
	flowEnrichment.PUT("/classification-profile", s.requirePermission("address.manage"), s.putFlowClassificationProfile)
	flowEnrichment.GET("/enrichment-publications", s.requirePermission("address.view"), s.listFlowEnrichmentPublications)
	flowEnrichment.POST("/enrichment-publications", s.requirePermission("address.publish"), s.publishFlowEnrichment)
	flowEnrichment.GET("/enrichment-publications/facets", s.requirePermission("address.view"), s.listFlowEnrichmentPublicationFacets)
	flowEnrichment.GET("/enrichment-publications/:publication_id", s.requirePermission("address.view"), s.getFlowEnrichmentPublication)
	flowEnrichment.GET("/enrichment-publications/:publication_id/acks", s.requirePermission("address.view"), s.listFlowEnrichmentACKs)
	flowEnrichment.GET("/enrichment-publications/:publication_id/acks/facets", s.requirePermission("address.view"), s.listFlowEnrichmentACKFacets)

	// Per-user resource-grant management (device/port/billing access rights).
	s.registerAccessRoutes(auth)
	s.registerPlatformOperationsRoutes(auth)

	s.registerExportRoutes(auth)

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

func (s *Server) requireInstalled(c *gin.Context) {
	if !s.installed.Load() {
		fail(c, http.StatusPreconditionRequired, "install_required", "watchdog must be installed before this endpoint is available")
		return
	}
	c.Next()
}

func requestBodyLimit(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		// The address-database uploads stream tens/hundreds of MB to disk and enforce
		// their own cfg.Address.MaxUploadBytes ceiling (multipart) or tus MaxSize
		// (resumable chunks), so the small JSON-body cap is skipped for both.
		if c.Request.Method == http.MethodPost && c.FullPath() == "/api/v1/address-imports" {
			c.Next()
			return
		}
		if strings.HasPrefix(c.FullPath(), "/api/v1/address-imports/uploads") {
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
	if !s.installed.Load() {
		c.JSON(http.StatusOK, gin.H{"status": "install_required", "installed": false, "mysql": dbOK, "clickhouse": false})
		return
	}
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
	st.RuntimeReady = s.runtimeReady.Load()
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
			c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS, HEAD")
			// The tus.* / Upload-* request headers and X-HTTP-Method-Override let the
			// embedded resumable-upload endpoint work cross-origin; the exposed set lets
			// a tus client (Uppy) read the upload URL and offset.
			c.Header("Access-Control-Allow-Headers", "Authorization, Content-Type, If-Match, X-CSRF-Token, X-Watchdog-Agent-Token, X-Request-ID, Tus-Resumable, Upload-Length, Upload-Offset, Upload-Metadata, Upload-Concat, X-HTTP-Method-Override")
			c.Header("Access-Control-Expose-Headers", "ETag, X-Request-ID, Location, Tus-Resumable, Tus-Version, Tus-Extension, Tus-Max-Size, Upload-Offset, Upload-Length, Upload-Metadata, X-Watchdog-Import-Id")
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

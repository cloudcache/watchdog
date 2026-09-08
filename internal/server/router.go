package server

import (
	"context"
	"net/http"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
)

// newRouter wires the KISS API surface by domain (docs/watchdog-kiss-architecture.md §7).
// Handlers other than health/install-status are scaffolds returning 501 until their
// work package (KISS-01B auth, KISS-02 RBAC/device, KISS-07 billing, KISS-L log/alert)
// fills them in. No PocketBase, no tenant, no generic dataset/provider envelope.
func (s *Server) newRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery(), s.cors())

	api := r.Group("/api/v1")
	api.GET("/health", s.health)
	api.GET("/install-status", s.installStatus)

	// session / auth (KISS-01B)
	session := api.Group("/session")
	session.POST("/login", s.todo)
	session.POST("/logout", s.todo)
	session.GET("/current", s.todo)
	session.POST("/password", s.todo)

	// RBAC (KISS-02): global users/roles + fixed ability catalogue
	crudStub(api.Group("/users"), s)
	crudStub(api.Group("/roles"), s)
	api.GET("/permissions", s.todo)

	// inventory (KISS-02): one device root + LibreNMS organisation
	crudStub(api.Group("/devices"), s)
	crudStub(api.Group("/device-groups"), s)
	crudStub(api.Group("/locations"), s)
	crudStub(api.Group("/snmp-profiles"), s)
	dev := api.Group("/devices/:id")
	dev.GET("/ports", s.todo)
	dev.GET("/addresses", s.todo)
	dev.GET("/bgp", s.todo)
	dev.GET("/inventory", s.todo)
	dev.GET("/events", s.todo) // eventlog from ClickHouse (KISS-L)

	// billing (KISS-07): accounts/ports/periods + three-layer reconciliation
	crudStub(api.Group("/billing/accounts"), s)
	crudStub(api.Group("/billing/parties"), s)
	bill := api.Group("/billing")
	bill.GET("/periods", s.todo)
	bill.POST("/periods/:id/calculate", s.todo)
	bill.POST("/periods/:id/reconcile", s.todo)
	bill.POST("/periods/:id/approve", s.todo)
	bill.POST("/periods/:id/export", s.todo)

	// agents (KISS-04): registration is the only extension point
	crudStub(api.Group("/agents"), s)

	// operations + audit (shared async state machine)
	api.GET("/jobs", s.todo)
	api.GET("/jobs/:id", s.todo)
	api.POST("/jobs/:id/cancel", s.todo)
	api.GET("/audit", s.todo)
	api.GET("/exports", s.todo)

	// alert delivery config (data/log/alert live in ClickHouse; subsystem is KISS-L)
	crudStub(api.Group("/alerts/channels"), s)
	crudStub(api.Group("/alerts/quiet-hours"), s)

	// optional single-binary UI hosting (deployment convenience only)
	if s.cfg.StaticDir != "" {
		r.NoRoute(s.spaFallback)
	}
	return r
}

// crudStub registers the standard collection+item verbs as scaffolds.
func crudStub(g *gin.RouterGroup, s *Server) {
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

// todo is the scaffold handler: a stable envelope reporting the endpoint is not
// yet implemented, so the frontend can be wired without guessing shapes.
func (s *Server) todo(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{
		"error": gin.H{
			"code":    "not_implemented",
			"message": "endpoint scaffolded; implemented in its KISS work package",
			"path":    c.FullPath(),
		},
	})
}

func (s *Server) spaFallback(c *gin.Context) {
	c.File(filepath.Join(s.cfg.StaticDir, "index.html"))
}

// cors is a minimal allowlist CORS middleware for the separate frontend build.
func (s *Server) cors() gin.HandlerFunc {
	allowed := make(map[string]bool, len(s.cfg.AllowedOrigins))
	for _, o := range s.cfg.AllowedOrigins {
		allowed[o] = true
	}
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if origin != "" && allowed[origin] {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Vary", "Origin")
			c.Header("Access-Control-Allow-Credentials", "true")
			c.Header("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
			c.Header("Access-Control-Allow-Headers", "Content-Type, If-Match, X-CSRF-Token")
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

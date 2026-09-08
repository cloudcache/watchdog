package server

import (
	"context"
	"net/http"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
)

// newRouter wires the KISS API surface by domain (docs/watchdog-kiss-architecture.md §7).
// Auth/RBAC (session, profile, users, roles, permissions) is implemented; the remaining
// domains are 501 scaffolds filled by their work packages (KISS-02 device, KISS-07 billing,
// KISS-04 agent, KISS-L log/alert). No PocketBase, no tenant.
func (s *Server) newRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery(), s.cors())

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

	// --- scaffolds for later work packages (authenticated) ---
	todoCRUD(auth.Group("/devices"), s)
	todoCRUD(auth.Group("/device-groups"), s)
	todoCRUD(auth.Group("/locations"), s)
	todoCRUD(auth.Group("/snmp-profiles"), s)
	dev := auth.Group("/devices/:id")
	dev.GET("/ports", s.todo)
	dev.GET("/addresses", s.todo)
	dev.GET("/bgp", s.todo)
	dev.GET("/inventory", s.todo)
	dev.GET("/events", s.todo)
	todoCRUD(auth.Group("/billing/accounts"), s)
	todoCRUD(auth.Group("/billing/parties"), s)
	todoCRUD(auth.Group("/agents"), s)
	todoCRUD(auth.Group("/alerts/channels"), s)
	todoCRUD(auth.Group("/alerts/quiet-hours"), s)
	auth.GET("/jobs", s.todo)
	auth.GET("/audit", s.todo)
	auth.GET("/exports", s.todo)

	if s.cfg.StaticDir != "" {
		r.NoRoute(s.spaFallback)
	}
	return r
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

func (s *Server) spaFallback(c *gin.Context) {
	c.File(filepath.Join(s.cfg.StaticDir, "index.html"))
}

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

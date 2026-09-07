package hub

import (
	"io/fs"
	"net/http"
	"strings"

	"github.com/cloudcache/watchdog/internal/hub/utils"
	"github.com/cloudcache/watchdog/internal/site"

	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
)

// startServer sets up the production server for Watchdog
func (h *Hub) startServer(se *core.ServeEvent) error {
	indexFile, _ := fs.ReadFile(site.DistDirFS, "index.html")
	html := modifyIndexHTML(h, indexFile)
	// Vite fingerprints build assets, so they can be cached permanently. Files
	// under /static keep stable names and must be revalidated after upgrades.
	serveStatic := apis.Static(site.DistDirFS, false)
	// get CSP configuration
	csp, cspExists := utils.GetEnv("CSP")
	// add route
	se.Router.GET("/{path...}", func(e *core.RequestEvent) error {
		if cacheControl, ok := staticCacheControl(e.Request.URL.Path); ok {
			e.Response.Header().Set("Cache-Control", cacheControl)
			return serveStatic(e)
		}
		if cspExists {
			e.Response.Header().Del("X-Frame-Options")
			e.Response.Header().Set("Content-Security-Policy", csp)
		}
		return e.HTML(http.StatusOK, html)
	})
	return nil
}

func staticCacheControl(requestPath string) (string, bool) {
	if strings.Contains(requestPath, "/assets/") {
		return "public, max-age=31536000, immutable", true
	}
	if strings.Contains(requestPath, "/static/") {
		return "no-cache", true
	}
	// favicon.ico has a stable name at the root (or under a configured base
	// path), so serve the file and revalidate it instead of falling through to
	// the SPA index route.
	if strings.HasSuffix(requestPath, "/favicon.ico") {
		return "no-cache", true
	}
	return "", false
}

import { defineConfig } from "vite"
import path from "node:path"
import tailwindcss from "@tailwindcss/vite"
import react from "@vitejs/plugin-react-swc"
import { lingui } from "@lingui/vite-plugin"

export default defineConfig({
	// Absolute base so hashed bundles, /watchdog-config.js and static assets load
	// from the site root regardless of the current SPA route. A relative base
	// ("./") makes a hard refresh on a deep route (e.g. /network/devices/)
	// resolve assets against that route, which the SPA fallback answers with
	// index.html — breaking module scripts with a text/html MIME type. Client-side
	// routing under a sub-path is handled separately at runtime by
	// WATCHDOG.BASE_PATH, not by the asset base.
	base: "/",
	// The dev server mirrors production: the API is reached on the same origin
	// under /api/, so neither CORS nor a hard-coded API port is involved.
	server: {
		proxy: {
			"/api": "http://127.0.0.1:8091",
		},
	},
	plugins: [
		react({
			plugins: [["@lingui/swc-plugin", {}]],
		}),
		lingui(),
		tailwindcss(),
	],
	esbuild: {
		legalComments: "external",
	},
	resolve: {
		alias: {
			"@": path.resolve(__dirname, "./src"),
		},
	},
	build: {
		rollupOptions: {
			output: {
				manualChunks(id) {
					if (id.includes("/node_modules/@visactor/vtable/")) return "vendor-vtable"
					if (id.includes("/node_modules/@visactor/vchart/")) return "vendor-vchart"
				},
			},
		},
	},
})

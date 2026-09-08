import { defineConfig } from "vite"
import path from "node:path"
import tailwindcss from "@tailwindcss/vite"
import react from "@vitejs/plugin-react-swc"
import { lingui } from "@lingui/vite-plugin"

export default defineConfig({
	base: "./",
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

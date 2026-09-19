// Runtime configuration, loaded before the application bundle. It is copied to
// the site root unchanged by the build, so a deployment edits this file in
// place instead of rebuilding.
//
// API_URL — origin of the Watchdog API. Empty means same-origin: the web server
// that serves this frontend proxies /api/ to the API process (see
// deploy/nginx/watchdog.conf), which needs no CORS configuration and keeps the
// API port off the network. Set an explicit origin such as
// "https://api.example.com" only for a split-origin deployment; that origin
// must then be listed in the API's `server.origins`.
globalThis.WATCHDOG_CONFIG = {
	API_URL: "",
}

// A host page may define WATCHDOG itself (base path, version, API origin); keep
// it when present, otherwise derive it from the configuration above.
const injected = globalThis.WATCHDOG
globalThis.WATCHDOG =
	injected && typeof injected === "object"
		? injected
		: {
				BASE_PATH: "/",
				VERSION: "",
				API_URL: globalThis.WATCHDOG_CONFIG.API_URL || "",
			}

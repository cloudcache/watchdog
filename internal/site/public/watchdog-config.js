// Local Vite development talks directly to the Hub; there is no API proxy.
// A standalone build may replace this file with its externally reachable API URL.
globalThis.WATCHDOG_CONFIG = {
	API_URL: "http://127.0.0.1:8091",
}

const injected = globalThis.WATCHDOG
globalThis.WATCHDOG =
	injected && typeof injected === "object"
		? injected
		: {
				BASE_PATH: "/",
				HUB_VERSION: "",
				API_URL: globalThis.WATCHDOG_CONFIG.API_URL || globalThis.location.origin,
			}

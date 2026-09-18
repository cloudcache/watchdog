// Frontend and API are separate processes on the same host by default. A
// deployment with a different API host may replace this file after building.
globalThis.WATCHDOG_CONFIG = {
	API_URL: `${globalThis.location.protocol}//${globalThis.location.hostname}:8091`,
}

const injected = globalThis.WATCHDOG
globalThis.WATCHDOG =
	injected && typeof injected === "object"
		? injected
		: {
				BASE_PATH: "/",
				VERSION: "",
				API_URL: globalThis.WATCHDOG_CONFIG.API_URL || globalThis.location.origin,
			}

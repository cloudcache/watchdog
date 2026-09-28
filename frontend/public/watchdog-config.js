// Runtime configuration, loaded before the application bundle. It is copied to
// the site root unchanged by the build, so a deployment edits this file in
// place instead of rebuilding.
//
// API_URL — origin of the Watchdog API. Empty means same-origin: the web server
// that serves this frontend proxies /api/ to the API process (see
// deploy/nginx/watchdog.conf). Set an explicit origin when the web server exposes
// the API on another public address. Editing this file requires no rebuild or
// process restart.
globalThis.WATCHDOG_CONFIG = {
	API_URL: "",
	API_BOOTSTRAP_ATTEMPT_TIMEOUT_MS: 2_500,
	API_BOOTSTRAP_RETRIES: 2,
	API_BOOTSTRAP_RETRY_DELAY_MS: 250,
}

// A host page may define WATCHDOG itself (version, API origin); keep it when
// present. MPA documents are always root-relative and do not support sub-path
// BASE_PATH rewrites.
const injected = globalThis.WATCHDOG
globalThis.WATCHDOG =
	injected && typeof injected === "object"
		? { ...injected, BASE_PATH: "/" }
		: {
				BASE_PATH: "/",
				VERSION: "",
				API_URL: globalThis.WATCHDOG_CONFIG.API_URL || "",
				API_BOOTSTRAP_ATTEMPT_TIMEOUT_MS: globalThis.WATCHDOG_CONFIG.API_BOOTSTRAP_ATTEMPT_TIMEOUT_MS,
				API_BOOTSTRAP_RETRIES: globalThis.WATCHDOG_CONFIG.API_BOOTSTRAP_RETRIES,
				API_BOOTSTRAP_RETRY_DELAY_MS: globalThis.WATCHDOG_CONFIG.API_BOOTSTRAP_RETRY_DELAY_MS,
			}

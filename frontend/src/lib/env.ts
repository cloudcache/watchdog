/** Development-only auth bypass, enabled with `VITE_WATCHDOG_DEV_AUTH=true` at build time. */
export const developmentAuth = import.meta.env.VITE_WATCHDOG_DEV_AUTH === "true"

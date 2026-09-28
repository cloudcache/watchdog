const installedCacheKey = "watchdog-installed-v1"

type MinimalStorage = Pick<Storage, "getItem" | "setItem" | "removeItem">

/** Cache only the stable positive install fact for the lifetime of this tab. */
export function readInstalledCache(storage: MinimalStorage | undefined): boolean {
	try {
		return storage?.getItem(installedCacheKey) === "1"
	} catch {
		return false
	}
}

export function writeInstalledCache(storage: MinimalStorage | undefined, installed: boolean) {
	try {
		if (installed) storage?.setItem(installedCacheKey, "1")
		else storage?.removeItem(installedCacheKey)
	} catch {
		// Browsers can deny storage in hardened/private contexts. The application
		// remains correct; it merely performs the install-status request again.
	}
}

import { routePatterns } from "../route-manifest.ts"

export const basePath = globalThis.WATCHDOG?.BASE_PATH || ""

export const prependBasePath = (path: string) => (basePath + path).replaceAll("//", "/")

export const pagePaths = Object.fromEntries(
	Object.entries(routePatterns).map(([name, pattern]) => [name, prependBasePath(pattern)])
) as { [Name in keyof typeof routePatterns]: (typeof routePatterns)[Name] }

export type PageName = keyof typeof pagePaths

/** Build a document URL without creating or mutating client-side route state. */
export function getPagePath(
	_registry: unknown,
	name: PageName,
	params?: Record<string, string | number | undefined>
): string {
	let path: string = pagePaths[name]
	path = path.replace(/\/:([A-Za-z0-9_]+)\?/g, (_match, key: string) => {
		const value = params?.[key]
		return value === undefined || value === "" ? "" : `/${encodeURIComponent(String(value))}`
	})
	path = path.replace(/:([A-Za-z0-9_]+)/g, (_match, key: string) => {
		const value = params?.[key]
		if (value === undefined || value === "") throw new Error(`Missing route parameter: ${key}`)
		return encodeURIComponent(String(value))
	})
	return path || "/"
}

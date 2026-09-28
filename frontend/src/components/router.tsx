import { basePath, getPagePath, type PageName, pagePaths, prependBasePath } from "@/lib/page-path.ts"

export { basePath, getPagePath, prependBasePath }

/** Compatibility value for existing typed URL builders; it contains no store. */
export const $router = { routes: pagePaths } as const

/** A document page is immutable until the browser loads another document. */
export type Page = {
	[Name in PageName]: { params: Record<string, string>; path: string; route: Name }
}[PageName]

/**
 * Resolve the page represented by this HTML document once. Production pages
 * carry their route name on `<html data-watchdog-page="…">`; the pathname
 * fallback keeps directly served documents useful. This deliberately has no
 * client route subscription and does not mutate browser history.
 */
export function resolveDocumentPage(): Page | undefined {
	const declaredRoute = document.documentElement.dataset.watchdogPage
	const pathname = window.location.pathname.replace(/\/$/, "") || "/"
	const candidates =
		declaredRoute && declaredRoute in pagePaths
			? [[declaredRoute as PageName, pagePaths[declaredRoute as PageName]] as const]
			: (Object.entries(pagePaths) as [PageName, string][])

	for (const [route, pattern] of candidates) {
		const params = matchPath(pattern, pathname)
		if (params) return { params, path: pathname, route } as Page
	}
	return undefined
}

function matchPath(pattern: string, pathname: string): Record<string, string> | undefined {
	const expected = pattern.split("/").filter(Boolean)
	const actual = pathname.split("/").filter(Boolean)
	const params: Record<string, string> = {}
	let actualIndex = 0

	for (const segment of expected) {
		if (segment.startsWith(":")) {
			const optional = segment.endsWith("?")
			const name = segment.slice(1, optional ? -1 : undefined)
			if (actualIndex >= actual.length) {
				if (optional) continue
				return undefined
			}
			try {
				params[name] = decodeURIComponent(actual[actualIndex])
			} catch {
				return undefined
			}
			actualIndex++
			continue
		}
		if (actual[actualIndex]?.toLowerCase() !== segment.toLowerCase()) return undefined
		actualIndex++
	}
	return actualIndex === actual.length ? params : undefined
}

/** Perform traditional document navigation. React never intercepts page links. */
export const navigate = (urlString: string) => {
	window.location.assign(urlString || prependBasePath("/"))
}

/** Replace the current document without retaining a redirect-only history item. */
export const redirect = (urlString: string) => {
	window.location.replace(urlString || prependBasePath("/"))
}

export function Link(props: React.AnchorHTMLAttributes<HTMLAnchorElement>) {
	return <a {...props}></a>
}

import { t } from "@lingui/core/macro"
import PocketBase from "pocketbase"
import { basePath } from "@/components/router"
import { toast } from "@/components/ui/use-toast"
import type { ChartTimes, UserSettings } from "@/types"
import { $alerts, $allSystemsById, $allSystemsByName, $userSettings } from "./stores"
import { chartTimeData } from "./utils"

/** PocketBase JS Client */
export const pb = new PocketBase(basePath)

const pocketBaseSend = pb.send.bind(pb)

pb.send = ((path: string, options = {}) => {
	if (path.startsWith("/api/v1")) {
		return sendWatchdogAPI(path, options)
	}
	return pocketBaseSend(path, options)
}) as typeof pb.send

export const isAdmin = () => import.meta.env.VITE_WATCHDOG_DEV_AUTH === "true" || pb.authStore.record?.role === "admin"
export const isReadOnlyUser = () => pb.authStore.record?.role === "readonly"

async function sendWatchdogAPI<T>(
	path: string,
	options: RequestInit & { body?: unknown; query?: Record<string, string | number | boolean | undefined> } = {}
): Promise<T> {
	const headers = new Headers(options.headers)
	const url = new URL(`${basePath}${path}`, window.location.origin)
	for (const [key, value] of Object.entries(options.query ?? {})) {
		if (value !== undefined) {
			url.searchParams.set(key, String(value))
		}
	}
	const init: RequestInit = {
		...options,
		headers,
	}
	delete (init as RequestInit & { query?: unknown }).query
	if (options.body && !(options.body instanceof FormData) && typeof options.body !== "string") {
		headers.set("Content-Type", "application/json")
		init.body = JSON.stringify(options.body)
	}
	const response = await fetch(url, init)
	if (!response.ok) {
		const message = await readAPIErrorMessage(response)
		throw new Error(message || `Request failed with status ${response.status}`)
	}
	if (response.status === 204) {
		return undefined as T
	}
	return response.json() as Promise<T>
}

async function readAPIErrorMessage(response: Response) {
	const text = await response.text()
	if (!text) {
		return ""
	}
	try {
		const data = JSON.parse(text) as { error?: { message?: string } }
		return data.error?.message || text
	} catch {
		return text
	}
}

export const verifyAuth = () => {
	pb.collection("users")
		.authRefresh()
		.catch(() => {
			logOut()
			toast({
				title: t`Failed to authenticate`,
				description: t`Please log in again`,
				variant: "destructive",
			})
		})
}

/** Logs the user out by clearing the auth store and unsubscribing from realtime updates. */
export function logOut() {
	$allSystemsByName.set({})
	$allSystemsById.set({})
	$alerts.set({})
	$userSettings.set({} as UserSettings)
	sessionStorage.setItem("lo", "t") // prevent auto login on logout
	pb.authStore.clear()
	pb.realtime.unsubscribe()
}

/** Fetch or create user settings in database */
export async function updateUserSettings() {
	try {
		const req = await pb.collection("user_settings").getFirstListItem("", { fields: "settings" })
		$userSettings.set(req.settings)
		return
	} catch (e) {
		console.error("get settings", e)
	}
	// create user settings if error fetching existing
	try {
		const createdSettings = await pb.collection("user_settings").create({ user: pb.authStore.record?.id })
		$userSettings.set(createdSettings.settings)
	} catch (e) {
		console.error("create settings", e)
	}
}

export function getPbTimestamp(timeString: ChartTimes, d?: Date) {
	d ||= chartTimeData[timeString].getOffset(new Date())
	const year = d.getUTCFullYear()
	const month = String(d.getUTCMonth() + 1).padStart(2, "0")
	const day = String(d.getUTCDate()).padStart(2, "0")
	const hours = String(d.getUTCHours()).padStart(2, "0")
	const minutes = String(d.getUTCMinutes()).padStart(2, "0")
	const seconds = String(d.getUTCSeconds()).padStart(2, "0")

	return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`
}

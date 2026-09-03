import PocketBase from "pocketbase"
import { basePath, prependBasePath } from "@/components/router"
import type { ChartTimes, UserSettings } from "@/types"
import {
	$platformIdentity,
	type PlatformAuthContext,
	type PlatformPermission,
	type PlatformTenant,
} from "./platform-auth"
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

const watchdogDevAuth = import.meta.env.VITE_WATCHDOG_DEV_AUTH === "true"

export const isAdmin = () => watchdogDevAuth || $platformIdentity.get().current?.isAdmin === true
export const isReadOnlyUser = () => {
	if (watchdogDevAuth) {
		return false
	}
	const identity = $platformIdentity.get()
	if (!identity.ready || !identity.current) {
		return true
	}
	if (identity.current.isAdmin) {
		return false
	}
	return !identity.current.grants.some((grant) =>
		grant.actions.some((action) => action === "configure" || action === "operate" || action === "admin")
	)
}

export function setWatchdogTenant(tenantID: string) {
	const subject = pb.authStore.record?.id
	if (!subject) {
		throw new Error("Cannot select a tenant before authentication")
	}
	const key = `watchdog.tenant.${subject}`
	if (tenantID.trim()) {
		localStorage.setItem(key, tenantID.trim())
	} else {
		localStorage.removeItem(key)
	}
}

export async function refreshWatchdogIdentity() {
	if (watchdogDevAuth) {
		$platformIdentity.set({
			ready: true,
			tenants: [{ id: "development", name: "Development", status: "active" }],
			current: { tenantID: "development", userID: "development", roleIDs: [], grants: [], isAdmin: true },
		})
		return
	}
	const response = await pb.send<{ items?: unknown[] }>("/api/v1/me/tenants", {})
	const tenants = (response.items ?? []).map(normalizePlatformTenant).filter((tenant) => tenant.id)
	let tenantID = getWatchdogTenant()
	if (!tenants.some((tenant) => tenant.id === tenantID)) {
		tenantID = tenants.length === 1 ? tenants[0].id : ""
		setWatchdogTenant(tenantID)
	}
	if (!tenantID) {
		$platformIdentity.set({ ready: true, tenants })
		return
	}
	const current = normalizePlatformAuthContext(await pb.send<Record<string, unknown>>("/api/v1/me", {}))
	$platformIdentity.set({ ready: true, tenants, current })
}

export async function selectWatchdogTenant(tenantID: string) {
	const tenant = $platformIdentity.get().tenants.find((item) => item.id === tenantID)
	if (!tenant) {
		throw new Error("Tenant access denied")
	}
	setWatchdogTenant(tenant.id)
	await refreshWatchdogIdentity()
}

function getWatchdogTenant() {
	const subject = pb.authStore.record?.id
	return subject ? localStorage.getItem(`watchdog.tenant.${subject}`) || "" : ""
}

function normalizePlatformTenant(value: unknown): PlatformTenant {
	const tenant = value as Record<string, unknown>
	return {
		id: String(tenant.id ?? tenant.ID ?? ""),
		name: String(tenant.name ?? tenant.Name ?? ""),
		status: String(tenant.status ?? tenant.Status ?? ""),
	}
}

function normalizePlatformAuthContext(value: Record<string, unknown>): PlatformAuthContext {
	const rawGrants = (value.grants ?? value.Grants ?? []) as Array<Record<string, unknown>>
	return {
		tenantID: String(value.tenant_id ?? value.TenantID ?? ""),
		userID: String(value.user_id ?? value.UserID ?? ""),
		roleIDs: ((value.role_ids ?? value.RoleIDs ?? []) as unknown[]).map(String),
		grants: rawGrants.map(
			(grant): PlatformPermission => ({
				actions: ((grant.actions ?? grant.Actions ?? []) as unknown[]).map(String),
			})
		),
		isAdmin: Boolean(value.is_admin ?? value.IsAdmin),
	}
}

async function sendWatchdogAPI<T>(
	path: string,
	options: RequestInit & { body?: unknown; query?: Record<string, string | number | boolean | undefined> } = {}
): Promise<T> {
	const headers = new Headers(options.headers)
	if (!headers.has("X-Request-ID")) {
		headers.set("X-Request-ID", crypto.randomUUID())
	}
	if (!headers.has("Authorization") && pb.authStore.token) {
		headers.set("Authorization", pb.authStore.token)
	}
	const subject = pb.authStore.record?.id
	if (!headers.has("X-Watchdog-Tenant-ID") && subject) {
		const tenantID = localStorage.getItem(`watchdog.tenant.${subject}`)
		if (tenantID) {
			headers.set("X-Watchdog-Tenant-ID", tenantID)
		}
	}
	const url = new URL(prependBasePath(path), window.location.origin)
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

/** Logs the user out by clearing the auth store and unsubscribing from realtime updates. */
export function logOut() {
	$allSystemsByName.set({})
	$allSystemsById.set({})
	$alerts.set({})
	$userSettings.set({} as UserSettings)
	$platformIdentity.set({ ready: false, tenants: [] })
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

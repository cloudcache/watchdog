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
	options: RequestInit & {
		body?: unknown
		query?: Record<string, string | number | boolean | undefined>
		// onResponse exposes the raw response (headers) so callers can read the
		// ETag for optimistic-concurrency edits.
		onResponse?: (response: Response) => void
	} = {}
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
	delete (init as RequestInit & { onResponse?: unknown }).onResponse
	if (options.body && !(options.body instanceof FormData) && typeof options.body !== "string") {
		headers.set("Content-Type", "application/json")
		init.body = JSON.stringify(options.body)
	}
	const response = await fetch(url, init)
	options.onResponse?.(response)
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

// UI display preferences live in MySQL (was part of the PocketBase
// user_settings collection). row_version drives optimistic concurrency: it
// comes back in the GET/PUT body and is echoed as the If-Match on the next
// write. Notification channels (emails/webhooks) now live in MySQL too and the
// alert delivery path reads them there (see saveNotificationSettings below).
let userPreferencesRowVersion = 0

type UserPreferencesResponse = { settings?: UserSettings; row_version?: number }

// The fields that belong in MySQL user_preferences (display prefs only).
const UI_PREFERENCE_KEYS = [
	"chartTime",
	"unitTemp",
	"unitNet",
	"unitDisk",
	"colorWarn",
	"colorCrit",
	"hourFormat",
	"layoutWidth",
] as const

function pickUIPreferences(settings: Partial<UserSettings>): Partial<UserSettings> {
	const out: Partial<UserSettings> = {}
	for (const key of UI_PREFERENCE_KEYS) {
		if (settings[key] !== undefined) {
			// biome-ignore lint/suspicious/noExplicitAny: narrow copy across a keyed union
			;(out as any)[key] = settings[key]
		}
	}
	return out
}

/** Load UI preferences and notification channels, both from MySQL. */
export async function updateUserSettings() {
	const merged: Partial<UserSettings> = { ...$userSettings.get() }
	try {
		const res = await pb.send<UserPreferencesResponse>("/api/v1/me/preferences", {})
		userPreferencesRowVersion = res.row_version ?? 0
		Object.assign(merged, pickUIPreferences(res.settings ?? {}))
	} catch (e) {
		console.error("get preferences", e)
	}
	try {
		const channels = await pb.send<{ emails?: string[]; webhooks?: string[] }>("/api/v1/me/notification-channels", {})
		merged.emails = channels.emails ?? []
		merged.webhooks = channels.webhooks ?? []
	} catch (e) {
		console.error("get notification channels", e)
	}
	$userSettings.set(merged as UserSettings)
}

/** Persist UI display preferences to MySQL (notification channels excluded). */
export async function saveUserPreferences(newSettings: Partial<UserSettings>): Promise<UserSettings> {
	const merged = { ...$userSettings.get(), ...newSettings }
	const uiOnly = pickUIPreferences(merged)
	const res = await pb.send<UserPreferencesResponse>("/api/v1/me/preferences", {
		method: "PUT",
		headers: userPreferencesRowVersion > 0 ? { "If-Match": `"${userPreferencesRowVersion}"` } : {},
		body: uiOnly,
	})
	userPreferencesRowVersion = res.row_version ?? userPreferencesRowVersion
	$userSettings.set(merged)
	return merged
}

/** Persist notification channels (emails/webhooks) to MySQL, where the alert
 * delivery path reads them. */
export async function saveNotificationSettings(channels: Pick<UserSettings, "emails" | "webhooks">): Promise<void> {
	const saved = await pb.send<{ emails?: string[]; webhooks?: string[] }>("/api/v1/me/notification-channels", {
		method: "PUT",
		body: { emails: channels.emails ?? [], webhooks: channels.webhooks ?? [] },
	})
	$userSettings.set({ ...$userSettings.get(), emails: saved.emails ?? [], webhooks: saved.webhooks ?? [] })
}

// Quiet hours live in MySQL (was the PocketBase quiet_hours collection). The
// alert silencing path reads a user's windows there by resolving the PocketBase
// user id to the MySQL user. `system` is the PocketBase system id the window
// applies to; "" means it is global (all systems).
export interface QuietHourWindow {
	id: string
	system: string
	type: "one-time" | "daily"
	start: string
	end: string
}

type QuietHourAPI = {
	id: string
	system_id?: string
	type: "one-time" | "daily"
	start: string
	end: string
}

function fromQuietHourAPI(w: QuietHourAPI): QuietHourWindow {
	return { id: w.id, system: w.system_id ?? "", type: w.type, start: w.start, end: w.end }
}

export async function fetchQuietHours(): Promise<QuietHourWindow[]> {
	const res = await pb.send<{ items?: QuietHourAPI[] }>("/api/v1/me/quiet-hours", {})
	return (res.items ?? []).map(fromQuietHourAPI)
}

export async function saveQuietHour(input: {
	id?: string
	system: string
	type: "one-time" | "daily"
	start: string
	end: string
}): Promise<QuietHourWindow> {
	const body = { system_id: input.system, type: input.type, start: input.start, end: input.end }
	const saved = await pb.send<QuietHourAPI>(
		input.id ? `/api/v1/me/quiet-hours/${input.id}` : "/api/v1/me/quiet-hours",
		{ method: input.id ? "PATCH" : "POST", body }
	)
	return fromQuietHourAPI(saved)
}

export async function deleteQuietHour(id: string): Promise<void> {
	await pb.send(`/api/v1/me/quiet-hours/${id}`, { method: "DELETE" })
}

// Alert history lives in MySQL (was the PocketBase alerts_history collection).
// The PocketBase alert engine writes it on trigger/resolve; a user reads and
// deletes only their own entries. `system` is the PocketBase system id.
export interface AlertHistoryEntry {
	id: string
	alert_id: string
	system: string
	name: string
	value: number
	created: string
	resolved: string | null
}

export async function fetchAlertsHistory(limit = 200): Promise<AlertHistoryEntry[]> {
	const res = await pb.send<{ items?: AlertHistoryEntry[] }>("/api/v1/me/alerts-history", { query: { limit } })
	return res.items ?? []
}

export async function deleteAlertHistory(id: string): Promise<void> {
	await pb.send(`/api/v1/me/alerts-history/${id}`, { method: "DELETE" })
}

// A page of the target list. GET /api/v1/targets paginates opt-in (only when a
// limit or cursor is passed); the Hosts view pages with exclude_kind=network.
export interface TargetListItem {
	id: string
	name: string
	kind: string
	host: string
	status: string
	labels?: Record<string, string>
	updated_at: string
}

export async function fetchTargetsPage(opts: {
	limit?: number
	cursor?: string
	excludeKind?: string
	search?: string
	status?: string
	kind?: string
	sort?: string
	order?: "asc" | "desc"
	offset?: number
}): Promise<{ items: TargetListItem[]; nextCursor: string; total?: number }> {
	const res = await pb.send<{ items?: TargetListItem[]; next_cursor?: string; total?: number }>("/api/v1/targets", {
		query: {
			limit: opts.limit ?? 100,
			cursor: opts.cursor || undefined,
			exclude_kind: opts.excludeKind || undefined,
			q: opts.search || undefined,
			status: opts.status || undefined,
			kind: opts.kind || undefined,
			sort: opts.sort || undefined,
			order: opts.order || undefined,
			offset: opts.offset || undefined,
		},
	})
	return { items: res.items ?? [], nextCursor: res.next_cursor ?? "", total: res.total }
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

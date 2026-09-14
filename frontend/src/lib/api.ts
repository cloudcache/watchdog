import { basePath } from "@/components/router"
import type { ChartTimes, UserSettings } from "@/types"
import { $platformIdentity, type PlatformAuthContext } from "./platform-auth"
import { resolveAPIBase, responseFilename } from "./api-transport"
import { $allSystemsById, $allSystemsByName, $authenticated, $authChecked, $userSettings } from "./stores"
import { chartTimeData } from "./utils"

const watchdogDevAuth = import.meta.env.VITE_WATCHDOG_DEV_AUTH === "true"

export type SessionUser = {
	id: string
	username: string
	email: string
	display_name: string
	status: string
	is_admin: boolean
	roles: string[]
	abilities: string[]
}

export type InstallStatus = {
	installed: boolean
	requires_install: boolean
	runtime_ready: boolean
	schema_version: string
	product_version: string
	admin_bootstrapped: boolean
	installed_at: string
}

export class WatchdogAPIError extends Error {
	status: number
	code: string
	data: { message: string }

	constructor(status: number, code: string, message: string) {
		super(message)
		this.name = "WatchdogAPIError"
		this.status = status
		this.code = code
		this.data = { message }
	}
}

let sessionUser: SessionUser | undefined

/** Native Watchdog HTTP client. Browser authentication is an HttpOnly session
 * cookie; there is no token cache, collection API or realtime side channel. */
export const api = {
	send: sendWatchdogAPI,
	buildURL: buildAPIURL,
}

export const isAdmin = () => watchdogDevAuth || $platformIdentity.get().current?.isAdmin === true
export const can = (ability: string) =>
	watchdogDevAuth || sessionUser?.is_admin === true || sessionUser?.abilities?.includes(ability) === true
export const canAny = (...abilities: string[]) => isAdmin() || abilities.some((ability) => can(ability))
export const canManageAddressLibrary = () =>
	watchdogDevAuth || can("address.manage") || can("address.publish")
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
	return !sessionUser?.abilities.some((ability) =>
		/\.(create|update|delete|manage|publish|operate|configure)$/.test(ability)
	)
}

export function currentSessionUser() {
	return sessionUser
}

export async function refreshWatchdogIdentity() {
	if (watchdogDevAuth) {
		$platformIdentity.set({
			ready: true,
			current: {
				userID: "development",
				roleIDs: [],
				isAdmin: true,
				canManageAddressLibrary: true,
			},
		})
		$authenticated.set(true)
		return
	}
	const user = await api.send<SessionUser>("/api/v1/session/current", {})
	setAuthenticatedUser(user)
}

export async function restoreSession() {
	try {
		await refreshWatchdogIdentity()
		return true
	} catch (error) {
		if (error instanceof WatchdogAPIError && error.status === 401) {
			clearAuthenticatedUser()
			return false
		}
		throw error
	}
}

export async function login(username: string, password: string) {
	const user = await api.send<SessionUser>("/api/v1/session/login", {
		method: "POST",
		body: { username, password },
	})
	setAuthenticatedUser(user)
	return user
}

export function fetchInstallStatus() {
	return api.send<InstallStatus>("/api/v1/install-status", {})
}

export function installWatchdog(input: { username: string; password: string; email?: string; display_name?: string }) {
	return api.send<InstallStatus>("/api/v1/install", { method: "POST", body: input })
}

function setAuthenticatedUser(user: SessionUser) {
	sessionUser = user
	const current = normalizePlatformAuthContext(user)
	$platformIdentity.set({ ready: true, current })
	$authenticated.set(true)
	$authChecked.set(true)
}

function clearAuthenticatedUser() {
	sessionUser = undefined
	$platformIdentity.set({ ready: true })
	$authenticated.set(false)
	$authChecked.set(true)
}

function normalizePlatformAuthContext(user: SessionUser): PlatformAuthContext {
	return {
		userID: user.id,
		roleIDs: user.roles ?? [],
		isAdmin: user.is_admin,
		canManageAddressLibrary: user.is_admin,
	}
}

async function sendWatchdogAPI<T>(path: string, options: WatchdogAPIOptions = {}): Promise<T> {
	const response = await fetchWatchdogAPI(path, options)
	if (!response.ok) {
		const error = await readAPIError(response)
		if (response.status === 401) {
			clearAuthenticatedUser()
		}
		throw error
	}
	if (response.status === 204) {
		return undefined as T
	}
	return response.json() as Promise<T>
}

type WatchdogAPIOptions = RequestInit & {
	body?: unknown
	query?: Record<string, string | number | boolean | undefined>
	// onResponse exposes raw headers such as ETag for optimistic concurrency.
	onResponse?: (response: Response) => void
}

export async function fetchWatchdogAPI(path: string, options: WatchdogAPIOptions = {}) {
	const headers = new Headers(options.headers)
	if (!headers.has("X-Request-ID")) {
		headers.set("X-Request-ID", crypto.randomUUID())
	}
	const method = (options.method ?? "GET").toUpperCase()
	if (!["GET", "HEAD", "OPTIONS"].includes(method) && !headers.has("X-CSRF-Token")) {
		const csrf = readCookie("wd_csrf")
		if (csrf) headers.set("X-CSRF-Token", csrf)
	}
	const url = new URL(buildAPIURL(path))
	for (const [key, value] of Object.entries(options.query ?? {})) {
		if (value !== undefined) {
			url.searchParams.set(key, String(value))
		}
	}
	const init: RequestInit = {
		...options,
		headers,
		credentials: "include",
	}
	delete (init as RequestInit & { query?: unknown }).query
	delete (init as RequestInit & { onResponse?: unknown }).onResponse
	if (options.body && !(options.body instanceof FormData) && typeof options.body !== "string") {
		headers.set("Content-Type", "application/json")
		init.body = JSON.stringify(options.body)
	}
	if (typeof options.body === "string" && options.body && !headers.has("Content-Type")) {
		headers.set("Content-Type", "application/json")
	}
	const response = await fetch(url, init)
	options.onResponse?.(response)
	return response
}

function buildAPIURL(path: string) {
	const configured = resolveAPIBase(globalThis.WATCHDOG?.API_URL, basePath)
	const base = configured ? new URL(configured, window.location.origin) : new URL(window.location.origin)
	return new URL(path, `${base.protocol}//${base.host}`).toString()
}

function readCookie(name: string) {
	const prefix = `${encodeURIComponent(name)}=`
	for (const part of document.cookie.split(";")) {
		const item = part.trim()
		if (item.startsWith(prefix)) return decodeURIComponent(item.slice(prefix.length))
	}
	return ""
}

export async function downloadWatchdogFile(path: string) {
	const response = await fetchWatchdogAPI(path)
	if (!response.ok) {
		throw await readAPIError(response)
	}
	const blobURL = URL.createObjectURL(await response.blob())
	const link = document.createElement("a")
	link.href = blobURL
	link.download = responseFilename(response.headers.get("Content-Disposition"))
	document.body.append(link)
	link.click()
	link.remove()
	URL.revokeObjectURL(blobURL)
}

async function readAPIError(response: Response) {
	const text = await response.text()
	let code = "request_failed"
	let message = text || `Request failed with status ${response.status}`
	try {
		const data = JSON.parse(text) as { error?: { code?: string; message?: string } }
		code = data.error?.code || code
		message = data.error?.message || message
	} catch {
		// Keep the response text for non-JSON failures.
	}
	return new WatchdogAPIError(response.status, code, message)
}

/** Log out on explicit user action; no background login/refresh is performed. */
export async function logOut() {
	try {
		if ($authenticated.get()) {
			await api.send<void>("/api/v1/session/logout", { method: "POST" })
		}
	} finally {
		clearAuthenticatedUser()
	}
	$allSystemsByName.set({})
	$allSystemsById.set({})
	$userSettings.set({} as UserSettings)
}

// UI display preferences live in MySQL. row_version drives optimistic concurrency: it
// comes back in the GET/PUT body and is echoed as the If-Match on the next write.
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

/** Load UI display preferences from MySQL. */
export async function updateUserSettings() {
	const merged: Partial<UserSettings> = { ...$userSettings.get() }
	try {
		const res = await api.send<UserPreferencesResponse>("/api/v1/me/preferences", {})
		userPreferencesRowVersion = res.row_version ?? 0
		Object.assign(merged, pickUIPreferences(res.settings ?? {}))
	} catch (e) {
		console.error("get preferences", e)
	}
	$userSettings.set(merged as UserSettings)
}

/** Persist UI display preferences to MySQL (notification channels excluded). */
export async function saveUserPreferences(newSettings: Partial<UserSettings>): Promise<UserSettings> {
	const merged = { ...$userSettings.get(), ...newSettings }
	const uiOnly = pickUIPreferences(merged)
	const res = await api.send<UserPreferencesResponse>("/api/v1/me/preferences", {
		method: "PUT",
		headers: userPreferencesRowVersion > 0 ? { "If-Match": `"${userPreferencesRowVersion}"` } : {},
		body: uiOnly,
	})
	userPreferencesRowVersion = res.row_version ?? userPreferencesRowVersion
	$userSettings.set(merged)
	return merged
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
	const res = await api.send<{ items?: TargetListItem[]; next_cursor?: string; total?: number }>("/api/v1/targets", {
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

import type { MessageDescriptor } from "@lingui/core"
import { msg } from "@lingui/core/macro"
import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, SaveIcon, SearchIcon, UserRoundCogIcon } from "lucide-react"
import { memo, useEffect, useMemo, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button, buttonVariants } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { toast } from "@/components/ui/use-toast"
import { api, can } from "@/lib/api"
import { cn } from "@/lib/utils"

type UserRecord = {
	id: string
	username: string
	email: string
	display_name: string
	status: string
	roles: string[]
}

type RoleRecord = {
	id: string
	name: string
	title: string
}

type UserAccess = {
	device_ids: string[]
	device_group_ids: string[]
	port_ids: string[]
	billing_account_ids: string[]
	aggregate_graph_ids: string[]
	metrics: string[]
}

type AccessOption = { id: string; label: string; description: string }

const emptyAccess = (): UserAccess => ({
	device_ids: [],
	device_group_ids: [],
	port_ids: [],
	billing_account_ids: [],
	aggregate_graph_ids: [],
	metrics: [],
})

const accessKinds = [
	{
		kind: "device",
		field: "device_ids",
		label: msg`Devices`,
		help: msg`Select the devices whose inventory and telemetry this user may see.`,
	},
	{
		kind: "device_group",
		field: "device_group_ids",
		label: msg`Device groups`,
		help: msg`Membership grants follow the selected device groups.`,
	},
	{
		kind: "port",
		field: "port_ids",
		label: msg`Ports`,
		help: msg`Grant individual ports without exposing every port on a device.`,
	},
	{
		kind: "metric",
		field: "metrics",
		label: msg`Metrics`,
		help: msg`Once selected, metrics become an allow-list. No selection keeps all metrics inside granted devices and ports.`,
	},
	{
		kind: "billing_account",
		field: "billing_account_ids",
		label: msg`Billing`,
		help: msg`Select the billing accounts and periods this user may inspect.`,
	},
	{
		kind: "aggregate_graph",
		field: "aggregate_graph_ids",
		label: msg`Saved graphs`,
		help: msg`Select saved aggregate graphs this user may open directly.`,
	},
] as const

type AccessKind = (typeof accessKinds)[number]

const builtInRoleTitles: Record<string, MessageDescriptor> = {
	administrator: msg`Administrator`,
	analyst: msg`Analyst`,
	billing: msg`Billing`,
	operator: msg({ message: "Operator", context: "User role" }),
	viewer: msg`Viewer`,
}

export default memo(({ id }: { id?: string }) => {
	const { i18n, t } = useLingui()
	const editing = Boolean(id)
	const manageAccess = can("user.manage")
	const [username, setUsername] = useState("")
	const [email, setEmail] = useState("")
	const [displayName, setDisplayName] = useState("")
	const [password, setPassword] = useState("")
	const [status, setStatus] = useState("active")
	const [roles, setRoles] = useState<RoleRecord[]>([])
	const [selectedRoles, setSelectedRoles] = useState<string[]>([])
	const [access, setAccess] = useState<UserAccess>(emptyAccess)
	const [activeKind, setActiveKind] = useState<AccessKind>(accessKinds[0])
	const [query, setQuery] = useState("")
	const [options, setOptions] = useState<AccessOption[]>([])
	const [total, setTotal] = useState(0)
	const [offset, setOffset] = useState(0)
	const [portDevices, setPortDevices] = useState<AccessOption[]>([])
	const [portDeviceID, setPortDeviceID] = useState("")
	const [portScopeIDs, setPortScopeIDs] = useState<string[]>([])
	const [loading, setLoading] = useState(true)
	const [loadingOptions, setLoadingOptions] = useState(false)
	const [loadingPortScope, setLoadingPortScope] = useState(false)
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")
	const limit = 50

	useEffect(() => {
		document.title = `${editing ? t`Edit User` : t`Add User`} / Watchdog`
		let current = true
		setLoading(true)
		setError("")
		Promise.all([
			manageAccess ? api.send<{ items?: RoleRecord[] }>("/api/v1/roles", {}) : Promise.resolve({ items: [] }),
			id ? api.send<UserRecord>(`/api/v1/users/${id}`, {}) : Promise.resolve(undefined),
			id && manageAccess ? api.send<UserAccess>(`/api/v1/users/${id}/access`, {}) : Promise.resolve(undefined),
		])
			.then(([roleData, user, userAccess]) => {
				if (!current) return
				setRoles(roleData.items ?? [])
				if (user) {
					setUsername(user.username)
					setEmail(user.email)
					setDisplayName(user.display_name)
					setStatus(user.status)
					setSelectedRoles(user.roles ?? [])
				}
				if (userAccess) {
					setAccess({
						device_ids: userAccess.device_ids ?? [],
						device_group_ids: userAccess.device_group_ids ?? [],
						port_ids: userAccess.port_ids ?? [],
						billing_account_ids: userAccess.billing_account_ids ?? [],
						aggregate_graph_ids: userAccess.aggregate_graph_ids ?? [],
						metrics: userAccess.metrics ?? [],
					})
				}
			})
			.catch((cause) => {
				if (current) setError(cause instanceof Error ? cause.message : t`Failed to load user`)
			})
			.finally(() => {
				if (current) setLoading(false)
			})
		return () => {
			current = false
		}
	}, [editing, id, manageAccess, t])

	useEffect(() => {
		if (!manageAccess) return
		let current = true
		api
			.send<{ items?: AccessOption[] }>("/api/v1/access-options", {
				query: { type: "device", limit: 500, offset: 0, sort: "label", order: "asc" },
			})
			.then((data) => {
				if (!current) return
				const devices = data.items ?? []
				setPortDevices(devices)
				setPortDeviceID((selectedDevice) =>
					devices.some((device) => device.id === selectedDevice) ? selectedDevice : (devices[0]?.id ?? "")
				)
			})
			.catch((cause) => {
				if (current) setError(cause instanceof Error ? cause.message : t`Failed to load devices`)
			})
		return () => {
			current = false
		}
	}, [manageAccess, t])

	useEffect(() => {
		if (!manageAccess || activeKind.kind !== "port" || !portDeviceID) {
			setPortScopeIDs([])
			setLoadingPortScope(false)
			return
		}
		let current = true
		setLoadingPortScope(true)
		api
			.send<{ ids?: string[] }>("/api/v1/access-options/port-ids", { query: { device_id: portDeviceID } })
			.then((data) => {
				if (current) setPortScopeIDs(data.ids ?? [])
			})
			.catch((cause) => {
				if (current) setError(cause instanceof Error ? cause.message : t`Failed to load ports`)
			})
			.finally(() => {
				if (current) setLoadingPortScope(false)
			})
		return () => {
			current = false
		}
	}, [activeKind.kind, manageAccess, portDeviceID, t])

	useEffect(() => {
		if (!manageAccess) return
		if (activeKind.kind === "port" && !portDeviceID) {
			setOptions([])
			setTotal(0)
			setLoadingOptions(false)
			return
		}
		let current = true
		setLoadingOptions(true)
		api
			.send<{ items?: AccessOption[]; total?: number }>("/api/v1/access-options", {
				query: {
					type: activeKind.kind,
					device_id: activeKind.kind === "port" ? portDeviceID : undefined,
					q: query.trim() || undefined,
					limit,
					offset,
					sort: "label",
					order: "asc",
				},
			})
			.then((data) => {
				if (!current) return
				setOptions(data.items ?? [])
				setTotal(data.total ?? 0)
			})
			.catch((cause) => {
				if (current) setError(cause instanceof Error ? cause.message : t`Failed to load resources`)
			})
			.finally(() => {
				if (current) setLoadingOptions(false)
			})
		return () => {
			current = false
		}
	}, [activeKind.kind, manageAccess, offset, portDeviceID, query, t])

	const selected = access[activeKind.field]
	const selectedPortCount = useMemo(() => {
		const selectedPorts = new Set(access.port_ids)
		return portScopeIDs.filter((portID) => selectedPorts.has(portID)).length
	}, [access.port_ids, portScopeIDs])
	const selectAllDevicePorts = () => {
		setAccess((current) => ({
			...current,
			port_ids: Array.from(new Set([...current.port_ids, ...portScopeIDs])),
		}))
	}
	const clearDevicePorts = () => {
		const scoped = new Set(portScopeIDs)
		setAccess((current) => ({ ...current, port_ids: current.port_ids.filter((portID) => !scoped.has(portID)) }))
	}
	const toggleAccess = (resourceID: string) => {
		setAccess((current) => ({
			...current,
			[activeKind.field]: current[activeKind.field].includes(resourceID)
				? current[activeKind.field].filter((value) => value !== resourceID)
				: [...current[activeKind.field], resourceID],
		}))
	}
	const toggleRole = (roleName: string) => {
		setSelectedRoles((current) =>
			current.includes(roleName) ? current.filter((value) => value !== roleName) : [...current, roleName]
		)
	}

	const save = async () => {
		setSaving(true)
		setError("")
		try {
			const body = {
				email: email.trim(),
				display_name: displayName.trim(),
				status,
				...(manageAccess ? { roles: selectedRoles, access } : {}),
			}
			if (id) {
				await api.send(`/api/v1/users/${id}`, { method: "PATCH", body })
			} else {
				await api.send("/api/v1/users", {
					method: "POST",
					body: { ...body, username: username.trim(), password },
				})
			}
			toast({ title: t`User saved` })
			navigate(getPagePath($router, "users_admin"))
		} catch (cause) {
			setError(cause instanceof Error ? cause.message : t`Request failed`)
		} finally {
			setSaving(false)
		}
	}

	const valid = Boolean(email.trim() && (editing || (username.trim() && password.length >= 8)))
	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<Link
						href={getPagePath($router, "users_admin")}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }))}
						aria-label={t`Back`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<UserRoundCogIcon className="h-5 w-5 text-muted-foreground" />
					<h1 className="text-xl font-semibold">{editing ? <Trans>Edit User</Trans> : <Trans>Add User</Trans>}</h1>
				</div>
				<Button onClick={save} disabled={loading || saving || !valid}>
					<SaveIcon className="me-2 h-4 w-4" />
					<Trans>Save</Trans>
				</Button>
			</div>
			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}

			<div className="grid gap-4 rounded-md border border-border bg-card p-4">
				<div>
					<h2 className="font-medium">
						<Trans>Account</Trans>
					</h2>
					<p className="text-sm text-muted-foreground">
						<Trans>Local login identity, status and operation roles.</Trans>
					</p>
				</div>
				<div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
					<label htmlFor="user-username" className="grid gap-1.5 text-sm">
						<span>
							<Trans>Username</Trans>
						</span>
						<Input
							id="user-username"
							value={username}
							onChange={(event) => setUsername(event.target.value)}
							disabled={editing}
							autoComplete="username"
						/>
					</label>
					<label htmlFor="user-email" className="grid gap-1.5 text-sm">
						<span>
							<Trans>Email</Trans>
						</span>
						<Input
							id="user-email"
							value={email}
							onChange={(event) => setEmail(event.target.value)}
							placeholder="ops@example.com"
						/>
					</label>
					<label htmlFor="user-display-name" className="grid gap-1.5 text-sm">
						<span>
							<Trans>Name</Trans>
						</span>
						<Input
							id="user-display-name"
							value={displayName}
							onChange={(event) => setDisplayName(event.target.value)}
						/>
					</label>
					{!editing ? (
						<label htmlFor="user-password" className="grid gap-1.5 text-sm">
							<span>
								<Trans>Password</Trans>
							</span>
							<Input
								id="user-password"
								type="password"
								value={password}
								onChange={(event) => setPassword(event.target.value)}
								autoComplete="new-password"
							/>
						</label>
					) : null}
					<label htmlFor="user-status" className="grid gap-1.5 text-sm">
						<span>
							<Trans>Status</Trans>
						</span>
						<Select value={status} onValueChange={setStatus}>
							<SelectTrigger id="user-status">
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="active">active</SelectItem>
								<SelectItem value="disabled">disabled</SelectItem>
							</SelectContent>
						</Select>
					</label>
				</div>
				{manageAccess ? (
					<div className="grid gap-2 text-sm">
						<span>
							<Trans>Roles</Trans>
						</span>
						<div className="grid gap-1 rounded-md border border-border p-2 sm:grid-cols-2 lg:grid-cols-4">
							{roles.length === 0 ? (
								<span className="text-muted-foreground">
									<Trans>No roles yet.</Trans>
								</span>
							) : (
								roles.map((role) => (
									<label
										key={role.id}
										htmlFor={`user-role-${role.id}`}
										className="flex cursor-pointer items-center gap-2 rounded px-2 py-1.5 text-start hover:bg-muted/60"
									>
										<Checkbox
											id={`user-role-${role.id}`}
											checked={selectedRoles.includes(role.name)}
											onCheckedChange={() => toggleRole(role.name)}
										/>
										<span>
											{builtInRoleTitles[role.name] ? i18n._(builtInRoleTitles[role.name]) : role.title || role.name}
										</span>
									</label>
								))
							)}
						</div>
					</div>
				) : null}
			</div>

			{manageAccess ? (
				<div className="grid gap-4 rounded-md border border-border bg-card p-4">
					<div>
						<h2 className="font-medium">
							<Trans>Data access</Trans>
						</h2>
						<p className="text-sm text-muted-foreground">
							<Trans>
								Roles decide permitted operations. These selections decide the concrete devices, ports, metrics, billing
								accounts and saved graphs visible to the user.
							</Trans>
						</p>
					</div>
					<div className="grid gap-4 lg:grid-cols-[210px_minmax(0,1fr)]">
						<div className="grid content-start gap-1">
							{accessKinds.map((kind) => (
								<Button
									key={kind.kind}
									type="button"
									variant={activeKind.kind === kind.kind ? "secondary" : "ghost"}
									className="justify-between"
									onClick={() => {
										setActiveKind(kind)
										setQuery("")
										setOffset(0)
									}}
								>
									<span>{i18n._(kind.label)}</span>
									<Badge variant="outline">{access[kind.field].length}</Badge>
								</Button>
							))}
						</div>
						<div className="grid min-w-0 gap-3">
							{activeKind.kind === "port" ? (
								<div className="grid gap-2 rounded-md border border-border bg-muted/20 p-3 md:grid-cols-[minmax(240px,1fr)_auto] md:items-end">
									<label htmlFor="port-device" className="grid gap-1.5 text-sm">
										<span>
											<Trans>Device</Trans>
										</span>
										<Select
											value={portDeviceID}
											onValueChange={(deviceID) => {
												setPortDeviceID(deviceID)
												setQuery("")
												setOffset(0)
											}}
										>
											<SelectTrigger id="port-device">
												<SelectValue placeholder={t`Select a device`} />
											</SelectTrigger>
											<SelectContent>
												{portDevices.map((device) => (
													<SelectItem key={device.id} value={device.id}>
														{device.label} · {device.description}
													</SelectItem>
												))}
											</SelectContent>
										</Select>
									</label>
									<div className="flex flex-wrap items-center gap-2">
										<span className="me-1 text-xs text-muted-foreground">
											{selectedPortCount} / {portScopeIDs.length} <Trans>ports selected</Trans>
										</span>
										<Button
											type="button"
											variant="outline"
											onClick={selectAllDevicePorts}
											disabled={!portDeviceID || loadingPortScope || portScopeIDs.length === 0}
										>
											<Trans>Select all ports</Trans>
										</Button>
										<Button
											type="button"
											variant="outline"
											onClick={clearDevicePorts}
											disabled={!portDeviceID || loadingPortScope || selectedPortCount === 0}
										>
											<Trans>Clear device ports</Trans>
										</Button>
									</div>
								</div>
							) : null}
							<div className="flex gap-2">
								<div className="relative min-w-0 flex-1">
									<SearchIcon className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
									<Input
										className="pl-9"
										value={query}
										disabled={activeKind.kind === "port" && !portDeviceID}
										onChange={(event) => {
											setQuery(event.target.value)
											setOffset(0)
										}}
										placeholder={t`Search available resources`}
									/>
								</div>
								{activeKind.kind !== "port" ? (
									<Button
										variant="outline"
										onClick={() => setAccess((current) => ({ ...current, [activeKind.field]: [] }))}
									>
										<Trans>Clear selection</Trans>
									</Button>
								) : null}
							</div>
							<div className="min-h-[340px] overflow-hidden rounded-md border border-border">
								{loadingOptions ? (
									<div className="p-3 text-sm text-muted-foreground">
										<Trans>Loading...</Trans>
									</div>
								) : null}
								{activeKind.kind === "port" && !portDeviceID ? (
									<div className="p-3 text-sm text-muted-foreground">
										<Trans>Select a device to view its ports.</Trans>
									</div>
								) : null}
								{!loadingOptions && options.length === 0 && (activeKind.kind !== "port" || portDeviceID) ? (
									<div className="p-3 text-sm text-muted-foreground">
										<Trans>No resources found.</Trans>
									</div>
								) : null}
								{options.map((option) => (
									<button
										key={option.id}
										type="button"
										className="flex w-full items-start gap-3 border-b border-border px-3 py-2 text-start last:border-b-0 hover:bg-muted/60"
										onClick={() => toggleAccess(option.id)}
									>
										<Checkbox className="pointer-events-none mt-0.5" checked={selected.includes(option.id)} />
										<span className="min-w-0">
											<span className="block truncate text-sm font-medium">{option.label}</span>
											<span className="block truncate text-xs text-muted-foreground">
												{option.description || option.id}
											</span>
										</span>
									</button>
								))}
							</div>
							<div className="flex items-center justify-between gap-3 text-xs text-muted-foreground">
								<span>{i18n._(activeKind.help)}</span>
								<div className="flex shrink-0 items-center gap-2">
									<span>
										{total === 0 ? 0 : offset + 1}-{Math.min(offset + limit, total)} / {total}
									</span>
									<Button
										variant="outline"
										size="sm"
										disabled={offset === 0}
										onClick={() => setOffset(Math.max(0, offset - limit))}
									>
										<Trans>Previous</Trans>
									</Button>
									<Button
										variant="outline"
										size="sm"
										disabled={offset + limit >= total}
										onClick={() => setOffset(offset + limit)}
									>
										<Trans>Next</Trans>
									</Button>
								</div>
							</div>
						</div>
					</div>
				</div>
			) : null}
		</div>
	)
})

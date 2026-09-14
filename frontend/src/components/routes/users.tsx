import { Trans, useLingui } from "@lingui/react/macro"
import {
	KeyRoundIcon,
	PencilIcon,
	PlusIcon,
	RefreshCwIcon,
	ShieldCheckIcon,
	Trash2Icon,
	UserRoundXIcon,
	UsersIcon,
} from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import {
	Dialog,
	DialogContent,
	DialogDescription,
	DialogFooter,
	DialogHeader,
	DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { api, can } from "@/lib/api"
import { toast } from "@/components/ui/use-toast"

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
	protected: boolean
	permission_count: number
}

type RoleDetail = RoleRecord & { permissions: string[] }

type PermissionRecord = { ability: string; subject: string }

type UserAccess = {
	user_id: string
	device_ids: string[]
	device_group_ids: string[]
	port_ids: string[]
	billing_account_ids: string[]
	aggregate_graph_ids: string[]
	metrics: string[]
}

type AccessOption = { id: string; label: string; description: string }

export default memo(() => {
	const { t } = useLingui()
	const canViewUsers = can("user.view")
	const canViewRoles = can("role.view")
	const [users, setUsers] = useState<UserRecord[]>([])
	const [roles, setRoles] = useState<RoleRecord[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const [editing, setEditing] = useState<UserRecord | null>(null)
	const [creating, setCreating] = useState(false)
	const [editingRole, setEditingRole] = useState<RoleRecord | null>(null)
	const [creatingRole, setCreatingRole] = useState(false)
	const [accessUser, setAccessUser] = useState<UserRecord | null>(null)

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const [userData, roleData] = await Promise.all([
				canViewUsers
					? api.send<{ items?: UserRecord[] }>("/api/v1/users", {})
					: Promise.resolve({ items: [] as UserRecord[] }),
				canViewRoles
					? api.send<{ items?: RoleRecord[] }>("/api/v1/roles", {})
					: Promise.resolve({ items: [] as RoleRecord[] }),
			])
			setUsers(userData.items ?? [])
			setRoles(roleData.items ?? [])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load users`)
		} finally {
			setLoading(false)
		}
	}, [canViewRoles, canViewUsers, t])

	useEffect(() => {
		document.title = `${t`Users & Roles`} / Watchdog`
		refresh()
	}, [refresh, t])

	const disableUser = useCallback(
		async (user: UserRecord) => {
			if (!globalThis.confirm(t`Disable this user? They will lose access until re-enabled.`)) {
				return
			}
			try {
				await api.send(`/api/v1/users/${user.id}`, { method: "PATCH", body: { status: "disabled" } })
				toast({ title: t`User disabled` })
				refresh()
			} catch (err) {
				toast({ title: err instanceof Error ? err.message : t`Request failed`, variant: "destructive" })
			}
		},
		[refresh, t]
	)

	const deleteRole = useCallback(
		async (role: RoleRecord) => {
			if (!globalThis.confirm(t`Delete this role? Its members and permission grants lose it immediately.`)) {
				return
			}
			try {
				await api.send(`/api/v1/roles/${role.id}`, { method: "DELETE" })
				refresh()
			} catch (err) {
				toast({ title: err instanceof Error ? err.message : t`Request failed`, variant: "destructive" })
			}
		},
		[refresh, t]
	)

	const deleteUser = useCallback(
		async (user: UserRecord) => {
			if (!globalThis.confirm(t`Delete this user and all of their resource grants?`)) return
			try {
				await api.send(`/api/v1/users/${user.id}`, { method: "DELETE" })
				toast({ title: t`User deleted` })
				refresh()
			} catch (err) {
				toast({ title: err instanceof Error ? err.message : t`Request failed`, variant: "destructive" })
			}
		},
		[refresh, t]
	)

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<UsersIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Users & Roles</Trans>
					</h1>
				</div>
				<div className="flex items-center gap-2">
					<Button variant="outline" size="sm" onClick={() => setCreating(true)} disabled={!can("user.create")}>
						<PlusIcon className="me-2 h-4 w-4" />
						<Trans>Add User</Trans>
					</Button>
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
			</div>
			{error ? <div className="text-sm text-destructive">{error}</div> : null}

			<div
				className={
					canViewUsers && canViewRoles
						? "grid items-start gap-4 lg:grid-cols-[minmax(0,1fr)_300px]"
						: "grid gap-4"
				}
			>
				{canViewUsers ? (
					<div className="overflow-hidden rounded-md border border-border bg-card">
					<Table>
						<TableHeader>
							<TableRow>
								<TableHead>
									<Trans>Email</Trans>
								</TableHead>
								<TableHead>
									<Trans>Name</Trans>
								</TableHead>
								<TableHead>
									<Trans>Status</Trans>
								</TableHead>
								<TableHead>
									<Trans>Roles</Trans>
								</TableHead>
								<TableHead>
									<Trans>Username</Trans>
								</TableHead>
								<TableHead className="w-24" />
							</TableRow>
						</TableHeader>
						<TableBody>
							{!loading && users.length === 0 ? (
								<TableRow>
									<TableCell colSpan={6} className="text-muted-foreground">
										<Trans>No users found.</Trans>
									</TableCell>
								</TableRow>
							) : null}
							{users.map((user) => (
								<TableRow key={user.id}>
									<TableCell className="font-medium">{user.email}</TableCell>
									<TableCell>{user.display_name || "—"}</TableCell>
									<TableCell>
										<Badge variant={user.status === "active" ? "success" : "secondary"}>{user.status}</Badge>
									</TableCell>
									<TableCell>
										<div className="flex flex-wrap gap-1">
											{user.roles.length === 0 ? (
												<span className="text-muted-foreground">—</span>
											) : (
												user.roles.map((roleName) => (
													<Badge key={roleName} variant="outline" className="font-normal">
														{roleName}
													</Badge>
												))
											)}
										</div>
									</TableCell>
									<TableCell className="text-xs text-muted-foreground">{user.username}</TableCell>
								<TableCell>
									<div className="flex items-center gap-1">
										<Button
											variant="ghost"
											size="icon"
											aria-label={t`Edit user`}
											disabled={!can("user.update")}
											onClick={() => setEditing(user)}
										>
											<PencilIcon className="h-4 w-4" />
										</Button>
										<Button
											variant="ghost"
											size="icon"
											aria-label={t`Manage resource access`}
											disabled={!can("user.manage")}
											onClick={() => setAccessUser(user)}
										>
											<KeyRoundIcon className="h-4 w-4" />
										</Button>
										<Button
											variant="ghost"
											size="icon"
											aria-label={t`Disable user`}
											disabled={!can("user.update") || user.status !== "active"}
											onClick={() => disableUser(user)}
										>
											<UserRoundXIcon className="h-4 w-4" />
										</Button>
										<Button
											variant="ghost"
											size="icon"
											aria-label={t`Delete user`}
											disabled={!can("user.delete")}
											onClick={() => deleteUser(user)}
										>
											<Trash2Icon className="h-4 w-4" />
										</Button>
									</div>
									</TableCell>
								</TableRow>
							))}
						</TableBody>
					</Table>
					</div>
				) : null}

				{canViewRoles ? (
					<div className="grid content-start gap-3 rounded-md border border-border p-3">
					<div className="flex items-center gap-2 text-sm font-medium">
						<ShieldCheckIcon className="h-4 w-4 text-muted-foreground" />
						<Trans>Roles</Trans>
					</div>
					<div className="flex justify-end">
						<Button
							variant="outline"
							size="sm"
							disabled={!can("role.create")}
							onClick={() => setCreatingRole(true)}
						>
							<PlusIcon className="me-2 h-4 w-4" />
							<Trans>Add Role</Trans>
						</Button>
					</div>
					<div className="grid gap-1">
						{roles.length === 0 ? (
							<div className="text-sm text-muted-foreground">
								<Trans>No roles yet.</Trans>
							</div>
						) : null}
						{roles.map((role) => (
							<div
								key={role.id}
								className="flex items-center justify-between rounded-md px-2 py-1.5 hover:bg-muted/60"
							>
								<div className="min-w-0">
									<div className="truncate text-sm">{role.title || role.name}</div>
									<div className="text-xs text-muted-foreground">{role.permission_count ?? 0} permissions</div>
								</div>
								<div className="flex items-center gap-1">
									<Button
										variant="ghost"
										size="icon"
										aria-label={t`Edit role`}
										disabled={!can("role.update")}
										onClick={() => setEditingRole(role)}
									>
									<PencilIcon className="h-4 w-4" />
									</Button>
									<Button
										variant="ghost"
										size="icon"
										aria-label={t`Delete role`}
										disabled={!can("role.delete") || role.protected}
										onClick={() => deleteRole(role)}
									>
										<Trash2Icon className="h-4 w-4" />
									</Button>
								</div>
							</div>
						))}
					</div>
					<div className="text-xs text-muted-foreground">
						<Trans>Built-in roles cannot be deleted.</Trans>
					</div>
					</div>
				) : null}
			</div>

			{creating ? (
				<UserDialog
					title={t`Add User`}
					roles={roles}
					initialRoles={[]}
					onClose={() => setCreating(false)}
					onSaved={() => {
						setCreating(false)
						refresh()
					}}
				/>
			) : null}
			{editing ? (
				<UserDialog
					title={t`Edit User`}
					user={editing}
					roles={roles}
					initialRoles={editing.roles}
					onClose={() => setEditing(null)}
					onSaved={() => {
						setEditing(null)
						refresh()
					}}
				/>
			) : null}
			{creatingRole ? (
				<RoleDialog
					onClose={() => setCreatingRole(false)}
					onSaved={() => {
						setCreatingRole(false)
						refresh()
					}}
				/>
			) : null}
			{editingRole ? (
				<RoleDialog
					role={editingRole}
					onClose={() => setEditingRole(null)}
					onSaved={() => {
						setEditingRole(null)
						refresh()
					}}
				/>
			) : null}
			{accessUser ? <AccessDialog user={accessUser} onClose={() => setAccessUser(null)} /> : null}
		</div>
	)
})

function UserDialog({
	title,
	user,
	roles,
	initialRoles,
	onClose,
	onSaved,
}: {
	title: string
	user?: UserRecord
	roles: RoleRecord[]
	initialRoles: string[]
	onClose: () => void
	onSaved: () => void
}) {
	const { t } = useLingui()
	const [username, setUsername] = useState(user?.username ?? "")
	const [email, setEmail] = useState(user?.email ?? "")
	const [name, setName] = useState(user?.display_name ?? "")
	const [password, setPassword] = useState("")
	const [status, setStatus] = useState(user?.status ?? "active")
	const [selectedRoles, setSelectedRoles] = useState<string[]>(initialRoles)
	const [saving, setSaving] = useState(false)

	const save = async () => {
		setSaving(true)
		try {
			if (user) {
				await api.send(`/api/v1/users/${user.id}`, {
					method: "PATCH",
					body: {
						email: email.trim(),
						display_name: name.trim(),
						status,
						...(can("user.manage") ? { roles: selectedRoles } : {}),
					},
				})
			} else {
				await api.send("/api/v1/users", {
					method: "POST",
					body: {
						username: username.trim(),
						email: email.trim(),
						display_name: name.trim(),
						password,
						...(can("user.manage") ? { roles: selectedRoles } : {}),
					},
				})
			}
			toast({ title: t`Saved` })
			onSaved()
		} catch (err) {
			toast({ title: err instanceof Error ? err.message : t`Request failed`, variant: "destructive" })
		} finally {
			setSaving(false)
		}
	}

	return (
		<Dialog open onOpenChange={(open) => !open && onClose()}>
			<DialogContent className="max-w-md">
				<DialogHeader>
					<DialogTitle>{title}</DialogTitle>
					<DialogDescription>
						<Trans>Manage the local account and its roles.</Trans>
					</DialogDescription>
				</DialogHeader>
				<div className="grid gap-3">
					<label htmlFor="user-username" className="grid gap-1.5 text-sm">
						<span className="text-muted-foreground">
							<Trans>Username</Trans>
						</span>
						<Input
							id="user-username"
							value={username}
							onChange={(event) => setUsername(event.target.value)}
							disabled={Boolean(user)}
							autoComplete="username"
						/>
					</label>
					<label htmlFor="user-email" className="grid gap-1.5 text-sm">
						<span className="text-muted-foreground">
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
						<span className="text-muted-foreground">
							<Trans>Name</Trans>
						</span>
						<Input id="user-display-name" value={name} onChange={(event) => setName(event.target.value)} />
					</label>
					{!user ? (
						<label htmlFor="user-password" className="grid gap-1.5 text-sm">
							<span className="text-muted-foreground">
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
						<span className="text-muted-foreground">
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
					{can("user.manage") ? <div className="grid gap-1.5 text-sm">
						<span className="text-muted-foreground">
							<Trans>Roles</Trans>
						</span>
						<div className="grid max-h-40 gap-1 overflow-auto rounded-md border border-border p-2">
							{roles.length === 0 ? (
								<span className="text-muted-foreground">
									<Trans>No roles yet.</Trans>
								</span>
							) : null}
							{roles.map((role) => (
								<button
									type="button"
									key={role.id}
									className="flex cursor-pointer items-center gap-2 rounded px-1 py-0.5 text-start hover:bg-muted/60"
									onClick={() =>
										setSelectedRoles((current) =>
											current.includes(role.name)
												? current.filter((name) => name !== role.name)
												: [...current, role.name]
										)
									}
								>
									<Checkbox className="pointer-events-none" checked={selectedRoles.includes(role.name)} />
									<span>{role.title || role.name}</span>
								</button>
							))}
						</div>
					</div> : null}
				</div>
				<DialogFooter>
					<Button variant="outline" onClick={onClose}>
						<Trans>Cancel</Trans>
					</Button>
					<Button
						onClick={save}
						disabled={saving || !email.trim() || (!user && (!username.trim() || password.length < 8))}
					>
						<Trans>Save</Trans>
					</Button>
				</DialogFooter>
			</DialogContent>
		</Dialog>
	)
}

function RoleDialog({ role, onClose, onSaved }: { role?: RoleRecord; onClose: () => void; onSaved: () => void }) {
	const { t } = useLingui()
	const [name, setName] = useState(role?.name ?? "")
	const [title, setTitle] = useState(role?.title ?? "")
	const [permissions, setPermissions] = useState<PermissionRecord[]>([])
	const [selected, setSelected] = useState<string[]>([])
	const [loading, setLoading] = useState(true)
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")

	useEffect(() => {
		let active = true
		const load = async () => {
			setLoading(true)
			setError("")
			try {
				const [catalog, detail] = await Promise.all([
					api.send<{ items: PermissionRecord[] }>("/api/v1/permissions", {}),
					role ? api.send<RoleDetail>(`/api/v1/roles/${role.id}`, {}) : Promise.resolve(undefined),
				])
				if (!active) return
				setPermissions(catalog.items ?? [])
				if (detail) {
					setName(detail.name)
					setTitle(detail.title)
					setSelected(detail.permissions ?? [])
				}
			} catch (cause) {
				if (active) setError(cause instanceof Error ? cause.message : t`Failed to load role`)
			} finally {
				if (active) setLoading(false)
			}
		}
		load()
		return () => {
			active = false
		}
	}, [role, t])

	const grouped = permissions.reduce<Record<string, PermissionRecord[]>>((result, permission) => {
		const section = permission.ability.includes(".view") ? "Visibility & data" : "Operations"
		;(result[section] ??= []).push(permission)
		return result
	}, {})

	const save = async () => {
		setSaving(true)
		try {
			if (role) {
				await api.send(`/api/v1/roles/${role.id}`, {
					method: "PATCH",
					body: role.protected ? { title: title.trim() } : { title: title.trim(), permissions: selected },
				})
			} else {
				await api.send("/api/v1/roles", {
					method: "POST",
					body: { name: name.trim(), title: title.trim(), permissions: selected },
				})
			}
			toast({ title: t`Role saved` })
			onSaved()
		} catch (cause) {
			toast({ title: cause instanceof Error ? cause.message : t`Request failed`, variant: "destructive" })
		} finally {
			setSaving(false)
		}
	}

	return (
		<Dialog open onOpenChange={(open) => !open && onClose()}>
			<DialogContent className="max-w-3xl">
				<DialogHeader>
					<DialogTitle>{role ? t`Edit Role` : t`Add Role`}</DialogTitle>
					<DialogDescription>
						<Trans>View abilities control menu and data visibility; operation abilities control actions. Resource rows are assigned per user.</Trans>
					</DialogDescription>
				</DialogHeader>
				{error ? <div className="text-sm text-destructive">{error}</div> : null}
				<div className="grid gap-3 sm:grid-cols-2">
					<label className="grid gap-1.5 text-sm">
						<span className="text-muted-foreground"><Trans>Role name</Trans></span>
						<Input value={name} disabled={Boolean(role)} onChange={(event) => setName(event.target.value)} placeholder="network-operator" />
					</label>
					<label className="grid gap-1.5 text-sm">
						<span className="text-muted-foreground"><Trans>Display title</Trans></span>
						<Input value={title} disabled={role?.protected} onChange={(event) => setTitle(event.target.value)} placeholder={t`Network Operator`} />
					</label>
				</div>
				<div className="grid max-h-[52vh] gap-4 overflow-auto rounded-md border border-border p-3">
					{loading ? <div className="text-sm text-muted-foreground"><Trans>Loading...</Trans></div> : null}
					{Object.entries(grouped).map(([section, values]) => (
						<div key={section} className="grid gap-2">
							<div className="text-sm font-medium">{section}</div>
							<div className="grid gap-1 sm:grid-cols-2 lg:grid-cols-3">
								{values.map((permission) => (
									<button
										type="button"
										key={permission.ability}
										disabled={role?.protected}
										className="flex items-start gap-2 rounded px-2 py-1.5 text-start hover:bg-muted/60 disabled:cursor-not-allowed disabled:opacity-60"
										onClick={() => setSelected((current) => current.includes(permission.ability) ? current.filter((ability) => ability !== permission.ability) : [...current, permission.ability])}
									>
										<Checkbox className="pointer-events-none mt-0.5" checked={selected.includes(permission.ability)} />
										<span className="min-w-0">
											<span className="block truncate text-sm">{permission.ability}</span>
											<span className="block text-xs text-muted-foreground">{permission.subject}</span>
										</span>
									</button>
								))}
							</div>
						</div>
					))}
				</div>
				{role?.protected ? <div className="text-xs text-muted-foreground"><Trans>Built-in administrator abilities are immutable.</Trans></div> : null}
				<DialogFooter>
					<Button variant="outline" onClick={onClose}><Trans>Cancel</Trans></Button>
					<Button onClick={save} disabled={loading || saving || !name.trim() || role?.protected}><Trans>Save</Trans></Button>
				</DialogFooter>
			</DialogContent>
		</Dialog>
	)
}

const accessKinds = [
	{ kind: "device", field: "device_ids", label: "Devices" },
	{ kind: "device_group", field: "device_group_ids", label: "Device groups" },
	{ kind: "port", field: "port_ids", label: "Ports" },
	{ kind: "billing_account", field: "billing_account_ids", label: "Billing" },
	{ kind: "aggregate_graph", field: "aggregate_graph_ids", label: "Saved graphs" },
	{ kind: "metric", field: "metrics", label: "Metrics" },
] as const

type AccessKind = (typeof accessKinds)[number]

function AccessDialog({ user, onClose }: { user: UserRecord; onClose: () => void }) {
	const { t } = useLingui()
	const [access, setAccess] = useState<UserAccess>()
	const [active, setActive] = useState<AccessKind>(accessKinds[0])
	const [query, setQuery] = useState("")
	const [offset, setOffset] = useState(0)
	const [options, setOptions] = useState<AccessOption[]>([])
	const [total, setTotal] = useState(0)
	const [loading, setLoading] = useState(true)
	const [saving, setSaving] = useState(false)
	const limit = 50

	useEffect(() => {
		let activeRequest = true
		api.send<UserAccess>(`/api/v1/users/${user.id}/access`, {})
			.then((data) => activeRequest && setAccess({
				...data,
				device_ids: data.device_ids ?? [], device_group_ids: data.device_group_ids ?? [], port_ids: data.port_ids ?? [],
				billing_account_ids: data.billing_account_ids ?? [], aggregate_graph_ids: data.aggregate_graph_ids ?? [], metrics: data.metrics ?? [],
			}))
			.catch((cause) => toast({ title: cause instanceof Error ? cause.message : t`Request failed`, variant: "destructive" }))
		return () => { activeRequest = false }
	}, [t, user.id])

	useEffect(() => {
		let activeRequest = true
		setLoading(true)
		api.send<{ items: AccessOption[]; total: number }>(`/api/v1/users/${user.id}/access-options`, {
			query: { type: active.kind, q: query || undefined, limit, offset, sort: "label", order: "asc" },
		})
			.then((data) => {
				if (!activeRequest) return
				setOptions(data.items ?? [])
				setTotal(data.total ?? 0)
			})
			.catch((cause) => activeRequest && toast({ title: cause instanceof Error ? cause.message : t`Request failed`, variant: "destructive" }))
			.finally(() => activeRequest && setLoading(false))
		return () => { activeRequest = false }
	}, [active.kind, offset, query, t, user.id])

	const selected = access?.[active.field] ?? []
	const toggle = (id: string) => {
		if (!access) return
		const values = access[active.field]
		setAccess({ ...access, [active.field]: values.includes(id) ? values.filter((value) => value !== id) : [...values, id] })
	}
	const save = async () => {
		if (!access) return
		setSaving(true)
		try {
			await api.send(`/api/v1/users/${user.id}/access`, {
				method: "PUT",
				body: {
					device_ids: access.device_ids,
					device_group_ids: access.device_group_ids,
					port_ids: access.port_ids,
					billing_account_ids: access.billing_account_ids,
					aggregate_graph_ids: access.aggregate_graph_ids,
					metrics: access.metrics,
				},
			})
			toast({ title: t`Resource access saved` })
			onClose()
		} catch (cause) {
			toast({ title: cause instanceof Error ? cause.message : t`Request failed`, variant: "destructive" })
		} finally {
			setSaving(false)
		}
	}

	return (
		<Dialog open onOpenChange={(open) => !open && onClose()}>
			<DialogContent className="max-w-4xl">
				<DialogHeader>
					<DialogTitle><Trans>Resource access</Trans>: {user.display_name || user.username}</DialogTitle>
					<DialogDescription><Trans>Roles decide operations. These grants decide which rows and measurements the user can access.</Trans></DialogDescription>
				</DialogHeader>
				<div className="grid gap-3 sm:grid-cols-[180px_minmax(0,1fr)]">
					<div className="grid content-start gap-1">
						{accessKinds.map((kind) => (
							<Button key={kind.kind} variant={active.kind === kind.kind ? "secondary" : "ghost"} className="justify-between" onClick={() => { setActive(kind); setOffset(0); setQuery("") }}>
								<span>{kind.label}</span><Badge variant="outline">{access?.[kind.field]?.length ?? 0}</Badge>
							</Button>
						))}
					</div>
					<div className="grid min-w-0 gap-2">
						<div className="flex gap-2">
							<Input value={query} onChange={(event) => { setQuery(event.target.value); setOffset(0) }} placeholder={t`Search available resources`} />
							<Button variant="outline" onClick={() => access && setAccess({ ...access, [active.field]: [] })}><Trans>Clear</Trans></Button>
						</div>
						<div className="h-[360px] overflow-auto rounded-md border border-border">
							{loading ? <div className="p-3 text-sm text-muted-foreground"><Trans>Loading...</Trans></div> : null}
							{!loading && options.length === 0 ? <div className="p-3 text-sm text-muted-foreground"><Trans>No resources found.</Trans></div> : null}
							{options.map((option) => (
								<button key={option.id} type="button" className="flex w-full items-start gap-3 border-b border-border px-3 py-2 text-start last:border-b-0 hover:bg-muted/60" onClick={() => toggle(option.id)}>
									<Checkbox className="pointer-events-none mt-0.5" checked={selected.includes(option.id)} />
									<span className="min-w-0"><span className="block truncate text-sm font-medium">{option.label}</span><span className="block truncate text-xs text-muted-foreground">{option.description || option.id}</span></span>
								</button>
							))}
						</div>
						<div className="flex items-center justify-between text-xs text-muted-foreground">
							<span>{total === 0 ? 0 : offset + 1}-{Math.min(offset + limit, total)} / {total}</span>
							<div className="flex gap-2"><Button variant="outline" size="sm" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - limit))}><Trans>Previous</Trans></Button><Button variant="outline" size="sm" disabled={offset + limit >= total} onClick={() => setOffset(offset + limit)}><Trans>Next</Trans></Button></div>
						</div>
						<div className="text-xs text-muted-foreground">
							{active.kind === "metric" ? <Trans>No metric selection means all metrics allowed by the device/port grants. Once selected, it becomes an allow-list.</Trans> : active.kind === "aggregate_graph" ? <Trans>A saved-graph grant authorizes that graph's aggregate output directly.</Trans> : <Trans>Without a view-all role ability, only selected resources are visible.</Trans>}
						</div>
					</div>
				</div>
				<DialogFooter><Button variant="outline" onClick={onClose}><Trans>Cancel</Trans></Button><Button disabled={!access || saving} onClick={save}><Trans>Save</Trans></Button></DialogFooter>
			</DialogContent>
		</Dialog>
	)
}

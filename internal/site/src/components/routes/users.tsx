import { Trans, useLingui } from "@lingui/react/macro"
import {
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
import { api } from "@/lib/api"
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
}

export default memo(() => {
	const { t } = useLingui()
	const [users, setUsers] = useState<UserRecord[]>([])
	const [roles, setRoles] = useState<RoleRecord[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const [editing, setEditing] = useState<UserRecord | null>(null)
	const [creating, setCreating] = useState(false)
	const [newRoleName, setNewRoleName] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const [userData, roleData] = await Promise.all([
				api.send<{ items?: UserRecord[] }>("/api/v1/users", {}),
				api.send<{ items?: RoleRecord[] }>("/api/v1/roles", {}),
			])
			setUsers(userData.items ?? [])
			setRoles(roleData.items ?? [])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load users`)
		} finally {
			setLoading(false)
		}
	}, [t])

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

	const createRole = useCallback(async () => {
		const name = newRoleName.trim()
		if (!name) {
			return
		}
		try {
			await api.send("/api/v1/roles", { method: "POST", body: { name } })
			setNewRoleName("")
			refresh()
		} catch (err) {
			toast({ title: err instanceof Error ? err.message : t`Request failed`, variant: "destructive" })
		}
	}, [newRoleName, refresh, t])

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
					<Button variant="outline" size="sm" onClick={() => setCreating(true)}>
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

			<div className="grid items-start gap-4 lg:grid-cols-[minmax(0,1fr)_300px]">
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
											<Button variant="ghost" size="icon" aria-label={t`Edit user`} onClick={() => setEditing(user)}>
												<PencilIcon className="h-4 w-4" />
											</Button>
											<Button
												variant="ghost"
												size="icon"
												aria-label={t`Disable user`}
												disabled={user.status !== "active"}
												onClick={() => disableUser(user)}
											>
												<UserRoundXIcon className="h-4 w-4" />
											</Button>
										</div>
									</TableCell>
								</TableRow>
							))}
						</TableBody>
					</Table>
				</div>

				<div className="grid content-start gap-3 rounded-md border border-border p-3">
					<div className="flex items-center gap-2 text-sm font-medium">
						<ShieldCheckIcon className="h-4 w-4 text-muted-foreground" />
						<Trans>Roles</Trans>
					</div>
					<div className="flex items-center gap-2">
						<Input
							placeholder={t`New role name`}
							value={newRoleName}
							onChange={(event) => setNewRoleName(event.target.value)}
							onKeyDown={(event) => event.key === "Enter" && createRole()}
						/>
						<Button variant="outline" size="icon" aria-label={t`Create role`} onClick={createRole}>
							<PlusIcon className="h-4 w-4" />
						</Button>
					</div>
					<div className="grid gap-1">
						{roles.length === 0 ? (
							<div className="text-sm text-muted-foreground">
								<Trans>No roles yet.</Trans>
							</div>
						) : null}
						{roles.map((role) => (
							<div key={role.id} className="flex items-center justify-between rounded-md px-2 py-1.5 hover:bg-muted/60">
								<span className="text-sm">{role.title || role.name}</span>
								<Button
									variant="ghost"
									size="icon"
									aria-label={t`Delete role`}
									disabled={role.protected}
									onClick={() => deleteRole(role)}
								>
									<Trash2Icon className="h-4 w-4" />
								</Button>
							</div>
						))}
					</div>
					<div className="text-xs text-muted-foreground">
						<Trans>Built-in roles cannot be deleted.</Trans>
					</div>
				</div>
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
						roles: selectedRoles,
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
						roles: selectedRoles,
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
					<div className="grid gap-1.5 text-sm">
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
					</div>
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

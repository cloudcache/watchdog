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
import { memo, useCallback, useEffect, useRef, useState } from "react"
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
import { pb } from "@/lib/api"
import { toast } from "@/components/ui/use-toast"

type UserRecord = {
	ID: string
	Email: string
	Name: string
	Status: string
	AuthProvider: string
	ExternalSubjectID: string
}

type RoleRecord = {
	ID: string
	Name: string
	Scope: string
}

export default memo(() => {
	const { t } = useLingui()
	const [users, setUsers] = useState<UserRecord[]>([])
	const [roles, setRoles] = useState<RoleRecord[]>([])
	const [rolesByUser, setRolesByUser] = useState<Record<string, string[]>>({})
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
				pb.send<{ items?: UserRecord[] }>("/api/v1/users", {}),
				pb.send<{ items?: RoleRecord[] }>("/api/v1/roles", {}),
			])
			const nextUsers = userData.items ?? []
			setUsers(nextUsers)
			setRoles(roleData.items ?? [])
			const memberships = await Promise.all(
				nextUsers.map(async (user) => {
					const data = await pb
						.send<{ items?: string[] }>(`/api/v1/users/${user.ID}/roles`, {})
						.catch(() => ({ items: [] as string[] }))
					return [user.ID, data.items ?? []] as const
				})
			)
			setRolesByUser(Object.fromEntries(memberships))
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
				await pb.send(`/api/v1/users/${user.ID}`, { method: "DELETE" })
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
			await pb.send("/api/v1/roles", { method: "POST", body: { name } })
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
				await pb.send(`/api/v1/roles/${role.ID}`, { method: "DELETE" })
				refresh()
			} catch (err) {
				toast({ title: err instanceof Error ? err.message : t`Request failed`, variant: "destructive" })
			}
		},
		[refresh, t]
	)

	const roleName = (roleID: string) => roles.find((role) => role.ID === roleID)?.Name ?? roleID

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
									<Trans>Identity</Trans>
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
								<TableRow key={user.ID}>
									<TableCell className="font-medium">{user.Email}</TableCell>
									<TableCell>{user.Name || "—"}</TableCell>
									<TableCell>
										<Badge variant={user.Status === "active" ? "success" : "secondary"}>{user.Status}</Badge>
									</TableCell>
									<TableCell>
										<div className="flex flex-wrap gap-1">
											{(rolesByUser[user.ID] ?? []).length === 0 ? (
												<span className="text-muted-foreground">—</span>
											) : (
												(rolesByUser[user.ID] ?? []).map((roleID) => (
													<Badge key={roleID} variant="outline" className="font-normal">
														{roleName(roleID)}
													</Badge>
												))
											)}
										</div>
									</TableCell>
									<TableCell className="text-xs text-muted-foreground">
										{user.AuthProvider ? `${user.AuthProvider}:${user.ExternalSubjectID}` : t`Not linked`}
									</TableCell>
									<TableCell>
										<div className="flex items-center gap-1">
											<Button variant="ghost" size="icon" aria-label={t`Edit user`} onClick={() => setEditing(user)}>
												<PencilIcon className="h-4 w-4" />
											</Button>
											<Button
												variant="ghost"
												size="icon"
												aria-label={t`Disable user`}
												disabled={user.Status !== "active"}
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
							<div key={role.ID} className="flex items-center justify-between rounded-md px-2 py-1.5 hover:bg-muted/60">
								<span className="text-sm">{role.Name}</span>
								<Button variant="ghost" size="icon" aria-label={t`Delete role`} onClick={() => deleteRole(role)}>
									<Trash2Icon className="h-4 w-4" />
								</Button>
							</div>
						))}
					</div>
					<div className="text-xs text-muted-foreground">
						<Trans>A role named "admin" grants tenant administration.</Trans>
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
					initialRoles={rolesByUser[editing.ID] ?? []}
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
	const [email, setEmail] = useState(user?.Email ?? "")
	const [name, setName] = useState(user?.Name ?? "")
	const [status, setStatus] = useState(user?.Status ?? "active")
	const [authProvider, setAuthProvider] = useState(user?.AuthProvider ?? "")
	const [externalSubject, setExternalSubject] = useState(user?.ExternalSubjectID ?? "")
	const [selectedRoles, setSelectedRoles] = useState<string[]>(initialRoles)
	const [saving, setSaving] = useState(false)

	// The user list has no per-row ETag, so fetch the single user when the edit
	// dialog opens to capture its weak ETag; it is echoed as If-Match on save so
	// a concurrent edit is rejected (412) instead of silently overwritten.
	const etagRef = useRef("")
	useEffect(() => {
		if (!user?.ID) return
		pb.send<UserRecord>(`/api/v1/users/${user.ID}`, {
			onResponse: (response) => {
				etagRef.current = response.headers.get("ETag") ?? ""
			},
		}).catch(() => {})
	}, [user?.ID])

	const save = async () => {
		setSaving(true)
		try {
			const body = {
				email: email.trim(),
				name: name.trim(),
				status,
				auth_provider: authProvider.trim(),
				external_subject_id: externalSubject.trim(),
			}
			let userID = user?.ID
			if (user) {
				await pb.send(`/api/v1/users/${user.ID}`, {
					method: "PATCH",
					headers: etagRef.current ? { "If-Match": etagRef.current } : undefined,
					body,
				})
			} else {
				const created = await pb.send<UserRecord>("/api/v1/users", { method: "POST", body })
				userID = created.ID
			}
			if (userID) {
				await pb.send(`/api/v1/users/${userID}/roles`, { method: "PUT", body: { role_ids: selectedRoles } })
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
						<Trans>Credentials live in the identity provider; this manages the authorization projection.</Trans>
					</DialogDescription>
				</DialogHeader>
				<div className="grid gap-3">
					<label className="grid gap-1.5 text-sm">
						<span className="text-muted-foreground">
							<Trans>Email</Trans>
						</span>
						<Input value={email} onChange={(event) => setEmail(event.target.value)} placeholder="ops@example.com" />
					</label>
					<label className="grid gap-1.5 text-sm">
						<span className="text-muted-foreground">
							<Trans>Name</Trans>
						</span>
						<Input value={name} onChange={(event) => setName(event.target.value)} />
					</label>
					<label className="grid gap-1.5 text-sm">
						<span className="text-muted-foreground">
							<Trans>Status</Trans>
						</span>
						<Select value={status} onValueChange={setStatus}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="active">active</SelectItem>
								<SelectItem value="disabled">disabled</SelectItem>
							</SelectContent>
						</Select>
					</label>
					<div className="grid grid-cols-2 gap-2">
						<label className="grid gap-1.5 text-sm">
							<span className="text-muted-foreground">
								<Trans>Auth provider</Trans>
							</span>
							<Input
								value={authProvider}
								onChange={(event) => setAuthProvider(event.target.value)}
								placeholder="pocketbase"
							/>
						</label>
						<label className="grid gap-1.5 text-sm">
							<span className="text-muted-foreground">
								<Trans>External subject</Trans>
							</span>
							<Input value={externalSubject} onChange={(event) => setExternalSubject(event.target.value)} />
						</label>
					</div>
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
									key={role.ID}
									className="flex cursor-pointer items-center gap-2 rounded px-1 py-0.5 text-start hover:bg-muted/60"
									onClick={() =>
										setSelectedRoles((current) =>
											current.includes(role.ID) ? current.filter((id) => id !== role.ID) : [...current, role.ID]
										)
									}
								>
									<Checkbox className="pointer-events-none" checked={selectedRoles.includes(role.ID)} />
									<span>{role.Name}</span>
								</button>
							))}
						</div>
					</div>
				</div>
				<DialogFooter>
					<Button variant="outline" onClick={onClose}>
						<Trans>Cancel</Trans>
					</Button>
					<Button onClick={save} disabled={saving || !email.trim()}>
						<Trans>Save</Trans>
					</Button>
				</DialogFooter>
			</DialogContent>
		</Dialog>
	)
}

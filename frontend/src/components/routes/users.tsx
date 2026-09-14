import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
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
import { $router, Link } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button, buttonVariants } from "@/components/ui/button"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
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
	protected: boolean
	permission_count: number
}

export default memo(() => {
	const { t } = useLingui()
	const canViewUsers = can("user.view")
	const canViewRoles = can("role.view")
	const [users, setUsers] = useState<UserRecord[]>([])
	const [roles, setRoles] = useState<RoleRecord[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const roleLabel = useCallback(
		(roleName: string, fallback = roleName) => {
			switch (roleName) {
				case "administrator":
					return t`Administrator`
				case "analyst":
					return t`Analyst`
				case "billing":
					return t`Billing`
				case "operator":
					return t({ message: "Operator", context: "User role" })
				case "viewer":
					return t`Viewer`
				default:
					return fallback
			}
		},
		[t]
	)

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
		} catch (cause) {
			setError(cause instanceof Error ? cause.message : t`Failed to load users`)
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
			if (!globalThis.confirm(t`Disable this user? They will lose access until re-enabled.`)) return
			try {
				await api.send(`/api/v1/users/${user.id}`, { method: "PATCH", body: { status: "disabled" } })
				toast({ title: t`User disabled` })
				refresh()
			} catch (cause) {
				toast({ title: cause instanceof Error ? cause.message : t`Request failed`, variant: "destructive" })
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
			} catch (cause) {
				toast({ title: cause instanceof Error ? cause.message : t`Request failed`, variant: "destructive" })
			}
		},
		[refresh, t]
	)

	const deleteRole = useCallback(
		async (role: RoleRecord) => {
			if (!globalThis.confirm(t`Delete this role? Its members and permission grants lose it immediately.`)) return
			try {
				await api.send(`/api/v1/roles/${role.id}`, { method: "DELETE" })
				refresh()
			} catch (cause) {
				toast({ title: cause instanceof Error ? cause.message : t`Request failed`, variant: "destructive" })
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
					{can("user.create") ? (
						<Link
							href={getPagePath($router, "user_new")}
							className={cn(buttonVariants({ variant: "outline", size: "sm" }))}
						>
							<PlusIcon className="me-2 h-4 w-4" />
							<Trans>Add User</Trans>
						</Link>
					) : null}
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
			</div>
			{error ? <div className="text-sm text-destructive">{error}</div> : null}

			<div
				className={
					canViewUsers && canViewRoles ? "grid items-start gap-4 lg:grid-cols-[minmax(0,1fr)_300px]" : "grid gap-4"
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
									<TableHead className="w-28" />
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
											<Badge variant={user.status === "active" ? "success" : "secondary"}>
												{user.status === "active" ? <Trans>Active</Trans> : <Trans>Disabled</Trans>}
											</Badge>
										</TableCell>
										<TableCell>
											<div className="flex flex-wrap gap-1">
												{user.roles.length === 0 ? (
													<span className="text-muted-foreground">—</span>
												) : (
													user.roles.map((roleName) => (
														<Badge key={roleName} variant="outline" className="font-normal">
															{roleLabel(roleName)}
														</Badge>
													))
												)}
											</div>
										</TableCell>
										<TableCell className="text-xs text-muted-foreground">{user.username}</TableCell>
										<TableCell>
											<div className="flex items-center gap-1">
												{can("user.update") || can("user.manage") ? (
													<Link
														href={getPagePath($router, "user_edit", { id: user.id })}
														className={cn(buttonVariants({ variant: "ghost", size: "icon" }))}
														aria-label={t`Edit user and resource access`}
													>
														<PencilIcon className="h-4 w-4" />
													</Link>
												) : null}
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
							{can("role.create") ? (
								<Link
									href={getPagePath($router, "role_new")}
									className={cn(buttonVariants({ variant: "outline", size: "sm" }))}
								>
									<PlusIcon className="me-2 h-4 w-4" />
									<Trans>Add Role</Trans>
								</Link>
							) : null}
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
										<div className="truncate text-sm">{roleLabel(role.name, role.title || role.name)}</div>
										<div className="text-xs text-muted-foreground">
											<Trans>{role.permission_count ?? 0} permissions</Trans>
										</div>
									</div>
									<div className="flex items-center gap-1">
										{can("role.update") ? (
											<Link
												href={getPagePath($router, "role_edit", { id: role.id })}
												className={cn(buttonVariants({ variant: "ghost", size: "icon" }))}
												aria-label={t`Edit role`}
											>
												<PencilIcon className="h-4 w-4" />
											</Link>
										) : null}
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
		</div>
	)
})

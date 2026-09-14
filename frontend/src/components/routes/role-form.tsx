import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, SaveIcon, ShieldCheckIcon } from "lucide-react"
import { memo, useEffect, useMemo, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { toast } from "@/components/ui/use-toast"
import { api } from "@/lib/api"
import { cn } from "@/lib/utils"

type PermissionRecord = { ability: string; subject: string }
type RoleDetail = { id: string; name: string; title: string; protected: boolean; permissions: string[] }

export default memo(({ id }: { id?: string }) => {
	const { t } = useLingui()
	const [name, setName] = useState("")
	const [title, setTitle] = useState("")
	const [protectedRole, setProtectedRole] = useState(false)
	const [permissions, setPermissions] = useState<PermissionRecord[]>([])
	const [selected, setSelected] = useState<string[]>([])
	const [loading, setLoading] = useState(true)
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")

	useEffect(() => {
		document.title = `${id ? t`Edit Role` : t`Add Role`} / Watchdog`
		let current = true
		Promise.all([
			api.send<{ items?: PermissionRecord[] }>("/api/v1/permissions", {}),
			id ? api.send<RoleDetail>(`/api/v1/roles/${id}`, {}) : Promise.resolve(undefined),
		])
			.then(([catalog, role]) => {
				if (!current) return
				setPermissions(catalog.items ?? [])
				if (role) {
					setName(role.name)
					setTitle(role.title)
					setProtectedRole(role.protected)
					setSelected(role.permissions ?? [])
				}
			})
			.catch((cause) => {
				if (current) setError(cause instanceof Error ? cause.message : t`Failed to load role`)
			})
			.finally(() => {
				if (current) setLoading(false)
			})
		return () => {
			current = false
		}
	}, [id, t])

	const grouped = useMemo(
		() =>
			permissions.reduce<Record<string, PermissionRecord[]>>((result, permission) => {
				const section = permission.ability.includes(".view") ? "Visibility & data" : "Operations"
				if (!result[section]) result[section] = []
				result[section].push(permission)
				return result
			}, {}),
		[permissions]
	)

	const save = async () => {
		setSaving(true)
		setError("")
		try {
			if (id) {
				await api.send(`/api/v1/roles/${id}`, { method: "PATCH", body: { title: title.trim(), permissions: selected } })
			} else {
				await api.send("/api/v1/roles", {
					method: "POST",
					body: { name: name.trim(), title: title.trim(), permissions: selected },
				})
			}
			toast({ title: t`Role saved` })
			navigate(getPagePath($router, "users_admin"))
		} catch (cause) {
			setError(cause instanceof Error ? cause.message : t`Request failed`)
		} finally {
			setSaving(false)
		}
	}

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
					<ShieldCheckIcon className="h-5 w-5 text-muted-foreground" />
					<h1 className="text-xl font-semibold">{id ? <Trans>Edit Role</Trans> : <Trans>Add Role</Trans>}</h1>
				</div>
				<Button onClick={save} disabled={loading || saving || !name.trim() || protectedRole}>
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
						<Trans>Role</Trans>
					</h2>
					<p className="text-sm text-muted-foreground">
						<Trans>
							Visibility abilities control menus and data domains; operation abilities control create, update, delete
							and management actions.
						</Trans>
					</p>
				</div>
				<div className="grid gap-4 sm:grid-cols-2">
					<label htmlFor="role-name" className="grid gap-1.5 text-sm">
						<span>
							<Trans>Role name</Trans>
						</span>
						<Input
							id="role-name"
							value={name}
							disabled={Boolean(id)}
							onChange={(event) => setName(event.target.value)}
							placeholder="network-operator"
						/>
					</label>
					<label htmlFor="role-title" className="grid gap-1.5 text-sm">
						<span>
							<Trans>Display title</Trans>
						</span>
						<Input
							id="role-title"
							value={title}
							disabled={protectedRole}
							onChange={(event) => setTitle(event.target.value)}
							placeholder={t`Network Operator`}
						/>
					</label>
				</div>
				{protectedRole ? (
					<div className="text-sm text-muted-foreground">
						<Trans>Built-in administrator abilities are immutable.</Trans>
					</div>
				) : (
					Object.entries(grouped).map(([section, values]) => (
						<section key={section} className="grid gap-2 rounded-md border border-border p-3">
							<h3 className="text-sm font-medium">{section}</h3>
							<div className="grid gap-1 sm:grid-cols-2 lg:grid-cols-3">
								{values.map((permission) => (
									<button
										type="button"
										key={permission.ability}
										className="flex items-start gap-2 rounded px-2 py-1.5 text-start hover:bg-muted/60"
										onClick={() =>
											setSelected((current) =>
												current.includes(permission.ability)
													? current.filter((ability) => ability !== permission.ability)
													: [...current, permission.ability]
											)
										}
									>
										<Checkbox className="pointer-events-none mt-0.5" checked={selected.includes(permission.ability)} />
										<span className="min-w-0">
											<span className="block truncate text-sm">{permission.ability}</span>
											<span className="block text-xs text-muted-foreground">{permission.subject}</span>
										</span>
									</button>
								))}
							</div>
						</section>
					))
				)}
			</div>
		</div>
	)
})

import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { PencilIcon, PlusIcon, RefreshCwIcon, ShieldCheckIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Button } from "@/components/ui/button"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { pb } from "@/lib/api"

type Permission = {
	ID?: string
	id?: string
	SubjectType?: string
	subject_type?: string
	SubjectID?: string
	subject_id?: string
	ResourceType?: string
	resource_type?: string
	ResourceID?: string
	resource_id?: string
	Actions?: string[]
	actions?: string[]
}

type PermissionsResponse = {
	items?: Permission[]
}

export default memo(() => {
	const { t } = useLingui()
	const [permissions, setPermissions] = useState<Permission[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const data = await pb.send<PermissionsResponse>("/api/v1/permissions", {})
			setPermissions(data.items ?? [])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load permissions`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		document.title = `${t`Permissions`} / WatchDog`
		refresh()
	}, [refresh, t])

	const deletePermission = async (permission: Permission) => {
		if (!window.confirm(t`Delete this permission?`)) {
			return
		}
		const id = permission.ID ?? permission.id ?? ""
		try {
			await pb.send(`/api/v1/permissions/${id}`, {
				method: "DELETE",
				body: permission,
			})
			await refresh()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to delete permission`)
		}
	}

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<ShieldCheckIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Permissions</Trans>
					</h1>
				</div>
				<div className="flex items-center gap-2">
					<Button variant="outline" size="sm" onClick={() => navigate(getPagePath($router, "permission_new"))}>
						<PlusIcon className="me-2 h-4 w-4" />
						<Trans>Create</Trans>
					</Button>
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
			</div>

			<div className="rounded-md border border-border bg-card">
				<Table>
					<TableHeader>
						<TableRow>
							<TableHead>
								<Trans>Subject</Trans>
							</TableHead>
							<TableHead>
								<Trans>Resource</Trans>
							</TableHead>
							<TableHead>
								<Trans>Actions</Trans>
							</TableHead>
							<TableHead className="w-32"></TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{loading ? (
							<TableRow>
								<TableCell colSpan={4} className="text-muted-foreground">
									<Trans>Loading...</Trans>
								</TableCell>
							</TableRow>
						) : error ? (
							<TableRow>
								<TableCell colSpan={4} className="text-destructive">
									{error}
								</TableCell>
							</TableRow>
						) : permissions.length === 0 ? (
							<TableRow>
								<TableCell colSpan={4} className="text-muted-foreground">
									<Trans>No permissions found.</Trans>
								</TableCell>
							</TableRow>
						) : (
							permissions.map((permission) => {
								const id = permission.ID ?? permission.id ?? ""
								return (
									<TableRow key={id}>
										<TableCell className="font-mono text-xs">
											{permission.SubjectType ?? permission.subject_type ?? "—"}:
											{permission.SubjectID ?? permission.subject_id ?? "—"}
										</TableCell>
										<TableCell className="font-mono text-xs">
											{permission.ResourceType ?? permission.resource_type ?? "—"}:
											{permission.ResourceID ?? permission.resource_id ?? "—"}
										</TableCell>
										<TableCell>{(permission.Actions ?? permission.actions ?? []).join(", ") || "—"}</TableCell>
										<TableCell>
											<div className="flex justify-end gap-1">
												<Link
													href={getPagePath($router, "permission_edit", { id })}
													className="inline-flex h-8 w-8 items-center justify-center rounded-md hover:bg-muted"
													aria-label={t`Edit permission`}
												>
													<PencilIcon className="h-4 w-4" />
												</Link>
												<Button
													variant="ghost"
													size="icon"
													onClick={() => deletePermission(permission)}
													aria-label={t`Delete permission`}
												>
													<Trash2Icon className="h-4 w-4" />
												</Button>
											</div>
										</TableCell>
									</TableRow>
								)
							})
						)}
					</TableBody>
				</Table>
			</div>
		</div>
	)
})

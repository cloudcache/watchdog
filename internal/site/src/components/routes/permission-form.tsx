import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, SaveIcon, ShieldCheckIcon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { pb } from "@/lib/api"
import { cn } from "@/lib/utils"

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

type PermissionFormProps = {
	id?: string
}

type FormState = {
	id: string
	subjectType: string
	subjectID: string
	resourceType: string
	resourceID: string
	actions: string[]
}

const availableActions = ["view", "configure", "operate", "export", "admin"]

export default memo(({ id }: PermissionFormProps) => {
	const { t } = useLingui()
	const isEditing = Boolean(id)
	const [form, setForm] = useState<FormState>(() => ({
		id: id ?? createPermissionID(),
		subjectType: "user",
		subjectID: "",
		resourceType: "target",
		resourceID: "",
		actions: ["view"],
	}))
	const [loading, setLoading] = useState(Boolean(id))
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")

	const loadPermission = useCallback(async () => {
		if (!id) {
			return
		}
		setLoading(true)
		setError("")
		try {
			const data = await pb.send<PermissionsResponse>("/api/v1/permissions", {})
			const permission = (data.items ?? []).find((item) => (item.ID ?? item.id) === id)
			if (!permission) {
				throw new Error(t`Permission not found`)
			}
			setForm({
				id: permission.ID ?? permission.id ?? id,
				subjectType: permission.SubjectType ?? permission.subject_type ?? "user",
				subjectID: permission.SubjectID ?? permission.subject_id ?? "",
				resourceType: permission.ResourceType ?? permission.resource_type ?? "target",
				resourceID: permission.ResourceID ?? permission.resource_id ?? "",
				actions: permission.Actions ?? permission.actions ?? ["view"],
			})
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load permission`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	useEffect(() => {
		document.title = `${isEditing ? t`Edit Permission` : t`Create Permission`} / Beszel`
		loadPermission()
	}, [isEditing, loadPermission, t])

	const save = async () => {
		setSaving(true)
		setError("")
		try {
			const body = {
				ID: form.id.trim(),
				SubjectType: form.subjectType,
				SubjectID: form.subjectID.trim(),
				ResourceType: form.resourceType,
				ResourceID: form.resourceID.trim(),
				Actions: form.actions,
			}
			await pb.send("/api/v1/permissions", {
				method: "PUT",
				body,
			})
			navigate(getPagePath($router, "permissions"))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save permission`)
		} finally {
			setSaving(false)
		}
	}

	const update = (patch: Partial<FormState>) => setForm((current) => ({ ...current, ...patch }))
	const canSave = form.id.trim() && form.subjectID.trim() && form.resourceID.trim() && form.actions.length > 0

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex min-w-0 items-center gap-2">
					<Link
						href={getPagePath($router, "permissions")}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to permissions`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<ShieldCheckIcon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="truncate text-xl font-semibold tracking-normal">
						{isEditing ? <Trans>Edit Permission</Trans> : <Trans>Create Permission</Trans>}
					</h1>
				</div>
				<Button size="sm" onClick={save} disabled={loading || saving || !canSave}>
					<SaveIcon className="me-2 h-4 w-4" />
					<Trans>Save</Trans>
				</Button>
			</div>

			{error ? <div className="rounded-md border border-border p-3 text-sm text-destructive">{error}</div> : null}

			<div className="grid gap-4 rounded-md border border-border p-4">
				<div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
					<Field label="ID">
						<Input
							value={form.id}
							onChange={(event) => update({ id: event.target.value })}
							disabled={isEditing || loading}
						/>
					</Field>
					<Field label={t`Subject type`}>
						<Select
							value={form.subjectType}
							onValueChange={(subjectType) => update({ subjectType })}
							disabled={loading}
						>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="user">user</SelectItem>
								<SelectItem value="role">role</SelectItem>
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`Subject ID`}>
						<Input
							value={form.subjectID}
							onChange={(event) => update({ subjectID: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<Field label={t`Resource type`}>
						<Select
							value={form.resourceType}
							onValueChange={(resourceType) => update({ resourceType })}
							disabled={loading}
						>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="tenant">tenant</SelectItem>
								<SelectItem value="target">target</SelectItem>
								<SelectItem value="port">port</SelectItem>
								<SelectItem value="export_task">export_task</SelectItem>
								<SelectItem value="billing_account">billing_account</SelectItem>
								<SelectItem value="billing_period">billing_period</SelectItem>
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`Resource ID`}>
						<Input
							value={form.resourceID}
							onChange={(event) => update({ resourceID: event.target.value })}
							disabled={loading}
						/>
					</Field>
				</div>
				<Field label={t`Actions`}>
					<div className="flex flex-wrap gap-3">
						{availableActions.map((action) => {
							const actionID = `permission-action-${action}`
							return (
								<div
									key={action}
									className="flex items-center gap-2 rounded-md border border-border px-3 py-2 text-sm"
								>
									<Checkbox
										id={actionID}
										checked={form.actions.includes(action)}
										onCheckedChange={(checked) =>
											update({ actions: toggleAction(form.actions, action, checked === true) })
										}
										disabled={loading}
									/>
									<Label htmlFor={actionID}>{action}</Label>
								</div>
							)
						})}
					</div>
				</Field>
			</div>
		</div>
	)
})

function Field({ label, children }: { label: string; children: React.ReactNode }) {
	return (
		<div className="grid gap-1.5">
			<Label>{label}</Label>
			{children}
		</div>
	)
}

function createPermissionID() {
	const bytes = new Uint8Array(8)
	crypto.getRandomValues(bytes)
	return `perm_${Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")}`
}

function toggleAction(actions: string[], action: string, checked: boolean) {
	if (checked) {
		return actions.includes(action) ? actions : [...actions, action]
	}
	return actions.filter((item) => item !== action)
}

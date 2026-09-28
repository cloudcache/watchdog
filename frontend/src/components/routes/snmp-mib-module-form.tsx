import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@/lib/page-path"
import { ArrowLeftIcon, DatabaseIcon, SaveIcon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { api } from "@/lib/api"
import { cn } from "@/lib/utils"

type MIBModule = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	Source?: string
	source?: string
	Version?: string
	version?: string
	Checksum?: string
	checksum?: string
	Enabled?: boolean
	enabled?: boolean
}

type MIBModulesResponse = {
	items?: MIBModule[]
}

type MIBModuleFormProps = {
	id?: string
}

type FormState = {
	id: string
	name: string
	source: string
	version: string
	checksum: string
	enabled: boolean
}

export default memo(({ id }: MIBModuleFormProps) => {
	const { t } = useLingui()
	const isEditing = Boolean(id)
	const [form, setForm] = useState<FormState>(() => ({
		id: id ?? createMIBModuleID(),
		name: "",
		source: "librenms",
		version: "",
		checksum: "",
		enabled: true,
	}))
	const [loading, setLoading] = useState(Boolean(id))
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")

	const loadModule = useCallback(async () => {
		if (!id) {
			return
		}
		setLoading(true)
		setError("")
		try {
			const data = await api.send<MIBModulesResponse>("/api/v1/snmp/mib-modules", {})
			const module = (data.items ?? []).find((item) => (item.ID ?? item.id) === id)
			if (!module) {
				throw new Error(t`MIB module not found`)
			}
			setForm({
				id: module.ID ?? module.id ?? id,
				name: module.Name ?? module.name ?? "",
				source: module.Source ?? module.source ?? "librenms",
				version: module.Version ?? module.version ?? "",
				checksum: module.Checksum ?? module.checksum ?? "",
				enabled: module.Enabled ?? module.enabled ?? false,
			})
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load MIB module`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	useEffect(() => {
		document.title = `${isEditing ? t`Edit MIB Module` : t`Create MIB Module`} / Watchdog`
		loadModule()
	}, [isEditing, loadModule, t])

	const save = async () => {
		setSaving(true)
		setError("")
		try {
			const saved = await api.send<MIBModule>("/api/v1/snmp/mib-modules", {
				method: "PUT",
				body: {
					ID: form.id.trim(),
					Name: form.name.trim(),
					Source: form.source.trim(),
					Version: form.version.trim(),
					Checksum: form.checksum.trim(),
					Enabled: form.enabled,
				},
			})
			navigate(getPagePath($router, "snmp_mib_module_edit", { id: saved.ID ?? saved.id ?? form.id }))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save MIB module`)
		} finally {
			setSaving(false)
		}
	}

	const update = (patch: Partial<FormState>) => setForm((current) => ({ ...current, ...patch }))
	const canSave = form.id.trim() && form.name.trim() && form.source.trim() && form.checksum.trim()

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex min-w-0 items-center gap-2">
					<Link
						href={getPagePath($router, "snmp_mib_modules")}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to MIB modules`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<DatabaseIcon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="truncate text-xl font-semibold tracking-normal">
						{isEditing ? <Trans>Edit MIB Module</Trans> : <Trans>Create MIB Module</Trans>}
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
					<Field label={t`Name`}>
						<Input value={form.name} onChange={(event) => update({ name: event.target.value })} disabled={loading} />
					</Field>
					<Field label={t`Source`}>
						<Input
							value={form.source}
							onChange={(event) => update({ source: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<Field label={t`Version`}>
						<Input
							value={form.version}
							onChange={(event) => update({ version: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<Field label={t`Checksum`}>
						<Input
							value={form.checksum}
							onChange={(event) => update({ checksum: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<div className="flex items-center gap-2 pt-7">
						<Checkbox
							id="mib-enabled"
							checked={form.enabled}
							onCheckedChange={(enabled) => update({ enabled: Boolean(enabled) })}
							disabled={loading}
						/>
						<Label htmlFor="mib-enabled">
							<Trans>Enabled</Trans>
						</Label>
					</div>
				</div>
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

function createMIBModuleID() {
	const bytes = new Uint8Array(8)
	crypto.getRandomValues(bytes)
	return `mib_${Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")}`
}

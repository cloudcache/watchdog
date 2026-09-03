import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, CrosshairIcon, SaveIcon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { KeyValueEditor } from "@/components/key-value-editor"
import { $router, Link, navigate } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { pb } from "@/lib/api"
import { cn } from "@/lib/utils"

type TargetRecord = {
	id: string
	name: string
	kind: string
	host: string
	status: string
	labels?: Record<string, string>
}

type TargetFormProps = {
	id?: string
	defaultKind?: "system" | "network"
}

type FormState = {
	id: string
	name: string
	kind: string
	host: string
	status: string
	labels: Record<string, string>
}

export default memo(({ id, defaultKind }: TargetFormProps) => {
	const { t } = useLingui()
	const isEditing = Boolean(id)
	const initialKind = defaultKind || new URLSearchParams(globalThis.location.search).get("kind") || "system"
	const [form, setForm] = useState<FormState>(() => ({
		id: id ?? createTargetID(),
		name: "",
		kind: initialKind === "network" ? "network" : "system",
		host: "",
		status: "pending",
		labels: {},
	}))
	const [loading, setLoading] = useState(Boolean(id))
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")

	const loadTarget = useCallback(async () => {
		if (!id) {
			return
		}
		setLoading(true)
		setError("")
		try {
			const target = await pb.send<TargetRecord>(`/api/v1/targets/${id}`, {})
			const targetID = target.id || id
			setForm({
				id: targetID,
				name: target.name,
				kind: target.kind || "system",
				host: target.host,
				status: target.status || "pending",
				labels: target.labels ?? {},
			})
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load target`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	useEffect(() => {
		document.title = `${isEditing ? t`Edit Resource` : t`Add Resource`} / Watchdog`
		loadTarget()
	}, [isEditing, loadTarget, t])

	const save = async () => {
		setSaving(true)
		setError("")
		try {
			const body = {
				id: form.id.trim(),
				name: form.name.trim(),
				kind: form.kind,
				host: form.host.trim(),
				status: form.status,
				labels: form.labels,
			}
			const saved = await pb.send<TargetRecord>(id ? `/api/v1/targets/${id}` : "/api/v1/targets", {
				method: id ? "PATCH" : "POST",
				body,
			})
			const savedID = saved.id || form.id
			// Network targets are surfaced on the Network page (device view).
			if (form.kind === "network") {
				navigate(getPagePath($router, "network"))
			} else {
				navigate(getPagePath($router, "target_detail", { id: savedID }))
			}
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save target`)
		} finally {
			setSaving(false)
		}
	}

	const update = (patch: Partial<FormState>) => setForm((current) => ({ ...current, ...patch }))

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex min-w-0 items-center gap-2">
					<Link
						href={id ? getPagePath($router, "target_detail", { id }) : getPagePath($router, "targets")}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to targets`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<CrosshairIcon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="truncate text-xl font-semibold tracking-normal">
						{isEditing ? <Trans>Edit Resource</Trans> : <Trans>Add Resource</Trans>}
					</h1>
				</div>
				<Button size="sm" onClick={save} disabled={loading || saving || !form.id.trim() || !form.name.trim()}>
					<SaveIcon className="me-2 h-4 w-4" />
					<Trans>Save</Trans>
				</Button>
			</div>

			{error ? <div className="rounded-md border border-border p-3 text-sm text-destructive">{error}</div> : null}

			<div className="grid gap-4 rounded-md border border-border p-4">
				<div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
					<Field label={t`Name`}>
						<Input value={form.name} onChange={(event) => update({ name: event.target.value })} disabled={loading} />
					</Field>
					<Field label={t`Type`}>
						<Select value={form.kind} onValueChange={(kind) => update({ kind })} disabled={loading}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="system">system</SelectItem>
								<SelectItem value="network">network</SelectItem>
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`Host`}>
						<Input value={form.host} onChange={(event) => update({ host: event.target.value })} disabled={loading} />
					</Field>
					{isEditing ? (
						<Field label="ID">
							<Input value={form.id} disabled />
						</Field>
					) : null}
					<Field label={t`Status`}>
						<Select value={form.status} onValueChange={(status) => update({ status })} disabled={loading}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="pending">pending</SelectItem>
								<SelectItem value="up">up</SelectItem>
								<SelectItem value="down">down</SelectItem>
								<SelectItem value="paused">paused</SelectItem>
							</SelectContent>
						</Select>
					</Field>
				</div>
				<Field label={t`Labels`}>
					<KeyValueEditor
						value={form.labels}
						onChange={(labels) => update({ labels })}
						disabled={loading}
						keyLabel={t`Label key`}
						valueLabel={t`Label value`}
						addLabel={t`Add label`}
					/>
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

function createTargetID() {
	return crypto.randomUUID()
}

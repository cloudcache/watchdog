import { Trans, useLingui } from "@lingui/react/macro"
import { DatabaseIcon, SaveIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { api } from "@/lib/api"

type TargetRecord = {
	ID?: string
	id?: string
	Name?: string
	name?: string
}

type TargetsResponse = {
	items?: TargetRecord[]
}

type RetentionPolicy = {
	ID?: string
	id?: string
	TargetID?: string
	target_id?: string
	HighPrecisionDays?: number
	high_precision_days?: number
	ManualCleanupEnabled?: boolean
	manual_cleanup_enabled?: boolean
	Notes?: string
	notes?: string
}

type RetentionPoliciesResponse = {
	items?: RetentionPolicy[]
}

type FormState = {
	id: string
	targetID: string
	highPrecisionDays: string
	manualCleanupEnabled: boolean
	notes: string
}

const globalScope = "__global__"

export default memo(() => {
	const { t } = useLingui()
	const [targets, setTargets] = useState<TargetRecord[]>([])
	const [policies, setPolicies] = useState<RetentionPolicy[]>([])
	const [form, setForm] = useState<FormState>({
		id: "",
		targetID: globalScope,
		highPrecisionDays: "400",
		manualCleanupEnabled: true,
		notes: "",
	})
	const [loading, setLoading] = useState(true)
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const [targetData, retentionData] = await Promise.all([
				api.send<TargetsResponse>("/api/v1/devices", {}),
				api.send<RetentionPoliciesResponse>("/api/v1/retention/policies", {}),
			])
			setTargets(targetData.items ?? [])
			setPolicies(retentionData.items ?? [])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load retention policies`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		document.title = `${t`Retention`} / Watchdog`
		refresh()
	}, [refresh, t])

	const edit = (policy: RetentionPolicy) => {
		setForm({
			id: policy.ID ?? policy.id ?? "",
			targetID: policy.TargetID ?? policy.target_id ?? globalScope,
			highPrecisionDays: String(policy.HighPrecisionDays ?? policy.high_precision_days ?? 400),
			manualCleanupEnabled: policy.ManualCleanupEnabled ?? policy.manual_cleanup_enabled ?? true,
			notes: policy.Notes ?? policy.notes ?? "",
		})
	}

	const reset = () => {
		setForm({ id: "", targetID: globalScope, highPrecisionDays: "400", manualCleanupEnabled: true, notes: "" })
	}

	const save = async () => {
		setSaving(true)
		setError("")
		try {
			await api.send<RetentionPolicy>("/api/v1/retention/policies", {
				method: "PUT",
				body: {
					ID: form.id,
					TargetID: form.targetID === globalScope ? "" : form.targetID,
					HighPrecisionDays: Number(form.highPrecisionDays),
					ManualCleanupEnabled: form.manualCleanupEnabled,
					Notes: form.notes,
				},
			})
			reset()
			await refresh()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save retention policy`)
		} finally {
			setSaving(false)
		}
	}

	const deletePolicy = async (policy: RetentionPolicy) => {
		const id = policy.ID ?? policy.id ?? ""
		if (!id || !globalThis.confirm(t`Delete this retention policy?`)) {
			return
		}
		try {
			await api.send(`/api/v1/retention/policies/${id}`, { method: "DELETE" })
			await refresh()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to delete retention policy`)
		}
	}

	const update = (patch: Partial<FormState>) => setForm((current) => ({ ...current, ...patch }))
	const canSave = Number(form.highPrecisionDays) > 0

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<DatabaseIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Retention</Trans>
					</h1>
				</div>
				<Button size="sm" onClick={save} disabled={loading || saving || !canSave}>
					<SaveIcon className="me-2 h-4 w-4" />
					<Trans>Save</Trans>
				</Button>
			</div>

			{error ? <div className="rounded-md border border-border p-3 text-sm text-destructive">{error}</div> : null}

			<div className="grid gap-4 rounded-md border border-border p-4">
				<div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
					<Field label={t`Scope`}>
						<Select value={form.targetID} onValueChange={(targetID) => update({ targetID })} disabled={loading}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value={globalScope}>
									<Trans>Global default</Trans>
								</SelectItem>
								{targets.map((target) => {
									const id = target.ID ?? target.id ?? ""
									return (
										<SelectItem key={id} value={id}>
											{target.Name ?? target.name ?? id}
										</SelectItem>
									)
								})}
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`High precision days`}>
						<Input
							type="number"
							min="1"
							value={form.highPrecisionDays}
							onChange={(event) => update({ highPrecisionDays: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<div className="flex items-center gap-2 pt-7">
						<Checkbox
							id="manual-cleanup"
							checked={form.manualCleanupEnabled}
							onCheckedChange={(value) => update({ manualCleanupEnabled: Boolean(value) })}
							disabled={loading}
						/>
						<Label htmlFor="manual-cleanup">
							<Trans>Manual cleanup</Trans>
						</Label>
					</div>
				</div>
				<Field label={t`Notes`}>
					<Textarea value={form.notes} onChange={(event) => update({ notes: event.target.value })} rows={4} />
				</Field>
			</div>

			<div className="rounded-md border border-border bg-card">
				<div className="grid grid-cols-[1.5fr_1fr_1fr_3rem] gap-3 border-b border-border p-3 text-xs font-medium text-muted-foreground">
					<div>
						<Trans>Scope</Trans>
					</div>
					<div>
						<Trans>High precision days</Trans>
					</div>
					<div>
						<Trans>Cleanup</Trans>
					</div>
					<div></div>
				</div>
				{policies.length === 0 ? (
					<div className="p-3 text-sm text-muted-foreground">
						<Trans>No retention policies found.</Trans>
					</div>
				) : (
					policies.map((policy) => {
						const id = policy.ID ?? policy.id ?? ""
						return (
							<button
								type="button"
								key={id}
								onClick={() => edit(policy)}
								className="grid w-full grid-cols-[1.5fr_1fr_1fr_3rem] gap-3 border-b border-border p-3 text-left text-sm last:border-b-0 hover:bg-muted/40"
							>
								<div>
									{(policy.TargetID ?? policy.target_id)
										? targetName(targets, policy.TargetID ?? policy.target_id ?? "")
										: t`Global default`}
								</div>
								<div>{policy.HighPrecisionDays ?? policy.high_precision_days}</div>
								<div>{(policy.ManualCleanupEnabled ?? policy.manual_cleanup_enabled) ? t`Manual` : t`Disabled`}</div>
								<div className="flex justify-end">
									<Button
										type="button"
										variant="ghost"
										size="icon"
										onClick={(event) => {
											event.stopPropagation()
											deletePolicy(policy)
										}}
										aria-label={t`Delete retention policy`}
									>
										<Trash2Icon className="h-4 w-4" />
									</Button>
								</div>
							</button>
						)
					})
				)}
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

function targetName(targets: TargetRecord[], id: string) {
	const target = targets.find((item) => (item.ID ?? item.id) === id)
	return target?.Name ?? target?.name ?? id
}

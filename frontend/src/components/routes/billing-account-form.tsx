import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, ReceiptTextIcon, SaveIcon } from "lucide-react"
import { memo, useCallback, useEffect, useRef, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { isAdmin, api } from "@/lib/api"
import { cn } from "@/lib/utils"

type BillingAccount = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	Status?: string
	status?: string
	BillingDay?: number
	billing_day?: number
	Aggregation?: string
	aggregation?: string
	ValueMode?: string
	value_mode?: string
	QuotaBytes?: number
	quota_bytes?: number
	Notes?: string
	notes?: string
}

type BillingAccountFormProps = {
	id?: string
}

type FormState = {
	id: string
	name: string
	status: string
	billingDay: string
	aggregation: string
	valueMode: string
	quotaBytes: string
	notes: string
}

export default memo(({ id }: BillingAccountFormProps) => {
	const { t } = useLingui()
	const isEditing = Boolean(id)
	const [form, setForm] = useState<FormState>(() => ({
		id: id ?? createBillingID(),
		name: "",
		status: "active",
		billingDay: "1",
		aggregation: "p95_5m",
		valueMode: "corrected",
		quotaBytes: "",
		notes: "",
	}))
	const [loading, setLoading] = useState(Boolean(id))
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")

	// Weak ETag from the last load, echoed as If-Match on save (optimistic
	// concurrency): a concurrent edit is rejected (412), not overwritten.
	const etagRef = useRef("")

	const loadAccount = useCallback(async () => {
		if (!id) {
			return
		}
		setLoading(true)
		setError("")
		try {
			const account = await api.send<BillingAccount>(`/api/v1/billing/accounts/${id}`, {
				onResponse: (response) => {
					etagRef.current = response.headers.get("ETag") ?? ""
				},
			})
			setForm({
				id: account.ID ?? account.id ?? id,
				name: account.Name ?? account.name ?? "",
				status: account.Status ?? account.status ?? "active",
				billingDay: String(account.BillingDay ?? account.billing_day ?? 1),
				aggregation: account.Aggregation ?? account.aggregation ?? "p95_5m",
				valueMode: account.ValueMode ?? account.value_mode ?? "corrected",
				quotaBytes: String(account.QuotaBytes ?? account.quota_bytes ?? ""),
				notes: account.Notes ?? account.notes ?? "",
			})
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load billing account`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	useEffect(() => {
		document.title = `${isEditing ? t`Edit Billing Account` : t`Create Billing Account`} / Watchdog`
		loadAccount()
	}, [isEditing, loadAccount, t])

	const save = async () => {
		setSaving(true)
		setError("")
		try {
			const body = {
				ID: form.id.trim(),
				Name: form.name.trim(),
				Status: form.status,
				BillingDay: Number(form.billingDay),
				Aggregation: form.aggregation,
				ValueMode: form.valueMode,
				QuotaBytes: Number(form.quotaBytes) || 0,
				Notes: form.notes,
			}
			const saved = await api.send<BillingAccount>(id ? `/api/v1/billing/accounts/${id}` : "/api/v1/billing/accounts", {
				method: id ? "PATCH" : "POST",
				headers: id && etagRef.current ? { "If-Match": etagRef.current } : undefined,
				body,
			})
			navigate(getPagePath($router, "billing_detail", { id: saved.ID ?? saved.id ?? form.id }))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save billing account`)
		} finally {
			setSaving(false)
		}
	}

	const update = (patch: Partial<FormState>) => setForm((current) => ({ ...current, ...patch }))
	const canSave = form.id.trim() && form.name.trim() && Number(form.billingDay) >= 1 && Number(form.billingDay) <= 31

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex min-w-0 items-center gap-2">
					<Link
						href={id ? getPagePath($router, "billing_detail", { id }) : getPagePath($router, "billing")}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to billing`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<ReceiptTextIcon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="truncate text-xl font-semibold tracking-normal">
						{isEditing ? <Trans>Edit Billing Account</Trans> : <Trans>Create Billing Account</Trans>}
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
					<Field label={t`Name`}>
						<Input value={form.name} onChange={(event) => update({ name: event.target.value })} disabled={loading} />
					</Field>
					<Field label={t`Status`}>
						<Select value={form.status} onValueChange={(status) => update({ status })} disabled={loading}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="active">active</SelectItem>
								<SelectItem value="paused">paused</SelectItem>
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`Billing Day`}>
						<Input
							type="number"
							min="1"
							max="31"
							value={form.billingDay}
							onChange={(event) => update({ billingDay: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<Field label={t`Aggregation`}>
						<Select
							value={form.aggregation}
							onValueChange={(aggregation) => update({ aggregation })}
							disabled={loading}
						>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="p95_5m">p95_5m</SelectItem>
								<SelectItem value="avg_5m">avg_5m</SelectItem>
								<SelectItem value="fourth_peak_5m">fourth_peak_5m</SelectItem>
								<SelectItem value="daily_p95">daily_p95</SelectItem>
								<SelectItem value="daily_avg">daily_avg</SelectItem>
								<SelectItem value="total_bytes">total_bytes</SelectItem>
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`Value`}>
						<Select value={form.valueMode} onValueChange={(valueMode) => update({ valueMode })} disabled={loading}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="corrected">corrected</SelectItem>
								{isAdmin() ? <SelectItem value="raw">raw</SelectItem> : null}
								{isAdmin() ? <SelectItem value="both">both</SelectItem> : null}
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`Quota bytes`}>
						<Input
							type="number"
							min="0"
							value={form.quotaBytes}
							onChange={(event) => update({ quotaBytes: event.target.value })}
							disabled={loading}
						/>
					</Field>
					{isEditing ? (
						<Field label="ID">
							<Input value={form.id} disabled />
						</Field>
					) : null}
				</div>
				<Field label={t`Notes`}>
					<Textarea value={form.notes} onChange={(event) => update({ notes: event.target.value })} disabled={loading} />
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

function createBillingID() {
	const bytes = new Uint8Array(8)
	crypto.getRandomValues(bytes)
	return `bill_${Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")}`
}

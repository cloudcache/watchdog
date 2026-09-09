import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, ReceiptTextIcon, SaveIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useRef, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { api } from "@/lib/api"
import { cn } from "@/lib/utils"

type BillingAccount = {
	id: string
	party_id?: string
	name: string
	status: string
	bill_type: "cdr" | "quota"
	algorithm: "95th" | "average" | "total"
	billing_day: number
	timezone: string
	direction: "in" | "out" | "agg"
	default_layer: "raw" | "supplier" | "customer"
	cdr_bps?: number
	quota_bytes?: number
	reconcile_abs: number
	reconcile_percent: number
	ref: string
	notes: string
}
type Party = { id: string; name: string; kind: string; status: string }
type FormState = Omit<
	BillingAccount,
	"id" | "billing_day" | "cdr_bps" | "quota_bytes" | "reconcile_abs" | "reconcile_percent"
> & {
	billing_day: string
	cdr_bps: string
	quota_bytes: string
	reconcile_abs: string
	reconcile_percent: string
}

const emptyForm: FormState = {
	party_id: "",
	name: "",
	status: "active",
	bill_type: "cdr",
	algorithm: "95th",
	billing_day: "1",
	timezone: "UTC",
	direction: "agg",
	default_layer: "customer",
	cdr_bps: "",
	quota_bytes: "",
	reconcile_abs: "0",
	reconcile_percent: "5",
	ref: "",
	notes: "",
}

export default memo(({ id }: { id?: string }) => {
	const { t } = useLingui()
	const [form, setForm] = useState<FormState>(emptyForm)
	const [parties, setParties] = useState<Party[]>([])
	const [loading, setLoading] = useState(Boolean(id))
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")
	const etag = useRef("")
	const load = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const partyPage = await api.send<{ items: Party[] }>("/api/v1/billing/parties", {
				query: { limit: 500, offset: 0, status: "active", sort: "name", order: "asc" },
			})
			setParties(partyPage.items ?? [])
			if (!id) return
			const response = await api.send<{ account: BillingAccount }>(`/api/v1/billing/accounts/${id}`, {
				onResponse: (raw) => {
					etag.current = raw.headers.get("ETag") ?? ""
				},
			})
			const item = response.account
			setForm({
				party_id: item.party_id ?? "",
				name: item.name,
				status: item.status,
				bill_type: item.bill_type,
				algorithm: item.algorithm,
				billing_day: String(item.billing_day),
				timezone: item.timezone,
				direction: item.direction,
				default_layer: item.default_layer,
				cdr_bps: String(item.cdr_bps ?? ""),
				quota_bytes: String(item.quota_bytes ?? ""),
				reconcile_abs: String(item.reconcile_abs),
				reconcile_percent: String(item.reconcile_percent),
				ref: item.ref,
				notes: item.notes,
			})
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load billing account`)
		} finally {
			setLoading(false)
		}
	}, [id, t])
	useEffect(() => {
		document.title = `${id ? t`Edit Billing Account` : t`Create Billing Account`} / Watchdog`
		load()
	}, [id, load, t])
	const update = (patch: Partial<FormState>) => setForm((current) => ({ ...current, ...patch }))
	const save = async () => {
		setSaving(true)
		setError("")
		try {
			const body = {
				party_id: form.party_id || undefined,
				name: form.name.trim(),
				status: form.status,
				bill_type: form.bill_type,
				algorithm: form.algorithm,
				billing_day: Number(form.billing_day),
				timezone: form.timezone.trim(),
				direction: form.direction,
				default_layer: form.default_layer,
				cdr_bps: form.bill_type === "cdr" ? Number(form.cdr_bps) : undefined,
				quota_bytes: form.bill_type === "quota" ? Number(form.quota_bytes) : undefined,
				reconcile_abs: Number(form.reconcile_abs),
				reconcile_percent: Number(form.reconcile_percent),
				ref: form.ref.trim(),
				notes: form.notes.trim(),
			}
			const saved = await api.send<BillingAccount>(id ? `/api/v1/billing/accounts/${id}` : "/api/v1/billing/accounts", {
				method: id ? "PATCH" : "POST",
				headers: id && etag.current ? { "If-Match": etag.current } : undefined,
				body,
			})
			navigate(getPagePath($router, "billing_detail", { id: saved.id }))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save billing account`)
		} finally {
			setSaving(false)
		}
	}
	const remove = async () => {
		if (!id || !etag.current || !window.confirm(t`Delete this billing account?`)) return
		try {
			await api.send(`/api/v1/billing/accounts/${id}`, { method: "DELETE", headers: { "If-Match": etag.current } })
			navigate(getPagePath($router, "billing"))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to delete billing account`)
		}
	}
	const allowance = form.bill_type === "cdr" ? form.cdr_bps : form.quota_bytes
	const validAllowance = allowance.trim() !== "" && Number.isFinite(Number(allowance)) && Number(allowance) >= 0
	const canSave =
		form.name.trim() &&
		form.timezone.trim() &&
		Number(form.billing_day) >= 1 &&
		Number(form.billing_day) <= 31 &&
		validAllowance
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
					<ReceiptTextIcon className="h-5 w-5 text-muted-foreground" />
					<h1 className="truncate text-xl font-semibold">
						{id ? <Trans>Edit Billing Account</Trans> : <Trans>Create Billing Account</Trans>}
					</h1>
				</div>
				<div className="flex gap-2">
					{id ? (
						<Button size="sm" variant="destructive" onClick={remove}>
							<Trash2Icon className="me-2 h-4 w-4" />
							<Trans>Delete</Trans>
						</Button>
					) : null}
					<Button size="sm" onClick={save} disabled={loading || saving || !canSave}>
						<SaveIcon className="me-2 h-4 w-4" />
						<Trans>Save</Trans>
					</Button>
				</div>
			</div>
			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			<div className="grid gap-4 rounded-md border border-border p-4 md:grid-cols-2 xl:grid-cols-3">
				<Field label={t`Name`}>
					<Input value={form.name} onChange={(event) => update({ name: event.target.value })} disabled={loading} />
				</Field>
				<Field label={t`Party`}>
					<Select
						value={form.party_id || "none"}
						onValueChange={(value) => update({ party_id: value === "none" ? "" : value })}
					>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="none">—</SelectItem>
							{parties.map((party) => (
								<SelectItem key={party.id} value={party.id}>
									{party.name} ({party.kind})
								</SelectItem>
							))}
						</SelectContent>
					</Select>
				</Field>
				<Field label={t`Status`}>
					<Select value={form.status} onValueChange={(status) => update({ status })}>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="active">active</SelectItem>
							<SelectItem value="paused">paused</SelectItem>
						</SelectContent>
					</Select>
				</Field>
				<Field label={t`Billing type`}>
					<Select
						value={form.bill_type}
						onValueChange={(value: "cdr" | "quota") =>
							update({
								bill_type: value,
								algorithm: value === "quota" ? "total" : form.algorithm === "total" ? "95th" : form.algorithm,
							})
						}
					>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="cdr">cdr</SelectItem>
							<SelectItem value="quota">quota</SelectItem>
						</SelectContent>
					</Select>
				</Field>
				<Field label={t`Algorithm`}>
					<Select
						value={form.algorithm}
						onValueChange={(value: "95th" | "average" | "total") => update({ algorithm: value })}
					>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							{form.bill_type === "cdr" ? (
								<>
									<SelectItem value="95th">95th</SelectItem>
									<SelectItem value="average">average</SelectItem>
								</>
							) : (
								<SelectItem value="total">total</SelectItem>
							)}
						</SelectContent>
					</Select>
				</Field>
				<Field label={t`Billing Day`}>
					<Input
						type="number"
						min="1"
						max="31"
						value={form.billing_day}
						onChange={(event) => update({ billing_day: event.target.value })}
					/>
				</Field>
				<Field label={t`Timezone`}>
					<Input
						value={form.timezone}
						onChange={(event) => update({ timezone: event.target.value })}
						placeholder="Asia/Singapore"
					/>
				</Field>
				<Field label={t`Direction`}>
					<Select value={form.direction} onValueChange={(value: "in" | "out" | "agg") => update({ direction: value })}>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="in">in</SelectItem>
							<SelectItem value="out">out</SelectItem>
							<SelectItem value="agg">in + out</SelectItem>
						</SelectContent>
					</Select>
				</Field>
				<Field label={t`Default layer`}>
					<Select
						value={form.default_layer}
						onValueChange={(value: "raw" | "supplier" | "customer") => update({ default_layer: value })}
					>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="raw">raw</SelectItem>
							<SelectItem value="supplier">supplier</SelectItem>
							<SelectItem value="customer">customer</SelectItem>
						</SelectContent>
					</Select>
				</Field>
				{form.bill_type === "cdr" ? (
					<Field label="CDR (bps)">
						<Input
							type="number"
							min="0"
							value={form.cdr_bps}
							onChange={(event) => update({ cdr_bps: event.target.value })}
						/>
					</Field>
				) : (
					<Field label={t`Quota bytes`}>
						<Input
							type="number"
							min="0"
							value={form.quota_bytes}
							onChange={(event) => update({ quota_bytes: event.target.value })}
						/>
					</Field>
				)}
				<Field label={t`Absolute threshold`}>
					<Input
						type="number"
						min="0"
						value={form.reconcile_abs}
						onChange={(event) => update({ reconcile_abs: event.target.value })}
					/>
				</Field>
				<Field label={t`Percent threshold`}>
					<Input
						type="number"
						min="0"
						max="100"
						step="0.01"
						value={form.reconcile_percent}
						onChange={(event) => update({ reconcile_percent: event.target.value })}
					/>
				</Field>
				<Field label={t`Reference`}>
					<Input value={form.ref} onChange={(event) => update({ ref: event.target.value })} />
				</Field>
				<div className="md:col-span-2 xl:col-span-3">
					<Field label={t`Notes`}>
						<Textarea value={form.notes} onChange={(event) => update({ notes: event.target.value })} />
					</Field>
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

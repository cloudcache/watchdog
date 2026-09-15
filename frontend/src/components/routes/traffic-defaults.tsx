import { Trans, useLingui } from "@lingui/react/macro"
import { GaugeIcon, SaveIcon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { api } from "@/lib/api"

type TrafficPolicyDefault = {
	ID?: string
	id?: string
	TenantID?: string
	tenant_id?: string
	SideType?: string
	side_type?: string
	BillingBaseBps?: number
	billing_base_bps?: number
	SampleStep?: number
	sample_step?: number
	CorrectionDirection?: string
	correction_direction?: string
	CorrectionMin?: number
	correction_min?: number
	CorrectionMax?: number
	correction_max?: number
}

type TrafficPolicyDefaults = {
	Provider?: TrafficPolicyDefault
	provider?: TrafficPolicyDefault
	Customer?: TrafficPolicyDefault
	customer?: TrafficPolicyDefault
}

const minute = 60 * 1_000_000_000

export default memo(() => {
	const { t } = useLingui()
	const [provider, setProvider] = useState<TrafficPolicyDefault>({ SideType: "provider" })
	const [customer, setCustomer] = useState<TrafficPolicyDefault>({ SideType: "customer" })
	const [loading, setLoading] = useState(true)
	const [saving, setSaving] = useState(false)
	const [message, setMessage] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setMessage("")
		try {
			const data = await api.send<TrafficPolicyDefaults>("/api/v1/network/traffic-policy-defaults", {})
			setProvider(data.Provider ?? data.provider ?? { SideType: "provider" })
			setCustomer(data.Customer ?? data.customer ?? { SideType: "customer" })
		} catch (err) {
			setMessage(err instanceof Error ? err.message : t`Failed to load traffic defaults`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		document.title = `${t`Traffic Defaults`} / Watchdog`
		refresh()
	}, [refresh, t])

	const save = async () => {
		setSaving(true)
		setMessage("")
		try {
			const data = await api.send<TrafficPolicyDefaults>("/api/v1/network/traffic-policy-defaults", {
				method: "PUT",
				body: { Provider: provider, Customer: customer },
			})
			setProvider(data.Provider ?? data.provider ?? provider)
			setCustomer(data.Customer ?? data.customer ?? customer)
			setMessage(t`Saved`)
		} catch (err) {
			setMessage(err instanceof Error ? err.message : t`Failed to save traffic defaults`)
		} finally {
			setSaving(false)
		}
	}

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<GaugeIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Traffic Defaults</Trans>
					</h1>
				</div>
				<Button variant="outline" size="sm" onClick={save} disabled={loading || saving}>
					<SaveIcon className="me-2 h-4 w-4" />
					<Trans>Save</Trans>
				</Button>
			</div>

			{message ? (
				<div className="rounded-md border border-border p-3 text-sm text-muted-foreground">{message}</div>
			) : null}

			<div className="grid gap-4 lg:grid-cols-2">
				<PolicyForm title={t`Provider`} value={provider} onChange={setProvider} />
				<PolicyForm title={t`Customer`} value={customer} onChange={setCustomer} />
			</div>
		</div>
	)
})

function PolicyForm({
	title,
	value,
	onChange,
}: {
	title: string
	value: TrafficPolicyDefault
	onChange: (value: TrafficPolicyDefault) => void
}) {
	const { t } = useLingui()
	const billingBase = value.BillingBaseBps ?? value.billing_base_bps ?? 0
	const sampleStep = value.SampleStep ?? value.sample_step ?? 5 * minute
	const direction = value.CorrectionDirection ?? value.correction_direction ?? "none"
	const correctionMin = value.CorrectionMin ?? value.correction_min ?? 0
	const correctionMax = value.CorrectionMax ?? value.correction_max ?? 0

	const update = (patch: Partial<TrafficPolicyDefault>) => onChange({ ...value, ...patch })

	return (
		<div className="grid gap-3 rounded-md border border-border p-4">
			<h2 className="text-base font-medium">{title}</h2>
			<Field label={t`Billing base bps`}>
				<Input
					type="number"
					value={billingBase}
					onChange={(event) => update({ BillingBaseBps: Number(event.target.value) })}
				/>
			</Field>
			<Field label={t`Sample step`}>
				<Select
					value={String(sampleStep / minute)}
					onValueChange={(value) => update({ SampleStep: Number(value) * minute })}
				>
					<SelectTrigger>
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="1">1m</SelectItem>
						<SelectItem value="5">5m</SelectItem>
					</SelectContent>
				</Select>
			</Field>
			<Field label={t`Correction`}>
				<Select value={direction} onValueChange={(value) => update({ CorrectionDirection: value })}>
					<SelectTrigger>
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="none">
							<Trans>No correction</Trans>
						</SelectItem>
						<SelectItem value="up">
							<Trans>Increase</Trans>
						</SelectItem>
						<SelectItem value="down">
							<Trans>Decrease</Trans>
						</SelectItem>
					</SelectContent>
				</Select>
			</Field>
			<div className="grid gap-3 sm:grid-cols-2">
				<Field label={`${t`Correction min`} (bps)`}>
					<Input
						type="number"
						value={correctionMin}
						onChange={(event) => update({ CorrectionMin: Number(event.target.value) })}
					/>
				</Field>
				<Field label={`${t`Correction max`} (bps)`}>
					<Input
						type="number"
						value={correctionMax}
						onChange={(event) => update({ CorrectionMax: Number(event.target.value) })}
					/>
				</Field>
			</div>
		</div>
	)
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
	return (
		<div className="grid gap-1.5">
			<Label>{label}</Label>
			{children}
		</div>
	)
}

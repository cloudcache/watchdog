import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@/lib/page-path"
import { ArrowLeftIcon, RefreshCwIcon, SaveIcon, SlidersHorizontalIcon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { $router, Link } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { api } from "@/lib/api"
import { formatBitsPerSecond } from "@/lib/metric-format"
import { cn } from "@/lib/utils"

type NetworkDevice = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	SysName?: string
	sys_name?: string
	Vendor?: string
	vendor?: string
	Model?: string
	model?: string
}

type NetworkPort = {
	ID?: string
	id?: string
	DeviceID?: string
	device_id?: string
	IfIndex?: number
	if_index?: number
	IfName?: string
	if_name?: string
	IfAlias?: string
	if_alias?: string
	IfDescr?: string
	if_descr?: string
	SpeedBps?: number
	speed_bps?: number
}

type NetworkPortResponse = {
	port?: NetworkPort
	device?: NetworkDevice
}

type PortPolicy = {
	ID?: string
	id?: string
	PortID?: string
	port_id?: string
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
	Enabled?: boolean
	enabled?: boolean
}

type PortPolicyProps = {
	id: string
}

const minute = 60 * 1_000_000_000

export default memo(({ id }: PortPolicyProps) => {
	const { t } = useLingui()
	const [port, setPort] = useState<NetworkPort | null>(null)
	const [device, setDevice] = useState<NetworkDevice | null>(null)
	const [policy, setPolicy] = useState<PortPolicy | null>(null)
	const [loading, setLoading] = useState(true)
	const [saving, setSaving] = useState(false)
	const [message, setMessage] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setMessage("")
		try {
			const [portData, policyData] = await Promise.all([
				api.send<NetworkPortResponse>(`/api/v1/ports/${id}`, {}),
				api.send<PortPolicy>(`/api/v1/ports/${id}/policy`, {}),
			])
			setPort(portData.port ?? null)
			setDevice(portData.device ?? null)
			setPolicy(policyData)
		} catch (err) {
			setMessage(err instanceof Error ? err.message : t`Failed to load port policy`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	useEffect(() => {
		document.title = `${t`Port Policy`} / Watchdog`
		refresh()
	}, [refresh, t])

	const save = async () => {
		if (!policy) {
			return
		}
		setSaving(true)
		setMessage("")
		try {
			const saved = await api.send<PortPolicy>(`/api/v1/ports/${id}/policy`, {
				method: "PATCH",
				body: normalizePolicy(policy, id),
			})
			setPolicy(saved)
			setMessage(t`Saved`)
		} catch (err) {
			setMessage(err instanceof Error ? err.message : t`Failed to save port policy`)
		} finally {
			setSaving(false)
		}
	}

	const deviceID = device?.ID ?? device?.id ?? port?.DeviceID ?? port?.device_id ?? ""
	const title = port?.IfName ?? port?.if_name ?? port?.IfDescr ?? port?.if_descr ?? id
	const update = (patch: Partial<PortPolicy>) => setPolicy((current) => ({ ...(current ?? {}), ...patch }))

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex min-w-0 items-center gap-2">
					<Link
						href={getPagePath($router, "network_port", { id })}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to network port`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<SlidersHorizontalIcon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="truncate text-xl font-semibold tracking-normal">{title}</h1>
				</div>
				<div className="flex items-center gap-2">
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading || saving}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
					<Button size="sm" onClick={save} disabled={loading || saving || !policy}>
						<SaveIcon className="me-2 h-4 w-4" />
						<Trans>Save</Trans>
					</Button>
				</div>
			</div>

			{message ? (
				<div className="rounded-md border border-border p-3 text-sm text-muted-foreground">{message}</div>
			) : null}

			<div className="grid gap-3 md:grid-cols-4">
				<InfoCell label={t`Device`} value={device?.SysName ?? device?.sys_name ?? device?.Name ?? device?.name} />
				<InfoCell label={t`Vendor`} value={device?.Vendor ?? device?.vendor} />
				<InfoCell label={t`Model`} value={device?.Model ?? device?.model} />
				<InfoCell label={t`ifIndex`} value={formatNumber(port?.IfIndex ?? port?.if_index)} mono />
				<InfoCell label={t`Alias`} value={port?.IfAlias ?? port?.if_alias} />
				<InfoCell label={t`Description`} value={port?.IfDescr ?? port?.if_descr} />
				<InfoCell label={t`Speed`} value={formatBitsPerSecond(port?.SpeedBps ?? port?.speed_bps)} />
				<InfoCell label={t`Device ID`} value={deviceID} mono />
			</div>

			<div className="grid gap-4 rounded-md border border-border p-4">
				<div className="flex items-center justify-between gap-3">
					<h2 className="text-base font-medium">
						<Trans>Traffic Policy</Trans>
					</h2>
					<div className="flex items-center gap-2 text-sm">
						<Switch checked={policyEnabled(policy)} onCheckedChange={(checked) => update({ Enabled: checked })} />
						<span className="text-muted-foreground">
							<Trans>Enabled</Trans>
						</span>
					</div>
				</div>

				<div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
					<Field label={t`Policy view`}>
						<Select value={policySide(policy)} onValueChange={(value) => update({ SideType: value })}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="provider">
									<Trans>Supplier</Trans>
								</SelectItem>
								<SelectItem value="customer">
									<Trans>Customer</Trans>
								</SelectItem>
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`Billing base bps`}>
						<Input
							type="number"
							value={policyBillingBase(policy)}
							onChange={(event) => update({ BillingBaseBps: Number(event.target.value) })}
						/>
					</Field>
					<Field label={t`Sample step`}>
						<Select
							value={String(policySampleStep(policy) / minute)}
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
						<Select value={policyCorrection(policy)} onValueChange={(value) => update({ CorrectionDirection: value })}>
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
					<Field label={`${t`Correction min`} (bps)`}>
						<Input
							type="number"
							value={policyCorrectionMin(policy)}
							onChange={(event) => update({ CorrectionMin: Number(event.target.value) })}
						/>
					</Field>
					<Field label={`${t`Correction max`} (bps)`}>
						<Input
							type="number"
							value={policyCorrectionMax(policy)}
							onChange={(event) => update({ CorrectionMax: Number(event.target.value) })}
						/>
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

function InfoCell({ label, value, mono }: { label: string; value?: string; mono?: boolean }) {
	return (
		<div className="rounded-md border border-border p-3">
			<div className="text-xs text-muted-foreground">{label}</div>
			<div className={cn("mt-1 truncate text-sm", mono && "font-mono text-xs")}>{value || "-"}</div>
		</div>
	)
}

function normalizePolicy(policy: PortPolicy, portID: string): PortPolicy {
	return {
		ID: policy.ID ?? policy.id,
		PortID: portID,
		SideType: policySide(policy),
		BillingBaseBps: policyBillingBase(policy),
		SampleStep: policySampleStep(policy),
		CorrectionDirection: policyCorrection(policy),
		CorrectionMin: policyCorrectionMin(policy),
		CorrectionMax: policyCorrectionMax(policy),
		Enabled: policyEnabled(policy),
	}
}

function policySide(policy: PortPolicy | null) {
	return policy?.SideType ?? policy?.side_type ?? "customer"
}

function policyBillingBase(policy: PortPolicy | null) {
	return (
		policy?.BillingBaseBps ??
		policy?.billing_base_bps ??
		(policySide(policy) === "provider" ? 1024 * 1024 * 1024 : 1000 * 1000 * 1000)
	)
}

function policySampleStep(policy: PortPolicy | null) {
	return policy?.SampleStep ?? policy?.sample_step ?? 5 * minute
}

function policyCorrection(policy: PortPolicy | null) {
	return policy?.CorrectionDirection ?? policy?.correction_direction ?? "none"
}

function policyCorrectionMin(policy: PortPolicy | null) {
	return policy?.CorrectionMin ?? policy?.correction_min ?? 0
}

function policyCorrectionMax(policy: PortPolicy | null) {
	return policy?.CorrectionMax ?? policy?.correction_max ?? 0
}

function policyEnabled(policy: PortPolicy | null) {
	return policy?.Enabled ?? policy?.enabled ?? true
}

function formatNumber(value?: number) {
	return typeof value === "number" ? String(value) : undefined
}

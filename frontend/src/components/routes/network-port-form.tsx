import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, CableIcon, SaveIcon } from "lucide-react"
import { memo, useCallback, useEffect, useRef, useState } from "react"
import { KeyValueEditor } from "@/components/key-value-editor"
import { $router, Link, navigate } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { api } from "@/lib/api"
import { cn } from "@/lib/utils"

type NetworkDevice = {
	ID?: string
	id?: string
	SysName?: string
	sys_name?: string
	Name?: string
	name?: string
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
	AdminStatus?: string
	admin_status?: string
	OperStatus?: string
	oper_status?: string
	SpeedBps?: number
	speed_bps?: number
	Metadata?: Record<string, string>
	metadata?: Record<string, string>
}

type NetworkPortResponse = {
	port?: NetworkPort
	device?: NetworkDevice
}

type NetworkPortFormProps = {
	id: string
}

type FormState = {
	id: string
	ifIndex: string
	ifName: string
	ifAlias: string
	ifDescr: string
	adminStatus: string
	operStatus: string
	speedBps: string
	metadata: Record<string, string>
}

export default memo(({ id }: NetworkPortFormProps) => {
	const { t } = useLingui()
	const [device, setDevice] = useState<NetworkDevice | null>(null)
	const [form, setForm] = useState<FormState>({
		id,
		ifIndex: "",
		ifName: "",
		ifAlias: "",
		ifDescr: "",
		adminStatus: "up",
		operStatus: "up",
		speedBps: "",
		metadata: {},
	})
	const [loading, setLoading] = useState(true)
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")

	// Weak ETag from the last load, echoed as If-Match on save (optimistic
	// concurrency): a concurrent edit is rejected (412), not overwritten.
	const etagRef = useRef("")

	const load = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const data = await api.send<NetworkPortResponse>(`/api/v1/ports/${id}`, {
				onResponse: (response) => {
					etagRef.current = response.headers.get("ETag") ?? ""
				},
			})
			const port = data.port
			setDevice(data.device ?? null)
			setForm({
				id: port?.ID ?? port?.id ?? id ?? "",
				ifIndex: String(port?.IfIndex ?? port?.if_index ?? ""),
				ifName: port?.IfName ?? port?.if_name ?? "",
				ifAlias: port?.IfAlias ?? port?.if_alias ?? "",
				ifDescr: port?.IfDescr ?? port?.if_descr ?? "",
				adminStatus: port?.AdminStatus ?? port?.admin_status ?? "up",
				operStatus: port?.OperStatus ?? port?.oper_status ?? "up",
				speedBps: String(port?.SpeedBps ?? port?.speed_bps ?? ""),
				metadata: port?.Metadata ?? port?.metadata ?? {},
			})
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load network port`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	useEffect(() => {
		document.title = `${t`Edit Network Port`} / Watchdog`
		load()
	}, [load, t])

	const save = async () => {
		setSaving(true)
		setError("")
		try {
			const saved = await api.send<NetworkPort>(`/api/v1/ports/${id}`, {
				method: "PATCH",
				headers: etagRef.current ? { "If-Match": etagRef.current } : undefined,
				body: {
					ID: form.id.trim(),
					IfIndex: Number(form.ifIndex),
					IfName: form.ifName.trim(),
					IfAlias: form.ifAlias.trim(),
					IfDescr: form.ifDescr.trim(),
					AdminStatus: form.adminStatus,
					OperStatus: form.operStatus,
					SpeedBps: Number(form.speedBps) || 0,
					Metadata: form.metadata,
				},
			})
			navigate(getPagePath($router, "network_port", { id: saved.ID ?? saved.id ?? form.id }))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save network port`)
		} finally {
			setSaving(false)
		}
	}

	const update = (patch: Partial<FormState>) => setForm((current) => ({ ...current, ...patch }))
	const currentDeviceID = device?.ID ?? device?.id ?? ""
	const title = form.ifName || form.ifDescr || form.id

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex min-w-0 items-center gap-2">
					<Link
						href={
							id
								? getPagePath($router, "network_port", { id })
								: getPagePath($router, "network_device", { id: currentDeviceID })
						}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to network port`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<CableIcon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="truncate text-xl font-semibold tracking-normal">{title}</h1>
				</div>
				<Button
					size="sm"
					onClick={save}
					disabled={loading || saving || !form.id.trim() || (!form.ifName.trim() && !form.ifDescr.trim())}
				>
					<SaveIcon className="me-2 h-4 w-4" />
					<Trans>Save</Trans>
				</Button>
			</div>

			{error ? <div className="rounded-md border border-border p-3 text-sm text-destructive">{error}</div> : null}

			<div className="grid gap-3 md:grid-cols-3">
				<InfoCell label={t`Device`} value={device?.SysName ?? device?.sys_name ?? device?.Name ?? device?.name} />
				<InfoCell label={t`Device ID`} value={currentDeviceID} mono />
				<InfoCell label="Port ID" value={form.id} mono />
			</div>

			<div className="grid gap-4 rounded-md border border-border p-4">
				<div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
					<Field label="Port ID">
						<Input value={form.id} disabled />
					</Field>
					<Field label={t`ifIndex`}>
						<Input
							type="number"
							value={form.ifIndex}
							onChange={(event) => update({ ifIndex: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<Field label={t`Name`}>
						<Input
							value={form.ifName}
							onChange={(event) => update({ ifName: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<Field label={t`Alias`}>
						<Input
							value={form.ifAlias}
							onChange={(event) => update({ ifAlias: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<Field label={t`Admin`}>
						<Select
							value={form.adminStatus}
							onValueChange={(adminStatus) => update({ adminStatus })}
							disabled={loading}
						>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="up">up</SelectItem>
								<SelectItem value="down">down</SelectItem>
								<SelectItem value="1">1</SelectItem>
								<SelectItem value="2">2</SelectItem>
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`Oper`}>
						<Select value={form.operStatus} onValueChange={(operStatus) => update({ operStatus })} disabled={loading}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="up">up</SelectItem>
								<SelectItem value="down">down</SelectItem>
								<SelectItem value="1">1</SelectItem>
								<SelectItem value="2">2</SelectItem>
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`Speed bps`}>
						<Input
							type="number"
							value={form.speedBps}
							onChange={(event) => update({ speedBps: event.target.value })}
							disabled={loading}
						/>
					</Field>
				</div>
				<Field label={t`Description`}>
					<Textarea
						value={form.ifDescr}
						onChange={(event) => update({ ifDescr: event.target.value })}
						disabled={loading}
						rows={4}
					/>
				</Field>
				<Field label={t`Metadata`}>
					<KeyValueEditor
						value={form.metadata}
						onChange={(metadata) => update({ metadata })}
						disabled={loading}
						keyLabel={t`Metadata key`}
						valueLabel={t`Metadata value`}
						addLabel={t`Add metadata`}
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

function InfoCell({ label, value, mono }: { label: string; value?: string; mono?: boolean }) {
	return (
		<div className="rounded-md border border-border p-3">
			<div className="text-xs text-muted-foreground">{label}</div>
			<div className={cn("mt-1 truncate text-sm", mono && "font-mono text-xs")}>{value || "-"}</div>
		</div>
	)
}

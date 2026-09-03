import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, NetworkIcon, SaveIcon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { KeyValueEditor } from "@/components/key-value-editor"
import { $router, Link, navigate } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { pb } from "@/lib/api"
import { cn } from "@/lib/utils"

type NetworkDevice = {
	ID?: string
	id?: string
	TargetID?: string
	target_id?: string
	Vendor?: string
	vendor?: string
	Model?: string
	model?: string
	Platform?: string
	platform?: string
	OSName?: string
	os_name?: string
	OSVersion?: string
	os_version?: string
	SysName?: string
	sys_name?: string
	SysDescr?: string
	sys_descr?: string
	SysObjectID?: string
	sys_object_id?: string
}

type TargetRecord = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	Host?: string
	host?: string
	Status?: string
	status?: string
	Labels?: Record<string, string>
	labels?: Record<string, string>
}

type NetworkDeviceFormProps = {
	id?: string
}

type FormState = {
	id: string
	targetID: string
	targetName: string
	host: string
	status: string
	labels: Record<string, string>
	vendor: string
	model: string
	platform: string
	osName: string
	osVersion: string
	sysName: string
	sysObjectID: string
	sysDescr: string
}

// One edit page per network box: the target half (name, host, status, labels)
// and the device half (inventory) used to live in two separate forms.
export default memo(({ id }: NetworkDeviceFormProps) => {
	const { t } = useLingui()
	const isEditing = Boolean(id)
	const [form, setForm] = useState<FormState>(() => ({
		id: id ?? "",
		targetID: "",
		targetName: "",
		host: "",
		status: "pending",
		labels: {},
		vendor: "",
		model: "",
		platform: "",
		osName: "",
		osVersion: "",
		sysName: "",
		sysObjectID: "",
		sysDescr: "",
	}))
	const [loading, setLoading] = useState(true)
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")

	const load = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			if (!id) {
				setError(t`Create a network target first; devices are created by discovery or agent reports.`)
				return
			}
			const device = await pb.send<NetworkDevice>(`/api/v1/network/devices/${id}`, {})
			const targetID = device.TargetID ?? device.target_id ?? ""
			let target: TargetRecord = {}
			if (targetID) {
				target = await pb.send<TargetRecord>(`/api/v1/targets/${targetID}`, {}).catch(() => ({}))
			}
			setForm({
				id: device.ID ?? device.id ?? id,
				targetID,
				targetName: target.Name ?? target.name ?? "",
				host: target.Host ?? target.host ?? "",
				status: target.Status ?? target.status ?? "pending",
				labels: target.Labels ?? target.labels ?? {},
				vendor: device.Vendor ?? device.vendor ?? "",
				model: device.Model ?? device.model ?? "",
				platform: device.Platform ?? device.platform ?? "",
				osName: device.OSName ?? device.os_name ?? "",
				osVersion: device.OSVersion ?? device.os_version ?? "",
				sysName: device.SysName ?? device.sys_name ?? "",
				sysObjectID: device.SysObjectID ?? device.sys_object_id ?? "",
				sysDescr: device.SysDescr ?? device.sys_descr ?? "",
			})
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load network device`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	useEffect(() => {
		document.title = `${t`Edit Network Device`} / Watchdog`
		load()
	}, [load, t])

	const save = async () => {
		if (!form.id.trim()) {
			setError(t`Network device ID is required`)
			return
		}
		if (!form.targetID) {
			setError(t`Target is required`)
			return
		}
		if (!form.targetName.trim() || !form.host.trim()) {
			setError(t`Name and host are required`)
			return
		}
		if (!form.sysName.trim() && !form.sysDescr.trim()) {
			setError(t`sysName or sysDescr is required`)
			return
		}
		setSaving(true)
		setError("")
		try {
			await pb.send(`/api/v1/targets/${form.targetID}`, {
				method: "PATCH",
				body: {
					ID: form.targetID,
					Name: form.targetName.trim(),
					Type: "network",
					Host: form.host.trim(),
					Status: form.status,
					Labels: form.labels,
				},
			})
			const body = {
				ID: form.id.trim(),
				TargetID: form.targetID,
				Vendor: form.vendor.trim(),
				Model: form.model.trim(),
				Platform: form.platform.trim(),
				OSName: form.osName.trim(),
				OSVersion: form.osVersion.trim(),
				SysName: form.sysName.trim(),
				SysObjectID: form.sysObjectID.trim(),
				SysDescr: form.sysDescr.trim(),
			}
			const saved = await pb.send<NetworkDevice>(`/api/v1/network/devices/${id}`, {
				method: "PATCH",
				body,
			})
			navigate(getPagePath($router, "network_device", { id: saved.ID ?? saved.id ?? form.id }))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save network device`)
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
						href={id ? getPagePath($router, "network_device", { id }) : getPagePath($router, "network")}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to network devices`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<NetworkIcon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="truncate text-xl font-semibold tracking-normal">
						<Trans>Edit Network Device</Trans>
					</h1>
				</div>
				<Button size="sm" onClick={save} disabled={loading || saving}>
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
					<Field label={t`Target`}>
						<Input value={form.targetID} disabled />
					</Field>
					<Field label={t`Name`}>
						<Input
							value={form.targetName}
							onChange={(event) => update({ targetName: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<Field label={t`Host`}>
						<Input value={form.host} onChange={(event) => update({ host: event.target.value })} disabled={loading} />
					</Field>
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
					<Field label={t`Vendor`}>
						<Input
							value={form.vendor}
							onChange={(event) => update({ vendor: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<Field label={t`Model`}>
						<Input value={form.model} onChange={(event) => update({ model: event.target.value })} disabled={loading} />
					</Field>
					<Field label={t`Platform`}>
						<Input
							value={form.platform}
							onChange={(event) => update({ platform: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<Field label={t`Operating System`}>
						<Input
							value={form.osName}
							onChange={(event) => update({ osName: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<Field label={t`Version`}>
						<Input
							value={form.osVersion}
							onChange={(event) => update({ osVersion: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<Field label="sysName">
						<Input
							value={form.sysName}
							onChange={(event) => update({ sysName: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<Field label="sysObjectID">
						<Input
							value={form.sysObjectID}
							onChange={(event) => update({ sysObjectID: event.target.value })}
							disabled={loading}
						/>
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
				<Field label="sysDescr">
					<Textarea
						value={form.sysDescr}
						onChange={(event) => update({ sysDescr: event.target.value })}
						disabled={loading}
						rows={8}
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

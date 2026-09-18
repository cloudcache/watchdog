import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, NetworkIcon, SaveIcon } from "lucide-react"
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
	TargetID?: string
	target_id?: string
	Name?: string
	name?: string
	Host?: string
	host?: string
	Status?: string
	status?: string
	Labels?: Record<string, string>
	labels?: Record<string, string>
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
	SNMPProfileID?: string
	snmp_profile_id?: string
	SNMPPort?: number
	snmp_port?: number
}

type SNMPProfile = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	Version?: string
	version?: string
}

type SNMPProfilesResponse = {
	items?: SNMPProfile[]
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
	snmpProfileID: string
	snmpPort: string
	snmpCommunity: string
}

// One edit page per network box: target identity, SNMP access, and discovered inventory.
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
		snmpProfileID: "",
		snmpPort: "161",
		snmpCommunity: "",
	}))
	const [snmpProfiles, setSNMPProfiles] = useState<SNMPProfile[]>([])
	const [profilesError, setProfilesError] = useState("")
	const [loading, setLoading] = useState(true)
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")
	// Weak ETag of the device from the last load, echoed as If-Match on save so a
	// concurrent edit is rejected (412) instead of silently overwritten.
	const deviceEtagRef = useRef("")

	const load = useCallback(async () => {
		setLoading(true)
		setError("")
		setProfilesError("")
		try {
			if (!id) {
				setError(t`Create a network target first; devices are created by discovery or agent reports.`)
				return
			}
			const [device, profilesResponse] = await Promise.all([
				api.send<NetworkDevice>(`/api/v1/devices/${id}`, {
					onResponse: (response) => {
						deviceEtagRef.current = response.headers.get("ETag") ?? ""
					},
				}),
				api.send<SNMPProfilesResponse>("/api/v1/snmp/profiles", {}).catch((err) => {
					setProfilesError(err instanceof Error ? err.message : t`Failed to load SNMP profiles`)
					return { items: [] }
				}),
			])
			const profiles = profilesResponse.items ?? []
			setSNMPProfiles(profiles)
			const deviceID = device.ID ?? device.id ?? id
			setForm({
				id: deviceID,
				targetID: deviceID,
				targetName: device.Name ?? device.name ?? "",
				host: device.Host ?? device.host ?? "",
				status: device.Status ?? device.status ?? "pending",
				labels: device.Labels ?? device.labels ?? {},
				vendor: device.Vendor ?? device.vendor ?? "",
				model: device.Model ?? device.model ?? "",
				platform: device.Platform ?? device.platform ?? "",
				osName: device.OSName ?? device.os_name ?? "",
				osVersion: device.OSVersion ?? device.os_version ?? "",
				sysName: device.SysName ?? device.sys_name ?? "",
				sysObjectID: device.SysObjectID ?? device.sys_object_id ?? "",
				sysDescr: device.SysDescr ?? device.sys_descr ?? "",
				snmpProfileID: device.SNMPProfileID ?? device.snmp_profile_id ?? "",
				snmpPort: String(device.SNMPPort ?? device.snmp_port ?? 161),
				snmpCommunity: "",
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
		if (!form.host.trim()) {
			setError(t`Host is required`)
			return
		}
		if (!form.snmpProfileID || Number(form.snmpPort) < 1 || Number(form.snmpPort) > 65535) {
			setError(t`A valid SNMP profile and port are required`)
			return
		}
		setSaving(true)
		setError("")
		try {
			const body: Record<string, unknown> = {
				display_name: form.targetName.trim(),
				kind: "network",
				host: form.host.trim(),
				status: form.status,
				labels: form.labels,
				vendor: form.vendor.trim(),
				model: form.model.trim(),
				platform: form.platform.trim(),
				os: form.osName.trim(),
				os_version: form.osVersion.trim(),
				sys_name: form.sysName.trim(),
				sys_object_id: form.sysObjectID.trim(),
				sys_descr: form.sysDescr.trim(),
				snmp_profile_id: form.snmpProfileID,
				snmp_port: Number(form.snmpPort),
			}
			if (form.snmpCommunity) {
				body.snmp_security = { community: form.snmpCommunity }
			}
			const saved = await api.send<NetworkDevice>(`/api/v1/devices/${form.id}`, {
				method: "PATCH",
				headers: deviceEtagRef.current ? { "If-Match": deviceEtagRef.current } : undefined,
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
					<Field label={t`Display name (optional)`}>
						<Input
							value={form.targetName}
							onChange={(event) => update({ targetName: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<Field label={t`Host / IP`}>
						<Input value={form.host} onChange={(event) => update({ host: event.target.value })} disabled={loading} />
					</Field>
					<Field label={t`SNMP profile`}>
						<Select
							value={form.snmpProfileID}
							onValueChange={(snmpProfileID) => update({ snmpProfileID })}
							disabled={loading || snmpProfiles.length === 0}
						>
							<SelectTrigger>
								<SelectValue placeholder={t`Select SNMP profile`} />
							</SelectTrigger>
							<SelectContent>
								{snmpProfiles.map((profile) => (
									<SelectItem key={profileID(profile)} value={profileID(profile)}>
										{profile.Name ?? profile.name ?? profileID(profile)} ({profile.Version ?? profile.version ?? "SNMP"}
										)
									</SelectItem>
								))}
							</SelectContent>
						</Select>
						{profilesError ? <p className="text-xs text-destructive">{profilesError}</p> : null}
					</Field>
					<Field label={t`SNMP port`}>
						<Input
							type="number"
							min="1"
							max="65535"
							value={form.snmpPort}
							onChange={(event) => update({ snmpPort: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<Field label={t`SNMP community`}>
						<Input
							type="password"
							autoComplete="new-password"
							value={form.snmpCommunity}
							onChange={(event) => update({ snmpCommunity: event.target.value })}
							disabled={loading}
						/>
						<p className="text-xs text-muted-foreground">
							<Trans>Leave blank to keep the current value or inherit the profile default.</Trans>
						</p>
					</Field>
					<Field label={t`Status`}>
						<Input value={form.status || "pending"} disabled />
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

function profileID(profile: SNMPProfile) {
	return profile.ID ?? profile.id ?? ""
}

import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, RadarIcon, SaveIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { api } from "@/lib/api"
import { cn } from "@/lib/utils"

type NetworkDevice = {
	ID?: string
	id?: string
	TargetID?: string
	target_id?: string
	SysName?: string
	sys_name?: string
	Model?: string
	model?: string
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

type DiscoveryResult = {
	Ports?: unknown[]
	ports?: unknown[]
	DeletedPorts?: number
	deleted_ports?: number
}

export default memo(() => {
	const { t } = useLingui()
	const [devices, setDevices] = useState<NetworkDevice[]>([])
	const [profiles, setProfiles] = useState<SNMPProfile[]>([])
	const [deviceID, setDeviceID] = useState("")
	const [profileID, setProfileID] = useState("")
	const [community, setCommunity] = useState("")
	const [snmpPort, setSNMPPort] = useState("161")
	const [loading, setLoading] = useState(true)
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")
	const [summary, setSummary] = useState("")

	const selectedDevice = useMemo(() => devices.find((device) => deviceIDOf(device) === deviceID), [deviceID, devices])

	const load = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const [deviceData, profileData] = await Promise.all([
				api.send<{ items?: NetworkDevice[] }>("/api/v1/network/devices", {}),
				api.send<{ items?: SNMPProfile[] }>("/api/v1/snmp/profiles", {}),
			])
			const nextDevices = deviceData.items ?? []
			const nextProfiles = profileData.items ?? []
			setDevices(nextDevices)
			setProfiles(nextProfiles)
			const firstDevice = nextDevices[0]
			if (firstDevice) {
				const nextDeviceID = deviceIDOf(firstDevice)
				setDeviceID((current) => current || nextDeviceID)
				setProfileID((current) => current || firstDevice.SNMPProfileID || firstDevice.snmp_profile_id || profileIDOf(nextProfiles[0]) || "")
				setSNMPPort(String(firstDevice.SNMPPort ?? firstDevice.snmp_port ?? 161))
			}
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load discovery inputs`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		document.title = `${t`Discover Network Device`} / Watchdog`
		load()
	}, [load, t])

	useEffect(() => {
		if (!selectedDevice) {
			return
		}
		setProfileID(selectedDevice.SNMPProfileID || selectedDevice.snmp_profile_id || profileIDOf(profiles[0]) || "")
		setSNMPPort(String(selectedDevice.SNMPPort ?? selectedDevice.snmp_port ?? 161))
	}, [deviceID])

	const save = async () => {
		if (!deviceID) {
			setError(t`Select a network device`)
			return
		}
		if (!profileID) {
			setError(t`Select an SNMP profile`)
			return
		}
		setSaving(true)
		setError("")
		setSummary("")
		try {
			await api.send(`/api/v1/network/devices/${deviceID}/snmp`, {
				method: "PATCH",
				body: {
					SNMPProfileID: profileID,
					SNMPPort: Number(snmpPort) || 161,
					SNMPSecurity: community.trim() ? { community: community.trim() } : undefined,
				},
			})
			const result = await api.send<DiscoveryResult>(`/api/v1/network/devices/${deviceID}/snmp/discover`, { method: "POST" })
			const ports = result.Ports ?? result.ports ?? []
			const deleted = result.DeletedPorts ?? result.deleted_ports ?? 0
			setSummary(t`${ports.length} ports imported, ${deleted} stale ports removed`)
			navigate(getPagePath($router, "network_device", { id: deviceID }))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to discover network device`)
		} finally {
			setSaving(false)
		}
	}

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex min-w-0 items-center gap-2">
					<Link
						href={getPagePath($router, "network")}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to network devices`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<RadarIcon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="truncate text-xl font-semibold tracking-normal">
						<Trans>Discover Network Device</Trans>
					</h1>
				</div>
				<Button size="sm" onClick={save} disabled={loading || saving || !deviceID || !profileID}>
					<SaveIcon className="me-2 h-4 w-4" />
					<Trans>Discover</Trans>
				</Button>
			</div>

			{error ? <div className="rounded-md border border-border p-3 text-sm text-destructive">{error}</div> : null}
			{summary ? <div className="rounded-md border border-border p-3 text-sm">{summary}</div> : null}

			<div className="grid gap-4 rounded-md border border-border p-4">
				<div className="grid gap-4 md:grid-cols-2">
					<Field label={t`Device`}>
						<Select value={deviceID} onValueChange={setDeviceID} disabled={loading}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								{devices.map((device) => (
									<SelectItem key={deviceIDOf(device)} value={deviceIDOf(device)}>
										{deviceLabel(device)}
									</SelectItem>
								))}
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`SNMP Profile`}>
						<Select value={profileID} onValueChange={setProfileID} disabled={loading}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								{profiles.map((profile) => (
									<SelectItem key={profileIDOf(profile)} value={profileIDOf(profile)}>
										{profileLabel(profile)}
									</SelectItem>
								))}
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`SNMP Port`}>
						<Input type="number" min={1} max={65535} value={snmpPort} onChange={(event) => setSNMPPort(event.target.value)} />
					</Field>
					<Field label={t`Community Override`}>
						<Input value={community} onChange={(event) => setCommunity(event.target.value)} disabled={loading} />
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

function deviceIDOf(device: NetworkDevice) {
	return device.ID ?? device.id ?? ""
}

function profileIDOf(profile?: SNMPProfile) {
	return profile?.ID ?? profile?.id ?? ""
}

function deviceLabel(device: NetworkDevice) {
	return [device.SysName ?? device.sys_name ?? deviceIDOf(device), device.Model ?? device.model].filter(Boolean).join(" ")
}

function profileLabel(profile: SNMPProfile) {
	return [profile.Name ?? profile.name ?? profileIDOf(profile), profile.Version ?? profile.version].filter(Boolean).join(" ")
}

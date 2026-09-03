import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, PencilIcon, RadarIcon, RefreshCwIcon, SaveIcon, SlidersHorizontalIcon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { $router, Link } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { pb } from "@/lib/api"
import { cn } from "@/lib/utils"

type NetworkDevice = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	TargetID?: string
	target_id?: string
	Vendor?: string
	vendor?: string
	Model?: string
	model?: string
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

type TargetRecord = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	Host?: string
	host?: string
}

type TargetsResponse = {
	items?: TargetRecord[]
}

type SNMPDiscoverResponse = {
	count?: number
	ports?: unknown[]
}

type DeviceSNMPProps = {
	id: string
}

export default memo(({ id }: DeviceSNMPProps) => {
	const { t } = useLingui()
	const [device, setDevice] = useState<NetworkDevice | null>(null)
	const [target, setTarget] = useState<TargetRecord | null>(null)
	const [profiles, setProfiles] = useState<SNMPProfile[]>([])
	const [snmpProfileID, setSNMPProfileID] = useState("")
	const [snmpPort, setSNMPPort] = useState("161")
	const [snmpCommunity, setSNMPCommunity] = useState("")
	const [loading, setLoading] = useState(true)
	const [saving, setSaving] = useState(false)
	const [discovering, setDiscovering] = useState(false)
	const [message, setMessage] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setMessage("")
		try {
			const [deviceData, profilesData, targetsData] = await Promise.all([
				pb.send<NetworkDevice>(`/api/v1/network/devices/${id}`, {}),
				pb.send<SNMPProfilesResponse>("/api/v1/snmp/profiles", {}),
				pb.send<TargetsResponse>("/api/v1/targets", {}),
			])
			setDevice(deviceData)
			const targetID = deviceData.TargetID ?? deviceData.target_id ?? ""
			setTarget((targetsData.items ?? []).find((item) => (item.ID ?? item.id) === targetID) ?? null)
			setProfiles(profilesData.items ?? [])
			setSNMPProfileID(deviceData.SNMPProfileID ?? deviceData.snmp_profile_id ?? "")
			setSNMPPort(String(deviceData.SNMPPort ?? deviceData.snmp_port ?? 161))
			setSNMPCommunity("")
		} catch (err) {
			setMessage(err instanceof Error ? err.message : t`Failed to load SNMP settings`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	useEffect(() => {
		document.title = `${t`Device SNMP`} / Watchdog`
		refresh()
	}, [refresh, t])

	const save = async () => {
		setSaving(true)
		setMessage("")
		try {
			const body: {
				SNMPProfileID?: string
				SNMPPort?: number
				SNMPSecurity?: { community?: string }
			} = {
				SNMPProfileID: snmpProfileID,
				SNMPPort: Number(snmpPort),
			}
			if (snmpCommunity) {
				body.SNMPSecurity = { community: snmpCommunity }
			}
			const updated = await pb.send<NetworkDevice>(`/api/v1/network/devices/${id}/snmp`, {
				method: "PATCH",
				body,
			})
			setDevice(updated)
			setSNMPCommunity("")
			setMessage(t`Saved`)
		} catch (err) {
			setMessage(err instanceof Error ? err.message : t`Failed to save SNMP settings`)
		} finally {
			setSaving(false)
		}
	}

	const discoverInterfaces = async () => {
		setDiscovering(true)
		setMessage("")
		try {
			const result = await pb.send<SNMPDiscoverResponse>(`/api/v1/network/devices/${id}/snmp/discover`, {
				method: "POST",
			})
			setMessage(t`Discovered ${result.count ?? result.ports?.length ?? 0} interfaces`)
		} catch (err) {
			setMessage(err instanceof Error ? err.message : t`Failed to discover interfaces`)
		} finally {
			setDiscovering(false)
		}
	}

	const targetID = device?.TargetID ?? device?.target_id ?? ""
	const targetName = target?.Name ?? target?.name ?? targetID
	const targetHost = target?.Host ?? target?.host ?? ""
	const title = firstValue(device?.SysName, device?.sys_name, device?.Name, device?.name, targetName, id)
	const selectedProfile = profiles.find((profile) => (profile.ID ?? profile.id) === snmpProfileID)

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex min-w-0 items-center gap-2">
					<Link
						href={getPagePath($router, "network_device", { id })}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to network device`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<SlidersHorizontalIcon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="truncate text-xl font-semibold tracking-normal">{title}</h1>
				</div>
				<div className="flex items-center gap-2">
					{targetID ? (
						<Link
							href={getPagePath($router, "target_edit", { id: targetID })}
							className={cn(buttonVariants({ variant: "outline", size: "sm" }))}
						>
							<PencilIcon className="me-2 h-4 w-4" />
							<Trans>Target</Trans>
						</Link>
					) : null}
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading || saving}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
					<Button variant="outline" size="sm" onClick={discoverInterfaces} disabled={loading || saving || discovering}>
						<RadarIcon className="me-2 h-4 w-4" />
						<Trans>Discover Interfaces</Trans>
					</Button>
					<Button size="sm" onClick={save} disabled={loading || saving}>
						<SaveIcon className="me-2 h-4 w-4" />
						<Trans>Save</Trans>
					</Button>
				</div>
			</div>

			{message ? (
				<div className="rounded-md border border-border p-3 text-sm text-muted-foreground">{message}</div>
			) : null}

			<div className="grid gap-3 md:grid-cols-4">
				<InfoCell label={t`Vendor`} value={device?.Vendor ?? device?.vendor} />
				<InfoCell label={t`Model`} value={device?.Model ?? device?.model} />
				<InfoCell label="sysObjectID" value={device?.SysObjectID ?? device?.sys_object_id} />
				<InfoLinkCell
					label={t`Target`}
					value={targetName}
					detail={targetHost}
					href={targetID ? getPagePath($router, "target_detail", { id: targetID }) : ""}
				/>
			</div>

			<div className="grid gap-4 rounded-md border border-border p-4">
				<h2 className="text-base font-medium">SNMP</h2>
				<div className="grid gap-4 md:grid-cols-[1fr_10rem]">
					<Field label="SNMP Profile">
						<Select
							value={snmpProfileID || "none"}
							onValueChange={(value) => setSNMPProfileID(value === "none" ? "" : value)}
						>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="none">none</SelectItem>
								{profiles.map((profile) => (
									<SelectItem key={profile.ID ?? profile.id} value={profile.ID ?? profile.id ?? ""}>
										{profile.Name ?? profile.name ?? profile.ID ?? profile.id} (
										{profile.Version ?? profile.version ?? "SNMP"})
									</SelectItem>
								))}
							</SelectContent>
						</Select>
					</Field>
					<Field label="SNMP Port">
						<Input
							type="number"
							min={1}
							max={65535}
							value={snmpPort}
							onChange={(event) => setSNMPPort(event.target.value)}
						/>
					</Field>
				</div>
				<Field label="Community Override">
					<Input type="password" value={snmpCommunity} onChange={(event) => setSNMPCommunity(event.target.value)} />
				</Field>
				<div className="grid gap-3 md:grid-cols-3">
					<InfoCell
						label="Selected profile"
						value={selectedProfile?.Name ?? selectedProfile?.name ?? snmpProfileID}
						mono
					/>
					<InfoCell label="Effective port" value={snmpPort} mono />
					<InfoCell label="Secret update" value={snmpCommunity ? "pending" : "unchanged"} />
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

function InfoLinkCell({ label, value, detail, href }: { label: string; value?: string; detail?: string; href: string }) {
	const content = (
		<>
			<div className="mt-1 truncate text-sm">{value || "-"}</div>
			{detail ? <div className="mt-1 truncate font-mono text-xs text-muted-foreground">{detail}</div> : null}
		</>
	)
	return (
		<div className="rounded-md border border-border p-3">
			<div className="text-xs text-muted-foreground">{label}</div>
			{href ? (
				<Link href={href} className="block hover:underline">
					{content}
				</Link>
			) : (
				content
			)}
		</div>
	)
}

function firstValue(...values: (string | undefined)[]) {
	return values.find((value) => value && value.trim()) ?? "-"
}

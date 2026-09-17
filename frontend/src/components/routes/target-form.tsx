import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, CrosshairIcon, SaveIcon } from "lucide-react"
import { memo, useCallback, useEffect, useRef, useState } from "react"
import { KeyValueEditor } from "@/components/key-value-editor"
import { $router, Link, navigate } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { api } from "@/lib/api"
import { cn } from "@/lib/utils"

type TargetRecord = {
	id: string
	name: string
	kind: string
	host: string
	status: string
	labels?: Record<string, string>
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

type TargetFormProps = {
	id?: string
	defaultKind?: "system" | "network"
}

type FormState = {
	id: string
	name: string
	kind: string
	host: string
	status: string
	labels: Record<string, string>
	snmpProfileID: string
	snmpPort: string
	snmpCommunity: string
}

export default memo(({ id, defaultKind }: TargetFormProps) => {
	const { t } = useLingui()
	const isEditing = Boolean(id)
	const initialKind = defaultKind || new URLSearchParams(globalThis.location.search).get("kind") || "system"
	const [form, setForm] = useState<FormState>(() => ({
		id: id ?? "",
		name: "",
		kind: initialKind === "network" ? "network" : "system",
		host: "",
		status: "pending",
		labels: {},
		snmpProfileID: "",
		snmpPort: "161",
		snmpCommunity: "",
	}))
	const [snmpProfiles, setSNMPProfiles] = useState<SNMPProfile[]>([])
	const [profilesLoading, setProfilesLoading] = useState(false)
	const [profilesError, setProfilesError] = useState("")
	const [loading, setLoading] = useState(Boolean(id))
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")

	// The weak ETag from the last load, echoed as If-Match on save so a
	// concurrent edit is rejected (412) instead of silently overwritten.
	const etagRef = useRef("")

	const loadTarget = useCallback(async () => {
		if (!id) {
			return
		}
		setLoading(true)
		setError("")
		try {
			const target = await api.send<TargetRecord>(`/api/v1/devices/${id}`, {
				onResponse: (response) => {
					etagRef.current = response.headers.get("ETag") ?? ""
				},
			})
			const targetID = target.id || id
			setForm({
				id: targetID,
				name: target.name,
				kind: target.kind || "system",
				host: target.host,
				status: target.status || "pending",
				labels: target.labels ?? {},
				snmpProfileID: "",
				snmpPort: "161",
				snmpCommunity: "",
			})
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load target`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	useEffect(() => {
		document.title = `${isEditing ? t`Edit Target` : defaultKind === "network" ? t`Add SNMP Device` : t`Add Target`} / Watchdog`
		loadTarget()
	}, [defaultKind, isEditing, loadTarget, t])

	useEffect(() => {
		if (isEditing || form.kind !== "network") {
			return
		}
		let cancelled = false
		setProfilesLoading(true)
		setProfilesError("")
		api.send<SNMPProfilesResponse>("/api/v1/snmp/profiles", {})
			.then((data) => {
				if (cancelled) return
				const profiles = data.items ?? []
				setSNMPProfiles(profiles)
				if (profiles.length === 1) {
					setForm((current) => ({ ...current, snmpProfileID: profileID(profiles[0]) }))
				}
			})
			.catch((err) => {
				if (!cancelled) setProfilesError(err instanceof Error ? err.message : t`Failed to load SNMP profiles`)
			})
			.finally(() => {
				if (!cancelled) setProfilesLoading(false)
			})
		return () => {
			cancelled = true
		}
	}, [form.kind, isEditing, t])

	const save = async () => {
		setSaving(true)
		setError("")
		try {
			const body: Record<string, unknown> = {
				name: form.name.trim(),
				kind: form.kind,
				host: form.host.trim(),
				status: form.status,
				labels: form.labels,
			}
			if (id) body.id = form.id.trim()
			if (!isEditing && form.kind === "network") {
				body.snmp_profile_id = form.snmpProfileID
				body.snmp_port = Number(form.snmpPort)
				if (form.snmpCommunity) {
					body.snmp_security = { community: form.snmpCommunity }
				}
			}
			const saved = await api.send<TargetRecord>(id ? `/api/v1/devices/${id}` : "/api/v1/devices", {
				method: id ? "PATCH" : "POST",
				headers: id && etagRef.current ? { "If-Match": etagRef.current } : undefined,
				body,
			})
			const savedID = saved.id || form.id
			// Network targets are surfaced on the Network page (device view).
			if (form.kind === "network") {
				navigate(getPagePath($router, "network"))
			} else {
				navigate(getPagePath($router, "target_detail", { id: savedID }))
			}
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save target`)
		} finally {
			setSaving(false)
		}
	}

	const update = (patch: Partial<FormState>) => setForm((current) => ({ ...current, ...patch }))
	const provisioningSNMP = !isEditing && form.kind === "network"
	const canSave =
		Boolean(form.host.trim()) &&
		(!provisioningSNMP ||
			(!profilesLoading && snmpProfiles.length > 0 && Boolean(form.snmpProfileID) && Number(form.snmpPort) > 0))
	const backPath = defaultKind === "network" ? getPagePath($router, "network") : getPagePath($router, "targets")

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex min-w-0 items-center gap-2">
					<Link
						href={id ? getPagePath($router, "target_detail", { id }) : backPath}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to targets`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<CrosshairIcon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="truncate text-xl font-semibold tracking-normal">
						{isEditing ? (
							<Trans>Edit Target</Trans>
						) : defaultKind === "network" ? (
							<Trans>Add SNMP Device</Trans>
						) : (
							<Trans>Add Target</Trans>
						)}
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
					<Field label={form.kind === "network" ? t`Host / IP` : t`Host`}>
						<Input value={form.host} onChange={(event) => update({ host: event.target.value })} disabled={loading} />
					</Field>
					{defaultKind !== "network" ? (
						<Field label={t`Type`}>
							<Select value={form.kind} onValueChange={(kind) => update({ kind })} disabled={loading}>
								<SelectTrigger>
									<SelectValue />
								</SelectTrigger>
								<SelectContent>
									<SelectItem value="system">
										<Trans>System</Trans>
									</SelectItem>
									<SelectItem value="network">
										<Trans>Network</Trans>
									</SelectItem>
								</SelectContent>
							</Select>
						</Field>
					) : null}
					<Field label={t`Display name (optional)`}>
						<Input value={form.name} onChange={(event) => update({ name: event.target.value })} disabled={loading} />
					</Field>
					{provisioningSNMP ? (
						<>
							<Field label={t`SNMP profile`}>
								<Select
									value={form.snmpProfileID}
									onValueChange={(snmpProfileID) => update({ snmpProfileID })}
									disabled={loading || profilesLoading || snmpProfiles.length === 0}
								>
									<SelectTrigger>
										<SelectValue placeholder={profilesLoading ? t`Loading...` : t`Select SNMP profile`} />
									</SelectTrigger>
									<SelectContent>
										{snmpProfiles.map((profile) => (
											<SelectItem key={profileID(profile)} value={profileID(profile)}>
												{profile.Name ?? profile.name ?? profileID(profile)} (
												{profile.Version ?? profile.version ?? "SNMP"})
											</SelectItem>
										))}
									</SelectContent>
								</Select>
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
							<Field label={t`SNMP community (optional override)`}>
								<Input
									type="password"
									autoComplete="new-password"
									value={form.snmpCommunity}
									onChange={(event) => update({ snmpCommunity: event.target.value })}
									disabled={loading}
								/>
								<p className="text-xs text-muted-foreground">
									<Trans>Leave blank to use the selected profile's community.</Trans>
								</p>
							</Field>
						</>
					) : null}
					{isEditing ? (
						<Field label="ID">
							<Input value={form.id} disabled />
						</Field>
					) : null}
					<Field label={t`Status`}>
						<Select value={form.status} onValueChange={(status) => update({ status })} disabled={loading}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="pending">
									<Trans>Pending</Trans>
								</SelectItem>
								<SelectItem value="up">
									<Trans>Up</Trans>
								</SelectItem>
								<SelectItem value="down">
									<Trans>Down</Trans>
								</SelectItem>
								<SelectItem value="paused">
									<Trans>Paused</Trans>
								</SelectItem>
							</SelectContent>
						</Select>
					</Field>
				</div>
				{provisioningSNMP && !profilesLoading && snmpProfiles.length === 0 ? (
					<div className="text-sm text-destructive">
						{profilesError || t`No SNMP profile is configured.`}{" "}
						<Link href={getPagePath($router, "snmp_profile_new")} className="underline">
							<Trans>Create SNMP profile</Trans>
						</Link>
					</div>
				) : profilesError ? (
					<div className="text-sm text-destructive">{profilesError}</div>
				) : null}
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

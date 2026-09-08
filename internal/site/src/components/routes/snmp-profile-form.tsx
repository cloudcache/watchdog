import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, SaveIcon, SlidersHorizontalIcon } from "lucide-react"
import { memo, useCallback, useEffect, useRef, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { api } from "@/lib/api"
import { cn } from "@/lib/utils"

type SNMPProfile = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	Version?: string
	version?: string
	Security?: Record<string, string>
	security?: Record<string, string>
	Timeout?: number
	timeout?: number
	Retries?: number
	retries?: number
}

type SNMPProfileFormProps = {
	id?: string
}

type FormState = {
	id: string
	name: string
	version: string
	security: string
	timeoutSeconds: string
	retries: string
}

export default memo(({ id }: SNMPProfileFormProps) => {
	const { t } = useLingui()
	const isEditing = Boolean(id)
	const [form, setForm] = useState<FormState>(() => ({
		id: id ?? createSNMPProfileID(),
		name: "",
		version: "2c",
		security: JSON.stringify({ community: "public" }, null, 2),
		timeoutSeconds: "5",
		retries: "2",
	}))
	const [loading, setLoading] = useState(Boolean(id))
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")
	// Weak ETag from the last load, echoed as If-Match on save (optimistic
	// concurrency): a concurrent edit is rejected (412), not overwritten.
	const etagRef = useRef("")

	const loadProfile = useCallback(async () => {
		if (!id) {
			return
		}
		setLoading(true)
		setError("")
		try {
			const profile = await api.send<SNMPProfile>(`/api/v1/snmp/profiles/${id}`, {
				onResponse: (response) => {
					etagRef.current = response.headers.get("ETag") ?? ""
				},
			})
			setForm({
				id: profile.ID ?? profile.id ?? id,
				name: profile.Name ?? profile.name ?? "",
				version: profile.Version ?? profile.version ?? "2c",
				security: JSON.stringify(profile.Security ?? profile.security ?? {}, null, 2),
				timeoutSeconds: String(
					Math.max(1, Math.round((profile.Timeout ?? profile.timeout ?? 5_000_000_000) / 1_000_000_000))
				),
				retries: String(profile.Retries ?? profile.retries ?? 2),
			})
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load SNMP profile`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	useEffect(() => {
		document.title = `${isEditing ? t`Edit SNMP Profile` : t`Create SNMP Profile`} / Watchdog`
		loadProfile()
	}, [isEditing, loadProfile, t])

	const save = async () => {
		setSaving(true)
		setError("")
		try {
			const security = parseSecurityJSON(form.security)
			const body = {
				ID: form.id.trim(),
				Name: form.name.trim(),
				Version: form.version,
				Security: security,
				Timeout: Number(form.timeoutSeconds) * 1_000_000_000,
				Retries: Number(form.retries),
			}
			const saved = await api.send<SNMPProfile>(id ? `/api/v1/snmp/profiles/${id}` : "/api/v1/snmp/profiles", {
				method: id ? "PATCH" : "POST",
				headers: id && etagRef.current ? { "If-Match": etagRef.current } : undefined,
				body,
			})
			navigate(getPagePath($router, "snmp_profile_edit", { id: saved.ID ?? saved.id ?? form.id }))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save SNMP profile`)
		} finally {
			setSaving(false)
		}
	}

	const update = (patch: Partial<FormState>) => setForm((current) => ({ ...current, ...patch }))
	const canSave = form.id.trim() && form.name.trim() && Number(form.timeoutSeconds) > 0 && Number(form.retries) >= 0

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex min-w-0 items-center gap-2">
					<Link
						href={getPagePath($router, "snmp_profiles")}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to SNMP profiles`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<SlidersHorizontalIcon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="truncate text-xl font-semibold tracking-normal">
						{isEditing ? <Trans>Edit SNMP Profile</Trans> : <Trans>Create SNMP Profile</Trans>}
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
					<Field label="ID">
						<Input
							value={form.id}
							onChange={(event) => update({ id: event.target.value })}
							disabled={isEditing || loading}
						/>
					</Field>
					<Field label={t`Name`}>
						<Input value={form.name} onChange={(event) => update({ name: event.target.value })} disabled={loading} />
					</Field>
					<Field label={t`Version`}>
						<Select value={form.version} onValueChange={(version) => update({ version })} disabled={loading}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="2c">2c</SelectItem>
								<SelectItem value="3">3</SelectItem>
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`Timeout seconds`}>
						<Input
							type="number"
							min="1"
							value={form.timeoutSeconds}
							onChange={(event) => update({ timeoutSeconds: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<Field label={t`Retries`}>
						<Input
							type="number"
							min="0"
							value={form.retries}
							onChange={(event) => update({ retries: event.target.value })}
							disabled={loading}
						/>
					</Field>
				</div>
				<Field label={t`Security JSON`}>
					<Textarea
						value={form.security}
						onChange={(event) => update({ security: event.target.value })}
						disabled={loading}
						rows={12}
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

function createSNMPProfileID() {
	const bytes = new Uint8Array(8)
	crypto.getRandomValues(bytes)
	return `snmp_${Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")}`
}

function parseSecurityJSON(value: string) {
	const parsed = JSON.parse(value || "{}")
	if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
		throw new Error("Security JSON must be an object")
	}
	return Object.fromEntries(Object.entries(parsed).map(([key, item]) => [key, String(item)]))
}

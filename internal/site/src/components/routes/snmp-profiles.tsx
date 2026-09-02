import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { PencilIcon, PlusIcon, RefreshCwIcon, SlidersHorizontalIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Button } from "@/components/ui/button"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { pb } from "@/lib/api"

type SNMPProfile = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	Version?: string
	version?: string
	Timeout?: number
	timeout?: number
	Retries?: number
	retries?: number
}

type SNMPProfilesResponse = {
	items?: SNMPProfile[]
}

export default memo(() => {
	const { t } = useLingui()
	const [profiles, setProfiles] = useState<SNMPProfile[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const data = await pb.send<SNMPProfilesResponse>("/api/v1/snmp/profiles", {})
			setProfiles(data.items ?? [])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load SNMP profiles`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		document.title = `${t`SNMP Profiles`} / WatchDog`
		refresh()
	}, [refresh, t])

	const deleteProfile = async (profile: SNMPProfile) => {
		if (!window.confirm(t`Delete this SNMP profile?`)) {
			return
		}
		const id = profile.ID ?? profile.id ?? ""
		try {
			await pb.send(`/api/v1/snmp/profiles/${id}`, { method: "DELETE" })
			await refresh()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to delete SNMP profile`)
		}
	}

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<SlidersHorizontalIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>SNMP Profiles</Trans>
					</h1>
				</div>
				<div className="flex items-center gap-2">
					<Button variant="outline" size="sm" onClick={() => navigate(getPagePath($router, "snmp_profile_new"))}>
						<PlusIcon className="me-2 h-4 w-4" />
						<Trans>Create</Trans>
					</Button>
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
			</div>

			<div className="rounded-md border border-border bg-card">
				<Table>
					<TableHeader>
						<TableRow>
							<TableHead>
								<Trans>Name</Trans>
							</TableHead>
							<TableHead>
								<Trans>Version</Trans>
							</TableHead>
							<TableHead>
								<Trans>Timeout</Trans>
							</TableHead>
							<TableHead>
								<Trans>Retries</Trans>
							</TableHead>
							<TableHead className="w-32"></TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{loading ? (
							<TableRow>
								<TableCell colSpan={5} className="text-muted-foreground">
									<Trans>Loading...</Trans>
								</TableCell>
							</TableRow>
						) : error ? (
							<TableRow>
								<TableCell colSpan={5} className="text-destructive">
									{error}
								</TableCell>
							</TableRow>
						) : profiles.length === 0 ? (
							<TableRow>
								<TableCell colSpan={5} className="text-muted-foreground">
									<Trans>No SNMP profiles found.</Trans>
								</TableCell>
							</TableRow>
						) : (
							profiles.map((profile) => {
								const id = profile.ID ?? profile.id ?? ""
								return (
									<TableRow key={id}>
										<TableCell className="font-medium">{profile.Name ?? profile.name ?? "—"}</TableCell>
										<TableCell>{profile.Version ?? profile.version ?? "—"}</TableCell>
										<TableCell>{formatDuration(profile.Timeout ?? profile.timeout)}</TableCell>
										<TableCell>{profile.Retries ?? profile.retries ?? "—"}</TableCell>
										<TableCell>
											<div className="flex justify-end gap-1">
												<Link
													href={getPagePath($router, "snmp_profile_edit", { id })}
													className="inline-flex h-8 w-8 items-center justify-center rounded-md hover:bg-muted"
													aria-label={t`Edit SNMP profile`}
												>
													<PencilIcon className="h-4 w-4" />
												</Link>
												<Button
													variant="ghost"
													size="icon"
													onClick={() => deleteProfile(profile)}
													aria-label={t`Delete SNMP profile`}
												>
													<Trash2Icon className="h-4 w-4" />
												</Button>
											</div>
										</TableCell>
									</TableRow>
								)
							})
						)}
					</TableBody>
				</Table>
			</div>
		</div>
	)
})

function formatDuration(value?: number) {
	if (!value || value <= 0) {
		return "—"
	}
	if (value > 1_000_000_000) {
		return `${(value / 1_000_000_000).toFixed(1)}s`
	}
	if (value > 1_000_000) {
		return `${Math.round(value / 1_000_000)}ms`
	}
	return `${value}ms`
}

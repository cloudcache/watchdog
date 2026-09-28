import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@/lib/page-path"
import { DatabaseIcon, PencilIcon, PlusIcon, RefreshCwIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { api } from "@/lib/api"

type MIBModule = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	Source?: string
	source?: string
	Version?: string
	version?: string
	Checksum?: string
	checksum?: string
	Enabled?: boolean
	enabled?: boolean
	Builtin?: boolean
	builtin?: boolean
}

type MIBModulesResponse = {
	items?: MIBModule[]
}

export default memo(() => {
	const { t } = useLingui()
	const [modules, setModules] = useState<MIBModule[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const data = await api.send<MIBModulesResponse>("/api/v1/snmp/mib-modules", {})
			setModules(data.items ?? [])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load MIB modules`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		document.title = `${t`MIB Modules`} / Watchdog`
		refresh()
	}, [refresh, t])

	const deleteModule = async (module: MIBModule) => {
		if (!globalThis.confirm(t`Delete this MIB module?`)) {
			return
		}
		try {
			await api.send(`/api/v1/snmp/mib-modules/${module.ID ?? module.id ?? ""}`, { method: "DELETE" })
			await refresh()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to delete MIB module`)
		}
	}

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<DatabaseIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>MIB Modules</Trans>
					</h1>
				</div>
				<div className="flex items-center gap-2">
					<Button variant="outline" size="sm" onClick={() => navigate(getPagePath($router, "snmp_mib_module_new"))}>
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
								<Trans>Source</Trans>
							</TableHead>
							<TableHead>
								<Trans>Version</Trans>
							</TableHead>
							<TableHead>
								<Trans>Checksum</Trans>
							</TableHead>
							<TableHead>
								<Trans>Status</Trans>
							</TableHead>
							<TableHead className="w-32"></TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{loading ? (
							<TableRow>
								<TableCell colSpan={6} className="text-muted-foreground">
									<Trans>Loading...</Trans>
								</TableCell>
							</TableRow>
						) : error ? (
							<TableRow>
								<TableCell colSpan={6} className="text-destructive">
									{error}
								</TableCell>
							</TableRow>
						) : modules.length === 0 ? (
							<TableRow>
								<TableCell colSpan={6} className="text-muted-foreground">
									<Trans>No MIB modules found.</Trans>
								</TableCell>
							</TableRow>
						) : (
							modules.map((module) => {
								const id = module.ID ?? module.id ?? ""
								const builtin = module.Builtin ?? module.builtin ?? false
								return (
									<TableRow key={id}>
										<TableCell className="font-medium">{module.Name ?? module.name ?? "—"}</TableCell>
										<TableCell>
											<div className="flex items-center gap-2">
												<span>{module.Source ?? module.source ?? "—"}</span>
												{builtin ? (
													<Badge variant="outline">
														<Trans>Built-in</Trans>
													</Badge>
												) : null}
											</div>
										</TableCell>
										<TableCell>{module.Version ?? module.version ?? "—"}</TableCell>
										<TableCell className="max-w-[18rem] truncate font-mono text-xs">
											{module.Checksum ?? module.checksum ?? "—"}
										</TableCell>
										<TableCell>
											<Badge variant={(module.Enabled ?? module.enabled) ? "success" : "outline"}>
												{(module.Enabled ?? module.enabled) ? t`Enabled` : t`Disabled`}
											</Badge>
										</TableCell>
										<TableCell>
											<div className="flex justify-end gap-1">
												{!builtin ? (
													<>
														<Link
															href={getPagePath($router, "snmp_mib_module_edit", { id })}
															className="inline-flex h-8 w-8 items-center justify-center rounded-md hover:bg-muted"
															aria-label={t`Edit MIB module`}
														>
															<PencilIcon className="h-4 w-4" />
														</Link>
														<Button
															variant="ghost"
															size="icon"
															onClick={() => deleteModule(module)}
															aria-label={t`Delete MIB module`}
														>
															<Trash2Icon className="h-4 w-4" />
														</Button>
													</>
												) : null}
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

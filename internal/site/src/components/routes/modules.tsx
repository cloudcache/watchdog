import { Trans, useLingui } from "@lingui/react/macro"
import { BlocksIcon, RefreshCwIcon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { toast } from "@/components/ui/use-toast"
import { pb } from "@/lib/api"
import { $platformIdentity } from "@/lib/platform-auth"

type ModuleState = {
	Key: string
	Version: string
	DisplayName: string
	Dependencies: string[] | null
	DefaultEnabled: boolean
	Enabled: boolean
}

type TargetKindItem = {
	Key: string
	ModuleKey: string
	DisplayName: string
	Readiness: string
	MenuGroup: string
	AllowedCapabilities: string[] | null
}

export default memo(() => {
	const { t } = useLingui()
	const [modules, setModules] = useState<ModuleState[]>([])
	const [kinds, setKinds] = useState<TargetKindItem[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const [moduleData, kindData] = await Promise.all([
				pb.send<{ items?: ModuleState[] }>("/api/v1/modules", {}),
				pb.send<{ items?: TargetKindItem[] }>("/api/v1/modules/target-kinds", {}),
			])
			setModules(moduleData.items ?? [])
			setKinds(kindData.items ?? [])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load modules`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		document.title = `${t`Modules`} / Watchdog`
		refresh()
	}, [refresh, t])

	const toggleModule = useCallback(
		async (module: ModuleState) => {
			const tenantID = $platformIdentity.get().current?.tenantID
			if (!tenantID) {
				toast({ title: t`Select a tenant first`, variant: "destructive" })
				return
			}
			try {
				await pb.send(`/api/v1/tenants/${tenantID}/modules`, {
					method: "PUT",
					body: { modules: { [module.Key]: !module.Enabled } },
				})
				refresh()
			} catch (err) {
				toast({ title: err instanceof Error ? err.message : t`Request failed`, variant: "destructive" })
			}
		},
		[refresh, t]
	)

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<BlocksIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Modules</Trans>
					</h1>
				</div>
				<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Refresh</Trans>
				</Button>
			</div>
			{error ? <div className="text-sm text-destructive">{error}</div> : null}

			<div className="overflow-hidden rounded-md border border-border bg-card">
				<Table>
					<TableHeader>
						<TableRow>
							<TableHead className="w-16">
								<Trans>Enabled</Trans>
							</TableHead>
							<TableHead>
								<Trans>Module</Trans>
							</TableHead>
							<TableHead>
								<Trans>Version</Trans>
							</TableHead>
							<TableHead>
								<Trans>Dependencies</Trans>
							</TableHead>
							<TableHead>
								<Trans>Default</Trans>
							</TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{modules.map((module) => (
							<TableRow key={module.Key}>
								<TableCell>
									<Checkbox
										checked={module.Enabled}
										disabled={module.Key === "core"}
										onCheckedChange={() => toggleModule(module)}
									/>
								</TableCell>
								<TableCell>
									<span className="font-medium">{module.DisplayName}</span>
									<span className="ms-2 font-mono text-xs text-muted-foreground">{module.Key}</span>
								</TableCell>
								<TableCell className="font-mono text-xs">{module.Version}</TableCell>
								<TableCell className="text-xs text-muted-foreground">
									{(module.Dependencies ?? []).join(", ") || "—"}
								</TableCell>
								<TableCell>
									<Badge variant="outline" className="font-normal">
										{module.DefaultEnabled ? t`enabled` : t`disabled`}
									</Badge>
								</TableCell>
							</TableRow>
						))}
					</TableBody>
				</Table>
			</div>

			<div className="grid gap-2">
				<h2 className="text-base font-medium">
					<Trans>Target Kinds</Trans>
				</h2>
				<div className="overflow-hidden rounded-md border border-border bg-card">
					<Table>
						<TableHeader>
							<TableRow>
								<TableHead>
									<Trans>Kind</Trans>
								</TableHead>
								<TableHead>
									<Trans>Module</Trans>
								</TableHead>
								<TableHead>
									<Trans>Readiness</Trans>
								</TableHead>
								<TableHead>
									<Trans>Capabilities</Trans>
								</TableHead>
							</TableRow>
						</TableHeader>
						<TableBody>
							{kinds.map((kind) => (
								<TableRow key={kind.Key}>
									<TableCell>
										<span className="font-medium">{kind.DisplayName}</span>
										<span className="ms-2 font-mono text-xs text-muted-foreground">{kind.Key}</span>
									</TableCell>
									<TableCell className="font-mono text-xs">{kind.ModuleKey}</TableCell>
									<TableCell>
										<Badge
											variant={
												kind.Readiness === "ga" ? "success" : kind.Readiness === "beta" ? "outline" : "secondary"
											}
											className="font-normal"
										>
											{kind.Readiness}
										</Badge>
									</TableCell>
									<TableCell className="text-xs text-muted-foreground">
										{(kind.AllowedCapabilities ?? []).join(", ") || "—"}
									</TableCell>
								</TableRow>
							))}
						</TableBody>
					</Table>
				</div>
			</div>
		</div>
	)
})

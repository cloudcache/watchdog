import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { DownloadIcon, FileDownIcon, RefreshCwIcon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { $router, Link } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button, buttonVariants } from "@/components/ui/button"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { pb } from "@/lib/api"
import { trafficViewFromValue, trafficViewLabel } from "@/lib/traffic-view"
import { cn } from "@/lib/utils"
import {
	exportDownloadURL,
	exportID,
	exportStatus as getExportStatus,
	formatExportRange,
	type ExportTask,
} from "./export-types"

type ExportTasksResponse = {
	items?: ExportTask[]
}

export default memo(() => {
	const { t } = useLingui()
	const [tasks, setTasks] = useState<ExportTask[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const data = await pb.send<ExportTasksResponse>("/api/v1/exports", {})
			setTasks(data.items ?? [])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load exports`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		document.title = `${t`Exports`} / WatchDog`
		refresh()
	}, [refresh, t])

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<FileDownIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Exports</Trans>
					</h1>
				</div>
				<div className="flex items-center gap-2">
					<Link
						href={getPagePath($router, "export_new")}
						className={cn(buttonVariants({ variant: "default", size: "sm" }))}
					>
						<FileDownIcon className="me-2 h-4 w-4" />
						<Trans>Create</Trans>
					</Link>
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
								<Trans>Status</Trans>
							</TableHead>
							<TableHead>
								<Trans>Target</Trans>
							</TableHead>
							<TableHead>
								<Trans>Port</Trans>
							</TableHead>
							<TableHead>
								<Trans>Range</Trans>
							</TableHead>
							<TableHead>
								<Trans>Aggregation</Trans>
							</TableHead>
							<TableHead>
								<Trans>View</Trans>
							</TableHead>
							<TableHead>
								<Trans>Value</Trans>
							</TableHead>
							<TableHead>
								<Trans>Format</Trans>
							</TableHead>
							<TableHead className="text-right">
								<Trans>Actions</Trans>
							</TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{loading ? (
							<TableRow>
								<TableCell colSpan={9} className="text-muted-foreground">
									<Trans>Loading...</Trans>
								</TableCell>
							</TableRow>
						) : error ? (
							<TableRow>
								<TableCell colSpan={9} className="text-destructive">
									{error}
								</TableCell>
							</TableRow>
						) : tasks.length === 0 ? (
							<TableRow>
								<TableCell colSpan={9} className="text-muted-foreground">
									<Trans>No exports found.</Trans>
								</TableCell>
							</TableRow>
						) : (
							tasks.map((task) => (
								<TableRow key={task.ID ?? task.id}>
									<TableCell>
										<Link
											className="hover:underline"
											href={getPagePath($router, "export_detail", { id: exportID(task) })}
										>
											<ExportStatus value={getExportStatus(task)} />
										</Link>
									</TableCell>
									<TableCell className="font-mono text-xs">{task.TargetID ?? task.target_id ?? "—"}</TableCell>
									<TableCell className="font-mono text-xs">{task.PortID ?? task.port_id ?? "—"}</TableCell>
									<TableCell>{formatExportRange(task)}</TableCell>
									<TableCell>{task.Aggregation ?? task.aggregation ?? "—"}</TableCell>
									<TableCell>
										{trafficViewLabel(
											trafficViewFromValue(task.ValueMode ?? task.value_mode, task.Aggregation ?? task.aggregation)
										)}
									</TableCell>
									<TableCell>{task.ValueMode ?? task.value_mode ?? "corrected"}</TableCell>
									<TableCell>{task.Format ?? task.format ?? "—"}</TableCell>
									<TableCell className="text-right">
										<Button
											variant="ghost"
											size="icon"
											disabled={getExportStatus(task) !== "complete"}
											onClick={() => downloadExport(task)}
											aria-label={t`Download`}
										>
											<DownloadIcon className="h-4 w-4" />
										</Button>
									</TableCell>
								</TableRow>
							))
						)}
					</TableBody>
				</Table>
			</div>
		</div>
	)
})

function ExportStatus({ value }: { value?: string }) {
	const normalized = value?.toLowerCase() ?? ""
	const variant: "success" | "danger" | "warning" | "outline" =
		normalized === "complete"
			? "success"
			: normalized === "failed"
				? "danger"
				: normalized === "running"
					? "warning"
					: "outline"
	return <Badge variant={variant}>{value || "—"}</Badge>
}

function downloadExport(task: ExportTask) {
	const url = exportDownloadURL(task)
	if (!url) {
		return
	}
	globalThis.location.href = url
}

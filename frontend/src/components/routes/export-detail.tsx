import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import {
	ArrowLeftIcon,
	DownloadIcon,
	FileDownIcon,
	RefreshCwIcon,
	RotateCcwIcon,
	Trash2Icon,
	XCircleIcon,
} from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import {
	AlertDialog,
	AlertDialogAction,
	AlertDialogCancel,
	AlertDialogContent,
	AlertDialogDescription,
	AlertDialogFooter,
	AlertDialogHeader,
	AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { Badge } from "@/components/ui/badge"
import { Button, buttonVariants } from "@/components/ui/button"
import { downloadWatchdogFile, api } from "@/lib/api"
import { trafficViewFromValue, trafficViewLabel } from "@/lib/traffic-view"
import { cn } from "@/lib/utils"
import {
	exportDownloadURL,
	exportID,
	exportStatus,
	exportValueLayer,
	formatExportDate,
	formatExportRange,
	formatExportStep,
	type ExportTask,
} from "./export-types"

type ExportDetailProps = {
	id: string
}

export default memo(({ id }: ExportDetailProps) => {
	const { t } = useLingui()
	const [task, setTask] = useState<ExportTask | null>(null)
	const [loading, setLoading] = useState(true)
	const [retrying, setRetrying] = useState(false)
	const [canceling, setCanceling] = useState(false)
	const [deleting, setDeleting] = useState(false)
	const [deleteOpen, setDeleteOpen] = useState(false)
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const data = await api.send<ExportTask>(`/api/v1/exports/${id}`, {})
			setTask(data)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load export`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	useEffect(() => {
		document.title = `${t`Export`} / Watchdog`
		refresh()
	}, [refresh, t])

	const status = task ? exportStatus(task) : ""
	const canDownload = task && status === "complete"
	const canRetry = task && (status === "failed" || status === "canceled")
	const canCancel =
		task && (status === "pending" || status === "running") && Boolean(task.OperationJobID ?? task.operation_job_id)
	const canDelete = task && (status === "complete" || status === "failed" || status === "canceled")
	const viewMode = task ? trafficViewLabel(exportTrafficView(task)) : "-"

	const retry = async () => {
		if (!task) {
			return
		}
		setRetrying(true)
		setError("")
		try {
			const data = await api.send<ExportTask>(`/api/v1/exports/${exportID(task)}/retry`, { method: "POST" })
			setTask(data)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to retry export`)
		} finally {
			setRetrying(false)
		}
	}

	const cancel = async () => {
		if (!task) return
		setCanceling(true)
		setError("")
		try {
			const data = await api.send<ExportTask>(`/api/v1/exports/${exportID(task)}/cancel`, { method: "POST" })
			setTask(data)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to cancel export`)
		} finally {
			setCanceling(false)
		}
	}

	const deleteExport = async () => {
		if (!task) return
		setDeleteOpen(false)
		setDeleting(true)
		setError("")
		try {
			const response = await api.send<{ id?: string }>(`/api/v1/exports/${exportID(task)}`, { method: "DELETE" })
			if (!response.id) throw new Error(t`Export deletion did not return an operation job`)
			for (let attempt = 0; attempt < 120; attempt++) {
				const job = await api.send<{ status?: string; last_error_detail?: string }>(
					`/api/v1/operation-jobs/${response.id}`,
					{}
				)
				if (job.status === "succeeded") {
					navigate(getPagePath($router, "exports"))
					return
				}
				if (job.status === "failed" || job.status === "canceled") {
					throw new Error(job.last_error_detail || t`Failed to delete export`)
				}
				await new Promise((resolve) => globalThis.setTimeout(resolve, 1000))
			}
			throw new Error(t`Export deletion is still running`)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to delete export`)
			setDeleting(false)
		}
	}

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex min-w-0 items-center gap-2">
					<Link
						href={getPagePath($router, "exports")}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to exports`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<FileDownIcon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="truncate text-xl font-semibold tracking-normal">{task ? exportID(task) : id}</h1>
				</div>
				<div className="flex items-center gap-2">
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
					<Button variant="outline" size="sm" disabled={!canRetry || retrying} onClick={retry}>
						<RotateCcwIcon className="me-2 h-4 w-4" />
						<Trans>Retry</Trans>
					</Button>
					<Button variant="outline" size="sm" disabled={!canCancel || canceling} onClick={cancel}>
						<XCircleIcon className="me-2 h-4 w-4" />
						<Trans>Cancel</Trans>
					</Button>
					<Button
						variant="outline"
						size="sm"
						disabled={!canDelete || deleting}
						onClick={() => setDeleteOpen(true)}
						className="text-destructive"
					>
						<Trash2Icon className="me-2 h-4 w-4" />
						<Trans>Delete</Trans>
					</Button>
					<Button
						size="sm"
						disabled={!canDownload}
						onClick={() =>
							task &&
							void downloadExport(task).catch((error) => setError(error instanceof Error ? error.message : String(error)))
						}
					>
						<DownloadIcon className="me-2 h-4 w-4" />
						<Trans>Download</Trans>
					</Button>
				</div>
			</div>

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}

			<div className="grid gap-3 md:grid-cols-4">
				<InfoCell label={t`Status`} value={status ? <ExportStatus value={status} /> : "-"} />
				<InfoCell label={t`Target`} value={task?.TargetID ?? task?.target_id} mono />
				<InfoCell label={t`Port`} value={task?.PortID ?? task?.port_id} mono />
				<InfoCell label={t`Format`} value={task?.Format ?? task?.format ?? "csv"} />
				<InfoCell label={t`View`} value={viewMode} />
				<InfoCell label={t`Aggregation`} value={task?.Aggregation ?? task?.aggregation} />
				<InfoCell label={t`Value`} value={task?.ValueMode ?? task?.value_mode ?? "corrected"} />
				<InfoCell label={t`Query hash`} value={task?.QueryHash ?? task?.query_hash} mono wide />
				<InfoCell label={t`Operation job`} value={task?.OperationJobID ?? task?.operation_job_id} mono wide />
				<InfoCell label={t`Rows`} value={task?.RowCount ?? task?.row_count} />
				<InfoCell label={t`Size`} value={formatBytes(task?.SizeBytes ?? task?.size_bytes)} />
				<InfoCell label={t`Checksum`} value={task?.Checksum ?? task?.checksum} mono wide />
				<InfoCell label={t`Expires`} value={formatExportDate(task?.ExpiresAt ?? task?.expires_at)} />
				<InfoCell label={t`Step`} value={formatExportStep(task?.Step ?? task?.step)} mono />
				<InfoCell label={t`Period`} value={task?.PeriodType ?? task?.period_type} />
				<InfoCell label={t`Range`} value={task ? formatExportRange(task) : "-"} wide />
				<InfoCell label={t`File`} value={task?.FileRef ?? task?.file_ref} mono wide />
				<InfoCell label={t`Created`} value={formatExportDate(task?.CreatedAt ?? task?.created_at)} />
				<InfoCell label={t`Updated`} value={formatExportDate(task?.UpdatedAt ?? task?.updated_at)} />
			</div>

			{task?.ErrorMessage || task?.error_message ? (
				<div className="rounded-md border border-destructive/30 p-4">
					<div className="text-xs text-muted-foreground">
						<Trans>Error</Trans>
					</div>
					<div className="mt-2 whitespace-pre-wrap text-sm text-destructive">
						{task.ErrorMessage ?? task.error_message}
					</div>
				</div>
			) : null}

			<AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
				<AlertDialogContent>
					<AlertDialogHeader>
						<AlertDialogTitle>
							<Trans>Delete export?</Trans>
						</AlertDialogTitle>
						<AlertDialogDescription>
							<Trans>The export record and generated file will be permanently deleted.</Trans>
						</AlertDialogDescription>
					</AlertDialogHeader>
					<AlertDialogFooter>
						<AlertDialogCancel>
							<Trans>Cancel</Trans>
						</AlertDialogCancel>
						<AlertDialogAction onClick={deleteExport} className="bg-destructive text-destructive-foreground">
							<Trans>Delete</Trans>
						</AlertDialogAction>
					</AlertDialogFooter>
				</AlertDialogContent>
			</AlertDialog>
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
	return <Badge variant={variant}>{value || "-"}</Badge>
}

function InfoCell({
	label,
	value,
	mono,
	wide,
}: {
	label: string
	value?: React.ReactNode
	mono?: boolean
	wide?: boolean
}) {
	return (
		<div className={cn("rounded-md border border-border p-3", wide && "md:col-span-2")}>
			<div className="text-xs text-muted-foreground">{label}</div>
			<div className={cn("mt-1 truncate text-sm", mono && "font-mono text-xs")}>{value || "-"}</div>
		</div>
	)
}

async function downloadExport(task: ExportTask) {
	const url = exportDownloadURL(task)
	if (url) await downloadWatchdogFile(url)
}

function exportTrafficView(task: ExportTask) {
	const layer = exportValueLayer(task)
	if (layer === "raw" || layer === "supplier" || layer === "customer") return layer
	return trafficViewFromValue(task.ValueMode ?? task.value_mode, task.Aggregation ?? task.aggregation)
}

function formatBytes(value?: number) {
	if (!value || value < 0) return "-"
	if (value < 1024) return `${value} B`
	if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`
	return `${(value / (1024 * 1024)).toFixed(1)} MiB`
}

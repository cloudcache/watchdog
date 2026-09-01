import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, DownloadIcon, FileDownIcon, RefreshCwIcon, RotateCcwIcon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { $router, Link } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button, buttonVariants } from "@/components/ui/button"
import { pb } from "@/lib/api"
import { trafficViewFromValue, trafficViewLabel } from "@/lib/traffic-view"
import { cn } from "@/lib/utils"
import {
	exportDownloadURL,
	exportID,
	exportStatus,
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
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const data = await pb.send<ExportTask>(`/api/v1/exports/${id}`, {})
			setTask(data)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load export`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	useEffect(() => {
		document.title = `${t`Export`} / Beszel`
		refresh()
	}, [refresh, t])

	const status = task ? exportStatus(task) : ""
	const canDownload = task && status === "complete"
	const canRetry = task && status === "failed"
	const viewMode = task
		? trafficViewLabel(trafficViewFromValue(task.ValueMode ?? task.value_mode, task.Aggregation ?? task.aggregation))
		: "-"

	const retry = async () => {
		if (!task) {
			return
		}
		setRetrying(true)
		setError("")
		try {
			const data = await pb.send<ExportTask>(`/api/v1/exports/${exportID(task)}/retry`, { method: "POST" })
			setTask(data)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to retry export`)
		} finally {
			setRetrying(false)
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
					<Button size="sm" disabled={!canDownload} onClick={() => task && downloadExport(task)}>
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

function downloadExport(task: ExportTask) {
	const url = exportDownloadURL(task)
	if (url) {
		globalThis.location.href = url
	}
}

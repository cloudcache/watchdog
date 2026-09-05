import { Trans, useLingui } from "@lingui/react/macro"
import { CircleStopIcon, RefreshCwIcon, ServerCogIcon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { toast } from "@/components/ui/use-toast"
import { pb } from "@/lib/api"

type OperationJob = {
	id: string
	job_type: string
	status: string
	progress_done: number
	result_ref?: string
	attempt_count: number
	last_error_code?: string
	last_error_detail?: string
	created_at: string
	next_attempt_at?: string
}

type OperationJobsResponse = {
	items?: OperationJob[]
	next_cursor?: string
}

const statuses = ["", "queued", "running", "cancel_requested", "succeeded", "failed", "canceled"]

const statusVariant: Record<string, "success" | "destructive" | "outline" | "secondary"> = {
	succeeded: "success",
	failed: "destructive",
	canceled: "secondary",
	running: "outline",
	cancel_requested: "outline",
	queued: "secondary",
}

export default memo(() => {
	const { t } = useLingui()
	const [items, setItems] = useState<OperationJob[]>([])
	const [nextCursor, setNextCursor] = useState("")
	const [status, setStatus] = useState("")
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")

	const fetchPage = useCallback(
		async (cursor: string, append: boolean) => {
			setLoading(true)
			setError("")
			try {
				const params = new URLSearchParams({ limit: "50" })
				if (status) {
					params.set("status", status)
				}
				if (cursor) {
					params.set("cursor", cursor)
				}
				const data = await pb.send<OperationJobsResponse>(`/api/v1/operation-jobs?${params.toString()}`, {})
				setItems((current) => (append ? [...current, ...(data.items ?? [])] : (data.items ?? [])))
				setNextCursor(data.next_cursor ?? "")
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to load jobs`)
			} finally {
				setLoading(false)
			}
		},
		[status, t]
	)

	useEffect(() => {
		document.title = `${t`Background Jobs`} / Watchdog`
	}, [t])

	useEffect(() => {
		fetchPage("", false)
	}, [fetchPage])

	const cancelJob = useCallback(
		async (job: OperationJob) => {
			try {
				await pb.send(`/api/v1/operation-jobs/${job.id}/actions/cancel`, { method: "POST" })
				fetchPage("", false)
			} catch (err) {
				toast({ title: err instanceof Error ? err.message : t`Request failed`, variant: "destructive" })
			}
		},
		[fetchPage, t]
	)

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<ServerCogIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Background Jobs</Trans>
					</h1>
				</div>
				<Button variant="outline" size="sm" onClick={() => fetchPage("", false)} disabled={loading}>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Refresh</Trans>
				</Button>
			</div>

			<div className="flex flex-wrap items-center gap-2">
				<Select value={status || "all"} onValueChange={(value) => setStatus(value === "all" ? "" : value)}>
					<SelectTrigger className="w-44">
						<SelectValue placeholder={t`Status`} />
					</SelectTrigger>
					<SelectContent>
						{statuses.map((value) => (
							<SelectItem key={value || "all"} value={value || "all"}>
								{value || t`All statuses`}
							</SelectItem>
						))}
					</SelectContent>
				</Select>
			</div>

			{error ? <div className="text-sm text-destructive">{error}</div> : null}

			<div className="overflow-hidden rounded-md border border-border bg-card">
				<Table>
					<TableHeader>
						<TableRow>
							<TableHead className="w-44">
								<Trans>Created</Trans>
							</TableHead>
							<TableHead>
								<Trans>Type</Trans>
							</TableHead>
							<TableHead>
								<Trans>Status</Trans>
							</TableHead>
							<TableHead>
								<Trans>Attempts</Trans>
							</TableHead>
							<TableHead>
								<Trans>Detail</Trans>
							</TableHead>
							<TableHead />
						</TableRow>
					</TableHeader>
					<TableBody>
						{items.length === 0 && !loading ? (
							<TableRow>
								<TableCell colSpan={6} className="text-center text-sm text-muted-foreground">
									<Trans>No background jobs.</Trans>
								</TableCell>
							</TableRow>
						) : (
							items.map((job) => (
								<TableRow key={job.id}>
									<TableCell className="whitespace-nowrap font-mono text-xs text-muted-foreground">
										{formatJobTime(job.created_at)}
									</TableCell>
									<TableCell className="font-mono text-xs">{job.job_type}</TableCell>
									<TableCell>
										<Badge variant={statusVariant[job.status] ?? "outline"} className="font-normal">
											{job.status}
										</Badge>
									</TableCell>
									<TableCell className="text-xs text-muted-foreground">{job.attempt_count}</TableCell>
									<TableCell className="max-w-96 truncate text-xs text-muted-foreground">
										{job.last_error_detail || job.result_ref || ""}
									</TableCell>
									<TableCell className="text-end">
										{job.status === "queued" || job.status === "running" ? (
											<Button variant="ghost" size="sm" onClick={() => cancelJob(job)}>
												<CircleStopIcon className="me-1.5 h-3.5 w-3.5" />
												<Trans>Cancel</Trans>
											</Button>
										) : null}
									</TableCell>
								</TableRow>
							))
						)}
					</TableBody>
				</Table>
			</div>

			{nextCursor ? (
				<div>
					<Button variant="outline" size="sm" onClick={() => fetchPage(nextCursor, true)} disabled={loading}>
						<Trans>Load more</Trans>
					</Button>
				</div>
			) : null}
		</div>
	)
})

function formatJobTime(value: string) {
	const time = new Date(value)
	return Number.isNaN(time.getTime()) ? value : time.toLocaleString()
}

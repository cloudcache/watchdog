import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@/lib/page-path"
import { ArrowLeftIcon, CrosshairIcon, PencilIcon, RefreshCwIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useRef, useState } from "react"
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
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { api } from "@/lib/api"
import { formatMetricValue } from "@/lib/metric-format"
import { cn } from "@/lib/utils"
import { type createLineChart, disposeChart, updateLineChart } from "@/lib/vchart"

type TargetRecord = {
	id: string
	name: string
	kind: string
	host: string
	status: string
	labels?: Record<string, string>
}

type VMRangeResponse = {
	data?: {
		result?: {
			values?: [number, string][]
		}[]
	}
}

type TargetDetailProps = {
	id: string
}

type DeleteImpact = {
	resource_type: string
	behavior: string
	count: number
	detail?: string
	items?: { id: string; name: string }[]
}

const targetCharts = [
	{ key: "cpu", name: "watchdog_system_cpu_percent", label: "CPU", unit: "percent" },
	{ key: "memory", name: "watchdog_system_memory_percent", label: "Memory", unit: "percent" },
	{ key: "disk", name: "watchdog_system_disk_percent", label: "Disk", unit: "percent" },
	{ key: "netIn", name: "watchdog_system_net_in_bps", label: "Net In", unit: "bps" },
	{ key: "netOut", name: "watchdog_system_net_out_bps", label: "Net Out", unit: "bps" },
]

const networkCharts = [
	{ key: "snmpIn", name: "watchdog_snmp_if_in_bps", label: "SNMP In", unit: "bps" },
	{ key: "snmpOut", name: "watchdog_snmp_if_out_bps", label: "SNMP Out", unit: "bps" },
]

export default memo(({ id }: TargetDetailProps) => {
	const { t } = useLingui()
	const chartRef = useRef<HTMLDivElement>(null)
	const chartInstance = useRef<ReturnType<typeof createLineChart> | null>(null)
	const [target, setTarget] = useState<TargetRecord | null>(null)
	const [window, setWindow] = useState("1h")
	const [series, setSeries] = useState<{ name: string; unit?: string; values: { time: number; value: number }[] }[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const [deleteOpen, setDeleteOpen] = useState(false)
	const [deleteImpacts, setDeleteImpacts] = useState<DeleteImpact[]>([])

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const targetData = await api.send<TargetRecord>(`/api/v1/devices/${id}`, {})
			setTarget(targetData)
			if (targetData.kind === "network") {
				// The device page is the canonical view for network targets;
				// this page only remains for network targets not yet discovered.
				const devices = await api.send<{
					items?: { ID?: string; id?: string; TargetID?: string; target_id?: string }[]
				}>("/api/v1/devices", {})
				const device = (devices.items ?? []).find((item) => (item.TargetID ?? item.target_id) === id)
				const deviceID = device?.ID ?? device?.id
				if (deviceID) {
					navigate(getPagePath($router, "network_device", { id: deviceID }))
					return
				}
				const params = new URLSearchParams({
					target_ids: id,
					aggregate: "sum",
					time_mode: "fixed",
					window,
					max_data_points: "600",
				})
				const responses = await Promise.all(
					networkCharts.map((chart) =>
						api.send<VMRangeResponse>(`/api/v1/metrics/aggregate?${params.toString()}&metric=${chart.name}`, {})
					)
				)
				setSeries(
					responses.map((response, index) => ({
						name: networkCharts[index].label,
						unit: networkCharts[index].unit,
						values: vmValues(response),
					}))
				)
			} else {
				const params = new URLSearchParams({
					target_id: id,
					time_mode: "fixed",
					window,
					max_data_points: "600",
				})
				const responses = await Promise.all(
					targetCharts.map((chart) =>
						api.send<VMRangeResponse>(`/api/v1/metrics/query?${params.toString()}&metric=${chart.name}`, {})
					)
				)
				setSeries(
					responses.map((response, index) => ({
						name: targetCharts[index].label,
						unit: targetCharts[index].unit,
						values: vmValues(response),
					}))
				)
			}
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load target metrics`)
		} finally {
			setLoading(false)
		}
	}, [id, t, window])

	const openDeletePreview = async () => {
		setError("")
		try {
			const preview = await api.send<{ impacts?: DeleteImpact[] }>(`/api/v1/devices/${id}/delete-preview`, {})
			setDeleteImpacts(preview.impacts ?? [])
		} catch {
			// Preview is advisory; deletion still confirms with an empty list.
			setDeleteImpacts([])
		}
		setDeleteOpen(true)
	}

	const deleteTarget = async () => {
		setDeleteOpen(false)
		setLoading(true)
		setError("")
		try {
			// Deletion is asynchronous: the API answers 202 with a job id and a
			// worker performs the cascade. Poll the job until it finishes.
			const response = await api.send<{ job_id?: string } | null>(`/api/v1/devices/${id}`, { method: "DELETE" })
			const jobID = response?.job_id
			if (jobID) {
				for (let attempt = 0; attempt < 120; attempt++) {
					const job = await api.send<{ status?: string; last_error_detail?: string }>(
						`/api/v1/operation-jobs/${jobID}`,
						{}
					)
					if (job.status === "succeeded") {
						break
					}
					if (job.status === "failed" || job.status === "canceled") {
						throw new Error(job.last_error_detail || t`Failed to delete target`)
					}
					await new Promise((resolve) => globalThis.setTimeout(resolve, 1000))
				}
			}
			navigate(getPagePath($router, "targets"))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to delete target`)
			setLoading(false)
		}
	}

	useEffect(() => {
		document.title = `${t`Target`} / Watchdog`
		refresh()
	}, [refresh, t])

	useEffect(() => {
		const timer = globalThis.setInterval(refresh, 30_000)
		return () => globalThis.clearInterval(timer)
	}, [refresh])

	useEffect(() => {
		if (!chartRef.current || loading || error) {
			return
		}
		if (series.every((item) => item.values.length === 0)) {
			disposeChart(chartInstance.current)
			chartInstance.current = null
			return
		}
		chartInstance.current = updateLineChart(chartInstance.current, chartRef.current, {
			series,
			yFormatter: (value) => formatValue(value, commonSeriesUnit(series)),
		})
		return () => {
			disposeChart(chartInstance.current)
			chartInstance.current = null
		}
	}, [error, loading, series])

	const name = target?.name ?? id

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex min-w-0 items-center gap-2">
					<Link
						href={getPagePath($router, "targets")}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to targets`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<CrosshairIcon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="truncate text-xl font-semibold tracking-normal">{name}</h1>
				</div>
				<div className="flex items-center gap-2">
					<Select value={window} onValueChange={setWindow}>
						<SelectTrigger className="w-28">
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="5m">5m</SelectItem>
							<SelectItem value="10m">10m</SelectItem>
							<SelectItem value="15m">15m</SelectItem>
							<SelectItem value="30m">30m</SelectItem>
							<SelectItem value="1h">1h</SelectItem>
							<SelectItem value="24h">24h</SelectItem>
							<SelectItem value="7d">7d</SelectItem>
							<SelectItem value="30d">30d</SelectItem>
						</SelectContent>
					</Select>
					<Link
						href={getPagePath($router, "target_edit", { id })}
						className={buttonVariants({ variant: "outline", size: "sm" })}
					>
						<PencilIcon className="me-2 h-4 w-4" />
						<Trans>Edit</Trans>
					</Link>
					<Button variant="outline" size="sm" onClick={openDeletePreview} disabled={loading}>
						<Trash2Icon className="me-2 h-4 w-4" />
						<Trans>Delete</Trans>
					</Button>
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
			</div>

			<div className="grid gap-3 md:grid-cols-4">
				<InfoCell label={t`Type`} value={target?.kind} />
				<InfoCell label={t`Host`} value={target?.host} />
				<InfoCell label={t`Status`} value={target?.status} />
				<InfoCell label="ID" value={id} mono />
				<InfoCell label={t`Labels`} value={formatLabels(target?.labels)} />
			</div>

			<div className="rounded-md border border-border p-4">
				{loading ? (
					<div className="text-sm text-muted-foreground">
						<Trans>Loading...</Trans>
					</div>
				) : null}
				{error ? <div className="text-sm text-destructive">{error}</div> : null}
				<div ref={chartRef} className="h-[360px] w-full" />
				{!loading && !error && series.every((item) => item.values.length === 0) ? (
					<div className="text-sm text-muted-foreground">
						<Trans>No data</Trans>
					</div>
				) : null}
			</div>

			<AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
				<AlertDialogContent>
					<AlertDialogHeader>
						<AlertDialogTitle>
							<Trans>Delete {name}?</Trans>
						</AlertDialogTitle>
						<AlertDialogDescription>
							<Trans>This shows everything the deletion touches. Removed data cannot be recovered.</Trans>
						</AlertDialogDescription>
					</AlertDialogHeader>
					<div className="max-h-64 space-y-2 overflow-y-auto text-sm">
						{deleteImpacts.length === 0 ? (
							<div className="text-muted-foreground">
								<Trans>No dependent resources found.</Trans>
							</div>
						) : (
							deleteImpacts.map((impact) => (
								<div
									key={impact.resource_type}
									className="flex items-start justify-between gap-3 rounded-md border border-border p-2"
								>
									<div className="min-w-0">
										<div className="font-medium">{impactLabel(impact.resource_type)}</div>
										{impact.items?.length ? (
											<div className="truncate text-xs text-muted-foreground">
												{impact.items.map((item) => item.name || item.id).join(", ")}
											</div>
										) : null}
										{impact.detail ? <div className="text-xs text-muted-foreground">{impact.detail}</div> : null}
									</div>
									<div className="flex shrink-0 items-center gap-2">
										{impact.count > 0 ? <span className="text-xs text-muted-foreground">×{impact.count}</span> : null}
										<Badge variant={impact.behavior === "deleted" ? "destructive" : "outline"} className="font-normal">
											{impact.behavior === "deleted" ? <Trans>deleted</Trans> : <Trans>detached</Trans>}
										</Badge>
									</div>
								</div>
							))
						)}
					</div>
					<AlertDialogFooter>
						<AlertDialogCancel>
							<Trans>Cancel</Trans>
						</AlertDialogCancel>
						<AlertDialogAction onClick={deleteTarget}>
							<Trans>Delete</Trans>
						</AlertDialogAction>
					</AlertDialogFooter>
				</AlertDialogContent>
			</AlertDialog>
		</div>
	)
})

function impactLabel(resourceType: string) {
	switch (resourceType) {
		case "network_device":
			return <Trans>Devices</Trans>
		case "network_port":
			return <Trans>Ports</Trans>
		case "aggregate_graph":
			return <Trans>Aggregate graphs</Trans>
		case "agent":
			return <Trans>Agents</Trans>
		case "collector_projection":
			return <Trans>Collector projections</Trans>
		case "agent_run_history":
			return <Trans>Run history</Trans>
		case "metric_retention_policy":
			return <Trans>Retention policies</Trans>
		case "export_task":
			return <Trans>Export tasks</Trans>
		case "metric_series":
			return <Trans>Metric series</Trans>
		default:
			return resourceType
	}
}

function InfoCell({ label, value, mono }: { label: React.ReactNode; value?: string; mono?: boolean }) {
	return (
		<div className="rounded-md border border-border p-3">
			<div className="text-xs text-muted-foreground">{label}</div>
			<div className={cn("mt-1 truncate text-sm", mono && "font-mono text-xs")}>{value || "-"}</div>
		</div>
	)
}

function vmValues(response: VMRangeResponse) {
	const values = response.data?.result?.[0]?.values ?? []
	return values
		.map(([time, value]) => ({ time: Math.round(time * 1000), value: Number(value) }))
		.filter((point) => Number.isFinite(point.time) && Number.isFinite(point.value))
}

function formatValue(value?: number | null, unit?: string) {
	return formatMetricValue(value, unit)
}

function commonSeriesUnit(series: { unit?: string }[]) {
	const units = new Set(series.map((item) => item.unit).filter(Boolean))
	return units.size === 1 ? [...units][0] : undefined
}

function formatLabels(labels?: Record<string, string>) {
	if (!labels || Object.keys(labels).length === 0) {
		return "-"
	}
	return Object.entries(labels)
		.map(([key, value]) => `${key}=${value}`)
		.join(", ")
}

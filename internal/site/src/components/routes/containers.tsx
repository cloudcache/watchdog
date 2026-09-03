import { Trans, useLingui } from "@lingui/react/macro"
import { ContainerIcon, RefreshCwIcon } from "lucide-react"
import { memo, useCallback, useEffect, useRef, useState } from "react"
import { Button } from "@/components/ui/button"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { pb } from "@/lib/api"
import { formatBitsPerSecond } from "@/lib/metric-format"
import { createLineChart, disposeChart } from "@/lib/vchart"

type TargetRecord = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	kind?: string
}

type TargetsResponse = {
	items?: TargetRecord[]
}

type VMRangeResponse = {
	data?: {
		result?: {
			metric?: Record<string, string>
			values?: [number, string][]
		}[]
	}
}

type Series = {
	name: string
	values: { time: number; value: number }[]
}

const containerMetrics = {
	cpu: "watchdog_container_cpu_percent",
	memory: "watchdog_container_memory_bytes",
	netTx: "watchdog_container_net_tx_bps",
	netRx: "watchdog_container_net_rx_bps",
} as const

export default memo(() => {
	const { t } = useLingui()
	const cpuRef = useRef<HTMLDivElement>(null)
	const memoryRef = useRef<HTMLDivElement>(null)
	const networkRef = useRef<HTMLDivElement>(null)
	const cpuChart = useRef<ReturnType<typeof createLineChart> | null>(null)
	const memoryChart = useRef<ReturnType<typeof createLineChart> | null>(null)
	const networkChart = useRef<ReturnType<typeof createLineChart> | null>(null)
	const [targets, setTargets] = useState<TargetRecord[]>([])
	const [targetID, setTargetID] = useState("")
	const [window, setWindow] = useState("1h")
	const [cpu, setCPU] = useState<Series[]>([])
	const [memory, setMemory] = useState<Series[]>([])
	const [network, setNetwork] = useState<Series[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const targetData = await pb.send<TargetsResponse>("/api/v1/targets", {})
			const items = targetData.items ?? []
			setTargets(items)
			const selectedTargetID = targetID || items[0]?.ID || items[0]?.id || ""
			if (!targetID && selectedTargetID) {
				setTargetID(selectedTargetID)
			}
			if (!selectedTargetID) {
				setCPU([])
				setMemory([])
				setNetwork([])
				return
			}
			const params = new URLSearchParams({
				target_id: selectedTargetID,
				time_mode: "fixed",
				window,
				max_data_points: "600",
			})
			const [cpuResponse, memoryResponse, netTxResponse, netRxResponse] = await Promise.all([
				getMetric(containerMetrics.cpu, params),
				getMetric(containerMetrics.memory, params),
				getMetric(containerMetrics.netTx, params),
				getMetric(containerMetrics.netRx, params),
			])
			setCPU(makeSeries(cpuResponse, 1, containerName))
			setMemory(makeSeries(memoryResponse, 1 / 1024 / 1024, containerName))
			setNetwork([
				...makeSeries(netTxResponse, 1, (metric) => `${containerName(metric)} tx`),
				...makeSeries(netRxResponse, 1, (metric) => `${containerName(metric)} rx`),
			])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load container metrics`)
		} finally {
			setLoading(false)
		}
	}, [t, targetID, window])

	useEffect(() => {
		document.title = `${t`Containers`} / Watchdog`
		refresh()
	}, [refresh, t])

	useEffect(() => {
		renderChart(cpuRef.current, cpuChart, cpu, formatPercent)
		renderChart(memoryRef.current, memoryChart, memory, formatMegabytes)
		renderChart(networkRef.current, networkChart, network, formatBitsPerSecond)
		return () => {
			disposeChart(cpuChart.current)
			cpuChart.current = null
			disposeChart(memoryChart.current)
			memoryChart.current = null
			disposeChart(networkChart.current)
			networkChart.current = null
		}
	}, [cpu, memory, network])

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex min-w-0 items-center gap-2">
					<ContainerIcon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="truncate text-xl font-semibold tracking-normal">
						<Trans>Containers</Trans>
					</h1>
				</div>
				<div className="flex items-center gap-2">
					<Select value={targetID} onValueChange={setTargetID}>
						<SelectTrigger className="w-56">
							<SelectValue placeholder={t`Target`} />
						</SelectTrigger>
						<SelectContent>
							{targets.map((target) => {
								const id = target.ID ?? target.id ?? ""
								return (
									<SelectItem key={id} value={id}>
										{target.Name ?? target.name ?? id}
									</SelectItem>
								)
							})}
						</SelectContent>
					</Select>
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
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
			</div>

			{error ? <div className="rounded-md border border-border p-3 text-sm text-destructive">{error}</div> : null}

			<ChartPanel title={t`CPU`} empty={!loading && cpu.length === 0}>
				<div ref={cpuRef} className="h-[320px] w-full" />
			</ChartPanel>
			<ChartPanel title={t`Memory`} empty={!loading && memory.length === 0}>
				<div ref={memoryRef} className="h-[320px] w-full" />
			</ChartPanel>
			<ChartPanel title={t`Network`} empty={!loading && network.length === 0}>
				<div ref={networkRef} className="h-[320px] w-full" />
			</ChartPanel>
		</div>
	)
})

function ChartPanel({ title, empty, children }: { title: string; empty: boolean; children: React.ReactNode }) {
	return (
		<div className="rounded-md border border-border p-4">
			<div className="mb-3 text-sm font-medium">{title}</div>
			{empty ? <div className="mb-3 text-sm text-muted-foreground">No samples found.</div> : null}
			{children}
		</div>
	)
}

async function getMetric(metric: string, baseParams: URLSearchParams) {
	const params = new URLSearchParams(baseParams)
	params.set("metric", metric)
	return await pb.send<VMRangeResponse>(`/api/v1/metrics/query?${params.toString()}`, {})
}

function makeSeries(
	response: VMRangeResponse,
	multiplier: number,
	name: (metric?: Record<string, string>) => string
): Series[] {
	return (response.data?.result ?? []).map((item) => ({
		name: name(item.metric),
		values: (item.values ?? []).map(([time, value]) => ({
			time: Math.round(time * 1000),
			value: (Number(value) || 0) * multiplier,
		})),
	}))
}

function renderChart(
	el: HTMLDivElement | null,
	ref: React.MutableRefObject<ReturnType<typeof createLineChart> | null>,
	series: Series[],
	yFormatter: (value?: number) => string
) {
	if (!el || series.length === 0) {
		disposeChart(ref.current)
		ref.current = null
		return
	}
	disposeChart(ref.current)
	ref.current = createLineChart(el, { series, yFormatter })
}

function containerName(metric?: Record<string, string>) {
	return metric?.container_name || metric?.container || metric?.name || "container"
}

function formatPercent(value?: number) {
	return value === undefined ? "-" : `${value.toFixed(1)}%`
}

function formatMegabytes(value?: number) {
	return value === undefined ? "-" : `${value.toFixed(1)} MB`
}

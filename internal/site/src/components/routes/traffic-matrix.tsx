import { Trans, useLingui } from "@lingui/react/macro"
import { BarChart3Icon, RefreshCwIcon } from "lucide-react"
import { memo, useCallback, useEffect, useRef, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { createLineChart, disposeChart } from "@/lib/vchart"
import { pb } from "@/lib/api"

type VMRangeResponse = {
	status: string
	data: {
		resultType: string
		result: {
			metric: Record<string, string>
			values: [number, string][]
		}[]
	}
}

type AddressSetItem = { id: string; name: string }
type AddressSetList = { items: AddressSetItem[] }
type DeviceItem = { ID?: string; id?: string; SysName?: string; sys_name?: string }
type DeviceList = { items: DeviceItem[] }

const TIME_OPTIONS = [
	{ value: "5m", label: "5 minutes" },
	{ value: "15m", label: "15 minutes" },
	{ value: "1h", label: "1 hour" },
	{ value: "6h", label: "6 hours" },
	{ value: "24h", label: "24 hours" },
	{ value: "7d", label: "7 days" },
	{ value: "30d", label: "30 days" },
	{ value: "custom", label: "Custom" },
]

function getStartEnd(timeRange: string): { start: string; end: string; step: string } {
	const end = new Date()
	let start = new Date()
	let step = "60"
	switch (timeRange) {
		case "5m": start = new Date(end.getTime() - 5 * 60_000); step = "15"; break
		case "15m": start = new Date(end.getTime() - 15 * 60_000); step = "30"; break
		case "1h": start = new Date(end.getTime() - 60 * 60_000); step = "60"; break
		case "6h": start = new Date(end.getTime() - 6 * 60 * 60_000); step = "300"; break
		case "24h": start = new Date(end.getTime() - 24 * 60 * 60_000); step = "300"; break
		case "7d": start = new Date(end.getTime() - 7 * 24 * 60 * 60_000); step = "1800"; break
		case "30d": start = new Date(end.getTime() - 30 * 24 * 60 * 60_000); step = "3600"; break
	}
	return { start: start.toISOString(), end: end.toISOString(), step }
}

function formatBytes(bps: number): string {
	if (bps > 1e9) return (bps / 1e9).toFixed(2) + " Gbps"
	if (bps > 1e6) return (bps / 1e6).toFixed(1) + " Mbps"
	if (bps > 1e3) return (bps / 1e3).toFixed(1) + " Kbps"
	return bps.toFixed(0) + " bps"
}

export default memo(function TrafficMatrix() {
	const { t } = useLingui()
	const [timeRange, setTimeRange] = useState("1h")
	const [customStart, setCustomStart] = useState("")
	const [customEnd, setCustomEnd] = useState("")
	const [addressSets, setAddressSets] = useState<AddressSetItem[]>([])
	const [devices, setDevices] = useState<DeviceItem[]>([])
	const [selectedSet, setSelectedSet] = useState("all")
	const [selectedDevice, setSelectedDevice] = useState("all")
	const [chartData, setChartData] = useState<{ labels: string[]; series: { name: string; values: number[] }[] }>({ labels: [], series: [] })
	const [matrix, setMatrix] = useState<{ device: string; set: string; bps: number }[]>([])
	const [loading, setLoading] = useState(false)
	const [error, setError] = useState("")
	const chartRef = useRef<HTMLDivElement>(null)
	const chartInstance = useRef<ReturnType<typeof createLineChart> | null>(null)

	const loadOptions = useCallback(async () => {
		try {
			const [sets, devs] = await Promise.all([
				pb.send<AddressSetList>("/api/v1/address-sets", {}),
				pb.send<DeviceList>("/api/v1/network/devices", {}),
			])
			setAddressSets(sets.items ?? [])
			setDevices(devs.items ?? [])
		} catch {}
	}, [])

	useEffect(() => { loadOptions() }, [loadOptions])

	const buildQuery = useCallback(() => {
		const parts: string[] = []
		if (selectedDevice !== "all") {
			parts.push(`device_id="${selectedDevice}"`)
		}
		if (selectedSet !== "all") {
			parts.push(`address_set="${selectedSet}"`)
		}
		const label = parts.length > 0 ? `{${parts.join(",")}}` : ""
		return `sum by (device_id, address_set, direction) (rate(watchdog_sflow_traffic_bytes${label}[5m])) * 8`
	}, [selectedDevice, selectedSet])

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			let start: string, end: string, step: string
			if (timeRange === "custom") {
				start = new Date(customStart).toISOString()
				end = new Date(customEnd).toISOString()
				step = "300"
			} else {
				const se = getStartEnd(timeRange)
				start = se.start; end = se.end; step = se.step
			}
			const query = buildQuery()
			const params = new URLSearchParams({ query, start, end, step })
			const resp = await pb.send<VMRangeResponse>(`/api/v1/metrics/vmquery?${params.toString()}`, {})
			if (resp.status !== "success" || !resp.data?.result) {
				setChartData({ labels: [], series: [] })
				setMatrix([])
				return
			}
			const labels: string[] = []
			const tsSet = new Set<number>()
			for (const r of resp.data.result) {
				for (const [ts] of r.values) tsSet.add(ts)
			}
			labels.push(...[...tsSet].sort().map((ts) => new Date(ts * 1000).toISOString()))
			const series = resp.data.result.map((r) => ({
				name: `${r.metric.device_id?.slice(0, 12) ?? "?"} / ${r.metric.address_set ?? "?"} / ${r.metric.direction ?? "?"}`,
				values: labels.map((label) => {
					const ts = new Date(label).getTime() / 1000
					const found = r.values.find((v) => v[0] === ts)
					return found ? Number(found[1]) : 0
				}),
			}))
			setChartData({ labels, series })

			// Build matrix: sum per device × set
			const matrixMap: Record<string, number> = {}
			for (const r of resp.data.result) {
				const dev = r.metric.device_id?.slice(0, 12) ?? "?"
				const set = r.metric.address_set ?? "?"
				const key = `${dev}|${set}`
				const maxVal = Math.max(...r.values.map((v) => Number(v[1])), 0)
				matrixMap[key] = (matrixMap[key] ?? 0) + maxVal
			}
			setMatrix(Object.entries(matrixMap).map(([k, bps]) => {
				const [device, set] = k.split("|")
				return { device, set, bps }
			}).sort((a, b) => b.bps - a.bps))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load`)
		}
		setLoading(false)
	}, [timeRange, customStart, customEnd, buildQuery, t])

	useEffect(() => {
		if (chartRef.current && chartData.labels.length > 0 && chartData.series.length > 0) {
			const chartDataForVChart = chartData.labels.map((label, i) => ({
				time: label,
				...Object.fromEntries(chartData.series.map((s) => [s.name, s.values[i]])),
			}))
			const seriesDefs = chartData.series.map((s, idx) => ({
				label: s.name,
				dataKey: (data: any) => data[s.name],
				color: idx,
			}))
			chartInstance.current?.dispose?.()
			chartInstance.current = createLineChart(chartRef.current, {
				chartData: { records: chartDataForVChart, stats: { lastUpdated: Date.now() } as any },
				dataSeries: seriesDefs as any,
			} as any)
		}
		return () => { disposeChart(chartInstance.current) }
	}, [chartData])

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<BarChart3Icon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal"><Trans>Traffic Matrix</Trans></h1>
				</div>
				<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Refresh</Trans>
				</Button>
			</div>

			<div className="grid gap-3 rounded-md border border-border bg-card p-4">
				<div className="flex flex-wrap items-end gap-3">
					<div className="grid gap-1.5">
						<Label className="text-xs"><Trans>Time Range</Trans></Label>
						<Select value={timeRange} onValueChange={setTimeRange}>
							<SelectTrigger className="w-40"><SelectValue /></SelectTrigger>
							<SelectContent>
								{TIME_OPTIONS.map((o) => (
									<SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
								))}
							</SelectContent>
						</Select>
					</div>
					{timeRange === "custom" && (
						<>
							<div className="grid gap-1.5">
								<Label className="text-xs"><Trans>Start</Trans></Label>
								<Input type="datetime-local" value={customStart} onChange={(e) => setCustomStart(e.target.value)} className="w-44" />
							</div>
							<div className="grid gap-1.5">
								<Label className="text-xs"><Trans>End</Trans></Label>
								<Input type="datetime-local" value={customEnd} onChange={(e) => setCustomEnd(e.target.value)} className="w-44" />
							</div>
						</>
					)}
					<div className="grid gap-1.5">
						<Label className="text-xs"><Trans>Device</Trans></Label>
						<Select value={selectedDevice} onValueChange={setSelectedDevice}>
							<SelectTrigger className="w-44"><SelectValue /></SelectTrigger>
							<SelectContent>
								<SelectItem value="all"><Trans>All Devices</Trans></SelectItem>
								{devices.map((d) => (
									<SelectItem key={d.ID ?? d.id} value={d.ID ?? d.id ?? ""}>
										{d.SysName ?? d.sys_name ?? d.ID ?? d.id}
									</SelectItem>
								))}
							</SelectContent>
						</Select>
					</div>
					<div className="grid gap-1.5">
						<Label className="text-xs"><Trans>Address Set</Trans></Label>
						<Select value={selectedSet} onValueChange={setSelectedSet}>
							<SelectTrigger className="w-44"><SelectValue /></SelectTrigger>
							<SelectContent>
								<SelectItem value="all"><Trans>All Sets</Trans></SelectItem>
								{addressSets.map((s) => (
									<SelectItem key={s.id} value={s.name}>{s.name}</SelectItem>
								))}
							</SelectContent>
						</Select>
					</div>
					<Button size="sm" onClick={refresh} disabled={loading}>
						<Trans>Query</Trans>
					</Button>
				</div>
			</div>

			{error && <div className="text-sm text-destructive">{error}</div>}

			{chartData.series.length > 0 && (
				<div className="rounded-md border border-border bg-card p-4">
					<div ref={chartRef} className="h-80" />
				</div>
			)}

			{matrix.length > 0 && (
				<div className="rounded-md border border-border bg-card overflow-hidden">
					<Table>
						<TableHeader>
							<TableRow>
								<TableHead><Trans>Device</Trans></TableHead>
								<TableHead><Trans>Address Set</Trans></TableHead>
								<TableHead className="text-right"><Trans>Peak Rate</Trans></TableHead>
							</TableRow>
						</TableHeader>
						<TableBody>
							{matrix.map((row, i) => (
								<TableRow key={i}>
									<TableCell className="font-mono text-xs">{row.device}</TableCell>
									<TableCell className="text-sm">{row.set}</TableCell>
									<TableCell className="text-right font-mono text-sm">{formatBytes(row.bps)}</TableCell>
								</TableRow>
							))}
						</TableBody>
					</Table>
				</div>
			)}
		</div>
	)
})

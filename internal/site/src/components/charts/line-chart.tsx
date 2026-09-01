import { type ReactNode, useEffect, useMemo, useRef, useState } from "react"
import type { ChartData, SystemStatsRecord } from "@/types"
import { useIntersectionObserver } from "@/lib/use-intersection-observer"
import { createLineChart, disposeChart } from "@/lib/vchart"

export type DataPoint<T = SystemStatsRecord> = {
	label: string
	dataKey: (data: T) => number | null | undefined
	color: number | string
	stackId?: string | number
	order?: number
	strokeOpacity?: number
	activeDot?: boolean
}

type LineChartDefaultProps = {
	chartData: ChartData
	// biome-ignore lint/suspicious/noExplicitAny: accepts different data source types (systemStats or containerData)
	customData?: any[]
	max?: number
	maxToggled?: boolean
	tickFormatter: (value: number, index: number) => string
	// biome-ignore lint/suspicious/noExplicitAny: recharts tooltip item interop
	contentFormatter: (item: any, key: string) => ReactNode
	// biome-ignore lint/suspicious/noExplicitAny: accepts DataPoint with different generic types
	dataPoints?: DataPoint<any>[]
	domain?: unknown
	legend?: boolean
	showTotal?: boolean
	// biome-ignore lint/suspicious/noExplicitAny: recharts tooltip item interop
	itemSorter?: (a: any, b: any) => number
	reverseStackOrder?: boolean
	hideYAxis?: boolean
	filter?: string
	truncate?: boolean
	chartProps?: Record<string, unknown>
}

export default function LineChartDefault(props: LineChartDefaultProps) {
	const { chartData, customData, maxToggled, tickFormatter, dataPoints } = props
	const { isIntersecting, ref } = useIntersectionObserver({ freeze: false })
	const chartRef = useRef<HTMLDivElement>(null)
	const chartInstance = useRef<ReturnType<typeof createLineChart> | null>(null)
	const sourceData = customData ?? chartData.systemStats
	const [displayData, setDisplayData] = useState(sourceData)
	const [displayMaxToggled, setDisplayMaxToggled] = useState(maxToggled)

	// Reduce chart redraws by only updating while visible or when chart time changes
	useEffect(() => {
		const shouldPrimeData = sourceData.length && !displayData.length
		const sourceChanged = sourceData !== displayData
		const shouldUpdate = shouldPrimeData || (sourceChanged && isIntersecting)
		if (shouldUpdate) {
			setDisplayData(sourceData)
		}
		if (isIntersecting && maxToggled !== displayMaxToggled) {
			setDisplayMaxToggled(maxToggled)
		}
	}, [displayData, displayMaxToggled, isIntersecting, maxToggled, sourceData])

	const series = useMemo(() => makeSeries(displayData, dataPoints), [displayData, dataPoints])

	useEffect(() => {
		if (!chartRef.current || !isIntersecting || series.every((item) => item.values.length === 0)) {
			return
		}
		disposeChart(chartInstance.current)
		chartInstance.current = createLineChart(chartRef.current, {
			series,
			yFormatter: (value) => tickFormatter(value, 0),
		})
		return () => disposeChart(chartInstance.current)
	}, [isIntersecting, series, tickFormatter])

	if (!displayData.length) {
		return null
	}
	return (
		<div ref={ref} className="absolute h-full w-full bg-card">
			<div ref={chartRef} className="h-full w-full" />
		</div>
	)
}

function makeSeries(
	// biome-ignore lint/suspicious/noExplicitAny: chart points accept different data shapes.
	data: any[],
	// biome-ignore lint/suspicious/noExplicitAny: accepts DataPoint with different generic types.
	dataPoints?: DataPoint<any>[]
) {
	return (dataPoints ?? []).map((point) => ({
		name: point.label,
		values: data
			.map((record) => ({
				time: Number(record?.created ?? 0),
				value: point.dataKey(record),
			}))
			.filter(
				(record): record is { time: number; value: number } => Boolean(record.time) && typeof record.value === "number"
			),
	}))
}

import { useStore } from "@nanostores/react"
import { HistoryIcon } from "lucide-react"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { $chartTime, $chartTimeRange } from "@/lib/stores"
import { chartTimeData, cn, compareSemVer, parseSemVer } from "@/lib/utils"
import type { ChartTimes, SemVer } from "@/types"
import { memo } from "react"

export default memo(function ChartTimeSelect({
	className,
	agentVersion,
}: {
	className?: string
	agentVersion: SemVer
}) {
	const chartTime = useStore($chartTime)
	const chartTimeRange = useStore($chartTimeRange)

	// remove chart times that are not supported by the system agent version
	const availableChartTimes = Object.entries(chartTimeData).filter(([_, { minVersion }]) => {
		if (!minVersion) {
			return true
		}
		return compareSemVer(agentVersion, parseSemVer(minVersion)) >= 0
	})

	return (
		<div className="flex flex-wrap items-center gap-2">
			<Select defaultValue="1h" value={chartTime} onValueChange={setChartTime}>
				<SelectTrigger className={cn(className, "relative ps-10 pe-5")}>
					<HistoryIcon className="h-4 w-4 absolute start-4 top-1/2 -translate-y-1/2 opacity-85" />
					<SelectValue />
				</SelectTrigger>
				<SelectContent>
					{availableChartTimes.map(([value, { label }]) => (
						<SelectItem key={value} value={value}>
							{label()}
						</SelectItem>
					))}
				</SelectContent>
			</Select>
			{chartTime === "custom" ? (
				<>
					<Input
						type="datetime-local"
						value={chartTimeRange.start}
						onChange={(event) => $chartTimeRange.set({ ...chartTimeRange, start: event.target.value })}
						className="w-44"
					/>
					<Input
						type="datetime-local"
						value={chartTimeRange.end}
						onChange={(event) => $chartTimeRange.set({ ...chartTimeRange, end: event.target.value })}
						className="w-44"
					/>
				</>
			) : null}
		</div>
	)
})

function setChartTime(value: ChartTimes) {
	if (value === "custom") {
		const current = $chartTimeRange.get()
		if (!current.start || !current.end) {
			const end = new Date()
			const start = new Date(end.getTime() - 60 * 60 * 1000)
			$chartTimeRange.set({
				start: toDateTimeLocalValue(start),
				end: toDateTimeLocalValue(end),
			})
		}
	}
	$chartTime.set(value)
}

function toDateTimeLocalValue(date: Date) {
	const offsetDate = new Date(date.getTime() - date.getTimezoneOffset() * 60_000)
	return offsetDate.toISOString().slice(0, 16)
}

import { Building2Icon, DatabaseIcon, HandshakeIcon } from "lucide-react"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"
import { trafficViewLabel, trafficViewModes, type TrafficViewMode } from "@/lib/traffic-view"

export function TrafficViewSwitcher({
	value,
	onChange,
	allowRaw = true,
	className,
}: {
	value: TrafficViewMode
	onChange: (value: TrafficViewMode) => void
	allowRaw?: boolean
	className?: string
}) {
	return (
		<div className={cn("flex flex-wrap items-center gap-1 rounded-md border border-border p-1", className)}>
			{trafficViewModes.map((mode) => {
				const disabled = mode !== "customer" && !allowRaw
				const Icon = mode === "customer" ? Building2Icon : mode === "supplier" ? HandshakeIcon : DatabaseIcon
				return (
					<Button
						key={mode}
						type="button"
						variant={value === mode ? "default" : "ghost"}
						size="sm"
						disabled={disabled}
						onClick={() => onChange(mode)}
						className="h-8"
					>
						<Icon className="me-2 h-4 w-4" />
						{trafficViewLabel(mode)}
					</Button>
				)
			})}
		</div>
	)
}

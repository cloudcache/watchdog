import { t } from "@lingui/core/macro"
import { CpuIcon, HardDriveIcon, MemoryStickIcon, ServerIcon } from "lucide-react"
import { EthernetIcon, GpuIcon } from "@/components/ui/icons"
import type { AlertInfo } from "@/types"
import { ThermometerIcon, BatteryMediumIcon, HourglassIcon } from "@/components/ui/icons"

/** Alert info for each alert type */
export const alertInfo: Record<string, AlertInfo> = {
	Status: {
		name: () => t`Status`,
		unit: "",
		icon: ServerIcon,
		desc: () => t`Triggers when status switches between up and down`,
		/** "for x minutes" is appended to desc when only one value */
		singleDesc: () => `${t`System`} ${t`Down`}`,
	},
	CPU: {
		name: () => t`CPU Usage`,
		unit: "%",
		icon: CpuIcon,
		desc: () => t`Triggers when CPU usage exceeds a threshold`,
	},
	Memory: {
		name: () => t`Memory Usage`,
		unit: "%",
		icon: MemoryStickIcon,
		desc: () => t`Triggers when memory usage exceeds a threshold`,
	},
	Disk: {
		name: () => t`Disk Usage`,
		unit: "%",
		icon: HardDriveIcon,
		desc: () => t`Triggers when usage of any disk exceeds a threshold`,
	},
	Bandwidth: {
		name: () => t`Bandwidth`,
		unit: " MB/s",
		icon: EthernetIcon,
		desc: () => t`Triggers when combined up/down exceeds a threshold`,
		max: 250,
	},
	GPU: {
		name: () => t`GPU Usage`,
		unit: "%",
		icon: GpuIcon,
		desc: () => t`Triggers when GPU usage exceeds a threshold`,
	},
	Temperature: {
		name: () => t`Temperature`,
		unit: "°C",
		icon: ThermometerIcon,
		desc: () => t`Triggers when any sensor exceeds a threshold`,
	},
	LoadAvg1: {
		name: () => t`Load Average 1m`,
		unit: "",
		icon: HourglassIcon,
		max: 100,
		min: 0.1,
		start: 10,
		step: 0.1,
		desc: () => t`Triggers when 1 minute load average exceeds a threshold`,
	},
	LoadAvg5: {
		name: () => t`Load Average 5m`,
		unit: "",
		icon: HourglassIcon,
		max: 100,
		min: 0.1,
		start: 10,
		step: 0.1,
		desc: () => t`Triggers when 5 minute load average exceeds a threshold`,
	},
	LoadAvg15: {
		name: () => t`Load Average 15m`,
		unit: "",
		icon: HourglassIcon,
		min: 0.1,
		max: 100,
		start: 10,
		step: 0.1,
		desc: () => t`Triggers when 15 minute load average exceeds a threshold`,
	},
	Battery: {
		name: () => t`Battery`,
		unit: "%",
		icon: BatteryMediumIcon,
		desc: () => t`Triggers when battery charge drops below a threshold`,
		start: 20,
		invert: true,
	},
} as const

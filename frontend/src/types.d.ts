import type { HourFormat, Unit } from "@/lib/enums"

// global window properties
declare global {
	var WATCHDOG: {
		BASE_PATH: string
		VERSION: string
		API_URL: string
	}
	var WATCHDOG_CONFIG: Partial<typeof WATCHDOG>
}

export type ChartTimes =
	| "1m"
	| "5m"
	| "10m"
	| "15m"
	| "30m"
	| "1h"
	| "6h"
	| "12h"
	| "24h"
	| "3d"
	| "1w"
	| "30d"
	| "custom"

export interface ChartTimeData {
	[key: string]: {
		type: string
		expectedInterval: number
		label: () => string
		ticks?: number
		format: (timestamp: string) => string
		getOffset: (endTime: Date) => Date
		minVersion?: string
	}
}

export interface UserSettings {
	chartTime: ChartTimes
	emails?: string[]
	webhooks?: string[]
	unitTemp?: Unit
	unitNet?: Unit
	unitDisk?: Unit
	colorWarn?: number
	colorCrit?: number
	hourFormat?: HourFormat
	layoutWidth?: number
}

export interface SemVer {
	major: number
	minor: number
	patch: number
}

import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import {
	CrosshairIcon,
	FileDownIcon,
	GaugeIcon,
	LayoutDashboardIcon,
	NetworkIcon,
	ReceiptTextIcon,
	ShieldCheckIcon,
} from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { $router, Link } from "@/components/router"
import { buttonVariants } from "@/components/ui/button"
import { api } from "@/lib/api"
import { cn } from "@/lib/utils"

type ListResponse = {
	items?: unknown[]
}

type OverviewMetric = {
	key: string
	label: string
	value: string
	href: string
	icon: React.ComponentType<{ className?: string; strokeWidth?: number }>
}

export default memo(() => {
	const { t } = useLingui()
	const [metrics, setMetrics] = useState<OverviewMetric[]>([])

	const refresh = useCallback(async () => {
		const [networkTargets, targets, exports, billing, permissions, snmp] = await Promise.allSettled([
			countItems("/api/v1/network/devices/summary"),
			countItems("/api/v1/targets"),
			countItems("/api/v1/exports"),
			countItems("/api/v1/billing/accounts"),
			countItems("/api/v1/permissions"),
			countItems("/api/v1/snmp/profiles"),
		])
		setMetrics([
			{
				key: "targets",
				label: t`Targets`,
				value: resultValue(targets),
				href: getPagePath($router, "targets"),
				icon: CrosshairIcon,
			},
			{
				key: "network-targets",
				label: t`Network Targets`,
				value: resultValue(networkTargets),
				href: getPagePath($router, "network"),
				icon: NetworkIcon,
			},
			{
				key: "exports",
				label: t`Exports`,
				value: resultValue(exports),
				href: getPagePath($router, "exports"),
				icon: FileDownIcon,
			},
			{
				key: "billing",
				label: t`Billing`,
				value: resultValue(billing),
				href: getPagePath($router, "billing"),
				icon: ReceiptTextIcon,
			},
			{
				key: "permissions",
				label: t`Permissions`,
				value: resultValue(permissions),
				href: getPagePath($router, "permissions"),
				icon: ShieldCheckIcon,
			},
			{
				key: "snmp",
				label: t`SNMP Profiles`,
				value: resultValue(snmp),
				href: getPagePath($router, "snmp_profiles"),
				icon: GaugeIcon,
			},
		])
	}, [t])

	useEffect(() => {
		document.title = `Watchdog / Watchdog`
		refresh()
	}, [refresh])

	return (
		<div className="grid gap-4">
			<div className="flex items-center gap-2">
				<LayoutDashboardIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
				<h1 className="text-xl font-semibold tracking-normal">Watchdog</h1>
			</div>

			<div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
				{metrics.map((metric) => (
					<Link
						key={metric.key}
						href={metric.href}
						className={cn(buttonVariants({ variant: "outline" }), "h-auto justify-start gap-3 p-4")}
					>
						<metric.icon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
						<span className="grid gap-1 text-start">
							<span className="text-sm text-muted-foreground">{metric.label}</span>
							<span className="text-lg font-semibold">{metric.value}</span>
						</span>
					</Link>
				))}
			</div>

			<div className="rounded-md border border-border p-4">
				<div className="grid gap-2 text-sm">
					<div className="flex items-center gap-2">
						<NetworkIcon className="h-4 w-4 text-muted-foreground" />
						<Trans>Network</Trans>
					</div>
					<div className="grid gap-2 sm:grid-cols-3">
						<Link href={getPagePath($router, "targets")} className="text-muted-foreground hover:underline">
							<Trans>Targets</Trans>
						</Link>
						<Link href={getPagePath($router, "network")} className="text-muted-foreground hover:underline">
							<Trans>Network Targets</Trans>
						</Link>
						<Link href={getPagePath($router, "traffic_defaults")} className="text-muted-foreground hover:underline">
							<Trans>Traffic Defaults</Trans>
						</Link>
					</div>
				</div>
			</div>

			<div className="rounded-md border border-border p-4">
				<div className="grid gap-2 text-sm">
					<div className="flex items-center gap-2">
						<FileDownIcon className="h-4 w-4 text-muted-foreground" />
						<Trans>Exports</Trans>
					</div>
					<div className="grid gap-2 sm:grid-cols-3">
						<Link href={getPagePath($router, "exports")} className="text-muted-foreground hover:underline">
							<Trans>Export Tasks</Trans>
						</Link>
						<Link href={getPagePath($router, "export_new")} className="text-muted-foreground hover:underline">
							<Trans>Create Export</Trans>
						</Link>
					</div>
				</div>
			</div>
		</div>
	)
})

async function countItems(path: string) {
	const data = await api.send<ListResponse>(path, {})
	return data.items?.length ?? 0
}

function resultValue(result: PromiseSettledResult<number>) {
	return result.status === "fulfilled" ? String(result.value) : "—"
}

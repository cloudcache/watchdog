import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { RefreshCwIcon, RouteIcon } from "lucide-react"
import { memo, useCallback, useEffect, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { Button } from "@/components/ui/button"
import { pb } from "@/lib/api"
import { createListTable, disposeTable, getRowRecord, type ListTable } from "@/lib/vtable"

type BGPSessionRecord = {
	ID?: string
	DeviceID?: string
	DeviceSysName?: string
	PeerAddr?: string
	PeerAS?: number
	LocalAS?: number
	AFI?: string
	SAFI?: string
	State?: string
	AcceptedPrefixes?: number
	AdvertisedPrefixes?: number
	// Go time.Duration marshals as nanoseconds
	Uptime?: number
}

type BGPSessionsResponse = {
	items?: BGPSessionRecord[]
}

export default memo(() => {
	const { t } = useLingui()
	const tableRef = useRef<HTMLDivElement>(null)
	const tableInstance = useRef<ListTable | null>(null)
	const [sessions, setSessions] = useState<BGPSessionRecord[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const data = await pb.send<BGPSessionsResponse>("/api/v1/network/bgp", {})
			setSessions(data.items ?? [])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load BGP sessions`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		document.title = `${t`Core (BGP)`} / Watchdog`
		refresh()
	}, [refresh, t])

	useEffect(() => {
		if (!tableRef.current || loading || error) {
			return
		}
		const records = sessions.map((session) => ({
			id: session.ID ?? "",
			deviceId: session.DeviceID ?? "",
			device: session.DeviceSysName || session.DeviceID || "—",
			peer: session.PeerAddr || "—",
			peerAs: session.PeerAS ? `AS${session.PeerAS}` : "—",
			family: [session.AFI, session.SAFI].filter(Boolean).join("/") || "—",
			state: session.State || "—",
			accepted: session.AcceptedPrefixes ?? 0,
			advertised: session.AdvertisedPrefixes ?? 0,
			uptime: formatUptime(session.Uptime),
		}))
		disposeTable(tableInstance.current)
		tableInstance.current = createListTable(tableRef.current, {
			records,
			columns: [
				{ field: "device", title: t`Device`, width: 220 },
				{ field: "peer", title: t`Peer`, width: 200 },
				{ field: "peerAs", title: t`Peer AS`, width: 110 },
				{ field: "family", title: t`Address Family`, width: 140 },
				{ field: "state", title: t`State`, width: 130 },
				{ field: "accepted", title: t`Accepted`, width: 110 },
				{ field: "advertised", title: t`Advertised`, width: 110 },
				{ field: "uptime", title: t`Uptime`, width: 140 },
			],
		})
		tableInstance.current.on("click_cell", (args: { col: number; row: number }) => {
			const record = getRowRecord(tableInstance.current, args)
			if (record?.deviceId) {
				navigate(getPagePath($router, "network_device", { id: record.deviceId }))
			}
		})
		return () => disposeTable(tableInstance.current)
	}, [error, loading, t, sessions])

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<RouteIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Core (BGP)</Trans>
					</h1>
					<span className="text-sm text-muted-foreground">({sessions.length})</span>
				</div>
				<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Refresh</Trans>
				</Button>
			</div>

			<div className="rounded-md border border-border bg-card">
				{loading ? (
					<div className="p-3 text-sm text-muted-foreground">
						<Trans>Loading...</Trans>
					</div>
				) : null}
				{error ? <div className="p-3 text-sm text-destructive">{error}</div> : null}
				{!loading && !error && sessions.length === 0 ? (
					<div className="p-3 text-sm text-muted-foreground">
						<Trans>No BGP sessions found. Sessions appear after SNMP discovery on network devices.</Trans>
					</div>
				) : null}
				<div ref={tableRef} className="h-[520px] w-full" />
			</div>
		</div>
	)
})

function formatUptime(nanoseconds?: number) {
	if (!nanoseconds || nanoseconds <= 0) {
		return "—"
	}
	let seconds = Math.floor(nanoseconds / 1e9)
	const days = Math.floor(seconds / 86400)
	seconds -= days * 86400
	const hours = Math.floor(seconds / 3600)
	seconds -= hours * 3600
	const minutes = Math.floor(seconds / 60)
	if (days > 0) {
		return `${days}d ${hours}h`
	}
	if (hours > 0) {
		return `${hours}h ${minutes}m`
	}
	return `${minutes}m`
}

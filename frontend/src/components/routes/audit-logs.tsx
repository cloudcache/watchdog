import { Trans, useLingui } from "@lingui/react/macro"
import { RefreshCwIcon, ScrollTextIcon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { api } from "@/lib/api"

type AuditLogItem = {
	id: string
	actor_id?: string
	action: string
	resource_type: string
	resource_id?: string
	detail?: Record<string, unknown>
	created_at: string
}

type AuditLogsResponse = {
	items?: AuditLogItem[]
	next_cursor?: string
}

const resourceTypes = [
	"",
	"user",
	"role",
	"device",
	"port",
	"location",
	"device_group",
	"agent",
	"flow_exporter_binding",
	"prefix",
	"address_set",
	"geography",
	"operator",
	"geo_line",
]

export default memo(() => {
	const { t } = useLingui()
	const [items, setItems] = useState<AuditLogItem[]>([])
	const [nextCursor, setNextCursor] = useState("")
	const [resourceType, setResourceType] = useState("")
	const [action, setAction] = useState("")
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")

	const fetchPage = useCallback(
		async (cursor: string, append: boolean) => {
			setLoading(true)
			setError("")
			try {
				const params = new URLSearchParams({ limit: "50" })
				if (resourceType) {
					params.set("resource_type", resourceType)
				}
				if (action.trim()) {
					params.set("action", action.trim())
				}
				if (cursor) {
					params.set("cursor", cursor)
				}
				const data = await api.send<AuditLogsResponse>(`/api/v1/audit-logs?${params.toString()}`, {})
				setItems((current) => (append ? [...current, ...(data.items ?? [])] : (data.items ?? [])))
				setNextCursor(data.next_cursor ?? "")
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to load audit logs`)
			} finally {
				setLoading(false)
			}
		},
		[action, resourceType, t]
	)

	useEffect(() => {
		document.title = `${t`Audit Logs`} / Watchdog`
	}, [t])

	useEffect(() => {
		fetchPage("", false)
	}, [fetchPage])

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<ScrollTextIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Audit Logs</Trans>
					</h1>
				</div>
				<Button variant="outline" size="sm" onClick={() => fetchPage("", false)} disabled={loading}>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Refresh</Trans>
				</Button>
			</div>

			<div className="flex flex-wrap items-center gap-2">
				<Select value={resourceType || "all"} onValueChange={(value) => setResourceType(value === "all" ? "" : value)}>
					<SelectTrigger className="w-44">
						<SelectValue placeholder={t`Resource type`} />
					</SelectTrigger>
					<SelectContent>
						{resourceTypes.map((value) => (
							<SelectItem key={value || "all"} value={value || "all"}>
								{value || t`All resources`}
							</SelectItem>
						))}
					</SelectContent>
				</Select>
				<Input
					className="w-56"
					placeholder={t`Action prefix, e.g. user.`}
					value={action}
					onChange={(event) => setAction(event.target.value)}
				/>
			</div>

			{error ? <div className="text-sm text-destructive">{error}</div> : null}

			<div className="overflow-hidden rounded-md border border-border bg-card">
				<Table>
					<TableHeader>
						<TableRow>
							<TableHead className="w-44">
								<Trans>Time</Trans>
							</TableHead>
							<TableHead>
								<Trans>Action</Trans>
							</TableHead>
							<TableHead>
								<Trans>Resource</Trans>
							</TableHead>
							<TableHead>
								<Trans>Actor</Trans>
							</TableHead>
							<TableHead>
								<Trans>Detail</Trans>
							</TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{items.length === 0 && !loading ? (
							<TableRow>
								<TableCell colSpan={5} className="text-center text-sm text-muted-foreground">
									<Trans>No audit records match the filters.</Trans>
								</TableCell>
							</TableRow>
						) : (
							items.map((item) => (
								<TableRow key={item.id}>
									<TableCell className="whitespace-nowrap font-mono text-xs text-muted-foreground">
										{formatAuditTime(item.created_at)}
									</TableCell>
									<TableCell>
										<Badge variant="outline" className="font-mono text-xs font-normal">
											{item.action}
										</Badge>
									</TableCell>
									<TableCell className="font-mono text-xs">
										{item.resource_type}
										{item.resource_id ? <span className="text-muted-foreground"> · {item.resource_id}</span> : null}
									</TableCell>
									<TableCell className="font-mono text-xs">{auditActor(item)}</TableCell>
									<TableCell className="max-w-96 truncate text-xs text-muted-foreground">
										{formatAuditDetail(item.detail)}
									</TableCell>
								</TableRow>
							))
						)}
					</TableBody>
				</Table>
			</div>

			{nextCursor ? (
				<div>
					<Button variant="outline" size="sm" onClick={() => fetchPage(nextCursor, true)} disabled={loading}>
						<Trans>Load more</Trans>
					</Button>
				</div>
			) : null}
		</div>
	)
})

function formatAuditTime(value: string) {
	const time = new Date(value)
	return Number.isNaN(time.getTime()) ? value : time.toLocaleString()
}

function auditActor(item: AuditLogItem) {
	if (item.actor_id) {
		return item.actor_id
	}
	const preserved = item.detail?.actor
	return typeof preserved === "string" ? preserved : "—"
}

function formatAuditDetail(detail?: Record<string, unknown>) {
	if (!detail) {
		return ""
	}
	const entries = Object.entries(detail).filter(([key]) => key !== "actor")
	if (entries.length === 0) {
		return ""
	}
	return entries.map(([key, value]) => `${key}=${typeof value === "object" ? JSON.stringify(value) : String(value)}`).join(", ")
}

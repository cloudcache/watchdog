import { Trans, useLingui } from "@lingui/react/macro"
import { SearchIcon } from "lucide-react"
import { memo, useCallback, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"

type FlowRecord = Record<string, any>

function protoName(p: any): string {
	const n = Number(p)
	switch (n) {
		case 1: return "ICMP"
		case 6: return "TCP"
		case 17: return "UDP"
		case 58: return "ICMPv6"
		default: return String(p)
	}
}

function formatBytes(b: any): string {
	const n = Number(b)
	if (n > 1e9) return (n / 1e9).toFixed(2) + " GB"
	if (n > 1e6) return (n / 1e6).toFixed(2) + " MB"
	if (n > 1e3) return (n / 1e3).toFixed(2) + " KB"
	return n + " B"
}

export default memo(function FlowSearch() {
	const { t } = useLingui()
	const [query, setQuery] = useState("")
	const [results, setResults] = useState<FlowRecord[]>([])
	const [loading, setLoading] = useState(false)
	const [error, setError] = useState("")
	const [searched, setSearched] = useState(false)

	const search = useCallback(async () => {
		setLoading(true)
		setError("")
		setSearched(true)
		try {
			const vlogsURL = "http://127.0.0.1:9428"
			const resp = await fetch(
				`${vlogsURL}/select/logsql/query?query=${encodeURIComponent(query || "*")}&limit=200`
			)
			if (!resp.ok) throw new Error(`VictoriaLogs error: ${resp.status}`)
			const text = await resp.text()
			const lines = text.trim().split("\n").filter(Boolean)
			const records: FlowRecord[] = lines.map((line) => {
				try { return JSON.parse(line) } catch { return { _raw: line } }
			})
			setResults(records)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to search`)
			setResults([])
		}
		setLoading(false)
	}, [query, t])

	const columns = ["_time", "device_id", "if_name", "direction", "src_ip", "dst_ip", "src_port", "dst_port", "proto", "estimated_bytes", "address_sets"]

	return (
		<div className="grid gap-4">
			<div className="flex items-center gap-2">
				<SearchIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
				<h1 className="text-xl font-semibold tracking-normal"><Trans>Flow Search</Trans></h1>
			</div>

			<div className="grid gap-3 rounded-md border border-border bg-card p-4">
				<div className="grid gap-2">
					<Label><Trans>LogsQL Query</Trans></Label>
					<div className="flex gap-2">
						<Input
							value={query}
							onChange={(e) => setQuery(e.target.value)}
							onKeyDown={(e) => { if (e.key === "Enter") search() }}
							placeholder='dst_ip:8.8.8.8 AND dst_port:443'
							className="font-mono text-sm"
						/>
						<Button size="sm" onClick={search} disabled={loading}>
							<SearchIcon className="me-2 h-4 w-4" />
							<Trans>Search</Trans>
						</Button>
					</div>
					<p className="text-xs text-muted-foreground">
						<Trans>Examples: dst_ip:8.8.8.8 AND dst_port:443 · src_ip:10.0.0.0/16 · proto:6 · address_sets:电信客户</Trans>
					</p>
				</div>
			</div>

			{error && <div className="text-sm text-destructive">{error}</div>}

			{searched && !loading && results.length === 0 && (
				<div className="text-sm text-muted-foreground"><Trans>No flows found.</Trans></div>
			)}

			{results.length > 0 && (
				<div className="rounded-md border border-border bg-card overflow-auto">
					<Table>
						<TableHeader>
							<TableRow>
								{columns.map((col) => (
									<TableHead key={col} className="whitespace-nowrap">{col}</TableHead>
								))}
							</TableRow>
						</TableHeader>
						<TableBody>
							{results.map((r, i) => (
								<TableRow key={i}>
									{columns.map((col) => {
										const val = r[col]
										let display = val ?? "—"
										if (col === "proto") display = protoName(val)
										if (col === "estimated_bytes") display = formatBytes(val)
										if (col === "address_sets" && Array.isArray(val)) display = val.join(", ")
										return <TableCell key={col} className="whitespace-nowrap font-mono text-xs">{display}</TableCell>
									})}
								</TableRow>
							))}
						</TableBody>
					</Table>
				</div>
			)}
		</div>
	)
})

import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { BarChart3Icon, PlusIcon, RefreshCwIcon, SearchIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button, buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { api } from "@/lib/api"
import { cn } from "@/lib/utils"

type GraphDevice = {
	ID?: string
	Name?: string
}

type AggregateGraph = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	Aggregation?: string
	aggregation?: string
	ValueMode?: string
	value_mode?: string
	Unit?: string
	unit?: string
	Description?: string
	description?: string
	UpdatedAt?: string
	updated_at?: string
	PortCount?: number
	Metrics?: string[]
	Devices?: GraphDevice[]
}

type AggregateGraphsResponse = {
	items?: AggregateGraph[]
}

type GraphGroup = {
	key: string
	title: string
	graphs: AggregateGraph[]
}

export default memo(() => {
	const { t } = useLingui()
	const [graphs, setGraphs] = useState<AggregateGraph[]>([])
	const [search, setSearch] = useState("")
	const [groupBy, setGroupBy] = useState<"device" | "none">("device")
	const [sortBy, setSortBy] = useState<"name" | "updated">("name")
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const data = await api.send<AggregateGraphsResponse>("/api/v1/aggregate-graphs", {})
			setGraphs(data.items ?? [])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load aggregate graphs`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		document.title = `${t`Saved Graphs`} / Watchdog`
		refresh()
	}, [refresh, t])

	const remove = async (id: string) => {
		if (!globalThis.confirm(t`Delete this aggregate graph?`)) {
			return
		}
		try {
			await api.send(`/api/v1/aggregate-graphs/${id}`, { method: "DELETE" })
			await refresh()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to delete aggregate graph`)
		}
	}

	const groups = useMemo(() => {
		const term = search.trim().toLowerCase()
		const filtered = graphs.filter((graph) => {
			if (!term) {
				return true
			}
			const haystack = [
				graphName(graph),
				graph.Description ?? graph.description ?? "",
				...(graph.Metrics ?? []),
				...(graph.Devices ?? []).map((device) => device.Name ?? ""),
			]
				.join(" ")
				.toLowerCase()
			return haystack.includes(term)
		})
		const sorted = [...filtered].sort((left, right) => {
			if (sortBy === "updated") {
				return graphUpdated(right).localeCompare(graphUpdated(left))
			}
			return graphName(left).localeCompare(graphName(right))
		})
		if (groupBy === "none") {
			return [{ key: "all", title: "", graphs: sorted }]
		}
		const byDevice = new Map<string, GraphGroup>()
		for (const graph of sorted) {
			const devices = graph.Devices ?? []
			let key: string
			let title: string
			if (devices.length === 1) {
				key = `device:${devices[0].ID ?? devices[0].Name ?? ""}`
				title = devices[0].Name ?? devices[0].ID ?? ""
			} else if (devices.length > 1) {
				key = "multi"
				title = t`Multiple devices`
			} else {
				key = "none"
				title = t`No ports`
			}
			const group = byDevice.get(key) ?? { key, title, graphs: [] }
			group.graphs.push(graph)
			byDevice.set(key, group)
		}
		return [...byDevice.values()].sort((left, right) => {
			// Device groups first (alphabetical), then multi-device, then unassigned.
			const rank = (group: GraphGroup) => (group.key === "none" ? 2 : group.key === "multi" ? 1 : 0)
			return rank(left) - rank(right) || left.title.localeCompare(right.title)
		})
	}, [graphs, groupBy, search, sortBy, t])

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<BarChart3Icon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Saved Graphs</Trans>
					</h1>
					<span className="text-sm text-muted-foreground">({graphs.length})</span>
				</div>
				<div className="flex items-center gap-2">
					<Link
						href={getPagePath($router, "aggregate_charts")}
						className={cn(buttonVariants({ variant: "outline", size: "sm" }))}
					>
						<PlusIcon className="me-2 h-4 w-4" />
						<Trans>Compose Chart</Trans>
					</Link>
					<Button variant="outline" size="sm" onClick={() => navigate(getPagePath($router, "aggregate_graph_new"))}>
						<PlusIcon className="me-2 h-4 w-4" />
						<Trans>Create</Trans>
					</Button>
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
			</div>

			<div className="flex flex-wrap items-center gap-2">
				<div className="relative">
					<SearchIcon className="absolute start-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
					<Input
						value={search}
						onChange={(event) => setSearch(event.target.value)}
						placeholder={t`Search graphs, devices, metrics`}
						className="w-64 ps-8"
					/>
				</div>
				<Select value={groupBy} onValueChange={(value) => setGroupBy(value as "device" | "none")}>
					<SelectTrigger className="w-40">
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="device">{t`Group by device`}</SelectItem>
						<SelectItem value="none">{t`No grouping`}</SelectItem>
					</SelectContent>
				</Select>
				<Select value={sortBy} onValueChange={(value) => setSortBy(value as "name" | "updated")}>
					<SelectTrigger className="w-40">
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="name">{t`Sort by name`}</SelectItem>
						<SelectItem value="updated">{t`Sort by updated`}</SelectItem>
					</SelectContent>
				</Select>
			</div>

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}

			{loading ? (
				<div className="rounded-md border border-border p-4 text-sm text-muted-foreground">
					<Trans>Loading...</Trans>
				</div>
			) : graphs.length === 0 ? (
				<div className="rounded-md border border-border p-4 text-sm text-muted-foreground">
					<Trans>No aggregate graphs found. Compose one from the Aggregate Charts page and save it.</Trans>
				</div>
			) : (
				groups.map((group) => (
					<div key={group.key} className="grid gap-2">
						{group.title ? (
							<div className="flex items-center gap-2 text-sm font-semibold">
								{group.title}
								<span className="font-normal text-muted-foreground">({group.graphs.length})</span>
							</div>
						) : null}
						<div className="rounded-md border border-border bg-card">
							<Table>
								<TableHeader>
									<TableRow>
										<TableHead>
											<Trans>Name</Trans>
										</TableHead>
										<TableHead>
											<Trans>Metrics</Trans>
										</TableHead>
										<TableHead>
											<Trans>Ports</Trans>
										</TableHead>
										<TableHead>
											<Trans>Aggregation</Trans>
										</TableHead>
										<TableHead>
											<Trans>Value</Trans>
										</TableHead>
										<TableHead className="text-right">
											<Trans>Actions</Trans>
										</TableHead>
									</TableRow>
								</TableHeader>
								<TableBody>
									{group.graphs.map((graph) => {
										const id = graph.ID ?? graph.id ?? ""
										return (
											<TableRow key={id}>
												<TableCell className="font-medium">
													<button
														type="button"
														className="hover:underline"
														onClick={() => navigate(getPagePath($router, "aggregate_graph", { id }))}
													>
														{graphName(graph)}
													</button>
													{(graph.Description ?? graph.description) ? (
														<div className="mt-0.5 max-w-[24rem] truncate text-xs text-muted-foreground">
															{graph.Description ?? graph.description}
														</div>
													) : null}
													{groupBy === "none" && (graph.Devices?.length ?? 0) > 0 ? (
														<div className="mt-1 flex flex-wrap gap-1">
															{(graph.Devices ?? []).map((device) => (
																<Badge key={device.ID ?? device.Name} variant="outline" className="text-[11px]">
																	{device.Name ?? device.ID}
																</Badge>
															))}
														</div>
													) : null}
												</TableCell>
												<TableCell className="max-w-[18rem]">
													<div className="flex flex-wrap gap-1">
														{(graph.Metrics ?? []).length > 0 ? (
															(graph.Metrics ?? []).map((metric) => (
																<Badge key={metric} variant="secondary" className="font-mono text-[11px] font-normal">
																	{shortMetric(metric)}
																</Badge>
															))
														) : (
															<span className="text-muted-foreground">—</span>
														)}
													</div>
												</TableCell>
												<TableCell className="font-mono text-xs">{graph.PortCount ?? 0}</TableCell>
												<TableCell>{graph.Aggregation ?? graph.aggregation ?? "—"}</TableCell>
												<TableCell>{graph.ValueMode ?? graph.value_mode ?? "corrected"}</TableCell>
												<TableCell className="text-right">
													<div className="flex justify-end gap-2">
														<Link
															href={getPagePath($router, "aggregate_graph_edit", { id })}
															className={cn(buttonVariants({ variant: "outline", size: "sm" }))}
														>
															<Trans>Edit</Trans>
														</Link>
														<Button variant="outline" size="sm" onClick={() => remove(id)}>
															<Trash2Icon className="me-2 h-4 w-4" />
															<Trans>Delete</Trans>
														</Button>
													</div>
												</TableCell>
											</TableRow>
										)
									})}
								</TableBody>
							</Table>
						</div>
					</div>
				))
			)}
		</div>
	)
})

function graphName(graph: AggregateGraph) {
	return graph.Name ?? graph.name ?? "—"
}

function graphUpdated(graph: AggregateGraph) {
	return graph.UpdatedAt ?? graph.updated_at ?? ""
}

function shortMetric(metric: string) {
	return metric.replace(/^watchdog_snmp_/, "").replace(/^watchdog_/, "")
}

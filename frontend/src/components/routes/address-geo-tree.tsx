import { Trans, useLingui } from "@lingui/react/macro"
import { ChevronDownIcon, ChevronRightIcon, GlobeIcon, NetworkIcon } from "lucide-react"
import { memo, type ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Button } from "@/components/ui/button"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { api } from "@/lib/api"

type GeoTreeNode = {
	id: string
	kind: string
	code: string
	name: string
	parent_id?: string
	child_count: number
}

type Operator = { id: string; name: string; asns?: number[] }

type EffectivePrefix = {
	cidr: string
	family: number
	country_code?: string
	subdivision_name?: string
	city_name?: string
	asn?: number
	operator_name?: string
	source: string
}

// A selection in either tree resolves to a label + the effective-view query it
// filters by (null query = a branch node with nothing to list, e.g. a continent).
type PrefixFilter = { label: string; query: Record<string, string | number> | null }

const INDENT = 16

// Two virtual hierarchy trees over the active base library — Geography
// (continent→country→province→city) and Operators (operator→ASN) — with the
// selected node's effective prefixes listed alongside. Addresses "分组+层级+下钻".
export default memo(function GeoTreeBrowser() {
	const [mode, setMode] = useState<"geo" | "operator">("geo")
	const [filter, setFilter] = useState<PrefixFilter | null>(null)
	const selectMode = useCallback((next: "geo" | "operator") => {
		setMode(next)
		setFilter(null)
	}, [])

	return (
		<div className="grid gap-3 lg:grid-cols-[minmax(260px,360px)_1fr]">
			<div className="rounded-md border border-border bg-card p-2">
				<div className="mb-2 inline-flex w-full rounded-md border border-border p-0.5">
					<Button
						variant={mode === "geo" ? "default" : "ghost"}
						size="sm"
						className="flex-1 gap-1.5"
						onClick={() => selectMode("geo")}
					>
						<GlobeIcon className="h-3.5 w-3.5" />
						<Trans>Geography</Trans>
					</Button>
					<Button
						variant={mode === "operator" ? "default" : "ghost"}
						size="sm"
						className="flex-1 gap-1.5"
						onClick={() => selectMode("operator")}
					>
						<NetworkIcon className="h-3.5 w-3.5" />
						<Trans>Operators</Trans>
					</Button>
				</div>
				{mode === "geo" ? <GeoTree onSelect={setFilter} /> : <OperatorTree onSelect={setFilter} />}
			</div>
			<NodePrefixes filter={filter} />
		</div>
	)
})

const GeoTree = memo(function GeoTree({ onSelect }: { onSelect: (filter: PrefixFilter) => void }) {
	const { t } = useLingui()
	const [roots, setRoots] = useState<GeoTreeNode[]>([])
	const [childrenById, setChildrenById] = useState<Record<string, GeoTreeNode[]>>({})
	const [expanded, setExpanded] = useState<Set<string>>(new Set())
	const [loadingIds, setLoadingIds] = useState<Set<string>>(new Set())
	const [selectedId, setSelectedId] = useState("")
	const [error, setError] = useState("")

	const kindLabel = useCallback(
		(kind: string) => {
			switch (kind) {
				case "continent":
					return t`Continent`
				case "country":
					return t`Country`
				case "province":
					return t`Province`
				case "city":
					return t`City`
				default:
					return kind
			}
		},
		[t]
	)

	const fetchChildren = useCallback(async (parent: string) => {
		const data = await api.send<{ items?: GeoTreeNode[] }>("/api/v1/geo/tree", {
			query: { parent: parent || undefined },
		})
		return data.items ?? []
	}, [])

	useEffect(() => {
		fetchChildren("")
			.then(setRoots)
			.catch((err) => setError(err instanceof Error ? err.message : "failed"))
	}, [fetchChildren])

	const toggle = useCallback(
		async (node: GeoTreeNode) => {
			const next = new Set(expanded)
			if (next.has(node.id)) {
				next.delete(node.id)
				setExpanded(next)
				return
			}
			next.add(node.id)
			setExpanded(next)
			if (!childrenById[node.id]) {
				setLoadingIds((prev) => new Set(prev).add(node.id))
				try {
					setChildrenById((prev) => ({ ...prev, [node.id]: [] })) // mark requested
					const kids = await fetchChildren(node.id)
					setChildrenById((prev) => ({ ...prev, [node.id]: kids }))
				} catch (err) {
					setError(err instanceof Error ? err.message : "failed")
				} finally {
					setLoadingIds((prev) => {
						const copy = new Set(prev)
						copy.delete(node.id)
						return copy
					})
				}
			}
		},
		[expanded, childrenById, fetchChildren]
	)

	const select = useCallback(
		(node: GeoTreeNode) => {
			setSelectedId(node.id)
			let query: Record<string, string> | null = null
			if (node.kind === "country") query = { country_code: node.code }
			else if (node.kind === "province" || node.kind === "city") query = { q: node.name }
			onSelect({ label: node.name, query })
		},
		[onSelect]
	)

	const renderNodes = (nodes: GeoTreeNode[], depth: number): ReactNode =>
		nodes.map((node) => {
			const isOpen = expanded.has(node.id)
			const hasChildren = node.child_count > 0
			return (
				<div key={node.id}>
					<div
						className={`flex items-center gap-1.5 rounded-sm py-1 pr-2 text-sm hover:bg-muted/60 ${
							selectedId === node.id ? "bg-muted" : ""
						}`}
						style={{ paddingLeft: depth * INDENT + 4 }}
					>
						{hasChildren ? (
							<button
								type="button"
								className="flex h-5 w-5 items-center justify-center rounded hover:bg-muted"
								onClick={() => toggle(node)}
								aria-label={isOpen ? t`Collapse` : t`Expand`}
							>
								{isOpen ? <ChevronDownIcon className="h-4 w-4" /> : <ChevronRightIcon className="h-4 w-4" />}
							</button>
						) : (
							<span className="inline-block h-5 w-5" />
						)}
						<button
							type="button"
							className="flex flex-1 items-center gap-2 truncate text-left"
							onClick={() => select(node)}
						>
							<span className="truncate">{node.name}</span>
							<span className="rounded bg-muted px-1.5 py-0.5 text-[10px] uppercase tracking-wide text-muted-foreground">
								{kindLabel(node.kind)}
							</span>
							{hasChildren ? <span className="text-xs text-muted-foreground">{node.child_count}</span> : null}
						</button>
					</div>
					{isOpen && !loadingIds.has(node.id) ? renderNodes(childrenById[node.id] ?? [], depth + 1) : null}
					{isOpen && loadingIds.has(node.id) ? (
						<div className="py-1 text-xs text-muted-foreground" style={{ paddingLeft: (depth + 1) * INDENT + 8 }}>
							<Trans>Loading...</Trans>
						</div>
					) : null}
				</div>
			)
		})

	return (
		<>
			{error ? <div className="px-1 py-2 text-sm text-destructive">{error}</div> : null}
			<div className="max-h-[520px] overflow-auto">{renderNodes(roots, 0)}</div>
		</>
	)
})

const OperatorTree = memo(function OperatorTree({ onSelect }: { onSelect: (filter: PrefixFilter) => void }) {
	const { t } = useLingui()
	const [operators, setOperators] = useState<Operator[]>([])
	const [expanded, setExpanded] = useState<Set<string>>(new Set())
	const [selectedKey, setSelectedKey] = useState("")
	const [error, setError] = useState("")

	useEffect(() => {
		api
			.send<{ items?: Operator[] }>("/api/v1/network/operators", { query: { limit: 500 } })
			.then((data) => setOperators(data.items ?? []))
			.catch((err) => setError(err instanceof Error ? err.message : "failed"))
	}, [])

	const toggle = useCallback((id: string) => {
		setExpanded((prev) => {
			const next = new Set(prev)
			if (next.has(id)) next.delete(id)
			else next.add(id)
			return next
		})
	}, [])

	return (
		<>
			{error ? <div className="px-1 py-2 text-sm text-destructive">{error}</div> : null}
			<div className="max-h-[520px] overflow-auto">
				{operators.map((op) => {
					const isOpen = expanded.has(op.id)
					const asns = op.asns ?? []
					return (
						<div key={op.id}>
							<div
								className={`flex items-center gap-1.5 rounded-sm py-1 pr-2 text-sm hover:bg-muted/60 ${
									selectedKey === `op:${op.id}` ? "bg-muted" : ""
								}`}
								style={{ paddingLeft: 4 }}
							>
								{asns.length > 0 ? (
									<button
										type="button"
										className="flex h-5 w-5 items-center justify-center rounded hover:bg-muted"
										onClick={() => toggle(op.id)}
										aria-label={isOpen ? t`Collapse` : t`Expand`}
									>
										{isOpen ? <ChevronDownIcon className="h-4 w-4" /> : <ChevronRightIcon className="h-4 w-4" />}
									</button>
								) : (
									<span className="inline-block h-5 w-5" />
								)}
								<button
									type="button"
									className="flex flex-1 items-center gap-2 truncate text-left"
									onClick={() => {
										setSelectedKey(`op:${op.id}`)
										onSelect({ label: op.name, query: { operator: op.name } })
									}}
								>
									<span className="truncate">{op.name}</span>
									<span className="rounded bg-muted px-1.5 py-0.5 text-[10px] uppercase tracking-wide text-muted-foreground">
										<Trans>Operator</Trans>
									</span>
									{asns.length > 0 ? <span className="text-xs text-muted-foreground">{asns.length}</span> : null}
								</button>
							</div>
							{isOpen
								? asns.map((asn) => (
										<div
											key={asn}
											className={`flex items-center gap-1.5 rounded-sm py-1 pr-2 text-sm hover:bg-muted/60 ${
												selectedKey === `asn:${asn}` ? "bg-muted" : ""
											}`}
											style={{ paddingLeft: INDENT + 4 }}
										>
											<span className="inline-block h-5 w-5" />
											<button
												type="button"
												className="flex flex-1 items-center gap-2 truncate text-left"
												onClick={() => {
													setSelectedKey(`asn:${asn}`)
													onSelect({ label: `AS${asn}`, query: { asn } })
												}}
											>
												<span className="truncate">AS{asn}</span>
												<span className="rounded bg-muted px-1.5 py-0.5 text-[10px] uppercase tracking-wide text-muted-foreground">
													ASN
												</span>
											</button>
										</div>
									))
								: null}
						</div>
					)
				})}
			</div>
		</>
	)
})

// NodePrefixes lists the effective prefixes for the current tree selection.
const NodePrefixes = memo(function NodePrefixes({ filter }: { filter: PrefixFilter | null }) {
	const { t } = useLingui()
	const [rows, setRows] = useState<EffectivePrefix[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(100)
	const [loading, setLoading] = useState(false)
	const [error, setError] = useState("")
	const seq = useRef(0)

	const query = filter?.query ?? null
	const queryKey = query ? JSON.stringify(query) : ""

	// Reset paging when the selection changes.
	useEffect(() => {
		setPage(0)
	}, [queryKey])

	useEffect(() => {
		if (!query) {
			setRows([])
			setTotal(0)
			return
		}
		const sequence = ++seq.current
		setLoading(true)
		setError("")
		api
			.send<{ items?: EffectivePrefix[]; total?: number }>("/api/v1/address-prefixes/effective", {
				query: {
					slot: "combined",
					...query,
					limit: pageSize,
					offset: page * pageSize || undefined,
					sort: "cidr",
					order: "asc",
				},
			})
			.then((data) => {
				if (sequence !== seq.current) return
				setRows(data.items ?? [])
				setTotal(data.total ?? 0)
			})
			.catch((err) => {
				if (sequence !== seq.current) return
				setRows([])
				setTotal(0)
				setError(err instanceof Error ? err.message : t`Failed to load`)
			})
			.finally(() => {
				if (sequence === seq.current) setLoading(false)
			})
		// queryKey captures the query object identity for the effect.
	}, [queryKey, page, pageSize, t])

	const records = useMemo(
		() =>
			rows.map((row) => ({
				cidr: row.cidr,
				family: `IPv${row.family}`,
				region: [row.country_code, row.subdivision_name, row.city_name].filter(Boolean).join(" / ") || "—",
				asn: row.asn ?? "—",
				operator: row.operator_name || "—",
				source: row.source === "correction" ? t`Correction` : t`Base`,
			})),
		[rows, t]
	)
	const columns = useMemo(
		() => [
			{ field: "cidr", title: t`CIDR`, width: 180, filter: false },
			{ field: "family", title: t`Family`, width: 80, filter: false },
			{ field: "region", title: t`Region`, width: 220, filter: false },
			{ field: "asn", title: "ASN", width: 100, filter: false },
			{ field: "operator", title: t`Operator`, width: 150, filter: false },
			{ field: "source", title: t`Source`, width: 100, filter: false },
		],
		[t]
	)

	if (!filter) {
		return (
			<div className="flex min-h-[200px] items-center justify-center rounded-md border border-dashed border-border text-sm text-muted-foreground">
				<Trans>Select a node to view its prefixes.</Trans>
			</div>
		)
	}
	if (!query) {
		return (
			<div className="flex min-h-[200px] items-center justify-center rounded-md border border-dashed border-border text-sm text-muted-foreground">
				<Trans>Drill into a country, province, or city to view prefixes.</Trans>
			</div>
		)
	}
	return (
		<div className="grid gap-2">
			<div className="text-sm font-medium">
				{filter.label} · <span className="text-muted-foreground">{total.toLocaleString()}</span>
			</div>
			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			<PagedVTable
				records={records}
				columns={columns}
				loading={loading}
				emptyText={t`No prefixes for this selection.`}
				showSearch={false}
				height={Math.min(540, 44 + records.length * 42)}
				serverPagination={{
					page,
					pageSize,
					totalCount: total,
					onPageChange: setPage,
					onPageSizeChange: (value) => {
						setPage(0)
						setPageSize(value)
					},
				}}
			/>
		</div>
	)
})

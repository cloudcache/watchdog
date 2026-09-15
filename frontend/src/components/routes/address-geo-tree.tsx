import { Trans, useLingui } from "@lingui/react/macro"
import { ChevronDownIcon, ChevronRightIcon, GlobeIcon } from "lucide-react"
import { memo, type ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react"
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

type EffectivePrefix = {
	cidr: string
	family: number
	country_code?: string
	country_name?: string
	subdivision_name?: string
	city_name?: string
	asn?: number
	operator_name?: string
	source: string
}

const INDENT = 16

// GeoTreeBrowser: lazily drill the geo hierarchy (continent → country →
// province → city); selecting a node lists its effective prefixes on the right.
export default memo(function GeoTreeBrowser() {
	const { t } = useLingui()
	const [roots, setRoots] = useState<GeoTreeNode[]>([])
	const [childrenById, setChildrenById] = useState<Record<string, GeoTreeNode[]>>({})
	const [expanded, setExpanded] = useState<Set<string>>(new Set())
	const [loadingIds, setLoadingIds] = useState<Set<string>>(new Set())
	const [selected, setSelected] = useState<GeoTreeNode | null>(null)
	const [treeError, setTreeError] = useState("")

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
			.catch((err) => setTreeError(err instanceof Error ? err.message : "failed"))
	}, [fetchChildren])

	const toggle = useCallback(
		async (node: GeoTreeNode) => {
			const isOpen = expanded.has(node.id)
			const next = new Set(expanded)
			if (isOpen) {
				next.delete(node.id)
				setExpanded(next)
				return
			}
			next.add(node.id)
			setExpanded(next)
			if (!childrenById[node.id]) {
				setLoadingIds((prev) => new Set(prev).add(node.id))
				try {
					const kids = await fetchChildren(node.id)
					setChildrenById((prev) => ({ ...prev, [node.id]: kids }))
				} catch (err) {
					setTreeError(err instanceof Error ? err.message : "failed")
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

	const renderNodes = (nodes: GeoTreeNode[], depth: number): ReactNode =>
		nodes.map((node) => {
			const isOpen = expanded.has(node.id)
			const hasChildren = node.child_count > 0
			return (
				<div key={node.id}>
					<div
						className={`flex items-center gap-1.5 rounded-sm py-1 pr-2 text-sm hover:bg-muted/60 ${
							selected?.id === node.id ? "bg-muted" : ""
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
							onClick={() => setSelected(node)}
						>
							<span className="truncate">{node.name}</span>
							<span className="rounded bg-muted px-1.5 py-0.5 text-[10px] uppercase tracking-wide text-muted-foreground">
								{kindLabel(node.kind)}
							</span>
							{hasChildren ? <span className="text-xs text-muted-foreground">{node.child_count}</span> : null}
						</button>
					</div>
					{isOpen ? (
						loadingIds.has(node.id) ? (
							<div className="py-1 text-xs text-muted-foreground" style={{ paddingLeft: (depth + 1) * INDENT + 8 }}>
								<Trans>Loading...</Trans>
							</div>
						) : (
							renderNodes(childrenById[node.id] ?? [], depth + 1)
						)
					) : null}
				</div>
			)
		})

	return (
		<div className="grid gap-3 lg:grid-cols-[minmax(260px,360px)_1fr]">
			<div className="rounded-md border border-border bg-card p-2">
				<div className="mb-1 flex items-center gap-1.5 px-1 text-xs font-medium uppercase tracking-wide text-muted-foreground">
					<GlobeIcon className="h-3.5 w-3.5" />
					<Trans>Geography</Trans>
				</div>
				{treeError ? <div className="px-1 py-2 text-sm text-destructive">{treeError}</div> : null}
				<div className="max-h-[540px] overflow-auto">{renderNodes(roots, 0)}</div>
			</div>
			<GeoNodePrefixes node={selected} />
		</div>
	)
})

// GeoNodePrefixes lists the effective prefixes for the selected geo node
// (country → country_code filter; province/city → location search).
const GeoNodePrefixes = memo(function GeoNodePrefixes({ node }: { node: GeoTreeNode | null }) {
	const { t } = useLingui()
	const [rows, setRows] = useState<EffectivePrefix[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(100)
	const [loading, setLoading] = useState(false)
	const [error, setError] = useState("")
	const seq = useRef(0)

	// Reset paging when the selected node changes.
	useEffect(() => {
		setPage(0)
	}, [node?.id])

	const query = useMemo(() => {
		if (!node) return null
		if (node.kind === "country") return { country_code: node.code }
		if (node.kind === "province" || node.kind === "city") return { q: node.name }
		return null // continent: drill into a country to view prefixes
	}, [node])

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
	}, [query, page, pageSize, t])

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

	if (!node) {
		return (
			<div className="flex min-h-[200px] items-center justify-center rounded-md border border-dashed border-border text-sm text-muted-foreground">
				<Trans>Select a region to view its prefixes.</Trans>
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
				{node.name} · <span className="text-muted-foreground">{total.toLocaleString()}</span>
			</div>
			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			<PagedVTable
				records={records}
				columns={columns}
				loading={loading}
				emptyText={t`No prefixes for this region.`}
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

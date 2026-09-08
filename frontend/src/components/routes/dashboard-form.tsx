import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import {
	ArrowDownIcon,
	ArrowLeftIcon,
	ArrowUpIcon,
	EyeIcon,
	LayoutDashboardIcon,
	PlusIcon,
	SaveIcon,
	SearchIcon,
	Trash2Icon,
} from "lucide-react"
import type React from "react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button, buttonVariants } from "@/components/ui/button"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { isReadOnlyUser, api } from "@/lib/api"
import {
	addDashboardPanel,
	type DashboardGraph,
	type DashboardGraphReference,
	type DashboardPanel,
	graphID,
	graphName,
	moveDashboardPanel,
	normalizeDashboardLayout,
	referenceMap,
	removeDashboardPanel,
} from "@/lib/dashboard-ui"
import { cn } from "@/lib/utils"

type Dashboard = {
	id: string
	name: string
	description?: string
	layout: unknown
	version: number
}

type DashboardPreview = {
	dashboard: Dashboard
	references: DashboardGraphReference[]
	missing_graph_ids: string[]
}

type DashboardFormProps = { id?: string }

export default memo(({ id }: DashboardFormProps) => {
	const { t } = useLingui()
	const editing = Boolean(id)
	const readOnly = isReadOnlyUser()
	const [name, setName] = useState("")
	const [description, setDescription] = useState("")
	const [panels, setPanels] = useState<DashboardPanel[]>([])
	const [preview, setPreview] = useState<DashboardPreview | null>(null)
	const [knownGraphs, setKnownGraphs] = useState<Record<string, DashboardGraph>>({})
	const [pickerOpen, setPickerOpen] = useState(false)
	const [loading, setLoading] = useState(editing)
	const [saving, setSaving] = useState(false)
	const [previewing, setPreviewing] = useState(false)
	const [error, setError] = useState("")
	const etagRef = useRef("")

	useEffect(() => {
		document.title = `${editing ? t`Edit Dashboard` : t`Create Dashboard`} / Watchdog`
		if (!id) return
		let cancelled = false
		setLoading(true)
		api.send<DashboardPreview>(`/api/v1/dashboards/${id}/preview`, {
			onResponse: (response) => {
				etagRef.current = response.headers.get("ETag") ?? ""
			},
		})
			.then((data) => {
				if (cancelled) return
				setName(data.dashboard.name)
				setDescription(data.dashboard.description ?? "")
				setPanels(normalizeDashboardLayout(data.dashboard.layout).panels)
				setPreview(data)
				setKnownGraphs(graphsFromReferences(data.references))
			})
			.catch((err) => {
				if (!cancelled) setError(err instanceof Error ? err.message : t`Failed to load dashboard`)
			})
			.finally(() => {
				if (!cancelled) setLoading(false)
			})
		return () => {
			cancelled = true
		}
	}, [editing, id, t])

	const layout = useMemo(() => ({ panels }), [panels])
	const references = useMemo(() => referenceMap(preview?.references ?? []), [preview])

	const validateDraft = () => {
		if (!name.trim()) {
			setError(t`Name is required`)
			return false
		}
		if (panels.some((panel) => !panel.graph_id.trim())) {
			setError(t`Every panel must reference a saved graph`)
			return false
		}
		return true
	}

	const runPreview = async () => {
		if (!validateDraft()) return
		setPreviewing(true)
		setError("")
		try {
			const data = await api.send<DashboardPreview>("/api/v1/dashboards/actions/preview", {
				method: "POST",
				body: { name: name.trim(), description: description.trim(), layout },
			})
			setPreview(data)
			setKnownGraphs((current) => ({ ...current, ...graphsFromReferences(data.references) }))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to preview dashboard`)
		} finally {
			setPreviewing(false)
		}
	}

	const submit = async (event: React.FormEvent) => {
		event.preventDefault()
		if (!validateDraft() || readOnly) return
		setSaving(true)
		setError("")
		try {
			let saved: Dashboard
			if (id) {
				saved = await api.send<Dashboard>(`/api/v1/dashboards/${id}`, {
					method: "PATCH",
					headers: etagRef.current ? { "If-Match": etagRef.current } : undefined,
					body: { name: name.trim(), description: description.trim(), layout },
					onResponse: (response) => {
						etagRef.current = response.headers.get("ETag") ?? etagRef.current
					},
				})
			} else {
				saved = await api.send<Dashboard>("/api/v1/dashboards", {
					method: "POST",
					body: { name: name.trim(), description: description.trim(), layout },
				})
			}
			navigate(getPagePath($router, "dashboard_edit", { id: saved.id }))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save dashboard`)
		} finally {
			setSaving(false)
		}
	}

	if (loading) return <div className="text-sm text-muted-foreground">{t`Loading...`}</div>

	return (
		<div className="grid gap-4">
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<Link
						href={getPagePath($router, "dashboards")}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to dashboards`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<LayoutDashboardIcon className="h-5 w-5 text-muted-foreground" />
					<h1 className="text-xl font-semibold tracking-normal">{editing ? t`Edit Dashboard` : t`Create Dashboard`}</h1>
				</div>
				<div className="flex items-center gap-2">
					<Button type="button" variant="outline" size="sm" onClick={runPreview} disabled={previewing}>
						<EyeIcon className="me-2 h-4 w-4" />
						<Trans>Validate preview</Trans>
					</Button>
					{!readOnly ? (
						<Button form="dashboard-form" type="submit" size="sm" disabled={saving}>
							<SaveIcon className="me-2 h-4 w-4" />
							<Trans>Save</Trans>
						</Button>
					) : null}
				</div>
			</div>

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			{preview?.missing_graph_ids.length ? (
				<div className="rounded-md border border-destructive/30 bg-destructive/5 p-3 text-sm text-destructive">
					<Trans>Missing graph references:</Trans> {preview.missing_graph_ids.join(", ")}
				</div>
			) : null}

			<form id="dashboard-form" onSubmit={submit} className="grid gap-4">
				<div className="grid gap-3 rounded-md border border-border p-4 md:grid-cols-2">
					<Field label={t`Name`}>
						<Input value={name} onChange={(event) => setName(event.target.value)} disabled={readOnly} maxLength={190} />
					</Field>
					<Field label={t`Description`}>
						<Textarea
							value={description}
							onChange={(event) => setDescription(event.target.value)}
							disabled={readOnly}
							rows={2}
							maxLength={4096}
						/>
					</Field>
				</div>

				<div className="grid gap-3 rounded-md border border-border p-4">
					<div className="flex flex-wrap items-center justify-between gap-3">
						<div>
							<h2 className="font-medium">
								<Trans>Panel layout</Trans>
							</h2>
							<p className="text-xs text-muted-foreground">
								<Trans>Panels render in this order. Preview validates every saved graph reference.</Trans>
							</p>
						</div>
						{!readOnly ? (
							<Button type="button" variant="outline" size="sm" onClick={() => setPickerOpen(true)}>
								<PlusIcon className="me-2 h-4 w-4" />
								<Trans>Add panel</Trans>
							</Button>
						) : null}
					</div>

					{panels.length === 0 ? (
						<div className="rounded-md border border-dashed p-8 text-center text-sm text-muted-foreground">
							<Trans>No panels. Add a saved graph to compose this dashboard.</Trans>
						</div>
					) : (
						<div className="grid gap-3 lg:grid-cols-2">
							{panels.map((panel, index) => {
								const reference = references.get(panel.graph_id)
								const graph = reference?.graph ?? knownGraphs[panel.graph_id]
								const missing = preview ? reference?.exists !== true : false
								return (
									<div
										key={panel.id}
										className={cn(
											"grid gap-3 rounded-md border p-3",
											panel.span === 2 && "lg:col-span-2",
											missing && "border-destructive/40"
										)}
									>
										<div className="flex min-w-0 items-start justify-between gap-2">
											<div className="min-w-0">
												<div className="truncate font-medium">
													{panel.title?.trim() || (graph ? graphName(graph) : panel.graph_id)}
												</div>
												<div className="truncate font-mono text-xs text-muted-foreground">{panel.graph_id}</div>
											</div>
											<div className="flex shrink-0 items-center gap-1">
												{reference?.exists ? (
													<Badge variant="success">{reference.series?.length ?? 0} series</Badge>
												) : null}
												{missing ? (
													<Badge variant="destructive">
														<Trans>Missing</Trans>
													</Badge>
												) : null}
											</div>
										</div>
										{(graph?.Description ?? graph?.description) ? (
											<p className="text-xs text-muted-foreground">{graph.Description ?? graph.description}</p>
										) : null}
										{!readOnly ? (
											<div className="grid gap-2 sm:grid-cols-[1fr_130px_auto]">
												<Input
													value={panel.title ?? ""}
													onChange={(event) => updatePanel(setPanels, panel.id, { title: event.target.value })}
													placeholder={t`Optional panel title`}
												/>
												<Select
													value={String(panel.span ?? 1)}
													onValueChange={(value) => updatePanel(setPanels, panel.id, { span: value === "2" ? 2 : 1 })}
												>
													<SelectTrigger aria-label={t`Panel width`}>
														<SelectValue />
													</SelectTrigger>
													<SelectContent>
														<SelectItem value="1">{t`Half width`}</SelectItem>
														<SelectItem value="2">{t`Full width`}</SelectItem>
													</SelectContent>
												</Select>
												<div className="flex items-center">
													<Button
														type="button"
														variant="ghost"
														size="icon"
														disabled={index === 0}
														onClick={() => setPanels((current) => moveDashboardPanel(current, index, -1))}
														aria-label={t`Move panel up`}
													>
														<ArrowUpIcon className="h-4 w-4" />
													</Button>
													<Button
														type="button"
														variant="ghost"
														size="icon"
														disabled={index === panels.length - 1}
														onClick={() => setPanels((current) => moveDashboardPanel(current, index, 1))}
														aria-label={t`Move panel down`}
													>
														<ArrowDownIcon className="h-4 w-4" />
													</Button>
													<Button
														type="button"
														variant="ghost"
														size="icon"
														onClick={() => {
															setPanels((current) => removeDashboardPanel(current, panel.id))
															setPreview(null)
														}}
														aria-label={t`Remove panel`}
													>
														<Trash2Icon className="h-4 w-4" />
													</Button>
												</div>
											</div>
										) : null}
									</div>
								)
							})}
						</div>
					)}
				</div>
			</form>

			<GraphPicker
				open={pickerOpen}
				onOpenChange={setPickerOpen}
				onSelect={(graph) => {
					const selectedID = graphID(graph)
					if (!selectedID) return
					setKnownGraphs((current) => ({ ...current, [selectedID]: graph }))
					setPanels((current) => addDashboardPanel(current, { id: crypto.randomUUID(), graph_id: selectedID, span: 1 }))
					setPreview(null)
					setPickerOpen(false)
				}}
			/>
		</div>
	)
})

function Field({ label, children }: { label: React.ReactNode; children: React.ReactNode }) {
	return (
		<div className="grid gap-1.5">
			<Label>{label}</Label>
			{children}
		</div>
	)
}

function updatePanel(
	setPanels: React.Dispatch<React.SetStateAction<DashboardPanel[]>>,
	id: string,
	patch: Partial<DashboardPanel>
) {
	setPanels((current) => current.map((panel) => (panel.id === id ? { ...panel, ...patch } : panel)))
}

function graphsFromReferences(references: DashboardGraphReference[]) {
	const graphs: Record<string, DashboardGraph> = {}
	for (const reference of references) {
		if (reference.graph) graphs[reference.graph_id] = reference.graph
	}
	return graphs
}

function GraphPicker({
	open,
	onOpenChange,
	onSelect,
}: {
	open: boolean
	onOpenChange: (open: boolean) => void
	onSelect: (graph: DashboardGraph) => void
}) {
	const { t } = useLingui()
	const [search, setSearch] = useState("")
	const [query, setQuery] = useState("")
	const [items, setItems] = useState<DashboardGraph[]>([])
	const [page, setPage] = useState(0)
	const [total, setTotal] = useState(0)
	const [loading, setLoading] = useState(false)
	const [error, setError] = useState("")
	const pageSize = 20
	const requestSequence = useRef(0)

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setPage(0)
			setQuery(search.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const load = useCallback(async () => {
		if (!open) return
		const sequence = ++requestSequence.current
		setLoading(true)
		setError("")
		try {
			const params = new URLSearchParams({ limit: String(pageSize), offset: String(page * pageSize) })
			if (query) params.set("q", query)
			const data = await api.send<{ items?: DashboardGraph[]; total?: number }>(
				`/api/v1/dashboards/graph-options?${params}`,
				{}
			)
			if (sequence === requestSequence.current) {
				setItems(data.items ?? [])
				setTotal(data.total ?? 0)
			}
		} catch (err) {
			if (sequence === requestSequence.current) {
				setItems([])
				setTotal(0)
				setError(err instanceof Error ? err.message : t`Failed to load saved graphs`)
			}
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [open, page, query, t])

	useEffect(() => {
		load()
	}, [load])

	return (
		<Dialog open={open} onOpenChange={onOpenChange}>
			<DialogContent className="max-h-[80dvh] w-[calc(100vw-2rem)] max-w-2xl overflow-y-auto">
				<DialogHeader>
					<DialogTitle>
						<Trans>Add dashboard panel</Trans>
					</DialogTitle>
					<DialogDescription>
						<Trans>Search saved graphs. Results are filtered and paged by the server.</Trans>
					</DialogDescription>
				</DialogHeader>
				<div className="relative">
					<SearchIcon className="absolute start-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
					<Input
						value={search}
						onChange={(event) => setSearch(event.target.value)}
						placeholder={t`Search saved graphs...`}
						className="ps-9"
					/>
				</div>
				{error ? <div className="text-sm text-destructive">{error}</div> : null}
				<div className="grid max-h-[45dvh] gap-2 overflow-y-auto">
					{loading ? (
						<div className="p-3 text-sm text-muted-foreground">
							<Trans>Loading...</Trans>
						</div>
					) : null}
					{!loading && items.length === 0 ? (
						<div className="p-3 text-sm text-muted-foreground">
							<Trans>No saved graphs found.</Trans>
						</div>
					) : null}
					{items.map((graph) => (
						<button
							key={graphID(graph)}
							type="button"
							className="grid gap-1 rounded-md border p-3 text-start hover:bg-muted/60"
							onClick={() => onSelect(graph)}
						>
							<span className="font-medium">{graphName(graph)}</span>
							<span className="text-xs text-muted-foreground">
								{graph.Description ??
									graph.description ??
									`${graph.Aggregation ?? graph.aggregation ?? "sum"} · ${graph.ValueMode ?? graph.value_mode ?? "corrected"}`}
							</span>
						</button>
					))}
				</div>
				<div className="flex items-center justify-between gap-3 text-sm text-muted-foreground">
					<span>
						{total} <Trans>saved graphs</Trans>
					</span>
					<div className="flex items-center gap-2">
						<Button
							type="button"
							variant="outline"
							size="sm"
							disabled={page === 0 || loading}
							onClick={() => setPage((value) => Math.max(0, value - 1))}
						>
							<Trans>Previous</Trans>
						</Button>
						<span>
							{page + 1} / {Math.max(1, Math.ceil(total / pageSize))}
						</span>
						<Button
							type="button"
							variant="outline"
							size="sm"
							disabled={(page + 1) * pageSize >= total || loading}
							onClick={() => setPage((value) => value + 1)}
						>
							<Trans>Next</Trans>
						</Button>
					</div>
				</div>
			</DialogContent>
		</Dialog>
	)
}

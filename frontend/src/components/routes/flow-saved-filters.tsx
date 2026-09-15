import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { BookmarkIcon, PlayIcon, PlusIcon, RefreshCwIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { api } from "@/lib/api"
import {
	formatFlowFilter,
	parseFlowFilter,
	type FlowFilterExpression,
} from "@/lib/flow-explorer-model"
import type { ColumnDefine, ServerFilterOption } from "@/lib/vtable"

type FlowSavedFilter = {
	id: string
	owner_user_id?: string
	owner_name?: string
	name: string
	description: string
	share_scope: "private" | "shared"
	filter: FlowFilterExpression
	row_version: number
	can_edit: boolean
	updated_at: string
}

type FlowSavedFilterList = {
	items?: FlowSavedFilter[]
	total?: number
	meta?: { can_share?: boolean }
}

type OwnerFacetResponse = {
	items?: { owner_user_id: string; owner_name?: string; count: number }[]
}

type FilterForm = {
	id: string
	rowVersion: number
	canEdit: boolean
	name: string
	description: string
	shareScope: "private" | "shared"
	expression: string
}

function newFilterForm(): FilterForm {
	return {
		id: "",
		rowVersion: 0,
		canEdit: true,
		name: "",
		description: "",
		shareScope: "private",
		expression: new URLSearchParams(window.location.search).get("filter") ?? "",
	}
}

export default memo(function FlowSavedFilters() {
	const { t } = useLingui()
	const [items, setItems] = useState<FlowSavedFilter[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [scope, setScope] = useState("")
	const [ownerID, setOwnerID] = useState("")
	const [sort, setSort] = useState("updated_at:desc")
	const [reloadKey, setReloadKey] = useState(0)
	const [loading, setLoading] = useState(true)
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")
	const [canShare, setCanShare] = useState(false)
	const [showForm, setShowForm] = useState(() => Boolean(new URLSearchParams(window.location.search).get("filter")))
	const [form, setForm] = useState<FilterForm>(newFilterForm)
	const requestSequence = useRef(0)

	useEffect(() => {
		document.title = `${t`Saved Flow Filters`} / Watchdog`
	}, [t])

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setPage(0)
			setDebouncedSearch(search.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const fetchPage = useCallback(async () => {
		const sequence = ++requestSequence.current
		const [sortField, order] = sort.split(":")
		setLoading(true)
		setError("")
		try {
			const data = await api.send<FlowSavedFilterList>("/api/v1/flow/filters", {
				query: {
					q: debouncedSearch || undefined,
					scope: scope || undefined,
					owner_id: ownerID || undefined,
					sort: sortField,
					order,
					limit: pageSize,
					offset: page * pageSize || undefined,
				},
			})
			if (sequence !== requestSequence.current) return
			setItems(data.items ?? [])
			setTotal(data.total ?? 0)
			setCanShare(data.meta?.can_share === true)
		} catch (reason) {
			if (sequence !== requestSequence.current) return
			setItems([])
			setTotal(0)
			setError(reason instanceof Error ? reason.message : t`Failed to load saved filters`)
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [debouncedSearch, ownerID, page, pageSize, reloadKey, scope, sort, t])

	useEffect(() => {
		fetchPage()
	}, [fetchPage])

	const selectItem = useCallback((item: FlowSavedFilter) => {
		setForm({
			id: item.id,
			rowVersion: item.row_version,
			canEdit: item.can_edit,
			name: item.name,
			description: item.description,
			shareScope: item.share_scope,
			expression: formatFlowFilter(item.filter),
		})
		setShowForm(true)
		setError("")
	}, [])

	const applyExpression = useCallback((expression: string) => {
		parseFlowFilter(expression)
		const path = getPagePath($router, "flow_dimensions")
		navigate(`${path}?filter=${encodeURIComponent(expression.trim())}`)
	}, [])

	const save = useCallback(async () => {
		if (!form.canEdit || saving) return
		setSaving(true)
		setError("")
		try {
			const filter = parseFlowFilter(form.expression)
			if (!filter) throw new Error(t`Filter expression is required`)
			const saved = await api.send<FlowSavedFilter>(form.id ? `/api/v1/flow/filters/${form.id}` : "/api/v1/flow/filters", {
				method: form.id ? "PATCH" : "POST",
				headers: form.id ? { "If-Match": `"${form.rowVersion}"` } : undefined,
				body: {
					name: form.name,
					description: form.description,
					share_scope: form.shareScope,
					filter,
				},
			})
			selectItem(saved)
			await fetchPage()
		} catch (reason) {
			setError(reason instanceof Error ? reason.message : t`Failed to save filter`)
		} finally {
			setSaving(false)
		}
	}, [fetchPage, form, saving, selectItem, t])

	const remove = useCallback(
		async (item: FlowSavedFilter) => {
			if (!item.can_edit || !confirm(t`Delete this saved filter?`)) return
			setError("")
			try {
				await api.send(`/api/v1/flow/filters/${item.id}`, {
					method: "DELETE",
					headers: { "If-Match": `"${item.row_version}"` },
				})
				if (form.id === item.id) {
					setForm(newFilterForm())
					setShowForm(false)
				}
				await fetchPage()
			} catch (reason) {
				setError(reason instanceof Error ? reason.message : t`Failed to delete filter`)
			}
		},
		[fetchPage, form.id, t]
	)

	const records = useMemo(
		() =>
			items.map((item) => ({
				name: item.name,
				scope: item.share_scope,
				owner: item.owner_name || item.owner_user_id || t`Deleted user`,
				owner_id: item.owner_user_id || "",
				description: item.description || "—",
				filter: formatFlowFilter(item.filter),
				updated: formatDate(item.updated_at),
				apply: t`Apply`,
				edit: item.can_edit ? t`Edit` : t`View`,
				remove: item.can_edit ? t`Delete` : "",
				item,
			})),
		[items, t]
	)
	const columns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "name", title: t`Name`, width: 190, filter: false },
			{ field: "scope", filterField: "scope", title: t`Scope`, width: 100 },
			{ field: "owner", filterField: "owner_id", title: t`Owner`, width: 170 },
			{ field: "description", title: t`Description`, width: 220, filter: false },
			{ field: "filter", title: t`Filter`, width: 360, filter: false },
			{ field: "updated", title: t`Updated`, width: 165, filter: false },
			{ field: "apply", title: t`Apply`, width: 75, filter: false, style: actionStyle("#2563eb") },
			{ field: "edit", title: t`Edit`, width: 75, filter: false, style: actionStyle("#2563eb") },
			{ field: "remove", title: t`Delete`, width: 75, filter: false, style: actionStyle("#dc2626") },
		],
		[t]
	)
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(
		() => ({
			field: sortField,
			direction: sortDirection,
			fields: { name: "name", scope: "share_scope", owner: "owner_user_id", updated: "updated_at" },
			onSortChange: (field: string, direction: "asc" | "desc") => {
				setPage(0)
				setSort(`${field}:${direction}`)
			},
		}),
		[sortDirection, sortField]
	)
	const serverFiltering = useMemo(
		() => ({
			options: {
				scope: [
					{ value: "private", label: t`Private` },
					{ value: "shared", label: t`Shared` },
				],
				owner_id: [] as ServerFilterOption[],
			},
			selected: { scope: scope ? [scope] : [], owner_id: ownerID ? [ownerID] : [] },
			selection: { scope: "single" as const, owner_id: "single" as const },
			loadOptions: async (field: string, facetSearch: string, signal: AbortSignal) => {
				if (field === "scope") {
					return [
						{ value: "private", label: t`Private` },
						{ value: "shared", label: t`Shared` },
					]
				}
				const data = await api.send<OwnerFacetResponse>("/api/v1/flow/filters/facets/owners", {
					query: { q: facetSearch || undefined, limit: 100 },
					signal,
				})
				return (data.items ?? []).map((item) => ({
					value: item.owner_user_id,
					label: item.owner_name || item.owner_user_id || t`Deleted user`,
					count: item.count,
				}))
			},
			onColumnFilterChange: (field: string, values: unknown[]) => {
				setPage(0)
				const value = values.length > 0 ? String(values[0]) : ""
				if (field === "scope") setScope(value)
				if (field === "owner_id") setOwnerID(value)
			},
			onClearAll: () => {
				setPage(0)
				setScope("")
				setOwnerID("")
			},
		}),
		[ownerID, scope, t]
	)

	return (
		<div className="grid gap-4">
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<BookmarkIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Saved Flow Filters</Trans>
					</h1>
				</div>
				<div className="flex gap-2">
					<Button variant="outline" size="sm" onClick={() => setReloadKey((value) => value + 1)} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
					<Button
						size="sm"
						onClick={() => {
							setForm(newFilterForm())
							setShowForm(true)
							setError("")
						}}
					>
						<PlusIcon className="me-2 h-4 w-4" />
						<Trans>New Filter</Trans>
					</Button>
				</div>
			</div>

			{showForm ? (
				<div className="grid gap-3 rounded-md border border-border bg-card p-4 md:grid-cols-2">
					<div className="grid gap-2">
						<Label><Trans>Name</Trans></Label>
						<Input disabled={!form.canEdit} value={form.name} onChange={(event) => setForm({ ...form, name: event.target.value })} />
					</div>
					<div className="grid gap-2">
						<Label><Trans>Scope</Trans></Label>
						<Select
							disabled={!form.canEdit}
							value={form.shareScope}
							onValueChange={(value: "private" | "shared") => setForm({ ...form, shareScope: value })}
						>
							<SelectTrigger><SelectValue /></SelectTrigger>
							<SelectContent>
								<SelectItem value="private"><Trans>Private</Trans></SelectItem>
								{canShare || form.shareScope === "shared" ? <SelectItem value="shared"><Trans>Shared</Trans></SelectItem> : null}
							</SelectContent>
						</Select>
					</div>
					<div className="grid gap-2 md:col-span-2">
						<Label><Trans>Description</Trans></Label>
						<Input disabled={!form.canEdit} value={form.description} onChange={(event) => setForm({ ...form, description: event.target.value })} />
					</div>
					<div className="grid gap-2 md:col-span-2">
						<Label><Trans>Filter expression</Trans></Label>
						<Textarea
							disabled={!form.canEdit}
							value={form.expression}
							onChange={(event) => setForm({ ...form, expression: event.target.value })}
							placeholder="geo.country=CN AND asn IN (4134, 4837)"
						/>
					</div>
					<div className="flex flex-wrap gap-2 md:col-span-2">
						{form.canEdit ? <Button size="sm" onClick={save} disabled={saving}><Trans>Save</Trans></Button> : null}
						<Button variant="outline" size="sm" onClick={() => applyExpression(form.expression)}>
							<PlayIcon className="me-2 h-4 w-4" /><Trans>Apply in Explorer</Trans>
						</Button>
						<Button variant="ghost" size="sm" onClick={() => setShowForm(false)}><Trans>Close</Trans></Button>
					</div>
				</div>
			) : null}

			{error ? <div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div> : null}

			<div className="overflow-hidden rounded-md border border-border bg-card">
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={t`No saved Flow filters found.`}
					searchPlaceholder={t`Search saved filters...`}
					searchValue={search}
					onSearchChange={setSearch}
					height={Math.min(620, Math.max(180, records.length * 42 + 38))}
					serverPagination={{
						page,
						pageSize,
						totalCount: total,
						onPageChange: setPage,
						onPageSizeChange: (value) => { setPage(0); setPageSize(value) },
					}}
					serverFiltering={serverFiltering}
					serverSorting={serverSorting}
					onCellClick={(record, field) => {
						const item = record.item as FlowSavedFilter | undefined
						if (!item) return
						if (field === "apply") applyExpression(formatFlowFilter(item.filter))
						if (field === "edit") selectItem(item)
						if (field === "remove") remove(item)
					}}
				/>
			</div>
		</div>
	)
})

function formatDate(value: string) {
	const date = new Date(value)
	return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
}

function actionStyle(color: string) {
	return { color, cursor: "pointer", fontSize: 12 }
}

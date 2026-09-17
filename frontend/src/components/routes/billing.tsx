import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { PlusIcon, ReceiptTextIcon, RefreshCwIcon, SaveIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Textarea } from "@/components/ui/textarea"
import { api } from "@/lib/api"
import type { ColumnDefine } from "@/lib/vtable"

type BillingAccount = {
	id: string
	party_id?: string
	name: string
	status: string
	measurement_type: string
	billing_method: string
	algorithm: string
	billing_day: number
	timezone: string
	direction: string
	default_layer: string
	minimum_percent: number
	traffic_allowance_bytes?: number
	row_version: number
}

type Party = {
	id: string
	kind: string
	status: string
	name: string
	ref: string
	notes: string
	row_version: number
}

type Page<T> = { items: T[]; total: number; limit: number; offset: number }
type SortDirection = "asc" | "desc"

export default memo(() => {
	const { t } = useLingui()
	useEffect(() => {
		document.title = `${t`Billing`} / Watchdog`
	}, [t])
	return (
		<div className="grid gap-4">
			<div className="flex items-center gap-2">
				<ReceiptTextIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
				<h1 className="text-xl font-semibold tracking-normal">
					<Trans>Billing</Trans>
				</h1>
			</div>
			<Tabs defaultValue="accounts">
				<TabsList>
					<TabsTrigger value="accounts">
						<Trans>Accounts</Trans>
					</TabsTrigger>
					<TabsTrigger value="parties">
						<Trans>Parties</Trans>
					</TabsTrigger>
				</TabsList>
				<TabsContent value="accounts" className="mt-4">
					<AccountsTable />
				</TabsContent>
				<TabsContent value="parties" className="mt-4">
					<PartiesTable />
				</TabsContent>
			</Tabs>
		</div>
	)
})

function AccountsTable() {
	const { t } = useLingui()
	const [items, setItems] = useState<BillingAccount[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [query, setQuery] = useState("")
	const [status, setStatus] = useState("")
	const [measurementType, setMeasurementType] = useState("")
	const [sort, setSort] = useState("name:asc")
	const [reload, setReload] = useState(0)
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const sequence = useRef(0)
	useEffect(() => {
		const timer = setTimeout(() => {
			setPage(0)
			setQuery(search.trim())
		}, 300)
		return () => clearTimeout(timer)
	}, [search])
	useEffect(() => {
		const current = ++sequence.current
		const [field, order] = sort.split(":")
		setLoading(true)
		setError("")
		api
			.send<Page<BillingAccount>>("/api/v1/billing/accounts", {
				query: {
					limit: pageSize,
					offset: page * pageSize,
					q: query || undefined,
					status: status || undefined,
					type: measurementType || undefined,
					sort: field,
					order,
				},
			})
			.then((data) => {
				if (current !== sequence.current) return
				setItems(data.items ?? [])
				setTotal(data.total ?? 0)
			})
			.catch((err) => {
				if (current !== sequence.current) return
				setItems([])
				setTotal(0)
				setError(err instanceof Error ? err.message : t`Failed to load billing accounts`)
			})
			.finally(() => {
				if (current === sequence.current) setLoading(false)
			})
	}, [measurementType, page, pageSize, query, reload, sort, status, t])
	const columns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "name", title: t`Name`, width: 240 },
			{ field: "status", title: t`Status`, width: 110, filterField: "status" },
			{ field: "measurement_display", title: t`Measurement type`, width: 150, filterField: "type" },
			{ field: "method_display", title: t`Billing method`, width: 170 },
			{ field: "billing_day", title: t`Billing Day`, width: 110 },
			{ field: "timezone", title: t`Timezone`, width: 180 },
			{ field: "direction", title: t`Direction`, width: 100 },
			{ field: "default_layer", title: t`Layer`, width: 110 },
			{ field: "allowance", title: t`Minimum / allowance`, width: 160 },
		],
		[t]
	)
	const records = useMemo(
		() =>
			items.map((item) => ({
				...item,
				measurement_display: item.measurement_type === "bandwidth" ? t`Bandwidth` : t`Traffic`,
				method_display:
					item.billing_method === "package_port"
						? t`Port package`
						: item.billing_method === "monthly_95th"
							? t`Monthly 95th`
							: item.billing_method === "daily_95th"
								? t`Daily 95th`
								: t`Monthly average`,
				allowance:
					item.measurement_type === "bandwidth"
						? `${item.minimum_percent ?? 0}%`
						: formatBytes(item.traffic_allowance_bytes),
			})),
		[items, t]
	)
	const filters = useMemo(
		() => ({
			options: {
				status: [
					{ value: "active", label: t`Active` },
					{ value: "paused", label: t`Paused` },
				],
				type: [
					{ value: "bandwidth", label: t`Bandwidth` },
					{ value: "traffic", label: t`Traffic` },
				],
			},
			selected: { status: status ? [status] : [], type: measurementType ? [measurementType] : [] },
			selection: { status: "single" as const, type: "single" as const },
			onColumnFilterChange: (field: string, values: unknown[]) => {
				setPage(0)
				field === "status" ? setStatus(String(values[0] ?? "")) : setMeasurementType(String(values[0] ?? ""))
			},
			onClearAll: () => {
				setPage(0)
				setStatus("")
				setMeasurementType("")
			},
		}),
		[measurementType, status, t]
	)
	const [sortField, sortDirection] = sort.split(":") as [string, SortDirection]
	return (
		<div className="grid gap-3">
			<div className="flex justify-end gap-2">
				<Button variant="outline" size="sm" onClick={() => navigate(getPagePath($router, "billing_new"))}>
					<PlusIcon className="me-2 h-4 w-4" />
					<Trans>Create</Trans>
				</Button>
				<Button variant="outline" size="sm" onClick={() => setReload((value) => value + 1)} disabled={loading}>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Refresh</Trans>
				</Button>
			</div>
			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			<div className="overflow-hidden rounded-md border border-border bg-card p-3">
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={t`No billing accounts found.`}
					searchPlaceholder={t`Search billing accounts...`}
					searchValue={search}
					onSearchChange={setSearch}
					serverFiltering={filters}
					serverSorting={{
						field: sortField,
						direction: sortDirection,
						fields: { name: "name", status: "status", measurement_display: "measurement_type", method_display: "billing_method" },
						onSortChange: (field, direction) => {
							setPage(0)
							setSort(`${field}:${direction}`)
						},
					}}
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
					onRowClick={(record) => navigate(getPagePath($router, "billing_detail", { id: String(record.id) }))}
				/>
			</div>
		</div>
	)
}

function PartiesTable() {
	const { t } = useLingui()
	const [items, setItems] = useState<Party[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [query, setQuery] = useState("")
	const [kind, setKind] = useState("")
	const [status, setStatus] = useState("")
	const [sort, setSort] = useState("name:asc")
	const [reload, setReload] = useState(0)
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const [editing, setEditing] = useState<Party | null>(null)
	const [etag, setETag] = useState("")
	useEffect(() => {
		const timer = setTimeout(() => {
			setPage(0)
			setQuery(search.trim())
		}, 300)
		return () => clearTimeout(timer)
	}, [search])
	useEffect(() => {
		const [field, order] = sort.split(":")
		setLoading(true)
		setError("")
		api
			.send<Page<Party>>("/api/v1/billing/parties", {
				query: {
					limit: pageSize,
					offset: page * pageSize,
					q: query || undefined,
					kind: kind || undefined,
					status: status || undefined,
					sort: field,
					order,
				},
			})
			.then((data) => {
				setItems(data.items ?? [])
				setTotal(data.total ?? 0)
			})
			.catch((err) => {
				setError(err instanceof Error ? err.message : t`Failed to load parties`)
				setItems([])
				setTotal(0)
			})
			.finally(() => setLoading(false))
	}, [kind, page, pageSize, query, reload, sort, status, t])
	const selectParty = useCallback(
		async (id: string) => {
			setError("")
			try {
				const item = await api.send<Party>(`/api/v1/billing/parties/${id}`, {
					onResponse: (response) => setETag(response.headers.get("ETag") ?? ""),
				})
				setEditing(item)
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to load party`)
			}
		},
		[t]
	)
	const save = async () => {
		if (!editing?.name.trim()) return
		try {
			const creating = !editing.id
			const item = await api.send<Party>(
				creating ? "/api/v1/billing/parties" : `/api/v1/billing/parties/${editing.id}`,
				{
					method: creating ? "POST" : "PATCH",
					headers: !creating && etag ? { "If-Match": etag } : undefined,
					body: {
						kind: editing.kind,
						status: editing.status,
						name: editing.name.trim(),
						ref: editing.ref,
						notes: editing.notes,
					},
					onResponse: (response) => setETag(response.headers.get("ETag") ?? ""),
				}
			)
			setEditing(item)
			setReload((value) => value + 1)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save party`)
		}
	}
	const remove = async () => {
		if (!editing?.id || !etag || !window.confirm(t`Delete this party?`)) return
		try {
			await api.send(`/api/v1/billing/parties/${editing.id}`, { method: "DELETE", headers: { "If-Match": etag } })
			setEditing(null)
			setETag("")
			setReload((value) => value + 1)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to delete party`)
		}
	}
	const columns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "name", title: t`Name`, width: 280 },
			{ field: "kind", title: t`Type`, width: 130, filterField: "kind" },
			{ field: "status", title: t`Status`, width: 130, filterField: "status" },
			{ field: "ref", title: t`Reference`, width: 180 },
			{ field: "notes", title: t`Notes`, width: 360 },
		],
		[t]
	)
	const filters = useMemo(
		() => ({
			options: {
				kind: [
					{ value: "customer", label: t`Customer` },
					{ value: "supplier", label: t`Supplier` },
				],
				status: [
					{ value: "active", label: t`Active` },
					{ value: "inactive", label: t`Inactive` },
				],
			},
			selected: { kind: kind ? [kind] : [], status: status ? [status] : [] },
			selection: { kind: "single" as const, status: "single" as const },
			onColumnFilterChange: (field: string, values: unknown[]) => {
				setPage(0)
				field === "kind" ? setKind(String(values[0] ?? "")) : setStatus(String(values[0] ?? ""))
			},
			onClearAll: () => {
				setPage(0)
				setKind("")
				setStatus("")
			},
		}),
		[kind, status, t]
	)
	const [sortField, sortDirection] = sort.split(":") as [string, SortDirection]
	return (
		<div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_360px]">
			<div className="grid gap-3">
				<div className="flex justify-end gap-2">
					<Button
						variant="outline"
						size="sm"
						onClick={() => {
							setEditing({ id: "", kind: "customer", status: "active", name: "", ref: "", notes: "", row_version: 0 })
							setETag("")
						}}
					>
						<PlusIcon className="me-2 h-4 w-4" />
						<Trans>Create</Trans>
					</Button>
					<Button variant="outline" size="sm" onClick={() => setReload((value) => value + 1)}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
				{error ? (
					<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
				) : null}
				<div className="overflow-hidden rounded-md border border-border bg-card p-3">
					<PagedVTable
						records={items}
						columns={columns}
						loading={loading}
						emptyText={t`No parties found.`}
						searchPlaceholder={t`Search parties...`}
						searchValue={search}
						onSearchChange={setSearch}
						serverFiltering={filters}
						serverSorting={{
							field: sortField,
							direction: sortDirection,
							fields: { name: "name", kind: "kind", status: "status" },
							onSortChange: (field, direction) => {
								setPage(0)
								setSort(`${field}:${direction}`)
							},
						}}
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
						onRowClick={(record) => selectParty(String(record.id))}
					/>
				</div>
			</div>
			{editing ? (
				<div className="grid content-start gap-3 rounded-md border border-border p-4">
					<h2 className="font-medium">{editing.id ? t`Edit party` : t`Create party`}</h2>
					<Field label={t`Name`}>
						<Input value={editing.name} onChange={(event) => setEditing({ ...editing, name: event.target.value })} />
					</Field>
					<Field label={t`Type`}>
						<Select value={editing.kind} onValueChange={(value) => setEditing({ ...editing, kind: value })}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="customer">customer</SelectItem>
								<SelectItem value="supplier">supplier</SelectItem>
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`Status`}>
						<Select value={editing.status} onValueChange={(value) => setEditing({ ...editing, status: value })}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="active">active</SelectItem>
								<SelectItem value="inactive">inactive</SelectItem>
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`Reference`}>
						<Input value={editing.ref} onChange={(event) => setEditing({ ...editing, ref: event.target.value })} />
					</Field>
					<Field label={t`Notes`}>
						<Textarea
							value={editing.notes}
							onChange={(event) => setEditing({ ...editing, notes: event.target.value })}
						/>
					</Field>
					<div className="flex justify-between">
						<Button variant="destructive" size="sm" onClick={remove} disabled={!editing.id}>
							<Trash2Icon className="me-2 h-4 w-4" />
							<Trans>Delete</Trans>
						</Button>
						<Button size="sm" onClick={save} disabled={!editing.name.trim()}>
							<SaveIcon className="me-2 h-4 w-4" />
							<Trans>Save</Trans>
						</Button>
					</div>
				</div>
			) : null}
		</div>
	)
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
	return (
		<div className="grid gap-1.5">
			<Label>{label}</Label>
			{children}
		</div>
	)
}

function formatBytes(value?: number) {
	if (!value) return "—"
	if (value >= 1_000_000_000_000) return `${(value / 1_000_000_000_000).toFixed(2)} TB`
	if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(2)} GB`
	if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)} MB`
	return `${value} B`
}

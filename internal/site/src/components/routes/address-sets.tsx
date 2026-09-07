import { Trans, useLingui } from "@lingui/react/macro"
import { LayersIcon, PencilIcon, PlusIcon, RefreshCwIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { AddressReferencePicker } from "@/components/address-reference-picker"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import {
	formatAddressSetSelector,
	formatSetLabelSelector,
	parseAddressEntries,
	parseSetLabelSelector,
	parseUnsignedIntegerEntries,
} from "@/lib/address-set-form"
import { pb } from "@/lib/api"

type AddressSet = {
	id: string
	name: string
	description: string
	selector: {
		labels?: Record<string, string | string[]>
		geo_node_ids?: string[]
		operator_ids?: string[]
		asns?: number[]
		families?: number[]
	}
	explicit_members: string[]
	explicit_exclude_members: string[]
	include_set_ids: string[]
	exclude_set_ids: string[]
	match_direction: "in" | "out" | "both"
	enabled: boolean
	row_version: number
}

type AddressSetList = { items?: AddressSet[]; total?: number }

const emptyForm = {
	id: "",
	rowVersion: 0,
	name: "",
	description: "",
	labels: "",
	geoNodeIDs: [] as string[],
	operatorIDs: [] as string[],
	asns: "",
	families: "",
	members: "",
	excludeMembers: "",
	includeSetIDs: [] as string[],
	excludeSetIDs: [] as string[],
	direction: "both" as "in" | "out" | "both",
	enabled: true,
}

export default memo(function AddressSets() {
	const { t } = useLingui()
	const [sets, setSets] = useState<AddressSet[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [direction, setDirection] = useState("")
	const [enabled, setEnabled] = useState("")
	const [sort, setSort] = useState("name:asc")
	const [reloadKey, setReloadKey] = useState(0)
	const [selected, setSelected] = useState<{ id: string; rowVersion: number }[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const [showForm, setShowForm] = useState(false)
	const [form, setForm] = useState(emptyForm)
	const requestSequence = useRef(0)

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
			const data = await pb.send<AddressSetList>("/api/v1/address-sets", {
				query: {
					q: debouncedSearch || undefined,
					match_direction: direction || undefined,
					enabled: enabled || undefined,
					limit: pageSize,
					offset: page * pageSize || undefined,
					sort: sortField,
					order,
				},
			})
			if (sequence !== requestSequence.current) return
			setSets(data.items ?? [])
			setTotal(data.total ?? 0)
		} catch (err) {
			if (sequence !== requestSequence.current) return
			setSets([])
			setTotal(0)
			setError(err instanceof Error ? err.message : t`Failed to load`)
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [debouncedSearch, direction, enabled, page, pageSize, reloadKey, sort, t])

	useEffect(() => {
		fetchPage()
	}, [fetchPage])

	const save = async () => {
		if (!form.name.trim()) return
		try {
			const labels = parseSetLabelSelector(form.labels)
			const asns = parseUnsignedIntegerEntries(form.asns, 1, 4_294_967_295, "ASN")
			const families = parseUnsignedIntegerEntries(form.families, 4, 6, "IP family")
			if (families.some((family) => family !== 4 && family !== 6)) throw new Error(t`IP families must be 4 or 6`)
			const selector = {
				...(Object.keys(labels).length ? { labels } : {}),
				...(form.geoNodeIDs.length ? { geo_node_ids: form.geoNodeIDs } : {}),
				...(form.operatorIDs.length ? { operator_ids: form.operatorIDs } : {}),
				...(asns.length ? { asns } : {}),
				...(families.length ? { families } : {}),
			}
			await pb.send(form.id ? `/api/v1/address-sets/${form.id}` : "/api/v1/address-sets", {
				method: form.id ? "PATCH" : "POST",
				headers: form.id ? { "If-Match": `"${form.rowVersion}"` } : undefined,
				body: {
					name: form.name.trim(),
					description: form.description.trim(),
					selector,
					explicit_members: parseAddressEntries(form.members),
					explicit_exclude_members: parseAddressEntries(form.excludeMembers),
					include_set_ids: form.includeSetIDs,
					exclude_set_ids: form.excludeSetIDs,
					match_direction: form.direction,
					enabled: form.enabled,
				},
			})
			setForm(emptyForm)
			setShowForm(false)
			await fetchPage()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save`)
		}
	}

	const edit = useCallback((record: Record<string, unknown>) => {
		const item = record.item as AddressSet | undefined
		if (!item) return
		setForm({
			id: item.id,
			rowVersion: item.row_version,
			name: item.name,
			description: item.description ?? "",
			labels: formatSetLabelSelector(item.selector.labels),
			geoNodeIDs: item.selector.geo_node_ids ?? [],
			operatorIDs: item.selector.operator_ids ?? [],
			asns: (item.selector.asns ?? []).join(","),
			families: (item.selector.families ?? []).join(","),
			members: (item.explicit_members ?? []).join("\n"),
			excludeMembers: (item.explicit_exclude_members ?? []).join("\n"),
			includeSetIDs: item.include_set_ids ?? [],
			excludeSetIDs: item.exclude_set_ids ?? [],
			direction: item.match_direction,
			enabled: item.enabled,
		})
		setShowForm(true)
		setError("")
	}, [])

	const remove = useCallback(
		async (record: Record<string, unknown>) => {
			const id = String(record.id ?? "")
			const rowVersion = Number(record.rowVersion ?? 0)
			if (!id || !rowVersion || !confirm(t`Delete this address set?`)) return
			try {
				await pb.send(`/api/v1/address-sets/${id}`, {
					method: "DELETE",
					headers: { "If-Match": `"${rowVersion}"` },
				})
				await fetchPage()
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to delete`)
			}
		},
		[fetchPage, t]
	)

	const removeSelected = useCallback(async () => {
		if (selected.length === 0 || !confirm(t`Delete ${selected.length} selected address sets?`)) return
		try {
			for (const { id, rowVersion } of selected) {
				await pb.send(`/api/v1/address-sets/${id}`, { method: "DELETE", headers: { "If-Match": `"${rowVersion}"` } })
			}
			setSelected([])
			await fetchPage()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to delete`)
			await fetchPage()
		}
	}, [selected, fetchPage, t])
	const selectable = useMemo(
		() => ({
			onSelectionChange: (records: Record<string, unknown>[]) =>
				setSelected(records.map((record) => ({ id: String(record.id), rowVersion: Number(record.rowVersion) }))),
		}),
		[]
	)

	const records = useMemo(
		() =>
			sets.map((set) => ({
				id: set.id,
				name: set.name,
				description: set.description || "—",
				selector: formatAddressSetSelector(set.selector),
				members: set.explicit_members?.join(", ") || "—",
				excludes:
					[...(set.explicit_exclude_members ?? []), ...(set.exclude_set_ids ?? []).map((id) => `set:${id}`)].join(
						", "
					) || "—",
				includes: set.include_set_ids?.join(", ") || "—",
				direction: set.match_direction,
				status: set.enabled ? t`Enabled` : t`Disabled`,
				edit: t`Edit`,
				remove: t`Delete`,
				rowVersion: set.row_version,
				item: set,
			})),
		[sets, t]
	)
	const columns = useMemo(
		() => [
			{ field: "name", title: t`Name`, width: 170, style: denseCellStyle() },
			{ field: "description", title: t`Description`, width: 210, style: denseCellStyle() },
			{ field: "selector", title: t`Selector`, width: 290, style: denseCellStyle() },
			{ field: "members", title: t`Members`, width: 220, style: denseCellStyle() },
			{ field: "includes", title: t`Included sets`, width: 180, style: denseCellStyle() },
			{ field: "excludes", title: t`Exclusions`, width: 220, style: denseCellStyle() },
			{ field: "direction", title: t`Direction`, width: 100, style: denseCellStyle() },
			{ field: "status", title: t`Status`, width: 100, style: denseCellStyle() },
			{
				field: "edit",
				title: t`Edit`,
				width: 90,
				filter: false,
				style: { ...denseCellStyle(), color: "#2563eb", cursor: "pointer" },
			},
			{
				field: "remove",
				title: t`Delete`,
				width: 90,
				filter: false,
				style: { ...denseCellStyle(), color: "#dc2626", cursor: "pointer" },
			},
		],
		[t]
	)
	const resetPage = (update: () => void) => {
		setPage(0)
		update()
	}
	const serverFiltering = useMemo(
		() => ({
			options: {
				direction: [{ value: "in" }, { value: "out" }, { value: "both" }],
				status: [
					{ value: "true", label: t`Enabled` },
					{ value: "false", label: t`Disabled` },
				],
			},
			selected: { direction: direction ? [direction] : [], status: enabled ? [enabled] : [] },
			selection: { direction: "single" as const, status: "single" as const },
			onColumnFilterChange: (field: string, values: unknown[]) =>
				resetPage(() => {
					const value = values.length > 0 ? String(values[0]) : ""
					if (field === "direction") setDirection(value)
					if (field === "status") setEnabled(value)
				}),
			onClearAll: () =>
				resetPage(() => {
					setDirection("")
					setEnabled("")
				}),
		}),
		[direction, enabled, t]
	)
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(
		() => ({
			field: sortField,
			direction: sortDirection,
			fields: { name: "name", direction: "direction", status: "enabled" },
			onSortChange: (field: string, direction: "asc" | "desc") => resetPage(() => setSort(`${field}:${direction}`)),
		}),
		[sortDirection, sortField]
	)

	return (
		<div className="grid gap-4">
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<LayersIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Address Sets</Trans>
					</h1>
				</div>
				<div className="flex gap-2">
					{selected.length > 0 ? (
						<Button variant="destructive" size="sm" onClick={removeSelected}>
							<Trash2Icon className="me-2 h-4 w-4" />
							<Trans>Delete selected</Trans> ({selected.length})
						</Button>
					) : null}
					<Button variant="outline" size="sm" onClick={() => setReloadKey((value) => value + 1)} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
					<Button
						size="sm"
						onClick={() => {
							setForm(emptyForm)
							setShowForm((visible) => !visible)
						}}
					>
						<PlusIcon className="me-2 h-4 w-4" />
						<Trans>Add Set</Trans>
					</Button>
				</div>
			</div>

			{showForm ? (
				<div className="grid gap-3 rounded-md border border-border bg-card p-4 md:grid-cols-2">
					<div className="flex items-center gap-2 font-medium md:col-span-2">
						{form.id ? <PencilIcon className="h-4 w-4" /> : <PlusIcon className="h-4 w-4" />}
						{form.id ? <Trans>Edit Address Set</Trans> : <Trans>Add Address Set</Trans>}
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Name</Trans>
						</Label>
						<Input
							value={form.name}
							onChange={(event) => setForm({ ...form, name: event.target.value })}
							placeholder="电信客户"
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Geographies</Trans>
						</Label>
						<AddressReferencePicker
							kind="geography"
							value={form.geoNodeIDs}
							onChange={(geoNodeIDs) => setForm({ ...form, geoNodeIDs })}
							placeholder={t`Choose geographies`}
							multiple
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Operators</Trans>
						</Label>
						<AddressReferencePicker
							kind="operator"
							value={form.operatorIDs}
							onChange={(operatorIDs) => setForm({ ...form, operatorIDs })}
							placeholder={t`Choose operators`}
							multiple
						/>
					</div>
					<div className="grid gap-2">
						<Label>ASNs</Label>
						<Input
							value={form.asns}
							onChange={(event) => setForm({ ...form, asns: event.target.value })}
							placeholder="4134, 4837"
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>IP families</Trans>
						</Label>
						<Input
							value={form.families}
							onChange={(event) => setForm({ ...form, families: event.target.value })}
							placeholder="4, 6"
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Description</Trans>
						</Label>
						<Input
							value={form.description}
							onChange={(event) => setForm({ ...form, description: event.target.value })}
							placeholder="电信客户网段"
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Label selector</Trans> (key=value1|value2)
						</Label>
						<Input
							value={form.labels}
							onChange={(event) => setForm({ ...form, labels: event.target.value })}
							placeholder="provider=电信,type=客户"
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Explicit members</Trans>
						</Label>
						<Input
							value={form.members}
							onChange={(event) => setForm({ ...form, members: event.target.value })}
							placeholder="10.0.0.0/8, 2001:db8::/32"
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Excluded members</Trans>
						</Label>
						<Input
							value={form.excludeMembers}
							onChange={(event) => setForm({ ...form, excludeMembers: event.target.value })}
							placeholder="10.10.0.0/16"
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Included sets</Trans>
						</Label>
						<AddressReferencePicker
							kind="address-set"
							value={form.includeSetIDs}
							onChange={(includeSetIDs) => setForm({ ...form, includeSetIDs })}
							placeholder={t`Choose included sets`}
							multiple
							excludeIDs={form.id ? [form.id] : []}
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Excluded sets</Trans>
						</Label>
						<AddressReferencePicker
							kind="address-set"
							value={form.excludeSetIDs}
							onChange={(excludeSetIDs) => setForm({ ...form, excludeSetIDs })}
							placeholder={t`Choose excluded sets`}
							multiple
							excludeIDs={form.id ? [form.id] : []}
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Direction</Trans>
						</Label>
						<Select
							value={form.direction}
							onValueChange={(value: "in" | "out" | "both") => setForm({ ...form, direction: value })}
						>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="in">
									<Trans>In</Trans>
								</SelectItem>
								<SelectItem value="out">
									<Trans>Out</Trans>
								</SelectItem>
								<SelectItem value="both">
									<Trans>Both</Trans>
								</SelectItem>
							</SelectContent>
						</Select>
					</div>
					<label className="flex items-center gap-2 self-end pb-2 text-sm">
						<input
							type="checkbox"
							checked={form.enabled}
							onChange={(event) => setForm({ ...form, enabled: event.target.checked })}
						/>
						<Trans>Enabled</Trans>
					</label>
					<div className="text-xs text-muted-foreground md:col-span-2">
						<Trans>
							Result = selector union explicit members union included sets, minus excluded members and excluded sets.
						</Trans>
					</div>
					<div className="flex gap-2 md:col-span-2">
						<Button size="sm" onClick={save}>
							<Trans>Save</Trans>
						</Button>
						<Button
							variant="ghost"
							size="sm"
							onClick={() => {
								setShowForm(false)
								setForm(emptyForm)
							}}
						>
							<Trans>Cancel</Trans>
						</Button>
					</div>
				</div>
			) : null}

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			<div className="overflow-hidden rounded-md border border-border bg-card">
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={t`No address sets found.`}
					searchValue={search}
					onSearchChange={setSearch}
					searchPlaceholder={t`Search name or description...`}
					height={560}
					serverPagination={{
						page,
						pageSize,
						totalCount: total,
						onPageChange: setPage,
						onPageSizeChange: (value) => resetPage(() => setPageSize(value)),
					}}
					serverFiltering={serverFiltering}
					serverSorting={serverSorting}
					selectable={selectable}
					onCellClick={(record, field) => {
						if (field === "edit") edit(record)
						if (field === "remove") remove(record)
					}}
				/>
			</div>
		</div>
	)
})

function denseCellStyle() {
	return { padding: [8, 10, 8, 10], textBaseline: "middle", autoWrapText: false }
}

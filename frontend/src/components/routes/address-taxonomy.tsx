import { Trans, useLingui } from "@lingui/react/macro"
import { GitBranchIcon, MapIcon, NetworkIcon, PlusIcon, RefreshCwIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { AddressReferencePicker } from "@/components/address-reference-picker"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { parseASNList, toggleListValue } from "@/lib/address-taxonomy-form"
import { api } from "@/lib/api"

type TaxonomyKind = "geography" | "operators" | "lines"

type GeoNode = {
	id: string
	kind: string
	code: string
	parent_id?: string
	name: string
	short_name?: string
	sort_order: number
	enabled: boolean
	row_version: number
}

type Operator = {
	id: string
	flow_isp_id: number
	code: string
	name: string
	short_name?: string
	category: string
	asns: number[]
	sort_order: number
	enabled: boolean
	row_version: number
}

type GeoLine = {
	id: string
	parent_id?: string
	code: string
	name: string
	description?: string
	geo_selector: { geo_node_ids?: string[]; families?: number[]; operator_ids?: string[]; asns?: number[] }
	members?: string[]
	exclude_line_ids?: string[]
	operator_id?: string
	address_set_id?: string
	sort_order: number
	enabled: boolean
	row_version: number
}

type TaxonomyItem = GeoNode | Operator | GeoLine
type ListResponse<T> = { items?: T[]; next_cursor?: string; total?: number }

type TaxonomyForm = {
	id: string
	rowVersion: number
	flowISPID: number
	code: string
	name: string
	shortName: string
	kind: string
	parentID: string
	category: string
	asns: string
	description: string
	geoNodeIDs: string[]
	families: number[]
	operatorIDs: string[]
	members: string
	excludeLineIDs: string[]
	operatorID: string
	addressSetID: string
	sortOrder: string
	enabled: boolean
}

const emptyForm: TaxonomyForm = {
	id: "",
	rowVersion: 0,
	flowISPID: 0,
	code: "",
	name: "",
	shortName: "",
	kind: "country",
	parentID: "",
	category: "other",
	asns: "",
	description: "",
	geoNodeIDs: [],
	families: [4, 6],
	operatorIDs: [],
	members: "",
	excludeLineIDs: [],
	operatorID: "",
	addressSetID: "",
	sortOrder: "0",
	enabled: true,
}

const endpointByKind: Record<TaxonomyKind, string> = {
	geography: "/api/v1/geo/dictionary",
	operators: "/api/v1/network/operators",
	lines: "/api/v1/geo/lines",
}

// geo_dict kinds: continent→country→province→city are the parented hierarchy;
// region + the three base-data kinds stand alone (P4).
const GEO_KINDS = [
	"continent",
	"region",
	"country",
	"province",
	"city",
	"search_engine",
	"cloud_provider",
	"natural_region",
] as const

export default memo(function AddressTaxonomy({ kind, fixedKind }: { kind: TaxonomyKind; fixedKind?: string }) {
	const { t } = useLingui()
	const [items, setItems] = useState<TaxonomyItem[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [geoKind, setGeoKind] = useState("")
	const [enabled, setEnabled] = useState("")
	const [sort, setSort] = useState("order:asc")
	const [reloadKey, setReloadKey] = useState(0)
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const [showForm, setShowForm] = useState(false)
	const [form, setForm] = useState<TaxonomyForm>(emptyForm)
	const [geoOptions, setGeoOptions] = useState<GeoNode[]>([])
	const [operatorOptions, setOperatorOptions] = useState<Operator[]>([])
	const [lineOptions, setLineOptions] = useState<GeoLine[]>([])
	const [setOptions, setSetOptions] = useState<Array<{ id: string; name: string }>>([])
	const requestSequence = useRef(0)

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setPage(0)
			setDebouncedSearch(search.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const loadReferences = useCallback(async () => {
		try {
			const [geography, operators, lines, sets] = await Promise.all([
				api.send<ListResponse<GeoNode>>("/api/v1/geo/dictionary", { query: { limit: 500 } }),
				api.send<ListResponse<Operator>>("/api/v1/network/operators", { query: { limit: 500 } }),
				api.send<ListResponse<GeoLine>>("/api/v1/geo/lines", { query: { limit: 500 } }),
				api.send<ListResponse<{ id: string; name: string }>>("/api/v1/address-sets", { query: { limit: 500 } }),
			])
			setGeoOptions(geography.items ?? [])
			setOperatorOptions(operators.items ?? [])
			setLineOptions(lines.items ?? [])
			setSetOptions(sets.items ?? [])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load address library references`)
		}
	}, [t])

	const fetchPage = useCallback(async () => {
		const sequence = ++requestSequence.current
		const [sortField, order] = sort.split(":")
		setLoading(true)
		setError("")
		try {
			const data = await api.send<ListResponse<TaxonomyItem>>(endpointByKind[kind], {
				query: {
					q: debouncedSearch || undefined,
					kind: kind === "geography" ? (fixedKind ?? (geoKind || undefined)) : undefined,
					enabled: enabled || undefined,
					limit: pageSize,
					offset: page * pageSize || undefined,
					sort: sortField,
					order,
				},
			})
			if (sequence !== requestSequence.current) return
			setItems(data.items ?? [])
			setTotal(data.total ?? 0)
			await loadReferences()
		} catch (err) {
			if (sequence !== requestSequence.current) return
			setItems([])
			setTotal(0)
			setError(err instanceof Error ? err.message : t`Failed to load`)
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [debouncedSearch, enabled, fixedKind, geoKind, kind, loadReferences, page, pageSize, reloadKey, sort, t])

	useEffect(() => {
		fetchPage()
	}, [fetchPage])

	useEffect(() => {
		setItems([])
		setPage(0)
		setSearch("")
		setDebouncedSearch("")
		setGeoKind("")
		setEnabled("")
		setSort("order:asc")
		setForm(emptyForm)
		setShowForm(false)
	}, [kind])

	const edit = useCallback(
		(item: TaxonomyItem) => {
			if (kind === "geography") {
				const geo = item as GeoNode
				setForm({
					...emptyForm,
					id: geo.id,
					rowVersion: geo.row_version,
					code: geo.code,
					name: geo.name,
					shortName: geo.short_name ?? "",
					kind: geo.kind,
					parentID: geo.parent_id ?? "",
					sortOrder: String(geo.sort_order),
					enabled: geo.enabled,
				})
			} else if (kind === "operators") {
				const operator = item as Operator
				setForm({
					...emptyForm,
					id: operator.id,
					rowVersion: operator.row_version,
					flowISPID: operator.flow_isp_id,
					code: operator.code,
					name: operator.name,
					shortName: operator.short_name ?? "",
					category: operator.category,
					asns: operator.asns.join(", "),
					sortOrder: String(operator.sort_order),
					enabled: operator.enabled,
				})
			} else {
				const line = item as GeoLine
				setForm({
					...emptyForm,
					id: line.id,
					rowVersion: line.row_version,
					code: line.code,
					name: line.name,
					parentID: line.parent_id ?? "",
					description: line.description ?? "",
					geoNodeIDs: line.geo_selector.geo_node_ids ?? [],
					families: line.geo_selector.families ?? [],
					operatorIDs: line.geo_selector.operator_ids ?? [],
					asns: (line.geo_selector.asns ?? []).join(", "),
					members: (line.members ?? []).join("\n"),
					excludeLineIDs: line.exclude_line_ids ?? [],
					operatorID: line.operator_id ?? "",
					addressSetID: line.address_set_id ?? "",
					sortOrder: String(line.sort_order),
					enabled: line.enabled,
				})
			}
			setShowForm(true)
		},
		[kind]
	)

	const save = async () => {
		setError("")
		try {
			const sortOrder = Number(form.sortOrder)
			if (!Number.isInteger(sortOrder)) throw new Error(t`Sort order must be an integer`)
			let body: Record<string, unknown>
			if (kind === "geography") {
				body = {
					kind: form.kind,
					code: form.code,
					parent_id: form.parentID,
					name: form.name,
					short_name: form.shortName,
					sort_order: sortOrder,
					enabled: form.enabled,
				}
			} else if (kind === "operators") {
				body = {
					code: form.code,
					name: form.name,
					short_name: form.shortName,
					category: form.category,
					asns: parseASNList(form.asns),
					sort_order: sortOrder,
					enabled: form.enabled,
				}
			} else {
				const members = form.members
					.split(/[\s,]+/)
					.map((value) => value.trim())
					.filter(Boolean)
				body = {
					parent_id: form.parentID,
					code: form.code,
					name: form.name,
					description: form.description,
					geo_selector: {
						geo_node_ids: form.geoNodeIDs,
						families: form.families,
						operator_ids: form.operatorIDs,
						asns: parseASNList(form.asns),
					},
					members,
					exclude_line_ids: form.excludeLineIDs,
					operator_id: form.operatorID,
					address_set_id: form.addressSetID,
					sort_order: sortOrder,
					enabled: form.enabled,
				}
			}
			await api.send(form.id ? `${endpointByKind[kind]}/${form.id}` : endpointByKind[kind], {
				method: form.id ? "PATCH" : "POST",
				headers: form.id ? { "If-Match": `"${form.rowVersion}"` } : undefined,
				body,
			})
			setShowForm(false)
			setForm(emptyForm)
			await fetchPage()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save`)
		}
	}

	const remove = useCallback(
		async (item: TaxonomyItem) => {
			if (!confirm(t`Delete this address taxonomy item?`)) return
			setError("")
			try {
				await api.send(`${endpointByKind[kind]}/${item.id}`, {
					method: "DELETE",
					headers: { "If-Match": `"${item.row_version}"` },
				})
				await fetchPage()
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to delete`)
			}
		},
		[fetchPage, kind, t]
	)

	const names = useMemo(
		() => ({
			geo: new Map(geoOptions.map((item) => [item.id, item.name])),
			operators: new Map(operatorOptions.map((item) => [item.id, item.name])),
			lines: new Map(lineOptions.map((item) => [item.id, item.name])),
			sets: new Map(setOptions.map((item) => [item.id, item.name])),
		}),
		[geoOptions, lineOptions, operatorOptions, setOptions]
	)

	const records = useMemo(
		() =>
			items.map((item) => {
				if (kind === "geography") {
					const geo = item as GeoNode
					return {
						id: geo.id,
						code: geo.code,
						name: geo.name,
						kind: geo.kind,
						parent: names.geo.get(geo.parent_id ?? "") ?? "—",
						shortName: geo.short_name || "—",
						enabled: geo.enabled ? t`Yes` : t`No`,
						order: geo.sort_order,
						edit: t`Edit`,
						delete: t`Delete`,
						item: geo,
					}
				}
				if (kind === "operators") {
					const operator = item as Operator
					return {
						id: operator.id,
						flowISPID: operator.flow_isp_id,
						code: operator.code,
						name: operator.name,
						category: operator.category,
						asns: operator.asns.join(", ") || "—",
						enabled: operator.enabled ? t`Yes` : t`No`,
						order: operator.sort_order,
						edit: t`Edit`,
						delete: t`Delete`,
						item: operator,
					}
				}
				const line = item as GeoLine
				return {
					id: line.id,
					code: line.code,
					name: line.name,
					parent: names.lines.get(line.parent_id ?? "") ?? "—",
					geography: (line.geo_selector.geo_node_ids ?? []).map((id) => names.geo.get(id) ?? id).join(", ") || "—",
					families: (line.geo_selector.families ?? []).map((value) => `IPv${value}`).join(", ") || "—",
					operator: names.operators.get(line.operator_id ?? "") ?? "—",
					addressSet: names.sets.get(line.address_set_id ?? "") ?? "—",
					enabled: line.enabled ? t`Yes` : t`No`,
					order: line.sort_order,
					edit: t`Edit`,
					delete: t`Delete`,
					item: line,
				}
			}),
		[items, kind, names, t]
	)

	const columns = useMemo(
		() =>
			taxonomyColumns(kind, {
				code: t`Code`,
				name: t`Name`,
				level: t`Level`,
				parent: t`Parent`,
				shortName: t`Short name`,
				category: t`Category`,
				geography: t`Geography`,
				families: t`Families`,
				operator: t`Operator`,
				addressSet: t`Address set`,
				sortOrder: t`Sort order`,
				enabled: t`Enabled`,
				edit: t`Edit`,
				delete: t`Delete`,
			}),
		[kind, t]
	)
	const title =
		kind === "geography"
			? t`Geography Dictionary`
			: kind === "operators"
				? t`ISP Operators`
				: t`Geography / Operator Lines`
	const resetPage = (update: () => void) => {
		setPage(0)
		update()
	}
	const serverFiltering = useMemo(() => {
		const options: Record<string, Array<{ value: string; label?: string }>> = {
			enabled: [
				{ value: "true", label: t`Enabled` },
				{ value: "false", label: t`Disabled` },
			],
		}
		const selected: Record<string, unknown[]> = { enabled: enabled ? [enabled] : [] }
		const selection: Record<string, "single"> = { enabled: "single" }
		if (kind === "geography" && !fixedKind) {
			options.kind = GEO_KINDS.map((value) => ({ value }))
			selected.kind = geoKind ? [geoKind] : []
			selection.kind = "single"
		}
		return {
			options,
			selected,
			selection,
			onColumnFilterChange: (field: string, values: unknown[]) =>
				resetPage(() => {
					const value = values.length > 0 ? String(values[0]) : ""
					if (field === "kind") setGeoKind(value)
					if (field === "enabled") setEnabled(value)
				}),
			onClearAll: () =>
				resetPage(() => {
					setGeoKind("")
					setEnabled("")
				}),
		}
	}, [enabled, fixedKind, geoKind, kind, t])
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(() => {
		const fields: Record<string, string> = { code: "code", name: "name", order: "order", enabled: "enabled" }
		if (kind === "geography") fields.kind = "kind"
		if (kind === "operators") {
			fields.flowISPID = "flow_isp_id"
			fields.category = "category"
		}
		if (kind === "lines") fields.parent = "parent"
		return {
			field: sortField,
			direction: sortDirection,
			fields,
			onSortChange: (field: string, direction: "asc" | "desc") => resetPage(() => setSort(`${field}:${direction}`)),
		}
	}, [kind, sortDirection, sortField])
	const Icon = kind === "geography" ? MapIcon : kind === "operators" ? NetworkIcon : GitBranchIcon

	return (
		<div className="grid gap-4">
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<Icon className="h-5 w-5 text-muted-foreground" />
					<h2 className="text-lg font-semibold">{title}</h2>
				</div>
				<div className="flex gap-2">
					<Button variant="outline" size="sm" onClick={() => setReloadKey((value) => value + 1)}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
					<Button
						size="sm"
						onClick={() => {
							setForm(fixedKind ? { ...emptyForm, kind: fixedKind } : emptyForm)
							setShowForm(true)
						}}
					>
						<PlusIcon className="me-2 h-4 w-4" />
						<Trans>Add</Trans>
					</Button>
				</div>
			</div>
			{showForm ? (
				<TaxonomyEditor
					kind={kind}
					fixedKind={fixedKind}
					form={form}
					setForm={setForm}
					geoOptions={geoOptions}
					operatorOptions={operatorOptions}
					lineOptions={lineOptions}
					setOptions={setOptions}
					onSave={save}
					onCancel={() => {
						setShowForm(false)
						setForm(emptyForm)
					}}
				/>
			) : null}
			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			<PagedVTable
				records={records}
				columns={columns}
				loading={loading}
				emptyText={t`No taxonomy entries found.`}
				searchValue={search}
				onSearchChange={setSearch}
				searchPlaceholder={t`Search code or name...`}
				height={440}
				serverPagination={{
					page,
					pageSize,
					totalCount: total,
					onPageChange: setPage,
					onPageSizeChange: (value) => resetPage(() => setPageSize(value)),
				}}
				serverFiltering={serverFiltering}
				serverSorting={serverSorting}
				onCellClick={(record, field) => {
					const item = record.item as TaxonomyItem
					if (field === "edit") edit(item)
					if (field === "delete") remove(item)
				}}
			/>
		</div>
	)
})

function TaxonomyEditor({
	kind,
	fixedKind,
	form,
	setForm,
	geoOptions,
	operatorOptions,
	lineOptions,
	setOptions,
	onSave,
	onCancel,
}: {
	kind: TaxonomyKind
	fixedKind?: string
	form: TaxonomyForm
	setForm: (value: TaxonomyForm) => void
	geoOptions: GeoNode[]
	operatorOptions: Operator[]
	lineOptions: GeoLine[]
	setOptions: Array<{ id: string; name: string }>
	onSave: () => void
	onCancel: () => void
}) {
	return (
		<div className="grid gap-3 rounded-md border border-border bg-card p-4 md:grid-cols-3">
			<FormInput label="Code" value={form.code} onChange={(code) => setForm({ ...form, code })} />
			<FormInput label="Name" value={form.name} onChange={(name) => setForm({ ...form, name })} />
			<FormInput
				label="Sort order"
				value={form.sortOrder}
				onChange={(sortOrder) => setForm({ ...form, sortOrder })}
				inputMode="numeric"
			/>
			{kind === "geography" ? (
				<>
					{fixedKind ? null : (
						<div className="grid gap-2">
							<Label>
								<Trans>Level</Trans>
							</Label>
							<Select value={form.kind} onValueChange={(value) => setForm({ ...form, kind: value })}>
								<SelectTrigger>
									<SelectValue />
								</SelectTrigger>
								<SelectContent>
									{GEO_KINDS.map((value) => (
										<SelectItem key={value} value={value}>
											{value}
										</SelectItem>
									))}
								</SelectContent>
							</Select>
						</div>
					)}
					{["continent", "search_engine", "cloud_provider", "natural_region"].includes(form.kind) ? null : (
						<ReferenceField
							label="Parent geography"
							kind="geography"
							value={form.parentID}
							onChange={(parentID) => setForm({ ...form, parentID })}
							placeholder="Choose parent geography"
							options={geoOptions.map((item) => ({
								id: item.id,
								label: `${item.kind} · ${item.name}`,
								description: item.code,
							}))}
							excludeIDs={form.id ? [form.id] : []}
						/>
					)}
					<FormInput
						label="Short name"
						value={form.shortName}
						onChange={(shortName) => setForm({ ...form, shortName })}
					/>
				</>
			) : null}
			{kind === "operators" ? (
				<>
					<FormInput
						label="Flow ISP ID"
						value={form.flowISPID ? String(form.flowISPID) : "Assigned on create"}
						onChange={() => undefined}
						readOnly
					/>
					<FormInput
						label="Short name"
						value={form.shortName}
						onChange={(shortName) => setForm({ ...form, shortName })}
					/>
					<FormInput label="Category" value={form.category} onChange={(category) => setForm({ ...form, category })} />
					<FormInput label="ASNs (comma separated)" value={form.asns} onChange={(asns) => setForm({ ...form, asns })} />
				</>
			) : null}
			{kind === "lines" ? (
				<>
					<ReferenceField
						label="Parent line"
						kind="line"
						value={form.parentID}
						onChange={(parentID) => setForm({ ...form, parentID })}
						placeholder="Choose parent line"
						options={lineOptions.map((item) => ({ id: item.id, label: item.name, description: item.code }))}
						excludeIDs={form.id ? [form.id] : []}
					/>
					<ReferenceField
						label="Operator"
						kind="operator"
						value={form.operatorID}
						onChange={(operatorID) => setForm({ ...form, operatorID })}
						placeholder="Choose operator"
						options={operatorOptions.map((item) => ({ id: item.id, label: item.name, description: item.code }))}
					/>
					<ReferenceField
						label="Address set"
						kind="address-set"
						value={form.addressSetID}
						onChange={(addressSetID) => setForm({ ...form, addressSetID })}
						placeholder="Choose address set"
						options={setOptions.map((item) => ({ id: item.id, label: item.name }))}
					/>
					<div className="grid gap-2 md:col-span-3">
						<Label>
							<Trans>Description</Trans>
						</Label>
						<Textarea
							value={form.description}
							onChange={(event) => setForm({ ...form, description: event.target.value })}
						/>
					</div>
					<div className="grid gap-2 md:col-span-2">
						<Label>
							<Trans>Geography combination</Trans>
						</Label>
						<AddressReferencePicker
							kind="geography"
							value={form.geoNodeIDs}
							onChange={(geoNodeIDs) => setForm({ ...form, geoNodeIDs })}
							placeholder="Choose geography combination"
							multiple
							initialOptions={geoOptions.map((item) => ({
								id: item.id,
								label: `${item.kind} · ${item.name}`,
								description: item.code,
							}))}
						/>
					</div>
					<div className="grid content-start gap-2">
						<Label>
							<Trans>Address families</Trans>
						</Label>
						<CheckOption
							checked={form.families.includes(4)}
							label="IPv4"
							onChange={(checked) => setForm({ ...form, families: toggleListValue(form.families, 4, checked) })}
						/>
						<CheckOption
							checked={form.families.includes(6)}
							label="IPv6"
							onChange={(checked) => setForm({ ...form, families: toggleListValue(form.families, 6, checked) })}
						/>
					</div>
					<div className="grid gap-2 md:col-span-2">
						<Label>
							<Trans>Operators (selector)</Trans>
						</Label>
						<AddressReferencePicker
							kind="operator"
							value={form.operatorIDs}
							onChange={(operatorIDs) => setForm({ ...form, operatorIDs })}
							placeholder="Add operators to the selector"
							multiple
							initialOptions={operatorOptions.map((item) => ({
								id: item.id,
								label: item.name,
								description: item.code,
							}))}
						/>
					</div>
					<FormInput
						label="Selector ASNs (comma separated)"
						value={form.asns}
						onChange={(asns) => setForm({ ...form, asns })}
					/>
					<div className="grid gap-2 md:col-span-2">
						<Label>
							<Trans>Exclude groups</Trans>
						</Label>
						<AddressReferencePicker
							kind="line"
							value={form.excludeLineIDs}
							onChange={(excludeLineIDs) => setForm({ ...form, excludeLineIDs })}
							placeholder="Subtract these groups"
							multiple
							excludeIDs={form.id ? [form.id] : []}
							initialOptions={lineOptions.map((item) => ({ id: item.id, label: item.name, description: item.code }))}
						/>
					</div>
					<div className="grid gap-2 md:col-span-3">
						<Label>
							<Trans>Members (one CIDR per line)</Trans>
						</Label>
						<Textarea
							value={form.members}
							onChange={(event) => setForm({ ...form, members: event.target.value })}
							placeholder="203.0.113.0/24"
						/>
					</div>
				</>
			) : null}
			<div className="flex items-center gap-2">
				<Checkbox checked={form.enabled} onCheckedChange={(value) => setForm({ ...form, enabled: value === true })} />
				<Label>
					<Trans>Enabled</Trans>
				</Label>
			</div>
			<div className="flex gap-2 md:col-span-3">
				<Button onClick={onSave}>
					<Trans>Save</Trans>
				</Button>
				<Button variant="outline" onClick={onCancel}>
					<Trans>Cancel</Trans>
				</Button>
			</div>
		</div>
	)
}

function FormInput({
	label,
	value,
	onChange,
	inputMode,
	readOnly = false,
}: {
	label: string
	value: string
	onChange: (value: string) => void
	inputMode?: "numeric"
	readOnly?: boolean
}) {
	return (
		<div className="grid gap-2">
			<Label>{label}</Label>
			<Input
				value={value}
				inputMode={inputMode}
				readOnly={readOnly}
				onChange={(event) => onChange(event.target.value)}
			/>
		</div>
	)
}

function ReferenceField({
	label,
	kind,
	value,
	options,
	onChange,
	placeholder,
	excludeIDs,
}: {
	label: string
	kind: "geography" | "operator" | "line" | "address-set"
	value: string
	options: Array<{ id: string; label: string; description?: string }>
	onChange: (value: string) => void
	placeholder: string
	excludeIDs?: string[]
}) {
	return (
		<div className="grid gap-2">
			<Label>{label}</Label>
			<AddressReferencePicker
				kind={kind}
				value={value ? [value] : []}
				onChange={(selected) => onChange(selected[0] ?? "")}
				placeholder={placeholder}
				initialOptions={options}
				excludeIDs={excludeIDs}
			/>
		</div>
	)
}

function CheckOption({
	checked,
	label,
	onChange,
}: {
	checked: boolean
	label: string
	onChange: (checked: boolean) => void
}) {
	return (
		<div className="flex items-center gap-2 text-sm">
			<Checkbox checked={checked} aria-label={label} onCheckedChange={(value) => onChange(value === true)} />
			<span>{label}</span>
		</div>
	)
}

type TaxonomyColumnLabels = {
	code: string
	name: string
	level: string
	parent: string
	shortName: string
	category: string
	geography: string
	families: string
	operator: string
	addressSet: string
	sortOrder: string
	enabled: string
	edit: string
	delete: string
}

function taxonomyColumns(kind: TaxonomyKind, labels: TaxonomyColumnLabels) {
	const common = [
		{ field: "code", title: labels.code, width: 140, style: denseCellStyle() },
		{ field: "name", title: labels.name, width: 180, style: denseCellStyle() },
	]
	const specific =
		kind === "geography"
			? [
					{ field: "kind", title: labels.level, width: 110, style: denseCellStyle() },
					{ field: "parent", title: labels.parent, width: 180, style: denseCellStyle() },
					{ field: "shortName", title: labels.shortName, width: 140, style: denseCellStyle() },
				]
			: kind === "operators"
				? [
						{ field: "flowISPID", title: "Flow ISP ID", width: 120, style: denseCellStyle() },
						{ field: "category", title: labels.category, width: 130, style: denseCellStyle() },
						{ field: "asns", title: "ASNs", width: 260, style: denseCellStyle() },
					]
				: [
						{ field: "parent", title: labels.parent, width: 150, style: denseCellStyle() },
						{ field: "geography", title: labels.geography, width: 260, style: denseCellStyle() },
						{ field: "families", title: labels.families, width: 110, style: denseCellStyle() },
						{ field: "operator", title: labels.operator, width: 160, style: denseCellStyle() },
						{ field: "addressSet", title: labels.addressSet, width: 160, style: denseCellStyle() },
					]
	return [
		...common,
		...specific,
		{ field: "order", title: labels.sortOrder, width: 100, style: denseCellStyle() },
		{ field: "enabled", title: labels.enabled, width: 100, style: denseCellStyle() },
		{ field: "edit", title: labels.edit, width: 80, filter: false, style: actionCellStyle("#2563eb") },
		{ field: "delete", title: labels.delete, width: 80, filter: false, style: actionCellStyle("#dc2626") },
	]
}

function denseCellStyle() {
	return { padding: [8, 10, 8, 10] as [number, number, number, number], fontSize: 13 }
}

function actionCellStyle(color: string) {
	return { ...denseCellStyle(), color, cursor: "pointer" }
}

import { Trans, useLingui } from "@lingui/react/macro"
import { GitBranchIcon, MapIcon, NetworkIcon, PlusIcon, RefreshCwIcon, SearchIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useState } from "react"
import { AddressReferencePicker } from "@/components/address-reference-picker"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { parseASNList, toggleListValue } from "@/lib/address-taxonomy-form"
import { pb } from "@/lib/api"

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
	geo_selector: { geo_node_ids?: string[]; families?: number[] }
	operator_id?: string
	address_set_id?: string
	sort_order: number
	enabled: boolean
	row_version: number
}

type TaxonomyItem = GeoNode | Operator | GeoLine
type ListResponse<T> = { items?: T[]; next_cursor?: string }

type TaxonomyForm = {
	id: string
	rowVersion: number
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
	operatorID: string
	addressSetID: string
	sortOrder: string
	enabled: boolean
}

const pageSize = 100
const emptyForm: TaxonomyForm = {
	id: "",
	rowVersion: 0,
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

export default memo(function AddressTaxonomy({ kind }: { kind: TaxonomyKind }) {
	const { t } = useLingui()
	const [items, setItems] = useState<TaxonomyItem[]>([])
	const [nextCursor, setNextCursor] = useState("")
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [geoKind, setGeoKind] = useState("all")
	const [enabled, setEnabled] = useState("all")
	const [loading, setLoading] = useState(true)
	const [loadingMore, setLoadingMore] = useState(false)
	const [error, setError] = useState("")
	const [showForm, setShowForm] = useState(false)
	const [form, setForm] = useState<TaxonomyForm>(emptyForm)
	const [geoOptions, setGeoOptions] = useState<GeoNode[]>([])
	const [operatorOptions, setOperatorOptions] = useState<Operator[]>([])
	const [lineOptions, setLineOptions] = useState<GeoLine[]>([])
	const [setOptions, setSetOptions] = useState<Array<{ id: string; name: string }>>([])

	useEffect(() => {
		const timer = window.setTimeout(() => setDebouncedSearch(search.trim()), 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const loadReferences = useCallback(async () => {
		try {
			const [geography, operators, lines, sets] = await Promise.all([
				pb.send<ListResponse<GeoNode>>("/api/v1/geo/dictionary", { query: { limit: 500 } }),
				pb.send<ListResponse<Operator>>("/api/v1/network/operators", { query: { limit: 500 } }),
				pb.send<ListResponse<GeoLine>>("/api/v1/geo/lines", { query: { limit: 500 } }),
				pb.send<ListResponse<{ id: string; name: string }>>("/api/v1/address-sets", { query: { limit: 500 } }),
			])
			setGeoOptions(geography.items ?? [])
			setOperatorOptions(operators.items ?? [])
			setLineOptions(lines.items ?? [])
			setSetOptions(sets.items ?? [])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load address library references`)
		}
	}, [t])

	const fetchPage = useCallback(
		async (cursor: string, append: boolean) => {
			append ? setLoadingMore(true) : setLoading(true)
			setError("")
			try {
				const data = await pb.send<ListResponse<TaxonomyItem>>(endpointByKind[kind], {
					query: {
						q: debouncedSearch || undefined,
						kind: kind === "geography" && geoKind !== "all" ? geoKind : undefined,
						enabled: enabled === "all" ? undefined : enabled,
						limit: pageSize,
						cursor: cursor || undefined,
					},
				})
				setItems((current) => (append ? [...current, ...(data.items ?? [])] : (data.items ?? [])))
				setNextCursor(data.next_cursor ?? "")
				if (!append) await loadReferences()
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to load`)
			} finally {
				append ? setLoadingMore(false) : setLoading(false)
			}
		},
		[debouncedSearch, enabled, geoKind, kind, loadReferences, t]
	)

	useEffect(() => {
		setItems([])
		setForm(emptyForm)
		setShowForm(false)
		fetchPage("", false)
	}, [fetchPage])

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
				body = {
					parent_id: form.parentID,
					code: form.code,
					name: form.name,
					description: form.description,
					geo_selector: { geo_node_ids: form.geoNodeIDs, families: form.families },
					operator_id: form.operatorID,
					address_set_id: form.addressSetID,
					sort_order: sortOrder,
					enabled: form.enabled,
				}
			}
			await pb.send(form.id ? `${endpointByKind[kind]}/${form.id}` : endpointByKind[kind], {
				method: form.id ? "PATCH" : "POST",
				headers: form.id ? { "If-Match": `"${form.rowVersion}"` } : undefined,
				body,
			})
			setShowForm(false)
			setForm(emptyForm)
			await fetchPage("", false)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save`)
		}
	}

	const remove = useCallback(
		async (item: TaxonomyItem) => {
			if (!confirm(t`Delete this address taxonomy item?`)) return
			setError("")
			try {
				await pb.send(`${endpointByKind[kind]}/${item.id}`, {
					method: "DELETE",
					headers: { "If-Match": `"${item.row_version}"` },
				})
				await fetchPage("", false)
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
						enabled: yesNo(geo.enabled),
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
						code: operator.code,
						name: operator.name,
						category: operator.category,
						asns: operator.asns.join(", ") || "—",
						enabled: yesNo(operator.enabled),
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
					enabled: yesNo(line.enabled),
					edit: t`Edit`,
					delete: t`Delete`,
					item: line,
				}
			}),
		[items, kind, names, t]
	)

	const columns = useMemo(() => taxonomyColumns(kind, t), [kind, t])
	const Icon = kind === "geography" ? MapIcon : kind === "operators" ? NetworkIcon : GitBranchIcon

	return (
		<div className="grid gap-4">
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<Icon className="h-5 w-5 text-muted-foreground" />
					<h2 className="text-lg font-semibold">{taxonomyTitle(kind)}</h2>
				</div>
				<div className="flex gap-2">
					<Button variant="outline" size="sm" onClick={() => fetchPage("", false)}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
					<Button
						size="sm"
						onClick={() => {
							setForm(emptyForm)
							setShowForm(true)
						}}
					>
						<PlusIcon className="me-2 h-4 w-4" />
						<Trans>Add</Trans>
					</Button>
				</div>
			</div>
			<div className="flex flex-wrap gap-2">
				<div className="relative min-w-64 max-w-sm flex-1">
					<SearchIcon className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
					<Input
						className="pl-9"
						value={search}
						onChange={(event) => setSearch(event.target.value)}
						placeholder={t`Search code or name...`}
					/>
				</div>
				{kind === "geography" ? (
					<Select value={geoKind} onValueChange={setGeoKind}>
						<SelectTrigger className="w-40">
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="all">
								<Trans>All levels</Trans>
							</SelectItem>
							{["continent", "region", "country", "province", "city"].map((value) => (
								<SelectItem key={value} value={value}>
									{value}
								</SelectItem>
							))}
						</SelectContent>
					</Select>
				) : null}
				<Select value={enabled} onValueChange={setEnabled}>
					<SelectTrigger className="w-36">
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="all">
							<Trans>All states</Trans>
						</SelectItem>
						<SelectItem value="true">
							<Trans>Enabled</Trans>
						</SelectItem>
						<SelectItem value="false">
							<Trans>Disabled</Trans>
						</SelectItem>
					</SelectContent>
				</Select>
			</div>
			{showForm ? (
				<TaxonomyEditor
					kind={kind}
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
				showSearch={false}
				height={440}
				onCellClick={(record, field) => {
					const item = record.item as TaxonomyItem
					if (field === "edit") edit(item)
					if (field === "delete") remove(item)
				}}
			/>
			{nextCursor ? (
				<Button variant="outline" onClick={() => fetchPage(nextCursor, true)} disabled={loadingMore}>
					{loadingMore ? <Trans>Loading...</Trans> : <Trans>Load more</Trans>}
				</Button>
			) : null}
		</div>
	)
})

function TaxonomyEditor({
	kind,
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
					<div className="grid gap-2">
						<Label>
							<Trans>Level</Trans>
						</Label>
						<Select value={form.kind} onValueChange={(value) => setForm({ ...form, kind: value })}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								{["continent", "region", "country", "province", "city"].map((value) => (
									<SelectItem key={value} value={value}>
										{value}
									</SelectItem>
								))}
							</SelectContent>
						</Select>
					</div>
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
}: {
	label: string
	value: string
	onChange: (value: string) => void
	inputMode?: "numeric"
}) {
	return (
		<div className="grid gap-2">
			<Label>{label}</Label>
			<Input value={value} inputMode={inputMode} onChange={(event) => onChange(event.target.value)} />
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

function taxonomyTitle(kind: TaxonomyKind) {
	if (kind === "geography") return "Geography Dictionary"
	if (kind === "operators") return "ISP Operators"
	return "Geography / Operator Lines"
}

function taxonomyColumns(kind: TaxonomyKind, t: (message: TemplateStringsArray) => string) {
	const common = [
		{ field: "code", title: t`Code`, width: 140, style: denseCellStyle() },
		{ field: "name", title: t`Name`, width: 180, style: denseCellStyle() },
	]
	const specific =
		kind === "geography"
			? [
					{ field: "kind", title: t`Level`, width: 110, style: denseCellStyle() },
					{ field: "parent", title: t`Parent`, width: 180, style: denseCellStyle() },
					{ field: "shortName", title: t`Short name`, width: 140, style: denseCellStyle() },
				]
			: kind === "operators"
				? [
						{ field: "category", title: t`Category`, width: 130, style: denseCellStyle() },
						{ field: "asns", title: "ASNs", width: 260, style: denseCellStyle() },
					]
				: [
						{ field: "parent", title: t`Parent`, width: 150, style: denseCellStyle() },
						{ field: "geography", title: t`Geography`, width: 260, style: denseCellStyle() },
						{ field: "families", title: t`Families`, width: 110, style: denseCellStyle() },
						{ field: "operator", title: t`Operator`, width: 160, style: denseCellStyle() },
						{ field: "addressSet", title: t`Address set`, width: 160, style: denseCellStyle() },
					]
	return [
		...common,
		...specific,
		{ field: "enabled", title: t`Enabled`, width: 100, style: denseCellStyle() },
		{ field: "edit", title: t`Edit`, width: 80, filter: false, style: actionCellStyle("#2563eb") },
		{ field: "delete", title: t`Delete`, width: 80, filter: false, style: actionCellStyle("#dc2626") },
	]
}

function yesNo(value: boolean) {
	return value ? "yes" : "no"
}

function denseCellStyle() {
	return { padding: [8, 10, 8, 10] as [number, number, number, number], fontSize: 13 }
}

function actionCellStyle(color: string) {
	return { ...denseCellStyle(), color, cursor: "pointer" }
}

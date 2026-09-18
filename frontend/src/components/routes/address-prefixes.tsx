import { Trans, useLingui } from "@lingui/react/macro"
import { GlobeIcon, PencilIcon, PlusIcon, RefreshCwIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { AddressReferencePicker } from "@/components/address-reference-picker"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { parsePrefixLabels } from "@/lib/address-set-form"
import { api } from "@/lib/api"
import { type AddressImport, type AddressImportSlot, ImportedPrefixBrowser } from "./address-imports"

type AddressPrefix = {
	id: string
	cidr: string
	family: number
	prefix_length: number
	labels: Record<string, string>
	geo_leaf_id?: string
	operator_id?: string
	asn?: number
	source: string
	row_version: number
}

type AddressPrefixList = { items?: AddressPrefix[]; total?: number }

type AddressOperationPreview = {
	result: string[]
	result_prefixes: number
	added_addresses_v4: string
	added_addresses_v6: string
}

type AddressPrefixMergeGroup = {
	geo_leaf_id?: string
	operator_id?: string
	asn?: number
	labels?: Record<string, string>
	source: string
	input_cidrs: string[]
	result_cidrs: string[]
}

type AddressPrefixMergePreview = {
	input_prefixes: number
	result_prefixes: number
	groups: AddressPrefixMergeGroup[]
}

const emptyForm = {
	id: "",
	rowVersion: 0,
	cidr: "",
	labels: "",
	source: "manual",
	asn: "",
	geoLeafID: "",
	operatorID: "",
	localNetwork: false,
}

const AddressPrefixes = memo(function AddressPrefixes() {
	const { t } = useLingui()
	const correctionDraft = useMemo(() => readCorrectionDraft(), [])
	const [prefixes, setPrefixes] = useState<AddressPrefix[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState(correctionDraft.search)
	const [debouncedSearch, setDebouncedSearch] = useState(correctionDraft.search)
	const [family, setFamily] = useState("")
	const [source, setSource] = useState("")
	const [sort, setSort] = useState("cidr:asc")
	const [reloadKey, setReloadKey] = useState(0)
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const [showForm, setShowForm] = useState(correctionDraft.open)
	const [form, setForm] = useState({
		...emptyForm,
		cidr: correctionDraft.ip,
		labels: correctionDraft.open ? "evidence=flow_report" : "",
	})
	const [selected, setSelected] = useState<AddressPrefix[]>([])
	const [coverPreview, setCoverPreview] = useState<AddressOperationPreview | null>(null)
	const [coverWorking, setCoverWorking] = useState(false)
	const [mergePreview, setMergePreview] = useState<AddressPrefixMergePreview | null>(null)
	const [mergeWorking, setMergeWorking] = useState(false)
	const [picker, setPicker] = useState<{ field: "geo" | "operator"; prefix: AddressPrefix } | null>(null)
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
			const data = await api.send<AddressPrefixList>("/api/v1/address-prefixes", {
				query: {
					q: debouncedSearch || undefined,
					family: family || undefined,
					source: source.trim() || undefined,
					limit: pageSize,
					offset: page * pageSize || undefined,
					sort: sortField,
					order,
				},
			})
			if (sequence !== requestSequence.current) return
			setPrefixes(data.items ?? [])
			setTotal(data.total ?? 0)
		} catch (err) {
			if (sequence !== requestSequence.current) return
			setPrefixes([])
			setTotal(0)
			setError(err instanceof Error ? err.message : t`Failed to load`)
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [debouncedSearch, family, page, pageSize, reloadKey, sort, source, t])

	useEffect(() => {
		fetchPage()
	}, [fetchPage])

	const save = async () => {
		if (!form.cidr.trim()) return
		try {
			const labels = parsePrefixLabels(form.labels)
			if (form.localNetwork) labels.flow = "local"
			else if (labels.flow === "local") delete labels.flow
			const asn = form.asn.trim() ? Number(form.asn) : undefined
			if (asn !== undefined && (!Number.isInteger(asn) || asn <= 0 || asn > 4_294_967_295)) {
				throw new Error(t`ASN must be an integer between 1 and 4294967295`)
			}
			await api.send(form.id ? `/api/v1/address-prefixes/${form.id}` : "/api/v1/address-prefixes", {
				method: form.id ? "PATCH" : "POST",
				headers: form.id ? { "If-Match": `"${form.rowVersion}"` } : undefined,
				body: {
					cidr: form.cidr.trim(),
					labels,
					source: form.source.trim() || "manual",
					asn: asn ?? 0,
					geo_leaf_id: form.geoLeafID.trim(),
					operator_id: form.operatorID.trim(),
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
		const prefix = record.item as AddressPrefix | undefined
		if (!prefix) return
		setForm({
			id: prefix.id,
			rowVersion: prefix.row_version,
			cidr: prefix.cidr,
			labels: Object.entries(prefix.labels ?? {})
				.filter(([key, value]) => key !== "flow" || value !== "local")
				.map(([key, value]) => `${key}=${value}`)
				.join(","),
			source: prefix.source,
			asn: prefix.asn ? String(prefix.asn) : "",
			geoLeafID: prefix.geo_leaf_id ?? "",
			operatorID: prefix.operator_id ?? "",
			localNetwork: prefix.labels?.flow === "local",
		})
		setShowForm(true)
		setError("")
	}, [])

	const remove = useCallback(
		async (record: Record<string, unknown>) => {
			const id = String(record.id ?? "")
			const rowVersion = Number(record.rowVersion ?? 0)
			if (!id || !rowVersion || !confirm(t`Delete this prefix?`)) return
			try {
				await api.send(`/api/v1/address-prefixes/${id}`, {
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

	const editCell = useCallback(
		async (record: Record<string, unknown>, field: string, value: string) => {
			const prefix = record.item as AddressPrefix | undefined
			if (!prefix) return
			let asn = prefix.asn ?? 0
			let source = prefix.source
			if (field === "asn") {
				const trimmed = value.trim()
				if (trimmed === "" || trimmed === "—") {
					asn = 0
				} else {
					const parsed = Number(trimmed)
					if (!Number.isInteger(parsed) || parsed <= 0 || parsed > 4_294_967_295) {
						setError(t`ASN must be an integer between 1 and 4294967295`)
						await fetchPage()
						return
					}
					asn = parsed
				}
			} else if (field === "source") {
				source = value.trim() || "manual"
			} else {
				return
			}
			try {
				const updated = await api.send<AddressPrefix>(`/api/v1/address-prefixes/${prefix.id}`, {
					method: "PATCH",
					headers: { "If-Match": `"${prefix.row_version}"` },
					body: {
						cidr: prefix.cidr,
						labels: prefix.labels ?? {},
						source,
						asn,
						geo_leaf_id: prefix.geo_leaf_id ?? "",
						operator_id: prefix.operator_id ?? "",
					},
				})
				setError("")
				setPrefixes((prev) => prev.map((item) => (item.id === prefix.id ? { ...item, ...updated } : item)))
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to save`)
				await fetchPage()
			}
		},
		[fetchPage, t]
	)
	const editable = useMemo(() => ({ fields: ["asn", "source"], onEdit: editCell }), [editCell])

	const handleCellDblClick = useCallback((record: Record<string, unknown>, field: string) => {
		if (field !== "geo" && field !== "operator") return
		const prefix = record.item as AddressPrefix | undefined
		if (prefix) setPicker({ field, prefix })
	}, [])

	const applyReference = useCallback(
		async (ids: string[]) => {
			if (!picker) return
			const { field, prefix } = picker
			setPicker(null)
			const referenceID = ids[0] ?? ""
			try {
				const updated = await api.send<AddressPrefix>(`/api/v1/address-prefixes/${prefix.id}`, {
					method: "PATCH",
					headers: { "If-Match": `"${prefix.row_version}"` },
					body: {
						cidr: prefix.cidr,
						labels: prefix.labels ?? {},
						source: prefix.source,
						asn: prefix.asn ?? 0,
						geo_leaf_id: field === "geo" ? referenceID : (prefix.geo_leaf_id ?? ""),
						operator_id: field === "operator" ? referenceID : (prefix.operator_id ?? ""),
					},
				})
				setError("")
				setPrefixes((prev) => prev.map((item) => (item.id === prefix.id ? { ...item, ...updated } : item)))
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to save`)
				await fetchPage()
			}
		},
		[picker, fetchPage, t]
	)

	const removeSelected = useCallback(async () => {
		if (selected.length === 0 || !confirm(t`Delete ${selected.length} selected prefixes?`)) return
		try {
			for (const prefix of selected) {
				await api.send(`/api/v1/address-prefixes/${prefix.id}`, {
					method: "DELETE",
					headers: { "If-Match": `"${prefix.row_version}"` },
				})
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
				setSelected(records.map((record) => record.item as AddressPrefix).filter(Boolean)),
		}),
		[]
	)

	const previewCover = useCallback(async () => {
		const cidrs = selected.map((entry) => entry.cidr).filter(Boolean)
		if (cidrs.length < 2) return
		setCoverWorking(true)
		setError("")
		try {
			const preview = await api.send<AddressOperationPreview>("/api/v1/address-sets/actions/preview", {
				method: "POST",
				body: { operation: "cover", left: cidrs },
			})
			setCoverPreview(preview)
		} catch (err) {
			setCoverPreview(null)
			setError(err instanceof Error ? err.message : t`Preview failed`)
		} finally {
			setCoverWorking(false)
		}
	}, [selected, t])

	const confirmCover = useCallback(async () => {
		if (!coverPreview || coverPreview.result.length === 0) return
		setCoverWorking(true)
		setError("")
		try {
			for (const cidr of coverPreview.result) {
				await api.send("/api/v1/address-prefixes", {
					method: "POST",
					body: { cidr, labels: {}, source: "manual", asn: 0, geo_leaf_id: "", operator_id: "" },
				})
			}
			setCoverPreview(null)
			setSelected([])
			await fetchPage()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save`)
		} finally {
			setCoverWorking(false)
		}
	}, [coverPreview, fetchPage, t])

	const previewMerge = useCallback(async () => {
		if (selected.length < 2) return
		setMergeWorking(true)
		setError("")
		try {
			const preview = await api.send<AddressPrefixMergePreview>("/api/v1/address-prefixes/actions/merge-preview", {
				method: "POST",
				body: {
					prefixes: selected.map((prefix) => ({
						cidr: prefix.cidr,
						geo_leaf_id: prefix.geo_leaf_id ?? "",
						operator_id: prefix.operator_id ?? "",
						asn: prefix.asn,
						labels: prefix.labels ?? {},
						source: prefix.source,
					})),
				},
			})
			setMergePreview(preview)
		} catch (err) {
			setMergePreview(null)
			setError(err instanceof Error ? err.message : t`Preview failed`)
		} finally {
			setMergeWorking(false)
		}
	}, [selected, t])

	const confirmMerge = useCallback(async () => {
		if (!mergePreview) return
		setMergeWorking(true)
		setError("")
		try {
			for (const group of mergePreview.groups) {
				// Only groups whose CIDRs actually coalesced change; the coalesced
				// result is coarser than any input, so it never collides with an
				// original before that original is deleted.
				if (group.result_cidrs.length >= group.input_cidrs.length) continue
				for (const cidr of group.result_cidrs) {
					await api.send("/api/v1/address-prefixes", {
						method: "POST",
						body: {
							cidr,
							labels: group.labels ?? {},
							source: group.source || "manual",
							asn: group.asn ?? 0,
							geo_leaf_id: group.geo_leaf_id ?? "",
							operator_id: group.operator_id ?? "",
						},
					})
				}
				const inputSet = new Set(group.input_cidrs)
				for (const prefix of selected) {
					if (inputSet.has(prefix.cidr)) {
						await api.send(`/api/v1/address-prefixes/${prefix.id}`, {
							method: "DELETE",
							headers: { "If-Match": `"${prefix.row_version}"` },
						})
					}
				}
			}
			setMergePreview(null)
			setSelected([])
			await fetchPage()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save`)
			await fetchPage()
		} finally {
			setMergeWorking(false)
		}
	}, [mergePreview, selected, fetchPage, t])

	const records = useMemo(
		() =>
			prefixes.map((prefix) => ({
				id: prefix.id,
				cidr: prefix.cidr,
				family: prefix.family ? `IPv${prefix.family}` : "—",
				prefixLength: prefix.prefix_length ?? "—",
				labels:
					Object.entries(prefix.labels ?? {})
						.map(([key, value]) => `${key}=${value}`)
						.join(", ") || "—",
				geo: prefix.geo_leaf_id || "—",
				operator: prefix.operator_id || "—",
				asn: prefix.asn ?? "—",
				source: prefix.source,
				edit: t`Edit`,
				remove: t`Delete`,
				rowVersion: prefix.row_version,
				item: prefix,
			})),
		[prefixes, t]
	)
	const columns = useMemo(
		() => [
			// Only Family has server-side filter options (serverFiltering.options); the
			// rest would open an empty filter popover, so their filter affordance is off.
			// Source has its own text filter below; CIDR search is the toolbar search.
			{ field: "cidr", title: t`CIDR`, width: 190, filter: false, style: denseCellStyle() },
			{ field: "family", title: t`Family`, width: 90, style: denseCellStyle() },
			{ field: "labels", title: t`Labels`, width: 300, filter: false, style: denseCellStyle() },
			{ field: "geo", title: t`Geography`, width: 160, filter: false, style: denseCellStyle() },
			{ field: "operator", title: t`Operator`, width: 160, filter: false, style: denseCellStyle() },
			{ field: "asn", title: "ASN", width: 100, filter: false, style: denseCellStyle() },
			{ field: "source", title: t`Source`, width: 110, filter: false, style: denseCellStyle() },
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
				family: [
					{ value: "4", label: "IPv4" },
					{ value: "6", label: "IPv6" },
				],
			},
			selected: { family: family ? [family] : [] },
			selection: { family: "single" as const },
			onColumnFilterChange: (_field: string, values: unknown[]) =>
				resetPage(() => setFamily(values.length > 0 ? String(values[0]) : "")),
			onClearAll: () => resetPage(() => setFamily("")),
		}),
		[family]
	)
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(
		() => ({
			field: sortField,
			direction: sortDirection,
			fields: {
				cidr: "cidr",
				family: "family",
				prefixLength: "prefix_length",
				asn: "asn",
				source: "source",
			},
			onSortChange: (field: string, direction: "asc" | "desc") => resetPage(() => setSort(`${field}:${direction}`)),
		}),
		[sortDirection, sortField]
	)

	return (
		<div className="grid gap-4">
			{correctionDraft.open ? (
				<div className="rounded-md border border-blue-200 bg-blue-50 p-3 text-sm text-blue-900">
					<Trans>Draft opened from a Flow report.</Trans> {correctionDraft.evidence}
				</div>
			) : null}
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<GlobeIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Address Prefixes</Trans>
					</h1>
				</div>
				<div className="flex gap-2">
					{selected.length >= 2 ? (
						<Button variant="outline" size="sm" onClick={previewMerge} disabled={mergeWorking}>
							<Trans>Merge selected</Trans> ({selected.length})
						</Button>
					) : null}
					{selected.length >= 2 ? (
						<Button variant="outline" size="sm" onClick={previewCover} disabled={coverWorking}>
							<Trans>Cover selected</Trans> ({selected.length})
						</Button>
					) : null}
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
						<Trans>Add Prefix</Trans>
					</Button>
				</div>
			</div>

			<div className="flex flex-wrap items-center justify-end gap-2">
				<Input
					className="w-40"
					value={source}
					onChange={(event) => resetPage(() => setSource(event.target.value))}
					placeholder={t`Source filter`}
				/>
			</div>

			{showForm ? (
				<div className="grid gap-3 rounded-md border border-border bg-card p-4 md:grid-cols-2">
					<div className="flex items-center gap-2 font-medium md:col-span-2">
						{form.id ? <PencilIcon className="h-4 w-4" /> : <PlusIcon className="h-4 w-4" />}
						{form.id ? <Trans>Edit Prefix</Trans> : <Trans>Add Prefix</Trans>}
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>CIDR, IP, or range</Trans>
						</Label>
						<Input
							value={form.cidr}
							onChange={(event) => setForm({ ...form, cidr: event.target.value })}
							placeholder="10.0.0.0/16"
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Source</Trans>
						</Label>
						<Input
							value={form.source}
							onChange={(event) => setForm({ ...form, source: event.target.value })}
							placeholder="manual"
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Labels</Trans> (key=value, comma separated)
						</Label>
						<Input
							value={form.labels}
							onChange={(event) => setForm({ ...form, labels: event.target.value })}
							placeholder="region=杭州,type=客户,provider=电信"
						/>
					</div>
					<label
						htmlFor="address-prefix-local-network"
						className="flex items-start gap-2 rounded-md border border-border p-3 md:col-span-2"
					>
						<Checkbox
							id="address-prefix-local-network"
							checked={form.localNetwork}
							onCheckedChange={(checked) => setForm({ ...form, localNetwork: checked === true })}
						/>
						<span className="grid gap-1 text-sm">
							<span className="font-medium">
								<Trans>Use as a local network prefix for Flow direction</Trans>
							</span>
							<span className="text-xs text-muted-foreground">
								<Trans>
									Flow uses these prefixes to decide inbound, outbound, internal and transit traffic. Rebuild and
									activate the address snapshot after changing them.
								</Trans>
							</span>
						</span>
					</label>
					<div className="grid gap-2">
						<Label>
							ASN (<Trans>optional</Trans>)
						</Label>
						<Input
							inputMode="numeric"
							value={form.asn}
							onChange={(event) => setForm({ ...form, asn: event.target.value })}
							placeholder="4134"
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Geography</Trans> (<Trans>optional</Trans>)
						</Label>
						<AddressReferencePicker
							kind="geography"
							value={form.geoLeafID ? [form.geoLeafID] : []}
							onChange={(value) => setForm({ ...form, geoLeafID: value[0] ?? "" })}
							placeholder={t`Choose geography`}
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Operator</Trans> (<Trans>optional</Trans>)
						</Label>
						<AddressReferencePicker
							kind="operator"
							value={form.operatorID ? [form.operatorID] : []}
							onChange={(value) => setForm({ ...form, operatorID: value[0] ?? "" })}
							placeholder={t`Choose operator`}
						/>
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

			{coverPreview ? (
				<div className="grid gap-3 rounded-md border border-border bg-card p-4">
					<div className="font-medium">
						<Trans>Cover prefix preview</Trans>
					</div>
					<div className="text-sm text-muted-foreground">
						<Trans>Covering CIDR</Trans>: {coverPreview.result.join(", ") || "—"}
						{" · "}
						<Trans>added addresses</Trans> IPv4 {coverPreview.added_addresses_v4}, IPv6{" "}
						{coverPreview.added_addresses_v6}
					</div>
					<div className="text-xs text-muted-foreground">
						<Trans>
							Creates a new manual prefix covering the selection; the selected prefixes are kept. Set its
							geography/operator inline afterward.
						</Trans>
					</div>
					<div className="flex gap-2">
						<Button size="sm" onClick={confirmCover} disabled={coverWorking}>
							<Trans>Create cover prefix</Trans>
						</Button>
						<Button variant="ghost" size="sm" onClick={() => setCoverPreview(null)} disabled={coverWorking}>
							<Trans>Cancel</Trans>
						</Button>
					</div>
				</div>
			) : null}

			{mergePreview ? (
				<div className="grid gap-3 rounded-md border border-border bg-card p-4">
					<div className="font-medium">
						<Trans>Attribution-preserving merge</Trans>
					</div>
					<div className="text-sm text-muted-foreground">
						{mergePreview.input_prefixes} → {mergePreview.result_prefixes} <Trans>prefixes</Trans>
						{" · "}
						{mergePreview.groups.length} <Trans>attribution group(s)</Trans>
					</div>
					<div className="text-xs text-muted-foreground">
						<Trans>
							Only prefixes with identical geography/operator/ASN/labels merge; coverage and attribution are preserved.
						</Trans>
					</div>
					{mergePreview.result_prefixes >= mergePreview.input_prefixes ? (
						<div className="text-xs text-muted-foreground">
							<Trans>Nothing to merge — no adjacent prefixes share the same attribution.</Trans>
						</div>
					) : null}
					<div className="flex gap-2">
						<Button
							size="sm"
							onClick={confirmMerge}
							disabled={mergeWorking || mergePreview.result_prefixes >= mergePreview.input_prefixes}
						>
							<Trans>Apply merge</Trans>
						</Button>
						<Button variant="ghost" size="sm" onClick={() => setMergePreview(null)} disabled={mergeWorking}>
							<Trans>Cancel</Trans>
						</Button>
					</div>
				</div>
			) : null}

			<div className="overflow-hidden rounded-md border border-border bg-card">
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={t`No prefixes found.`}
					searchValue={search}
					onSearchChange={setSearch}
					searchPlaceholder={t`Search CIDR or labels...`}
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
					editable={editable}
					selectable={selectable}
					onCellDblClick={handleCellDblClick}
					onCellClick={(record, field) => {
						if (field === "edit") edit(record)
						if (field === "remove") remove(record)
					}}
				/>
			</div>

			{picker ? (
				<AddressReferencePicker
					key={`${picker.field}:${picker.prefix.id}`}
					kind={picker.field === "geo" ? "geography" : "operator"}
					value={
						picker.field === "geo"
							? picker.prefix.geo_leaf_id
								? [picker.prefix.geo_leaf_id]
								: []
							: picker.prefix.operator_id
								? [picker.prefix.operator_id]
								: []
					}
					onChange={applyReference}
					autoOpen
					onClose={() => setPicker(null)}
					placeholder={picker.field === "geo" ? t`Choose geography` : t`Choose operator`}
				/>
			) : null}
		</div>
	)
})

function denseCellStyle() {
	return { padding: [8, 10, 8, 10], textBaseline: "middle", autoWrapText: false }
}

function readCorrectionDraft() {
	if (typeof window === "undefined") return { search: "", ip: "", open: false, evidence: "" }
	const query = new URLSearchParams(window.location.search)
	const ip = (query.get("ip") ?? "").trim()
	const evidence = [
		query.get("from") && `from=${query.get("from")}`,
		query.get("to") && `to=${query.get("to")}`,
		query.get("dimension_snapshot_id") && `snapshot=${query.get("dimension_snapshot_id")}`,
		query.get("geo_version") && `geo=${query.get("geo_version")}`,
		query.get("classification_version") && `classification=${query.get("classification_version")}`,
	]
		.filter(Boolean)
		.join(" · ")
	return {
		search: (query.get("q") ?? ip).trim(),
		ip,
		open: query.get("draft") === "1" && ip !== "",
		evidence,
	}
}

const IMPORT_SLOTS = ["combined", "geo", "asn"] as const

// Read-only browse of the imported base prefixes backing the active import slots
// (address_base_prefixes — the large, immutable per-import table). Resolves the
// slot's active import and reuses the Imports tab's ImportedPrefixBrowser so the
// library and the imported data live under one Prefixes tab instead of two pages.
const AddressImportedPrefixes = memo(function AddressImportedPrefixes({
	onBackToLibrary,
}: {
	onBackToLibrary: () => void
}) {
	const { t } = useLingui()
	const [slots, setSlots] = useState<Record<string, AddressImportSlot | null>>({})
	const [slot, setSlot] = useState("combined")
	const [activeImport, setActiveImport] = useState<AddressImport | null>(null)
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")

	// Load every slot once so the selector can default to a populated slot.
	useEffect(() => {
		let cancelled = false
		const loadSlots = async () => {
			const next: Record<string, AddressImportSlot | null> = {}
			for (const name of IMPORT_SLOTS) {
				try {
					const resolved = await api.send<AddressImportSlot>(`/api/v1/address-import-slots/${name}`, {})
					next[name] = resolved.import_id ? resolved : null
				} catch {
					next[name] = null
				}
			}
			if (cancelled) return
			setSlots(next)
			const firstActive = IMPORT_SLOTS.find((name) => next[name]?.import_id)
			if (firstActive) setSlot(firstActive)
		}
		loadSlots()
		return () => {
			cancelled = true
		}
	}, [])

	// Resolve the chosen slot's active import (needed for the browser header + id).
	useEffect(() => {
		let cancelled = false
		const importID = slots[slot]?.import_id
		if (!importID) {
			setActiveImport(null)
			setLoading(false)
			return
		}
		setLoading(true)
		setError("")
		const loadImport = async () => {
			try {
				const imp = await api.send<AddressImport>(`/api/v1/address-imports/${importID}`, {})
				if (!cancelled) setActiveImport(imp)
			} catch (err) {
				if (!cancelled) {
					setActiveImport(null)
					setError(err instanceof Error ? err.message : t`Failed to load import`)
				}
			} finally {
				if (!cancelled) setLoading(false)
			}
		}
		loadImport()
		return () => {
			cancelled = true
		}
	}, [slot, slots, t])

	return (
		<div className="grid gap-3">
			<div className="flex flex-wrap items-center gap-2">
				<Label className="text-sm text-muted-foreground">
					<Trans>Import slot</Trans>
				</Label>
				<Select value={slot} onValueChange={setSlot}>
					<SelectTrigger className="w-40">
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						{IMPORT_SLOTS.map((name) => (
							<SelectItem key={name} value={name}>
								{slotLabel(name)}
							</SelectItem>
						))}
					</SelectContent>
				</Select>
			</div>
			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			{loading ? (
				<div className="rounded-md border border-border bg-card p-6 text-center text-sm text-muted-foreground">
					<Trans>Loading…</Trans>
				</div>
			) : activeImport ? (
				<ImportedPrefixBrowser item={activeImport} onClose={onBackToLibrary} />
			) : (
				<div className="rounded-md border border-border bg-card p-6 text-center text-sm text-muted-foreground">
					<Trans>No active import in this slot. Upload and activate one in the Imports tab.</Trans>
				</div>
			)}
		</div>
	)
})

function slotLabel(slot: string) {
	if (slot === "geo") return "Geo"
	if (slot === "asn") return "ASN"
	return "Combined"
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

// Effective view: active base import overlaid with editable corrections. Select
// rows → bulk reassign operator/ASN → lands as a correction (source=correction).
const EffectiveWorkbench = memo(function EffectiveWorkbench() {
	const { t } = useLingui()
	const [family, setFamily] = useState("")
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(200)
	const [country, setCountry] = useState("")
	const [operator, setOperator] = useState("")
	const [asn, setASN] = useState("")
	const [line, setLine] = useState("")
	const [search, setSearch] = useState("")
	const [debounced, setDebounced] = useState("")
	const [rows, setRows] = useState<EffectivePrefix[]>([])
	const [total, setTotal] = useState(0)
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const [notice, setNotice] = useState("")
	const [selectedCidrs, setSelectedCidrs] = useState<string[]>([])
	const [operators, setOperators] = useState<{ id: string; name: string }[]>([])
	const [groups, setGroups] = useState<{ id: string; name: string }[]>([])
	const [reassignOpen, setReassignOpen] = useState(false)
	const [reassignOp, setReassignOp] = useState("")
	const [reassignGeo, setReassignGeo] = useState("")
	const [reassignAsn, setReassignAsn] = useState("")
	const [working, setWorking] = useState(false)
	const seq = useRef(0)

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setPage(0)
			setDebounced(search.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [search])

	useEffect(() => {
		api
			.send<{ items?: { id: string; name: string }[] }>("/api/v1/network/operators", { query: { limit: 500 } })
			.then((data) => setOperators(data.items ?? []))
			.catch(() => setOperators([]))
		api
			.send<{ items?: { id: string; name: string }[] }>("/api/v1/geo/lines", { query: { limit: 500 } })
			.then((data) => setGroups(data.items ?? []))
			.catch(() => setGroups([]))
	}, [])

	const fetchPage = useCallback(async () => {
		const sequence = ++seq.current
		setLoading(true)
		setError("")
		try {
			// A selected region group filters by its effective set (?line=), which the
			// backend resolves from the group's selector/members/exclude — the column
			// filters do not apply in that mode.
			const query = line
				? { slot: "combined", line, family: family || undefined, limit: pageSize, offset: page * pageSize || undefined }
				: {
						slot: "combined",
						family: family || undefined,
						country_code: country.trim() || undefined,
						operator: operator.trim() || undefined,
						asn: asn.trim() || undefined,
						q: debounced || undefined,
						limit: pageSize,
						offset: page * pageSize || undefined,
						sort: "cidr",
						order: "asc",
					}
			const data = await api.send<{ items?: EffectivePrefix[]; total?: number }>("/api/v1/address-prefixes/effective", {
				query,
			})
			if (sequence !== seq.current) return
			setRows(data.items ?? [])
			setTotal(data.total ?? 0)
		} catch (err) {
			if (sequence !== seq.current) return
			setRows([])
			setTotal(0)
			setError(err instanceof Error ? err.message : t`Failed to load`)
		} finally {
			if (sequence === seq.current) setLoading(false)
		}
	}, [asn, country, debounced, family, line, operator, page, pageSize, t])

	useEffect(() => {
		fetchPage()
	}, [fetchPage])

	const records = useMemo(
		() =>
			rows.map((row) => ({
				cidr: row.cidr,
				family: `IPv${row.family}`,
				country: [row.country_code, row.country_name].filter(Boolean).join(" · ") || "—",
				region: [row.subdivision_name, row.city_name].filter(Boolean).join(" / ") || "—",
				asn: row.asn ?? "—",
				operator: row.operator_name || "—",
				source: row.source === "correction" ? t`Correction` : t`Base`,
			})),
		[rows, t]
	)
	const columns = useMemo(
		() => [
			{ field: "cidr", title: t`CIDR`, width: 180, filter: false, style: denseCellStyle() },
			{ field: "family", title: t`Family`, width: 80, filter: false, style: denseCellStyle() },
			{ field: "country", title: t`Country`, width: 160, filter: false, style: denseCellStyle() },
			{ field: "region", title: t`Province / City`, width: 200, filter: false, style: denseCellStyle() },
			{ field: "asn", title: "ASN", width: 100, filter: false, style: denseCellStyle() },
			{ field: "operator", title: t`Operator`, width: 150, style: denseCellStyle() },
			{ field: "source", title: t`Source`, width: 100, filter: false, style: denseCellStyle() },
		],
		[t]
	)
	const selectable = useMemo(
		() => ({
			onSelectionChange: (recs: Record<string, unknown>[]) => setSelectedCidrs(recs.map((r) => String(r.cidr))),
		}),
		[]
	)
	const resetPage = (update: () => void) => {
		setPage(0)
		update()
	}

	// Inline edit: double-click asn/operator, commit lands as a single-CIDR
	// correction (same bulk-reassign endpoint) then the page refetches.
	const applyInlineEdit = useCallback(
		async (record: Record<string, unknown>, field: string, value: string) => {
			const cidr = String(record.cidr)
			const trimmed = value.trim()
			const body: { cidrs: string[]; operator_id?: string; asn?: number } = { cidrs: [cidr] }
			if (field === "asn") {
				if (trimmed === "" || trimmed === "—") return
				const num = Number(trimmed)
				if (!Number.isInteger(num) || num < 0) {
					setError(t`ASN must be a non-negative integer`)
					return
				}
				body.asn = num
			} else if (field === "operator") {
				if (trimmed === "" || trimmed === "—") return
				const match = operators.find((op) => op.name.toLocaleLowerCase() === trimmed.toLocaleLowerCase())
				if (!match) {
					setError(t`Unknown operator "${trimmed}" — type an exact operator name`)
					return
				}
				body.operator_id = match.id
			} else {
				return
			}
			setError("")
			setNotice("")
			try {
				await api.send("/api/v1/address-prefixes/bulk-reassign", { method: "POST", body })
				setNotice(t`Updated ${cidr}`)
				await fetchPage()
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Update failed`)
			}
		},
		[operators, fetchPage, t]
	)
	const editable = useMemo(() => ({ fields: ["asn", "operator"], onEdit: applyInlineEdit }), [applyInlineEdit])
	// Server-side header filter for Operator (17 options, single-select) — drives
	// the same query state as the top-bar inputs so it filters the whole dataset.
	const serverFiltering = useMemo(
		() => ({
			options: { operator: operators.map((op) => ({ value: op.name, label: op.name })) },
			selected: { operator: operator ? [operator] : [] },
			selection: { operator: "single" as const },
			onColumnFilterChange: (field: string, values: unknown[]) => {
				if (field !== "operator") return
				setPage(0)
				setOperator(values.length ? String(values[0]) : "")
			},
			onClearAll: () => {
				setPage(0)
				setOperator("")
			},
		}),
		[operators, operator]
	)

	const applyReassign = async () => {
		if (!selectedCidrs.length) return
		setWorking(true)
		setError("")
		setNotice("")
		try {
			await api.send("/api/v1/address-prefixes/bulk-reassign", {
				method: "POST",
				body: {
					cidrs: selectedCidrs,
					operator_id: reassignOp || undefined,
					geo_leaf_id: reassignGeo || undefined,
					asn: reassignAsn.trim() ? Number(reassignAsn) : undefined,
				},
			})
			setNotice(t`Reassigned ${selectedCidrs.length} prefixes`)
			setReassignOpen(false)
			setReassignOp("")
			setReassignGeo("")
			setReassignAsn("")
			setSelectedCidrs([])
			await fetchPage()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Reassign failed`)
		} finally {
			setWorking(false)
		}
	}

	return (
		<div className="grid gap-3">
			<div className="flex flex-wrap items-center gap-3">
				<div className="inline-flex w-fit rounded-md border border-border p-0.5">
					{(
						[
							["", t`All`],
							["4", "IPv4"],
							["6", "IPv6"],
						] as const
					).map(([value, label]) => (
						<Button
							key={value}
							variant={family === value ? "default" : "ghost"}
							size="sm"
							onClick={() => resetPage(() => setFamily(value))}
						>
							{label}
						</Button>
					))}
				</div>
				<Select
					value={line || "__none__"}
					onValueChange={(value) => resetPage(() => setLine(value === "__none__" ? "" : value))}
				>
					<SelectTrigger className="w-48">
						<SelectValue placeholder={t`Region group`} />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="__none__">{t`All (no group)`}</SelectItem>
						{groups.map((group) => (
							<SelectItem key={group.id} value={group.id}>
								{group.name}
							</SelectItem>
						))}
					</SelectContent>
				</Select>
				<Input
					className="w-28"
					value={country}
					onChange={(event) => resetPage(() => setCountry(event.target.value))}
					placeholder={t`Country`}
					disabled={!!line}
				/>
				<Input
					className="w-28"
					value={asn}
					onChange={(event) => resetPage(() => setASN(event.target.value))}
					placeholder="ASN"
					inputMode="numeric"
					disabled={!!line}
				/>
				{selectedCidrs.length > 0 ? (
					<div className="flex items-center gap-2 text-sm">
						<span className="text-muted-foreground">
							<Trans>Selected {selectedCidrs.length}</Trans>
						</span>
						<Button size="sm" onClick={() => setReassignOpen((value) => !value)}>
							<Trans>Reassign</Trans>
						</Button>
						<Button variant="ghost" size="sm" onClick={() => setSelectedCidrs([])}>
							<Trans>Clear</Trans>
						</Button>
					</div>
				) : null}
			</div>
			{reassignOpen && selectedCidrs.length > 0 ? (
				<div className="flex flex-wrap items-end gap-2 rounded-md border border-border bg-card p-3">
					<div className="grid gap-1">
						<Label className="text-xs text-muted-foreground">
							<Trans>Operator</Trans>
						</Label>
						<Select value={reassignOp} onValueChange={setReassignOp}>
							<SelectTrigger className="w-44">
								<SelectValue placeholder={t`Keep current`} />
							</SelectTrigger>
							<SelectContent>
								{operators.map((op) => (
									<SelectItem key={op.id} value={op.id}>
										{op.name}
									</SelectItem>
								))}
							</SelectContent>
						</Select>
					</div>
					<div className="grid gap-1">
						<Label className="text-xs text-muted-foreground">
							<Trans>Geography</Trans>
						</Label>
						<div className="w-52">
							<AddressReferencePicker
								kind="geography"
								value={reassignGeo ? [reassignGeo] : []}
								onChange={(ids) => setReassignGeo(ids[0] ?? "")}
								placeholder={t`Keep current`}
							/>
						</div>
					</div>
					<div className="grid gap-1">
						<Label className="text-xs text-muted-foreground">ASN</Label>
						<Input
							className="w-28"
							value={reassignAsn}
							onChange={(event) => setReassignAsn(event.target.value)}
							placeholder={t`Keep`}
							inputMode="numeric"
						/>
					</div>
					<Button onClick={applyReassign} disabled={working || (!reassignOp && !reassignGeo && !reassignAsn.trim())}>
						<Trans>Apply to {selectedCidrs.length}</Trans>
					</Button>
				</div>
			) : null}
			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			{notice ? <div className="rounded-md border border-green-500/30 p-3 text-sm text-green-700">{notice}</div> : null}
			<PagedVTable
				records={records}
				columns={columns}
				loading={loading}
				emptyText={t`No prefixes found. Activate an import first.`}
				searchValue={search}
				onSearchChange={setSearch}
				searchPlaceholder={t`Search CIDR or location...`}
				height={Math.min(560, 44 + records.length * 42)}
				serverPagination={{
					page,
					pageSize,
					totalCount: total,
					onPageChange: setPage,
					onPageSizeChange: (value) => resetPage(() => setPageSize(value)),
				}}
				selectable={selectable}
				editable={editable}
				serverFiltering={serverFiltering}
			/>
		</div>
	)
})

export default memo(function AddressPrefixesTab() {
	const [mode, setMode] = useState<"effective" | "library" | "imported">("effective")
	return (
		<div className="grid gap-3">
			<div className="inline-flex w-fit rounded-md border border-border p-0.5">
				<Button variant={mode === "effective" ? "default" : "ghost"} size="sm" onClick={() => setMode("effective")}>
					<Trans>Workbench</Trans>
				</Button>
				<Button variant={mode === "library" ? "default" : "ghost"} size="sm" onClick={() => setMode("library")}>
					<Trans>Library</Trans>
				</Button>
				<Button variant={mode === "imported" ? "default" : "ghost"} size="sm" onClick={() => setMode("imported")}>
					<Trans>Imported</Trans>
				</Button>
			</div>
			{mode === "effective" ? (
				<EffectiveWorkbench />
			) : mode === "imported" ? (
				<AddressImportedPrefixes onBackToLibrary={() => setMode("library")} />
			) : (
				<AddressPrefixes />
			)}
		</div>
	)
})

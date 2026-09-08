import { Trans, useLingui } from "@lingui/react/macro"
import { GlobeIcon, PencilIcon, PlusIcon, RefreshCwIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { AddressReferencePicker } from "@/components/address-reference-picker"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { parsePrefixLabels } from "@/lib/address-set-form"
import { pb } from "@/lib/api"

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
}

export default memo(function AddressPrefixes() {
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
	const [form, setForm] = useState({ ...emptyForm, cidr: correctionDraft.ip, labels: correctionDraft.open ? "evidence=flow_report" : "" })
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
			const data = await pb.send<AddressPrefixList>("/api/v1/address-prefixes", {
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
			const asn = form.asn.trim() ? Number(form.asn) : undefined
			if (asn !== undefined && (!Number.isInteger(asn) || asn <= 0 || asn > 4_294_967_295)) {
				throw new Error(t`ASN must be an integer between 1 and 4294967295`)
			}
			await pb.send(form.id ? `/api/v1/address-prefixes/${form.id}` : "/api/v1/address-prefixes", {
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
				.map(([key, value]) => `${key}=${value}`)
				.join(","),
			source: prefix.source,
			asn: prefix.asn ? String(prefix.asn) : "",
			geoLeafID: prefix.geo_leaf_id ?? "",
			operatorID: prefix.operator_id ?? "",
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
				await pb.send(`/api/v1/address-prefixes/${id}`, {
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
				const updated = await pb.send<AddressPrefix>(`/api/v1/address-prefixes/${prefix.id}`, {
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
				const updated = await pb.send<AddressPrefix>(`/api/v1/address-prefixes/${prefix.id}`, {
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
				await pb.send(`/api/v1/address-prefixes/${prefix.id}`, {
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
			const preview = await pb.send<AddressOperationPreview>("/api/v1/address-sets/actions/preview", {
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
				await pb.send("/api/v1/address-prefixes", {
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
			const preview = await pb.send<AddressPrefixMergePreview>("/api/v1/address-prefixes/actions/merge-preview", {
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
					await pb.send("/api/v1/address-prefixes", {
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
						await pb.send(`/api/v1/address-prefixes/${prefix.id}`, {
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
			{ field: "cidr", title: t`CIDR`, width: 190, style: denseCellStyle() },
			{ field: "family", title: t`Family`, width: 90, style: denseCellStyle() },
			{ field: "labels", title: t`Labels`, width: 300, style: denseCellStyle() },
			{ field: "geo", title: t`Geography`, width: 160, style: denseCellStyle() },
			{ field: "operator", title: t`Operator`, width: 160, style: denseCellStyle() },
			{ field: "asn", title: "ASN", width: 100, style: denseCellStyle() },
			{ field: "source", title: t`Source`, width: 110, style: denseCellStyle() },
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
	].filter(Boolean).join(" · ")
	return {
		search: (query.get("q") ?? ip).trim(),
		ip,
		open: query.get("draft") === "1" && ip !== "",
		evidence,
	}
}

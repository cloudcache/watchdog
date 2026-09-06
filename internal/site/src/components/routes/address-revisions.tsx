import { Trans, useLingui } from "@lingui/react/macro"
import { EyeIcon, RefreshCwIcon, ReplaceIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { AddressReferencePicker } from "@/components/address-reference-picker"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Textarea } from "@/components/ui/textarea"
import { buildAddressPrefixRevisionOperations, type AddressPrefixRevisionForm } from "@/lib/address-revision-form"
import { pb } from "@/lib/api"

type AddressPrefix = {
	id: string
	cidr: string
	family: number
	labels: Record<string, string>
	geo_leaf_id?: string
	operator_id?: string
	asn?: number
	source: string
	row_version: number
}

type RevisionChange = { index: number; action: string; prefix_id: string; cidr: string }
type AddressDraftRevision = {
	id: string
	scope: string
	base_digest: string
	request_digest: string
	result_digest?: string
	operation_count: number
	status: string
	row_version: number
	created_at: string
	expires_at: string
	applied_at?: string
	preview: {
		create_count: number
		delete_count: number
		before_prefix_count: number
		after_prefix_count: number
		expected_result_digest: string
		changes: RevisionChange[]
	}
}

type PrefixList = { items?: AddressPrefix[]; total?: number }
type RevisionList = { items?: AddressDraftRevision[]; total?: number }
type RevisionChangeList = { items?: RevisionChange[]; total?: number }

const emptyBatch: AddressPrefixRevisionForm = {
	cidrs: "",
	labels: "",
	source: "manual",
	asn: "",
	geoLeafID: "",
	operatorID: "",
}

export default memo(function AddressRevisions() {
	const { t } = useLingui()
	const [prefixes, setPrefixes] = useState<AddressPrefix[]>([])
	const [prefixTotal, setPrefixTotal] = useState(0)
	const [prefixPage, setPrefixPage] = useState(0)
	const [prefixPageSize, setPrefixPageSize] = useState(25)
	const [prefixSort, setPrefixSort] = useState("cidr:asc")
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [family, setFamily] = useState("")
	const [selected, setSelected] = useState<Record<string, AddressPrefix>>({})
	const [form, setForm] = useState(emptyBatch)
	const [revision, setRevision] = useState<AddressDraftRevision | null>(null)
	const [revisionETag, setRevisionETag] = useState("")
	const [revisions, setRevisions] = useState<AddressDraftRevision[]>([])
	const [revisionTotal, setRevisionTotal] = useState(0)
	const [revisionPage, setRevisionPage] = useState(0)
	const [revisionPageSize, setRevisionPageSize] = useState(25)
	const [revisionSearch, setRevisionSearch] = useState("")
	const [debouncedRevisionSearch, setDebouncedRevisionSearch] = useState("")
	const [revisionSort, setRevisionSort] = useState("created:desc")
	const [status, setStatus] = useState("")
	const [changes, setChanges] = useState<RevisionChange[]>([])
	const [changeTotal, setChangeTotal] = useState(0)
	const [changePage, setChangePage] = useState(0)
	const [changePageSize, setChangePageSize] = useState(25)
	const [changeSearch, setChangeSearch] = useState("")
	const [debouncedChangeSearch, setDebouncedChangeSearch] = useState("")
	const [changeAction, setChangeAction] = useState("")
	const [changeSort, setChangeSort] = useState("index:asc")
	const [loadingChanges, setLoadingChanges] = useState(false)
	const [loadingPrefixes, setLoadingPrefixes] = useState(true)
	const [loadingRevisions, setLoadingRevisions] = useState(true)
	const [working, setWorking] = useState(false)
	const [error, setError] = useState("")
	const [notice, setNotice] = useState("")
	const prefixRequestSequence = useRef(0)
	const revisionRequestSequence = useRef(0)
	const changeRequestSequence = useRef(0)

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setPrefixPage(0)
			setDebouncedSearch(search.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [search])
	useEffect(() => {
		const timer = window.setTimeout(() => {
			setRevisionPage(0)
			setDebouncedRevisionSearch(revisionSearch.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [revisionSearch])
	useEffect(() => {
		const timer = window.setTimeout(() => {
			setChangePage(0)
			setDebouncedChangeSearch(changeSearch.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [changeSearch])

	const fetchPrefixes = useCallback(async () => {
		const sequence = ++prefixRequestSequence.current
		const [sort, order] = prefixSort.split(":")
		setLoadingPrefixes(true)
		setError("")
		try {
			const data = await pb.send<PrefixList>("/api/v1/address-prefixes", {
				query: {
					q: debouncedSearch || undefined,
					family: family || undefined,
					limit: prefixPageSize,
					offset: prefixPage * prefixPageSize || undefined,
					sort,
					order,
				},
			})
			if (sequence !== prefixRequestSequence.current) return
			setPrefixes(data.items ?? [])
			setPrefixTotal(data.total ?? 0)
		} catch (err) {
			if (sequence !== prefixRequestSequence.current) return
			setPrefixes([])
			setPrefixTotal(0)
			setError(err instanceof Error ? err.message : t`Failed to load prefixes`)
		} finally {
			if (sequence === prefixRequestSequence.current) setLoadingPrefixes(false)
		}
	}, [debouncedSearch, family, prefixPage, prefixPageSize, prefixSort, t])

	const fetchRevisions = useCallback(async () => {
		const sequence = ++revisionRequestSequence.current
		const [sort, order] = revisionSort.split(":")
		setLoadingRevisions(true)
		setError("")
		try {
			const data = await pb.send<RevisionList>("/api/v1/address-draft-revisions", {
				query: {
					q: debouncedRevisionSearch || undefined,
					status: status || undefined,
					limit: revisionPageSize,
					offset: revisionPage * revisionPageSize || undefined,
					sort,
					order,
				},
			})
			if (sequence !== revisionRequestSequence.current) return
			setRevisions(data.items ?? [])
			setRevisionTotal(data.total ?? 0)
		} catch (err) {
			if (sequence !== revisionRequestSequence.current) return
			setRevisions([])
			setRevisionTotal(0)
			setError(err instanceof Error ? err.message : t`Failed to load revisions`)
		} finally {
			if (sequence === revisionRequestSequence.current) setLoadingRevisions(false)
		}
	}, [debouncedRevisionSearch, revisionPage, revisionPageSize, revisionSort, status, t])

	const fetchChanges = useCallback(async () => {
		if (!revision?.id) {
			setChanges([])
			setChangeTotal(0)
			return
		}
		const sequence = ++changeRequestSequence.current
		const [sort, order] = changeSort.split(":")
		setLoadingChanges(true)
		setError("")
		try {
			const data = await pb.send<RevisionChangeList>(`/api/v1/address-draft-revisions/${revision.id}/changes`, {
				query: {
					q: debouncedChangeSearch || undefined,
					action: changeAction || undefined,
					limit: changePageSize,
					offset: changePage * changePageSize || undefined,
					sort,
					order,
				},
			})
			if (sequence !== changeRequestSequence.current) return
			setChanges(data.items ?? [])
			setChangeTotal(data.total ?? 0)
		} catch (err) {
			if (sequence !== changeRequestSequence.current) return
			setChanges([])
			setChangeTotal(0)
			setError(err instanceof Error ? err.message : t`Failed to load revision changes`)
		} finally {
			if (sequence === changeRequestSequence.current) setLoadingChanges(false)
		}
	}, [changeAction, changePage, changePageSize, changeSort, debouncedChangeSearch, revision?.id, t])

	useEffect(() => {
		fetchPrefixes()
	}, [fetchPrefixes])
	useEffect(() => {
		fetchRevisions()
	}, [fetchRevisions])
	useEffect(() => {
		fetchChanges()
	}, [fetchChanges])

	const invalidatePreview = () => {
		setRevision(null)
		setRevisionETag("")
		setNotice("")
	}
	const updateForm = (patch: Partial<AddressPrefixRevisionForm>) => {
		setForm((current) => ({ ...current, ...patch }))
		invalidatePreview()
	}
	const togglePrefix = useCallback((record: Record<string, unknown>) => {
		const prefix = record.item as AddressPrefix | undefined
		if (!prefix) return
		setSelected((current) => {
			const next = { ...current }
			if (next[prefix.id]) delete next[prefix.id]
			else next[prefix.id] = prefix
			return next
		})
		setRevision(null)
		setRevisionETag("")
		setNotice("")
	}, [])

	const previewBatch = async () => {
		setWorking(true)
		setError("")
		setNotice("")
		try {
			const operations = buildAddressPrefixRevisionOperations(Object.values(selected), form)
			if (!operations.length) throw new Error(t`Select old prefixes or enter new prefixes first`)
			let etag = ""
			const item = await pb.send<AddressDraftRevision>("/api/v1/address-draft-revisions/preview", {
				method: "POST",
				body: { operations },
				onResponse: (response) => {
					etag = response.headers.get("ETag") ?? ""
				},
			})
			setChangePage(0)
			setChangeSearch("")
			setChangeAction("")
			setChangeSort("index:asc")
			setRevision(item)
			setRevisionETag(etag || `"${item.row_version}"`)
		} catch (err) {
			setRevision(null)
			setRevisionETag("")
			setError(err instanceof Error ? err.message : t`Preview failed`)
		} finally {
			setWorking(false)
		}
	}

	const applyBatch = async () => {
		if (
			!revision ||
			revision.status !== "prepared" ||
			!revisionETag ||
			!confirm(t`Apply this address replacement atomically?`)
		)
			return
		setWorking(true)
		setError("")
		try {
			let etag = ""
			const applied = await pb.send<AddressDraftRevision>(`/api/v1/address-draft-revisions/${revision.id}/apply`, {
				method: "POST",
				headers: { "If-Match": revisionETag },
				onResponse: (response) => {
					etag = response.headers.get("ETag") ?? ""
				},
			})
			setRevision(applied)
			setRevisionETag(etag || `"${applied.row_version}"`)
			setSelected({})
			setForm(emptyBatch)
			setNotice(t`Address replacement applied`)
			await Promise.all([fetchPrefixes(), fetchRevisions()])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Apply failed`)
		} finally {
			setWorking(false)
		}
	}

	const viewRevision = useCallback(
		async (record: Record<string, unknown>) => {
			const id = String(record.id ?? "")
			if (!id) return
			setWorking(true)
			setError("")
			try {
				let etag = ""
				const item = await pb.send<AddressDraftRevision>(`/api/v1/address-draft-revisions/${id}`, {
					onResponse: (response) => {
						etag = response.headers.get("ETag") ?? ""
					},
				})
				setChangePage(0)
				setChangeSearch("")
				setChangeAction("")
				setChangeSort("index:asc")
				setRevision(item)
				setRevisionETag(etag || `"${item.row_version}"`)
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to load revision`)
			} finally {
				setWorking(false)
			}
		},
		[t]
	)

	const prefixRecords = useMemo(
		() =>
			prefixes.map((prefix) => ({
				id: prefix.id,
				selected: selected[prefix.id] ? t`Selected` : t`Select`,
				cidr: prefix.cidr,
				family: `IPv${prefix.family}`,
				labels:
					Object.entries(prefix.labels ?? {})
						.map(([key, value]) => `${key}=${value}`)
						.join(", ") || "—",
				geo: prefix.geo_leaf_id || "—",
				operator: prefix.operator_id || "—",
				asn: prefix.asn ?? "—",
				source: prefix.source,
				item: prefix,
			})),
		[prefixes, selected, t]
	)
	const prefixColumns = useMemo(
		() => [
			{
				field: "selected",
				title: t`Batch`,
				width: 100,
				filter: false,
				style: { ...denseCellStyle(), color: "#2563eb", cursor: "pointer" },
			},
			{ field: "cidr", title: t`CIDR`, width: 190, style: denseCellStyle() },
			{ field: "family", title: t`Family`, width: 90, style: denseCellStyle() },
			{ field: "labels", title: t`Labels`, width: 300, style: denseCellStyle() },
			{ field: "geo", title: t`Geography`, width: 150, style: denseCellStyle() },
			{ field: "operator", title: t`Operator`, width: 150, style: denseCellStyle() },
			{ field: "asn", title: "ASN", width: 100, style: denseCellStyle() },
			{ field: "source", title: t`Source`, width: 110, style: denseCellStyle() },
		],
		[t]
	)
	const revisionRecords = useMemo(
		() =>
			revisions.map((item) => ({
				id: item.id,
				view: t`View`,
				status: item.status,
				operations: item.operation_count,
				creates: item.preview.create_count,
				deletes: item.preview.delete_count,
				before: item.preview.before_prefix_count,
				after: item.preview.after_prefix_count,
				created: formatDate(item.created_at),
				expires: formatDate(item.expires_at),
			})),
		[revisions, t]
	)
	const revisionColumns = useMemo(
		() => [
			{
				field: "view",
				title: t`View`,
				width: 80,
				filter: false,
				style: { ...denseCellStyle(), color: "#2563eb", cursor: "pointer" },
			},
			{ field: "status", title: t`Status`, width: 110, style: denseCellStyle() },
			{ field: "operations", title: t`Operations`, width: 110, style: denseCellStyle() },
			{ field: "creates", title: t`Creates`, width: 100, style: denseCellStyle() },
			{ field: "deletes", title: t`Deletes`, width: 100, style: denseCellStyle() },
			{ field: "before", title: t`Before`, width: 100, style: denseCellStyle() },
			{ field: "after", title: t`After`, width: 100, style: denseCellStyle() },
			{ field: "created", title: t`Created`, width: 190, style: denseCellStyle() },
			{ field: "expires", title: t`Expires`, width: 190, style: denseCellStyle() },
		],
		[t]
	)
	const changeRecords = useMemo(
		() =>
			changes.map((change) => ({
				index: change.index,
				action: change.action,
				cidr: change.cidr,
				prefixID: change.prefix_id,
			})),
		[changes]
	)
	const changeColumns = useMemo(
		() => [
			{ field: "index", title: "#", width: 70, style: denseCellStyle() },
			{ field: "action", title: t`Action`, width: 100, style: denseCellStyle() },
			{ field: "cidr", title: t`CIDR`, width: 220, style: denseCellStyle() },
			{ field: "prefixID", title: t`Prefix ID`, width: 320, style: denseCellStyle() },
		],
		[t]
	)
	const resetPrefixPage = (update: () => void) => {
		setPrefixPage(0)
		update()
	}
	const prefixFiltering = useMemo(
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
				resetPrefixPage(() => setFamily(values.length > 0 ? String(values[0]) : "")),
			onClearAll: () => resetPrefixPage(() => setFamily("")),
		}),
		[family]
	)
	const [prefixSortField, prefixSortDirection] = prefixSort.split(":") as [string, "asc" | "desc"]
	const prefixSorting = useMemo(
		() => ({
			field: prefixSortField,
			direction: prefixSortDirection,
			fields: { cidr: "cidr", family: "family", asn: "asn", source: "source" },
			onSortChange: (field: string, direction: "asc" | "desc") =>
				resetPrefixPage(() => setPrefixSort(`${field}:${direction}`)),
		}),
		[prefixSortDirection, prefixSortField]
	)
	const resetRevisionPage = (update: () => void) => {
		setRevisionPage(0)
		update()
	}
	const revisionFiltering = useMemo(
		() => ({
			options: {
				status: ["prepared", "applied", "superseded", "cancelled"].map((value) => ({ value, label: value })),
			},
			selected: { status: status ? [status] : [] },
			selection: { status: "single" as const },
			onColumnFilterChange: (_field: string, values: unknown[]) =>
				resetRevisionPage(() => setStatus(values.length > 0 ? String(values[0]) : "")),
			onClearAll: () => resetRevisionPage(() => setStatus("")),
		}),
		[status]
	)
	const [revisionSortField, revisionSortDirection] = revisionSort.split(":") as [string, "asc" | "desc"]
	const revisionSorting = useMemo(
		() => ({
			field: revisionSortField,
			direction: revisionSortDirection,
			fields: {
				status: "status",
				operations: "operations",
				creates: "creates",
				deletes: "deletes",
				before: "before",
				after: "after",
				created: "created",
				expires: "expires",
			},
			onSortChange: (field: string, direction: "asc" | "desc") =>
				resetRevisionPage(() => setRevisionSort(`${field}:${direction}`)),
		}),
		[revisionSortDirection, revisionSortField]
	)
	const resetChangePage = (update: () => void) => {
		setChangePage(0)
		update()
	}
	const changeFiltering = useMemo(
		() => ({
			options: {
				action: [
					{ value: "create", label: "create" },
					{ value: "delete", label: "delete" },
				],
			},
			selected: { action: changeAction ? [changeAction] : [] },
			selection: { action: "single" as const },
			onColumnFilterChange: (_field: string, values: unknown[]) =>
				resetChangePage(() => setChangeAction(values.length > 0 ? String(values[0]) : "")),
			onClearAll: () => resetChangePage(() => setChangeAction("")),
		}),
		[changeAction]
	)
	const [changeSortField, changeSortDirection] = changeSort.split(":") as [string, "asc" | "desc"]
	const changeSorting = useMemo(
		() => ({
			field: changeSortField,
			direction: changeSortDirection,
			fields: { index: "index", action: "action", cidr: "cidr", prefixID: "prefix_id" },
			onSortChange: (field: string, direction: "asc" | "desc") =>
				resetChangePage(() => setChangeSort(`${field}:${direction}`)),
		}),
		[changeSortDirection, changeSortField]
	)

	return (
		<div className="grid gap-5">
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<ReplaceIcon className="h-5 w-5 text-muted-foreground" />
					<h2 className="text-lg font-semibold">
						<Trans>Atomic Prefix Replacement</Trans>
					</h2>
				</div>
				<Button variant="outline" size="sm" onClick={() => Promise.all([fetchPrefixes(), fetchRevisions()])}>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Refresh</Trans>
				</Button>
			</div>

			<div className="grid gap-3 rounded-md border border-border bg-card p-4">
				<div className="text-sm text-muted-foreground">
					<Trans>
						Select the existing prefixes to remove, enter their replacement set, then preview. Nothing changes until you
						apply the immutable revision.
					</Trans>
				</div>
				<div className="grid gap-4 lg:grid-cols-2">
					<div className="grid gap-2">
						<Label>
							<Trans>Replacement CIDR, IP, or range</Trans>
						</Label>
						<Textarea
							rows={8}
							className="font-mono text-xs"
							value={form.cidrs}
							onChange={(event) => updateForm({ cidrs: event.target.value })}
							placeholder={"10.0.0.0/24\n192.0.2.1-192.0.2.20\n2001:db8::/48"}
						/>
					</div>
					<div className="grid content-start gap-3 sm:grid-cols-2">
						<FormInput label={t`Labels`} value={form.labels} onChange={(value) => updateForm({ labels: value })} />
						<FormInput label={t`Source`} value={form.source} onChange={(value) => updateForm({ source: value })} />
						<FormInput label="ASN" value={form.asn} onChange={(value) => updateForm({ asn: value })} />
						<div className="grid gap-2">
							<Label>
								<Trans>Geography</Trans>
							</Label>
							<AddressReferencePicker
								kind="geography"
								value={form.geoLeafID ? [form.geoLeafID] : []}
								onChange={(value) => updateForm({ geoLeafID: value[0] ?? "" })}
								placeholder={t`Choose geography`}
							/>
						</div>
						<div className="grid gap-2">
							<Label>
								<Trans>Operator</Trans>
							</Label>
							<AddressReferencePicker
								kind="operator"
								value={form.operatorID ? [form.operatorID] : []}
								onChange={(value) => updateForm({ operatorID: value[0] ?? "" })}
								placeholder={t`Choose operator`}
							/>
						</div>
						<div className="rounded border p-2 text-sm">
							<div className="text-xs text-muted-foreground">
								<Trans>Selected old prefixes</Trans>
							</div>
							<div className="font-medium">{Object.keys(selected).length.toLocaleString()}</div>
						</div>
					</div>
				</div>
				<div className="flex flex-wrap gap-2">
					<Button onClick={previewBatch} disabled={working}>
						<Trans>Preview replacement</Trans>
					</Button>
					<Button onClick={applyBatch} disabled={working || revision?.status !== "prepared" || !revisionETag}>
						<Trans>Apply atomically</Trans>
					</Button>
					<Button
						variant="ghost"
						onClick={() => {
							setSelected({})
							setForm(emptyBatch)
							invalidatePreview()
						}}
					>
						<Trans>Clear</Trans>
					</Button>
				</div>
			</div>

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			{notice ? <div className="rounded-md border border-green-500/30 p-3 text-sm text-green-700">{notice}</div> : null}

			{revision ? (
				<div className="grid gap-3 rounded-md border border-border bg-card p-4">
					<div className="flex flex-wrap items-center justify-between gap-2">
						<div className="flex items-center gap-2 font-medium">
							<EyeIcon className="h-4 w-4" />
							<Trans>Revision Preview</Trans> {revision.id}
						</div>
						<div className="text-sm">{revision.status}</div>
					</div>
					<div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-6">
						<Stat label={t`Creates`} value={revision.preview.create_count.toLocaleString()} />
						<Stat label={t`Deletes`} value={revision.preview.delete_count.toLocaleString()} />
						<Stat label={t`Before`} value={revision.preview.before_prefix_count.toLocaleString()} />
						<Stat label={t`After`} value={revision.preview.after_prefix_count.toLocaleString()} />
						<Stat label={t`Expires`} value={formatDate(revision.expires_at)} />
						<Stat label={t`Result digest`} value={revision.preview.expected_result_digest} />
					</div>
					<PagedVTable
						records={changeRecords}
						columns={changeColumns}
						loading={loadingChanges}
						emptyText={t`No changes in this revision.`}
						searchPlaceholder={t`Search revision changes...`}
						searchValue={changeSearch}
						onSearchChange={setChangeSearch}
						height={320}
						serverPagination={{
							page: changePage,
							pageSize: changePageSize,
							totalCount: changeTotal,
							onPageChange: setChangePage,
							onPageSizeChange: (value) => resetChangePage(() => setChangePageSize(value)),
						}}
						serverFiltering={changeFiltering}
						serverSorting={changeSorting}
					/>
				</div>
			) : null}

			<div className="grid gap-3">
				<h3 className="font-medium">
					<Trans>Select Existing Prefixes</Trans>
				</h3>
				<div className="overflow-hidden rounded-md border border-border bg-card">
					<PagedVTable
						records={prefixRecords}
						columns={prefixColumns}
						loading={loadingPrefixes}
						emptyText={t`No prefixes found.`}
						searchValue={search}
						onSearchChange={setSearch}
						searchPlaceholder={t`Search CIDR or labels...`}
						height={420}
						serverPagination={{
							page: prefixPage,
							pageSize: prefixPageSize,
							totalCount: prefixTotal,
							onPageChange: setPrefixPage,
							onPageSizeChange: (value) => resetPrefixPage(() => setPrefixPageSize(value)),
						}}
						serverFiltering={prefixFiltering}
						serverSorting={prefixSorting}
						onCellClick={(record, field) => {
							if (field === "selected") togglePrefix(record)
						}}
					/>
				</div>
			</div>

			<div className="grid gap-3">
				<div className="flex flex-wrap items-center justify-between gap-2">
					<h3 className="font-medium">
						<Trans>Revision History</Trans>
					</h3>
				</div>
				<PagedVTable
					records={revisionRecords}
					columns={revisionColumns}
					loading={loadingRevisions}
					emptyText={t`No revisions found.`}
					searchPlaceholder={t`Search revisions...`}
					searchValue={revisionSearch}
					onSearchChange={setRevisionSearch}
					height={360}
					serverPagination={{
						page: revisionPage,
						pageSize: revisionPageSize,
						totalCount: revisionTotal,
						onPageChange: setRevisionPage,
						onPageSizeChange: (value) => resetRevisionPage(() => setRevisionPageSize(value)),
					}}
					serverFiltering={revisionFiltering}
					serverSorting={revisionSorting}
					onCellClick={(record, field) => {
						if (field === "view") viewRevision(record)
					}}
				/>
			</div>
		</div>
	)
})

function FormInput({ label, value, onChange }: { label: string; value: string; onChange: (value: string) => void }) {
	return (
		<div className="grid gap-2">
			<Label>{label}</Label>
			<Input value={value} onChange={(event) => onChange(event.target.value)} />
		</div>
	)
}

function Stat({ label, value }: { label: string; value: string }) {
	return (
		<div className="min-w-0 rounded border p-2">
			<div className="text-xs text-muted-foreground">{label}</div>
			<div className="truncate font-medium" title={value}>
				{value}
			</div>
		</div>
	)
}

function formatDate(value?: string) {
	if (!value) return "—"
	const date = new Date(value)
	return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
}

function denseCellStyle() {
	return { padding: [8, 10, 8, 10], textBaseline: "middle", autoWrapText: false }
}

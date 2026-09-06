import { Trans, useLingui } from "@lingui/react/macro"
import { EyeIcon, RefreshCwIcon, ReplaceIcon, SearchIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
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

type RevisionChange = { action: string; prefix_id: string; cidr: string }
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

type PrefixList = { items?: AddressPrefix[]; next_cursor?: string }
type RevisionList = { items?: AddressDraftRevision[]; next_cursor?: string }

const pageSize = 100
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
	const [prefixCursor, setPrefixCursor] = useState("")
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [family, setFamily] = useState("all")
	const [selected, setSelected] = useState<Record<string, AddressPrefix>>({})
	const [form, setForm] = useState(emptyBatch)
	const [revision, setRevision] = useState<AddressDraftRevision | null>(null)
	const [revisionETag, setRevisionETag] = useState("")
	const [revisions, setRevisions] = useState<AddressDraftRevision[]>([])
	const [revisionCursor, setRevisionCursor] = useState("")
	const [status, setStatus] = useState("all")
	const [loadingPrefixes, setLoadingPrefixes] = useState(true)
	const [loadingRevisions, setLoadingRevisions] = useState(true)
	const [working, setWorking] = useState(false)
	const [error, setError] = useState("")
	const [notice, setNotice] = useState("")

	useEffect(() => {
		const timer = window.setTimeout(() => setDebouncedSearch(search.trim()), 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const fetchPrefixes = useCallback(
		async (cursor = "", append = false) => {
			setLoadingPrefixes(true)
			setError("")
			try {
				const data = await pb.send<PrefixList>("/api/v1/address-prefixes", {
					query: {
						q: debouncedSearch || undefined,
						family: family === "all" ? undefined : family,
						limit: pageSize,
						cursor: cursor || undefined,
					},
				})
				setPrefixes((current) => (append ? [...current, ...(data.items ?? [])] : (data.items ?? [])))
				setPrefixCursor(data.next_cursor ?? "")
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to load prefixes`)
			} finally {
				setLoadingPrefixes(false)
			}
		},
		[debouncedSearch, family, t]
	)

	const fetchRevisions = useCallback(
		async (cursor = "", append = false) => {
			setLoadingRevisions(true)
			setError("")
			try {
				const data = await pb.send<RevisionList>("/api/v1/address-draft-revisions", {
					query: { status: status === "all" ? undefined : status, limit: pageSize, cursor: cursor || undefined },
				})
				setRevisions((current) => (append ? [...current, ...(data.items ?? [])] : (data.items ?? [])))
				setRevisionCursor(data.next_cursor ?? "")
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to load revisions`)
			} finally {
				setLoadingRevisions(false)
			}
		},
		[status, t]
	)

	useEffect(() => {
		fetchPrefixes()
	}, [fetchPrefixes])
	useEffect(() => {
		fetchRevisions()
	}, [fetchRevisions])

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
			(revision?.preview.changes ?? []).map((change, index) => ({
				index: index + 1,
				action: change.action,
				cidr: change.cidr,
				prefixID: change.prefix_id,
			})),
		[revision]
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
						<FormInput
							label={t`Geography node ID`}
							value={form.geoLeafID}
							onChange={(value) => updateForm({ geoLeafID: value })}
						/>
						<FormInput
							label={t`Operator ID`}
							value={form.operatorID}
							onChange={(value) => updateForm({ operatorID: value })}
						/>
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
						emptyText={t`No changes in this revision.`}
						searchPlaceholder={t`Search revision changes...`}
						height={320}
					/>
				</div>
			) : null}

			<div className="grid gap-3">
				<h3 className="font-medium">
					<Trans>Select Existing Prefixes</Trans>
				</h3>
				<div className="flex flex-wrap gap-2">
					<div className="relative min-w-64 max-w-sm flex-1">
						<SearchIcon className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
						<Input
							value={search}
							onChange={(event) => setSearch(event.target.value)}
							placeholder={t`Search CIDR or labels...`}
							className="pl-9"
						/>
					</div>
					<Select value={family} onValueChange={setFamily}>
						<SelectTrigger className="w-32">
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="all">
								<Trans>All families</Trans>
							</SelectItem>
							<SelectItem value="4">IPv4</SelectItem>
							<SelectItem value="6">IPv6</SelectItem>
						</SelectContent>
					</Select>
				</div>
				<div className="overflow-hidden rounded-md border border-border bg-card">
					<PagedVTable
						records={prefixRecords}
						columns={prefixColumns}
						loading={loadingPrefixes}
						emptyText={t`No prefixes found.`}
						showSearch={false}
						height={420}
						onCellClick={(record, field) => {
							if (field === "selected") togglePrefix(record)
						}}
					/>
				</div>
				{prefixCursor ? (
					<Button variant="outline" onClick={() => fetchPrefixes(prefixCursor, true)}>
						<Trans>Load more</Trans>
					</Button>
				) : null}
			</div>

			<div className="grid gap-3">
				<div className="flex flex-wrap items-center justify-between gap-2">
					<h3 className="font-medium">
						<Trans>Revision History</Trans>
					</h3>
					<Select value={status} onValueChange={setStatus}>
						<SelectTrigger className="w-44">
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							{["all", "prepared", "applied", "superseded", "cancelled"].map((value) => (
								<SelectItem key={value} value={value}>
									{value}
								</SelectItem>
							))}
						</SelectContent>
					</Select>
				</div>
				<PagedVTable
					records={revisionRecords}
					columns={revisionColumns}
					loading={loadingRevisions}
					emptyText={t`No revisions found.`}
					searchPlaceholder={t`Search revisions...`}
					height={360}
					onCellClick={(record, field) => {
						if (field === "view") viewRevision(record)
					}}
				/>
				{revisionCursor ? (
					<Button variant="outline" onClick={() => fetchRevisions(revisionCursor, true)}>
						<Trans>Load more</Trans>
					</Button>
				) : null}
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

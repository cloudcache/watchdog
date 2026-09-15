import { Trans, useLingui } from "@lingui/react/macro"
import { ClockIcon, RefreshCwIcon, RocketIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { api, can } from "@/lib/api"
import FlowEnrichmentPublications from "./flow-enrichment-publications"

type AddressDimensionSnapshot = {
	id: string
	version: number
	effective_from: string
	checksum: string
	draft_digest: string
	bundle_schema_version: number
	entry_count: number
	prefix_count: number
	address_set_count: number
	max_address_sets_per_record: number
	status: string
	approval_state: string
	row_version: number
	decided_by?: string
	decided_at?: string
	created_at: string
}

type AddressDimensionPreview = {
	draft_digest: string
	bundle_schema_version: number
	effective_from: string
	prefix_count: number
	address_set_count: number
	operator_count?: number
	enabled_address_set_count: number
	max_address_sets_per_record: number
	definition_bytes: number
}

type ListResponse = { items?: AddressDimensionSnapshot[]; total?: number }

export default memo(function AddressPublications() {
	const { t } = useLingui()
	const [items, setItems] = useState<AddressDimensionSnapshot[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [status, setStatus] = useState("")
	const [sort, setSort] = useState("version:desc")
	const [effectiveFrom, setEffectiveFrom] = useState(defaultEffectiveFrom)
	const [preview, setPreview] = useState<AddressDimensionPreview | null>(null)
	const [loading, setLoading] = useState(true)
	const [working, setWorking] = useState(false)
	const [error, setError] = useState("")
	const [notice, setNotice] = useState("")
	const [selectedId, setSelectedId] = useState<string | null>(null)
	const requestSequence = useRef(0)
	const canPublish = can("address.publish")

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
			const data = await api.send<ListResponse>("/api/v1/dimensions/address/versions", {
				query: {
					q: debouncedSearch || undefined,
					status: status || undefined,
					limit: pageSize,
					offset: page * pageSize || undefined,
					sort: sortField,
					order,
				},
			})
			if (sequence !== requestSequence.current) return
			setItems(data.items ?? [])
			setTotal(data.total ?? 0)
		} catch (err) {
			if (sequence !== requestSequence.current) return
			setItems([])
			setTotal(0)
			setError(err instanceof Error ? err.message : t`Failed to load publications`)
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [debouncedSearch, page, pageSize, sort, status, t])

	useEffect(() => {
		fetchPage()
	}, [fetchPage])

	const runPreview = async () => {
		setWorking(true)
		setError("")
		setNotice("")
		try {
			const timestamp = parseEffectiveFrom(effectiveFrom)
			setPreview(
				await api.send<AddressDimensionPreview>("/api/v1/dimensions/address/preview", {
					method: "POST",
					body: { effective_from: timestamp },
				})
			)
		} catch (err) {
			setPreview(null)
			setError(err instanceof Error ? err.message : t`Preview failed`)
		} finally {
			setWorking(false)
		}
	}

	const publish = async () => {
		if (!preview || !confirm(t`Publish this immutable address dimension snapshot?`)) return
		setWorking(true)
		setError("")
		try {
			const response = await api.send<{ job?: { id?: string } }>("/api/v1/dimensions/address/publish", {
				method: "POST",
				body: { effective_from: preview.effective_from, preview_digest: preview.draft_digest },
			})
			setNotice(response.job?.id ? t`Publish job queued: ${response.job.id}` : t`Publish job queued`)
			setPreview(null)
			await fetchPage()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Publish failed`)
		} finally {
			setWorking(false)
		}
	}

	const selected = useMemo(() => items.find((item) => item.id === selectedId) ?? null, [items, selectedId])

	// One authenticated confirmation drives every lifecycle verb; accountability is
	// the audit log + version rollback (the ed25519 approval signature was dropped).
	const runAction = useCallback(
		async (snap: AddressDimensionSnapshot, verb: string, body?: Record<string, unknown>) => {
			setWorking(true)
			setError("")
			setNotice("")
			try {
				await api.send(`/api/v1/dimensions/address/versions/${snap.id}/actions/${verb}`, {
					method: "POST",
					headers: { "If-Match": `"${snap.row_version}"` },
					body,
				})
				setNotice(t`Version ${snap.version}: ${verb} applied`)
				await fetchPage()
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Action failed`)
			} finally {
				setWorking(false)
			}
		},
		[fetchPage, t]
	)
	const approve = (snap: AddressDimensionSnapshot) => {
		if (confirm(t`Approve version ${snap.version}? It becomes eligible to activate.`)) runAction(snap, "approve")
	}
	const reject = (snap: AddressDimensionSnapshot) => {
		const reason = prompt(t`Reason for rejecting version ${snap.version}?`)
		if (reason?.trim()) runAction(snap, "reject", { reason: reason.trim() })
	}
	const activate = (snap: AddressDimensionSnapshot) => {
		if (confirm(t`Activate version ${snap.version}? It goes live for all flow workers.`)) runAction(snap, "activate")
	}
	const rollback = (snap: AddressDimensionSnapshot) => {
		if (confirm(t`Roll back to version ${snap.version} now? It becomes the live snapshot.`))
			runAction(snap, "rollback", { effective_from: currentMinuteISO() })
	}
	const retire = (snap: AddressDimensionSnapshot) => {
		const reason = prompt(t`Reason for retiring version ${snap.version}?`)
		if (reason === null) return
		runAction(snap, "retire", reason.trim() ? { reason: reason.trim() } : undefined)
	}

	const records = useMemo(
		() =>
			items.map((item) => ({
				id: item.id,
				version: item.version,
				status: item.status,
				approval: item.approval_state,
				effective: formatDate(item.effective_from),
				prefixes: item.prefix_count.toLocaleString(),
				sets: item.address_set_count.toLocaleString(),
				maxMembership: item.max_address_sets_per_record,
				schema: item.bundle_schema_version,
				checksum: item.checksum,
				created: formatDate(item.created_at),
				manage: canPublish ? t`Manage` : "",
				rowVersion: item.row_version,
			})),
		[items, canPublish, t]
	)
	const columns = useMemo(
		() => [
			{ field: "version", title: t`Version`, width: 90, style: denseCellStyle() },
			{ field: "status", title: t`Status`, width: 100, style: denseCellStyle() },
			{ field: "approval", title: t`Approval`, width: 110, style: denseCellStyle() },
			{ field: "effective", title: t`Effective from`, width: 190, style: denseCellStyle() },
			{ field: "prefixes", title: t`Prefixes`, width: 110, style: denseCellStyle() },
			{ field: "sets", title: t`Sets`, width: 100, style: denseCellStyle() },
			{ field: "maxMembership", title: t`Max memberships`, width: 150, style: denseCellStyle() },
			{ field: "schema", title: t`Schema`, width: 90, style: denseCellStyle() },
			{ field: "checksum", title: t`Checksum`, width: 300, style: denseCellStyle() },
			{ field: "created", title: t`Created`, width: 190, style: denseCellStyle() },
			...(canPublish
				? [
						{
							field: "manage",
							title: t`Actions`,
							width: 90,
							filter: false,
							style: { ...denseCellStyle(), color: "#2563eb", cursor: "pointer" },
						},
					]
				: []),
		],
		[t, canPublish]
	)
	const resetPage = (update: () => void) => {
		setPage(0)
		update()
	}
	const serverFiltering = useMemo(
		() => ({
			options: {
				status: [
					{ value: "active", label: t`Active` },
					{ value: "retired", label: t`Retired` },
				],
			},
			selected: { status: status ? [status] : [] },
			selection: { status: "single" as const },
			onColumnFilterChange: (_field: string, values: unknown[]) =>
				resetPage(() => setStatus(values.length > 0 ? String(values[0]) : "")),
			onClearAll: () => resetPage(() => setStatus("")),
		}),
		[status, t]
	)
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(
		() => ({
			field: sortField,
			direction: sortDirection,
			fields: {
				version: "version",
				status: "status",
				effective: "effective",
				prefixes: "prefixes",
				sets: "sets",
				maxMembership: "max_membership",
				schema: "schema",
				checksum: "checksum",
				created: "created",
			},
			onSortChange: (field: string, direction: "asc" | "desc") => resetPage(() => setSort(`${field}:${direction}`)),
		}),
		[sortDirection, sortField]
	)

	return (
		<div className="grid gap-4">
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<RocketIcon className="h-5 w-5 text-muted-foreground" />
					<h2 className="text-lg font-semibold">
						<Trans>Dimension Publications</Trans>
					</h2>
				</div>
				<Button variant="outline" size="sm" onClick={() => fetchPage()}>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Refresh</Trans>
				</Button>
			</div>
			<div className="grid gap-3 rounded-md border border-border bg-card p-4 md:grid-cols-[minmax(240px,1fr)_auto_auto]">
				<div className="grid gap-2">
					<Label htmlFor="dimension-effective-from">
						<Trans>Effective from</Trans>
					</Label>
					<div className="relative">
						<ClockIcon className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
						<Input
							id="dimension-effective-from"
							className="pl-9"
							type="datetime-local"
							value={effectiveFrom}
							onChange={(event) => {
								setEffectiveFrom(event.target.value)
								setPreview(null)
							}}
						/>
					</div>
				</div>
				<div className="flex items-end">
					<Button variant="outline" onClick={runPreview} disabled={working}>
						<Trans>Validate & Preview</Trans>
					</Button>
				</div>
				<div className="flex items-end">
					<Button onClick={publish} disabled={working || !preview}>
						<RocketIcon className="me-2 h-4 w-4" />
						<Trans>Publish asynchronously</Trans>
					</Button>
				</div>
				{preview ? (
					<div className="grid gap-2 text-sm md:col-span-3 sm:grid-cols-2 lg:grid-cols-6">
						<PreviewStat label={t`Prefixes`} value={preview.prefix_count.toLocaleString()} />
						<PreviewStat label={t`Operators`} value={(preview.operator_count ?? 0).toLocaleString()} />
						<PreviewStat
							label={t`Enabled sets`}
							value={`${preview.enabled_address_set_count}/${preview.address_set_count}`}
						/>
						<PreviewStat label={t`Max memberships`} value={String(preview.max_address_sets_per_record)} />
						<PreviewStat label={t`Definition size`} value={formatBytes(preview.definition_bytes)} />
						<PreviewStat label={t`Draft digest`} value={preview.draft_digest} />
					</div>
				) : null}
			</div>
			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			{notice ? <div className="rounded-md border border-green-500/30 p-3 text-sm text-green-700">{notice}</div> : null}
			{selected && canPublish ? (
				<div className="flex flex-wrap items-center gap-3 rounded-md border border-border bg-card p-3 text-sm">
					<span className="font-medium">
						<Trans>Version {selected.version}</Trans>
					</span>
					<span className="text-muted-foreground">
						{selected.approval_state} · {selected.status}
					</span>
					<div className="ms-auto flex flex-wrap gap-2">
						{selected.approval_state === "pending" && selected.status === "active" ? (
							<>
								<Button size="sm" onClick={() => approve(selected)} disabled={working}>
									<Trans>Approve</Trans>
								</Button>
								<Button size="sm" variant="outline" onClick={() => reject(selected)} disabled={working}>
									<Trans>Reject</Trans>
								</Button>
							</>
						) : null}
						{selected.approval_state === "approved" ? (
							<>
								{selected.status === "active" ? (
									<Button size="sm" onClick={() => activate(selected)} disabled={working}>
										<Trans>Activate</Trans>
									</Button>
								) : null}
								<Button size="sm" variant="outline" onClick={() => rollback(selected)} disabled={working}>
									<Trans>Roll back to this</Trans>
								</Button>
								{selected.status === "active" ? (
									<Button size="sm" variant="outline" onClick={() => retire(selected)} disabled={working}>
										<Trans>Retire</Trans>
									</Button>
								) : null}
							</>
						) : null}
						<Button size="sm" variant="ghost" onClick={() => setSelectedId(null)}>
							<Trans>Close</Trans>
						</Button>
					</div>
				</div>
			) : null}
			<PagedVTable
				records={records}
				columns={columns}
				loading={loading}
				emptyText={t`No dimension publications found.`}
				searchPlaceholder={t`Search versions or checksums...`}
				searchValue={search}
				onSearchChange={setSearch}
				height={420}
				onCellClick={(record, field) => {
					if (field === "manage") setSelectedId(String(record.id))
				}}
				serverPagination={{
					page,
					pageSize,
					totalCount: total,
					onPageChange: setPage,
					onPageSizeChange: (value) => resetPage(() => setPageSize(value)),
				}}
				serverFiltering={serverFiltering}
				serverSorting={serverSorting}
			/>
			<FlowEnrichmentPublications />
		</div>
	)
})

function PreviewStat({ label, value }: { label: string; value: string }) {
	return (
		<div className="min-w-0 rounded border p-2">
			<div className="text-xs text-muted-foreground">{label}</div>
			<div className="truncate font-medium" title={value}>
				{value}
			</div>
		</div>
	)
}

function defaultEffectiveFrom() {
	const date = new Date(Math.ceil(Date.now() / 60_000) * 60_000 + 60_000)
	const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000)
	return local.toISOString().slice(0, 16)
}

function parseEffectiveFrom(value: string) {
	const date = new Date(value)
	if (!value || Number.isNaN(date.getTime()) || date.getSeconds() !== 0 || date.getMilliseconds() !== 0)
		throw new Error("Effective time must be a minute boundary")
	return date.toISOString()
}

function currentMinuteISO() {
	return new Date(Math.floor(Date.now() / 60_000) * 60_000).toISOString()
}

function denseCellStyle() {
	return { padding: [8, 10, 8, 10] as [number, number, number, number], fontSize: 13 }
}

function formatDate(value: string) {
	const date = new Date(value)
	return Number.isNaN(date.getTime()) ? value || "—" : date.toLocaleString()
}

function formatBytes(value: number) {
	if (!Number.isFinite(value) || value <= 0) return "0 B"
	const units = ["B", "KiB", "MiB", "GiB"]
	const index = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1)
	return `${(value / 1024 ** index).toFixed(index === 0 ? 0 : 1)} ${units[index]}`
}

import { Trans, useLingui } from "@lingui/react/macro"
import { ClockIcon, RefreshCwIcon, RocketIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { pb } from "@/lib/api"

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
	estimated_bundle_bytes: number
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
			const data = await pb.send<ListResponse>("/api/v1/dimensions/address/versions", {
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
				await pb.send<AddressDimensionPreview>("/api/v1/dimensions/address/preview", {
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
			const response = await pb.send<{ job?: { id?: string } }>("/api/v1/dimensions/address/publish", {
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

	const records = useMemo(
		() =>
			items.map((item) => ({
				version: item.version,
				status: item.status,
				effective: formatDate(item.effective_from),
				prefixes: item.prefix_count.toLocaleString(),
				sets: item.address_set_count.toLocaleString(),
				maxMembership: item.max_address_sets_per_record,
				schema: item.bundle_schema_version,
				checksum: item.checksum,
				created: formatDate(item.created_at),
			})),
		[items]
	)
	const columns = useMemo(
		() => [
			{ field: "version", title: t`Version`, width: 90, style: denseCellStyle() },
			{ field: "status", title: t`Status`, width: 100, style: denseCellStyle() },
			{ field: "effective", title: t`Effective from`, width: 190, style: denseCellStyle() },
			{ field: "prefixes", title: t`Prefixes`, width: 110, style: denseCellStyle() },
			{ field: "sets", title: t`Sets`, width: 100, style: denseCellStyle() },
			{ field: "maxMembership", title: t`Max memberships`, width: 150, style: denseCellStyle() },
			{ field: "schema", title: t`Schema`, width: 90, style: denseCellStyle() },
			{ field: "checksum", title: t`Checksum`, width: 300, style: denseCellStyle() },
			{ field: "created", title: t`Created`, width: 190, style: denseCellStyle() },
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
						<PreviewStat label={t`Estimated size`} value={formatBytes(preview.estimated_bundle_bytes)} />
						<PreviewStat label={t`Draft digest`} value={preview.draft_digest} />
					</div>
				) : null}
			</div>
			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			{notice ? <div className="rounded-md border border-green-500/30 p-3 text-sm text-green-700">{notice}</div> : null}
			<PagedVTable
				records={records}
				columns={columns}
				loading={loading}
				emptyText={t`No dimension publications found.`}
				searchPlaceholder={t`Search versions or checksums...`}
				searchValue={search}
				onSearchChange={setSearch}
				height={420}
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

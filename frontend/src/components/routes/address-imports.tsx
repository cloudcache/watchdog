import { Trans, useLingui } from "@lingui/react/macro"
import { Uppy } from "@uppy/core"
import Tus from "@uppy/tus"
import { DatabaseZapIcon, RefreshCwIcon, UploadIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { api } from "@/lib/api"

// The tus resumable-upload endpoint lives under the same API base + auth as the rest
// of the client; requests must carry the session cookie (withCredentials) and the
// double-submit CSRF token, exactly like api.send does for other mutations.
function tusUploadEndpoint(): string {
	const base = (globalThis.WATCHDOG?.API_URL ?? "").replace(/\/+$/, "")
	return `${base}/api/v1/address-imports/uploads/`
}

function readCsrfToken(): string {
	const match = document.cookie.match(/(?:^|; )wd_csrf=([^;]+)/)
	return match ? decodeURIComponent(match[1]) : ""
}

export type AddressImport = {
	id: string
	source_slot: string
	format: string
	original_name: string
	size_bytes: number
	database_type?: string
	status: string
	row_count_v4: number
	row_count_v6: number
	error_code?: string
	error_detail?: string
	created_at: string
}

export type AddressImportSlot = {
	source_slot: string
	import_id: string
	row_version: number
}

type ImportedPrefix = {
	id: number
	cidr: string
	family: number
	country_code?: string
	country_name?: string
	subdivision_name?: string
	city_name?: string
	asn?: number
	operator_name?: string
	labels?: Record<string, string>
}

type ListResponse<T> = { items?: T[]; next_cursor?: string; total?: number }

const importSlots = ["geo", "asn", "combined"] as const

export default memo(function AddressImports() {
	const { t } = useLingui()
	const [imports, setImports] = useState<AddressImport[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [slots, setSlots] = useState<Record<string, AddressImportSlot>>({})
	const [sourceSlot, setSourceSlot] = useState("")
	const [status, setStatus] = useState("")
	const [format, setFormat] = useState("")
	const [sort, setSort] = useState("created:desc")
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const [showUpload, setShowUpload] = useState(false)
	const [uploading, setUploading] = useState(false)
	const [uploadProgress, setUploadProgress] = useState(0)
	const [upload, setUpload] = useState<{ file: File | null; sourceSlot: string; language: string }>({
		file: null,
		sourceSlot: "geo",
		language: "en",
	})
	const [selected, setSelected] = useState<AddressImport | null>(null)
	const requestSequence = useRef(0)

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setPage(0)
			setDebouncedSearch(search.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const refreshSlots = useCallback(async () => {
		const next: Record<string, AddressImportSlot> = {}
		await Promise.all(
			importSlots.map(async (slot) => {
				try {
					next[slot] = await api.send<AddressImportSlot>(`/api/v1/address-import-slots/${slot}`, {})
				} catch {
					// A slot has no active generation until its first successful activation.
				}
			})
		)
		setSlots(next)
	}, [])

	const fetchPage = useCallback(async () => {
		const sequence = ++requestSequence.current
		const [sortField, order] = sort.split(":")
		setLoading(true)
		setError("")
		try {
			const data = await api.send<ListResponse<AddressImport>>("/api/v1/address-imports", {
				query: {
					q: debouncedSearch || undefined,
					source_slot: sourceSlot || undefined,
					status: status || undefined,
					format: format || undefined,
					limit: pageSize,
					offset: page * pageSize || undefined,
					sort: sortField,
					order,
				},
			})
			if (sequence !== requestSequence.current) return
			setImports(data.items ?? [])
			setTotal(data.total ?? 0)
			await refreshSlots()
		} catch (err) {
			if (sequence !== requestSequence.current) return
			setImports([])
			setTotal(0)
			setError(err instanceof Error ? err.message : t`Failed to load`)
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [debouncedSearch, format, page, pageSize, refreshSlots, sort, sourceSlot, status, t])

	useEffect(() => {
		fetchPage()
	}, [fetchPage])

	// submitUpload streams the source database via tus (resumable): a dropped
	// connection resumes from the last acked offset instead of restarting. On
	// completion the server has already created the queued import + decode job.
	const submitUpload = () => {
		const file = upload.file
		if (!file) {
			setError(t`Select an MMDB or IPDB file`)
			return
		}
		setUploading(true)
		setError("")
		setUploadProgress(0)
		const uppy = new Uppy({ autoProceed: true, allowMultipleUploadBatches: false })
		uppy.use(Tus, {
			endpoint: tusUploadEndpoint(),
			withCredentials: true,
			chunkSize: 50 * 1024 * 1024,
			retryDelays: [0, 1000, 3000, 5000, 10000],
			headers: () => ({ "X-CSRF-Token": readCsrfToken() }),
		})
		uppy.setMeta({ source_slot: upload.sourceSlot, language: upload.language.trim() })
		uppy.on("upload-progress", (_file, progress) => {
			if (progress.bytesTotal) {
				setUploadProgress(Math.round((progress.bytesUploaded / progress.bytesTotal) * 100))
			}
		})
		uppy.on("complete", async (result) => {
			uppy.destroy()
			setUploading(false)
			if (result.failed?.length) {
				setError(result.failed[0].error || t`Upload failed`)
				return
			}
			setShowUpload(false)
			setUpload({ file: null, sourceSlot: "geo", language: "en" })
			await fetchPage()
		})
		uppy.on("error", (err) => {
			uppy.destroy()
			setUploading(false)
			setError(err instanceof Error ? err.message : t`Upload failed`)
		})
		try {
			uppy.addFile({ name: file.name, type: file.type, data: file })
		} catch (err) {
			uppy.destroy()
			setUploading(false)
			setError(err instanceof Error ? err.message : t`Upload failed`)
		}
	}

	const activate = useCallback(
		async (item: AddressImport) => {
			if (item.status !== "ready") return
			const current = slots[item.source_slot]
			if (!confirm(t`Activate this immutable generation for its source slot?`)) return
			setError("")
			try {
				await api.send(`/api/v1/address-imports/${item.id}/actions/activate`, {
					method: "POST",
					headers: { "If-Match": `"${current?.row_version ?? 0}"` },
				})
				await fetchPage()
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to activate`)
			}
		},
		[fetchPage, slots, t]
	)

	const deleteImport = useCallback(
		async (item: AddressImport) => {
			if (!confirm(t`Delete this import generation and its stored source file? This cannot be undone.`)) return
			setError("")
			try {
				await api.send(`/api/v1/address-imports/${item.id}`, { method: "DELETE" })
				await fetchPage()
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to delete`)
			}
		},
		[fetchPage, t]
	)

	const records = useMemo(
		() =>
			imports.map((item) => ({
				id: item.id,
				name: item.original_name,
				slot: item.source_slot,
				format: item.format.toUpperCase(),
				type: item.database_type || "—",
				status: slots[item.source_slot]?.import_id === item.id ? `${item.status} · active` : item.status,
				rows: `${item.row_count_v4.toLocaleString()} v4 / ${item.row_count_v6.toLocaleString()} v6`,
				size: formatBytes(item.size_bytes),
				created: formatDate(item.created_at),
				error: [item.error_code, item.error_detail].filter(Boolean).join(": ") || "—",
				browse: t`Browse`,
				activate: item.status === "ready" && slots[item.source_slot]?.import_id !== item.id ? t`Activate` : "—",
				delete: slots[item.source_slot]?.import_id === item.id ? "—" : t`Delete`,
				item,
			})),
		[imports, slots, t]
	)
	const columns = useMemo(
		() => [
			{ field: "name", title: t`File`, width: 220, style: denseCellStyle() },
			{ field: "slot", title: t`Slot`, width: 100, style: denseCellStyle() },
			{ field: "format", title: t`Format`, width: 100, style: denseCellStyle() },
			{ field: "type", title: t`Database type`, width: 150, style: denseCellStyle() },
			{ field: "status", title: t`Status`, width: 130, style: denseCellStyle() },
			{ field: "rows", title: t`Rows`, width: 190, style: denseCellStyle() },
			{ field: "size", title: t`Size`, width: 110, style: denseCellStyle() },
			{ field: "created", title: t`Created`, width: 180, style: denseCellStyle() },
			{ field: "error", title: t`Error`, width: 240, style: denseCellStyle() },
			{ field: "browse", title: t`Browse`, width: 90, filter: false, style: actionCellStyle() },
			{ field: "activate", title: t`Activation`, width: 100, filter: false, style: actionCellStyle() },
			{ field: "delete", title: t`Delete`, width: 90, filter: false, style: actionCellStyle() },
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
				slot: importSlots.map((value) => ({ value, label: value })),
				format: [
					{ value: "mmdb", label: "MMDB" },
					{ value: "ipdb", label: "IPDB" },
				],
				status: ["quarantined", "queued", "importing", "ready", "failed", "retired"].map((value) => ({
					value,
					label: value,
				})),
			},
			selected: {
				slot: sourceSlot ? [sourceSlot] : [],
				format: format ? [format] : [],
				status: status ? [status] : [],
			},
			selection: { slot: "single" as const, format: "single" as const, status: "single" as const },
			onColumnFilterChange: (field: string, values: unknown[]) => {
				const value = values.length > 0 ? String(values[0]) : ""
				resetPage(() => {
					if (field === "slot") setSourceSlot(value)
					if (field === "format") setFormat(value)
					if (field === "status") setStatus(value)
				})
			},
			onClearAll: () =>
				resetPage(() => {
					setSourceSlot("")
					setFormat("")
					setStatus("")
				}),
		}),
		[format, sourceSlot, status]
	)
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(
		() => ({
			field: sortField,
			direction: sortDirection,
			fields: {
				name: "name",
				slot: "slot",
				format: "format",
				type: "type",
				status: "status",
				rows: "rows",
				size: "size",
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
					<DatabaseZapIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h2 className="text-lg font-semibold">
						<Trans>Database Imports</Trans>
					</h2>
				</div>
				<div className="flex gap-2">
					<Button variant="outline" size="sm" onClick={fetchPage} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
					<Button size="sm" onClick={() => setShowUpload((value) => !value)}>
						<UploadIcon className="me-2 h-4 w-4" />
						<Trans>Upload Database</Trans>
					</Button>
				</div>
			</div>

			{showUpload ? (
				<div className="grid gap-3 rounded-md border border-border bg-card p-4 md:grid-cols-3">
					<div className="grid gap-2 md:col-span-3">
						<Label htmlFor="address-database-file">
							<Trans>MMDB or IPDB file</Trans>
						</Label>
						<Input
							id="address-database-file"
							type="file"
							accept=".mmdb,.ipdb"
							onChange={(event) => setUpload({ ...upload, file: event.target.files?.[0] ?? null })}
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Source slot</Trans>
						</Label>
						<Select value={upload.sourceSlot} onValueChange={(value) => setUpload({ ...upload, sourceSlot: value })}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="geo">Geo</SelectItem>
								<SelectItem value="asn">ASN</SelectItem>
								<SelectItem value="combined">
									<Trans>Combined</Trans>
								</SelectItem>
							</SelectContent>
						</Select>
					</div>
					<div className="grid gap-2">
						<Label htmlFor="address-database-language">
							<Trans>Language</Trans>
						</Label>
						<Input
							id="address-database-language"
							value={upload.language}
							maxLength={16}
							onChange={(event) => setUpload({ ...upload, language: event.target.value })}
							placeholder="en"
						/>
					</div>
					<div className="flex items-end gap-2">
						<Button onClick={submitUpload} disabled={uploading || !upload.file}>
							{uploading ? <Trans>Uploading… {uploadProgress}%</Trans> : <Trans>Start Import</Trans>}
						</Button>
						<Button variant="outline" onClick={() => setShowUpload(false)}>
							<Trans>Cancel</Trans>
						</Button>
					</div>
				</div>
			) : null}

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			<PagedVTable
				records={records}
				columns={columns}
				loading={loading}
				emptyText={t`No address imports found.`}
				searchPlaceholder={t`Search imported files...`}
				searchValue={search}
				onSearchChange={setSearch}
				height={Math.min(420, 44 + records.length * 42)}
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
					const item = record.item as AddressImport
					if (field === "browse") setSelected(item)
					if (field === "activate" && record.activate !== "—") activate(item)
					if (field === "delete" && record.delete !== "—") deleteImport(item)
				}}
			/>
			{selected ? <ImportedPrefixBrowser item={selected} onClose={() => setSelected(null)} /> : null}
		</div>
	)
})

export function ImportedPrefixBrowser({ item, onClose }: { item: AddressImport; onClose: () => void }) {
	const { t } = useLingui()
	const [prefixes, setPrefixes] = useState<ImportedPrefix[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [family, setFamily] = useState("")
	const [country, setCountry] = useState("")
	const [operator, setOperator] = useState("")
	const [asn, setASN] = useState("")
	const [sort, setSort] = useState("cidr:asc")
	const [lookupActive, setLookupActive] = useState(false)
	const [lookupIP, setLookupIP] = useState("")
	const [selectedCidrs, setSelectedCidrs] = useState<string[]>([])
	const requestSequence = useRef(0)
	const selectable = useMemo(
		() => ({
			onSelectionChange: (recs: Record<string, unknown>[]) => setSelectedCidrs(recs.map((r) => String(r.cidr))),
		}),
		[]
	)

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setPage(0)
			setDebouncedSearch(search.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const fetchPrefixes = useCallback(async () => {
		const sequence = ++requestSequence.current
		const [sortField, order] = sort.split(":")
		setLoading(true)
		setError("")
		setLookupActive(false)
		try {
			const parsedASN = asn.trim() ? Number(asn) : undefined
			if (parsedASN !== undefined && (!Number.isInteger(parsedASN) || parsedASN <= 0))
				throw new Error(t`ASN must be a positive integer`)
			const data = await api.send<ListResponse<ImportedPrefix>>(`/api/v1/address-imports/${item.id}/prefixes`, {
				query: {
					q: debouncedSearch || undefined,
					family: family || undefined,
					country_code: country.trim() || undefined,
					operator: operator.trim() || undefined,
					asn: parsedASN,
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
			setError(err instanceof Error ? err.message : t`Failed to load prefixes`)
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [asn, country, debouncedSearch, family, item.id, operator, page, pageSize, sort, t])

	useEffect(() => {
		fetchPrefixes()
	}, [fetchPrefixes])

	const lookup = async () => {
		if (!lookupIP.trim()) return
		const sequence = ++requestSequence.current
		setLoading(true)
		setError("")
		try {
			const data = await api.send<ListResponse<ImportedPrefix>>(`/api/v1/address-imports/${item.id}/lookup`, {
				query: { ip: lookupIP.trim(), limit: 100 },
			})
			if (sequence !== requestSequence.current) return
			setPrefixes(data.items ?? [])
			setTotal(data.items?.length ?? 0)
			setPage(0)
			setLookupActive(true)
		} catch (err) {
			if (sequence !== requestSequence.current) return
			setError(err instanceof Error ? err.message : t`Lookup failed`)
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}

	const records = useMemo(
		() =>
			prefixes.map((prefix) => ({
				cidr: prefix.cidr,
				family: `IPv${prefix.family}`,
				country: [prefix.country_code, prefix.country_name].filter(Boolean).join(" · ") || "—",
				region: [prefix.subdivision_name, prefix.city_name].filter(Boolean).join(" / ") || "—",
				asn: prefix.asn ?? "—",
				operator: prefix.operator_name || "—",
				labels:
					Object.entries(prefix.labels ?? {})
						.map(([key, value]) => `${key}=${value}`)
						.join(", ") || "—",
			})),
		[prefixes]
	)
	const columns = useMemo(
		() => [
			{ field: "cidr", title: t`CIDR`, width: 190, style: denseCellStyle() },
			{ field: "family", title: t`Family`, width: 90, style: denseCellStyle() },
			{ field: "country", title: t`Country`, width: 200, style: denseCellStyle() },
			{ field: "region", title: t`Province / City`, width: 220, style: denseCellStyle() },
			{ field: "asn", title: "ASN", width: 110, style: denseCellStyle() },
			{ field: "operator", title: t`Operator`, width: 180, style: denseCellStyle() },
			{ field: "labels", title: t`Labels`, width: 260, style: denseCellStyle() },
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
				country: "country",
				region: "region",
				asn: "asn",
				operator: "operator",
			},
			onSortChange: (field: string, direction: "asc" | "desc") => resetPage(() => setSort(`${field}:${direction}`)),
		}),
		[sortDirection, sortField]
	)

	return (
		<div className="grid gap-3 rounded-md border border-border bg-card p-4">
			<div className="flex flex-wrap items-center justify-between gap-2">
				<div>
					<h3 className="font-semibold">{item.original_name}</h3>
					<p className="text-xs text-muted-foreground">{item.id}</p>
				</div>
				<Button variant="outline" size="sm" onClick={onClose}>
					<Trans>Close</Trans>
				</Button>
			</div>
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
				{selectedCidrs.length > 0 ? (
					<div className="flex items-center gap-2 text-sm">
						<span className="text-muted-foreground">
							<Trans>Selected {selectedCidrs.length}</Trans>
						</span>
						<Button variant="ghost" size="sm" onClick={() => setSelectedCidrs([])}>
							<Trans>Clear</Trans>
						</Button>
					</div>
				) : null}
			</div>
			<div className="flex flex-wrap gap-2">
				<Input
					className="w-28"
					value={country}
					onChange={(event) => setCountry(event.target.value)}
					placeholder={t`Country`}
				/>
				<Input
					className="w-36"
					value={operator}
					onChange={(event) => setOperator(event.target.value)}
					placeholder={t`Operator`}
				/>
				<Input
					className="w-28"
					value={asn}
					onChange={(event) => setASN(event.target.value)}
					placeholder="ASN"
					inputMode="numeric"
				/>
			</div>
			<div className="flex flex-wrap gap-2">
				<Input
					className="max-w-sm"
					value={lookupIP}
					onChange={(event) => setLookupIP(event.target.value)}
					placeholder={t`Exact IP lookup`}
				/>
				<Button variant="outline" onClick={lookup}>
					<Trans>Lookup</Trans>
				</Button>
				<Button
					variant="outline"
					onClick={() => {
						setLookupIP("")
						if (page === 0) fetchPrefixes()
						else setPage(0)
					}}
				>
					<Trans>Reset</Trans>
				</Button>
				{lookupActive ? (
					<span className="self-center text-xs text-muted-foreground">
						<Trans>Exact lookup result</Trans>
					</span>
				) : null}
			</div>
			{error ? <div className="text-sm text-destructive">{error}</div> : null}
			<PagedVTable
				records={records}
				columns={columns}
				loading={loading}
				emptyText={t`No imported prefixes found.`}
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
				serverFiltering={serverFiltering}
				serverSorting={serverSorting}
				selectable={selectable}
			/>
		</div>
	)
}

function denseCellStyle() {
	return { padding: [8, 10, 8, 10] as [number, number, number, number], fontSize: 13 }
}

function actionCellStyle() {
	return { ...denseCellStyle(), color: "#2563eb", cursor: "pointer" }
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

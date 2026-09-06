import { Trans, useLingui } from "@lingui/react/macro"
import { DatabaseZapIcon, RefreshCwIcon, SearchIcon, UploadIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { pb } from "@/lib/api"

type AddressImport = {
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

type AddressImportSlot = {
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

type ListResponse<T> = { items?: T[]; next_cursor?: string }
type UploadResponse = { import: AddressImport }

const pageSize = 100
const importSlots = ["geo", "asn", "combined"] as const

export default memo(function AddressImports() {
	const { t } = useLingui()
	const [imports, setImports] = useState<AddressImport[]>([])
	const [nextCursor, setNextCursor] = useState("")
	const [slots, setSlots] = useState<Record<string, AddressImportSlot>>({})
	const [sourceSlot, setSourceSlot] = useState("all")
	const [status, setStatus] = useState("all")
	const [loading, setLoading] = useState(true)
	const [loadingMore, setLoadingMore] = useState(false)
	const [error, setError] = useState("")
	const [showUpload, setShowUpload] = useState(false)
	const [uploading, setUploading] = useState(false)
	const [upload, setUpload] = useState<{ file: File | null; sourceSlot: string; language: string }>({
		file: null,
		sourceSlot: "geo",
		language: "en",
	})
	const [selected, setSelected] = useState<AddressImport | null>(null)

	const refreshSlots = useCallback(async () => {
		const next: Record<string, AddressImportSlot> = {}
		await Promise.all(
			importSlots.map(async (slot) => {
				try {
					next[slot] = await pb.send<AddressImportSlot>(`/api/v1/address-import-slots/${slot}`, {})
				} catch {
					// A slot has no active generation until its first successful activation.
				}
			})
		)
		setSlots(next)
	}, [])

	const fetchPage = useCallback(
		async (cursor: string, append: boolean) => {
			append ? setLoadingMore(true) : setLoading(true)
			setError("")
			try {
				const data = await pb.send<ListResponse<AddressImport>>("/api/v1/address-imports", {
					query: {
						source_slot: sourceSlot === "all" ? undefined : sourceSlot,
						status: status === "all" ? undefined : status,
						limit: pageSize,
						cursor: cursor || undefined,
					},
				})
				setImports((current) => (append ? [...current, ...(data.items ?? [])] : (data.items ?? [])))
				setNextCursor(data.next_cursor ?? "")
				if (!append) await refreshSlots()
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to load`)
			} finally {
				append ? setLoadingMore(false) : setLoading(false)
			}
		},
		[refreshSlots, sourceSlot, status, t]
	)

	useEffect(() => {
		fetchPage("", false)
	}, [fetchPage])

	const submitUpload = async () => {
		if (!upload.file) {
			setError(t`Select an MMDB or IPDB file`)
			return
		}
		setUploading(true)
		setError("")
		try {
			const body = new FormData()
			body.append("file", upload.file)
			body.append("source_slot", upload.sourceSlot)
			body.append("language", upload.language.trim())
			const response = await pb.send<UploadResponse>("/api/v1/address-imports", { method: "POST", body })
			setSelected(response.import)
			setShowUpload(false)
			setUpload({ file: null, sourceSlot: "geo", language: "en" })
			await fetchPage("", false)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to upload`)
		} finally {
			setUploading(false)
		}
	}

	const activate = useCallback(
		async (item: AddressImport) => {
			if (item.status !== "ready") return
			const current = slots[item.source_slot]
			if (!confirm(t`Activate this immutable generation for its source slot?`)) return
			setError("")
			try {
				await pb.send(`/api/v1/address-imports/${item.id}/actions/activate`, {
					method: "POST",
					headers: { "If-Match": `"${current?.row_version ?? 0}"` },
				})
				await fetchPage("", false)
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to activate`)
			}
		},
		[fetchPage, slots, t]
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
		],
		[t]
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
					<Button variant="outline" size="sm" onClick={() => fetchPage("", false)} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
					<Button size="sm" onClick={() => setShowUpload((value) => !value)}>
						<UploadIcon className="me-2 h-4 w-4" />
						<Trans>Upload Database</Trans>
					</Button>
				</div>
			</div>

			<div className="flex flex-wrap gap-2">
				<Select value={sourceSlot} onValueChange={setSourceSlot}>
					<SelectTrigger className="w-40">
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="all">
							<Trans>All slots</Trans>
						</SelectItem>
						<SelectItem value="geo">Geo</SelectItem>
						<SelectItem value="asn">ASN</SelectItem>
						<SelectItem value="combined">
							<Trans>Combined</Trans>
						</SelectItem>
					</SelectContent>
				</Select>
				<Select value={status} onValueChange={setStatus}>
					<SelectTrigger className="w-40">
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="all">
							<Trans>All statuses</Trans>
						</SelectItem>
						{["queued", "importing", "ready", "failed", "retired"].map((value) => (
							<SelectItem key={value} value={value}>
								{value}
							</SelectItem>
						))}
					</SelectContent>
				</Select>
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
						<Button onClick={submitUpload} disabled={uploading}>
							{uploading ? <Trans>Uploading...</Trans> : <Trans>Start Import</Trans>}
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
				height={420}
				onCellClick={(record, field) => {
					const item = record.item as AddressImport
					if (field === "browse") setSelected(item)
					if (field === "activate" && record.activate !== "—") activate(item)
				}}
			/>
			{nextCursor ? (
				<Button variant="outline" onClick={() => fetchPage(nextCursor, true)} disabled={loadingMore}>
					{loadingMore ? <Trans>Loading...</Trans> : <Trans>Load more</Trans>}
				</Button>
			) : null}
			{selected ? <ImportedPrefixBrowser item={selected} onClose={() => setSelected(null)} /> : null}
		</div>
	)
})

function ImportedPrefixBrowser({ item, onClose }: { item: AddressImport; onClose: () => void }) {
	const { t } = useLingui()
	const [prefixes, setPrefixes] = useState<ImportedPrefix[]>([])
	const [nextCursor, setNextCursor] = useState("")
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [family, setFamily] = useState("all")
	const [country, setCountry] = useState("")
	const [operator, setOperator] = useState("")
	const [asn, setASN] = useState("")
	const [lookupIP, setLookupIP] = useState("")

	useEffect(() => {
		const timer = window.setTimeout(() => setDebouncedSearch(search.trim()), 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const fetchPrefixes = useCallback(
		async (cursor = "", append = false) => {
			setLoading(true)
			setError("")
			try {
				const parsedASN = asn.trim() ? Number(asn) : undefined
				if (parsedASN !== undefined && (!Number.isInteger(parsedASN) || parsedASN <= 0))
					throw new Error(t`ASN must be a positive integer`)
				const data = await pb.send<ListResponse<ImportedPrefix>>(`/api/v1/address-imports/${item.id}/prefixes`, {
					query: {
						q: debouncedSearch || undefined,
						family: family === "all" ? undefined : family,
						country_code: country.trim() || undefined,
						operator: operator.trim() || undefined,
						asn: parsedASN,
						limit: pageSize,
						cursor: cursor || undefined,
					},
				})
				setPrefixes((current) => (append ? [...current, ...(data.items ?? [])] : (data.items ?? [])))
				setNextCursor(data.next_cursor ?? "")
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to load prefixes`)
			} finally {
				setLoading(false)
			}
		},
		[asn, country, debouncedSearch, family, item.id, operator, t]
	)

	useEffect(() => {
		fetchPrefixes()
	}, [fetchPrefixes])

	const lookup = async () => {
		if (!lookupIP.trim()) return
		setLoading(true)
		setError("")
		try {
			const data = await pb.send<ListResponse<ImportedPrefix>>(`/api/v1/address-imports/${item.id}/lookup`, {
				query: { ip: lookupIP.trim(), limit: 100 },
			})
			setPrefixes(data.items ?? [])
			setNextCursor("")
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Lookup failed`)
		} finally {
			setLoading(false)
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
			<div className="flex flex-wrap gap-2">
				<div className="relative min-w-60 flex-1">
					<SearchIcon className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
					<Input
						className="pl-9"
						value={search}
						onChange={(event) => setSearch(event.target.value)}
						placeholder={t`Search CIDR or location...`}
					/>
				</div>
				<Select value={family} onValueChange={setFamily}>
					<SelectTrigger className="w-28">
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="all">
							<Trans>All</Trans>
						</SelectItem>
						<SelectItem value="4">IPv4</SelectItem>
						<SelectItem value="6">IPv6</SelectItem>
					</SelectContent>
				</Select>
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
				<Button variant="outline" onClick={() => fetchPrefixes()}>
					<Trans>Reset</Trans>
				</Button>
			</div>
			{error ? <div className="text-sm text-destructive">{error}</div> : null}
			<PagedVTable
				records={records}
				columns={columns}
				loading={loading}
				emptyText={t`No imported prefixes found.`}
				showSearch={false}
				height={380}
			/>
			{nextCursor ? (
				<Button variant="outline" onClick={() => fetchPrefixes(nextCursor, true)}>
					<Trans>Load more</Trans>
				</Button>
			) : null}
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

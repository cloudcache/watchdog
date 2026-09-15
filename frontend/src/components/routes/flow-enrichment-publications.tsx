import { Trans, useLingui } from "@lingui/react/macro"
import { RefreshCwIcon, SaveIcon, SendIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { api, can } from "@/lib/api"

type ClassificationDraft = {
	home_province: string
	home_city: string
	home_isp_ids: number[]
	home_asns: number[]
	overseas_includes_hmt: boolean
	internal_policy: "count" | "drop"
	transit_policy: "count" | "drop"
}

type ClassificationProfile = {
	definition: ClassificationDraft
	definition_digest: string
	row_version: number
	updated_at?: string
}

type EnrichmentPublication = {
	id: string
	classification_version: number
	effective_from: string
	dimension_snapshot_id: string
	dimension_version: number
	dimension_checksum: string
	classification_checksum: string
	signing_key_id: string
	signed_at: string
	created_at: string
}

type PublicationPage = { items?: EnrichmentPublication[]; total?: number }
type FacetResponse = { items?: { value: string; label?: string; count?: number }[] }

type EnrichmentACK = {
	publication_id: string
	worker_id: string
	worker_name: string
	boot_id: string
	software_version: string
	state: string
	attempted_at: string
	downloaded_at?: string
	installed_at?: string
	error_code?: string
	error_message?: string
}

const emptyDraft: ClassificationDraft = {
	home_province: "",
	home_city: "",
	home_isp_ids: [],
	home_asns: [],
	overseas_includes_hmt: false,
	internal_policy: "count",
	transit_policy: "count",
}

const publicationFilterFields = [
	"id",
	"classification_version",
	"dimension_version",
	"dimension_snapshot_id",
	"effective_from",
	"dimension_checksum",
	"classification_checksum",
	"signing_key_id",
	"created_at",
] as const

const acknowledgementFilterFields = ["worker_id", "state", "software_version", "error_code"] as const

export default memo(function FlowEnrichmentPublications() {
	const { t } = useLingui()
	const [profile, setProfile] = useState<ClassificationDraft>(emptyDraft)
	const [profileVersion, setProfileVersion] = useState(0)
	const [ispIDs, setISpIDs] = useState("")
	const [asns, setASNs] = useState("")
	const [effectiveFrom, setEffectiveFrom] = useState(defaultEffectiveFrom)
	const [items, setItems] = useState<EnrichmentPublication[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [sort, setSort] = useState("classification_version:desc")
	const [filters, setFilters] = useState<Record<string, string>>({})
	const [acks, setACKs] = useState<EnrichmentACK[]>([])
	const [ackTotal, setACKTotal] = useState(0)
	const [ackPage, setACKPage] = useState(0)
	const [ackPageSize, setACKPageSize] = useState(25)
	const [ackSearch, setACKSearch] = useState("")
	const [debouncedACKSearch, setDebouncedACKSearch] = useState("")
	const [ackSort, setACKSort] = useState("attempted_at:desc")
	const [ackFilters, setACKFilters] = useState<Record<string, string>>({})
	const [ackLoading, setACKLoading] = useState(false)
	const [selectedPublication, setSelectedPublication] = useState("")
	const [loading, setLoading] = useState(true)
	const [working, setWorking] = useState(false)
	const [error, setError] = useState("")
	const [notice, setNotice] = useState("")
	const requestSequence = useRef(0)
	const canManage = can("address.manage")
	const canPublish = can("address.publish")

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setPage(0)
			setDebouncedSearch(search.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [search])

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setACKPage(0)
			setDebouncedACKSearch(ackSearch.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [ackSearch])

	const fetchProfile = useCallback(async () => {
		const data = await api.send<ClassificationProfile>("/api/v1/flow/classification-profile")
		const definition = data.definition ?? emptyDraft
		setProfile({ ...emptyDraft, ...definition })
		setISpIDs((definition.home_isp_ids ?? []).join(", "))
		setASNs((definition.home_asns ?? []).join(", "))
		setProfileVersion(data.row_version ?? 0)
	}, [])

	const fetchPublications = useCallback(async () => {
		const sequence = ++requestSequence.current
		const [sortField, order] = sort.split(":")
		setLoading(true)
		try {
			const filterQuery = Object.fromEntries(Object.entries(filters).filter(([, value]) => value))
			const data = await api.send<PublicationPage>("/api/v1/flow/enrichment-publications", {
				query: {
					...filterQuery,
					q: debouncedSearch || undefined,
					limit: pageSize,
					offset: page * pageSize || undefined,
					sort: sortField,
					order,
				},
			})
			if (sequence !== requestSequence.current) return
			setItems(data.items ?? [])
			setTotal(data.total ?? 0)
		} catch (cause) {
			if (sequence !== requestSequence.current) return
			setItems([])
			setTotal(0)
			setError(cause instanceof Error ? cause.message : t`Failed to load Flow enrichment publications`)
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [debouncedSearch, filters, page, pageSize, sort, t])

	useEffect(() => {
		Promise.all([fetchProfile(), fetchPublications()]).catch((cause) =>
			setError(cause instanceof Error ? cause.message : t`Failed to load Flow enrichment settings`)
		)
	}, [fetchProfile, fetchPublications, t])

	const saveProfile = async () => {
		setWorking(true)
		setError("")
		setNotice("")
		try {
			const definition: ClassificationDraft = {
				...profile,
				home_isp_ids: parsePositiveIntegers(ispIDs, 65_535, t`Home ISP IDs must be comma-separated positive integers`),
				home_asns: parsePositiveIntegers(asns, 4_294_967_295, t`Home ASNs must be comma-separated positive integers`),
			}
			const saved = await api.send<ClassificationProfile>("/api/v1/flow/classification-profile", {
				method: "PUT",
				headers: { "If-Match": `"${profileVersion}"` },
				body: definition,
			})
			setProfile(saved.definition)
			setProfileVersion(saved.row_version)
			setISpIDs(saved.definition.home_isp_ids.join(", "))
			setASNs(saved.definition.home_asns.join(", "))
			setNotice(t`Classification profile saved`)
		} catch (cause) {
			setError(cause instanceof Error ? cause.message : t`Failed to save classification profile`)
		} finally {
			setWorking(false)
		}
	}

	const publishPair = async () => {
		setWorking(true)
		setError("")
		setNotice("")
		try {
			const published = await api.send<EnrichmentPublication>("/api/v1/flow/enrichment-publications", {
				method: "POST",
				body: { effective_from: parseEffectiveFrom(effectiveFrom, t`Effective time must be a minute boundary`) },
			})
			setNotice(t`Flow enrichment version ${published.classification_version} published`)
			await fetchPublications()
		} catch (cause) {
			setError(cause instanceof Error ? cause.message : t`Failed to publish Flow enrichment version`)
		} finally {
			setWorking(false)
		}
	}

	const fetchACKs = useCallback(async () => {
		if (!selectedPublication) return
		const [sortField, order] = ackSort.split(":")
		setACKLoading(true)
		setError("")
		try {
			const filterQuery = Object.fromEntries(Object.entries(ackFilters).filter(([, value]) => value))
			const data = await api.send<{ items?: EnrichmentACK[]; total?: number }>(
				`/api/v1/flow/enrichment-publications/${encodeURIComponent(selectedPublication)}/acks`,
				{
					query: {
						...filterQuery,
						q: debouncedACKSearch || undefined,
						limit: ackPageSize,
						offset: ackPage * ackPageSize || undefined,
						sort: sortField,
						order,
					},
				}
			)
			setACKs(data.items ?? [])
			setACKTotal(data.total ?? 0)
		} catch (cause) {
			setACKs([])
			setACKTotal(0)
			setError(cause instanceof Error ? cause.message : t`Failed to load worker acknowledgements`)
		} finally {
			setACKLoading(false)
		}
	}, [ackFilters, ackPage, ackPageSize, ackSort, debouncedACKSearch, selectedPublication, t])

	useEffect(() => {
		fetchACKs()
	}, [fetchACKs])

	const openACKs = (publicationID: string) => {
		setACKs([])
		setACKTotal(0)
		setACKPage(0)
		setACKSearch("")
		setDebouncedACKSearch("")
		setACKFilters({})
		setSelectedPublication(publicationID)
	}

	const records = useMemo(
		() =>
			items.map((item) => ({
				id: item.id,
				classificationVersion: item.classification_version,
				dimensionVersion: item.dimension_version,
				dimensionSnapshot: item.dimension_snapshot_id,
				effective: formatDate(item.effective_from),
				dimensionChecksum: item.dimension_checksum,
				classificationChecksum: item.classification_checksum,
				signingKey: item.signing_key_id,
				created: formatDate(item.created_at),
				acks: t`View ACKs`,
			})),
		[items, t]
	)
	const columns = useMemo(
		() => [
			{ field: "id", filterField: "id", title: t`Publication`, width: 220, style: denseCellStyle() },
			{
				field: "classificationVersion",
				filterField: "classification_version",
				title: t`Classification version`,
				width: 150,
				style: denseCellStyle(),
			},
			{
				field: "dimensionVersion",
				filterField: "dimension_version",
				title: t`Address version`,
				width: 125,
				style: denseCellStyle(),
			},
			{
				field: "dimensionSnapshot",
				filterField: "dimension_snapshot_id",
				title: t`Address snapshot`,
				width: 210,
				style: denseCellStyle(),
			},
			{
				field: "effective",
				filterField: "effective_from",
				title: t`Effective from`,
				width: 180,
				style: denseCellStyle(),
			},
			{
				field: "dimensionChecksum",
				filterField: "dimension_checksum",
				title: t`Address checksum`,
				width: 260,
				style: denseCellStyle(),
			},
			{
				field: "classificationChecksum",
				filterField: "classification_checksum",
				title: t`Classification checksum`,
				width: 260,
				style: denseCellStyle(),
			},
			{
				field: "signingKey",
				filterField: "signing_key_id",
				title: t`Signing key`,
				width: 180,
				style: denseCellStyle(),
			},
			{ field: "created", filterField: "created_at", title: t`Created`, width: 180, style: denseCellStyle() },
			{
				field: "acks",
				title: t`Worker ACKs`,
				width: 110,
				filter: false,
				style: { ...denseCellStyle(), color: "#2563eb", cursor: "pointer" },
			},
		],
		[t]
	)
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(
		() => ({
			field: sortField,
			direction: sortDirection,
			fields: {
				id: "id",
				classificationVersion: "classification_version",
				dimensionVersion: "dimension_version",
				dimensionSnapshot: "dimension_snapshot_id",
				effective: "effective_from",
				dimensionChecksum: "dimension_checksum",
				classificationChecksum: "classification_checksum",
				signingKey: "signing_key_id",
				created: "created_at",
			},
			onSortChange: (field: string, direction: "asc" | "desc") => {
				setPage(0)
				setSort(`${field}:${direction}`)
			},
		}),
		[sortDirection, sortField]
	)
	const serverFiltering = useMemo(
		() => ({
			options: Object.fromEntries(publicationFilterFields.map((field) => [field, []])),
			selected: Object.fromEntries(
				publicationFilterFields.map((field) => [field, filters[field] ? [filters[field]] : []])
			),
			selection: Object.fromEntries(publicationFilterFields.map((field) => [field, "single" as const])),
			loadOptions: async (field: string, query: string, signal: AbortSignal) => {
				const data = await api.send<FacetResponse>("/api/v1/flow/enrichment-publications/facets", {
					query: { field, q: query || undefined, limit: 50 },
					signal,
				})
				return data.items ?? []
			},
			onColumnFilterChange: (field: string, values: unknown[]) => {
				setPage(0)
				setFilters((current) => ({ ...current, [field]: values.length > 0 ? String(values[0]) : "" }))
			},
			onClearAll: () => {
				setPage(0)
				setFilters({})
			},
		}),
		[filters]
	)
	const ackRecords = useMemo(
		() =>
			acks.map((item) => ({
				worker: item.worker_name || item.worker_id,
				state: item.state,
				boot: item.boot_id,
				software: item.software_version,
				attempted: formatDate(item.attempted_at),
				installed: item.installed_at ? formatDate(item.installed_at) : "—",
				error: [item.error_code, item.error_message].filter(Boolean).join(": ") || "—",
			})),
		[acks]
	)
	const ackColumns = useMemo(
		() => [
			{ field: "worker", filterField: "worker_id", title: t`Worker`, width: 180, style: denseCellStyle() },
			{ field: "state", filterField: "state", title: t`State`, width: 110, style: denseCellStyle() },
			{ field: "boot", title: t`Boot ID`, width: 160, filter: false, style: denseCellStyle() },
			{
				field: "software",
				filterField: "software_version",
				title: t`Software version`,
				width: 140,
				style: denseCellStyle(),
			},
			{ field: "attempted", title: t`Attempted`, width: 180, filter: false, style: denseCellStyle() },
			{ field: "installed", title: t`Installed`, width: 180, filter: false, style: denseCellStyle() },
			{ field: "error", filterField: "error_code", title: t`Error`, width: 280, style: denseCellStyle() },
		],
		[t]
	)
	const [ackSortField, ackSortDirection] = ackSort.split(":") as [string, "asc" | "desc"]
	const ackServerSorting = useMemo(
		() => ({
			field: ackSortField,
			direction: ackSortDirection,
			fields: {
				worker: "worker_name",
				state: "state",
				boot: "boot_id",
				software: "software_version",
				attempted: "attempted_at",
				installed: "installed_at",
				error: "error_code",
			},
			onSortChange: (field: string, direction: "asc" | "desc") => {
				setACKPage(0)
				setACKSort(`${field}:${direction}`)
			},
		}),
		[ackSortDirection, ackSortField]
	)
	const ackServerFiltering = useMemo(
		() => ({
			options: Object.fromEntries(acknowledgementFilterFields.map((field) => [field, []])),
			selected: Object.fromEntries(
				acknowledgementFilterFields.map((field) => [field, ackFilters[field] ? [ackFilters[field]] : []])
			),
			selection: Object.fromEntries(acknowledgementFilterFields.map((field) => [field, "single" as const])),
			loadOptions: async (field: string, query: string, signal: AbortSignal) => {
				const data = await api.send<FacetResponse>(
					`/api/v1/flow/enrichment-publications/${encodeURIComponent(selectedPublication)}/acks/facets`,
					{ query: { field, q: query || undefined, limit: 50 }, signal }
				)
				return data.items ?? []
			},
			onColumnFilterChange: (field: string, values: unknown[]) => {
				setACKPage(0)
				setACKFilters((current) => ({ ...current, [field]: values.length > 0 ? String(values[0]) : "" }))
			},
			onClearAll: () => {
				setACKPage(0)
				setACKFilters({})
			},
		}),
		[ackFilters, selectedPublication]
	)

	return (
		<section className="grid gap-4 border-t border-border pt-5">
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div>
					<h2 className="text-lg font-semibold">
						<Trans>Flow Classification Publication</Trans>
					</h2>
					<p className="text-sm text-muted-foreground">
						<Trans>
							The active approved address snapshot and this profile are signed and published as one worker version.
						</Trans>
					</p>
				</div>
				<Button
					variant="outline"
					size="sm"
					onClick={() => Promise.all([fetchProfile(), fetchPublications()])}
					disabled={working}
				>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Refresh</Trans>
				</Button>
			</div>
			<div className="grid gap-4 rounded-md border border-border bg-card p-4 lg:grid-cols-2">
				<div className="grid gap-3 sm:grid-cols-2">
					<Field
						id="flow-classification-home-province"
						label={t`Home province code`}
						value={profile.home_province}
						disabled={!canManage}
						onChange={(value) => setProfile((current) => ({ ...current, home_province: value }))}
					/>
					<Field
						id="flow-classification-home-city"
						label={t`Home city code`}
						value={profile.home_city}
						disabled={!canManage}
						onChange={(value) => setProfile((current) => ({ ...current, home_city: value }))}
					/>
					<Field
						id="flow-classification-home-isp-ids"
						label={t`Home ISP IDs`}
						value={ispIDs}
						disabled={!canManage}
						placeholder="1, 2"
						onChange={setISpIDs}
					/>
					<Field
						id="flow-classification-home-asns"
						label={t`Home ASNs`}
						value={asns}
						disabled={!canManage}
						placeholder="4134, 4812"
						onChange={setASNs}
					/>
					<PolicyField
						id="flow-classification-internal-policy"
						label={t`Internal traffic policy`}
						value={profile.internal_policy}
						disabled={!canManage}
						onChange={(value) => setProfile((current) => ({ ...current, internal_policy: value }))}
					/>
					<PolicyField
						id="flow-classification-transit-policy"
						label={t`Transit traffic policy`}
						value={profile.transit_policy}
						disabled={!canManage}
						onChange={(value) => setProfile((current) => ({ ...current, transit_policy: value }))}
					/>
					<label htmlFor="flow-classification-overseas-hmt" className="flex items-center gap-2 text-sm sm:col-span-2">
						<Checkbox
							id="flow-classification-overseas-hmt"
							checked={profile.overseas_includes_hmt}
							disabled={!canManage}
							onCheckedChange={(checked) =>
								setProfile((current) => ({ ...current, overseas_includes_hmt: checked === true }))
							}
						/>
						<Trans>Count Hong Kong, Macao and Taiwan as overseas</Trans>
					</label>
					{canManage ? (
						<div className="sm:col-span-2">
							<Button onClick={saveProfile} disabled={working}>
								<SaveIcon className="me-2 h-4 w-4" />
								<Trans>Save classification profile</Trans>
							</Button>
						</div>
					) : null}
				</div>
				<div className="grid content-start gap-3">
					<Label htmlFor="flow-enrichment-effective-from">
						<Trans>Effective from</Trans>
					</Label>
					<Input
						id="flow-enrichment-effective-from"
						type="datetime-local"
						value={effectiveFrom}
						onChange={(event) => setEffectiveFrom(event.target.value)}
						disabled={!canPublish}
					/>
					<p className="text-xs text-muted-foreground">
						<Trans>
							Publishing is immutable. Workers pull it asynchronously and keep the last-known-good pair for offline
							restart.
						</Trans>
					</p>
					{canPublish ? (
						<Button onClick={publishPair} disabled={working}>
							<SendIcon className="me-2 h-4 w-4" />
							<Trans>Publish Flow version pair</Trans>
						</Button>
					) : null}
				</div>
			</div>
			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			{notice ? <div className="rounded-md border border-green-500/30 p-3 text-sm text-green-700">{notice}</div> : null}
			<PagedVTable
				records={records}
				columns={columns}
				loading={loading}
				emptyText={t`No Flow enrichment publications found.`}
				searchPlaceholder={t`Search publication, snapshot, checksum, or signing key...`}
				searchValue={search}
				onSearchChange={setSearch}
				height={360}
				onCellClick={(record, field) => {
					if (field === "acks") openACKs(String(record.id))
				}}
				serverPagination={{
					page,
					pageSize,
					totalCount: total,
					onPageChange: setPage,
					onPageSizeChange: (value) => {
						setPage(0)
						setPageSize(value)
					},
				}}
				serverFiltering={serverFiltering}
				serverSorting={serverSorting}
			/>
			{selectedPublication ? (
				<div className="grid gap-3 rounded-md border border-border bg-card p-4">
					<div className="flex items-center justify-between gap-2">
						<h3 className="font-semibold">
							<Trans>Worker acknowledgements</Trans>
						</h3>
						<Button
							variant="ghost"
							size="sm"
							onClick={() => {
								setSelectedPublication("")
								setACKs([])
								setACKTotal(0)
							}}
						>
							<Trans>Close</Trans>
						</Button>
					</div>
					<PagedVTable
						records={ackRecords}
						columns={ackColumns}
						loading={ackLoading}
						emptyText={t`No worker acknowledgements found.`}
						searchPlaceholder={t`Search worker acknowledgements...`}
						searchValue={ackSearch}
						onSearchChange={setACKSearch}
						height={260}
						serverPagination={{
							page: ackPage,
							pageSize: ackPageSize,
							totalCount: ackTotal,
							onPageChange: setACKPage,
							onPageSizeChange: (value) => {
								setACKPage(0)
								setACKPageSize(value)
							},
						}}
						serverFiltering={ackServerFiltering}
						serverSorting={ackServerSorting}
					/>
				</div>
			) : null}
		</section>
	)
})

function Field({
	id,
	label,
	value,
	disabled,
	placeholder,
	onChange,
}: {
	id: string
	label: string
	value: string
	disabled: boolean
	placeholder?: string
	onChange: (value: string) => void
}) {
	return (
		<div className="grid gap-2">
			<Label htmlFor={id}>{label}</Label>
			<Input
				id={id}
				value={value}
				disabled={disabled}
				placeholder={placeholder}
				onChange={(event) => onChange(event.target.value)}
			/>
		</div>
	)
}

function PolicyField({
	id,
	label,
	value,
	disabled,
	onChange,
}: {
	id: string
	label: string
	value: "count" | "drop"
	disabled: boolean
	onChange: (value: "count" | "drop") => void
}) {
	return (
		<div className="grid gap-2">
			<Label htmlFor={id}>{label}</Label>
			<Select value={value} disabled={disabled} onValueChange={(next) => onChange(next as "count" | "drop")}>
				<SelectTrigger id={id}>
					<SelectValue />
				</SelectTrigger>
				<SelectContent>
					<SelectItem value="count">
						<Trans>Count</Trans>
					</SelectItem>
					<SelectItem value="drop">
						<Trans>Drop</Trans>
					</SelectItem>
				</SelectContent>
			</Select>
		</div>
	)
}

function parsePositiveIntegers(value: string, maximum: number, errorMessage: string) {
	if (!value.trim()) return []
	const numbers = value.split(",").map((part) => Number(part.trim()))
	if (numbers.some((item) => !Number.isSafeInteger(item) || item < 1 || item > maximum)) throw new Error(errorMessage)
	return [...new Set(numbers)].sort((left, right) => left - right)
}

function defaultEffectiveFrom() {
	const date = new Date(Math.ceil(Date.now() / 60_000) * 60_000 + 60_000)
	const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000)
	return local.toISOString().slice(0, 16)
}

function parseEffectiveFrom(value: string, errorMessage: string) {
	const date = new Date(value)
	if (!value || Number.isNaN(date.getTime()) || date.getSeconds() !== 0 || date.getMilliseconds() !== 0)
		throw new Error(errorMessage)
	return date.toISOString()
}

function denseCellStyle() {
	return { padding: [8, 10, 8, 10] as [number, number, number, number], fontSize: 13 }
}

function formatDate(value: string) {
	const date = new Date(value)
	return Number.isNaN(date.getTime()) ? value || "—" : date.toLocaleString()
}

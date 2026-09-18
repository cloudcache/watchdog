import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { RefreshCwIcon, SaveIcon, SendIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Button } from "@/components/ui/button"
import { $router } from "@/components/router"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { api, can } from "@/lib/api"

type ClassificationDraft = {
	device_profiles: ClassificationDeviceProfile[]
}

type ClassificationDeviceProfile = {
	device_id: string
	source_prefix_ids: string[]
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

type AddressPrefix = {
	id: string
	cidr: string
	labels?: Record<string, string>
	geo_leaf_id?: string
	operator_id?: string
}

type ListResponse<T> = { items?: T[] }

type NetworkDevice = {
	id: string
	name?: string
	host: string
}

type FlowExporter = {
	device_id: string
	device_name: string
	device_host: string
	enabled: boolean
}

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
	device_profiles: [],
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

export default memo(function FlowEnrichmentPublications({ onChanged }: { onChanged?: () => void | Promise<void> }) {
	const { t } = useLingui()
	const [profile, setProfile] = useState<ClassificationDraft>(emptyDraft)
	const [profileVersion, setProfileVersion] = useState(0)
	const [sourcePrefixes, setSourcePrefixes] = useState<AddressPrefix[]>([])
	const [devices, setDevices] = useState<NetworkDevice[]>([])
	const [selectedDeviceID, setSelectedDeviceID] = useState("")
	const [referencesLoading, setReferencesLoading] = useState(true)
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
		setProfile({ ...emptyDraft, ...definition, device_profiles: definition.device_profiles ?? [] })
		setProfileVersion(data.row_version ?? 0)
	}, [])

	const fetchReferences = useCallback(async () => {
		setReferencesLoading(true)
		try {
			const [prefixResult, exporterResult] = await Promise.all([
				api.send<ListResponse<AddressPrefix>>("/api/v1/address-prefixes", {
					query: { limit: 500, sort: "cidr", order: "asc" },
				}),
				api.send<ListResponse<FlowExporter>>("/api/v1/flow/devices", {
					query: { enabled: true, limit: 500, sort: "device", order: "asc" },
				}),
			])
			setSourcePrefixes(prefixResult.items ?? [])
			const uniqueDevices = new Map<string, NetworkDevice>()
			for (const exporter of exporterResult.items ?? []) {
				if (!uniqueDevices.has(exporter.device_id)) {
					uniqueDevices.set(exporter.device_id, {
						id: exporter.device_id,
						name: exporter.device_name,
						host: exporter.device_host,
					})
				}
			}
			setDevices([...uniqueDevices.values()])
		} catch (cause) {
			setError(cause instanceof Error ? cause.message : t`Failed to load classification choices`)
		} finally {
			setReferencesLoading(false)
		}
	}, [t])

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
		Promise.all([fetchProfile(), fetchPublications(), fetchReferences()]).catch((cause) =>
			setError(cause instanceof Error ? cause.message : t`Failed to load Flow enrichment settings`)
		)
	}, [fetchProfile, fetchPublications, fetchReferences, t])

	useEffect(() => {
		if (selectedDeviceID) return
		setSelectedDeviceID(profile.device_profiles[0]?.device_id ?? devices[0]?.id ?? "")
	}, [devices, profile.device_profiles, selectedDeviceID])

	const selectedDeviceProfile = useMemo(
		() => profile.device_profiles.find((item) => item.device_id === selectedDeviceID),
		[profile.device_profiles, selectedDeviceID]
	)
	const updateSelectedDeviceProfile = (changes: Partial<ClassificationDeviceProfile>) => {
		if (!selectedDeviceID) return
		setProfile((current) => {
			const existing = current.device_profiles.find((item) => item.device_id === selectedDeviceID) ?? {
				device_id: selectedDeviceID,
				source_prefix_ids: [],
			}
			const next = { ...existing, ...changes }
			return {
				...current,
				device_profiles: [...current.device_profiles.filter((item) => item.device_id !== selectedDeviceID), next].sort(
					(left, right) => left.device_id.localeCompare(right.device_id)
				),
			}
		})
	}

	const removeSelectedDeviceProfile = () => {
		if (!selectedDeviceID) return
		setProfile((current) => ({
			...current,
			device_profiles: current.device_profiles.filter((item) => item.device_id !== selectedDeviceID),
		}))
	}

	const saveProfile = async () => {
		setWorking(true)
		setError("")
		setNotice("")
		try {
			if (profile.device_profiles.length === 0) throw new Error(t`Configure at least one Flow observation device`)
			for (const item of profile.device_profiles) {
				if (item.source_prefix_ids.length === 0)
					throw new Error(t`Select at least one customer source prefix for every configured device`)
			}
			const definition: ClassificationDraft = {
				...profile,
			}
			const saved = await api.send<ClassificationProfile>("/api/v1/flow/classification-profile", {
				method: "PUT",
				headers: { "If-Match": `"${profileVersion}"` },
				body: definition,
			})
			setProfile(saved.definition)
			setProfileVersion(saved.row_version)
			setNotice(t`Classification profile saved`)
			await onChanged?.()
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
			await onChanged?.()
		} catch (cause) {
			setError(cause instanceof Error ? cause.message : t`Failed to publish Flow enrichment version`)
		} finally {
			setWorking(false)
		}
	}

	const bootstrapPair = async () => {
		setWorking(true)
		setError("")
		setNotice("")
		try {
			const published = await api.send<EnrichmentPublication>("/api/v1/flow/enrichment-publications/bootstrap", {
				method: "POST",
				body: {},
			})
			setNotice(t`Unclassified Flow ingestion version ${published.classification_version} published`)
			await fetchPublications()
			await onChanged?.()
		} catch (cause) {
			setError(cause instanceof Error ? cause.message : t`Failed to publish unclassified Flow ingestion version`)
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
					onClick={() => Promise.all([fetchProfile(), fetchPublications(), fetchReferences()])}
					disabled={working}
				>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Refresh</Trans>
				</Button>
			</div>
			<div className="grid gap-4 rounded-md border border-border bg-card p-4 lg:grid-cols-2">
				<div className="grid gap-3 sm:grid-cols-2">
					<div className="grid gap-2 sm:col-span-2">
						<div className="flex items-center justify-between gap-2">
							<Label htmlFor="flow-classification-device">
								<Trans>Flow observation device</Trans>
							</Label>
							<span className="text-xs text-muted-foreground">
								<Trans>{profile.device_profiles.length} devices configured</Trans>
							</span>
						</div>
						<Select value={selectedDeviceID || undefined} disabled={!canManage || referencesLoading} onValueChange={setSelectedDeviceID}>
							<SelectTrigger id="flow-classification-device">
								<SelectValue placeholder={t`Select the device that exports Flow`} />
							</SelectTrigger>
							<SelectContent>
								{devices.map((device) => (
									<SelectItem key={device.id} value={device.id}>
										{deviceLabel(device)}
									</SelectItem>
								))}
							</SelectContent>
						</Select>
						<p className="text-xs text-muted-foreground">
							<Trans>Select this device's customer source prefixes. Province, city, and operator are read from the paired address snapshot.</Trans>
						</p>
						{selectedDeviceProfile && canManage ? (
							<Button type="button" variant="outline" size="sm" className="w-fit" onClick={removeSelectedDeviceProfile}>
								<Trans>Remove device classification</Trans>
							</Button>
						) : null}
					</div>
					<div className="grid gap-2 sm:col-span-2">
						<Label>
							<Trans>Customer source prefixes</Trans>
						</Label>
						<p className="text-xs text-muted-foreground">
							<Trans>The same CIDRs determine inbound and outbound direction. Their address-library attributes determine the six traffic categories.</Trans>
						</p>
						<div className="grid max-h-64 gap-2 overflow-y-auto rounded-md border border-border p-3 sm:grid-cols-2">
							{sourcePrefixes.map((prefix) => {
								const checked = selectedDeviceProfile?.source_prefix_ids.includes(prefix.id) ?? false
								const checkboxID = `flow-classification-prefix-${selectedDeviceID}-${prefix.id}`
								return (
									<label key={prefix.id} htmlFor={checkboxID} className="flex items-start gap-2 text-sm">
										<Checkbox
											id={checkboxID}
											checked={checked}
											disabled={!canManage || referencesLoading || !selectedDeviceID}
											onCheckedChange={(next) =>
												updateSelectedDeviceProfile({
													source_prefix_ids: next
														? [...(selectedDeviceProfile?.source_prefix_ids ?? []), prefix.id].sort()
														: (selectedDeviceProfile?.source_prefix_ids ?? []).filter((id) => id !== prefix.id),
												})
											}
										/>
										<span className="grid">
											<span className="font-medium">{prefix.cidr}</span>
											<span className="text-xs text-muted-foreground">{prefixSummary(prefix)}</span>
										</span>
									</label>
								)
							})}
							{!referencesLoading && sourcePrefixes.length === 0 ? (
								<p className="text-sm text-muted-foreground">
									<Trans>No customer source prefixes are available in the address library.</Trans>
								</p>
							) : null}
						</div>
					</div>
					<p className="rounded-md border border-amber-500/30 bg-amber-500/5 p-3 text-xs text-amber-800 sm:col-span-2">
						<Trans>Maintain the CIDR, geography, and operator in the address library, then select the CIDR here for each Flow device.</Trans>{" "}
						<a className="font-medium underline" href={getPagePath($router, "address_prefixes")}>
							<Trans>Manage address prefixes</Trans>
						</a>
						. <Trans>Rebuild and activate the address snapshot after changing address attributes.</Trans>
					</p>
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
					{profileVersion === 0 ? (
						<div className="grid gap-2 rounded-md border border-amber-500/30 bg-amber-500/5 p-3 text-sm text-amber-700">
							<p>
								<Trans>
									Customer source CIDRs are not configured. Start ingestion now as unclassified traffic, then publish a
									precise version after configuring each Flow device.
								</Trans>
							</p>
							{canPublish && total === 0 ? (
								<Button type="button" variant="outline" onClick={bootstrapPair} disabled={working}>
									<SendIcon className="me-2 h-4 w-4" />
									<Trans>Start unclassified Flow ingestion</Trans>
								</Button>
							) : null}
						</div>
					) : null}
					{canPublish ? (
						<Button onClick={publishPair} disabled={working || profileVersion === 0}>
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

function deviceLabel(device: NetworkDevice) {
	const name = device.name || device.host
	return name === device.host ? name : `${name} (${device.host})`
}

function prefixSummary(prefix: AddressPrefix) {
	const labels = Object.entries(prefix.labels ?? {})
		.filter(([key]) => key !== "flow")
		.map(([key, value]) => `${key}=${value}`)
	if (prefix.geo_leaf_id) labels.push(`geo=${prefix.geo_leaf_id}`)
	if (prefix.operator_id) labels.push(`operator=${prefix.operator_id}`)
	return labels.join(" · ") || "—"
}

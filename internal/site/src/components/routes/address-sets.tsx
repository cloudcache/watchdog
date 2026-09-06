import { Trans, useLingui } from "@lingui/react/macro"
import { LayersIcon, PlusIcon, RefreshCwIcon, SearchIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { parseAddressEntries, parseSetLabelSelector } from "@/lib/address-set-form"
import { pb } from "@/lib/api"

type AddressSet = {
	id: string
	name: string
	description: string
	selector: {
		labels?: Record<string, string[]>
		geo_node_ids?: string[]
		operator_ids?: string[]
		asns?: number[]
		families?: number[]
	}
	explicit_members: string[]
	explicit_exclude_members: string[]
	include_set_ids: string[]
	exclude_set_ids: string[]
	match_direction: "in" | "out" | "both"
	enabled: boolean
	row_version: number
}

type AddressSetList = { items?: AddressSet[]; next_cursor?: string }

const pageSize = 100
const emptyForm = {
	name: "",
	description: "",
	labels: "",
	members: "",
	excludeMembers: "",
	includeSetIDs: "",
	excludeSetIDs: "",
	direction: "both" as "in" | "out" | "both",
	enabled: true,
}

export default memo(function AddressSets() {
	const { t } = useLingui()
	const [sets, setSets] = useState<AddressSet[]>([])
	const [nextCursor, setNextCursor] = useState("")
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [direction, setDirection] = useState("all")
	const [enabled, setEnabled] = useState("all")
	const [loading, setLoading] = useState(true)
	const [loadingMore, setLoadingMore] = useState(false)
	const [error, setError] = useState("")
	const [showForm, setShowForm] = useState(false)
	const [form, setForm] = useState(emptyForm)

	useEffect(() => {
		const timer = window.setTimeout(() => setDebouncedSearch(search.trim()), 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const fetchPage = useCallback(
		async (cursor: string, append: boolean) => {
			append ? setLoadingMore(true) : setLoading(true)
			setError("")
			try {
				const data = await pb.send<AddressSetList>("/api/v1/address-sets", {
					query: {
						q: debouncedSearch || undefined,
						match_direction: direction === "all" ? undefined : direction,
						enabled: enabled === "all" ? undefined : enabled,
						limit: pageSize,
						cursor: cursor || undefined,
					},
				})
				setSets((current) => (append ? [...current, ...(data.items ?? [])] : (data.items ?? [])))
				setNextCursor(data.next_cursor ?? "")
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to load`)
			} finally {
				append ? setLoadingMore(false) : setLoading(false)
			}
		},
		[debouncedSearch, direction, enabled, t]
	)

	useEffect(() => {
		fetchPage("", false)
	}, [fetchPage])

	const add = async () => {
		if (!form.name.trim()) return
		try {
			const labels = parseSetLabelSelector(form.labels)
			await pb.send("/api/v1/address-sets", {
				method: "POST",
				body: {
					name: form.name.trim(),
					description: form.description.trim(),
					selector: Object.keys(labels).length ? { labels } : {},
					explicit_members: parseAddressEntries(form.members),
					explicit_exclude_members: parseAddressEntries(form.excludeMembers),
					include_set_ids: parseAddressEntries(form.includeSetIDs),
					exclude_set_ids: parseAddressEntries(form.excludeSetIDs),
					match_direction: form.direction,
					enabled: form.enabled,
				},
			})
			setForm(emptyForm)
			setShowForm(false)
			await fetchPage("", false)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to create`)
		}
	}

	const remove = useCallback(
		async (record: Record<string, unknown>) => {
			const id = String(record.id ?? "")
			const rowVersion = Number(record.rowVersion ?? 0)
			if (!id || !rowVersion || !confirm(t`Delete this address set?`)) return
			try {
				await pb.send(`/api/v1/address-sets/${id}`, {
					method: "DELETE",
					headers: { "If-Match": `"${rowVersion}"` },
				})
				await fetchPage("", false)
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to delete`)
			}
		},
		[fetchPage, t]
	)

	const records = useMemo(
		() =>
			sets.map((set) => ({
				id: set.id,
				name: set.name,
				description: set.description || "—",
				selector: formatSelector(set.selector),
				members: set.explicit_members?.join(", ") || "—",
				excludes:
					[...(set.explicit_exclude_members ?? []), ...(set.exclude_set_ids ?? []).map((id) => `set:${id}`)].join(
						", "
					) || "—",
				includes: set.include_set_ids?.join(", ") || "—",
				direction: set.match_direction,
				status: set.enabled ? t`Enabled` : t`Disabled`,
				action: t`Delete`,
				rowVersion: set.row_version,
			})),
		[sets, t]
	)
	const columns = useMemo(
		() => [
			{ field: "name", title: t`Name`, width: 170, style: denseCellStyle() },
			{ field: "description", title: t`Description`, width: 210, style: denseCellStyle() },
			{ field: "selector", title: t`Selector`, width: 290, style: denseCellStyle() },
			{ field: "members", title: t`Members`, width: 220, style: denseCellStyle() },
			{ field: "includes", title: t`Included sets`, width: 180, style: denseCellStyle() },
			{ field: "excludes", title: t`Exclusions`, width: 220, style: denseCellStyle() },
			{ field: "direction", title: t`Direction`, width: 100, style: denseCellStyle() },
			{ field: "status", title: t`Status`, width: 100, style: denseCellStyle() },
			{
				field: "action",
				title: t`Actions`,
				width: 90,
				filter: false,
				style: { ...denseCellStyle(), color: "#dc2626", cursor: "pointer" },
			},
		],
		[t]
	)

	return (
		<div className="grid gap-4">
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<LayersIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Address Sets</Trans>
					</h1>
				</div>
				<div className="flex gap-2">
					<Button variant="outline" size="sm" onClick={() => fetchPage("", false)} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
					<Button size="sm" onClick={() => setShowForm((visible) => !visible)}>
						<PlusIcon className="me-2 h-4 w-4" />
						<Trans>Add Set</Trans>
					</Button>
				</div>
			</div>

			<div className="flex flex-wrap items-center gap-2">
				<div className="relative min-w-64 max-w-sm flex-1">
					<SearchIcon className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
					<Input
						value={search}
						onChange={(event) => setSearch(event.target.value)}
						placeholder={t`Search name or description...`}
						className="pl-9"
					/>
				</div>
				<Select value={direction} onValueChange={setDirection}>
					<SelectTrigger className="w-36">
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="all">
							<Trans>All directions</Trans>
						</SelectItem>
						<SelectItem value="in">
							<Trans>In</Trans>
						</SelectItem>
						<SelectItem value="out">
							<Trans>Out</Trans>
						</SelectItem>
						<SelectItem value="both">
							<Trans>Both</Trans>
						</SelectItem>
					</SelectContent>
				</Select>
				<Select value={enabled} onValueChange={setEnabled}>
					<SelectTrigger className="w-32">
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="all">
							<Trans>All states</Trans>
						</SelectItem>
						<SelectItem value="true">
							<Trans>Enabled</Trans>
						</SelectItem>
						<SelectItem value="false">
							<Trans>Disabled</Trans>
						</SelectItem>
					</SelectContent>
				</Select>
			</div>

			{showForm ? (
				<div className="grid gap-3 rounded-md border border-border bg-card p-4 md:grid-cols-2">
					<div className="grid gap-2">
						<Label>
							<Trans>Name</Trans>
						</Label>
						<Input
							value={form.name}
							onChange={(event) => setForm({ ...form, name: event.target.value })}
							placeholder="电信客户"
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Description</Trans>
						</Label>
						<Input
							value={form.description}
							onChange={(event) => setForm({ ...form, description: event.target.value })}
							placeholder="电信客户网段"
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Label selector</Trans> (key=value1|value2)
						</Label>
						<Input
							value={form.labels}
							onChange={(event) => setForm({ ...form, labels: event.target.value })}
							placeholder="provider=电信,type=客户"
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Explicit members</Trans>
						</Label>
						<Input
							value={form.members}
							onChange={(event) => setForm({ ...form, members: event.target.value })}
							placeholder="10.0.0.0/8, 2001:db8::/32"
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Excluded members</Trans>
						</Label>
						<Input
							value={form.excludeMembers}
							onChange={(event) => setForm({ ...form, excludeMembers: event.target.value })}
							placeholder="10.10.0.0/16"
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Included set IDs</Trans>
						</Label>
						<Input
							value={form.includeSetIDs}
							onChange={(event) => setForm({ ...form, includeSetIDs: event.target.value })}
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Excluded set IDs</Trans>
						</Label>
						<Input
							value={form.excludeSetIDs}
							onChange={(event) => setForm({ ...form, excludeSetIDs: event.target.value })}
						/>
					</div>
					<div className="grid gap-2">
						<Label>
							<Trans>Direction</Trans>
						</Label>
						<Select
							value={form.direction}
							onValueChange={(value: "in" | "out" | "both") => setForm({ ...form, direction: value })}
						>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="in">
									<Trans>In</Trans>
								</SelectItem>
								<SelectItem value="out">
									<Trans>Out</Trans>
								</SelectItem>
								<SelectItem value="both">
									<Trans>Both</Trans>
								</SelectItem>
							</SelectContent>
						</Select>
					</div>
					<label className="flex items-center gap-2 self-end pb-2 text-sm">
						<input
							type="checkbox"
							checked={form.enabled}
							onChange={(event) => setForm({ ...form, enabled: event.target.checked })}
						/>
						<Trans>Enabled</Trans>
					</label>
					<div className="text-xs text-muted-foreground md:col-span-2">
						<Trans>
							Result = selector union explicit members union included sets, minus excluded members and excluded sets.
						</Trans>
					</div>
					<div className="flex gap-2 md:col-span-2">
						<Button size="sm" onClick={add}>
							<Trans>Add</Trans>
						</Button>
						<Button variant="ghost" size="sm" onClick={() => setShowForm(false)}>
							<Trans>Cancel</Trans>
						</Button>
					</div>
				</div>
			) : null}

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			<div className="overflow-hidden rounded-md border border-border bg-card">
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={t`No address sets found.`}
					showSearch={false}
					height={560}
					onCellClick={(record, field) => {
						if (field === "action") remove(record)
					}}
				/>
			</div>
			{nextCursor ? (
				<div className="flex justify-center">
					<Button variant="outline" size="sm" onClick={() => fetchPage(nextCursor, true)} disabled={loadingMore}>
						{loadingMore ? <Trans>Loading...</Trans> : <Trans>Load more</Trans>}
					</Button>
				</div>
			) : null}
		</div>
	)
})

function formatSelector(selector: AddressSet["selector"]) {
	const parts: string[] = []
	for (const [key, values] of Object.entries(selector.labels ?? {})) parts.push(`${key}=${values.join("|")}`)
	if (selector.geo_node_ids?.length) parts.push(`geo:${selector.geo_node_ids.join("|")}`)
	if (selector.operator_ids?.length) parts.push(`operator:${selector.operator_ids.join("|")}`)
	if (selector.asns?.length) parts.push(`asn:${selector.asns.join("|")}`)
	if (selector.families?.length) parts.push(`IPv${selector.families.join("|IPv")}`)
	return parts.join(", ") || "—"
}

function denseCellStyle() {
	return { padding: [8, 10, 8, 10], textBaseline: "middle", autoWrapText: false }
}

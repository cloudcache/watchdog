import { Trans, useLingui } from "@lingui/react/macro"
import { GlobeIcon, PencilIcon, PlusIcon, RefreshCwIcon, SearchIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useState } from "react"
import { AddressReferencePicker } from "@/components/address-reference-picker"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
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

type AddressPrefixList = { items?: AddressPrefix[]; next_cursor?: string }

const pageSize = 100
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
	const [prefixes, setPrefixes] = useState<AddressPrefix[]>([])
	const [nextCursor, setNextCursor] = useState("")
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [family, setFamily] = useState("all")
	const [source, setSource] = useState("")
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
				const data = await pb.send<AddressPrefixList>("/api/v1/address-prefixes", {
					query: {
						q: debouncedSearch || undefined,
						family: family === "all" ? undefined : family,
						source: source.trim() || undefined,
						limit: pageSize,
						cursor: cursor || undefined,
					},
				})
				setPrefixes((current) => (append ? [...current, ...(data.items ?? [])] : (data.items ?? [])))
				setNextCursor(data.next_cursor ?? "")
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to load`)
			} finally {
				append ? setLoadingMore(false) : setLoading(false)
			}
		},
		[debouncedSearch, family, source, t]
	)

	useEffect(() => {
		fetchPage("", false)
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
			await fetchPage("", false)
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
				await fetchPage("", false)
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to delete`)
			}
		},
		[fetchPage, t]
	)

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

	return (
		<div className="grid gap-4">
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<GlobeIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Address Prefixes</Trans>
					</h1>
				</div>
				<div className="flex gap-2">
					<Button variant="outline" size="sm" onClick={() => fetchPage("", false)} disabled={loading}>
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

			<div className="flex flex-wrap items-center gap-2">
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
				<Input
					className="w-40"
					value={source}
					onChange={(event) => setSource(event.target.value)}
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

			<div className="overflow-hidden rounded-md border border-border bg-card">
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={t`No prefixes found.`}
					showSearch={false}
					height={560}
					onCellClick={(record, field) => {
						if (field === "edit") edit(record)
						if (field === "remove") remove(record)
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

function denseCellStyle() {
	return { padding: [8, 10, 8, 10], textBaseline: "middle", autoWrapText: false }
}

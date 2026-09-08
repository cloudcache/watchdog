import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, PlusIcon, RefreshCwIcon, SaveIcon, ShieldCheckIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, Link } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { api } from "@/lib/api"
import {
	optionalPositive,
	optionalRatio,
	parseIdentifiers,
	parsePositiveIntegers,
	type VPNRuleMatchInput,
} from "@/lib/flow-vpn-rule-model"
import type { ColumnDefine, ServerFilterOption } from "@/lib/vtable"
import { cn } from "@/lib/utils"

type VPNRule = {
	id: string
	name: string
	kind: string
	match: VPNRuleMatchInput
	effect: string
	weight: number
	priority: number
	status: string
	row_version: number
	updated_at: string
}

type RuleResponse = { items?: VPNRule[]; total?: number }
type SortDirection = "asc" | "desc"
type RuleForm = {
	name: string
	kind: string
	effect: string
	weight: string
	priority: string
	status: string
	remotePorts: string
	protocols: string
	remoteASNs: string
	remotePrefixIDs: string
	remoteCountries: string
	transportHints: string
	minDurationMS: string
	minTotalBytes: string
	minFlowRecords: string
	minActiveBuckets: string
	minSymmetryRatio: string
	minDominanceRatio: string
}

const emptyForm: RuleForm = {
	name: "",
	kind: "passive",
	effect: "score",
	weight: "10",
	priority: "0",
	status: "draft",
	remotePorts: "",
	protocols: "",
	remoteASNs: "",
	remotePrefixIDs: "",
	remoteCountries: "",
	transportHints: "",
	minDurationMS: "",
	minTotalBytes: "",
	minFlowRecords: "",
	minActiveBuckets: "",
	minSymmetryRatio: "",
	minDominanceRatio: "",
}

const filterOptions: Record<string, ServerFilterOption[]> = {
	kind: ["passive", "intelligence", "probe"].map(valueOption),
	effect: ["score", "allow", "suppress"].map(valueOption),
	status: ["draft", "active", "suspended", "retired"].map(valueOption),
}

export default memo(() => {
	const { t } = useLingui()
	const [rules, setRules] = useState<VPNRule[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [query, setQuery] = useState("")
	const [filters, setFilters] = useState<Record<string, unknown[]>>({})
	const [sortField, setSortField] = useState("updated_at")
	const [sortDirection, setSortDirection] = useState<SortDirection>("desc")
	const [editing, setEditing] = useState<VPNRule | null>(null)
	const [form, setForm] = useState<RuleForm>(emptyForm)
	const [showForm, setShowForm] = useState(false)
	const [loading, setLoading] = useState(true)
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")
	const requestSequence = useRef(0)

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setPage(0)
			setQuery(search.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const refresh = useCallback(async () => {
		const sequence = ++requestSequence.current
		setLoading(true)
		setError("")
		const params = new URLSearchParams({
			limit: String(pageSize),
			offset: String(page * pageSize),
			sort: sortField,
			order: sortDirection,
		})
		if (query) params.set("q", query)
		for (const field of ["kind", "effect", "status"]) {
			const value = filters[field]?.[0]
			if (value) params.set(field, String(value))
		}
		try {
			const data = await api.send<RuleResponse>(`/api/v1/flow/vpn/rules?${params}`, {})
			if (sequence !== requestSequence.current) return
			setRules(data.items ?? [])
			setTotal(data.total ?? 0)
		} catch (reason) {
			if (sequence !== requestSequence.current) return
			setRules([])
			setTotal(0)
			setError(reason instanceof Error ? reason.message : t`Failed to load VPN rules`)
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [filters, page, pageSize, query, sortDirection, sortField, t])

	useEffect(() => {
		document.title = `${t`VPN Rules`} / Watchdog`
		refresh()
	}, [refresh, t])

	const columns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "name", title: t`Name`, width: 230 },
			{ field: "kind", filterField: "kind", title: t`Kind`, width: 130 },
			{ field: "effect", filterField: "effect", title: t`Effect`, width: 110 },
			{ field: "weight", title: t`Weight`, width: 90 },
			{ field: "priority", title: t`Priority`, width: 90 },
			{ field: "status", filterField: "status", title: t`Status`, width: 120 },
			{ field: "updated", title: t`Updated`, width: 180 },
		],
		[t]
	)
	const records = useMemo(
		() => rules.map((rule) => ({ ...rule, updated: new Date(rule.updated_at).toLocaleString(), rule })),
		[rules]
	)
	const serverFiltering = useMemo(
		() => ({
			options: filterOptions,
			selected: filters,
			onColumnFilterChange: (field: string, values: unknown[]) => {
				setPage(0)
				setFilters((current) => ({ ...current, [field]: values.slice(-1) }))
			},
			onClearAll: () => {
				setPage(0)
				setFilters({})
			},
		}),
		[filters]
	)
	const serverSorting = useMemo(
		() => ({
			field: sortField,
			direction: sortDirection,
			fields: {
				name: "name",
				kind: "kind",
				effect: "effect",
				weight: "weight",
				priority: "priority",
				status: "status",
				updated: "updated_at",
			},
			onSortChange: (field: string, direction: SortDirection) => {
				setPage(0)
				setSortField(field)
				setSortDirection(direction)
			},
		}),
		[sortDirection, sortField]
	)

	const beginCreate = () => {
		setEditing(null)
		setForm(emptyForm)
		setShowForm(true)
	}
	const beginEdit = (rule: VPNRule) => {
		setEditing(rule)
		setForm(ruleToForm(rule))
		setShowForm(true)
	}
	const save = async () => {
		setSaving(true)
		setError("")
		try {
			const body = formToRequest(form)
			await api.send(editing ? `/api/v1/flow/vpn/rules/${editing.id}` : "/api/v1/flow/vpn/rules", {
				method: editing ? "PATCH" : "POST",
				headers: editing ? { "If-Match": `"${editing.row_version}"` } : undefined,
				body,
			})
			setShowForm(false)
			setEditing(null)
			await refresh()
		} catch (reason) {
			setError(reason instanceof Error ? reason.message : t`Failed to save VPN rule`)
		} finally {
			setSaving(false)
		}
	}
	const remove = async () => {
		if (!editing || !window.confirm(t`Delete this VPN rule?`)) return
		setSaving(true)
		setError("")
		try {
			await api.send(`/api/v1/flow/vpn/rules/${editing.id}`, {
				method: "DELETE",
				headers: { "If-Match": `"${editing.row_version}"` },
			})
			setShowForm(false)
			setEditing(null)
			await refresh()
		} catch (reason) {
			setError(reason instanceof Error ? reason.message : t`Failed to delete VPN rule`)
		} finally {
			setSaving(false)
		}
	}

	return (
		<div className="grid gap-4">
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<ShieldCheckIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<div>
						<h1 className="text-xl font-semibold">
							<Trans>VPN Rules</Trans> ({total})
						</h1>
						<p className="text-sm text-muted-foreground">
							<Trans>Editable drafts. A rule is usable only after an immutable publication is installed.</Trans>
						</p>
					</div>
				</div>
				<div className="flex gap-2">
					<Link className={cn(buttonVariants({ variant: "outline" }))} href={getPagePath($router, "flow_vpn")}>
						<ArrowLeftIcon className="me-2 h-4 w-4" />
						<Trans>Findings</Trans>
					</Link>
					<Button variant="outline" onClick={refresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
					<Button onClick={beginCreate}>
						<PlusIcon className="me-2 h-4 w-4" />
						<Trans>Add rule</Trans>
					</Button>
				</div>
			</div>

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			<div className="overflow-hidden rounded-md border border-border bg-card p-3">
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={t`No VPN rule drafts found.`}
					searchPlaceholder={t`Search rule name or ID...`}
					searchValue={search}
					onSearchChange={setSearch}
					onSearchSubmit={(value) => {
						setPage(0)
						setQuery(value.trim())
					}}
					serverFiltering={serverFiltering}
					serverSorting={serverSorting}
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
					height={440}
					onRowClick={(record) => beginEdit(record.rule as VPNRule)}
				/>
			</div>

			{showForm ? (
				<div className="grid gap-4 rounded-md border border-border bg-card p-4">
					<div className="flex items-center justify-between gap-3">
						<h2 className="font-semibold">{editing ? <Trans>Edit VPN rule</Trans> : <Trans>Create VPN rule</Trans>}</h2>
						<div className="flex gap-2">
							{editing ? (
								<Button variant="destructive" onClick={remove} disabled={saving}>
									<Trash2Icon className="me-2 h-4 w-4" />
									<Trans>Delete</Trans>
								</Button>
							) : null}
							<Button onClick={save} disabled={saving}>
								<SaveIcon className="me-2 h-4 w-4" />
								<Trans>Save draft</Trans>
							</Button>
						</div>
					</div>
					<RuleFormFields form={form} onChange={(patch) => setForm((current) => ({ ...current, ...patch }))} />
				</div>
			) : null}
		</div>
	)
})

function RuleFormFields({ form, onChange }: { form: RuleForm; onChange: (patch: Partial<RuleForm>) => void }) {
	return (
		<div className="grid gap-4">
			<div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
				<TextField label="Name" value={form.name} onChange={(name) => onChange({ name })} />
				<SelectField
					label="Kind"
					value={form.kind}
					values={["passive", "intelligence", "probe"]}
					onChange={(kind) => onChange({ kind })}
				/>
				<SelectField
					label="Effect"
					value={form.effect}
					values={["score", "allow", "suppress"]}
					onChange={(effect) => onChange({ effect, weight: effect === "score" ? form.weight || "10" : "0" })}
				/>
				<SelectField
					label="Status"
					value={form.status}
					values={["draft", "active", "suspended", "retired"]}
					onChange={(status) => onChange({ status })}
				/>
				<TextField
					label="Weight"
					value={form.weight}
					disabled={form.effect !== "score"}
					onChange={(weight) => onChange({ weight })}
				/>
				<TextField label="Priority" value={form.priority} onChange={(priority) => onChange({ priority })} />
				<TextField
					label="Remote ports"
					value={form.remotePorts}
					placeholder="443, 8443"
					onChange={(remotePorts) => onChange({ remotePorts })}
				/>
				<TextField
					label="IP protocols"
					value={form.protocols}
					placeholder="6, 17"
					onChange={(protocols) => onChange({ protocols })}
				/>
				<TextField
					label="Remote ASNs"
					value={form.remoteASNs}
					placeholder="4134, 4837"
					onChange={(remoteASNs) => onChange({ remoteASNs })}
				/>
				<TextField
					label="Remote prefix IDs"
					value={form.remotePrefixIDs}
					onChange={(remotePrefixIDs) => onChange({ remotePrefixIDs })}
				/>
				<TextField
					label="Remote countries"
					value={form.remoteCountries}
					placeholder="CN, US"
					onChange={(remoteCountries) => onChange({ remoteCountries })}
				/>
				<TextField
					label="Transport hints"
					value={form.transportHints}
					placeholder="tcp, tls, quic"
					onChange={(transportHints) => onChange({ transportHints })}
				/>
				<TextField
					label="Minimum duration (ms)"
					value={form.minDurationMS}
					onChange={(minDurationMS) => onChange({ minDurationMS })}
				/>
				<TextField
					label="Minimum bytes"
					value={form.minTotalBytes}
					onChange={(minTotalBytes) => onChange({ minTotalBytes })}
				/>
				<TextField
					label="Minimum flow records"
					value={form.minFlowRecords}
					onChange={(minFlowRecords) => onChange({ minFlowRecords })}
				/>
				<TextField
					label="Minimum active buckets"
					value={form.minActiveBuckets}
					onChange={(minActiveBuckets) => onChange({ minActiveBuckets })}
				/>
				<TextField
					label="Minimum symmetry ratio"
					value={form.minSymmetryRatio}
					placeholder="0..1"
					onChange={(minSymmetryRatio) => onChange({ minSymmetryRatio })}
				/>
				<TextField
					label="Minimum dominance ratio"
					value={form.minDominanceRatio}
					placeholder="0..1"
					onChange={(minDominanceRatio) => onChange({ minDominanceRatio })}
				/>
			</div>
			<p className="text-xs text-muted-foreground">
				<Trans>
					At least one match signal is required. TLS and QUIC are explicit upstream hints; ports never imply them.
				</Trans>
			</p>
		</div>
	)
}

function TextField({
	label,
	value,
	placeholder,
	disabled,
	onChange,
}: {
	label: string
	value: string
	placeholder?: string
	disabled?: boolean
	onChange: (value: string) => void
}) {
	return (
		<div className="grid gap-1.5">
			<Label>{label}</Label>
			<Input
				value={value}
				placeholder={placeholder}
				disabled={disabled}
				onChange={(event) => onChange(event.target.value)}
			/>
		</div>
	)
}

function SelectField({
	label,
	value,
	values,
	onChange,
}: {
	label: string
	value: string
	values: string[]
	onChange: (value: string) => void
}) {
	return (
		<div className="grid gap-1.5">
			<Label>{label}</Label>
			<Select value={value} onValueChange={onChange}>
				<SelectTrigger>
					<SelectValue />
				</SelectTrigger>
				<SelectContent>
					{values.map((item) => (
						<SelectItem key={item} value={item}>
							{item}
						</SelectItem>
					))}
				</SelectContent>
			</Select>
		</div>
	)
}

function formToRequest(form: RuleForm) {
	const match: VPNRuleMatchInput = {
		remote_ports: parsePositiveIntegers(form.remotePorts, 65535),
		protocols: parsePositiveIntegers(form.protocols, 255),
		remote_asns: parsePositiveIntegers(form.remoteASNs, 4_294_967_295),
		remote_prefix_ids: parseIdentifiers(form.remotePrefixIDs),
		remote_countries: parseIdentifiers(form.remoteCountries, (value) => value.toUpperCase()),
		transport_hints: parseIdentifiers(form.transportHints, (value) => value.toLowerCase()),
		min_duration_ms: optionalPositive(form.minDurationMS),
		min_total_bytes: optionalPositive(form.minTotalBytes),
		min_flow_records: optionalPositive(form.minFlowRecords),
		min_active_buckets: optionalPositive(form.minActiveBuckets),
		min_symmetry_ratio: optionalRatio(form.minSymmetryRatio),
		min_dominance_ratio: optionalRatio(form.minDominanceRatio),
	}
	return {
		name: form.name,
		kind: form.kind,
		match,
		effect: form.effect,
		weight: Number(form.weight),
		priority: Number(form.priority),
		status: form.status,
	}
}

function ruleToForm(rule: VPNRule): RuleForm {
	const match = rule.match ?? {}
	return {
		name: rule.name,
		kind: rule.kind,
		effect: rule.effect,
		weight: String(rule.weight),
		priority: String(rule.priority),
		status: rule.status,
		remotePorts: join(match.remote_ports),
		protocols: join(match.protocols),
		remoteASNs: join(match.remote_asns),
		remotePrefixIDs: join(match.remote_prefix_ids),
		remoteCountries: join(match.remote_countries),
		transportHints: join(match.transport_hints),
		minDurationMS: optionalString(match.min_duration_ms),
		minTotalBytes: optionalString(match.min_total_bytes),
		minFlowRecords: optionalString(match.min_flow_records),
		minActiveBuckets: optionalString(match.min_active_buckets),
		minSymmetryRatio: optionalString(match.min_symmetry_ratio),
		minDominanceRatio: optionalString(match.min_dominance_ratio),
	}
}

function valueOption(value: string): ServerFilterOption {
	return { value, label: value }
}
function join(values?: Array<string | number>) {
	return values?.join(", ") ?? ""
}
function optionalString(value?: number) {
	return value == null ? "" : String(value)
}

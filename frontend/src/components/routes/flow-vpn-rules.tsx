import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@/lib/page-path"
import { ArrowLeftIcon, PlusIcon, RefreshCwIcon, SaveIcon, SendIcon, ShieldCheckIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, Link } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { api, can } from "@/lib/api"
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
type VPNRuleSet = {
	id: string
	version: number
	effective_from: string
	entry_count: number
	status: string
	approval_state: string
	checksum: string
	row_version: number
	created_at: string
}
type RuleSetResponse = { items?: VPNRuleSet[]; total?: number }
type RuleSetPreview = { draft_digest: string }
type VPNRuleSetConsumer = {
	worker_id: string
	software_version: string
	target_state: string
	drift: string
	target_error_code?: string
	target_attempted_at?: string
}
type ConsumerResponse = { items?: VPNRuleSetConsumer[] }
type RuleSetPolicy = {
	medium_threshold: number
	high_threshold: number
	critical_threshold: number
	probe_threshold: number
	minimum_completeness: number
}
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
	const [ruleSets, setRuleSets] = useState<VPNRuleSet[]>([])
	const [ruleSetTotal, setRuleSetTotal] = useState(0)
	const [ruleSetPage, setRuleSetPage] = useState(0)
	const [ruleSetPageSize, setRuleSetPageSize] = useState(25)
	const [ruleSetSearch, setRuleSetSearch] = useState("")
	const [ruleSetQuery, setRuleSetQuery] = useState("")
	const [ruleSetFilters, setRuleSetFilters] = useState<Record<string, unknown[]>>({})
	const [ruleSetSort, setRuleSetSort] = useState("version")
	const [ruleSetOrder, setRuleSetOrder] = useState<SortDirection>("desc")
	const [ruleSetLoading, setRuleSetLoading] = useState(true)
	const [selectedRuleSet, setSelectedRuleSet] = useState<VPNRuleSet | null>(null)
	const [consumers, setConsumers] = useState<VPNRuleSetConsumer[]>([])
	const [effectiveFrom, setEffectiveFrom] = useState(defaultEffectiveFrom)
	const [policy, setPolicy] = useState<RuleSetPolicy>({
		medium_threshold: 30,
		high_threshold: 60,
		critical_threshold: 85,
		probe_threshold: 70,
		minimum_completeness: 0.8,
	})
	const [publishing, setPublishing] = useState(false)
	const [error, setError] = useState("")
	const requestSequence = useRef(0)
	const publicationRequestSequence = useRef(0)
	const canPublish = can("flow.vpn.publish")

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setPage(0)
			setQuery(search.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [search])

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setRuleSetPage(0)
			setRuleSetQuery(ruleSetSearch.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [ruleSetSearch])

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

	const refreshRuleSets = useCallback(async () => {
		const sequence = ++publicationRequestSequence.current
		setRuleSetLoading(true)
		const params = new URLSearchParams({
			limit: String(ruleSetPageSize),
			offset: String(ruleSetPage * ruleSetPageSize),
			sort: ruleSetSort,
			order: ruleSetOrder,
		})
		if (ruleSetQuery) params.set("q", ruleSetQuery)
		const status = ruleSetFilters.status?.[0]
		const approval = ruleSetFilters.approval_state?.[0]
		if (status) params.set("status", String(status))
		if (approval) params.set("approval_state", String(approval))
		try {
			const data = await api.send<RuleSetResponse>(`/api/v1/flow/vpn/rule-sets?${params}`, {})
			if (sequence !== publicationRequestSequence.current) return
			setRuleSets(data.items ?? [])
			setRuleSetTotal(data.total ?? 0)
		} catch (reason) {
			if (sequence !== publicationRequestSequence.current) return
			setRuleSets([])
			setRuleSetTotal(0)
			setError(reason instanceof Error ? reason.message : t`Failed to load VPN rule-set publications`)
		} finally {
			if (sequence === publicationRequestSequence.current) setRuleSetLoading(false)
		}
	}, [ruleSetFilters, ruleSetOrder, ruleSetPage, ruleSetPageSize, ruleSetQuery, ruleSetSort, t])

	useEffect(() => {
		document.title = `${t`VPN Rules`} / Watchdog`
		refresh()
		refreshRuleSets()
	}, [refresh, refreshRuleSets, t])

	const loadConsumers = useCallback(
		async (ruleSet: VPNRuleSet) => {
			try {
				const response = await api.send<ConsumerResponse>(
					`/api/v1/flow/vpn/rule-sets/${ruleSet.id}/consumers?limit=100`,
					{}
				)
				setConsumers(response.items ?? [])
			} catch (reason) {
				setConsumers([])
				setError(reason instanceof Error ? reason.message : t`Failed to load VPN rule-set consumers`)
			}
		},
		[t]
	)

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
	const ruleSetColumns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "version", title: t`Version`, width: 100 },
			{ field: "effective", title: t`Effective from`, width: 190 },
			{ field: "rules", title: t`Rules`, width: 90 },
			{ field: "approval_state", filterField: "approval_state", title: t`Approval`, width: 120 },
			{ field: "status", filterField: "status", title: t`Status`, width: 110 },
			{ field: "checksum_short", title: t`Checksum`, width: 190 },
			{ field: "created", title: t`Created`, width: 190 },
		],
		[t]
	)
	const ruleSetRecords = useMemo(
		() =>
			ruleSets.map((item) => ({
				...item,
				effective: new Date(item.effective_from).toLocaleString(),
				rules: item.entry_count,
				checksum_short: item.checksum.slice(0, 24),
				created: new Date(item.created_at).toLocaleString(),
				ruleSet: item,
			})),
		[ruleSets]
	)
	const ruleSetFiltering = useMemo(
		() => ({
			options: {
				status: ["active", "retired"].map(valueOption),
				approval_state: ["pending", "approved", "rejected"].map(valueOption),
			},
			selected: ruleSetFilters,
			onColumnFilterChange: (field: string, values: unknown[]) => {
				setRuleSetPage(0)
				setRuleSetFilters((current) => ({ ...current, [field]: values.slice(-1) }))
			},
			onClearAll: () => {
				setRuleSetPage(0)
				setRuleSetFilters({})
			},
		}),
		[ruleSetFilters]
	)
	const ruleSetSorting = useMemo(
		() => ({
			field: ruleSetSort,
			direction: ruleSetOrder,
			fields: {
				version: "version",
				effective: "effective",
				approval_state: "approval",
				status: "status",
				checksum_short: "checksum",
				created: "created",
			},
			onSortChange: (field: string, direction: SortDirection) => {
				setRuleSetPage(0)
				setRuleSetSort(field)
				setRuleSetOrder(direction)
			},
		}),
		[ruleSetOrder, ruleSetSort]
	)

	const publish = async () => {
		setPublishing(true)
		setError("")
		try {
			const effective_from = parseEffectiveFrom(effectiveFrom)
			const preview = await api.send<RuleSetPreview>("/api/v1/flow/vpn/rule-sets/preview", {
				method: "POST",
				body: { effective_from, policy },
			})
			await api.send("/api/v1/flow/vpn/rule-sets/publish", {
				method: "POST",
				body: { effective_from, preview_digest: preview.draft_digest, policy },
			})
			window.setTimeout(refreshRuleSets, 500)
		} catch (reason) {
			setError(reason instanceof Error ? reason.message : t`Failed to publish VPN rule set`)
		} finally {
			setPublishing(false)
		}
	}
	const lifecycle = async (action: "approve" | "reject" | "activate" | "rollback" | "retire") => {
		if (!selectedRuleSet) return
		setPublishing(true)
		setError("")
		try {
			const body =
				action === "rollback"
					? { effective_from: parseEffectiveFrom(effectiveFrom) }
					: action === "reject" || action === "retire"
						? { reason: "operator request" }
						: undefined
			await api.send(`/api/v1/flow/vpn/rule-sets/${selectedRuleSet.id}/actions/${action}`, {
				method: "POST",
				headers: { "If-Match": `"${selectedRuleSet.row_version}"` },
				body,
			})
			setSelectedRuleSet(null)
			setConsumers([])
			await refreshRuleSets()
		} catch (reason) {
			setError(reason instanceof Error ? reason.message : t`Failed to update VPN rule-set publication`)
		} finally {
			setPublishing(false)
		}
	}

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

			<div className="grid gap-3 rounded-md border border-border bg-card p-3">
				<div className="flex flex-wrap items-end justify-between gap-3">
					<div>
						<h2 className="font-semibold">
							<Trans>Rule-set publications</Trans> ({ruleSetTotal})
						</h2>
						<p className="text-sm text-muted-foreground">
							<Trans>Only an approved, activated and installed version is used for scoring.</Trans>
						</p>
					</div>
					<div className="flex flex-wrap items-end gap-2">
						<PolicyInput
							label={t`Medium`}
							value={policy.medium_threshold}
							onChange={(medium_threshold) => setPolicy((current) => ({ ...current, medium_threshold }))}
						/>
						<PolicyInput
							label={t`High`}
							value={policy.high_threshold}
							onChange={(high_threshold) => setPolicy((current) => ({ ...current, high_threshold }))}
						/>
						<PolicyInput
							label={t`Critical`}
							value={policy.critical_threshold}
							onChange={(critical_threshold) => setPolicy((current) => ({ ...current, critical_threshold }))}
						/>
						<PolicyInput
							label={t`Probe`}
							value={policy.probe_threshold}
							onChange={(probe_threshold) => setPolicy((current) => ({ ...current, probe_threshold }))}
						/>
						<PolicyInput
							label={t`Completeness`}
							value={policy.minimum_completeness}
							step="0.05"
							onChange={(minimum_completeness) => setPolicy((current) => ({ ...current, minimum_completeness }))}
						/>
						<div className="grid gap-1">
							<Label htmlFor="vpn-rule-set-effective">
								<Trans>Effective from</Trans>
							</Label>
							<Input
								id="vpn-rule-set-effective"
								type="datetime-local"
								value={effectiveFrom}
								onChange={(event) => setEffectiveFrom(event.target.value)}
							/>
						</div>
						<Button variant="outline" onClick={refreshRuleSets} disabled={ruleSetLoading}>
							<RefreshCwIcon className="me-2 h-4 w-4" />
							<Trans>Refresh</Trans>
						</Button>
						{canPublish ? (
							<Button onClick={publish} disabled={publishing}>
								<SendIcon className="me-2 h-4 w-4" />
								<Trans>Preview and publish</Trans>
							</Button>
						) : null}
					</div>
				</div>
				<PagedVTable
					records={ruleSetRecords}
					columns={ruleSetColumns}
					loading={ruleSetLoading}
					emptyText={t`No VPN rule-set publications found.`}
					searchPlaceholder={t`Search version, ID or checksum...`}
					searchValue={ruleSetSearch}
					onSearchChange={setRuleSetSearch}
					onSearchSubmit={(value) => {
						setRuleSetPage(0)
						setRuleSetQuery(value.trim())
					}}
					serverFiltering={ruleSetFiltering}
					serverSorting={ruleSetSorting}
					serverPagination={{
						page: ruleSetPage,
						pageSize: ruleSetPageSize,
						totalCount: ruleSetTotal,
						onPageChange: setRuleSetPage,
						onPageSizeChange: (value) => {
							setRuleSetPage(0)
							setRuleSetPageSize(value)
						},
					}}
					height={320}
					onRowClick={(record) => {
						const item = record.ruleSet as VPNRuleSet
						setSelectedRuleSet(item)
						loadConsumers(item)
					}}
				/>
				{selectedRuleSet ? (
					<div className="grid gap-3 rounded-md border border-border p-3">
						<div className="flex flex-wrap items-center justify-between gap-2">
							<div className="text-sm">
								<Trans>Selected version</Trans> {selectedRuleSet.version}: {selectedRuleSet.approval_state} /{" "}
								{selectedRuleSet.status}
							</div>
							{canPublish ? (
								<div className="flex flex-wrap gap-2">
									{selectedRuleSet.approval_state === "pending" ? (
										<>
											<Button size="sm" onClick={() => lifecycle("approve")}>
												<Trans>Approve</Trans>
											</Button>
											<Button size="sm" variant="outline" onClick={() => lifecycle("reject")}>
												<Trans>Reject</Trans>
											</Button>
										</>
									) : null}
									{selectedRuleSet.approval_state === "approved" && selectedRuleSet.status === "active" ? (
										<>
											<Button size="sm" onClick={() => lifecycle("activate")}>
												<Trans>Activate</Trans>
											</Button>
											<Button size="sm" variant="outline" onClick={() => lifecycle("rollback")}>
												<Trans>Rollback</Trans>
											</Button>
											<Button size="sm" variant="destructive" onClick={() => lifecycle("retire")}>
												<Trans>Retire</Trans>
											</Button>
										</>
									) : null}
								</div>
							) : null}
						</div>
						<div className="grid gap-2 text-sm">
							<div className="font-medium">
								<Trans>Worker installation status</Trans>
							</div>
							{consumers.length ? (
								consumers.map((consumer) => (
									<div key={consumer.worker_id} className="grid grid-cols-4 gap-2 rounded border border-border p-2">
										<span>{consumer.worker_id}</span>
										<span>{consumer.software_version}</span>
										<span>{consumer.target_state}</span>
										<span>
											{consumer.drift}
											{consumer.target_error_code ? ` · ${consumer.target_error_code}` : ""}
										</span>
									</div>
								))
							) : (
								<div className="text-muted-foreground">
									<Trans>No worker has reported this version.</Trans>
								</div>
							)}
						</div>
					</div>
				) : null}
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
	const { t } = useLingui()
	return (
		<div className="grid gap-4">
			<div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
				<TextField label={t`Name`} value={form.name} onChange={(name) => onChange({ name })} />
				<SelectField
					label={t`Kind`}
					value={form.kind}
					values={["passive", "intelligence", "probe"]}
					onChange={(kind) => onChange({ kind })}
				/>
				<SelectField
					label={t`Effect`}
					value={form.effect}
					values={["score", "allow", "suppress"]}
					onChange={(effect) => onChange({ effect, weight: effect === "score" ? form.weight || "10" : "0" })}
				/>
				<SelectField
					label={t`Status`}
					value={form.status}
					values={["draft", "active", "suspended", "retired"]}
					onChange={(status) => onChange({ status })}
				/>
				<TextField
					label={t`Weight`}
					value={form.weight}
					disabled={form.effect !== "score"}
					onChange={(weight) => onChange({ weight })}
				/>
				<TextField label={t`Priority`} value={form.priority} onChange={(priority) => onChange({ priority })} />
				<TextField
					label={t`Remote ports`}
					value={form.remotePorts}
					placeholder="443, 8443"
					onChange={(remotePorts) => onChange({ remotePorts })}
				/>
				<TextField
					label={t`IP protocols`}
					value={form.protocols}
					placeholder="6, 17"
					onChange={(protocols) => onChange({ protocols })}
				/>
				<TextField
					label={t`Remote ASNs`}
					value={form.remoteASNs}
					placeholder="4134, 4837"
					onChange={(remoteASNs) => onChange({ remoteASNs })}
				/>
				<TextField
					label={t`Remote prefix IDs`}
					value={form.remotePrefixIDs}
					onChange={(remotePrefixIDs) => onChange({ remotePrefixIDs })}
				/>
				<TextField
					label={t`Remote countries`}
					value={form.remoteCountries}
					placeholder="CN, US"
					onChange={(remoteCountries) => onChange({ remoteCountries })}
				/>
				<TextField
					label={t`Transport hints`}
					value={form.transportHints}
					placeholder="tcp, tls, quic"
					onChange={(transportHints) => onChange({ transportHints })}
				/>
				<TextField
					label={t`Minimum duration (ms)`}
					value={form.minDurationMS}
					onChange={(minDurationMS) => onChange({ minDurationMS })}
				/>
				<TextField
					label={t`Minimum bytes`}
					value={form.minTotalBytes}
					onChange={(minTotalBytes) => onChange({ minTotalBytes })}
				/>
				<TextField
					label={t`Minimum flow records`}
					value={form.minFlowRecords}
					onChange={(minFlowRecords) => onChange({ minFlowRecords })}
				/>
				<TextField
					label={t`Minimum active buckets`}
					value={form.minActiveBuckets}
					onChange={(minActiveBuckets) => onChange({ minActiveBuckets })}
				/>
				<TextField
					label={t`Minimum symmetry ratio`}
					value={form.minSymmetryRatio}
					placeholder="0..1"
					onChange={(minSymmetryRatio) => onChange({ minSymmetryRatio })}
				/>
				<TextField
					label={t`Minimum dominance ratio`}
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

function PolicyInput({
	label,
	value,
	step = "1",
	onChange,
}: {
	label: string
	value: number
	step?: string
	onChange: (value: number) => void
}) {
	return (
		<div className="grid w-24 gap-1">
			<Label>{label}</Label>
			<Input
				type="number"
				min="0"
				step={step}
				value={value}
				onChange={(event) => onChange(Number(event.target.value))}
			/>
		</div>
	)
}

function defaultEffectiveFrom() {
	const now = new Date(Math.ceil(Date.now() / 60_000) * 60_000)
	return new Date(now.getTime() - now.getTimezoneOffset() * 60_000).toISOString().slice(0, 16)
}

function parseEffectiveFrom(value: string) {
	const parsed = new Date(value)
	if (!value || Number.isNaN(parsed.getTime()) || parsed.getSeconds() !== 0 || parsed.getMilliseconds() !== 0) {
		throw new Error("Effective time must be a minute boundary")
	}
	return parsed.toISOString()
}

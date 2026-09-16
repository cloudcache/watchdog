import { Trans, useLingui } from "@lingui/react/macro"
import { ArchiveIcon, SaveIcon } from "lucide-react"
import { useCallback, useEffect, useMemo, useState } from "react"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { api } from "@/lib/api"
import type { ColumnDefine } from "@/lib/vtable"

type FlowStoragePolicy = {
	id: string
	policy_version: number
	status: "draft" | "published" | "retired"
	bootstrap_from: string
	raw_retention_seconds: number
	archive_resolution_seconds: number
	archive_retention_seconds: number
	late_arrival_seconds: number
	delete_grace_seconds: number
	max_partitions_per_run: number
	require_backup_before_delete: boolean
	row_version: number
	created_at: string
	published_at?: string
}

type ListResponse<T> = { items?: T[]; total?: number }

type FormState = {
	id: string
	rowVersion: number
	bootstrapFrom: string
	rawRetentionDays: string
	archiveRetentionDays: string
	lateArrivalHours: string
	deleteGraceHours: string
	maxPartitionsPerRun: string
	requireBackup: boolean
}

const emptyForm = (): FormState => ({
	id: "",
	rowVersion: 0,
	bootstrapFrom: "",
	rawRetentionDays: "",
	archiveRetentionDays: "0",
	lateArrivalHours: "",
	deleteGraceHours: "",
	maxPartitionsPerRun: "",
	requireBackup: true,
})

export default function FlowStorageLifecycle() {
	const { t } = useLingui()
	const [policies, setPolicies] = useState<FlowStoragePolicy[]>([])
	const [total, setTotal] = useState(0)
	const [counts, setCounts] = useState({ partitions: 0, watermarks: 0, receipts: 0 })
	const [form, setForm] = useState<FormState>(emptyForm)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [query, setQuery] = useState("")
	const [status, setStatus] = useState("")
	const [sortField, setSortField] = useState("version")
	const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc")
	const [loading, setLoading] = useState(true)
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const [policyData, partitions, watermarks, receipts] = await Promise.all([
				api.send<ListResponse<FlowStoragePolicy>>("/api/v1/flow/storage/policies", {
					query: {
						limit: pageSize,
						offset: page * pageSize,
						q: query || undefined,
						status: status || undefined,
						sort: sortField,
						order: sortDirection,
					},
				}),
				api.send<ListResponse<unknown>>("/api/v1/flow/storage/partitions", { query: { limit: 1 } }),
				api.send<ListResponse<unknown>>("/api/v1/flow/storage/watermarks", { query: { limit: 1 } }),
				api.send<ListResponse<unknown>>("/api/v1/flow/storage/deletion-receipts", { query: { limit: 1 } }),
			])
			setPolicies(policyData.items ?? [])
			setTotal(policyData.total ?? 0)
			setCounts({
				partitions: partitions.total ?? 0,
				watermarks: watermarks.total ?? 0,
				receipts: receipts.total ?? 0,
			})
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load Flow storage lifecycle`)
		} finally {
			setLoading(false)
		}
	}, [page, pageSize, query, sortDirection, sortField, status, t])

	useEffect(() => {
		refresh()
	}, [refresh])

	const update = (patch: Partial<FormState>) => setForm((current) => ({ ...current, ...patch }))
	const payload = useMemo(
		() => ({
			bootstrap_from: form.bootstrapFrom,
			raw_retention_seconds: daysToSeconds(form.rawRetentionDays),
			archive_retention_seconds: daysToSeconds(form.archiveRetentionDays),
			late_arrival_seconds: hoursToSeconds(form.lateArrivalHours),
			delete_grace_seconds: hoursToSeconds(form.deleteGraceHours),
			max_partitions_per_run: Number(form.maxPartitionsPerRun),
			require_backup_before_delete: form.requireBackup,
		}),
		[form]
	)
	const canSave =
		form.bootstrapFrom !== "" &&
		Number(form.rawRetentionDays) >= 1 &&
		Number(form.lateArrivalHours) >= 0 &&
		Number(form.deleteGraceHours) >= 1 &&
		Number(form.maxPartitionsPerRun) >= 1
	const records = useMemo(
		() =>
			policies.map((policy) => ({
				version: `v${policy.policy_version}`,
				status: statusLabel(policy.status, t),
				bootstrap: policy.bootstrap_from.slice(0, 10),
				rawRetention: durationDays(policy.raw_retention_seconds, t`days`),
				archiveRetention:
					policy.archive_retention_seconds === 0
						? t`Unlimited`
						: durationDays(policy.archive_retention_seconds, t`days`),
				lateArrival: durationHours(policy.late_arrival_seconds, t`hours`),
				editAction: policy.status === "draft" ? t`Edit` : "",
				stateAction: policy.status === "draft" ? t`Publish` : policy.status === "published" ? t`Retire` : "",
				deleteAction: policy.status === "draft" ? t`Delete` : "",
				policy,
			})),
		[policies, t]
	)
	const columns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "version", title: t`Version`, width: 90, filter: false, style: denseCellStyle() },
			{ field: "status", title: t`Status`, width: 110, filterField: "status", style: denseCellStyle() },
			{ field: "bootstrap", title: t`Bootstrap date`, width: 140, filter: false, style: denseCellStyle() },
			{ field: "rawRetention", title: t`Raw retention`, width: 140, filter: false, style: denseCellStyle() },
			{ field: "archiveRetention", title: t`Archive retention`, width: 160, filter: false, style: denseCellStyle() },
			{ field: "lateArrival", title: t`Late arrival`, width: 130, filter: false, style: denseCellStyle() },
			{ field: "editAction", title: t`Edit`, width: 80, filter: false, style: actionCellStyle() },
			{ field: "stateAction", title: t`Publish`, width: 90, filter: false, style: actionCellStyle() },
			{ field: "deleteAction", title: t`Delete`, width: 80, filter: false, style: actionCellStyle() },
		],
		[t]
	)
	const serverFiltering = useMemo(
		() => ({
			options: {
				status: [
					{ value: "draft", label: t`Draft` },
					{ value: "published", label: t`Published` },
					{ value: "retired", label: t`Retired` },
				],
			},
			selected: { status: status ? [status] : [] },
			selection: { status: "single" as const },
			onColumnFilterChange: (_field: string, values: unknown[]) => {
				setPage(0)
				setStatus(values.length ? String(values[0]) : "")
			},
			onClearAll: () => {
				setPage(0)
				setStatus("")
			},
		}),
		[status, t]
	)
	const serverSorting = useMemo(
		() => ({
			field: sortField,
			direction: sortDirection,
			fields: { version: "version", status: "status", bootstrap: "bootstrap_from" },
			onSortChange: (field: string, direction: "asc" | "desc") => {
				setPage(0)
				setSortField(field)
				setSortDirection(direction)
			},
		}),
		[sortDirection, sortField]
	)

	const save = async () => {
		setSaving(true)
		setError("")
		try {
			if (form.id) {
				await api.send(`/api/v1/flow/storage/policies/${form.id}`, {
					method: "PATCH",
					headers: { "If-Match": `"${form.rowVersion}"` },
					body: payload,
				})
			} else {
				await api.send("/api/v1/flow/storage/policies", { method: "POST", body: payload })
			}
			setForm(emptyForm())
			await refresh()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save Flow storage policy`)
		} finally {
			setSaving(false)
		}
	}

	const edit = (policy: FlowStoragePolicy) => {
		if (policy.status !== "draft") return
		setForm({
			id: policy.id,
			rowVersion: policy.row_version,
			bootstrapFrom: policy.bootstrap_from.slice(0, 10),
			rawRetentionDays: secondsToDays(policy.raw_retention_seconds),
			archiveRetentionDays: secondsToDays(policy.archive_retention_seconds),
			lateArrivalHours: secondsToHours(policy.late_arrival_seconds),
			deleteGraceHours: secondsToHours(policy.delete_grace_seconds),
			maxPartitionsPerRun: String(policy.max_partitions_per_run),
			requireBackup: policy.require_backup_before_delete,
		})
	}

	const act = async (policy: FlowStoragePolicy, action: "publish" | "retire" | "delete") => {
		setSaving(true)
		setError("")
		try {
			const path =
				action === "delete"
					? `/api/v1/flow/storage/policies/${policy.id}`
					: `/api/v1/flow/storage/policies/${policy.id}/actions/${action}`
			await api.send(path, {
				method: action === "delete" ? "DELETE" : "POST",
				headers: { "If-Match": `"${policy.row_version}"` },
			})
			if (form.id === policy.id) setForm(emptyForm())
			await refresh()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Flow storage lifecycle action failed`)
		} finally {
			setSaving(false)
		}
	}

	return (
		<section className="grid gap-4 rounded-md border border-border p-4">
			<div className="flex flex-wrap items-start justify-between gap-3">
				<div>
					<h2 className="flex items-center gap-2 text-base font-semibold">
						<ArchiveIcon className="h-4 w-4 text-muted-foreground" />
						<Trans>Flow storage lifecycle</Trans>
					</h2>
					<p className="mt-1 text-sm text-muted-foreground">
						<Trans>Published policy revisions drive archive eligibility. Physical deletion remains locked.</Trans>
					</p>
				</div>
				<Button size="sm" onClick={save} disabled={loading || saving || !canSave}>
					<SaveIcon className="me-2 h-4 w-4" />
					{form.id ? <Trans>Save draft</Trans> : <Trans>Create draft</Trans>}
				</Button>
			</div>

			{error ? <div className="rounded-md border border-border p-3 text-sm text-destructive">{error}</div> : null}

			<div className="grid gap-3 md:grid-cols-3">
				<Summary label={t`Partition states`} value={counts.partitions} />
				<Summary label={t`Kafka watermarks`} value={counts.watermarks} />
				<Summary label={t`Deletion receipts`} value={counts.receipts} />
			</div>

			<div className="grid gap-4 rounded-md border border-border p-4 md:grid-cols-2 xl:grid-cols-4">
				<Field label={t`Bootstrap date`}>
					<Input
						type="date"
						value={form.bootstrapFrom}
						onChange={(event) => update({ bootstrapFrom: event.target.value })}
					/>
				</Field>
				<Field label={t`Raw retention (days)`}>
					<Input
						type="number"
						min="1"
						value={form.rawRetentionDays}
						onChange={(event) => update({ rawRetentionDays: event.target.value })}
					/>
				</Field>
				<Field label={t`Archive retention (days, 0 = unlimited)`}>
					<Input
						type="number"
						min="0"
						value={form.archiveRetentionDays}
						onChange={(event) => update({ archiveRetentionDays: event.target.value })}
					/>
				</Field>
				<Field label={t`Late-arrival window (hours)`}>
					<Input
						type="number"
						min="0"
						value={form.lateArrivalHours}
						onChange={(event) => update({ lateArrivalHours: event.target.value })}
					/>
				</Field>
				<Field label={t`Delete grace (hours)`}>
					<Input
						type="number"
						min="1"
						value={form.deleteGraceHours}
						onChange={(event) => update({ deleteGraceHours: event.target.value })}
					/>
				</Field>
				<Field label={t`Maximum partitions per run`}>
					<Input
						type="number"
						min="1"
						max="366"
						value={form.maxPartitionsPerRun}
						onChange={(event) => update({ maxPartitionsPerRun: event.target.value })}
					/>
				</Field>
				<div className="flex items-center gap-2 pt-7">
					<Checkbox
						id="flow-require-backup"
						checked={form.requireBackup}
						onCheckedChange={(value) => update({ requireBackup: Boolean(value) })}
					/>
					<Label htmlFor="flow-require-backup">
						<Trans>Require verified backup</Trans>
					</Label>
				</div>
				{form.id ? (
					<div className="flex items-end">
						<Button type="button" variant="outline" onClick={() => setForm(emptyForm())}>
							<Trans>Cancel editing</Trans>
						</Button>
					</div>
				) : null}
			</div>

			<div className="overflow-hidden rounded-md border border-border bg-card p-3">
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={t`No Flow storage policies found.`}
					searchPlaceholder={t`Search policy ID or status...`}
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
					height={Math.min(440, Math.max(80, records.length * 42 + 38))}
					onCellClick={(record, field) => {
						const policy = record.policy as FlowStoragePolicy
						if (field === "editAction" && policy.status === "draft") edit(policy)
						if (field === "stateAction" && policy.status === "draft") act(policy, "publish")
						if (field === "stateAction" && policy.status === "published") act(policy, "retire")
						if (field === "deleteAction" && policy.status === "draft") act(policy, "delete")
					}}
				/>
			</div>
		</section>
	)
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
	return (
		<div className="grid gap-1.5">
			<Label>{label}</Label>
			{children}
		</div>
	)
}

function Summary({ label, value }: { label: string; value: number }) {
	return (
		<div className="rounded-md border border-border p-3">
			<div className="text-xs text-muted-foreground">{label}</div>
			<div className="mt-1 text-xl font-semibold">{value}</div>
		</div>
	)
}

function daysToSeconds(value: string) {
	return Math.round(Number(value) * 86400)
}

function hoursToSeconds(value: string) {
	return Math.round(Number(value) * 3600)
}

function secondsToDays(value: number) {
	return String(value / 86400)
}

function secondsToHours(value: number) {
	return String(value / 3600)
}

function durationDays(value: number, unit: string) {
	return `${value / 86400} ${unit}`
}

function durationHours(value: number, unit: string) {
	return `${value / 3600} ${unit}`
}

function statusLabel(status: FlowStoragePolicy["status"], translate: (message: TemplateStringsArray) => string) {
	if (status === "draft") return translate`Draft`
	if (status === "published") return translate`Published`
	return translate`Retired`
}

function denseCellStyle() {
	return { padding: [8, 10, 8, 10], textBaseline: "middle", autoWrapText: false }
}

function actionCellStyle() {
	return { ...denseCellStyle(), color: "#2563eb", cursor: "pointer" }
}

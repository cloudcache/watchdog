import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, BracesIcon, PlayIcon, RefreshCwIcon } from "lucide-react"
import { memo, useEffect, useMemo, useRef, useState } from "react"
import type { ReactNode } from "react"
import { $router, navigate } from "@/components/router"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Textarea } from "@/components/ui/textarea"
import { api } from "@/lib/api"
import type { ColumnDefine } from "@/lib/vtable"

type AgentRecord = {
	id: string
	kind: string
	capabilities?: string[]
	desired_plan_version?: number
	acked_plan_version?: number
}

type PlanRecord = {
	id: string
	plan_version: number
	schema_version: number
	payload_sha256: string
	signing_key_id: string
	expires_at: string
	created_at: string
	payload?: { required_capabilities?: string[]; config?: Record<string, unknown> }
}

type PlansResponse = { items?: PlanRecord[]; total?: number }
type PublicKeyResponse = { key_id?: string }

export default memo(({ id }: { id: string }) => {
	const { t } = useLingui()
	const [agent, setAgent] = useState<AgentRecord | null>(null)
	const [plans, setPlans] = useState<PlanRecord[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [schemaFilter, setSchemaFilter] = useState("")
	const [keyFilter, setKeyFilter] = useState("")
	const [signingKeyID, setSigningKeyID] = useState("")
	const [sort, setSort] = useState("plan_version:desc")
	const [capabilities, setCapabilities] = useState("")
	const [config, setConfig] = useState("{}")
	const [expiresIn, setExpiresIn] = useState("31536000")
	const [loading, setLoading] = useState(true)
	const [publishing, setPublishing] = useState(false)
	const [error, setError] = useState("")
	const [reloadKey, setReloadKey] = useState(0)
	const requestSequence = useRef(0)

	useEffect(() => {
		const handle = setTimeout(() => {
			setPage(0)
			setDebouncedSearch(search.trim())
		}, 300)
		return () => clearTimeout(handle)
	}, [search])

	useEffect(() => {
		document.title = `${id} / ${t`Agent Plans`} / Watchdog`
		Promise.all([
			api.send<AgentRecord>(`/api/v1/agents/${id}`, {}),
			api.send<PublicKeyResponse>("/api/v1/agents/plan-public-key", {}),
		])
			.then(([loadedAgent, key]) => {
				setAgent(loadedAgent)
				setSigningKeyID(key.key_id ?? "")
				setCapabilities((loadedAgent.capabilities ?? []).join(", "))
			})
			.catch((reason) => setError(reason instanceof Error ? reason.message : t`Failed to load agent`))
	}, [id, reloadKey, t])

	useEffect(() => {
		const sequence = ++requestSequence.current
		const [sortField, order] = sort.split(":")
		setLoading(true)
		setError("")
		api
			.send<PlansResponse>(`/api/v1/agents/${id}/plans`, {
				query: {
					q: debouncedSearch || undefined,
					schema_version: schemaFilter || undefined,
					signing_key_id: keyFilter || undefined,
					sort: sortField,
					order,
					limit: pageSize,
					offset: page * pageSize || undefined,
				},
			})
			.then((data) => {
				if (sequence !== requestSequence.current) return
				setPlans(data.items ?? [])
				setTotal(data.total ?? 0)
			})
			.catch((reason) => {
				if (sequence !== requestSequence.current) return
				setPlans([])
				setTotal(0)
				setError(reason instanceof Error ? reason.message : t`Failed to load agent plans`)
			})
			.finally(() => {
				if (sequence === requestSequence.current) setLoading(false)
			})
	}, [debouncedSearch, id, keyFilter, page, pageSize, reloadKey, schemaFilter, sort, t])

	const publish = async () => {
		setPublishing(true)
		setError("")
		try {
			const parsed = JSON.parse(config)
			if (!parsed || Array.isArray(parsed) || typeof parsed !== "object")
				throw new Error(t`Config must be a JSON object.`)
			const required = [
				...new Set(
					capabilities
						.split(",")
						.map((value) => value.trim())
						.filter(Boolean)
				),
			].sort()
			if (required.length === 0) throw new Error(t`At least one required capability is needed.`)
			const seconds = Number(expiresIn)
			if (!Number.isSafeInteger(seconds) || seconds < 60) throw new Error(t`Expiry must be at least 60 seconds.`)
			await api.send(`/api/v1/agents/${id}/plans`, {
				method: "POST",
				body: {
					required_capabilities: required,
					config: parsed,
					expires_in_seconds: seconds,
					expected_desired_plan_version: agent?.desired_plan_version ?? 0,
				},
			})
			setReloadKey((value) => value + 1)
			setPage(0)
		} catch (reason) {
			setError(reason instanceof Error ? reason.message : t`Failed to publish agent plan`)
		} finally {
			setPublishing(false)
		}
	}

	const records = useMemo(
		() =>
			plans.map((plan) => ({
				plan_version: plan.plan_version,
				schema_version: plan.schema_version,
				capabilities: plan.payload?.required_capabilities?.join(", ") ?? "-",
				config: JSON.stringify(plan.payload?.config ?? {}),
				signing_key_id: plan.signing_key_id,
				payload_sha256: plan.payload_sha256,
				expires_at: formatDate(plan.expires_at),
				created_at: formatDate(plan.created_at),
				id: plan.id,
			})),
		[plans]
	)
	const columns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "plan_version", title: t`Version`, width: 100 },
			{ field: "schema_version", title: t`Schema`, width: 100 },
			{ field: "capabilities", title: t`Required capabilities`, width: 300 },
			{ field: "config", title: t`Config`, width: 360 },
			{ field: "signing_key_id", title: t`Signing key`, width: 210 },
			{ field: "payload_sha256", title: "SHA-256", width: 260 },
			{ field: "expires_at", title: t`Expires`, width: 190 },
			{ field: "created_at", title: t`Created`, width: 190 },
			{ field: "id", title: "ID", width: 190 },
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
				schema_version: [{ value: "1", label: "v1" }],
				signing_key_id: signingKeyID ? [{ value: signingKeyID }] : [],
			},
			selected: {
				schema_version: schemaFilter ? [schemaFilter] : [],
				signing_key_id: keyFilter ? [keyFilter] : [],
			},
			selection: { schema_version: "single" as const, signing_key_id: "single" as const },
			onColumnFilterChange: (field: string, values: unknown[]) =>
				resetPage(() => {
					const value = values.length > 0 ? String(values[0]) : ""
					if (field === "schema_version") setSchemaFilter(value)
					if (field === "signing_key_id") setKeyFilter(value)
				}),
			onClearAll: () =>
				resetPage(() => {
					setSchemaFilter("")
					setKeyFilter("")
				}),
		}),
		[keyFilter, schemaFilter, signingKeyID]
	)
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(
		() => ({
			field: sortField,
			direction: sortDirection,
			fields: {
				plan_version: "plan_version",
				schema_version: "schema_version",
				signing_key_id: "signing_key_id",
				expires_at: "expires_at",
				created_at: "created_at",
			},
			onSortChange: (field: string, direction: "asc" | "desc") => resetPage(() => setSort(`${field}:${direction}`)),
		}),
		[sortDirection, sortField]
	)

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<Button variant="ghost" size="icon" onClick={() => navigate(getPagePath($router, "agents"))}>
						<ArrowLeftIcon className="h-4 w-4" />
					</Button>
					<BracesIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Agent Plans</Trans>
					</h1>
				</div>
				<div className="flex items-center gap-2">
					<Button variant="outline" size="sm" onClick={() => navigate(getPagePath($router, "agent_runs", { id }))}>
						<PlayIcon className="me-2 h-4 w-4" />
						<Trans>Runs</Trans>
					</Button>
					<Button variant="outline" size="sm" onClick={() => setReloadKey((value) => value + 1)} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
			</div>

			<div className="grid gap-4 rounded-md border border-border bg-card p-4">
				<div className="grid gap-4 md:grid-cols-3">
					<Field label={t`Agent`}>
						<Input value={`${id} · ${agent?.kind ?? "-"}`} disabled />
					</Field>
					<Field label={t`Required capabilities`}>
						<Input value={capabilities} onChange={(event) => setCapabilities(event.target.value)} />
					</Field>
					<Field label={t`Expires in seconds`}>
						<Input type="number" min={60} value={expiresIn} onChange={(event) => setExpiresIn(event.target.value)} />
					</Field>
				</div>
				<Field label={t`Config (JSON object)`}>
					<Textarea
						rows={5}
						value={config}
						onChange={(event) => setConfig(event.target.value)}
						className="font-mono text-xs"
					/>
				</Field>
				<div className="flex items-center justify-between gap-3">
					<div className="text-xs text-muted-foreground">
						<Trans>Desired / acknowledged</Trans>: {agent?.desired_plan_version ?? 0} / {agent?.acked_plan_version ?? 0}
					</div>
					<Button size="sm" onClick={publish} disabled={publishing || !agent}>
						<BracesIcon className="me-2 h-4 w-4" />
						<Trans>Publish immutable plan</Trans>
					</Button>
				</div>
			</div>

			<div className="rounded-md border border-border bg-card p-3">
				{error ? <div className="p-3 text-sm text-destructive">{error}</div> : null}
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={t`No agent plans found.`}
					searchPlaceholder={t`Search by plan ID, digest, or signing key...`}
					height={560}
					rowHeight={46}
					searchValue={search}
					onSearchChange={setSearch}
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
		</div>
	)
})

function Field({ label, children }: { label: string; children: ReactNode }) {
	return (
		<div className="grid gap-2">
			<Label>{label}</Label>
			{children}
		</div>
	)
}

function formatDate(value?: string) {
	if (!value) return "-"
	const date = new Date(value)
	return Number.isNaN(date.getTime()) ? "-" : date.toLocaleString()
}

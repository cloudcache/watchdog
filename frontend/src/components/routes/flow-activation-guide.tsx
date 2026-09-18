import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import {
	AlertTriangleIcon,
	CheckCircle2Icon,
	CircleIcon,
	ExternalLinkIcon,
	LoaderCircleIcon,
	RefreshCwIcon,
} from "lucide-react"
import { memo, type ReactNode, useCallback, useEffect, useMemo, useState } from "react"
import { $router } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { WatchdogAPIError, api, can } from "@/lib/api"

type Snapshot = {
	id: string
	version: number
	status: string
	approval_state: string
	row_version: number
}

type RuntimeStatus = {
	snapshot: Snapshot
	consumers?: { total?: number; installed?: number; failed?: number }
}

type BuildJob = {
	id: string
	status: string
	progress_total?: number
	progress_done?: number
	last_error_detail?: string
}

type EnrichmentPublication = {
	id: string
	classification_version: number
	dimension_snapshot_id: string
	dimension_version: number
}

type Worker = {
	id: string
	status: string
	health?: string
	last_seen?: string
}

type WorkerACK = {
	state: string
	worker_id: string
}

type LifecycleState = {
	latestSnapshot: Snapshot | null
	activeSnapshot: Snapshot | null
	buildJob: BuildJob | null
	publication: EnrichmentPublication | null
	workers: Worker[]
	acks: WorkerACK[]
}

const emptyState: LifecycleState = {
	latestSnapshot: null,
	activeSnapshot: null,
	buildJob: null,
	publication: null,
	workers: [],
	acks: [],
}

export default memo(function FlowActivationGuide({ onChanged }: { onChanged?: () => void | Promise<void> }) {
	const { t } = useLingui()
	const [state, setState] = useState<LifecycleState>(emptyState)
	const [loading, setLoading] = useState(true)
	const [working, setWorking] = useState("")
	const [error, setError] = useState("")
	const canPublish = can("address.publish")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const [versions, jobs, publications, workers, runtime] = await Promise.all([
				api.send<{ items?: Snapshot[] }>("/api/v1/dimensions/address/versions", {
					query: { limit: 1, offset: 0, sort: "version", order: "desc" },
				}),
				api.send<{ items?: BuildJob[] }>("/api/v1/operation-jobs", {
					query: { job_type: "address_snapshot_build", limit: 1 },
				}),
				api.send<{ items?: EnrichmentPublication[] }>("/api/v1/flow/enrichment-publications", {
					query: { limit: 1, offset: 0, sort: "classification_version", order: "desc" },
				}),
				api.send<{ items?: Worker[] }>("/api/v1/agents", {
					query: { kind: "flow_worker", limit: 100, offset: 0, sort: "updated_at", order: "desc" },
				}),
				loadRuntimeStatus(),
			])
			const publication = publications.items?.[0] ?? null
			let acks: WorkerACK[] = []
			if (publication) {
				const response = await api.send<{ items?: WorkerACK[] }>(
					`/api/v1/flow/enrichment-publications/${encodeURIComponent(publication.id)}/acks`,
					{ query: { limit: 100, offset: 0 } }
				)
				acks = response.items ?? []
			}
			setState({
				latestSnapshot: versions.items?.[0] ?? null,
				activeSnapshot: runtime?.snapshot ?? null,
				buildJob: jobs.items?.[0] ?? null,
				publication,
				workers: workers.items ?? [],
				acks,
			})
		} catch (cause) {
			setError(cause instanceof Error ? cause.message : t`Failed to load Flow activation status`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		refresh()
	}, [refresh])

	useEffect(() => {
		if (!state.buildJob || !["queued", "running", "cancel_requested"].includes(state.buildJob.status)) return
		const timer = window.setInterval(refresh, 2_000)
		return () => window.clearInterval(timer)
	}, [refresh, state.buildJob])

	const run = useCallback(
		async (name: string, action: () => Promise<void>) => {
			setWorking(name)
			setError("")
			try {
				await action()
				await refresh()
				await onChanged?.()
			} catch (cause) {
				setError(cause instanceof Error ? cause.message : t`Operation failed`)
			} finally {
				setWorking("")
			}
		},
		[onChanged, refresh, t]
	)

	const buildSnapshot = () =>
		run("build", async () => {
			const effectiveFrom = currentMinuteISO()
			const preview = await api.send<{ draft_digest: string; effective_from: string }>(
				"/api/v1/dimensions/address/preview",
				{ method: "POST", body: { effective_from: effectiveFrom } }
			)
			await api.send("/api/v1/dimensions/address/publish", {
				method: "POST",
				body: { effective_from: preview.effective_from, preview_digest: preview.draft_digest },
			})
		})

	const approveAndActivate = () => {
		const snapshot = state.latestSnapshot
		if (!snapshot) return
		run("activate", async () => {
			let current = snapshot
			if (current.approval_state === "pending") {
				current = await api.send<Snapshot>(`/api/v1/dimensions/address/versions/${current.id}/actions/approve`, {
					method: "POST",
					headers: { "If-Match": `"${current.row_version}"` },
				})
			}
			await api.send(`/api/v1/dimensions/address/versions/${current.id}/actions/activate`, {
				method: "POST",
				headers: { "If-Match": `"${current.row_version}"` },
			})
		})
	}

	const publishFlowVersion = () =>
		run("flow", async () => {
			await api.send("/api/v1/flow/enrichment-publications", {
				method: "POST",
				body: { effective_from: nextMinuteISO() },
			})
		})

	const activeWorkerCount = state.workers.filter((worker) => worker.status === "active").length
	const installedWorkerIDs = useMemo(
		() => new Set(state.acks.filter((ack) => ack.state === "installed").map((ack) => ack.worker_id)),
		[state.acks]
	)
	const publicationMatchesActive = Boolean(
		state.publication && state.activeSnapshot && state.publication.dimension_snapshot_id === state.activeSnapshot.id
	)
	const latestSnapshotIsActive = Boolean(
		state.latestSnapshot && state.activeSnapshot && state.latestSnapshot.id === state.activeSnapshot.id
	)
	const buildRunning = Boolean(
		state.buildJob && ["queued", "running", "cancel_requested"].includes(state.buildJob.status)
	)
	const workerReady = activeWorkerCount > 0 && state.workers.some((worker) => installedWorkerIDs.has(worker.id))
	const snapshotDetail = state.latestSnapshot
		? t`WADS v${state.latestSnapshot.version} · ${state.latestSnapshot.approval_state}`
		: !state.buildJob
			? t`No WADS snapshot has been built.`
			: state.buildJob.status === "failed"
				? state.buildJob.last_error_detail || t`The last build failed.`
				: t`Job ${state.buildJob.id} · ${state.buildJob.status}`
	const activationStatus = latestSnapshotIsActive
		? t`WADS v${state.activeSnapshot?.version ?? 0} is active.`
		: state.latestSnapshot
			? t`WADS v${state.latestSnapshot.version} is waiting for approval or activation.`
			: t`Build a WADS snapshot first.`
	const publicationStatus =
		publicationMatchesActive && state.publication
			? t`Flow v${state.publication.classification_version} · WADS v${state.publication.dimension_version}`
			: state.publication
				? t`Flow v${state.publication.classification_version} uses an older address snapshot.`
				: state.activeSnapshot
					? t`Publish the active address snapshot with the saved classification profile.`
					: t`Activate a WADS snapshot first.`
	const workerStatus =
		state.workers.length === 0
			? t`No Flow worker is enrolled.`
			: t`${activeWorkerCount}/${state.workers.length} active · ${installedWorkerIDs.size} installed the latest version`

	return (
		<section className="grid gap-4 rounded-lg border border-border bg-card p-4">
			<div className="flex flex-wrap items-start justify-between gap-3">
				<div>
					<h2 className="text-lg font-semibold">
						<Trans>Flow activation</Trans>
					</h2>
					<p className="text-sm text-muted-foreground">
						<Trans>
							Compile the address library, activate it, publish the classification version, then start a worker.
						</Trans>
					</p>
				</div>
				<Button variant="outline" size="sm" onClick={refresh} disabled={loading || Boolean(working)}>
					<RefreshCwIcon className={`me-2 h-4 w-4 ${loading ? "animate-spin" : ""}`} />
					<Trans>Refresh status</Trans>
				</Button>
			</div>

			<div className="grid gap-3 lg:grid-cols-4">
				<Step
					number={1}
					title={t`Build address snapshot`}
					detail={snapshotDetail}
					state={
						state.latestSnapshot
							? "done"
							: buildRunning
								? "running"
								: state.buildJob?.status === "failed"
									? "error"
									: "todo"
					}
					action={
						canPublish && !state.latestSnapshot ? (
							<Button size="sm" onClick={buildSnapshot} disabled={Boolean(working) || buildRunning}>
								<Trans>Build WADS snapshot</Trans>
							</Button>
						) : undefined
					}
				/>
				<Step
					number={2}
					title={t`Approve and activate`}
					detail={activationStatus}
					state={latestSnapshotIsActive ? "done" : state.latestSnapshot ? "todo" : "blocked"}
					action={
						canPublish && state.latestSnapshot && !latestSnapshotIsActive ? (
							<Button size="sm" onClick={approveAndActivate} disabled={Boolean(working)}>
								<Trans>Approve and activate</Trans>
							</Button>
						) : undefined
					}
				/>
				<Step
					number={3}
					title={t`Publish Flow classification`}
					detail={publicationStatus}
					state={publicationMatchesActive ? "done" : state.activeSnapshot ? "todo" : "blocked"}
					action={
						canPublish && state.activeSnapshot && !publicationMatchesActive ? (
							<Button size="sm" onClick={publishFlowVersion} disabled={Boolean(working)}>
								<Trans>Publish Flow version</Trans>
							</Button>
						) : undefined
					}
				/>
				<Step
					number={4}
					title={t`Start Flow worker`}
					detail={workerStatus}
					state={workerReady ? "done" : publicationMatchesActive ? "todo" : "blocked"}
					action={
						publicationMatchesActive && !workerReady ? (
							<Button size="sm" variant="outline" asChild>
								<a href={`${getPagePath($router, "agents")}?enroll=flow_worker`}>
									<Trans>Enroll and start worker</Trans>
									<ExternalLinkIcon className="ms-2 h-3.5 w-3.5" />
								</a>
							</Button>
						) : undefined
					}
				/>
			</div>

			{working ? (
				<div className="flex items-center gap-2 text-sm text-muted-foreground">
					<LoaderCircleIcon className="h-4 w-4 animate-spin" />
					<Trans>Applying lifecycle change…</Trans>
				</div>
			) : null}
			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
		</section>
	)
})

function Step({
	number,
	title,
	detail,
	state,
	action,
}: {
	number: number
	title: string
	detail: string
	state: "done" | "running" | "error" | "todo" | "blocked"
	action?: ReactNode
}) {
	const { t } = useLingui()
	const Icon =
		state === "done"
			? CheckCircle2Icon
			: state === "running"
				? LoaderCircleIcon
				: state === "error"
					? AlertTriangleIcon
					: CircleIcon
	const label =
		state === "done"
			? t`Complete`
			: state === "running"
				? t`Running`
				: state === "error"
					? t`Failed`
					: state === "blocked"
						? t`Blocked`
						: t`Pending`
	return (
		<div className="grid min-h-44 content-start gap-3 rounded-md border border-border p-3">
			<div className="flex items-center justify-between gap-2">
				<div className="flex items-center gap-2 text-sm font-semibold">
					<Icon
						className={`h-4 w-4 ${state === "done" ? "text-green-600" : state === "error" ? "text-destructive" : "text-muted-foreground"} ${state === "running" ? "animate-spin" : ""}`}
					/>
					<span>
						{number}. {title}
					</span>
				</div>
				<Badge variant={state === "done" ? "success" : state === "error" ? "destructive" : "secondary"}>{label}</Badge>
			</div>
			<p className="min-h-10 text-xs leading-5 text-muted-foreground">{detail}</p>
			{action ? <div className="mt-auto">{action}</div> : null}
		</div>
	)
}

async function loadRuntimeStatus(): Promise<RuntimeStatus | null> {
	try {
		return await api.send<RuntimeStatus>("/api/v1/dimensions/address/status")
	} catch (cause) {
		if (cause instanceof WatchdogAPIError && cause.status === 404) return null
		throw cause
	}
}

function currentMinuteISO() {
	return new Date(Math.floor(Date.now() / 60_000) * 60_000).toISOString()
}

function nextMinuteISO() {
	return new Date(Math.ceil(Date.now() / 60_000) * 60_000).toISOString()
}

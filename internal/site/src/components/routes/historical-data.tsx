import { Trans, useLingui } from "@lingui/react/macro"
import { ArchiveIcon, EyeIcon, HistoryIcon, Trash2Icon } from "lucide-react"
import { memo, useEffect, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { pb } from "@/lib/api"

type HistoricalPreview = {
	EstimatedSamples?: number
	estimated_samples?: number
	EstimatedBytes?: number
	estimated_bytes?: number
}

type HistoricalOperation = {
	ID?: string
	id?: string
	Status?: string
	status?: string
}

export default memo(() => {
	const { t } = useLingui()
	const [resourceType, setResourceType] = useState("DeviceID")
	const [resourceID, setResourceID] = useState("")
	const [start, setStart] = useState("")
	const [end, setEnd] = useState("")
	const [sampleStep, setSampleStep] = useState("300")
	const [preview, setPreview] = useState<HistoricalPreview | null>(null)
	const [operation, setOperation] = useState<HistoricalOperation | null>(null)
	const [message, setMessage] = useState("")
	const [busy, setBusy] = useState(false)

	useEffect(() => {
		document.title = `${t`Historical Data`} / WatchDog`
	}, [t])

	const requestBody = () => ({
		[resourceType]: resourceID,
		Start: toISOString(start),
		End: toISOString(end),
	})

	const previewData = async () => {
		setBusy(true)
		setMessage("")
		setOperation(null)
		try {
			const data = await pb.send<HistoricalPreview>(`/api/v1/historical/preview?sample_step=${sampleStep}`, {
				method: "POST",
				body: requestBody(),
			})
			setPreview(data)
		} catch (err) {
			setMessage(err instanceof Error ? err.message : t`Failed to preview historical data`)
		} finally {
			setBusy(false)
		}
	}

	const createOperation = async (action: "archive" | "delete") => {
		setBusy(true)
		setMessage("")
		setPreview(null)
		try {
			const operationID = `hist-${Date.now()}`
			const data = await pb.send<HistoricalOperation>(`/api/v1/historical/${action}?operation_id=${operationID}`, {
				method: "POST",
				body: requestBody(),
			})
			setOperation(data)
		} catch (err) {
			setMessage(err instanceof Error ? err.message : t`Failed to create historical operation`)
		} finally {
			setBusy(false)
		}
	}

	return (
		<div className="grid gap-4">
			<div className="flex items-center gap-2">
				<HistoryIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
				<h1 className="text-xl font-semibold tracking-normal">
					<Trans>Historical Data</Trans>
				</h1>
			</div>

			<div className="grid gap-4 rounded-md border border-border p-4 lg:grid-cols-5">
				<Field label={t`Resource`}>
					<Select value={resourceType} onValueChange={setResourceType}>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="TargetID">target</SelectItem>
							<SelectItem value="DeviceID">device</SelectItem>
							<SelectItem value="PortID">port</SelectItem>
						</SelectContent>
					</Select>
				</Field>
				<Field label={t`ID`}>
					<Input value={resourceID} onChange={(event) => setResourceID(event.target.value)} />
				</Field>
				<Field label={t`Start`}>
					<Input type="datetime-local" value={start} onChange={(event) => setStart(event.target.value)} />
				</Field>
				<Field label={t`End`}>
					<Input type="datetime-local" value={end} onChange={(event) => setEnd(event.target.value)} />
				</Field>
				<Field label={t`Step`}>
					<Select value={sampleStep} onValueChange={setSampleStep}>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="60">1m</SelectItem>
							<SelectItem value="300">5m</SelectItem>
							<SelectItem value="900">15m</SelectItem>
							<SelectItem value="1800">30m</SelectItem>
						</SelectContent>
					</Select>
				</Field>
			</div>

			<div className="flex flex-wrap gap-2">
				<Button variant="outline" onClick={previewData} disabled={busy}>
					<EyeIcon className="me-2 h-4 w-4" />
					<Trans>Preview</Trans>
				</Button>
				<Button variant="outline" onClick={() => createOperation("archive")} disabled={busy}>
					<ArchiveIcon className="me-2 h-4 w-4" />
					<Trans>Archive</Trans>
				</Button>
				<Button variant="outline" onClick={() => createOperation("delete")} disabled={busy}>
					<Trash2Icon className="me-2 h-4 w-4" />
					<Trans>Delete</Trans>
				</Button>
			</div>

			{message ? (
				<div className="rounded-md border border-border p-3 text-sm text-muted-foreground">{message}</div>
			) : null}
			{preview ? (
				<div className="grid gap-3 rounded-md border border-border p-4 sm:grid-cols-2">
					<Info label={t`Samples`} value={String(preview.EstimatedSamples ?? preview.estimated_samples ?? 0)} />
					<Info label={t`Bytes`} value={formatBytes(preview.EstimatedBytes ?? preview.estimated_bytes)} />
				</div>
			) : null}
			{operation ? (
				<div className="grid gap-3 rounded-md border border-border p-4 sm:grid-cols-2">
					<Info label={t`Operation`} value={operation.ID ?? operation.id ?? "—"} />
					<Info label={t`Status`} value={operation.Status ?? operation.status ?? "—"} />
				</div>
			) : null}
		</div>
	)
})

function Field({ label, children }: { label: string; children: React.ReactNode }) {
	return (
		<div className="grid gap-1.5">
			<Label>{label}</Label>
			{children}
		</div>
	)
}

function Info({ label, value }: { label: string; value: string }) {
	return (
		<div>
			<div className="text-xs text-muted-foreground">{label}</div>
			<div className="mt-1 font-mono text-sm">{value}</div>
		</div>
	)
}

function toISOString(value: string) {
	if (!value) {
		return ""
	}
	const date = new Date(value)
	return Number.isNaN(date.getTime()) ? value : date.toISOString()
}

function formatBytes(value?: number) {
	if (!value || value <= 0) {
		return "0"
	}
	if (value >= 1_000_000_000) {
		return `${(value / 1_000_000_000).toFixed(2)} GB`
	}
	if (value >= 1_000_000) {
		return `${(value / 1_000_000).toFixed(2)} MB`
	}
	return `${value} B`
}

export type ExportTask = {
	ID?: string
	id?: string
	TargetID?: string
	target_id?: string
	PortID?: string
	port_id?: string
	PeriodType?: string
	period_type?: string
	RangeStart?: string
	range_start?: string
	RangeEnd?: string
	range_end?: string
	Step?: number
	step?: number
	Aggregation?: string
	aggregation?: string
	ValueMode?: string
	value_mode?: string
	Format?: string
	format?: string
	Status?: string
	status?: string
	FileRef?: string
	file_ref?: string
	ErrorMessage?: string
	error_message?: string
	CreatedAt?: string
	created_at?: string
	UpdatedAt?: string
	updated_at?: string
}

export function exportID(task: ExportTask) {
	return task.ID ?? task.id ?? ""
}

export function exportStatus(task: ExportTask) {
	return task.Status ?? task.status ?? ""
}

export function exportDownloadURL(task: ExportTask) {
	const id = exportID(task)
	return id ? `/api/v1/exports/${id}/download` : ""
}

export function formatExportRange(task: ExportTask) {
	const start = task.RangeStart ?? task.range_start
	const end = task.RangeEnd ?? task.range_end
	if (!start || !end) {
		return "-"
	}
	return `${formatExportDate(start)} - ${formatExportDate(end)}`
}

export function formatExportDate(value?: string) {
	if (!value) {
		return "-"
	}
	const date = new Date(value)
	return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
}

export function formatExportStep(value?: number) {
	if (!value || value <= 0) {
		return "-"
	}
	const seconds = value / 1_000_000_000
	if (seconds % 3600 === 0) {
		return `${seconds / 3600}h`
	}
	if (seconds % 60 === 0) {
		return `${seconds / 60}m`
	}
	return `${seconds}s`
}

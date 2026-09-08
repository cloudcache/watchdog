export type ExportTask = {
	ID?: string
	id?: string
	TargetID?: string
	target_id?: string
	PortID?: string
	port_id?: string
	ContractVersion?: number
	contract_version?: number
	DatasetKey?: string
	dataset_key?: string
	QueryHash?: string
	query_hash?: string
	ValueLayer?: string
	value_layer?: string
	OperationJobID?: string
	operation_job_id?: string
	RetentionSeconds?: number
	retention_seconds?: number
	ArtifactSchemaVersion?: number
	artifact_schema_version?: number
	ContentType?: string
	content_type?: string
	RowCount?: number
	row_count?: number
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
	Checksum?: string
	checksum?: string
	SizeBytes?: number
	size_bytes?: number
	ExpiresAt?: string
	expires_at?: string
	RowVersion?: number
	row_version?: number
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

export function exportValueLayer(task: ExportTask) {
	return task.ValueLayer ?? task.value_layer ?? ""
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

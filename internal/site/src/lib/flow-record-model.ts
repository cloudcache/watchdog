export type FlowRecordRow = {
	event_time: string
	source_coordinate: {
		source_stream_id: string
		kafka_partition: number
		kafka_offset: number
		record_index: number
	}
	values: Record<string, unknown>
}

export function buildFlowRecordRows(rows: FlowRecordRow[]) {
	return rows.map((row) => ({
		event_time: new Date(row.event_time).toLocaleString(),
		src_ip: String(row.values.src_ip ?? ""),
		dst_ip: String(row.values.dst_ip ?? ""),
		src_port: row.values.src_port ?? "",
		dst_port: row.values.dst_port ?? "",
		protocol: flowProtocolLabel(row.values.ip_protocol),
		direction: String(row.values.business_direction ?? ""),
		category: String(row.values.category ?? ""),
		source_asn: row.values.source_asn ?? "",
		destination_asn: row.values.destination_asn ?? "",
		remote_asn: row.values.remote_asn ?? "",
		country: String(row.values.remote_country ?? ""),
		raw_bytes: formatFlowBytes(row.values.raw_bytes),
		estimated_bytes: formatFlowBytes(row.values.estimated_bytes),
		sampling_rate: row.values.sampling_rate ?? "",
		quality_flags: row.values.quality_flags ?? "",
		source_coordinate: `${row.source_coordinate.source_stream_id}/${row.source_coordinate.kafka_partition}/${row.source_coordinate.kafka_offset}/${row.source_coordinate.record_index}`,
	}))
}

export function updateFlowRecordCursors(cursors: string[], page: number, nextCursor?: string): string[] {
	return [...cursors.slice(0, page + 1), nextCursor ?? ""]
}

export function flowProtocolLabel(value: unknown) {
	const protocol = Number(value)
	if (protocol === 6) return "TCP (6)"
	if (protocol === 17) return "UDP (17)"
	if (protocol === 1) return "ICMP (1)"
	if (protocol === 58) return "ICMPv6 (58)"
	return Number.isFinite(protocol) ? String(protocol) : ""
}

export function formatFlowBytes(value: unknown) {
	const bytes = Number(value)
	if (!Number.isFinite(bytes)) return ""
	if (bytes >= 1e9) return `${(bytes / 1e9).toFixed(2)} GB`
	if (bytes >= 1e6) return `${(bytes / 1e6).toFixed(2)} MB`
	if (bytes >= 1e3) return `${(bytes / 1e3).toFixed(2)} KB`
	return `${bytes} B`
}

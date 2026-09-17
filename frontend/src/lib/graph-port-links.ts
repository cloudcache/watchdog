export type GraphPortLink = { href?: string; port_id?: string; status?: string }
export type GraphPortStatus = "up" | "down" | "disabled" | "unknown"

export function graphPortIDFromLink(link: GraphPortLink): string {
	if (link.port_id?.trim()) return link.port_id.trim()
	const match = link.href?.match(/\/network\/ports\/([^/?#]+)/)
	return match ? decodeURIComponent(match[1]) : ""
}

export function normalizeGraphPortStatus(value?: string): GraphPortStatus {
	switch (value?.trim().toLowerCase()) {
		case "up":
		case "1":
			return "up"
		case "down":
		case "2":
			return "down"
		case "disabled":
		case "admin_down":
			return "disabled"
		default:
			return "unknown"
	}
}

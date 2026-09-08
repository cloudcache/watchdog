import { parseAddressEntries, parsePrefixLabels, parseUnsignedIntegerEntries } from "./address-set-form.ts"

export type ExistingAddressPrefix = {
	id: string
	row_version: number
}

export type AddressPrefixRevisionForm = {
	cidrs: string
	labels: string
	source: string
	asn: string
	geoLeafID: string
	operatorID: string
}

export type AddressPrefixRevisionOperation = {
	action: "create" | "delete"
	prefix_id?: string
	expected_version?: number
	cidr?: string
	labels?: Record<string, string>
	source?: string
	asn?: number
	geo_leaf_id?: string
	operator_id?: string
}

export function buildAddressPrefixRevisionOperations(
	selected: ExistingAddressPrefix[],
	form: AddressPrefixRevisionForm
): AddressPrefixRevisionOperation[] {
	const labels = parsePrefixLabels(form.labels)
	const asns = parseUnsignedIntegerEntries(form.asn, 1, 4_294_967_295, "ASN")
	if (asns.length > 1) throw new Error("Only one ASN can be assigned to a prefix batch")
	const deletes: AddressPrefixRevisionOperation[] = [...selected]
		.sort((left, right) => left.id.localeCompare(right.id))
		.map((prefix) => ({ action: "delete", prefix_id: prefix.id, expected_version: prefix.row_version }))
	const creates = parseAddressEntries(form.cidrs).map(
		(cidr): AddressPrefixRevisionOperation => ({
			action: "create",
			cidr,
			labels,
			source: form.source.trim() || "manual",
			...(asns.length ? { asn: asns[0] } : {}),
			...(form.geoLeafID.trim() ? { geo_leaf_id: form.geoLeafID.trim() } : {}),
			...(form.operatorID.trim() ? { operator_id: form.operatorID.trim() } : {}),
		})
	)
	return [...deletes, ...creates]
}

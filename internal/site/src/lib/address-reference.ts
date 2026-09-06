export type AddressReferenceOption = {
	id: string
	label: string
	description?: string
}

export function mergeAddressReferenceOptions(current: AddressReferenceOption[], incoming: AddressReferenceOption[]) {
	const merged = new Map(current.map((option) => [option.id, option]))
	for (const option of incoming) merged.set(option.id, option)
	return [...merged.values()]
}

export function toggleAddressReference(values: string[], id: string, checked: boolean, multiple: boolean) {
	if (!checked) return values.filter((value) => value !== id)
	if (!multiple) return [id]
	return values.includes(id) ? values : [...values, id]
}

export function addressReferenceSummary(
	values: string[],
	options: AddressReferenceOption[],
	placeholder: string,
	selectedLabel: string
) {
	if (!values.length) return placeholder
	if (values.length === 1) return options.find((option) => option.id === values[0])?.label ?? selectedLabel
	return `${values.length.toLocaleString()} ${selectedLabel}`
}

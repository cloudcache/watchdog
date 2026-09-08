export function parseASNList(value: string) {
	if (!value.trim()) return []
	return [
		...new Set(
			value
				.split(/[\s,]+/)
				.filter(Boolean)
				.map((item) => {
					const asn = Number(item)
					if (!Number.isInteger(asn) || asn <= 0 || asn > 4_294_967_295) {
						throw new Error(`Invalid ASN: ${item}`)
					}
					return asn
				})
		),
	].sort((left, right) => left - right)
}

export function toggleListValue<T>(values: T[], value: T, checked: boolean) {
	return checked ? [...new Set([...values, value])] : values.filter((item) => item !== value)
}

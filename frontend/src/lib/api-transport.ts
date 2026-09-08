export function resolveAPIBase(configuredAPIURL: string | undefined, basePath: string) {
	return configuredAPIURL?.trim() || basePath
}

export function responseFilename(disposition: string | null) {
	if (!disposition) return ""
	const encoded = disposition.match(/filename\*=UTF-8''([^;]+)/i)?.[1]
	if (encoded) {
		try {
			return decodeURIComponent(encoded)
		} catch {
			return encoded
		}
	}
	return disposition.match(/filename="?([^";]+)"?/i)?.[1] ?? ""
}

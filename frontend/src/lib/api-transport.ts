export function resolveAPIBase(configuredAPIURL: string | undefined, basePath: string) {
	return configuredAPIURL?.trim() || basePath
}

export function isHTMLAPIResponse(contentType: string | null, text: string) {
	return contentType?.toLowerCase().includes("text/html") === true || /^\s*(?:<!doctype\s+html|<html\b)/i.test(text)
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

export const apiRuntimeDefaults = {
	bootstrapAttemptTimeoutMS: 2_500,
	bootstrapRetries: 2,
	bootstrapRetryDelayMS: 250,
} as const

export function positiveRuntimeInteger(value: number | undefined, fallback: number) {
	return Number.isSafeInteger(value) && Number(value) > 0 ? Number(value) : fallback
}

export function nonNegativeRuntimeInteger(value: number | undefined, fallback: number) {
	return Number.isSafeInteger(value) && Number(value) >= 0 ? Number(value) : fallback
}

export async function retryTransient<T>(
	operation: (attempt: number) => Promise<T>,
	shouldRetry: (error: unknown) => boolean,
	delaysMS: readonly number[]
) {
	for (let attempt = 0; ; attempt++) {
		try {
			return await operation(attempt)
		} catch (error) {
			if (attempt >= delaysMS.length || !shouldRetry(error)) throw error
			const delay = delaysMS[attempt]
			if (delay > 0) await new Promise((resolve) => setTimeout(resolve, delay))
		}
	}
}

/** Compose caller cancellation with an explicit operation deadline, when one
 * is supplied. Normal API requests deliberately have no transport timeout. */
export function requestDeadline(callerSignal: AbortSignal | null | undefined, timeoutMs = 0) {
	const controller = new AbortController()
	let timedOut = false
	let timer: ReturnType<typeof setTimeout> | undefined

	const abortFromCaller = () => controller.abort(callerSignal?.reason)
	if (callerSignal?.aborted) {
		abortFromCaller()
	} else {
		callerSignal?.addEventListener("abort", abortFromCaller, { once: true })
	}
	if (Number.isFinite(timeoutMs) && timeoutMs > 0) {
		timer = setTimeout(() => {
			timedOut = true
			controller.abort()
		}, timeoutMs)
	}

	return {
		signal: controller.signal,
		didTimeout: () => timedOut,
		cleanup: () => {
			if (timer !== undefined) clearTimeout(timer)
			callerSignal?.removeEventListener("abort", abortFromCaller)
		},
	}
}

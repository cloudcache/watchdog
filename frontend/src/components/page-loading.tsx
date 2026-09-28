import { Trans } from "@lingui/react/macro"
import { LoaderCircleIcon } from "lucide-react"
import { useEffect, useState } from "react"
import { Button } from "@/components/ui/button"

/** Shared loading state which always grows an escape hatch instead of spinning forever. */
export function PageLoading({ onRetry, timeoutMs = 10_000 }: { onRetry?: () => void; timeoutMs?: number }) {
	const [slow, setSlow] = useState(false)
	useEffect(() => {
		const timer = window.setTimeout(() => setSlow(true), timeoutMs)
		return () => window.clearTimeout(timer)
	}, [timeoutMs])
	return (
		<div className="grid place-items-center gap-3 py-16 text-center text-sm text-muted-foreground">
			<output className="flex items-center justify-center gap-2">
				<LoaderCircleIcon className="size-4 animate-spin" />
				<Trans>Loading...</Trans>
			</output>
			{slow ? (
				<div className="grid gap-2">
					<p>
						<Trans>The request is taking longer than expected.</Trans>
					</p>
					<Button type="button" variant="outline" size="sm" onClick={onRetry ?? (() => window.location.reload())}>
						<Trans>Retry</Trans>
					</Button>
				</div>
			) : null}
		</div>
	)
}

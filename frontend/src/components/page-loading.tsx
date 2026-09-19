import { Trans } from "@lingui/react/macro"
import { LoaderCircleIcon } from "lucide-react"

/** Shared loading state for the app shell and lazy route chunks. */
export function PageLoading() {
	return (
		<output className="flex items-center justify-center gap-2 py-16 text-sm text-muted-foreground">
			<LoaderCircleIcon className="size-4 animate-spin" />
			<Trans>Loading...</Trans>
		</output>
	)
}

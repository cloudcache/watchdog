import { Trans } from "@lingui/react/macro"
import { Component, type ErrorInfo, type ReactNode } from "react"
import { Button } from "@/components/ui/button"

type Props = {
	children: ReactNode
	/** Changing this value (e.g. the current route path) clears a caught error. */
	resetKey?: string
}
type State = { error?: Error }

/** Last-resort boundary. Without it a render error or a failed lazy chunk (typical
 * after a deploy replaces the hashed bundles) unmounts the whole tree and leaves a
 * blank page with nothing to click. */
export class ErrorBoundary extends Component<Props, State> {
	state: State = {}

	static getDerivedStateFromError(error: Error): State {
		return { error }
	}

	componentDidCatch(error: Error, info: ErrorInfo) {
		console.error("render error", error, info.componentStack)
	}

	componentDidUpdate(previous: Props) {
		if (this.state.error && previous.resetKey !== this.props.resetKey) {
			this.setState({ error: undefined })
		}
	}

	render() {
		const { error } = this.state
		if (!error) return this.props.children
		return (
			<div role="alert" className="my-14 grid place-content-center gap-3 px-4 text-center">
				<h1 className="text-xl font-semibold">
					<Trans>Something went wrong</Trans>
				</h1>
				<p className="text-sm text-muted-foreground break-all">{error.message}</p>
				<div className="flex justify-center gap-2">
					<Button variant="outline" size="sm" onClick={() => this.setState({ error: undefined })}>
						<Trans>Retry</Trans>
					</Button>
					<Button size="sm" onClick={() => window.location.reload()}>
						<Trans>Reload page</Trans>
					</Button>
				</div>
			</div>
		)
	}
}

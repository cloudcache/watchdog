import { Trans } from "@lingui/react/macro"
import { ShieldCheckIcon } from "lucide-react"

export default function FlowVPN() {
	return (
		<div className="grid gap-4">
			<div className="flex items-center gap-2">
				<ShieldCheckIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
				<div>
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>VPN Risk</Trans>
					</h1>
					<p className="text-sm text-muted-foreground">
						<Trans>Review scored candidates, evidence, probes and dispositions.</Trans>
					</p>
				</div>
			</div>
			<div className="rounded-md border border-border bg-card p-6 text-sm text-muted-foreground">
				<Trans>
					VPN candidate scoring is available in the Flow data plane. The tenant-scoped findings and rule publication API
					must be enabled before this page can display production evidence.
				</Trans>
			</div>
		</div>
	)
}

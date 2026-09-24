import { Trans } from "@lingui/react/macro"
import { getPagePath } from "@/lib/page-path"
import { DatabaseIcon, MapPinnedIcon } from "lucide-react"
import { memo, type ReactNode } from "react"
import { $router, Link } from "@/components/router"
import { buttonVariants } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import FlowCustomerBoundaries from "./flow-customer-boundaries"
import FlowWorkerDeployments from "./flow-worker-deployments"

export default memo(function FlowAttribution() {
	return (
		<div className="my-4 grid gap-4">
			<div>
				<h1 className="text-2xl font-semibold">
					<Trans>Flow Data Attribution</Trans>
				</h1>
				<p className="text-sm text-muted-foreground">
					<Trans>
						Keep the shared Geo/operator library separate from each Flow device's customer source boundaries.
					</Trans>
				</p>
			</div>
			<div className="grid gap-3 lg:grid-cols-2">
				<AttributionStep
					icon={MapPinnedIcon}
					title={<Trans>Shared Geo and operator library</Trans>}
					description={
						<Trans>
							Maintain reusable Internet geography, operators, and ASN evidence. Do not add customer-only source ranges
							here.
						</Trans>
					}
					href={getPagePath($router, "address_library", { section: "base-data" })}
					linkLabel={<Trans>Manage base data</Trans>}
				/>
				<AttributionStep
					icon={DatabaseIcon}
					title={<Trans>Address snapshot lifecycle</Trans>}
					description={
						<Trans>
							Build and activate the shared Geo/operator snapshot independently, then pair it with customer boundaries
							when publishing Flow.
						</Trans>
					}
					href={getPagePath($router, "address_library", { section: "publications" })}
					linkLabel={<Trans>Manage address versions</Trans>}
				/>
			</div>
			<FlowCustomerBoundaries />
			<FlowWorkerDeployments />
		</div>
	)
})

function AttributionStep({
	icon: Icon,
	title,
	description,
	href,
	linkLabel,
}: {
	icon: typeof DatabaseIcon
	title: ReactNode
	description: ReactNode
	href: string
	linkLabel: ReactNode
}) {
	return (
		<Card>
			<CardHeader>
				<div className="flex items-center gap-2">
					<Icon className="h-5 w-5 text-muted-foreground" />
					<CardTitle className="text-base">{title}</CardTitle>
				</div>
				<CardDescription>{description}</CardDescription>
			</CardHeader>
			<CardContent>
				<Link href={href} className={buttonVariants({ variant: "outline", size: "sm" })}>
					{linkLabel}
				</Link>
			</CardContent>
		</Card>
	)
}

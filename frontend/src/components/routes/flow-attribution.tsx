import { Trans } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { DatabaseIcon, MapPinnedIcon, NetworkIcon } from "lucide-react"
import { memo, type ReactNode } from "react"
import { $router, Link } from "@/components/router"
import { buttonVariants } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import FlowEnrichmentPublications from "./flow-enrichment-publications"

export default memo(function FlowAttribution() {
	return (
		<div className="my-4 grid gap-4">
			<div>
				<h1 className="text-2xl font-semibold">
					<Trans>Flow Data Attribution</Trans>
				</h1>
				<p className="text-sm text-muted-foreground">
					<Trans>Maintain one relationship chain: geography and operator → customer source prefix → Flow device.</Trans>
				</p>
			</div>
			<div className="grid gap-3 lg:grid-cols-3">
				<AttributionStep
					icon={MapPinnedIcon}
					title={<Trans>1. Geography and operators</Trans>}
					description={<Trans>Maintain the reusable geography tree, operator names, and operator ASN sets.</Trans>}
					href={getPagePath($router, "address_library", { section: "base-data" })}
					linkLabel={<Trans>Manage base data</Trans>}
				/>
				<AttributionStep
					icon={DatabaseIcon}
					title={<Trans>2. Customer source prefixes</Trans>}
					description={<Trans>Create each customer CIDR and assign its geography and operator.</Trans>}
					href={getPagePath($router, "address_library", { section: "prefixes" })}
					linkLabel={<Trans>Manage address prefixes</Trans>}
				/>
				<AttributionStep
					icon={NetworkIcon}
					title={<Trans>3. Device association and publication</Trans>}
					description={<Trans>Select the customer source prefixes observed behind each Flow device, then publish one immutable version pair.</Trans>}
					href={getPagePath($router, "address_library", { section: "publications" })}
					linkLabel={<Trans>Manage address versions</Trans>}
				/>
			</div>
			<FlowEnrichmentPublications />
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

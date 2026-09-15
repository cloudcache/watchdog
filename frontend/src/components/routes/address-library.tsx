import { Trans } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { DatabaseIcon } from "lucide-react"
import { memo } from "react"
import { $router, navigate } from "@/components/router"
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs"
import AddressImports from "./address-imports"
import AddressLines from "./address-lines"
import AddressMath from "./address-math"
import AddressPublications from "./address-publications"
import AddressPrefixes from "./address-prefixes"
import AddressRevisions from "./address-revisions"
import AddressSets from "./address-sets"
import AddressTaxonomy from "./address-taxonomy"

const sections = [
	"imports",
	"prefixes",
	"sets",
	"tools",
	"batch",
	"publications",
	"geography",
	"operators",
	"lines",
] as const
type AddressLibrarySection = (typeof sections)[number]

function normalizeSection(value: string): AddressLibrarySection {
	return sections.includes(value as AddressLibrarySection) ? (value as AddressLibrarySection) : "imports"
}

export default memo(function AddressLibrary({ section }: { section: string }) {
	const active = normalizeSection(section)
	return (
		<div className="grid gap-4">
			<div className="flex items-center gap-2">
				<DatabaseIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
				<h1 className="text-xl font-semibold tracking-normal">
					<Trans>Address Library</Trans>
				</h1>
			</div>
			<Tabs
				value={active}
				onValueChange={(value) => navigate(getPagePath($router, "address_library", { section: value }))}
			>
				<TabsList className="h-11 w-full justify-start overflow-x-auto p-1.5">
					<TabsTrigger value="imports">
						<Trans>Imports</Trans>
					</TabsTrigger>
					<TabsTrigger value="prefixes">
						<Trans>Prefixes</Trans>
					</TabsTrigger>
					<TabsTrigger value="sets">
						<Trans>Sets</Trans>
					</TabsTrigger>
					<TabsTrigger value="tools">
						<Trans>Set Tools</Trans>
					</TabsTrigger>
					<TabsTrigger value="batch">
						<Trans>Batch Apply</Trans>
					</TabsTrigger>
					<TabsTrigger value="publications">
						<Trans>Publications</Trans>
					</TabsTrigger>
					<TabsTrigger value="geography">
						<Trans>Geography</Trans>
					</TabsTrigger>
					<TabsTrigger value="operators">
						<Trans>Operators</Trans>
					</TabsTrigger>
					<TabsTrigger value="lines">
						<Trans>Lines</Trans>
					</TabsTrigger>
				</TabsList>
			</Tabs>
			{active === "imports" ? <AddressImports /> : null}
			{active === "prefixes" ? <AddressPrefixes /> : null}
			{active === "sets" ? <AddressSets /> : null}
			{active === "tools" ? <AddressMath /> : null}
			{active === "batch" ? <AddressRevisions /> : null}
			{active === "publications" ? <AddressPublications /> : null}
			{active === "geography" ? <AddressTaxonomy kind="geography" /> : null}
			{active === "operators" ? <AddressTaxonomy kind="operators" /> : null}
			{active === "lines" ? <AddressLines /> : null}
		</div>
	)
})

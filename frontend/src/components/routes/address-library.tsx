import { Trans } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { DatabaseIcon } from "lucide-react"
import { memo, type ReactNode } from "react"
import { $router, navigate } from "@/components/router"
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs"
import AddressBaseData from "./address-base-data"
import AddressImports from "./address-imports"
import AddressLines from "./address-lines"
import AddressMath from "./address-math"
import AddressPrefixes from "./address-prefixes"
import AddressPublications from "./address-publications"
import AddressRevisions from "./address-revisions"
import AddressSets from "./address-sets"

// P5 IA convergence: the flat tabs collapse into four top-level groups, with the
// prefix-working features as a Workbench sub-strip. Geography/Operators fold into
// Base Data (kept as legacy section aliases so old URLs still resolve).
type Leaf = "prefixes" | "imports" | "sets" | "tools" | "batch" | "lines" | "base-data" | "publications"

const LEAF_SECTIONS: Leaf[] = ["prefixes", "imports", "sets", "tools", "batch", "lines", "base-data", "publications"]
const LEGACY_ALIASES: Record<string, Leaf> = { geography: "base-data", operators: "base-data" }

function normalizeSection(value: string): Leaf {
	if (LEAF_SECTIONS.includes(value as Leaf)) return value as Leaf
	return LEGACY_ALIASES[value] ?? "prefixes"
}

const LEAF_COMPONENTS: Record<Leaf, ReactNode> = {
	prefixes: <AddressPrefixes />,
	imports: <AddressImports />,
	sets: <AddressSets />,
	tools: <AddressMath />,
	batch: <AddressRevisions />,
	lines: <AddressLines />,
	"base-data": <AddressBaseData />,
	publications: <AddressPublications />,
}

export default memo(function AddressLibrary({ section }: { section: string }) {
	const active = normalizeSection(section)
	const groups: { key: string; label: ReactNode; sections: { key: Leaf; label: ReactNode }[] }[] = [
		{
			key: "workbench",
			label: <Trans>Workbench</Trans>,
			sections: [
				{ key: "prefixes", label: <Trans>Prefixes</Trans> },
				{ key: "imports", label: <Trans>Imports</Trans> },
				{ key: "sets", label: <Trans>Sets</Trans> },
				{ key: "tools", label: <Trans>Set Tools</Trans> },
				{ key: "batch", label: <Trans>Batch Apply</Trans> },
			],
		},
		{ key: "lines", label: <Trans>Lines & Regions</Trans>, sections: [{ key: "lines", label: <Trans>Lines</Trans> }] },
		{
			key: "base-data",
			label: <Trans>Base Data</Trans>,
			sections: [{ key: "base-data", label: <Trans>Base Data</Trans> }],
		},
		{
			key: "publications",
			label: <Trans>Versions</Trans>,
			sections: [{ key: "publications", label: <Trans>Publications</Trans> }],
		},
	]
	const activeGroup = groups.find((group) => group.sections.some((entry) => entry.key === active)) ?? groups[0]
	const go = (leaf: Leaf) => navigate(getPagePath($router, "address_library", { section: leaf }))

	return (
		<div className="grid gap-4">
			<div className="flex items-center gap-2">
				<DatabaseIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
				<h1 className="text-xl font-semibold tracking-normal">
					<Trans>Address Library</Trans>
				</h1>
			</div>
			<Tabs
				value={activeGroup.key}
				onValueChange={(value) => go(groups.find((g) => g.key === value)?.sections[0].key ?? "prefixes")}
			>
				<TabsList className="h-11 w-full justify-start overflow-x-auto p-1.5">
					{groups.map((group) => (
						<TabsTrigger key={group.key} value={group.key}>
							{group.label}
						</TabsTrigger>
					))}
				</TabsList>
			</Tabs>
			{activeGroup.sections.length > 1 ? (
				<Tabs value={active} onValueChange={(value) => go(value as Leaf)}>
					<TabsList className="h-9 w-full justify-start overflow-x-auto bg-transparent p-0">
						{activeGroup.sections.map((entry) => (
							<TabsTrigger key={entry.key} value={entry.key}>
								{entry.label}
							</TabsTrigger>
						))}
					</TabsList>
				</Tabs>
			) : null}
			{LEAF_COMPONENTS[active]}
		</div>
	)
})

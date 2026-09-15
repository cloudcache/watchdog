import { Trans } from "@lingui/react/macro"
import type { ReactNode } from "react"
import { memo, useState } from "react"
import { Button } from "@/components/ui/button"
import AddressTaxonomy from "./address-taxonomy"

type Category = {
	key: string
	label: ReactNode
	kind: "geography" | "operators"
	fixedKind?: string
}

// 基础数据 (P4): one entry for every dictionary — the geo hierarchy levels, the
// three flat base-data kinds, and operators — each a kind-locked CRUD.
export default memo(function AddressBaseData() {
	const categories: Category[] = [
		{ key: "continent", label: <Trans>Continents</Trans>, kind: "geography", fixedKind: "continent" },
		{ key: "country", label: <Trans>Countries</Trans>, kind: "geography", fixedKind: "country" },
		{ key: "province", label: <Trans>Provinces</Trans>, kind: "geography", fixedKind: "province" },
		{ key: "city", label: <Trans>Cities</Trans>, kind: "geography", fixedKind: "city" },
		{ key: "region", label: <Trans>Regions</Trans>, kind: "geography", fixedKind: "region" },
		{ key: "operators", label: <Trans>Operators</Trans>, kind: "operators" },
		{ key: "search_engine", label: <Trans>Search engines</Trans>, kind: "geography", fixedKind: "search_engine" },
		{ key: "cloud_provider", label: <Trans>Cloud providers</Trans>, kind: "geography", fixedKind: "cloud_provider" },
		{ key: "natural_region", label: <Trans>Natural regions</Trans>, kind: "geography", fixedKind: "natural_region" },
	]
	const [active, setActive] = useState(categories[0].key)
	const current = categories.find((category) => category.key === active) ?? categories[0]

	return (
		<div className="grid gap-3 lg:grid-cols-[180px_1fr]">
			<div className="flex flex-row flex-wrap gap-1 lg:flex-col">
				{categories.map((category) => (
					<Button
						key={category.key}
						variant={active === category.key ? "default" : "ghost"}
						size="sm"
						className="justify-start"
						onClick={() => setActive(category.key)}
					>
						{category.label}
					</Button>
				))}
			</div>
			<div>
				{current.kind === "operators" ? (
					<AddressTaxonomy key="operators" kind="operators" />
				) : (
					<AddressTaxonomy key={current.key} kind="geography" fixedKind={current.fixedKind} />
				)}
			</div>
		</div>
	)
})

import { Trans } from "@lingui/react/macro"
import { memo, useState } from "react"
import { Button } from "@/components/ui/button"
import GeoTreeBrowser from "./address-geo-tree"
import AddressTaxonomy from "./address-taxonomy"

// The Lines/Regions tab: a virtual geo-hierarchy browser (drill continents →
// cities and view each region's prefixes) plus the geo_lines definition CRUD.
export default memo(function AddressLines() {
	const [mode, setMode] = useState<"browse" | "definitions">("browse")
	return (
		<div className="grid gap-3">
			<div className="inline-flex w-fit rounded-md border border-border p-0.5">
				<Button variant={mode === "browse" ? "default" : "ghost"} size="sm" onClick={() => setMode("browse")}>
					<Trans>Browse</Trans>
				</Button>
				<Button variant={mode === "definitions" ? "default" : "ghost"} size="sm" onClick={() => setMode("definitions")}>
					<Trans>Lines</Trans>
				</Button>
			</div>
			{mode === "browse" ? <GeoTreeBrowser /> : <AddressTaxonomy kind="lines" />}
		</div>
	)
})

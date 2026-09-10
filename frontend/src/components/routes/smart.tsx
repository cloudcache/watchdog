import { useEffect } from "react"
import { FooterRepoLink } from "@/components/footer-repo-link"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"

export default function Smart() {
	useEffect(() => {
		document.title = `S.M.A.R.T. / Watchdog`
	}, [])

	return (
		<>
			<div className="grid gap-4">
				<Card>
					<CardHeader>
						<CardTitle>S.M.A.R.T.</CardTitle>
						<CardDescription>Storage telemetry is not enabled in this installation.</CardDescription>
					</CardHeader>
					<CardContent className="text-sm text-muted-foreground">
						Storage data will be available after the system agent ClickHouse slice is enabled.
					</CardContent>
				</Card>
			</div>
			<FooterRepoLink />
		</>
	)
}

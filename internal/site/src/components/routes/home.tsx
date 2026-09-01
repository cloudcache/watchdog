import { useLingui } from "@lingui/react/macro"
import { memo, Suspense, useEffect, useMemo } from "react"
import { ActiveAlerts } from "@/components/active-alerts"
import { FooterRepoLink } from "@/components/footer-repo-link"
import Targets from "@/components/routes/targets"

export default memo(() => {
	const { t } = useLingui()

	useEffect(() => {
		document.title = `${t`All Systems`} / Beszel`
	}, [t])

	return useMemo(
		() => (
			<>
				<div className="flex flex-col gap-4">
					<ActiveAlerts />
					<Suspense>
						<Targets />
					</Suspense>
				</div>
				<FooterRepoLink />
			</>
		),
		[]
	)
})

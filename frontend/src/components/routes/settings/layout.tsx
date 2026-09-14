import { t } from "@lingui/core/macro"
import { Trans, useLingui } from "@lingui/react/macro"
import { useStore } from "@nanostores/react"
import { getPagePath, redirectPage } from "@nanostores/router"
import { SettingsIcon } from "lucide-react"
import { lazy, useEffect } from "react"
import { $router } from "@/components/router.tsx"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card.tsx"
import { toast } from "@/components/ui/use-toast.ts"
import { saveUserPreferences } from "@/lib/api"
import { $userSettings } from "@/lib/stores.ts"
import type { UserSettings } from "@/types"
import { Separator } from "../../ui/separator"
import { SidebarNav } from "./sidebar-nav.tsx"

const generalSettingsImport = () => import("./general.tsx")
const GeneralSettings = lazy(generalSettingsImport)

export async function saveSettings(newSettings: Partial<UserSettings>) {
	try {
		await saveUserPreferences(newSettings)
		toast({
			title: t`Settings saved`,
			description: t`Your user settings have been updated.`,
		})
	} catch (e) {
		toast({
			title: t`Failed to save settings`,
			description: e instanceof Error ? e.message : t`Request failed`,
			variant: "destructive",
		})
	}
}

export default function SettingsLayout() {
	const { t } = useLingui()

	const sidebarNavItems = [
		{
			title: t({ message: `General`, comment: "Context: General settings" }),
			href: getPagePath($router, "settings", { name: "general" }),
			icon: SettingsIcon,
		},
	]

	const page = useStore($router)

	useEffect(() => {
		document.title = `${t`Settings`} / Watchdog`
		// @ts-expect-error redirect to account page if no page is specified
		if (!page?.params?.name) {
			redirectPage($router, "settings", { name: "general" })
		}
	}, [])

	return (
		<Card className="pt-5 px-4 api-8 min-h-96 mb-14 sm:pt-6 sm:px-7">
			<CardHeader className="p-0">
				<CardTitle className="mb-1">
					<Trans>Settings</Trans>
				</CardTitle>
				<CardDescription>
					<Trans>Manage display preferences.</Trans>
				</CardDescription>
			</CardHeader>
			<CardContent className="p-0">
				<Separator className="hidden md:block my-5" />
				<div className="flex flex-col gap-3.5 md:flex-row md:gap-5 lg:gap-12">
					<aside className="md:max-w-52 min-w-40">
						<SidebarNav items={sidebarNavItems} />
					</aside>
					<div className="flex-1 min-w-0">
						{/* @ts-ignore */}
						<SettingsContent name={page?.params?.name ?? "general"} />
					</div>
				</div>
			</CardContent>
		</Card>
	)
}

function SettingsContent({ name }: { name: string }) {
	const userSettings = useStore($userSettings)

	switch (name) {
		case "general":
			return <GeneralSettings userSettings={userSettings} />
	}
}

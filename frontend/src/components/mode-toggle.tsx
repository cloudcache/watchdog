import { Trans, useLingui } from "@lingui/react/macro"
import { CheckIcon, MoonStarIcon, SunIcon, SunMoonIcon } from "lucide-react"
import { useTheme } from "@/components/theme-provider"
import { Button } from "@/components/ui/button"
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu"
import { cn } from "@/lib/utils"

const themes = ["light", "dark", "system"] as const
const icons = [SunIcon, MoonStarIcon, SunMoonIcon] as const

export function ModeToggle() {
	const { t } = useLingui()
	const { theme, setTheme } = useTheme()

	const currentIndex = Math.max(0, themes.indexOf(theme))
	const Icon = icons[currentIndex]

	return (
		<DropdownMenu>
			<DropdownMenuTrigger asChild>
				<Button variant="ghost" size="icon" aria-label={t`Theme`}>
					<Icon
						className={cn(
							"animate-in fade-in spin-in-[-30deg] duration-200",
							currentIndex === 2 ? "size-[1.35rem]" : "size-[1.2rem]"
						)}
					/>
				</Button>
			</DropdownMenuTrigger>
			<DropdownMenuContent align="end" className="min-w-36">
				{themes.map((value) => (
					<DropdownMenuItem key={value} onSelect={() => setTheme(value)}>
						{theme === value ? <CheckIcon className="me-2 h-4 w-4" /> : <span className="me-2 h-4 w-4" />}
						{value === "light" ? <Trans>Light</Trans> : value === "dark" ? <Trans>Dark</Trans> : <Trans>System</Trans>}
					</DropdownMenuItem>
				))}
			</DropdownMenuContent>
		</DropdownMenu>
	)
}

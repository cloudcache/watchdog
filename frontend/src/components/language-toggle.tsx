import { t } from "@lingui/core/macro"
import { useLingui } from "@lingui/react"
import { CheckIcon, LanguagesIcon } from "lucide-react"
import { Button } from "@/components/ui/button"
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu"
import { dynamicActivate } from "@/lib/i18n"
import languages from "@/lib/languages"

export function LanguageToggle() {
	const { i18n } = useLingui()
	return (
		<DropdownMenu>
			<DropdownMenuTrigger asChild>
				<Button variant="ghost" size="icon" aria-label={t`Language`}>
					<LanguagesIcon className="h-[1.2rem] w-[1.2rem]" />
				</Button>
			</DropdownMenuTrigger>
			<DropdownMenuContent align="end" className="min-w-40">
				{languages.map(([locale, label, flag]) => (
					<DropdownMenuItem key={locale} onSelect={() => dynamicActivate(locale)}>
						{i18n.locale === locale ? <CheckIcon className="me-2 h-4 w-4" /> : <span className="me-2 h-4 w-4" />}
						<span className="me-2">{flag}</span>
						{label}
					</DropdownMenuItem>
				))}
			</DropdownMenuContent>
		</DropdownMenu>
	)
}

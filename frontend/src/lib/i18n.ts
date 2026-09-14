import type { Messages } from "@lingui/core"
import { i18n } from "@lingui/core"
import { t } from "@lingui/core/macro"
import { detect, fromNavigator, fromStorage } from "@lingui/detect-locale"
import languages from "@/lib/languages"
import { messages as enMessages } from "@/locales/en/en"
import { BatteryState } from "./enums"
import { $direction } from "./stores"

const rtlLanguages = new Set(["ar", "fa", "he"])

// activates locale
function activateLocale(locale: string, messages: Messages = enMessages) {
	i18n.load(locale, messages)
	i18n.activate(locale)
	document.documentElement.lang = locale
	localStorage.setItem("lang", locale)
	$direction.set(rtlLanguages.has(locale) ? "rtl" : "ltr")
}

activateLocale("en")

// dynamically loads translations for the given locale
export async function dynamicActivate(locale: string) {
	if (locale === "en") {
		activateLocale(locale)
	} else {
		try {
			const { messages }: { messages: Messages } = await import(`../locales/${locale}/${locale}.ts`)
			activateLocale(locale, messages)
		} catch (error) {
			console.error(`Error loading ${locale}`, error)
			activateLocale("en")
		}
	}
}

export function getLocale() {
	// let locale = detect(fromUrl("lang"), fromStorage("lang"), fromNavigator(), "en")
	let locale = detect(fromStorage("lang"), fromNavigator(), "en")
	// log if dev
	if (import.meta.env.DEV) {
		console.log("detected locale", locale)
	}
	// only Simplified Chinese + English ship; route every Chinese variant to zh-CN
	if (locale?.startsWith("zh")) {
		return "zh-CN"
	}
	locale = (locale || "en").split("-")[0]
	// use en if locale is not in languages
	if (!languages.some((l) => l[0] === locale)) {
		locale = "en"
	}
	return locale
}

////////////////////////////////////////////////////////

export const batteryStateTranslations = {
	[BatteryState.Unknown]: () => t({ message: "Unknown", comment: "Context: Battery state" }),
	[BatteryState.Empty]: () => t({ message: "Empty", comment: "Context: Battery state" }),
	[BatteryState.Full]: () => t({ message: "Full", comment: "Context: Battery state" }),
	[BatteryState.Charging]: () => t({ message: "Charging", comment: "Context: Battery state" }),
	[BatteryState.Discharging]: () => t({ message: "Discharging", comment: "Context: Battery state" }),
	[BatteryState.Idle]: () => t({ message: "Idle", comment: "Context: Battery state" }),
} as const

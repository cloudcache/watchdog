import { atom, map } from "nanostores"
import type { UserSettings } from "@/types"
import { Unit } from "./enums"
import { developmentAuth } from "./env"

/** Default layout width. Used as fallback when user setting is unset. */
export const defaultLayoutWidth = 1580

/** Store if user is authenticated */
export const $authenticated = atom(developmentAuth)
/** Whether a protected-route session check has completed. Public login/reset
 * pages never trigger this check themselves. */
export const $authChecked = atom(developmentAuth)

/** User settings */
export const $userSettings = map<UserSettings>({
	chartTime: "1h",
	emails: [],
	unitNet: Unit.Bytes,
	unitTemp: Unit.Celsius,
})

/** Fallback copy to clipboard dialog content */
export const $copyContent = atom("")

/** Direction for localization */
export const $direction = atom<"ltr" | "rtl">("ltr")

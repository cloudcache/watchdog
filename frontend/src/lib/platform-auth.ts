import { atom } from "nanostores"

export type PlatformAuthContext = {
	userID: string
	roleIDs: string[]
	isAdmin: boolean
	canManageAddressLibrary: boolean
}

export type PlatformIdentityState = {
	ready: boolean
	current?: PlatformAuthContext
}

export const $platformIdentity = atom<PlatformIdentityState>({
	ready: false,
})

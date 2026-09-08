import { atom } from "nanostores"

export type PlatformTenant = {
	id: string
	name: string
	status: string
}

export type PlatformPermission = {
	actions: string[]
}

export type PlatformAuthContext = {
	tenantID: string
	userID: string
	roleIDs: string[]
	grants: PlatformPermission[]
	isAdmin: boolean
	canManageAddressLibrary: boolean
}

export type PlatformIdentityState = {
	ready: boolean
	tenants: PlatformTenant[]
	current?: PlatformAuthContext
}

export const $platformIdentity = atom<PlatformIdentityState>({
	ready: false,
	tenants: [],
})

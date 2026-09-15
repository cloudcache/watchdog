/** Routes that can render without resolving a browser session. */
export function isPublicSessionRoute(route: string | undefined) {
	return route === "forgot_password"
}

/** The installed application redirects this route before checking a session. */
export function isInstallRedirectRoute(route: string | undefined) {
	return route === "install"
}

export function shouldRestoreSession(input: {
	installed: boolean
	developmentAuth: boolean
	authChecked: boolean
	route: string | undefined
}) {
	return (
		input.installed &&
		!input.developmentAuth &&
		!input.authChecked &&
		!isPublicSessionRoute(input.route) &&
		!isInstallRedirectRoute(input.route)
	)
}

import assert from "node:assert/strict"
import test from "node:test"
import { isInstallRedirectRoute, isPublicSessionRoute, shouldRestoreSession } from "./auth-route-policy.ts"

test("public password recovery never restores a session", () => {
	assert.equal(isPublicSessionRoute("forgot_password"), true)
	assert.equal(
		shouldRestoreSession({ installed: true, developmentAuth: false, authChecked: false, route: "forgot_password" }),
		false
	)
})

test("installed setup route redirects before restoring a session", () => {
	assert.equal(isInstallRedirectRoute("install"), true)
	assert.equal(
		shouldRestoreSession({ installed: true, developmentAuth: false, authChecked: false, route: "install" }),
		false
	)
})

test("protected and unknown routes restore a session exactly at the route boundary", () => {
	for (const route of ["home", "network", "flow_overview", undefined]) {
		assert.equal(
			shouldRestoreSession({ installed: true, developmentAuth: false, authChecked: false, route }),
			true,
			route
		)
	}
})

test("install state, development auth and completed checks suppress restoration", () => {
	assert.equal(
		shouldRestoreSession({ installed: false, developmentAuth: false, authChecked: false, route: "home" }),
		false
	)
	assert.equal(
		shouldRestoreSession({ installed: true, developmentAuth: true, authChecked: false, route: "home" }),
		false
	)
	assert.equal(
		shouldRestoreSession({ installed: true, developmentAuth: false, authChecked: true, route: "home" }),
		false
	)
})

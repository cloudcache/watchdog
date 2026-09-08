import { t } from "@lingui/core/macro"
import { useStore } from "@nanostores/react"
import { useEffect, useMemo } from "react"
import { UserAuthForm } from "@/components/login/auth-form"
import { Logo } from "../logo"
import { ModeToggle } from "../mode-toggle"
import { $router } from "../router"
import { useTheme } from "../theme-provider"
import ForgotPassword from "./forgot-pass-form"

export default function Login() {
	const page = useStore($router)
	const { theme } = useTheme()

	useEffect(() => {
		document.title = [t`Login`, "Watchdog"].join(" / ")
	}, [])

	const forgotPassword = page?.route === "forgot_password"
	const subtitle = useMemo(
		() => (forgotPassword ? t`Enter email address to reset password` : t`Please sign in to your account`),
		[forgotPassword]
	)

	return (
		<div className="min-h-svh grid items-center py-12">
			<div
				className="grid gap-5 w-full px-4 mx-auto"
				// @ts-expect-error custom theme variable
				style={{ maxWidth: "21.5em", "--border": theme === "light" ? "hsl(30, 8%, 70%)" : "hsl(220, 3%, 25%)" }}
			>
				<div className="absolute top-3 right-3">
					<ModeToggle />
				</div>
				<div className="text-center">
					<h1 className="mb-3">
						<Logo className="h-8 mx-auto" />
					</h1>
					<p className="text-sm text-muted-foreground">{subtitle}</p>
				</div>
				{forgotPassword ? <ForgotPassword /> : <UserAuthForm />}
			</div>
		</div>
	)
}

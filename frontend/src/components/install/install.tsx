import { t } from "@lingui/core/macro"
import { Trans } from "@lingui/react/macro"
import { LoaderCircle } from "lucide-react"
import { useState } from "react"
import { Logo } from "@/components/logo"
import { ModeToggle } from "@/components/mode-toggle"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { installWatchdog, type InstallStatus } from "@/lib/api"

export default function Install({ onInstalled }: { onInstalled: (status: InstallStatus) => void }) {
	const [loading, setLoading] = useState(false)
	const [error, setError] = useState("")

	async function submit(event: React.FormEvent<HTMLFormElement>) {
		event.preventDefault()
		setError("")
		const values = Object.fromEntries(new FormData(event.currentTarget))
		const password = String(values.password ?? "")
		if (password !== String(values.confirm_password ?? "")) {
			setError(t`Passwords do not match.`)
			return
		}
		setLoading(true)
		try {
			const status = await installWatchdog({
				username: String(values.username ?? "").trim(),
				password,
				email: String(values.email ?? "").trim(),
				display_name: String(values.display_name ?? "").trim(),
			})
			onInstalled(status)
		} catch (cause) {
			setError((cause as Error).message)
		} finally {
			setLoading(false)
		}
	}

	return (
		<div className="min-h-svh grid items-center py-12">
			<div className="absolute top-3 right-3">
				<ModeToggle />
			</div>
			<div className="grid gap-5 w-full max-w-md px-4 mx-auto">
				<div className="text-center">
					<Logo className="h-8 mx-auto mb-3" />
					<h1 className="text-2xl font-semibold">
						<Trans>Install Watchdog</Trans>
					</h1>
					<p className="mt-2 text-sm text-muted-foreground">
						<Trans>Initialize the databases and create the first administrator.</Trans>
					</p>
				</div>
				<form className="grid gap-4" onSubmit={submit}>
					<div className="grid gap-2">
						<Label htmlFor="install-username">
							<Trans>Administrator username</Trans>
						</Label>
						<Input id="install-username" name="username" defaultValue="admin" required autoComplete="username" />
					</div>
					<div className="grid gap-2">
						<Label htmlFor="install-display-name">
							<Trans>Display name</Trans>
						</Label>
						<Input id="install-display-name" name="display_name" defaultValue="Administrator" />
					</div>
					<div className="grid gap-2">
						<Label htmlFor="install-email">
							<Trans>Email (optional)</Trans>
						</Label>
						<Input id="install-email" name="email" type="email" autoComplete="email" />
					</div>
					<div className="grid gap-2">
						<Label htmlFor="install-password">
							<Trans>Password</Trans>
						</Label>
						<Input
							id="install-password"
							name="password"
							type="password"
							minLength={8}
							maxLength={72}
							required
							autoComplete="new-password"
						/>
					</div>
					<div className="grid gap-2">
						<Label htmlFor="install-confirm-password">
							<Trans>Confirm password</Trans>
						</Label>
						<Input
							id="install-confirm-password"
							name="confirm_password"
							type="password"
							minLength={8}
							maxLength={72}
							required
							autoComplete="new-password"
						/>
					</div>
					{error && (
						<p role="alert" className="text-sm text-destructive">
							{error}
						</p>
					)}
					<Button type="submit" disabled={loading}>
						{loading && <LoaderCircle className="me-2 h-4 w-4 animate-spin" />}
						<Trans>Initialize Watchdog</Trans>
					</Button>
				</form>
			</div>
		</div>
	)
}

import { t } from "@lingui/core/macro"
import { Trans } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { LoaderCircle, LockIcon, LogInIcon, UserIcon } from "lucide-react"
import { useCallback, useState } from "react"
import * as v from "valibot"
import { buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { login } from "@/lib/api"
import { cn } from "@/lib/utils"
import { $router, Link } from "../router"
import { toast } from "../ui/use-toast"

const LoginSchema = v.object({
	username: v.pipe(v.string(), v.trim(), v.minLength(1, t`Username is required.`)),
	password: v.pipe(
		v.string(),
		v.minLength(8, t`Password must be at least 8 characters.`),
		v.maxBytes(72, t`Password must be less than 72 bytes.`)
	),
})

export const showLoginFailedToast = (description = t`Please check your credentials and try again`) => {
	toast({ title: t`Login attempt failed`, description, variant: "destructive" })
}

export function UserAuthForm({ className, ...props }: { className?: string }) {
	const [isLoading, setIsLoading] = useState(false)
	const [errors, setErrors] = useState<Record<string, string | undefined>>({})

	const handleSubmit = useCallback(async (event: React.FormEvent<HTMLFormElement>) => {
		event.preventDefault()
		setIsLoading(true)
		try {
			const result = v.safeParse(LoginSchema, Object.fromEntries(new FormData(event.currentTarget)))
			if (!result.success) {
				const next: Record<string, string> = {}
				for (const issue of result.issues) {
					const key = issue.path?.[0]?.key
					if (typeof key === "string") next[key] = issue.message
				}
				setErrors(next)
				return
			}
			await login(result.output.username, result.output.password)
		} catch (error) {
			showLoginFailedToast((error as Error).message)
		} finally {
			setIsLoading(false)
		}
	}, [])

	return (
		<div className={cn("grid gap-6", className)} {...props}>
			<form onSubmit={handleSubmit} onChange={() => setErrors({})}>
				<div className="grid gap-2.5">
					<div className="grid gap-1 relative">
						<UserIcon className="absolute left-3 top-3 h-4 w-4 text-muted-foreground" />
						<Label className="sr-only" htmlFor="username">
							<Trans>Username</Trans>
						</Label>
						<Input
							id="username"
							name="username"
							required
							placeholder={t`Username`}
							autoCapitalize="none"
							autoComplete="username"
							disabled={isLoading}
							className={cn("ps-9", errors.username && "border-red-500")}
						/>
						{errors.username && <p className="px-1 text-xs text-red-600">{errors.username}</p>}
					</div>
					<div className="grid gap-1 relative">
						<LockIcon className="absolute left-3 top-3 h-4 w-4 text-muted-foreground" />
						<Label className="sr-only" htmlFor="password">
							<Trans>Password</Trans>
						</Label>
						<Input
							id="password"
							name="password"
							placeholder={t`Password`}
							required
							type="password"
							autoComplete="current-password"
							disabled={isLoading}
							className={cn("ps-9", errors.password && "border-red-500")}
						/>
						{errors.password && <p className="px-1 text-xs text-red-600">{errors.password}</p>}
					</div>
					<button className={cn(buttonVariants())} disabled={isLoading}>
						{isLoading ? (
							<LoaderCircle className="me-2 h-4 w-4 animate-spin" />
						) : (
							<LogInIcon className="me-2 h-4 w-4" />
						)}
						<Trans>Sign in</Trans>
					</button>
				</div>
			</form>
			<Link
				href={getPagePath($router, "forgot_password")}
				className="text-sm mx-auto hover:text-brand underline underline-offset-4 opacity-70 hover:opacity-100 transition-opacity"
			>
				<Trans>Forgot password?</Trans>
			</Link>
		</div>
	)
}

import { Trans, useLingui } from "@lingui/react/macro"
import { RefreshCwIcon, ShieldCheckIcon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { Button } from "@/components/ui/button"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { api } from "@/lib/api"

type Permission = {
	ability: string
	subject: string
}

type PermissionsResponse = {
	items?: Permission[]
}

export default memo(() => {
	const { t } = useLingui()
	const [permissions, setPermissions] = useState<Permission[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const data = await api.send<PermissionsResponse>("/api/v1/permissions", {})
			setPermissions(data.items ?? [])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load permissions`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		document.title = `${t`Permissions`} / Watchdog`
		refresh()
	}, [refresh, t])

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<ShieldCheckIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Permissions</Trans>
					</h1>
				</div>
				<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Refresh</Trans>
				</Button>
			</div>

			<div className="rounded-md border border-border bg-card">
				<Table>
					<TableHeader>
						<TableRow>
							<TableHead>
								<Trans>Domain</Trans>
							</TableHead>
							<TableHead>
								<Trans>Ability</Trans>
							</TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{loading ? (
							<TableRow>
								<TableCell colSpan={2} className="text-muted-foreground">
									<Trans>Loading...</Trans>
								</TableCell>
							</TableRow>
						) : error ? (
							<TableRow>
								<TableCell colSpan={2} className="text-destructive">
									{error}
								</TableCell>
							</TableRow>
						) : permissions.length === 0 ? (
							<TableRow>
								<TableCell colSpan={2} className="text-muted-foreground">
									<Trans>No permissions found.</Trans>
								</TableCell>
							</TableRow>
						) : (
							permissions.map((permission) => (
								<TableRow key={permission.ability}>
									<TableCell className="font-mono text-xs">{permission.subject}</TableCell>
									<TableCell className="font-mono text-xs">{permission.ability}</TableCell>
								</TableRow>
							))
						)}
					</TableBody>
				</Table>
			</div>
		</div>
	)
})

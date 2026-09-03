import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { PlusIcon, ReceiptTextIcon, RefreshCwIcon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { $router, navigate } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { pb } from "@/lib/api"

type BillingAccount = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	Status?: string
	status?: string
	BillingDay?: number
	billing_day?: number
	Aggregation?: string
	aggregation?: string
	ValueMode?: string
	value_mode?: string
	QuotaBytes?: number
	quota_bytes?: number
}

type BillingAccountsResponse = {
	items?: BillingAccount[]
}

export default memo(() => {
	const { t } = useLingui()
	const [accounts, setAccounts] = useState<BillingAccount[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const data = await pb.send<BillingAccountsResponse>("/api/v1/billing/accounts", {})
			setAccounts(data.items ?? [])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load billing accounts`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		document.title = `${t`Billing`} / Watchdog`
		refresh()
	}, [refresh, t])

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<ReceiptTextIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Billing</Trans>
					</h1>
				</div>
				<div className="flex items-center gap-2">
					<Button variant="outline" size="sm" onClick={() => navigate(getPagePath($router, "billing_new"))}>
						<PlusIcon className="me-2 h-4 w-4" />
						<Trans>Create</Trans>
					</Button>
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
			</div>

			<div className="rounded-md border border-border bg-card">
				<Table>
					<TableHeader>
						<TableRow>
							<TableHead>
								<Trans>Name</Trans>
							</TableHead>
							<TableHead>
								<Trans>Status</Trans>
							</TableHead>
							<TableHead>
								<Trans>Billing Day</Trans>
							</TableHead>
							<TableHead>
								<Trans>Aggregation</Trans>
							</TableHead>
							<TableHead>
								<Trans>Value</Trans>
							</TableHead>
							<TableHead>
								<Trans>Quota</Trans>
							</TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{loading ? (
							<TableRow>
								<TableCell colSpan={6} className="text-muted-foreground">
									<Trans>Loading...</Trans>
								</TableCell>
							</TableRow>
						) : error ? (
							<TableRow>
								<TableCell colSpan={6} className="text-destructive">
									{error}
								</TableCell>
							</TableRow>
						) : accounts.length === 0 ? (
							<TableRow>
								<TableCell colSpan={6} className="text-muted-foreground">
									<Trans>No billing accounts found.</Trans>
								</TableCell>
							</TableRow>
						) : (
							accounts.map((account) => (
								<TableRow
									key={account.ID ?? account.id}
									className="cursor-pointer"
									onClick={() =>
										navigate(getPagePath($router, "billing_detail", { id: account.ID ?? account.id ?? "" }))
									}
								>
									<TableCell className="font-medium">{account.Name ?? account.name ?? "—"}</TableCell>
									<TableCell>
										<AccountStatus value={account.Status ?? account.status} />
									</TableCell>
									<TableCell>{account.BillingDay ?? account.billing_day ?? "—"}</TableCell>
									<TableCell>{account.Aggregation ?? account.aggregation ?? "—"}</TableCell>
									<TableCell>{account.ValueMode ?? account.value_mode ?? "corrected"}</TableCell>
									<TableCell>{formatBytes(account.QuotaBytes ?? account.quota_bytes)}</TableCell>
								</TableRow>
							))
						)}
					</TableBody>
				</Table>
			</div>
		</div>
	)
})

function AccountStatus({ value }: { value?: string }) {
	const normalized = value?.toLowerCase() ?? ""
	const variant: "success" | "danger" | "outline" =
		normalized === "active" ? "success" : normalized === "disabled" ? "danger" : "outline"
	return <Badge variant={variant}>{value || "—"}</Badge>
}

function formatBytes(value?: number) {
	if (!value || value <= 0) {
		return "—"
	}
	if (value >= 1_000_000_000_000) {
		return `${(value / 1_000_000_000_000).toFixed(2)} TB`
	}
	if (value >= 1_000_000_000) {
		return `${(value / 1_000_000_000).toFixed(2)} GB`
	}
	if (value >= 1_000_000) {
		return `${(value / 1_000_000).toFixed(2)} MB`
	}
	return `${value} B`
}

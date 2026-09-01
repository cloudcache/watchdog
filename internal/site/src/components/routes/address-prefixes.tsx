import { Trans, useLingui } from "@lingui/react/macro"
import { GlobeIcon, PlusIcon, RefreshCwIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { pb } from "@/lib/api"
import { cn } from "@/lib/utils"

type AddressPrefix = {
	id: string
	cidr: string
	labels: Record<string, string>
	source: string
}

type AddressPrefixList = { items: AddressPrefix[] }

export default memo(function AddressPrefixes() {
	const { t } = useLingui()
	const [prefixes, setPrefixes] = useState<AddressPrefix[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const [showForm, setShowForm] = useState(false)
	const [form, setForm] = useState({ cidr: "", labels: "" })

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const data = await pb.send<AddressPrefixList>("/api/v1/address-prefixes", {})
			setPrefixes(data.items ?? [])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load`)
		}
		setLoading(false)
	}, [t])

	useEffect(() => { refresh() }, [refresh])

	const add = async () => {
		if (!form.cidr) return
		const labels: Record<string, string> = {}
		if (form.labels) {
			for (const pair of form.labels.split(",")) {
				const [k, v] = pair.trim().split("=")
				if (k && v) labels[k.trim()] = v.trim()
			}
		}
		try {
			await pb.send("/api/v1/address-prefixes", {
				method: "POST",
				body: { cidr: form.cidr, labels },
			})
			setForm({ cidr: "", labels: "" })
			setShowForm(false)
			await refresh()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to create`)
		}
	}

	const remove = async (id: string) => {
		if (!confirm(t`Delete this prefix?`)) return
		try {
			await pb.send(`/api/v1/address-prefixes/${id}`, { method: "DELETE" })
			await refresh()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to delete`)
		}
	}

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<GlobeIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal"><Trans>Address Prefixes</Trans></h1>
				</div>
				<div className="flex gap-2">
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
					<Button size="sm" onClick={() => setShowForm(!showForm)}>
						<PlusIcon className="me-2 h-4 w-4" />
						<Trans>Add Prefix</Trans>
					</Button>
				</div>
			</div>

			{showForm && (
				<div className="grid gap-3 rounded-md border border-border bg-card p-4">
					<div className="grid gap-2">
						<Label><Trans>CIDR</Trans></Label>
						<Input value={form.cidr} onChange={(e) => setForm({ ...form, cidr: e.target.value })} placeholder="10.0.0.0/16" />
					</div>
					<div className="grid gap-2">
						<Label><Trans>Labels</Trans> (key=value, comma separated)</Label>
						<Input value={form.labels} onChange={(e) => setForm({ ...form, labels: e.target.value })} placeholder="region=杭州,type=客户,provider=电信" />
					</div>
					<div className="flex gap-2">
						<Button size="sm" onClick={add}><Trans>Add</Trans></Button>
						<Button variant="ghost" size="sm" onClick={() => setShowForm(false)}><Trans>Cancel</Trans></Button>
					</div>
				</div>
			)}

			{error && <div className="text-sm text-destructive">{error}</div>}

			<div className="rounded-md border border-border bg-card overflow-hidden">
				<Table>
					<TableHeader>
						<TableRow>
							<TableHead><Trans>CIDR</Trans></TableHead>
							<TableHead><Trans>Labels</Trans></TableHead>
							<TableHead><Trans>Source</Trans></TableHead>
							<TableHead className="text-right"><Trans>Actions</Trans></TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{loading ? (
							<TableRow><TableCell colSpan={4} className="text-muted-foreground"><Trans>Loading...</Trans></TableCell></TableRow>
						) : prefixes.length === 0 ? (
							<TableRow><TableCell colSpan={4} className="text-muted-foreground"><Trans>No prefixes found.</Trans></TableCell></TableRow>
						) : (
							prefixes.map((p) => (
								<TableRow key={p.id}>
									<TableCell className="font-mono text-sm">{p.cidr}</TableCell>
									<TableCell className="text-sm">
										{Object.entries(p.labels).map(([k, v]) => (
											<span key={k} className="me-2 rounded bg-muted px-1.5 py-0.5 text-xs">{k}={v}</span>
										))}
									</TableCell>
									<TableCell className="text-xs text-muted-foreground">{p.source}</TableCell>
									<TableCell className="text-right">
										<Button variant="ghost" size="sm" onClick={() => remove(p.id)}>
											<Trash2Icon className="h-3.5 w-3.5" />
										</Button>
									</TableCell>
								</TableRow>
							))
						)}
					</TableBody>
				</Table>
			</div>
		</div>
	)
})

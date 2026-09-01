import { Trans, useLingui } from "@lingui/react/macro"
import { LayersIcon, PlusIcon, RefreshCwIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { pb } from "@/lib/api"

type AddressSet = {
	id: string
	name: string
	description: string
	selector: Record<string, any>
	match_direction: string
	enabled: boolean
}

type AddressSetList = { items: AddressSet[] }

export default memo(function AddressSets() {
	const { t } = useLingui()
	const [sets, setSets] = useState<AddressSet[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const [showForm, setShowForm] = useState(false)
	const [form, setForm] = useState({ name: "", description: "", selector: "" })

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const data = await pb.send<AddressSetList>("/api/v1/address-sets", {})
			setSets(data.items ?? [])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load`)
		}
		setLoading(false)
	}, [t])

	useEffect(() => { refresh() }, [refresh])

	const add = async () => {
		if (!form.name || !form.selector) return
		let selector: Record<string, any>
		try {
			selector = JSON.parse(form.selector)
		} catch {
			setError(t`Invalid JSON selector`)
			return
		}
		try {
			await pb.send("/api/v1/address-sets", {
				method: "POST",
				body: { name: form.name, description: form.description, selector },
			})
			setForm({ name: "", description: "", selector: "" })
			setShowForm(false)
			await refresh()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to create`)
		}
	}

	const remove = async (id: string) => {
		if (!confirm(t`Delete this address set?`)) return
		try {
			await pb.send(`/api/v1/address-sets/${id}`, { method: "DELETE" })
			await refresh()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to delete`)
		}
	}

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<LayersIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal"><Trans>Address Sets</Trans></h1>
				</div>
				<div className="flex gap-2">
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
					<Button size="sm" onClick={() => setShowForm(!showForm)}>
						<PlusIcon className="me-2 h-4 w-4" />
						<Trans>Add Set</Trans>
					</Button>
				</div>
			</div>

			{showForm && (
				<div className="grid gap-3 rounded-md border border-border bg-card p-4">
					<div className="grid gap-2">
						<Label><Trans>Name</Trans></Label>
						<Input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} placeholder="电信客户" />
					</div>
					<div className="grid gap-2">
						<Label><Trans>Description</Trans></Label>
						<Input value={form.description} onChange={(e) => setForm({ ...form, description: e.target.value })} placeholder="电信客户网段" />
					</div>
					<div className="grid gap-2">
						<Label><Trans>Selector</Trans> (JSON)</Label>
						<Input value={form.selector} onChange={(e) => setForm({ ...form, selector: e.target.value })} placeholder='{"labels":{"provider":"电信","type":"客户"}}' className="font-mono text-sm" />
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
							<TableHead><Trans>Name</Trans></TableHead>
							<TableHead><Trans>Selector</Trans></TableHead>
							<TableHead><Trans>Direction</Trans></TableHead>
							<TableHead className="text-right"><Trans>Actions</Trans></TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{loading ? (
							<TableRow><TableCell colSpan={4} className="text-muted-foreground"><Trans>Loading...</Trans></TableCell></TableRow>
						) : sets.length === 0 ? (
							<TableRow><TableCell colSpan={4} className="text-muted-foreground"><Trans>No address sets found.</Trans></TableCell></TableRow>
						) : (
							sets.map((s) => (
								<TableRow key={s.id}>
									<TableCell className="font-medium">{s.name}</TableCell>
									<TableCell className="font-mono text-xs text-muted-foreground">{JSON.stringify(s.selector)}</TableCell>
									<TableCell className="text-xs">{s.match_direction}</TableCell>
									<TableCell className="text-right">
										<Button variant="ghost" size="sm" onClick={() => remove(s.id)}>
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

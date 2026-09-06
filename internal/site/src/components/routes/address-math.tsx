import { Trans, useLingui } from "@lingui/react/macro"
import { CalculatorIcon, CopyIcon, RefreshCwIcon } from "lucide-react"
import { memo, useMemo, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { parseAddressEntries } from "@/lib/address-set-form"
import { pb } from "@/lib/api"

type AddressOperation = "normalize" | "union" | "intersection" | "difference" | "complement" | "cover"

type AddressOverlap = {
	left_operand: string
	left_index: number
	right_operand: string
	right_index: number
}

type AddressOperationPreview = {
	operation: AddressOperation
	canonical_left: string[]
	canonical_right?: string[]
	canonical_universe?: string[]
	result: string[]
	input_expressions: number
	result_prefixes: number
	result_addresses_v4: string
	result_addresses_v6: string
	added_addresses_v4: string
	added_addresses_v6: string
	overlap_count: number
	overlaps?: AddressOverlap[]
	overlap_details_cut_off: boolean
	lossless: boolean
	requires_confirmation: boolean
}

const operations: AddressOperation[] = ["normalize", "union", "intersection", "difference", "complement", "cover"]

export default memo(function AddressMath() {
	const { t } = useLingui()
	const [operation, setOperation] = useState<AddressOperation>("normalize")
	const [left, setLeft] = useState("")
	const [right, setRight] = useState("")
	const [universe, setUniverse] = useState("")
	const [targetV4, setTargetV4] = useState("24")
	const [targetV6, setTargetV6] = useState("48")
	const [preview, setPreview] = useState<AddressOperationPreview | null>(null)
	const [working, setWorking] = useState(false)
	const [error, setError] = useState("")
	const [notice, setNotice] = useState("")

	const run = async () => {
		setWorking(true)
		setError("")
		setNotice("")
		try {
			const body: Record<string, unknown> = { operation, left: parseAddressEntries(left) }
			if (["union", "intersection", "difference"].includes(operation)) body.right = parseAddressEntries(right)
			if (operation === "complement") body.universe = parseAddressEntries(universe)
			if (operation === "cover") {
				if (targetV4.trim()) body.target_prefix_v4 = parsePrefixLength(targetV4, 32, "IPv4")
				if (targetV6.trim()) body.target_prefix_v6 = parsePrefixLength(targetV6, 128, "IPv6")
			}
			setPreview(
				await pb.send<AddressOperationPreview>("/api/v1/address-sets/actions/preview", { method: "POST", body })
			)
		} catch (err) {
			setPreview(null)
			setError(err instanceof Error ? err.message : t`Preview failed`)
		} finally {
			setWorking(false)
		}
	}

	const reset = () => {
		setLeft("")
		setRight("")
		setUniverse("")
		setPreview(null)
		setError("")
		setNotice("")
	}

	const copyResult = async () => {
		if (!preview) return
		try {
			await navigator.clipboard.writeText(preview.result.join("\n"))
			setNotice(t`Canonical result copied`)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Copy failed`)
		}
	}

	const overlaps = useMemo(
		() =>
			(preview?.overlaps ?? []).map((item, index) => ({
				index: index + 1,
				left: `${item.left_operand}[${item.left_index}]`,
				right: `${item.right_operand}[${item.right_index}]`,
			})),
		[preview]
	)
	const overlapColumns = useMemo(
		() => [
			{ field: "index", title: "#", width: 70, style: denseCellStyle() },
			{ field: "left", title: t`Left range`, width: 320, style: denseCellStyle() },
			{ field: "right", title: t`Right range`, width: 320, style: denseCellStyle() },
		],
		[t]
	)

	return (
		<div className="grid gap-4">
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<CalculatorIcon className="h-5 w-5 text-muted-foreground" />
					<h2 className="text-lg font-semibold">
						<Trans>Address Set Tools</Trans>
					</h2>
				</div>
				<Button variant="outline" size="sm" onClick={reset}>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Reset</Trans>
				</Button>
			</div>

			<div className="grid gap-4 rounded-md border border-border bg-card p-4">
				<div className="grid gap-2 sm:max-w-xs">
					<Label>
						<Trans>Operation</Trans>
					</Label>
					<Select
						value={operation}
						onValueChange={(value: AddressOperation) => {
							setOperation(value)
							setPreview(null)
						}}
					>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							{operations.map((value) => (
								<SelectItem key={value} value={value}>
									{operationLabel(value)}
								</SelectItem>
							))}
						</SelectContent>
					</Select>
				</div>
				<div className="grid gap-4 lg:grid-cols-2">
					<AddressInput label={t`Left / input set`} value={left} onChange={setLeft} />
					{["union", "intersection", "difference"].includes(operation) ? (
						<AddressInput label={t`Right set`} value={right} onChange={setRight} />
					) : null}
					{operation === "complement" ? (
						<AddressInput label={t`Explicit finite universe`} value={universe} onChange={setUniverse} />
					) : null}
				</div>
				{operation === "cover" ? (
					<div className="grid gap-3 sm:grid-cols-2 sm:max-w-xl">
						<div className="grid gap-2">
							<Label>
								<Trans>IPv4 target prefix</Trans>
							</Label>
							<Input
								type="number"
								min={0}
								max={32}
								value={targetV4}
								onChange={(event) => setTargetV4(event.target.value)}
							/>
						</div>
						<div className="grid gap-2">
							<Label>
								<Trans>IPv6 target prefix</Trans>
							</Label>
							<Input
								type="number"
								min={0}
								max={128}
								value={targetV6}
								onChange={(event) => setTargetV6(event.target.value)}
							/>
						</div>
					</div>
				) : null}
				<div className="text-xs text-muted-foreground">
					<Trans>
						Accepts CIDR, a single IP, or start-end. Merge and set algebra preserve the exact set. Cover may add
						addresses and always requires review.
					</Trans>
				</div>
				<div>
					<Button onClick={run} disabled={working}>
						<CalculatorIcon className="me-2 h-4 w-4" />
						{working ? <Trans>Calculating...</Trans> : <Trans>Validate & Preview</Trans>}
					</Button>
				</div>
			</div>

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			{notice ? <div className="rounded-md border border-green-500/30 p-3 text-sm text-green-700">{notice}</div> : null}

			{preview ? (
				<div className="grid gap-4">
					<div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-6">
						<Stat label={t`Inputs`} value={String(preview.input_expressions)} />
						<Stat label={t`Result prefixes`} value={preview.result_prefixes.toLocaleString()} />
						<Stat label={t`IPv4 addresses`} value={preview.result_addresses_v4} />
						<Stat label={t`IPv6 addresses`} value={preview.result_addresses_v6} />
						<Stat label={t`Overlaps`} value={preview.overlap_count.toLocaleString()} />
						<Stat
							label={t`Semantics`}
							value={preview.requires_confirmation ? t`Expansion — confirm` : t`Lossless`}
							warning={preview.requires_confirmation}
						/>
					</div>
					{preview.requires_confirmation ? (
						<div className="rounded-md border border-amber-500/40 bg-amber-500/5 p-3 text-sm text-amber-800">
							<Trans>
								This cover adds {preview.added_addresses_v4} IPv4 and {preview.added_addresses_v6} IPv6 addresses. The
								source set has not been changed.
							</Trans>
						</div>
					) : null}
					<div className="grid gap-2">
						<div className="flex items-center justify-between">
							<Label>
								<Trans>Canonical result</Trans>
							</Label>
							<div className="flex gap-2">
								<Button
									variant="outline"
									size="sm"
									onClick={() => {
										setLeft(preview.result.join("\n"))
										setPreview(null)
									}}
								>
									<Trans>Use as next input</Trans>
								</Button>
								<Button variant="outline" size="sm" onClick={copyResult}>
									<CopyIcon className="me-2 h-4 w-4" />
									<Trans>Copy</Trans>
								</Button>
							</div>
						</div>
						<Textarea
							readOnly
							rows={Math.min(18, Math.max(5, preview.result.length))}
							value={preview.result.join("\n")}
							className="font-mono text-xs"
						/>
					</div>
					{preview.overlap_count > 0 ? (
						<div className="grid gap-2">
							<h3 className="font-medium">
								<Trans>Overlap details</Trans>
							</h3>
							{preview.overlap_details_cut_off ? (
								<div className="text-xs text-muted-foreground">
									<Trans>Only the bounded first results are shown.</Trans>
								</div>
							) : null}
							<PagedVTable
								records={overlaps}
								columns={overlapColumns}
								emptyText={t`No overlaps found.`}
								searchPlaceholder={t`Search overlaps...`}
								height={320}
							/>
						</div>
					) : null}
				</div>
			) : null}
		</div>
	)
})

function AddressInput({ label, value, onChange }: { label: string; value: string; onChange: (value: string) => void }) {
	return (
		<div className="grid gap-2">
			<Label>{label}</Label>
			<Textarea
				value={value}
				onChange={(event) => onChange(event.target.value)}
				rows={8}
				className="font-mono text-xs"
				placeholder={"10.0.0.0/8\n192.0.2.1-192.0.2.20\n2001:db8::/32"}
			/>
		</div>
	)
}

function Stat({ label, value, warning = false }: { label: string; value: string; warning?: boolean }) {
	return (
		<div className={`min-w-0 rounded border p-2 ${warning ? "border-amber-500/40 bg-amber-500/5" : ""}`}>
			<div className="text-xs text-muted-foreground">{label}</div>
			<div className="truncate font-medium" title={value}>
				{value}
			</div>
		</div>
	)
}

function parsePrefixLength(value: string, maximum: number, family: string) {
	const result = Number(value)
	if (!Number.isInteger(result) || result < 0 || result > maximum)
		throw new Error(`${family} target prefix must be between 0 and ${maximum}`)
	return result
}

function operationLabel(operation: AddressOperation) {
	return {
		normalize: "Normalize / exact merge",
		union: "Union",
		intersection: "Intersection",
		difference: "Difference (left - right)",
		complement: "Finite complement (universe - left)",
		cover: "Cover / prefix expansion",
	}[operation]
}

function denseCellStyle() {
	return { padding: [8, 10, 8, 10] as [number, number, number, number], fontSize: 13 }
}

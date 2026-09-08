import { Trans, useLingui } from "@lingui/react/macro"
import { CheckIcon, ChevronsUpDownIcon, LoaderCircleIcon, SearchIcon, XIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Button } from "@/components/ui/button"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import {
	addressReferenceSummary,
	type AddressReferenceOption,
	mergeAddressReferenceOptions,
	toggleAddressReference,
} from "@/lib/address-reference"
import { pb } from "@/lib/api"
import { cn } from "@/lib/utils"

export type AddressReferenceKind = "geography" | "operator" | "line" | "address-set"

type ReferenceItem = {
	id: string
	flow_isp_id?: number
	name: string
	code?: string
	kind?: string
	category?: string
	description?: string
}

type ReferenceList = { items?: ReferenceItem[]; next_cursor?: string }

const referencePageSize = 50
const selectedHydrationLimit = 50
const noInitialOptions: AddressReferenceOption[] = []
const noExcludedIDs: string[] = []
const referenceEndpoints: Record<AddressReferenceKind, string> = {
	geography: "/api/v1/geo/dictionary",
	operator: "/api/v1/network/operators",
	line: "/api/v1/geo/lines",
	"address-set": "/api/v1/address-sets",
}

export const AddressReferencePicker = memo(function AddressReferencePicker({
	kind,
	value,
	onChange,
	multiple = false,
	placeholder,
	initialOptions = noInitialOptions,
	excludeIDs = noExcludedIDs,
	disabled = false,
	autoOpen = false,
	onClose,
}: {
	kind: AddressReferenceKind
	value: string[]
	onChange: (value: string[]) => void
	multiple?: boolean
	placeholder: string
	initialOptions?: AddressReferenceOption[]
	excludeIDs?: string[]
	disabled?: boolean
	autoOpen?: boolean
	onClose?: () => void
}) {
	const { t } = useLingui()
	const [open, setOpen] = useState(Boolean(autoOpen))
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [options, setOptions] = useState<AddressReferenceOption[]>(initialOptions)
	const [pageItems, setPageItems] = useState<AddressReferenceOption[]>([])
	const [nextCursor, setNextCursor] = useState("")
	const [loading, setLoading] = useState(false)
	const [loadingMore, setLoadingMore] = useState(false)
	const [error, setError] = useState("")
	const requestSequence = useRef(0)
	const endpoint = referenceEndpoints[kind]
	const excludedKey = excludeIDs.join("\u0000")
	const excluded = useMemo(() => new Set(excludedKey ? excludedKey.split("\u0000") : []), [excludedKey])
	const optionsByID = useMemo(() => new Map(options.map((option) => [option.id, option])), [options])

	useEffect(() => {
		setOptions((current) => mergeAddressReferenceOptions(current, initialOptions))
	}, [initialOptions])

	useEffect(() => {
		const timer = window.setTimeout(() => setDebouncedSearch(search.trim()), 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const fetchPage = useCallback(
		async (cursor = "", append = false) => {
			const sequence = ++requestSequence.current
			append ? setLoadingMore(true) : setLoading(true)
			setError("")
			try {
				const data = await pb.send<ReferenceList>(endpoint, {
					query: {
						q: debouncedSearch || undefined,
						limit: referencePageSize,
						cursor: cursor || undefined,
					},
				})
				if (sequence !== requestSequence.current) return
				const incoming = (data.items ?? [])
					.filter((item) => !excluded.has(item.id))
					.map((item) => toReferenceOption(kind, item))
				setPageItems((current) => (append ? mergeAddressReferenceOptions(current, incoming) : incoming))
				setOptions((current) => mergeAddressReferenceOptions(current, incoming))
				setNextCursor(data.next_cursor ?? "")
			} catch (err) {
				if (sequence !== requestSequence.current) return
				setError(err instanceof Error ? err.message : t`Failed to load references`)
			} finally {
				if (sequence === requestSequence.current) {
					append ? setLoadingMore(false) : setLoading(false)
				}
			}
		},
		[debouncedSearch, endpoint, excluded, kind, t]
	)

	useEffect(() => {
		if (!open) return
		fetchPage()
	}, [fetchPage, open])

	useEffect(() => {
		if ((!open && multiple) || !value.length) return
		const missing = value.filter((id) => !optionsByID.has(id) && !excluded.has(id)).slice(0, selectedHydrationLimit)
		if (!missing.length) return
		let cancelled = false
		const load = async () => {
			const resolved: AddressReferenceOption[] = []
			for (let offset = 0; offset < missing.length && !cancelled; offset += 8) {
				const chunk = await Promise.all(
					missing.slice(offset, offset + 8).map(async (id) => {
						try {
							const item = await pb.send<ReferenceItem>(`${endpoint}/${encodeURIComponent(id)}`)
							return toReferenceOption(kind, item)
						} catch {
							return null
						}
					})
				)
				resolved.push(...chunk.filter((item): item is AddressReferenceOption => item !== null))
			}
			if (!cancelled && resolved.length) {
				setOptions((current) => mergeAddressReferenceOptions(current, resolved))
			}
		}
		load()
		return () => {
			cancelled = true
		}
	}, [endpoint, excluded, kind, multiple, open, optionsByID, value])

	const choose = (id: string) => {
		const selected = value.includes(id)
		onChange(toggleAddressReference(value, id, !selected, multiple))
		if (!multiple) setOpen(false)
	}
	const selectedOptions = value.map((id) => optionsByID.get(id)).filter(Boolean) as AddressReferenceOption[]
	const summary = addressReferenceSummary(value, options, placeholder, t`selected`)

	return (
		<>
			{autoOpen ? null : (
				<Button
					type="button"
					variant="outline"
					className="h-10 w-full min-w-0 justify-between px-3 font-normal"
					disabled={disabled}
					onClick={() => setOpen(true)}
				>
					<span className={cn("truncate", !value.length && "text-muted-foreground")}>{summary}</span>
					<ChevronsUpDownIcon className="ms-2 h-4 w-4 shrink-0 opacity-50" />
				</Button>
			)}
			<Dialog
				open={open}
				onOpenChange={(next) => {
					setOpen(next)
					if (!next) onClose?.()
				}}
			>
				<DialogContent className="max-h-[min(80dvh,42rem)] w-[calc(100vw-2rem)] max-w-xl overflow-x-hidden overflow-y-auto p-0">
					<DialogHeader className="border-b px-5 py-4 pe-12">
						<DialogTitle>{placeholder}</DialogTitle>
						<DialogDescription>
							<Trans>Searches the server and stores the stable reference ID.</Trans>
						</DialogDescription>
					</DialogHeader>
					<div className="grid min-h-0 gap-3 px-5 pb-5">
						<div className="relative">
							<SearchIcon className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
							<Input
								autoFocus
								value={search}
								onChange={(event) => setSearch(event.target.value)}
								placeholder={t`Search by name or code...`}
								className="pl-9"
							/>
						</div>
						{value.length ? (
							<div className="flex max-h-24 flex-wrap gap-1.5 overflow-y-auto rounded-md border p-2">
								{selectedOptions.map((option) => (
									<Button
										key={option.id}
										type="button"
										variant="secondary"
										size="sm"
										className="h-7 max-w-full gap-1 px-2"
										onClick={() => onChange(toggleAddressReference(value, option.id, false, multiple))}
									>
										<span className="truncate">{option.label}</span>
										<XIcon className="h-3 w-3 shrink-0" />
									</Button>
								))}
								{selectedOptions.length < value.length ? (
									<span className="self-center px-1 text-xs text-muted-foreground">
										{value.length - selectedOptions.length} <Trans>more selected</Trans>
									</span>
								) : null}
							</div>
						) : null}
						<div className="min-h-48 overflow-y-auto rounded-md border" role="listbox" aria-multiselectable={multiple}>
							{loading ? (
								<div className="flex h-48 items-center justify-center text-muted-foreground">
									<LoaderCircleIcon className="me-2 h-4 w-4 animate-spin" />
									<Trans>Loading...</Trans>
								</div>
							) : pageItems.length ? (
								<div className="grid gap-1 p-1">
									{pageItems.map((option) => {
										const selected = value.includes(option.id)
										return (
											<button
												type="button"
												role="option"
												aria-selected={selected}
												key={option.id}
												className="flex min-w-0 items-center gap-3 rounded px-3 py-2 text-start hover:bg-accent focus-visible:bg-accent focus-visible:outline-none"
												onClick={() => choose(option.id)}
											>
												<CheckIcon className={cn("h-4 w-4 shrink-0", selected ? "opacity-100" : "opacity-0")} />
												<span className="min-w-0 flex-1">
													<span className="block truncate text-sm">{option.label}</span>
													{option.description ? (
														<span className="block truncate text-xs text-muted-foreground">{option.description}</span>
													) : null}
												</span>
											</button>
										)
									})}
								</div>
							) : (
								<div className="flex h-48 items-center justify-center text-sm text-muted-foreground">
									<Trans>No references found.</Trans>
								</div>
							)}
						</div>
						{error ? <div className="text-sm text-destructive">{error}</div> : null}
						<div className="flex flex-wrap justify-between gap-2">
							<Button type="button" variant="ghost" onClick={() => onChange([])} disabled={!value.length}>
								<Trans>Clear selection</Trans>
							</Button>
							<div className="flex gap-2">
								{nextCursor ? (
									<Button
										type="button"
										variant="outline"
										onClick={() => fetchPage(nextCursor, true)}
										disabled={loadingMore}
									>
										{loadingMore ? <Trans>Loading...</Trans> : <Trans>Load more</Trans>}
									</Button>
								) : null}
								{multiple ? (
									<Button type="button" onClick={() => setOpen(false)}>
										<Trans>Done</Trans>
									</Button>
								) : null}
							</div>
						</div>
					</div>
				</DialogContent>
			</Dialog>
		</>
	)
})

function toReferenceOption(kind: AddressReferenceKind, item: ReferenceItem): AddressReferenceOption {
	const details = [
		kind === "geography" ? item.kind : kind === "operator" ? item.category : "",
		kind === "operator" && item.flow_isp_id ? `ISP ${item.flow_isp_id}` : "",
		item.code,
	]
		.filter(Boolean)
		.join(" · ")
	return {
		id: item.id,
		label: kind === "geography" && item.kind ? `${item.kind} · ${item.name}` : item.name,
		description: details || item.description || undefined,
	}
}

import { PlusIcon, Trash2Icon } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"

type KeyValueEditorProps = {
	value: Record<string, string>
	onChange: (value: Record<string, string>) => void
	disabled?: boolean
	keyLabel: string
	valueLabel: string
	addLabel: string
}

export function KeyValueEditor({ value, onChange, disabled, keyLabel, valueLabel, addLabel }: KeyValueEditorProps) {
	const rows = Object.entries(value)

	const updateRow = (index: number, nextKey: string, nextValue: string) => {
		const nextRows = rows.map(([key, item], currentIndex) =>
			currentIndex === index ? [nextKey, nextValue] : [key, item]
		)
		onChange(Object.fromEntries(nextRows.filter(([key]) => key.trim()).map(([key, item]) => [key.trim(), item])))
	}

	const removeRow = (index: number) => {
		onChange(Object.fromEntries(rows.filter((_, currentIndex) => currentIndex !== index)))
	}

	const addRow = () => {
		let key = "key"
		let index = 1
		while (Object.hasOwn(value, key)) {
			index += 1
			key = `key_${index}`
		}
		onChange({ ...value, [key]: "" })
	}

	return (
		<div className="grid gap-2">
			<div className="grid gap-2">
				{rows.map(([key, item], index) => (
					<div key={`${key}-${index}`} className="grid gap-2 sm:grid-cols-[minmax(9rem,16rem)_1fr_auto]">
						<Input
							aria-label={keyLabel}
							value={key}
							onChange={(event) => updateRow(index, event.target.value, item)}
							disabled={disabled}
						/>
						<Input
							aria-label={valueLabel}
							value={item}
							onChange={(event) => updateRow(index, key, event.target.value)}
							disabled={disabled}
						/>
						<Button type="button" variant="outline" size="icon" onClick={() => removeRow(index)} disabled={disabled}>
							<Trash2Icon className="h-4 w-4" />
						</Button>
					</div>
				))}
			</div>
			<Button type="button" variant="outline" size="sm" className="w-fit" onClick={addRow} disabled={disabled}>
				<PlusIcon className="me-2 h-4 w-4" />
				{addLabel}
			</Button>
		</div>
	)
}

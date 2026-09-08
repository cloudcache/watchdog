export function calculateFilterPopoverPosition(
	anchor: { x: number; y: number },
	popover: { width: number; height: number },
	viewport: { width: number; height: number }
): { left: number; top: number } {
	const gutter = 8
	const gap = 10
	const maxLeft = Math.max(gutter, viewport.width - popover.width - gutter)
	const left = Math.min(Math.max(gutter, anchor.x - popover.width + 20), maxLeft)
	const below = anchor.y + gap
	const above = anchor.y - popover.height - gap
	const maxTop = Math.max(gutter, viewport.height - popover.height - gutter)
	const top = below + popover.height <= viewport.height - gutter ? below : Math.min(Math.max(gutter, above), maxTop)
	return { left, top }
}

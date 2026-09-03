import { prependBasePath } from "./router"

export function Logo({ className }: { className?: string }) {
	return (
		<span className={`inline-flex items-center justify-center gap-2 ${className ?? ""}`}>
			<img src={prependBasePath("/static/watchdog-logo.svg")} alt="" aria-hidden="true" className="h-full w-auto" />
			<span className="text-2xl font-bold leading-none tracking-tight">Watchdog</span>
		</span>
	)
}

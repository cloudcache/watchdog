import { t } from "@lingui/core/macro"
import { Trans } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { DialogDescription } from "@radix-ui/react-dialog"
import {
	BookIcon,
	ContainerIcon,
	FingerprintIcon,
	HardDriveIcon,
	Server,
	ServerIcon,
	SettingsIcon,
	UsersIcon,
} from "lucide-react"
import { memo, useEffect, useMemo } from "react"
import {
	CommandDialog,
	CommandEmpty,
	CommandGroup,
	CommandInput,
	CommandItem,
	CommandList,
	CommandSeparator,
	CommandShortcut,
} from "@/components/ui/command"
import { isAdmin } from "@/lib/api"
import { $systems } from "@/lib/stores"
import { getHostDisplayValue, listen } from "@/lib/utils"
import { $router, basePath, navigate } from "./router"

export default memo(function CommandPalette({ open, setOpen }: { open: boolean; setOpen: (open: boolean) => void }) {
	useEffect(() => {
		const down = (e: KeyboardEvent) => {
			if (e.key === "k" && (e.metaKey || e.ctrlKey)) {
				e.preventDefault()
				setOpen(!open)
			}
		}
		return listen(document, "keydown", down)
	}, [open, setOpen])

	return useMemo(() => {
		const systems = $systems.get()
		const SettingsShortcut = (
			<CommandShortcut>
				<Trans>Settings</Trans>
			</CommandShortcut>
		)
		const AdminShortcut = (
			<CommandShortcut>
				<Trans>Admin</Trans>
			</CommandShortcut>
		)
		return (
			<CommandDialog open={open} onOpenChange={setOpen}>
				<DialogDescription className="sr-only">Command palette</DialogDescription>
				<CommandInput placeholder={t`Search for systems or settings...`} />
				<CommandList>
					{systems.length > 0 && (
						<>
							<CommandGroup>
								{systems.map((system) => (
									<CommandItem
										key={system.id}
										onSelect={() => {
											navigate(getPagePath($router, "system", { id: system.id }))
											setOpen(false)
										}}
									>
										<Server className="me-2 size-4" />
										<span className="max-w-60 truncate">{system.name}</span>
										<CommandShortcut>{getHostDisplayValue(system)}</CommandShortcut>
									</CommandItem>
								))}
							</CommandGroup>
							<CommandSeparator className="mb-1.5" />
						</>
					)}
					<CommandGroup heading={t`Pages / Settings`}>
						<CommandItem
							keywords={["home"]}
							onSelect={() => {
								navigate(basePath)
								setOpen(false)
							}}
						>
							<ServerIcon className="me-2 size-4" />
							<span>
								<Trans>All Systems</Trans>
							</span>
							<CommandShortcut>
								<Trans>Page</Trans>
							</CommandShortcut>
						</CommandItem>
						<CommandItem
							onSelect={() => {
								navigate(getPagePath($router, "containers"))
								setOpen(false)
							}}
						>
							<ContainerIcon className="me-2 size-4" />
							<span>
								<Trans>All Containers</Trans>
							</span>
							<CommandShortcut>
								<Trans>Page</Trans>
							</CommandShortcut>
						</CommandItem>
						<CommandItem
							onSelect={() => {
								navigate(getPagePath($router, "smart"))
								setOpen(false)
							}}
						>
							<HardDriveIcon className="me-2 size-4" />
							<span>S.M.A.R.T.</span>
							<CommandShortcut>
								<Trans>Page</Trans>
							</CommandShortcut>
						</CommandItem>
						<CommandItem
							onSelect={() => {
								navigate(getPagePath($router, "settings", { name: "general" }))
								setOpen(false)
							}}
						>
							<SettingsIcon className="me-2 size-4" />
							<span>
								<Trans>Settings</Trans>
							</span>
							{SettingsShortcut}
						</CommandItem>
						<CommandItem
							keywords={[t`Universal token`]}
							onSelect={() => {
								navigate(getPagePath($router, "agents"))
								setOpen(false)
							}}
						>
							<FingerprintIcon className="me-2 size-4" />
							<span>
								<Trans>Tokens & Fingerprints</Trans>
							</span>
							{SettingsShortcut}
						</CommandItem>
						<CommandItem
							keywords={["help", "oauth", "oidc"]}
							onSelect={() => {
								window.location.href = "https://github.com/cloudcache/watchdog"
							}}
						>
							<BookIcon className="me-2 size-4" />
							<span>
								<Trans>Documentation</Trans>
							</span>
							<CommandShortcut>GitHub</CommandShortcut>
						</CommandItem>
					</CommandGroup>
					{isAdmin() && (
						<>
							<CommandSeparator className="mb-1.5" />
							<CommandGroup heading={t`Admin`}>
								<CommandItem
									keywords={["database"]}
									onSelect={() => {
										navigate(getPagePath($router, "users_admin"))
										setOpen(false)
									}}
								>
									<UsersIcon className="me-2 size-4" />
									<span>
										<Trans>Users</Trans>
									</span>
									{AdminShortcut}
								</CommandItem>
							</CommandGroup>
						</>
					)}
					<CommandEmpty>
						<Trans>No results found.</Trans>
					</CommandEmpty>
				</CommandList>
			</CommandDialog>
		)
	}, [open])
})

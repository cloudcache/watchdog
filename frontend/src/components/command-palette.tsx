import { t } from "@lingui/core/macro"
import { Trans } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { DialogDescription } from "@radix-ui/react-dialog"
import { BookIcon, FingerprintIcon, ServerIcon, SettingsIcon, UsersIcon } from "lucide-react"
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
import { can, canAny } from "@/lib/api"
import { listen } from "@/lib/utils"
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
					<CommandGroup heading={t`Pages / Settings`}>
						{can("device.view") ? (
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
						) : null}
						{can("agent.view") ? (
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
						) : null}
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
					{canAny("user.view", "role.view") && (
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

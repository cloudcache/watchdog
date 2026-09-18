import { Trans } from "@lingui/react/macro"
import { useStore } from "@nanostores/react"
import { getPagePath } from "@nanostores/router"
import {
	ActivityIcon,
	BotIcon,
	BarChart3Icon,
	BookmarkIcon,
	CrosshairIcon,
	RouteIcon,
	ServerIcon,
	DatabaseIcon,
	FileDownIcon,
	GaugeIcon,
	GlobeIcon,
	LayoutDashboardIcon,
	ScrollTextIcon,
	ServerCogIcon,
	LibraryIcon,
	LogOutIcon,
	MenuIcon,
	NetworkIcon,
	PlusIcon,
	ReceiptTextIcon,
	SearchIcon,
	SettingsIcon,
	ShieldCheckIcon,
	SlidersHorizontalIcon,
	UserIcon,
	UsersIcon,
} from "lucide-react"
import { lazy, Suspense, useState } from "react"
import { LanguageToggle } from "@/components/language-toggle"
import { ModeToggle } from "@/components/mode-toggle"
import { Button, buttonVariants } from "@/components/ui/button"
import {
	DropdownMenu,
	DropdownMenuContent,
	DropdownMenuGroup,
	DropdownMenuItem,
	DropdownMenuLabel,
	DropdownMenuSeparator,
	DropdownMenuSub,
	DropdownMenuSubContent,
	DropdownMenuSubTrigger,
	DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { can, canAny, canManageAddressLibrary, currentSessionUser, logOut } from "@/lib/api"
import { $platformIdentity } from "@/lib/platform-auth"
import { cn, runOnce } from "@/lib/utils"
import { $router, basePath, Link, navigate, prependBasePath } from "./router"

const CommandPalette = lazy(() => import("./command-palette"))

const isMac = navigator.platform.toUpperCase().indexOf("MAC") >= 0

type NavItemProps = {
	href: string
	icon: React.ComponentType<{ className?: string; strokeWidth?: number }>
	children: React.ReactNode
}

function NavItem({ href, icon: Icon, children }: NavItemProps) {
	return (
		<DropdownMenuItem onSelect={() => navigate(href)}>
			<Icon className="me-2.5 h-4 w-4" strokeWidth={1.5} />
			{children}
		</DropdownMenuItem>
	)
}

function DesktopNavMenu({
	label,
	icon: Icon,
	children,
}: {
	label: React.ReactNode
	icon: NavItemProps["icon"]
	children: React.ReactNode
}) {
	return (
		<DropdownMenu>
			<DropdownMenuTrigger asChild>
				<Button variant="ghost" className="gap-2 px-2.5">
					<Icon className="h-[1.15rem] w-[1.15rem]" strokeWidth={1.5} />
					<span className="hidden lg:inline">{label}</span>
				</Button>
			</DropdownMenuTrigger>
			<DropdownMenuContent align="start" className="min-w-48">
				{children}
			</DropdownMenuContent>
		</DropdownMenu>
	)
}

export default function Navbar() {
	const [commandPaletteOpen, setCommandPaletteOpen] = useState(false)
	useStore($platformIdentity)

	return (
		<div className="flex items-center h-12 md:h-14 bg-card px-4 pe-3 sm:px-6 border border-border bt-0 rounded-md my-3">
			<Suspense>
				<CommandPalette open={commandPaletteOpen} setOpen={setCommandPaletteOpen} />
			</Suspense>

			<Link
				href={basePath}
				aria-label="Home"
				className="p-2 ps-0 me-3 group"
				onMouseEnter={runOnce(() => import("@/components/routes/targets"))}
			>
				<img src={prependBasePath("/static/watchdog-logo.svg")} alt="Watchdog" className="h-6 md:h-7" />
			</Link>
			<Button
				variant="outline"
				className="hidden md:block text-sm text-muted-foreground px-4"
				onClick={() => setCommandPaletteOpen(true)}
			>
				<span className="flex items-center">
					<SearchIcon className="me-1.5 h-4 w-4" />
					<Trans>Search</Trans>
					<span className="flex items-center ms-3.5">
						<Kbd>{isMac ? "⌘" : "Ctrl"}</Kbd>
						<Kbd>K</Kbd>
					</span>
				</span>
			</Button>

			<div className="ms-auto flex items-center text-xl md:hidden">
				<LanguageToggle />
				<ModeToggle />
				<Button variant="ghost" size="icon" onClick={() => setCommandPaletteOpen(true)} aria-label="Search">
					<SearchIcon className="h-[1.2rem] w-[1.2rem]" />
				</Button>
				<DropdownMenu>
					<DropdownMenuTrigger className="ms-2" aria-label="Open Menu">
						<MenuIcon />
					</DropdownMenuTrigger>
					<DropdownMenuContent align="end" className="min-w-56">
						<DropdownMenuLabel>
							<Trans>Overview</Trans>
						</DropdownMenuLabel>
						<NavItem href={getPagePath($router, "watchdog_overview")} icon={LayoutDashboardIcon}>
							Watchdog
						</NavItem>
						<DropdownMenuSeparator />
						<DropdownMenuLabel>
							<Trans>Targets</Trans>
						</DropdownMenuLabel>
						<ResourceItems />
						<DropdownMenuSeparator />
						<DropdownMenuLabel>
							<Trans>Analysis</Trans>
						</DropdownMenuLabel>
						<AnalysisItems />
						<DropdownMenuSeparator />
						<DropdownMenuLabel>
							<Trans>Flow</Trans>
						</DropdownMenuLabel>
						<FlowItems />
						<DropdownMenuSeparator />
						<DropdownMenuLabel>
							<Trans>Data</Trans>
						</DropdownMenuLabel>
						<NavItem href={getPagePath($router, "exports")} icon={FileDownIcon}>
							<Trans>Exports</Trans>
						</NavItem>
						<DropdownMenuSeparator />
						<NavItem href={getPagePath($router, "settings", { name: "general" })} icon={SettingsIcon}>
							<Trans>Settings</Trans>
						</NavItem>
						{canViewAdministration() && <AdminSubmenu />}
						{can("device.create") && (
							<NavItem href={getPagePath($router, "target_new")} icon={PlusIcon}>
								<Trans>Add Target</Trans>
							</NavItem>
						)}
						<DropdownMenuSeparator />
						<DropdownMenuLabel className="max-w-52 truncate">{currentSessionUser()?.username}</DropdownMenuLabel>
						<DropdownMenuItem onSelect={logOut}>
							<LogOutIcon className="me-2.5 h-4 w-4" />
							<Trans>Log Out</Trans>
						</DropdownMenuItem>
					</DropdownMenuContent>
				</DropdownMenu>
			</div>

			<div className="hidden md:flex items-center ms-auto gap-0.5">
				<Link
					href={getPagePath($router, "watchdog_overview")}
					className={cn(buttonVariants({ variant: "ghost" }), "gap-2 px-2.5")}
					aria-label="Watchdog"
				>
					<LayoutDashboardIcon className="h-[1.15rem] w-[1.15rem]" strokeWidth={1.5} />
					<span className="hidden xl:inline">Watchdog</span>
				</Link>
				{can("device.view") ? <DesktopNavMenu label={<Trans>Targets</Trans>} icon={CrosshairIcon}>
					<ResourceItems />
				</DesktopNavMenu> : null}
				{can("device.view") ? <DesktopNavMenu label={<Trans>Analysis</Trans>} icon={BarChart3Icon}>
					<AnalysisItems />
				</DesktopNavMenu> : null}
				{canViewFlow() || canManageAddressLibrary() ? <DesktopNavMenu label={<Trans>Flow</Trans>} icon={ActivityIcon}>
					<FlowItems />
				</DesktopNavMenu> : null}
				{canAny("device.view", "flow.export.customer", "flow.export.supplier", "flow.export.raw") ? <Link
					href={getPagePath($router, "exports")}
					className={cn(buttonVariants({ variant: "ghost" }), "gap-2 px-2.5")}
					aria-label="Exports"
				>
					<FileDownIcon className="h-[1.15rem] w-[1.15rem]" strokeWidth={1.5} />
					<span className="hidden xl:inline">
						<Trans>Data</Trans>
					</span>
				</Link> : null}
				<LanguageToggle />
				<ModeToggle />
				<UserMenu />
				{can("device.create") && (
					<Button
						variant="outline"
						className="flex gap-1 ms-1.5"
						onClick={() => navigate(getPagePath($router, "target_new"))}
					>
						<PlusIcon className="h-4 w-4 -ms-1" />
						<span className="hidden lg:inline">
							<Trans>Add Target</Trans>
						</span>
					</Button>
				)}
			</div>
		</div>
	)
}

// Menu follows the five target kinds frozen in the platform architecture
// (§7.1/7.2): host / network / storage / edge / core. Edge is planned and
// stays hidden until its probe agent ships.
function ResourceItems() {
	if (!can("device.view")) return null
	return (
		<DropdownMenuGroup>
			<NavItem href={getPagePath($router, "targets")} icon={ServerIcon}>
				<Trans>Hosts</Trans>
			</NavItem>
			<NavItem href={getPagePath($router, "network")} icon={NetworkIcon}>
				<Trans>Network</Trans>
			</NavItem>
			<NavItem href={getPagePath($router, "core")} icon={RouteIcon}>
				<Trans>Core (BGP)</Trans>
			</NavItem>
		</DropdownMenuGroup>
	)
}

function AnalysisItems() {
	if (!can("device.view")) return null
	return (
		<DropdownMenuGroup>
			<NavItem href={getPagePath($router, "dashboards")} icon={LayoutDashboardIcon}>
				<Trans>Dashboards</Trans>
			</NavItem>
			<NavItem href={getPagePath($router, "aggregate_charts")} icon={BarChart3Icon}>
				<Trans>Aggregate Charts</Trans>
			</NavItem>
			<NavItem href={getPagePath($router, "aggregate_graphs")} icon={LibraryIcon}>
				<Trans>Saved Graphs</Trans>
			</NavItem>
		</DropdownMenuGroup>
	)
}

function FlowItems() {
	return (
		<DropdownMenuGroup>
			{canViewFlow() ? <NavItem href={getPagePath($router, "flow_overview")} icon={ActivityIcon}>
				<Trans>Flow Overview</Trans>
			</NavItem> : null}
			{canViewFlow() ? <NavItem href={getPagePath($router, "flow_dimensions")} icon={BarChart3Icon}>
				<Trans>Multi-dimensional Analysis</Trans>
			</NavItem> : null}
			{canViewFlow() ? <NavItem href={getPagePath($router, "flow_source")} icon={SearchIcon}>
				<Trans>Source IP Analysis</Trans>
			</NavItem> : null}
			{canViewFlow() ? <NavItem href={getPagePath($router, "flow_destination")} icon={CrosshairIcon}>
				<Trans>Destination IP Analysis</Trans>
			</NavItem> : null}
			{canViewFlow() ? <NavItem href={getPagePath($router, "flow_overseas")} icon={GlobeIcon}>
				<Trans>Overseas Traffic</Trans>
			</NavItem> : null}
			{can("flow.vpn.view") ? <NavItem href={getPagePath($router, "flow_vpn")} icon={ShieldCheckIcon}>
				<Trans>VPN Risk</Trans>
			</NavItem> : null}
			{canViewFlow() ? <NavItem href={getPagePath($router, "flow_filters")} icon={BookmarkIcon}>
				<Trans>Saved Filters</Trans>
			</NavItem> : null}
			{canManageAddressLibrary() ? (
				<>
					<DropdownMenuSeparator />
					<NavItem href={getPagePath($router, "flow_attribution")} icon={RouteIcon}>
						<Trans>Flow Data Attribution</Trans>
					</NavItem>
					<NavItem href={getPagePath($router, "address_library", { section: "prefixes" })} icon={GlobeIcon}>
						<Trans>Address Library</Trans>
					</NavItem>
				</>
			) : null}
		</DropdownMenuGroup>
	)
}

function UserMenu() {
	return (
		<DropdownMenu>
			<DropdownMenuTrigger asChild>
				<button aria-label="User Actions" className={cn(buttonVariants({ variant: "ghost", size: "icon" }))}>
					<UserIcon className="h-[1.2rem] w-[1.2rem]" />
				</button>
			</DropdownMenuTrigger>
			<DropdownMenuContent align="end" className="min-w-52">
				<DropdownMenuLabel className="max-w-52 truncate">{currentSessionUser()?.username}</DropdownMenuLabel>
				<DropdownMenuSeparator />
				<NavItem href={getPagePath($router, "settings", { name: "general" })} icon={SettingsIcon}>
					<Trans>Settings</Trans>
				</NavItem>
				{canViewAdministration() && <AdminSubmenu />}
				<DropdownMenuSeparator />
				<DropdownMenuItem onSelect={logOut}>
					<LogOutIcon className="me-2.5 h-4 w-4" />
					<Trans>Log Out</Trans>
				</DropdownMenuItem>
			</DropdownMenuContent>
		</DropdownMenu>
	)
}

function AdminSubmenu() {
	return (
		<DropdownMenuSub>
			<DropdownMenuSubTrigger>
				<ShieldCheckIcon className="me-2.5 h-4 w-4" />
				<Trans>Administration</Trans>
			</DropdownMenuSubTrigger>
			<DropdownMenuSubContent className="min-w-56">
				<AdminDropdownContent />
			</DropdownMenuSubContent>
		</DropdownMenuSub>
	)
}

function AdminDropdownContent() {
	return (
		<>
			{can("bill.view") ? <DropdownMenuLabel>
				<Trans>Access & Billing</Trans>
			</DropdownMenuLabel> : null}
			{can("bill.view") ? <NavItem href={getPagePath($router, "billing")} icon={ReceiptTextIcon}>
				<Trans>Billing</Trans>
			</NavItem> : null}
			{can("role.view") ? <NavItem href={getPagePath($router, "permissions")} icon={ShieldCheckIcon}>
				<Trans>Permissions</Trans>
			</NavItem> : null}
			{can("device.update") ? <DropdownMenuSeparator /> : null}
			{can("device.update") ? <DropdownMenuLabel>
				<Trans>Network Configuration</Trans>
			</DropdownMenuLabel> : null}
			{can("device.update") ? <NavItem href={getPagePath($router, "snmp_profiles")} icon={SlidersHorizontalIcon}>
				<Trans>SNMP Profiles</Trans>
			</NavItem> : null}
			{can("device.update") ? <NavItem href={getPagePath($router, "snmp_mib_modules")} icon={DatabaseIcon}>
				<Trans>MIB Modules</Trans>
			</NavItem> : null}
			{can("device.update") ? <NavItem href={getPagePath($router, "traffic_defaults")} icon={GaugeIcon}>
				<Trans>Traffic Defaults</Trans>
			</NavItem> : null}
			{can("job.manage") ? <DropdownMenuSeparator /> : null}
			{can("job.manage") ? <DropdownMenuLabel>
				<Trans>Data Management</Trans>
			</DropdownMenuLabel> : null}
			{can("job.manage") ? <NavItem href={getPagePath($router, "retention")} icon={DatabaseIcon}>
				<Trans>Retention</Trans>
			</NavItem> : null}
			<DropdownMenuSeparator />
			<DropdownMenuLabel>
				<Trans>Platform</Trans>
			</DropdownMenuLabel>
			{canAny("user.view", "role.view") ? <NavItem href={getPagePath($router, "users_admin")} icon={UsersIcon}>
				<Trans>Users & Roles</Trans>
			</NavItem> : null}
			{can("audit.view") ? <NavItem href={getPagePath($router, "audit_logs")} icon={ScrollTextIcon}>
				<Trans>Audit Logs</Trans>
			</NavItem> : null}
			{can("job.view") ? <NavItem href={getPagePath($router, "operation_jobs")} icon={ServerCogIcon}>
				<Trans>Background Jobs</Trans>
			</NavItem> : null}
			{can("agent.view") ? <NavItem href={getPagePath($router, "agents")} icon={BotIcon}>
				<Trans>Agents</Trans>
			</NavItem> : null}
		</>
	)
}

function canViewFlow() {
	return canAny("flow.view.customer", "flow.view.supplier", "flow.view.raw")
}

function canViewAdministration() {
	return canAny("user.view", "role.view", "bill.view", "device.update", "job.view", "job.manage", "audit.view", "agent.view")
}

const Kbd = ({ children }: { children: React.ReactNode }) => (
	<kbd className="pointer-events-none inline-flex h-5 select-none items-center gap-1 rounded border bg-muted px-1.5 font-mono text-[10px] font-medium text-muted-foreground opacity-100">
		{children}
	</kbd>
)

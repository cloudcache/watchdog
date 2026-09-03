import { Trans } from "@lingui/react/macro"
import { useStore } from "@nanostores/react"
import { getPagePath } from "@nanostores/router"
import {
	BarChart3Icon,
	Building2Icon,
	ContainerIcon,
	CrosshairIcon,
	DatabaseBackupIcon,
	DatabaseIcon,
	FileDownIcon,
	GaugeIcon,
	GlobeIcon,
	HardDriveIcon,
	HistoryIcon,
	LayoutDashboardIcon,
	LayersIcon,
	LibraryIcon,
	LogOutIcon,
	LogsIcon,
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
import { Button, buttonVariants } from "@/components/ui/button"
import {
	DropdownMenu,
	DropdownMenuContent,
	DropdownMenuGroup,
	DropdownMenuItem,
	DropdownMenuLabel,
	DropdownMenuRadioGroup,
	DropdownMenuRadioItem,
	DropdownMenuSeparator,
	DropdownMenuSub,
	DropdownMenuSubContent,
	DropdownMenuSubTrigger,
	DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { isAdmin, isReadOnlyUser, logOut, pb, selectWatchdogTenant } from "@/lib/api"
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
				<img src="/static/watchdog-logo.svg" alt="Watchdog" className="h-6 md:h-7" />
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
							<Trans>Resources</Trans>
						</DropdownMenuLabel>
						<ResourceItems />
						<DropdownMenuSeparator />
						<DropdownMenuLabel>
							<Trans>Analysis</Trans>
						</DropdownMenuLabel>
						<AnalysisItems />
						<DropdownMenuSeparator />
						<DropdownMenuLabel>
							<Trans>Data</Trans>
						</DropdownMenuLabel>
						<NavItem href={getPagePath($router, "exports")} icon={FileDownIcon}>
							<Trans>Exports</Trans>
						</NavItem>
						<DropdownMenuSeparator />
						<TenantSelector />
						<NavItem href={getPagePath($router, "settings", { name: "general" })} icon={SettingsIcon}>
							<Trans>Settings</Trans>
						</NavItem>
						{isAdmin() && <AdminSubmenu />}
						{!isReadOnlyUser() && (
							<NavItem href={getPagePath($router, "target_new")} icon={PlusIcon}>
								<Trans>Add Target</Trans>
							</NavItem>
						)}
						<DropdownMenuSeparator />
						<DropdownMenuLabel className="max-w-52 truncate">{pb.authStore.record?.email}</DropdownMenuLabel>
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
				<DesktopNavMenu label={<Trans>Resources</Trans>} icon={CrosshairIcon}>
					<ResourceItems />
				</DesktopNavMenu>
				<DesktopNavMenu label={<Trans>Analysis</Trans>} icon={BarChart3Icon}>
					<AnalysisItems />
				</DesktopNavMenu>
				<Link
					href={getPagePath($router, "exports")}
					className={cn(buttonVariants({ variant: "ghost" }), "gap-2 px-2.5")}
					aria-label="Exports"
				>
					<FileDownIcon className="h-[1.15rem] w-[1.15rem]" strokeWidth={1.5} />
					<span className="hidden xl:inline">
						<Trans>Data</Trans>
					</span>
				</Link>
				<UserMenu />
				{!isReadOnlyUser() && (
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

function ResourceItems() {
	return (
		<DropdownMenuGroup>
			<NavItem href={getPagePath($router, "targets")} icon={CrosshairIcon}>
				<Trans>Targets</Trans>
			</NavItem>
			<NavItem href={getPagePath($router, "network")} icon={NetworkIcon}>
				<Trans>Network Targets</Trans>
			</NavItem>
			<NavItem href={getPagePath($router, "containers")} icon={ContainerIcon}>
				<Trans>All Containers</Trans>
			</NavItem>
			<NavItem href={getPagePath($router, "smart")} icon={HardDriveIcon}>
				<Trans>Disk Health</Trans> (S.M.A.R.T.)
			</NavItem>
		</DropdownMenuGroup>
	)
}

function AnalysisItems() {
	return (
		<DropdownMenuGroup>
			<NavItem href={getPagePath($router, "aggregate_charts")} icon={BarChart3Icon}>
				<Trans>Aggregate Charts</Trans>
			</NavItem>
			<NavItem href={getPagePath($router, "aggregate_graphs")} icon={LibraryIcon}>
				<Trans>Saved Graphs</Trans>
			</NavItem>
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
				<DropdownMenuLabel className="max-w-52 truncate">{pb.authStore.record?.email}</DropdownMenuLabel>
				<TenantSelector />
				<DropdownMenuSeparator />
				<NavItem href={getPagePath($router, "settings", { name: "general" })} icon={SettingsIcon}>
					<Trans>Settings</Trans>
				</NavItem>
				{isAdmin() && <AdminSubmenu />}
				<DropdownMenuSeparator />
				<DropdownMenuItem onSelect={logOut}>
					<LogOutIcon className="me-2.5 h-4 w-4" />
					<Trans>Log Out</Trans>
				</DropdownMenuItem>
			</DropdownMenuContent>
		</DropdownMenu>
	)
}

function TenantSelector() {
	const identity = useStore($platformIdentity)
	if (!identity.ready || identity.tenants.length === 0) {
		return null
	}
	return (
		<>
			<DropdownMenuSeparator />
			<DropdownMenuLabel className="flex items-center gap-2">
				<Building2Icon className="h-4 w-4" />
				<Trans>Tenant</Trans>
			</DropdownMenuLabel>
			<DropdownMenuRadioGroup
				value={identity.current?.tenantID ?? ""}
				onValueChange={(tenantID) => {
					selectWatchdogTenant(tenantID)
						.then(() => window.location.reload())
						.catch((error) => console.error("select tenant", error))
				}}
			>
				{identity.tenants.map((tenant) => (
					<DropdownMenuRadioItem key={tenant.id} value={tenant.id}>
						{tenant.name || tenant.id}
					</DropdownMenuRadioItem>
				))}
			</DropdownMenuRadioGroup>
		</>
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
			<DropdownMenuLabel>
				<Trans>Access & Billing</Trans>
			</DropdownMenuLabel>
			<NavItem href={getPagePath($router, "billing")} icon={ReceiptTextIcon}>
				<Trans>Billing</Trans>
			</NavItem>
			<NavItem href={getPagePath($router, "permissions")} icon={ShieldCheckIcon}>
				<Trans>Permissions</Trans>
			</NavItem>
			<DropdownMenuSeparator />
			<DropdownMenuLabel>
				<Trans>Network Configuration</Trans>
			</DropdownMenuLabel>
			<NavItem href={getPagePath($router, "snmp_profiles")} icon={SlidersHorizontalIcon}>
				<Trans>SNMP Profiles</Trans>
			</NavItem>
			<NavItem href={getPagePath($router, "snmp_mib_modules")} icon={DatabaseIcon}>
				<Trans>MIB Modules</Trans>
			</NavItem>
			<NavItem href={getPagePath($router, "traffic_defaults")} icon={GaugeIcon}>
				<Trans>Traffic Defaults</Trans>
			</NavItem>
			<NavItem href={getPagePath($router, "address_prefixes")} icon={GlobeIcon}>
				<Trans>Address Prefixes</Trans>
			</NavItem>
			<NavItem href={getPagePath($router, "address_sets")} icon={LayersIcon}>
				<Trans>Address Sets</Trans>
			</NavItem>
			<DropdownMenuSeparator />
			<DropdownMenuLabel>
				<Trans>Flow & Data</Trans>
			</DropdownMenuLabel>
			<NavItem href={getPagePath($router, "traffic_matrix")} icon={BarChart3Icon}>
				<Trans>Traffic Matrix</Trans>
			</NavItem>
			<NavItem href={getPagePath($router, "retention")} icon={DatabaseIcon}>
				<Trans>Retention</Trans>
			</NavItem>
			<NavItem href={getPagePath($router, "historical_data")} icon={HistoryIcon}>
				<Trans>Historical Data</Trans>
			</NavItem>
			<DropdownMenuSeparator />
			<DropdownMenuLabel>
				<Trans>Platform</Trans>
			</DropdownMenuLabel>
			<DropdownMenuItem asChild>
				<a href={prependBasePath("/_/")} target="_blank" rel="noreferrer">
					<UsersIcon className="me-2.5 h-4 w-4" />
					<Trans>Users</Trans>
				</a>
			</DropdownMenuItem>
			<DropdownMenuItem asChild>
				<a href={prependBasePath("/_/#/logs")} target="_blank" rel="noreferrer">
					<LogsIcon className="me-2.5 h-4 w-4" />
					<Trans>Logs</Trans>
				</a>
			</DropdownMenuItem>
			<DropdownMenuItem asChild>
				<a href={prependBasePath("/_/#/settings/backups")} target="_blank" rel="noreferrer">
					<DatabaseBackupIcon className="me-2.5 h-4 w-4" />
					<Trans>Backups</Trans>
				</a>
			</DropdownMenuItem>
		</>
	)
}

const Kbd = ({ children }: { children: React.ReactNode }) => (
	<kbd className="pointer-events-none inline-flex h-5 select-none items-center gap-1 rounded border bg-muted px-1.5 font-mono text-[10px] font-medium text-muted-foreground opacity-100">
		{children}
	</kbd>
)

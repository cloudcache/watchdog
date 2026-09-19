import "./index.css"
import { i18n } from "@lingui/core"
import { I18nProvider } from "@lingui/react"
import { Trans } from "@lingui/react/macro"
import { useStore } from "@nanostores/react"
import { DirectionProvider } from "@radix-ui/react-direction"
import { lazy, memo, type ReactNode, Suspense, useCallback, useEffect, useState } from "react"
import ReactDOM from "react-dom/client"
import { ErrorBoundary } from "@/components/error-boundary.tsx"
import Navbar from "@/components/navbar.tsx"
import { PageLoading } from "@/components/page-loading.tsx"
import { $router, navigate, type Page, prependBasePath } from "@/components/router.tsx"
import Settings from "@/components/routes/settings/layout.tsx"
import { ThemeProvider } from "@/components/theme-provider.tsx"
import { Toaster } from "@/components/ui/toaster.tsx"
import { canAny, fetchInstallStatus, type InstallStatus, restoreSession } from "@/lib/api.ts"
import { isInstallRedirectRoute, isPublicSessionRoute, shouldRestoreSession } from "@/lib/auth-route-policy.ts"
import { developmentAuth } from "@/lib/env.ts"
import { dynamicActivate, getLocale } from "@/lib/i18n"
import { $platformIdentity } from "@/lib/platform-auth"
import {
	$authenticated,
	$authChecked,
	$copyContent,
	$direction,
	$userSettings,
	defaultLayoutWidth,
} from "@/lib/stores.ts"

const LoginPage = lazy(() => import("@/components/login/login.tsx"))
const InstallPage = lazy(() => import("@/components/install/install.tsx"))
const AggregateCharts = lazy(() => import("@/components/routes/aggregate-charts.tsx"))
const AggregateGraphs = lazy(() => import("@/components/routes/aggregate-graphs.tsx"))
const AggregateGraphForm = lazy(() => import("@/components/routes/aggregate-graph-form.tsx"))
const AggregateGraphDetail = lazy(() => import("@/components/routes/aggregate-graph-detail.tsx"))
const Dashboards = lazy(() => import("@/components/routes/dashboards.tsx"))
const DashboardForm = lazy(() => import("@/components/routes/dashboard-form.tsx"))
const AgentForm = lazy(() => import("@/components/routes/agent-form.tsx"))
const AgentRuns = lazy(() => import("@/components/routes/agent-runs.tsx"))
const AgentPlans = lazy(() => import("@/components/routes/agent-plans.tsx"))
const Agents = lazy(() => import("@/components/routes/agents.tsx"))
const Billing = lazy(() => import("@/components/routes/billing.tsx"))
const BillingAccountDetail = lazy(() => import("@/components/routes/billing-account-detail.tsx"))
const BillingAccountForm = lazy(() => import("@/components/routes/billing-account-form.tsx"))
const ExportDetail = lazy(() => import("@/components/routes/export-detail.tsx"))
const ExportNew = lazy(() => import("@/components/routes/export-new.tsx"))
const Exports = lazy(() => import("@/components/routes/exports.tsx"))
const WatchdogOverview = lazy(() => import("@/components/routes/watchdog-overview.tsx"))
const UsersAdmin = lazy(() => import("@/components/routes/users.tsx"))
const UserForm = lazy(() => import("@/components/routes/user-form.tsx"))
const RoleForm = lazy(() => import("@/components/routes/role-form.tsx"))
const AuditLogs = lazy(() => import("@/components/routes/audit-logs.tsx"))
const OperationJobs = lazy(() => import("@/components/routes/operation-jobs.tsx"))
const CoreBGP = lazy(() => import("@/components/routes/core.tsx"))
const NetworkDeviceDetail = lazy(() => import("@/components/routes/network-device.tsx"))
const NetworkDeviceForm = lazy(() => import("@/components/routes/network-device-form.tsx"))
const NetworkDeviceSNMP = lazy(() => import("@/components/routes/network-device-snmp.tsx"))
const NetworkDevices = lazy(() => import("@/components/routes/network-devices.tsx"))
const NetworkDiscover = lazy(() => import("@/components/routes/network-discover.tsx"))
const NetworkPortDetail = lazy(() => import("@/components/routes/network-port.tsx"))
const NetworkPortForm = lazy(() => import("@/components/routes/network-port-form.tsx"))
const NetworkPortPolicy = lazy(() => import("@/components/routes/network-port-policy.tsx"))
const Permissions = lazy(() => import("@/components/routes/permissions.tsx"))
const Retention = lazy(() => import("@/components/routes/retention.tsx"))
const SNMPMIBModuleForm = lazy(() => import("@/components/routes/snmp-mib-module-form.tsx"))
const SNMPMIBModules = lazy(() => import("@/components/routes/snmp-mib-modules.tsx"))
const SNMPProfileForm = lazy(() => import("@/components/routes/snmp-profile-form.tsx"))
const SNMPProfiles = lazy(() => import("@/components/routes/snmp-profiles.tsx"))
const TargetDetail = lazy(() => import("@/components/routes/target-detail.tsx"))
const TargetForm = lazy(() => import("@/components/routes/target-form.tsx"))
const Targets = lazy(() => import("@/components/routes/targets.tsx"))
const TrafficDefaults = lazy(() => import("@/components/routes/traffic-defaults.tsx"))
const AddressLibrary = lazy(() => import("@/components/routes/address-library.tsx"))
const AddressPrefixes = lazy(() => import("@/components/routes/address-prefixes.tsx"))
const AddressSets = lazy(() => import("@/components/routes/address-sets.tsx"))
const TrafficMatrix = lazy(() => import("@/components/routes/traffic-matrix.tsx"))
const FlowReports = lazy(() => import("@/components/routes/flow-reports.tsx"))
const FlowAttribution = lazy(() => import("@/components/routes/flow-attribution.tsx"))
const FlowVPN = lazy(() => import("@/components/routes/flow-vpn.tsx"))
const FlowVPNRules = lazy(() => import("@/components/routes/flow-vpn-rules.tsx"))
const FlowSavedFilters = lazy(() => import("@/components/routes/flow-saved-filters.tsx"))
const CopyToClipboardDialog = lazy(() => import("@/components/copy-to-clipboard.tsx"))

type Route = Page["route"]
type PageOf<R extends Route> = Extract<Page, { route: R }>

type PageDefinition<R extends Route> = {
	/** Any one of these abilities grants access; administrators always pass. */
	abilities?: string[]
	/** Replaces the generic permission-denied explanation. */
	deniedMessage?: () => ReactNode
	render: (page: PageOf<R>) => ReactNode
}

const deviceView = ["device.view"]
const deviceUpdate = ["device.update"]
const flowView = ["flow.view.customer", "flow.view.supplier", "flow.view.raw"]
const addressLibrary = {
	abilities: ["address.manage", "address.publish"],
	deniedMessage: () => <Trans>Address library maintenance requires an administrator.</Trans>,
}

/** One entry per route in router.tsx: who may open it and what it renders. */
const pages: { [R in Route]: PageDefinition<R> } = {
	home: { abilities: deviceView, render: () => <Targets /> },
	targets: { abilities: deviceView, render: () => <Targets /> },
	target_new: { abilities: ["device.create"], render: () => <TargetForm /> },
	target_edit: { abilities: deviceUpdate, render: ({ params }) => <TargetForm id={params.id} /> },
	target_detail: { abilities: deviceView, render: ({ params }) => <TargetDetail id={params.id} /> },
	system: { abilities: deviceView, render: ({ params }) => <TargetDetail id={params.id} /> },
	core: { abilities: deviceView, render: () => <CoreBGP /> },
	network: { abilities: deviceView, render: () => <NetworkDevices /> },
	network_discover: { abilities: deviceUpdate, render: () => <NetworkDiscover /> },
	network_device_new: { abilities: ["device.create"], render: () => <TargetForm defaultKind="network" /> },
	network_device_edit: { abilities: deviceUpdate, render: ({ params }) => <NetworkDeviceForm id={params.id} /> },
	network_device: { abilities: deviceView, render: ({ params }) => <NetworkDeviceDetail id={params.id} /> },
	network_device_snmp: { abilities: deviceUpdate, render: ({ params }) => <NetworkDeviceSNMP id={params.id} /> },
	network_port: { abilities: ["port.view"], render: ({ params }) => <NetworkPortDetail id={params.id} /> },
	network_port_edit: { abilities: ["port.update"], render: ({ params }) => <NetworkPortForm id={params.id} /> },
	network_port_policy: { abilities: ["port.update"], render: ({ params }) => <NetworkPortPolicy id={params.id} /> },
	traffic_defaults: { abilities: deviceUpdate, render: () => <TrafficDefaults /> },
	snmp_profiles: { render: () => <SNMPProfiles /> },
	snmp_profile_new: { abilities: deviceUpdate, render: () => <SNMPProfileForm /> },
	snmp_profile_edit: { abilities: deviceUpdate, render: ({ params }) => <SNMPProfileForm id={params.id} /> },
	snmp_mib_modules: { render: () => <SNMPMIBModules /> },
	snmp_mib_module_new: { abilities: deviceUpdate, render: () => <SNMPMIBModuleForm /> },
	snmp_mib_module_edit: { abilities: deviceUpdate, render: ({ params }) => <SNMPMIBModuleForm id={params.id} /> },
	aggregate_charts: { abilities: deviceView, render: () => <AggregateCharts /> },
	aggregate_graphs: { abilities: deviceView, render: () => <AggregateGraphs /> },
	aggregate_graph_new: { abilities: deviceUpdate, render: () => <AggregateGraphForm /> },
	aggregate_graph_edit: { abilities: deviceUpdate, render: ({ params }) => <AggregateGraphForm id={params.id} /> },
	aggregate_graph: { abilities: deviceView, render: ({ params }) => <AggregateGraphDetail id={params.id} /> },
	dashboards: { abilities: deviceView, render: () => <Dashboards /> },
	dashboard_new: { abilities: deviceUpdate, render: () => <DashboardForm /> },
	dashboard_edit: { abilities: deviceUpdate, render: ({ params }) => <DashboardForm id={params.id} /> },
	agents: { abilities: ["agent.view"], render: () => <Agents /> },
	agent_new: { abilities: ["agent.manage"], render: () => <AgentForm /> },
	agent_edit: { abilities: ["agent.manage"], render: ({ params }) => <AgentForm id={params.id} /> },
	agent_runs: { abilities: ["agent.view"], render: ({ params }) => <AgentRuns id={params.id} /> },
	agent_plans: { abilities: ["agent.view"], render: ({ params }) => <AgentPlans id={params.id} /> },
	billing: { abilities: ["bill.view"], render: () => <Billing /> },
	billing_new: { abilities: ["bill.create"], render: () => <BillingAccountForm /> },
	billing_edit: { abilities: ["bill.update"], render: ({ params }) => <BillingAccountForm id={params.id} /> },
	billing_detail: { abilities: ["bill.view"], render: ({ params }) => <BillingAccountDetail id={params.id} /> },
	exports: { render: () => <Exports /> },
	export_new: { render: () => <ExportNew /> },
	// "/exports/new" matched export_detail before export_new existed; keep the alias.
	export_detail: { render: ({ params }) => (params.id === "new" ? <ExportNew /> : <ExportDetail id={params.id} />) },
	users_admin: { abilities: ["user.view", "role.view"], render: () => <UsersAdmin /> },
	user_new: { abilities: ["user.create"], render: () => <UserForm /> },
	user_edit: { abilities: ["user.update", "user.manage"], render: ({ params }) => <UserForm id={params.id} /> },
	role_new: { abilities: ["role.create"], render: () => <RoleForm /> },
	role_edit: { abilities: ["role.update"], render: ({ params }) => <RoleForm id={params.id} /> },
	permissions: { abilities: ["role.view"], render: () => <Permissions /> },
	audit_logs: { abilities: ["audit.view"], render: () => <AuditLogs /> },
	operation_jobs: { abilities: ["job.view"], render: () => <OperationJobs /> },
	retention: { abilities: ["job.manage"], render: () => <Retention /> },
	watchdog_overview: { render: () => <WatchdogOverview /> },
	settings: { render: () => <Settings /> },
	address_library_root: { ...addressLibrary, render: () => <AddressLibrary section="prefixes" /> },
	address_library: { ...addressLibrary, render: ({ params }) => <AddressLibrary section={params.section} /> },
	address_prefixes: { ...addressLibrary, render: () => <AddressPrefixes /> },
	address_sets: { ...addressLibrary, render: () => <AddressSets /> },
	flow_overview: { abilities: flowView, render: () => <FlowReports surface="overview" /> },
	flow_attribution: { abilities: ["address.view"], render: () => <FlowAttribution /> },
	traffic_matrix: { abilities: flowView, render: () => <TrafficMatrix key="advanced" surface="overview" /> },
	flow_dimensions: { abilities: flowView, render: () => <FlowReports surface="dimensions" /> },
	flow_source: { abilities: flowView, render: () => <FlowReports surface="source" /> },
	flow_destination: { abilities: flowView, render: () => <FlowReports surface="destination" /> },
	flow_overseas: { abilities: flowView, render: () => <FlowReports surface="overseas" /> },
	flow_vpn: {
		abilities: ["flow.vpn.view"],
		render: () => (
			<>
				<FlowReports surface="vpn" />
				<FlowVPN />
			</>
		),
	},
	flow_vpn_rules: { abilities: ["flow.vpn.manage"], render: () => <FlowVPNRules /> },
	flow_filters: { abilities: flowView, render: () => <FlowSavedFilters /> },
	// Public routes render before the authenticated shell; once signed in they go home.
	forgot_password: { render: () => <RedirectHome /> },
	install: { render: () => <RedirectHome /> },
}

function pageDefinition<R extends Route>(route: R): PageDefinition<R> {
	return pages[route]
}

const App = memo(() => {
	const page = useStore($router)
	const platformIdentity = useStore($platformIdentity)

	// Authenticated pages mount only after the route-triggered session check.
	if (!developmentAuth && !platformIdentity.ready) {
		return <PageLoading />
	}
	if (!developmentAuth && !platformIdentity.current) {
		return (
			<p className="p-3 text-sm text-muted-foreground">
				<Trans>Authentication required.</Trans>
			</p>
		)
	}
	if (!page) {
		return <h1 className="text-3xl text-center my-14">404</h1>
	}
	const definition = pageDefinition(page.route)
	if (definition.abilities?.length && !canAny(...definition.abilities)) {
		return <PermissionDenied message={definition.deniedMessage?.()} />
	}
	return definition.render(page)
})

function PermissionDenied({ message }: { message?: ReactNode }) {
	return (
		<div className="my-14 text-center">
			<h1 className="text-2xl font-semibold">
				<Trans>Permission denied</Trans>
			</h1>
			<p className="mt-2 text-sm text-muted-foreground">
				{message ?? <Trans>Your role does not grant access to this page.</Trans>}
			</p>
		</div>
	)
}

function RedirectHome() {
	useEffect(() => {
		navigate(prependBasePath("/"))
	}, [])
	return null
}

const Layout = () => {
	const authenticated = useStore($authenticated)
	const authChecked = useStore($authChecked)
	const page = useStore($router)
	const copyContent = useStore($copyContent)
	const direction = useStore($direction)
	const { layoutWidth } = useStore($userSettings, { keys: ["layoutWidth"] })
	const [installStatus, setInstallStatus] = useState<InstallStatus>()
	const [installError, setInstallError] = useState("")

	const checkInstallation = useCallback(async () => {
		setInstallError("")
		try {
			setInstallStatus(await fetchInstallStatus())
		} catch (cause) {
			setInstallError((cause as Error).message)
		}
	}, [])

	useEffect(() => {
		checkInstallation()
	}, [checkInstallation])

	useEffect(() => {
		document.documentElement.dir = direction
	}, [direction])

	useEffect(() => {
		const route = page?.route
		// Login/reset pages are passive: rendering them must not make an auth
		// request. The installed setup route redirects before the protected
		// destination restores its session.
		if (installStatus?.installed && !developmentAuth && !authChecked && isPublicSessionRoute(route)) {
			$authChecked.set(true)
			return
		}
		if (isInstallRedirectRoute(route)) return
		if (!shouldRestoreSession({ installed: installStatus?.installed === true, developmentAuth, authChecked, route }))
			return
		restoreSession().catch(() => $authChecked.set(true))
	}, [authChecked, installStatus?.installed, page?.route])

	useEffect(() => {
		if (installStatus?.requires_install && page?.route !== "install") {
			navigate(prependBasePath("/install"))
		} else if (installStatus?.installed && page?.route === "install") {
			navigate(prependBasePath("/"))
		}
	}, [installStatus?.installed, installStatus?.requires_install, page?.route])

	if (installError) {
		return (
			<div className="min-h-svh grid place-content-center gap-4 px-4 text-center">
				<p role="alert" className="text-sm text-destructive">
					{installError}
				</p>
				<button type="button" className="text-sm underline" onClick={checkInstallation}>
					<Trans>Retry</Trans>
				</button>
			</div>
		)
	}

	// Two round trips (install status, then session) precede the first page; show
	// progress instead of an empty document.
	if (!installStatus) {
		return <PageLoading />
	}

	if (installStatus.requires_install) {
		return (
			<DirectionProvider dir={direction}>
				<Suspense fallback={<PageLoading />}>
					<InstallPage
						onInstalled={(status) => {
							setInstallStatus(status)
							$platformIdentity.set({ ready: true })
							$authenticated.set(false)
							$authChecked.set(true)
							navigate(prependBasePath("/"))
						}}
					/>
				</Suspense>
			</DirectionProvider>
		)
	}

	if (!authChecked) {
		return <PageLoading />
	}

	return (
		<DirectionProvider dir={direction}>
			{!authenticated ? (
				<Suspense fallback={<PageLoading />}>
					<LoginPage />
				</Suspense>
			) : (
				<div style={{ "--container": `${layoutWidth ?? defaultLayoutWidth}px` } as React.CSSProperties}>
					<div className="container">
						<Navbar />
					</div>
					<div className="container relative">
						<ErrorBoundary resetKey={page?.path}>
							<Suspense fallback={<PageLoading />}>
								<App />
							</Suspense>
						</ErrorBoundary>
						{copyContent && (
							<Suspense>
								<CopyToClipboardDialog content={copyContent} />
							</Suspense>
						)}
					</div>
				</div>
			)}
		</DirectionProvider>
	)
}

const I18nApp = () => {
	useEffect(() => {
		dynamicActivate(getLocale())
	}, [])

	return (
		<I18nProvider i18n={i18n}>
			<ThemeProvider>
				<Layout />
				<Toaster />
			</ThemeProvider>
		</I18nProvider>
	)
}

ReactDOM.createRoot(document.getElementById("app") as HTMLElement).render(
	// strict mode in dev mounts / unmounts components twice
	// and breaks the clipboard dialog
	//<StrictMode>
	<I18nApp />
	//</StrictMode>
)

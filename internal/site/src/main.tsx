import "./index.css"
import { i18n } from "@lingui/core"
import { I18nProvider } from "@lingui/react"
import { useStore } from "@nanostores/react"
import { DirectionProvider } from "@radix-ui/react-direction"
// import { Suspense, lazy, useEffect, StrictMode } from "react"
import { lazy, memo, Suspense, useEffect } from "react"
import ReactDOM from "react-dom/client"
import Navbar from "@/components/navbar.tsx"
import { $router } from "@/components/router.tsx"
import Settings from "@/components/routes/settings/layout.tsx"
import { ThemeProvider } from "@/components/theme-provider.tsx"
import { Toaster } from "@/components/ui/toaster.tsx"
import { alertManager } from "@/lib/alerts"
import { isAdmin, pb, refreshWatchdogIdentity, updateUserSettings } from "@/lib/api.ts"
import { dynamicActivate, getLocale } from "@/lib/i18n"
import { $platformIdentity } from "@/lib/platform-auth"
import {
	$authenticated,
	$copyContent,
	$direction,
	$newVersion,
	$publicKey,
	$userSettings,
	defaultLayoutWidth,
} from "@/lib/stores.ts"
import type { WatchdogInfo, UpdateInfo } from "./types"

const LoginPage = lazy(() => import("@/components/login/login.tsx"))
const AggregateCharts = lazy(() => import("@/components/routes/aggregate-charts.tsx"))
const AggregateGraphs = lazy(() => import("@/components/routes/aggregate-graphs.tsx"))
const AggregateGraphForm = lazy(() => import("@/components/routes/aggregate-graph-form.tsx"))
const AggregateGraphDetail = lazy(() => import("@/components/routes/aggregate-graph-detail.tsx"))
const Dashboards = lazy(() => import("@/components/routes/dashboards.tsx"))
const DashboardForm = lazy(() => import("@/components/routes/dashboard-form.tsx"))
const AgentForm = lazy(() => import("@/components/routes/agent-form.tsx"))
const AgentRuns = lazy(() => import("@/components/routes/agent-runs.tsx"))
const Agents = lazy(() => import("@/components/routes/agents.tsx"))
const Billing = lazy(() => import("@/components/routes/billing.tsx"))
const BillingAccountDetail = lazy(() => import("@/components/routes/billing-account-detail.tsx"))
const BillingAccountForm = lazy(() => import("@/components/routes/billing-account-form.tsx"))
const ExportDetail = lazy(() => import("@/components/routes/export-detail.tsx"))
const ExportNew = lazy(() => import("@/components/routes/export-new.tsx"))
const Exports = lazy(() => import("@/components/routes/exports.tsx"))
const HistoricalData = lazy(() => import("@/components/routes/historical-data.tsx"))
const WatchdogOverview = lazy(() => import("@/components/routes/watchdog-overview.tsx"))
const UsersAdmin = lazy(() => import("@/components/routes/users.tsx"))
const ModulesAdmin = lazy(() => import("@/components/routes/modules.tsx"))
const AuditLogs = lazy(() => import("@/components/routes/audit-logs.tsx"))
const OperationJobs = lazy(() => import("@/components/routes/operation-jobs.tsx"))
const Containers = lazy(() => import("@/components/routes/containers.tsx"))
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
const PermissionForm = lazy(() => import("@/components/routes/permission-form.tsx"))
const Retention = lazy(() => import("@/components/routes/retention.tsx"))
const Smart = lazy(() => import("@/components/routes/smart.tsx"))
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
const FlowVPN = lazy(() => import("@/components/routes/flow-vpn.tsx"))
const CopyToClipboardDialog = lazy(() => import("@/components/copy-to-clipboard.tsx"))

const watchdogDevAuth = import.meta.env.VITE_WATCHDOG_DEV_AUTH === "true"

const App = memo(() => {
	const page = useStore($router)
	const platformIdentity = useStore($platformIdentity)

	useEffect(() => {
		// change auth store on auth change
		const unsubscribeAuth = pb.authStore.onChange(() => {
			$authenticated.set(watchdogDevAuth || pb.authStore.isValid)
		})
		if (watchdogDevAuth) {
			return () => unsubscribeAuth()
		}
		const identityReady = refreshWatchdogIdentity().catch((error) => {
			console.error("initialize platform identity", error)
		})
		// get general info for authenticated users, such as public key and version
		pb.send<WatchdogInfo>("/api/watchdog/info", {}).then((data) => {
			$publicKey.set(data.key)
			// Wait for the MySQL authorization projection before showing admin-only updates.
			identityReady.then(() => {
				if (data.cu && isAdmin()) {
					pb.send<UpdateInfo>("/api/watchdog/update", {}).then($newVersion.set)
				}
			})
		})
		// get user settings
		updateUserSettings()
		alertManager.refresh().then(alertManager.subscribe)
		return () => {
			unsubscribeAuth()
			alertManager.unsubscribe()
		}
	}, [])

	// Tenant-scoped pages must not mount until identity discovery has replaced
	// any stale tenant selection left by a previous login or migration.
	if (!watchdogDevAuth && !platformIdentity.ready) {
		return <div className="p-3 text-sm text-muted-foreground">Loading...</div>
	}
	if (!watchdogDevAuth && !platformIdentity.current) {
		return <div className="p-3 text-sm text-muted-foreground">Select a tenant from the account menu.</div>
	}
	if (!page) {
		return <h1 className="text-3xl text-center my-14">404</h1>
	} else if (page.route === "aggregate_charts") {
		return <AggregateCharts />
	} else if (page.route === "aggregate_graphs") {
		return <AggregateGraphs />
	} else if (page.route === "aggregate_graph_new") {
		return <AggregateGraphForm />
	} else if (page.route === "aggregate_graph_edit") {
		return <AggregateGraphForm id={page.params.id} />
	} else if (page.route === "aggregate_graph") {
		return <AggregateGraphDetail id={page.params.id} />
	} else if (page.route === "dashboards") {
		return <Dashboards />
	} else if (page.route === "dashboard_new") {
		return <DashboardForm />
	} else if (page.route === "dashboard_edit") {
		return <DashboardForm id={page.params.id} />
	} else if (page.route === "agents") {
		return <Agents />
	} else if (page.route === "agent_new") {
		return <AgentForm />
	} else if (page.route === "agent_edit") {
		return <AgentForm id={page.params.id} />
	} else if (page.route === "agent_runs") {
		return <AgentRuns id={page.params.id} />
	} else if (page.route === "billing") {
		return <Billing />
	} else if (page.route === "billing_new") {
		return <BillingAccountForm />
	} else if (page.route === "billing_edit") {
		return <BillingAccountForm id={page.params.id} />
	} else if (page.route === "billing_detail") {
		return <BillingAccountDetail id={page.params.id} />
	} else if (page.route === "watchdog_overview") {
		return <WatchdogOverview />
	} else if (page.route === "home") {
		return <Targets />
	} else if (page.route === "system") {
		return <TargetDetail id={page.params.id} />
	} else if (page.route === "targets") {
		return <Targets />
	} else if (page.route === "target_new") {
		return <TargetForm />
	} else if (page.route === "target_edit") {
		return <TargetForm id={page.params.id} />
	} else if (page.route === "target_detail") {
		return <TargetDetail id={page.params.id} />
	} else if (page.route === "containers") {
		return <Containers />
	} else if (page.route === "core") {
		return <CoreBGP />
	} else if (page.route === "export_new") {
		return <ExportNew />
	} else if (page.route === "export_detail") {
		if (page.params.id === "new") {
			return <ExportNew />
		}
		return <ExportDetail id={page.params.id} />
	} else if (page.route === "exports") {
		return <Exports />
	} else if (page.route === "historical_data") {
		return <HistoricalData />
	} else if (page.route === "network") {
		return <NetworkDevices />
	} else if (page.route === "network_discover") {
		return <NetworkDiscover />
	} else if (page.route === "network_device_new") {
		return <TargetForm defaultKind="network" />
	} else if (page.route === "network_device_edit") {
		return <NetworkDeviceForm id={page.params.id} />
	} else if (page.route === "network_device") {
		return <NetworkDeviceDetail id={page.params.id} />
	} else if (page.route === "network_device_snmp") {
		return <NetworkDeviceSNMP id={page.params.id} />
	} else if (page.route === "network_port") {
		return <NetworkPortDetail id={page.params.id} />
	} else if (page.route === "network_port_edit") {
		return <NetworkPortForm id={page.params.id} />
	} else if (page.route === "network_port_policy") {
		return <NetworkPortPolicy id={page.params.id} />
	} else if (page.route === "users_admin") {
		return <UsersAdmin />
	} else if (page.route === "modules_admin") {
		return <ModulesAdmin />
	} else if (page.route === "audit_logs") {
		return <AuditLogs />
	} else if (page.route === "operation_jobs") {
		return <OperationJobs />
	} else if (page.route === "permissions") {
		return <Permissions />
	} else if (page.route === "permission_new") {
		return <PermissionForm />
	} else if (page.route === "permission_edit") {
		return <PermissionForm id={page.params.id} />
	} else if (page.route === "retention") {
		return <Retention />
	} else if (page.route === "smart") {
		return <Smart />
	} else if (page.route === "snmp_profiles") {
		return <SNMPProfiles />
	} else if (page.route === "snmp_profile_new") {
		return <SNMPProfileForm />
	} else if (page.route === "snmp_profile_edit") {
		return <SNMPProfileForm id={page.params.id} />
	} else if (page.route === "snmp_mib_modules") {
		return <SNMPMIBModules />
	} else if (page.route === "snmp_mib_module_new") {
		return <SNMPMIBModuleForm />
	} else if (page.route === "snmp_mib_module_edit") {
		return <SNMPMIBModuleForm id={page.params.id} />
	} else if (page.route === "settings") {
		return <Settings />
	} else if (page.route === "traffic_defaults") {
		return <TrafficDefaults />
	} else if (page.route === "address_library_root") {
		return <AddressLibrary section="imports" />
	} else if (page.route === "address_library") {
		return <AddressLibrary section={page.params.section} />
	} else if (page.route === "address_prefixes") {
		return <AddressPrefixes />
	} else if (page.route === "address_sets") {
		return <AddressSets />
	} else if (page.route === "flow_overview" || page.route === "traffic_matrix") {
		return <TrafficMatrix key="overview" surface="overview" />
	} else if (page.route === "flow_dimensions") {
		return <TrafficMatrix key="dimensions" surface="dimensions" />
	} else if (page.route === "flow_source") {
		return <TrafficMatrix key="source" surface="source" />
	} else if (page.route === "flow_destination") {
		return <TrafficMatrix key="destination" surface="destination" />
	} else if (page.route === "flow_overseas") {
		return <TrafficMatrix key="overseas" surface="overseas" />
	} else if (page.route === "flow_vpn") {
		return <FlowVPN />
	}
})

const Layout = () => {
	const authenticated = useStore($authenticated)
	const copyContent = useStore($copyContent)
	const direction = useStore($direction)
	const { layoutWidth } = useStore($userSettings, { keys: ["layoutWidth"] })

	useEffect(() => {
		document.documentElement.dir = direction
	}, [direction])

	return (
		<DirectionProvider dir={direction}>
			{!authenticated ? (
				<Suspense>
					<LoginPage />
				</Suspense>
			) : (
				<div style={{ "--container": `${layoutWidth ?? defaultLayoutWidth}px` } as React.CSSProperties}>
					<div className="container">
						<Navbar />
					</div>
					<div className="container relative">
						<App />
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

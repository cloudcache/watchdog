import { createRouter } from "@nanostores/router"

const routes = {
	home: "/",
	agents: "/agents",
	agent_new: "/agents/new",
	agent_edit: "/agents/:id/edit",
	agent_runs: "/agents/:id/runs",
	aggregate_charts: "/aggregate-charts",
	aggregate_graphs: "/aggregate-graphs",
	aggregate_graph_new: "/aggregate-graphs/new",
	aggregate_graph_edit: "/aggregate-graphs/:id/edit",
	aggregate_graph: "/aggregate-graphs/:id",
	billing: "/billing",
	billing_new: "/billing/new",
	billing_detail: "/billing/:id",
	billing_edit: "/billing/:id/edit",
	containers: "/containers",
	core: "/core",
	exports: "/exports",
	export_new: "/exports/new",
	export_detail: "/exports/:id",
	historical_data: "/historical-data",
	network: "/network",
	network_discover: "/network/discover",
	network_device_new: "/network/devices/new",
	network_device_edit: "/network/devices/:id/edit",
	network_device: "/network/devices/:id",
	network_device_snmp: "/network/devices/:id/snmp",
	network_port: "/network/ports/:id",
	network_port_edit: "/network/ports/:id/edit",
	network_port_policy: "/network/ports/:id/policy",
	permissions: "/permissions",
	users_admin: "/users",
	modules_admin: "/modules",
	audit_logs: "/audit-logs",
	permission_new: "/permissions/new",
	permission_edit: "/permissions/:id/edit",
	retention: "/retention",
	watchdog_overview: "/watchdog",
	smart: "/smart",
	snmp_profiles: "/snmp/profiles",
	snmp_profile_new: "/snmp/profiles/new",
	snmp_profile_edit: "/snmp/profiles/:id/edit",
	snmp_mib_modules: "/snmp/mib-modules",
	snmp_mib_module_new: "/snmp/mib-modules/new",
	snmp_mib_module_edit: "/snmp/mib-modules/:id/edit",
	system: `/system/:id`,
	targets: "/targets",
	target_new: "/targets/new",
	target_edit: "/targets/:id/edit",
	target_detail: "/targets/:id",
	traffic_defaults: "/network/traffic-defaults",
	address_prefixes: "/address-prefixes",
	address_sets: "/address-sets",
	traffic_matrix: "/traffic-matrix",
	settings: `/settings/:name?`,
	forgot_password: `/forgot-password`,
	request_otp: `/request-otp`,
} as const

/**
 * The base path of the application.
 * This is used to prepend the base path to all routes.
 */
export const basePath = WATCHDOG?.BASE_PATH || ""

/**
 * Prepends the base path to the given path.
 * @param path The path to prepend the base path to.
 * @returns The path with the base path prepended.
 */
export const prependBasePath = (path: string) => (basePath + path).replaceAll("//", "/")

// prepend base path to routes
for (const route in routes) {
	// @ts-expect-error need as const above to get nanostores to parse types properly
	routes[route] = prependBasePath(routes[route])
}

export const $router = createRouter(routes, { links: false })

/** Navigate to url using router
 *  Base path is automatically prepended if serving from subpath
 */
export const navigate = (urlString: string) => {
	$router.open(urlString)
}

export function Link(props: React.AnchorHTMLAttributes<HTMLAnchorElement>) {
	return (
		<a
			{...props}
			onClick={(e) => {
				e.preventDefault()
				const href = props.href || ""
				if (e.ctrlKey || e.metaKey) {
					window.open(href, "_blank")
				} else {
					navigate(href)
					props.onClick?.(e)
				}
			}}
		></a>
	)
}

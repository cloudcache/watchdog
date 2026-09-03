export const vendorLogos: Record<string, string> = {
	arista: "/static/vendor-logos/arista.svg",
	cisco: "/static/vendor-logos/cisco.svg",
	huawei: "/static/vendor-logos/huawei.svg",
	junos: "/static/vendor-logos/junos.png",
	zte: "/static/vendor-logos/zte.svg",
}

export function vendorLogoFor(vendor: string) {
	const key = vendor.trim().toLowerCase()
	if (vendorLogos[key]) return vendorLogos[key]
	if (key.includes("huawei")) return vendorLogos.huawei
	if (key.includes("cisco")) return vendorLogos.cisco
	if (key.includes("juniper") || key.includes("junos")) return vendorLogos.junos
	if (key.includes("arista")) return vendorLogos.arista
	if (key.includes("zte")) return vendorLogos.zte
	return ""
}

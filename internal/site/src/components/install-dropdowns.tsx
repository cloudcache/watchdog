import { memo } from "react"
import { copyToClipboard, getHubURL } from "@/lib/utils"
import { DropdownMenuContent, DropdownMenuItem } from "./ui/dropdown-menu"

// const isbeta = watchdog.hub_version.includes("beta")
// const imagetag = isbeta ? ":edge" : ""

const rawRepositoryURL = "https://raw.githubusercontent.com/cloudcache/watchdog/main/supplemental/scripts"

export function copyDockerCompose(port = "45876", publicKey: string, token: string) {
	copyToClipboard(`services:
  watchdog-agent:
    image: cloudcache/watchdog-agent
    container_name: watchdog-agent
    restart: unless-stopped
    network_mode: host
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - ./watchdog_agent_data:/var/lib/watchdog-agent
      # monitor other disks / partitions by mounting a folder in /extra-filesystems
      # - /mnt/disk/.watchdog:/extra-filesystems/sda1:ro
    environment:
      LISTEN: ${port}
      KEY: '${publicKey}'
      TOKEN: ${token}
      HUB_URL: ${getHubURL()}`)
}

export function copyDockerRun(port = "45876", publicKey: string, token: string) {
	copyToClipboard(
		`docker run -d --name watchdog-agent --network host --restart unless-stopped -v /var/run/docker.sock:/var/run/docker.sock:ro -v watchdog_agent_data:/var/lib/watchdog-agent -e KEY="${publicKey}" -e LISTEN=${port} -e TOKEN="${token}" -e HUB_URL="${getHubURL()}" cloudcache/watchdog-agent`
	)
}

export function copyLinuxCommand(port = "45876", publicKey: string, token: string, brew = false) {
	const script = brew ? "install-agent-brew.sh" : "install-agent.sh"
	const cmd = `curl -fsSL ${rawRepositoryURL}/${script} -o /tmp/install-agent.sh && chmod +x /tmp/install-agent.sh && /tmp/install-agent.sh -p ${port} -k "${publicKey}" -t "${token}" -url "${getHubURL()}"`
	copyToClipboard(cmd)
}

export function copyWindowsCommand(port = "45876", publicKey: string, token: string) {
	copyToClipboard(
		`& iwr -useb ${rawRepositoryURL}/install-agent.ps1 -OutFile "$env:TEMP\\install-agent.ps1"; & Powershell -ExecutionPolicy Bypass -File "$env:TEMP\\install-agent.ps1" -Key "${publicKey}" -Port ${port} -Token "${token}" -Url "${getHubURL()}"`
	)
}

export interface DropdownItem {
	text: string
	onClick?: () => void
	url?: string
	icons?: React.ComponentType<React.SVGProps<SVGSVGElement>>[]
}

export const InstallDropdown = memo(({ items }: { items: DropdownItem[] }) => {
	return (
		<DropdownMenuContent align="end">
			{items.map((item, index) => {
				const className = "cursor-pointer flex items-center gap-1.5"
				return item.url ? (
					<DropdownMenuItem key={index} asChild>
						<a href={item.url} className={className} target="_blank" rel="noopener noreferrer">
							{item.text}{" "}
							{item.icons?.map((Icon, iconIndex) => (
								<Icon key={iconIndex} className="size-4" />
							))}
						</a>
					</DropdownMenuItem>
				) : (
					<DropdownMenuItem key={index} onClick={item.onClick} className={className}>
						{item.text}{" "}
						{item.icons?.map((Icon, iconIndex) => (
							<Icon key={iconIndex} className="size-4" />
						))}
					</DropdownMenuItem>
				)
			})}
		</DropdownMenuContent>
	)
})

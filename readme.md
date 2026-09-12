# Watchdog

Watchdog is a monitoring and traffic-analysis platform for systems, network devices, and flow data.

It has a web interface, local user and role management, extensible agents, SNMP monitoring, and high-throughput sFlow/NetFlow analysis.

[![agent Docker Image Size](https://img.shields.io/docker/image-size/cloudcache/watchdog-agent/latest?logo=docker&label=agent%20image%20size)](https://hub.docker.com/r/cloudcache/watchdog-agent)
[![hub Docker Image Size](https://img.shields.io/docker/image-size/cloudcache/watchdog/latest?logo=docker&label=hub%20image%20size)](https://hub.docker.com/r/cloudcache/watchdog)
[![MIT license](https://img.shields.io/github/license/cloudcache/watchdog?color=%239944ee)](https://github.com/cloudcache/watchdog/blob/main/LICENSE)

## Features

- **Lightweight**: Smaller and less resource-intensive than leading solutions.
- **Simple**: Easy setup with little manual configuration required.
- **Docker stats**: Tracks CPU, memory, and network usage history for each container.
- **Alerts**: Configurable alerts for CPU, memory, disk, bandwidth, temperature, load average, and status.
- **Users and roles**: Local accounts use explicit device, port, billing, and flow permissions.
- **Automatic backups**: Save to and restore from disk or S3-compatible storage.
<!-- - **REST API**: Use or update your data in your own scripts and applications. -->

## Architecture

Watchdog consists of two main components: the **server** and the **agent**.

- **Server**: A Gin API backed by MySQL for authentication and management data, with ClickHouse for telemetry and flow data.
- **Agent**: Runs on each system you want to monitor and communicates system metrics to the server.

## Getting started

Start with the repository's [installation guide](docs/watchdog-install.md). Architecture, operations, and flow-module design documents are available in [docs](docs/).

## Supported metrics

- **CPU usage** - Host system and Docker / Podman containers.
- **Memory usage** - Host system and containers. Includes swap and ZFS ARC.
- **Disk usage** - Host system. Supports multiple partitions and devices.
- **Disk I/O** - Host system. Supports multiple partitions and devices.
- **Network usage** - Host system and containers.
- **Load average** - Host system.
- **Temperature** - Host system sensors.
- **GPU usage / power draw** - Nvidia, AMD, and Intel.
- **Battery** - Host system battery charge.
- **Containers** - Status and metrics of all running Docker / Podman containers.
- **S.M.A.R.T.** - Host system disk health (includes eMMC wear/EOL and Linux mdraid array health via sysfs when available).

## Help and discussion

Please search existing issues and discussions before opening a new one. I try my best to respond, but may not always have time to do so.

#### Bug reports and feature requests

Bug reports and feature requests can be posted on [GitHub issues](https://github.com/cloudcache/watchdog/issues).

#### Support and general discussion

Support requests and general discussion can be posted on [GitHub discussions](https://github.com/cloudcache/watchdog/discussions).

## License

Watchdog is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

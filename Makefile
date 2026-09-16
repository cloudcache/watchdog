# Default OS/ARCH values
OS ?= $(shell go env GOOS)
ARCH ?= $(shell go env GOARCH)
# Controls NVML/glibc agent build tag behavior:
# - auto (default): enable on linux/amd64 glibc hosts
# - true: always enable
# - false: always disable
NVML ?= auto

# Detect glibc host for local linux/amd64 builds.
HOST_GLIBC := $(shell \
	if [ "$(OS)" = "linux" ] && [ "$(ARCH)" = "amd64" ]; then \
		for p in /lib64/ld-linux-x86-64.so.2 /lib/x86_64-linux-gnu/ld-linux-x86-64.so.2 /lib/ld-linux-x86-64.so.2; do \
			[ -e "$$p" ] && { echo true; exit 0; }; \
		done; \
		if command -v ldd >/dev/null 2>&1; then \
			if ldd --version 2>&1 | tr '[:upper:]' '[:lower:]' | awk '/gnu libc|glibc/{found=1} END{exit !found}'; then \
				echo true; \
			else \
				echo false; \
			fi; \
		else \
			echo false; \
		fi; \
	else \
		echo false; \
	fi)

# Enable glibc build tag for NVML on supported Linux builds.
AGENT_GO_TAGS :=
ifeq ($(NVML),true)
AGENT_GO_TAGS := -tags glibc
else ifeq ($(NVML),auto)
ifeq ($(HOST_GLIBC),true)
AGENT_GO_TAGS := -tags glibc
endif
endif

# Set executable extension based on target OS
EXE_EXT := $(if $(filter windows,$(OS)),.exe,)

.PHONY: tidy build build-runtime build-flow-tools build-agent build-server build-snmp-collector build-snmp-agent build-flow-collect build-flow-worker build-web-ui clean lint dev-agent dev-server dev-frontend dev-snmp-collector generate-locales local-secrets flow-dev-up flow-dev-down flow-dev-status
.DEFAULT_GOAL := build

WATCHDOG_CLICKHOUSE_PASSWORD ?= watchdog-local
LOCAL_CLICKHOUSE_PASSWORD_FILE ?= data/secrets/clickhouse-password

clean:
	go clean
	rm -rf ./build

lint:
	golangci-lint run

test:
	go test -tags=testing ./...

tidy:
	go mod tidy

build-web-ui:
	npm --prefix ./frontend run build

# Conditional .NET build - only for Windows
build-dotnet-conditional:
	@if [ "$(OS)" = "windows" ]; then \
		echo "Building .NET executable for Windows..."; \
		if command -v dotnet >/dev/null 2>&1; then \
			rm -rf ./agent/lhm/bin; \
			dotnet build -c Release ./agent/lhm/watchdog_lhm.csproj; \
		else \
			echo "Error: dotnet not found. Install .NET SDK to build Windows agent."; \
			exit 1; \
		fi; \
	fi

# Update build-agent to include conditional .NET build
build-agent: build-dotnet-conditional
	GOOS=$(OS) GOARCH=$(ARCH) go build $(AGENT_GO_TAGS) -o ./build/watchdog-agent_$(OS)_$(ARCH)$(EXE_EXT) -ldflags "-w -s" ./internal/cmd/agent

build-server:
	GOOS=$(OS) GOARCH=$(ARCH) go build -o ./build/watchdog-server$(EXE_EXT) ./cmd/watchdog-server

build-snmp-collector:
	GOOS=$(OS) GOARCH=$(ARCH) go build -o ./build/watchdog-snmp-collector$(EXE_EXT) ./cmd/watchdog-snmp-collector

build-snmp-agent:
	GOOS=$(OS) GOARCH=$(ARCH) go build -o ./build/watchdog-snmp-agent$(EXE_EXT) ./cmd/watchdog-snmp-agent

build-flow-collect:
	GOOS=$(OS) GOARCH=$(ARCH) go build -o ./build/watchdog-flow-collect$(EXE_EXT) ./cmd/watchdog-flow-collect

build-flow-worker:
	GOOS=$(OS) GOARCH=$(ARCH) go build -o ./build/watchdog-flow-worker$(EXE_EXT) ./cmd/watchdog-flow-worker

build-flow-tools:
	GOOS=$(OS) GOARCH=$(ARCH) go build -o ./build/watchdog-flow-migrate$(EXE_EXT) ./cmd/watchdog-flow-migrate
	GOOS=$(OS) GOARCH=$(ARCH) go build -o ./build/watchdog-flow-restore-drill$(EXE_EXT) ./cmd/watchdog-flow-restore-drill

# The five independently deployed product processes have one canonical build
# location. Extra agents (system and SNMP trap) remain separate optional builds.
build-runtime: build-web-ui build-server build-snmp-collector build-flow-collect build-flow-worker

build: build-agent build-runtime build-snmp-agent

generate-locales:
	@if [ ! -f ./frontend/src/locales/en/en.ts ]; then \
		echo "Generating locales..."; \
		npm install --prefix ./frontend && npm run --prefix ./frontend sync; \
	fi

local-secrets:
	@mkdir -p "$(dir $(LOCAL_CLICKHOUSE_PASSWORD_FILE))"
	@umask 077; printf '%s\n' "$(WATCHDOG_CLICKHOUSE_PASSWORD)" > "$(LOCAL_CLICKHOUSE_PASSWORD_FILE)"

dev-server: local-secrets build-server
	./build/watchdog-server --config config/watchdog.yaml

dev-frontend:
	npm --prefix ./frontend run dev

dev-snmp-collector: local-secrets build-snmp-collector
	./build/watchdog-snmp-collector --config config/watchdog.yaml --discover=false --poll=true --loop=true

dev-agent:
	@if command -v entr >/dev/null 2>&1; then \
		find ./internal/cmd/agent/*.go ./agent/*.go | entr -r go run $(AGENT_GO_TAGS) github.com/cloudcache/watchdog/internal/cmd/agent; \
	else \
		go run $(AGENT_GO_TAGS) github.com/cloudcache/watchdog/internal/cmd/agent; \
	fi

flow-dev-up: local-secrets
	WATCHDOG_CLICKHOUSE_PASSWORD="$(WATCHDOG_CLICKHOUSE_PASSWORD)" docker compose -f deploy/compose.flow-dev.yml up -d

flow-dev-down:
	docker compose -f deploy/compose.flow-dev.yml down

flow-dev-status:
	docker compose -f deploy/compose.flow-dev.yml ps

build-dotnet:
	@if command -v dotnet >/dev/null 2>&1; then \
		rm -rf ./agent/lhm/bin; \
		dotnet build -c Release ./agent/lhm/watchdog_lhm.csproj; \
	else \
		echo "dotnet not found"; \
	fi

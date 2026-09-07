# Default OS/ARCH values
OS ?= $(shell go env GOOS)
ARCH ?= $(shell go env GOARCH)
# Skip building the web UI if true
SKIP_WEB ?= false
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

.PHONY: tidy build-agent build-hub build clean lint dev-agent dev-hub generate-locales watchdog-dev-db watchdog-install watchdog-dev-install flow-dev-up flow-dev-down flow-dev-status
.DEFAULT_GOAL := build

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
	npm install --prefix ./internal/site
	npm run --prefix ./internal/site build

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
build-agent: tidy build-dotnet-conditional
	GOOS=$(OS) GOARCH=$(ARCH) go build $(AGENT_GO_TAGS) -o ./build/watchdog-agent_$(OS)_$(ARCH)$(EXE_EXT) -ldflags "-w -s" ./internal/cmd/agent

build-hub: tidy $(if $(filter false,$(SKIP_WEB)),build-web-ui)
	GOOS=$(OS) GOARCH=$(ARCH) go build -o ./build/watchdog_$(OS)_$(ARCH)$(EXE_EXT) -ldflags "-w -s" ./internal/cmd/hub

build: build-agent build-hub

generate-locales:
	@if [ ! -f ./internal/site/src/locales/en/en.ts ]; then \
		echo "Generating locales..."; \
		npm install --prefix ./internal/site && npm run --prefix ./internal/site sync; \
	fi

dev-hub: build-web-ui
	@if command -v entr >/dev/null 2>&1; then \
		find ./internal -type f -name '*.go' | entr -r -s "go run ./internal/cmd/hub serve --http 0.0.0.0:8090 --watchdog-config config/watchdog.dev.yaml"; \
	else \
		go run ./internal/cmd/hub serve --http 0.0.0.0:8090 --watchdog-config config/watchdog.dev.yaml; \
	fi

dev-agent:
	@if command -v entr >/dev/null 2>&1; then \
		find ./internal/cmd/agent/*.go ./agent/*.go | entr -r go run $(AGENT_GO_TAGS) github.com/cloudcache/watchdog/internal/cmd/agent; \
	else \
		go run $(AGENT_GO_TAGS) github.com/cloudcache/watchdog/internal/cmd/agent; \
	fi

watchdog-dev-db:
	./scripts/watchdog-dev-db.sh

watchdog-install:
	go run ./cmd/watchdog-install --config config/watchdog.yaml --init-sql install/init.sql --lock .watchdog.lock

watchdog-dev-install:
	go run ./cmd/watchdog-install --config config/watchdog.dev.yaml --init-sql install/init.sql --lock .watchdog-dev.lock

flow-dev-up:
	docker compose -f deploy/compose.flow-dev.yml up -d

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

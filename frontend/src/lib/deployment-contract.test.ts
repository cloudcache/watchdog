import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

const nginx = readFileSync(new URL("../../../deploy/nginx/watchdog.conf", import.meta.url), "utf8")
const serverConfig = readFileSync(new URL("../../../config/watchdog.yaml", import.meta.url), "utf8")
const viteConfig = readFileSync(new URL("../../vite.config.ts", import.meta.url), "utf8")
const releaseWorkflow = readFileSync(new URL("../../../.github/workflows/release.yml", import.meta.url), "utf8")
const dockerWorkflow = readFileSync(new URL("../../../.github/workflows/docker-images.yml", import.meta.url), "utf8")
const router = readFileSync(new URL("../components/router.tsx", import.meta.url), "utf8")
const main = readFileSync(new URL("../main.tsx", import.meta.url), "utf8")
const login = readFileSync(new URL("../components/login/login.tsx", import.meta.url), "utf8")
const flowAttribution = readFileSync(new URL("../components/routes/flow-attribution.tsx", import.meta.url), "utf8")
const flowCustomerBoundaries = readFileSync(
	new URL("../components/routes/flow-customer-boundaries.tsx", import.meta.url),
	"utf8"
)
const flowWorkerDeployments = readFileSync(
	new URL("../components/routes/flow-worker-deployments.tsx", import.meta.url),
	"utf8"
)
const packageJSON = JSON.parse(readFileSync(new URL("../../package.json", import.meta.url), "utf8")) as {
	dependencies: Record<string, string>
	scripts: Record<string, string>
}

test("nginx serves explicit MPA documents and never applies an application fallback", () => {
	assert.match(nginx, /server 127\.0\.0\.1:8091;/)
	assert.match(nginx, /upstream watchdog_api/)
	assert.match(nginx, /location \^~ \/api\//)
	assert.match(nginx, /location \^~ \/api\/[\s\S]*?proxy_pass http:\/\/watchdog_api;/)
	assert.match(nginx, /location \/assets\/[\s\S]*?try_files \$uri =404;/)
	assert.match(nginx, /location \^~ \/static\/[\s\S]*?try_files \$uri =404;/)
	assert.match(nginx, /include \/var\/www\/watchdog\/watchdog-mpa-routes\.conf;/)
	assert.match(nginx, /location \/ \{[\s\S]*?try_files \$uri =404;/)
	assert.doesNotMatch(nginx, /try_files\s+\$uri[^;]*\/index\.html/)
	const staticRoot = nginx.match(/location \/ \{([\s\S]*?)\n\s*\}/)?.[1] ?? ""
	assert.doesNotMatch(staticRoot, /proxy_pass/)
})

test("nginx never caches the release entry point or runtime API configuration", () => {
	assert.match(nginx, /location = \/index\.html[\s\S]*?Cache-Control "no-store"/)
	assert.match(nginx, /location = \/watchdog-config\.js[\s\S]*?Cache-Control "no-store"/)
	assert.match(nginx, /location = \/watchdog-config\.js[\s\S]*?try_files \/watchdog-config\.js =404;/)
	assert.doesNotMatch(nginx, /alias .*watchdog-config\.js/)
})

test("deployment owns public routing while the API keeps its canonical 8091 listener", () => {
	assert.match(serverConfig, /listen: "127\.0\.0\.1:8091"/)
	assert.doesNotMatch(viteConfig, /\bproxy\s*:/)
	assert.doesNotMatch(viteConfig, /8091/)
})

test("tag releases publish the independent web build exactly once", () => {
	assert.match(releaseWorkflow, /tar -C frontend\/dist -czf "\$WEB_ARCHIVE" \./)
	assert.match(releaseWorkflow, /gh release upload "\$GITHUB_REF_NAME" "\$WEB_ARCHIVE" --clobber/)
	assert.doesNotMatch(dockerWorkflow, /Build site|npm run --prefix \.\/frontend build/)
})

test("page links use browser document navigation and the build emits MPA documents", () => {
	assert.match(router, /window\.location\.assign/)
	assert.match(router, /window\.location\.replace/)
	assert.doesNotMatch(router, /preventDefault/)
	assert.doesNotMatch(router, /\$router\.open/)
	assert.match(packageJSON.scripts.build, /build-mpa\.ts/)
})

test("each MPA document resolves its page once without a client route subscription", () => {
	assert.match(router, /document\.documentElement\.dataset\.watchdogPage/)
	assert.match(main, /const documentPage = resolveDocumentPage\(\)/)
	assert.doesNotMatch(main, /useStore\(\$router\)/)
	assert.doesNotMatch(login, /useStore\(\$router\)/)
	assert.doesNotMatch(router, /createRouter|popstate|pushState|replaceState/)
	assert.equal(packageJSON.dependencies["@nanostores/router"], undefined)
})

test("Flow customer ranges have one editor without a second publication UI or polling loop", () => {
	assert.match(flowAttribution, /<FlowCustomerBoundaries\s*\/>/)
	assert.match(flowAttribution, /<FlowWorkerDeployments\s*\/>/)
	assert.doesNotMatch(flowAttribution, /FlowActivationGuide|FlowEnrichmentPublications/)
	assert.doesNotMatch(flowCustomerBoundaries, /setInterval|\/api\/v1\/flow\/enrichment-publications/)
	assert.doesNotMatch(flowCustomerBoundaries, /Flow processing worker|selectedWorkerID/)
	assert.match(flowWorkerDeployments, /\/api\/v1\/flow\/deployments\/validate/)
	assert.match(flowWorkerDeployments, /\/api\/v1\/flow\/deployments/)
	assert.match(flowWorkerDeployments, /\/api\/v1\/operation-jobs/)
	assert.match(flowWorkerDeployments, /ack_state/)
})

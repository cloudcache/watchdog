// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var (
	frontendImportPattern  = regexp.MustCompile(`(?:from\s*|import\s*\(\s*)["']([^"']+)["']`)
	frontendAPIPathPattern = regexp.MustCompile("[\\\"'`](/api/v1/[^\\\"'`[:space:]]*)[\\\"'`]")
)

// TestReachableFrontendAPIPathsAreRegistered prevents a mounted page from
// drifting to an unregistered Gin URL. Only modules reachable from main.tsx
// are scanned, so source retained for a postponed feature cannot create a
// browser-side 404 until that feature is mounted again.
func TestReachableFrontendAPIPathsAreRegistered(t *testing.T) {
	frontendRoot := filepath.Clean(filepath.Join("..", "..", "frontend", "src"))
	files := reachableFrontendModules(t, frontendRoot, filepath.Join(frontendRoot, "main.tsx"))
	registered := make([]string, 0)
	for _, route := range (&Server{}).newRouter().Routes() {
		registered = append(registered, route.Path)
	}

	missing := make(map[string][]string)
	for _, name := range files {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range frontendAPIPathPattern.FindAllStringSubmatch(string(data), -1) {
			path := normalizeFrontendAPIPath(match[1])
			if path == "" || anyGinPathMatches(path, registered) {
				continue
			}
			relative, _ := filepath.Rel(frontendRoot, name)
			missing[path] = append(missing[path], filepath.ToSlash(relative))
		}
	}
	if len(missing) == 0 {
		return
	}
	paths := make([]string, 0, len(missing))
	for path := range missing {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	var detail strings.Builder
	for _, path := range paths {
		sort.Strings(missing[path])
		detail.WriteString("\n  ")
		detail.WriteString(path)
		detail.WriteString(" <- ")
		detail.WriteString(strings.Join(missing[path], ", "))
	}
	t.Fatalf("reachable frontend API paths are not registered by Gin:%s", detail.String())
}

func reachableFrontendModules(t *testing.T, root, entry string) []string {
	t.Helper()
	queue := []string{entry}
	seen := make(map[string]bool)
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		if seen[name] {
			continue
		}
		seen[name] = true
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read reachable frontend module %s: %v", name, err)
		}
		for _, match := range frontendImportPattern.FindAllStringSubmatch(string(data), -1) {
			if imported := resolveFrontendModule(root, filepath.Dir(name), match[1]); imported != "" && !seen[imported] {
				queue = append(queue, imported)
			}
		}
	}
	files := make([]string, 0, len(seen))
	for name := range seen {
		files = append(files, name)
	}
	sort.Strings(files)
	return files
}

func resolveFrontendModule(root, currentDir, specifier string) string {
	var base string
	switch {
	case strings.HasPrefix(specifier, "@/"):
		base = filepath.Join(root, strings.TrimPrefix(specifier, "@/"))
	case strings.HasPrefix(specifier, "."):
		base = filepath.Join(currentDir, specifier)
	default:
		return ""
	}
	if extension := filepath.Ext(base); extension != "" {
		if extension == ".ts" || extension == ".tsx" {
			if info, err := os.Stat(base); err == nil && !info.IsDir() {
				return filepath.Clean(base)
			}
		}
		return ""
	}
	for _, candidate := range []string{base + ".ts", base + ".tsx", filepath.Join(base, "index.ts"), filepath.Join(base, "index.tsx")} {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return filepath.Clean(candidate)
		}
	}
	return ""
}

func normalizeFrontendAPIPath(path string) string {
	if index := strings.IndexByte(path, '?'); index >= 0 {
		path = path[:index]
	}
	parts := strings.Split(strings.TrimSuffix(path, "/"), "/")
	for index, part := range parts {
		if strings.Contains(part, "${") {
			parts[index] = ":value"
		}
	}
	return strings.Join(parts, "/")
}

func anyGinPathMatches(frontend string, registered []string) bool {
	for _, candidate := range registered {
		if ginPathMatches(frontend, candidate) {
			return true
		}
	}
	return false
}

func ginPathMatches(frontend, registered string) bool {
	want := strings.Split(strings.Trim(frontend, "/"), "/")
	have := strings.Split(strings.Trim(registered, "/"), "/")
	for index := 0; index < len(want) && index < len(have); index++ {
		if strings.HasPrefix(have[index], "*") {
			return true
		}
		if want[index] == have[index] || strings.HasPrefix(want[index], ":") || strings.HasPrefix(have[index], ":") {
			continue
		}
		return false
	}
	return len(want) == len(have) || (len(have) > 0 && strings.HasPrefix(have[len(have)-1], "*"))
}

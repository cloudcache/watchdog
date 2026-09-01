package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	defaultAPIURL   = "https://api.github.com/repos/librenms/librenms/contents/html/images/os?ref=master"
	defaultRawBase  = "https://raw.githubusercontent.com/librenms/librenms/master/html/images/os"
	defaultVendors  = "huawei,cisco,junos,arista,zte"
	defaultOutDir   = "internal/site/public/static/vendor-logos"
	defaultManifest = "internal/site/src/lib/vendor-logos.ts"
)

type githubContent struct {
	Name        string `json:"name"`
	DownloadURL string `json:"download_url"`
	Type        string `json:"type"`
}

type iconAsset struct {
	Vendor string
	File   string
	URL    string
}

func main() {
	root := flag.String("root", "", "optional LibreNMS checkout root; when empty, download from GitHub")
	vendorsFlag := flag.String("vendors", defaultVendors, "comma-separated LibreNMS icon slugs")
	outDir := flag.String("out-dir", defaultOutDir, "frontend public output directory")
	manifest := flag.String("manifest", defaultManifest, "TypeScript manifest output path")
	apiURL := flag.String("api-url", defaultAPIURL, "GitHub contents API URL")
	rawBase := flag.String("raw-base", defaultRawBase, "raw file base URL")
	flag.Parse()

	vendors := parseVendors(*vendorsFlag)
	if len(vendors) == 0 {
		exitf("no vendors requested")
	}
	assets, err := resolveAssets(*root, *apiURL, *rawBase, vendors)
	if err != nil {
		exitf("resolve assets: %v", err)
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		exitf("create output dir: %v", err)
	}
	for _, asset := range assets {
		if err := writeAsset(*root, *outDir, asset); err != nil {
			exitf("write %s: %v", asset.Vendor, err)
		}
	}
	if err := writeManifest(*manifest, assets); err != nil {
		exitf("write manifest: %v", err)
	}
	fmt.Printf("extracted %d LibreNMS vendor icons to %s\n", len(assets), *outDir)
}

func parseVendors(value string) []string {
	seen := map[string]struct{}{}
	var vendors []string
	for _, item := range strings.Split(value, ",") {
		item = strings.ToLower(strings.TrimSpace(item))
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		vendors = append(vendors, item)
	}
	return vendors
}

func resolveAssets(root string, apiURL string, rawBase string, vendors []string) ([]iconAsset, error) {
	if strings.TrimSpace(root) != "" {
		return resolveLocalAssets(root, vendors)
	}
	contents, err := fetchGitHubContents(apiURL)
	if err != nil {
		return nil, err
	}
	files := map[string]githubContent{}
	for _, item := range contents {
		if item.Type == "file" {
			files[strings.ToLower(item.Name)] = item
		}
	}
	var assets []iconAsset
	for _, vendor := range vendors {
		name, ok := chooseIconFile(files, vendor)
		if !ok {
			continue
		}
		item := files[name]
		url := item.DownloadURL
		if url == "" {
			url = strings.TrimRight(rawBase, "/") + "/" + item.Name
		}
		assets = append(assets, iconAsset{Vendor: vendor, File: item.Name, URL: url})
	}
	return assets, nil
}

func resolveLocalAssets(root string, vendors []string) ([]iconAsset, error) {
	dir := filepath.Join(root, "html", "images", "os")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := map[string]githubContent{}
	for _, entry := range entries {
		if !entry.IsDir() {
			files[strings.ToLower(entry.Name())] = githubContent{Name: entry.Name(), Type: "file"}
		}
	}
	var assets []iconAsset
	for _, vendor := range vendors {
		name, ok := chooseIconFile(files, vendor)
		if ok {
			assets = append(assets, iconAsset{Vendor: vendor, File: files[name].Name, URL: filepath.Join(dir, files[name].Name)})
		}
	}
	return assets, nil
}

func chooseIconFile(files map[string]githubContent, vendor string) (string, bool) {
	for _, ext := range []string{".svg", ".png", ".jpg", ".jpeg", ".gif"} {
		name := vendor + ext
		if _, ok := files[name]; ok {
			return name, true
		}
	}
	return "", false
}

func fetchGitHubContents(apiURL string) ([]githubContent, error) {
	client := http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest(http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "watchdog-librenms-icons")
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("GitHub contents request failed: %s", res.Status)
	}
	var contents []githubContent
	if err := json.NewDecoder(res.Body).Decode(&contents); err != nil {
		return nil, err
	}
	return contents, nil
}

func writeAsset(root string, outDir string, asset iconAsset) error {
	outPath := filepath.Join(outDir, asset.File)
	if strings.TrimSpace(root) != "" {
		input, err := os.Open(asset.URL)
		if err != nil {
			return err
		}
		defer input.Close()
		output, err := os.Create(outPath)
		if err != nil {
			return err
		}
		defer output.Close()
		_, err = io.Copy(output, input)
		return err
	}
	client := http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest(http.MethodGet, asset.URL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "watchdog-librenms-icons")
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("download failed: %s", res.Status)
	}
	output, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer output.Close()
	_, err = io.Copy(output, res.Body)
	return err
}

func writeManifest(path string, assets []iconAsset) error {
	sort.Slice(assets, func(i, j int) bool { return assets[i].Vendor < assets[j].Vendor })
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	var b strings.Builder
	b.WriteString("export const vendorLogos: Record<string, string> = {\n")
	for _, asset := range assets {
		fmt.Fprintf(&b, "\t%q: %q,\n", asset.Vendor, "/static/vendor-logos/"+asset.File)
	}
	b.WriteString("}\n\n")
	b.WriteString("export function vendorLogoFor(vendor: string) {\n")
	b.WriteString("\tconst key = vendor.trim().toLowerCase()\n")
	b.WriteString("\tif (vendorLogos[key]) return vendorLogos[key]\n")
	b.WriteString("\tif (key.includes(\"huawei\")) return vendorLogos.huawei\n")
	b.WriteString("\tif (key.includes(\"cisco\")) return vendorLogos.cisco\n")
	b.WriteString("\tif (key.includes(\"juniper\") || key.includes(\"junos\")) return vendorLogos.junos\n")
	b.WriteString("\tif (key.includes(\"arista\")) return vendorLogos.arista\n")
	b.WriteString("\tif (key.includes(\"zte\")) return vendorLogos.zte\n")
	b.WriteString("\treturn \"\"\n")
	b.WriteString("}\n")
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

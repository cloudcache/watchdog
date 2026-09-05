#!/usr/bin/env python3
"""Export the EdgeManager geo library as a watchdog flow-geo-v2 bundle.

The EdgeManager database already holds the merged+corrected interval tables
(geo_base_v4/geo_base_v6 with admin_code, isp_group, asn), the operator
registry (isp_operators) and the display dictionary (geo_dict). This script
does NO parsing or importing of its own: it reads those four tables over a
plain MySQL connection (mysql CLI, TSV) and writes the versioned bundle
described in docs/flow-module-design.md §4.1.

Usage (read-only account is enough):
  python3 scripts/edgemanager-geo-export.py \
      --host 1.2.3.4 --port 3306 --user em_ro --password ... \
      --database edgemanager --out dev/flow-geo

Or against a local restore of a production dump:
  python3 scripts/edgemanager-geo-export.py --database edgemanager_restore

Approved geo_subnets overrides are applied on top of the base intervals
(most-specific wins) unless --skip-subnets is passed, so the bundle is always
the flattened merged+corrected view.

Hong Kong / Macau / Taiwan are normalized to country=CN with GB/T admin codes
(810000/820000/710000) per the frozen flow-geo-v2 contract.
"""

import argparse
import hashlib
import ipaddress
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

HMT_ADMIN = {"HK": "810000", "MO": "820000", "TW": "710000"}


def effective_time(value, now):
    """Return the frozen UTC minute boundary used for event-time selection."""
    if not value:
        return now.replace(second=0, microsecond=0)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SystemExit(f"invalid --effective-from: {exc}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise SystemExit("--effective-from must be an explicit UTC timestamp")
    if parsed.second or parsed.microsecond:
        raise SystemExit("--effective-from must be a UTC minute boundary")
    return parsed.astimezone(timezone.utc)


def build_manifest(version, generated_at, effective_from, files):
    return {
        "schema": "flow-geo-v2",
        "version": version,
        "generated_at": generated_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "effective_from": effective_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "admin_code_system": "GB/T2260-6",
        "unknown_country": "ZZ",
        "files": files,
    }


def bundle_content_hash(v4, v6, operators, dictionary):
    """Hash every semantic input without building one huge serialized value."""
    digest = hashlib.sha256()
    for label, values in (("ipv4", v4), ("ipv6", v6),
                          ("operators", operators), ("geo_dict", dictionary)):
        digest.update(label.encode("ascii") + b"\0")
        for value in values:
            encoded = json.dumps(value, ensure_ascii=False, sort_keys=True,
                                 separators=(",", ":")).encode("utf-8")
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
    return digest.hexdigest()


def mysql_tsv(args, query):
    cmd = ["mysql", "-h", args.host, "-P", str(args.port), "-u", args.user,
           "--default-character-set=utf8mb4", "-N", "--batch", args.database, "-e", query]
    env = dict(os.environ)
    if args.password:
        env["MYSQL_PWD"] = args.password
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        raise SystemExit(f"mysql query failed: {result.stderr.strip()}")
    rows = []
    for line in result.stdout.splitlines():
        rows.append(line.split("\t"))
    return rows


def norm(value):
    return "" if value in ("NULL", None) else value


def normalize_hmt(country, admin_code):
    country = country or "ZZ"
    if country in HMT_ADMIN:
        return "CN", admin_code or HMT_ADMIN[country]
    return country, admin_code


def promote_admin(country, admin_code, subdivision):
    """EdgeManager stores GB/T codes in `subdivision`; the bundle contract puts
    codes in admin_code and keeps subdivision for display names."""
    if country == "CN" and not admin_code and len(subdivision) == 6 and subdivision.isdigit():
        return subdivision, ""
    return admin_code, subdivision


def load_base_v4(args):
    rows = mysql_tsv(args, """
        SELECT ip_start, ip_end, country, COALESCE(admin_code,''), COALESCE(subdivision,''),
               isp_group, asn
        FROM geo_base_v4 ORDER BY ip_start
    """)
    out = []
    for r in rows:
        country, admin = normalize_hmt(norm(r[2]), norm(r[3]))
        admin, subdivision = promote_admin(country, admin, norm(r[4]))
        out.append([int(r[0]), int(r[1]), country, admin, subdivision, "", int(r[5]), int(r[6])])
    return out


def load_base_v6(args):
    rows = mysql_tsv(args, """
        SELECT LOWER(HEX(ip_start)), LOWER(HEX(ip_end)), country, COALESCE(admin_code,''),
               COALESCE(subdivision,''), isp_group, asn
        FROM geo_base_v6 ORDER BY ip_start
    """)
    out = []
    for r in rows:
        country, admin = normalize_hmt(norm(r[2]), norm(r[3]))
        admin, subdivision = promote_admin(country, admin, norm(r[4]))
        out.append([int(r[0], 16), int(r[1], 16), country, admin, subdivision, "", int(r[5]), int(r[6])])
    return out


def load_subnets(args):
    rows = mysql_tsv(args, """
        SELECT cidr, COALESCE(country,''), COALESCE(subdivision,''), COALESCE(asn,0),
               COALESCE(isp_group,''), COALESCE(op,'set')
        FROM geo_subnets WHERE status='approved' ORDER BY id
    """)
    v4, v6 = [], []
    for r in rows:
        try:
            network = ipaddress.ip_network(r[0].strip(), strict=False)
        except ValueError:
            print(f"skip invalid geo_subnets cidr {r[0]!r}", file=sys.stderr)
            continue
        start = int(network.network_address)
        end = int(network.broadcast_address)
        entry = {
            "start": start, "end": end, "prefix": network.prefixlen,
            "country": norm(r[1]), "subdivision": norm(r[2]),
            "asn": int(r[3] or 0), "isp_group": norm(r[4]), "op": norm(r[5]) or "set",
        }
        (v4 if network.version == 4 else v6).append(entry)
    return v4, v6


def apply_overrides(base, overrides, operators_by_name):
    """Splice approved overrides over sorted base rows, most specific last so
    it wins on the covered range. Rows are [start,end,country,admin,subdiv,city,isp,asn]."""
    if not overrides:
        return base
    overrides = sorted(overrides, key=lambda o: o["prefix"])  # broad → specific
    for override in overrides:
        if override["op"] == "del":
            replacement = []
        else:
            country = override["country"]
            country, admin = normalize_hmt(country, "")
            admin, subdivision = promote_admin(country, admin, override["subdivision"])
            isp_id = 0
            if override["isp_group"]:
                if override["isp_group"].isdigit():
                    isp_id = int(override["isp_group"])
                else:
                    isp_id = operators_by_name.get(override["isp_group"], 0)
            replacement = [[override["start"], override["end"], country, admin,
                            subdivision, "", isp_id, override["asn"]]]
        next_base = []
        for row in base:
            if row[1] < override["start"] or row[0] > override["end"]:
                next_base.append(row)
                continue
            if row[0] < override["start"]:
                next_base.append([row[0], override["start"] - 1] + row[2:])
            if row[1] > override["end"]:
                next_base.append([override["end"] + 1, row[1]] + row[2:])
        next_base.extend(replacement)
        next_base.sort(key=lambda r: r[0])
        base = next_base
    return base


def validate(rows, label):
    previous_end = -1
    for row in rows:
        if row[0] > row[1]:
            raise SystemExit(f"{label}: inverted interval {row[:2]}")
        if row[0] <= previous_end:
            raise SystemExit(f"{label}: overlap at {row[:2]} (previous end {previous_end})")
        previous_end = row[1]


def attach_geo_leaf(rows, dictionary):
    """Attach one stable leaf code per flattened range.

    EdgeManager already uses ISO country and GB/T province/city codes as the
    stable geo_dict codes. A CN city/province is therefore the most-specific
    leaf; other ranges use the country. Fully unknown ranges use the explicit
    synthetic ZZ country node.
    """
    by_code = {}
    for entry in dictionary:
        code = entry["code"]
        if code in by_code:
            raise SystemExit(f"geo_dict contains duplicate global code {code!r}")
        by_code[code] = entry
    result = []
    for row in rows:
        country = row[2] or "ZZ"
        row = list(row)
        row[2] = country
        leaf = row[3] if country == "CN" and row[3] else country
        entry = by_code.get(leaf)
        if not entry:
            raise SystemExit(f"range leaf {leaf!r} is missing from geo_dict")
        if not entry.get("enabled", False):
            raise SystemExit(f"range leaf {leaf!r} is disabled in geo_dict")
        result.append(row + [leaf])
    return result


def format_ip(value, version):
    if version == 4:
        return str(ipaddress.IPv4Address(value))
    return str(ipaddress.IPv6Address(value))


def write_csv_zst(path, rows, version):
    csv_lines = ["ip_start,ip_end,country,admin_code,subdivision,city,isp_id,asn,geo_leaf_code"]
    for row in rows:
        fields = [format_ip(row[0], version), format_ip(row[1], version),
                  row[2], row[3], row[4], row[5], str(row[6]), str(row[7]), row[8]]
        csv_lines.append(",".join(field.replace(",", " ") for field in fields))
    data = ("\n".join(csv_lines) + "\n").encode("utf-8")
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(data)
        raw_path = tmp.name
    subprocess.run(["zstd", "-q", "-f", "-o", path, raw_path], check=True)
    os.unlink(raw_path)
    return len(rows), hashlib.sha256(open(path, "rb").read()).hexdigest()


def write_json(path, payload):
    data = json.dumps(payload, ensure_ascii=False, indent=1).encode("utf-8")
    with open(path, "wb") as f:
        f.write(data)
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=3306, type=int)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--database", default="edgemanager")
    parser.add_argument("--out", default="dev/flow-geo")
    parser.add_argument("--effective-from", default="",
                        help="UTC minute boundary, default: current UTC minute")
    parser.add_argument("--skip-subnets", action="store_true",
                        help="base tables already contain the merged corrections")
    args = parser.parse_args()

    operators = mysql_tsv(args, """
        SELECT id, name, COALESCE(short_name,''), category, enabled
        FROM isp_operators ORDER BY id
    """)
    operators_json = [{"id": int(r[0]), "name": r[1], "short_name": norm(r[2]),
                       "category": r[3], "enabled": r[4] in ("1", "true")} for r in operators]
    operators_by_name = {op["name"]: op["id"] for op in operators_json}
    for op in operators_json:
        if op["short_name"]:
            operators_by_name.setdefault(op["short_name"], op["id"])

    dictionary = mysql_tsv(args, """
        SELECT kind, code, name, COALESCE(parent_code,''), enabled
        FROM geo_dict ORDER BY kind, code
    """)
    dict_json = [{"kind": r[0], "code": r[1], "name": r[2], "parent_code": norm(r[3]),
                  "enabled": r[4] in ("1", "true")} for r in dictionary]
    if not any(entry["code"] == "ZZ" for entry in dict_json):
        dict_json.append({"kind": "country", "code": "ZZ", "name": "Unknown",
                          "parent_code": "", "enabled": True})

    v4 = load_base_v4(args)
    v6 = load_base_v6(args)
    if not args.skip_subnets:
        over4, over6 = load_subnets(args)
        v4 = apply_overrides(v4, over4, operators_by_name)
        v6 = apply_overrides(v6, over6, operators_by_name)
    validate(v4, "ipv4")
    validate(v6, "ipv6")
    v4 = attach_geo_leaf(v4, dict_json)
    v6 = attach_geo_leaf(v6, dict_json)
    if not v4:
        raise SystemExit("geo_base_v4 produced no rows; refusing to publish an empty bundle")

    content_hash = bundle_content_hash(v4, v6, operators_json, dict_json)
    generated_at = datetime.now(timezone.utc)
    effective_from = effective_time(args.effective_from, generated_at)
    version = generated_at.strftime("%Y-%m-%dT%H%M%SZ") + "-" + content_hash[:12]
    bundle_dir = os.path.join(args.out, version)
    os.makedirs(bundle_dir, exist_ok=False)

    files = {}
    rows4, sha4 = write_csv_zst(os.path.join(bundle_dir, "ipv4.csv.zst"), v4, 4)
    files["ipv4.csv.zst"] = {"sha256": sha4, "rows": rows4}
    rows6, sha6 = write_csv_zst(os.path.join(bundle_dir, "ipv6.csv.zst"), v6, 6)
    files["ipv6.csv.zst"] = {"sha256": sha6, "rows": rows6}
    files["operators.json"] = {
        "sha256": write_json(os.path.join(bundle_dir, "operators.json"), operators_json),
        "rows": len(operators_json),
    }
    files["geo_dict.json"] = {
        "sha256": write_json(os.path.join(bundle_dir, "geo_dict.json"), dict_json),
        "rows": len(dict_json),
    }
    manifest = build_manifest(version, generated_at, effective_from, files)
    write_json(os.path.join(bundle_dir, "manifest.json"), manifest)

    current = os.path.join(args.out, "current")
    tmp_link = current + ".tmp"
    if os.path.islink(tmp_link) or os.path.exists(tmp_link):
        os.unlink(tmp_link)
    os.symlink(version, tmp_link)
    os.replace(tmp_link, current)
    print(f"published {bundle_dir} (v4={rows4} v6={rows6} operators={len(operators_json)} dict={len(dict_json)})")


if __name__ == "__main__":
    main()

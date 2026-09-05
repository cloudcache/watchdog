import importlib.util
import pathlib
import unittest
from datetime import datetime, timezone


SCRIPT = pathlib.Path(__file__).with_name("edgemanager-geo-export.py")
SPEC = importlib.util.spec_from_file_location("edgemanager_geo_export", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ExportContractTest(unittest.TestCase):
    def test_manifest_exactly_matches_strict_loader_contract(self):
        generated = datetime(2026, 9, 5, 1, 2, 3, tzinfo=timezone.utc)
        effective = MODULE.effective_time("2026-09-05T01:02:00Z", generated)
        files = {
            "ipv4.csv.zst": {"sha256": "a" * 64, "rows": 1},
            "ipv6.csv.zst": {"sha256": "b" * 64, "rows": 0},
            "operators.json": {"sha256": "c" * 64, "rows": 1},
            "geo_dict.json": {"sha256": "d" * 64, "rows": 1},
        }
        manifest = MODULE.build_manifest("geo-1", generated, effective, files)
        self.assertEqual(
            set(manifest),
            {"schema", "version", "generated_at", "effective_from", "admin_code_system", "unknown_country", "files"},
        )
        self.assertEqual(manifest["effective_from"], "2026-09-05T01:02:00Z")
        self.assertEqual(set(manifest["files"]), set(files))
        self.assertEqual(manifest["schema"], "flow-geo-v2")

    def test_default_effective_time_is_current_utc_minute(self):
        now = datetime(2026, 9, 5, 1, 2, 59, 123, tzinfo=timezone.utc)
        self.assertEqual(MODULE.effective_time("", now), datetime(2026, 9, 5, 1, 2, tzinfo=timezone.utc))

    def test_non_minute_or_non_utc_effective_time_is_rejected(self):
        now = datetime.now(timezone.utc)
        for value in ("2026-09-05T01:02:03Z", "2026-09-05T01:02:00+08:00", "not-a-time"):
            with self.subTest(value=value), self.assertRaises(SystemExit):
                MODULE.effective_time(value, now)

    def test_geo_leaf_uses_city_country_and_explicit_unknown_nodes(self):
        dictionary = [
            {"kind": "country", "code": "CN", "name": "China", "enabled": True},
            {"kind": "city", "code": "330100", "name": "Hangzhou", "enabled": True},
            {"kind": "country", "code": "US", "name": "United States", "enabled": True},
            {"kind": "country", "code": "ZZ", "name": "Unknown", "enabled": True},
        ]
        rows = [
            [0, 9, "CN", "330100", "Zhejiang", "Hangzhou", 0, 0],
            [10, 19, "US", "", "California", "", 0, 0],
            [20, 29, "", "", "", "", 0, 0],
        ]
        attached = MODULE.attach_geo_leaf(rows, dictionary)
        self.assertEqual([row[-1] for row in attached], ["330100", "US", "ZZ"])
        self.assertEqual(attached[2][2], "ZZ")

    def test_geo_leaf_rejects_missing_disabled_and_duplicate_codes(self):
        row = [[0, 9, "CN", "330100", "", "", 0, 0]]
        with self.assertRaises(SystemExit):
            MODULE.attach_geo_leaf(row, [{"code": "CN", "enabled": True}])
        with self.assertRaises(SystemExit):
            MODULE.attach_geo_leaf(row, [{"code": "330100", "enabled": False}])
        with self.assertRaises(SystemExit):
            MODULE.attach_geo_leaf(row, [
                {"code": "330100", "enabled": True},
                {"code": "330100", "enabled": True},
            ])

    def test_bundle_content_hash_covers_every_semantic_input(self):
        inputs = (
            [[0, 9, "CN", "330100", "", "", 0, 0, "330100"]],
            [[0, 9, "US", "", "", "", 0, 0, "US"]],
            [{"id": 1, "name": "carrier"}],
            [{"kind": "country", "code": "CN", "name": "China"}],
        )
        baseline = MODULE.bundle_content_hash(*inputs)
        for position in range(len(inputs)):
            changed = [list(values) for values in inputs]
            changed[position] = changed[position] + [{"changed": position}]
            with self.subTest(input=position):
                self.assertNotEqual(MODULE.bundle_content_hash(*changed), baseline)


if __name__ == "__main__":
    unittest.main()

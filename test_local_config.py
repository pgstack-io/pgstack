import tempfile
import unittest
from pathlib import Path

from local_config import load_config, normalize_config

TABLE = {"name": "public.products", "indexColumns": ["description"], "storeColumns": ["id", "description"]}


class LocalConfigTest(unittest.TestCase):
    def test_audit_only_default(self):
        self.assertEqual(normalize_config({}), {"audit": {"enabled": True}, "search": {"enabled": False, "tables": []}})

    def test_search_only_and_both(self):
        for audit in (True, False):
            result = normalize_config({"audit": {"enabled": audit}, "search": {"enabled": True, "tables": [TABLE]}})
            self.assertEqual(result["audit"]["enabled"], audit)
            self.assertEqual(result["search"]["tables"], [TABLE])

    def test_invalid_configuration(self):
        for config in (
            {"audit": {"enabled": False}},
            {"audit": {"enabled": "false"}},
            {"audti": {}},
            {"search": {"enabled": True}},
            {"search": {"tables": [TABLE]}},
            {"search": {"enabled": True, "tables": [TABLE, TABLE]}},
            {"search": {"enabled": True, "tables": [{**TABLE, "name": "public.products,other.table"}]}},
            {"search": {"enabled": True, "tables": [{**TABLE, "indexColumns": []}]}},
            {"search": {"enabled": True, "tables": [{**TABLE, "storeColumns": ["id", "id"]}]}},
        ):
            with self.subTest(config=config), self.assertRaises(ValueError):
                normalize_config(config)

    def test_yaml_loading_and_duplicate_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pgstack.yaml"
            self.assertTrue(load_config(path)["audit"]["enabled"])
            path.write_text("audit:\n  enabled: true\nsearch:\n  enabled: false\n")
            self.assertEqual(load_config(path), normalize_config({}))
            path.write_text("audit: {}\naudit: {}\n")
            with self.assertRaises(ValueError):
                load_config(path)

    def test_unsafe_yaml_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pgstack.yaml"
            path.write_text("!!python/object/apply:os.system ['false']")
            import yaml
            with self.assertRaises(yaml.YAMLError):
                load_config(path)


class EntrypointTest(unittest.TestCase):
    """Exercise startup and cleanup with fake services; no database is touched."""

    def run_entrypoint(self, audit, search, fail_prepare, api_key="test-not-sent", password="test-password"):
        import json
        import os
        import shlex
        import shutil
        import subprocess
        import sys
        import yaml

        package = Path(__file__).resolve().parent
        entrypoint = package / "local-entrypoint.sh"
        if not entrypoint.exists():
            entrypoint = package.parent / "local-entrypoint.sh"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries = root / "bin"
            binaries.mkdir()
            shutil.copy(package / "local_config.py", root / "local_config.py")
            (root / "pgstack.yaml").write_text(yaml.safe_dump({
                "audit": {"enabled": audit},
                "search": {"enabled": search, "tables": [TABLE] if search else []},
            }))
            script = entrypoint.read_text().replace('/var/lib/pgstack', str(root / 'data')).replace('/app/', str(root) + '/').replace('/tmp/pgstack-local-nats.conf', str(root / 'nats.conf'))
            (root / "entrypoint.sh").write_text(script)
            stubs = {
                "python3": f"#!/bin/sh\nexec {shlex.quote(sys.executable)} \"$@\"\n",
                "psql": f"#!{sys.executable}\n" + '''import json, os, sys
keys = ('AUDIT_ENABLED', 'AUDIT_EXCLUDED_TABLES', 'AUDIT_INCLUDED_TABLES', 'SEARCH_SNAPSHOT', 'SEARCH_TABLES', 'SEARCH_TABLES_JSON')
with open(os.environ['CALLS'], 'a') as output:
    output.write(json.dumps({'args': sys.argv[1:], 'env': {key: os.environ.get(key) for key in keys}}) + '\\n')
sys.exit(int(os.environ['FAIL_PREPARE']))
''',
                "nats": "#!/bin/sh\nexit 1\n",
                "nats-server": "#!/bin/sh\nexit 0\n",
                "sleep": "#!/bin/sh\nexit 0\n",
            }
            for name, content in stubs.items():
                path = binaries / name
                path.write_text(content)
                path.chmod(0o755)
            env = {
                **os.environ,
                "PATH": str(binaries) + os.pathsep + os.environ["PATH"],
                "DATABASE_URL": "postgres://unused/test",
                "PGSTACK_PASSWORD": password,
                "OPENAI_API_KEY": api_key,
                "CALLS": str(root / "calls.jsonl"),
                "FAIL_PREPARE": "1" if fail_prepare else "0",
                # The wrapper must clear conflicting inherited service settings.
                "AUDIT_INCLUDED_TABLES": "private.table",
                "AUDIT_EXCLUDED_TABLES": "private.table",
            }
            result = subprocess.run(["bash", str(root / "entrypoint.sh")], env=env, text=True, capture_output=True, timeout=20)
            self.assertNotEqual(result.returncode, 0)
            self.assertTrue((root / "calls.jsonl").exists(), result.stderr)
            return [json.loads(line) for line in (root / "calls.jsonl").read_text().splitlines()]

    def test_feature_modes_and_failed_setup_do_not_cleanup(self):
        import json
        for audit, search in ((True, False), (False, True), (True, True)):
            with self.subTest(audit=audit, search=search):
                calls = self.run_entrypoint(audit, search, True)
                self.assertEqual(len(calls), 1, "failed setup must not drop a replication slot")
                env = calls[0]["env"]
                self.assertEqual(env["AUDIT_ENABLED"], str(audit).lower())
                self.assertEqual(env["AUDIT_EXCLUDED_TABLES"], "" if audit else None)
                self.assertIsNone(env["AUDIT_INCLUDED_TABLES"])
                self.assertEqual(env["SEARCH_SNAPSHOT"], str(search).lower())
                self.assertEqual(json.loads(env["SEARCH_TABLES_JSON"]), [TABLE] if search else [])
                self.assertEqual(env["SEARCH_TABLES"], TABLE["name"] if search else "")

    def test_passwordless_keyword_only_search(self):
        import json
        calls = self.run_entrypoint(False, True, True, api_key="", password="")
        tables = json.loads(calls[0]["env"]["SEARCH_TABLES_JSON"])
        self.assertEqual(tables, [{**TABLE, "keywordOnly": True}])

    def test_failure_after_source_setup_cleans_up(self):
        calls = self.run_entrypoint(True, False, False)
        self.assertEqual(len(calls), 3)
        self.assertTrue(any("pg_drop_replication_slot" in arg for arg in calls[-1]["args"]))

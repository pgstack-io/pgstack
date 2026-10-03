#!/usr/bin/env python3
"""Validate local YAML and emit the internal configuration as JSON."""
import json
from pathlib import Path
import sys

import yaml


class UniqueKeyLoader(yaml.SafeLoader):
    pass


def unique_mapping(loader, node):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node)
        if not isinstance(key, str) or key in result:
            raise ValueError("YAML keys must be unique strings")
        result[key] = loader.construct_object(value_node)
    return result


UniqueKeyLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, unique_mapping)


def mapping(value, allowed, location):
    if not isinstance(value, dict) or set(value) - allowed:
        raise ValueError(f"{location} must be a mapping with keys: {', '.join(sorted(allowed))}")
    return value


def enabled(section, default, location):
    value = section.get("enabled", default)
    if type(value) is not bool:
        raise ValueError(f"{location}.enabled must be true or false")
    return value


def columns(value, location):
    if not isinstance(value, list) or not value or any(not isinstance(v, str) or not v.strip() for v in value):
        raise ValueError(f"{location} must be a nonempty list of column names")
    if len(set(value)) != len(value):
        raise ValueError(f"{location} contains duplicate columns")
    return value


def normalize_config(raw):
    root = mapping(raw, {"audit", "search"}, "configuration")
    audit = mapping(root.get("audit", {}), {"enabled"}, "audit")
    search = mapping(root.get("search", {}), {"enabled", "tables"}, "search")
    audit_enabled = enabled(audit, True, "audit")
    search_enabled = enabled(search, False, "search")
    tables = search.get("tables", [])
    if not isinstance(tables, list):
        raise ValueError("search.tables must be a list")
    if search_enabled != bool(tables):
        raise ValueError("Search requires both enabled: true and at least one table; omit tables when disabled")
    if not audit_enabled and not search_enabled:
        raise ValueError("Enable Audit, Search, or both")
    names = set()
    for table in tables:
        mapping(table, {"name", "indexColumns", "storeColumns"}, "search table")
        name = table.get("name")
        if not isinstance(name, str) or len(name.split(".")) != 2 or any(not part or any(c.isspace() or c == ',' for c in part) for part in name.split(".")):
            raise ValueError("Table names must be schema.table without whitespace or commas")
        if name in names:
            raise ValueError(f"Duplicate Search table: {name}")
        names.add(name)
        columns(table.get("indexColumns"), f"{name}.indexColumns")
        columns(table.get("storeColumns"), f"{name}.storeColumns")
    return {"audit": {"enabled": audit_enabled}, "search": {"enabled": search_enabled, "tables": tables}}


def load_config(path):
    if not path.exists():
        return normalize_config({})
    if not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise ValueError("Configuration must be a regular YAML file smaller than 1 MiB")
    with path.open() as source:
        raw = yaml.load(source, Loader=UniqueKeyLoader)
    return normalize_config(raw if raw is not None else {})


if __name__ == "__main__":
    try:
        print(json.dumps(load_config(Path(sys.argv[1] if len(sys.argv) > 1 else "/app/pgstack.yaml"))))
    except (ValueError, OSError, yaml.YAMLError) as error:
        sys.exit(f"Invalid PgStack configuration: {error}")

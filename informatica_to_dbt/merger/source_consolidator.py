"""Consolidate dbt source definitions across multiple mapping outputs.

When multiple Informatica mappings read from the same source tables, each
mapping's LLM output will include its own ``_sources.yml``.  This module
merges them into deduplicated source files — one per source database/schema
combination — so that the unified dbt project has no duplicate source
definitions.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Tuple

import yaml

from informatica_to_dbt.generator.response_parser import GeneratedFile

logger = logging.getLogger("informatica_dbt")


def consolidate_sources(source_files: List[GeneratedFile]) -> List[GeneratedFile]:
    """Merge all ``_sources.yml`` files into deduplicated source definitions.

    Each mapping may produce a ``_sources.yml`` that defines sources.  If two
    mappings reference the same Snowflake source (same ``name`` + ``schema`` +
    ``database``), we merge their ``tables`` lists so each table appears only
    once.

    The output is one ``_sources.yml`` per mapping directory (preserving the
    mapping-level grouping), but with tables deduplicated across all mappings
    that share the same logical source.

    Args:
        source_files: List of ``GeneratedFile`` objects whose paths end in
            ``_sources.yml`` or ``sources.yml``.

    Returns:
        A list of consolidated ``GeneratedFile`` objects.
    """
    if not source_files:
        return []

    # Parse all source YAML files
    parsed_sources: List[Tuple[str, dict]] = []  # (original_path, parsed_yaml)
    for gf in source_files:
        try:
            doc = yaml.safe_load(gf.content)
            if isinstance(doc, dict) and "sources" in doc:
                parsed_sources.append((gf.path, doc))
            else:
                logger.warning(
                    "Source file '%s' has no 'sources' key — passing through",
                    gf.path,
                )
                parsed_sources.append((gf.path, doc or {}))
        except yaml.YAMLError as exc:
            logger.warning(
                "Failed to parse source YAML '%s': %s — passing through raw",
                gf.path, exc,
            )
            # Pass through unparseable files as-is
            parsed_sources.append((gf.path, None))

    # Build a global source registry: source_key → merged source dict
    # source_key = (source_name, database, schema)
    global_sources: Dict[Tuple[str, str, str], dict] = {}
    # Track which tables belong to each source
    global_tables: Dict[Tuple[str, str, str], Dict[str, dict]] = {}

    for path, doc in parsed_sources:
        if doc is None or "sources" not in doc:
            continue
        for source in doc.get("sources", []):
            if not isinstance(source, dict) or "name" not in source:
                continue
            src_name = source["name"]
            src_db = source.get("database", "")
            src_schema = source.get("schema", "")
            key = (src_name, src_db, src_schema)

            if key not in global_sources:
                # First time seeing this source — capture base definition
                base = {k: v for k, v in source.items() if k != "tables"}
                global_sources[key] = base
                global_tables[key] = {}

            # Merge tables
            for table in source.get("tables", []):
                if not isinstance(table, dict) or "name" not in table:
                    continue
                tbl_name = table["name"]
                if tbl_name not in global_tables[key]:
                    global_tables[key][tbl_name] = table
                else:
                    # Merge columns if both define them
                    existing = global_tables[key][tbl_name]
                    _merge_table_columns(existing, table)

    # Rebuild source files consolidated by layer directory.
    # Group by layer dir (e.g. models/staging) so all mappings that share
    # the same source layer end up in one _sources.yml file.
    layer_sources: Dict[str, Dict[Tuple[str, str, str], set]] = {}

    for path, doc in parsed_sources:
        if doc is None or "sources" not in doc:
            continue
        layer_dir = _extract_layer_dir(path)
        if layer_dir not in layer_sources:
            layer_sources[layer_dir] = {}

        for source in doc.get("sources", []):
            if not isinstance(source, dict) or "name" not in source:
                continue
            key = (
                source["name"],
                source.get("database", ""),
                source.get("schema", ""),
            )
            if key not in layer_sources[layer_dir]:
                layer_sources[layer_dir][key] = set()
            for table in source.get("tables", []):
                if isinstance(table, dict) and "name" in table:
                    layer_sources[layer_dir][key].add(table["name"])

    # Generate consolidated source files — one per layer directory
    consolidated: List[GeneratedFile] = []
    for layer_dir, src_keys in layer_sources.items():
        sources_list = []
        for key in sorted(src_keys.keys()):
            source_def = dict(global_sources[key])
            table_names = src_keys[key]
            tables = [
                global_tables[key][tname]
                for tname in sorted(table_names)
                if tname in global_tables[key]
            ]
            if tables:
                source_def["tables"] = tables
            sources_list.append(source_def)

        doc = {"version": 2, "sources": sources_list}
        content = yaml.dump(doc, default_flow_style=False, sort_keys=False)
        out_path = f"{layer_dir}/_sources.yml"
        consolidated.append(GeneratedFile(path=out_path, content=content))

    logger.info(
        "Consolidated %d source file(s) into %d deduplicated file(s) "
        "(%d unique sources, %d total tables)",
        len(source_files),
        len(consolidated),
        len(global_sources),
        sum(len(t) for t in global_tables.values()),
    )
    return consolidated


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_layer_dir(path: str) -> str:
    """Extract the layer directory prefix from a source file path.

    E.g. ``models/staging/_sources.yml`` → ``models/staging``
         ``staging/_sources.yml``        → ``staging``
    """
    parts = path.replace("\\", "/").split("/")
    # Remove the filename to get the directory
    dir_parts = parts[:-1] if len(parts) > 1 else parts
    return "/".join(dir_parts) if dir_parts else "models/staging"


def _merge_table_columns(existing: dict, new: dict) -> None:
    """Merge column definitions from ``new`` into ``existing`` table dict."""
    if "columns" not in new:
        return
    if "columns" not in existing:
        existing["columns"] = new["columns"]
        return

    existing_cols = {
        c["name"]: c for c in existing["columns"]
        if isinstance(c, dict) and "name" in c
    }
    for col in new.get("columns", []):
        if isinstance(col, dict) and "name" in col:
            if col["name"] not in existing_cols:
                existing["columns"].append(col)

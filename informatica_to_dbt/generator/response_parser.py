"""Parse structured LLM responses into individual file artifacts.

The LLM is instructed to return output as a sequence of file blocks
delimited by ``-- FILE: <relative_path>`` headers.  This module splits
the raw response text into a list of :class:`GeneratedFile` objects.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import List

from informatica_to_dbt.exceptions import ResponseParseError

logger = logging.getLogger("informatica_dbt")

# Matches lines like:  -- FILE: models/staging/stg_foo.sql
_FILE_HEADER_RE = re.compile(
    r"^--\s*FILE:\s*(.+)$",
    re.MULTILINE,
)


@dataclass
class GeneratedFile:
    """A single file extracted from the LLM response."""

    path: str
    content: str

    @property
    def is_sql(self) -> bool:
        return self.path.endswith(".sql")

    @property
    def is_yaml(self) -> bool:
        return self.path.endswith((".yml", ".yaml"))

    @property
    def layer(self) -> str:
        """Infer the dbt layer from the file path."""
        if "staging" in self.path:
            return "staging"
        if "intermediate" in self.path:
            return "intermediate"
        if "marts" in self.path:
            return "marts"
        if "snapshots" in self.path:
            return "snapshots"
        if "macros" in self.path:
            return "macros"
        return "other"


def parse_response(raw: str) -> List[GeneratedFile]:
    """Split an LLM response into individual file artifacts.

    Steps:
        1. Strip any markdown code fences that the LLM may have added.
        2. Locate ``-- FILE:`` markers.
        3. Extract content between consecutive markers (or to end of text).

    Raises :class:`ParseError` if no valid file blocks are found.
    """
    # Strip outer markdown code fences (```sql ... ```, ```yaml ... ```, etc.)
    cleaned = _strip_code_fences(raw)

    matches = list(_FILE_HEADER_RE.finditer(cleaned))
    if not matches:
        raise ResponseParseError(
            "No '-- FILE:' markers found in LLM response. "
            "The model may have returned unstructured output."
        )

    files: List[GeneratedFile] = []
    for i, match in enumerate(matches):
        file_path = match.group(1).strip()
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(cleaned)

        content = cleaned[start:end].strip()

        if not content:
            logger.warning("Empty content for file '%s' — skipping", file_path)
            continue

        # Normalise path separators
        file_path = file_path.replace("\\", "/")

        files.append(GeneratedFile(path=file_path, content=content))

    if not files:
        raise ResponseParseError("All extracted file blocks were empty.")

    logger.info(
        "Parsed %d file(s) from LLM response: %s",
        len(files),
        [f.path for f in files],
    )
    return files


def merge_chunk_files(
    chunks_files: List[List[GeneratedFile]],
) -> List[GeneratedFile]:
    """Merge generated files from multiple chunks.

    When the same file path appears in multiple chunks (common for
    schema.yml or sources.yml), the contents are concatenated.
    For SQL files, later chunks overwrite earlier ones (the last
    chunk producing that path wins).
    """
    by_path: dict[str, GeneratedFile] = {}

    for chunk_files in chunks_files:
        for gf in chunk_files:
            if gf.path in by_path:
                existing = by_path[gf.path]
                if gf.is_yaml:
                    # YAML schema files — merge by appending models/sources
                    merged = _merge_yaml_content(existing.content, gf.content)
                    by_path[gf.path] = GeneratedFile(path=gf.path, content=merged)
                else:
                    # SQL files — last chunk wins
                    by_path[gf.path] = gf
            else:
                by_path[gf.path] = gf

    merged_list = list(by_path.values())
    logger.info("Merged into %d unique file(s)", len(merged_list))
    return merged_list


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_CODE_FENCE_RE = re.compile(r"```(?:\w+)?\n(.*?)```", re.DOTALL)


def _strip_code_fences(text: str) -> str:
    """Remove markdown code fences, keeping inner content."""
    # If the entire response is wrapped in a single fence, unwrap it
    stripped = text.strip()
    if stripped.startswith("```") and stripped.endswith("```"):
        inner = _CODE_FENCE_RE.findall(stripped)
        if inner:
            return "\n\n".join(inner)
    # Otherwise, remove fences inline
    result = _CODE_FENCE_RE.sub(r"\1", text)
    return result


def _merge_yaml_content(existing: str, new: str) -> str:
    """Schema-aware YAML merge for dbt schema/sources files.

    Merges models, sources, and their nested columns/tables lists by name,
    avoiding duplicates. Falls back to naive concatenation if PyYAML is
    unavailable or parsing fails.
    """
    try:
        import yaml
    except ImportError:
        logger.warning("PyYAML not available — falling back to naive YAML merge")
        return existing.rstrip() + "\n\n" + new.lstrip()

    try:
        doc_a = yaml.safe_load(existing)
        doc_b = yaml.safe_load(new)
    except Exception:
        logger.warning("YAML parse failed during merge — falling back to naive merge")
        return existing.rstrip() + "\n\n" + new.lstrip()

    if not isinstance(doc_a, dict) or not isinstance(doc_b, dict):
        return existing.rstrip() + "\n\n" + new.lstrip()

    merged = dict(doc_a)

    # Preserve version from either side
    if "version" not in merged and "version" in doc_b:
        merged["version"] = doc_b["version"]

    # Merge sources lists by source name
    if "sources" in doc_b:
        merged["sources"] = _merge_named_list(
            merged.get("sources", []),
            doc_b["sources"],
            sub_key="tables",
        )

    # Merge models lists by model name
    if "models" in doc_b:
        merged["models"] = _merge_named_list(
            merged.get("models", []),
            doc_b["models"],
            sub_key="columns",
        )

    # Merge any other top-level keys (seeds, snapshots, etc.)
    for key in doc_b:
        if key not in ("version", "sources", "models"):
            if key not in merged:
                merged[key] = doc_b[key]
            elif isinstance(merged[key], list) and isinstance(doc_b[key], list):
                merged[key] = _merge_named_list(merged[key], doc_b[key])
            # else: keep existing value

    return yaml.dump(merged, default_flow_style=False, sort_keys=False)


def _merge_named_list(
    list_a: list,
    list_b: list,
    sub_key: str | None = None,
) -> list:
    """Merge two lists of dicts by 'name' key, deduplicating.

    If ``sub_key`` is provided (e.g. 'tables', 'columns'), the sub-lists
    within matching items are also merged by name.
    """
    if not isinstance(list_a, list):
        list_a = []
    if not isinstance(list_b, list):
        list_b = []

    # Index existing items by name
    by_name: dict[str, dict] = {}
    result = []
    for item in list_a:
        if isinstance(item, dict) and "name" in item:
            by_name[item["name"]] = item
            result.append(item)
        else:
            result.append(item)

    for item in list_b:
        if not isinstance(item, dict) or "name" not in item:
            result.append(item)
            continue

        name = item["name"]
        if name in by_name:
            existing = by_name[name]
            # Merge sub-lists if sub_key is set
            if sub_key and sub_key in item:
                existing[sub_key] = _merge_named_list(
                    existing.get(sub_key, []),
                    item[sub_key],
                )
            # Merge any other keys from new item
            for k, v in item.items():
                if k not in existing and k != sub_key:
                    existing[k] = v
        else:
            by_name[name] = item
            result.append(item)

    return result

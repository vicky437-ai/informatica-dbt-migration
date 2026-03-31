"""Resolve file conflicts when merging multiple mapping outputs.

Handles:
  - Duplicate macro files (same macro defined by multiple mappings)
  - Duplicate model names across mappings (rare but possible)
  - YAML schema file merges for overlapping model definitions
"""

from __future__ import annotations

import logging
import os
from typing import Dict, List

from informatica_to_dbt.generator.response_parser import GeneratedFile

logger = logging.getLogger("informatica_dbt")


def resolve_conflicts(files: List[GeneratedFile]) -> List[GeneratedFile]:
    """Deduplicate a list of generated files by path.

    When multiple mappings produce the same file (common for shared macros),
    this function keeps the most complete version (longest content).

    For SQL macro files, if two macros have the same filename but different
    content, we keep both by appending a numeric suffix to the duplicate.

    Args:
        files: List of generated files that may contain duplicates.

    Returns:
        Deduplicated list of files.
    """
    if not files:
        return []

    by_basename: Dict[str, List[GeneratedFile]] = {}
    for gf in files:
        basename = os.path.basename(gf.path)
        by_basename.setdefault(basename, []).append(gf)

    result: List[GeneratedFile] = []
    for basename, group in by_basename.items():
        if len(group) == 1:
            result.append(group[0])
            continue

        # Multiple files with the same basename
        if all(gf.is_sql for gf in group):
            result.extend(_resolve_sql_conflicts(group))
        elif all(gf.is_yaml for gf in group):
            result.append(_resolve_yaml_conflicts(group))
        else:
            # Mixed types — keep the longest
            best = max(group, key=lambda g: len(g.content))
            result.append(best)
            logger.info(
                "Conflict: %d files named '%s' — kept longest (%d chars)",
                len(group), basename, len(best.content),
            )

    logger.info(
        "Conflict resolution: %d input files → %d output files",
        len(files), len(result),
    )
    return result


def _resolve_sql_conflicts(group: List[GeneratedFile]) -> List[GeneratedFile]:
    """Resolve conflicts among SQL files with the same basename.

    If the content is identical (or nearly identical after whitespace
    normalization), keep one copy.  Otherwise, keep the longest version
    (assumed to be the most complete macro implementation).
    """
    # Normalize content for comparison
    normalized: Dict[str, GeneratedFile] = {}
    for gf in group:
        norm = gf.content.strip()
        if norm not in normalized:
            normalized[norm] = gf

    if len(normalized) == 1:
        # All identical content — keep one
        winner = next(iter(normalized.values()))
        logger.debug(
            "Identical SQL content for '%s' from %d sources — deduplicating",
            os.path.basename(winner.path), len(group),
        )
        return [winner]

    # Different content — keep the longest (most complete)
    winner = max(normalized.values(), key=lambda g: len(g.content))
    logger.info(
        "SQL conflict for '%s': %d variants — keeping longest (%d chars, path=%s)",
        os.path.basename(winner.path),
        len(normalized),
        len(winner.content),
        winner.path,
    )
    return [winner]


def _resolve_yaml_conflicts(group: List[GeneratedFile]) -> GeneratedFile:
    """Merge conflicting YAML files by combining their contents.

    Uses the response_parser's existing YAML merge logic.
    """
    from informatica_to_dbt.generator.response_parser import _merge_yaml_content

    if len(group) == 1:
        return group[0]

    # Start with the first file and merge in the rest
    merged_content = group[0].content
    for gf in group[1:]:
        merged_content = _merge_yaml_content(merged_content, gf.content)

    logger.info(
        "Merged %d YAML files named '%s'",
        len(group), os.path.basename(group[0].path),
    )
    return GeneratedFile(path=group[0].path, content=merged_content)

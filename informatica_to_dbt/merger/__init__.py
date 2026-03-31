"""Merger module — consolidates per-mapping outputs into a unified dbt project."""

from informatica_to_dbt.merger.project_merger import ProjectMerger, MergeResult
from informatica_to_dbt.merger.source_consolidator import consolidate_sources
from informatica_to_dbt.merger.conflict_resolver import resolve_conflicts

__all__ = [
    "ProjectMerger",
    "MergeResult",
    "consolidate_sources",
    "resolve_conflicts",
]

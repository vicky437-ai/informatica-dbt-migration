"""Multi-workflow coordination: shared folder resolution and cross-folder linking.

Informatica exports often contain multiple FOLDERs in a single XML file:
shared folders with reusable sources/targets/transformations, and a main
folder with the mapping that references them via SHORTCUT elements.

This module resolves shortcuts to their underlying definitions so that
downstream modules (chunker, prompt builder) have a complete, self-contained
view of each mapping.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from informatica_to_dbt.xml_parser.models import (
    Folder,
    Mapping,
    Repository,
    Shortcut,
    Source,
    Target,
    Transformation,
)

logger = logging.getLogger("informatica_dbt")


# ---------------------------------------------------------------------------
# Lookup index: folder → object name → object
# ---------------------------------------------------------------------------

@dataclass
class ObjectIndex:
    """Fast lookup for objects across all folders in a repository."""

    sources: Dict[Tuple[str, str], Source] = field(default_factory=dict)
    targets: Dict[Tuple[str, str], Target] = field(default_factory=dict)
    transformations: Dict[Tuple[str, str], Transformation] = field(default_factory=dict)

    @classmethod
    def from_repository(cls, repo: Repository) -> ObjectIndex:
        """Build an index keyed by ``(folder_name, object_name)``."""
        idx = cls()
        for folder in repo.folders:
            for src in folder.sources:
                idx.sources[(folder.name, src.name)] = src
            for tgt in folder.targets:
                idx.targets[(folder.name, tgt.name)] = tgt
            for tx in folder.transformations:
                idx.transformations[(folder.name, tx.name)] = tx
        return idx


def resolve_shortcut(
    shortcut: Shortcut,
    index: ObjectIndex,
) -> Optional[Source | Target | Transformation]:
    """Resolve a SHORTCUT to the underlying object it references.

    Returns ``None`` if the referenced object cannot be found in the index.
    """
    folder = shortcut.folder_name
    obj_name = shortcut.shortcut_to

    ref_type = shortcut.reference_type.lower()

    if "source" in ref_type:
        obj = index.sources.get((folder, obj_name))
        if obj:
            return obj
    if "target" in ref_type:
        obj = index.targets.get((folder, obj_name))
        if obj:
            return obj
    if "transformation" in ref_type or "expression" in ref_type:
        obj = index.transformations.get((folder, obj_name))
        if obj:
            return obj

    # Fallback: search all categories by name across all folders
    for (_, name), src in index.sources.items():
        if name == obj_name:
            return src
    for (_, name), tgt in index.targets.items():
        if name == obj_name:
            return tgt
    for (_, name), tx in index.transformations.items():
        if name == obj_name:
            return tx

    logger.warning(
        "Cannot resolve shortcut '%s' → '%s' in folder '%s' (type=%s)",
        shortcut.name, obj_name, folder, shortcut.reference_type,
    )
    return None


# ---------------------------------------------------------------------------
# Enriched mapping: mapping + resolved shared objects
# ---------------------------------------------------------------------------

@dataclass
class EnrichedMapping:
    """A mapping together with all resolved shared objects it depends on.

    This is the primary input to the chunker and prompt builder — it
    provides a complete, self-contained view of the data flow.
    """

    mapping: Mapping
    folder_name: str
    shared_sources: List[Source] = field(default_factory=list)
    shared_targets: List[Target] = field(default_factory=list)
    shared_transformations: List[Transformation] = field(default_factory=list)
    unresolved_shortcuts: List[Shortcut] = field(default_factory=list)

    @property
    def all_sources(self) -> List[Source]:
        return self.mapping.sources + self.shared_sources

    @property
    def all_targets(self) -> List[Target]:
        return self.mapping.targets + self.shared_targets

    @property
    def all_transformations(self) -> List[Transformation]:
        return self.mapping.transformations + self.shared_transformations


def enrich_mapping(
    mapping: Mapping,
    folder: Folder,
    index: ObjectIndex,
) -> EnrichedMapping:
    """Resolve all shortcuts referenced by a mapping's parent folder.

    Walks the folder-level shortcuts and the mapping-level shortcuts,
    resolves each via the :class:`ObjectIndex`, and attaches the resolved
    objects to an :class:`EnrichedMapping`.
    """
    enriched = EnrichedMapping(mapping=mapping, folder_name=folder.name)

    all_shortcuts = list(folder.shortcuts) + list(mapping.shortcuts)

    for sc in all_shortcuts:
        obj = resolve_shortcut(sc, index)
        if obj is None:
            enriched.unresolved_shortcuts.append(sc)
        elif isinstance(obj, Source):
            if obj.name not in {s.name for s in enriched.shared_sources}:
                enriched.shared_sources.append(obj)
        elif isinstance(obj, Target):
            if obj.name not in {t.name for t in enriched.shared_targets}:
                enriched.shared_targets.append(obj)
        elif isinstance(obj, Transformation):
            if obj.name not in {t.name for t in enriched.shared_transformations}:
                enriched.shared_transformations.append(obj)

    logger.info(
        "Enriched mapping '%s': +%d sources, +%d targets, +%d transformations, "
        "%d unresolved",
        mapping.name,
        len(enriched.shared_sources),
        len(enriched.shared_targets),
        len(enriched.shared_transformations),
        len(enriched.unresolved_shortcuts),
    )
    return enriched


# ---------------------------------------------------------------------------
# Top-level: process an entire repository
# ---------------------------------------------------------------------------

def enrich_repository(repo: Repository) -> List[EnrichedMapping]:
    """Resolve all mappings in a repository, returning enriched mappings.

    Skips shared folders (they contain definitions, not executable mappings).
    """
    index = ObjectIndex.from_repository(repo)
    results: List[EnrichedMapping] = []

    for folder in repo.folders:
        if folder.shared:
            logger.debug("Skipping shared folder '%s'", folder.name)
            continue
        for mapping in folder.mappings:
            enriched = enrich_mapping(mapping, folder, index)
            results.append(enriched)

    logger.info(
        "Repository '%s': enriched %d mappings from %d non-shared folders",
        repo.name,
        len(results),
        sum(1 for f in repo.folders if not f.shared),
    )
    return results

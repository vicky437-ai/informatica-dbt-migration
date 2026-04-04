"""Dependency-aware chunking that respects transformation chain boundaries.

Serialises an :class:`EnrichedMapping` into structured text for the LLM.
If the serialised payload exceeds ``chunk_token_limit``, it is split along
weakly-connected-component (chain) boundaries so that every chunk is a
self-contained slice of the data-flow graph.

Shared context (sources, targets, reusable transformations resolved from
shortcuts) is prepended to **every** chunk so the LLM always has the full
schema context.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from informatica_to_dbt.analyzer.complexity import ComplexityResult
from informatica_to_dbt.analyzer.multi_workflow import EnrichedMapping
from informatica_to_dbt.analyzer.transformation_registry import (
    ContextPriority,
    get_context_priority,
    get_critical_attributes_for_type,
    get_conversion_hint,
)
from informatica_to_dbt.config import Config
from informatica_to_dbt.exceptions import ChunkingError
from informatica_to_dbt.utils import estimate_token_count
from informatica_to_dbt.xml_parser.dependency_graph import (
    build_instance_graph,
    get_transformation_chains,
)
from informatica_to_dbt.xml_parser.models import (
    Connector,
    Instance,
    Mapping,
    Source,
    Target,
    Transformation,
    TransformField,
)

logger = logging.getLogger("informatica_dbt")


# ---------------------------------------------------------------------------
# Chunk dataclass
# ---------------------------------------------------------------------------

@dataclass
class MappingChunk:
    """One chunk of a mapping ready for LLM submission."""

    mapping_name: str
    chunk_index: int
    total_chunks: int
    content: str                       # serialised text payload
    token_estimate: int
    chain_names: List[str] = field(default_factory=list)  # instance chains in this chunk
    is_complete: bool = True           # False when mapping was split


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _serialize_field(f: TransformField, indent: str = "    ") -> str:
    parts = [f"{indent}{f.name} ({f.datatype}, prec={f.precision}, scale={f.scale})"]
    if f.port_type:
        parts.append(f" [{f.port_type}]")
    if f.expression:
        parts.append(f"\n{indent}  EXPR: {f.expression}")
    if f.default_value:
        parts.append(f"\n{indent}  DEFAULT: {f.default_value}")
    if f.group:
        parts.append(f"\n{indent}  GROUP: {f.group}")
    return "".join(parts)


def _serialize_source(src: Source) -> str:
    lines = [f"SOURCE: {src.name} (db_type={src.database_type}, owner={src.owner_name})"]
    for f in src.fields:
        lines.append(_serialize_field(f))
    return "\n".join(lines)


def _serialize_target(tgt: Target) -> str:
    lines = [f"TARGET: {tgt.name} (db_type={tgt.database_type}, owner={tgt.owner_name})"]
    for f in tgt.fields:
        lines.append(_serialize_field(f))
    return "\n".join(lines)


def _serialize_transformation(
    tx: Transformation,
    compact: bool = False,
) -> str:
    """Serialize a transformation with all its attributes and fields.

    When *compact* is ``True`` (used for LOW-priority transformations under
    space pressure), only critical attributes and output ports with
    expressions are included.
    """
    priority = get_context_priority(tx.type)
    hint = get_conversion_hint(tx.type)
    critical_attrs = set(get_critical_attributes_for_type(tx.type))

    lines = [f"TRANSFORMATION: {tx.name} (type={tx.type}, reusable={tx.is_reusable})"]

    if hint:
        lines.append(f"  HINT: {hint}")
    if tx.groups:
        lines.append(f"  GROUPS: {', '.join(tx.groups)}")

    # --- Attributes ---
    # Serialize ALL TABLEATTRIBUTE entries.  Critical attributes are always
    # shown; others are included unless running in compact mode.
    for attr_name, attr_value in tx.attributes.items():
        if not attr_value:
            continue
        if compact and attr_name not in critical_attrs:
            continue
        lines.append(f"  {attr_name}: {attr_value}")

    # --- Fields / Ports ---
    if compact:
        # Only output ports with expressions (the transformation logic)
        expr_fields = [
            f for f in tx.fields
            if f.expression or f.default_value
        ]
        if expr_fields:
            lines.append(f"  PORTS ({len(tx.fields)} total, {len(expr_fields)} with expressions):")
            for f in expr_fields:
                lines.append(_serialize_field(f))
    else:
        for f in tx.fields:
            lines.append(_serialize_field(f))

    return "\n".join(lines)


def _serialize_instance(inst: Instance) -> str:
    parts = [f"INSTANCE: {inst.name} -> {inst.transformation_name} ({inst.transformation_type})"]
    if inst.associated_source:
        parts.append(f" [assoc_source={inst.associated_source}]")
    return "".join(parts)


def _serialize_connector(conn: Connector) -> str:
    return (
        f"  {conn.from_instance}.{conn.from_field} "
        f"-> {conn.to_instance}.{conn.to_field}"
    )


# ---------------------------------------------------------------------------
# Build the shared context block (prepended to every chunk)
# ---------------------------------------------------------------------------

def _build_shared_context(enriched: EnrichedMapping) -> str:
    """Serialize shared/resolved objects that give the LLM schema context."""
    sections: List[str] = []

    if enriched.shared_sources or enriched.mapping.sources:
        sections.append("=== SOURCES ===")
        for src in enriched.all_sources:
            sections.append(_serialize_source(src))

    if enriched.shared_targets or enriched.mapping.targets:
        sections.append("\n=== TARGETS ===")
        for tgt in enriched.all_targets:
            sections.append(_serialize_target(tgt))

    if enriched.shared_transformations:
        sections.append("\n=== SHARED / REUSABLE TRANSFORMATIONS ===")
        for tx in enriched.shared_transformations:
            sections.append(_serialize_transformation(tx))

    return "\n".join(sections)


# ---------------------------------------------------------------------------
# Build a chunk body for a subset of chains
# ---------------------------------------------------------------------------

def _collect_chain_objects(
    mapping: Mapping,
    chain_instance_names: List[str],
    connector_scope: Optional[List[str]] = None,
) -> tuple[List[Transformation], List[Instance], List[Connector]]:
    """Collect transformations, instances, and connectors relevant to a set of chains.

    Args:
        mapping: The mapping to collect from.
        chain_instance_names: Instance names for this chunk.
        connector_scope: If provided, include connectors where at least one
            end is in ``chain_instance_names`` AND the other end is in
            ``connector_scope``.  This preserves inter-sub-chain connectors
            when a parent chain is split into sub-groups (H5 fix).
            When ``None``, only connectors where **both** ends are in
            ``chain_instance_names`` are included (original behaviour).
    """
    chain_set = set(chain_instance_names)
    scope_set = set(connector_scope) if connector_scope else None

    # Map instance name → transformation name
    inst_tx_names = set()
    relevant_instances: List[Instance] = []
    for inst in mapping.instances:
        if inst.name in chain_set:
            relevant_instances.append(inst)
            inst_tx_names.add(inst.transformation_name)

    # Transformations that belong to these instances
    relevant_transforms: List[Transformation] = []
    for tx in mapping.transformations:
        if tx.name in inst_tx_names:
            relevant_transforms.append(tx)

    # Connectors
    relevant_connectors: List[Connector] = []
    for conn in mapping.connectors:
        if scope_set is not None:
            # Sub-chain mode: include connector if at least one end is in
            # this sub-group AND the other end is in the parent chain scope.
            from_in = conn.from_instance in chain_set
            to_in = conn.to_instance in chain_set
            from_scope = conn.from_instance in scope_set
            to_scope = conn.to_instance in scope_set
            if (from_in or to_in) and (from_scope and to_scope):
                relevant_connectors.append(conn)
        else:
            # Normal mode: both ends must be in the chain set
            if conn.from_instance in chain_set and conn.to_instance in chain_set:
                relevant_connectors.append(conn)

    return relevant_transforms, relevant_instances, relevant_connectors


def _serialize_chain_body(
    mapping: Mapping,
    chain_instance_names: List[str],
    compact: bool = False,
    connector_scope: Optional[List[str]] = None,
) -> str:
    """Serialize the transformations, instances, and connectors for a set of chains.

    When *compact* is ``True``, LOW-priority transformations are serialized
    in compact mode (fewer ports, only critical attributes).

    When *connector_scope* is provided, it is passed to
    :func:`_collect_chain_objects` to preserve boundary connectors during
    sub-chain splitting (H5 fix).
    """
    transforms, instances, connectors = _collect_chain_objects(
        mapping, chain_instance_names, connector_scope=connector_scope,
    )

    lines: List[str] = []

    if transforms:
        lines.append("=== TRANSFORMATIONS ===")
        for tx in transforms:
            # In compact mode, only LOW-priority types get compacted;
            # CRITICAL/HIGH types always get full detail.
            use_compact = compact and get_context_priority(tx.type) == ContextPriority.LOW
            lines.append(_serialize_transformation(tx, compact=use_compact))

    if instances:
        lines.append("\n=== INSTANCES ===")
        for inst in instances:
            lines.append(_serialize_instance(inst))

    if connectors:
        lines.append("\n=== DATA FLOW (CONNECTORS) ===")
        # Deduplicate at instance level for readability, show field detail
        for conn in connectors:
            lines.append(_serialize_connector(conn))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def chunk_mapping(
    enriched: EnrichedMapping,
    complexity: ComplexityResult,
    config: Config,
) -> List[MappingChunk]:
    """Split an enriched mapping into token-limited chunks.

    If the entire serialised mapping fits within ``config.chunk_token_limit``,
    a single chunk is returned.  Otherwise the mapping is split along chain
    boundaries (weakly connected components of the instance graph) with the
    shared context duplicated in each chunk.

    Raises :class:`ChunkingError` if a single chain exceeds the token limit.
    """
    limit = config.chunk_token_limit
    mapping = enriched.mapping

    # Shared context is prepended to every chunk
    shared_ctx = _build_shared_context(enriched)
    shared_tokens = estimate_token_count(shared_ctx)

    header = (
        f"MAPPING: {mapping.name} "
        f"(strategy={complexity.strategy.value}, "
        f"score={complexity.score}/100)\n"
    )

    # Try single-chunk first (most common for simple/staged mappings)
    g = build_instance_graph(mapping)
    chains = get_transformation_chains(g)
    all_instance_names = [name for chain in chains for name in chain]
    full_body = _serialize_chain_body(mapping, all_instance_names)
    full_content = f"{header}\n{shared_ctx}\n\n{full_body}"
    full_tokens = estimate_token_count(full_content)

    if full_tokens <= limit:
        logger.info(
            "Mapping '%s' fits in single chunk (%d tokens <= %d limit)",
            mapping.name, full_tokens, limit,
        )
        return [
            MappingChunk(
                mapping_name=mapping.name,
                chunk_index=0,
                total_chunks=1,
                content=full_content,
                token_estimate=full_tokens,
                chain_names=[f"chain_{i}" for i in range(len(chains))],
                is_complete=True,
            )
        ]

    # Multi-chunk: group chains to stay under the limit
    logger.info(
        "Mapping '%s' exceeds limit (%d > %d tokens), splitting by chains",
        mapping.name, full_tokens, limit,
    )

    available = limit - shared_tokens - estimate_token_count(header) - 200  # margin
    if available < 1000:
        raise ChunkingError(
            f"Shared context alone ({shared_tokens} tokens) nearly exceeds "
            f"chunk limit ({limit}). Cannot chunk mapping '{mapping.name}'."
        )

    # Pre-serialize each chain; split oversized chains into sub-chains
    # by walking the topological order and cutting when token budget fills.
    chain_bodies: List[str] = []
    chain_tokens: List[int] = []
    chain_labels_raw: List[str] = []  # human-readable label per segment

    for ci, chain in enumerate(chains):
        body = _serialize_chain_body(mapping, chain)
        tokens = estimate_token_count(body)

        if tokens <= available:
            chain_bodies.append(body)
            chain_tokens.append(tokens)
            chain_labels_raw.append(f"chain_{ci}")
        else:
            # Split this chain into sub-groups along topological order
            logger.info(
                "Chain %d (%d instances, %d tokens) exceeds available %d — "
                "splitting into sub-chains",
                ci, len(chain), tokens, available,
            )
            sub_group: List[str] = []
            sub_tokens = 0
            part = 0
            for inst_name in chain:
                inst_body = _serialize_chain_body(mapping, [inst_name], connector_scope=chain)
                inst_tokens = estimate_token_count(inst_body)
                if sub_group and sub_tokens + inst_tokens > available:
                    chain_bodies.append(_serialize_chain_body(mapping, sub_group, connector_scope=chain))
                    chain_tokens.append(sub_tokens)
                    chain_labels_raw.append(f"chain_{ci}_part{part}")
                    part += 1
                    sub_group = [inst_name]
                    sub_tokens = inst_tokens
                else:
                    sub_group.append(inst_name)
                    sub_tokens += inst_tokens
            if sub_group:
                chain_bodies.append(_serialize_chain_body(mapping, sub_group, connector_scope=chain))
                chain_tokens.append(estimate_token_count(chain_bodies[-1]))
                chain_labels_raw.append(f"chain_{ci}_part{part}")

    # Greedy bin-packing: group segments that fit together
    groups: List[List[int]] = []
    current_group: List[int] = []
    current_size = 0

    for i, tokens in enumerate(chain_tokens):
        if current_size + tokens > available and current_group:
            groups.append(current_group)
            current_group = [i]
            current_size = tokens
        else:
            current_group.append(i)
            current_size += tokens

    if current_group:
        groups.append(current_group)

    # Build chunks
    total = len(groups)
    chunks: List[MappingChunk] = []
    for idx, group in enumerate(groups):
        body_parts = [chain_bodies[i] for i in group]
        chunk_header = (
            f"{header}"
            f"[Chunk {idx + 1}/{total}]\n"
        )
        content = f"{chunk_header}\n{shared_ctx}\n\n" + "\n\n".join(body_parts)
        tokens = estimate_token_count(content)
        chain_labels = [chain_labels_raw[i] for i in group]
        chunks.append(
            MappingChunk(
                mapping_name=mapping.name,
                chunk_index=idx,
                total_chunks=total,
                content=content,
                token_estimate=tokens,
                chain_names=chain_labels,
                is_complete=False,
            )
        )

    logger.info(
        "Split mapping '%s' into %d chunks: %s tokens each",
        mapping.name,
        total,
        [c.token_estimate for c in chunks],
    )
    return chunks

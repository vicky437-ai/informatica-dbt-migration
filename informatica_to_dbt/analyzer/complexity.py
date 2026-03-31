"""Workflow complexity analysis and model strategy selection.

Scores an Informatica mapping on a 0–100 scale based on structural
features (graph depth, branching, transformation types, expression
density, lookup count, etc.) and maps the score to a ``ModelStrategy``
that drives downstream chunking and prompt construction.

Uses the :mod:`transformation_registry` for data-driven type weights
so that *any* Informatica transformation type contributes appropriately
to the score without hard-coding.
"""

from __future__ import annotations

import enum
import logging
from dataclasses import dataclass, field
from typing import Dict, List

import networkx as nx

from informatica_to_dbt.xml_parser.models import Mapping, Transformation
from informatica_to_dbt.xml_parser.dependency_graph import (
    build_instance_graph,
    get_longest_path_length,
    get_source_instances,
    get_target_instances,
    get_transformation_chains,
    detect_cycles,
)
from informatica_to_dbt.analyzer.transformation_registry import (
    DbtPattern,
    complexity_weight as registry_complexity_weight,
    get_transformation_type,
)

logger = logging.getLogger("informatica_dbt")


# ---------------------------------------------------------------------------
# Strategy enum
# ---------------------------------------------------------------------------

class ModelStrategy(enum.Enum):
    """How many dbt model layers to generate for a mapping."""

    DIRECT = "direct"          # 0–30: single staging model
    STAGED = "staged"          # 31–55: staging + intermediate
    LAYERED = "layered"        # 56–80: staging + intermediate + mart
    COMPLEX = "complex"        # 81–100: multi-chain, needs manual review hints


# ---------------------------------------------------------------------------
# Scoring weights (each sub-score normalised to 0–100, then weighted)
# ---------------------------------------------------------------------------

_WEIGHTS: Dict[str, float] = {
    "depth":            0.12,  # longest path in DAG
    "breadth":          0.08,  # number of source instances
    "transformations":  0.12,  # total transformation count
    "expressions":      0.12,  # fields with non-trivial EXPRESSION
    "lookups":          0.12,  # Lookup Procedure count
    "routing":          0.08,  # Router / Union / Filter presence
    "chains":           0.08,  # number of independent chains
    "scd":              0.08,  # SCD Type-2 pattern detection
    "joiners":          0.06,  # Joiner / multi-source joins
    "type_weight":      0.08,  # aggregated registry-driven type complexity
    "update_strategy":  0.06,  # Update Strategy / incremental patterns
}

# Expression patterns that indicate SCD Type-2 logic
_SCD2_KEYWORDS = {
    "EFFECTIVE_DATE", "EXPIRY_DATE", "TYPE_2", "SCD", "CURRENT_FLAG",
    "EFF_DT", "EXP_DT", "IS_CURRENT", "VERSION_NUM", "VALID_FROM",
    "VALID_TO", "ACTIVE_FLAG", "ROW_VERSION",
}


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class ComplexityResult:
    """Output of the complexity analysis for a single mapping."""

    mapping_name: str
    score: int                          # 0–100
    strategy: ModelStrategy
    sub_scores: Dict[str, int] = field(default_factory=dict)
    transformation_type_counts: Dict[str, int] = field(default_factory=dict)
    num_sources: int = 0
    num_targets: int = 0
    num_instances: int = 0
    num_connectors: int = 0
    num_chains: int = 0
    longest_path: int = 0
    has_cycles: bool = False
    has_scd2: bool = False

    @property
    def summary(self) -> str:
        parts = [
            f"Mapping: {self.mapping_name}",
            f"Score: {self.score}/100 → {self.strategy.value}",
            f"Sources={self.num_sources} Targets={self.num_targets} "
            f"Instances={self.num_instances} Connectors={self.num_connectors}",
            f"Chains={self.num_chains} LongestPath={self.longest_path} "
            f"Cycles={self.has_cycles} SCD2={self.has_scd2}",
            f"Types: {self.transformation_type_counts}",
        ]
        return " | ".join(parts)


# ---------------------------------------------------------------------------
# Scorers
# ---------------------------------------------------------------------------

def _score_depth(longest_path: int) -> int:
    """Score based on longest path (0-3 → low, 4-8 → mid, 9+ → high)."""
    if longest_path <= 2:
        return 10
    if longest_path <= 5:
        return 40
    if longest_path <= 8:
        return 65
    return min(100, 50 + longest_path * 5)


def _score_breadth(num_sources: int) -> int:
    if num_sources <= 1:
        return 10
    if num_sources <= 3:
        return 40
    return min(100, 30 + num_sources * 15)


def _score_transformations(count: int) -> int:
    if count <= 3:
        return 10
    if count <= 8:
        return 35
    if count <= 20:
        return 60
    return min(100, 40 + count * 2)


def _score_expressions(total_expr_fields: int) -> int:
    if total_expr_fields <= 5:
        return 10
    if total_expr_fields <= 30:
        return 35
    if total_expr_fields <= 100:
        return 60
    return min(100, 50 + total_expr_fields // 5)


def _score_lookups(lookup_count: int) -> int:
    if lookup_count == 0:
        return 0
    if lookup_count <= 2:
        return 30
    if lookup_count <= 6:
        return 55
    return min(100, 40 + lookup_count * 7)


def _score_routing(has_router: bool, has_union: bool, has_filter: bool) -> int:
    score = 0
    if has_router:
        score += 45
    if has_union:
        score += 35
    if has_filter:
        score += 20
    return min(score, 100)


def _score_chains(num_chains: int) -> int:
    if num_chains <= 1:
        return 10
    if num_chains <= 3:
        return 40
    return min(100, 30 + num_chains * 15)


def _score_joiners(joiner_count: int) -> int:
    """Score based on Joiner transformation count."""
    if joiner_count == 0:
        return 0
    if joiner_count == 1:
        return 30
    if joiner_count <= 3:
        return 55
    return min(100, 40 + joiner_count * 12)


def _score_type_weight(transformations: List[Transformation]) -> int:
    """Aggregated complexity from registry weights for all transformation types.

    Each transformation type contributes its registry ``complexity_weight``
    (0.0–1.0).  The total is normalised to a 0–100 score.
    """
    if not transformations:
        return 0
    total_weight = sum(
        registry_complexity_weight(tx.type) for tx in transformations
    )
    # Normalise: a total weight of 5.0+ maps to 100
    return min(100, int(total_weight * 20))


def _score_update_strategy(
    update_count: int,
    has_dual_target: bool,
) -> int:
    """Score based on Update Strategy transformations and dual-target patterns."""
    score = 0
    if update_count == 1:
        score = 30
    elif update_count >= 2:
        score = 55
    if has_dual_target:
        score += 25
    return min(score, 100)


def _detect_scd2(transformations: List[Transformation]) -> bool:
    """Heuristic: detect SCD Type-2 pattern from update strategies and field names."""
    has_update_strategy = False
    has_scd_fields = False
    for tx in transformations:
        if tx.type == "Update Strategy":
            has_update_strategy = True
        for fld in tx.fields:
            name_upper = fld.name.upper()
            if any(kw in name_upper for kw in _SCD2_KEYWORDS):
                has_scd_fields = True
            expr_upper = (fld.expression or "").upper()
            if "DD_UPDATE" in expr_upper or "DD_INSERT" in expr_upper:
                has_update_strategy = True
    return has_update_strategy and has_scd_fields


def _detect_dual_target(mapping: Mapping) -> bool:
    """Detect dual-target pattern (Insert + Update targets pointing to same table)."""
    target_names = [t.name for t in mapping.targets]
    if len(target_names) < 2:
        return False
    # Check for common patterns: Insert_X / Update_X or X_Insert / X_Update
    name_set = {n.upper() for n in target_names}
    for name in name_set:
        for prefix in ("INSERT_", "UPDATE_", "INS_", "UPD_"):
            base = name.replace(prefix, "", 1)
            counterpart = ("UPDATE_" if "INSERT" in prefix else "INSERT_") + base
            if counterpart in name_set:
                return True
    return False


def analyze_complexity(mapping: Mapping) -> ComplexityResult:
    """Score a mapping's complexity and determine its model strategy.

    Returns a :class:`ComplexityResult` with the overall score, per-dimension
    sub-scores, and the recommended :class:`ModelStrategy`.
    """
    g = build_instance_graph(mapping)

    # Gather metrics
    sources = get_source_instances(g)
    targets = get_target_instances(g)
    chains = get_transformation_chains(g)
    longest_path = get_longest_path_length(g)
    cycles = detect_cycles(g)

    type_counts: Dict[str, int] = {}
    total_expr_fields = 0
    lookup_count = 0
    joiner_count = 0
    update_strategy_count = 0
    has_router = False
    has_union = False
    has_filter = False

    for tx in mapping.transformations:
        type_counts[tx.type] = type_counts.get(tx.type, 0) + 1
        total_expr_fields += sum(1 for f in tx.fields if f.expression)

        # Use registry to classify — but also keep explicit counters for
        # the scoring dimensions that need them
        ttype = get_transformation_type(tx.type)

        if tx.type == "Lookup Procedure":
            lookup_count += 1
        if tx.type == "Joiner":
            joiner_count += 1
        if tx.type == "Update Strategy":
            update_strategy_count += 1

        # Routing detection: anything the registry says is CASE_WHEN, FILTER, or UNION
        if ttype.dbt_pattern == DbtPattern.CASE_WHEN:
            has_router = True
        elif ttype.dbt_pattern == DbtPattern.UNION:
            has_union = True
        elif ttype.dbt_pattern == DbtPattern.FILTER:
            has_filter = True

    has_scd2 = _detect_scd2(mapping.transformations)
    has_dual_target = _detect_dual_target(mapping)

    # Compute sub-scores
    sub = {
        "depth": _score_depth(longest_path),
        "breadth": _score_breadth(len(sources)),
        "transformations": _score_transformations(len(mapping.transformations)),
        "expressions": _score_expressions(total_expr_fields),
        "lookups": _score_lookups(lookup_count),
        "routing": _score_routing(has_router, has_union, has_filter),
        "chains": _score_chains(len(chains)),
        "scd": 80 if has_scd2 else 0,
        "joiners": _score_joiners(joiner_count),
        "type_weight": _score_type_weight(mapping.transformations),
        "update_strategy": _score_update_strategy(update_strategy_count, has_dual_target),
    }

    # Weighted total
    total = sum(sub[k] * _WEIGHTS[k] for k in _WEIGHTS)
    score = min(100, max(0, int(round(total))))

    # Map score → strategy
    if score <= 30:
        strategy = ModelStrategy.DIRECT
    elif score <= 55:
        strategy = ModelStrategy.STAGED
    elif score <= 80:
        strategy = ModelStrategy.LAYERED
    else:
        strategy = ModelStrategy.COMPLEX

    result = ComplexityResult(
        mapping_name=mapping.name,
        score=score,
        strategy=strategy,
        sub_scores=sub,
        transformation_type_counts=type_counts,
        num_sources=len(sources),
        num_targets=len(targets),
        num_instances=len(mapping.instances),
        num_connectors=len(mapping.connectors),
        num_chains=len(chains),
        longest_path=longest_path,
        has_cycles=len(cycles) > 0,
        has_scd2=has_scd2,
    )

    logger.info("Complexity: %s", result.summary)
    return result

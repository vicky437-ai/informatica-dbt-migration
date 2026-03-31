"""Workflow analysis: complexity scoring and multi-workflow coordination."""

from informatica_to_dbt.analyzer.complexity import (
    ComplexityResult,
    ModelStrategy,
    analyze_complexity,
)
from informatica_to_dbt.analyzer.multi_workflow import (
    EnrichedMapping,
    ObjectIndex,
    enrich_mapping,
    enrich_repository,
    resolve_shortcut,
)
from informatica_to_dbt.analyzer.transformation_registry import (
    ContextPriority,
    DbtPattern,
    TransformationType,
    get_transformation_type,
    get_all_types,
    get_critical_attributes_for_type,
    get_conversion_hint,
    get_function_equivalent,
    get_all_function_names,
    get_context_priority,
    get_dbt_pattern,
    complexity_weight,
    build_function_regex_pattern,
    FUNCTION_MAP,
)

__all__ = [
    "ComplexityResult",
    "ModelStrategy",
    "analyze_complexity",
    "EnrichedMapping",
    "ObjectIndex",
    "enrich_mapping",
    "enrich_repository",
    "resolve_shortcut",
    "ContextPriority",
    "DbtPattern",
    "TransformationType",
    "get_transformation_type",
    "get_all_types",
    "get_critical_attributes_for_type",
    "get_conversion_hint",
    "get_function_equivalent",
    "get_all_function_names",
    "get_context_priority",
    "get_dbt_pattern",
    "complexity_weight",
    "build_function_regex_pattern",
    "FUNCTION_MAP",
]

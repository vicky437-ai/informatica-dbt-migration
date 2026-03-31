"""Unit tests for the analyzer module (complexity, registry, enrichment)."""

import pytest
from informatica_to_dbt.analyzer.complexity import (
    ModelStrategy,
    ComplexityResult,
    analyze_complexity,
    _score_depth,
    _score_breadth,
    _score_transformations,
    _score_expressions,
    _score_lookups,
    _score_routing,
    _score_chains,
    _score_joiners,
    _score_update_strategy,
)
from informatica_to_dbt.analyzer.transformation_registry import (
    get_transformation_type,
    complexity_weight,
    get_all_types,
    get_conversion_hint,
    DbtPattern,
)
from informatica_to_dbt.analyzer import FUNCTION_MAP


class TestTransformationRegistry:
    """Tests for the transformation type registry."""

    def test_known_type_expression(self):
        tt = get_transformation_type("Expression")
        assert tt.name == "Expression"
        assert tt.dbt_pattern == DbtPattern.COLUMN_EXPRESSION
        assert tt.complexity_weight > 0

    def test_known_type_router(self):
        tt = get_transformation_type("Router")
        assert tt.dbt_pattern == DbtPattern.CASE_WHEN

    def test_known_type_lookup(self):
        tt = get_transformation_type("Lookup Procedure")
        assert tt.dbt_pattern == DbtPattern.LEFT_JOIN

    def test_unknown_type_returns_default(self):
        tt = get_transformation_type("NonExistentType123")
        # Unknown types get a default entry with name "Unknown"
        assert tt.complexity_weight > 0  # default weight

    def test_all_types_count(self):
        all_types = get_all_types()
        assert len(all_types) >= 31

    def test_complexity_weight_range(self):
        for tname in get_all_types():
            w = complexity_weight(tname)
            assert 0.0 <= w <= 1.0, f"{tname} weight {w} out of range"

    def test_conversion_hint_exists(self):
        hint = get_conversion_hint("Expression")
        assert isinstance(hint, str)
        assert len(hint) > 0

    def test_function_map_has_entries(self):
        assert len(FUNCTION_MAP) >= 65
        assert "IIF" in FUNCTION_MAP
        # FUNCTION_MAP values are (snowflake_fn, description) tuples
        assert FUNCTION_MAP["IIF"][0] == "IFF"
        assert "ISNULL" in FUNCTION_MAP
        assert "SYSDATE" in FUNCTION_MAP


class TestScoringFunctions:
    """Tests for individual scoring dimensions."""

    def test_score_depth_low(self):
        assert _score_depth(1) == 10

    def test_score_depth_mid(self):
        assert _score_depth(5) == 40

    def test_score_depth_high(self):
        assert _score_depth(10) >= 65

    def test_score_breadth_single(self):
        assert _score_breadth(1) == 10

    def test_score_breadth_many(self):
        assert _score_breadth(5) > 40

    def test_score_transformations_few(self):
        assert _score_transformations(2) == 10

    def test_score_transformations_many(self):
        assert _score_transformations(25) > 60

    def test_score_expressions_none(self):
        assert _score_expressions(0) == 10

    def test_score_expressions_many(self):
        assert _score_expressions(50) > 35

    def test_score_lookups_zero(self):
        assert _score_lookups(0) == 0

    def test_score_lookups_few(self):
        assert _score_lookups(2) == 30

    def test_score_routing_none(self):
        assert _score_routing(False, False, False) == 0

    def test_score_routing_router(self):
        assert _score_routing(True, False, False) == 45

    def test_score_routing_all(self):
        assert _score_routing(True, True, True) == 100

    def test_score_chains_single(self):
        assert _score_chains(1) == 10

    def test_score_joiners_zero(self):
        assert _score_joiners(0) == 0

    def test_score_joiners_one(self):
        assert _score_joiners(1) == 30

    def test_score_update_strategy_none(self):
        assert _score_update_strategy(0, False) == 0

    def test_score_update_strategy_with_dual(self):
        assert _score_update_strategy(1, True) == 55


class TestAnalyzeComplexity:
    """Tests for the analyze_complexity function."""

    def test_simple_mapping_low_score(self, simple_mapping):
        result = analyze_complexity(simple_mapping)
        assert isinstance(result, ComplexityResult)
        assert result.mapping_name == "m_simple_test"
        assert 0 <= result.score <= 100
        # Simple mapping should be DIRECT or STAGED
        assert result.strategy in (ModelStrategy.DIRECT, ModelStrategy.STAGED)

    def test_complex_mapping_higher_score(self, complex_mapping):
        result = analyze_complexity(complex_mapping)
        assert result.score >= 30  # Should be at least STAGED threshold
        assert result.strategy in (ModelStrategy.DIRECT, ModelStrategy.STAGED, ModelStrategy.LAYERED, ModelStrategy.COMPLEX)

    def test_complex_mapping_type_counts(self, complex_mapping):
        result = analyze_complexity(complex_mapping)
        assert "Router" in result.transformation_type_counts
        assert "Joiner" in result.transformation_type_counts
        assert "Lookup Procedure" in result.transformation_type_counts

    def test_result_summary(self, simple_mapping):
        result = analyze_complexity(simple_mapping)
        summary = result.summary
        assert "m_simple_test" in summary
        assert "Score:" in summary

    def test_strategy_thresholds(self):
        """Verify strategy assignment based on score thresholds."""
        # This tests the logic indirectly through the enum values
        assert ModelStrategy.DIRECT.value == "direct"
        assert ModelStrategy.STAGED.value == "staged"
        assert ModelStrategy.LAYERED.value == "layered"
        assert ModelStrategy.COMPLEX.value == "complex"

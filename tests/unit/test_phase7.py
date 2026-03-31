"""Tests for Phase 7 — Notebook Integration & End-to-End Readiness.

Covers:
- PipelineResult class and properties
- format_report() output
- retry_failed_mappings() orchestration
- _extract_error_category() helper
- Enriched ConversionSummary and _build_summary()
"""

import pytest
from unittest.mock import MagicMock, patch

from informatica_to_dbt.exceptions import ErrorCategory
from informatica_to_dbt.metrics import MappingMetrics, RepositoryMetrics
from informatica_to_dbt.notebook_entry import PipelineResult, format_report
from informatica_to_dbt.orchestrator import (
    MappingResult,
    _build_summary,
    _extract_error_category,
    retry_failed_mappings,
)
from informatica_to_dbt.persistence.snowflake_io import ConversionSummary


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_result(name, status="success", error_message=None, files=None,
                 metrics=None, heal_attempts=0, chunks=1, elapsed=1.0):
    """Create a MappingResult for testing."""
    return MappingResult(
        mapping_name=name,
        workflow_name="test_wf",
        files=files or [],
        status=status,
        error_message=error_message,
        heal_attempts=heal_attempts,
        chunks_processed=chunks,
        elapsed_seconds=elapsed,
        metrics=metrics,
    )


def _make_metrics(name="test", status="success", llm_calls=2,
                  llm_total_seconds=3.5, heal_attempts=0,
                  quality_score=85, transformation_count=5,
                  estimated_input_tokens=1000):
    """Create a MappingMetrics for testing."""
    return MappingMetrics(
        mapping_name=name,
        status=status,
        llm_calls=llm_calls,
        llm_total_seconds=llm_total_seconds,
        heal_attempts=heal_attempts,
        quality_score=quality_score,
        transformation_count=transformation_count,
        estimated_input_tokens=estimated_input_tokens,
    )


# ===========================================================================
# PipelineResult
# ===========================================================================

class TestPipelineResult:
    """Test the PipelineResult container class."""

    def test_constructor(self):
        results = [_make_result("m1"), _make_result("m2", status="failed")]
        metrics = RepositoryMetrics()
        pr = PipelineResult(results, metrics)
        assert pr.results is results
        assert pr.metrics is metrics

    def test_success_count(self):
        results = [
            _make_result("m1", status="success"),
            _make_result("m2", status="success"),
            _make_result("m3", status="failed"),
        ]
        pr = PipelineResult(results, RepositoryMetrics())
        assert pr.success_count == 2

    def test_failed_count(self):
        results = [
            _make_result("m1", status="failed"),
            _make_result("m2", status="success"),
            _make_result("m3", status="failed"),
        ]
        pr = PipelineResult(results, RepositoryMetrics())
        assert pr.failed_count == 2

    def test_partial_count(self):
        results = [
            _make_result("m1", status="partial"),
            _make_result("m2", status="success"),
        ]
        pr = PipelineResult(results, RepositoryMetrics())
        assert pr.partial_count == 1

    def test_repr(self):
        results = [
            _make_result("m1", status="success"),
            _make_result("m2", status="partial"),
            _make_result("m3", status="failed"),
        ]
        pr = PipelineResult(results, RepositoryMetrics())
        r = repr(pr)
        assert "mappings=3" in r
        assert "success=1" in r
        assert "partial=1" in r
        assert "failed=1" in r

    def test_retryable_failures_identifies_llm_errors(self):
        results = [
            _make_result("m1", status="success"),
            _make_result("m2", status="failed", error_message="[llm] Rate limit"),
            _make_result("m3", status="failed", error_message="[input] Bad XML"),
        ]
        pr = PipelineResult(results, RepositoryMetrics())
        retryable = pr.retryable_failures
        assert len(retryable) == 1
        assert retryable[0].mapping_name == "m2"

    def test_retryable_failures_identifies_persistence_errors(self):
        results = [
            _make_result("m1", status="failed",
                         error_message="[persistence] Stage write failed"),
        ]
        pr = PipelineResult(results, RepositoryMetrics())
        retryable = pr.retryable_failures
        assert len(retryable) == 1

    def test_retryable_failures_excludes_non_retryable(self):
        """input, parse, validation are not retryable."""
        results = [
            _make_result("m1", status="failed", error_message="[input] Bad XML"),
            _make_result("m2", status="failed", error_message="[parse] Syntax error"),
            _make_result("m3", status="failed", error_message="[validation] Schema issue"),
        ]
        pr = PipelineResult(results, RepositoryMetrics())
        assert pr.retryable_failures == []

    def test_retryable_failures_skips_success(self):
        results = [
            _make_result("m1", status="success", error_message="[llm] Transient"),
        ]
        pr = PipelineResult(results, RepositoryMetrics())
        assert pr.retryable_failures == []

    def test_retryable_failures_skips_no_error_message(self):
        results = [_make_result("m1", status="failed", error_message=None)]
        pr = PipelineResult(results, RepositoryMetrics())
        assert pr.retryable_failures == []

    def test_empty_results(self):
        pr = PipelineResult([], RepositoryMetrics())
        assert pr.success_count == 0
        assert pr.failed_count == 0
        assert pr.partial_count == 0
        assert pr.retryable_failures == []

    def test_report_property_returns_string(self):
        results = [_make_result("m1")]
        metrics = RepositoryMetrics()
        pr = PipelineResult(results, metrics)
        report = pr.report
        assert isinstance(report, str)
        assert "CONVERSION REPORT" in report


# ===========================================================================
# format_report
# ===========================================================================

class TestFormatReport:
    """Test the format_report function."""

    def test_includes_mapping_names(self):
        results = [
            _make_result("m_orders"),
            _make_result("m_customers", status="failed",
                         error_message="[llm] Timeout"),
        ]
        metrics = RepositoryMetrics()
        report = format_report(results, metrics)
        assert "m_orders" in report
        assert "m_customers" in report

    def test_includes_status_tags(self):
        results = [
            _make_result("m1", status="success"),
            _make_result("m2", status="failed"),
            _make_result("m3", status="partial"),
        ]
        report = format_report(results, RepositoryMetrics())
        assert "SUCCESS" in report
        assert "FAILED" in report
        assert "PARTIAL" in report

    def test_includes_summary_section(self):
        mm = _make_metrics("m1", quality_score=80, llm_calls=3)
        metrics = RepositoryMetrics()
        metrics.add(mm)
        results = [_make_result("m1")]
        report = format_report(results, metrics)
        assert "SUMMARY" in report
        assert "Total mappings:" in report
        assert "LLM calls:" in report
        assert "Avg quality:" in report

    def test_includes_error_message_for_failed(self):
        results = [
            _make_result("m1", status="failed",
                         error_message="[llm] API rate limit exceeded"),
        ]
        report = format_report(results, RepositoryMetrics())
        assert "API rate limit exceeded" in report

    def test_empty_results(self):
        report = format_report([], RepositoryMetrics())
        assert "CONVERSION REPORT" in report
        assert "SUMMARY" in report
        assert "Total mappings:     0" in report


# ===========================================================================
# _extract_error_category
# ===========================================================================

class TestExtractErrorCategory:
    """Test the _extract_error_category helper."""

    def test_extracts_llm(self):
        assert _extract_error_category("[llm] Rate limit") == ErrorCategory.LLM

    def test_extracts_input(self):
        assert _extract_error_category("[input] Bad XML") == ErrorCategory.INPUT

    def test_extracts_parse(self):
        assert _extract_error_category("[parse] Syntax error") == ErrorCategory.PARSE

    def test_extracts_validation(self):
        assert _extract_error_category("[validation] Schema") == ErrorCategory.VALIDATION

    def test_extracts_persistence(self):
        assert _extract_error_category("[persistence] Write fail") == ErrorCategory.PERSISTENCE

    def test_extracts_internal(self):
        assert _extract_error_category("[internal] Bug") == ErrorCategory.INTERNAL

    def test_returns_none_for_no_bracket(self):
        assert _extract_error_category("plain error") is None

    def test_returns_none_for_unknown_category(self):
        assert _extract_error_category("[unknown] Something") is None

    def test_returns_none_for_empty_string(self):
        assert _extract_error_category("") is None

    def test_returns_none_for_incomplete_bracket(self):
        assert _extract_error_category("[llm incomplete") is None


# ===========================================================================
# retry_failed_mappings
# ===========================================================================

class TestRetryFailedMappings:
    """Test the retry_failed_mappings orchestration function."""

    def test_no_retryable_failures_returns_original(self, config):
        results = [
            _make_result("m1", status="success"),
            _make_result("m2", status="failed", error_message="[input] Bad XML"),
        ]
        updated, metrics = retry_failed_mappings(
            results, enriched_map={}, config=config,
            llm_client=MagicMock(), io=MagicMock(),
        )
        # Should return the same list since [input] is not retryable
        assert len(updated) == 2
        assert updated[0].mapping_name == "m1"
        assert updated[1].mapping_name == "m2"
        assert metrics.total_mappings == 0

    def test_retries_llm_failures(self, config):
        failed_mr = _make_result(
            "m_fail", status="failed", error_message="[llm] Rate limit",
        )
        success_mr = _make_result("m_ok", status="success")

        # Mock enriched mapping
        mock_enriched = MagicMock()
        enriched_map = {"m_fail": mock_enriched}

        # Mock convert_mapping to return success on retry
        retry_result = _make_result(
            "m_fail", status="success",
            metrics=_make_metrics("m_fail"),
        )

        mock_llm = MagicMock()
        mock_io = MagicMock()

        with patch("informatica_to_dbt.orchestrator.convert_mapping",
                    return_value=retry_result) as mock_convert:
            updated, metrics = retry_failed_mappings(
                [success_mr, failed_mr],
                enriched_map=enriched_map,
                config=config,
                llm_client=mock_llm,
                io=mock_io,
            )

        assert mock_convert.call_count == 1
        assert updated[0].mapping_name == "m_ok"
        assert updated[0].status == "success"
        assert updated[1].mapping_name == "m_fail"
        assert updated[1].status == "success"
        assert metrics.total_mappings == 1

    def test_retries_persistence_failures(self, config):
        failed_mr = _make_result(
            "m_persist", status="failed",
            error_message="[persistence] Stage write failed",
        )
        mock_enriched = MagicMock()
        enriched_map = {"m_persist": mock_enriched}

        retry_result = _make_result(
            "m_persist", status="success",
            metrics=_make_metrics("m_persist"),
        )

        with patch("informatica_to_dbt.orchestrator.convert_mapping",
                    return_value=retry_result):
            updated, metrics = retry_failed_mappings(
                [failed_mr],
                enriched_map=enriched_map,
                config=config,
                llm_client=MagicMock(),
                io=MagicMock(),
            )

        assert updated[0].status == "success"

    def test_skips_missing_enriched_mapping(self, config):
        failed_mr = _make_result(
            "m_missing", status="failed",
            error_message="[llm] Timeout",
        )

        with patch("informatica_to_dbt.orchestrator.convert_mapping") as mock_convert:
            updated, metrics = retry_failed_mappings(
                [failed_mr],
                enriched_map={},  # no entry for m_missing
                config=config,
                llm_client=MagicMock(),
                io=MagicMock(),
            )

        mock_convert.assert_not_called()
        assert updated[0].status == "failed"

    def test_max_retries_limits_rounds(self, config):
        """If retry fails again with retryable error, max_retries limits attempts."""
        failed_mr = _make_result(
            "m_stubborn", status="failed",
            error_message="[llm] Timeout",
        )
        mock_enriched = MagicMock()
        enriched_map = {"m_stubborn": mock_enriched}

        # Retry also fails with retryable error
        still_failed = _make_result(
            "m_stubborn", status="failed",
            error_message="[llm] Still timing out",
            metrics=_make_metrics("m_stubborn", status="failed"),
        )

        with patch("informatica_to_dbt.orchestrator.convert_mapping",
                    return_value=still_failed) as mock_convert:
            updated, metrics = retry_failed_mappings(
                [failed_mr],
                enriched_map=enriched_map,
                config=config,
                llm_client=MagicMock(),
                io=MagicMock(),
                max_retries=2,
            )

        assert mock_convert.call_count == 2
        assert updated[0].status == "failed"

    def test_does_not_mutate_original_list(self, config):
        original = [
            _make_result("m1", status="failed", error_message="[llm] Error"),
        ]
        mock_enriched = MagicMock()
        retry_result = _make_result(
            "m1", status="success",
            metrics=_make_metrics("m1"),
        )

        with patch("informatica_to_dbt.orchestrator.convert_mapping",
                    return_value=retry_result):
            updated, _ = retry_failed_mappings(
                original,
                enriched_map={"m1": mock_enriched},
                config=config,
                llm_client=MagicMock(),
                io=MagicMock(),
            )

        # Original list should be untouched
        assert original[0].status == "failed"
        assert updated[0].status == "success"

    def test_persistence_error_during_retry_logged_not_raised(self, config):
        """If io.write_files_to_stage fails during retry, it's logged not raised."""
        from informatica_to_dbt.generator.response_parser import GeneratedFile
        failed_mr = _make_result(
            "m1", status="failed", error_message="[llm] Error",
        )
        retry_result = _make_result(
            "m1", status="success",
            files=[GeneratedFile("models/staging/stg_x.sql", "select 1")],
            metrics=_make_metrics("m1"),
        )

        mock_io = MagicMock()
        mock_io.write_files_to_stage.side_effect = RuntimeError("stage broke")

        with patch("informatica_to_dbt.orchestrator.convert_mapping",
                    return_value=retry_result):
            # Should not raise
            updated, _ = retry_failed_mappings(
                [failed_mr],
                enriched_map={"m1": MagicMock()},
                config=config,
                llm_client=MagicMock(),
                io=mock_io,
            )

        assert updated[0].status == "success"


# ===========================================================================
# ConversionSummary enrichment
# ===========================================================================

class TestConversionSummaryEnrichment:
    """Test the enriched ConversionSummary dataclass."""

    def test_default_new_fields(self):
        s = ConversionSummary(
            mapping_name="m1", workflow_name="wf1",
            complexity_score=10, strategy="DIRECT",
            num_files_generated=3, sql_errors=0, sql_warnings=0,
            yaml_errors=0, yaml_warnings=0, project_errors=0,
            total_chunks=1, elapsed_seconds=1.5, status="success",
        )
        assert s.llm_calls == 0
        assert s.llm_total_seconds == 0.0
        assert s.heal_attempts == 0
        assert s.quality_score == 0.0
        assert s.transformation_count == 0
        assert s.estimated_input_tokens == 0
        assert s.error_category is None

    def test_new_fields_set_explicitly(self):
        s = ConversionSummary(
            mapping_name="m1", workflow_name="wf1",
            complexity_score=10, strategy="LAYERED",
            num_files_generated=5, sql_errors=1, sql_warnings=2,
            yaml_errors=0, yaml_warnings=1, project_errors=0,
            total_chunks=2, elapsed_seconds=12.5, status="partial",
            llm_calls=4, llm_total_seconds=8.2,
            heal_attempts=1, quality_score=72.5,
            transformation_count=8, estimated_input_tokens=5000,
            error_category="llm",
        )
        assert s.llm_calls == 4
        assert s.llm_total_seconds == 8.2
        assert s.heal_attempts == 1
        assert s.quality_score == 72.5
        assert s.transformation_count == 8
        assert s.estimated_input_tokens == 5000
        assert s.error_category == "llm"


# ===========================================================================
# _build_summary enrichment
# ===========================================================================

class TestBuildSummaryEnrichment:
    """Test that _build_summary populates new metrics fields."""

    def test_populates_metrics_from_mapping_metrics(self):
        mm = _make_metrics(
            "m1", llm_calls=3, llm_total_seconds=7.0,
            heal_attempts=1, quality_score=90,
            transformation_count=6, estimated_input_tokens=2500,
        )
        mr = _make_result("m1", metrics=mm)

        summary = _build_summary(mr, "TestFolder")
        assert summary.llm_calls == 3
        assert summary.llm_total_seconds == 7.0
        assert summary.heal_attempts == 1
        assert summary.quality_score == 90
        assert summary.transformation_count == 6
        assert summary.estimated_input_tokens == 2500
        assert summary.error_category is None

    def test_populates_error_category_from_error_message(self):
        mr = _make_result(
            "m1", status="failed",
            error_message="[llm] Rate limit exceeded",
            metrics=_make_metrics("m1", status="failed"),
        )
        summary = _build_summary(mr, "TestFolder")
        assert summary.error_category == "llm"

    def test_no_metrics_uses_defaults(self):
        mr = _make_result("m1", metrics=None)
        summary = _build_summary(mr, "TestFolder")
        assert summary.llm_calls == 0
        assert summary.llm_total_seconds == 0.0
        assert summary.heal_attempts == 0
        assert summary.quality_score == 0.0
        assert summary.transformation_count == 0
        assert summary.estimated_input_tokens == 0

    def test_error_category_none_for_success(self):
        mr = _make_result("m1", status="success", metrics=_make_metrics("m1"))
        summary = _build_summary(mr, "TestFolder")
        assert summary.error_category is None

    def test_error_category_none_for_unbracketed_error(self):
        mr = _make_result(
            "m1", status="failed",
            error_message="Something went wrong",
            metrics=_make_metrics("m1", status="failed"),
        )
        summary = _build_summary(mr, "TestFolder")
        assert summary.error_category is None

    def test_preserves_original_fields(self):
        """Ensure the original summary fields still work."""
        mr = _make_result(
            "m1", status="success", chunks=3, elapsed=5.5,
            metrics=_make_metrics("m1"),
        )
        summary = _build_summary(mr, "MyWorkflow")
        assert summary.mapping_name == "m1"
        assert summary.workflow_name == "test_wf"
        assert summary.total_chunks == 3
        assert summary.elapsed_seconds == 5.5
        assert summary.status == "success"

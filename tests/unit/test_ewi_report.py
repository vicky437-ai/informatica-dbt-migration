"""Unit tests for the EWI report generator."""

from __future__ import annotations

from pathlib import Path

import pytest

from informatica_to_dbt.metrics import MappingMetrics, RepositoryMetrics
from informatica_to_dbt.reports.ewi_report import EWIItem, EWIReport, EWIReportGenerator


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_repo_metrics() -> RepositoryMetrics:
    """Build a RepositoryMetrics with mixed success/partial/failed mappings."""
    rm = RepositoryMetrics(total_seconds=120.5)
    rm.add(MappingMetrics(
        mapping_name="m_load_orders",
        workflow_name="wf_orders",
        complexity_score=45,
        strategy="STAGED",
        files_generated=6,
        quality_score=85,
        llm_calls=3,
        heal_attempts=0,
        status="success",
    ))
    rm.add(MappingMetrics(
        mapping_name="m_load_customers",
        workflow_name="wf_customers",
        complexity_score=62,
        strategy="LAYERED",
        files_generated=8,
        sql_errors=1,
        yaml_errors=0,
        quality_score=70,
        llm_calls=5,
        heal_attempts=2,
        status="partial",
    ))
    rm.add(MappingMetrics(
        mapping_name="m_complex_pipeline",
        workflow_name="wf_complex",
        complexity_score=90,
        strategy="COMPLEX",
        files_generated=0,
        quality_score=0,
        llm_calls=2,
        heal_attempts=3,
        status="failed",
        error_message="LLM response parse error",
    ))
    return rm


@pytest.fixture
def repo_metrics():
    return _make_repo_metrics()


@pytest.fixture
def generator():
    return EWIReportGenerator()


# ---------------------------------------------------------------------------
# EWIItem
# ---------------------------------------------------------------------------

class TestEWIItem:
    def test_css_class_error(self):
        item = EWIItem(level="error", mapping_name="m1", code="E001", message="fail")
        assert item.css_class == "ewi-error"

    def test_css_class_warning(self):
        item = EWIItem(level="warning", mapping_name="m1", code="W001", message="warn")
        assert item.css_class == "ewi-warning"

    def test_css_class_info(self):
        item = EWIItem(level="info", mapping_name="m1", code="I001", message="ok")
        assert item.css_class == "ewi-info"


# ---------------------------------------------------------------------------
# EWIReport
# ---------------------------------------------------------------------------

class TestEWIReport:
    def test_errors_warnings_infos_filter(self):
        report = EWIReport(items=[
            EWIItem(level="error", mapping_name="m1", code="E001", message="e1"),
            EWIItem(level="warning", mapping_name="m1", code="W001", message="w1"),
            EWIItem(level="info", mapping_name="m1", code="I001", message="i1"),
            EWIItem(level="info", mapping_name="m2", code="I002", message="i2"),
        ])
        assert len(report.errors) == 1
        assert len(report.warnings) == 1
        assert len(report.infos) == 2

    def test_to_json(self):
        report = EWIReport(
            title="Test Report",
            items=[
                EWIItem(level="error", mapping_name="m1", code="E001", message="bad"),
            ],
        )
        json_str = report.to_json()
        assert "Test Report" in json_str
        assert "E001" in json_str
        assert "bad" in json_str


# ---------------------------------------------------------------------------
# EWIReportGenerator — classification
# ---------------------------------------------------------------------------

class TestEWIReportGeneratorClassification:
    def test_generates_report_with_all_levels(self, generator, repo_metrics):
        report = generator.generate(repo_metrics)
        assert len(report.errors) >= 1   # failed mapping
        assert len(report.warnings) >= 1  # partial mapping + sql_errors
        assert len(report.infos) >= 1     # success mapping

    def test_failed_mapping_creates_error(self, generator, repo_metrics):
        report = generator.generate(repo_metrics)
        error_items = [i for i in report.errors if i.mapping_name == "m_complex_pipeline"]
        assert len(error_items) >= 1
        assert "E001" in error_items[0].code

    def test_partial_mapping_creates_warning(self, generator, repo_metrics):
        report = generator.generate(repo_metrics)
        partial_items = [i for i in report.warnings if i.mapping_name == "m_load_customers"]
        assert len(partial_items) >= 1

    def test_success_mapping_creates_info(self, generator, repo_metrics):
        report = generator.generate(repo_metrics)
        info_items = [i for i in report.infos if i.mapping_name == "m_load_orders"]
        assert len(info_items) >= 1

    def test_sql_errors_create_warning(self, generator, repo_metrics):
        report = generator.generate(repo_metrics)
        sql_warns = [i for i in report.warnings if i.code == "W010"]
        assert len(sql_warns) >= 1
        assert sql_warns[0].mapping_name == "m_load_customers"

    def test_heal_attempts_create_info(self, generator, repo_metrics):
        report = generator.generate(repo_metrics)
        heal_items = [i for i in report.infos if i.code == "I010"]
        assert len(heal_items) >= 1  # m_load_customers has heal_attempts=2

    def test_high_complexity_creates_info(self, generator, repo_metrics):
        report = generator.generate(repo_metrics)
        complex_items = [i for i in report.infos if i.code == "I020"]
        assert len(complex_items) >= 1
        assert complex_items[0].mapping_name == "m_complex_pipeline"

    def test_repository_metrics_attached(self, generator, repo_metrics):
        report = generator.generate(repo_metrics)
        assert report.repository_metrics is repo_metrics
        assert report.repository_metrics.total_mappings == 3


# ---------------------------------------------------------------------------
# EWIReportGenerator — HTML output
# ---------------------------------------------------------------------------

class TestEWIReportGeneratorHTML:
    def test_write_html(self, generator, repo_metrics, tmp_path):
        report = generator.generate(repo_metrics)
        out_path = str(tmp_path / "report.html")
        result = generator.write_html(report, out_path)
        assert Path(result).exists()

        content = Path(result).read_text(encoding="utf-8")
        assert "<!DOCTYPE html>" in content
        assert "m_load_orders" in content
        assert "m_complex_pipeline" in content
        assert "Executive Summary" in content

    def test_html_contains_ewi_codes(self, generator, repo_metrics, tmp_path):
        report = generator.generate(repo_metrics)
        out_path = str(tmp_path / "report.html")
        generator.write_html(report, out_path)

        content = Path(out_path).read_text(encoding="utf-8")
        assert "E001" in content
        assert "W001" in content or "W010" in content
        assert "I001" in content

    def test_html_escapes_special_chars(self, generator, tmp_path):
        rm = RepositoryMetrics(total_seconds=1.0)
        rm.add(MappingMetrics(
            mapping_name="m_test<script>",
            workflow_name="wf&test",
            status="success",
            files_generated=1,
            quality_score=90,
        ))
        report = generator.generate(rm)
        out_path = str(tmp_path / "report.html")
        generator.write_html(report, out_path)

        content = Path(out_path).read_text(encoding="utf-8")
        assert "<script>" not in content  # should be escaped
        assert "&lt;script&gt;" in content
        assert "&amp;test" in content


# ---------------------------------------------------------------------------
# EWIReportGenerator — JSON output
# ---------------------------------------------------------------------------

class TestEWIReportGeneratorJSON:
    def test_write_json(self, generator, repo_metrics, tmp_path):
        report = generator.generate(repo_metrics)
        out_path = str(tmp_path / "report.json")
        result = generator.write_json(report, out_path)
        assert Path(result).exists()

        import json
        data = json.loads(Path(result).read_text(encoding="utf-8"))
        assert data["title"] == "Informatica-to-dbt Migration Assessment"
        assert len(data["items"]) > 0
        assert "summary" in data

    def test_json_contains_all_mappings(self, generator, repo_metrics, tmp_path):
        report = generator.generate(repo_metrics)
        out_path = str(tmp_path / "report.json")
        generator.write_json(report, out_path)

        import json
        data = json.loads(Path(out_path).read_text(encoding="utf-8"))
        mapping_names = {i["mapping_name"] for i in data["items"]}
        assert "m_load_orders" in mapping_names
        assert "m_load_customers" in mapping_names
        assert "m_complex_pipeline" in mapping_names


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEWIReportEdgeCases:
    def test_empty_repository(self, generator):
        rm = RepositoryMetrics(total_seconds=0.0)
        report = generator.generate(rm)
        assert len(report.items) == 0

    def test_all_successful(self, generator, tmp_path):
        rm = RepositoryMetrics(total_seconds=10.0)
        for i in range(5):
            rm.add(MappingMetrics(
                mapping_name=f"m_{i}",
                workflow_name=f"wf_{i}",
                status="success",
                files_generated=3,
                quality_score=90,
            ))
        report = generator.generate(rm)
        assert len(report.errors) == 0
        assert len(report.warnings) == 0
        assert len(report.infos) == 5

        # Should still produce valid HTML
        out_path = str(tmp_path / "report.html")
        generator.write_html(report, out_path)
        content = Path(out_path).read_text(encoding="utf-8")
        assert "100%" in content  # 100% success rate

    def test_all_failed(self, generator):
        rm = RepositoryMetrics(total_seconds=5.0)
        for i in range(3):
            rm.add(MappingMetrics(
                mapping_name=f"m_fail_{i}",
                workflow_name=f"wf_fail_{i}",
                status="failed",
                error_message=f"Error {i}",
            ))
        report = generator.generate(rm)
        assert len(report.errors) == 3
        assert len(report.infos) == 0

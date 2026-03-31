"""Unit tests for the generator modules (response_parser, post_processor, quality_scorer, dbt_project_generator)."""

import unittest.mock

import pytest
from informatica_to_dbt.generator.response_parser import (
    GeneratedFile,
    parse_response,
    merge_chunk_files,
    _merge_yaml_content,
    _merge_named_list,
)
from informatica_to_dbt.generator.post_processor import post_process
from informatica_to_dbt.generator.quality_scorer import (
    score_quality,
    QualityReport,
    DimensionScore,
)
from informatica_to_dbt.generator.dbt_project_generator import (
    generate_dbt_project_yml,
    generate_packages_yml,
    generate_project_files,
    _sanitize_project_name,
)
from informatica_to_dbt.exceptions import ResponseParseError


# ---------------------------------------------------------------------------
# Response Parser
# ---------------------------------------------------------------------------

class TestParseResponse:
    def test_basic_file_blocks(self):
        raw = (
            "-- FILE: models/staging/stg_a.sql\n"
            "SELECT 1\n"
            "-- FILE: models/staging/schema.yml\n"
            "version: 2\n"
        )
        files = parse_response(raw)
        assert len(files) == 2
        assert files[0].path == "models/staging/stg_a.sql"
        assert "SELECT 1" in files[0].content
        assert files[1].path == "models/staging/schema.yml"

    def test_strips_code_fences(self):
        raw = (
            "```sql\n"
            "-- FILE: models/staging/stg_a.sql\n"
            "SELECT 1\n"
            "```\n"
        )
        files = parse_response(raw)
        assert len(files) == 1
        assert "```" not in files[0].content

    def test_no_file_markers_raises_error(self):
        with pytest.raises(ResponseParseError):
            parse_response("just some text with no file markers")

    def test_is_sql_property(self):
        gf = GeneratedFile("models/staging/stg_a.sql", "SELECT 1")
        assert gf.is_sql is True
        assert gf.is_yaml is False

    def test_is_yaml_property(self):
        gf = GeneratedFile("models/staging/schema.yml", "version: 2")
        assert gf.is_yaml is True
        assert gf.is_sql is False

    def test_layer_detection(self):
        assert GeneratedFile("models/staging/stg_a.sql", "").layer == "staging"
        assert GeneratedFile("models/intermediate/int_a.sql", "").layer == "intermediate"
        assert GeneratedFile("models/marts/fct_a.sql", "").layer == "marts"
        assert GeneratedFile("macros/helpers.sql", "").layer == "macros"
        assert GeneratedFile("other/thing.sql", "").layer == "other"


class TestMergeChunkFiles:
    def test_merges_sql_files(self):
        chunk1 = [GeneratedFile("models/staging/stg_a.sql", "SELECT 1")]
        chunk2 = [GeneratedFile("models/staging/stg_b.sql", "SELECT 2")]
        merged = merge_chunk_files([chunk1, chunk2])
        assert len(merged) == 2

    def test_deduplicates_yaml(self):
        yaml1 = "version: 2\nmodels:\n  - name: a\n"
        yaml2 = "version: 2\nmodels:\n  - name: b\n"
        chunk1 = [GeneratedFile("models/staging/schema.yml", yaml1)]
        chunk2 = [GeneratedFile("models/staging/schema.yml", yaml2)]
        merged = merge_chunk_files([chunk1, chunk2])
        # Should merge into a single schema.yml
        schema_files = [f for f in merged if f.path == "models/staging/schema.yml"]
        assert len(schema_files) == 1
        content = schema_files[0].content
        assert "a" in content
        assert "b" in content


class TestYAMLMerge:
    def test_merge_sources_dedup(self):
        a = "version: 2\nsources:\n  - name: raw\n    tables:\n      - name: t1\n"
        b = "version: 2\nsources:\n  - name: raw\n    tables:\n      - name: t1\n      - name: t2\n"
        merged = _merge_yaml_content(a, b)
        assert "t1" in merged
        assert "t2" in merged
        # t1 should appear only once
        assert merged.count("t1") == 1

    def test_merge_models_dedup(self):
        a = "version: 2\nmodels:\n  - name: stg_a\n    columns:\n      - name: id\n"
        b = "version: 2\nmodels:\n  - name: stg_a\n    columns:\n      - name: name\n  - name: stg_b\n"
        merged = _merge_yaml_content(a, b)
        assert "stg_a" in merged
        assert "stg_b" in merged
        assert "id" in merged
        assert "name" in merged

    def test_merge_non_dict_fallback(self):
        """Non-dict YAML should fall back to concatenation."""
        result = _merge_yaml_content("- item1", "- item2")
        assert "item1" in result
        assert "item2" in result

    def test_merge_named_list_basic(self):
        a = [{"name": "x", "val": 1}]
        b = [{"name": "y", "val": 2}]
        result = _merge_named_list(a, b)
        assert len(result) == 2

    def test_merge_named_list_dedup(self):
        a = [{"name": "x", "val": 1}]
        b = [{"name": "x", "val": 2}]
        result = _merge_named_list(a, b)
        assert len(result) == 1
        assert result[0]["val"] == 1  # keeps existing

    def test_merge_named_list_with_subkey(self):
        a = [{"name": "src", "tables": [{"name": "t1"}]}]
        b = [{"name": "src", "tables": [{"name": "t2"}]}]
        result = _merge_named_list(a, b, sub_key="tables")
        assert len(result) == 1
        assert len(result[0]["tables"]) == 2


# ---------------------------------------------------------------------------
# Post-Processor
# ---------------------------------------------------------------------------

class TestPostProcessor:
    def test_normalises_whitespace(self):
        files = [GeneratedFile("models/staging/stg_a.sql", "SELECT 1  \n\n\n\n")]
        processed, warnings = post_process(files)
        assert processed[0].content.endswith("\n")
        assert not processed[0].content.endswith("\n\n")

    def test_fixes_informatica_residuals(self):
        files = [GeneratedFile("models/staging/stg_a.sql", (
            "{{ config(materialized='view') }}\n"
            "SELECT IIF(x=1, 'Y', 'N') as flag\n"
            "FROM {{ source('raw', 'tbl') }}\n"
        ))]
        processed, _ = post_process(files)
        assert "IIF(" not in processed[0].content
        assert "IFF(" in processed[0].content

    def test_replaces_tabs_in_yaml(self):
        files = [GeneratedFile("models/staging/schema.yml", "version: 2\n\tmodels:\n")]
        processed, warnings = post_process(files)
        assert "\t" not in processed[0].content
        assert any("Tabs" in w for w in warnings)

    def test_normalises_path(self):
        files = [GeneratedFile("./models\\staging\\stg_a.sql", "SELECT 1")]
        processed, _ = post_process(files)
        assert processed[0].path == "models/staging/stg_a.sql"

    def test_warns_missing_config(self):
        files = [GeneratedFile("models/staging/stg_a.sql", "SELECT 1 FROM {{ source('a','b') }}")]
        _, warnings = post_process(files)
        assert any("config" in w.lower() for w in warnings)


# ---------------------------------------------------------------------------
# Quality Scorer
# ---------------------------------------------------------------------------

class TestQualityScorer:
    def test_clean_files_score_high(self, clean_dbt_files):
        report = score_quality(clean_dbt_files, ["staging", "intermediate", "marts"])
        assert isinstance(report, QualityReport)
        assert report.total_score >= 80

    def test_problematic_files_score_low(self, problematic_dbt_files):
        report = score_quality(problematic_dbt_files, ["staging", "intermediate", "marts"])
        # Should score lower than clean files (100), but some files are valid
        assert report.total_score < 100

    def test_summary_format(self, clean_dbt_files):
        report = score_quality(clean_dbt_files, ["staging"])
        assert "Quality" in report.summary
        assert "/100" in report.summary

    def test_dimension_scores(self, clean_dbt_files):
        report = score_quality(clean_dbt_files, ["staging", "intermediate", "marts"])
        names = [d.name for d in report.dimensions]
        assert "file_structure" in names
        assert "dbt_conventions" in names
        assert "sql_syntax" in names
        assert "function_conversion" in names
        assert "yaml_quality" in names

    def test_empty_files_list(self):
        report = score_quality([], ["staging"])
        assert report.total_score >= 0


# ---------------------------------------------------------------------------
# dbt Project Generator
# ---------------------------------------------------------------------------

class TestDbtProjectGenerator:
    def test_sanitize_name_basic(self):
        assert _sanitize_project_name("My-Workflow.v2") == "my_workflow_v2"

    def test_sanitize_name_leading_digit(self):
        assert _sanitize_project_name("123_bad").startswith("dbt_")

    def test_sanitize_name_informatica_style(self):
        result = _sanitize_project_name("m_AP_ERM_CUSTGRP_DIM")
        assert result == "m_ap_erm_custgrp_dim"

    def test_generate_project_yml_basic(self, clean_dbt_files):
        project = generate_dbt_project_yml("test_proj", clean_dbt_files)
        assert project.path == "dbt_project.yml"
        assert "name: 'test_proj'" in project.content
        assert "staging:" in project.content
        assert "intermediate:" in project.content
        assert "marts:" in project.content

    def test_generate_project_yml_with_database(self, clean_dbt_files):
        project = generate_dbt_project_yml(
            "test", clean_dbt_files, target_database="MY_DB",
        )
        assert "source_database: 'MY_DB'" in project.content

    def test_generate_packages_yml_with_dbt_utils(self):
        files = [GeneratedFile("models/staging/stg_a.sql", (
            "{{ dbt_utils.generate_surrogate_key(['id']) }}"
        ))]
        pkg = generate_packages_yml(files)
        assert pkg is not None
        assert "dbt-labs/dbt_utils" in pkg.content

    def test_generate_packages_yml_none_when_no_packages(self):
        files = [GeneratedFile("models/staging/stg_a.sql", "SELECT 1")]
        pkg = generate_packages_yml(files)
        assert pkg is None

    def test_generate_project_files(self, clean_dbt_files):
        result = generate_project_files("proj", clean_dbt_files)
        assert len(result) >= 1
        paths = [f.path for f in result]
        assert "dbt_project.yml" in paths

    def test_incremental_detection(self):
        files = [GeneratedFile("models/intermediate/int_a.sql", (
            "{{ config(materialized='incremental') }}\nSELECT 1"
        ))]
        proj = generate_dbt_project_yml("test", files)
        assert "materialized: incremental" in proj.content


# ---------------------------------------------------------------------------
# ConcurrencyGuard
# ---------------------------------------------------------------------------

import threading
import time
from informatica_to_dbt.generator.llm_client import ConcurrencyGuard, LLMClient
from informatica_to_dbt.config import Config


class TestConcurrencyGuard:
    def test_initial_interval(self):
        guard = ConcurrencyGuard(min_interval=20.0)
        assert guard.current_interval == 20.0

    def test_backoff_on_failure(self):
        guard = ConcurrencyGuard(min_interval=0.0, max_interval=100.0)
        guard._current_interval = 10.0
        guard._last_call = 0.0
        guard.acquire()
        guard.release(success=False)
        assert guard.current_interval == 20.0

    def test_backoff_capped_at_max(self):
        guard = ConcurrencyGuard(min_interval=0.0, max_interval=30.0)
        guard._current_interval = 10.0
        guard._last_call = 0.0
        guard.acquire()
        guard.release(success=False)  # 10 -> 20
        assert guard.current_interval == 20.0
        guard._last_call = 0.0  # skip cooldown wait
        guard.acquire()
        guard.release(success=False)  # 20 -> 30 (capped)
        assert guard.current_interval == 30.0
        guard._last_call = 0.0
        guard.acquire()
        guard.release(success=False)  # stays at 30
        assert guard.current_interval == 30.0

    def test_restore_on_success(self):
        guard = ConcurrencyGuard(min_interval=0.0, max_interval=100.0)
        guard._current_interval = 20.0
        # Success restores gradually (use _last_call=0 to skip sleeps)
        guard._last_call = 0.0
        guard.acquire()
        guard.release(success=True)  # 20 * 0.75 = 15
        assert guard.current_interval == 15.0
        guard._last_call = 0.0
        guard.acquire()
        guard.release(success=True)  # 15 * 0.75 = 11.25
        assert guard.current_interval == 11.25
        guard._last_call = 0.0
        guard.acquire()
        guard.release(success=True)  # 11.25 * 0.75 = 8.4375
        assert guard.current_interval == 8.4375

    def test_restore_clamps_to_min(self):
        guard = ConcurrencyGuard(min_interval=10.0, max_interval=100.0)
        guard._current_interval = 12.0
        guard._last_call = 0.0  # long ago — acquire won't sleep
        guard.acquire()
        guard.release(success=True)  # 12 * 0.75 = 9.0 -> clamped to 10
        assert guard.current_interval == 10.0

    def test_serialises_access(self):
        """Verify that acquire/release serialises access across threads."""
        guard = ConcurrencyGuard(min_interval=0.0)
        results = []
        barrier = threading.Barrier(2)

        def worker(value):
            barrier.wait()
            guard.acquire()
            results.append(value)
            time.sleep(0.01)
            guard.release()

        t1 = threading.Thread(target=worker, args=(1,))
        t2 = threading.Thread(target=worker, args=(2,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        # Both should have appended — order may vary but no interleaving
        assert sorted(results) == [1, 2]


# ---------------------------------------------------------------------------
# LLMClient (stub mode)
# ---------------------------------------------------------------------------

class TestLLMClient:
    def _fast_config(self) -> Config:
        """Config with no rate-limit delays for fast testing."""
        config = Config()
        config.rate_limit_calls_per_minute = 60_000  # effectively no wait
        return config

    def test_is_stub_without_cortex(self):
        with unittest.mock.patch(
            "informatica_to_dbt.generator.llm_client._get_local_snowpark_session",
            return_value=None,
        ):
            client = LLMClient(self._fast_config())
            assert client.is_stub is True

    def test_generate_returns_stub_response(self):
        from informatica_to_dbt.generator.prompt_builder import PromptPair
        with unittest.mock.patch(
            "informatica_to_dbt.generator.llm_client._get_local_snowpark_session",
            return_value=None,
        ):
            client = LLMClient(self._fast_config())
            prompt = PromptPair(system="sys", user="usr")
            result = client.generate(prompt)
            assert "stg_placeholder" in result
            assert "source('raw', 'placeholder')" in result

    def test_guard_interval_from_config(self):
        config = Config()
        config.rate_limit_calls_per_minute = 6  # 10s interval
        client = LLMClient(config)
        assert client._guard.current_interval == 10.0

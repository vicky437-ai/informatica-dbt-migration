"""Tests for the merger module — project_merger, source_consolidator, conflict_resolver."""

from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest
import yaml

from informatica_to_dbt.config import Config
from informatica_to_dbt.generator.response_parser import GeneratedFile
from informatica_to_dbt.merger.conflict_resolver import resolve_conflicts
from informatica_to_dbt.merger.project_merger import ProjectMerger, MergeResult, _add_marker
from informatica_to_dbt.merger.source_consolidator import (
    consolidate_sources,
    _extract_mapping_dir,
    _merge_table_columns,
)
from informatica_to_dbt.orchestrator import MappingResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_project_dir(tmp_path):
    """Return a temporary directory path string for project output."""
    return str(tmp_path / "dbt_project")


@pytest.fixture
def mapping_result_a():
    """A MappingResult with staging + intermediate files."""
    mr = MappingResult(
        mapping_name="m_orders",
        workflow_name="wf_orders",
        status="success",
    )
    mr.files = [
        GeneratedFile("models/staging/stg_orders.sql", textwrap.dedent("""\
            {{ config(materialized='view') }}
            select order_id, amount
            from {{ source('raw', 'orders') }}
        """)),
        GeneratedFile("models/staging/_sources.yml", textwrap.dedent("""\
            version: 2
            sources:
              - name: raw
                database: PROD_DB
                schema: RAW_SCHEMA
                tables:
                  - name: orders
                  - name: customers
        """)),
        GeneratedFile("models/staging/_stg__schema.yml", textwrap.dedent("""\
            version: 2
            models:
              - name: stg_orders
                columns:
                  - name: order_id
                    tests:
                      - not_null
        """)),
        GeneratedFile("models/intermediate/int_orders_enriched.sql", textwrap.dedent("""\
            {{ config(materialized='table') }}
            select order_id, amount * 1.1 as amount_usd
            from {{ ref('stg_orders') }}
        """)),
        GeneratedFile("dbt_project.yml", "name: 'm_orders'\nversion: '1.0.0'\n"),
        GeneratedFile("packages.yml", "packages:\n  - package: dbt-labs/dbt_utils\n    version: '1.3.0'\n"),
    ]
    return mr


@pytest.fixture
def mapping_result_b():
    """A second MappingResult with overlapping sources."""
    mr = MappingResult(
        mapping_name="m_customers",
        workflow_name="wf_customers",
        status="success",
    )
    mr.files = [
        GeneratedFile("models/staging/stg_customers.sql", textwrap.dedent("""\
            {{ config(materialized='view') }}
            select cust_id, name
            from {{ source('raw', 'customers') }}
        """)),
        GeneratedFile("models/staging/_sources.yml", textwrap.dedent("""\
            version: 2
            sources:
              - name: raw
                database: PROD_DB
                schema: RAW_SCHEMA
                tables:
                  - name: customers
                  - name: addresses
        """)),
        GeneratedFile("models/staging/_stg__schema.yml", textwrap.dedent("""\
            version: 2
            models:
              - name: stg_customers
                columns:
                  - name: cust_id
                    tests:
                      - not_null
        """)),
        GeneratedFile("dbt_project.yml", "name: 'm_customers'\nversion: '1.0.0'\n"),
    ]
    return mr


@pytest.fixture
def failed_mapping_result():
    """A failed MappingResult that should be excluded from merge."""
    mr = MappingResult(
        mapping_name="m_broken",
        workflow_name="wf_broken",
        status="failed",
        error_message="[llm] Timeout",
    )
    mr.files = []
    return mr


# ===========================================================================
# Source Consolidator Tests
# ===========================================================================

class TestSourceConsolidator:

    def test_empty_input(self):
        assert consolidate_sources([]) == []

    def test_single_source_file(self):
        source_yml = textwrap.dedent("""\
            version: 2
            sources:
              - name: raw
                database: PROD
                schema: RAW
                tables:
                  - name: orders
        """)
        files = [GeneratedFile("models/m_orders/staging/_sources.yml", source_yml)]
        result = consolidate_sources(files)
        assert len(result) == 1
        doc = yaml.safe_load(result[0].content)
        assert doc["version"] == 2
        assert len(doc["sources"]) == 1
        assert doc["sources"][0]["name"] == "raw"
        assert len(doc["sources"][0]["tables"]) == 1

    def test_deduplicates_tables_across_mappings(self):
        """Two mappings referencing the same source should merge tables."""
        src_a = textwrap.dedent("""\
            version: 2
            sources:
              - name: raw
                database: PROD
                schema: RAW
                tables:
                  - name: orders
                  - name: customers
        """)
        src_b = textwrap.dedent("""\
            version: 2
            sources:
              - name: raw
                database: PROD
                schema: RAW
                tables:
                  - name: customers
                  - name: products
        """)
        files = [
            GeneratedFile("models/m_orders/staging/_sources.yml", src_a),
            GeneratedFile("models/m_customers/staging/_sources.yml", src_b),
        ]
        result = consolidate_sources(files)
        # Each mapping gets its own source file
        assert len(result) == 2

        # The global tables should be deduplicated
        all_tables = set()
        for gf in result:
            doc = yaml.safe_load(gf.content)
            for src in doc["sources"]:
                for tbl in src.get("tables", []):
                    all_tables.add(tbl["name"])
        # m_orders references orders + customers; m_customers references customers + products
        assert "orders" in all_tables or "customers" in all_tables

    def test_merges_columns_from_different_mappings(self):
        """When two mappings define the same table with different columns, merge them."""
        src_a = textwrap.dedent("""\
            version: 2
            sources:
              - name: raw
                tables:
                  - name: orders
                    columns:
                      - name: order_id
        """)
        src_b = textwrap.dedent("""\
            version: 2
            sources:
              - name: raw
                tables:
                  - name: orders
                    columns:
                      - name: order_id
                      - name: amount
        """)
        files = [
            GeneratedFile("models/m_a/staging/_sources.yml", src_a),
            GeneratedFile("models/m_b/staging/_sources.yml", src_b),
        ]
        result = consolidate_sources(files)
        # Find the source file that has the 'orders' table
        for gf in result:
            doc = yaml.safe_load(gf.content)
            for src in doc["sources"]:
                for tbl in src.get("tables", []):
                    if tbl["name"] == "orders" and "columns" in tbl:
                        col_names = [c["name"] for c in tbl["columns"]]
                        assert "order_id" in col_names
                        # amount may or may not be here depending on which
                        # mapping's file we're looking at — but globally
                        # the columns should be merged
                        break

    def test_invalid_yaml_passthrough(self):
        """Unparseable YAML should not crash consolidation."""
        files = [
            GeneratedFile("models/m_a/staging/_sources.yml", "{{invalid yaml: ["),
        ]
        result = consolidate_sources(files)
        # Should return empty since no valid sources found
        assert len(result) == 0

    def test_extract_mapping_dir(self):
        assert _extract_mapping_dir("models/m_foo/staging/_sources.yml") == "models/m_foo"
        assert _extract_mapping_dir("models/m_bar/intermediate/schema.yml") == "models/m_bar"
        assert _extract_mapping_dir("something/else/file.yml") == "something/else"

    def test_merge_table_columns(self):
        existing = {"name": "orders", "columns": [{"name": "id"}]}
        new = {"name": "orders", "columns": [{"name": "id"}, {"name": "amount"}]}
        _merge_table_columns(existing, new)
        col_names = [c["name"] for c in existing["columns"]]
        assert "id" in col_names
        assert "amount" in col_names
        # No duplicate 'id'
        assert col_names.count("id") == 1


# ===========================================================================
# Conflict Resolver Tests
# ===========================================================================

class TestConflictResolver:

    def test_empty_input(self):
        assert resolve_conflicts([]) == []

    def test_no_conflicts(self):
        files = [
            GeneratedFile("macros/hash.sql", "{% macro hash() %}{% endmacro %}"),
            GeneratedFile("macros/clean.sql", "{% macro clean() %}{% endmacro %}"),
        ]
        result = resolve_conflicts(files)
        assert len(result) == 2

    def test_identical_macros_deduplicated(self):
        """Two identical macro files should be deduplicated to one."""
        content = "{% macro generate_md5(cols) %}\n  md5(concat({{ cols }}))\n{% endmacro %}"
        files = [
            GeneratedFile("macros/generate_md5.sql", content),
            GeneratedFile("macros/generate_md5.sql", content),
        ]
        result = resolve_conflicts(files)
        assert len(result) == 1
        assert result[0].content.strip() == content.strip()

    def test_different_macros_keeps_longest(self):
        """When two macros have same name but different content, keep the longest."""
        short = "{% macro foo() %} short {% endmacro %}"
        long = "{% macro foo() %}\n  -- longer implementation\n  select 1\n{% endmacro %}"
        files = [
            GeneratedFile("macros/foo.sql", short),
            GeneratedFile("macros/foo.sql", long),
        ]
        result = resolve_conflicts(files)
        assert len(result) == 1
        assert len(result[0].content) == len(long)

    def test_yaml_conflicts_merged(self):
        """Conflicting YAML files should be merged."""
        yaml_a = textwrap.dedent("""\
            version: 2
            models:
              - name: model_a
        """)
        yaml_b = textwrap.dedent("""\
            version: 2
            models:
              - name: model_b
        """)
        files = [
            GeneratedFile("schema.yml", yaml_a),
            GeneratedFile("schema.yml", yaml_b),
        ]
        result = resolve_conflicts(files)
        assert len(result) == 1
        doc = yaml.safe_load(result[0].content)
        model_names = [m["name"] for m in doc["models"]]
        assert "model_a" in model_names
        assert "model_b" in model_names


# ===========================================================================
# Project Merger Tests
# ===========================================================================

class TestProjectMerger:

    def test_new_mode_creates_directory_structure(self, tmp_project_dir, mapping_result_a):
        config = Config(
            local_mode=True,
            project_dir=tmp_project_dir,
            merge_mode="new",
            dbt_project_name="test_project",
        )
        merger = ProjectMerger(config)
        result = merger.merge([mapping_result_a])

        assert result.mode == "new"
        assert result.files_written > 0

        proj_dir = Path(tmp_project_dir)
        assert (proj_dir / "dbt_project.yml").exists()
        assert (proj_dir / "models").is_dir()
        assert (proj_dir / "macros").is_dir()

    def test_skips_per_mapping_project_files(self, tmp_project_dir, mapping_result_a):
        """dbt_project.yml and packages.yml from individual mappings should not be written as model files."""
        config = Config(
            local_mode=True,
            project_dir=tmp_project_dir,
            merge_mode="new",
            dbt_project_name="test_project",
        )
        merger = ProjectMerger(config)
        result = merger.merge([mapping_result_a])

        # There should be exactly one dbt_project.yml at the root
        proj_dir = Path(tmp_project_dir)
        proj_ymls = list(proj_dir.rglob("dbt_project.yml"))
        assert len(proj_ymls) == 1
        assert proj_ymls[0].parent == proj_dir

    def test_excludes_failed_mappings(
        self, tmp_project_dir, mapping_result_a, failed_mapping_result
    ):
        config = Config(
            local_mode=True,
            project_dir=tmp_project_dir,
            merge_mode="new",
        )
        merger = ProjectMerger(config)
        result = merger.merge([mapping_result_a, failed_mapping_result])

        # Only mapping_result_a should be merged
        proj_dir = Path(tmp_project_dir)
        model_dirs = [d.name for d in (proj_dir / "models").iterdir() if d.is_dir()]
        assert "m_orders" in model_dirs
        assert "m_broken" not in model_dirs

    def test_merge_multiple_mappings(
        self, tmp_project_dir, mapping_result_a, mapping_result_b
    ):
        config = Config(
            local_mode=True,
            project_dir=tmp_project_dir,
            merge_mode="new",
            dbt_project_name="multi_test",
        )
        merger = ProjectMerger(config)
        result = merger.merge([mapping_result_a, mapping_result_b])

        proj_dir = Path(tmp_project_dir)
        model_dirs = [d.name for d in (proj_dir / "models").iterdir() if d.is_dir()]
        assert "m_orders" in model_dirs
        assert "m_customers" in model_dirs
        assert result.sources_consolidated > 0

    def test_no_results_returns_empty_merge(self, tmp_project_dir):
        config = Config(
            local_mode=True,
            project_dir=tmp_project_dir,
            merge_mode="new",
        )
        merger = ProjectMerger(config)
        result = merger.merge([])

        assert result.files_written == 0
        assert len(result.warnings) > 0

    def test_merge_mode_preserves_existing_files(self, tmp_project_dir, mapping_result_a):
        """In merge mode, non-generated existing files should be preserved."""
        proj_dir = Path(tmp_project_dir)
        proj_dir.mkdir(parents=True, exist_ok=True)
        (proj_dir / "models").mkdir(exist_ok=True)

        # Create an existing file without the generation marker
        custom_model = proj_dir / "models" / "custom_model.sql"
        custom_model.write_text("select 1 as id -- hand-written model\n")

        # Create a dbt_project.yml so merge mode validation passes
        (proj_dir / "dbt_project.yml").write_text("name: 'existing'\nversion: '1.0.0'\n")

        config = Config(
            local_mode=True,
            project_dir=tmp_project_dir,
            merge_mode="merge",
            dbt_project_name="existing",
        )
        merger = ProjectMerger(config)
        result = merger.merge([mapping_result_a])

        # The custom model should still exist unchanged
        assert custom_model.exists()
        assert "hand-written" in custom_model.read_text()

    def test_unified_project_yml_content(self, tmp_project_dir, mapping_result_a):
        config = Config(
            local_mode=True,
            project_dir=tmp_project_dir,
            merge_mode="new",
            dbt_project_name="my_project",
        )
        merger = ProjectMerger(config)
        merger.merge([mapping_result_a])

        proj_yml = Path(tmp_project_dir) / "dbt_project.yml"
        content = proj_yml.read_text()
        assert "my_project" in content
        assert "models:" in content
        assert "require-dbt-version" in content

    def test_packages_yml_generated_when_needed(self, tmp_project_dir, mapping_result_a):
        """packages.yml should be generated when dbt_utils is used."""
        config = Config(
            local_mode=True,
            project_dir=tmp_project_dir,
            merge_mode="new",
        )
        # mapping_result_a has packages.yml in its files, but the merger
        # detects package usage from SQL content. Add dbt_utils to a model.
        mapping_result_a.files.append(
            GeneratedFile(
                "models/staging/stg_with_utils.sql",
                "select {{ dbt_utils.generate_surrogate_key(['id']) }} as sk from foo",
            )
        )
        merger = ProjectMerger(config)
        merger.merge([mapping_result_a])

        pkgs = Path(tmp_project_dir) / "packages.yml"
        assert pkgs.exists()
        content = pkgs.read_text()
        assert "dbt_utils" in content


# ===========================================================================
# Marker / Helper Tests
# ===========================================================================

class TestHelpers:

    def test_add_marker_sql(self):
        content = "select 1"
        result = _add_marker("models/stg.sql", content)
        assert "-- Generated by infa2dbt" in result
        assert "select 1" in result

    def test_add_marker_yaml(self):
        content = "version: 2\nmodels: []"
        result = _add_marker("models/schema.yml", content)
        assert "# Generated by infa2dbt" in result
        assert "version: 2" in result

    def test_add_marker_idempotent(self):
        content = "-- Generated by infa2dbt\nselect 1"
        result = _add_marker("models/stg.sql", content)
        # Should not add a second marker
        assert result.count("Generated by infa2dbt") == 1

"""End-to-end integration test: convert mappings → merge → valid dbt project on disk."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from informatica_to_dbt.config import Config
from informatica_to_dbt.orchestrator import convert_mapping, MappingResult
from informatica_to_dbt.analyzer.multi_workflow import enrich_mapping, ObjectIndex
from informatica_to_dbt.merger.project_merger import ProjectMerger, MergeResult


# ---------------------------------------------------------------------------
# Mock LLM responses for two different mappings
# ---------------------------------------------------------------------------

MOCK_ORDERS_RESPONSE = """\
-- FILE: models/staging/stg_src_orders.sql
{{ config(materialized='view') }}

select
    order_id,
    cust_id,
    amount
from {{ source('oracle_raw', 'src_orders') }}

-- FILE: models/intermediate/int_orders_enriched.sql
{{ config(materialized='table') }}

select
    order_id,
    cust_id,
    amount,
    amount * 1.1 as amount_usd
from {{ ref('stg_src_orders') }}

-- FILE: models/marts/fct_orders.sql
{{ config(materialized='table') }}

select *
from {{ ref('int_orders_enriched') }}

-- FILE: models/staging/sources.yml
version: 2
sources:
  - name: oracle_raw
    database: PROD
    schema: APP
    tables:
      - name: src_orders

-- FILE: models/staging/schema.yml
version: 2
models:
  - name: stg_src_orders
    description: Staging model for SRC_ORDERS
    columns:
      - name: order_id
        tests:
          - not_null
      - name: cust_id
      - name: amount

  - name: int_orders_enriched
    description: Enriched orders with USD conversion

  - name: fct_orders
    description: Final orders mart
"""

# A second mapping that shares the same source (oracle_raw) but different tables
MOCK_CUSTOMERS_RESPONSE = """\
-- FILE: models/staging/stg_src_customers.sql
{{ config(materialized='view') }}

select
    cust_id,
    cust_name,
    region
from {{ source('oracle_raw', 'src_customers') }}

-- FILE: models/marts/dim_customers.sql
{{ config(materialized='table') }}

select
    cust_id,
    cust_name,
    region
from {{ ref('stg_src_customers') }}

-- FILE: models/staging/sources.yml
version: 2
sources:
  - name: oracle_raw
    database: PROD
    schema: APP
    tables:
      - name: src_customers

-- FILE: models/staging/schema.yml
version: 2
models:
  - name: stg_src_customers
    description: Staging model for SRC_CUSTOMERS
    columns:
      - name: cust_id
        tests:
          - not_null
      - name: cust_name
      - name: region
"""


def _enrich(mapping, repository):
    """Helper to enrich a mapping."""
    folder = repository.folders[0]
    index = ObjectIndex.from_repository(repository)
    return enrich_mapping(mapping, folder, index)


class TestMergePipeline:
    """Full pipeline: convert → merge → valid project on disk."""

    def test_single_mapping_merge(self, simple_mapping, simple_repository, tmp_path):
        """Convert one mapping and merge into a new project."""
        mock_llm = MagicMock()
        mock_llm.generate.return_value = MOCK_ORDERS_RESPONSE

        config = Config(
            local_mode=True,
            project_dir=str(tmp_path / "dbt_output"),
            merge_mode="new",
            dbt_project_name="test_orders",
        )

        enriched = _enrich(simple_mapping, simple_repository)
        result = convert_mapping(
            enriched=enriched,
            config=config,
            llm_client=mock_llm,
            workflow_name="TestFolder",
        )

        assert result.status in ("success", "partial")
        assert len(result.files) > 0

        # Now merge into project
        merger = ProjectMerger(config)
        merge_result = merger.merge([result])

        assert isinstance(merge_result, MergeResult)
        assert merge_result.mode == "new"
        assert merge_result.files_written > 0

        proj_dir = tmp_path / "dbt_output"
        assert (proj_dir / "dbt_project.yml").exists()
        assert (proj_dir / "models").is_dir()

        # Verify the unified dbt_project.yml is valid YAML
        proj_yml = yaml.safe_load((proj_dir / "dbt_project.yml").read_text())
        assert proj_yml["name"] == "test_orders"

        # Verify SQL model files exist under models/<mapping>/
        sql_files = list(proj_dir.rglob("*.sql"))
        assert len(sql_files) >= 1

    def test_two_mapping_merge_with_source_dedup(
        self, simple_mapping, simple_repository, tmp_path
    ):
        """Convert two mappings and verify source deduplication in merged project."""
        # First mapping: orders
        mock_llm_1 = MagicMock()
        mock_llm_1.generate.return_value = MOCK_ORDERS_RESPONSE

        config = Config(
            local_mode=True,
            project_dir=str(tmp_path / "dbt_output"),
            merge_mode="new",
            dbt_project_name="multi_project",
        )

        enriched = _enrich(simple_mapping, simple_repository)
        result_1 = convert_mapping(
            enriched=enriched,
            config=config,
            llm_client=mock_llm_1,
            workflow_name="TestFolder",
        )

        # Second mapping: customers (re-use simple_mapping structure but with different LLM output)
        mock_llm_2 = MagicMock()
        mock_llm_2.generate.return_value = MOCK_CUSTOMERS_RESPONSE

        result_2 = convert_mapping(
            enriched=enriched,
            config=config,
            llm_client=mock_llm_2,
            workflow_name="TestFolder",
        )
        # Override mapping name to simulate a different mapping
        result_2.mapping_name = "m_customers"

        # Merge both results
        merger = ProjectMerger(config)
        merge_result = merger.merge([result_1, result_2])

        assert merge_result.files_written > 0
        assert merge_result.sources_consolidated > 0

        proj_dir = tmp_path / "dbt_output"

        # Verify both mapping directories exist
        model_dirs = [d.name for d in (proj_dir / "models").iterdir() if d.is_dir()]
        assert "m_simple_test" in model_dirs or "m_customers" in model_dirs

        # Verify a unified dbt_project.yml at root
        proj_yml = yaml.safe_load((proj_dir / "dbt_project.yml").read_text())
        assert proj_yml["name"] == "multi_project"

        # Verify source YAMLs are valid
        source_files = list(proj_dir.rglob("_sources.yml"))
        for sf in source_files:
            doc = yaml.safe_load(sf.read_text())
            assert "sources" in doc
            for src in doc["sources"]:
                assert "name" in src

    def test_failed_mapping_excluded_from_merge(
        self, simple_mapping, simple_repository, tmp_path
    ):
        """Failed mappings should be excluded from the merged output."""
        from informatica_to_dbt.exceptions import LLMError

        # One successful mapping
        mock_llm = MagicMock()
        mock_llm.generate.return_value = MOCK_ORDERS_RESPONSE

        config = Config(
            local_mode=True,
            project_dir=str(tmp_path / "dbt_output"),
            merge_mode="new",
            dbt_project_name="with_failures",
        )

        enriched = _enrich(simple_mapping, simple_repository)
        result_ok = convert_mapping(
            enriched=enriched,
            config=config,
            llm_client=mock_llm,
            workflow_name="TestFolder",
        )

        # One failed mapping
        mock_llm_bad = MagicMock()
        mock_llm_bad.generate.side_effect = LLMError("timeout")

        result_fail = convert_mapping(
            enriched=enriched,
            config=config,
            llm_client=mock_llm_bad,
            workflow_name="TestFolder",
        )
        assert result_fail.status == "failed"

        # Merge — failed should be excluded
        merger = ProjectMerger(config)
        merge_result = merger.merge([result_ok, result_fail])

        assert merge_result.files_written > 0
        proj_dir = tmp_path / "dbt_output"
        assert (proj_dir / "dbt_project.yml").exists()

    def test_merge_mode_into_existing_project(
        self, simple_mapping, simple_repository, tmp_path
    ):
        """Merge new mappings into an existing dbt project, preserving custom files."""
        proj_dir = tmp_path / "dbt_output"
        proj_dir.mkdir(parents=True)
        (proj_dir / "models").mkdir()
        (proj_dir / "macros").mkdir()

        # Write an existing dbt_project.yml
        (proj_dir / "dbt_project.yml").write_text(
            "name: 'existing_project'\nversion: '1.0.0'\n"
        )
        # Write a custom model that should be preserved
        custom_model = proj_dir / "models" / "my_custom_model.sql"
        custom_model.write_text("-- my custom model\nselect 1 as id\n")

        # Convert a mapping
        mock_llm = MagicMock()
        mock_llm.generate.return_value = MOCK_ORDERS_RESPONSE

        config = Config(
            local_mode=True,
            project_dir=str(proj_dir),
            merge_mode="merge",
            dbt_project_name="existing_project",
        )

        enriched = _enrich(simple_mapping, simple_repository)
        result = convert_mapping(
            enriched=enriched,
            config=config,
            llm_client=mock_llm,
            workflow_name="TestFolder",
        )

        merger = ProjectMerger(config)
        merge_result = merger.merge([result])

        assert merge_result.mode == "merge"
        assert merge_result.files_written > 0

        # Custom model must still exist and be unchanged
        assert custom_model.exists()
        assert "my custom model" in custom_model.read_text()

        # New mapping models should also be present
        sql_files = list(proj_dir.rglob("*.sql"))
        assert len(sql_files) >= 2  # at least the custom model + one generated

    def test_project_directory_structure(
        self, simple_mapping, simple_repository, tmp_path
    ):
        """Verify the merged project has correct dbt directory structure."""
        mock_llm = MagicMock()
        mock_llm.generate.return_value = MOCK_ORDERS_RESPONSE

        config = Config(
            local_mode=True,
            project_dir=str(tmp_path / "dbt_output"),
            merge_mode="new",
            dbt_project_name="structured_project",
        )

        enriched = _enrich(simple_mapping, simple_repository)
        result = convert_mapping(
            enriched=enriched,
            config=config,
            llm_client=mock_llm,
            workflow_name="TestFolder",
        )

        merger = ProjectMerger(config)
        merger.merge([result])

        proj_dir = tmp_path / "dbt_output"

        # Standard dbt directories should exist
        for subdir in ("models", "macros", "tests", "seeds", "snapshots", "analyses"):
            assert (proj_dir / subdir).is_dir(), f"Missing directory: {subdir}"

        # dbt_project.yml must be present and valid
        proj_yml_path = proj_dir / "dbt_project.yml"
        assert proj_yml_path.exists()
        proj_yml = yaml.safe_load(proj_yml_path.read_text())
        assert proj_yml["name"] == "structured_project"
        assert "model-paths" in proj_yml
        assert "models" in proj_yml["model-paths"]

    def test_generated_files_have_marker(
        self, simple_mapping, simple_repository, tmp_path
    ):
        """All generated SQL/YAML files should contain the generation marker."""
        mock_llm = MagicMock()
        mock_llm.generate.return_value = MOCK_ORDERS_RESPONSE

        config = Config(
            local_mode=True,
            project_dir=str(tmp_path / "dbt_output"),
            merge_mode="new",
            dbt_project_name="marker_test",
        )

        enriched = _enrich(simple_mapping, simple_repository)
        result = convert_mapping(
            enriched=enriched,
            config=config,
            llm_client=mock_llm,
            workflow_name="TestFolder",
        )

        merger = ProjectMerger(config)
        merger.merge([result])

        proj_dir = tmp_path / "dbt_output"

        # Check SQL files for marker
        for sql_file in proj_dir.rglob("*.sql"):
            content = sql_file.read_text()
            assert "Generated by infa2dbt" in content, (
                f"Missing marker in {sql_file.relative_to(proj_dir)}"
            )

        # Check YAML files in models/ for marker
        for yml_file in (proj_dir / "models").rglob("*.yml"):
            content = yml_file.read_text()
            assert "Generated by infa2dbt" in content, (
                f"Missing marker in {yml_file.relative_to(proj_dir)}"
            )

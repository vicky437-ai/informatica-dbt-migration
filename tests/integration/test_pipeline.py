"""Integration test: end-to-end pipeline with mock LLM client."""

import pytest
from unittest.mock import MagicMock, patch

from informatica_to_dbt.config import Config
from informatica_to_dbt.orchestrator import convert_mapping, MappingResult
from informatica_to_dbt.analyzer.multi_workflow import enrich_mapping, ObjectIndex
from informatica_to_dbt.generator.prompt_builder import PromptPair


# A realistic LLM response matching what the prompt expects
MOCK_LLM_RESPONSE = """\
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


def _enrich(simple_mapping, simple_repository):
    """Helper to enrich a mapping with correct signature."""
    folder = simple_repository.folders[0]
    index = ObjectIndex.from_repository(simple_repository)
    return enrich_mapping(simple_mapping, folder, index)


class TestEndToEndPipeline:
    """Integration test using a mock LLM client."""

    def test_convert_simple_mapping(self, simple_mapping, simple_repository, config):
        """Test the full conversion pipeline with a mock LLM."""
        mock_llm = MagicMock()
        mock_llm.generate.return_value = MOCK_LLM_RESPONSE

        enriched = _enrich(simple_mapping, simple_repository)

        result = convert_mapping(
            enriched=enriched,
            config=config,
            llm_client=mock_llm,
            workflow_name="TestFolder",
        )

        assert isinstance(result, MappingResult)
        assert result.mapping_name == "m_simple_test"
        assert result.status in ("success", "partial")
        assert len(result.files) > 0

        assert mock_llm.generate.called

        sql_files = [f for f in result.files if f.is_sql]
        yaml_files = [f for f in result.files if f.is_yaml]
        assert len(sql_files) >= 1
        assert len(yaml_files) >= 1

        # Verify dbt_project.yml was generated
        project_files = [f for f in result.files if f.path == "dbt_project.yml"]
        assert len(project_files) == 1

        assert result.quality_report is not None
        assert result.quality_report.total_score > 0

        assert result.sql_validation is not None
        assert result.yaml_validation is not None
        assert result.project_validation is not None

    def test_convert_with_llm_error_returns_failed(self, simple_mapping, simple_repository, config):
        """Test that LLM errors are handled gracefully."""
        from informatica_to_dbt.exceptions import LLMError

        mock_llm = MagicMock()
        mock_llm.generate.side_effect = LLMError("API rate limit exceeded")

        enriched = _enrich(simple_mapping, simple_repository)

        result = convert_mapping(
            enriched=enriched,
            config=config,
            llm_client=mock_llm,
        )

        assert result.status == "failed"
        assert result.error_message is not None

    def test_self_healing_loop(self, simple_mapping, simple_repository, config):
        """Test that self-healing retries on validation errors."""
        bad_response = """\
-- FILE: models/staging/stg_src_orders.sql
{{ config(materialized='view') }}
SELECT IIF(x=1, 'Y', 'N') FROM {{ source('oracle_raw', 'src_orders') }};
"""
        mock_llm = MagicMock()
        mock_llm.generate.side_effect = [bad_response, MOCK_LLM_RESPONSE]

        enriched = _enrich(simple_mapping, simple_repository)

        result = convert_mapping(
            enriched=enriched,
            config=config,
            llm_client=mock_llm,
        )

        # LLM should have been called at least twice (initial + heal)
        assert mock_llm.generate.call_count >= 2
        assert result.heal_attempts >= 1

    def test_mapping_result_elapsed_time(self, simple_mapping, simple_repository, config):
        """Test that elapsed time is tracked."""
        mock_llm = MagicMock()
        mock_llm.generate.return_value = MOCK_LLM_RESPONSE

        enriched = _enrich(simple_mapping, simple_repository)

        result = convert_mapping(
            enriched=enriched,
            config=config,
            llm_client=mock_llm,
        )

        assert result.elapsed_seconds > 0

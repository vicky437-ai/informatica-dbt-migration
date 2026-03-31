"""Unit tests for all three validators (SQL, YAML, project)."""

import pytest
from informatica_to_dbt.generator.response_parser import GeneratedFile
from informatica_to_dbt.validator.sql_validator import validate_sql, SQLValidationResult
from informatica_to_dbt.validator.yaml_validator import validate_yaml, YAMLValidationResult
from informatica_to_dbt.validator.project_validator import validate_project, ProjectValidationResult


# ---------------------------------------------------------------------------
# SQL Validator
# ---------------------------------------------------------------------------

class TestSQLValidator:
    def test_clean_file_no_errors(self):
        files = [GeneratedFile("models/staging/stg_a.sql", (
            "{{ config(materialized='view') }}\n"
            "SELECT id, name\n"
            "FROM {{ source('raw', 'tbl') }}\n"
        ))]
        result = validate_sql(files)
        assert result.error_count == 0

    def test_detects_trailing_semicolon(self):
        files = [GeneratedFile("models/staging/stg_a.sql", (
            "{{ config(materialized='view') }}\n"
            "SELECT * FROM {{ source('raw', 'tbl') }};\n"
        ))]
        result = validate_sql(files)
        assert result.error_count > 0
        assert any("semicolon" in i.message.lower() for i in result.issues)

    def test_detects_hardcoded_three_part_name(self):
        files = [GeneratedFile("models/staging/stg_a.sql", (
            "{{ config(materialized='view') }}\n"
            "SELECT * FROM PROD.APP.ORDERS\n"
        ))]
        result = validate_sql(files)
        assert any("ardcoded" in i.message for i in result.issues)

    def test_detects_informatica_functions(self):
        files = [GeneratedFile("models/staging/stg_a.sql", (
            "{{ config(materialized='view') }}\n"
            "SELECT IIF(x=1, 'Y', 'N'), ISNULL(col)\n"
            "FROM {{ source('raw', 'tbl') }}\n"
        ))]
        result = validate_sql(files)
        assert any("Informatica function" in i.message for i in result.issues)

    def test_detects_informatica_patterns(self):
        files = [GeneratedFile("models/staging/stg_a.sql", (
            "{{ config(materialized='view') }}\n"
            "SELECT $$PARAM_VAL, :LKP.lookup1(x)\n"
            "FROM {{ source('raw', 'tbl') }}\n"
        ))]
        result = validate_sql(files)
        assert any("pattern" in i.message.lower() for i in result.issues)

    def test_detects_unbalanced_parens(self):
        files = [GeneratedFile("models/staging/stg_a.sql", (
            "{{ config(materialized='view') }}\n"
            "SELECT COALESCE(a, (b + c)\n"
            "FROM {{ source('raw', 'tbl') }}\n"
        ))]
        result = validate_sql(files)
        assert any("parenthes" in i.message.lower() for i in result.issues)

    def test_detects_unbalanced_jinja(self):
        files = [GeneratedFile("models/staging/stg_a.sql", (
            "{{ config(materialized='view') }}\n"
            "SELECT * FROM {{ ref('a') }\n"
        ))]
        result = validate_sql(files)
        assert any("jinja" in i.message.lower() for i in result.issues)

    def test_warns_no_ref_or_source(self):
        files = [GeneratedFile("models/staging/stg_a.sql", (
            "{{ config(materialized='view') }}\n"
            "SELECT * FROM my_table\n"
        ))]
        result = validate_sql(files)
        assert any("ref()" in i.message for i in result.issues)

    def test_skips_macro_files(self):
        files = [GeneratedFile("macros/helpers.sql", (
            "{% macro my_macro() %}\nSELECT 1\n{% endmacro %}\n"
        ))]
        result = validate_sql(files)
        assert result.error_count == 0

    def test_empty_files_list(self):
        result = validate_sql([])
        assert result.is_valid is True


# ---------------------------------------------------------------------------
# YAML Validator
# ---------------------------------------------------------------------------

class TestYAMLValidator:
    def test_valid_yaml_no_errors(self):
        files = [GeneratedFile("models/staging/schema.yml", (
            "version: 2\nmodels:\n  - name: stg_a\n"
        ))]
        result = validate_yaml(files)
        assert result.error_count == 0

    def test_detects_invalid_yaml(self):
        files = [GeneratedFile("models/staging/schema.yml", (
            "version: 2\nmodels:\n  - name: stg_a\n    bad_indent:\n  wrong\n"
        ))]
        result = validate_yaml(files)
        assert result.error_count > 0

    def test_empty_files_list(self):
        result = validate_yaml([])
        assert result.is_valid is True


# ---------------------------------------------------------------------------
# Project Validator
# ---------------------------------------------------------------------------

class TestProjectValidator:
    def test_clean_project_no_errors(self, clean_dbt_files):
        result = validate_project(clean_dbt_files)
        assert result.error_count == 0

    def test_detects_broken_ref(self):
        files = [
            GeneratedFile("models/staging/stg_a.sql", (
                "{{ config(materialized='view') }}\n"
                "SELECT * FROM {{ ref('nonexistent_model') }}\n"
            )),
        ]
        result = validate_project(files)
        assert any("nonexistent_model" in i.message for i in result.issues)
        assert result.error_count > 0

    def test_detects_dag_cycle(self):
        files = [
            GeneratedFile("models/staging/model_a.sql", (
                "SELECT * FROM {{ ref('model_b') }}\n"
            )),
            GeneratedFile("models/staging/model_b.sql", (
                "SELECT * FROM {{ ref('model_a') }}\n"
            )),
        ]
        result = validate_project(files)
        assert any("ircular" in i.message for i in result.issues)

    def test_detects_undefined_source(self):
        files = [
            GeneratedFile("models/staging/stg_a.sql", (
                "SELECT * FROM {{ source('unknown_src', 'unknown_tbl') }}\n"
            )),
            GeneratedFile("models/staging/sources.yml", (
                "version: 2\nsources:\n  - name: raw\n    tables:\n      - name: tbl\n"
            )),
        ]
        result = validate_project(files)
        assert any("not defined" in i.message for i in result.issues)

    def test_detects_duplicate_models(self, problematic_dbt_files):
        result = validate_project(problematic_dbt_files)
        assert any("uplicate" in i.message for i in result.issues)

    def test_detects_wrong_layer_prefix(self, problematic_dbt_files):
        result = validate_project(problematic_dbt_files)
        assert any("prefix" in i.message.lower() for i in result.issues)

    def test_detects_schema_sql_mismatch(self, problematic_dbt_files):
        result = validate_project(problematic_dbt_files)
        # stg_phantom in schema.yml but no SQL file
        assert any("stg_phantom" in i.message for i in result.issues)

    def test_detects_orphan_models(self, problematic_dbt_files):
        result = validate_project(problematic_dbt_files)
        assert any("orphan" in i.message.lower() for i in result.issues)

    def test_empty_files_list(self):
        result = validate_project([])
        assert result.is_valid is True
        assert result.error_count == 0

    def test_is_valid_property(self, clean_dbt_files):
        result = validate_project(clean_dbt_files)
        assert result.is_valid is True

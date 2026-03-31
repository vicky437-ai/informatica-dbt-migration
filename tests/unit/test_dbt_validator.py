"""Unit tests for the dbt validation module."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from informatica_to_dbt.validation.dbt_validator import (
    DbtMessage,
    DbtValidationResult,
    DbtValidator,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def valid_project(tmp_path):
    """Create a minimal valid dbt project structure."""
    (tmp_path / "dbt_project.yml").write_text("name: test_project\n", encoding="utf-8")
    (tmp_path / "profiles.yml").write_text("test_project:\n  target: dev\n", encoding="utf-8")
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "example.sql").write_text("SELECT 1", encoding="utf-8")
    return str(tmp_path)


@pytest.fixture
def validator(valid_project):
    return DbtValidator(project_dir=valid_project)


# ---------------------------------------------------------------------------
# DbtMessage
# ---------------------------------------------------------------------------

class TestDbtMessage:
    def test_to_dict(self):
        msg = DbtMessage(level="error", message="Something broke", file_path="models/foo.sql", line_number=10)
        d = msg.to_dict()
        assert d["level"] == "error"
        assert d["message"] == "Something broke"
        assert d["file_path"] == "models/foo.sql"
        assert d["line_number"] == 10

    def test_defaults(self):
        msg = DbtMessage()
        assert msg.level == "info"
        assert msg.message == ""
        assert msg.file_path is None


# ---------------------------------------------------------------------------
# DbtValidationResult
# ---------------------------------------------------------------------------

class TestDbtValidationResult:
    def test_errors_and_warnings(self):
        result = DbtValidationResult(
            messages=[
                DbtMessage(level="error", message="e1"),
                DbtMessage(level="warn", message="w1"),
                DbtMessage(level="info", message="i1"),
                DbtMessage(level="error", message="e2"),
            ]
        )
        assert len(result.errors) == 2
        assert len(result.warnings) == 1

    def test_summary_compile(self):
        result = DbtValidationResult(
            command="dbt compile",
            success=True,
            models_ok=10,
            elapsed_seconds=2.5,
        )
        s = result.summary()
        assert "PASS" in s
        assert "10" in s

    def test_summary_run_fail(self):
        result = DbtValidationResult(
            command="dbt run",
            success=False,
            models_ok=8,
            models_error=2,
            elapsed_seconds=15.0,
            messages=[DbtMessage(level="error", message="Model X failed")],
        )
        s = result.summary()
        assert "FAIL" in s
        assert "Model X failed" in s

    def test_summary_test(self):
        result = DbtValidationResult(
            command="dbt test",
            success=True,
            tests_pass=50,
            tests_fail=0,
            elapsed_seconds=10.0,
        )
        s = result.summary()
        assert "50" in s


# ---------------------------------------------------------------------------
# DbtValidator — project structure
# ---------------------------------------------------------------------------

class TestDbtValidatorStructure:
    def test_valid_project_passes(self, valid_project):
        v = DbtValidator(project_dir=valid_project)
        errors = v.validate_project_structure()
        assert errors == []

    def test_missing_dbt_project_yml(self, tmp_path):
        (tmp_path / "models").mkdir()
        (tmp_path / "profiles.yml").write_text("x:\n", encoding="utf-8")
        v = DbtValidator(project_dir=str(tmp_path))
        errors = v.validate_project_structure()
        assert any("dbt_project.yml" in e for e in errors)

    def test_missing_models_dir(self, tmp_path):
        (tmp_path / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
        (tmp_path / "profiles.yml").write_text("x:\n", encoding="utf-8")
        v = DbtValidator(project_dir=str(tmp_path))
        errors = v.validate_project_structure()
        assert any("models" in e for e in errors)

    def test_missing_profiles_yml(self, tmp_path):
        (tmp_path / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
        (tmp_path / "models").mkdir()
        v = DbtValidator(project_dir=str(tmp_path))
        errors = v.validate_project_structure()
        assert any("profiles.yml" in e for e in errors)

    def test_nonexistent_directory(self, tmp_path):
        v = DbtValidator(project_dir=str(tmp_path / "nope"))
        errors = v.validate_project_structure()
        assert any("does not exist" in e for e in errors)

    def test_custom_profiles_dir(self, tmp_path):
        project = tmp_path / "project"
        profiles = tmp_path / "profiles"
        project.mkdir()
        profiles.mkdir()
        (project / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
        (project / "models").mkdir()
        (profiles / "profiles.yml").write_text("x:\n", encoding="utf-8")

        v = DbtValidator(project_dir=str(project), profiles_dir=str(profiles))
        errors = v.validate_project_structure()
        assert errors == []


# ---------------------------------------------------------------------------
# DbtValidator — subprocess execution (mocked)
# ---------------------------------------------------------------------------

class TestDbtValidatorExecution:
    @patch("subprocess.run")
    def test_compile_success(self, mock_run, validator):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Finished running 10 view models in 5s\n",
            stderr="",
        )
        result = validator.compile()
        assert result.success
        assert result.command == "dbt compile"
        assert result.models_ok == 10

    @patch("subprocess.run")
    def test_compile_failure(self, mock_run, validator):
        mock_run.return_value = MagicMock(
            returncode=2,
            stdout="",
            stderr="Compilation Error in model foo\n  Missing source",
        )
        result = validator.compile()
        assert not result.success
        assert len(result.errors) > 0

    @patch("subprocess.run")
    def test_run_with_select(self, mock_run, validator):
        mock_run.return_value = MagicMock(returncode=0, stdout="Done", stderr="")
        validator.run(select="tag:my_mapping")
        args = mock_run.call_args[0][0]
        assert "--select" in args
        assert "tag:my_mapping" in args

    @patch("subprocess.run")
    def test_run_with_full_refresh(self, mock_run, validator):
        mock_run.return_value = MagicMock(returncode=0, stdout="Done", stderr="")
        validator.run(full_refresh=True)
        args = mock_run.call_args[0][0]
        assert "--full-refresh" in args

    @patch("subprocess.run")
    def test_deps(self, mock_run, validator):
        mock_run.return_value = MagicMock(returncode=0, stdout="Installed packages", stderr="")
        result = validator.deps()
        assert result.success
        assert result.command == "dbt deps"

    @patch("subprocess.run")
    def test_test_command(self, mock_run, validator):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Done. PASS=50 WARN=2 ERROR=0 SKIP=0 TOTAL=52\n",
            stderr="",
        )
        result = validator.test()
        assert result.success
        assert result.tests_pass == 50
        assert result.tests_warn == 2

    @patch("subprocess.run", side_effect=FileNotFoundError)
    def test_dbt_not_found(self, mock_run, validator):
        result = validator.compile()
        assert not result.success
        assert result.return_code == 127
        assert "not found" in result.stderr

    @patch("subprocess.run")
    def test_parse_stats_pass_error_skip(self, mock_run, validator):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Done. PASS=45 WARN=3 ERROR=2 SKIP=1 TOTAL=51\n",
            stderr="",
        )
        result = validator.test()
        assert result.tests_pass == 45
        assert result.tests_warn == 3
        assert result.tests_error == 2
        assert result.tests_skip == 1

    @patch("subprocess.run")
    def test_parse_completed_with_errors(self, mock_run, validator):
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="Completed with 3 errors and 1 warnings\n",
            stderr="",
        )
        result = validator.run()
        assert result.models_error == 3

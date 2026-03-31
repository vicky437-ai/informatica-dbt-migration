"""Unit tests for the Snowflake deployment module."""

from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from informatica_to_dbt.deployment.deployer import Deployer, DeployResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def project_dir(tmp_path):
    (tmp_path / "dbt_project.yml").write_text("name: test\n", encoding="utf-8")
    (tmp_path / "models").mkdir()
    return str(tmp_path)


@pytest.fixture
def deployer(project_dir):
    return Deployer(
        project_dir=project_dir,
        database="TEST_DB",
        schema="TEST_SCHEMA",
        project_name="TEST_PROJECT",
        warehouse="TEST_WH",
        connection="myconn",
    )


# ---------------------------------------------------------------------------
# DeployResult
# ---------------------------------------------------------------------------

class TestDeployResult:
    def test_output_combined(self):
        r = DeployResult(stdout="ok", stderr="warn")
        assert "ok" in r.output
        assert "warn" in r.output

    def test_summary_success(self):
        r = DeployResult(
            mode="direct",
            success=True,
            project_fqn="DB.S.P",
            elapsed_seconds=3.0,
        )
        s = r.summary()
        assert "SUCCESS" in s
        assert "DB.S.P" in s

    def test_summary_failure(self):
        r = DeployResult(
            mode="git",
            success=False,
            stderr="Connection refused",
            elapsed_seconds=1.0,
        )
        s = r.summary()
        assert "FAILED" in s
        assert "Connection refused" in s


# ---------------------------------------------------------------------------
# Deployer — properties
# ---------------------------------------------------------------------------

class TestDeployerProperties:
    def test_project_fqn(self, deployer):
        assert deployer.project_fqn == "TEST_DB.TEST_SCHEMA.TEST_PROJECT"


# ---------------------------------------------------------------------------
# Deployer — direct deployment
# ---------------------------------------------------------------------------

class TestDeployDirect:
    @patch("subprocess.run")
    def test_deploy_direct_success(self, mock_run, deployer):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Project deployed successfully\n",
            stderr="",
        )
        result = deployer.deploy_direct()
        assert result.success
        assert result.mode == "direct"
        assert result.project_fqn == "TEST_DB.TEST_SCHEMA.TEST_PROJECT"

        # Verify snow command was called correctly
        args = mock_run.call_args[0][0]
        assert args[0] == "snow"
        assert "dbt" in args
        assert "deploy" in args
        assert "--connection" in args
        assert "myconn" in args

    @patch("subprocess.run")
    def test_deploy_direct_failure(self, mock_run, deployer):
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="Deployment failed: authentication error",
        )
        result = deployer.deploy_direct()
        assert not result.success
        assert "authentication error" in result.stderr

    @patch("subprocess.run", side_effect=FileNotFoundError)
    def test_snow_not_found(self, mock_run, deployer):
        result = deployer.deploy_direct()
        assert not result.success
        assert result.return_code == 127
        assert "snow CLI not found" in result.stderr


# ---------------------------------------------------------------------------
# Deployer — Git deployment SQL generation
# ---------------------------------------------------------------------------

class TestDeployGitSQL:
    def test_generate_basic_git_sql(self, deployer):
        stmts = deployer.generate_git_deploy_sql(
            git_repo_name="MY_REPO",
            git_url="https://github.com/org/repo.git",
            git_branch="main",
        )
        # Should have: CREATE GIT REPOSITORY, ALTER FETCH, CREATE DBT PROJECT
        assert len(stmts) == 3
        assert "CREATE OR REPLACE GIT REPOSITORY" in stmts[0]
        assert "MY_REPO" in stmts[0]
        assert "FETCH" in stmts[1]
        assert "CREATE OR REPLACE DBT PROJECT" in stmts[2]
        assert "branches/main" in stmts[2]

    def test_generate_git_sql_with_integration(self, deployer):
        stmts = deployer.generate_git_deploy_sql(
            git_repo_name="MY_REPO",
            git_url="https://github.com/org/repo.git",
            api_integration_name="MY_INTEGRATION",
        )
        # Should have 4 statements: API INTEGRATION, GIT REPO, FETCH, DBT PROJECT
        assert len(stmts) == 4
        assert "CREATE OR REPLACE API INTEGRATION" in stmts[0]
        assert "MY_INTEGRATION" in stmts[0]
        assert "API_INTEGRATION" in stmts[1]

    def test_generate_git_sql_with_secret(self, deployer):
        stmts = deployer.generate_git_deploy_sql(
            git_repo_name="MY_REPO",
            git_url="https://github.com/org/repo.git",
            git_secret_name="MY_SECRET",
        )
        assert any("GIT_CREDENTIALS" in s for s in stmts)

    def test_generate_git_sql_with_subpath(self, deployer):
        stmts = deployer.generate_git_deploy_sql(
            git_repo_name="MY_REPO",
            git_url="https://github.com/org/repo.git",
            dbt_subpath="dbt_project/",
        )
        assert any("dbt_project" in s for s in stmts)

    def test_git_sql_uses_correct_database_schema(self, deployer):
        stmts = deployer.generate_git_deploy_sql(
            git_repo_name="MY_REPO",
            git_url="https://github.com/org/repo.git",
        )
        assert any("TEST_DB.TEST_SCHEMA.MY_REPO" in s for s in stmts)
        assert any("TEST_DB.TEST_SCHEMA.TEST_PROJECT" in s for s in stmts)


# ---------------------------------------------------------------------------
# Deployer — Git deployment execution
# ---------------------------------------------------------------------------

class TestDeployFromGit:
    @patch("subprocess.run")
    def test_deploy_from_git_success(self, mock_run, deployer):
        mock_run.return_value = MagicMock(returncode=0, stdout="OK\n", stderr="")
        result = deployer.deploy_from_git(
            git_repo_name="MY_REPO",
            git_url="https://github.com/org/repo.git",
        )
        assert result.success
        assert result.mode == "git"
        assert result.git_repo_name == "MY_REPO"
        assert len(result.sql_statements) == 3

    @patch("subprocess.run")
    def test_deploy_from_git_failure(self, mock_run, deployer):
        # First SQL succeeds, second fails
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="OK\n", stderr=""),
            MagicMock(returncode=1, stdout="", stderr="SQL compilation error"),
        ]
        result = deployer.deploy_from_git(
            git_repo_name="MY_REPO",
            git_url="https://github.com/org/repo.git",
        )
        assert not result.success
        assert "SQL compilation error" in result.stderr


# ---------------------------------------------------------------------------
# Deployer — schedule SQL generation
# ---------------------------------------------------------------------------

class TestDeployScheduleSQL:
    def test_generate_schedule_sql(self, deployer):
        stmts = deployer.generate_schedule_sql(
            cron="0 2 * * *",
            timezone="America/New_York",
        )
        assert len(stmts) == 2
        assert "CREATE OR REPLACE TASK" in stmts[0]
        assert "CRON 0 2 * * * America/New_York" in stmts[0]
        assert "EXECUTE DBT PROJECT" in stmts[0]
        assert "RESUME" in stmts[1]

    def test_schedule_sql_custom_task_name(self, deployer):
        stmts = deployer.generate_schedule_sql(task_name="my_custom_task")
        assert any("my_custom_task" in s for s in stmts)

    def test_schedule_sql_custom_dbt_args(self, deployer):
        stmts = deployer.generate_schedule_sql(dbt_args="test")
        assert any("ARGS = 'test'" in s for s in stmts)

    def test_schedule_sql_default_task_name(self, deployer):
        stmts = deployer.generate_schedule_sql()
        assert any("dbt_test_project_task" in s for s in stmts)

    def test_schedule_sql_uses_warehouse(self, deployer):
        stmts = deployer.generate_schedule_sql()
        assert any("TEST_WH" in s for s in stmts)


# ---------------------------------------------------------------------------
# Deployer — schedule deployment
# ---------------------------------------------------------------------------

class TestDeployWithSchedule:
    @patch("subprocess.run")
    def test_deploy_schedule_success(self, mock_run, deployer):
        mock_run.return_value = MagicMock(returncode=0, stdout="Task created\n", stderr="")
        result = deployer.deploy_with_schedule()
        assert result.success
        assert result.mode == "schedule"
        assert "task" in result.task_name.lower()

    @patch("subprocess.run")
    def test_deploy_schedule_failure(self, mock_run, deployer):
        mock_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="Insufficient privileges"
        )
        result = deployer.deploy_with_schedule()
        assert not result.success


# ---------------------------------------------------------------------------
# Deployer — execute
# ---------------------------------------------------------------------------

class TestDeployExecute:
    @patch("subprocess.run")
    def test_execute_run(self, mock_run, deployer):
        mock_run.return_value = MagicMock(returncode=0, stdout="Executed\n", stderr="")
        result = deployer.execute(dbt_args="run")
        assert result.success
        assert result.mode == "execute"
        assert "EXECUTE DBT PROJECT" in result.sql_statements[0]

    @patch("subprocess.run")
    def test_execute_test(self, mock_run, deployer):
        mock_run.return_value = MagicMock(returncode=0, stdout="Tests passed\n", stderr="")
        result = deployer.execute(dbt_args="test")
        assert result.success
        assert "'test'" in result.sql_statements[0]

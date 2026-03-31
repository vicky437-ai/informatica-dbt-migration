"""Snowflake deployment — direct (``snow dbt deploy``), Git-based, and TASK scheduling.

Three deployment modes:

1. **Direct** — uses ``snow dbt deploy`` to push a local dbt project to Snowflake.
2. **Git** — creates a Snowflake Git Repository integration and deploys from Git.
3. **Schedule** — creates a Snowflake TASK to run the dbt project on a cron schedule.
"""

from __future__ import annotations

import logging
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger("informatica_dbt.deployment")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class DeployResult:
    """Result from a deployment operation."""

    mode: str = ""                # direct | git | schedule
    success: bool = False
    return_code: int = -1
    stdout: str = ""
    stderr: str = ""
    elapsed_seconds: float = 0.0

    # Snowflake object references
    project_fqn: str = ""        # e.g. DB.SCHEMA.PROJECT_NAME
    git_repo_name: str = ""      # e.g. my_git_repo
    task_name: str = ""          # e.g. dbt_nightly_run

    # SQL statements generated (for review)
    sql_statements: list[str] = field(default_factory=list)

    @property
    def output(self) -> str:
        return (self.stdout + "\n" + self.stderr).strip()

    def summary(self) -> str:
        lines = [f"Deploy ({self.mode}) — {'SUCCESS' if self.success else 'FAILED'} ({self.elapsed_seconds:.1f}s)"]
        if self.project_fqn:
            lines.append(f"  Project: {self.project_fqn}")
        if self.git_repo_name:
            lines.append(f"  Git repo: {self.git_repo_name}")
        if self.task_name:
            lines.append(f"  Task: {self.task_name}")
        if not self.success and self.stderr:
            lines.append(f"  Error: {self.stderr[:200]}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Deployer
# ---------------------------------------------------------------------------

class Deployer:
    """Deploy dbt projects to Snowflake.

    Parameters
    ----------
    project_dir : str
        Path to the local dbt project directory.
    database : str
        Snowflake database for the dbt project object.
    schema : str
        Snowflake schema for the dbt project object.
    project_name : str
        Name for the Snowflake dbt project object.
    warehouse : str
        Snowflake warehouse for execution.
    connection : str | None
        Snowflake CLI connection name (from ``connections.toml``).
    snow_executable : str
        Path or name of the ``snow`` CLI binary.
    """

    def __init__(
        self,
        project_dir: str,
        database: str = "TPC_DI_RAW_DATA",
        schema: str = "INFORMATICA_TO_DBT",
        project_name: str = "INFORMATICA_TO_DBT",
        warehouse: str = "SMALL_WH",
        connection: Optional[str] = None,
        snow_executable: str = "snow",
    ) -> None:
        self.project_dir = Path(project_dir).resolve()
        self.database = database
        self.schema = schema
        self.project_name = project_name
        self.warehouse = warehouse
        self.connection = connection
        self.snow_executable = snow_executable

    @property
    def project_fqn(self) -> str:
        """Fully qualified project name."""
        return f"{self.database}.{self.schema}.{self.project_name}"

    # ------------------------------------------------------------------
    # Direct deployment (snow dbt deploy)
    # ------------------------------------------------------------------

    def deploy_direct(self) -> DeployResult:
        """Deploy using ``snow dbt deploy`` CLI.

        Equivalent to:
            snow dbt deploy --source ./project_dir \\
                --database DB --schema SCHEMA PROJECT_NAME
        """
        result = DeployResult(mode="direct", project_fqn=self.project_fqn)
        start = time.time()

        cmd = [
            self.snow_executable, "dbt", "deploy",
            "--source", str(self.project_dir),
            "--database", self.database,
            "--schema", self.schema,
            self.project_name,
        ]
        if self.connection:
            cmd.extend(["--connection", self.connection])

        result.sql_statements = [" ".join(cmd)]
        logger.info("Running: %s", " ".join(cmd))

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,
            )
            result.stdout = proc.stdout
            result.stderr = proc.stderr
            result.return_code = proc.returncode
            result.success = proc.returncode == 0
        except FileNotFoundError:
            result.stderr = (
                f"snow CLI not found: '{self.snow_executable}'. "
                "Install Snowflake CLI: pip install snowflake-cli"
            )
            result.return_code = 127
        except subprocess.TimeoutExpired:
            result.stderr = "snow dbt deploy timed out after 300 seconds"
            result.return_code = 124
        except Exception as exc:
            result.stderr = f"Failed to run snow dbt deploy: {exc}"
            result.return_code = 1

        result.elapsed_seconds = time.time() - start
        return result

    # ------------------------------------------------------------------
    # Git-based deployment
    # ------------------------------------------------------------------

    def generate_git_deploy_sql(
        self,
        git_repo_name: str,
        git_url: str,
        git_branch: str = "main",
        git_secret_name: Optional[str] = None,
        api_integration_name: Optional[str] = None,
        dbt_subpath: str = "",
    ) -> list[str]:
        """Generate SQL statements for Git-based deployment.

        Returns a list of SQL statements that:
        1. Create an API integration (if specified)
        2. Create a Git repository
        3. Fetch from Git
        4. Create or replace the dbt project from the Git repo

        Parameters
        ----------
        git_repo_name : str
            Name for the Snowflake GIT REPOSITORY object.
        git_url : str
            HTTPS URL of the Git repository.
        git_branch : str
            Branch to deploy from.
        git_secret_name : str | None
            Name of the Snowflake SECRET for Git authentication.
        api_integration_name : str | None
            Name of the API INTEGRATION for Git.
        dbt_subpath : str
            Subdirectory within the repo containing the dbt project.
        """
        stmts: list[str] = []

        # API integration (if specified)
        if api_integration_name:
            stmts.append(
                f"CREATE OR REPLACE API INTEGRATION {api_integration_name}\n"
                f"  API_PROVIDER = git_https_api\n"
                f"  API_ALLOWED_PREFIXES = ('{git_url}')\n"
                f"  ENABLED = TRUE;"
            )

        # Git repository
        git_repo_sql = (
            f"CREATE OR REPLACE GIT REPOSITORY {self.database}.{self.schema}.{git_repo_name}\n"
            f"  ORIGIN = '{git_url}'"
        )
        if api_integration_name:
            git_repo_sql += f"\n  API_INTEGRATION = {api_integration_name}"
        if git_secret_name:
            git_repo_sql += f"\n  GIT_CREDENTIALS = {git_secret_name}"
        git_repo_sql += ";"
        stmts.append(git_repo_sql)

        # Fetch
        stmts.append(
            f"ALTER GIT REPOSITORY {self.database}.{self.schema}.{git_repo_name} FETCH;"
        )

        # Deploy from Git
        stage_path = f"@{self.database}.{self.schema}.{git_repo_name}/branches/{git_branch}"
        if dbt_subpath:
            stage_path += f"/{dbt_subpath.strip('/')}"

        stmts.append(
            f"CREATE OR REPLACE DBT PROJECT {self.project_fqn}\n"
            f"  FROM '{stage_path}';"
        )

        return stmts

    def deploy_from_git(
        self,
        git_repo_name: str,
        git_url: str,
        git_branch: str = "main",
        git_secret_name: Optional[str] = None,
        api_integration_name: Optional[str] = None,
        dbt_subpath: str = "",
    ) -> DeployResult:
        """Deploy from a Git repository via Snowflake Git integration.

        Generates and returns the SQL statements.  Actual execution requires
        a Snowflake connection (via ``snow sql`` or Snowpark).
        """
        result = DeployResult(
            mode="git",
            project_fqn=self.project_fqn,
            git_repo_name=git_repo_name,
        )
        start = time.time()

        stmts = self.generate_git_deploy_sql(
            git_repo_name=git_repo_name,
            git_url=git_url,
            git_branch=git_branch,
            git_secret_name=git_secret_name,
            api_integration_name=api_integration_name,
            dbt_subpath=dbt_subpath,
        )
        result.sql_statements = stmts

        # Execute each SQL statement via snow sql
        for stmt in stmts:
            exec_result = self._run_sql(stmt)
            if not exec_result.success:
                result.success = False
                result.stderr = exec_result.stderr
                result.return_code = exec_result.return_code
                result.elapsed_seconds = time.time() - start
                return result

        result.success = True
        result.return_code = 0
        result.elapsed_seconds = time.time() - start
        return result

    # ------------------------------------------------------------------
    # TASK scheduling
    # ------------------------------------------------------------------

    def generate_schedule_sql(
        self,
        cron: str = "0 2 * * *",
        timezone: str = "America/New_York",
        task_name: Optional[str] = None,
        dbt_args: str = "run",
    ) -> list[str]:
        """Generate SQL for a Snowflake TASK to run the dbt project on schedule.

        Parameters
        ----------
        cron : str
            Cron expression (5-field).
        timezone : str
            IANA timezone for the schedule.
        task_name : str | None
            Name for the TASK object.  Defaults to ``dbt_<project_name>_task``.
        dbt_args : str
            Arguments to pass to ``EXECUTE DBT PROJECT`` (e.g. "run", "test").
        """
        task_name = task_name or f"dbt_{self.project_name.lower()}_task"

        stmts = [
            (
                f"CREATE OR REPLACE TASK {self.database}.{self.schema}.{task_name}\n"
                f"  WAREHOUSE = {self.warehouse}\n"
                f"  SCHEDULE = 'USING CRON {cron} {timezone}'\n"
                f"AS\n"
                f"  EXECUTE DBT PROJECT {self.project_fqn} ARGS = '{dbt_args}';"
            ),
            f"ALTER TASK {self.database}.{self.schema}.{task_name} RESUME;",
        ]
        return stmts

    def deploy_with_schedule(
        self,
        cron: str = "0 2 * * *",
        timezone: str = "America/New_York",
        task_name: Optional[str] = None,
        dbt_args: str = "run",
    ) -> DeployResult:
        """Create a TASK to run the dbt project on a schedule."""
        task_name = task_name or f"dbt_{self.project_name.lower()}_task"
        result = DeployResult(
            mode="schedule",
            project_fqn=self.project_fqn,
            task_name=f"{self.database}.{self.schema}.{task_name}",
        )
        start = time.time()

        stmts = self.generate_schedule_sql(
            cron=cron,
            timezone=timezone,
            task_name=task_name,
            dbt_args=dbt_args,
        )
        result.sql_statements = stmts

        for stmt in stmts:
            exec_result = self._run_sql(stmt)
            if not exec_result.success:
                result.success = False
                result.stderr = exec_result.stderr
                result.return_code = exec_result.return_code
                result.elapsed_seconds = time.time() - start
                return result

        result.success = True
        result.return_code = 0
        result.elapsed_seconds = time.time() - start
        return result

    # ------------------------------------------------------------------
    # Execute dbt project
    # ------------------------------------------------------------------

    def execute(self, dbt_args: str = "run") -> DeployResult:
        """Execute the dbt project on Snowflake.

        Runs: EXECUTE DBT PROJECT <fqn> ARGS = '<dbt_args>';
        """
        result = DeployResult(mode="execute", project_fqn=self.project_fqn)
        start = time.time()

        sql = f"EXECUTE DBT PROJECT {self.project_fqn} ARGS = '{dbt_args}';"
        result.sql_statements = [sql]

        exec_result = self._run_sql(sql)
        result.success = exec_result.success
        result.stdout = exec_result.stdout
        result.stderr = exec_result.stderr
        result.return_code = exec_result.return_code
        result.elapsed_seconds = time.time() - start
        return result

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run_sql(self, sql: str) -> DeployResult:
        """Execute a SQL statement via ``snow sql``."""
        result = DeployResult()

        cmd = [self.snow_executable, "sql", "-q", sql]
        if self.connection:
            cmd.extend(["--connection", self.connection])

        logger.info("Running SQL via snow: %s", sql[:100])

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,
            )
            result.stdout = proc.stdout
            result.stderr = proc.stderr
            result.return_code = proc.returncode
            result.success = proc.returncode == 0
        except FileNotFoundError:
            result.stderr = (
                f"snow CLI not found: '{self.snow_executable}'. "
                "Install Snowflake CLI: pip install snowflake-cli"
            )
            result.return_code = 127
        except subprocess.TimeoutExpired:
            result.stderr = "snow sql timed out after 600 seconds"
            result.return_code = 124
        except Exception as exc:
            result.stderr = f"Failed to run snow sql: {exc}"
            result.return_code = 1

        return result

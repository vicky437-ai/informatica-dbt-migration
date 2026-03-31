"""Live dbt validation via ``dbt compile`` and ``dbt run`` subprocesses.

This module wraps dbt CLI commands to validate generated dbt projects against
the target Snowflake warehouse.  It captures stdout/stderr, parses errors, and
returns structured results suitable for the CLI and assessment reports.
"""

from __future__ import annotations

import logging
import os
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger("informatica_dbt.validation")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class DbtMessage:
    """A single message extracted from dbt output."""

    level: str = "info"           # info | warn | error
    message: str = ""
    file_path: Optional[str] = None
    line_number: Optional[int] = None

    def to_dict(self) -> dict:
        return {
            "level": self.level,
            "message": self.message,
            "file_path": self.file_path,
            "line_number": self.line_number,
        }


@dataclass
class DbtValidationResult:
    """Result from a dbt validation run (compile or run)."""

    command: str = ""             # e.g. "dbt compile", "dbt run"
    success: bool = False
    return_code: int = -1
    stdout: str = ""
    stderr: str = ""
    elapsed_seconds: float = 0.0
    messages: list[DbtMessage] = field(default_factory=list)

    # Parsed stats (from dbt output)
    models_ok: int = 0
    models_error: int = 0
    models_skip: int = 0
    tests_pass: int = 0
    tests_fail: int = 0
    tests_warn: int = 0
    tests_error: int = 0
    tests_skip: int = 0

    @property
    def errors(self) -> list[DbtMessage]:
        return [m for m in self.messages if m.level == "error"]

    @property
    def warnings(self) -> list[DbtMessage]:
        return [m for m in self.messages if m.level == "warn"]

    def summary(self) -> str:
        """Return a human-readable summary string."""
        lines = [f"{self.command} — {'PASS' if self.success else 'FAIL'} ({self.elapsed_seconds:.1f}s)"]
        if "compile" in self.command:
            lines.append(f"  Models compiled: {self.models_ok}")
            if self.models_error:
                lines.append(f"  Compile errors:  {self.models_error}")
        elif "run" in self.command:
            lines.append(f"  Models OK:    {self.models_ok}")
            if self.models_error:
                lines.append(f"  Models ERROR: {self.models_error}")
            if self.models_skip:
                lines.append(f"  Models SKIP:  {self.models_skip}")
        elif "test" in self.command:
            lines.append(f"  Tests pass: {self.tests_pass}")
            if self.tests_fail:
                lines.append(f"  Tests fail: {self.tests_fail}")
            if self.tests_warn:
                lines.append(f"  Tests warn: {self.tests_warn}")
            if self.tests_error:
                lines.append(f"  Tests error: {self.tests_error}")

        if self.errors:
            lines.append(f"  Errors ({len(self.errors)}):")
            for e in self.errors[:10]:
                lines.append(f"    - {e.message[:120]}")
            if len(self.errors) > 10:
                lines.append(f"    ... and {len(self.errors) - 10} more")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------

class DbtValidator:
    """Run dbt CLI commands against a project directory.

    Parameters
    ----------
    project_dir : str
        Path to the dbt project root (must contain ``dbt_project.yml``).
    profiles_dir : str | None
        Path to the directory containing ``profiles.yml``.
        Defaults to ``project_dir`` itself (common for generated projects).
    dbt_executable : str
        Path or name of the dbt binary.  Defaults to ``"dbt"``.
    """

    def __init__(
        self,
        project_dir: str,
        profiles_dir: Optional[str] = None,
        dbt_executable: str = "dbt",
    ) -> None:
        self.project_dir = Path(project_dir).resolve()
        self.profiles_dir = Path(profiles_dir).resolve() if profiles_dir else self.project_dir
        self.dbt_executable = dbt_executable

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compile(self, select: Optional[str] = None) -> DbtValidationResult:
        """Run ``dbt compile`` and return structured results."""
        args = ["compile"]
        if select:
            args.extend(["--select", select])
        return self._run(args)

    def run(self, select: Optional[str] = None, full_refresh: bool = False) -> DbtValidationResult:
        """Run ``dbt run`` and return structured results."""
        args = ["run"]
        if select:
            args.extend(["--select", select])
        if full_refresh:
            args.append("--full-refresh")
        return self._run(args)

    def test(self, select: Optional[str] = None) -> DbtValidationResult:
        """Run ``dbt test`` and return structured results."""
        args = ["test"]
        if select:
            args.extend(["--select", select])
        return self._run(args)

    def deps(self) -> DbtValidationResult:
        """Run ``dbt deps`` to install packages."""
        return self._run(["deps"])

    def validate_project_structure(self) -> list[str]:
        """Quick local check that the project directory looks valid.

        Returns a list of error messages (empty = valid).
        """
        errors: list[str] = []
        if not self.project_dir.is_dir():
            errors.append(f"Project directory does not exist: {self.project_dir}")
            return errors
        if not (self.project_dir / "dbt_project.yml").is_file():
            errors.append(f"Missing dbt_project.yml in {self.project_dir}")
        if not (self.project_dir / "models").is_dir():
            errors.append(f"Missing models/ directory in {self.project_dir}")
        if not (self.profiles_dir / "profiles.yml").is_file():
            errors.append(f"Missing profiles.yml in {self.profiles_dir}")
        return errors

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run(self, args: list[str]) -> DbtValidationResult:
        """Execute a dbt command and parse the output."""
        cmd = [
            self.dbt_executable,
            *args,
            "--project-dir", str(self.project_dir),
            "--profiles-dir", str(self.profiles_dir),
        ]

        cmd_str = " ".join(cmd)
        logger.info("Running: %s", cmd_str)

        result = DbtValidationResult(command=f"dbt {args[0]}")
        start = time.time()

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=str(self.project_dir),
                timeout=600,  # 10 minute timeout
            )
            result.stdout = proc.stdout
            result.stderr = proc.stderr
            result.return_code = proc.returncode
            result.success = proc.returncode == 0
        except FileNotFoundError:
            result.stderr = (
                f"dbt executable not found: '{self.dbt_executable}'. "
                "Install dbt-snowflake: pip install dbt-snowflake"
            )
            result.return_code = 127
        except subprocess.TimeoutExpired:
            result.stderr = f"dbt command timed out after 600 seconds: {cmd_str}"
            result.return_code = 124
        except Exception as exc:
            result.stderr = f"Failed to run dbt command: {exc}"
            result.return_code = 1

        result.elapsed_seconds = time.time() - start

        # Parse output
        self._parse_output(result)

        return result

    def _parse_output(self, result: DbtValidationResult) -> None:
        """Extract messages and stats from dbt stdout/stderr."""
        combined = result.stdout + "\n" + result.stderr

        for line in combined.splitlines():
            stripped = line.strip()
            if not stripped:
                continue

            # Detect error lines
            if "ERROR" in stripped or "Compilation Error" in stripped:
                result.messages.append(DbtMessage(
                    level="error",
                    message=stripped,
                ))
            elif "WARNING" in stripped or "WARN" in stripped.split():
                result.messages.append(DbtMessage(
                    level="warn",
                    message=stripped,
                ))

            # Parse dbt summary lines like "Completed successfully"
            # "Done. PASS=51 WARN=0 ERROR=0 SKIP=0 TOTAL=51"
            if "PASS=" in stripped:
                self._parse_stats_line(stripped, result)

            # Parse "N of N OK" style lines
            if " of " in stripped and (" OK " in stripped or " ERROR " in stripped):
                self._parse_run_stats(stripped, result)

        # Parse final summary from dbt output
        # "Finished running 51 view models, 0 table models"
        self._parse_summary(combined, result)

    def _parse_stats_line(self, line: str, result: DbtValidationResult) -> None:
        """Parse 'PASS=N WARN=N ERROR=N SKIP=N TOTAL=N' style lines."""
        import re
        for key, attr in [
            ("PASS", "tests_pass"),
            ("WARN", "tests_warn"),
            ("ERROR", "tests_error"),
            ("SKIP", "tests_skip"),
            ("FAIL", "tests_fail"),
        ]:
            match = re.search(rf"{key}=(\d+)", line)
            if match:
                setattr(result, attr, int(match.group(1)))

    def _parse_run_stats(self, line: str, result: DbtValidationResult) -> None:
        """Parse 'N of N OK / ERROR / SKIP' from model run output."""
        import re
        ok_match = re.search(r"(\d+)\s+of\s+\d+\s+OK", line)
        if ok_match:
            result.models_ok = max(result.models_ok, int(ok_match.group(1)))
        err_match = re.search(r"(\d+)\s+of\s+\d+\s+ERROR", line)
        if err_match:
            result.models_error = max(result.models_error, int(err_match.group(1)))

    def _parse_summary(self, output: str, result: DbtValidationResult) -> None:
        """Parse the 'Finished running N view models...' summary."""
        import re
        # "Finished running 51 view models in ..."
        view_match = re.search(r"(\d+)\s+view\s+model", output)
        table_match = re.search(r"(\d+)\s+table\s+model", output)
        incr_match = re.search(r"(\d+)\s+incremental\s+model", output)

        total = 0
        if view_match:
            total += int(view_match.group(1))
        if table_match:
            total += int(table_match.group(1))
        if incr_match:
            total += int(incr_match.group(1))

        if total > 0 and result.models_ok == 0 and result.success:
            result.models_ok = total

        # "Completed with 0 errors and 0 warnings"
        comp_match = re.search(r"Completed with (\d+) error", output)
        if comp_match:
            result.models_error = max(result.models_error, int(comp_match.group(1)))

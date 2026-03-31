"""Git integration — init, branch, commit, push, and PR operations.

Wraps ``git`` CLI commands executed as subprocesses.  Designed for use
by the ``infa2dbt git-push`` CLI command.
"""

from __future__ import annotations

import logging
import os
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger("informatica_dbt.git")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class GitResult:
    """Result from a Git operation."""

    command: str = ""
    success: bool = False
    return_code: int = -1
    stdout: str = ""
    stderr: str = ""
    elapsed_seconds: float = 0.0

    @property
    def output(self) -> str:
        """Combined stdout + stderr."""
        return (self.stdout + "\n" + self.stderr).strip()


# ---------------------------------------------------------------------------
# GitManager
# ---------------------------------------------------------------------------

class GitManager:
    """Manage Git operations for a dbt project directory.

    Parameters
    ----------
    project_dir : str
        Path to the dbt project root (the repo root or a subdirectory).
    git_executable : str
        Path or name of the git binary.  Defaults to ``"git"``.
    """

    def __init__(
        self,
        project_dir: str,
        git_executable: str = "git",
    ) -> None:
        self.project_dir = Path(project_dir).resolve()
        self.git_executable = git_executable

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_repo(self) -> bool:
        """Check if the project directory is inside a Git repository."""
        result = self._run(["rev-parse", "--is-inside-work-tree"])
        return result.success and result.stdout.strip() == "true"

    def init(self) -> GitResult:
        """Initialize a new Git repository if one doesn't exist."""
        if self.is_repo():
            return GitResult(
                command="git init",
                success=True,
                return_code=0,
                stdout="Already a git repository",
            )
        result = self._run(["init"])
        if result.success:
            # Create a default .gitignore if one doesn't exist
            gitignore = self.project_dir / ".gitignore"
            if not gitignore.exists():
                gitignore.write_text(
                    "\n".join([
                        "# dbt",
                        "target/",
                        "dbt_packages/",
                        "dbt_modules/",
                        "logs/",
                        "",
                        "# Python",
                        "__pycache__/",
                        "*.pyc",
                        ".venv/",
                        "",
                        "# infa2dbt cache",
                        ".infa2dbt/cache/",
                        "",
                        "# OS",
                        ".DS_Store",
                        "Thumbs.db",
                        "",
                    ]),
                    encoding="utf-8",
                )
        return result

    def current_branch(self) -> str:
        """Return the name of the current branch, or empty string."""
        result = self._run(["rev-parse", "--abbrev-ref", "HEAD"])
        if result.success:
            return result.stdout.strip()
        return ""

    def create_branch(self, name: str) -> GitResult:
        """Create and switch to a new branch."""
        # Check if branch exists
        check = self._run(["rev-parse", "--verify", name])
        if check.success:
            # Branch exists, just switch to it
            return self._run(["checkout", name])
        return self._run(["checkout", "-b", name])

    def status(self) -> GitResult:
        """Run ``git status --porcelain``."""
        return self._run(["status", "--porcelain"])

    def has_changes(self) -> bool:
        """Check if there are uncommitted changes."""
        result = self.status()
        return result.success and bool(result.stdout.strip())

    def add_all(self) -> GitResult:
        """Stage all changes (``git add -A``)."""
        return self._run(["add", "-A"])

    def commit(self, message: str) -> GitResult:
        """Create a commit with the given message.

        Stages all changes first via ``git add -A``.
        """
        add_result = self.add_all()
        if not add_result.success:
            return add_result

        if not self.has_changes():
            return GitResult(
                command="git commit",
                success=True,
                return_code=0,
                stdout="Nothing to commit, working tree clean",
            )

        return self._run(["commit", "-m", message])

    def push(
        self,
        remote: str = "origin",
        branch: Optional[str] = None,
        set_upstream: bool = True,
    ) -> GitResult:
        """Push to a remote repository.

        Parameters
        ----------
        remote : str
            Remote name (default ``"origin"``).
        branch : str | None
            Branch to push.  Defaults to the current branch.
        set_upstream : bool
            Whether to set the upstream tracking reference.
        """
        branch = branch or self.current_branch()
        if not branch:
            return GitResult(
                command="git push",
                success=False,
                return_code=1,
                stderr="Cannot determine current branch",
            )

        args = ["push"]
        if set_upstream:
            args.extend(["-u", remote, branch])
        else:
            args.extend([remote, branch])

        return self._run(args)

    def add_remote(self, name: str, url: str) -> GitResult:
        """Add a remote if it doesn't already exist."""
        # Check if remote already exists
        check = self._run(["remote", "get-url", name])
        if check.success:
            existing_url = check.stdout.strip()
            if existing_url == url:
                return GitResult(
                    command=f"git remote add {name}",
                    success=True,
                    return_code=0,
                    stdout=f"Remote '{name}' already configured with {url}",
                )
            # Update existing remote URL
            return self._run(["remote", "set-url", name, url])

        return self._run(["remote", "add", name, url])

    def log(self, n: int = 5) -> GitResult:
        """Show recent commit log."""
        return self._run(["log", f"--oneline", f"-{n}"])

    def tag(self, name: str, message: Optional[str] = None) -> GitResult:
        """Create a tag."""
        if message:
            return self._run(["tag", "-a", name, "-m", message])
        return self._run(["tag", name])

    def full_commit_and_push(
        self,
        message: str,
        remote: str = "origin",
        branch: Optional[str] = None,
        remote_url: Optional[str] = None,
    ) -> list[GitResult]:
        """Convenience: init → add remote → commit → push.

        Returns a list of all intermediate results.
        """
        results: list[GitResult] = []

        # Init if needed
        init_result = self.init()
        results.append(init_result)
        if not init_result.success:
            return results

        # Add remote if URL provided
        if remote_url:
            remote_result = self.add_remote(remote, remote_url)
            results.append(remote_result)
            if not remote_result.success:
                return results

        # Switch branch if requested
        if branch:
            current = self.current_branch()
            if current != branch:
                branch_result = self.create_branch(branch)
                results.append(branch_result)
                if not branch_result.success:
                    return results

        # Commit
        commit_result = self.commit(message)
        results.append(commit_result)
        if not commit_result.success:
            return results

        # Push
        push_result = self.push(remote=remote, branch=branch)
        results.append(push_result)

        return results

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run(self, args: list[str]) -> GitResult:
        """Execute a git command and return the result."""
        cmd = [self.git_executable, *args]
        cmd_str = " ".join(cmd)
        logger.debug("Running: %s (cwd=%s)", cmd_str, self.project_dir)

        result = GitResult(command=f"git {args[0]}")
        start = time.time()

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=str(self.project_dir),
                timeout=120,
            )
            result.stdout = proc.stdout
            result.stderr = proc.stderr
            result.return_code = proc.returncode
            result.success = proc.returncode == 0
        except FileNotFoundError:
            result.stderr = (
                f"git executable not found: '{self.git_executable}'. "
                "Please install git."
            )
            result.return_code = 127
        except subprocess.TimeoutExpired:
            result.stderr = f"git command timed out after 120 seconds: {cmd_str}"
            result.return_code = 124
        except Exception as exc:
            result.stderr = f"Failed to run git command: {exc}"
            result.return_code = 1

        result.elapsed_seconds = time.time() - start

        if not result.success and result.stderr:
            logger.warning("git %s failed: %s", args[0], result.stderr.strip())

        return result

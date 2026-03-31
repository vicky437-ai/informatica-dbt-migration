"""Unit tests for the Git integration module."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from informatica_to_dbt.git.git_manager import GitManager, GitResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def project_dir(tmp_path):
    """Create a temporary project directory."""
    (tmp_path / "dbt_project.yml").write_text("name: test\n", encoding="utf-8")
    (tmp_path / "models").mkdir()
    return str(tmp_path)


@pytest.fixture
def git_mgr(project_dir):
    return GitManager(project_dir=project_dir)


# ---------------------------------------------------------------------------
# GitResult
# ---------------------------------------------------------------------------

class TestGitResult:
    def test_output_combined(self):
        r = GitResult(stdout="out", stderr="err")
        assert "out" in r.output
        assert "err" in r.output

    def test_defaults(self):
        r = GitResult()
        assert not r.success
        assert r.return_code == -1
        assert r.command == ""


# ---------------------------------------------------------------------------
# GitManager — is_repo
# ---------------------------------------------------------------------------

class TestGitManagerIsRepo:
    @patch("subprocess.run")
    def test_is_repo_true(self, mock_run, git_mgr):
        mock_run.return_value = MagicMock(returncode=0, stdout="true\n", stderr="")
        assert git_mgr.is_repo()

    @patch("subprocess.run")
    def test_is_repo_false(self, mock_run, git_mgr):
        mock_run.return_value = MagicMock(returncode=128, stdout="", stderr="not a git repository")
        assert not git_mgr.is_repo()


# ---------------------------------------------------------------------------
# GitManager — init
# ---------------------------------------------------------------------------

class TestGitManagerInit:
    @patch("subprocess.run")
    def test_init_already_repo(self, mock_run, git_mgr):
        # First call is is_repo check, returns true
        mock_run.return_value = MagicMock(returncode=0, stdout="true\n", stderr="")
        result = git_mgr.init()
        assert result.success
        assert "Already" in result.stdout

    @patch("subprocess.run")
    def test_init_new_repo(self, mock_run, git_mgr):
        # First call (is_repo check) fails, second call (git init) succeeds
        mock_run.side_effect = [
            MagicMock(returncode=128, stdout="", stderr="not a git repo"),
            MagicMock(returncode=0, stdout="Initialized empty Git repository\n", stderr=""),
        ]
        result = git_mgr.init()
        assert result.success
        # Should have created .gitignore
        gitignore = Path(git_mgr.project_dir) / ".gitignore"
        assert gitignore.exists()
        content = gitignore.read_text()
        assert "target/" in content
        assert "dbt_packages/" in content

    @patch("subprocess.run")
    def test_init_no_overwrite_gitignore(self, mock_run, git_mgr):
        # Create a pre-existing .gitignore
        gitignore = Path(git_mgr.project_dir) / ".gitignore"
        gitignore.write_text("custom content\n", encoding="utf-8")

        mock_run.side_effect = [
            MagicMock(returncode=128, stdout="", stderr="not a git repo"),
            MagicMock(returncode=0, stdout="Initialized\n", stderr=""),
        ]
        result = git_mgr.init()
        assert result.success
        # Should NOT overwrite existing .gitignore
        assert gitignore.read_text() == "custom content\n"


# ---------------------------------------------------------------------------
# GitManager — branch operations
# ---------------------------------------------------------------------------

class TestGitManagerBranch:
    @patch("subprocess.run")
    def test_current_branch(self, mock_run, git_mgr):
        mock_run.return_value = MagicMock(returncode=0, stdout="main\n", stderr="")
        assert git_mgr.current_branch() == "main"

    @patch("subprocess.run")
    def test_current_branch_empty_on_failure(self, mock_run, git_mgr):
        mock_run.return_value = MagicMock(returncode=128, stdout="", stderr="fatal")
        assert git_mgr.current_branch() == ""

    @patch("subprocess.run")
    def test_create_branch_new(self, mock_run, git_mgr):
        mock_run.side_effect = [
            # rev-parse --verify fails (branch doesn't exist)
            MagicMock(returncode=128, stdout="", stderr=""),
            # checkout -b succeeds
            MagicMock(returncode=0, stdout="Switched to new branch 'feat'\n", stderr=""),
        ]
        result = git_mgr.create_branch("feat")
        assert result.success

    @patch("subprocess.run")
    def test_create_branch_existing(self, mock_run, git_mgr):
        mock_run.side_effect = [
            # rev-parse --verify succeeds (branch exists)
            MagicMock(returncode=0, stdout="abc123\n", stderr=""),
            # checkout succeeds
            MagicMock(returncode=0, stdout="Switched to branch 'feat'\n", stderr=""),
        ]
        result = git_mgr.create_branch("feat")
        assert result.success


# ---------------------------------------------------------------------------
# GitManager — status and changes
# ---------------------------------------------------------------------------

class TestGitManagerStatus:
    @patch("subprocess.run")
    def test_has_changes_true(self, mock_run, git_mgr):
        mock_run.return_value = MagicMock(returncode=0, stdout="M  models/foo.sql\n", stderr="")
        assert git_mgr.has_changes()

    @patch("subprocess.run")
    def test_has_changes_false(self, mock_run, git_mgr):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        assert not git_mgr.has_changes()


# ---------------------------------------------------------------------------
# GitManager — commit
# ---------------------------------------------------------------------------

class TestGitManagerCommit:
    @patch("subprocess.run")
    def test_commit_with_changes(self, mock_run, git_mgr):
        mock_run.side_effect = [
            # add -A
            MagicMock(returncode=0, stdout="", stderr=""),
            # status --porcelain
            MagicMock(returncode=0, stdout="M  file.sql\n", stderr=""),
            # commit -m
            MagicMock(returncode=0, stdout="[main abc123] My message\n", stderr=""),
        ]
        result = git_mgr.commit("My message")
        assert result.success

    @patch("subprocess.run")
    def test_commit_nothing_to_commit(self, mock_run, git_mgr):
        mock_run.side_effect = [
            # add -A
            MagicMock(returncode=0, stdout="", stderr=""),
            # status --porcelain (empty = no changes)
            MagicMock(returncode=0, stdout="", stderr=""),
        ]
        result = git_mgr.commit("My message")
        assert result.success
        assert "Nothing to commit" in result.stdout


# ---------------------------------------------------------------------------
# GitManager — push
# ---------------------------------------------------------------------------

class TestGitManagerPush:
    @patch("subprocess.run")
    def test_push_success(self, mock_run, git_mgr):
        mock_run.side_effect = [
            # current_branch
            MagicMock(returncode=0, stdout="main\n", stderr=""),
            # push
            MagicMock(returncode=0, stdout="Pushed to origin/main\n", stderr=""),
        ]
        result = git_mgr.push()
        assert result.success

    @patch("subprocess.run")
    def test_push_explicit_branch(self, mock_run, git_mgr):
        mock_run.return_value = MagicMock(returncode=0, stdout="Pushed\n", stderr="")
        result = git_mgr.push(remote="origin", branch="develop")
        assert result.success
        args = mock_run.call_args[0][0]
        assert "develop" in args


# ---------------------------------------------------------------------------
# GitManager — remote
# ---------------------------------------------------------------------------

class TestGitManagerRemote:
    @patch("subprocess.run")
    def test_add_remote_new(self, mock_run, git_mgr):
        mock_run.side_effect = [
            # remote get-url fails (doesn't exist)
            MagicMock(returncode=2, stdout="", stderr="No such remote"),
            # remote add succeeds
            MagicMock(returncode=0, stdout="", stderr=""),
        ]
        result = git_mgr.add_remote("origin", "https://github.com/org/repo.git")
        assert result.success

    @patch("subprocess.run")
    def test_add_remote_already_exists_same_url(self, mock_run, git_mgr):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="https://github.com/org/repo.git\n",
            stderr="",
        )
        result = git_mgr.add_remote("origin", "https://github.com/org/repo.git")
        assert result.success
        assert "already configured" in result.stdout

    @patch("subprocess.run")
    def test_add_remote_update_url(self, mock_run, git_mgr):
        mock_run.side_effect = [
            # get-url returns old URL
            MagicMock(returncode=0, stdout="https://github.com/old/repo.git\n", stderr=""),
            # set-url succeeds
            MagicMock(returncode=0, stdout="", stderr=""),
        ]
        result = git_mgr.add_remote("origin", "https://github.com/new/repo.git")
        assert result.success


# ---------------------------------------------------------------------------
# GitManager — error handling
# ---------------------------------------------------------------------------

class TestGitManagerErrors:
    @patch("subprocess.run", side_effect=FileNotFoundError)
    def test_git_not_found(self, mock_run, git_mgr):
        result = git_mgr.init()
        # First call is is_repo, which will fail with git not found
        assert not result.success
        assert result.return_code == 127

    @patch("subprocess.run", side_effect=subprocess.TimeoutExpired("git", 120))
    def test_timeout(self, mock_run, git_mgr):
        result = git_mgr.status()
        assert not result.success
        assert result.return_code == 124
        assert "timed out" in result.stderr


# ---------------------------------------------------------------------------
# GitManager — full_commit_and_push
# ---------------------------------------------------------------------------

class TestGitManagerFullPipeline:
    @patch("subprocess.run")
    def test_full_commit_and_push(self, mock_run, git_mgr):
        # Sequence: is_repo → remote get-url → remote add → current_branch → checkout -b →
        #           add -A → status → commit → current_branch → push
        mock_run.side_effect = [
            # is_repo check
            MagicMock(returncode=0, stdout="true\n", stderr=""),
            # remote get-url fails (new)
            MagicMock(returncode=2, stdout="", stderr="No such remote"),
            # remote add
            MagicMock(returncode=0, stdout="", stderr=""),
            # current_branch (for branch check)
            MagicMock(returncode=0, stdout="main\n", stderr=""),
            # rev-parse --verify (branch doesn't exist)
            MagicMock(returncode=128, stdout="", stderr=""),
            # checkout -b
            MagicMock(returncode=0, stdout="Switched\n", stderr=""),
            # add -A (from commit)
            MagicMock(returncode=0, stdout="", stderr=""),
            # status (from has_changes in commit)
            MagicMock(returncode=0, stdout="M  file\n", stderr=""),
            # commit
            MagicMock(returncode=0, stdout="[feat abc] msg\n", stderr=""),
            # current_branch (from push)
            MagicMock(returncode=0, stdout="feat\n", stderr=""),
            # push
            MagicMock(returncode=0, stdout="Pushed\n", stderr=""),
        ]

        results = git_mgr.full_commit_and_push(
            message="test commit",
            remote="origin",
            branch="feat",
            remote_url="https://github.com/org/repo.git",
        )
        assert all(r.success for r in results)
        assert len(results) >= 3  # init + remote + branch + commit + push

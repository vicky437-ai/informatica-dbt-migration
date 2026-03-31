"""SQL validation for generated dbt models.

Performs lightweight syntax checks on generated SQL without requiring
a live Snowflake connection.  Phase 4 will add Snowflake-side
``EXPLAIN`` validation via ``snowflake_sql_execute``.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import List

from informatica_to_dbt.exceptions import SQLValidationError
from informatica_to_dbt.generator.response_parser import GeneratedFile
from informatica_to_dbt.validator.patterns import (
    CONFIG_RE as _CONFIG_RE,
    INFORMATICA_FN_RE as _INFORMATICA_FN_RE,
    INFORMATICA_PATTERN_RE as _INFORMATICA_PATTERN_RE,
    REF_RE as _REF_RE,
    SEMICOLON_RE as _SEMICOLON_RE,
    SOURCE_RE as _SOURCE_RE,
)

logger = logging.getLogger("informatica_dbt")


@dataclass
class SQLIssue:
    """A single SQL validation finding."""

    file_path: str
    severity: str  # "error" | "warning"
    message: str
    line: int | None = None


@dataclass
class SQLValidationResult:
    """Aggregated validation result for one or more SQL files."""

    issues: List[SQLIssue] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not any(i.severity == "error" for i in self.issues)

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "warning")


# ---------------------------------------------------------------------------
# Regex patterns for common SQL issues
# ---------------------------------------------------------------------------

_SELECT_RE = re.compile(r"\bSELECT\b", re.IGNORECASE)
_FROM_RE = re.compile(r"\bFROM\b", re.IGNORECASE)
_HARDCODED_3PART_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+(?!{)([A-Z_]\w*\.[A-Z_]\w*\.[A-Z_]\w+)\b",
    re.IGNORECASE,
)


def validate_sql(files: List[GeneratedFile]) -> SQLValidationResult:
    """Run SQL validation checks across all SQL files.

    Returns a :class:`SQLValidationResult` with all findings.
    """
    result = SQLValidationResult()

    sql_files = [f for f in files if f.is_sql]
    if not sql_files:
        logger.info("No SQL files to validate")
        return result

    for gf in sql_files:
        _check_file(gf, result)

    # Cross-file check: verify ref() targets exist within the file set
    _check_ref_targets(sql_files, result)

    logger.info(
        "SQL validation: %d file(s), %d error(s), %d warning(s)",
        len(sql_files),
        result.error_count,
        result.warning_count,
    )
    return result


def _check_file(gf: GeneratedFile, result: SQLValidationResult) -> None:
    """Run all checks against a single SQL file."""
    path = gf.path
    content = gf.content

    # Skip macro files — different structure
    if gf.layer == "macros":
        return

    # 1. Must contain SELECT
    if not _SELECT_RE.search(content):
        result.issues.append(SQLIssue(path, "error", "No SELECT statement found"))

    # 2. Must contain FROM (unless it's a pure CTE or snapshot config)
    if not _FROM_RE.search(content) and "snapshot" not in path:
        result.issues.append(SQLIssue(path, "warning", "No FROM clause found"))

    # 3. Should have a config block
    if not _CONFIG_RE.search(content):
        result.issues.append(SQLIssue(path, "warning", "Missing {{ config(...) }} block"))

    # 4. Must use ref() or source() — not hardcoded table names
    has_ref = bool(_REF_RE.search(content))
    has_source = bool(_SOURCE_RE.search(content))
    if not has_ref and not has_source:
        result.issues.append(
            SQLIssue(path, "warning", "No ref() or source() calls — using hardcoded table names?")
        )

    # 5. Check for hardcoded three-part names
    hardcoded = _HARDCODED_3PART_RE.findall(content)
    if hardcoded:
        result.issues.append(
            SQLIssue(
                path,
                "error",
                f"Hardcoded database references found: {', '.join(hardcoded[:3])}. "
                "Use source() or ref() instead.",
            )
        )

    # 6. Trailing semicolons (dbt doesn't allow them)
    if _SEMICOLON_RE.search(content):
        result.issues.append(
            SQLIssue(path, "error", "Trailing semicolons found — dbt SQL must not end with ';'")
        )

    # 7. Unconverted Informatica functions
    informatica_fns = _INFORMATICA_FN_RE.findall(content)
    if informatica_fns:
        # Clean up matched strings for display
        cleaned = {fn.strip().rstrip("(").strip() for fn in informatica_fns}
        result.issues.append(
            SQLIssue(
                path,
                "error",
                f"Unconverted Informatica functions: {', '.join(sorted(cleaned))}",
            )
        )

    # 7b. Unconverted Informatica patterns (variables, lookups, etc.)
    informatica_patterns = _INFORMATICA_PATTERN_RE.findall(content)
    if informatica_patterns:
        cleaned = {p.strip().rstrip("(").strip() for p in informatica_patterns}
        result.issues.append(
            SQLIssue(
                path,
                "error",
                f"Unconverted Informatica patterns: {', '.join(sorted(cleaned))}",
            )
        )

    # 8. Balanced parentheses check
    opens = content.count("(")
    closes = content.count(")")
    if opens != closes:
        result.issues.append(
            SQLIssue(
                path,
                "error",
                f"Unbalanced parentheses: {opens} '(' vs {closes} ')'",
            )
        )

    # 9. Balanced Jinja braces
    jinja_opens = content.count("{{")
    jinja_closes = content.count("}}")
    if jinja_opens != jinja_closes:
        result.issues.append(
            SQLIssue(
                path,
                "error",
                f"Unbalanced Jinja braces: {jinja_opens} '{{{{' vs {jinja_closes} '}}}}'",
            )
        )

    # 10. Truncation detection — incomplete SQL from LLM output limits
    stripped = content.rstrip()
    if stripped and not stripped.endswith(")") and not stripped.endswith("}}") and not stripped.endswith("\n"):
        last_line = stripped.splitlines()[-1] if stripped.splitlines() else ""
        # Check for signs of mid-statement truncation
        if last_line.rstrip().endswith(",") or last_line.rstrip().endswith("AND") or last_line.rstrip().endswith("OR"):
            result.issues.append(
                SQLIssue(path, "error", f"Possible truncated SQL — file ends with: ...{last_line[-60:]}")
            )
    # Also check if file has SELECT but no final FROM/WHERE/GROUP/closing paren
    if _SELECT_RE.search(content) and len(content.strip()) < 50:
        result.issues.append(
            SQLIssue(path, "warning", "Suspiciously short SQL model — may be truncated")
        )

    # 11. Markdown artifact detection — LLM sometimes includes ```
    if "```" in content:
        result.issues.append(
            SQLIssue(path, "error", "Markdown code fence (```) found in SQL — LLM output artifact")
        )

    # 12. Comment-only file detection — file has no real SQL, just comments/config
    non_comment_lines = [
        line for line in content.splitlines()
        if line.strip() and not line.strip().startswith("--") and not line.strip().startswith("{")
    ]
    if not non_comment_lines and content.strip():
        result.issues.append(
            SQLIssue(path, "warning", "File contains only comments or Jinja config — no SQL statements")
        )


def _check_ref_targets(sql_files: List[GeneratedFile], result: SQLValidationResult) -> None:
    """Verify that all ref() targets correspond to an actual SQL model in the set."""
    # Build set of known model names from file paths
    known_models: set[str] = set()
    for gf in sql_files:
        # Extract model name from path: models/.../my_model.sql → my_model
        parts = gf.path.replace("\\", "/").split("/")
        filename = parts[-1] if parts else ""
        if filename.endswith(".sql"):
            known_models.add(filename[:-4])

    # Check each file's ref() calls against known models
    for gf in sql_files:
        refs = _REF_RE.findall(gf.content)
        for ref_name in refs:
            if ref_name not in known_models:
                result.issues.append(
                    SQLIssue(
                        gf.path,
                        "warning",
                        f"ref('{ref_name}') target not found in generated models — may cause dbt error",
                    )
                )

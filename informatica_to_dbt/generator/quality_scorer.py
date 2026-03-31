"""Score the quality of generated dbt output on multiple dimensions.

Provides a 0–100 quality score based on structural completeness,
dbt convention adherence, and absence of unconverted Informatica artifacts.
Used by the orchestrator to decide whether self-healing is worthwhile and
to attach quality metadata to conversion results.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List

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

# ---------------------------------------------------------------------------
# Scoring constants
# ---------------------------------------------------------------------------

# Weight of each dimension in the final score (must sum to 100)
_WEIGHTS: Dict[str, int] = {
    "file_structure": 20,       # expected files present per strategy
    "dbt_conventions": 25,      # config(), ref(), source() usage
    "sql_syntax": 20,           # balanced parens/braces, no semicolons
    "function_conversion": 25,  # no leftover Informatica functions/patterns
    "yaml_quality": 10,         # YAML files present and parseable
}


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class DimensionScore:
    """Score for a single quality dimension."""

    name: str
    score: float           # 0.0 – 1.0 (fraction achieved)
    weight: int            # weight in final score
    details: str = ""      # human-readable explanation


@dataclass
class QualityReport:
    """Aggregated quality report for a set of generated files."""

    dimensions: List[DimensionScore] = field(default_factory=list)

    @property
    def total_score(self) -> int:
        """Weighted total score (0–100)."""
        return round(sum(d.score * d.weight for d in self.dimensions))

    @property
    def summary(self) -> str:
        """One-line summary for logging."""
        parts = [f"{d.name}={d.score:.0%}" for d in self.dimensions]
        return f"Quality {self.total_score}/100 ({', '.join(parts)})"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def score_quality(
    files: List[GeneratedFile],
    expected_layers: List[str] | None = None,
) -> QualityReport:
    """Score the quality of generated dbt files.

    Args:
        files: Generated files to evaluate.
        expected_layers: If provided, the layers that should be present
            (e.g. ``["staging", "intermediate"]`` for STAGED strategy).

    Returns:
        A :class:`QualityReport` with per-dimension scores and a total.
    """
    report = QualityReport()
    sql_files = [f for f in files if f.is_sql]
    yaml_files = [f for f in files if f.is_yaml]

    report.dimensions.append(_score_file_structure(files, sql_files, yaml_files, expected_layers))
    report.dimensions.append(_score_dbt_conventions(sql_files))
    report.dimensions.append(_score_sql_syntax(sql_files))
    report.dimensions.append(_score_function_conversion(sql_files))
    report.dimensions.append(_score_yaml_quality(yaml_files))

    logger.info("Quality scoring: %s", report.summary)
    return report


# ---------------------------------------------------------------------------
# Dimension scorers
# ---------------------------------------------------------------------------

def _score_file_structure(
    all_files: List[GeneratedFile],
    sql_files: List[GeneratedFile],
    yaml_files: List[GeneratedFile],
    expected_layers: List[str] | None,
) -> DimensionScore:
    """Check that the expected file structure is present."""
    weight = _WEIGHTS["file_structure"]

    if not all_files:
        return DimensionScore("file_structure", 0.0, weight, "No files generated")

    checks_passed = 0
    checks_total = 0

    # Must have at least one SQL file
    checks_total += 1
    if sql_files:
        checks_passed += 1

    # Must have at least one YAML file
    checks_total += 1
    if yaml_files:
        checks_passed += 1

    # Check sources.yml exists
    checks_total += 1
    has_sources = any("sources" in f.path for f in yaml_files)
    if has_sources:
        checks_passed += 1

    # Check schema.yml exists
    checks_total += 1
    has_schema = any("schema" in f.path for f in yaml_files)
    if has_schema:
        checks_passed += 1

    # Check expected layers are covered
    if expected_layers:
        present_layers = {f.layer for f in sql_files}
        for layer in expected_layers:
            checks_total += 1
            if layer in present_layers:
                checks_passed += 1

    score = checks_passed / checks_total if checks_total > 0 else 0.0
    details = f"{checks_passed}/{checks_total} structure checks passed"
    return DimensionScore("file_structure", score, weight, details)


def _score_dbt_conventions(sql_files: List[GeneratedFile]) -> DimensionScore:
    """Check dbt convention adherence: config(), ref()/source() usage."""
    weight = _WEIGHTS["dbt_conventions"]

    if not sql_files:
        return DimensionScore("dbt_conventions", 0.0, weight, "No SQL files")

    files_with_config = 0
    files_with_ref_or_source = 0
    # Staging files should use source(), others should use ref()
    staging_files_with_source = 0
    staging_count = 0

    for f in sql_files:
        if f.layer == "macros":
            continue
        if _CONFIG_RE.search(f.content):
            files_with_config += 1
        if _REF_RE.search(f.content) or _SOURCE_RE.search(f.content):
            files_with_ref_or_source += 1
        if f.layer == "staging":
            staging_count += 1
            if _SOURCE_RE.search(f.content):
                staging_files_with_source += 1

    non_macro = [f for f in sql_files if f.layer != "macros"]
    total = len(non_macro) or 1

    config_ratio = files_with_config / total
    ref_source_ratio = files_with_ref_or_source / total
    staging_ratio = staging_files_with_source / staging_count if staging_count > 0 else 1.0

    # Weighted average of sub-checks
    score = (config_ratio * 0.3) + (ref_source_ratio * 0.4) + (staging_ratio * 0.3)
    details = (
        f"config={files_with_config}/{total}, "
        f"ref/source={files_with_ref_or_source}/{total}, "
        f"staging_source={staging_files_with_source}/{staging_count}"
    )
    return DimensionScore("dbt_conventions", score, weight, details)


def _score_sql_syntax(sql_files: List[GeneratedFile]) -> DimensionScore:
    """Check for balanced parens/braces and no semicolons."""
    weight = _WEIGHTS["sql_syntax"]

    if not sql_files:
        return DimensionScore("sql_syntax", 0.0, weight, "No SQL files")

    clean_files = 0
    issues: List[str] = []

    for f in sql_files:
        file_ok = True
        # Balanced parentheses
        if f.content.count("(") != f.content.count(")"):
            file_ok = False
            issues.append(f"{f.path}: unbalanced parens")
        # Balanced Jinja braces
        if f.content.count("{{") != f.content.count("}}"):
            file_ok = False
            issues.append(f"{f.path}: unbalanced Jinja braces")
        # No semicolons
        if _SEMICOLON_RE.search(f.content):
            file_ok = False
            issues.append(f"{f.path}: trailing semicolons")
        if file_ok:
            clean_files += 1

    score = clean_files / len(sql_files)
    details = f"{clean_files}/{len(sql_files)} files clean"
    if issues:
        details += f"; issues: {'; '.join(issues[:3])}"
    return DimensionScore("sql_syntax", score, weight, details)


def _score_function_conversion(sql_files: List[GeneratedFile]) -> DimensionScore:
    """Check that no Informatica functions or patterns remain."""
    weight = _WEIGHTS["function_conversion"]

    if not sql_files:
        return DimensionScore("function_conversion", 0.0, weight, "No SQL files")

    clean_files = 0
    leftover_functions: List[str] = []

    for f in sql_files:
        fn_matches = _INFORMATICA_FN_RE.findall(f.content)
        pattern_matches = _INFORMATICA_PATTERN_RE.findall(f.content)
        if not fn_matches and not pattern_matches:
            clean_files += 1
        else:
            all_matches = set()
            for m in fn_matches:
                all_matches.add(m.strip().rstrip("(").strip())
            for m in pattern_matches:
                all_matches.add(m.strip().rstrip("(").strip())
            leftover_functions.extend(sorted(all_matches))

    score = clean_files / len(sql_files)
    details = f"{clean_files}/{len(sql_files)} files fully converted"
    if leftover_functions:
        unique = sorted(set(leftover_functions))
        details += f"; leftover: {', '.join(unique[:5])}"
    return DimensionScore("function_conversion", score, weight, details)


def _score_yaml_quality(yaml_files: List[GeneratedFile]) -> DimensionScore:
    """Check that YAML files parse and have expected structure."""
    weight = _WEIGHTS["yaml_quality"]

    if not yaml_files:
        return DimensionScore("yaml_quality", 0.0, weight, "No YAML files")

    try:
        import yaml
    except ImportError:
        return DimensionScore("yaml_quality", 0.5, weight, "PyYAML not available — skipped")

    parseable = 0
    well_structured = 0

    for f in yaml_files:
        try:
            doc = yaml.safe_load(f.content)
            parseable += 1
            if isinstance(doc, dict) and ("version" in doc):
                well_structured += 1
        except Exception:
            pass

    parse_ratio = parseable / len(yaml_files)
    struct_ratio = well_structured / len(yaml_files)
    score = (parse_ratio * 0.6) + (struct_ratio * 0.4)
    details = f"{parseable}/{len(yaml_files)} parseable, {well_structured}/{len(yaml_files)} well-structured"
    return DimensionScore("yaml_quality", score, weight, details)

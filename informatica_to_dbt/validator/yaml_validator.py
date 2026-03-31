"""YAML validation for generated dbt schema and source files.

Performs structural checks on YAML files to ensure they conform to
dbt's expected format for ``schema.yml`` and ``sources.yml``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List

from informatica_to_dbt.exceptions import YAMLValidationError
from informatica_to_dbt.generator.response_parser import GeneratedFile

logger = logging.getLogger("informatica_dbt")

try:
    import yaml

    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False


@dataclass
class YAMLIssue:
    """A single YAML validation finding."""

    file_path: str
    severity: str  # "error" | "warning"
    message: str


@dataclass
class YAMLValidationResult:
    """Aggregated validation result for YAML files."""

    issues: List[YAMLIssue] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not any(i.severity == "error" for i in self.issues)

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "warning")


def validate_yaml(files: List[GeneratedFile]) -> YAMLValidationResult:
    """Run YAML validation checks across all YAML files.

    Returns a :class:`YAMLValidationResult` with all findings.
    """
    result = YAMLValidationResult()

    yaml_files = [f for f in files if f.is_yaml]
    if not yaml_files:
        logger.info("No YAML files to validate")
        return result

    if not _HAS_YAML:
        result.issues.append(
            YAMLIssue("*", "warning", "PyYAML not installed — skipping YAML parse validation")
        )
        return result

    for gf in yaml_files:
        _check_file(gf, result)

    logger.info(
        "YAML validation: %d file(s), %d error(s), %d warning(s)",
        len(yaml_files),
        result.error_count,
        result.warning_count,
    )
    return result


def _check_file(gf: GeneratedFile, result: YAMLValidationResult) -> None:
    """Validate a single YAML file."""
    path = gf.path

    # 1. Parse YAML
    try:
        doc = yaml.safe_load(gf.content)
    except yaml.YAMLError as exc:
        result.issues.append(YAMLIssue(path, "error", f"YAML parse error: {exc}"))
        return

    if not isinstance(doc, dict):
        result.issues.append(YAMLIssue(path, "error", "YAML root must be a mapping"))
        return

    # 2. Check version key
    if "version" not in doc:
        result.issues.append(YAMLIssue(path, "warning", "Missing 'version' key"))

    # 3. Detect file type and validate structure
    if "sources" in path or "sources" in doc:
        _validate_sources(path, doc, result)
    elif "schema" in path or "models" in doc:
        _validate_schema(path, doc, result)
    else:
        # Generic — just check it parsed
        logger.debug("YAML file '%s' has no recognised dbt structure", path)


def _validate_sources(
    path: str, doc: Dict[str, Any], result: YAMLValidationResult
) -> None:
    """Validate a dbt sources.yml structure."""
    sources = doc.get("sources")
    if sources is None:
        result.issues.append(YAMLIssue(path, "warning", "No 'sources' key found"))
        return

    if not isinstance(sources, list):
        result.issues.append(YAMLIssue(path, "error", "'sources' must be a list"))
        return

    for i, src in enumerate(sources):
        if not isinstance(src, dict):
            result.issues.append(
                YAMLIssue(path, "error", f"sources[{i}] must be a mapping")
            )
            continue
        if "name" not in src:
            result.issues.append(
                YAMLIssue(path, "error", f"sources[{i}] missing 'name'")
            )
        tables = src.get("tables", [])
        if not tables:
            result.issues.append(
                YAMLIssue(path, "warning", f"Source '{src.get('name', '?')}' has no tables")
            )
        for j, tbl in enumerate(tables):
            if isinstance(tbl, dict) and "name" not in tbl:
                result.issues.append(
                    YAMLIssue(
                        path,
                        "error",
                        f"Source '{src.get('name', '?')}' table[{j}] missing 'name'",
                    )
                )


def _validate_schema(
    path: str, doc: Dict[str, Any], result: YAMLValidationResult
) -> None:
    """Validate a dbt schema.yml structure."""
    models = doc.get("models")
    if models is None:
        result.issues.append(YAMLIssue(path, "warning", "No 'models' key found"))
        return

    if not isinstance(models, list):
        result.issues.append(YAMLIssue(path, "error", "'models' must be a list"))
        return

    for i, mdl in enumerate(models):
        if not isinstance(mdl, dict):
            result.issues.append(
                YAMLIssue(path, "error", f"models[{i}] must be a mapping")
            )
            continue
        if "name" not in mdl:
            result.issues.append(
                YAMLIssue(path, "error", f"models[{i}] missing 'name'")
            )
        columns = mdl.get("columns", [])
        if not columns:
            result.issues.append(
                YAMLIssue(
                    path,
                    "warning",
                    f"Model '{mdl.get('name', '?')}' has no columns defined",
                )
            )

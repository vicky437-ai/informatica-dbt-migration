"""Generate dbt_project.yml and packages.yml for a converted Informatica mapping.

Inspects the generated files to determine which layers exist, what
materializations to configure, and whether any dbt packages are needed
(e.g. dbt_utils for surrogate keys).
"""

from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Set

from informatica_to_dbt.generator.response_parser import GeneratedFile

logger = logging.getLogger("informatica_dbt")

# Patterns to detect dbt package usage in generated SQL/YAML
_DBT_UTILS_RE = re.compile(r"\bdbt_utils\b")
_DBT_DATE_SPINE_RE = re.compile(r"\bdbt_date\b")
_DBT_EXPECTATIONS_RE = re.compile(r"\bdbt_expectations\b")

# Package versions — update as needed
_PACKAGE_VERSIONS: Dict[str, str] = {
    "dbt-labs/dbt_utils": "1.3.0",
    "calogica/dbt_date": "0.10.1",
    "calogica/dbt_expectations": "0.10.4",
}


def generate_dbt_project_yml(
    project_name: str,
    files: List[GeneratedFile],
    profile: str = "snowflake",
    target_database: Optional[str] = None,
    target_schema: Optional[str] = None,
) -> GeneratedFile:
    """Generate a dbt_project.yml based on generated model files.

    Args:
        project_name: Name for the dbt project (sanitized to snake_case).
        files: All generated files — used to detect layers and materializations.
        profile: dbt profile name (default ``snowflake``).
        target_database: Optional Snowflake database override for vars.
        target_schema: Optional Snowflake schema override for vars.

    Returns:
        A :class:`GeneratedFile` with path ``dbt_project.yml``.
    """
    safe_name = _sanitize_project_name(project_name)
    layers = _detect_layers(files)
    has_incremental = _detect_incremental(files)

    lines = [
        f"name: '{safe_name}'",
        "version: '1.0.0'",
        "",
        f"profile: '{profile}'",
        "",
        "# Require dbt 1.7+",
        "require-dbt-version: ['>=1.7.0', '<2.0.0']",
        "",
        "model-paths: ['models']",
        "analysis-paths: ['analyses']",
        "test-paths: ['tests']",
        "seed-paths: ['seeds']",
        "macro-paths: ['macros']",
        "snapshot-paths: ['snapshots']",
        "",
        "clean-targets:",
        "  - 'target'",
        "  - 'dbt_packages'",
    ]

    # Model configurations per layer
    lines.append("")
    lines.append("models:")
    lines.append(f"  {safe_name}:")

    if "staging" in layers:
        lines.extend([
            "    staging:",
            "      +materialized: view",
            "      +schema: staging",
        ])

    if "intermediate" in layers:
        mat = "incremental" if has_incremental else "table"
        lines.extend([
            "    intermediate:",
            f"      +materialized: {mat}",
            "      +schema: intermediate",
        ])

    if "marts" in layers:
        lines.extend([
            "    marts:",
            "      +materialized: table",
            "      +schema: marts",
        ])

    # Vars section (useful for source database/schema overrides)
    if target_database or target_schema:
        lines.append("")
        lines.append("vars:")
        if target_database:
            lines.append(f"  source_database: '{target_database}'")
        if target_schema:
            lines.append(f"  source_schema: '{target_schema}'")

    content = "\n".join(lines) + "\n"
    return GeneratedFile(path="dbt_project.yml", content=content)


def generate_packages_yml(files: List[GeneratedFile]) -> Optional[GeneratedFile]:
    """Generate packages.yml if generated code uses dbt packages.

    Returns ``None`` if no packages are needed.
    """
    needed = _detect_packages(files)
    if not needed:
        return None

    lines = ["packages:"]
    for pkg in sorted(needed):
        version = _PACKAGE_VERSIONS.get(pkg, "")
        lines.append(f"  - package: {pkg}")
        if version:
            lines.append(f"    version: '{version}'")

    content = "\n".join(lines) + "\n"
    return GeneratedFile(path="packages.yml", content=content)


def generate_project_files(
    project_name: str,
    files: List[GeneratedFile],
    profile: str = "snowflake",
    target_database: Optional[str] = None,
    target_schema: Optional[str] = None,
) -> List[GeneratedFile]:
    """Generate all project-level files (dbt_project.yml + packages.yml).

    Returns a list of :class:`GeneratedFile` objects to be added to the
    generated output.
    """
    result = [
        generate_dbt_project_yml(
            project_name, files, profile, target_database, target_schema,
        )
    ]

    packages = generate_packages_yml(files)
    if packages:
        result.append(packages)

    logger.info(
        "Generated %d project file(s) for '%s'",
        len(result), project_name,
    )
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sanitize_project_name(name: str) -> str:
    """Convert mapping/workflow name to valid dbt project name (snake_case)."""
    # Replace non-alphanumeric with underscore, lowercase
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", name).lower()
    # Collapse multiple underscores
    safe = re.sub(r"_+", "_", safe).strip("_")
    # Must start with a letter
    if safe and not safe[0].isalpha():
        safe = "dbt_" + safe
    return safe or "dbt_project"


def _detect_layers(files: List[GeneratedFile]) -> Set[str]:
    """Detect which dbt layers have SQL files."""
    layers: Set[str] = set()
    for gf in files:
        if gf.is_sql:
            layer = gf.layer
            if layer in ("staging", "intermediate", "marts"):
                layers.add(layer)
    return layers


def _detect_incremental(files: List[GeneratedFile]) -> bool:
    """Check if any generated SQL uses incremental materialization."""
    for gf in files:
        if gf.is_sql and "materialized='incremental'" in gf.content:
            return True
    return False


def _detect_packages(files: List[GeneratedFile]) -> Set[str]:
    """Scan generated files for dbt package references."""
    packages: Set[str] = set()
    all_content = "\n".join(gf.content for gf in files)

    if _DBT_UTILS_RE.search(all_content):
        packages.add("dbt-labs/dbt_utils")
    if _DBT_DATE_SPINE_RE.search(all_content):
        packages.add("calogica/dbt_date")
    if _DBT_EXPECTATIONS_RE.search(all_content):
        packages.add("calogica/dbt_expectations")

    return packages

"""Project-level validation: ref() integrity and DAG cycle detection.

Ensures that every ``ref('x')`` call targets a model that exists in
the generated output, and that no circular dependencies exist.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Set

from informatica_to_dbt.exceptions import DAGCycleError, RefIntegrityError
from informatica_to_dbt.generator.response_parser import GeneratedFile

logger = logging.getLogger("informatica_dbt")

_REF_RE = re.compile(r"\{\{\s*ref\s*\(\s*['\"](\w+)['\"]\s*\)\s*\}\}")
_SOURCE_RE = re.compile(
    r"\{\{\s*source\s*\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\s*\)\s*\}\}"
)


@dataclass
class ProjectIssue:
    """A single project-level validation finding."""

    file_path: str
    severity: str
    message: str


@dataclass
class ProjectValidationResult:
    """Aggregated project-level validation result."""

    issues: List[ProjectIssue] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not any(i.severity == "error" for i in self.issues)

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "warning")


def validate_project(files: List[GeneratedFile]) -> ProjectValidationResult:
    """Run project-level validation across all generated files.

    Checks:
        1. ref() integrity — all ref() targets exist as generated models.
        2. DAG acyclicity — no circular ref() chains.
        3. Source consistency — sources referenced match sources.yml.
    """
    result = ProjectValidationResult()

    sql_files = [f for f in files if f.is_sql]
    yaml_files = [f for f in files if f.is_yaml]

    if not sql_files:
        return result

    # Build model name → file path index
    model_index = _build_model_index(sql_files)

    # Build source index from YAML
    source_index = _build_source_index(yaml_files)

    # Check ref() integrity
    _check_ref_integrity(sql_files, model_index, result)

    # Check for DAG cycles
    _check_dag_cycles(sql_files, model_index, result)

    # Check source() references
    _check_source_integrity(sql_files, source_index, result)

    # Check duplicate model names
    _check_duplicate_models(sql_files, result)

    # Check layer naming conventions
    _check_layer_naming(sql_files, result)

    # Cross-validate schema YAML ↔ SQL files
    schema_model_index = _build_schema_model_index(yaml_files)
    _check_schema_sql_consistency(sql_files, schema_model_index, model_index, result)

    # Check for orphan models (no refs pointing to them, not in marts)
    _check_orphan_models(sql_files, model_index, result)

    logger.info(
        "Project validation: %d error(s), %d warning(s)",
        result.error_count,
        result.warning_count,
    )
    return result


def _build_model_index(sql_files: List[GeneratedFile]) -> Dict[str, str]:
    """Map model name → file path for all SQL files."""
    index: Dict[str, str] = {}
    for gf in sql_files:
        # Extract model name from path: models/staging/stg_foo.sql → stg_foo
        name = gf.path.rsplit("/", 1)[-1].replace(".sql", "")
        index[name] = gf.path
    return index


def _build_source_index(yaml_files: List[GeneratedFile]) -> Set[str]:
    """Build a set of (source_name, table_name) tuples from YAML files."""
    sources: Set[str] = set()
    try:
        import yaml
    except ImportError:
        return sources

    for gf in yaml_files:
        try:
            doc = yaml.safe_load(gf.content)
        except Exception:
            continue
        if not isinstance(doc, dict):
            continue
        for src in doc.get("sources", []):
            if not isinstance(src, dict):
                continue
            src_name = src.get("name", "")
            for tbl in src.get("tables", []):
                if isinstance(tbl, dict):
                    tbl_name = tbl.get("name", "")
                    sources.add(f"{src_name}.{tbl_name}")
    return sources


def _check_ref_integrity(
    sql_files: List[GeneratedFile],
    model_index: Dict[str, str],
    result: ProjectValidationResult,
) -> None:
    """Ensure every ref() call targets an existing model."""
    for gf in sql_files:
        refs = _REF_RE.findall(gf.content)
        for ref_name in refs:
            if ref_name not in model_index:
                result.issues.append(
                    ProjectIssue(
                        gf.path,
                        "error",
                        f"ref('{ref_name}') targets a model that doesn't exist "
                        f"in the generated output",
                    )
                )


def _check_dag_cycles(
    sql_files: List[GeneratedFile],
    model_index: Dict[str, str],
    result: ProjectValidationResult,
) -> None:
    """Detect circular ref() dependencies using DFS."""
    # Build adjacency list
    graph: Dict[str, List[str]] = {}
    for gf in sql_files:
        model_name = gf.path.rsplit("/", 1)[-1].replace(".sql", "")
        refs = _REF_RE.findall(gf.content)
        graph[model_name] = refs

    # DFS cycle detection
    visited: Set[str] = set()
    in_stack: Set[str] = set()

    def _dfs(node: str, path: List[str]) -> bool:
        if node in in_stack:
            cycle = path[path.index(node) :] + [node]
            result.issues.append(
                ProjectIssue(
                    model_index.get(node, "unknown"),
                    "error",
                    f"Circular dependency detected: {' → '.join(cycle)}",
                )
            )
            return True
        if node in visited:
            return False
        visited.add(node)
        in_stack.add(node)
        for dep in graph.get(node, []):
            if _dfs(dep, path + [node]):
                return True
        in_stack.discard(node)
        return False

    for model_name in graph:
        if model_name not in visited:
            _dfs(model_name, [])


def _check_source_integrity(
    sql_files: List[GeneratedFile],
    source_index: Set[str],
    result: ProjectValidationResult,
) -> None:
    """Ensure every source() call matches a defined source."""
    if not source_index:
        # No sources.yml parsed — skip
        return

    for gf in sql_files:
        sources = _SOURCE_RE.findall(gf.content)
        for src_name, tbl_name in sources:
            key = f"{src_name}.{tbl_name}"
            if key not in source_index:
                result.issues.append(
                    ProjectIssue(
                        gf.path,
                        "warning",
                        f"source('{src_name}', '{tbl_name}') not defined in sources.yml",
                    )
                )


# -- Phase 4 new checks -----------------------------------------------------

# Layer prefix → expected directory names
_LAYER_PREFIXES = {
    "stg_": {"staging"},
    "int_": {"intermediate"},
    "fct_": {"marts"},
    "dim_": {"marts"},
}


def _check_duplicate_models(
    sql_files: List[GeneratedFile],
    result: ProjectValidationResult,
) -> None:
    """Detect two or more SQL files that produce the same model name."""
    seen: Dict[str, str] = {}
    for gf in sql_files:
        name = gf.path.rsplit("/", 1)[-1].replace(".sql", "")
        if name in seen:
            result.issues.append(
                ProjectIssue(
                    gf.path,
                    "error",
                    f"Duplicate model name '{name}' — also generated at {seen[name]}",
                )
            )
        else:
            seen[name] = gf.path


def _check_layer_naming(
    sql_files: List[GeneratedFile],
    result: ProjectValidationResult,
) -> None:
    """Warn when file prefix doesn't match its directory layer."""
    for gf in sql_files:
        name = gf.path.rsplit("/", 1)[-1].replace(".sql", "")
        layer = gf.layer  # staging / intermediate / marts / macros / other

        if layer == "macros" or layer == "other":
            continue

        for prefix, expected_layers in _LAYER_PREFIXES.items():
            if name.startswith(prefix) and layer not in expected_layers:
                result.issues.append(
                    ProjectIssue(
                        gf.path,
                        "warning",
                        f"Model '{name}' has prefix '{prefix}' but is in '{layer}/' "
                        f"(expected {'/'.join(expected_layers)}/)",
                    )
                )
                break  # one warning per file


def _build_schema_model_index(yaml_files: List[GeneratedFile]) -> Set[str]:
    """Extract model names defined in schema.yml files."""
    model_names: Set[str] = set()
    try:
        import yaml
    except ImportError:
        return model_names

    for gf in yaml_files:
        # Only look at schema files (not sources-only files)
        try:
            doc = yaml.safe_load(gf.content)
        except Exception:
            continue
        if not isinstance(doc, dict):
            continue
        for model in doc.get("models", []):
            if isinstance(model, dict) and "name" in model:
                model_names.add(model["name"])
    return model_names


def _check_schema_sql_consistency(
    sql_files: List[GeneratedFile],
    schema_model_index: Set[str],
    model_index: Dict[str, str],
    result: ProjectValidationResult,
) -> None:
    """Cross-validate schema YAML models against SQL files.

    - Models in schema.yml without a corresponding SQL file → error
    - SQL files without a schema.yml entry → warning (undocumented)
    """
    if not schema_model_index:
        return

    # schema.yml references a model that doesn't exist as SQL
    for schema_model in schema_model_index:
        if schema_model not in model_index:
            result.issues.append(
                ProjectIssue(
                    "schema.yml",
                    "error",
                    f"Model '{schema_model}' defined in schema.yml but no "
                    f"corresponding SQL file found",
                )
            )

    # SQL files not documented in schema.yml
    for model_name, model_path in model_index.items():
        if model_name not in schema_model_index:
            result.issues.append(
                ProjectIssue(
                    model_path,
                    "warning",
                    f"Model '{model_name}' has no entry in schema.yml "
                    f"(undocumented model)",
                )
            )


def _check_orphan_models(
    sql_files: List[GeneratedFile],
    model_index: Dict[str, str],
    result: ProjectValidationResult,
) -> None:
    """Warn about models that nothing references (not in marts layer).

    Mart models are expected endpoints. Staging/intermediate models that
    are never ref()'d by another model may indicate a broken DAG.
    """
    # Collect all ref() targets across all files
    referenced: Set[str] = set()
    for gf in sql_files:
        referenced.update(_REF_RE.findall(gf.content))

    for gf in sql_files:
        name = gf.path.rsplit("/", 1)[-1].replace(".sql", "")
        layer = gf.layer

        # Marts are terminal — skip
        if layer in ("marts", "macros", "other"):
            continue

        if name not in referenced:
            result.issues.append(
                ProjectIssue(
                    gf.path,
                    "warning",
                    f"Model '{name}' in {layer}/ is never referenced by another model "
                    f"(orphan or missing ref())",
                )
            )

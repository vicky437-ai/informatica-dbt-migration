"""Consolidate per-mapping conversion outputs into a single dbt project.

Supports two modes:
  - ``new``:   Create a fresh dbt project from scratch.
  - ``merge``: Merge generated models into an existing dbt project,
               preserving manually-added models and configuration.

The merger takes a list of :class:`MappingResult` objects (from the
orchestrator) and writes a unified project directory with:
  - Consolidated ``dbt_project.yml``
  - Deduplicated ``models/<mapping>/`` directories (staging / intermediate / marts)
  - Merged ``_sources.yml`` files (no duplicate source definitions)
  - Consolidated ``packages.yml``
  - Shared macros
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set

import yaml

from informatica_to_dbt.config import Config
from informatica_to_dbt.generator.dbt_project_generator import _sanitize_project_name
from informatica_to_dbt.generator.response_parser import GeneratedFile
from informatica_to_dbt.merger.conflict_resolver import resolve_conflicts

logger = logging.getLogger("informatica_dbt")


@dataclass
class MergeResult:
    """Outcome of a project merge operation."""

    project_dir: str
    files_written: int = 0
    files_skipped: int = 0
    sources_consolidated: int = 0
    conflicts_resolved: int = 0
    warnings: List[str] = field(default_factory=list)
    mode: str = "new"

    @property
    def summary(self) -> str:
        return (
            f"MergeResult(mode={self.mode}, written={self.files_written}, "
            f"skipped={self.files_skipped}, sources_consolidated={self.sources_consolidated}, "
            f"conflicts={self.conflicts_resolved})"
        )


class ProjectMerger:
    """Merge per-mapping outputs into a unified dbt project directory.

    Args:
        config: Pipeline configuration (uses ``project_dir``, ``merge_mode``,
                ``dbt_project_name``).
    """

    def __init__(self, config: Config):
        self._config = config
        self._project_dir = Path(config.project_dir or "dbt_output")
        self._mode = config.merge_mode  # "new" or "merge"
        self._project_name = _sanitize_project_name(
            config.dbt_project_name or "informatica_dbt"
        )

    def merge(self, mapping_results: List) -> MergeResult:
        """Merge all mapping results into the target project directory.

        Args:
            mapping_results: List of ``MappingResult`` objects from the
                orchestrator.  Only results with ``status != 'failed'``
                and non-empty ``files`` are included.

        Returns:
            A :class:`MergeResult` describing what was written.
        """
        result = MergeResult(
            project_dir=str(self._project_dir),
            mode=self._mode,
        )

        # Filter to successful results with files
        valid_results = [
            mr for mr in mapping_results
            if mr.status != "failed" and mr.files
        ]
        if not valid_results:
            result.warnings.append("No successful mappings to merge")
            logger.warning("No successful mappings to merge")
            return result

        logger.info(
            "Merging %d mapping(s) into '%s' (mode=%s)",
            len(valid_results), self._project_dir, self._mode,
        )

        # Prepare target directory
        if self._mode == "new":
            self._init_new_project()
        else:
            self._validate_existing_project(result)

        # Collect macro files separately; all other files stay grouped per mapping.
        all_macro_files: List[GeneratedFile] = []
        # mapping_name → list of non-macro files (SQL, sources YAML, schema YAML)
        all_mapping_files: Dict[str, List[GeneratedFile]] = {}

        for mr in valid_results:
            mapping_name = mr.mapping_name
            mapping_files = []
            for gf in mr.files:
                # Skip per-mapping dbt_project.yml and packages.yml — we generate unified ones
                if gf.path in ("dbt_project.yml", "packages.yml"):
                    continue
                if "macros/" in gf.path:
                    all_macro_files.append(gf)
                else:
                    mapping_files.append(gf)
            all_mapping_files[mapping_name] = mapping_files

        # 1. Write all per-mapping files under models/<mapping_name>/<layer>/
        #    The LLM generates paths like "models/staging/stg_foo.sql" or
        #    "models/staging/_sources.yml".  We rewrite them to
        #    "models/<mapping_name>/staging/stg_foo.sql" etc.
        for mapping_name, files in all_mapping_files.items():
            for gf in files:
                rel_path = gf.path
                if rel_path.startswith("models/"):
                    # Strip leading "models/" and re-prefix with mapping dir
                    # e.g. "models/staging/stg_foo.sql" → "models/<mapping>/staging/stg_foo.sql"
                    rel_path = f"models/{mapping_name}/{rel_path[len('models/'):]}"
                elif not rel_path.startswith("models/"):
                    # Path without models/ prefix — nest under mapping dir
                    rel_path = f"models/{mapping_name}/{rel_path}"
                written = self._write_file(rel_path, gf.content, result)
                if written:
                    result.files_written += 1
                else:
                    result.files_skipped += 1

        # 2. Resolve and write macros (deduplicate by name)
        unique_macros = resolve_conflicts(all_macro_files)
        result.conflicts_resolved = len(all_macro_files) - len(unique_macros)
        for gf in unique_macros:
            rel_path = gf.path
            if not rel_path.startswith("macros/"):
                rel_path = f"macros/{os.path.basename(gf.path)}"
            self._write_file(rel_path, gf.content, result)
            result.files_written += 1

        # 3. Generate unified dbt_project.yml
        self._write_project_yml(valid_results, result)

        # 4. Generate unified packages.yml
        self._write_packages_yml(valid_results, result)

        # 5. Inject source schema override into all _sources.yml files
        if self._config.source_schema_override:
            self._inject_source_schema(result)

        logger.info("Merge complete: %s", result.summary)
        return result

    # ------------------------------------------------------------------
    # Project initialization
    # ------------------------------------------------------------------

    def _init_new_project(self) -> None:
        """Create a fresh project directory structure."""
        for subdir in ("models", "macros", "tests", "seeds", "snapshots", "analyses"):
            (self._project_dir / subdir).mkdir(parents=True, exist_ok=True)
        logger.info("Initialized new dbt project at '%s'", self._project_dir)

    def _validate_existing_project(self, result: MergeResult) -> None:
        """Validate that the target is an existing dbt project."""
        proj_yml = self._project_dir / "dbt_project.yml"
        if not proj_yml.exists():
            result.warnings.append(
                f"No dbt_project.yml found at {self._project_dir} — "
                f"creating as new project instead"
            )
            logger.warning(
                "merge mode requested but no dbt_project.yml at '%s'; "
                "falling back to new project init",
                self._project_dir,
            )
            self._init_new_project()

    # ------------------------------------------------------------------
    # File writing
    # ------------------------------------------------------------------

    def _write_file(
        self,
        rel_path: str,
        content: str,
        result: MergeResult,
    ) -> bool:
        """Write a file to the project directory.

        In ``merge`` mode, existing files that were NOT generated by this
        tool are preserved (skipped).  Generated files are overwritten.

        Returns True if the file was written, False if skipped.
        """
        out_path = self._project_dir / rel_path
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if self._mode == "merge" and out_path.exists():
            existing = out_path.read_text(encoding="utf-8")
            # If the existing file doesn't have our marker, skip it
            if _GENERATED_MARKER not in existing:
                logger.debug("Skipping existing non-generated file: %s", rel_path)
                return False

        # Add generation marker to content
        marked_content = _add_marker(rel_path, content)
        out_path.write_text(marked_content, encoding="utf-8")
        logger.debug("Wrote: %s", rel_path)
        return True

    # ------------------------------------------------------------------
    # Source schema injection
    # ------------------------------------------------------------------

    def _inject_source_schema(self, result: MergeResult) -> None:
        """Inject source_schema_override into all _sources.yml files.

        Walks the models/ directory, finds every _sources.yml, parses it,
        sets ``schema: <override>`` on every source definition, and writes
        it back.
        """
        schema = self._config.source_schema_override
        models_dir = self._project_dir / "models"
        if not models_dir.exists():
            return

        sources_files = list(models_dir.rglob("_sources.yml"))
        patched = 0
        for src_path in sources_files:
            try:
                raw = src_path.read_text(encoding="utf-8")
                data = yaml.safe_load(raw)
                if not isinstance(data, dict) or "sources" not in data:
                    continue

                changed = False
                for source_def in data["sources"]:
                    if isinstance(source_def, dict):
                        if source_def.get("schema") != schema:
                            source_def["schema"] = schema
                            changed = True

                if changed:
                    # Preserve the generation marker comment
                    marker_line = ""
                    if raw.startswith("# Generated by infa2dbt"):
                        marker_line = raw.splitlines()[0] + "\n"

                    dumped = yaml.dump(data, default_flow_style=False, sort_keys=False)
                    src_path.write_text(marker_line + dumped, encoding="utf-8")
                    patched += 1
                    logger.info("Injected schema '%s' into %s", schema, src_path.name)

            except Exception as exc:
                result.warnings.append(f"Failed to patch {src_path}: {exc}")
                logger.warning("Failed to inject schema into %s: %s", src_path, exc)

        if patched:
            logger.info("Source schema override: patched %d _sources.yml file(s)", patched)

    # ------------------------------------------------------------------
    # Unified project files
    # ------------------------------------------------------------------

    def _write_project_yml(self, mapping_results: List, result: MergeResult) -> None:
        """Generate a unified dbt_project.yml covering all mappings.

        Uses a read-merge-write pattern: if an existing dbt_project.yml is
        present, its mapping configurations are preserved and new mappings
        from the current run are merged in.  This prevents overwriting
        configs from prior runs (C1 fix).
        """
        # Detect which layers exist per mapping in THIS run
        new_mapping_layers: Dict[str, Set[str]] = {}
        for mr in mapping_results:
            if mr.status == "failed" or not mr.files:
                continue
            layers: Set[str] = set()
            for gf in mr.files:
                if gf.is_sql:
                    layer = gf.layer
                    if layer in ("staging", "intermediate", "marts"):
                        layers.add(layer)
            new_mapping_layers[mr.mapping_name] = layers

        # Read existing dbt_project.yml to preserve prior mapping configs
        existing_mapping_layers: Dict[str, Set[str]] = {}
        out_path = self._project_dir / "dbt_project.yml"
        if out_path.exists():
            try:
                existing_data = yaml.safe_load(out_path.read_text(encoding="utf-8"))
                if isinstance(existing_data, dict):
                    models_section = existing_data.get("models", {})
                    project_models = models_section.get(self._project_name, {})
                    if isinstance(project_models, dict):
                        for mapping_name, mapping_cfg in project_models.items():
                            if not isinstance(mapping_cfg, dict):
                                continue
                            layers: Set[str] = set()
                            for layer_name in ("staging", "intermediate", "marts"):
                                if layer_name in mapping_cfg:
                                    layers.add(layer_name)
                            existing_mapping_layers[mapping_name] = layers
                logger.debug(
                    "Read %d existing mapping config(s) from dbt_project.yml",
                    len(existing_mapping_layers),
                )
            except Exception as exc:
                logger.warning(
                    "Could not parse existing dbt_project.yml for merge: %s", exc
                )

        # Merge: new mappings override existing ones with same name;
        # existing mappings not in current run are preserved.
        merged_mapping_layers: Dict[str, Set[str]] = {}
        merged_mapping_layers.update(existing_mapping_layers)
        merged_mapping_layers.update(new_mapping_layers)

        lines = [
            f"# {_GENERATED_MARKER}",
            f"name: '{self._project_name}'",
            "version: '1.0.0'",
            "",
            "profile: 'informatica_to_dbt_migration'",
            "",
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
            "",
            "models:",
            f"  {self._project_name}:",
        ]

        # Per-mapping config blocks with tags and layer materializations
        for mapping_name in sorted(merged_mapping_layers.keys()):
            layers = merged_mapping_layers[mapping_name]
            lines.append(f"    {mapping_name}:")
            lines.append(f"      +tags: ['{mapping_name}']")
            if "staging" in layers:
                lines.append("      staging:")
                lines.append("        +materialized: view")
            if "intermediate" in layers:
                lines.append("      intermediate:")
                lines.append("        +materialized: view")
            if "marts" in layers:
                lines.append("      marts:")
                lines.append("        +materialized: table")

        content = "\n".join(lines) + "\n"
        out_path.write_text(content, encoding="utf-8")
        result.files_written += 1
        logger.info("Wrote unified dbt_project.yml (%d mapping(s))", len(merged_mapping_layers))

    def _write_packages_yml(self, mapping_results: List, result: MergeResult) -> None:
        """Generate a unified packages.yml from all mapping outputs."""
        needed: Set[str] = set()
        pkg_patterns = {
            "dbt-labs/dbt_utils": re.compile(r"\bdbt_utils\b"),
            "calogica/dbt_date": re.compile(r"\bdbt_date\b"),
            "calogica/dbt_expectations": re.compile(r"\bdbt_expectations\b"),
        }
        pkg_versions = {
            "dbt-labs/dbt_utils": "1.3.0",
            "calogica/dbt_date": "0.10.1",
            "calogica/dbt_expectations": "0.10.4",
        }

        for mr in mapping_results:
            all_content = "\n".join(gf.content for gf in mr.files)
            for pkg, pat in pkg_patterns.items():
                if pat.search(all_content):
                    needed.add(pkg)

        if not needed:
            return

        lines = ["packages:"]
        for pkg in sorted(needed):
            version = pkg_versions.get(pkg, "")
            lines.append(f"  - package: {pkg}")
            if version:
                lines.append(f"    version: '{version}'")

        content = "\n".join(lines) + "\n"
        out_path = self._project_dir / "packages.yml"
        out_path.write_text(content, encoding="utf-8")
        result.files_written += 1
        logger.info("Wrote unified packages.yml")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_GENERATED_MARKER = "-- Generated by infa2dbt"


def _add_marker(path: str, content: str) -> str:
    """Add a generation marker comment to file content."""
    if path.endswith(".sql"):
        if _GENERATED_MARKER not in content:
            return f"{_GENERATED_MARKER}\n{content}"
    elif path.endswith((".yml", ".yaml")):
        marker = _GENERATED_MARKER.replace("--", "#")
        if marker not in content:
            return f"{marker}\n{content}"
    return content

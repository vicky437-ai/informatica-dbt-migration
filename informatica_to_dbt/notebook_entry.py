"""Snowflake Notebook entry point for the Informatica-to-dbt converter.

Paste this into a Snowflake Notebook cell (or import as a module) to run
the full conversion pipeline against XML files on a Snowflake stage.

Prerequisites:
    - Upload the ``informatica_to_dbt`` package to a Snowflake stage and
      add it to the notebook's Packages section, or use ``sys.path`` to
      make it importable.
    - Ensure the input stage (``@XML_INPUT``) contains the XML exports.
    - Tables for output and summary must exist (DDL provided below).

Usage (Notebook Cell):
    %run notebook_entry.py

    Or call directly:
        from informatica_to_dbt.notebook_entry import run_pipeline
        results, metrics = run_pipeline()
"""

from __future__ import annotations

import logging
from typing import List, Optional, Tuple

from informatica_to_dbt.config import Config
from informatica_to_dbt.metrics import MappingMetrics, RepositoryMetrics
from informatica_to_dbt.orchestrator import MappingResult


# ---------------------------------------------------------------------------
# Pipeline result container
# ---------------------------------------------------------------------------

class PipelineResult:
    """Container for the full pipeline output — results + metrics + report.

    Provides convenient access patterns for Snowflake Notebook cells::

        pr = run_pipeline()
        print(pr.report)          # human-readable summary
        pr.metrics.to_dict()      # JSON-serialisable metrics
        pr.results                # per-mapping results
        pr.retryable_failures     # mappings that can be retried
    """

    def __init__(
        self,
        results: List[MappingResult],
        metrics: RepositoryMetrics,
    ):
        self.results = results
        self.metrics = metrics

    @property
    def report(self) -> str:
        """Human-readable conversion report."""
        return format_report(self.results, self.metrics)

    @property
    def success_count(self) -> int:
        return sum(1 for r in self.results if r.status == "success")

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if r.status == "failed")

    @property
    def partial_count(self) -> int:
        return sum(1 for r in self.results if r.status == "partial")

    @property
    def retryable_failures(self) -> List[MappingResult]:
        """Mappings that failed with a retryable error category."""
        from informatica_to_dbt.exceptions import ErrorCategory
        retryable = []
        for r in self.results:
            if r.status != "failed" or not r.error_message:
                continue
            # Error messages are formatted as "[category] ..."
            for cat in ErrorCategory:
                if r.error_message.startswith(f"[{cat.value}]") and cat.is_retryable:
                    retryable.append(r)
                    break
        return retryable

    def __repr__(self) -> str:
        return (
            f"PipelineResult(mappings={len(self.results)}, "
            f"success={self.success_count}, partial={self.partial_count}, "
            f"failed={self.failed_count})"
        )


# ---------------------------------------------------------------------------
# Report formatter
# ---------------------------------------------------------------------------

def format_report(
    results: List[MappingResult],
    metrics: RepositoryMetrics,
) -> str:
    """Build a human-readable conversion report string."""
    lines: list[str] = []
    lines.append("=" * 70)
    lines.append("INFORMATICA-TO-DBT CONVERSION REPORT")
    lines.append("=" * 70)
    lines.append("")

    # Per-mapping details
    for r in results:
        status_tag = r.status.upper()
        complexity_info = ""
        if r.complexity:
            complexity_info = (
                f" | complexity={r.complexity.score}, "
                f"strategy={r.complexity.strategy.name}"
            )

        lines.append(
            f"  {r.mapping_name:<40s} {status_tag}{complexity_info}"
        )
        lines.append(
            f"    files={len(r.files)}, "
            f"chunks={r.chunks_processed}, "
            f"heals={r.heal_attempts}, "
            f"time={r.elapsed_seconds:.1f}s"
        )

        # Quality score
        if r.quality_report:
            lines.append(
                f"    quality={r.quality_report.total_score}/100"
            )

        # Validation summary
        errs = []
        if r.sql_validation and r.sql_validation.error_count:
            errs.append(f"sql_errors={r.sql_validation.error_count}")
        if r.yaml_validation and r.yaml_validation.error_count:
            errs.append(f"yaml_errors={r.yaml_validation.error_count}")
        if r.project_validation and r.project_validation.error_count:
            errs.append(f"project_errors={r.project_validation.error_count}")
        if errs:
            lines.append(f"    validation: {', '.join(errs)}")

        # Error message for failed mappings
        if r.error_message:
            lines.append(f"    error: {r.error_message}")

        lines.append("")

    # Aggregate summary
    lines.append("-" * 70)
    lines.append("SUMMARY")
    lines.append("-" * 70)
    lines.append(f"  Total mappings:     {len(results)}")
    lines.append(f"  Success:            {metrics.success_count}")
    lines.append(f"  Partial:            {metrics.partial_count}")
    lines.append(f"  Failed:             {metrics.failed_count}")
    lines.append(f"  Success rate:       {metrics.success_rate:.0%}")
    lines.append(f"  Files generated:    {metrics.total_files_generated}")
    lines.append(f"  LLM calls:          {metrics.total_llm_calls}")
    lines.append(f"  LLM time:           {metrics.total_llm_seconds:.1f}s")
    lines.append(f"  Heal attempts:      {metrics.total_heal_attempts}")
    lines.append(f"  Avg quality:        {metrics.avg_quality_score:.0f}/100")
    lines.append(f"  Avg complexity:     {metrics.avg_complexity:.0f}")
    lines.append(f"  Total errors:       {metrics.total_errors}")
    lines.append(f"  Total warnings:     {metrics.total_warnings}")
    lines.append(f"  Total time:         {metrics.total_seconds:.1f}s")
    lines.append("=" * 70)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main pipeline entry point
# ---------------------------------------------------------------------------

def run_pipeline(
    xml_file_path: Optional[str] = None,
    xml_dir_path: Optional[str] = None,
    config_overrides: Optional[dict] = None,
    show_progress: bool = True,
) -> PipelineResult:
    """Run the Informatica-to-dbt conversion pipeline.

    Args:
        xml_file_path: Optional local file path to a single XML file.
            If not provided, reads all XML files from the configured input stage.
        xml_dir_path: Optional local directory containing XML files.
            All ``*.XML`` / ``*.xml`` files in the directory will be processed.
            Mutually exclusive with ``xml_file_path``.
        config_overrides: Optional dict of Config field overrides.
        show_progress: If True, prints live progress to stdout
            (useful in Snowflake Notebooks).

    Returns:
        A :class:`PipelineResult` with results, metrics, and report.
    """
    import glob as globmod
    import os

    if xml_file_path and xml_dir_path:
        raise ValueError("Provide either xml_file_path or xml_dir_path, not both.")

    # -- Setup ---------------------------------------------------------------
    from informatica_to_dbt.generator.llm_client import LLMClient
    from informatica_to_dbt.orchestrator import convert_repository
    from informatica_to_dbt.persistence.snowflake_io import SnowflakeIO
    from informatica_to_dbt.progress import (
        CompositeProgressCallback,
        LoggingProgressCallback,
        PrintProgressCallback,
    )
    from informatica_to_dbt.utils import Timer, setup_logging
    from informatica_to_dbt.xml_parser.parser import InformaticaXMLParser

    overrides = dict(config_overrides or {})
    # Enable local_mode automatically when using local paths
    if xml_file_path or xml_dir_path:
        overrides.setdefault("local_mode", True)

    config = Config(**overrides)
    errors = config.validate()
    if errors:
        raise ValueError(f"Invalid config: {'; '.join(errors)}")

    logger = setup_logging(config.log_level)
    logger.info("Starting Informatica-to-dbt conversion pipeline v1.0")

    parser = InformaticaXMLParser()
    llm_client = LLMClient(config)
    io = SnowflakeIO(config)

    logger.info("LLM client mode: %s", llm_client.mode)

    # Build progress callback
    callbacks = [LoggingProgressCallback()]
    if show_progress:
        callbacks.append(PrintProgressCallback())
    progress = CompositeProgressCallback(callbacks)

    # -- Single file mode ----------------------------------------------------
    if xml_file_path:
        logger.info("Parsing single file: %s", xml_file_path)
        with Timer("parse"):
            repo = parser.parse_file(xml_file_path)

        with Timer("convert_all"):
            results, repo_metrics = convert_repository(
                repo, config, llm_client, io, progress,
            )

        pipeline_result = PipelineResult(results, repo_metrics)
        if show_progress:
            print(pipeline_result.report)
        return pipeline_result

    # -- Batch local directory mode ------------------------------------------
    if xml_dir_path:
        xml_dir_path = os.path.abspath(xml_dir_path)
        if not os.path.isdir(xml_dir_path):
            raise ValueError(f"xml_dir_path is not a directory: {xml_dir_path}")

        xml_files = sorted(
            globmod.glob(os.path.join(xml_dir_path, "*.XML"))
            + globmod.glob(os.path.join(xml_dir_path, "*.xml"))
        )
        # Deduplicate (case-insensitive filesystems)
        seen: set[str] = set()
        unique_files: list[str] = []
        for f in xml_files:
            real = os.path.realpath(f)
            if real not in seen:
                seen.add(real)
                unique_files.append(f)
        xml_files = unique_files

        logger.info("Found %d XML file(s) in %s", len(xml_files), xml_dir_path)
        if not xml_files:
            logger.warning("No XML files found in directory — nothing to convert")
            return PipelineResult([], RepositoryMetrics())

        all_results: List[MappingResult] = []
        combined_metrics = RepositoryMetrics()

        for idx, fpath in enumerate(xml_files, 1):
            fname = os.path.basename(fpath)
            logger.info("[%d/%d] Processing: %s", idx, len(xml_files), fname)
            if show_progress:
                print(f"\n{'='*60}")
                print(f"[{idx}/{len(xml_files)}] {fname}")
                print(f"{'='*60}")
            try:
                with Timer(f"parse:{fname}"):
                    repo = parser.parse_file(fpath)
                with Timer(f"convert:{fname}"):
                    results, repo_metrics = convert_repository(
                        repo, config, llm_client, io, progress,
                    )
                all_results.extend(results)
                for mm in repo_metrics.mapping_metrics:
                    combined_metrics.add(mm)
            except Exception as exc:
                logger.error("Failed to process '%s': %s", fname, exc)
                if show_progress:
                    print(f"  ERROR: {exc}")

        combined_metrics.total_seconds = sum(
            m.total_seconds for m in combined_metrics.mapping_metrics
        )

        pipeline_result = PipelineResult(all_results, combined_metrics)
        if show_progress:
            print(pipeline_result.report)
        return pipeline_result

    # -- Batch stage mode (Snowflake Notebook) -------------------------------
    if not io.is_local:
        # Batch mode: read all XML files from stage
        logger.info("Listing XML files on stage %s", config.input_stage)
        xml_files = io.list_xml_files()
        logger.info("Found %d XML file(s) on stage", len(xml_files))

        if not xml_files:
            logger.warning("No XML files found on stage — nothing to convert")
            return PipelineResult([], RepositoryMetrics())

        # Parse and convert each file, aggregating results
        all_results: List[MappingResult] = []
        combined_metrics = RepositoryMetrics()

        for fname in xml_files:
            logger.info("Processing stage file: %s", fname)
            try:
                xml_bytes = io.read_xml_from_stage(fname)
                with Timer(f"parse:{fname}"):
                    repo = parser.parse(xml_bytes)
                with Timer(f"convert:{fname}"):
                    results, repo_metrics = convert_repository(
                        repo, config, llm_client, io, progress,
                    )
                all_results.extend(results)
                # Merge metrics from each file's repository
                for mm in repo_metrics.mapping_metrics:
                    combined_metrics.add(mm)
            except Exception as exc:
                logger.error("Failed to process '%s': %s", fname, exc)

        combined_metrics.total_seconds = sum(
            m.total_seconds for m in combined_metrics.mapping_metrics
        )

        pipeline_result = PipelineResult(all_results, combined_metrics)
        if show_progress:
            print(pipeline_result.report)
        return pipeline_result

    else:
        raise ValueError(
            "No xml_file_path provided and no Snowpark session available. "
            "Provide a local file path or run inside a Snowflake Notebook."
        )


# ---------------------------------------------------------------------------
# DDL for output tables (run once to set up)
# ---------------------------------------------------------------------------

SETUP_DDL = """\
-- Run this once to create the output tables

CREATE TABLE IF NOT EXISTS TPC_DI_RAW_DATA.DBT_INGEST.INFORMATICA_DBT_CONVERTED_FILES (
    MAPPING_NAME    VARCHAR(500),
    FILE_PATH       VARCHAR(1000),
    FILE_CONTENT    VARCHAR(16777216),
    FILE_TYPE       VARCHAR(50),
    LAYER           VARCHAR(100),
    CHUNK_INDEX     INTEGER,
    TOTAL_CHUNKS    INTEGER,
    CREATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS TPC_DI_RAW_DATA.DBT_INGEST.INFORMATICA_DBT_CONVERSION_SUMMARY (
    MAPPING_NAME            VARCHAR(500),
    WORKFLOW_NAME           VARCHAR(500),
    COMPLEXITY_SCORE        INTEGER,
    STRATEGY                VARCHAR(50),
    NUM_FILES_GENERATED     INTEGER,
    SQL_ERRORS              INTEGER,
    SQL_WARNINGS            INTEGER,
    YAML_ERRORS             INTEGER,
    YAML_WARNINGS           INTEGER,
    PROJECT_ERRORS          INTEGER,
    TOTAL_CHUNKS            INTEGER,
    ELAPSED_SECONDS         FLOAT,
    STATUS                  VARCHAR(50),
    ERROR_MESSAGE           VARCHAR(10000),
    LLM_CALLS               INTEGER,
    LLM_TOTAL_SECONDS       FLOAT,
    HEAL_ATTEMPTS           INTEGER,
    QUALITY_SCORE           INTEGER,
    TRANSFORMATION_COUNT    INTEGER,
    ESTIMATED_INPUT_TOKENS  INTEGER,
    ERROR_CATEGORY          VARCHAR(50),
    CREATED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
"""


# Allow direct execution
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Informatica-to-dbt converter")
    group = ap.add_mutually_exclusive_group()
    group.add_argument("xml_file", nargs="?", help="Path to single Informatica XML file")
    group.add_argument("--dir", dest="xml_dir", help="Directory containing XML files")
    ap.add_argument("--log-level", default="INFO", help="Logging level")
    ap.add_argument("--no-progress", action="store_true", help="Suppress progress output")
    args = ap.parse_args()

    pipeline_result = run_pipeline(
        xml_file_path=args.xml_file,
        xml_dir_path=args.xml_dir,
        config_overrides={"log_level": args.log_level},
        show_progress=not args.no_progress,
    )
    print(pipeline_result)

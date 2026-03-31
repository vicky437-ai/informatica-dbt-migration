"""Snowflake I/O: stage read/write, table persistence, summary reporting.

Handles reading XML from Snowflake stages, writing generated dbt files
to output stages, and persisting conversion results to summary tables.

When running outside Snowflake (local dev), operations are stubbed with
local filesystem equivalents.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from informatica_to_dbt.config import Config
from informatica_to_dbt.exceptions import PersistenceError
from informatica_to_dbt.generator.response_parser import GeneratedFile

logger = logging.getLogger("informatica_dbt")


def _get_session():
    """Lazily obtain the active Snowpark session (only inside Snowflake)."""
    try:
        from snowflake.snowpark.context import get_active_session  # type: ignore[import-untyped]
        return get_active_session()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Data classes for persistence records
# ---------------------------------------------------------------------------

@dataclass
class ConversionRecord:
    """A single file produced by the conversion pipeline."""

    mapping_name: str
    file_path: str
    file_content: str
    file_type: str  # "sql" | "yaml" | "macro"
    layer: str  # "staging" | "intermediate" | "marts" | "snapshots" | "macros"
    chunk_index: int = 0
    total_chunks: int = 1
    created_at: str = field(
        default_factory=lambda: datetime.datetime.utcnow().isoformat()
    )


@dataclass
class ConversionSummary:
    """Summary record for a completed mapping conversion."""

    mapping_name: str
    workflow_name: str
    complexity_score: int
    strategy: str
    num_files_generated: int
    sql_errors: int
    sql_warnings: int
    yaml_errors: int
    yaml_warnings: int
    project_errors: int
    total_chunks: int
    elapsed_seconds: float
    status: str  # "success" | "partial" | "failed"
    error_message: Optional[str] = None
    # --- Phase 7 enrichment fields ---
    llm_calls: int = 0
    llm_total_seconds: float = 0.0
    heal_attempts: int = 0
    quality_score: float = 0.0
    transformation_count: int = 0
    estimated_input_tokens: int = 0
    error_category: Optional[str] = None
    created_at: str = field(
        default_factory=lambda: datetime.datetime.utcnow().isoformat()
    )


# ---------------------------------------------------------------------------
# SnowflakeIO — the persistence layer
# ---------------------------------------------------------------------------

class SnowflakeIO:
    """Read from input stage, write to output stage and tables.

    Falls back to local filesystem operations when no Snowpark session
    is available (local development).
    """

    def __init__(self, config: Config):
        self._config = config
        self._session = _get_session()
        # In local_mode, always use local filesystem even if a session exists
        self._is_local = config.local_mode or self._session is None

        if self._is_local:
            logger.warning(
                "SnowflakeIO running in LOCAL mode. "
                "Files will be written to ./output/"
            )

    @property
    def is_local(self) -> bool:
        return self._is_local

    # -----------------------------------------------------------------------
    # Read XML from stage
    # -----------------------------------------------------------------------

    def list_xml_files(self) -> List[str]:
        """List XML files on the input stage."""
        if self._is_local:
            raise PersistenceError(
                "Cannot list stage files in local mode — provide file paths directly."
            )
        try:
            stage = self._config.input_stage
            rows = self._session.sql(f"LIST {stage}").collect()
            return [
                row["name"]
                for row in rows
                if row["name"].upper().endswith(".XML")
            ]
        except Exception as exc:
            raise PersistenceError(f"Failed to list files on {stage}: {exc}") from exc

    def read_xml_from_stage(self, file_name: str) -> bytes:
        """Read a single XML file from the input stage as bytes."""
        if self._is_local:
            raise PersistenceError("Cannot read stage in local mode.")
        try:
            import io
            stage_path = f"{self._config.input_stage}/{file_name}"
            # Use Snowpark file operations
            stream = self._session.file.get_stream(stage_path)
            return stream.read()
        except Exception as exc:
            raise PersistenceError(
                f"Failed to read '{file_name}' from stage: {exc}"
            ) from exc

    # -----------------------------------------------------------------------
    # Write generated files to output stage
    # -----------------------------------------------------------------------

    def write_files_to_stage(
        self, mapping_name: str, files: List[GeneratedFile]
    ) -> List[str]:
        """Write generated files to the output stage.

        Returns the list of stage paths written.
        """
        if self._is_local:
            return self._write_files_local(mapping_name, files)

        written: List[str] = []
        for gf in files:
            stage_path = f"{self._config.output_stage}/{mapping_name}/{gf.path}"
            try:
                self._session.sql(
                    f"PUT 'file:///dev/stdin' '{stage_path}' AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                ).collect()
                # Alternative: use session.file.put_stream
                written.append(stage_path)
            except Exception as exc:
                logger.error("Failed to write '%s': %s", stage_path, exc)
        return written

    def _write_files_local(
        self, mapping_name: str, files: List[GeneratedFile]
    ) -> List[str]:
        """Write files to local ./output/ directory."""
        base = Path("output") / mapping_name
        written: List[str] = []
        for gf in files:
            out_path = base / gf.path
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(gf.content, encoding="utf-8")
            written.append(str(out_path))
            logger.debug("LOCAL: wrote %s", out_path)
        logger.info("LOCAL: wrote %d file(s) to %s", len(files), base)
        return written

    # -----------------------------------------------------------------------
    # Persist conversion records to table
    # -----------------------------------------------------------------------

    def save_records(
        self, mapping_name: str, files: List[GeneratedFile]
    ) -> int:
        """Save generated files as rows in the output table.

        Returns the number of rows inserted.
        """
        if self._is_local:
            logger.info("LOCAL: skipping table persistence for '%s'", mapping_name)
            return 0

        records = [
            ConversionRecord(
                mapping_name=mapping_name,
                file_path=gf.path,
                file_content=gf.content,
                file_type="sql" if gf.is_sql else ("yaml" if gf.is_yaml else "other"),
                layer=gf.layer,
            )
            for gf in files
        ]

        try:
            rows = [
                (
                    r.mapping_name,
                    r.file_path,
                    r.file_content,
                    r.file_type,
                    r.layer,
                    r.chunk_index,
                    r.total_chunks,
                    r.created_at,
                )
                for r in records
            ]
            df = self._session.create_dataframe(
                rows,
                schema=[
                    "MAPPING_NAME",
                    "FILE_PATH",
                    "FILE_CONTENT",
                    "FILE_TYPE",
                    "LAYER",
                    "CHUNK_INDEX",
                    "TOTAL_CHUNKS",
                    "CREATED_AT",
                ],
            )
            df.write.mode("append").save_as_table(self._config.output_table)
            logger.info(
                "Saved %d record(s) for '%s' to %s",
                len(records), mapping_name, self._config.output_table,
            )
            return len(records)
        except Exception as exc:
            raise PersistenceError(
                f"Failed to save records for '{mapping_name}': {exc}"
            ) from exc

    # -----------------------------------------------------------------------
    # Save conversion summary
    # -----------------------------------------------------------------------

    def save_summary(self, summary: ConversionSummary) -> None:
        """Insert a summary row into the summary table."""
        if self._is_local:
            logger.info(
                "LOCAL: Conversion summary for '%s': score=%d, strategy=%s, "
                "files=%d, status=%s",
                summary.mapping_name,
                summary.complexity_score,
                summary.strategy,
                summary.num_files_generated,
                summary.status,
            )
            return

        try:
            row = (
                summary.mapping_name,
                summary.workflow_name,
                summary.complexity_score,
                summary.strategy,
                summary.num_files_generated,
                summary.sql_errors,
                summary.sql_warnings,
                summary.yaml_errors,
                summary.yaml_warnings,
                summary.project_errors,
                summary.total_chunks,
                summary.elapsed_seconds,
                summary.status,
                summary.error_message,
                summary.llm_calls,
                summary.llm_total_seconds,
                summary.heal_attempts,
                summary.quality_score,
                summary.transformation_count,
                summary.estimated_input_tokens,
                summary.error_category,
                summary.created_at,
            )
            df = self._session.create_dataframe(
                [row],
                schema=[
                    "MAPPING_NAME",
                    "WORKFLOW_NAME",
                    "COMPLEXITY_SCORE",
                    "STRATEGY",
                    "NUM_FILES_GENERATED",
                    "SQL_ERRORS",
                    "SQL_WARNINGS",
                    "YAML_ERRORS",
                    "YAML_WARNINGS",
                    "PROJECT_ERRORS",
                    "TOTAL_CHUNKS",
                    "ELAPSED_SECONDS",
                    "STATUS",
                    "ERROR_MESSAGE",
                    "LLM_CALLS",
                    "LLM_TOTAL_SECONDS",
                    "HEAL_ATTEMPTS",
                    "QUALITY_SCORE",
                    "TRANSFORMATION_COUNT",
                    "ESTIMATED_INPUT_TOKENS",
                    "ERROR_CATEGORY",
                    "CREATED_AT",
                ],
            )
            df.write.mode("append").save_as_table(self._config.summary_table)
            logger.info(
                "Summary saved for '%s' → %s",
                summary.mapping_name, self._config.summary_table,
            )
        except Exception as exc:
            raise PersistenceError(
                f"Failed to save summary for '{summary.mapping_name}': {exc}"
            ) from exc

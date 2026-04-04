"""Main pipeline orchestrator — ties all modules together.

Provides :func:`convert_mapping` for single-mapping conversion and
:func:`convert_repository` for batch processing of all mappings in a
parsed Informatica XML export.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from informatica_to_dbt.analyzer.complexity import ComplexityResult, ModelStrategy, analyze_complexity
from informatica_to_dbt.analyzer.multi_workflow import (
    EnrichedMapping,
    enrich_mapping,
    enrich_repository,
)
from informatica_to_dbt.chunker.context_preserving import MappingChunk, chunk_mapping
from informatica_to_dbt.config import Config
from informatica_to_dbt.exceptions import ConversionError, ErrorCategory, classify_error
from informatica_to_dbt.generator.dbt_project_generator import generate_project_files
from informatica_to_dbt.generator.llm_client import LLMClient
from informatica_to_dbt.generator.post_processor import post_process
from informatica_to_dbt.generator.prompt_builder import build_prompt, build_correction_prompt
from informatica_to_dbt.generator.quality_scorer import QualityReport, score_quality
from informatica_to_dbt.generator.response_parser import (
    GeneratedFile,
    merge_chunk_files,
    parse_response,
)
from informatica_to_dbt.metrics import MappingMetrics, RepositoryMetrics, Stopwatch
from informatica_to_dbt.persistence.snowflake_io import (
    ConversionSummary,
    SnowflakeIO,
)
from informatica_to_dbt.progress import (
    ProgressCallback,
    ProgressPhase,
    make_event,
)
from informatica_to_dbt.utils import Timer, estimate_token_count
from informatica_to_dbt.validator.project_validator import (
    ProjectValidationResult,
    validate_project,
)
from informatica_to_dbt.validator.sql_validator import SQLValidationResult, validate_sql
from informatica_to_dbt.validator.yaml_validator import (
    YAMLValidationResult,
    validate_yaml,
)
from informatica_to_dbt.xml_parser.models import Repository

logger = logging.getLogger("informatica_dbt")

# Lazy import to avoid circular dependency — merger imports GeneratedFile
_ProjectMerger = None
_MergeResult = None

# Re-export for convenience — callers can import from orchestrator
from informatica_to_dbt.cache.conversion_cache import ConversionCache  # noqa: E402


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class MappingResult:
    """Result of converting a single mapping."""

    mapping_name: str
    workflow_name: str
    complexity: Optional[ComplexityResult] = None
    files: List[GeneratedFile] = field(default_factory=list)
    sql_validation: Optional[SQLValidationResult] = None
    yaml_validation: Optional[YAMLValidationResult] = None
    project_validation: Optional[ProjectValidationResult] = None
    post_processing_warnings: List[str] = field(default_factory=list)
    quality_report: Optional[QualityReport] = None
    chunks_processed: int = 0
    heal_attempts: int = 0
    elapsed_seconds: float = 0.0
    cache_hit: bool = False
    status: str = "pending"  # "success" | "partial" | "failed"
    error_message: Optional[str] = None
    metrics: Optional[MappingMetrics] = None


# ---------------------------------------------------------------------------
# Single mapping conversion
# ---------------------------------------------------------------------------

def convert_mapping(
    enriched: EnrichedMapping,
    config: Config,
    llm_client: LLMClient,
    workflow_name: str = "",
    extra_context: Optional[str] = None,
    progress: Optional[ProgressCallback] = None,
    current_mapping: int = 0,
    total_mappings: int = 0,
    cache: Optional[ConversionCache] = None,
    xml_content: str = "",
) -> MappingResult:
    """Convert a single Informatica mapping to dbt files.

    Steps:
        1. Analyze complexity → determine strategy.
        1b. Check cache — if hit, skip LLM and return cached files.
        2. Chunk the enriched mapping for LLM context limits.
        3. For each chunk, build prompt → call LLM → parse response.
        4. Merge chunk outputs → post-process → validate.

    Returns a :class:`MappingResult` with all generated files and validation.
    """
    mapping = enriched.mapping
    result = MappingResult(
        mapping_name=mapping.name,
        workflow_name=workflow_name or enriched.folder_name,
    )
    metrics = MappingMetrics(
        mapping_name=mapping.name,
        workflow_name=workflow_name or enriched.folder_name,
    )
    result.metrics = metrics
    start = time.time()

    def _emit(phase: ProgressPhase, detail: str = "") -> None:
        if progress is not None:
            progress.on_progress(make_event(
                phase, mapping.name, detail,
                current_mapping, total_mappings,
            ))

    _emit(ProgressPhase.STARTED)

    try:
        # --- Step 1: Complexity analysis ---
        logger.info("Analyzing complexity for mapping '%s'", mapping.name)
        _emit(ProgressPhase.ANALYZING)
        with Stopwatch(metrics, "analysis_seconds"):
            complexity = analyze_complexity(mapping)
        result.complexity = complexity
        metrics.complexity_score = complexity.score
        metrics.strategy = complexity.strategy.name
        metrics.transformation_count = len(mapping.transformations)
        metrics.source_count = len(mapping.sources) if hasattr(mapping, "sources") else 0
        metrics.target_count = len(mapping.targets) if hasattr(mapping, "targets") else 0
        logger.info(
            "Mapping '%s': score=%d, strategy=%s",
            mapping.name, complexity.score, complexity.strategy.name,
        )

        # --- Step 1b: Cache lookup ---
        cache_key = ""
        if cache is not None and cache.enabled and xml_content:
            cache_key = ConversionCache.compute_key(
                xml_content=xml_content,
                converter_version=config.converter_version,
                llm_model=config.llm_model,
                mapping_name=mapping.name,
                prompt_hash=config.prompt_version,
            )
            cached_files = cache.get(cache_key)
            if cached_files is not None:
                logger.info(
                    "Cache HIT for mapping '%s' — skipping LLM generation",
                    mapping.name,
                )
                result.files = cached_files
                result.cache_hit = True
                result.status = "success"
                result.elapsed_seconds = time.time() - start
                metrics.total_seconds = result.elapsed_seconds
                metrics.status = "success"
                metrics.files_generated = len(cached_files)
                metrics.sql_files = sum(1 for f in cached_files if f.is_sql)
                metrics.yaml_files = sum(1 for f in cached_files if f.is_yaml)
                _emit(ProgressPhase.COMPLETED, f"cache_hit=True, files={len(cached_files)}")
                return result

        # --- Step 2: Chunk ---
        logger.info("Chunking mapping '%s' (limit=%d tokens)", mapping.name, config.chunk_token_limit)
        _emit(ProgressPhase.CHUNKING)
        with Stopwatch(metrics, "chunking_seconds"):
            chunks = chunk_mapping(enriched, complexity, config)
        result.chunks_processed = len(chunks)
        metrics.chunks_total = len(chunks)
        # Estimate input tokens from chunk content
        for chunk in chunks:
            if hasattr(chunk, "xml_content"):
                metrics.estimated_input_tokens += estimate_token_count(chunk.xml_content)
        logger.info(
            "Mapping '%s' split into %d chunk(s)", mapping.name, len(chunks)
        )

        # --- Step 3: Generate dbt code per chunk ---
        all_chunk_files: List[List[GeneratedFile]] = []
        _emit(ProgressPhase.GENERATING, f"{len(chunks)} chunk(s)")
        with Stopwatch(metrics, "generation_seconds"):
            for chunk in chunks:
                prompt = build_prompt(chunk, complexity, extra_context)
                logger.info(
                    "Calling LLM for '%s' chunk %d/%d",
                    mapping.name, chunk.chunk_index + 1, chunk.total_chunks,
                )
                llm_start = time.perf_counter()
                raw_response = llm_client.generate(prompt)
                metrics.llm_total_seconds += time.perf_counter() - llm_start
                metrics.llm_calls += 1

                chunk_files = parse_response(raw_response)
                all_chunk_files.append(chunk_files)
                logger.info(
                    "Chunk %d/%d produced %d file(s)",
                    chunk.chunk_index + 1, chunk.total_chunks, len(chunk_files),
                )

        # --- Step 4: Merge chunks ---
        _emit(ProgressPhase.MERGING)
        if len(all_chunk_files) > 1:
            merged_files = merge_chunk_files(all_chunk_files)
        else:
            merged_files = all_chunk_files[0] if all_chunk_files else []

        # --- Step 5: Post-process ---
        _emit(ProgressPhase.POSTPROCESSING)
        with Stopwatch(metrics, "postprocess_seconds"):
            processed_files, warnings = post_process(merged_files)
        result.files = processed_files
        result.post_processing_warnings = warnings

        # --- Step 6: Validate + self-healing retry loop ---
        _emit(ProgressPhase.VALIDATING)
        with Stopwatch(metrics, "validation_seconds"):
            result.sql_validation = validate_sql(processed_files)
            result.yaml_validation = validate_yaml(processed_files)
            result.project_validation = validate_project(processed_files)

        total_errors = (
            result.sql_validation.error_count
            + result.yaml_validation.error_count
            + result.project_validation.error_count
        )

        # Self-healing: if validation found errors, ask LLM to fix them
        heal_attempt = 0
        while total_errors > 0 and heal_attempt < config.self_heal_max_attempts:
            heal_attempt += 1
            result.heal_attempts = heal_attempt
            metrics.heal_attempts = heal_attempt
            _emit(ProgressPhase.HEALING, f"attempt {heal_attempt}/{config.self_heal_max_attempts}")
            logger.info(
                "Self-heal attempt %d/%d for '%s' (%d error(s))",
                heal_attempt, config.self_heal_max_attempts,
                mapping.name, total_errors,
            )

            # Collect all error messages
            validation_errors = _collect_validation_errors(result)

            # Build correction prompt and call LLM
            correction_prompt = build_correction_prompt(
                processed_files, validation_errors,
            )
            logger.info(
                "Calling LLM for self-heal correction on '%s'", mapping.name,
            )
            llm_start = time.perf_counter()
            raw_correction = llm_client.generate(correction_prompt)
            metrics.llm_total_seconds += time.perf_counter() - llm_start
            metrics.llm_calls += 1
            metrics.heal_calls += 1

            # Parse, post-process, and re-validate corrected output
            try:
                corrected_files = parse_response(raw_correction)
                processed_files, warnings = post_process(corrected_files)
                result.files = processed_files
                result.post_processing_warnings = warnings

                result.sql_validation = validate_sql(processed_files)
                result.yaml_validation = validate_yaml(processed_files)
                result.project_validation = validate_project(processed_files)

                total_errors = (
                    result.sql_validation.error_count
                    + result.yaml_validation.error_count
                    + result.project_validation.error_count
                )
                logger.info(
                    "Self-heal attempt %d: %d error(s) remaining for '%s'",
                    heal_attempt, total_errors, mapping.name,
                )
            except Exception as exc:
                logger.warning(
                    "Self-heal attempt %d failed to parse correction for '%s': %s",
                    heal_attempt, mapping.name, exc,
                )
                # Keep previous files — don't overwrite with a broken correction
                break

        # --- Model escalation: retry with fallback model if self-heal exhausted ---
        if (
            total_errors > 0
            and config.llm_fallback_model
            and config.llm_fallback_model != config.llm_model
        ):
            logger.info(
                "Escalating mapping '%s' from %s to %s (%d error(s) remain)",
                mapping.name, config.llm_model, config.llm_fallback_model,
                total_errors,
            )
            _emit(ProgressPhase.GENERATING, f"escalation to {config.llm_fallback_model}")
            try:
                # Re-generate ALL chunks with the fallback model (H6 fix).
                # Previously only chunks[0] was sent, losing data for
                # multi-chunk mappings.
                esc_all_chunk_files: List[List[GeneratedFile]] = []
                for chunk in chunks:
                    escalation_prompt = build_prompt(chunk, complexity, extra_context)
                    llm_start = time.perf_counter()
                    raw_escalation = llm_client.generate_with_model(
                        escalation_prompt, config.llm_fallback_model,
                    )
                    metrics.llm_total_seconds += time.perf_counter() - llm_start
                    metrics.llm_calls += 1
                    esc_all_chunk_files.append(parse_response(raw_escalation))

                # Merge escalated chunks (same pattern as Step 4)
                if len(esc_all_chunk_files) > 1:
                    esc_merged = merge_chunk_files(esc_all_chunk_files)
                else:
                    esc_merged = esc_all_chunk_files[0] if esc_all_chunk_files else []

                processed_files, warnings = post_process(esc_merged)
                result.files = processed_files
                result.post_processing_warnings = warnings

                result.sql_validation = validate_sql(processed_files)
                result.yaml_validation = validate_yaml(processed_files)
                result.project_validation = validate_project(processed_files)

                total_errors = (
                    result.sql_validation.error_count
                    + result.yaml_validation.error_count
                    + result.project_validation.error_count
                )
                logger.info(
                    "Model escalation for '%s': %d error(s) remaining",
                    mapping.name, total_errors,
                )
            except Exception as exc:
                logger.warning(
                    "Model escalation failed for '%s': %s", mapping.name, exc,
                )

        # Determine status
        if total_errors == 0:
            result.status = "success"
        else:
            result.status = "partial"
            logger.warning(
                "Mapping '%s' completed with %d validation error(s) "
                "after %d heal attempt(s)",
                mapping.name, total_errors, result.heal_attempts,
            )

        # --- Step 7: Quality scoring ---
        _emit(ProgressPhase.SCORING)
        expected_layers = _expected_layers_for_strategy(complexity.strategy)
        result.quality_report = score_quality(processed_files, expected_layers)
        logger.info(
            "Mapping '%s': %s",
            mapping.name, result.quality_report.summary,
        )

        # --- Step 8: Generate dbt project files ---
        _emit(ProgressPhase.PROJECT_GEN)
        project_files = generate_project_files(
            project_name=mapping.name,
            files=processed_files,
        )
        result.files = processed_files + project_files

        # --- Step 9: Cache store (on success) ---
        if cache is not None and cache.enabled and cache_key and result.status == "success":
            quality = result.quality_report.total_score if result.quality_report else None
            cache.put(
                cache_key=cache_key,
                files=result.files,
                xml_filename=xml_content[:80] if xml_content else "",
                mapping_name=mapping.name,
                converter_version=config.converter_version,
                llm_model=config.llm_model,
                quality_score=quality,
            )
            logger.info("Cached output for mapping '%s'", mapping.name)

        # --- Populate final metrics ---
        metrics.files_generated = len(result.files)
        metrics.sql_files = sum(1 for f in result.files if f.is_sql)
        metrics.yaml_files = sum(1 for f in result.files if f.is_yaml)
        metrics.layers_covered = len({f.layer for f in result.files if f.is_sql})
        metrics.sql_errors = result.sql_validation.error_count if result.sql_validation else 0
        metrics.sql_warnings = result.sql_validation.warning_count if result.sql_validation else 0
        metrics.yaml_errors = result.yaml_validation.error_count if result.yaml_validation else 0
        metrics.yaml_warnings = result.yaml_validation.warning_count if result.yaml_validation else 0
        metrics.project_errors = result.project_validation.error_count if result.project_validation else 0
        metrics.project_warnings = result.project_validation.warning_count if result.project_validation else 0
        metrics.quality_score = result.quality_report.total_score if result.quality_report else 0

    except ConversionError as exc:
        category = classify_error(exc)
        result.status = "failed"
        result.error_message = f"[{category.value}] {exc}"
        metrics.error_message = result.error_message
        _emit(ProgressPhase.FAILED, result.error_message)
        logger.error(
            "Mapping '%s' failed (%s, retryable=%s): %s",
            mapping.name, category.value, category.is_retryable, exc,
        )
    except Exception as exc:
        category = classify_error(exc)
        result.status = "failed"
        result.error_message = f"[{category.value}] Unexpected: {exc}"
        metrics.error_message = result.error_message
        _emit(ProgressPhase.FAILED, result.error_message)
        logger.exception("Mapping '%s' failed unexpectedly (%s)", mapping.name, category.value)

    result.elapsed_seconds = time.time() - start
    metrics.total_seconds = result.elapsed_seconds
    metrics.status = result.status
    logger.info(
        "Mapping '%s' completed in %.1fs — status=%s, files=%d, llm_calls=%d",
        mapping.name, result.elapsed_seconds, result.status,
        len(result.files), metrics.llm_calls,
    )
    if result.status != "failed":
        _emit(ProgressPhase.COMPLETED, f"status={result.status}, files={len(result.files)}")
    return result


# ---------------------------------------------------------------------------
# Checkpoint helpers for idempotent resume
# ---------------------------------------------------------------------------

_CHECKPOINT_DIR = ".infa2dbt"
_CHECKPOINT_FILE = "checkpoint.json"


def _checkpoint_path() -> Path:
    return Path(_CHECKPOINT_DIR) / _CHECKPOINT_FILE


def _load_checkpoint() -> dict[str, str]:
    """Load checkpoint mapping_name → status. Returns empty dict if none."""
    cp = _checkpoint_path()
    if cp.exists():
        try:
            data = json.loads(cp.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not read checkpoint file: %s", exc)
    return {}


def _save_checkpoint(checkpoint: dict[str, str]) -> None:
    """Persist checkpoint to disk."""
    cp = _checkpoint_path()
    cp.parent.mkdir(parents=True, exist_ok=True)
    cp.write_text(json.dumps(checkpoint, indent=2), encoding="utf-8")


def _clear_checkpoint() -> None:
    """Remove the checkpoint file."""
    cp = _checkpoint_path()
    if cp.exists():
        cp.unlink()
        logger.info("Cleared checkpoint file")


# ---------------------------------------------------------------------------
# Batch conversion of an entire repository
# ---------------------------------------------------------------------------

def convert_repository(
    repo: Repository,
    config: Config,
    llm_client: Optional[LLMClient] = None,
    io: Optional[SnowflakeIO] = None,
    progress: Optional[ProgressCallback] = None,
    cache: Optional[ConversionCache] = None,
    xml_content: str = "",
    extra_context: Optional[str] = None,
) -> tuple[List[MappingResult], RepositoryMetrics]:
    """Convert all mappings in a parsed repository.

    Enriches mappings (resolves shortcuts), then converts each one.
    Optionally persists results via :class:`SnowflakeIO`.

    Args:
        repo: Parsed Informatica repository.
        config: Pipeline configuration.
        llm_client: Optional; created from config if not provided.
        io: Optional; created from config if not provided.
        progress: Optional progress callback.
        cache: Optional conversion cache for deterministic re-runs.
        xml_content: Raw XML content for cache key computation.

    Returns a tuple of (results list, aggregated RepositoryMetrics).
    """
    if llm_client is None:
        llm_client = LLMClient(config)
    if io is None:
        io = SnowflakeIO(config)

    repo_metrics = RepositoryMetrics()
    repo_start = time.time()

    # Enrich all mappings (resolve cross-folder shortcuts)
    enriched_list = enrich_repository(repo)
    total = len(enriched_list)
    logger.info("Enriched %d mapping(s) from repository", total)

    # Load checkpoint for resume support
    checkpoint = _load_checkpoint()
    skipped = 0

    if progress is not None:
        progress.on_progress(make_event(
            ProgressPhase.REPO_STARTED, total_mappings=total,
        ))

    results: List[MappingResult] = []

    for idx, enriched in enumerate(enriched_list, 1):
        mapping = enriched.mapping

        # Skip mappings already completed in a previous run
        if checkpoint.get(mapping.name) == "success":
            skipped += 1
            logger.info(
                "Checkpoint: skipping '%s' (already succeeded)", mapping.name,
            )
            continue

        with Timer(f"convert:{mapping.name}"):
            mr = convert_mapping(
                enriched=enriched,
                config=config,
                llm_client=llm_client,
                extra_context=extra_context,
                progress=progress,
                current_mapping=idx,
                total_mappings=total,
                cache=cache,
                xml_content=xml_content,
            )
        results.append(mr)

        # Collect metrics
        if mr.metrics:
            repo_metrics.add(mr.metrics)

        # Persist results — graceful degradation on persistence failures
        if mr.files:
            try:
                io.write_files_to_stage(mapping.name, mr.files)
                io.save_records(mapping.name, mr.files)
            except Exception as exc:
                cat = classify_error(exc)
                logger.error(
                    "Persistence failed for '%s' (%s, retryable=%s): %s",
                    mapping.name, cat.value, cat.is_retryable, exc,
                )
                # Don't abort the batch — mark the mapping as partial
                if mr.status == "success":
                    mr.status = "partial"
                    mr.error_message = f"[{cat.value}] Persistence: {exc}"

        # Save summary — graceful degradation
        try:
            summary = _build_summary(mr, enriched.folder_name)
            io.save_summary(summary)
        except Exception as exc:
            cat = classify_error(exc)
            logger.error(
                "Summary save failed for '%s' (%s): %s",
                mapping.name, cat.value, exc,
            )

        # Update checkpoint after each mapping
        checkpoint[mapping.name] = mr.status
        _save_checkpoint(checkpoint)

    if skipped:
        logger.info("Checkpoint: skipped %d already-succeeded mapping(s)", skipped)

    repo_metrics.total_seconds = time.time() - repo_start

    # Clear checkpoint if all mappings succeeded
    all_succeeded = all(
        checkpoint.get(e.mapping.name) == "success" for e in enriched_list
    )
    if all_succeeded:
        _clear_checkpoint()

    # Final report
    logger.info(repo_metrics.summary())
    if progress is not None:
        progress.on_progress(make_event(
            ProgressPhase.REPO_COMPLETED,
            detail=repo_metrics.summary(),
            total_mappings=total,
            current_mapping=total,
        ))
    return results, repo_metrics


# ---------------------------------------------------------------------------
# Retry failed mappings
# ---------------------------------------------------------------------------

def retry_failed_mappings(
    results: List[MappingResult],
    enriched_map: dict[str, EnrichedMapping],
    config: Config,
    llm_client: Optional[LLMClient] = None,
    io: Optional[SnowflakeIO] = None,
    progress: Optional[ProgressCallback] = None,
    max_retries: int = 1,
) -> tuple[List[MappingResult], RepositoryMetrics]:
    """Retry mappings that failed with a retryable error category.

    Scans *results* for failed mappings whose error category is retryable
    (LLM, PERSISTENCE) and re-runs them up to *max_retries* times.

    Args:
        results: The original list of MappingResult from convert_repository.
        enriched_map: Dict mapping mapping_name → EnrichedMapping.
            Build with ``{e.mapping.name: e for e in enrich_repository(repo)}``.
        config: Pipeline config.
        llm_client: Optional; created from config if not provided.
        io: Optional; created from config if not provided.
        progress: Optional progress callback.
        max_retries: Maximum retry rounds (default 1).

    Returns:
        A tuple of (updated results list, metrics for retried mappings only).
        The returned results list is a *new* list where retried mappings
        replace the failed originals.
    """
    if llm_client is None:
        llm_client = LLMClient(config)
    if io is None:
        io = SnowflakeIO(config)

    retry_metrics = RepositoryMetrics()
    retry_start = time.time()

    # Identify retryable failures
    retryable_indices: list[int] = []
    for i, mr in enumerate(results):
        if mr.status != "failed" or not mr.error_message:
            continue
        category = _extract_error_category(mr.error_message)
        if category is not None and category.is_retryable:
            retryable_indices.append(i)

    if not retryable_indices:
        logger.info("No retryable failures found — nothing to retry")
        return results, retry_metrics

    logger.info(
        "Found %d retryable failure(s) — retrying (max_retries=%d)",
        len(retryable_indices), max_retries,
    )

    updated = list(results)  # shallow copy
    total = len(retryable_indices)

    for round_num in range(max_retries):
        still_failed: list[int] = []
        for seq, idx in enumerate(retryable_indices, 1):
            mr = updated[idx]
            enriched = enriched_map.get(mr.mapping_name)
            if enriched is None:
                logger.warning(
                    "Cannot retry '%s' — enriched mapping not found", mr.mapping_name,
                )
                continue

            logger.info(
                "Retry round %d: mapping '%s' (%d/%d)",
                round_num + 1, mr.mapping_name, seq, total,
            )
            new_mr = convert_mapping(
                enriched=enriched,
                config=config,
                llm_client=llm_client,
                progress=progress,
                current_mapping=seq,
                total_mappings=total,
            )

            if new_mr.metrics:
                retry_metrics.add(new_mr.metrics)

            # Persist if improved
            if new_mr.files and io is not None:
                try:
                    io.write_files_to_stage(mr.mapping_name, new_mr.files)
                    io.save_records(mr.mapping_name, new_mr.files)
                except Exception as exc:
                    cat = classify_error(exc)
                    logger.error(
                        "Retry persistence failed for '%s' (%s): %s",
                        mr.mapping_name, cat.value, exc,
                    )

            updated[idx] = new_mr

            if new_mr.status == "failed":
                cat = _extract_error_category(new_mr.error_message or "")
                if cat is not None and cat.is_retryable:
                    still_failed.append(idx)

        retryable_indices = still_failed
        if not retryable_indices:
            break

    retry_metrics.total_seconds = time.time() - retry_start
    logger.info(
        "Retry complete: %d mapping(s) retried in %.1fs",
        total, retry_metrics.total_seconds,
    )
    return updated, retry_metrics


def _extract_error_category(error_message: str) -> Optional[ErrorCategory]:
    """Extract ErrorCategory from a bracketed error message like '[llm] ...'."""
    if not error_message.startswith("["):
        return None
    bracket_end = error_message.find("]")
    if bracket_end < 0:
        return None
    tag = error_message[1:bracket_end]
    try:
        return ErrorCategory(tag)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _expected_layers_for_strategy(strategy: ModelStrategy) -> List[str]:
    """Return the dbt layers expected for a given strategy."""
    if strategy == ModelStrategy.DIRECT:
        return ["staging"]
    if strategy == ModelStrategy.STAGED:
        return ["staging", "intermediate"]
    if strategy == ModelStrategy.LAYERED:
        return ["staging", "intermediate", "marts"]
    if strategy == ModelStrategy.COMPLEX:
        return ["staging", "intermediate", "marts"]
    return ["staging"]


def _collect_validation_errors(result: MappingResult) -> List[str]:
    """Collect all error-severity messages from validation results."""
    errors: List[str] = []
    if result.sql_validation:
        for issue in result.sql_validation.issues:
            if issue.severity == "error":
                errors.append(f"[SQL] {issue.file_path}: {issue.message}")
    if result.yaml_validation:
        for issue in result.yaml_validation.issues:
            if issue.severity == "error":
                errors.append(f"[YAML] {issue.file_path}: {issue.message}")
    if result.project_validation:
        for issue in result.project_validation.issues:
            if issue.severity == "error":
                errors.append(f"[PROJECT] {issue.file_path}: {issue.message}")
    return errors


def _build_summary(mr: MappingResult, folder_name: str) -> ConversionSummary:
    """Build a ConversionSummary from a MappingResult."""
    m = mr.metrics
    error_cat = None
    if mr.error_message:
        cat = _extract_error_category(mr.error_message)
        if cat is not None:
            error_cat = cat.value

    return ConversionSummary(
        mapping_name=mr.mapping_name,
        workflow_name=mr.workflow_name or folder_name,
        complexity_score=mr.complexity.score if mr.complexity else 0,
        strategy=mr.complexity.strategy.name if mr.complexity else "UNKNOWN",
        num_files_generated=len(mr.files),
        sql_errors=mr.sql_validation.error_count if mr.sql_validation else 0,
        sql_warnings=mr.sql_validation.warning_count if mr.sql_validation else 0,
        yaml_errors=mr.yaml_validation.error_count if mr.yaml_validation else 0,
        yaml_warnings=mr.yaml_validation.warning_count if mr.yaml_validation else 0,
        project_errors=mr.project_validation.error_count if mr.project_validation else 0,
        total_chunks=mr.chunks_processed,
        elapsed_seconds=mr.elapsed_seconds,
        status=mr.status,
        error_message=mr.error_message,
        llm_calls=m.llm_calls if m else 0,
        llm_total_seconds=m.llm_total_seconds if m else 0.0,
        heal_attempts=m.heal_attempts if m else 0,
        quality_score=m.quality_score if m else 0.0,
        transformation_count=m.transformation_count if m else 0,
        estimated_input_tokens=m.estimated_input_tokens if m else 0,
        error_category=error_cat,
    )


# ---------------------------------------------------------------------------
# High-level: convert + merge into a unified dbt project
# ---------------------------------------------------------------------------

def convert_and_merge(
    xml_paths: List[str],
    config: Config,
    llm_client: Optional[LLMClient] = None,
    io: Optional[SnowflakeIO] = None,
    progress: Optional[ProgressCallback] = None,
) -> tuple[List[MappingResult], RepositoryMetrics, "MergeResult"]:
    """Convert XML file(s) and merge results into a unified dbt project.

    This is the top-level API that combines XML parsing, per-mapping
    conversion, and project merging into a single call.

    Args:
        xml_paths: List of paths to Informatica XML files.
        config: Pipeline configuration (must have ``project_dir`` set).
        llm_client: Optional; created from config if not provided.
        io: Optional; created from config if not provided.
        progress: Optional progress callback.

    Returns:
        Tuple of (all mapping results, aggregated metrics, merge result).
    """
    from informatica_to_dbt.merger.project_merger import ProjectMerger, MergeResult
    from informatica_to_dbt.xml_parser.parser import InformaticaXMLParser

    if llm_client is None:
        llm_client = LLMClient(config)
    if io is None:
        io = SnowflakeIO(config)

    parser = InformaticaXMLParser()
    all_results: List[MappingResult] = []
    combined_metrics = RepositoryMetrics()

    # Initialize cache
    cache = ConversionCache(
        cache_dir=config.cache_dir,
        enabled=config.cache_enabled,
    )

    for fpath in xml_paths:
        logger.info("Processing XML file: %s", fpath)
        try:
            # Read raw content for cache key computation
            raw_xml = Path(fpath).read_text(encoding="utf-8")
            repo = parser.parse_file(fpath)
            results, repo_metrics = convert_repository(
                repo, config, llm_client, io, progress,
                cache=cache,
                xml_content=raw_xml,
            )
            all_results.extend(results)
            for mm in repo_metrics.mapping_metrics:
                combined_metrics.add(mm)
        except Exception as exc:
            category = classify_error(exc)
            logger.error(
                "Failed to process '%s' (%s): %s", fpath, category.value, exc,
            )

    combined_metrics.total_seconds = sum(
        m.total_seconds for m in combined_metrics.mapping_metrics
    )

    # Merge into unified project
    merger = ProjectMerger(config)
    merge_result = merger.merge(all_results)

    return all_results, combined_metrics, merge_result

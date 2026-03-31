"""Conversion metrics collection and aggregation.

Tracks per-mapping and repository-level statistics: timing, LLM call
counts, token estimates, error/warning rates, quality scores, and
self-healing effectiveness.  Designed for observability — metrics can
be logged, serialised to JSON, or persisted to Snowflake tables.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Per-mapping metrics
# ---------------------------------------------------------------------------

@dataclass
class MappingMetrics:
    """Metrics for a single mapping conversion."""

    mapping_name: str = ""
    workflow_name: str = ""

    # Complexity
    complexity_score: int = 0
    strategy: str = ""
    transformation_count: int = 0
    source_count: int = 0
    target_count: int = 0

    # Chunking
    chunks_total: int = 0
    estimated_input_tokens: int = 0

    # LLM
    llm_calls: int = 0
    llm_total_seconds: float = 0.0

    # Self-healing
    heal_attempts: int = 0
    heal_calls: int = 0  # LLM calls spent on healing specifically

    # Output
    files_generated: int = 0
    sql_files: int = 0
    yaml_files: int = 0
    layers_covered: int = 0

    # Validation
    sql_errors: int = 0
    sql_warnings: int = 0
    yaml_errors: int = 0
    yaml_warnings: int = 0
    project_errors: int = 0
    project_warnings: int = 0

    # Quality
    quality_score: int = 0

    # Timing
    total_seconds: float = 0.0
    analysis_seconds: float = 0.0
    chunking_seconds: float = 0.0
    generation_seconds: float = 0.0
    postprocess_seconds: float = 0.0
    validation_seconds: float = 0.0

    # Status
    status: str = "pending"
    error_message: Optional[str] = None

    @property
    def total_errors(self) -> int:
        return self.sql_errors + self.yaml_errors + self.project_errors

    @property
    def total_warnings(self) -> int:
        return self.sql_warnings + self.yaml_warnings + self.project_warnings

    @classmethod
    def from_dict(cls, d: Dict) -> "MappingMetrics":
        """Reconstruct a MappingMetrics from a dictionary (inverse of to_dict)."""
        return cls(
            mapping_name=d.get("mapping_name", ""),
            workflow_name=d.get("workflow_name", ""),
            complexity_score=d.get("complexity_score", 0),
            strategy=d.get("strategy", ""),
            transformation_count=d.get("transformation_count", 0),
            source_count=d.get("source_count", 0),
            target_count=d.get("target_count", 0),
            chunks_total=d.get("chunks_total", 0),
            estimated_input_tokens=d.get("estimated_input_tokens", 0),
            llm_calls=d.get("llm_calls", 0),
            llm_total_seconds=d.get("llm_total_seconds", 0.0),
            heal_attempts=d.get("heal_attempts", 0),
            heal_calls=d.get("heal_calls", 0),
            files_generated=d.get("files_generated", 0),
            sql_files=d.get("sql_files", 0),
            yaml_files=d.get("yaml_files", 0),
            layers_covered=d.get("layers_covered", 0),
            sql_errors=d.get("sql_errors", 0),
            sql_warnings=d.get("sql_warnings", 0),
            yaml_errors=d.get("yaml_errors", 0),
            yaml_warnings=d.get("yaml_warnings", 0),
            project_errors=d.get("project_errors", 0),
            project_warnings=d.get("project_warnings", 0),
            quality_score=d.get("quality_score", 0),
            total_seconds=d.get("total_seconds", 0.0),
            analysis_seconds=d.get("analysis_seconds", 0.0),
            chunking_seconds=d.get("chunking_seconds", 0.0),
            generation_seconds=d.get("generation_seconds", 0.0),
            postprocess_seconds=d.get("postprocess_seconds", 0.0),
            validation_seconds=d.get("validation_seconds", 0.0),
            status=d.get("status", "pending"),
            error_message=d.get("error_message"),
        )

    def to_dict(self) -> Dict:
        """Serialise to a flat dictionary (JSON-safe)."""
        return {
            "mapping_name": self.mapping_name,
            "workflow_name": self.workflow_name,
            "complexity_score": self.complexity_score,
            "strategy": self.strategy,
            "transformation_count": self.transformation_count,
            "source_count": self.source_count,
            "target_count": self.target_count,
            "chunks_total": self.chunks_total,
            "estimated_input_tokens": self.estimated_input_tokens,
            "llm_calls": self.llm_calls,
            "llm_total_seconds": round(self.llm_total_seconds, 3),
            "heal_attempts": self.heal_attempts,
            "heal_calls": self.heal_calls,
            "files_generated": self.files_generated,
            "sql_files": self.sql_files,
            "yaml_files": self.yaml_files,
            "layers_covered": self.layers_covered,
            "sql_errors": self.sql_errors,
            "sql_warnings": self.sql_warnings,
            "yaml_errors": self.yaml_errors,
            "yaml_warnings": self.yaml_warnings,
            "project_errors": self.project_errors,
            "project_warnings": self.project_warnings,
            "quality_score": self.quality_score,
            "total_seconds": round(self.total_seconds, 3),
            "analysis_seconds": round(self.analysis_seconds, 3),
            "chunking_seconds": round(self.chunking_seconds, 3),
            "generation_seconds": round(self.generation_seconds, 3),
            "postprocess_seconds": round(self.postprocess_seconds, 3),
            "validation_seconds": round(self.validation_seconds, 3),
            "total_errors": self.total_errors,
            "total_warnings": self.total_warnings,
            "status": self.status,
            "error_message": self.error_message,
        }


# ---------------------------------------------------------------------------
# Repository-level aggregate metrics
# ---------------------------------------------------------------------------

@dataclass
class RepositoryMetrics:
    """Aggregate metrics across all mappings in a repository conversion."""

    mapping_metrics: List[MappingMetrics] = field(default_factory=list)
    total_seconds: float = 0.0

    @property
    def total_mappings(self) -> int:
        return len(self.mapping_metrics)

    @property
    def success_count(self) -> int:
        return sum(1 for m in self.mapping_metrics if m.status == "success")

    @property
    def partial_count(self) -> int:
        return sum(1 for m in self.mapping_metrics if m.status == "partial")

    @property
    def failed_count(self) -> int:
        return sum(1 for m in self.mapping_metrics if m.status == "failed")

    @property
    def success_rate(self) -> float:
        if not self.mapping_metrics:
            return 0.0
        return self.success_count / len(self.mapping_metrics)

    @property
    def total_llm_calls(self) -> int:
        return sum(m.llm_calls for m in self.mapping_metrics)

    @property
    def total_llm_seconds(self) -> float:
        return sum(m.llm_total_seconds for m in self.mapping_metrics)

    @property
    def total_files_generated(self) -> int:
        return sum(m.files_generated for m in self.mapping_metrics)

    @property
    def total_errors(self) -> int:
        return sum(m.total_errors for m in self.mapping_metrics)

    @property
    def total_warnings(self) -> int:
        return sum(m.total_warnings for m in self.mapping_metrics)

    @property
    def total_heal_attempts(self) -> int:
        return sum(m.heal_attempts for m in self.mapping_metrics)

    @property
    def avg_quality_score(self) -> float:
        scored = [m for m in self.mapping_metrics if m.quality_score > 0]
        if not scored:
            return 0.0
        return sum(m.quality_score for m in scored) / len(scored)

    @property
    def avg_complexity(self) -> float:
        if not self.mapping_metrics:
            return 0.0
        return sum(m.complexity_score for m in self.mapping_metrics) / len(self.mapping_metrics)

    @property
    def estimated_total_tokens(self) -> int:
        return sum(m.estimated_input_tokens for m in self.mapping_metrics)

    @classmethod
    def from_dict(cls, d: Dict) -> "RepositoryMetrics":
        """Reconstruct a RepositoryMetrics from a dictionary (inverse of to_dict)."""
        mappings = [MappingMetrics.from_dict(m) for m in d.get("mappings", [])]
        return cls(
            mapping_metrics=mappings,
            total_seconds=d.get("total_seconds", 0.0),
        )

    def add(self, metrics: MappingMetrics) -> None:
        """Add a mapping's metrics to the aggregate."""
        self.mapping_metrics.append(metrics)

    def summary(self) -> str:
        """One-line summary for logging."""
        return (
            f"Repository: {self.total_mappings} mapping(s) — "
            f"{self.success_count} success, {self.partial_count} partial, "
            f"{self.failed_count} failed | "
            f"LLM calls={self.total_llm_calls}, "
            f"files={self.total_files_generated}, "
            f"errors={self.total_errors}, "
            f"avg_quality={self.avg_quality_score:.0f}/100, "
            f"elapsed={self.total_seconds:.1f}s"
        )

    def to_dict(self) -> Dict:
        """Serialise aggregate stats to a flat dictionary."""
        return {
            "total_mappings": self.total_mappings,
            "success_count": self.success_count,
            "partial_count": self.partial_count,
            "failed_count": self.failed_count,
            "success_rate": round(self.success_rate, 3),
            "total_llm_calls": self.total_llm_calls,
            "total_llm_seconds": round(self.total_llm_seconds, 3),
            "total_files_generated": self.total_files_generated,
            "total_errors": self.total_errors,
            "total_warnings": self.total_warnings,
            "total_heal_attempts": self.total_heal_attempts,
            "avg_quality_score": round(self.avg_quality_score, 1),
            "avg_complexity": round(self.avg_complexity, 1),
            "estimated_total_tokens": self.estimated_total_tokens,
            "total_seconds": round(self.total_seconds, 3),
            "mappings": [m.to_dict() for m in self.mapping_metrics],
        }


# ---------------------------------------------------------------------------
# Stopwatch helper — lightweight context manager for timing sub-phases
# ---------------------------------------------------------------------------

class Stopwatch:
    """Tiny context manager that records elapsed seconds to a target attribute.

    Usage::

        metrics = MappingMetrics()
        with Stopwatch(metrics, "analysis_seconds"):
            analyze_complexity(mapping)
    """

    def __init__(self, target: object, attr: str):
        self._target = target
        self._attr = attr
        self._start: float = 0.0

    def __enter__(self) -> "Stopwatch":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_exc) -> None:
        elapsed = time.perf_counter() - self._start
        current = getattr(self._target, self._attr, 0.0)
        setattr(self._target, self._attr, current + elapsed)

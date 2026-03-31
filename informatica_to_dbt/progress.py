"""Callback-based progress reporting for the conversion pipeline.

Provides a :class:`ProgressCallback` protocol and concrete implementations
for logging, Snowflake Notebook display, and composite (fan-out) use.
The orchestrator calls the callback at each pipeline phase so callers
can display live progress or forward events to a UI.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Callable, List, Optional, Protocol, runtime_checkable


logger = logging.getLogger("informatica_dbt")


# ---------------------------------------------------------------------------
# Progress event
# ---------------------------------------------------------------------------

class ProgressPhase(str, Enum):
    """Named phases of the conversion pipeline."""

    STARTED = "started"
    ANALYZING = "analyzing"
    CHUNKING = "chunking"
    GENERATING = "generating"
    MERGING = "merging"
    POSTPROCESSING = "postprocessing"
    VALIDATING = "validating"
    HEALING = "healing"
    SCORING = "scoring"
    PROJECT_GEN = "project_gen"
    COMPLETED = "completed"
    FAILED = "failed"

    # Repository-level phases
    REPO_STARTED = "repo_started"
    REPO_COMPLETED = "repo_completed"


@dataclass
class ProgressEvent:
    """A single progress event emitted during conversion."""

    phase: ProgressPhase
    mapping_name: str = ""
    detail: str = ""
    current_step: int = 0
    total_steps: int = 0
    # Repository-level progress
    current_mapping: int = 0
    total_mappings: int = 0

    @property
    def pct(self) -> float:
        """Percentage complete (0.0–1.0) within the current mapping."""
        if self.total_steps > 0:
            return self.current_step / self.total_steps
        return 0.0

    @property
    def repo_pct(self) -> float:
        """Percentage complete (0.0–1.0) across all mappings."""
        if self.total_mappings > 0:
            return self.current_mapping / self.total_mappings
        return 0.0


# ---------------------------------------------------------------------------
# Callback protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class ProgressCallback(Protocol):
    """Interface for receiving progress events."""

    def on_progress(self, event: ProgressEvent) -> None:
        """Handle a progress event."""
        ...


# ---------------------------------------------------------------------------
# Concrete implementations
# ---------------------------------------------------------------------------

class LoggingProgressCallback:
    """Emits progress events to the standard logger."""

    def on_progress(self, event: ProgressEvent) -> None:
        detail = f" — {event.detail}" if event.detail else ""
        if event.total_mappings > 0:
            logger.info(
                "[%d/%d] %s '%s' (step %d/%d)%s",
                event.current_mapping, event.total_mappings,
                event.phase.value, event.mapping_name,
                event.current_step, event.total_steps, detail,
            )
        else:
            logger.info(
                "%s '%s' (step %d/%d)%s",
                event.phase.value, event.mapping_name,
                event.current_step, event.total_steps, detail,
            )


class PrintProgressCallback:
    """Prints progress to stdout — useful in Snowflake Notebooks."""

    def on_progress(self, event: ProgressEvent) -> None:
        if event.phase == ProgressPhase.REPO_STARTED:
            print(f"Starting conversion of {event.total_mappings} mapping(s)...")
            return
        if event.phase == ProgressPhase.REPO_COMPLETED:
            print(f"Conversion complete: {event.detail}")
            return
        if event.phase == ProgressPhase.FAILED:
            print(f"  FAILED: {event.mapping_name} — {event.detail}")
            return

        bar = ""
        if event.total_steps > 0:
            filled = int(20 * event.pct)
            bar = f" [{'=' * filled}{' ' * (20 - filled)}]"

        mapping_prefix = ""
        if event.total_mappings > 0:
            mapping_prefix = f"[{event.current_mapping}/{event.total_mappings}] "

        detail = f" — {event.detail}" if event.detail else ""
        print(
            f"  {mapping_prefix}{event.phase.value}: "
            f"{event.mapping_name}{bar}{detail}"
        )


class CompositeProgressCallback:
    """Fans out progress events to multiple callbacks."""

    def __init__(self, callbacks: List[ProgressCallback]):
        self._callbacks = list(callbacks)

    def add(self, callback: ProgressCallback) -> None:
        self._callbacks.append(callback)

    def on_progress(self, event: ProgressEvent) -> None:
        for cb in self._callbacks:
            try:
                cb.on_progress(event)
            except Exception as exc:
                logger.warning("Progress callback error: %s", exc)


class FunctionProgressCallback:
    """Wraps a plain function as a progress callback."""

    def __init__(self, fn: Callable[[ProgressEvent], None]):
        self._fn = fn

    def on_progress(self, event: ProgressEvent) -> None:
        self._fn(event)


# ---------------------------------------------------------------------------
# Helper to build the mapping-level step count
# ---------------------------------------------------------------------------

# Total steps in convert_mapping (analysis, chunking, generating, merging,
# postprocessing, validating, scoring, project_gen)
MAPPING_TOTAL_STEPS = 8

_PHASE_STEP: dict[ProgressPhase, int] = {
    ProgressPhase.STARTED: 0,
    ProgressPhase.ANALYZING: 1,
    ProgressPhase.CHUNKING: 2,
    ProgressPhase.GENERATING: 3,
    ProgressPhase.MERGING: 4,
    ProgressPhase.POSTPROCESSING: 5,
    ProgressPhase.VALIDATING: 6,
    ProgressPhase.HEALING: 6,  # sub-step of validation
    ProgressPhase.SCORING: 7,
    ProgressPhase.PROJECT_GEN: 8,
    ProgressPhase.COMPLETED: 8,
    ProgressPhase.FAILED: 0,
}


def make_event(
    phase: ProgressPhase,
    mapping_name: str = "",
    detail: str = "",
    current_mapping: int = 0,
    total_mappings: int = 0,
) -> ProgressEvent:
    """Convenience factory that auto-populates step numbers."""
    return ProgressEvent(
        phase=phase,
        mapping_name=mapping_name,
        detail=detail,
        current_step=_PHASE_STEP.get(phase, 0),
        total_steps=MAPPING_TOTAL_STEPS,
        current_mapping=current_mapping,
        total_mappings=total_mappings,
    )

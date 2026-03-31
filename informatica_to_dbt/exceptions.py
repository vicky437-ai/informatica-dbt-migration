"""Custom exception hierarchy and error categorization for the converter.

Every exception carries an :attr:`category` (:class:`ErrorCategory`) so that
callers can decide on retry strategy, logging severity, and user messaging
without parsing exception class names.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# Error categories
# ---------------------------------------------------------------------------

class ErrorCategory(str, Enum):
    """Broad classification of errors for routing and reporting."""

    INPUT = "input"               # Bad input data (XML format, missing files)
    LLM = "llm"                   # LLM backend errors (rate limits, timeouts)
    PARSE = "parse"               # Failed to parse LLM response
    VALIDATION = "validation"     # Generated code failed validation
    PERSISTENCE = "persistence"   # Could not write results to Snowflake
    INTERNAL = "internal"         # Unexpected bugs / invariant violations

    @property
    def is_retryable(self) -> bool:
        """Whether errors of this category may succeed on retry."""
        return self in {ErrorCategory.LLM, ErrorCategory.PERSISTENCE}

    @property
    def is_transient(self) -> bool:
        """Whether this is a transient (infrastructure) error."""
        return self in {ErrorCategory.LLM, ErrorCategory.PERSISTENCE}


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------

class ConversionError(Exception):
    """Base exception for all conversion errors."""

    category: ErrorCategory = ErrorCategory.INTERNAL

    def __init__(self, message: str, *, cause: Optional[Exception] = None):
        super().__init__(message)
        self.cause = cause


class XMLReadError(ConversionError):
    """Error reading or accessing XML content from stage."""
    category = ErrorCategory.INPUT


class XMLParseError(ConversionError):
    """Error parsing XML structure (malformed or unsupported elements)."""
    category = ErrorCategory.INPUT


class ChunkingError(ConversionError):
    """Error during XML chunking (e.g. single chain exceeds token limit)."""
    category = ErrorCategory.INPUT


class LLMError(ConversionError):
    """Error calling the LLM via Cortex complete()."""
    category = ErrorCategory.LLM


class ResponseParseError(ConversionError):
    """Error parsing the LLM response into file artifacts."""
    category = ErrorCategory.PARSE


class ValidationError(ConversionError):
    """Base class for validation failures."""
    category = ErrorCategory.VALIDATION


class SQLValidationError(ValidationError):
    """Invalid SQL syntax or semantics in generated model."""


class YAMLValidationError(ValidationError):
    """Invalid YAML structure in generated schema/source file."""


class RefIntegrityError(ValidationError):
    """A ref() or source() call targets a non-existent model or source."""


class DAGCycleError(ValidationError):
    """Circular dependency detected in generated model DAG."""


class PersistenceError(ConversionError):
    """Error writing results to Snowflake stage or table."""
    category = ErrorCategory.PERSISTENCE


# ---------------------------------------------------------------------------
# Error classification helper
# ---------------------------------------------------------------------------

def classify_error(exc: Exception) -> ErrorCategory:
    """Classify any exception into an :class:`ErrorCategory`.

    For :class:`ConversionError` subclasses, returns the class-level
    ``category`` attribute.  For other exceptions, attempts heuristic
    classification based on common patterns, falling back to
    :attr:`ErrorCategory.INTERNAL`.
    """
    if isinstance(exc, ConversionError):
        return exc.category

    msg = str(exc).lower()

    # Heuristic: rate-limit / timeout patterns from Snowflake Cortex
    if any(kw in msg for kw in ("rate limit", "429", "timeout", "throttl")):
        return ErrorCategory.LLM

    # Heuristic: connection / permission patterns from Snowflake
    if any(kw in msg for kw in ("connection", "permission denied", "access denied",
                                 "insufficient privileges")):
        return ErrorCategory.PERSISTENCE

    # Heuristic: XML/parse patterns
    if any(kw in msg for kw in ("xml", "syntax error", "not well-formed")):
        return ErrorCategory.INPUT

    return ErrorCategory.INTERNAL

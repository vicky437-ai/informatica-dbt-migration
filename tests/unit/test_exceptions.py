"""Unit tests for error categorization and exception hierarchy."""

import pytest
from informatica_to_dbt.exceptions import (
    ChunkingError,
    ConversionError,
    DAGCycleError,
    ErrorCategory,
    LLMError,
    PersistenceError,
    RefIntegrityError,
    ResponseParseError,
    SQLValidationError,
    ValidationError,
    XMLParseError,
    XMLReadError,
    YAMLValidationError,
    classify_error,
)


# ---------------------------------------------------------------------------
# ErrorCategory properties
# ---------------------------------------------------------------------------

class TestErrorCategory:
    def test_retryable_categories(self):
        assert ErrorCategory.LLM.is_retryable is True
        assert ErrorCategory.PERSISTENCE.is_retryable is True

    def test_non_retryable_categories(self):
        assert ErrorCategory.INPUT.is_retryable is False
        assert ErrorCategory.PARSE.is_retryable is False
        assert ErrorCategory.VALIDATION.is_retryable is False
        assert ErrorCategory.INTERNAL.is_retryable is False

    def test_transient_categories(self):
        assert ErrorCategory.LLM.is_transient is True
        assert ErrorCategory.PERSISTENCE.is_transient is True

    def test_non_transient_categories(self):
        assert ErrorCategory.INPUT.is_transient is False
        assert ErrorCategory.PARSE.is_transient is False
        assert ErrorCategory.VALIDATION.is_transient is False
        assert ErrorCategory.INTERNAL.is_transient is False

    def test_values_are_strings(self):
        assert ErrorCategory.LLM.value == "llm"
        assert ErrorCategory.INPUT.value == "input"
        assert ErrorCategory.PARSE.value == "parse"
        assert ErrorCategory.VALIDATION.value == "validation"
        assert ErrorCategory.PERSISTENCE.value == "persistence"
        assert ErrorCategory.INTERNAL.value == "internal"


# ---------------------------------------------------------------------------
# Exception hierarchy — category assignment
# ---------------------------------------------------------------------------

class TestExceptionCategories:
    def test_base_conversion_error(self):
        assert ConversionError.category == ErrorCategory.INTERNAL

    def test_xml_read_error(self):
        assert XMLReadError.category == ErrorCategory.INPUT

    def test_xml_parse_error(self):
        assert XMLParseError.category == ErrorCategory.INPUT

    def test_chunking_error(self):
        assert ChunkingError.category == ErrorCategory.INPUT

    def test_llm_error(self):
        assert LLMError.category == ErrorCategory.LLM

    def test_response_parse_error(self):
        assert ResponseParseError.category == ErrorCategory.PARSE

    def test_validation_error(self):
        assert ValidationError.category == ErrorCategory.VALIDATION

    def test_sql_validation_inherits(self):
        assert SQLValidationError.category == ErrorCategory.VALIDATION

    def test_yaml_validation_inherits(self):
        assert YAMLValidationError.category == ErrorCategory.VALIDATION

    def test_ref_integrity_inherits(self):
        assert RefIntegrityError.category == ErrorCategory.VALIDATION

    def test_dag_cycle_inherits(self):
        assert DAGCycleError.category == ErrorCategory.VALIDATION

    def test_persistence_error(self):
        assert PersistenceError.category == ErrorCategory.PERSISTENCE


# ---------------------------------------------------------------------------
# ConversionError — cause chaining
# ---------------------------------------------------------------------------

class TestConversionErrorCause:
    def test_default_cause_is_none(self):
        err = ConversionError("oops")
        assert err.cause is None

    def test_cause_preserved(self):
        original = ValueError("bad value")
        err = LLMError("LLM failed", cause=original)
        assert err.cause is original

    def test_message_accessible(self):
        err = XMLParseError("bad XML")
        assert str(err) == "bad XML"


# ---------------------------------------------------------------------------
# classify_error — ConversionError subclasses
# ---------------------------------------------------------------------------

class TestClassifyConversionErrors:
    def test_classify_xml_read(self):
        assert classify_error(XMLReadError("gone")) == ErrorCategory.INPUT

    def test_classify_llm(self):
        assert classify_error(LLMError("timeout")) == ErrorCategory.LLM

    def test_classify_parse(self):
        assert classify_error(ResponseParseError("no files")) == ErrorCategory.PARSE

    def test_classify_validation(self):
        assert classify_error(SQLValidationError("bad SQL")) == ErrorCategory.VALIDATION

    def test_classify_persistence(self):
        assert classify_error(PersistenceError("stage write")) == ErrorCategory.PERSISTENCE

    def test_classify_base_conversion(self):
        assert classify_error(ConversionError("unknown")) == ErrorCategory.INTERNAL


# ---------------------------------------------------------------------------
# classify_error — non-ConversionError heuristics
# ---------------------------------------------------------------------------

class TestClassifyHeuristics:
    def test_rate_limit_pattern(self):
        err = RuntimeError("rate limit exceeded for model")
        assert classify_error(err) == ErrorCategory.LLM

    def test_429_pattern(self):
        err = Exception("HTTP 429 Too Many Requests")
        assert classify_error(err) == ErrorCategory.LLM

    def test_timeout_pattern(self):
        err = TimeoutError("Request timeout after 30s")
        assert classify_error(err) == ErrorCategory.LLM

    def test_throttle_pattern(self):
        err = Exception("Request was throttled by Cortex")
        assert classify_error(err) == ErrorCategory.LLM

    def test_connection_pattern(self):
        err = ConnectionError("connection refused to Snowflake")
        assert classify_error(err) == ErrorCategory.PERSISTENCE

    def test_permission_denied_pattern(self):
        err = RuntimeError("permission denied on stage @DBT_OUTPUT")
        assert classify_error(err) == ErrorCategory.PERSISTENCE

    def test_access_denied_pattern(self):
        err = RuntimeError("access denied: insufficient privileges on table")
        assert classify_error(err) == ErrorCategory.PERSISTENCE

    def test_xml_pattern(self):
        err = Exception("XML syntax error at line 42")
        assert classify_error(err) == ErrorCategory.INPUT

    def test_not_well_formed_pattern(self):
        err = Exception("document is not well-formed")
        assert classify_error(err) == ErrorCategory.INPUT

    def test_unknown_defaults_to_internal(self):
        err = ZeroDivisionError("division by zero")
        assert classify_error(err) == ErrorCategory.INTERNAL

    def test_empty_message_defaults_to_internal(self):
        err = Exception("")
        assert classify_error(err) == ErrorCategory.INTERNAL

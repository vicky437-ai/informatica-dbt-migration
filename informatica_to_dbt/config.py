"""Centralized configuration for the Informatica-to-dbt converter."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from typing import Any, Optional


def _env(key: str, default: str) -> str:
    """Read from environment variable with fallback."""
    return os.environ.get(key, default)


@dataclass
class Config:
    """All tuneable settings for the conversion pipeline.

    Values can be overridden via environment variables prefixed with ``INFA_DBT_``.
    """

    # -- Snowflake stage paths ------------------------------------------------
    input_stage: str = field(default_factory=lambda: _env("INFA_DBT_INPUT_STAGE", "@XML_INPUT"))
    output_stage: str = field(default_factory=lambda: _env("INFA_DBT_OUTPUT_STAGE", "@DBT_OUTPUT"))

    # -- LLM settings ---------------------------------------------------------
    llm_model: str = field(default_factory=lambda: _env("INFA_DBT_LLM_MODEL", "claude-4-sonnet"))
    max_context_tokens: int = 80_000       # safe limit for model context
    chunk_token_limit: int = 75_000        # max tokens per chunk sent to LLM
    llm_temperature: float = 0.1           # low temp for deterministic output
    llm_call_timeout_seconds: int = 300    # 5-minute timeout per LLM call
    llm_fallback_model: Optional[str] = field(
        default_factory=lambda: _env("INFA_DBT_LLM_FALLBACK_MODEL", "claude-4-opus"),
    )

    # -- Rate limiting & retries ----------------------------------------------
    rate_limit_calls_per_minute: int = 3
    max_retries: int = 3
    retry_base_delay_seconds: int = 5      # exponential backoff base

    # -- Self-healing ---------------------------------------------------------
    self_heal_max_attempts: int = 2        # max LLM correction rounds per mapping

    # -- Processing -----------------------------------------------------------
    batch_size: int = 5

    # -- Persistence ----------------------------------------------------------
    output_table: str = field(
        default_factory=lambda: _env(
            "INFA_DBT_OUTPUT_TABLE",
            "TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.INFORMATICA_DBT_CONVERTED_FILES",
        )
    )
    summary_table: str = field(
        default_factory=lambda: _env(
            "INFA_DBT_SUMMARY_TABLE",
            "TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.INFORMATICA_DBT_CONVERSION_SUMMARY",
        )
    )
    file_format: str = "xml_format"

    # -- Logging --------------------------------------------------------------
    log_level: str = field(default_factory=lambda: _env("INFA_DBT_LOG_LEVEL", "INFO"))

    # -- Optional: project-specific overrides ---------------------------------
    dbt_project_name: Optional[str] = None

    # -- Local execution mode -------------------------------------------------
    local_mode: bool = False  # skip stage validation; read XMLs from filesystem

    # -- Project merger -------------------------------------------------------
    project_dir: Optional[str] = None          # target dbt project directory
    merge_mode: str = "new"                    # "new" = create fresh, "merge" = into existing

    # -- Git integration ------------------------------------------------------
    git_remote: Optional[str] = None           # e.g. https://github.com/org/repo.git
    git_branch: str = "main"

    # -- Output caching -------------------------------------------------------
    cache_dir: str = field(default_factory=lambda: _env("INFA_DBT_CACHE_DIR", ".infa2dbt/cache"))
    cache_enabled: bool = True
    converter_version: str = "1.0.0"           # bump to invalidate cache

    # -- Snowflake connection -------------------------------------------------
    connection_name: Optional[str] = None      # Snowflake connection name for CLI mode

    # -- Source schema override -----------------------------------------------
    source_schema_override: Optional[str] = None   # e.g. "MOCK_SOURCES" — injected into all _sources.yml
    schema_discovery: Optional[Any] = None          # populated at runtime with SchemaDiscovery

    @property
    def prompt_version(self) -> str:
        """SHA-256 hash of the system prompt for cache invalidation."""
        from informatica_to_dbt.generator.prompt_builder import SYSTEM_PROMPT  # noqa: delay import
        return hashlib.sha256(SYSTEM_PROMPT.encode()).hexdigest()[:16]

    def validate(self) -> list[str]:
        """Return a list of validation error messages (empty = valid)."""
        errors: list[str] = []
        if self.max_context_tokens < 10_000:
            errors.append("max_context_tokens must be >= 10 000")
        if self.chunk_token_limit >= self.max_context_tokens:
            errors.append("chunk_token_limit must be < max_context_tokens")
        if self.rate_limit_calls_per_minute < 1:
            errors.append("rate_limit_calls_per_minute must be >= 1")
        if self.llm_call_timeout_seconds < 30:
            errors.append("llm_call_timeout_seconds must be >= 30")
        if not self.local_mode:
            if not self.input_stage.startswith("@"):
                errors.append(f"input_stage must start with '@', got '{self.input_stage}'")
            if not self.output_stage.startswith("@"):
                errors.append(f"output_stage must start with '@', got '{self.output_stage}'")
        if self.merge_mode not in ("new", "merge"):
            errors.append(f"merge_mode must be 'new' or 'merge', got '{self.merge_mode}'")
        return errors

"""LLM client wrapping Snowflake Cortex ``complete()`` with rate limiting and retry.

In a Snowflake Notebook the call goes through ``snowflake.cortex.complete()``.
When running locally with a Snowflake connection, falls back to SQL-based
Cortex calls via a Snowpark session.  As a last resort (no Snowflake
connectivity at all), a stub mode returns placeholder responses.

The client enforces:
- Thread-safe sequential access via :class:`ConcurrencyGuard`.
- Minimum inter-call interval (rate limiting) with adaptive backoff on 429s.
- Exponential-backoff retry on transient errors.
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import threading
import time
from typing import Optional

from informatica_to_dbt.config import Config
from informatica_to_dbt.exceptions import LLMError, classify_error, ErrorCategory
from informatica_to_dbt.generator.prompt_builder import PromptPair

logger = logging.getLogger("informatica_dbt")


def _get_cortex_complete():
    """Lazily import ``snowflake.cortex.complete`` (only available inside Snowflake)."""
    try:
        from snowflake.cortex import complete  # type: ignore[import-untyped]
        return complete
    except ImportError:
        return None


def _get_local_snowpark_session():
    """Try to create a local Snowpark session for SQL-based Cortex calls.

    Tries in order:
    1. Default Snowpark session (``~/.snowflake/connections.toml`` / env vars).
    2. Credentials from dbt ``profiles.yml`` (``~/.dbt/profiles.yml``).

    Returns None if Snowpark is unavailable or connection fails.
    """
    try:
        from snowflake.snowpark import Session  # type: ignore[import-untyped]
    except ImportError:
        return None

    # Attempt 1: default session
    try:
        session = Session.builder.getOrCreate()
        session.sql("SELECT 1").collect()
        logger.info("Local Snowpark session created via default connection")
        return session
    except Exception:
        pass

    # Attempt 2: read from dbt profiles.yml
    try:
        import os
        import yaml  # type: ignore[import-untyped]

        profiles_path = os.path.expanduser("~/.dbt/profiles.yml")
        if not os.path.exists(profiles_path):
            logger.debug("No dbt profiles.yml found at %s", profiles_path)
            return None

        with open(profiles_path) as f:
            profiles = yaml.safe_load(f)

        # Try known profile names, then fall back to first profile
        candidates = ["informatica_to_dbt_migration"]
        for profile_name, profile in profiles.items():
            if profile_name not in candidates and isinstance(profile, dict):
                candidates.append(profile_name)

        for profile_name in candidates:
            profile = profiles.get(profile_name)
            if not isinstance(profile, dict):
                continue
            target_name = profile.get("target", "dev")
            outputs = profile.get("outputs", {})
            target = outputs.get(target_name, {})
            if target.get("type") != "snowflake":
                continue

            conn_params = {
                "account": target["account"],
                "user": target["user"],
                "password": target.get("password", ""),
                "database": target.get("database", ""),
                "schema": target.get("schema", ""),
                "warehouse": target.get("warehouse", ""),
                "role": target.get("role", ""),
            }
            session = Session.builder.configs(conn_params).create()
            session.sql("SELECT 1").collect()
            logger.info(
                "Local Snowpark session created from dbt profile '%s'",
                profile_name,
            )
            return session

    except Exception as exc:
        logger.debug("Could not create Snowpark session from dbt profiles: %s", exc)

    return None


# ---------------------------------------------------------------------------
# Concurrency guard
# ---------------------------------------------------------------------------

class ConcurrencyGuard:
    """Thread-safe gate that serialises LLM calls and enforces cooldown.

    Only one thread may execute an LLM call at a time.  After each call,
    a minimum cooldown (``min_interval``) is enforced before the next call
    can proceed.  On rate-limit errors the interval is temporarily doubled
    (up to ``max_interval``) and restored on the next success.
    """

    def __init__(self, min_interval: float, max_interval: float = 120.0):
        self._lock = threading.Lock()
        self._min_interval = min_interval
        self._max_interval = max_interval
        self._current_interval = min_interval
        self._last_call: float = 0.0

    @property
    def current_interval(self) -> float:
        return self._current_interval

    def acquire(self) -> None:
        """Acquire the lock and wait for the cooldown period."""
        self._lock.acquire()
        elapsed = time.time() - self._last_call
        if elapsed < self._current_interval:
            wait = self._current_interval - elapsed
            logger.debug("ConcurrencyGuard: waiting %.2fs for cooldown", wait)
            time.sleep(wait)

    def release(self, *, success: bool = True) -> None:
        """Release the lock.  On failure, widen the cooldown interval."""
        self._last_call = time.time()
        if success:
            # Gradually restore to base interval on success
            self._current_interval = max(
                self._min_interval,
                self._current_interval * 0.75,
            )
        else:
            # Back off on failure (capped at max)
            self._current_interval = min(
                self._max_interval,
                self._current_interval * 2.0,
            )
            logger.info(
                "ConcurrencyGuard: backoff interval now %.1fs",
                self._current_interval,
            )
        self._lock.release()


class LLMClient:
    """Rate-limited, retrying wrapper around the LLM backend.

    Three execution modes (tried in order):

    1. **Cortex Python API** — ``snowflake.cortex.complete()`` (inside Snowflake).
    2. **SQL-based Cortex** — ``session.sql("SELECT SNOWFLAKE.CORTEX.COMPLETE(…)")``
       via a local Snowpark session (running locally with Snowflake connectivity).
    3. **Stub mode** — placeholder output (no Snowflake connectivity at all).

    Usage::

        client = LLMClient(config)
        response_text = client.generate(prompt_pair)
    """

    def __init__(self, config: Config):
        self._model = config.llm_model
        self._temperature = config.llm_temperature
        self._max_retries = config.max_retries
        self._base_delay = config.retry_base_delay_seconds
        self._timeout = config.llm_call_timeout_seconds
        self._fallback_model = config.llm_fallback_model
        min_interval = 60.0 / config.rate_limit_calls_per_minute
        self._guard = ConcurrencyGuard(min_interval)

        # Mode 1: Cortex Python API (Snowflake Notebook)
        self._cortex_complete = _get_cortex_complete()
        # Mode 2: SQL-based Cortex via local Snowpark session
        self._snowpark_session = None

        if self._cortex_complete is not None:
            logger.info("Using Cortex Python API (snowflake.cortex.complete)")
        else:
            logger.info("snowflake.cortex.complete not available — trying SQL fallback")
            self._snowpark_session = _get_local_snowpark_session()
            if self._snowpark_session is not None:
                logger.info(
                    "Using SQL-based Cortex via local Snowpark session (model=%s)",
                    self._model,
                )
            else:
                logger.warning(
                    "No Snowflake connectivity — running in LOCAL STUB mode. "
                    "LLM calls will return placeholder text."
                )

    @property
    def is_stub(self) -> bool:
        """True only when neither Cortex Python API nor SQL fallback is available."""
        return self._cortex_complete is None and self._snowpark_session is None

    @property
    def model(self) -> str:
        """Return the currently configured model name."""
        return self._model

    @property
    def fallback_model(self) -> Optional[str]:
        """Return the fallback/escalation model name."""
        return self._fallback_model

    @property
    def mode(self) -> str:
        """Return the active execution mode name."""
        if self._cortex_complete is not None:
            return "cortex_python"
        if self._snowpark_session is not None:
            return "cortex_sql"
        return "stub"

    def generate(self, prompt: PromptPair) -> str:
        """Send the prompt to the LLM and return the raw response text.

        Applies concurrency guard (thread-safe rate limiting) and retries
        with exponential backoff on transient errors.

        Raises :class:`LLMError` after exhausting all retries.
        """
        for attempt in range(self._max_retries):
            self._guard.acquire()
            success = False
            try:
                result = self._call(prompt)
                success = True
                return result
            except LLMError:
                raise
            except Exception as exc:
                category = classify_error(exc)
                if attempt == self._max_retries - 1:
                    raise LLMError(
                        f"LLM call failed after {self._max_retries} attempts: {exc}",
                        cause=exc,
                    ) from exc
                wait = self._base_delay * (2 ** attempt)
                logger.warning(
                    "LLM attempt %d/%d failed (%s): %s — retrying in %ds",
                    attempt + 1, self._max_retries, category.value, exc, wait,
                )
                time.sleep(wait)
            finally:
                self._guard.release(success=success)

        raise LLMError("Unreachable")  # satisfies type checker

    def generate_with_model(self, prompt: PromptPair, model: str) -> str:
        """Generate using a specific model (for escalation).

        Temporarily swaps the model, calls ``generate()``, then restores.
        """
        original = self._model
        self._model = model
        logger.info("Model escalation: switching from %s to %s", original, model)
        try:
            return self.generate(prompt)
        finally:
            self._model = original

    def _call(self, prompt: PromptPair) -> str:
        """Execute the actual LLM call (Cortex Python, Cortex SQL, or stub)."""
        # Mode 1: Cortex Python API
        if self._cortex_complete is not None:
            return self._call_cortex_python(prompt)
        # Mode 2: SQL-based Cortex
        if self._snowpark_session is not None:
            return self._call_cortex_sql(prompt)
        # Mode 3: Stub
        return self._stub_response(prompt)

    def _call_cortex_python(self, prompt: PromptPair) -> str:
        """Call via ``snowflake.cortex.complete()`` Python API with timeout."""
        def _invoke():
            messages = [
                {"role": "system", "content": prompt.system},
                {"role": "user", "content": prompt.user},
            ]
            return self._cortex_complete(
                self._model,
                messages,
                {
                    "temperature": self._temperature,
                    "max_tokens": 16_384,
                },
            )

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(_invoke)
                response = future.result(timeout=self._timeout)
            if isinstance(response, str):
                return response
            if isinstance(response, dict):
                choices = response.get("choices", [])
                if choices:
                    return choices[0].get("message", {}).get("content", "")
            return str(response)
        except concurrent.futures.TimeoutError:
            raise LLMError(
                f"Cortex Python API timed out after {self._timeout}s"
            )
        except LLMError:
            raise
        except Exception as exc:
            raise LLMError(f"Cortex complete() failed: {exc}") from exc

    def _call_cortex_sql(self, prompt: PromptPair) -> str:
        """Call via SQL ``SNOWFLAKE.CORTEX.COMPLETE()`` through a Snowpark session.

        Uses bind parameters to safely pass JSON content that may contain
        newlines, quotes, backslashes, and other special characters.
        """
        try:
            messages = [
                {"role": "system", "content": prompt.system},
                {"role": "user", "content": prompt.user},
            ]
            messages_json = json.dumps(messages)
            options_json = json.dumps({
                "temperature": self._temperature,
                "max_tokens": 16_384,
            })

            sql = (
                "SELECT SNOWFLAKE.CORTEX.COMPLETE("
                "?, PARSE_JSON(?), PARSE_JSON(?)"
                ") AS response"
            )
            cursor = self._snowpark_session.connection.cursor()
            try:
                cursor.execute(sql, (self._model, messages_json, options_json),
                               timeout=self._timeout)
                row = cursor.fetchone()
            finally:
                cursor.close()

            if not row:
                raise LLMError("Cortex SQL returned no rows")

            raw = row[0]
            # The SQL form returns a JSON string with choices array
            if isinstance(raw, str):
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, dict):
                        choices = parsed.get("choices", [])
                        if choices:
                            msg = choices[0].get("messages", choices[0].get("message", ""))
                            if isinstance(msg, str):
                                return msg
                            if isinstance(msg, dict):
                                return msg.get("content", str(msg))
                    if isinstance(parsed, str):
                        return parsed
                except json.JSONDecodeError:
                    pass
                return raw
            return str(raw)
        except LLMError:
            raise
        except Exception as exc:
            raise LLMError(f"Cortex SQL failed: {exc}") from exc

    def _stub_response(self, prompt: PromptPair) -> str:
        """Return a placeholder response for local development."""
        logger.info("STUB: Generating placeholder dbt output")
        return (
            "-- FILE: models/staging/stg_placeholder.sql\n"
            "{{ config(materialized='view') }}\n\n"
            "SELECT\n"
            "    *\n"
            "FROM {{ source('raw', 'placeholder') }}\n\n"
            "-- FILE: models/staging/_sources.yml\n"
            "version: 2\n\n"
            "sources:\n"
            "  - name: raw\n"
            "    tables:\n"
            "      - name: placeholder\n\n"
            "-- FILE: models/staging/_stg__schema.yml\n"
            "version: 2\n\n"
            "models:\n"
            "  - name: stg_placeholder\n"
            "    columns:\n"
            "      - name: id\n"
            "        tests:\n"
            "          - not_null\n"
            "          - unique\n"
        )

"""Shared utilities: token estimation, timing, rate limiting, retry logic."""

from __future__ import annotations

import logging
import time
from functools import wraps
from typing import Callable, TypeVar

F = TypeVar("F", bound=Callable)

logger = logging.getLogger("informatica_dbt")


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configure and return the package-level logger."""
    root = logging.getLogger("informatica_dbt")
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.handlers.clear()

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)
    return root


# ---------------------------------------------------------------------------
# Token estimation
# ---------------------------------------------------------------------------

def estimate_token_count(text: str) -> int:
    """Heuristic token count for Claude models.

    Uses character-type weighting with a 20 % buffer for XML overhead.
    """
    if not text:
        return 0

    whitespace = sum(1 for c in text if c.isspace())
    alnum = sum(1 for c in text if c.isalnum())
    special = len(text) - whitespace - alnum

    estimated = (alnum + special) / 3.5 + whitespace / 6
    return int(estimated * 1.20)


# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------

def rate_limit(calls_per_minute: int = 1):
    """Decorator that enforces a minimum interval between calls."""
    min_interval = 60.0 / calls_per_minute
    last_called = [0.0]

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):
            elapsed = time.time() - last_called[0]
            if elapsed < min_interval:
                sleep_time = min_interval - elapsed
                logger.debug("Rate limiting: sleeping %.2fs", sleep_time)
                time.sleep(sleep_time)
            result = func(*args, **kwargs)
            last_called[0] = time.time()
            return result
        return wrapper  # type: ignore[return-value]
    return decorator


def retry_on_failure(max_retries: int = 3, base_delay: int = 2):
    """Decorator with exponential backoff retry logic."""
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    if attempt == max_retries - 1:
                        raise
                    wait = base_delay * (2 ** attempt)
                    logger.warning(
                        "Attempt %d/%d failed: %s — retrying in %ds",
                        attempt + 1, max_retries, exc, wait,
                    )
                    time.sleep(wait)
            return None  # unreachable but satisfies type checker
        return wrapper  # type: ignore[return-value]
    return decorator


class Timer:
    """Context manager for timing operations."""

    def __init__(self, label: str):
        self.label = label
        self.elapsed: float = 0.0

    def __enter__(self):
        self._start = time.time()
        return self

    def __exit__(self, *_):
        self.elapsed = time.time() - self._start
        logger.info("%s completed in %.2fs", self.label, self.elapsed)

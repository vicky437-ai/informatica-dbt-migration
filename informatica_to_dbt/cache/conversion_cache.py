"""SHA-256 fingerprinted conversion cache.

Ensures that re-running the framework on the same XML input (with the same
converter version and LLM model) produces identical output by caching
generated files after the first successful conversion.

Cache key = SHA-256(XML content + converter_version + llm_model)

Cache structure on disk::

    .infa2dbt/cache/
    ├── a1b2c3d4e5f6.../
    │   ├── metadata.json    # source filename, timestamp, quality score
    │   └── files/           # cached GeneratedFile objects serialised as files
    │       ├── models/staging/stg_orders.sql
    │       ├── models/staging/_sources.yml
    │       └── ...
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import shutil
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, Generator, List, Optional

from informatica_to_dbt.generator.response_parser import GeneratedFile

logger = logging.getLogger("informatica_dbt")


@dataclass
class CacheEntry:
    """Metadata stored alongside cached files."""

    cache_key: str
    xml_filename: str
    mapping_name: str
    converter_version: str
    llm_model: str
    quality_score: Optional[float] = None
    created_at: float = field(default_factory=time.time)
    file_count: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> CacheEntry:
        # Ignore unknown keys for forward-compatibility
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in data.items() if k in known})


class ConversionCache:
    """File-system cache for per-mapping conversion outputs.

    Args:
        cache_dir: Root directory for cache storage.
            Defaults to ``.infa2dbt/cache``.
        enabled: If False, all cache operations are no-ops.
    """

    def __init__(self, cache_dir: str = ".infa2dbt/cache", enabled: bool = True):
        self._cache_dir = Path(cache_dir)
        self._enabled = enabled

    @contextmanager
    def _entry_lock(self, cache_key: str) -> Generator[None, None, None]:
        """Acquire an exclusive file lock for a cache entry.

        Creates a ``.lock`` file alongside the entry directory.  The lock is
        released automatically when the context manager exits.
        """
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        lock_path = self._cache_dir / f"{cache_key}.lock"
        fd = open(lock_path, "w")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            fd.close()
            # Best-effort cleanup — another process may recreate immediately
            try:
                lock_path.unlink()
            except OSError:
                pass

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def cache_dir(self) -> Path:
        return self._cache_dir

    # ------------------------------------------------------------------
    # Key generation
    # ------------------------------------------------------------------

    @staticmethod
    def compute_key(
        xml_content: str,
        converter_version: str = "1.0.0",
        llm_model: str = "claude-4-sonnet",
        mapping_name: str = "",
        prompt_hash: str = "",
    ) -> str:
        """Compute a SHA-256 cache key from the input parameters.

        The key is deterministic: same inputs always yield the same key.
        When a single XML contains multiple mappings, *mapping_name*
        differentiates the cache entries.  The *prompt_hash* ensures
        that cache entries are invalidated when the system prompt changes.
        """
        payload = (
            f"{xml_content}\x00{converter_version}\x00{llm_model}"
            f"\x00{mapping_name}\x00{prompt_hash}"
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def get(self, cache_key: str) -> Optional[List[GeneratedFile]]:
        """Retrieve cached files for a cache key.

        Returns:
            List of ``GeneratedFile`` objects if the key exists and is valid,
            or ``None`` if not cached.
        """
        if not self._enabled:
            return None

        with self._entry_lock(cache_key):
            entry_dir = self._cache_dir / cache_key
            meta_path = entry_dir / "metadata.json"
            files_dir = entry_dir / "files"

            if not meta_path.exists() or not files_dir.exists():
                return None

            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                entry = CacheEntry.from_dict(meta)
            except (json.JSONDecodeError, TypeError, KeyError) as exc:
                logger.warning("Corrupt cache metadata for %s: %s", cache_key[:12], exc)
                return None

            # Reconstruct GeneratedFile list from the files/ directory
            generated: List[GeneratedFile] = []
            for root, _dirs, filenames in os.walk(files_dir):
                for fname in filenames:
                    abs_path = Path(root) / fname
                    rel_path = str(abs_path.relative_to(files_dir))
                    content = abs_path.read_text(encoding="utf-8")
                    generated.append(GeneratedFile(path=rel_path, content=content))

            if len(generated) != entry.file_count:
                logger.warning(
                    "Cache %s: expected %d files but found %d — treating as miss",
                    cache_key[:12], entry.file_count, len(generated),
                )
                return None

            logger.info(
                "Cache HIT for %s (%s, %d files)",
                cache_key[:12], entry.xml_filename, len(generated),
            )
            return generated

    # ------------------------------------------------------------------
    # Store
    # ------------------------------------------------------------------

    def put(
        self,
        cache_key: str,
        files: List[GeneratedFile],
        xml_filename: str = "",
        mapping_name: str = "",
        converter_version: str = "1.0.0",
        llm_model: str = "claude-4-sonnet",
        quality_score: Optional[float] = None,
    ) -> None:
        """Store conversion output in the cache.

        Args:
            cache_key: The SHA-256 key (from :meth:`compute_key`).
            files: List of generated files to cache.
            xml_filename: Original XML filename (for metadata).
            mapping_name: Informatica mapping name.
            converter_version: Framework version string.
            llm_model: LLM model used for generation.
            quality_score: Optional quality score from the scorer.
        """
        if not self._enabled:
            return

        with self._entry_lock(cache_key):
            entry_dir = self._cache_dir / cache_key
            files_dir = entry_dir / "files"

            # Clean any partial previous entry
            if entry_dir.exists():
                shutil.rmtree(entry_dir)

            files_dir.mkdir(parents=True, exist_ok=True)

            # Write each GeneratedFile
            for gf in files:
                out_path = files_dir / gf.path
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(gf.content, encoding="utf-8")

            # Write metadata
            entry = CacheEntry(
                cache_key=cache_key,
                xml_filename=xml_filename,
                mapping_name=mapping_name,
                converter_version=converter_version,
                llm_model=llm_model,
                quality_score=quality_score,
                file_count=len(files),
            )
            meta_path = entry_dir / "metadata.json"
            meta_path.write_text(
                json.dumps(entry.to_dict(), indent=2), encoding="utf-8"
            )

            logger.info(
                "Cache STORE for %s (%s, %d files)",
                cache_key[:12], xml_filename, len(files),
            )

    # ------------------------------------------------------------------
    # Management
    # ------------------------------------------------------------------

    def has(self, cache_key: str) -> bool:
        """Check if a cache entry exists (without loading files)."""
        if not self._enabled:
            return False
        meta_path = self._cache_dir / cache_key / "metadata.json"
        return meta_path.exists()

    def remove(self, cache_key: str) -> bool:
        """Remove a single cache entry. Returns True if it existed."""
        entry_dir = self._cache_dir / cache_key
        if entry_dir.exists():
            shutil.rmtree(entry_dir)
            logger.info("Cache REMOVE: %s", cache_key[:12])
            return True
        return False

    def clear(self) -> int:
        """Remove all cached entries. Returns count of entries removed."""
        if not self._cache_dir.exists():
            return 0

        count = 0
        for child in self._cache_dir.iterdir():
            if child.is_dir() and (child / "metadata.json").exists():
                shutil.rmtree(child)
                count += 1

        logger.info("Cache CLEAR: removed %d entries", count)
        return count

    def list_entries(self) -> List[CacheEntry]:
        """List all cache entries with their metadata."""
        entries: List[CacheEntry] = []
        if not self._cache_dir.exists():
            return entries

        for child in sorted(self._cache_dir.iterdir()):
            meta_path = child / "metadata.json"
            if meta_path.exists():
                try:
                    data = json.loads(meta_path.read_text(encoding="utf-8"))
                    entries.append(CacheEntry.from_dict(data))
                except (json.JSONDecodeError, TypeError):
                    logger.warning("Skipping corrupt cache entry: %s", child.name[:12])

        return entries

    def stats(self) -> Dict[str, int]:
        """Return summary statistics about the cache."""
        entries = self.list_entries()
        total_files = sum(e.file_count for e in entries)
        return {
            "entries": len(entries),
            "total_files": total_files,
        }

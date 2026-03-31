"""Unit tests for the conversion cache module."""

from __future__ import annotations

import json
import os

import pytest

from informatica_to_dbt.cache.conversion_cache import CacheEntry, ConversionCache
from informatica_to_dbt.generator.response_parser import GeneratedFile


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def cache_dir(tmp_path):
    return str(tmp_path / "cache")


@pytest.fixture
def cache(cache_dir):
    return ConversionCache(cache_dir=cache_dir, enabled=True)


@pytest.fixture
def sample_files():
    return [
        GeneratedFile(path="models/staging/stg_orders.sql", content="SELECT * FROM {{ source('raw', 'orders') }}"),
        GeneratedFile(path="models/staging/_sources.yml", content="version: 2\nsources:\n  - name: raw\n"),
    ]


# ---------------------------------------------------------------------------
# CacheEntry
# ---------------------------------------------------------------------------

class TestCacheEntry:
    def test_round_trip(self):
        entry = CacheEntry(
            cache_key="abc123",
            xml_filename="test.xml",
            mapping_name="m_load_orders",
            converter_version="1.0.0",
            llm_model="claude-4-sonnet",
            quality_score=85.0,
            file_count=3,
        )
        d = entry.to_dict()
        restored = CacheEntry.from_dict(d)
        assert restored.cache_key == "abc123"
        assert restored.mapping_name == "m_load_orders"
        assert restored.quality_score == 85.0
        assert restored.file_count == 3

    def test_from_dict_ignores_unknown_keys(self):
        d = {
            "cache_key": "k1",
            "xml_filename": "f.xml",
            "mapping_name": "m1",
            "converter_version": "1.0.0",
            "llm_model": "claude-4-sonnet",
            "future_field": "should_be_ignored",
        }
        entry = CacheEntry.from_dict(d)
        assert entry.cache_key == "k1"
        assert not hasattr(entry, "future_field")


# ---------------------------------------------------------------------------
# ConversionCache — key generation
# ---------------------------------------------------------------------------

class TestCacheKeyGeneration:
    def test_deterministic(self):
        k1 = ConversionCache.compute_key("xml content", "1.0.0", "claude-4-sonnet")
        k2 = ConversionCache.compute_key("xml content", "1.0.0", "claude-4-sonnet")
        assert k1 == k2

    def test_different_content_different_key(self):
        k1 = ConversionCache.compute_key("xml content A", "1.0.0", "claude-4-sonnet")
        k2 = ConversionCache.compute_key("xml content B", "1.0.0", "claude-4-sonnet")
        assert k1 != k2

    def test_different_version_different_key(self):
        k1 = ConversionCache.compute_key("same", "1.0.0", "claude-4-sonnet")
        k2 = ConversionCache.compute_key("same", "2.0.0", "claude-4-sonnet")
        assert k1 != k2

    def test_different_model_different_key(self):
        k1 = ConversionCache.compute_key("same", "1.0.0", "claude-4-sonnet")
        k2 = ConversionCache.compute_key("same", "1.0.0", "llama3-70b")
        assert k1 != k2

    def test_different_mapping_name_different_key(self):
        k1 = ConversionCache.compute_key("same", "1.0.0", "claude-4-sonnet", mapping_name="m1")
        k2 = ConversionCache.compute_key("same", "1.0.0", "claude-4-sonnet", mapping_name="m2")
        assert k1 != k2

    def test_key_is_hex_sha256(self):
        k = ConversionCache.compute_key("test")
        assert len(k) == 64  # SHA-256 hex digest length
        assert all(c in "0123456789abcdef" for c in k)


# ---------------------------------------------------------------------------
# ConversionCache — store and retrieve
# ---------------------------------------------------------------------------

class TestCacheStoreRetrieve:
    def test_put_and_get(self, cache, sample_files):
        key = ConversionCache.compute_key("test xml")
        cache.put(key, sample_files, xml_filename="test.xml", mapping_name="m1")

        result = cache.get(key)
        assert result is not None
        assert len(result) == 2
        assert result[0].path == "models/staging/stg_orders.sql"
        assert "SELECT * FROM" in result[0].content

    def test_get_nonexistent_returns_none(self, cache):
        assert cache.get("nonexistent_key") is None

    def test_has(self, cache, sample_files):
        key = ConversionCache.compute_key("has test")
        assert not cache.has(key)
        cache.put(key, sample_files)
        assert cache.has(key)

    def test_put_overwrites_existing(self, cache, sample_files):
        key = ConversionCache.compute_key("overwrite test")
        cache.put(key, sample_files, mapping_name="v1")
        cache.put(key, [sample_files[0]], mapping_name="v2")

        result = cache.get(key)
        assert result is not None
        assert len(result) == 1  # overwritten with single file

    def test_file_count_mismatch_returns_none(self, cache, sample_files):
        """If files are missing from disk, treat as cache miss."""
        key = ConversionCache.compute_key("mismatch test")
        cache.put(key, sample_files)

        # Manually delete one file
        files_dir = cache.cache_dir / key / "files"
        first_file = next(files_dir.rglob("*.sql"))
        first_file.unlink()

        assert cache.get(key) is None

    def test_corrupt_metadata_returns_none(self, cache, sample_files):
        key = ConversionCache.compute_key("corrupt test")
        cache.put(key, sample_files)

        # Corrupt the metadata
        meta_path = cache.cache_dir / key / "metadata.json"
        meta_path.write_text("not valid json{{{", encoding="utf-8")

        assert cache.get(key) is None


# ---------------------------------------------------------------------------
# ConversionCache — disabled mode
# ---------------------------------------------------------------------------

class TestCacheDisabled:
    def test_disabled_put_is_noop(self, cache_dir, sample_files):
        c = ConversionCache(cache_dir=cache_dir, enabled=False)
        key = ConversionCache.compute_key("disabled")
        c.put(key, sample_files)
        # Nothing should be written
        assert not os.path.exists(os.path.join(cache_dir, key))

    def test_disabled_get_returns_none(self, cache_dir, sample_files):
        # First store with enabled cache
        c_on = ConversionCache(cache_dir=cache_dir, enabled=True)
        key = ConversionCache.compute_key("disabled get")
        c_on.put(key, sample_files)

        # Then try to get with disabled cache
        c_off = ConversionCache(cache_dir=cache_dir, enabled=False)
        assert c_off.get(key) is None

    def test_disabled_has_returns_false(self, cache_dir, sample_files):
        c_on = ConversionCache(cache_dir=cache_dir, enabled=True)
        key = ConversionCache.compute_key("disabled has")
        c_on.put(key, sample_files)

        c_off = ConversionCache(cache_dir=cache_dir, enabled=False)
        assert not c_off.has(key)


# ---------------------------------------------------------------------------
# ConversionCache — management
# ---------------------------------------------------------------------------

class TestCacheManagement:
    def test_remove(self, cache, sample_files):
        key = ConversionCache.compute_key("remove test")
        cache.put(key, sample_files)
        assert cache.has(key)

        assert cache.remove(key) is True
        assert not cache.has(key)

    def test_remove_nonexistent(self, cache):
        assert cache.remove("nonexistent") is False

    def test_clear(self, cache, sample_files):
        cache.put(ConversionCache.compute_key("a"), sample_files)
        cache.put(ConversionCache.compute_key("b"), sample_files)

        count = cache.clear()
        assert count == 2
        assert cache.stats()["entries"] == 0

    def test_clear_empty(self, cache):
        assert cache.clear() == 0

    def test_list_entries(self, cache, sample_files):
        cache.put(
            ConversionCache.compute_key("list1"),
            sample_files,
            mapping_name="mapping_a",
        )
        cache.put(
            ConversionCache.compute_key("list2"),
            [sample_files[0]],
            mapping_name="mapping_b",
        )

        entries = cache.list_entries()
        assert len(entries) == 2
        names = {e.mapping_name for e in entries}
        assert names == {"mapping_a", "mapping_b"}

    def test_stats(self, cache, sample_files):
        cache.put(ConversionCache.compute_key("s1"), sample_files)
        cache.put(ConversionCache.compute_key("s2"), [sample_files[0]])

        stats = cache.stats()
        assert stats["entries"] == 2
        assert stats["total_files"] == 3  # 2 + 1

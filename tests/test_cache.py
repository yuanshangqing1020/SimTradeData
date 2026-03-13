"""Tests for MemoryCache: LRU eviction and TTL expiry."""

import time

import pytest

from simtradedata.cache.cache import MemoryCache


@pytest.mark.unit
class TestMemoryCacheBasic:
    """Basic get/set/delete operations."""

    def test_get_missing_key_returns_none(self):
        cache = MemoryCache()
        assert cache.get("nonexistent") is None

    def test_set_and_get(self):
        cache = MemoryCache()
        cache.set("key1", "value1", ttl=60)
        assert cache.get("key1") == "value1"

    def test_delete(self):
        cache = MemoryCache()
        cache.set("key1", "value1", ttl=60)
        cache.delete("key1")
        assert cache.get("key1") is None

    def test_delete_missing_key_no_error(self):
        cache = MemoryCache()
        cache.delete("nonexistent")

    def test_clear(self):
        cache = MemoryCache()
        cache.set("a", 1, ttl=60)
        cache.set("b", 2, ttl=60)
        cache.clear()
        assert cache.size == 0
        assert cache.get("a") is None

    def test_size(self):
        cache = MemoryCache()
        assert cache.size == 0
        cache.set("a", 1, ttl=60)
        assert cache.size == 1
        cache.set("b", 2, ttl=60)
        assert cache.size == 2

    def test_overwrite_existing_key(self):
        cache = MemoryCache()
        cache.set("k", "old", ttl=60)
        cache.set("k", "new", ttl=60)
        assert cache.get("k") == "new"
        assert cache.size == 1


@pytest.mark.unit
class TestMemoryCacheTTL:
    """TTL expiry via lazy check on get."""

    def test_expired_entry_returns_none(self):
        cache = MemoryCache()
        cache.set("k", "v", ttl=0.01)
        time.sleep(0.02)
        assert cache.get("k") is None

    def test_expired_entry_is_removed_from_size(self):
        cache = MemoryCache()
        cache.set("k", "v", ttl=0.01)
        time.sleep(0.02)
        cache.get("k")
        assert cache.size == 0

    def test_non_expired_entry_still_works(self):
        cache = MemoryCache()
        cache.set("k", "v", ttl=10)
        assert cache.get("k") == "v"


@pytest.mark.unit
class TestMemoryCacheLRU:
    """LRU eviction when max_size is exceeded."""

    def test_evicts_oldest_when_full(self):
        cache = MemoryCache(max_size=3)
        cache.set("a", 1, ttl=60)
        cache.set("b", 2, ttl=60)
        cache.set("c", 3, ttl=60)
        cache.set("d", 4, ttl=60)
        assert cache.get("a") is None
        assert cache.get("d") == 4
        assert cache.size == 3

    def test_access_refreshes_lru_order(self):
        cache = MemoryCache(max_size=3)
        cache.set("a", 1, ttl=60)
        cache.set("b", 2, ttl=60)
        cache.set("c", 3, ttl=60)
        cache.get("a")  # refresh "a"
        cache.set("d", 4, ttl=60)  # evicts "b"
        assert cache.get("a") == 1
        assert cache.get("b") is None

    def test_overwrite_does_not_increase_size(self):
        cache = MemoryCache(max_size=2)
        cache.set("a", 1, ttl=60)
        cache.set("b", 2, ttl=60)
        cache.set("a", 10, ttl=60)
        assert cache.size == 2
        assert cache.get("b") == 2

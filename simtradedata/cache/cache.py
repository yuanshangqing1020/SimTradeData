"""Thread-safe in-memory cache with LRU eviction and TTL expiry."""

import threading
import time
from collections import OrderedDict
from dataclasses import dataclass


@dataclass
class _CacheEntry:
    value: object
    expire_at: float


class MemoryCache:
    """
    In-memory LRU cache with per-entry TTL.

    - Max entries capped at *max_size*; least-recently-used evicted on insert.
    - Expiry is lazy: checked on ``get()``, not via background sweep.
    - Thread-safe via ``threading.RLock``.
    """

    def __init__(self, max_size: int = 1000):
        self._max_size = max_size
        self._data: OrderedDict[str, _CacheEntry] = OrderedDict()
        self._lock = threading.RLock()

    def get(self, key: str) -> object:
        """Return cached value or None if missing/expired."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            if time.monotonic() > entry.expire_at:
                del self._data[key]
                return None
            self._data.move_to_end(key)
            return entry.value

    def set(self, key: str, value: object, ttl: float) -> None:
        """Store *value* under *key* with *ttl* seconds until expiry."""
        with self._lock:
            if key in self._data:
                del self._data[key]
            self._data[key] = _CacheEntry(
                value=value,
                expire_at=time.monotonic() + ttl,
            )
            self._data.move_to_end(key)
            while len(self._data) > self._max_size:
                self._data.popitem(last=False)

    def delete(self, key: str) -> None:
        """Remove *key* if present."""
        with self._lock:
            self._data.pop(key, None)

    def clear(self) -> None:
        """Remove all entries."""
        with self._lock:
            self._data.clear()

    @property
    def size(self) -> int:
        """Number of entries (including possibly expired ones)."""
        return len(self._data)

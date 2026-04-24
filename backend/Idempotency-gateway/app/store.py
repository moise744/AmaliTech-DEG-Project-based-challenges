"""
In-memory idempotency store.

The problem with a naive implementation: two concurrent requests for a new key
can both read "not found" before either writes, and both start processing the
payment. The fix is locking, but a single global lock would block unrelated
payments for no reason.

Per-key locks solve it cleanly — requests for the same key block each other,
requests for different keys run in parallel. To create those per-key Lock
objects without a race of their own, there's a thin meta-lock that's only held
long enough to do a dict lookup or insert.

Neither lock is ever held while the payment itself is running.

What happens when two requests for key K arrive at the same time:

  Request A gets in first:
    meta_lock → fetch/create Lock(K) → release meta_lock
    Lock(K) → store[K] empty → insert IN_FLIGHT record → release Lock(K)
    [ runs payment — no lock ]
    Lock(K) → writes result, marks COMPLETED → release Lock(K)
    fires record.event   (always after data is written)

  Request B arrives while A is running:
    meta_lock → fetch Lock(K) → release meta_lock
    Lock(K) → store[K] is IN_FLIGHT → release Lock(K)
    waits on record.event.wait()
    wakes up, reads the cached result, done
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from app.models import IdempotencyRecord, RecordStatus

logger = logging.getLogger(__name__)


class IdempotencyStore:
    def __init__(self) -> None:
        self._store: dict[str, IdempotencyRecord] = {}
        self._key_locks: dict[str, asyncio.Lock] = {}
        self._meta_lock = asyncio.Lock()

    async def _key_lock(self, key: str) -> asyncio.Lock:
        async with self._meta_lock:
            if key not in self._key_locks:
                self._key_locks[key] = asyncio.Lock()
            return self._key_locks[key]

    async def get_or_create(
        self, key: str, request_hash: str
    ) -> tuple[IdempotencyRecord, bool]:
        """Look up key; create a fresh IN_FLIGHT record if it doesn't exist yet.

        The insert happens while holding the key lock, so two concurrent callers
        can't both see "not found" and both decide to process the payment.

        Returns (record, created). created=True means this caller owns the key
        and must call complete() when the payment finishes.
        """
        lock = await self._key_lock(key)
        async with lock:
            existing = self._store.get(key)
            if existing is not None:
                return existing, False
            record = IdempotencyRecord(key=key, request_hash=request_hash)
            self._store[key] = record
            logger.info("new key: %s", key)
            return record, True

    async def complete(self, key: str, response_body: dict, response_status: int) -> None:
        """Save the result and wake any requests that are waiting on this key.

        The data is always written before event.set(), so a waiter that wakes up
        is guaranteed to see a fully populated record.
        """
        lock = await self._key_lock(key)
        async with lock:
            record = self._store.get(key)
            if record is None:
                logger.error("complete() called on missing key: %s", key)
                return
            record.response_body = response_body
            record.response_status = response_status
            record.status = RecordStatus.COMPLETED
            record.completed_at = datetime.now(timezone.utc)

        # set() happens outside the lock so waiters don't have to fight for it
        record.event.set()
        logger.info("completed key: %s", key)

    async def evict_expired(self, ttl_seconds: int) -> int:
        """Delete COMPLETED records that are older than ttl_seconds.

        IN_FLIGHT records are left alone no matter how old they are. Deleting
        one mid-flight would strand any waiting callers with nothing to wake them,
        and complete() would silently write to a deleted record.
        """
        now = datetime.now(timezone.utc)
        to_evict: list[str] = []

        async with self._meta_lock:
            candidates = list(self._store.keys())

        for key in candidates:
            lock = await self._key_lock(key)
            async with lock:
                record = self._store.get(key)
                if not record or record.status != RecordStatus.COMPLETED:
                    continue
                if record.completed_at is None:
                    continue
                ts = record.completed_at
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if (now - ts).total_seconds() > ttl_seconds:
                    del self._store[key]
                    to_evict.append(key)

        if to_evict:
            async with self._meta_lock:
                for key in to_evict:
                    self._key_locks.pop(key, None)
            logger.info("evicted %d key(s)", len(to_evict))

        return len(to_evict)

    def size(self) -> int:
        return len(self._store)


store = IdempotencyStore()

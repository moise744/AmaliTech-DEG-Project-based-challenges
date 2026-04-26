"""
Tests for the idempotency gateway.

Four scenarios from the spec + seven edge cases.
Concurrency tests use real async sleeps so the actual lock/event paths run.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.models import IdempotencyRecord, RecordStatus
from app.store import IdempotencyStore


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest_asyncio.fixture
async def client():
    from app import store as s
    s.store._store.clear()
    s.store._key_locks.clear()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac


ENDPOINT = "/process-payment"


def fresh_key() -> str:
    return str(uuid.uuid4())


async def pay(client, key, amount=100.0, currency="GHS"):
    return await client.post(
        ENDPOINT,
        json={"amount": amount, "currency": currency},
        headers={"Idempotency-Key": key},
    )


# --- Scenario 1: first request ---

@pytest.mark.anyio
async def test_first_request_is_processed(client):
    with patch("app.service.asyncio.sleep"):
        resp = await pay(client, fresh_key())

    assert resp.status_code == 201
    body = resp.json()
    assert body["status"] == "success"
    assert body["message"] == "Charged 100 GHS"
    assert body.get("transaction_id")
    assert resp.headers.get("X-Cache-Hit") is None


# --- Scenario 2: duplicate request ---

@pytest.mark.anyio
async def test_duplicate_returns_cached_response(client):
    key = fresh_key()
    call_count = 0
    orig = asyncio.sleep

    async def counting_sleep(_):
        nonlocal call_count
        call_count += 1
        await orig(0)

    with patch("app.service.asyncio.sleep", side_effect=counting_sleep):
        r1 = await pay(client, key)
        r2 = await pay(client, key)

    assert r1.status_code == r2.status_code == 201
    assert r1.json() == r2.json()
    assert r1.headers.get("X-Cache-Hit") is None
    assert r2.headers.get("X-Cache-Hit") == "true"
    assert call_count == 1  # payment ran once


# --- Scenario 3: same key, different body ---

@pytest.mark.anyio
async def test_different_body_same_key_returns_422(client):
    key = fresh_key()
    with patch("app.service.asyncio.sleep"):
        r1 = await pay(client, key, amount=100)
    assert r1.status_code == 201

    with patch("app.service.asyncio.sleep"):
        r2 = await pay(client, key, amount=500)
    assert r2.status_code == 422
    assert "different request body" in r2.json()["detail"]


# --- Scenario 4: concurrent duplicates ---

@pytest.mark.anyio
async def test_concurrent_duplicates_process_once(client):
    key = fresh_key()
    call_count = 0
    orig = asyncio.sleep

    async def slow(_):
        nonlocal call_count
        call_count += 1
        await orig(0.05)

    with patch("app.service.asyncio.sleep", side_effect=slow):
        ra, rb = await asyncio.gather(pay(client, key), pay(client, key))

    assert call_count == 1
    assert ra.status_code == rb.status_code == 201
    assert ra.json() == rb.json()
    hits = sum(1 for r in (ra, rb) if r.headers.get("X-Cache-Hit") == "true")
    assert hits == 1


# --- Edge A: missing header ---

@pytest.mark.anyio
async def test_missing_header_returns_400(client):
    resp = await client.post(ENDPOINT, json={"amount": 100, "currency": "GHS"})
    assert resp.status_code == 400
    assert "Missing" in resp.json()["detail"]


# --- Edge B: negative amount ---

@pytest.mark.anyio
async def test_negative_amount_returns_422(client):
    resp = await client.post(ENDPOINT, json={"amount": -50, "currency": "GHS"}, headers={"Idempotency-Key": fresh_key()})
    assert resp.status_code == 422


# --- Edge C: missing currency ---

@pytest.mark.anyio
async def test_missing_currency_returns_422(client):
    resp = await client.post(ENDPOINT, json={"amount": 100}, headers={"Idempotency-Key": fresh_key()})
    assert resp.status_code == 422


# --- Edge D: conflict during in-flight ---

@pytest.mark.anyio
async def test_conflict_during_in_flight_returns_422_immediately(client):
    """Request A (amount=100) is mid-flight; Request B (amount=500) must get 422, not wait."""
    key = fresh_key()
    orig = asyncio.sleep
    a_flying = asyncio.Event()

    async def gate(_):
        a_flying.set()
        await orig(0.1)

    with patch("app.service.asyncio.sleep", side_effect=gate):
        task_a = asyncio.create_task(pay(client, key, amount=100))
        await a_flying.wait()
        rb = await pay(client, key, amount=500)
        ra = await task_a

    assert ra.status_code == 201
    assert ra.json()["message"] == "Charged 100 GHS"
    assert rb.status_code == 422
    assert "different request body" in rb.json()["detail"]


# --- Edge E: different keys run in parallel ---

@pytest.mark.anyio
async def test_different_keys_run_concurrently(client):
    """Per-key locks must not serialise unrelated keys."""
    ka, kb = fresh_key(), fresh_key()
    orig = asyncio.sleep

    async def short(_):
        await orig(0.05)

    with patch("app.service.asyncio.sleep", side_effect=short):
        t0 = time.monotonic()
        ra, rb = await asyncio.gather(pay(client, ka, amount=100), pay(client, kb, amount=200))
        elapsed = time.monotonic() - t0

    assert ra.status_code == rb.status_code == 201
    assert elapsed < 0.09, f"looks serialised ({elapsed:.3f}s) — check per-key locking"


# --- Edge F: expired records evicted ---

@pytest.mark.anyio
async def test_evict_expired_removes_old_completed_records():
    s = IdempotencyStore()

    old = IdempotencyRecord(
        key="old", request_hash="aaa", status=RecordStatus.COMPLETED,
        response_body={"status": "success"}, response_status=201,
        completed_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    old.event.set()
    s._store["old"] = old

    recent = IdempotencyRecord(
        key="recent", request_hash="bbb", status=RecordStatus.COMPLETED,
        response_body={"status": "success"}, response_status=201,
        completed_at=datetime.now(timezone.utc) - timedelta(seconds=30),
    )
    recent.event.set()
    s._store["recent"] = recent

    evicted = await s.evict_expired(ttl_seconds=3_600)
    assert evicted == 1
    assert "old" not in s._store
    assert "recent" in s._store


# --- Edge G: in-flight never evicted ---

@pytest.mark.anyio
async def test_evict_expired_skips_in_flight_records():
    s = IdempotencyStore()
    s._store["stuck"] = IdempotencyRecord(
        key="stuck", request_hash="ccc", status=RecordStatus.IN_FLIGHT,
        created_at=datetime.now(timezone.utc) - timedelta(hours=99),
    )
    assert await s.evict_expired(ttl_seconds=1) == 0
    assert "stuck" in s._store

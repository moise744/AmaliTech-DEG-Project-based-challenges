"""
Idempotency logic and payment processing.

Every request through process_payment() ends up in one of four places depending
on what the store already knows about that key:

  - Key is brand new: we take ownership, run the payment, cache the result.
    This is the only place _simulate_payment() ever gets called.

  - Key exists but the body hash doesn't match: reject with 422 right away.
    This check has to happen before any waiting — if we waited and then handed
    back the cached result for a different payment, that would be wrong.

  - Key exists, body matches, first request still running: block on the event
    until the first request finishes, then return what it cached.

  - Key exists, body matches, already done: return the cached result directly.

Failed payments are cached just like successful ones. Retrying a key that already
failed returns the same error — the payment engine doesn't run again.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from typing import Any

from fastapi import HTTPException

from app import config
from app.models import PaymentRequest, RecordStatus
from app.store import IdempotencyStore

logger = logging.getLogger(__name__)


def _canonical_hash(body: dict[str, Any]) -> str:
    """SHA-256 of the request body with keys sorted.

    Sorting keys means {"amount":100,"currency":"GHS"} and
    {"currency":"GHS","amount":100} produce the same hash.
    """
    s = json.dumps(body, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode()).hexdigest()


async def _simulate_payment(request: PaymentRequest) -> dict[str, Any]:
    """Stand-in for a real payment processor call.

    In production this would hit Stripe, Paystack, or similar.
    The 2-second sleep is the window where a client might time out and retry.
    """
    logger.info("processing payment %.4g %s", request.amount, request.currency)
    await asyncio.sleep(config.PAYMENT_PROCESSING_DELAY)
    return {
        "status": "success",
        "message": f"Charged {request.amount:g} {request.currency}",
        "transaction_id": str(uuid.uuid4()),
    }


async def process_payment(
    idempotency_key: str,
    request: PaymentRequest,
    idempotency_store: IdempotencyStore,
) -> tuple[dict[str, Any], int, bool]:
    """Run a payment request through the idempotency layer.

    Returns (response_body, http_status, cache_hit).
    cache_hit=True means the response came from the store, not the payment engine.
    """
    request_hash = _canonical_hash(request.model_dump())

    record, created = await idempotency_store.get_or_create(
        key=idempotency_key,
        request_hash=request_hash,
    )

    if created:
        # We own this key — run the payment and cache whatever comes back.
        try:
            body = await _simulate_payment(request)
            status = 201
        except Exception as exc:
            logger.exception("payment failed for key %s", idempotency_key)
            body = {"status": "error", "message": str(exc)}
            status = 500
        await idempotency_store.complete(idempotency_key, body, status)
        return body, status, False

    # Key exists. Check the body hash before doing anything else — if it doesn't
    # match we reject immediately rather than waiting and returning the wrong result.
    if record.request_hash != request_hash:
        logger.warning(
            "body conflict on key %s (stored=%.8s incoming=%.8s)",
            idempotency_key, record.request_hash, request_hash,
        )
        raise HTTPException(
            status_code=422,
            detail="Idempotency key already used for a different request body.",
        )

    # Same key, same body, first request still running — wait for it.
    if record.status == RecordStatus.IN_FLIGHT:
        logger.info("key %s in-flight, waiting up to %.0fs", idempotency_key, config.IN_FLIGHT_TIMEOUT_SECONDS)
        try:
            await asyncio.wait_for(record.event.wait(), timeout=config.IN_FLIGHT_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=504,
                detail="Timed out waiting for the in-flight request to complete.",
            )

    # Either it was already done when we arrived, or we just waited for it.
    logger.info("cache hit for key %s (status=%s)", idempotency_key, record.response_status)
    return record.response_body, record.response_status, True

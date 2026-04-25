from __future__ import annotations

import logging

from fastapi import APIRouter, Header, HTTPException, Response
from fastapi.responses import JSONResponse

from app.models import ErrorResponse, PaymentRequest
from app.service import process_payment
from app.store import store

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post(
    "/process-payment",
    status_code=201,
    summary="Submit a payment",
    responses={
        201: {"description": "Payment processed or replayed from cache"},
        400: {"model": ErrorResponse, "description": "Missing Idempotency-Key header"},
        422: {"model": ErrorResponse, "description": "Invalid body or key reused with different body"},
        504: {"model": ErrorResponse, "description": "Timed out waiting for in-flight request"},
    },
)
async def submit_payment(
    body: PaymentRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> Response:
    if not idempotency_key or not idempotency_key.strip():
        raise HTTPException(status_code=400, detail="Missing Idempotency-Key header.")

    response_body, status_code, cache_hit = await process_payment(
        idempotency_key=idempotency_key.strip(),
        request=body,
        idempotency_store=store,
    )

    headers = {"X-Cache-Hit": "true"} if cache_hit else {}
    return JSONResponse(content=response_body, status_code=status_code, headers=headers)


@router.get("/health", include_in_schema=False)
async def health() -> dict:
    # Developer-added, not in the spec. Useful for checking store growth.
    return {"status": "ok", "store_size": store.size()}

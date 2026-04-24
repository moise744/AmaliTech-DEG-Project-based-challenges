from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator


# --- Public API shapes (validated at the HTTP boundary) ---

class PaymentRequest(BaseModel):
    amount: float = Field(..., gt=0, examples=[100.0])
    currency: str = Field(..., min_length=3, max_length=3, examples=["GHS"])

    @field_validator("currency")
    @classmethod
    def upper(cls, v: str) -> str:
        return v.upper()


class PaymentResponse(BaseModel):
    status: str
    message: str
    transaction_id: str


class ErrorResponse(BaseModel):
    detail: str


# --- Internal store record ---

class RecordStatus(str, Enum):
    IN_FLIGHT = "in_flight"
    COMPLETED = "completed"


@dataclass
class IdempotencyRecord:
    """One entry in the idempotency store, keyed by Idempotency-Key header value.

    The event is a wake signal only — it carries no data. Response data is always
    written under the key lock before the event fires, so any waiter that wakes
    up is guaranteed to see a complete record.
    """

    key: str
    request_hash: str
    status: RecordStatus = RecordStatus.IN_FLIGHT

    response_body: dict[str, Any] | None = None
    response_status: int | None = None

    event: asyncio.Event = field(default_factory=asyncio.Event)

    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None

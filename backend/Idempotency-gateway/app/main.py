from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app import config
from app.routes import router
from app.store import store

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)


async def _expiry_loop() -> None:
    """Background task: sweep completed idempotency keys older than KEY_TTL_SECONDS.

    Without this the store grows without bound. IN_FLIGHT records are never
    touched here — see store.evict_expired() for why.
    """
    logger.info("expiry loop started (TTL=%ds, interval=%ds)", config.KEY_TTL_SECONDS, config.CLEANUP_INTERVAL_SECONDS)
    while True:
        await asyncio.sleep(config.CLEANUP_INTERVAL_SECONDS)
        n = await store.evict_expired(config.KEY_TTL_SECONDS)
        logger.info("expiry sweep done, evicted %d", n)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_expiry_loop())
    logger.info("ready")
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def create_app() -> FastAPI:
    app = FastAPI(
        title="Idempotency Gateway",
        description="Pay-once payment middleware — processes each logical transaction exactly once regardless of retries.",
        version="1.0.0",
        lifespan=lifespan,
    )
    # No prefix — spec says POST /process-payment
    app.include_router(router, tags=["payments"])

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception):
        logger.exception("unhandled error on %s %s", request.method, request.url)
        return JSONResponse(status_code=500, content={"detail": "Internal server error."})

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)

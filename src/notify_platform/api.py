from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

from notify_platform.config import Settings, load_settings
from notify_platform.db import close_pool, configure_pool, get_pool
from notify_platform.logging import setup_logging
from notify_platform.metrics import CONNECTED
from notify_platform.metrics import render as render_metrics
from notify_platform.plane import (
    cursor_gap_too_old,
    inbox_since,
    jsonable,
    list_deliveries,
    list_inbox,
    publish,
    seed_lab,
    set_muted,
    upsert_endpoint,
)

logger = logging.getLogger("notify.api")
settings: Settings = load_settings()


class PublishBody(BaseModel):
    tenant: str = Field(min_length=1, max_length=100)
    event_type: str = Field(min_length=1, max_length=100)
    idempotency_key: str = Field(min_length=1, max_length=200)
    payload: dict[str, Any] = Field(default_factory=dict)
    recipients: list[str] = Field(min_length=1, max_length=50_000)


class MuteBody(BaseModel):
    tenant: str
    user_id: str
    event_type: str
    muted: bool = True


class EndpointBody(BaseModel):
    tenant: str
    url: str
    secret: str = "lab-secret"


def _error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"code": code, "message": message})


@asynccontextmanager
async def lifespan(_app: FastAPI):  # type: ignore[no-untyped-def]
    global settings
    settings = load_settings()
    setup_logging(settings.log_level)
    configure_pool(settings.database_url)
    with get_pool().connection() as conn:
        seed_lab(conn)
    yield
    close_pool()


app = FastAPI(title="realtime-event-platform", lifespan=lifespan)


@app.middleware("http")
async def limit_body(request: Request, call_next):  # type: ignore[no-untyped-def]
    length = request.headers.get("content-length")
    if length and int(length) > settings.max_body_bytes:
        return _error(413, "payload_too_large", "request body exceeds MAX_BODY_BYTES")
    return await call_next(request)


@app.get("/health")
def health() -> dict[str, str]:
    with get_pool().connection() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
    return Response(content=render_metrics(), media_type="text/plain; version=0.0.4")


@app.post("/events")
def post_event(body: PublishBody) -> Any:
    return publish(
        body.tenant, body.event_type, body.idempotency_key, body.payload, body.recipients
    )


@app.get("/inbox")
def get_inbox(
    tenant: str,
    user_id: str,
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    with get_pool().connection() as conn:
        rows = list_inbox(conn, tenant, user_id, limit)
    return {"items": jsonable(rows)}


@app.get("/deliveries")
def get_deliveries(status: str | None = None) -> dict[str, Any]:
    with get_pool().connection() as conn:
        rows = list_deliveries(conn, status)
    return {"deliveries": jsonable(rows)}


@app.post("/prefs/mute")
def mute(body: MuteBody) -> dict[str, Any]:
    with get_pool().connection() as conn:
        set_muted(conn, body.tenant, body.user_id, body.event_type, body.muted)
    return {"ok": True, "muted": body.muted}


@app.post("/endpoints")
def endpoints(body: EndpointBody) -> dict[str, str]:
    with get_pool().connection() as conn:
        upsert_endpoint(conn, body.tenant, body.url, body.secret)
    return {"ok": "true"}


@app.get("/stream")
async def stream(
    tenant: str,
    user_id: str,
    cursor: int = Query(default=0, ge=0),
    limit: int | None = Query(default=None, ge=1, le=500),
) -> StreamingResponse:
    cap = settings.replay_cap

    async def gen() -> AsyncIterator[str]:
        CONNECTED.inc()
        last = cursor
        sent = 0
        try:
            with get_pool().connection() as conn:
                if cursor_gap_too_old(conn, tenant, user_id, cursor, cap):
                    meta = {
                        "code": "cursor_expired",
                        "snapshot": f"/inbox?tenant={tenant}&user_id={user_id}",
                    }
                    yield "event: meta\n" + f"data: {json.dumps(meta)}\n\n"
                    return
                replay = inbox_since(conn, tenant, user_id, last, cap)
            for row in replay:
                last = int(row["id"])
                payload = {
                    "id": row["id"],
                    "event_id": str(row["event_id"]),
                    "event_type": row["event_type"],
                    "body": row["body"],
                }
                yield f"id: {row['id']}\ndata: {json.dumps(payload, default=str)}\n\n"
                sent += 1
                if limit is not None and sent >= limit:
                    return
            while True:
                await asyncio.sleep(settings.poll_interval_seconds)
                with get_pool().connection() as conn:
                    nxt = inbox_since(conn, tenant, user_id, last, cap)
                for row in nxt:
                    last = int(row["id"])
                    payload = {
                        "id": row["id"],
                        "event_id": str(row["event_id"]),
                        "event_type": row["event_type"],
                        "body": row["body"],
                    }
                    yield f"id: {row['id']}\ndata: {json.dumps(payload, default=str)}\n\n"
                    sent += 1
                    if limit is not None and sent >= limit:
                        return
        finally:
            CONNECTED.dec()

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "cache-control": "no-cache",
            "connection": "keep-alive",
            "x-accel-buffering": "no",
        },
    )


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    path = Path(__file__).with_name("static") / "index.html"
    return path.read_text(encoding="utf-8")


def main() -> None:
    cfg = load_settings()
    uvicorn.run("notify_platform.api:app", host=cfg.api_host, port=cfg.api_port, reload=False)


if __name__ == "__main__":
    main()

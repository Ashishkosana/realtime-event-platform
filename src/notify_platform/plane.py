"""Publish, fan-out, inbox, preferences. At-least-once deliveries are the worker."""

from __future__ import annotations

import logging
import time
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Json

from notify_platform.config import load_settings
from notify_platform.db import get_pool
from notify_platform.logging import log_event
from notify_platform.metrics import INBOX, MUTED, PUBLISH, PUBLISH_MS

logger = logging.getLogger("notify")


def jsonable(obj: Any) -> Any:
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: jsonable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [jsonable(v) for v in obj]
    return obj


def notify_live(conn: Any) -> None:
    conn.execute("NOTIFY inbox_wake")


def is_muted(conn: Any, tenant: str, user_id: str, event_type: str) -> bool:
    row = conn.execute(
        """
        SELECT muted FROM ntf_prefs
        WHERE tenant = %s AND user_id = %s AND event_type = %s
        """,
        (tenant, user_id, event_type),
    ).fetchone()
    return bool(row and row["muted"])


def set_muted(conn: Any, tenant: str, user_id: str, event_type: str, muted: bool) -> None:
    conn.execute(
        """
        INSERT INTO ntf_prefs (tenant, user_id, event_type, muted)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (tenant, user_id, event_type) DO UPDATE SET muted = EXCLUDED.muted
        """,
        (tenant, user_id, event_type, muted),
    )
    conn.commit()


def upsert_endpoint(conn: Any, tenant: str, url: str, secret: str) -> None:
    conn.execute(
        """
        INSERT INTO ntf_endpoints (tenant, webhook_url, webhook_secret)
        VALUES (%s, %s, %s)
        ON CONFLICT (tenant) DO UPDATE
        SET webhook_url = EXCLUDED.webhook_url, webhook_secret = EXCLUDED.webhook_secret
        """,
        (tenant, url, secret),
    )
    conn.commit()


def _write_inbox_and_deliveries(
    conn: Any,
    event_id: UUID,
    tenant: str,
    event_type: str,
    payload: dict[str, Any],
    user_id: str,
) -> None:
    settings = load_settings()
    inbox = conn.execute(
        """
        INSERT INTO ntf_inbox (tenant, user_id, event_id, event_type, body)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """,
        (tenant, user_id, event_id, event_type, Json(payload)),
    ).fetchone()
    INBOX.labels(event_type=event_type).inc()
    inbox_id = inbox["id"]
    endpoint = conn.execute(
        "SELECT webhook_url, webhook_secret FROM ntf_endpoints WHERE tenant = %s",
        (tenant,),
    ).fetchone()
    if endpoint:
        conn.execute(
            """
            INSERT INTO ntf_deliveries (
              id, event_id, inbox_id, channel, destination, status, max_attempts
            ) VALUES (%s, %s, %s, 'webhook', %s, 'pending', %s)
            """,
            (
                uuid4(),
                event_id,
                inbox_id,
                endpoint["webhook_url"],
                settings.webhook_max_attempts,
            ),
        )
    conn.execute(
        """
        INSERT INTO ntf_deliveries (
          id, event_id, inbox_id, channel, destination, status, max_attempts
        ) VALUES (%s, %s, %s, 'email', %s, 'pending', %s)
        """,
        (uuid4(), event_id, inbox_id, f"fake-email:{user_id}", settings.webhook_max_attempts),
    )


def _apply_fanout_rows(conn: Any, event: dict[str, Any], limit: int) -> int:
    rows = conn.execute(
        """
        SELECT user_id FROM ntf_fanout
        WHERE event_id = %s AND status = 'pending'
        ORDER BY seq
        LIMIT %s
        FOR UPDATE SKIP LOCKED
        """,
        (event["id"], limit),
    ).fetchall()
    done = 0
    for row in rows:
        user_id = row["user_id"]
        if is_muted(conn, event["tenant"], user_id, event["event_type"]):
            conn.execute(
                "UPDATE ntf_fanout SET status = 'muted' WHERE event_id = %s AND user_id = %s",
                (event["id"], user_id),
            )
            MUTED.labels(event_type=event["event_type"]).inc()
        else:
            _write_inbox_and_deliveries(
                conn,
                event["id"],
                event["tenant"],
                event["event_type"],
                event["payload"],
                user_id,
            )
            conn.execute(
                "UPDATE ntf_fanout SET status = 'done' WHERE event_id = %s AND user_id = %s",
                (event["id"], user_id),
            )
        done += 1
    if done:
        conn.execute(
            """
            UPDATE ntf_events
            SET fanout_done = fanout_done + %s
            WHERE id = %s
            """,
            (done, event["id"]),
        )
    remaining = conn.execute(
        "SELECT count(*) AS n FROM ntf_fanout WHERE event_id = %s AND status = 'pending'",
        (event["id"],),
    ).fetchone()
    if remaining and remaining["n"] == 0:
        conn.execute(
            "UPDATE ntf_events SET fanout_status = 'complete' WHERE id = %s",
            (event["id"],),
        )
    return done


def continue_fanout(conn: Any, chunk: int | None = None) -> dict[str, Any] | None:
    settings = load_settings()
    limit = chunk if chunk is not None else settings.fanout_chunk
    event = conn.execute(
        """
        SELECT * FROM ntf_events
        WHERE fanout_status IN ('pending', 'running')
        ORDER BY created_at
        LIMIT 1
        FOR UPDATE SKIP LOCKED
        """
    ).fetchone()
    if event is None:
        return None
    conn.execute(
        "UPDATE ntf_events SET fanout_status = 'running' WHERE id = %s",
        (event["id"],),
    )
    n = _apply_fanout_rows(conn, dict(event), limit)
    notify_live(conn)
    conn.commit()
    return {"event_id": str(event["id"]), "chunk": n}


def publish(
    tenant: str,
    event_type: str,
    idempotency_key: str,
    payload: dict[str, Any],
    recipients: list[str],
) -> dict[str, Any]:
    t0 = time.perf_counter()
    settings = load_settings()
    unique: list[str] = []
    seen: set[str] = set()
    for user in recipients:
        if user and user not in seen:
            seen.add(user)
            unique.append(user)
    event_id = uuid4()
    with get_pool().connection() as conn:
        inserted = conn.execute(
            """
            INSERT INTO ntf_events (
              id, tenant, event_type, idempotency_key, payload,
              fanout_status, fanout_total
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant, event_type, idempotency_key) DO NOTHING
            RETURNING *
            """,
            (
                event_id,
                tenant,
                event_type,
                idempotency_key,
                Json(payload),
                "pending",
                len(unique),
            ),
        ).fetchone()
        if inserted is None:
            existing = conn.execute(
                """
                SELECT * FROM ntf_events
                WHERE tenant = %s AND event_type = %s AND idempotency_key = %s
                """,
                (tenant, event_type, idempotency_key),
            ).fetchone()
            conn.commit()
            PUBLISH.labels(event_type=event_type, duplicate="true").inc()
            return jsonable({"created": False, "event": dict(existing) if existing else None})
        for seq, user_id in enumerate(unique, start=1):
            conn.execute(
                """
                INSERT INTO ntf_fanout (event_id, tenant, user_id, seq)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (event_id, user_id) DO NOTHING
                """,
                (inserted["id"], tenant, user_id, seq),
            )
        # V1: small lists complete in this transaction (all-or-nothing).
        # V2: larger lists stay pending for resumable chunk workers.
        if len(unique) <= settings.fanout_chunk:
            _apply_fanout_rows(conn, dict(inserted), settings.fanout_chunk)
        notify_live(conn)
        conn.commit()
    PUBLISH.labels(event_type=event_type, duplicate="false").inc()
    PUBLISH_MS.observe((time.perf_counter() - t0) * 1000)
    log_event(
        logger,
        "publish",
        tenant=tenant,
        event_type=event_type,
        event_id=str(inserted["id"]),
        recipients=len(unique),
        sync=len(unique) <= settings.fanout_chunk,
    )
    with get_pool().connection() as conn:
        row = conn.execute("SELECT * FROM ntf_events WHERE id = %s", (inserted["id"],)).fetchone()
    return jsonable({"created": True, "event": dict(row) if row else dict(inserted)})


def inbox_since(
    conn: Any,
    tenant: str,
    user_id: str,
    cursor: int,
    limit: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT * FROM ntf_inbox
        WHERE tenant = %s AND user_id = %s AND id > %s
        ORDER BY id
        LIMIT %s
        """,
        (tenant, user_id, cursor, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def cursor_gap_too_old(conn: Any, tenant: str, user_id: str, cursor: int, cap: int) -> bool:
    if cursor <= 0:
        return False
    stats = conn.execute(
        """
        SELECT coalesce(min(id), 0) AS lo, coalesce(max(id), 0) AS hi, count(*) AS n
        FROM ntf_inbox WHERE tenant = %s AND user_id = %s AND id > %s
        """,
        (tenant, user_id, cursor),
    ).fetchone()
    if not stats:
        return False
    return int(stats["n"]) > cap


def list_inbox(conn: Any, tenant: str, user_id: str, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT * FROM ntf_inbox
        WHERE tenant = %s AND user_id = %s
        ORDER BY id DESC LIMIT %s
        """,
        (tenant, user_id, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def list_deliveries(conn: Any, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    if status:
        rows = conn.execute(
            "SELECT * FROM ntf_deliveries WHERE status = %s ORDER BY updated_at DESC LIMIT %s",
            (status, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM ntf_deliveries ORDER BY updated_at DESC LIMIT %s",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def seed_lab(conn: Any) -> None:
    upsert_endpoint(conn, "acme", "fake://ok", "lab-secret")
    conn.commit()

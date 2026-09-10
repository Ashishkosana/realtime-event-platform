from __future__ import annotations

import hmac
import logging
import os
import random
import signal
import socket
import threading
import time
import uuid
from hashlib import sha256
from typing import Any

import httpx

from notify_platform.config import Settings, load_settings
from notify_platform.db import configure_pool, get_pool
from notify_platform.logging import log_event, setup_logging
from notify_platform.metrics import DELIVER
from notify_platform.plane import continue_fanout

logger = logging.getLogger("notify.worker")

CLAIM_SQL = """
UPDATE ntf_deliveries d
SET status = 'running',
    worker_id = %(worker_id)s,
    fencing_token = d.fencing_token + 1,
    attempt_count = d.attempt_count + 1,
    leased_until = now() + %(ttl)s::interval,
    updated_at = now()
WHERE d.id = (
  SELECT id FROM ntf_deliveries
  WHERE status IN ('pending', 'running')
    AND run_after <= now()
    AND (status = 'pending' OR leased_until IS NULL OR leased_until < now())
  ORDER BY run_after
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
RETURNING *
"""


def _sign(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), body, sha256).hexdigest()


def _backoff(attempt: int, base: float, cap: float, rng: random.Random) -> float:
    raw = min(cap, base * (2 ** max(0, attempt - 1)))
    return rng.random() * raw


def _deliver(row: dict[str, Any], secret: str | None) -> None:
    dest = row["destination"]
    body = f'{{"event_id":"{row["event_id"]}","delivery_id":"{row["id"]}"}}'.encode()
    if dest.startswith("fake://ok"):
        return
    if dest.startswith("fake://fail"):
        raise RuntimeError("injected webhook 500")
    if dest.startswith("fake://slow"):
        time.sleep(2.0)
        return
    if dest.startswith("fake-email:"):
        user_id = dest.split(":", 1)[1]
        with get_pool().connection() as conn:
            conn.execute(
                """
                INSERT INTO ntf_lab_email (tenant, user_id, event_id, body)
                VALUES (
                  (SELECT tenant FROM ntf_events WHERE id = %s),
                  %s, %s, %s::jsonb
                )
                """,
                (row["event_id"], user_id, row["event_id"], body.decode()),
            )
            conn.commit()
        return
    headers = {"content-type": "application/json", "x-notify-id": str(row["id"])}
    if secret:
        headers["x-notify-signature"] = _sign(secret, body)
    resp = httpx.post(dest, content=body, headers=headers, timeout=2.0)
    if resp.status_code >= 500:
        raise RuntimeError(f"webhook {resp.status_code}")
    if resp.status_code >= 400:
        raise RuntimeError(f"webhook {resp.status_code} not retryable")


def claim(conn: Any, worker_id: str, ttl: str) -> dict[str, Any] | None:
    return conn.execute(CLAIM_SQL, {"worker_id": worker_id, "ttl": ttl}).fetchone()


def finish(conn: Any, row: dict[str, Any], ok: bool, error: str | None, settings: Settings) -> str:
    rng = random.Random()
    if ok:
        conn.execute(
            """
            UPDATE ntf_deliveries
            SET status = 'succeeded', last_error = NULL, leased_until = NULL, updated_at = now()
            WHERE id = %s AND fencing_token = %s AND status = 'running'
            """,
            (row["id"], row["fencing_token"]),
        )
        conn.commit()
        DELIVER.labels(channel=row["channel"], outcome="succeeded").inc()
        return "succeeded"
    if (not ok) and error and "not retryable" in error:
        dead = True
    else:
        dead = int(row["attempt_count"]) >= int(row["max_attempts"])
    if dead:
        conn.execute(
            """
            UPDATE ntf_deliveries
            SET status = 'dead_lettered', last_error = %s, leased_until = NULL, updated_at = now()
            WHERE id = %s AND fencing_token = %s
            """,
            (error, row["id"], row["fencing_token"]),
        )
        conn.commit()
        DELIVER.labels(channel=row["channel"], outcome="dead_lettered").inc()
        return "dead_lettered"
    delay = _backoff(
        int(row["attempt_count"]), settings.base_backoff_seconds, settings.max_backoff_seconds, rng
    )
    conn.execute(
        """
        UPDATE ntf_deliveries
        SET status = 'pending', last_error = %s, leased_until = NULL,
            run_after = now() + (%s || ' seconds')::interval, updated_at = now()
        WHERE id = %s AND fencing_token = %s
        """,
        (error, str(delay), row["id"], row["fencing_token"]),
    )
    conn.commit()
    DELIVER.labels(channel=row["channel"], outcome="retry").inc()
    return "retry"


def execute(row: dict[str, Any], settings: Settings) -> None:
    secret = None
    with get_pool().connection() as conn:
        ev = conn.execute(
            "SELECT tenant FROM ntf_events WHERE id = %s", (row["event_id"],)
        ).fetchone()
        if ev:
            ep = conn.execute(
                "SELECT webhook_secret FROM ntf_endpoints WHERE tenant = %s",
                (ev["tenant"],),
            ).fetchone()
            if ep:
                secret = ep["webhook_secret"]
    try:
        _deliver(dict(row), secret)
        err = None
        ok = True
    except Exception as exc:
        ok = False
        err = str(exc)
        if "not retryable" not in err and "4" == err[-3:-2]:
            err = err + " not retryable"
    with get_pool().connection() as conn:
        result = finish(conn, dict(row), ok, err, settings)
    log_event(
        logger,
        "deliver",
        delivery_id=str(row["id"]),
        channel=row["channel"],
        result=result,
        attempt=row["attempt_count"],
    )


def slot_loop(slot: int, settings: Settings, stop: threading.Event, boot: str) -> None:
    worker_id = f"{socket.gethostname()}:{os.getpid()}:{slot}:{boot}"
    listen_conn = None
    if settings.wake_mode == "listen":
        try:
            from psycopg import connect
            from psycopg.rows import dict_row

            listen_conn = connect(settings.database_url, autocommit=True, row_factory=dict_row)
            listen_conn.execute("LISTEN inbox_wake")
        except Exception:
            logger.exception("LISTEN failed")
            listen_conn = None
    while not stop.is_set():
        progressed = False
        try:
            with get_pool().connection() as conn:
                fan = continue_fanout(conn)
                if fan:
                    progressed = True
                claimed = claim(conn, worker_id, settings.lease_ttl_sql)
                conn.commit()
        except Exception:
            logger.exception("claim failed")
            stop.wait(0.4)
            continue
        if claimed:
            execute(dict(claimed), settings)
            progressed = True
        if progressed:
            continue
        if listen_conn is not None:
            try:
                for _ in listen_conn.notifies(timeout=settings.poll_interval_seconds, stop_after=1):
                    break
            except Exception:
                stop.wait(settings.poll_interval_seconds)
        else:
            stop.wait(settings.poll_interval_seconds)
    if listen_conn is not None:
        listen_conn.close()


def main() -> None:
    settings = load_settings()
    setup_logging(settings.log_level)
    configure_pool(settings.database_url, max_size=max(4, settings.worker_concurrency * 3))
    stop = threading.Event()

    def handle(signum: int, _frame: object) -> None:
        log_event(logger, "shutdown", signal=signum)
        stop.set()

    signal.signal(signal.SIGTERM, handle)
    signal.signal(signal.SIGINT, handle)
    boot = uuid.uuid4().hex[:8]
    threads = [
        threading.Thread(target=slot_loop, args=(i, settings, stop, boot), daemon=True)
        for i in range(max(1, settings.worker_concurrency))
    ]
    for thread in threads:
        thread.start()
    try:
        while not stop.is_set():
            stop.wait(0.5)
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=5)


if __name__ == "__main__":
    main()

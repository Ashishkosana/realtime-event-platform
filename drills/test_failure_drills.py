from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from notify_platform.config import load_settings
from notify_platform.db import get_pool
from notify_platform.plane import continue_fanout, publish, upsert_endpoint
from notify_platform.worker import claim, execute


def _drain(n: int = 200) -> None:
    settings = load_settings()
    for _ in range(n):
        with get_pool().connection() as conn:
            conn.execute(
                "UPDATE ntf_deliveries SET run_after = now() WHERE status = 'pending'"
            )
            row = claim(conn, "drill", settings.lease_ttl_sql)
            conn.commit()
        if row is None:
            return
        execute(dict(row), settings)


def test_poison_webhook_dead_letters() -> None:
    with get_pool().connection() as conn:
        upsert_endpoint(conn, "acme", "fake://fail", "s")
    publish("acme", "order.paid", "poison", {}, ["u1"])
    for _ in range(8):
        _drain(20)
    with get_pool().connection() as conn:
        row = conn.execute(
            "SELECT status, attempt_count FROM ntf_deliveries WHERE channel = 'webhook'"
        ).fetchone()
    assert row["status"] == "dead_lettered"


def test_resumable_fanout_chunk(monkeypatch) -> None:
    monkeypatch.setenv("FANOUT_CHUNK", "3")
    out = publish("acme", "blast", "big", {}, [f"u{i}" for i in range(10)])
    assert out["created"] is True
    with get_pool().connection() as conn:
        ev = conn.execute(
            "SELECT fanout_status, fanout_total FROM ntf_events WHERE idempotency_key = 'big'"
        ).fetchone()
        inbox = conn.execute("SELECT count(*) AS n FROM ntf_inbox").fetchone()
    assert ev["fanout_status"] == "pending"
    assert inbox["n"] == 0
    for _ in range(5):
        with get_pool().connection() as conn:
            continue_fanout(conn, chunk=3)
    with get_pool().connection() as conn:
        ev = conn.execute(
            "SELECT fanout_status, fanout_done FROM ntf_events WHERE idempotency_key = 'big'"
        ).fetchone()
        inbox = conn.execute("SELECT count(*) AS n FROM ntf_inbox").fetchone()
    assert ev["fanout_status"] == "complete"
    assert inbox["n"] == 10


def test_slow_webhook_does_not_block_email() -> None:
    with get_pool().connection() as conn:
        upsert_endpoint(conn, "acme", "fake://slow", "s")
    publish("acme", "order.paid", "slow1", {}, ["u1"])
    settings = load_settings()

    def one() -> str | None:
        with get_pool().connection() as conn:
            row = claim(conn, "iso", settings.lease_ttl_sql)
            conn.commit()
        if row is None:
            return None
        execute(dict(row), settings)
        return row["channel"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(one) for _ in range(2)]
        channels = [f.result() for f in futs]
    assert "email" in channels

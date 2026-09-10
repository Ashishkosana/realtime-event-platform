from __future__ import annotations

from notify_platform.config import load_settings
from notify_platform.plane import publish
from notify_platform.worker import claim, execute


def _drain(limit: int = 50) -> None:
    settings = load_settings()
    from notify_platform.db import get_pool

    for _ in range(limit):
        with get_pool().connection() as conn:
            row = claim(conn, "test-worker", settings.lease_ttl_sql)
            conn.commit()
        if row is None:
            return
        execute(dict(row), settings)


def test_health(client) -> None:
    assert client.get("/health").json()["status"] == "ok"


def test_duplicate_publish(client) -> None:
    body = {
        "tenant": "acme",
        "event_type": "order.paid",
        "idempotency_key": "same",
        "payload": {"n": 1},
        "recipients": ["u1"],
    }
    a = client.post("/events", json=body).json()
    b = client.post("/events", json=body).json()
    assert a["created"] is True
    assert b["created"] is False
    inbox = client.get("/inbox?tenant=acme&user_id=u1").json()["items"]
    assert len(inbox) == 1


def test_mute_skips_inbox(client) -> None:
    client.post(
        "/prefs/mute",
        json={"tenant": "acme", "user_id": "u1", "event_type": "promo", "muted": True},
    )
    client.post(
        "/events",
        json={
            "tenant": "acme",
            "event_type": "promo",
            "idempotency_key": "p1",
            "payload": {},
            "recipients": ["u1", "u2"],
        },
    )
    assert client.get("/inbox?tenant=acme&user_id=u1").json()["items"] == []
    assert len(client.get("/inbox?tenant=acme&user_id=u2").json()["items"]) == 1


def test_sse_replay(client) -> None:
    client.post(
        "/events",
        json={
            "tenant": "acme",
            "event_type": "order.paid",
            "idempotency_key": "s1",
            "payload": {"x": 1},
            "recipients": ["u1"],
        },
    )
    with client.stream("GET", "/stream?tenant=acme&user_id=u1&cursor=0&limit=1") as resp:
        assert resp.status_code == 200
        buf = b""
        for chunk in resp.iter_bytes():
            buf += chunk
            if b"\n\n" in buf:
                break
    assert b"order.paid" in buf


def test_email_delivery_at_least_once() -> None:
    publish("acme", "order.paid", "e1", {"ok": True}, ["u1"])
    _drain()
    from notify_platform.db import get_pool

    with get_pool().connection() as conn:
        n = conn.execute("SELECT count(*) AS n FROM ntf_lab_email").fetchone()
        d = conn.execute(
            """
            SELECT count(*) AS n FROM ntf_deliveries
            WHERE channel = 'email' AND status = 'succeeded'
            """
        ).fetchone()
    assert n["n"] == 1
    assert d["n"] == 1

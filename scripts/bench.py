#!/usr/bin/env python3
"""Honest ingest/fan-out numbers. Does not invent results."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from notify_platform.db import close_pool, configure_pool
from notify_platform.plane import publish, seed_lab

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    dsn = os.environ.get("DATABASE_URL", "postgresql://workflow:workflow@127.0.0.1:5432/notify")
    configure_pool(dsn)
    from psycopg import connect
    from psycopg.rows import dict_row

    with connect(dsn, row_factory=dict_row) as conn:
        seed_lab(conn)
        conn.execute(
            """
            TRUNCATE ntf_lab_email, ntf_deliveries, ntf_inbox, ntf_fanout, ntf_events
            RESTART IDENTITY CASCADE
            """
        )
        conn.commit()
    n = 50
    t0 = time.perf_counter()
    for i in range(n):
        publish("acme", "order.paid", f"b-{i}", {"i": i}, ["u1"])
    elapsed = time.perf_counter() - t0
    t1 = time.perf_counter()
    publish("acme", "blast", "fan-1k", {}, [f"u{i}" for i in range(200)])
    fan = time.perf_counter() - t1
    report = {
        "tiny_fanout_publishes": n,
        "tiny_fanout_seconds": round(elapsed, 4),
        "tiny_per_s": round(n / elapsed, 2),
        "fanout_200_seconds": round(fan, 4),
        "note": "200 recipients is the default FANOUT_CHUNK sync path. Larger N is V2 chunked.",
    }
    (ROOT / "bench-results.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    close_pool()


if __name__ == "__main__":
    main()

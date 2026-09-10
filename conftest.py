from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient
from psycopg import connect
from psycopg.rows import dict_row

from notify_platform.db import configure_pool, get_pool
from notify_platform.plane import seed_lab

TEST_DSN = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://workflow:workflow@127.0.0.1:5432/notify_test",
)


def _ensure_db() -> None:
    admin = os.environ.get(
        "ADMIN_DATABASE_URL",
        "postgresql://workflow:workflow@127.0.0.1:5432/postgres",
    )
    name = TEST_DSN.rsplit("/", 1)[-1]
    with connect(admin, autocommit=True) as conn:
        row = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone()
        if row is None:
            conn.execute(f'CREATE DATABASE "{name}"')


@pytest.fixture(scope="session")
def database_url() -> str:
    _ensure_db()
    os.environ["DATABASE_URL"] = TEST_DSN
    os.environ["API_PORT"] = "43200"
    os.environ["FANOUT_CHUNK"] = "200"
    os.environ["WAKE_MODE"] = "poll"
    os.environ["BASE_BACKOFF_SECONDS"] = "0.02"
    os.environ["WEBHOOK_MAX_ATTEMPTS"] = "4"
    from notify_platform.db import apply_schema

    with connect(TEST_DSN, autocommit=True) as conn:
        apply_schema(conn)
        seed_lab(conn)
    configure_pool(TEST_DSN)
    return TEST_DSN


@pytest.fixture
def client(database_url: str) -> TestClient:
    from notify_platform.api import app

    with TestClient(app) as test_client:
        yield test_client
    configure_pool(database_url)


@pytest.fixture(autouse=True)
def clean_db(database_url: str) -> None:
    try:
        get_pool()
    except RuntimeError:
        configure_pool(database_url)
    with connect(database_url, row_factory=dict_row, autocommit=True) as conn:
        conn.execute(
            """
            TRUNCATE ntf_lab_email, ntf_deliveries, ntf_inbox, ntf_fanout,
                     ntf_events, ntf_prefs RESTART IDENTITY CASCADE
            """
        )
        seed_lab(conn)
    yield

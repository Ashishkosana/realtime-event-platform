from __future__ import annotations

from pathlib import Path

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

_pool: ConnectionPool | None = None


def schema_sql() -> str:
    pkg = Path(__file__).with_name("schema.sql")
    root = Path(__file__).resolve().parents[2] / "schema.sql"
    path = root if root.exists() else pkg
    return path.read_text(encoding="utf-8")


def split_sql(script: str) -> list[str]:
    statements: list[str] = []
    buf: list[str] = []
    for line in script.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or stripped == "":
            if buf:
                buf.append(line)
            continue
        buf.append(line)
        if stripped.endswith(";"):
            stmt = "\n".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
    tail = "\n".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def apply_schema(conn: Connection) -> None:  # type: ignore[type-arg]
    for statement in split_sql(schema_sql()):
        conn.execute(statement)
    conn.commit()


def configure_pool(dsn: str, max_size: int = 16) -> ConnectionPool:
    global _pool
    if _pool is not None:
        _pool.close()
    _pool = ConnectionPool(
        conninfo=dsn,
        min_size=1,
        max_size=max_size,
        kwargs={"row_factory": dict_row, "autocommit": False},
        open=True,
    )
    with _pool.connection() as conn:
        apply_schema(conn)
    return _pool


def get_pool() -> ConnectionPool:
    if _pool is None:
        raise RuntimeError("pool not configured")
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None

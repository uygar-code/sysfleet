"""Async engine, session factory, and SQLite tuning.

Kept deliberately free of SQLite-specific types so the move to PostgreSQL is a
URL change. The one SQLite-only part -- the PRAGMA tuning below -- is guarded by
a dialect check and simply does not fire on other backends.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator

from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from .config import get_settings

logger = logging.getLogger("sysfleet.hub.db")


class Base(DeclarativeBase):
    """Declarative base. Alembic autogenerate reads metadata off this."""


def _build_engine() -> AsyncEngine:
    settings = get_settings()
    url = settings.database_url
    is_sqlite = url.startswith("sqlite")

    engine = create_async_engine(
        url,
        echo=False,
        future=True,
        # SQLite has no real server-side pooling and aiosqlite runs each
        # connection on its own thread; a small pool avoids spawning threads we
        # never use. On Postgres the defaults are sensible, so leave them.
        pool_pre_ping=not is_sqlite,
    )

    if is_sqlite:
        _tune_sqlite(engine)

    return engine


def _tune_sqlite(engine: AsyncEngine) -> None:
    """Apply the PRAGMAs that make write-heavy SQLite viable.

    Defaults are tuned for correctness on a single connection, not for a
    process ingesting a sample per second per agent:

    * **WAL** lets readers (the REST API) run concurrently with the batch
      writer. In the default rollback-journal mode a write transaction blocks
      every reader, so a flush would stall ``GET /machines`` every few seconds.
    * **synchronous=NORMAL** stops fsync-ing on every single commit. Combined
      with WAL the durability loss is bounded to the last few committed
      transactions on an OS crash -- an acceptable trade for metrics, which are
      by nature lossy and replaced a second later.
    * **busy_timeout** makes a contended write wait rather than immediately
      raising "database is locked".
    """

    @event.listens_for(engine.sync_engine, "connect")
    def _set_pragmas(dbapi_connection, _connection_record):  # noqa: ANN001
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA busy_timeout=5000")
            # Foreign keys are OFF by default in SQLite; without this the FK
            # from metrics -> machines would be decorative only.
            cursor.execute("PRAGMA foreign_keys=ON")
        finally:
            cursor.close()


engine: AsyncEngine = _build_engine()

SessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,  # keep attributes usable after commit
    autoflush=False,
)


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency yielding a request-scoped session."""
    async with SessionLocal() as session:
        yield session


async def dispose_engine() -> None:
    await engine.dispose()

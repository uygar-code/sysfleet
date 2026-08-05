"""Buffered metric ingestion.

Agents each push a sample per second. Committing one INSERT per sample would
make write throughput a function of fsync latency, and SQLite would spend its
life in transaction overhead for data that nobody reads in real time (the live
view is served straight off the WebSocket fan-out, never off the database).

So samples land in an in-memory queue and a single background task drains it,
flushing every ``flush_interval_s`` or once ``flush_max_rows`` have piled up --
whichever comes first. One transaction, one executemany, N rows.

The queue is bounded. If the writer stalls (disk full, database locked), we drop
the *oldest* buffered samples rather than grow without limit: a monitoring tool
that OOMs the box it is monitoring has failed at its one job. Dropping the oldest
also means what survives is the most recent data, which is what an operator
staring at an incident actually wants.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import async_sessionmaker

from .config import Settings
from .models import Metric

logger = logging.getLogger("sysfleet.hub.ingest")


@dataclass
class IngestStats:
    """Counters for /health. Cheap to maintain, invaluable when debugging."""

    queued: int = 0
    written: int = 0
    dropped: int = 0
    flushes: int = 0
    failed_flushes: int = 0
    last_flush_rows: int = 0
    last_error: str | None = field(default=None)

    def as_dict(self, pending: int) -> dict[str, object]:
        return {
            "queued": self.queued,
            "written": self.written,
            "dropped": self.dropped,
            "pending": pending,
            "flushes": self.flushes,
            "failed_flushes": self.failed_flushes,
            "last_flush_rows": self.last_flush_rows,
            "last_error": self.last_error,
        }


class MetricWriter:
    """Owns the buffer and the single flushing task."""

    def __init__(
        self, session_factory: async_sessionmaker, settings: Settings
    ) -> None:
        self._session_factory = session_factory
        self._settings = settings
        self._queue: asyncio.Queue[dict] = asyncio.Queue(
            maxsize=settings.ingest_queue_max
        )
        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()
        self.stats = IngestStats()

    # ------------------------------------------------------------- lifecycle

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stopping.clear()
            self._task = asyncio.create_task(self._run(), name="metric-writer")
            logger.info(
                "Metric writer started (flush every %.1fs or %d rows)",
                self._settings.flush_interval_s,
                self._settings.flush_max_rows,
            )

    async def stop(self) -> None:
        """Signal shutdown and flush whatever is still buffered.

        Without this final drain, up to flush_interval_s of samples would be
        lost on every deploy or Ctrl-C -- the batching equivalent of a memory
        leak, except the data is simply gone.
        """
        self._stopping.set()
        if self._task and not self._task.done():
            await self._task
        remaining = self._drain_nowait()
        if remaining:
            logger.info("Flushing %d buffered rows on shutdown", len(remaining))
            await self._write(remaining)

    # ---------------------------------------------------------------- submit

    def submit(self, machine_id: int, ts: datetime, sample: dict) -> None:
        """Enqueue one sample. Never blocks, never raises.

        Called from the WebSocket receive path, which must stay responsive: a
        slow database must not translate into backpressure on every agent in
        the fleet.
        """
        row = {
            "machine_id": machine_id,
            "ts": ts,
            "cpu_percent": sample.get("cpu_percent"),
            "mem_percent": sample.get("mem_percent"),
            "mem_used_gb": sample.get("mem_used_gb"),
            "disk_percent": sample.get("disk_percent"),
            "disk_read_mbs": sample.get("disk_read_mbs"),
            "disk_write_mbs": sample.get("disk_write_mbs"),
            "net_up_kbs": sample.get("net_up_kbs"),
            "net_down_kbs": sample.get("net_down_kbs"),
            "uptime_seconds": sample.get("uptime_seconds"),
        }

        try:
            self._queue.put_nowait(row)
            self.stats.queued += 1
        except asyncio.QueueFull:
            # Make room by discarding the oldest sample, then retry once.
            try:
                self._queue.get_nowait()
                self._queue.task_done()
                self.stats.dropped += 1
                self._queue.put_nowait(row)
                self.stats.queued += 1
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                self.stats.dropped += 1

            if self.stats.dropped % 100 == 1:
                logger.warning(
                    "Ingest queue saturated, dropped %d samples so far",
                    self.stats.dropped,
                )

    # ------------------------------------------------------------------ loop

    async def _run(self) -> None:
        interval = self._settings.flush_interval_s
        max_rows = self._settings.flush_max_rows

        try:
            while not self._stopping.is_set():
                batch: list[dict] = []
                deadline = asyncio.get_running_loop().time() + interval

                # Accumulate until the window closes or the batch fills up.
                while len(batch) < max_rows:
                    timeout = deadline - asyncio.get_running_loop().time()
                    if timeout <= 0:
                        break
                    try:
                        row = await asyncio.wait_for(self._queue.get(), timeout=timeout)
                    except (asyncio.TimeoutError, TimeoutError):
                        break
                    batch.append(row)
                    self._queue.task_done()

                    if self._stopping.is_set():
                        break

                if batch:
                    await self._write(batch)

        except asyncio.CancelledError:
            logger.info("Metric writer cancelled")
            raise

    async def _write(self, rows: list[dict]) -> None:
        """One transaction, one multi-row INSERT."""
        try:
            async with self._session_factory() as session:
                async with session.begin():
                    await session.execute(insert(Metric), rows)

            self.stats.written += len(rows)
            self.stats.flushes += 1
            self.stats.last_flush_rows = len(rows)
            logger.debug("Flushed %d metric rows", len(rows))

        except Exception as exc:
            # Losing a batch is survivable and self-correcting: the next sample
            # is a second away. Re-queueing risks an unbounded retry storm
            # against a database that is already unhappy.
            self.stats.failed_flushes += 1
            self.stats.last_error = f"{type(exc).__name__}: {exc}"
            logger.exception("Failed to flush %d metric rows", len(rows))

    def _drain_nowait(self) -> list[dict]:
        rows: list[dict] = []
        while True:
            try:
                rows.append(self._queue.get_nowait())
                self._queue.task_done()
            except asyncio.QueueEmpty:
                return rows

    # ----------------------------------------------------------------- stats

    @property
    def pending(self) -> int:
        return self._queue.qsize()

    def stats_dict(self) -> dict[str, object]:
        return self.stats.as_dict(self.pending)

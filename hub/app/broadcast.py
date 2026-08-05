"""Fan-out of live metrics to dashboard subscribers.

This is the decoupling seam for a future frontend. Agent ingest never calls into
a UI concern directly; it publishes here, and whatever is listening (a browser,
a CLI tail, nothing at all) gets a copy. The Hub behaves identically with zero
subscribers, so the live path costs nothing when unused.

Deliberately *not* backed by the database: a dashboard wants the sample that
arrived 40ms ago, not one that survived a batch flush. Live view reads from this
broadcast; historical view reads from SQL. Two paths, two very different
latency and durability requirements.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

from fastapi import WebSocket

logger = logging.getLogger("sysfleet.hub.broadcast")

# Per-subscriber outbound buffer. A dashboard on a slow link must not be able to
# slow down metric ingestion, so each subscriber gets a bounded queue and is
# dropped if it falls too far behind.
SUBSCRIBER_QUEUE_MAX = 100


class DashboardBroadcaster:
    """Tracks dashboard sockets and pushes frames to them."""

    def __init__(self) -> None:
        self._subscribers: dict[WebSocket, asyncio.Queue[dict[str, Any]]] = {}
        self._lock = asyncio.Lock()

    async def subscribe(self, websocket: WebSocket) -> asyncio.Queue:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
            maxsize=SUBSCRIBER_QUEUE_MAX
        )
        async with self._lock:
            self._subscribers[websocket] = queue
        logger.info("Dashboard subscribed (%d total)", len(self._subscribers))
        return queue

    async def unsubscribe(self, websocket: WebSocket) -> None:
        async with self._lock:
            self._subscribers.pop(websocket, None)
        logger.info("Dashboard unsubscribed (%d remaining)", len(self._subscribers))

    def publish(self, payload: dict[str, Any]) -> None:
        """Hand a frame to every subscriber. Synchronous and non-blocking.

        Called from the agent receive path, so it must never await on a slow
        consumer. Frames are dropped per-subscriber when their queue is full;
        for live telemetry a dropped frame is strictly better than delaying
        ingest for everyone else.
        """
        if not self._subscribers:
            return

        for websocket, queue in list(self._subscribers.items()):
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                logger.debug("Dashboard subscriber lagging, dropping frame")

    async def sender_loop(self, websocket: WebSocket, queue: asyncio.Queue) -> None:
        """Drain one subscriber's queue onto its socket."""
        with contextlib.suppress(asyncio.CancelledError):
            while True:
                payload = await queue.get()
                await websocket.send_json(payload)

    @property
    def client_count(self) -> int:
        return len(self._subscribers)

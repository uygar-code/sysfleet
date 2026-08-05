"""SysFleet agent entry point.

Protocol (agent -> Hub, over a single WebSocket):

    1. ``hello``    sent once immediately after the socket opens. Carries the
                    agent_key plus static machine facts so the Hub can register
                    or look up the machine row before any metric arrives.
    2. ``metrics``  sent every INTERVAL_MS thereafter, one flat sample per frame.

Hub -> agent is minimal: a ``welcome`` ack, and an optional ``config`` frame that
can retune the sampling interval remotely. Anything else is ignored, so the Hub
can add frame types later without breaking older agents.

Run with:  python -m agent.main
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
from typing import Any

try:  # websockets >= 13 moved the asyncio client here
    from websockets.asyncio.client import connect
except ImportError:  # pragma: no cover - older websockets fallback
    from websockets.client import connect  # type: ignore[no-redef]

from . import __version__
from .collector import MetricsCollector, system_info
from .config import MAX_INTERVAL_MS, MIN_INTERVAL_MS, AgentConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("sysfleet.agent")


class _Session:
    """Mutable per-connection state.

    Only the sampling interval lives here for now: the Hub may push a ``config``
    frame mid-stream, and the send loop needs to observe that change without the
    two coroutines sharing anything more tangled than one float.
    """

    def __init__(self, interval_s: float) -> None:
        self.interval_s = interval_s


async def _read_loop(ws: Any, session: _Session) -> None:
    """Drain inbound frames.

    Even though this agent is almost write-only, something must consume the
    receive side. If nobody reads, frames queue up in memory inside the
    websockets library and a close frame from the Hub goes unnoticed, so the
    send loop would keep sampling into a dead socket.
    """
    async for raw in ws:
        try:
            message = json.loads(raw)
        except (TypeError, ValueError):
            logger.debug("Ignoring non-JSON frame from hub")
            continue

        if not isinstance(message, dict):
            continue

        kind = message.get("type")

        if kind == "welcome":
            logger.info(
                "Hub acknowledged registration (machine_id=%s)",
                message.get("machine_id", "?"),
            )

        elif kind == "config":
            requested = message.get("interval_ms")
            try:
                ms = int(requested)
            except (TypeError, ValueError):
                continue
            # Clamp even though it came from our own Hub: a misconfigured
            # server should not be able to make every agent in the fleet spin
            # at a 1ms tick and peg a core on the machines we are monitoring.
            ms = max(MIN_INTERVAL_MS, min(MAX_INTERVAL_MS, ms))
            session.interval_s = ms / 1000
            logger.info("Hub retuned sampling interval to %dms", ms)

        else:
            logger.debug("Ignoring unknown frame type from hub: %r", kind)


async def _send_loop(ws: Any, collector: MetricsCollector, session: _Session) -> None:
    """Sample and ship, on a drift-corrected schedule."""
    # Absolute deadlines rather than `sleep(interval)`: sampling and JSON
    # encoding both take real time, so a naive sleep would make the effective
    # period interval+work and slowly drift away from a true 1s cadence.
    next_tick = time.monotonic()

    while True:
        # psutil's calls are blocking syscalls; off-thread them so a slow disk
        # stat can't stall this agent's own keepalive pings.
        payload = await asyncio.to_thread(collector.sample)
        await ws.send(json.dumps(payload))

        next_tick += session.interval_s
        delay = next_tick - time.monotonic()
        if delay < 0:
            # We fell behind (busy host, or the interval was just shortened).
            # Resync to now instead of firing a burst of catch-up samples.
            next_tick = time.monotonic()
            delay = 0.0
        await asyncio.sleep(delay)


async def _stream_once(cfg: AgentConfig, collector: MetricsCollector) -> None:
    """One full connection lifecycle: handshake, then stream until it breaks."""
    session = _Session(cfg.interval_s)

    # ping_interval lets the library detect a half-open TCP connection (host
    # sleeps, cable pulled, NAT timeout) instead of blocking forever on a socket
    # the kernel still believes is fine.
    async with connect(cfg.hub_url, ping_interval=20, ping_timeout=20) as ws:
        logger.info("Connected to hub at %s", cfg.hub_url)

        hello = system_info(cfg.agent_key, cfg.agent_name)
        hello["type"] = "hello"
        hello["agent_version"] = __version__
        hello["interval_ms"] = cfg.interval_ms
        await ws.send(json.dumps(hello))

        reader = asyncio.create_task(_read_loop(ws, session), name="agent-reader")
        sender = asyncio.create_task(
            _send_loop(ws, collector, session), name="agent-sender"
        )

        # Whichever task fails first (usually the send raising ConnectionClosed)
        # ends the connection; cancel its sibling and let the caller reconnect.
        #
        # The cleanup lives in `finally` because this agent is normally stopped
        # by cancelling the task that runs it. That CancelledError fires at the
        # `await` below and would otherwise skip straight past the teardown,
        # orphaning both children: the socket then closes underneath them and
        # asyncio reports "Task exception was never retrieved" at GC time --
        # a scary-looking traceback for what is just an ordinary shutdown.
        done: set[asyncio.Task] = set()
        try:
            done, _pending = await asyncio.wait(
                {reader, sender}, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for task in (reader, sender):
                if not task.done():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
                else:
                    # Retrieve the result even for tasks we are discarding, so
                    # no exception goes unobserved.
                    with contextlib.suppress(Exception, asyncio.CancelledError):
                        task.result()

        # Re-raise the first real failure so the retry loop can log the cause.
        for task in done:
            if not task.cancelled() and task.exception() is not None:
                raise task.exception()  # type: ignore[misc]


async def run(cfg: AgentConfig) -> None:
    """Connect, stream, and reconnect forever with exponential backoff."""
    collector = MetricsCollector()
    backoff = cfg.backoff_initial_s
    first_attempt = True

    logger.info(
        "SysFleet agent %s starting: name=%s key=%s interval=%dms",
        __version__,
        cfg.agent_name,
        cfg.agent_key,
        cfg.interval_ms,
    )

    while True:
        try:
            if not first_attempt:
                # There was a gap since the last sample. Re-prime the cumulative
                # counters so the first post-reconnect reading isn't a spike.
                collector.reset_baseline()

            await _stream_once(cfg, collector)
            # A clean return means the Hub closed the socket deliberately.
            logger.info("Hub closed the connection")
            backoff = cfg.backoff_initial_s

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "Connection failed (%s: %s)", type(exc).__name__, exc or "no detail"
            )

        first_attempt = False

        # Jitter spreads the herd: when the Hub restarts, every agent in the
        # fleet is trying to reconnect on the same schedule, and a synchronised
        # retry storm is exactly what a just-booting server can least afford.
        sleep_for = backoff + random.uniform(0, backoff * 0.25)
        logger.info("Reconnecting in %.1fs", sleep_for)
        await asyncio.sleep(sleep_for)
        backoff = min(backoff * 2, cfg.backoff_max_s)


def main() -> None:
    cfg = AgentConfig.from_env()
    try:
        asyncio.run(run(cfg))
    except KeyboardInterrupt:
        logger.info("Agent stopped")


if __name__ == "__main__":
    main()
